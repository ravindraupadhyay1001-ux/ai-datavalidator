# Workspace — How It Works

## Overview

Workspace is a multi-tenant automation platform built into the application. It lets users:

- Save reusable data connections (SFTP, S3, databases, APIs, and more)
- Define scheduled reconciliation and data quality jobs
- Track DQ score trends and reconciliation break-rate trends over time
- Configure SLA thresholds and email alerts
- Maintain a full audit trail of user activity

All state is persisted in a SQLite database (`workspace.db`) by default, with MSSQL support for production deployments (`WORKSPACE_DB=mssql`).

## Architecture

| File | Purpose |
|---|---|
| `workspace/db.py` | Database layer — DDL, CRUD, queries |
| `workspace/auth.py` | Auth middleware and username resolution/RBAC |
| `workspace/sso.py` | LDAP direct-bind auth, OIDC (Authorization Code flow), SAML delegation, role resolution |
| `workspace/saml_sso.py` | SAML 2.0 ACS handling (opt-in, requires `python3-saml`) |
| `workspace/local_auth.py` | Local username/password auth (bcrypt-hashed, session cookies) |
| `workspace/connectors.py` | 21 data source adapters + dynamic date-token substitution |
| `workspace/scheduler.py` | APScheduler integration for cron jobs, fan-out, SLA evaluation |
| `workspace/audit_log.py` | Event logging and SSE streaming |
| `main.py` (`/api/ws/*`, `/login`, `/sso/*`) | REST API endpoints |

## Database Tables

| Table | Purpose |
|---|---|
| `ws_users` | User directory (role, display name, email) |
| `ws_connections` | Saved data source configs (config JSON is base64-encoded, not encrypted — treat `workspace.db` as sensitive); governance metadata: `owner`, `business_domain`, `sensitivity`, `description` |
| `ws_rulesets` | Reusable Data Quality rule sets |
| `ws_jobs` | Scheduled job definitions, including `fan_out_pairs` (bulk jobs), `sla_json` (thresholds), `ai_hints_json` |
| `ws_run_history` | Execution records per job run |
| `ws_saved_runs` | Manually saved analysis snapshots |
| `ws_dq_history` | DQ score snapshots for trend tracking, keyed by schema fingerprint |
| `ws_recon_history` | Reconciliation break-rate snapshots for trend tracking |
| `ws_audit_log` | Full user action trail |
| `ws_token_usage` | LLM token accounting per user/module |
| `ws_local_sessions` | Session tokens for local username/password auth |

## Authentication

**Resolution order** (`workspace/auth.py:_resolve_username`):

1. `WORKSPACE_DEV_USER` env var (explicit dev override)
2. `dv_local_session` cookie (local username/password auth, when `LOCAL_AUTH_ENABLED=true`)
3. `dv_session` cookie (SSO — SAML, OIDC, or LDAP; all three write this same cookie via `create_sso_session_token`)
4. `X-Remote-User` / `Remote-User` / `X-Windows-User` headers (IIS Windows Auth)
5. OS login name fallback (dev only)

**Login front door** (`GET`/`POST /login`): tries local auth first if `LOCAL_AUTH_ENABLED`; otherwise, if `LDAP_ENABLED`, authenticates directly against LDAP/AD (bind as the user to verify the password, then bind as a service account to look up group membership). SAML/OIDC use their own redirect-based flow at `/sso/login`.

**RBAC roles:** `admin` > `analyst` > `readonly`. Roles are resolved from AD/LDAP or SSO group membership via the `SSO_ROLE_MAP` env var (JSON: `{"group-or-claim-value": "role"}`), re-synced on every login **only when `SSO_ROLE_MAP` is configured** — this prevents a manually-promoted role (set via the workspace user admin UI) from being silently reset to `analyst` when no group mapping is configured. New users default to `analyst`, except the very first user ever created, who becomes `admin`.

## Data Connections

21 source types are supported. Each connection stores credentials in base64-encoded JSON config, with passwords never written to logs.

| Category | Sources |
|---|---|
| File transfer | SFTP, FTP |
| Cloud storage | S3, Azure Blob, GCS |
| Databases | MSSQL, Azure SQL, PostgreSQL, MySQL, Oracle, DB2, Snowflake, Databricks |
| APIs / Web | REST API, SharePoint, Salesforce |
| Messaging | Kafka |
| Trading systems | Bloomberg (`blpapi` Session/Request API), Refinitiv (Data Platform OAuth2 REST), Murex (MX.3 REST Connectivity or Datamart DB), Calypso (REST Web Services or reporting DB) |

Murex and Calypso have no single universal API across bank deployments — the built-in connectors assume a REST connectivity layer; where a deployment instead exposes a Datamart reporting database, use the `mssql`/`oracle`/`postgres` connector against that database directly.

### Dynamic date tokens

Any string value in a connection's config (paths, S3 keys, SQL queries, etc.) can reference a token that resolves at fetch time — so a daily EOD feed never needs manual editing:

| Token | Resolves to |
|---|---|
| `{YYYYMMDD}` | Today, e.g. `20260712` |
| `{D-1}` / `{D+1}` | Yesterday / tomorrow, same format |
| `{YYYYMMDD-2}` | Two days ago (any `+N`/`-N` offset works) |
| `{YYYY}` | Current year |
| `{strftime:%Y-%m-%d}` | Arbitrary `strftime` format, optionally with a `+N`/`-N` day offset suffix |

Example: `"remote_path": "/feeds/trades_{YYYYMMDD}.csv"`. Applied centrally in `BaseConnector.__init__`, so every connector type supports it automatically.

## Jobs & Scheduling

Jobs define a recurring analysis: which connections to use, what action to run, when to run it, and where to send results.

| Field | Purpose |
|---|---|
| `action` | `compare`, `quality`, `profile`, `governance`, `lineage`, `parse`, `xref` |
| `conn_a_id`, `conn_b_id` | Source and target connections (compare) |
| `source_conn_id` | Single source connection (quality/profile/etc.) |
| `key_columns` / `exclude_columns` | Join/match columns to use or skip |
| `ruleset_id` | Linked validation ruleset |
| `schedule_cron` | 5-field UTC cron (e.g. `0 8 * * 1-5`); the Jobs UI offers 1m/2m/5m/15m/1h/daily presets for fast-interval polling |
| `notify_email` | Comma-separated email recipients |
| `sla_json` | SLA thresholds (see below) |
| `fan_out_pairs` | Optional list of `{conn_a_id, conn_b_id, label}` — when set, the same compare logic runs across every pair on one schedule, aggregated into one run |

**Near real-time reconciliation:** pair a Kafka connection (with a `group_id` set, so each poll only reads new messages since the last one — Kafka's own consumer-group offset tracking handles this) with a 1–5 minute cron schedule for continuous streaming reconciliation.

### Scheduler lifecycle (`workspace/scheduler.py`)

1. On startup: `load_all_jobs()` registers every cron-enabled job with APScheduler
2. At cron time: `_execute_job(job_id, username)` runs the full pipeline — fetch → analyze → evaluate SLA → conditionally email → record run history
3. On error: job status set to `error`; the run is recorded as `failed` with the error message, without crashing the scheduler

### SLA Thresholds

Evaluated after every run via `_evaluate_sla(sla, result)`:

| Threshold | Meaning |
|---|---|
| `max_breaks` | Maximum allowed reconciliation breaks (file1_only + file2_only + modified) |
| `min_dq_score` | Minimum acceptable DQ score (0–100) |
| `max_null_pct` | Maximum null percentage per column |
| `alert_on_schema_drift` | Alert if the column schema changed since the last run (reuses `analyze_quality()`'s own drift detection against its stored baseline) |
| `alert_only_on_fail` | Suppress the routine success email; only send when a threshold is actually breached |

A breach prepends a red "SLA BREACH" banner to the emailed HTML report and prefixes the subject line.

## DQ Score Trend Tracking

Every Data Quality run writes a snapshot to `ws_dq_history`, keyed by schema fingerprint. This enables:

- A "vs. baseline" delta banner and a break-rate/DQ-score trend line chart in the UI
- Drift detection: if the schema fingerprint changes between runs, `analyze_quality()` flags it in `schema_drift`

Reconciliation has the equivalent via `ws_recon_history` — a 30-day break-rate trend and baseline comparison.

## Audit Log & Event Streaming

Every user action is logged to `ws_audit_log` with timestamp, username, action, and a free-text detail string (created/updated/deleted/triggered — connections, rulesets, jobs, run results).

**Real-time SSE stream** at `GET /api/logs/stream`: connected clients receive events as they happen; a keep-alive ping is sent periodically.

## REST API Endpoints

All endpoints are under `/api/ws/*` and require authentication (resolved per the order above).

### Connections
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/ws/connections` | List all connections |
| POST | `/api/ws/connections` | Create or update a connection |
| GET | `/api/ws/connections/{id}` | Fetch connection details (password fields masked) |
| DELETE | `/api/ws/connections/{id}` | Delete connection |
| POST | `/api/ws/connections/{id}/test` | Validate connectivity |
| GET | `/api/ws/connections/{id}/preview` | Preview a sample of the data |

### Rulesets
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/ws/rulesets` | List all rulesets |
| POST | `/api/ws/rulesets` | Create or update a ruleset |
| GET | `/api/ws/rulesets/{id}` | Fetch ruleset with full rules array |
| DELETE | `/api/ws/rulesets/{id}` | Delete ruleset |

### Jobs
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/ws/jobs` | List all jobs |
| POST | `/api/ws/jobs` | Create or update a job |
| DELETE | `/api/ws/jobs/{id}` | Delete job and its history |
| POST | `/api/ws/jobs/{id}/run` | Trigger job immediately |
| GET | `/api/ws/jobs/{id}/runs` | Get run history |
| GET | `/api/ws/jobs/{id}/sla` | Get saved SLA thresholds |
| POST | `/api/ws/jobs/ai-suggest` | LLM-assisted job/SLA configuration suggestion |

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `WORKSPACE_DB` | `sqlite` | Backend: `sqlite` or `mssql` |
| `WORKSPACE_SQLITE_PATH` | `workspace.db` | SQLite file location |
| `MSSQL_SERVER` / `MSSQL_DATABASE` | (unset) / `WorkspaceDB` | MSSQL host/database name |
| `WORKSPACE_DEV_USER` | (unset) | Force a username for local dev |
| `LOCAL_AUTH_ENABLED` | `false` | Enable local username/password login (session length is a fixed 8 hours, `workspace/local_auth.py:_SESSION_HOURS`) |
| `JWT_SECRET` | `change-this-in-production` | Signing key for local-auth and SSO session tokens — **must** be overridden in production |
| `LDAP_ENABLED` | `false` | Enable LDAP/AD direct-bind login |
| `LDAP_SERVER` | (unset) | e.g. `ldap://dc.corp.com` |
| `LDAP_BASE_DN` | (unset) | e.g. `DC=corp,DC=com` |
| `LDAP_BIND_DN` / `LDAP_BIND_PASSWORD` | (unset) | Service account for group lookup |
| `SSO_ROLE_MAP` | `{}` | JSON mapping of AD group / SSO claim value → app role |
| `OIDC_ENABLED` / `OIDC_ISSUER` / `OIDC_CLIENT_ID` / `OIDC_CLIENT_SECRET` / `OIDC_REDIRECT_URI` | (unset) | OIDC Authorization Code flow config |
| `SAML_*` | (unset) | SAML config — see `workspace/saml_sso.py` (requires `python3-saml`) |

## Security

- Passwords in connection configs are base64-encoded at rest, never logged (obfuscation, not encryption — treat the DB file as sensitive)
- All SQL queries are parameterized (no string concatenation)
- RBAC enforced on every sensitive endpoint via `require_role()`
- Local auth sessions are signed JWTs tied to username, with a fixed 8-hour expiry
- Windows Auth / LDAP / SSO integration means no passwords need to be stored locally in production deployments that use them
