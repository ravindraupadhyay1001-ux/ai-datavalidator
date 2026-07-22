"""
Workspace database layer — SQLite (default) or MSSQL.

Env vars:
  WORKSPACE_DB           sqlite | mssql           (default: sqlite)
  WORKSPACE_SQLITE_PATH  path to .db file         (default: workspace.db)
  MSSQL_SERVER           server name              (if mssql)
  MSSQL_DATABASE         database name            (default: WorkspaceDB)

All CRUD functions filter by username — no user can see another user's data.
config_json / rules_json / sources are stored as base64-encoded JSON
(obfuscation of secrets at rest, not encryption).
"""

import base64
import json
import os
import threading
from datetime import datetime, timezone, timedelta

_DB_KIND = os.getenv("WORKSPACE_DB", "sqlite").lower()
_SQLITE_PATH = os.getenv("WORKSPACE_SQLITE_PATH", "workspace.db")
_MSSQL_SERVER = os.getenv("MSSQL_SERVER", "")
_MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "WorkspaceDB")
# Comma-separated usernames that should always be admin, regardless of
# registration order -- set once as a Railway env var so a specific person
# doesn't depend on being the first to ever log in / can be restored to admin
# after an accidental demotion. Re-applied every time ensure_user() runs.
_FORCED_ADMINS = {
    u.strip().lower() for u in os.getenv("WORKSPACE_ADMIN_USERS", "").split(",") if u.strip()
}

_local = threading.local()


# --------------------------------------------------------------------------
# Connection handling
# --------------------------------------------------------------------------
def _is_mssql() -> bool:
    return _DB_KIND == "mssql"


def _conn():
    """Thread-local connection."""
    existing = getattr(_local, "conn", None)
    if existing is not None:
        return existing
    if _is_mssql():
        import pyodbc
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={_MSSQL_SERVER};DATABASE={_MSSQL_DATABASE};"
            f"Trusted_Connection=yes;"
        )
    else:
        import sqlite3
        # Ensure the parent dir exists so WORKSPACE_SQLITE_PATH can point at a
        # fresh mounted volume (e.g. /data/workspace.db) without a manual mkdir.
        _db_dir = os.path.dirname(_SQLITE_PATH)
        if _db_dir:
            os.makedirs(_db_dir, exist_ok=True)
        conn = sqlite3.connect(_SQLITE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
    _local.conn = conn
    return conn


def _ph() -> str:
    """Parameter placeholder."""
    return "?"  # both sqlite3 and pyodbc use ?


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))


def _trial_expiry() -> str:
    """Default access-expiry date for a new non-admin user: today + TRIAL_DAYS."""
    return (datetime.now(timezone.utc) + timedelta(days=TRIAL_DAYS)).date().isoformat()


def _b64(obj) -> str:
    return base64.b64encode(json.dumps(obj, default=str).encode("utf-8")).decode("ascii")


def _unb64(s):
    if not s:
        return None
    try:
        return json.loads(base64.b64decode(s).decode("utf-8"))
    except Exception:
        return None


def _rows(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------
_DDL = [
    """CREATE TABLE IF NOT EXISTS ws_users (
        username TEXT PRIMARY KEY,
        display_name TEXT,
        email TEXT,
        role TEXT DEFAULT 'analyst',
        created_at TEXT
    )""",
    # migration for pre-existing databases created before the role column existed
    "ALTER TABLE ws_users ADD COLUMN role TEXT DEFAULT 'analyst'",
    # migration for pre-existing databases created before local username/password auth existed
    "ALTER TABLE ws_users ADD COLUMN password_hash TEXT",
    # Subscription / free-trial: ISO date (YYYY-MM-DD) the user's access ends.
    # NULL = unlimited (admins, and pre-existing users grandfathered in).
    "ALTER TABLE ws_users ADD COLUMN access_expiry TEXT",
    """CREATE TABLE IF NOT EXISTS ws_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        name TEXT NOT NULL,
        source_type TEXT NOT NULL,
        config_json TEXT,
        created_at TEXT,
        updated_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_rulesets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        rules_json TEXT,
        created_at TEXT,
        updated_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        name TEXT NOT NULL,
        action TEXT NOT NULL,
        source_conn_id INTEGER,
        conn_a_id INTEGER,
        conn_b_id INTEGER,
        key_columns TEXT,
        exclude_columns TEXT,
        ruleset_id INTEGER,
        schedule_cron TEXT,
        from_email TEXT,
        notify_email TEXT,
        status TEXT,
        last_run_at TEXT,
        next_run_at TEXT,
        created_at TEXT,
        updated_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_run_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        username TEXT,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        summary_json TEXT,
        error_msg TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_saved_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        name TEXT,
        action TEXT,
        sources TEXT,
        session_id TEXT,
        summary_json TEXT,
        key_columns TEXT,
        created_at TEXT,
        saved_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        action TEXT,
        detail TEXT,
        session_id TEXT,
        created_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ws_dq_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        username TEXT,
        score REAL,
        grade TEXT,
        completeness REAL,
        uniqueness REAL,
        validity REAL,
        run_at TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_dq_file ON ws_dq_history(file_name)",
    "CREATE INDEX IF NOT EXISTS idx_dq_user ON ws_dq_history(username)",
    # migrations for pre-existing databases created before these columns existed
    "ALTER TABLE ws_dq_history ADD COLUMN schema_fingerprint TEXT",
    "ALTER TABLE ws_dq_history ADD COLUMN total_rows INTEGER",
    "ALTER TABLE ws_dq_history ADD COLUMN rule_fails INTEGER",
    "ALTER TABLE ws_dq_history ADD COLUMN crit_fails INTEGER",
    "ALTER TABLE ws_dq_history ADD COLUMN session_id TEXT",
    "ALTER TABLE ws_dq_history ADD COLUMN bfsi_pack TEXT",
    "ALTER TABLE ws_dq_history ADD COLUMN di_scope TEXT",
    "ALTER TABLE ws_saved_runs ADD COLUMN conn_a_id INTEGER",
    "ALTER TABLE ws_saved_runs ADD COLUMN conn_b_id INTEGER",
    "ALTER TABLE ws_saved_runs ADD COLUMN source_conn_id INTEGER",
    # Email outcome per run -- a background-run job can't return this value
    # to whoever triggered it (no one's still waiting on it), so it has to be
    # persisted for the "Run Now" button to poll and show the real reason.
    "ALTER TABLE ws_run_history ADD COLUMN email_sent INTEGER",
    "ALTER TABLE ws_run_history ADD COLUMN email_skipped_reason TEXT",
    "ALTER TABLE ws_run_history ADD COLUMN email_error TEXT",
    """CREATE TABLE IF NOT EXISTS ws_recon_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_label TEXT,
        username TEXT,
        schema_fingerprint TEXT,
        session_id TEXT,
        matched_count INTEGER,
        file1_only_count INTEGER,
        file2_only_count INTEGER,
        modified_count INTEGER,
        total_rows INTEGER,
        break_rate REAL,
        status TEXT,
        method TEXT,
        run_at TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_recon_fp ON ws_recon_history(schema_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_recon_user ON ws_recon_history(username)",
    # Bulk/fan-out jobs: run the same compare logic across many connection
    # pairs at once (e.g. one per branch/region) instead of a single pair.
    # NULL/empty means "ordinary single-pair job" -- fully backward compatible.
    "ALTER TABLE ws_jobs ADD COLUMN fan_out_pairs TEXT",
    # SLA thresholds and AI-suggested-schedule hints -- the /api/ws/jobs route
    # already parsed these from the request body but had nowhere to put them.
    "ALTER TABLE ws_jobs ADD COLUMN sla_json TEXT",
    "ALTER TABLE ws_jobs ADD COLUMN ai_hints_json TEXT",
    # Cross Reference (xref) scheduled jobs: 2-5 source connections, stored as
    # base64 JSON [{"conn_id":.., "label":..}, ...] -- same encoding as
    # fan_out_pairs above, since a job needs more sources than the single
    # conn_a_id/conn_b_id pair supports.
    "ALTER TABLE ws_jobs ADD COLUMN xref_sources TEXT",
    # Governance metadata for connections -- /api/ws/connections already parsed
    # these from the request body but had nowhere to put them.
    "ALTER TABLE ws_connections ADD COLUMN owner TEXT",
    "ALTER TABLE ws_connections ADD COLUMN business_domain TEXT",
    "ALTER TABLE ws_connections ADD COLUMN sensitivity TEXT",
    "ALTER TABLE ws_connections ADD COLUMN description TEXT",
    # Admin user management -- block a user without deleting their data, and
    # show when they were last seen in the admin panel.
    "ALTER TABLE ws_users ADD COLUMN is_blocked INTEGER DEFAULT 0",
    "ALTER TABLE ws_users ADD COLUMN last_active TEXT",
    """CREATE TABLE IF NOT EXISTS ws_token_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        module TEXT,
        call_type TEXT,
        input_tokens INTEGER,
        output_tokens INTEGER,
        model TEXT,
        created_at TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_token_user ON ws_token_usage(username)",
    # Admin-controlled module visibility -- single-row table (id=1) holding a
    # comma-separated list of nav data-tab values hidden from non-admin
    # users while a module is still in development. Deliberately not in the
    # .env-backed /api/settings mechanism -- that requires an app restart to
    # take effect, which is a poor fit for something toggled live.
    """CREATE TABLE IF NOT EXISTS ws_app_config (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        hidden_modules TEXT,
        updated_at TEXT
    )""",
]


def init_db():
    conn = _conn()
    cur = conn.cursor()
    for stmt in _DDL:
        if _is_mssql():
            stmt = stmt.replace(
                "INTEGER PRIMARY KEY AUTOINCREMENT",
                "INT IDENTITY(1,1) PRIMARY KEY",
            ).replace("IF NOT EXISTS ", "")
        try:
            cur.execute(stmt)
        except Exception:
            pass
    conn.commit()


def ensure_user(username, display_name=None, email=None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"SELECT username FROM ws_users WHERE username={_ph()}", (username,))
    if not cur.fetchall():
        # No one is admin by default, not even the very first account --
        # WORKSPACE_ADMIN_USERS is the only way to get admin access, an
        # admin then promotes everyone else from the Users & Roles panel.
        # Brand-new accounts start as readonly ("Run Only") until promoted.
        cur.execute("SELECT COUNT(*) FROM ws_users")
        is_first = list(cur.fetchall())[0][0] == 0
        role = "admin" if username.lower() in _FORCED_ADMINS else ("readonly" if is_first else "analyst")
        # Non-admins get a free trial by default; admins are unlimited (NULL).
        expiry = None if role == "admin" else _trial_expiry()
        cur.execute(
            f"INSERT INTO ws_users (username, display_name, email, role, created_at, access_expiry) "
            f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
            (username, display_name or username, email or "", role, _now(), expiry),
        )
        conn.commit()
    elif username.lower() in _FORCED_ADMINS:
        cur.execute(
            f"UPDATE ws_users SET role='admin' WHERE username={_ph()} AND role!='admin'",
            (username,),
        )
        conn.commit()


def get_user_role(username) -> str:
    cur = _conn().cursor()
    cur.execute(f"SELECT role FROM ws_users WHERE username={_ph()}", (username,))
    rows = _rows(cur)
    return (rows[0].get("role") or "analyst") if rows else "analyst"


def set_user_role(username, role):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_users SET role={_ph()} WHERE username={_ph()}", (role, username))
    conn.commit()


def get_hidden_modules() -> list[str]:
    """Global (not per-user) list of nav data-tab values hidden from
    non-admin users -- admin-controlled via POST /api/ws/app-config."""
    cur = _conn().cursor()
    cur.execute("SELECT hidden_modules FROM ws_app_config WHERE id=1")
    rows = _rows(cur)
    val = rows[0].get("hidden_modules") if rows else None
    return [m.strip() for m in val.split(",") if m.strip()] if val else []


def set_hidden_modules(modules: list[str]) -> None:
    conn = _conn()
    cur = conn.cursor()
    val = ",".join(modules)
    cur.execute("SELECT id FROM ws_app_config WHERE id=1")
    if _rows(cur):
        cur.execute(
            f"UPDATE ws_app_config SET hidden_modules={_ph()}, updated_at={_ph()} WHERE id=1",
            (val, _now()),
        )
    else:
        cur.execute(
            f"INSERT INTO ws_app_config (id, hidden_modules, updated_at) VALUES (1,{_ph()},{_ph()})",
            (val, _now()),
        )
    conn.commit()


def list_users():
    cur = _conn().cursor()
    cur.execute(
        "SELECT username, display_name, email, role, created_at, last_active, "
        "access_expiry, COALESCE(is_blocked, 0) AS is_blocked FROM ws_users ORDER BY created_at ASC"
    )
    return _rows(cur)


def get_user_access_expiry(username):
    cur = _conn().cursor()
    cur.execute(f"SELECT access_expiry FROM ws_users WHERE username={_ph()}", (username,))
    rows = _rows(cur)
    return (rows[0].get("access_expiry") if rows else None) or None


def set_user_access_expiry(username, expiry):
    """expiry: 'YYYY-MM-DD' string, or None/'' to clear (unlimited)."""
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_users SET access_expiry={_ph()} WHERE username={_ph()}",
        ((expiry or None), username),
    )
    conn.commit()


def subscription_status(username, role=None):
    """Returns {active, expiry, days_left, state} for a user. Admins and users
    with no expiry set are unlimited. state: unlimited | active | expired."""
    from datetime import date
    if role is None:
        role = get_user_role(username)
    expiry = get_user_access_expiry(username)
    if role == "admin" or not expiry:
        return {"active": True, "expiry": expiry, "days_left": None, "state": "unlimited"}
    try:
        exp = date.fromisoformat(expiry[:10])
    except Exception:
        return {"active": True, "expiry": expiry, "days_left": None, "state": "unlimited"}
    days = (exp - date.today()).days
    return {
        "active": days >= 0,
        "expiry": exp.isoformat(),
        "days_left": days,
        "state": "active" if days >= 0 else "expired",
    }


def get_user_by_username_or_email(identifier):
    """Look up a user by exact username or exact email match (case-insensitive
    on email) -- used by the forgot-password/forgot-username flow."""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT username, display_name, email FROM ws_users "
        f"WHERE username={_ph()} OR LOWER(email)={_ph()}",
        (identifier, identifier.lower()),
    )
    rows = _rows(cur)
    return rows[0] if rows else None


def touch_last_active(username):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_users SET last_active={_ph()} WHERE username={_ph()}", (_now(), username))
    conn.commit()


def is_user_blocked(username) -> bool:
    cur = _conn().cursor()
    cur.execute(f"SELECT is_blocked FROM ws_users WHERE username={_ph()}", (username,))
    rows = _rows(cur)
    return bool(rows[0].get("is_blocked")) if rows else False


def set_user_blocked(username, blocked: bool):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_users SET is_blocked={_ph()} WHERE username={_ph()}",
        (1 if blocked else 0, username),
    )
    conn.commit()


def delete_user(username):
    """Remove a user and every piece of data scoped to them across the
    workspace tables. Feedback-store (Dataset Memory) rules are not touched
    here -- those live in a separate JSON store, see agent.feedback_store."""
    conn = _conn()
    cur = conn.cursor()
    for table in (
        "ws_connections", "ws_rulesets", "ws_jobs", "ws_run_history",
        "ws_saved_runs", "ws_dq_history", "ws_recon_history",
        "ws_token_usage", "ws_audit_log", "ws_users",
    ):
        try:
            cur.execute(f"DELETE FROM {table} WHERE username={_ph()}", (username,))
        except Exception:
            pass
    conn.commit()


def purge_user_data(username):
    """Wipe a user's workspace DATA (connections, jobs, saved runs, history,
    rulesets, token usage) but KEEP the account row and the audit trail --
    for cleaning up a no-longer-active user without deleting the account.
    Dataset Memory rules live in a separate JSON store; the caller clears
    those via agent.feedback_store (delete_user_data)."""
    conn = _conn()
    cur = conn.cursor()
    for table in (
        "ws_connections", "ws_rulesets", "ws_jobs", "ws_run_history",
        "ws_saved_runs", "ws_dq_history", "ws_recon_history", "ws_token_usage",
    ):
        try:
            cur.execute(f"DELETE FROM {table} WHERE username={_ph()}", (username,))
        except Exception:
            pass
    conn.commit()


def count_local_users() -> int:
    """Count of users who have actually registered via local username/password
    auth (as opposed to being auto-provisioned by Windows Auth / SSO)."""
    cur = _conn().cursor()
    cur.execute("SELECT COUNT(*) FROM ws_users WHERE password_hash IS NOT NULL")
    return list(cur.fetchall())[0][0]


def get_user_password_hash(username):
    cur = _conn().cursor()
    cur.execute(f"SELECT password_hash FROM ws_users WHERE username={_ph()}", (username,))
    rows = _rows(cur)
    return rows[0].get("password_hash") if rows else None


def create_local_user(username, password_hash, full_name="", email=""):
    """Register a new username/password user. Raises ValueError if the
    username is already taken. No one is admin by default -- see
    WORKSPACE_ADMIN_USERS in ensure_user(). The very first local account
    starts as readonly ("Run Only") just like everyone else; an admin
    promotes it from the Users & Roles panel."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"SELECT username FROM ws_users WHERE username={_ph()}", (username,))
    if cur.fetchall():
        raise ValueError(f"Username '{username}' is already taken.")
    is_first = count_local_users() == 0
    role = "admin" if username.lower() in _FORCED_ADMINS else ("readonly" if is_first else "analyst")
    expiry = None if role == "admin" else _trial_expiry()
    cur.execute(
        f"INSERT INTO ws_users (username, display_name, email, role, created_at, password_hash, access_expiry) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, full_name or username, email or "", role, _now(), password_hash, expiry),
    )
    conn.commit()


def set_user_password_hash(username, password_hash):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_users SET password_hash={_ph()} WHERE username={_ph()}",
        (password_hash, username),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------------
def list_connections(username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT id, name, source_type, owner, business_domain, sensitivity, "
        f"description, created_at, updated_at "
        f"FROM ws_connections WHERE username={_ph()} ORDER BY name",
        (username,),
    )
    return _rows(cur)


def get_connection(conn_id, username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT * FROM ws_connections WHERE id={_ph()} AND username={_ph()}",
        (conn_id, username),
    )
    rows = _rows(cur)
    if not rows:
        return None
    row = rows[0]
    row["config"] = _unb64(row.pop("config_json", None)) or {}
    return row


def save_connection(username, name, source_type, config, conn_id=None,
                     owner=None, business_domain=None, sensitivity=None, description=None):
    conn = _conn()
    cur = conn.cursor()
    if conn_id:
        cur.execute(
            f"UPDATE ws_connections SET name={_ph()}, source_type={_ph()}, "
            f"config_json={_ph()}, owner={_ph()}, business_domain={_ph()}, "
            f"sensitivity={_ph()}, description={_ph()}, updated_at={_ph()} "
            f"WHERE id={_ph()} AND username={_ph()}",
            (name, source_type, _b64(config), owner, business_domain,
             sensitivity, description, _now(), conn_id, username),
        )
        conn.commit()
        return conn_id
    cur.execute(
        f"INSERT INTO ws_connections "
        f"(username, name, source_type, config_json, owner, business_domain, "
        f"sensitivity, description, created_at, updated_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, source_type, _b64(config), owner, business_domain,
         sensitivity, description, _now(), _now()),
    )
    conn.commit()
    return cur.lastrowid


def delete_connection(conn_id, username):
    conn = _conn()
    conn.cursor().execute(
        f"DELETE FROM ws_connections WHERE id={_ph()} AND username={_ph()}",
        (conn_id, username),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Rulesets
# --------------------------------------------------------------------------
def list_rulesets(username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT id, name, description, created_at, updated_at "
        f"FROM ws_rulesets WHERE username={_ph()} ORDER BY name",
        (username,),
    )
    return _rows(cur)


def get_ruleset(rs_id, username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT * FROM ws_rulesets WHERE id={_ph()} AND username={_ph()}",
        (rs_id, username),
    )
    rows = _rows(cur)
    if not rows:
        return None
    row = rows[0]
    row["rules"] = _unb64(row.pop("rules_json", None)) or []
    return row


def save_ruleset(username, name, description, rules, rs_id=None):
    conn = _conn()
    cur = conn.cursor()
    if rs_id:
        cur.execute(
            f"UPDATE ws_rulesets SET name={_ph()}, description={_ph()}, "
            f"rules_json={_ph()}, updated_at={_ph()} "
            f"WHERE id={_ph()} AND username={_ph()}",
            (name, description, _b64(rules), _now(), rs_id, username),
        )
        conn.commit()
        return rs_id
    cur.execute(
        f"INSERT INTO ws_rulesets "
        f"(username, name, description, rules_json, created_at, updated_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, description, _b64(rules), _now(), _now()),
    )
    conn.commit()
    return cur.lastrowid


def delete_ruleset(rs_id, username):
    conn = _conn()
    conn.cursor().execute(
        f"DELETE FROM ws_rulesets WHERE id={_ph()} AND username={_ph()}",
        (rs_id, username),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Jobs + run history
# --------------------------------------------------------------------------
def list_jobs(username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT * FROM ws_jobs WHERE username={_ph()} ORDER BY name", (username,)
    )
    return _rows(cur)


def get_job(job_id, username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT * FROM ws_jobs WHERE id={_ph()} AND username={_ph()}",
        (job_id, username),
    )
    rows = _rows(cur)
    if not rows:
        return None
    row = rows[0]
    row["fan_out_pairs"] = _unb64(row.get("fan_out_pairs")) or []
    row["xref_sources"] = _unb64(row.get("xref_sources")) or []
    row["sla"] = json.loads(row["sla_json"]) if row.get("sla_json") else {}
    row["ai_hints"] = json.loads(row["ai_hints_json"]) if row.get("ai_hints_json") else {}
    return row


def get_job_sla(job_id, username):
    job = get_job(job_id, username)
    return job.get("sla", {}) if job else {}


def save_job(username, name, action, source_conn_id=None, conn_a_id=None,
             conn_b_id=None, key_columns=None, exclude_columns=None,
             ruleset_id=None, schedule_cron=None, from_email=None,
             notify_email=None, job_id=None, fan_out_pairs=None,
             sla_json=None, ai_hints_json=None, xref_sources=None):
    """fan_out_pairs: optional list of {"conn_a_id":..,"conn_b_id":..,"label":..}
    dicts -- when non-empty, the scheduler runs the same compare logic across
    every pair instead of just conn_a_id/conn_b_id (which stay as a fallback
    single pair for callers/UI that don't know about fan-out). xref_sources:
    optional list of {"conn_id":..,"label":..} dicts, 2-5 entries, for a
    scheduled Cross Reference job (N-way, more sources than conn_a_id/
    conn_b_id can hold). sla_json/ai_hints_json are pre-serialised JSON
    strings (or None), passed straight through -- the caller already has
    dicts and is responsible for the dumps."""
    conn = _conn()
    cur = conn.cursor()
    fan_out_json = _b64(fan_out_pairs) if fan_out_pairs else None
    xref_sources_json = _b64(xref_sources) if xref_sources else None
    if job_id:
        cur.execute(
            f"UPDATE ws_jobs SET name={_ph()}, action={_ph()}, source_conn_id={_ph()}, "
            f"conn_a_id={_ph()}, conn_b_id={_ph()}, key_columns={_ph()}, "
            f"exclude_columns={_ph()}, ruleset_id={_ph()}, schedule_cron={_ph()}, "
            f"from_email={_ph()}, notify_email={_ph()}, fan_out_pairs={_ph()}, "
            f"sla_json={_ph()}, ai_hints_json={_ph()}, xref_sources={_ph()}, updated_at={_ph()} "
            f"WHERE id={_ph()} AND username={_ph()}",
            (name, action, source_conn_id, conn_a_id, conn_b_id, key_columns,
             exclude_columns, ruleset_id, schedule_cron, from_email,
             notify_email, fan_out_json, sla_json, ai_hints_json, xref_sources_json,
             _now(), job_id, username),
        )
        conn.commit()
        return job_id
    cur.execute(
        f"INSERT INTO ws_jobs (username, name, action, source_conn_id, conn_a_id, "
        f"conn_b_id, key_columns, exclude_columns, ruleset_id, schedule_cron, "
        f"from_email, notify_email, fan_out_pairs, sla_json, ai_hints_json, "
        f"xref_sources, status, created_at, updated_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},"
        f"{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, action, source_conn_id, conn_a_id, conn_b_id,
         key_columns, exclude_columns, ruleset_id, schedule_cron, from_email,
         notify_email, fan_out_json, sla_json, ai_hints_json, xref_sources_json,
         "active", _now(), _now()),
    )
    conn.commit()
    return cur.lastrowid


def update_job_status(job_id, status, last_run_at=None):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_jobs SET status={_ph()}, last_run_at={_ph()} WHERE id={_ph()}",
        (status, last_run_at or _now(), job_id),
    )
    conn.commit()


def delete_job(job_id, username):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"DELETE FROM ws_run_history WHERE job_id={_ph()}", (job_id,))
    cur.execute(
        f"DELETE FROM ws_jobs WHERE id={_ph()} AND username={_ph()}",
        (job_id, username),
    )
    conn.commit()


def create_run(job_id, username):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO ws_run_history (job_id, username, started_at, status) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()})",
        (job_id, username, _now(), "running"),
    )
    conn.commit()
    return cur.lastrowid


def finish_run(run_id, status, summary=None, error_msg=None,
                email_sent=None, email_skipped_reason=None, email_error=None):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_run_history SET finished_at={_ph()}, status={_ph()}, "
        f"summary_json={_ph()}, error_msg={_ph()}, email_sent={_ph()}, "
        f"email_skipped_reason={_ph()}, email_error={_ph()} WHERE id={_ph()}",
        (_now(), status, _b64(summary) if summary else None, error_msg,
         1 if email_sent else 0 if email_sent is not None else None,
         email_skipped_reason, email_error, run_id),
    )
    conn.commit()


def _decode_run_summary(row):
    # summary_json is stored base64-encoded (like config_json/rules_json
    # elsewhere) -- re-serialise it back to a plain JSON string so callers
    # (the History table's `JSON.parse(r.summary_json)`) get real JSON
    # instead of a base64 blob that silently fails to parse.
    raw = row.get("summary_json")
    if raw:
        decoded = _unb64(raw)
        row["summary_json"] = json.dumps(decoded) if decoded is not None else None
    return row


def list_runs(username, job_id=None, limit=50):
    cur = _conn().cursor()
    if job_id:
        cur.execute(
            f"SELECT * FROM ws_run_history WHERE username={_ph()} AND job_id={_ph()} "
            f"ORDER BY id DESC",
            (username, job_id),
        )
    else:
        cur.execute(
            f"SELECT * FROM ws_run_history WHERE username={_ph()} ORDER BY id DESC",
            (username,),
        )
    return [_decode_run_summary(r) for r in _rows(cur)[:limit]]


def get_run(run_id, username):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT * FROM ws_run_history WHERE id={_ph()} AND username={_ph()}",
        (run_id, username),
    )
    rows = _rows(cur)
    return _decode_run_summary(rows[0]) if rows else None


# --------------------------------------------------------------------------
# Saved runs
# --------------------------------------------------------------------------
def save_manual_run(username, name, action, sources, session_id,
                    summary, key_columns=None, conn_a_id=None, conn_b_id=None,
                    source_conn_id=None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO ws_saved_runs "
        f"(username, name, action, sources, session_id, summary_json, "
        f"key_columns, conn_a_id, conn_b_id, source_conn_id, created_at, saved_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, action, _b64(sources), session_id, _b64(summary),
         key_columns, conn_a_id, conn_b_id, source_conn_id, _now(), _now()),
    )
    conn.commit()
    return cur.lastrowid


def list_saved_runs(username, limit=100, source_conn_id=None, conn_a_id=None, conn_b_id=None):
    """source_conn_id / conn_a_id+conn_b_id let a caller narrow the list to
    saved runs that used the same connector(s) as a given job -- there's no
    direct job_id link (a saved run isn't necessarily tied to any job), but
    matching on connector is a meaningful, honest proxy for "runs related to
    this job" rather than a raw, unfiltered dump of every saved run ever."""
    cur = _conn().cursor()
    clauses, params = [f"username={_ph()}"], [username]
    if source_conn_id is not None:
        clauses.append(f"source_conn_id={_ph()}")
        params.append(source_conn_id)
    if conn_a_id is not None and conn_b_id is not None:
        clauses.append(f"conn_a_id={_ph()} AND conn_b_id={_ph()}")
        params.extend([conn_a_id, conn_b_id])
    cur.execute(
        f"SELECT id, name, action, session_id, key_columns, saved_at, "
        f"conn_a_id, conn_b_id, source_conn_id "
        f"FROM ws_saved_runs WHERE {' AND '.join(clauses)} ORDER BY id DESC",
        params,
    )
    return _rows(cur)[:limit]


def delete_saved_run(run_id, username):
    conn = _conn()
    conn.cursor().execute(
        f"DELETE FROM ws_saved_runs WHERE id={_ph()} AND username={_ph()}",
        (run_id, username),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Audit log
# --------------------------------------------------------------------------
def insert_audit(username, action, detail, session_id=None):
    conn = _conn()
    conn.cursor().execute(
        f"INSERT INTO ws_audit_log (username, action, detail, session_id, created_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, action, detail, session_id, _now()),
    )
    conn.commit()


def list_audit(username=None, action=None, limit=200):
    """Most recent audit log entries, optionally filtered by username/action."""
    clauses, params = [], []
    if username:
        clauses.append(f"username={_ph()}")
        params.append(username)
    if action:
        clauses.append(f"action={_ph()}")
        params.append(action)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT username, action, detail, session_id, created_at FROM ws_audit_log "
        f"{where} ORDER BY created_at DESC LIMIT {int(limit)}",
        params,
    )
    return _rows(cur)


# --------------------------------------------------------------------------
# DQ score history
# --------------------------------------------------------------------------
def save_dq_history(username, file_name, schema_fingerprint, dq_score, total_rows,
                    rule_fails, crit_fails, session_id=None, bfsi_pack=None, di_scope=None):
    """Persist a full DQ run's score breakdown for trend tracking. dq_score is
    the dict returned by _dq_score() (score, grade, completeness, uniqueness,
    validity, ...)."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO ws_dq_history "
        f"(file_name, username, score, grade, completeness, uniqueness, validity, "
        f"schema_fingerprint, total_rows, rule_fails, crit_fails, session_id, "
        f"bfsi_pack, di_scope, run_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},"
        f"{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (file_name, username, dq_score.get("score"), dq_score.get("grade"),
         dq_score.get("completeness"), dq_score.get("uniqueness"), dq_score.get("validity"),
         schema_fingerprint, total_rows, rule_fails, crit_fails, session_id,
         bfsi_pack, di_scope, _now()),
    )
    conn.commit()
    return cur.lastrowid


def get_dq_history(file_name, username, days=30):
    """DQ score trend for a file, most recent `days` days, oldest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cur = _conn().cursor()
    cur.execute(
        f"SELECT file_name, score, grade, completeness, uniqueness, validity, run_at "
        f"FROM ws_dq_history "
        f"WHERE file_name={_ph()} AND username={_ph()} AND run_at >= {_ph()} "
        f"ORDER BY run_at ASC",
        (file_name, username, cutoff),
    )
    return _rows(cur)


def get_dq_baseline(file_name, username):
    """Earliest recorded score for a file — used as the comparison baseline."""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT file_name, score, grade, run_at FROM ws_dq_history "
        f"WHERE file_name={_ph()} AND username={_ph()} ORDER BY run_at ASC LIMIT 1",
        (file_name, username),
    )
    rows = _rows(cur)
    return rows[0] if rows else None


# --------------------------------------------------------------------------
# Reconciliation run history (trend tracking) -- mirrors DQ history above
# --------------------------------------------------------------------------
def save_recon_history(username, dataset_label, schema_fingerprint, session_id,
                        matched_count, file1_only_count, file2_only_count,
                        modified_count, method=None):
    conn = _conn()
    cur = conn.cursor()
    total_rows = (matched_count or 0) + (file1_only_count or 0) + (file2_only_count or 0) + (modified_count or 0)
    breaks = (file1_only_count or 0) + (file2_only_count or 0) + (modified_count or 0)
    break_rate = round(breaks / total_rows, 4) if total_rows else 0.0
    status = "PASS" if breaks == 0 else ("WARN" if break_rate < 0.05 else "FAIL")
    cur.execute(
        f"INSERT INTO ws_recon_history "
        f"(dataset_label, username, schema_fingerprint, session_id, matched_count, "
        f"file1_only_count, file2_only_count, modified_count, total_rows, break_rate, "
        f"status, method, run_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},"
        f"{_ph()},{_ph()},{_ph()},{_ph()})",
        (dataset_label, username, schema_fingerprint, session_id, matched_count,
         file1_only_count, file2_only_count, modified_count, total_rows, break_rate,
         status, method, _now()),
    )
    conn.commit()
    return cur.lastrowid


def get_recon_history(schema_fingerprint, username, days=30):
    """Reconciliation break-rate trend for a schema, most recent `days` days, oldest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cur = _conn().cursor()
    cur.execute(
        f"SELECT dataset_label, matched_count, file1_only_count, file2_only_count, "
        f"modified_count, total_rows, break_rate, status, method, run_at "
        f"FROM ws_recon_history "
        f"WHERE schema_fingerprint={_ph()} AND username={_ph()} AND run_at >= {_ph()} "
        f"ORDER BY run_at ASC",
        (schema_fingerprint, username, cutoff),
    )
    return _rows(cur)


def get_recon_baseline(schema_fingerprint, username):
    """Earliest recorded run for a schema -- used as the comparison baseline."""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT dataset_label, break_rate, status, run_at FROM ws_recon_history "
        f"WHERE schema_fingerprint={_ph()} AND username={_ph()} ORDER BY run_at ASC LIMIT 1",
        (schema_fingerprint, username),
    )
    rows = _rows(cur)
    return rows[0] if rows else None


# --------------------------------------------------------------------------
# LLM token usage (per user, for the /api/usage panel and admin user list)
# --------------------------------------------------------------------------
def log_token_usage(username, module, call_type, input_tokens, output_tokens, model=None):
    conn = _conn()
    conn.cursor().execute(
        f"INSERT INTO ws_token_usage "
        f"(username, module, call_type, input_tokens, output_tokens, model, created_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, module, call_type, input_tokens, output_tokens, model, _now()),
    )
    conn.commit()


def get_token_usage_month_total(username, month_key):
    """Total input+output tokens for one user in one 'YYYY-MM' month."""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), COUNT(*) "
        f"FROM ws_token_usage WHERE username={_ph()} AND created_at LIKE {_ph()}",
        (username, f"{month_key}%"),
    )
    row = list(cur.fetchall())[0]
    input_tokens, output_tokens, calls = row[0], row[1], row[2]
    return {"input_tokens": input_tokens, "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens, "calls": calls}


def get_token_usage_summary(username, months=3):
    """Per-month token usage for the last `months` calendar months, oldest first."""
    cur = _conn().cursor()
    cur.execute(
        f"SELECT substr(created_at,1,7) AS month, "
        f"COALESCE(SUM(input_tokens),0) AS input_tokens, "
        f"COALESCE(SUM(output_tokens),0) AS output_tokens, COUNT(*) AS calls "
        f"FROM ws_token_usage WHERE username={_ph()} "
        f"GROUP BY substr(created_at,1,7) ORDER BY month DESC LIMIT {int(months)}",
        (username,),
    )
    rows = _rows(cur)
    rows.reverse()
    return rows
