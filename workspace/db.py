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
        # bootstrap: the very first user ever seen becomes admin so there's
        # always someone able to promote/demote others.
        cur.execute("SELECT COUNT(*) FROM ws_users")
        is_first = list(cur.fetchall())[0][0] == 0
        role = "admin" if is_first else "analyst"
        cur.execute(
            f"INSERT INTO ws_users (username, display_name, email, role, created_at) "
            f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
            (username, display_name or username, email or "", role, _now()),
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


def list_users():
    cur = _conn().cursor()
    cur.execute("SELECT username, display_name, email, role, created_at FROM ws_users ORDER BY created_at ASC")
    return _rows(cur)


def count_users() -> int:
    cur = _conn().cursor()
    cur.execute("SELECT COUNT(*) FROM ws_users")
    return list(cur.fetchall())[0][0]


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
    username is already taken. The very first LOCAL user (not counting
    Windows-Auth/SSO-provisioned rows with no password) becomes admin."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"SELECT username FROM ws_users WHERE username={_ph()}", (username,))
    if cur.fetchall():
        raise ValueError(f"Username '{username}' is already taken.")
    is_first = count_local_users() == 0
    role = "admin" if is_first else "analyst"
    cur.execute(
        f"INSERT INTO ws_users (username, display_name, email, role, created_at, password_hash) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, full_name or username, email or "", role, _now(), password_hash),
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
        f"SELECT id, name, source_type, created_at, updated_at "
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


def save_connection(username, name, source_type, config, conn_id=None):
    conn = _conn()
    cur = conn.cursor()
    if conn_id:
        cur.execute(
            f"UPDATE ws_connections SET name={_ph()}, source_type={_ph()}, "
            f"config_json={_ph()}, updated_at={_ph()} "
            f"WHERE id={_ph()} AND username={_ph()}",
            (name, source_type, _b64(config), _now(), conn_id, username),
        )
        conn.commit()
        return conn_id
    cur.execute(
        f"INSERT INTO ws_connections "
        f"(username, name, source_type, config_json, created_at, updated_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, source_type, _b64(config), _now(), _now()),
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
    return rows[0] if rows else None


def save_job(username, name, action, source_conn_id=None, conn_a_id=None,
             conn_b_id=None, key_columns=None, exclude_columns=None,
             ruleset_id=None, schedule_cron=None, from_email=None,
             notify_email=None, job_id=None):
    conn = _conn()
    cur = conn.cursor()
    if job_id:
        cur.execute(
            f"UPDATE ws_jobs SET name={_ph()}, action={_ph()}, source_conn_id={_ph()}, "
            f"conn_a_id={_ph()}, conn_b_id={_ph()}, key_columns={_ph()}, "
            f"exclude_columns={_ph()}, ruleset_id={_ph()}, schedule_cron={_ph()}, "
            f"from_email={_ph()}, notify_email={_ph()}, updated_at={_ph()} "
            f"WHERE id={_ph()} AND username={_ph()}",
            (name, action, source_conn_id, conn_a_id, conn_b_id, key_columns,
             exclude_columns, ruleset_id, schedule_cron, from_email,
             notify_email, _now(), job_id, username),
        )
        conn.commit()
        return job_id
    cur.execute(
        f"INSERT INTO ws_jobs (username, name, action, source_conn_id, conn_a_id, "
        f"conn_b_id, key_columns, exclude_columns, ruleset_id, schedule_cron, "
        f"from_email, notify_email, status, created_at, updated_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},"
        f"{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, action, source_conn_id, conn_a_id, conn_b_id,
         key_columns, exclude_columns, ruleset_id, schedule_cron, from_email,
         notify_email, "active", _now(), _now()),
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


def finish_run(run_id, status, summary=None, error_msg=None):
    conn = _conn()
    conn.cursor().execute(
        f"UPDATE ws_run_history SET finished_at={_ph()}, status={_ph()}, "
        f"summary_json={_ph()}, error_msg={_ph()} WHERE id={_ph()}",
        (_now(), status, _b64(summary) if summary else None, error_msg, run_id),
    )
    conn.commit()


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
    return _rows(cur)[:limit]


# --------------------------------------------------------------------------
# Saved runs
# --------------------------------------------------------------------------
def save_manual_run(username, name, action, sources, session_id,
                    summary, key_columns=None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO ws_saved_runs "
        f"(username, name, action, sources, session_id, summary_json, "
        f"key_columns, created_at, saved_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (username, name, action, _b64(sources), session_id, _b64(summary),
         key_columns, _now(), _now()),
    )
    conn.commit()
    return cur.lastrowid


def list_saved_runs(username, limit=100):
    cur = _conn().cursor()
    cur.execute(
        f"SELECT id, name, action, session_id, key_columns, saved_at "
        f"FROM ws_saved_runs WHERE username={_ph()} ORDER BY id DESC",
        (username,),
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
def insert_dq_history(file_name, username, score, grade,
                      completeness=None, uniqueness=None, validity=None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO ws_dq_history "
        f"(file_name, username, score, grade, completeness, uniqueness, validity, run_at) "
        f"VALUES ({_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()},{_ph()})",
        (file_name, username, score, grade, completeness, uniqueness, validity, _now()),
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
