"""
Provider-side license server (deploy this separately from the client app).

Responsibilities:
  - Issue RS256-signed JWT license tokens for clients.
  - Record client heartbeats (usage telemetry / last-seen).
  - Optional admin listing of issued licenses.

Key generation (run once on the provider machine):
  openssl genrsa -out license_private.pem 2048
  openssl rsa -in license_private.pem -pubout -out license_public.pem

The PUBLIC key is baked into the client build (LICENSE_PUBLIC_KEY).
The PRIVATE key NEVER leaves the provider.

Run:
  set LICENSE_PRIVATE_KEY_PATH=license_private.pem
  set LICENSE_ADMIN_TOKEN=some-long-secret
  python -m uvicorn license.license_server:app --port 9000
"""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone, timedelta

import jwt
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Data Validation Agent — License Server")

_PRIVATE_KEY_PATH = os.getenv("LICENSE_PRIVATE_KEY_PATH", "license_private.pem")
_ADMIN_TOKEN = os.getenv("LICENSE_ADMIN_TOKEN", "")
_DB_PATH = os.getenv("LICENSE_SERVER_DB", "license_server.db")
_lock = threading.RLock()

_TIER_FEATURES = {
    "starter": ["compare", "quality", "profile"],
    "professional": ["compare", "quality", "profile", "parse", "governance",
                     "mapping", "lineage", "workspace"],
    "enterprise": ["compare", "quality", "profile", "parse", "governance",
                   "mapping", "lineage", "workspace", "api", "sso"],
}
_TIER_LIMITS = {
    "starter": {"max_jobs": 3, "max_users": 2, "max_file_mb": 50},
    "professional": {"max_jobs": 20, "max_users": 10, "max_file_mb": 500},
    "enterprise": {"max_jobs": 0, "max_users": 0, "max_file_mb": 0},
}


def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS licenses (
        client_id TEXT PRIMARY KEY, tier TEXT, features TEXT, limits TEXT,
        expires_at TEXT, issued_at TEXT, token TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS heartbeats (
        id INTEGER PRIMARY KEY AUTOINCREMENT, client_id TEXT, usage TEXT, seen_at TEXT)""")
    return conn


def _private_key():
    if not os.path.exists(_PRIVATE_KEY_PATH):
        raise HTTPException(500, f"Private key not found at {_PRIVATE_KEY_PATH}.")
    with open(_PRIVATE_KEY_PATH, "r", encoding="utf-8") as fh:
        return fh.read()


def _require_admin(token):
    if not _ADMIN_TOKEN:
        raise HTTPException(500, "LICENSE_ADMIN_TOKEN not configured on server.")
    if token != _ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token.")


# --------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------
class IssueRequest(BaseModel):
    client_id: str
    tier: str = "professional"
    valid_days: int = 365
    features: list | None = None
    limits: dict | None = None


class HeartbeatRequest(BaseModel):
    client_id: str
    usage: dict | None = None


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------
@app.post("/api/issue")
def issue_license(req: IssueRequest, x_admin_token: str = Header("")):
    _require_admin(x_admin_token)
    if req.tier not in _TIER_FEATURES:
        raise HTTPException(400, f"Unknown tier '{req.tier}'.")
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=req.valid_days)
    payload = {
        "client_id": req.client_id,
        "tier": req.tier,
        "features": req.features or _TIER_FEATURES[req.tier],
        "limits": req.limits or _TIER_LIMITS[req.tier],
        "expires_at": expires.isoformat(),
        "iat": int(now.timestamp()),
    }
    token = jwt.encode(payload, _private_key(), algorithm="RS256")
    with _lock:
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO licenses "
            "(client_id, tier, features, limits, expires_at, issued_at, token) "
            "VALUES (?,?,?,?,?,?,?)",
            (req.client_id, req.tier, json.dumps(payload["features"]),
             json.dumps(payload["limits"]), payload["expires_at"],
             now.isoformat(), token))
        conn.commit()
        conn.close()
    return {"client_id": req.client_id, "tier": req.tier,
            "expires_at": payload["expires_at"], "token": token}


@app.post("/api/heartbeat")
def heartbeat(req: HeartbeatRequest):
    with _lock:
        conn = _db()
        conn.execute("INSERT INTO heartbeats (client_id, usage, seen_at) VALUES (?,?,?)",
                     (req.client_id, json.dumps(req.usage or {}),
                      datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    return {"ok": True}


@app.get("/api/licenses")
def list_licenses(x_admin_token: str = Header("")):
    _require_admin(x_admin_token)
    conn = _db()
    rows = [dict(r) for r in conn.execute(
        "SELECT client_id, tier, expires_at, issued_at FROM licenses").fetchall()]
    conn.close()
    return rows


@app.get("/api/licenses/{client_id}/heartbeats")
def client_heartbeats(client_id: str, x_admin_token: str = Header("")):
    _require_admin(x_admin_token)
    conn = _db()
    rows = [dict(r) for r in conn.execute(
        "SELECT usage, seen_at FROM heartbeats WHERE client_id=? "
        "ORDER BY id DESC LIMIT 100", (client_id,)).fetchall()]
    conn.close()
    return rows


@app.get("/health")
def health():
    return {"ok": True, "service": "license-server"}
