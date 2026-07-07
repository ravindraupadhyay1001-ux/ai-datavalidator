"""
Audit log: persists actions via workspace.db (ws_audit_log table) and
broadcasts them in real time to any /api/logs/stream SSE subscribers.
"""

import asyncio
import json
from datetime import datetime, timezone

from workspace.db import insert_audit, list_audit

_subscribers: set["asyncio.Queue[str]"] = set()


def log_action(username: str, action: str, detail: str = "", session_id: str | None = None) -> None:
    """Persist an audit entry and broadcast it to any live SSE subscribers."""
    insert_audit(username, action, detail, session_id)
    event = json.dumps({
        "username": username,
        "action": action,
        "detail": detail,
        "session_id": session_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    for q in list(_subscribers):
        try:
            q.put_nowait(event)
        except Exception:
            pass


def subscribe() -> "asyncio.Queue[str]":
    q: "asyncio.Queue[str]" = asyncio.Queue()
    _subscribers.add(q)
    return q


def unsubscribe(q: "asyncio.Queue[str]") -> None:
    _subscribers.discard(q)


def list_audit_log(username: str | None = None, action: str | None = None, limit: int = 200) -> list[dict]:
    return list_audit(username=username, action=action, limit=limit)
