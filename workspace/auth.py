"""
Workspace authentication middleware.

Production: IIS injects X-Remote-User header (Windows Auth).
Dev/local: falls back to OS login name automatically.
Override with WORKSPACE_DEV_USER env var.
"""

import getpass
import os
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from workspace.db import ensure_user

WS_ENABLED = True

_API_PREFIX = "/api/ws"
_DEV_USER = os.getenv("WORKSPACE_DEV_USER", "").strip()
try:
    _OS_USER = getpass.getuser().lower()
except Exception:
    _OS_USER = ""


class WorkspaceAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith(_API_PREFIX):
            return await call_next(request)
        username = _resolve_username(request)
        if not username:
            return JSONResponse(
                {"detail": "Could not determine user identity."},
                status_code=401,
            )
        display_name = request.headers.get("X-Display-Name", username)
        email = request.headers.get("X-User-Email", "")
        ensure_user(username, display_name, email)
        request.state.username = username
        request.state.display_name = display_name
        return await call_next(request)


def _resolve_username(request: Request) -> Optional[str]:
    """
    Priority:
    1. WORKSPACE_DEV_USER env var
    2. X-Remote-User / Remote-User / X-Windows-User headers (IIS Windows Auth)
    3. OS login name (local dev fallback)
    """
    if _DEV_USER:
        return _DEV_USER
    for header in ("x-remote-user", "remote_user", "x-windows-user"):
        val = request.headers.get(header, "").strip()
        if val:
            if "\\" in val:
                val = val.split("\\", 1)[1]
            elif "@" in val:
                val = val.split("@")[0]
            return val.lower()
    if _OS_USER:
        return _OS_USER
    return None


def get_current_user(request: Request) -> str:
    username = getattr(request.state, "username", None)
    if not username:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")
    return username
