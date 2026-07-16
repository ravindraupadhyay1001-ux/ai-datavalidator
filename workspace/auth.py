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

from workspace.db import ensure_user, get_user_role, is_user_blocked, touch_last_active

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
        if is_user_blocked(username):
            return JSONResponse(
                {"detail": "Your account has been blocked. Contact your administrator."},
                status_code=403,
            )
        touch_last_active(username)
        role = get_user_role(username)
        # Three access tiers: admin (everything), analyst (Workspace + run
        # modules), readonly (run modules only -- no Workspace access at all,
        # /api/ws/me excepted since the frontend needs it to know who's logged
        # in and render the right role-aware UI).
        if role == "readonly" and request.url.path != f"{_API_PREFIX}/me":
            return JSONResponse(
                {"detail": "Your role does not have access to Workspace."},
                status_code=403,
            )
        request.state.username = username
        request.state.display_name = display_name
        request.state.role = role
        return await call_next(request)


def _resolve_username(request: Request) -> Optional[str]:
    """
    Priority:
    1. WORKSPACE_DEV_USER env var
    2. dv_local_session cookie (local username/password auth)
    3. dv_session cookie (SSO -- SAML/OIDC)
    4. X-Remote-User / Remote-User / X-Windows-User headers (IIS Windows Auth)
    5. OS login name (local dev fallback)
    """
    if _DEV_USER:
        return _DEV_USER

    local_token = request.cookies.get("dv_local_session", "")
    if local_token:
        try:
            from workspace.local_auth import verify_session as _verify_local
            username = _verify_local(local_token)
            if username:
                return username
        except Exception:
            pass

    sso_token = request.cookies.get("dv_session", "")
    if sso_token:
        try:
            from workspace.sso import verify_sso_token as _verify_sso
            claims = _verify_sso(sso_token)
            if claims and claims.get("sub"):
                return claims["sub"]
        except Exception:
            pass

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


def resolve_role_for_user(username: str) -> str:
    return get_user_role(username)


def get_current_user(request: Request) -> str:
    username = getattr(request.state, "username", None)
    if not username:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")
    return username


def get_current_role(request: Request) -> str:
    return getattr(request.state, "role", None) or "analyst"


def require_role(request: Request, *allowed: str):
    role = get_current_role(request)
    if role not in allowed:
        from fastapi import HTTPException
        raise HTTPException(403, f"Role '{role}' cannot perform this action (requires: {', '.join(allowed)}).")
