"""
Local username/password authentication -- fallback for deployments without
Windows Auth / IIS or enterprise SSO. Enabled via LOCAL_AUTH_ENABLED=true.

Sessions are stateless signed JWTs (HS256, JWT_SECRET) stored in the
dv_local_session cookie -- no server-side session store needed, so logout
is just clearing the cookie client-side.
"""

import os
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from workspace.db import (
    count_local_users,
    create_local_user,
    get_user_password_hash,
    set_user_password_hash,
)

LOCAL_AUTH_ENABLED = os.getenv("LOCAL_AUTH_ENABLED", "false").lower() == "true"
_JWT_SECRET = os.getenv("JWT_SECRET", "change-this-in-production")
_SESSION_HOURS = 8


class AuthError(Exception):
    """Raised for any local-auth failure (bad credentials, duplicate username, etc.)."""


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("ascii")


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def has_any_users() -> bool:
    return count_local_users() > 0


def _make_token(username: str) -> str:
    payload = {
        "sub": username,
        "exp": datetime.now(timezone.utc) + timedelta(hours=_SESSION_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


def verify_session(token: str) -> "str | None":
    """Return the username if the session token is valid and unexpired, else None."""
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        return payload.get("sub")
    except Exception:
        return None


def login(username: str, password: str) -> str:
    """Validate credentials and return a fresh session token. Raises AuthError."""
    username = username.strip().lower()
    if not username or not password:
        raise AuthError("Username and password are required.")
    password_hash = get_user_password_hash(username)
    if not password_hash or not _verify_password(password, password_hash):
        raise AuthError("Invalid username or password.")
    return _make_token(username)


def register(username: str, password: str, full_name: str = "", email: str = "") -> None:
    """Create a new local user. Raises AuthError on invalid input or duplicate username."""
    username = username.strip().lower()
    if not username or not password:
        raise AuthError("Username and password are required.")
    if len(password) < 8:
        raise AuthError("Password must be at least 8 characters.")
    password_hash = _hash_password(password)
    try:
        create_local_user(username, password_hash, full_name=full_name, email=email)
    except ValueError as exc:
        raise AuthError(str(exc)) from exc


def logout(token: str) -> None:
    # Sessions are stateless JWTs -- nothing to invalidate server-side.
    pass


def change_password(username: str, old_password: str, new_password: str) -> None:
    """Verify the old password and set a new one. Raises AuthError."""
    if len(new_password) < 8:
        raise AuthError("New password must be at least 8 characters.")
    password_hash = get_user_password_hash(username)
    if not password_hash or not _verify_password(old_password, password_hash):
        raise AuthError("Current password is incorrect.")
    set_user_password_hash(username, _hash_password(new_password))
