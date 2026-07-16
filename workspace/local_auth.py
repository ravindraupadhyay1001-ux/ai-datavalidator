"""
Local username/password authentication -- fallback for deployments without
Windows Auth / IIS or enterprise SSO. Enabled via LOCAL_AUTH_ENABLED=true.

Sessions are stateless signed JWTs (HS256, JWT_SECRET) stored in the
dv_local_session cookie -- no server-side session store needed, so logout
is just clearing the cookie client-side.
"""

import hashlib
import os
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from workspace.db import (
    count_local_users,
    create_local_user,
    get_user_password_hash,
    is_user_blocked,
    set_user_password_hash,
    touch_last_active,
)

LOCAL_AUTH_ENABLED = os.getenv("LOCAL_AUTH_ENABLED", "false").lower() == "true"
_JWT_SECRET = os.getenv("JWT_SECRET", "change-this-in-production")
_SESSION_HOURS = 8
_RESET_MINUTES = 30


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
        "purpose": "session",
        "exp": datetime.now(timezone.utc) + timedelta(hours=_SESSION_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


def verify_session(token: str) -> "str | None":
    """Return the username if the session token is valid and unexpired, else None."""
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        if payload.get("purpose") != "session":
            return None
        return payload.get("sub")
    except Exception:
        return None


def _pwh_fingerprint(password_hash: str) -> str:
    return hashlib.sha256((password_hash or "").encode("utf-8")).hexdigest()[:16]


def make_reset_token(username: str) -> "str | None":
    """Short-lived, single-use password reset token. Binds a fingerprint of
    the user's *current* password hash into the token, so it stops working
    the moment the password actually changes (via this token or any other
    route) -- no separate used-token table needed."""
    password_hash = get_user_password_hash(username)
    if password_hash is None:
        return None
    payload = {
        "sub": username,
        "purpose": "reset",
        "pwh": _pwh_fingerprint(password_hash),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=_RESET_MINUTES),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


def verify_reset_token(token: str) -> "str | None":
    """Return the username if this is a valid, unexpired, not-yet-used reset
    token, else None."""
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
        if payload.get("purpose") != "reset":
            return None
        username = payload.get("sub")
        password_hash = get_user_password_hash(username)
        if password_hash is None or _pwh_fingerprint(password_hash) != payload.get("pwh"):
            return None
        return username
    except Exception:
        return None


def request_password_reset(identifier: str) -> "tuple[str, str, str] | None":
    """Look up a user by username or email. Returns (username, email, reset_token)
    if found and has an email on file to send it to, else None. Callers should
    show the same generic "check your email" message either way, to avoid
    leaking which usernames/emails exist."""
    from workspace.db import get_user_by_username_or_email
    user = get_user_by_username_or_email(identifier.strip().lower())
    if not user or not user.get("email"):
        return None
    token = make_reset_token(user["username"])
    if not token:
        return None
    return user["username"], user["email"], token


def reset_password(token: str, new_password: str) -> None:
    """Consume a reset token and set a new password. Raises AuthError."""
    if len(new_password) < 8:
        raise AuthError("New password must be at least 8 characters.")
    username = verify_reset_token(token)
    if not username:
        raise AuthError("This reset link is invalid or has expired. Please request a new one.")
    if is_user_blocked(username):
        raise AuthError("This account has been blocked. Contact your administrator.")
    set_user_password_hash(username, _hash_password(new_password))


def login(username: str, password: str) -> str:
    """Validate credentials and return a fresh session token. Raises AuthError."""
    username = username.strip().lower()
    if not username or not password:
        raise AuthError("Username and password are required.")
    password_hash = get_user_password_hash(username)
    if not password_hash or not _verify_password(password, password_hash):
        raise AuthError("Invalid username or password.")
    if is_user_blocked(username):
        raise AuthError("This account has been blocked. Contact your administrator.")
    touch_last_active(username)
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
