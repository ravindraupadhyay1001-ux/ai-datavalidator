"""
License manager — RS256 JWT validation with offline grace period.

Token format: RS256 JWT signed by the provider's private key.
  Payload: client_id, tier, features[], limits{}, expires_at (ISO string)

Dev mode: if LICENSE_PUBLIC_KEY is empty or contains "REPLACE_THIS",
all features are unlocked (no token required).
"""

import json
import os
from datetime import datetime, timezone, timedelta

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
    "enterprise": {"max_jobs": 0, "max_users": 0, "max_file_mb": 0},  # 0 = unlimited
}

# Public aliases + upgrade-modal metadata -- /api/license/status imports these
# by these exact names to build its "all_tiers"/"all_limits"/"tier_pricing"/
# "feature_labels" response fields, but they never existed here (only the
# module-private _TIER_FEATURES/_TIER_LIMITS did), so every call to that
# endpoint raised ImportError and 500'd -- on every page load, since it's
# fetched unconditionally on init, not just when a user opens Settings.
FEATURE_TIERS = _TIER_FEATURES
TIER_LIMITS = _TIER_LIMITS

# No fabricated price points -- this app doesn't have a public price list, so
# claiming specific numbers here would be actively misleading if this ever
# reaches a real upgrade UI. "Contact sales" is honest until real pricing exists.
TIER_PRICING = {
    "starter": {"label": "Starter", "price_monthly": None, "note": "Contact sales for pricing"},
    "professional": {"label": "Professional", "price_monthly": None, "note": "Contact sales for pricing"},
    "enterprise": {"label": "Enterprise", "price_monthly": None, "note": "Contact sales for pricing"},
}

FEATURE_LABELS = {
    "compare": "Reconciliation",
    "quality": "Data Quality",
    "profile": "Data Profile",
    "parse": "Unstructured Parser",
    "governance": "Governance & PII",
    "mapping": "Column Mapping",
    "lineage": "Lineage / Cross Reference",
    "workspace": "Workspace (connections, jobs, scheduling)",
    "api": "API Access",
    "sso": "SSO / SAML / OIDC",
}

_state = {
    "valid": False,
    "dev_mode": False,
    "tier": "none",
    "client_id": "",
    "features": [],
    "limits": {},
    "expires_at": None,
    "message": "",
}

_CACHE_PATH = os.path.join(os.getenv("APP_ROOT", "."), ".license_cache.json")
_KEY_FILE = os.path.join(os.getenv("APP_ROOT", "."), "license.key")


def _public_key():
    return os.getenv("LICENSE_PUBLIC_KEY", "").strip()


def _is_dev_mode():
    pk = _public_key()
    return (not pk) or ("REPLACE_THIS" in pk)


def _read_token():
    tok = os.getenv("LICENSE_KEY", "").strip()
    if tok:
        return tok
    if os.path.exists(_KEY_FILE):
        try:
            with open(_KEY_FILE, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except Exception:
            pass
    return ""


def _grace_days():
    try:
        return int(os.getenv("LICENSE_GRACE_DAYS", "7"))
    except ValueError:
        return 7


def _set_state(**kw):
    _state.update(kw)


def _unlock_dev():
    _set_state(
        valid=True, dev_mode=True, tier="enterprise",
        client_id="DEV", features=_TIER_FEATURES["enterprise"],
        limits=_TIER_LIMITS["enterprise"], expires_at=None,
        message="Dev mode - all features unlocked (no public key configured).",
    )


def _save_cache():
    try:
        with open(_CACHE_PATH, "w", encoding="utf-8") as fh:
            json.dump({"state": _state, "cached_at": datetime.now(timezone.utc).isoformat()}, fh)
    except Exception:
        pass


def _load_cache():
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def load_license():
    """Read token -> validate -> save cache -> set state."""
    if _is_dev_mode():
        _unlock_dev()
        return get_state()

    token = _read_token()
    if not token:
        _set_state(valid=False, tier="none", features=[], limits={},
                   message="No license token found.")
        return get_state()

    try:
        import jwt
        payload = jwt.decode(token, _public_key(), algorithms=["RS256"])
        exp = payload.get("expires_at")
        if exp:
            exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
            if exp_dt < datetime.now(timezone.utc):
                raise ValueError("Token hard-expired.")
        tier = payload.get("tier", "starter")
        _set_state(
            valid=True, dev_mode=False, tier=tier,
            client_id=payload.get("client_id", ""),
            features=payload.get("features") or _TIER_FEATURES.get(tier, []),
            limits=payload.get("limits") or _TIER_LIMITS.get(tier, {}),
            expires_at=exp,
            message="License valid.",
        )
        _save_cache()
        return get_state()
    except Exception as e:
        # Grace period: use cache if recent and token not hard-expired
        cache = _load_cache()
        if cache and "hard-expired" not in str(e):
            try:
                cached_at = datetime.fromisoformat(cache["cached_at"])
                if datetime.now(timezone.utc) - cached_at < timedelta(days=_grace_days()):
                    _state.update(cache["state"])
                    _state["message"] = f"Offline grace period active ({e})."
                    return get_state()
            except Exception:
                pass
        _set_state(valid=False, tier="none", features=[], limits={},
                   message=f"License validation failed: {e}")
        return get_state()


def get_state():
    return dict(_state)


def is_valid():
    return bool(_state.get("valid"))


def is_feature_allowed(feature):
    if _state.get("dev_mode"):
        return True
    return feature in _state.get("features", [])


def check_limit(key, value):
    """True if within limit. 0 (or missing) means unlimited."""
    if _state.get("dev_mode"):
        return True
    limit = _state.get("limits", {}).get(key, 0)
    if not limit:
        return True
    return value <= limit


def activate_license(token):
    """Save token to license.key and re-validate."""
    try:
        with open(_KEY_FILE, "w", encoding="utf-8") as fh:
            fh.write(token.strip())
    except Exception as e:
        return {"ok": False, "error": str(e)}
    os.environ["LICENSE_KEY"] = token.strip()
    load_license()
    return {"ok": is_valid(), "state": get_state()}


def heartbeat(usage=None):
    """POST usage to the license server (best effort; no-op in dev mode)."""
    if _state.get("dev_mode"):
        return {"ok": True, "dev": True}
    url = os.getenv("LICENSE_SERVER_URL", "").strip()
    if not url:
        return {"ok": False, "error": "No LICENSE_SERVER_URL configured."}
    try:
        import httpx
        r = httpx.post(
            url.rstrip("/") + "/api/heartbeat",
            json={"client_id": _state.get("client_id"), "usage": usage or {}},
            timeout=10,
        )
        return {"ok": r.status_code == 200}
    except Exception as e:
        return {"ok": False, "error": str(e)}
