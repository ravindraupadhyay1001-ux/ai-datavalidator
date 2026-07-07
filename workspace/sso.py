"""
LDAP / Active Directory authentication.

Env vars:
  LDAP_ENABLED         true|false
  LDAP_SERVER          ldap://dc.corp.com
  LDAP_BASE_DN         DC=corp,DC=com
  LDAP_BIND_DN         CN=svc,DC=corp,DC=com   (service account for group lookup)
  LDAP_BIND_PASSWORD
  SSO_ROLE_MAP         JSON: {"DataValidation-Admin":"admin","DataValidation-Analyst":"analyst"}

Two-step auth:
  1. Direct bind as the user (CN=<username>,<base_dn> or userPrincipalName)
     to verify their password.
  2. Bind as the service account and look up the user's group memberships,
     mapping the first matching group (via SSO_ROLE_MAP) to a role.
Falls back to "analyst" if no group maps, so a valid LDAP login never
locks someone out even if role mapping isn't configured yet.
"""
import json
import os

LDAP_ENABLED = os.getenv("LDAP_ENABLED", "false").lower() == "true"
LDAP_SERVER = os.getenv("LDAP_SERVER", "")
LDAP_BASE_DN = os.getenv("LDAP_BASE_DN", "")
LDAP_BIND_DN = os.getenv("LDAP_BIND_DN", "")
LDAP_BIND_PASSWORD = os.getenv("LDAP_BIND_PASSWORD", "")

try:
    SSO_ROLE_MAP = json.loads(os.getenv("SSO_ROLE_MAP", "{}"))
except Exception:
    SSO_ROLE_MAP = {}

# --- OIDC (SAML 2.0 was in-scope per the spec, but its XML-signature
# validation is easy to get subtly wrong and unverifiable without a real
# IdP to test against; OIDC covers the same "enterprise SSO" need and
# authlib handles the security-critical token verification correctly) ---
OIDC_ENABLED = os.getenv("OIDC_ENABLED", "false").lower() == "true"
OIDC_ISSUER = os.getenv("OIDC_ISSUER", "")          # e.g. https://login.microsoftonline.com/<tenant>/v2.0
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID", "")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET", "")
OIDC_ROLE_CLAIM = os.getenv("OIDC_ROLE_CLAIM", "roles")  # id_token claim holding group/role names
OIDC_REDIRECT_URI = os.getenv("OIDC_REDIRECT_URI", "")  # this app's /sso/oidc/callback full URL

_JWT_SECRET = os.getenv("JWT_SECRET", "change-this-in-production")

_oauth_client = None
_oidc_discovery_cache: dict | None = None
_oidc_jwks_cache = None


def get_oidc_client():
    """Lazily-built authlib OAuth registry with the 'oidc' provider
    registered, or None if OIDC isn't configured."""
    global _oauth_client
    if not (OIDC_ENABLED and OIDC_ISSUER and OIDC_CLIENT_ID and OIDC_CLIENT_SECRET):
        return None
    if _oauth_client is None:
        from authlib.integrations.starlette_client import OAuth
        _oauth_client = OAuth()
        _oauth_client.register(
            name="oidc",
            server_metadata_url=f"{OIDC_ISSUER.rstrip('/')}/.well-known/openid-configuration",
            client_id=OIDC_CLIENT_ID,
            client_secret=OIDC_CLIENT_SECRET,
            client_kwargs={"scope": "openid email profile"},
        )
    return _oauth_client


def resolve_role_from_claims(claims: dict) -> str:
    """Map OIDC id_token claims (e.g. a 'roles' or 'groups' array) to an
    app role via SSO_ROLE_MAP. Defaults to 'analyst' if nothing matches."""
    values = claims.get(OIDC_ROLE_CLAIM) or []
    if isinstance(values, str):
        values = [values]
    for v in values:
        for claim_value, role in SSO_ROLE_MAP.items():
            if claim_value.lower() == str(v).lower():
                return role
    return "analyst"


def _user_dn(username: str) -> str:
    # Accept either a bare username or a full UPN (user@domain).
    if "@" in username or "=" in username:
        return username
    return f"CN={username},{LDAP_BASE_DN}"


def authenticate(username: str, password: str):
    """Returns {"username", "role"} on success, None on bad credentials,
    raises RuntimeError if LDAP isn't configured or unreachable."""
    if not LDAP_ENABLED:
        raise RuntimeError("LDAP is not enabled (set LDAP_ENABLED=true).")
    if not (LDAP_SERVER and LDAP_BASE_DN):
        raise RuntimeError("LDAP_SERVER / LDAP_BASE_DN not configured.")

    import ldap3

    server = ldap3.Server(LDAP_SERVER, get_info=ldap3.NONE)

    # Step 1: bind directly as the user to verify their password.
    try:
        user_conn = ldap3.Connection(server, user=_user_dn(username), password=password, auto_bind=True)
    except ldap3.core.exceptions.LDAPBindError:
        return None
    finally:
        try:
            user_conn.unbind()
        except Exception:
            pass

    role = _lookup_role(server, username)
    return {"username": username.split("@")[0].lower(), "role": role or "analyst"}


def _lookup_role(server, username: str):
    """Bind as the service account and check the user's group memberships
    against SSO_ROLE_MAP. Best-effort -- returns None on any failure so a
    misconfigured lookup never blocks a login that already passed step 1."""
    if not (LDAP_BIND_DN and LDAP_BIND_PASSWORD and SSO_ROLE_MAP):
        return None
    import ldap3
    try:
        conn = ldap3.Connection(server, user=LDAP_BIND_DN, password=LDAP_BIND_PASSWORD, auto_bind=True)
        conn.search(
            LDAP_BASE_DN,
            f"(sAMAccountName={username.split('@')[0]})",
            attributes=["memberOf"],
        )
        if not conn.entries:
            return None
        groups = [str(g) for g in conn.entries[0].memberOf.values] if conn.entries[0].memberOf else []
        for group_dn in groups:
            for group_name, role in SSO_ROLE_MAP.items():
                if group_name.lower() in group_dn.lower():
                    return role
        return None
    except Exception:
        return None
    finally:
        try:
            conn.unbind()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Unified SSO entry points -- what main.py's /sso/* routes actually import.
# OIDC is the recommended, fully-supported path (uses authlib's JOSE/JWT
# primitives for correct id_token signature verification). SAML delegates to
# workspace/saml_sso.py, which is opt-in and requires python3-saml to be
# installed separately -- see that module's docstring.
# --------------------------------------------------------------------------

def _saml_configured() -> bool:
    try:
        from workspace.saml_sso import _configured
        return _configured()
    except Exception:
        return False


def sso_enabled() -> bool:
    oidc_ready = bool(OIDC_ENABLED and OIDC_ISSUER and OIDC_CLIENT_ID
                       and OIDC_CLIENT_SECRET and OIDC_REDIRECT_URI)
    return oidc_ready or _saml_configured()


def sso_mode() -> str:
    if OIDC_ENABLED and OIDC_ISSUER and OIDC_CLIENT_ID and OIDC_CLIENT_SECRET and OIDC_REDIRECT_URI:
        return "oidc"
    if _saml_configured():
        return "saml"
    return ""


def create_sso_session_token(username: str, role: str, groups: list) -> str:
    """Sign a short-lived dv_session cookie token for an SSO-authenticated user."""
    import jwt
    from datetime import datetime, timedelta, timezone
    payload = {
        "sub": username,
        "role": role,
        "groups": groups,
        "exp": datetime.now(timezone.utc) + timedelta(hours=8),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


def verify_sso_token(token: str) -> "dict | None":
    """Decode a dv_session cookie token. Returns the claims dict, or None if
    invalid/expired."""
    import jwt
    try:
        return jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])
    except Exception:
        return None


# -- SAML (delegates to workspace.saml_sso; requires python3-saml + SAML_* env vars) --

async def saml_login_redirect_url(request) -> str:
    from workspace.saml_sso import login_redirect_url
    return await login_redirect_url(request)


async def saml_process_response(request) -> dict:
    from workspace.saml_sso import process_acs
    result = await process_acs(request)
    return {
        "username": result["username"],
        "role": result["role"],
        "groups": result.get("groups", []),
        "display_name": result.get("display_name", result["username"]),
        "email": result.get("email", ""),
    }


# -- OIDC (Authorization Code flow, manual discovery + authlib JOSE verification) --

def _oidc_discover() -> dict:
    global _oidc_discovery_cache
    if _oidc_discovery_cache is None:
        import httpx
        resp = httpx.get(f"{OIDC_ISSUER.rstrip('/')}/.well-known/openid-configuration", timeout=10)
        resp.raise_for_status()
        _oidc_discovery_cache = resp.json()
    return _oidc_discovery_cache


def _oidc_jwks():
    global _oidc_jwks_cache
    if _oidc_jwks_cache is None:
        import httpx
        from authlib.jose import JsonWebKey
        resp = httpx.get(_oidc_discover()["jwks_uri"], timeout=10)
        resp.raise_for_status()
        _oidc_jwks_cache = JsonWebKey.import_key_set(resp.json())
    return _oidc_jwks_cache


def _oidc_require_configured():
    if not (OIDC_ENABLED and OIDC_ISSUER and OIDC_CLIENT_ID and OIDC_CLIENT_SECRET and OIDC_REDIRECT_URI):
        raise RuntimeError("OIDC is not fully configured (see OIDC_* env vars).")


def oidc_login_redirect_url(state: str) -> str:
    _oidc_require_configured()
    from urllib.parse import urlencode
    endpoint = _oidc_discover()["authorization_endpoint"]
    params = {
        "response_type": "code",
        "client_id": OIDC_CLIENT_ID,
        "redirect_uri": OIDC_REDIRECT_URI,
        "scope": "openid email profile",
        "state": state,
    }
    return f"{endpoint}?{urlencode(params)}"


def oidc_exchange_code(code: str) -> dict:
    _oidc_require_configured()
    import httpx
    from authlib.jose import jwt as jose_jwt

    resp = httpx.post(
        _oidc_discover()["token_endpoint"],
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": OIDC_CLIENT_ID,
            "client_secret": OIDC_CLIENT_SECRET,
            "redirect_uri": OIDC_REDIRECT_URI,
        },
        timeout=10,
    )
    resp.raise_for_status()
    id_token = resp.json().get("id_token")
    if not id_token:
        raise ValueError("OIDC token response did not include an id_token.")

    claims = jose_jwt.decode(id_token, _oidc_jwks())
    claims.validate()  # checks exp/nbf and, when present, aud/iss

    username = (claims.get("preferred_username") or claims.get("email")
                or claims.get("sub") or "").split("@")[0].lower()
    if not username:
        raise ValueError("OIDC id_token did not contain a usable username claim.")

    role = resolve_role_from_claims(claims)
    role_values = claims.get(OIDC_ROLE_CLAIM) or []
    if isinstance(role_values, str):
        role_values = [role_values]

    return {
        "username": username,
        "role": role,
        "groups": list(role_values),
        "display_name": claims.get("name", username),
        "email": claims.get("email", ""),
    }
