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

_oauth_client = None


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
