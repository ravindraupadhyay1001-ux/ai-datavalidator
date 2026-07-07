"""
SAML 2.0 SSO (SP-initiated).

NOT installed by default -- python3-saml depends on the `xmlsec` Python
package, which wraps the native libxmlsec1 C library. That's not present
in a stock Railway/Nixpacks build, and pip installing `xmlsec` without it
fails at BUILD TIME, taking down the whole app, not just SAML. So this
stays out of requirements.txt; to actually use SAML:
  1. `pip install python3-saml` (needs libxmlsec1 + libxmlsec1-dev + \
     pkg-config on the host, or a Dockerfile step: \
     `apt-get install -y libxmlsec1-dev pkg-config`)
  2. Set SAML_ENABLED=true and the env vars below.
OIDC (workspace/sso.py) covers the same "enterprise SSO" need without
this risk, and is the safer default recommendation.

Env vars:
  SAML_ENABLED          true|false
  SAML_SP_ENTITY_ID      this app's SAML entity ID (e.g. the app's URL)
  SAML_SP_ACS_URL        Assertion Consumer Service URL (…/auth/saml/acs)
  SAML_IDP_ENTITY_ID     IdP entity ID
  SAML_IDP_SSO_URL       IdP's SSO redirect endpoint
  SAML_IDP_X509_CERT     IdP's signing certificate (PEM, no headers, one line)
  SAML_ROLE_ATTRIBUTE    assertion attribute holding group/role names (default "roles")
"""
import os

SAML_ENABLED = os.getenv("SAML_ENABLED", "false").lower() == "true"
SAML_SP_ENTITY_ID = os.getenv("SAML_SP_ENTITY_ID", "")
SAML_SP_ACS_URL = os.getenv("SAML_SP_ACS_URL", "")
SAML_IDP_ENTITY_ID = os.getenv("SAML_IDP_ENTITY_ID", "")
SAML_IDP_SSO_URL = os.getenv("SAML_IDP_SSO_URL", "")
SAML_IDP_X509_CERT = os.getenv("SAML_IDP_X509_CERT", "")
SAML_ROLE_ATTRIBUTE = os.getenv("SAML_ROLE_ATTRIBUTE", "roles")

try:
    from workspace.sso import SSO_ROLE_MAP
except Exception:
    SSO_ROLE_MAP = {}


def _settings() -> dict:
    return {
        "sp": {
            "entityId": SAML_SP_ENTITY_ID,
            "assertionConsumerService": {
                "url": SAML_SP_ACS_URL,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
        },
        "idp": {
            "entityId": SAML_IDP_ENTITY_ID,
            "singleSignOnService": {
                "url": SAML_IDP_SSO_URL,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": SAML_IDP_X509_CERT,
        },
    }


def _configured() -> bool:
    return bool(SAML_ENABLED and SAML_SP_ENTITY_ID and SAML_SP_ACS_URL
                and SAML_IDP_ENTITY_ID and SAML_IDP_SSO_URL and SAML_IDP_X509_CERT)


async def _auth_from_request(request):
    """Builds a OneLogin_Saml2_Auth from a Starlette Request. Raises
    ImportError with a clear message if python3-saml isn't installed."""
    try:
        from onelogin.saml2.auth import OneLogin_Saml2_Auth
    except ImportError as e:
        raise ImportError(
            "SAML_ENABLED=true but python3-saml isn't installed. "
            "It needs the system libxmlsec1 library -- see workspace/saml_sso.py "
            "module docstring for setup steps."
        ) from e
    body = await request.body()
    form = {}
    if body:
        from urllib.parse import parse_qs
        form = {k: v[0] for k, v in parse_qs(body.decode()).items()}
    req_data = {
        "https": "on" if request.url.scheme == "https" else "off",
        "http_host": request.url.hostname,
        "script_name": request.url.path,
        "get_data": dict(request.query_params),
        "post_data": form,
    }
    return OneLogin_Saml2_Auth(req_data, _settings())


async def login_redirect_url(request) -> str:
    if not _configured():
        raise RuntimeError("SAML is not fully configured (see SAML_* env vars).")
    auth = await _auth_from_request(request)
    return auth.login()


async def process_acs(request):
    """Validates the SAML response POSTed to the ACS endpoint. Returns
    {"username", "role"} on success, raises on invalid/unsigned assertions."""
    if not _configured():
        raise RuntimeError("SAML is not fully configured (see SAML_* env vars).")
    auth = await _auth_from_request(request)
    auth.process_response()
    errors = auth.get_errors()
    if errors:
        raise ValueError(f"SAML validation failed: {', '.join(errors)} -- {auth.get_last_error_reason()}")
    if not auth.is_authenticated():
        raise ValueError("SAML assertion did not authenticate.")

    attrs = auth.get_attributes()
    nameid = auth.get_nameid() or ""
    username = nameid.split("@")[0].lower()
    role = "analyst"
    for value in attrs.get(SAML_ROLE_ATTRIBUTE, []):
        for claim_value, mapped_role in SSO_ROLE_MAP.items():
            if claim_value.lower() == str(value).lower():
                role = mapped_role
                break
    return {"username": username, "role": role}
