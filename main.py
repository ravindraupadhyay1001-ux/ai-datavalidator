"""
Data Validation Agent — main FastAPI application.

Implements the full app: file parsing, key inference, compare / quality /
mapping / governance / lineage / parse engines, AI chat copilot, feedback
rules, download, the workspace (connectors + scheduler + CRUD), licensing,
login/session auth, audit-log SSE streaming, and email delivery.
"""

import asyncio
import io
import json
import math
import os
import re
import time
import uuid
import warnings

warnings.filterwarnings("ignore", message="Could not infer format")
from collections import Counter
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile, HTTPException
from fastapi.responses import (HTMLResponse, JSONResponse, StreamingResponse,
                               RedirectResponse)
from fastapi.templating import Jinja2Templates

load_dotenv()

# --- License -------------------------------------------------------------
from license import license_manager as lic  # noqa: E402

# --- Feedback store ------------------------------------------------------
from agent import feedback_store as fb  # noqa: E402

APP_ROOT = os.getenv("APP_ROOT", os.path.dirname(os.path.abspath(__file__)))
TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

app = FastAPI(title="Data Validation AGENT")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

_chat_contexts: dict = {}
_results_store: dict = {}
_session_owners: dict = {}

# --- Session / login auth ------------------------------------------------
JWT_SECRET = os.getenv("JWT_SECRET", "change-this-in-production")
_USERS_PATH = os.path.join(APP_ROOT, "users.json")
_SESSION_COOKIE = "dva_session"
_SESSION_HOURS = 8


_VALID_ROLES = ("admin", "analyst", "readonly")


def _load_users() -> dict:
    """username -> {"hash": bcrypt_hash, "role": admin|analyst|readonly}.
    From AUTH_USERS env JSON or users.json file. Accepts both the new
    dict shape and the legacy flat `username -> hash` shape (legacy
    entries are treated as "admin" to preserve their pre-RBAC full access)."""
    raw = {}
    env = os.getenv("AUTH_USERS", "").strip()
    if env:
        try:
            raw = json.loads(env)
        except Exception:
            raw = {}
    elif os.path.exists(_USERS_PATH):
        try:
            with open(_USERS_PATH, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except Exception:
            raw = {}
    users = {}
    for uname, val in raw.items():
        if isinstance(val, dict):
            role = val.get("role") if val.get("role") in _VALID_ROLES else "analyst"
            users[uname] = {"hash": val.get("hash", ""), "role": role}
        else:
            users[uname] = {"hash": val, "role": "admin"}
    return users


_USERS = _load_users()
AUTH_ENABLED = bool(_USERS)


def _verify_password(plain: str, hashed: str) -> bool:
    # Prefer the bcrypt library directly (passlib 1.7 is incompatible with
    # bcrypt >= 4.1); fall back to passlib for non-bcrypt hashes.
    try:
        import bcrypt as _bcrypt
        return _bcrypt.checkpw(plain.encode("utf-8")[:72], hashed.encode("utf-8"))
    except Exception:
        pass
    try:
        from passlib.context import CryptContext
        return CryptContext(schemes=["bcrypt"], deprecated="auto").verify(plain, hashed)
    except Exception:
        return False


def hash_password(plain: str) -> str:
    """Helper to generate a bcrypt hash for users.json / AUTH_USERS."""
    import bcrypt as _bcrypt
    return _bcrypt.hashpw(plain.encode("utf-8")[:72], _bcrypt.gensalt()).decode("ascii")


def _make_session_token(username: str, role: str = "analyst") -> str:
    import jwt
    exp = datetime.now(timezone.utc) + timedelta(hours=_SESSION_HOURS)
    return jwt.encode({"sub": username, "role": role, "exp": int(exp.timestamp())},
                      JWT_SECRET, algorithm="HS256")


def _verify_session_token(token: str):
    """Returns {"username": str, "role": str} or None."""
    import jwt
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        username = payload.get("sub")
        if not username:
            return None
        return {"username": username, "role": payload.get("role") or "analyst"}
    except Exception:
        return None


def _require_role(request: Request, *allowed: str):
    """Block if the logged-in session's role isn't in `allowed`. No-op when
    login auth is disabled (dev mode) or for sessions predating RBAC."""
    role = getattr(request.state, "session_role", None)
    if role is None:
        return  # no session system active (dev mode / workspace-only auth)
    if role not in allowed:
        raise HTTPException(403, f"Role '{role}' cannot perform this action (requires: {', '.join(allowed)}).")

MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "")

# --- Workspace bootstrap (degrade gracefully) ----------------------------
WS_ENABLED = False
_scheduler_mod = None
try:
    from workspace import db as ws_db
    from workspace.db import init_db as ws_init_db
    from workspace.auth import WorkspaceAuthMiddleware, get_current_user, require_role as _ws_require_role, _resolve_username as _ws_resolve_username
    from workspace.connectors import BaseConnector
    from workspace import scheduler as _scheduler_mod
    app.add_middleware(WorkspaceAuthMiddleware)
    WS_ENABLED = True
except Exception as _ws_err:  # pragma: no cover
    print(f"[workspace] disabled: {_ws_err}")

    def get_current_user(request: Request) -> str:  # type: ignore
        return os.getenv("WORKSPACE_DEV_USER", "localdev")

    def _ws_require_role(request: Request, *allowed: str):  # type: ignore
        pass

    def _ws_resolve_username(request: Request):  # type: ignore
        return None


@app.on_event("startup")
def _startup():
    lic.load_license()
    if WS_ENABLED:
        try:
            ws_init_db()
        except Exception as e:
            print(f"[workspace] init_db failed: {e}")
        try:
            _scheduler_mod.start_scheduler()
            _scheduler_mod.load_all_jobs()
            # license heartbeat every 24h
            from apscheduler.triggers.interval import IntervalTrigger
            _scheduler_mod._scheduler.add_job(
                lambda: lic.heartbeat(), IntervalTrigger(hours=24),
                id="license_heartbeat", replace_existing=True)
        except Exception as e:
            print(f"[scheduler] startup failed: {e}")


@app.on_event("shutdown")
def _shutdown():
    if WS_ENABLED and _scheduler_mod:
        try:
            _scheduler_mod.stop_scheduler()
        except Exception:
            pass


_AUTH_EXEMPT = ("/login", "/logout", "/health", "/api/license", "/static",
                "/favicon.ico")


@app.middleware("http")
async def _session_auth(request: Request, call_next):
    """When login users are configured, require a valid session cookie.
    Page routes redirect to /login; API routes get 401."""
    if not AUTH_ENABLED:
        return await call_next(request)
    path = request.url.path
    if any(path.startswith(p) for p in _AUTH_EXEMPT):
        return await call_next(request)
    session = _verify_session_token(request.cookies.get(_SESSION_COOKIE, ""))
    if not session:
        if path.startswith("/api") or path.startswith("/analyze") or path.startswith("/chat"):
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        return RedirectResponse("/login", status_code=302)
    request.state.session_user = session["username"]
    request.state.session_role = session["role"]
    return await call_next(request)


# =========================================================================
# Helpers
# =========================================================================
def _require_feature(feature: str):
    if not lic.is_feature_allowed(feature):
        raise HTTPException(status_code=402,
                            detail=f"Feature '{feature}' not in your license.")


def _sanitize_json(obj):
    """Recursively convert NaN/Inf/numpy/pandas types to JSON-safe values."""
    if isinstance(obj, dict):
        return {str(k): _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [_sanitize_json(v) for v in obj.tolist()]
    if obj is pd.NaT or (isinstance(obj, float) and pd.isna(obj)):
        return None
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    return obj


# --- Multi-provider LLM layer (agent/llm.py) + tool-calling agent --------
from agent import llm as agent_llm  # noqa: E402
from agent import executor as agent_executor  # noqa: E402
from agent import tools as agent_tools  # noqa: E402
from agent.memory import ChatMemory  # noqa: E402

AI_CONFIGURED = agent_llm.AI_CONFIGURED
_ask_llm = agent_llm.ask
_ask_llm_safe = agent_llm.ask_safe
_agent_memory = ChatMemory()
_ref_docs_store: dict = {}


# =========================================================================
# File parsing engine
# =========================================================================
_NULL_SENTINELS = {"null", "none", "nan", "n/a", "na", "#n/a", ""}


def _detect_encoding(raw: bytes) -> str:
    try:
        import chardet
        guess = chardet.detect(raw[:100000])
        return guess.get("encoding") or "utf-8"
    except Exception:
        return "utf-8"


def _sniff_delimiter(text: str) -> str:
    import csv
    sample = "\n".join(text.splitlines()[:50])
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t|;:").delimiter
    except Exception:
        counts = {d: sample.count(d) for d in [",", "\t", "|", ";", ":"]}
        return max(counts, key=counts.get) if any(counts.values()) else ","


def _read_delimited(text: str, sep: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(text), sep=sep, engine="python",
                       dtype=str, keep_default_na=False)


def _parse_text(raw: bytes, encoding: str, delim=None) -> pd.DataFrame:
    text = raw.decode(encoding, errors="replace")
    stripped = text.lstrip()
    # NDJSON
    if stripped.startswith("{") and "\n" in stripped:
        try:
            rows = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
            if rows:
                return pd.json_normalize(rows).astype(str)
        except Exception:
            pass
    sep = delim or _sniff_delimiter(text)
    return _read_delimited(text, sep)


def _parse_json(raw: bytes) -> pd.DataFrame:
    data = json.loads(raw.decode("utf-8", errors="replace"))
    if isinstance(data, list):
        return pd.json_normalize(data).astype(str)
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return pd.json_normalize(v).astype(str)
        return pd.json_normalize([data]).astype(str)
    return pd.DataFrame({"value": [data]})


def _parse_xml(raw: bytes) -> pd.DataFrame:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(raw.decode("utf-8", errors="replace"))
    # find the most common repeating child tag
    children = list(root)
    if children:
        tags = Counter(c.tag for c in children)
        common = tags.most_common(1)[0][0]
        records = []
        for el in children:
            if el.tag != common:
                continue
            row = {}
            for sub in el.iter():
                if sub is el:
                    continue
                tag = sub.tag.split("}")[-1]
                if sub.text and sub.text.strip():
                    row[tag] = sub.text.strip()
            records.append(row)
        if records:
            return pd.DataFrame(records).astype(str)
    # fallback: flatten root
    row = {sub.tag.split("}")[-1]: (sub.text or "").strip() for sub in root.iter()}
    return pd.DataFrame([row]).astype(str)


def _parse_json_nested(raw: bytes) -> pd.DataFrame:
    """BFS to the first list-of-dicts, then json_normalize (unwraps nesting)."""
    import collections
    data = json.loads(raw.decode("utf-8", errors="replace"))
    if isinstance(data, list):
        return pd.json_normalize(data).astype(str)
    queue = collections.deque([data])
    while queue:
        node = queue.popleft()
        if isinstance(node, dict):
            for v in node.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return pd.json_normalize(v).astype(str)
                if isinstance(v, (dict, list)):
                    queue.append(v)
        elif isinstance(node, list):
            for v in node:
                queue.append(v)
    return pd.json_normalize([data]).astype(str)


# ---- SWIFT MT ----
def _is_swift_mt(raw: bytes, enc: str) -> bool:
    head = raw[:200].decode(enc, errors="replace")
    return bool(re.search(r"\{1:[^}]+\}", head)) and "{4:" in head


def _parse_swift_mt(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    messages = re.split(r"(?=\{1:)", text)
    rows = []
    for msg in messages:
        if "{4:" not in msg:
            continue
        row = {}
        for blk, val in re.findall(r"\{(\d):([^{}]*)\}", msg):
            row[f"block{blk}"] = val.strip()
        body = re.search(r"\{4:(.*?)-?\}", msg, re.S)
        if body:
            # layout A: SOH/pipe numeric tags  :20:value
            for tag, val in re.findall(r":(\w+):([^\n:]*)", body.group(1)):
                row[f"tag_{tag}"] = val.strip()
        # layout B: named KEY=VALUE
        if len(row) <= 4:
            for k, v in re.findall(r"(\w+)=([^\n|]+)", msg):
                row[k] = v.strip()
        if row:
            rows.append(row)
    return pd.DataFrame(rows).astype(str) if rows else pd.DataFrame()


# ---- FIX ----
def _is_fix(raw: bytes, enc: str) -> bool:
    return bool(re.match(r"^8=FIX", raw[:32].decode(enc, errors="replace")))


def _parse_fix(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    sep = "\x01" if "\x01" in text else ("|" if "|" in text else "\n")
    rows = []
    for line in re.split(r"(?=8=FIX)", text):
        line = line.strip()
        if not line.startswith("8=FIX"):
            continue
        row = {}
        for pair in line.split(sep):
            if "=" in pair:
                tag, val = pair.split("=", 1)
                row[f"tag_{tag.strip()}"] = val.strip()
        if row:
            rows.append(row)
    return pd.DataFrame(rows).astype(str) if rows else pd.DataFrame()


# ---- ISO 20022 ----
def _is_iso20022(raw: bytes) -> bool:
    return b"urn:iso:std:iso:20022" in raw[:2000]


def _parse_iso20022(raw: bytes) -> pd.DataFrame:
    """Leaf collection with path/element/value columns."""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(raw.decode("utf-8", errors="replace"))
    rows = []

    def walk(el, path):
        children = list(el)
        tag = el.tag.split("}")[-1]
        cur = f"{path}/{tag}"
        if not children:
            if el.text and el.text.strip():
                rows.append({"path": cur, "element": tag, "value": el.text.strip()})
        else:
            for c in children:
                walk(c, cur)

    walk(root, "")
    return pd.DataFrame(rows).astype(str) if rows else pd.DataFrame()


# ---- FpML / XBRL ----
def _parse_fpml(raw: bytes) -> pd.DataFrame:
    """FpML trade XML flattened to dot-path -> value (one row)."""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(raw.decode("utf-8", errors="replace"))
    row = {}

    def walk(el, path):
        tag = el.tag.split("}")[-1]
        cur = f"{path}.{tag}" if path else tag
        kids = list(el)
        if not kids and el.text and el.text.strip():
            row[cur] = el.text.strip()
        for c in kids:
            walk(c, cur)

    walk(root, "")
    return pd.DataFrame([row]).astype(str) if row else pd.DataFrame()


def _parse_xbrl(raw: bytes) -> pd.DataFrame:
    """Standard XBRL facts + inline iXBRL context/fact pairs."""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(raw.decode("utf-8", errors="replace"))
    rows = []
    for el in root.iter():
        tag = el.tag.split("}")[-1]
        ctx = el.attrib.get("contextRef") or el.attrib.get("name")
        if ctx and el.text and el.text.strip():
            rows.append({"concept": el.attrib.get("name", tag),
                         "context": ctx, "value": el.text.strip(),
                         "unit": el.attrib.get("unitRef", "")})
    return pd.DataFrame(rows).astype(str) if rows else _parse_xml(raw)


# ---- NACHA (fixed-width 94-char) ----
def _parse_nacha(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    rows = []
    for line in text.splitlines():
        if len(line) < 1:
            continue
        rtype = line[0]
        if rtype == "6":  # entry detail
            rows.append({
                "record_type": "entry", "transaction_code": line[1:3].strip(),
                "routing": line[3:11].strip(), "account": line[12:29].strip(),
                "amount": line[29:39].strip(), "individual_name": line[54:76].strip(),
            })
        elif rtype == "5":  # batch header
            rows.append({"record_type": "batch_header",
                         "company_name": line[4:20].strip()})
        elif rtype == "1":
            rows.append({"record_type": "file_header"})
    return pd.DataFrame(rows).astype(str) if rows else pd.DataFrame()


# ---- PDF (pdfplumber tables + LLM fallback) ----
def _parse_pdf(raw: bytes) -> pd.DataFrame:
    try:
        import pdfplumber
        frames = []
        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            for page in pdf.pages:
                for tbl in page.extract_tables() or []:
                    if tbl and len(tbl) > 1:
                        frames.append(pd.DataFrame(tbl[1:], columns=tbl[0]))
        if frames:
            return pd.concat(frames, ignore_index=True).astype(str)
        # text -> LLM fallback
        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        res = parse_unstructured(text.encode("utf-8"), "doc.pdf")
        if res.get("columns"):
            return pd.DataFrame(res["rows"], columns=res["columns"]).astype(str)
    except Exception:
        pass
    return pd.DataFrame()


# ---- Vendor formats ----
def _parse_bloomberg_dlx(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    m = re.search(r"START-OF-DATA(.*?)END-OF-DATA", text, re.S)
    body = m.group(1).strip() if m else text
    return _read_delimited(body, _sniff_delimiter(body))


def _parse_marex_mond(raw: bytes) -> pd.DataFrame:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(raw.decode("utf-8", errors="replace"))
    rows = []
    for el in root.iter():
        if el.tag.split("}")[-1].upper() in ("TRADE", "DEAL", "TRANSACTION"):
            row = {c.tag.split("}")[-1]: (c.text or "").strip() for c in el}
            row.update({k: v for k, v in el.attrib.items()})
            if row:
                rows.append(row)
    return pd.DataFrame(rows).astype(str) if rows else _parse_xml(raw)


def _parse_markitwire(raw: bytes) -> pd.DataFrame:
    if b"fpml" in raw[:2000].lower() or b"FpML" in raw[:2000]:
        return _parse_fpml(raw)
    return _parse_xml(raw)


def _parse_dtcc_gtr(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    return _read_delimited(text, "|" if "|" in text.split("\n")[0] else _sniff_delimiter(text))


def _parse_reuters_rtc(raw: bytes, enc: str) -> pd.DataFrame:
    text = raw.decode(enc, errors="replace")
    return _read_delimited(text, _sniff_delimiter(text))


def _parse_avro(raw: bytes) -> pd.DataFrame:
    import fastavro
    return pd.json_normalize(list(fastavro.reader(io.BytesIO(raw)))).astype(str)


def _parse_orc(raw: bytes) -> pd.DataFrame:
    import pyarrow.orc as orc
    return orc.ORCFile(io.BytesIO(raw)).read().to_pandas().astype(str)


def _file_format_label(name: str, raw: bytes, enc: str) -> str:
    ext = os.path.splitext(name)[1].lower().lstrip(".")
    if _is_swift_mt(raw, enc):
        return "SWIFT MT"
    if _is_fix(raw, enc):
        return "FIX"
    if _is_iso20022(raw):
        return "ISO 20022"
    return ext.upper() if ext else "TEXT"


def load_file(upload_name: str, raw: bytes, delimiter=None):
    """Master dispatcher: route by extension, auto-sniff for .txt/unknown."""
    ext = os.path.splitext(upload_name)[1].lower()
    enc = _detect_encoding(raw)
    df = None

    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(io.BytesIO(raw), dtype=str)
    elif ext == ".parquet":
        df = pd.read_parquet(io.BytesIO(raw)).astype(str)
    elif ext == ".avro":
        df = _parse_avro(raw)
    elif ext == ".orc":
        df = _parse_orc(raw)
    elif ext == ".json":
        df = _parse_json_nested(raw)
    elif ext in (".fpml",):
        df = _parse_fpml(raw)
    elif ext in (".xbrl",):
        df = _parse_xbrl(raw)
    elif ext in (".mwire", ".markitwire"):
        df = _parse_markitwire(raw)
    elif ext in (".mx", ".pacs", ".pain", ".camt", ".sepa"):
        df = _parse_iso20022(raw) if _is_iso20022(raw) else _parse_xml(raw)
    elif ext in (".xml", ".mxml"):
        df = _parse_marex_mond(raw) if b"MOND" in raw[:500] or b"<Trade" in raw[:2000] else _parse_xml(raw)
    elif ext in (".mt", ".fin", ".swift"):
        df = _parse_swift_mt(raw, enc)
    elif ext == ".fix":
        df = _parse_fix(raw, enc)
    elif ext in (".ach", ".nacha"):
        df = _parse_nacha(raw, enc)
    elif ext == ".pdf":
        df = _parse_pdf(raw)
    elif ext in (".bbg", ".dlx", ".bdl"):
        df = _parse_bloomberg_dlx(raw, enc)
    elif ext in (".gtr", ".trace"):
        df = _parse_dtcc_gtr(raw, enc)
    elif ext == ".rtc":
        df = _parse_reuters_rtc(raw, enc)
    elif ext == ".csv":
        df = _parse_text(raw, enc, delimiter or ",")
    elif ext == ".tsv":
        df = _parse_text(raw, enc, delimiter or "\t")
    else:
        # .txt / unknown -> content sniffing
        if _is_swift_mt(raw, enc):
            df = _parse_swift_mt(raw, enc)
        elif _is_fix(raw, enc):
            df = _parse_fix(raw, enc)
        elif _is_iso20022(raw):
            df = _parse_iso20022(raw)
        elif raw[:1] in (b"{", b"["):
            df = _parse_json_nested(raw)
        elif raw[:1] == b"<":
            df = _parse_xml(raw)
        else:
            df = _parse_text(raw, enc, delimiter)

    if df is None or df.empty and df.shape[1] == 0:
        df = _parse_text(raw, enc, delimiter)
    df = df.fillna("")
    df.attrs["_format"] = _file_format_label(upload_name, raw, enc)
    df.attrs["_delimiter"] = delimiter or ""
    return df


# =========================================================================
# Key inference  (metadata detection + Phases 1-6)
# =========================================================================
_SYSTEM_GEN_EXACT = {
    "id", "pk", "row_id", "rowid", "rownum", "message_index",
    "created_at", "updated_at", "index", "idx", "_id", "seq", "sequence",
    "line_no", "lineno", "loaded_at", "ingested_at", "etl_timestamp",
}
_KEY_HINTS_EXACT = {
    "code", "uuid", "isin", "cusip", "sedol", "lei", "account", "accountid",
    "key", "ref", "reference", "tradeid", "id",
}
_KEY_SUFFIX_RE = re.compile(
    r"(_id|_key|_no|_ref|_code|_uuid|^id_|^ref_|^key_)$|^(id_|ref_|key_)", re.I)
_METADATA_PATTERNS = [
    re.compile(p, re.I) for p in (
        r"created.?(at|on|date|by)", r"updated.?(at|on|date|by)",
        r"modified.?(at|on|date|by)", r"loaded.?(at|on)", r"ingest",
        r"etl", r"audit", r"timestamp$", r"^ts$", r"last.?(run|seen|update)",
        r"row.?(num|id)", r"batch.?(id|num)", r"_seq$",
    )
]
_AUDIT_NAME_HINTS = ("created", "updated", "modified", "loaded", "audit",
                     "timestamp", "etl", "ingest", "by", "user")


_PROFILE_SAMPLE = 3000  # rows sampled for expensive heuristics on large frames


def _coerce_dt_q(series):
    return pd.to_datetime(series.replace("", np.nan), errors="coerce")


def _dt_ratio(series) -> float:
    """Fraction of (sampled) non-null values parseable as dates.
    Samples to keep dateutil's per-element parsing off the full column."""
    s = series.astype(str).str.strip()
    s = s[~s.str.lower().isin(_NULL_SENTINELS)]
    if len(s) == 0:
        return 0.0
    if len(s) > _PROFILE_SAMPLE:
        s = s.sample(_PROFILE_SAMPLE, random_state=0)
    # cheap pre-filter: must look date-ish before invoking the parser
    looks = s.str.contains(r"\d{1,4}[-/.:]\d", regex=True, na=False)
    if looks.mean() < 0.5:
        return 0.0
    return pd.to_datetime(s, errors="coerce").notna().mean()


def _is_metadata_col(col: str, dtype) -> bool:
    c = col.lower().strip()
    if c in _SYSTEM_GEN_EXACT:
        return True
    if any(p.search(c) for p in _METADATA_PATTERNS):
        return True
    if "datetime" in str(dtype) and any(h in c for h in _AUDIT_NAME_HINTS):
        return True
    return False


def _divergence(s1: pd.Series, s2: pd.Series) -> float:
    """1 - jaccard value overlap (high => the two files barely share values)."""
    return 1.0 - _cross_overlap(s1, s2)


def _metadata_score(col, df1, df2):
    """Data-driven signal that a column is metadata/audit rather than business.
    Returns (is_meta, reason). S4 business override fires first."""
    s1, s2 = df1[col], df2[col]
    overlap = _cross_overlap(s1, s2)
    # S4: >=90% value overlap => business dimension (hard override)
    if overlap >= 0.90:
        return False, "S4 business dimension (>=90% value overlap)"
    div = 1.0 - overlap
    c = col.lower()
    nm_audit = any(h in c for h in _AUDIT_NAME_HINTS)
    # S1: disjoint unique integers => surrogate key
    num1 = pd.to_numeric(s1.replace("", np.nan), errors="coerce")
    if num1.notna().mean() > 0.95 and _col_uniqueness(s1) > 0.99 and overlap < 0.05:
        if (num1.dropna() == num1.dropna().round()).all():
            return True, "S1 surrogate key (disjoint unique integers)"
    # S2: datetime + >80% divergence + audit name => audit timestamp
    # (only invoke date parsing when the name hints audit — avoids scanning every column)
    if div > 0.80 and nm_audit and ("datetime" in str(s1.dtype) or _dt_ratio(s1) > 0.8):
        return True, "S2 audit timestamp"
    # S3: low-cardinality string + <15% overlap + audit name => audit user
    if s1.nunique() < max(2, 0.2 * len(s1)) and overlap < 0.15 and nm_audit:
        return True, "S3 audit user"
    # N1: name pattern + >60% divergence => metadata fallback
    if _is_metadata_col(col, s1.dtype) and div > 0.60:
        return True, "N1 metadata fallback"
    return False, ""


def _split_meta_cols(cols, df1, df2):
    business, meta = [], []
    for c in cols:
        is_meta, _ = _metadata_score(c, df1, df2)
        (meta if is_meta else business).append(c)
    return business, meta


def _col_uniqueness(series: pd.Series) -> float:
    n = len(series)
    if n == 0:
        return 0.0
    nulls = (series.astype(str).str.strip().str.lower().isin(_NULL_SENTINELS)).sum()
    if nulls / n > 0.4:
        return 0.0
    return series.nunique(dropna=True) / n


def _combined_uniqueness(df, cols, sample=20000):
    sub = df[cols].head(sample)
    if len(sub) == 0:
        return 0.0
    return sub.drop_duplicates().shape[0] / len(sub)


def _is_constant(series):
    if len(series) <= 1:
        return False
    return series.nunique(dropna=True) <= 1


def _name_score(col: str) -> float:
    c = col.lower().strip()
    if c in _KEY_HINTS_EXACT:
        return 0.5
    if _KEY_SUFFIX_RE.search(c):
        return 0.3
    if any(h in c for h in ("id", "key", "ref", "code", "uuid")):
        return 0.1
    return 0.0


def _dtype_bonus(series):
    num = pd.to_numeric(series.replace("", np.nan), errors="coerce")
    if num.notna().mean() > 0.95:
        if (num.dropna() == num.dropna().round()).all():
            return 0.05  # integer
        return 0.02      # int-valued float
    return 0.03          # string


def _cross_overlap(s1: pd.Series, s2: pd.Series) -> float:
    a = set(s1.astype(str).head(5000)) - _NULL_SENTINELS
    b = set(s2.astype(str).head(5000)) - _NULL_SENTINELS
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _col_score(col, s1, s2):
    return min(_col_uniqueness(s1), _col_uniqueness(s2)) + _name_score(col) + _dtype_bonus(s1)


def infer_keys(df1, df2, common_cols, matched_cols=None):
    # Pre-filter: drop system-gen, >40% null, constant, zero cross-overlap.
    # If matched_cols given, restrict to semantically matched columns.
    pool = [c for c in common_cols if not matched_cols or c in set(matched_cols)]
    candidates = []
    for c in pool:
        if c.lower() in _SYSTEM_GEN_EXACT:
            continue
        u1, u2 = _col_uniqueness(df1[c]), _col_uniqueness(df2[c])
        if u1 == 0 or u2 == 0 or _is_constant(df1[c]):
            continue
        if _cross_overlap(df1[c], df2[c]) == 0:
            continue
        candidates.append((c, _col_score(c, df1[c], df2[c]), min(u1, u2)))
    candidates.sort(key=lambda x: x[1], reverse=True)
    cand_cols = [c for c, _, _ in candidates]

    # Phase 1: perfect single-col key (100% unique in both)
    for c, _, uniq in candidates:
        if uniq >= 0.999:
            return [c]

    from itertools import combinations
    # Phases 2-4: composite keys (2,3,4 cols) >= 99.5% unique
    top = cand_cols[:8]
    for size in (2, 3, 4):
        best = None
        for combo in combinations(top, size):
            combo = list(combo)
            if _combined_uniqueness(df1, combo) >= 0.995 and \
               _combined_uniqueness(df2, combo) >= 0.995:
                return combo
        if best:
            return best

    # Phase 5: best-effort single col > 50% unique
    if candidates and candidates[0][2] > 0.5:
        return [candidates[0][0]]

    # Phase 6: content-based fallback (no usable key)
    return []


# =========================================================================
# Compare engine
# =========================================================================
_MAX_DIFF_ROWS = 500
_MAX_DATA_COLS = 25


def _safe_str(v) -> str:
    s = str(v).strip()
    return "" if s.lower() in _NULL_SENTINELS else s


def _norm_cols(df, cols):
    """Vectorized: cast to str, strip, blank out null sentinels. Fast on big frames."""
    out = df.loc[:, cols].astype(str)
    for c in cols:
        s = out[c].str.strip()
        out[c] = s.mask(s.str.lower().isin(_NULL_SENTINELS), "")
    return out


def _make_key(df, keys):
    """Vectorized composite key string (C-level concat, no row-wise apply)."""
    ks = df[keys[0]].astype(str)
    for k in keys[1:]:
        ks = ks + "||" + df[k].astype(str)
    return ks


def _key_based_diff(df1, df2, keys, common_cols):
    """Fully vectorized: unique-key fast path, cumcount disambiguation for
    non-unique keys, numpy matrix compare for modified rows. O(n), not O(n^2)."""
    data_cols = [c for c in common_cols if c not in keys][:_MAX_DATA_COLS]
    a = _norm_cols(df1, keys + data_cols)
    b = _norm_cols(df2, keys + data_cols)
    a_key = _make_key(a, keys)
    b_key = _make_key(b, keys)

    # non-unique keys -> positional alignment via cumcount suffix
    if a_key.duplicated().any() or b_key.duplicated().any():
        a_key = a_key + "\x00" + a_key.groupby(a_key).cumcount().astype(str)
        b_key = b_key + "\x00" + b_key.groupby(b_key).cumcount().astype(str)

    a = a.set_index(pd.Index(a_key.to_numpy(dtype=object)))
    b = b.set_index(pd.Index(b_key.to_numpy(dtype=object)))
    a_idx, b_idx = a.index, b.index

    only1_idx = a_idx.difference(b_idx)
    only2_idx = b_idx.difference(a_idx)
    both = a_idx.intersection(b_idx)

    modified, modified_count = [], 0
    if len(both) and data_cols:
        aa = a.loc[both, data_cols]
        bb = b.loc[both, data_cols]
        neq = aa.to_numpy() != bb.to_numpy()          # vectorized bool matrix
        changed = neq.any(axis=1)
        modified_count = int(changed.sum())
        cap = both[changed][:_MAX_DIFF_ROWS]
        if len(cap):
            av = a.loc[cap, data_cols].to_numpy()
            bv = b.loc[cap, data_cols].to_numpy()
            cap_neq = av != bv
            for i, k in enumerate(cap):
                changes = {data_cols[j]: {"file1": str(av[i, j]), "file2": str(bv[i, j])}
                           for j in range(len(data_cols)) if cap_neq[i, j]}
                if changes:
                    modified.append({"key": str(k).split("\x00")[0], "changes": changes})

    clean = lambda k: str(k).split("\x00")[0]
    return {
        "keys": keys,
        "file1_only": [{"key": clean(k)} for k in only1_idx[:_MAX_DIFF_ROWS]],
        "file2_only": [{"key": clean(k)} for k in only2_idx[:_MAX_DIFF_ROWS]],
        "modified": modified,
        "counts": {
            "file1_only": int(len(only1_idx)),
            "file2_only": int(len(only2_idx)),
            "modified": modified_count,
            "matched": int(len(both)),
        },
    }


def _content_based_diff(df1, df2, common_cols):
    h1 = Counter(tuple(_safe_str(v) for v in row) for row in
                 df1[common_cols].head(10000).itertuples(index=False))
    h2 = Counter(tuple(_safe_str(v) for v in row) for row in
                 df2[common_cols].head(10000).itertuples(index=False))
    only1 = sum((h1 - h2).values())
    only2 = sum((h2 - h1).values())
    return {
        "keys": [],
        "method": "content-based (no key found)",
        "counts": {
            "file1_only": only1,
            "file2_only": only2,
            "matched": sum((h1 & h2).values()),
        },
    }


# --- Complex Reconciliation: side/quantity extraction from free text -----
# For trade data with no clean join key (e.g. a single "description" column
# like "BUY 500 shares AAPL" / "sold x200"), extract a buy/sell side and a
# quantity to use as a synthetic composite key so rows can still be matched.

_SIDE_BUY_RX = re.compile(r"\b(BUY|BOUGHT|LONG|PURCHASE[D]?)\b", re.I)
_SIDE_SELL_RX = re.compile(r"\b(SELL|SOLD|SHORT|SALE)\b", re.I)
_QTY_RX = re.compile(
    r"(?:QTY|QUANTITY|X)\s*[:#]?\s*([\d,]+(?:\.\d+)?)"
    r"|([\d,]+(?:\.\d+)?)\s*(?:SHARES?|UNITS?|LOTS?)\b",
    re.I,
)


def _extract_side_quantity(text) -> dict:
    """Best-effort buy/sell + quantity extraction from a free-text field."""
    t = str(text or "")
    side = None
    if _SIDE_BUY_RX.search(t):
        side = "BUY"
    elif _SIDE_SELL_RX.search(t):
        side = "SELL"
    qty = None
    m = _QTY_RX.search(t)
    if m:
        raw = m.group(1) or m.group(2)
        try:
            qty = float(raw.replace(",", ""))
        except ValueError:
            qty = None
    return {"side": side, "quantity": qty}


def complex_reconciliation(df1, df2, text_col, common_cols):
    """Fallback reconciliation when no clean key exists: derives a synthetic
    (side, quantity) key from a free-text description column on each side
    and reuses the standard keyed diff engine against it."""
    if text_col not in df1.columns or text_col not in df2.columns:
        return {"error": f"Column '{text_col}' not found in both files."}

    d1, d2 = df1.copy(), df2.copy()
    for d in (d1, d2):
        extracted = d[text_col].map(_extract_side_quantity)
        d["_side_extracted"] = [e["side"] or "" for e in extracted]
        d["_qty_extracted"] = [
            (f"{e['quantity']:g}" if e["quantity"] is not None else "") for e in extracted
        ]

    synthetic_keys = ["_side_extracted", "_qty_extracted"]
    data_cols = [c for c in common_cols if c not in synthetic_keys]
    result = _key_based_diff(d1, d2, synthetic_keys, data_cols)
    result["method"] = "complex-recon (side/quantity extracted from free text)"
    result["source_column"] = text_col
    result["extraction_coverage"] = {
        "file1_pct": round(float((d1["_side_extracted"] != "").mean() * 100), 1) if len(d1) else 0,
        "file2_pct": round(float((d2["_side_extracted"] != "").mean() * 100), 1) if len(d2) else 0,
    }
    return result


def _col_stats(df1, df2, common_cols):
    stats = {}
    for c in common_cols:
        s1, s2 = df1[c], df2[c]
        entry = {
            "dtype_file1": str(s1.dtype),
            "dtype_file2": str(s2.dtype),
            "nulls_file1": int((s1.astype(str).str.strip() == "").sum()),
            "nulls_file2": int((s2.astype(str).str.strip() == "").sum()),
            "unique_file1": int(s1.nunique()),
            "unique_file2": int(s2.nunique()),
        }
        stats[c] = entry
    return stats


def _inferred_dtype(series):
    nn = series.astype(str).str.strip()
    nn = nn[~nn.str.lower().isin(_NULL_SENTINELS)]
    if len(nn) == 0:
        return "empty"
    sample = nn.sample(_PROFILE_SAMPLE, random_state=0) if len(nn) > _PROFILE_SAMPLE else nn
    if pd.to_numeric(sample, errors="coerce").notna().mean() > 0.95:
        return "numeric"
    if _dt_ratio(sample) > 0.95:
        return "datetime"
    return "string"


def _detect_null_column_exceptions(df1, df2, data_cols):
    """Columns that hold data on one side but are entirely null on the other."""
    out = []
    for c in data_cols:
        s1 = df1[c].astype(str).str.strip()
        s2 = df2[c].astype(str).str.strip()
        f1_null = s1.str.lower().isin(_NULL_SENTINELS).all() and len(s1)
        f2_null = s2.str.lower().isin(_NULL_SENTINELS).all() and len(s2)
        f1_has = (~s1.str.lower().isin(_NULL_SENTINELS)).any()
        f2_has = (~s2.str.lower().isin(_NULL_SENTINELS)).any()
        if f1_has and f2_null:
            out.append({"column": c, "data_in": "file1", "null_in": "file2"})
        elif f2_has and f1_null:
            out.append({"column": c, "data_in": "file2", "null_in": "file1"})
    return out


def _type_mismatches(df1, df2, common_cols):
    out = []
    for c in common_cols:
        t1, t2 = _inferred_dtype(df1[c]), _inferred_dtype(df2[c])
        if t1 != t2 and "empty" not in (t1, t2):
            out.append({"column": c, "file1": t1, "file2": t2})
    return out


def _duplicate_detection(df, keys, common_cols):
    sub = df[keys] if keys else df[common_cols]
    dups = int(sub.duplicated(keep=False).sum())
    return {"duplicate_rows": dups,
            "duplicate_keys": int(sub.duplicated().sum()) if keys else dups}


def compare_dataframes(df1, df2, manual_keys=None, exclude_cols=None,
                       key_hints=None, matched_cols=None, force_data_cols=None):
    exclude_cols = set(exclude_cols or [])
    cols1 = [c for c in df1.columns if c not in exclude_cols]
    cols2 = [c for c in df2.columns if c not in exclude_cols]
    common = [c for c in cols1 if c in set(cols2)]
    schema_changes = {
        "only_in_file1": [c for c in cols1 if c not in set(cols2)],
        "only_in_file2": [c for c in cols2 if c not in set(cols1)],
    }
    if not common:
        return {
            "error": "No common columns between the two files.",
            "schema_changes": schema_changes,
        }
    # business vs metadata column split
    business, meta = _split_meta_cols(common, df1, df2)
    compare_cols = force_data_cols or business or common

    # Priority: manual_keys -> key_hints -> infer_keys()
    keys = manual_keys or (key_hints if key_hints else None) \
        or infer_keys(df1, df2, compare_cols, matched_cols=matched_cols)
    keys = [k for k in (keys or []) if k in common]

    if keys:
        diff = _key_based_diff(df1, df2, keys, compare_cols)
    else:
        diff = _content_based_diff(df1, df2, compare_cols)
    diff["schema_changes"] = schema_changes
    diff["col_stats"] = _col_stats(df1, df2, common)
    diff["row_counts"] = {"file1": len(df1), "file2": len(df2)}
    diff["type_mismatches"] = _type_mismatches(df1, df2, common)
    diff["null_column_exceptions"] = _detect_null_column_exceptions(df1, df2, compare_cols)
    diff["metadata_columns"] = meta
    diff["duplicates"] = {
        "file1": _duplicate_detection(df1, keys, compare_cols),
        "file2": _duplicate_detection(df2, keys, compare_cols),
    }
    return diff


# =========================================================================
# Data Quality engine (Phase 2 — full rule catalog + 7 weighted dimensions)
# =========================================================================
import re as _re

_DQ_FORMAT_PATTERNS = {
    "isin": _re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$"),
    "cusip": _re.compile(r"^[0-9A-Z]{9}$"),
    "sedol": _re.compile(r"^[0-9A-Z]{7}$"),
    "lei": _re.compile(r"^[A-Z0-9]{18}\d{2}$"),
    "bic": _re.compile(r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$"),
    "iban": _re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$"),
    "email": _re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "phone": _re.compile(r"^\+?[\d\s\-().]{7,20}$"),
    "date_iso": _re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$"),
    "url": _re.compile(r"^https?://[^\s]+$", _re.I),
    "currency_code": _re.compile(r"^[A-Z]{3}$"),
    "mic": _re.compile(r"^[A-Z0-9]{4}$"),
}


def _grade(score):
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _coerce_num(series):
    return pd.to_numeric(series.replace("", np.nan), errors="coerce")


def _coerce_dt(series):
    return pd.to_datetime(series.replace("", np.nan), errors="coerce")


# --- BFSI identifier checksum validators (structural regex above only
# confirms shape; these confirm the check digit) ---------------------------

def _validate_isin_checksum(isin: str) -> bool:
    """ISO 6166 — Luhn (mod 10) over the letter-expanded 12-char ISIN."""
    digits = ""
    for ch in isin:
        digits += ch if ch.isdigit() else str(ord(ch) - 55)  # A=10 .. Z=35
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _validate_cusip_checksum(cusip: str) -> bool:
    """9-char CUSIP modulus-10 weighted check digit."""
    total = 0
    for i, ch in enumerate(cusip[:-1]):
        if ch.isdigit():
            v = int(ch)
        elif ch == "*":
            v = 36
        elif ch == "@":
            v = 37
        elif ch == "#":
            v = 38
        else:
            v = ord(ch) - 55  # A=10 .. Z=35
        if i % 2 == 1:
            v *= 2
        total += v // 10 + v % 10
    check = (10 - (total % 10)) % 10
    return check == int(cusip[-1]) if cusip[-1].isdigit() else False


def _validate_lei_checksum(lei: str) -> bool:
    """ISO 17442 — mod-97 == 1 over the letter-expanded 20-char LEI."""
    converted = "".join(ch if ch.isdigit() else str(ord(ch) - 55) for ch in lei)
    return int(converted) % 97 == 1


def _validate_iban_checksum(iban: str) -> bool:
    """ISO 13616 — mod-97 == 1 after moving the first 4 chars to the end."""
    rearranged = iban[4:] + iban[:4]
    converted = "".join(ch if ch.isdigit() else str(ord(ch) - 55) for ch in rearranged)
    return int(converted) % 97 == 1


_BFSI_CHECKSUM_VALIDATORS = {
    "isin": _validate_isin_checksum,
    "cusip": _validate_cusip_checksum,
    "lei": _validate_lei_checksum,
    "iban": _validate_iban_checksum,
    # bic has no ISO check-digit — structural regex above is the full check
}


def _apply_rule(series, rule):
    """Apply one rule to a Series. Returns {passed, failed, skipped, total, ...}."""
    rtype = rule.get("type", "")
    s = series.astype(str).str.strip()
    nonnull = s[~s.str.lower().isin(_NULL_SENTINELS)]
    total = len(s)
    out = {"type": rtype, "total": total, "passed": 0, "failed": 0, "skipped": 0}

    def _mask_fail(mask, scope=None):
        base = scope if scope is not None else s
        failed = int(mask.sum())
        out["failed"] = failed
        out["passed"] = len(base) - failed
        out["skipped"] = total - len(base)

    try:
        if rtype == "not_null":
            _mask_fail(s.str.lower().isin(_NULL_SENTINELS))
        elif rtype == "unique":
            dup = nonnull.duplicated(keep=False)
            _mask_fail(dup, nonnull)
        elif rtype == "no_duplicates_in_set":
            _mask_fail(nonnull.duplicated(keep=False), nonnull)
        elif rtype in ("min", "max", "range", "positive", "non_negative",
                       "negative", "integer_only"):
            num = _coerce_num(nonnull)
            valid = num.dropna()
            if rtype == "min":
                fail = valid < float(rule["value"])
            elif rtype == "max":
                fail = valid > float(rule["value"])
            elif rtype == "range":
                fail = (valid < float(rule["min"])) | (valid > float(rule["max"]))
            elif rtype == "positive":
                fail = valid <= 0
            elif rtype == "non_negative":
                fail = valid < 0
            elif rtype == "negative":
                fail = valid >= 0
            else:  # integer_only
                fail = valid != valid.round()
            _mask_fail(fail, valid)
        elif rtype in ("min_length", "max_length", "exact_length"):
            ln = nonnull.str.len()
            if rtype == "min_length":
                fail = ln < int(rule["value"])
            elif rtype == "max_length":
                fail = ln > int(rule["value"])
            else:
                fail = ln != int(rule["value"])
            _mask_fail(fail, nonnull)
        elif rtype == "pattern":
            rx = _re.compile(rule["value"])
            _mask_fail(~nonnull.map(lambda v: bool(rx.match(v))), nonnull)
        elif rtype == "allowed_values":
            allowed = set(str(x) for x in rule["value"])
            _mask_fail(~nonnull.isin(allowed), nonnull)
        elif rtype == "not_allowed_values":
            banned = set(str(x) for x in rule["value"])
            _mask_fail(nonnull.isin(banned), nonnull)
        elif rtype in ("no_whitespace", "no_special_chars", "uppercase",
                       "lowercase", "numeric_only"):
            if rtype == "no_whitespace":
                fail = nonnull.str.contains(r"\s")
            elif rtype == "no_special_chars":
                fail = nonnull.str.contains(r"[^A-Za-z0-9 ]")
            elif rtype == "uppercase":
                fail = nonnull != nonnull.str.upper()
            elif rtype == "lowercase":
                fail = nonnull != nonnull.str.lower()
            else:  # numeric_only
                fail = ~nonnull.str.match(r"^\d+$")
            _mask_fail(fail, nonnull)
        elif rtype in ("date_format", "date_iso", "not_future_date",
                       "not_past_date", "date_range"):
            dt = _coerce_dt(nonnull)
            valid = dt.dropna()
            if rtype in ("date_format", "date_iso"):
                fail = dt.isna()
                _mask_fail(fail, nonnull)
            elif rtype == "not_future_date":
                _mask_fail(valid > pd.Timestamp.now(), valid)
            elif rtype == "not_past_date":
                _mask_fail(valid < pd.Timestamp.now(), valid)
            else:  # date_range
                lo = pd.Timestamp(rule["min"]); hi = pd.Timestamp(rule["max"])
                _mask_fail((valid < lo) | (valid > hi), valid)
        elif rtype == "freshness_days":
            dt = _coerce_dt(nonnull).dropna()
            age = (pd.Timestamp.now() - dt).dt.days
            _mask_fail(age > int(rule["value"]), dt)
        elif rtype in ("completeness_pct", "uniqueness_pct"):
            if rtype == "completeness_pct":
                actual = len(nonnull) / total * 100 if total else 0
            else:
                actual = nonnull.nunique() / total * 100 if total else 0
            ok = actual >= float(rule["value"])
            out["passed"], out["failed"] = (total, 0) if ok else (0, total)
            out["actual_pct"] = round(actual, 2)
        elif rtype == "decimal_places":
            dp = int(rule["value"])
            def _dp_ok(v):
                return ("." in v) and len(v.split(".")[1]) <= dp or "." not in v
            _mask_fail(~nonnull.map(_dp_ok), nonnull)
        elif rtype in _DQ_FORMAT_PATTERNS or rtype.replace("_format", "") in _DQ_FORMAT_PATTERNS:
            key = rtype.replace("_format", "")
            rx = _DQ_FORMAT_PATTERNS[key]
            checksum_fn = _BFSI_CHECKSUM_VALIDATORS.get(key)
            if checksum_fn:
                def _fmt_ok(v, _rx=rx, _fn=checksum_fn):
                    v = v.upper()
                    if not _rx.match(v):
                        return False
                    try:
                        return _fn(v)
                    except (ValueError, ZeroDivisionError):
                        return False
                _mask_fail(~nonnull.map(_fmt_ok), nonnull)
            else:
                _mask_fail(~nonnull.str.upper().map(lambda v: bool(rx.match(v))), nonnull)
        elif rtype in ("row_count_min", "row_count_max"):
            ok = (total >= int(rule["value"])) if rtype == "row_count_min" else (total <= int(rule["value"]))
            out["passed"], out["failed"] = (total, 0) if ok else (0, total)
        else:
            out["skipped"] = total
            out["note"] = f"unknown rule type '{rtype}'"
    except Exception as e:
        out["skipped"] = total
        out["error"] = str(e)
    return out


def _dq_score(total_rows, dup_rows, col_reports, rule_results):
    """7 dimensions with dynamic weights. Inactive dims redistribute to completeness."""
    weights = {"completeness": 0.25, "uniqueness": 0.15, "validity": 0.20,
               "consistency": 0.15, "conformity": 0.10, "precision": 0.10,
               "timeliness": 0.05}

    comp = float(np.mean([c["completeness_pct"] for c in col_reports])) if col_reports else 100.0
    uniq = float(np.mean([c["uniqueness_pct"] for c in col_reports])) if col_reports else 100.0

    # validity from rule pass rate
    tot_checked = sum(r["passed"] + r["failed"] for r in rule_results)
    tot_passed = sum(r["passed"] for r in rule_results)
    validity = (tot_passed / tot_checked * 100) if tot_checked else 100.0

    consistency = 100.0 - (dup_rows / total_rows * 100 if total_rows else 0)

    # conformity: outlier rate (suppressed for highly skewed cols)
    conf_vals = [c["conformity_pct"] for c in col_reports if "conformity_pct" in c]
    conformity = float(np.mean(conf_vals)) if conf_vals else None

    # precision: only active when a column declared decimal places / had dp range
    prec_vals = [c["precision_pct"] for c in col_reports if "precision_pct" in c]
    precision = float(np.mean(prec_vals)) if prec_vals else None

    # timeliness: only active when freshness rule fired
    fresh = [r for r in rule_results if r["type"] == "freshness_days" and (r["passed"] + r["failed"])]
    if fresh:
        fp = sum(r["passed"] for r in fresh); ft = sum(r["passed"] + r["failed"] for r in fresh)
        timeliness = fp / ft * 100 if ft else 100.0
    else:
        timeliness = None

    dims = {"completeness": comp, "uniqueness": uniq, "validity": validity,
            "consistency": consistency, "conformity": conformity,
            "precision": precision, "timeliness": timeliness}

    active = {k: v for k, v in dims.items() if v is not None}
    active_weight = sum(weights[k] for k in active)
    score = sum(dims[k] * weights[k] for k in active) / active_weight if active_weight else 0.0
    return score, {k: round(v, 1) for k, v in dims.items() if v is not None}


def analyze_quality(df, name="dataset", data_dict=None, rules=None, user_hints=None):
    n = len(df)
    cols = list(df.columns)
    user_hints = user_hints or {}
    rules = rules or []
    col_reports = []
    rule_results = []
    near_keys = []
    numeric_cols = []

    for c in cols:
        s = df[c].astype(str).str.strip()
        nonnull = s[~s.str.lower().isin(_NULL_SENTINELS)]
        nulls = n - len(nonnull)
        completeness = (len(nonnull) / n * 100) if n else 0.0
        uniqueness = (df[c].nunique() / n * 100) if n else 0.0
        if 80.0 <= uniqueness < 95.0:
            near_keys.append(c)
        report = {
            "column": c,
            "completeness_pct": round(completeness, 2),
            "uniqueness_pct": round(uniqueness, 2),
            "null_count": int(nulls),
            "distinct": int(df[c].nunique()),
        }

        num = _coerce_num(nonnull)
        is_numeric = num.notna().sum() > max(1, 0.8 * len(nonnull)) if len(nonnull) else False
        if is_numeric:
            numeric_cols.append(c)
            valid = num.dropna()
            mean, std = float(valid.mean()), float(valid.std() or 0)
            skew = float(valid.skew()) if len(valid) > 2 else 0.0
            report["numeric"] = {
                "min": _sanitize_json(valid.min()), "max": _sanitize_json(valid.max()),
                "mean": round(mean, 4), "std": round(std, 4),
                "skew": round(skew, 3),
            }
            # conformity (outliers via IQR) — suppressed if |skew|>2
            if std > 0 and abs(skew) <= 2 and len(valid) >= 8:
                q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
                iqr = q3 - q1
                outliers = ((valid < q1 - 1.5 * iqr) | (valid > q3 + 1.5 * iqr)).sum()
                report["conformity_pct"] = round((1 - outliers / len(valid)) * 100, 2)
                report["outliers"] = int(outliers)
            # precision: declared decimal places or observed dp range
            dps = nonnull.map(lambda v: len(v.split(".")[1]) if "." in v else 0)
            declared = (user_hints.get("precision_hints") or {}).get(c)
            if declared is not None or (dps.max() - dps.min() > 0):
                target = int(declared) if declared is not None else int(dps.mode().iloc[0])
                report["precision_pct"] = round((dps <= target).mean() * 100, 2)
                report["decimal_places"] = target
        else:
            lengths = nonnull.str.len()
            if len(lengths):
                report["string"] = {
                    "min_length": int(lengths.min()), "max_length": int(lengths.max()),
                    "avg_length": round(float(lengths.mean()), 1),
                    "leading_trailing_space": int((nonnull != s[~s.str.lower().isin(_NULL_SENTINELS)]).sum()),
                }
            # BFSI format auto-detect
            for fmt, rx in _DQ_FORMAT_PATTERNS.items():
                if len(nonnull) and nonnull.str.upper().map(lambda v: bool(rx.match(v))).mean() > 0.8:
                    report["detected_format"] = fmt
                    break

        # ---- auto-detected rules ----
        auto = []
        nm = c.lower()
        if 0 < nulls / n <= 0.5 if n else False:
            auto.append({"type": "not_null", "column": c, "auto": True})
        if uniqueness >= 99.5 and (_name_score(c) > 0 or nm in _KEY_HINTS_EXACT):
            auto.append({"type": "unique", "column": c, "auto": True})
        if not is_numeric and 0 < df[c].nunique() <= 20:
            auto.append({"type": "allowed_values", "column": c,
                         "value": sorted(nonnull.unique().tolist()), "auto": True})
        if "detected_format" in report:
            auto.append({"type": report["detected_format"] + "_format",
                         "column": c, "auto": True})
        # mandatory / force-unique hints
        if c in (user_hints.get("mandatory") or []):
            auto.append({"type": "not_null", "column": c, "auto": True})
        if c in (user_hints.get("force_unique") or []):
            auto.append({"type": "unique", "column": c, "auto": True})

        report["rules_applied"] = []
        for rule in auto + [r for r in rules if r.get("column") == c]:
            res = _apply_rule(df[c], rule)
            res["column"] = c
            res["auto"] = rule.get("auto", False)
            rule_results.append(res)
            report["rules_applied"].append(res)

        col_reports.append(report)

    # dataset-level rules (no column)
    for rule in [r for r in rules if not r.get("column")]:
        rtype = rule.get("type")
        if rtype in ("row_count_min", "row_count_max"):
            res = _apply_rule(df.iloc[:, 0] if cols else pd.Series([], dtype=str), rule)
            rule_results.append(res)

    dup_rows = int(df.duplicated().sum())
    score, dims = _dq_score(n, dup_rows, col_reports, rule_results)

    # Cross-column correlations (numeric pairs, |r| > 0.7) — pandas .corr()
    # does pairwise-complete NaN handling, so per-column null counts differing
    # is fine.
    correlations = []
    if len(numeric_cols) >= 2:
        numeric_df = df[numeric_cols].apply(
            lambda col: pd.to_numeric(col.astype(str).str.strip(), errors="coerce"))
        corr_matrix = numeric_df.corr()
        for i, c1 in enumerate(numeric_cols):
            for c2 in numeric_cols[i + 1:]:
                r = corr_matrix.loc[c1, c2]
                if pd.notna(r) and abs(r) > 0.7:
                    correlations.append({"col_a": c1, "col_b": c2, "r": round(float(r), 3)})

    return {
        "name": name,
        "rows": n,
        "columns": len(cols),
        "duplicate_rows": dup_rows,
        "dimensions": dims,
        "score": round(score, 1),
        "grade": _grade(score),
        "columns_detail": col_reports,
        "rules_evaluated": len(rule_results),
        "rules_failed": sum(1 for r in rule_results if r["failed"] > 0),
        "near_key_columns": near_keys,
        "correlations": correlations,
    }


# =========================================================================
# Reference document handling
# =========================================================================
_DQ_KEYS = {"data type", "datatype", "type", "description", "definition", "format"}
_RULES_KEYS = {"rule", "constraint", "validation", "check", "condition", "severity"}
_MAP_KEYS = {"source", "target", "mapping", "source column", "target column", "transformation"}


def _classify_ref_doc(df):
    cols = {str(c).lower().strip() for c in df.columns}
    if cols & _MAP_KEYS:
        return "mapping"
    if cols & _RULES_KEYS:
        return "rules"
    if cols & _DQ_KEYS:
        return "data_dict"
    return "general"


def _parse_data_dictionary(df):
    out = {}
    lc = {str(c).lower().strip(): c for c in df.columns}
    name_col = next((lc[k] for k in ("column", "field", "name", "column name") if k in lc), df.columns[0])
    for _, row in df.iterrows():
        key = str(row[name_col]).strip()
        if key:
            out[key] = {str(c): str(row[c]) for c in df.columns}
    return out


def _parse_business_rules(df):
    return [{str(c): str(row[c]) for c in df.columns} for _, row in df.iterrows()]


def _parse_mapping_spec(df):
    return [{str(c): str(row[c]) for c in df.columns} for _, row in df.iterrows()]


def _load_and_classify_ref_docs(uploads):
    merged = {"data_dict": {}, "rules": [], "mapping": [], "general": []}
    for name, raw in uploads:
        try:
            df = load_file(name, raw)
        except Exception:
            continue
        kind = _classify_ref_doc(df)
        if kind == "data_dict":
            merged["data_dict"].update(_parse_data_dictionary(df))
        elif kind == "rules":
            merged["rules"].extend(_parse_business_rules(df))
        elif kind == "mapping":
            merged["mapping"].extend(_parse_mapping_spec(df))
        else:
            merged["general"].append(name)
    return merged


# =========================================================================
# Column Mapping engine
# =========================================================================
def _jaccard_values(s1, s2, sample=2000):
    a = set(s1.astype(str).head(sample)) - _NULL_SENTINELS
    b = set(s2.astype(str).head(sample)) - _NULL_SENTINELS
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def analyze_mapping(df1, df2, file1_name="file1", file2_name="file2", ref=None):
    cols1 = list(df1.columns)
    cols2 = list(df2.columns)
    norm = lambda x: _re.sub(r"[^a-z0-9]", "", str(x).lower())
    n2 = {norm(c): c for c in cols2}
    matched, used2 = [], set()

    # Step 1: exact (normalised) name match
    for c1 in cols1:
        if norm(c1) in n2 and n2[norm(c1)] not in used2:
            t = n2[norm(c1)]
            matched.append({"source": c1, "target": t, "method": "exact_name", "confidence": 1.0})
            used2.add(t)

    remaining1 = [c for c in cols1 if c not in {m["source"] for m in matched}]
    remaining2 = [c for c in cols2 if c not in used2]

    # Step 2: fuzzy name match
    for c1 in list(remaining1):
        best, score = None, 0.0
        for c2 in remaining2:
            r = SequenceMatcher(None, norm(c1), norm(c2)).ratio()
            if r > score:
                best, score = c2, r
        if best and score >= 0.7:
            matched.append({"source": c1, "target": best, "method": "fuzzy_name",
                            "confidence": round(score, 2)})
            used2.add(best); remaining2.remove(best); remaining1.remove(c1)

    # Step 3: sample-based content match (Jaccard)
    for c1 in list(remaining1):
        best, score = None, 0.0
        for c2 in remaining2:
            j = _jaccard_values(df1[c1], df2[c2])
            if j > score:
                best, score = c2, j
        if best and score >= 0.6:
            matched.append({"source": c1, "target": best, "method": "content_jaccard",
                            "confidence": round(score, 2)})
            used2.add(best); remaining2.remove(best); remaining1.remove(c1)

    # Step 4: LLM semantic match for any remaining columns
    llm_used = False
    if AI_CONFIGURED and remaining1 and remaining2:
        sys = ("Match source columns to target columns by meaning. "
               f"Source (unmatched): {remaining1}. Target (unmatched): {remaining2}. "
               "Return ONLY a JSON list of {source, target, confidence} (0-1). "
               "Omit a source if there is no good target.")
        ans = _ask_llm_safe([{"role": "user", "content": "match them"}], system=sys)
        try:
            pairs = json.loads(ans[ans.find("["):ans.rfind("]") + 1])
            for p in pairs:
                s, t = p.get("source"), p.get("target")
                if s in remaining1 and t in remaining2:
                    matched.append({"source": s, "target": t, "method": "llm_semantic",
                                    "confidence": round(float(p.get("confidence", 0.5)), 2)})
                    remaining1.remove(s); remaining2.remove(t)
                    llm_used = True
        except Exception:
            pass

    specs = [_build_field_spec(m["source"], m["target"], df1, df2, ref) for m in matched]
    return {
        "file1": file1_name, "file2": file2_name,
        "matched": matched,
        "field_specs": specs,
        "unmatched_file1": remaining1,
        "unmatched_file2": remaining2,
        "llm_semantic_used": llm_used,
    }


def _build_field_spec(src, tgt, df1, df2, ref=None):
    s1 = df1[src].astype(str).str.strip()
    s2 = df2[tgt].astype(str).str.strip()
    nn1 = s1[~s1.str.lower().isin(_NULL_SENTINELS)]
    nn2 = s2[~s2.str.lower().isin(_NULL_SENTINELS)]
    sdt = "numeric" if _coerce_num(nn1).notna().mean() > 0.8 else "string"
    tdt = "numeric" if _coerce_num(nn2).notna().mean() > 0.8 else "string"
    spec = {
        "source_column": src, "target_column": tgt,
        "source_dtype": sdt, "target_dtype": tdt,
        "data_type": tdt,
        "mandatory": bool(len(nn2) == len(s2)),
        "not_null": bool(len(nn2) == len(s2)),
        "unique": bool(df2[tgt].nunique() == len(s2)),
        "value_in_list": sorted(nn2.unique().tolist())[:20] if 0 < df2[tgt].nunique() <= 20 else None,
        "min_value": _sanitize_json(_coerce_num(nn2).min()) if tdt == "numeric" else None,
        "max_value": _sanitize_json(_coerce_num(nn2).max()) if tdt == "numeric" else None,
        "regex_pattern": None,
        "condition": None,
        "transformation": "direct" if sdt == tdt else f"cast {sdt}->{tdt}",
        "business_rule": None,
        "severity": "high" if len(nn2) == len(s2) else "medium",
        "description": "",
        "sample_source_values": nn1.head(3).tolist(),
        "sample_target_values": nn2.head(3).tolist(),
        "null_pct_source": round((len(s1) - len(nn1)) / len(s1) * 100, 1) if len(s1) else 0.0,
        "null_pct_target": round((len(s2) - len(nn2)) / len(s2) * 100, 1) if len(s2) else 0.0,
    }
    spec["exceptions"] = _compute_field_exceptions(spec, df1, df2)
    return spec


def _compute_field_exceptions(spec, df1, df2):
    src, tgt = spec["source_column"], spec["target_column"]
    s1 = df1[src].astype(str).str.strip()
    s2 = df2[tgt].astype(str).str.strip()
    exc = []
    if spec["source_dtype"] != spec["target_dtype"]:
        exc.append({"type": "type_mismatch",
                    "detail": f"{spec['source_dtype']} vs {spec['target_dtype']}"})
    if spec["not_null"]:
        nv = int(s2.str.lower().isin(_NULL_SENTINELS).sum())
        if nv:
            exc.append({"type": "null_violation", "count": nv})
    if spec["unique"]:
        dv = int(s2[~s2.str.lower().isin(_NULL_SENTINELS)].duplicated().sum())
        if dv:
            exc.append({"type": "uniqueness_violation", "count": dv})
    if spec.get("value_in_list"):
        allowed = set(str(x) for x in spec["value_in_list"])
        bad = int((~s2[~s2.str.lower().isin(_NULL_SENTINELS)].isin(allowed)).sum())
        if bad:
            exc.append({"type": "value_not_in_list", "count": bad})
    if spec.get("min_value") is not None:
        num = _coerce_num(s2).dropna()
        below = int((num < spec["min_value"]).sum())
        if below:
            exc.append({"type": "below_min", "count": below})
    if spec.get("max_value") is not None:
        num = _coerce_num(s2).dropna()
        above = int((num > spec["max_value"]).sum())
        if above:
            exc.append({"type": "above_max", "count": above})
    return exc


# =========================================================================
# Governance engine (PII / sensitivity)
# =========================================================================
_PII_PATTERNS = {
    "email": _DQ_FORMAT_PATTERNS["email"],
    "phone": _re.compile(r"^\+?[\d\s\-().]{9,20}$"),
    "ssn": _re.compile(r"^\d{3}-?\d{2}-?\d{4}$"),
    "credit_card": _re.compile(r"^\d{13,19}$"),
    "ip_address": _re.compile(r"^\d{1,3}(\.\d{1,3}){3}$"),
    "iban": _DQ_FORMAT_PATTERNS["iban"],
}
_PII_NAME_HINTS = {
    "name": "name", "firstname": "name", "lastname": "name", "fullname": "name",
    "surname": "name", "email": "email", "mail": "email", "phone": "phone",
    "mobile": "phone", "ssn": "ssn", "social": "ssn", "dob": "dob",
    "birth": "dob", "address": "address", "street": "address", "zip": "address",
    "postcode": "address", "passport": "national_id", "nationalid": "national_id",
}


def analyze_governance(df, name="dataset"):
    cols_out = []
    risk_rank = {"public": 0, "internal": 1, "confidential": 2, "restricted": 3}
    overall = "public"
    for c in df.columns:
        s = df[c].astype(str).str.strip()
        nonnull = s[~s.str.lower().isin(_NULL_SENTINELS)]
        pii_types = []
        nm = _re.sub(r"[^a-z0-9]", "", str(c).lower())
        for hint, kind in _PII_NAME_HINTS.items():
            if hint in nm:
                pii_types.append(kind)
                break
        for kind, rx in _PII_PATTERNS.items():
            if len(nonnull) and nonnull.map(lambda v: bool(rx.match(v))).mean() > 0.7:
                if kind not in pii_types:
                    pii_types.append(kind)
        pii = bool(pii_types)
        if any(t in ("ssn", "credit_card", "national_id", "passport") for t in pii_types):
            sensitivity = "restricted"
        elif any(t in ("email", "phone", "dob", "address", "iban") for t in pii_types):
            sensitivity = "confidential"
        elif pii:
            sensitivity = "internal"
        else:
            sensitivity = "public"
        if risk_rank[sensitivity] > risk_rank[overall]:
            overall = sensitivity
        cols_out.append({
            "name": c, "pii_detected": pii, "pii_types": pii_types,
            "sensitivity": sensitivity,
        })
    recs = []
    if overall in ("confidential", "restricted"):
        recs.append("Apply column-level encryption / masking to PII columns.")
        recs.append("Restrict access with role-based controls and audit logging.")
    if any(co["sensitivity"] == "restricted" for co in cols_out):
        recs.append("Define and enforce a retention policy for restricted data.")
    return {
        "name": name,
        "columns": cols_out,
        "overall_risk": {"public": "low", "internal": "medium",
                         "confidential": "high", "restricted": "critical"}[overall],
        "recommendations": recs,
    }


# =========================================================================
# Lineage / reconciliation engine
# =========================================================================
def _apply_transform(df, op):
    """Apply a single transform op {col, op, params} to a DataFrame copy."""
    o = op.get("op")
    col = op.get("col")
    p = op.get("params", {}) or {}
    out = df.copy()
    try:
        if o == "rename_col":
            out = out.rename(columns={col: p["to"]})
        elif o == "drop_col":
            out = out.drop(columns=[col], errors="ignore")
        elif o == "fill_null":
            out[col] = out[col].replace("", p.get("value", "")).fillna(p.get("value", ""))
        elif o == "to_upper":
            out[col] = out[col].astype(str).str.upper()
        elif o == "to_lower":
            out[col] = out[col].astype(str).str.lower()
        elif o == "strip_whitespace":
            out[col] = out[col].astype(str).str.strip()
        elif o == "cast_type":
            if p.get("to") == "numeric":
                out[col] = _coerce_num(out[col].astype(str))
            elif p.get("to") == "datetime":
                out[col] = _coerce_dt(out[col].astype(str))
            else:
                out[col] = out[col].astype(str)
        elif o == "round_numeric":
            out[col] = _coerce_num(out[col].astype(str)).round(int(p.get("places", 2)))
        elif o == "abs_value":
            out[col] = _coerce_num(out[col].astype(str)).abs()
        elif o in ("multiply", "divide", "add", "subtract"):
            v = float(p["value"]); base = _coerce_num(out[col].astype(str))
            out[col] = {"multiply": base * v, "divide": base / v,
                        "add": base + v, "subtract": base - v}[o]
        elif o == "replace_value":
            out[col] = out[col].astype(str).replace(p.get("from", ""), p.get("to", ""))
        elif o == "add_col":
            out[p["name"]] = p.get("value", "")
        elif o == "concat_cols":
            out[p["name"]] = out[p["cols"]].astype(str).agg(p.get("sep", "").join, axis=1)
        elif o == "filter_rows":
            out = out[out[col].astype(str) == str(p.get("equals"))]
    except Exception:
        pass
    return out


def _normalize_numeric_str(series):
    """Stringify a column, rendering integral floats without a trailing '.0'
    so transformed numerics compare cleanly against source strings."""
    num = _coerce_num(series.astype(str))
    if len(num) and num.notna().mean() > 0.95:
        def fmt(x):
            if pd.isna(x):
                return ""
            f = float(x)
            return str(int(f)) if f.is_integer() else repr(f)
        return num.map(fmt)
    return series.astype(str)


_AGG_FUNCS = {"sum", "mean", "count", "min", "max", "median", "std", "nunique"}


def _apply_aggregation(df, agg):
    """agg = {group_by_cols:[...], agg_col:str, agg_func:str}."""
    gb = agg.get("group_by_cols") or []
    col = agg.get("agg_col")
    func = (agg.get("agg_func") or "sum").lower()
    if not gb or not col or func not in _AGG_FUNCS:
        return df
    work = df.copy()
    work[col] = _coerce_num(work[col].astype(str))
    grouped = work.groupby(gb, dropna=False)[col].agg(func).reset_index()
    return grouped


def _apply_parse_cols(df, parse_cols):
    """parse_cols: list of [src, dst] pairs computing a derived numeric column,
    or {name, expr} where expr uses existing column names (safe arithmetic)."""
    out = df.copy()
    for spec in parse_cols or []:
        try:
            if isinstance(spec, dict) and spec.get("expr"):
                # evaluate a restricted arithmetic expression over numeric cols
                env = {c: _coerce_num(out[c].astype(str)) for c in out.columns}
                out[spec["name"]] = eval(spec["expr"], {"__builtins__": {}}, env)
            elif isinstance(spec, (list, tuple)) and len(spec) == 2:
                src, dst = spec
                out[dst] = _coerce_num(out[src].astype(str))
        except Exception:
            pass
    return out


def _extract_lineage_params(query, cols1, cols2):
    """Use the LLM to turn a natural-language reconciliation request into
    {parse_cols, col_map, transforms, agg}. Returns {} if no LLM / parse fails."""
    if not AI_CONFIGURED or not query:
        return {}
    sys = (
        "Convert the user's reconciliation request into JSON with keys: "
        "parse_cols (list of {name,expr}), col_map (file1->file2 name dict), "
        "transforms (list of {op,col,params}), agg ({group_by_cols,agg_col,agg_func}). "
        f"File1 columns: {cols1}. File2 columns: {cols2}. "
        "Valid ops: rename_col, drop_col, fill_null, cast_type, to_upper, to_lower, "
        "strip_whitespace, round_numeric, abs_value, multiply, divide, add, subtract, "
        "replace_value, add_col, concat_cols, filter_rows. Return ONLY JSON."
    )
    ans = _ask_llm_safe([{"role": "user", "content": query}], system=sys)
    try:
        return json.loads(ans[ans.find("{"):ans.rfind("}") + 1])
    except Exception:
        return {}


def analyze_lineage(df1, df2, transforms=None, col_map=None, manual_keys=None,
                    parse_cols=None, agg=None, query=None):
    # Optionally derive the spec from a natural-language query.
    if query:
        nl = _extract_lineage_params(query, list(df1.columns), list(df2.columns))
        transforms = transforms or nl.get("transforms")
        col_map = col_map or nl.get("col_map")
        parse_cols = parse_cols or nl.get("parse_cols")
        agg = agg or nl.get("agg")

    transforms = transforms or []
    applied = []
    a = df1.copy()
    a = _apply_parse_cols(a, parse_cols)
    for op in transforms:
        a = _apply_transform(a, op)
        applied.append(op)
    if col_map:
        a = a.rename(columns={k: v for k, v in col_map.items() if k in a.columns})
    b = df2.copy()
    if agg:
        a = _apply_aggregation(a, agg)
        b = _apply_aggregation(b, agg)
    for c in a.columns:
        a[c] = _normalize_numeric_str(a[c])
    for c in b.columns:
        b[c] = _normalize_numeric_str(b[c])
    result = compare_dataframes(a, b, manual_keys=manual_keys)
    result["transforms_applied"] = applied
    result["column_mapping"] = col_map or {}
    result["parse_cols"] = parse_cols or []
    result["aggregation"] = agg or {}
    return result


# =========================================================================
# Parse agent (unstructured -> table)
# =========================================================================
def parse_unstructured(raw_bytes, filename):
    enc = _detect_encoding(raw_bytes)
    text = raw_bytes.decode(enc, errors="replace")
    if not AI_CONFIGURED:
        # fallback: best-effort delimited parse
        try:
            df = _parse_text(raw_bytes, enc)
            return {"columns": list(df.columns),
                    "rows": df.astype(str).values.tolist()[:500], "error": None}
        except Exception as e:
            return {"columns": [], "rows": [], "error": f"No LLM configured and parse failed: {e}"}
    system = ("Extract the tabular data from the text into JSON with keys "
              "'columns' (list of strings) and 'rows' (list of lists). "
              "Return ONLY valid JSON, no prose.")
    try:
        ans = _ask_llm([{"role": "user", "content": text[:12000]}], system=system)
        ans = ans[ans.find("{"):ans.rfind("}") + 1]
        data = json.loads(ans)
        return {"columns": data.get("columns", []), "rows": data.get("rows", []), "error": None}
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}


# =========================================================================
# Routes — pages
# =========================================================================
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/parse", response_class=HTMLResponse)
def parse_page(request: Request):
    try:
        return templates.TemplateResponse(request, "parse.html")
    except Exception:
        return HTMLResponse("<h1>Parser UI coming in Phase 5</h1>")


# =========================================================================
# Routes — login / auth flow
# =========================================================================
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = ""):
    if not AUTH_ENABLED:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"error": error})


@app.post("/login")
async def login_submit(request: Request,
                       username: str = Form(...), password: str = Form(...)):
    if not AUTH_ENABLED:
        return RedirectResponse("/", status_code=302)
    record = _USERS.get(username)
    if not record or not _verify_password(password, record["hash"]):
        return RedirectResponse("/login?error=Invalid+credentials", status_code=302)
    token = _make_session_token(username, record["role"])
    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie(_SESSION_COOKIE, token, httponly=True, samesite="lax",
                    max_age=_SESSION_HOURS * 3600)
    log_action("login", f"user {username} signed in", username)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie(_SESSION_COOKIE)
    return resp


# =========================================================================
# Routes — license
# =========================================================================
@app.get("/api/license/status")
def license_status():
    return _sanitize_json(lic.get_state())


@app.post("/api/license/activate")
async def license_activate(request: Request):
    _require_role(request, "admin")
    body = await request.json()
    return _sanitize_json(lic.activate_license(body.get("token", "")))


@app.post("/api/license/heartbeat")
def license_heartbeat():
    return _sanitize_json(lic.heartbeat())


# =========================================================================
# Routes — analysis
# =========================================================================
def _df_from_connection(conn_id, username):
    """Fetch a DataFrame from a saved workspace connection."""
    if not WS_ENABLED:
        raise HTTPException(503, "Workspace not enabled — cannot load from connection.")
    rec = ws_db.get_connection(int(conn_id), username)
    if not rec:
        raise HTTPException(404, f"Connection {conn_id} not found.")
    connector = BaseConnector.from_type(rec["source_type"], rec["config"])
    df = connector.fetch().astype(str)
    df.attrs["_format"] = f"connection:{rec['source_type']}"
    return df


@app.post("/analyze")
async def analyze(
    request: Request,
    action: str = Form("compare"),
    file1: UploadFile = File(None),
    file2: UploadFile = File(None),
    ref_doc: list[UploadFile] = File(None),
    key_cols: str = Form(""),
    exclude_cols: str = Form(""),
    delimiter: str = Form(""),
    hints: str = Form(""),
    conn_a_id: str = Form(""),
    conn_b_id: str = Form(""),
    conn_source_id: str = Form(""),
):
    _require_role(request, "admin", "analyst")
    keys = [k.strip() for k in key_cols.split(",") if k.strip()]
    excludes = [c.strip() for c in exclude_cols.split(",") if c.strip()]
    delim = delimiter or None
    try:
        hint_obj = json.loads(hints) if hints else {}
    except Exception:
        hint_obj = {}

    try:
        user = get_current_user(request)
    except Exception:
        user = "localdev"

    # reference documents (data dictionary / rules / mapping spec)
    ref = None
    if ref_doc:
        uploads = [(f.filename, await f.read()) for f in ref_doc if f and f.filename]
        if uploads:
            ref = _load_and_classify_ref_docs(uploads)

    feat = "parse" if action == "parse" else ("compare" if action == "complex_recon" else action)
    _require_feature(feat)

    async def _load_primary(upload, conn_id, label):
        """Load from a saved connection if given, else from the upload."""
        if conn_id:
            return _df_from_connection(conn_id, user)
        if upload:
            return load_file(upload.filename, await upload.read(), delim)
        raise HTTPException(400, f"{action} requires {label} (upload or connection).")

    if action == "compare":
        df1 = await _load_primary(file1, conn_a_id, "file1")
        df2 = await _load_primary(file2, conn_b_id, "file2")
        result = compare_dataframes(
            df1, df2, manual_keys=keys or None, exclude_cols=excludes,
            key_hints=hint_obj.get("key_hints"),
            matched_cols=hint_obj.get("matched_cols"),
            force_data_cols=hint_obj.get("force_data_cols"))
        fingerprint = fb.compute_fingerprint(list(df1.columns), list(df2.columns))
        result["fingerprint"] = fingerprint
        result["saved_rules"] = fb.get_rules(fingerprint)
        result["formats"] = {"file1": df1.attrs.get("_format"),
                             "file2": df2.attrs.get("_format")}
    elif action == "quality":
        df1 = await _load_primary(file1, conn_source_id or conn_a_id, "file1")
        result = analyze_quality(
            df1, name=getattr(file1, "filename", "connection"),
            data_dict=(ref or {}).get("data_dict"),
            rules=hint_obj.get("rules") or (ref or {}).get("rules"),
            user_hints=hint_obj,
        )
        if WS_ENABLED:
            try:
                # Use the workspace identity (OS/IIS-resolved), NOT the login
                # session user — /api/ws/dq/history queries resolve identity
                # that way, and the two auth systems track separate usernames.
                dq_user = _ws_resolve_username(request) or user
                dims = result.get("dimensions") or {}
                ws_db.insert_dq_history(
                    file_name=result.get("name") or "dataset",
                    username=dq_user,
                    score=result.get("score"),
                    grade=result.get("grade"),
                    completeness=dims.get("completeness"),
                    uniqueness=dims.get("uniqueness"),
                    validity=dims.get("validity"),
                )
            except Exception:
                pass
    elif action == "mapping":
        df1 = await _load_primary(file1, conn_a_id, "file1")
        df2 = await _load_primary(file2, conn_b_id, "file2")
        result = analyze_mapping(df1, df2, getattr(file1, "filename", "file1"),
                                 getattr(file2, "filename", "file2"), ref=ref)
    elif action == "governance":
        df1 = await _load_primary(file1, conn_source_id or conn_a_id, "file1")
        result = analyze_governance(df1, name=getattr(file1, "filename", "connection"))
    elif action == "lineage":
        df1 = await _load_primary(file1, conn_a_id, "file1")
        df2 = await _load_primary(file2, conn_b_id, "file2")
        result = analyze_lineage(
            df1, df2,
            transforms=hint_obj.get("transforms"),
            col_map=hint_obj.get("col_map"),
            parse_cols=hint_obj.get("parse_cols"),
            agg=hint_obj.get("agg"),
            query=hint_obj.get("query"),
            manual_keys=keys or None,
        )
    elif action == "complex_recon":
        df1 = await _load_primary(file1, conn_a_id, "file1")
        df2 = await _load_primary(file2, conn_b_id, "file2")
        common = [c for c in df1.columns if c in set(df2.columns)]
        text_col = hint_obj.get("text_col") or (keys[0] if keys else None)
        if not text_col:
            raise HTTPException(400, "complex_recon requires hints.text_col (or key_cols) naming the free-text description column.")
        result = complex_reconciliation(df1, df2, text_col, common)
    elif action == "parse":
        if not file1:
            raise HTTPException(400, "parse requires file1.")
        result = parse_unstructured(await file1.read(), file1.filename)
    else:
        raise HTTPException(400, f"Unknown action '{action}'.")

    session_id = uuid.uuid4().hex
    result["session_id"] = session_id
    result["action"] = action
    _results_store[session_id] = result
    _ref_docs_store[session_id] = ref or {}
    try:
        _session_owners[session_id] = get_current_user(request)
    except Exception:
        _session_owners[session_id] = "localdev"
    return JSONResponse(_sanitize_json(result))


@app.post("/api/analyze-parse")
async def analyze_parse(file1: UploadFile = File(...)):
    _require_feature("parse")
    return JSONResponse(_sanitize_json(
        parse_unstructured(await file1.read(), file1.filename)))


@app.post("/api/analyze-quality-single")
async def analyze_quality_single(file1: UploadFile = File(...),
                                 delimiter: str = Form("")):
    _require_feature("quality")
    df = load_file(file1.filename, await file1.read(), delimiter or None)
    return JSONResponse(_sanitize_json(analyze_quality(df, name=file1.filename)))


# =========================================================================
# Routes — feedback rules
# =========================================================================
@app.get("/api/rules")
def list_rules():
    return _sanitize_json(fb.list_all_datasets())


@app.get("/api/rules/{fingerprint}")
def get_rules(fingerprint: str):
    return _sanitize_json(fb.get_rules(fingerprint))


@app.post("/api/rules/{fingerprint}")
async def add_rule(fingerprint: str, request: Request):
    _require_role(request, "admin", "analyst")
    body = await request.json()
    idx, queued = fb.save_rule(
        fingerprint, body.get("rule", ""),
        category=body.get("category", "general"),
        dataset_label=body.get("label"),
    )
    return {"index": idx, "queued": queued}


@app.delete("/api/rules/{fingerprint}/{index}")
def remove_rule(fingerprint: str, index: int, request: Request):
    _require_role(request, "admin", "analyst")
    ok, queued = fb.delete_rule(fingerprint, index)
    return {"deleted": ok, "queued": queued}


@app.put("/api/rules/{fingerprint}/{index}")
async def edit_rule(fingerprint: str, index: int, request: Request):
    _require_role(request, "admin", "analyst")
    body = await request.json()
    ok, queued = fb.update_rule(
        fingerprint, index,
        rule_text=body.get("rule"),
        category=body.get("category"),
        direction=body.get("direction"),
    )
    return {"updated": ok, "queued": queued}


# =========================================================================
# Routes — quality AI rule suggestions
# =========================================================================
_SUGGESTABLE_RULES = [
    "not_null", "unique", "min", "max", "range", "min_length", "max_length",
    "pattern", "allowed_values", "freshness_days", "date_format", "date_range",
    "not_future_date", "positive", "non_negative", "isin_format", "cusip_format",
    "lei_format", "bic_format", "iban_format", "email_format", "phone_format",
    "currency_code_format", "mic_format",
]


@app.post("/api/suggest-rules")
async def suggest_rules(request: Request):
    """Given a column profile, suggest DQ rules. Uses LLM if available,
    otherwise applies heuristics over the profile."""
    profile = await request.json()
    col = profile.get("column", "")
    suggestions = []

    # Heuristic suggestions (always available)
    if profile.get("null_count", 0) == 0:
        suggestions.append({"type": "not_null", "reason": "no nulls observed"})
    if profile.get("uniqueness_pct", 0) >= 99.5:
        suggestions.append({"type": "unique", "reason": "values are unique"})
    if profile.get("detected_format"):
        suggestions.append({"type": profile["detected_format"] + "_format",
                            "reason": f"matches {profile['detected_format']} pattern"})
    num = profile.get("numeric")
    if num:
        if num.get("min", -1) >= 0:
            suggestions.append({"type": "non_negative", "reason": "all values >= 0"})
        suggestions.append({"type": "range", "min": num.get("min"),
                            "max": num.get("max"), "reason": "observed numeric range"})
    if 0 < profile.get("distinct", 999) <= 20 and profile.get("allowed_values"):
        suggestions.append({"type": "allowed_values",
                            "value": profile["allowed_values"],
                            "reason": "low-cardinality categorical"})

    # Optional LLM enrichment
    if AI_CONFIGURED and profile:
        sys = ("Suggest data-quality rules for this column. Reply with a JSON list "
               f"of objects {{type, reason}}. Valid types: {_SUGGESTABLE_RULES}. "
               "Return ONLY JSON.")
        ans = _ask_llm_safe([{"role": "user", "content": json.dumps(profile)[:4000]}], system=sys)
        try:
            extra = json.loads(ans[ans.find("["):ans.rfind("]") + 1])
            seen = {s["type"] for s in suggestions}
            for e in extra:
                if e.get("type") in _SUGGESTABLE_RULES and e["type"] not in seen:
                    suggestions.append(e)
        except Exception:
            pass

    return {"column": col, "suggestions": suggestions}


# =========================================================================
# Routes — AI chat copilot
# =========================================================================
@app.post("/chat")
async def chat(request: Request):
    body = await request.json()
    session_id = body.get("session_id", "")
    message = body.get("message", "")
    ctx = _results_store.get(session_id, {})
    history = _chat_contexts.setdefault(session_id, [])

    fingerprint = ctx.get("fingerprint", "")
    rules_text = fb.get_rules_as_text(fingerprint) if fingerprint else ""
    summary = json.dumps(_sanitize_json(
        ctx.get("counts") or ctx.get("dimensions")
        or {"matched": ctx.get("matched"), "overall_risk": ctx.get("overall_risk"),
            "action": ctx.get("action")}))
    system = (
        "You are a data validation copilot. Help the user understand the analysis "
        "results below. Be concise and specific.\n"
        f"Analysis summary: {summary}{rules_text}"
    )
    history.append({"role": "user", "content": message})
    answer = _ask_llm_safe(history[-8:], system=system)
    history.append({"role": "assistant", "content": answer})
    return {"response": answer, "session_id": session_id}


@app.post("/agent-chat")
async def agent_chat(request: Request):
    """Tool-calling AI Copilot: the LLM decides which of the 6 tools to call
    (inspect compare/quality/mapping/governance/lineage results, or search
    reference docs) rather than being handed a single canned summary."""
    body = await request.json()
    session_id = body.get("session_id", "")
    message = body.get("message", "")
    if not message:
        raise HTTPException(400, "message is required.")

    session_results = _results_store.get(session_id, {})
    ref_docs = _ref_docs_store.get(session_id, {})
    fingerprint = session_results.get("fingerprint", "")
    extra_rules = fb.get_rules_as_text(fingerprint) if fingerprint else ""

    dispatch = agent_tools.make_tool_dispatch(session_results, ref_docs)
    history = _agent_memory.get(session_id)
    answer = agent_executor.run_agent(message, history[-8:], dispatch, extra_rules=extra_rules)
    _agent_memory.append(session_id, "user", message)
    _agent_memory.append(session_id, "assistant", answer)
    return {"response": answer, "session_id": session_id}


# =========================================================================
# Routes — download (Excel)
# =========================================================================
def _build_excel(result) -> io.BytesIO:
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    buf = io.BytesIO()
    action = result.get("action", "compare")
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        # ---- Summary (built as a labelled key/value sheet) ----
        rows = [("Report type", action),
                ("Generated", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))]
        if result.get("keys"):
            rows.append(("Match keys", ", ".join(result["keys"])))
        for k, v in (result.get("counts") or {}).items():
            rows.append((k.replace("_", " ").title(), v))
        for k, v in (result.get("dimensions") or {}).items():
            rows.append((f"DQ: {k.title()}", v))
        if "score" in result:
            rows.append(("Score", result["score"]))
            rows.append(("Grade", result.get("grade", "")))
        if result.get("overall_risk"):
            rows.append(("Overall risk", result["overall_risk"]))
        pd.DataFrame(rows, columns=["Metric", "Value"]).to_excel(
            xl, sheet_name="Summary", index=False)

        if result.get("file1_only"):
            pd.DataFrame(result["file1_only"]).to_excel(xl, sheet_name="File1 Only", index=False)
        if result.get("file2_only"):
            pd.DataFrame(result["file2_only"]).to_excel(xl, sheet_name="File2 Only", index=False)
        if result.get("modified"):
            mod = [{"key": m["key"], **{k: f"{v['file1']} -> {v['file2']}"
                                        for k, v in m["changes"].items()}}
                   for m in result["modified"]]
            pd.DataFrame(mod).to_excel(xl, sheet_name="Modified", index=False)
        if result.get("columns_detail"):
            flat = []
            for c in result["columns_detail"]:
                flat.append({k: v for k, v in c.items()
                             if not isinstance(v, (dict, list))})
            pd.DataFrame(flat).to_excel(xl, sheet_name="Column Stats", index=False)
        if result.get("type_mismatches"):
            pd.DataFrame(result["type_mismatches"]).to_excel(xl, sheet_name="Type Mismatches", index=False)
        if result.get("null_column_exceptions"):
            pd.DataFrame(result["null_column_exceptions"]).to_excel(xl, sheet_name="Null Col Exceptions", index=False)
        if result.get("field_specs"):
            fs = [{k: (", ".join(map(str, v)) if isinstance(v, list) else v)
                   for k, v in s.items() if k != "exceptions"}
                  for s in result["field_specs"]]
            pd.DataFrame(fs).to_excel(xl, sheet_name="Field Specs", index=False)
        if result.get("columns") and action == "governance":
            pd.DataFrame([{**c, "pii_types": ", ".join(c.get("pii_types", []))}
                          for c in result["columns"]]).to_excel(
                xl, sheet_name="Governance", index=False)

        # ---- styling: gradient-like header, banding, conditional colors ----
        header_fill = PatternFill("solid", fgColor="0EA5E9")
        accent_fill = PatternFill("solid", fgColor="6366F1")
        band = PatternFill("solid", fgColor="EEF2FF")
        header_font = Font(bold=True, color="FFFFFF", size=11)
        thin = Side(style="thin", color="D7DEEA")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)
        grade_colors = {"A": "10B981", "B": "84CC16", "C": "F59E0B",
                        "D": "F97316", "F": "EF4444"}

        for name, ws in zip(xl.sheets, xl.book.worksheets):
            fill = accent_fill if ws.title == "Summary" else header_fill
            for cell in ws[1]:
                cell.fill = fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = border
            for i, r in enumerate(ws.iter_rows(min_row=2), start=2):
                for cell in r:
                    cell.border = border
                    if i % 2 == 0:
                        cell.fill = band
                    val = str(cell.value)
                    if val in grade_colors and cell.column == 2:
                        cell.fill = PatternFill("solid", fgColor=grade_colors[val])
                        cell.font = Font(bold=True, color="FFFFFF")
                    if val in ("restricted", "critical", "high"):
                        cell.font = Font(bold=True, color="B91C1C")
            for col in ws.columns:
                width = max((len(str(c.value)) for c in col if c.value is not None), default=10)
                ws.column_dimensions[get_column_letter(col[0].column)].width = min(width + 3, 55)
            ws.freeze_panes = "A2"
    buf.seek(0)
    return buf


@app.get("/download/{session_id}")
def download(session_id: str):
    result = _results_store.get(session_id)
    if not result:
        raise HTTPException(404, "Session not found.")
    buf = _build_excel(result)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="result_{session_id[:8]}.xlsx"'},
    )


@app.post("/send-email")
async def send_email(request: Request):
    body = await request.json()
    session_id = body.get("session_id", "")
    to_email = body.get("to_email", "")
    if not to_email:
        raise HTTPException(400, "to_email is required.")
    result = _results_store.get(session_id)
    if not result:
        raise HTTPException(404, "Session not found.")
    subject = body.get("subject") or f"Data Validation result — {result.get('action','')}"
    from_email = body.get("from_email") or os.getenv("EMAIL_FROM", "")
    xlsx = _build_excel(result).getvalue()
    fname = f"result_{session_id[:8]}.xlsx"
    try:
        if os.name == "nt" and not os.getenv("SMTP_HOST"):
            import tempfile
            import win32com.client
            tmp = os.path.join(tempfile.gettempdir(), fname)
            with open(tmp, "wb") as fh:
                fh.write(xlsx)
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = to_email
            mail.Subject = subject
            mail.Body = "Please find the attached data validation result."
            if from_email:
                mail.SentOnBehalfOfName = from_email
            mail.Attachments.Add(tmp)
            mail.Send()
        else:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.application import MIMEApplication
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"] = from_email or "no-reply@datavalidation.local"
            msg["To"] = to_email
            msg.attach(MIMEText("Please find the attached data validation result."))
            part = MIMEApplication(xlsx, Name=fname)
            part["Content-Disposition"] = f'attachment; filename="{fname}"'
            msg.attach(part)
            with smtplib.SMTP(os.getenv("SMTP_HOST", "localhost"),
                              int(os.getenv("SMTP_PORT", "25")), timeout=20) as s:
                s.send_message(msg)
        log_action("email", f"sent {fname} to {to_email}",
                   _session_owners.get(session_id, ""), session_id)
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


# =========================================================================
# Routes — audit log (SSE)
# =========================================================================
_audit_subscribers: set = set()


def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _audit_subscribers.add(q)
    return q


def unsubscribe(q: asyncio.Queue):
    _audit_subscribers.discard(q)


def log_action(action, detail, username="", session_id=None):
    """Persist an audit event and broadcast it to all SSE subscribers."""
    event = {"action": action, "detail": detail, "username": username,
             "session_id": session_id, "ts": datetime.now(timezone.utc).isoformat()}
    if WS_ENABLED:
        try:
            ws_db.insert_audit(username, action, detail, session_id)
        except Exception:
            pass
    for q in list(_audit_subscribers):
        try:
            q.put_nowait(event)
        except Exception:
            pass


@app.post("/api/audit/log")
async def audit_log(request: Request):
    body = await request.json()
    user = ""
    try:
        user = get_current_user(request)
    except Exception:
        pass
    log_action(body.get("action", ""), body.get("detail", ""), user,
               body.get("session_id"))
    return {"ok": True}


@app.get("/api/audit/stream")
async def audit_stream(request: Request):
    async def event_gen():
        q = subscribe()
        try:
            # initial comment to open the stream
            yield ": connected\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            unsubscribe(q)

    return StreamingResponse(event_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# =========================================================================
# Routes — workspace
# =========================================================================
def _ws_guard():
    if not WS_ENABLED:
        raise HTTPException(503, "Workspace module is not enabled on this server.")


@app.get("/api/ws/me")
def ws_me(request: Request):
    user = get_current_user(request)
    return {"username": user,
            "display_name": getattr(request.state, "display_name", user),
            "role": getattr(request.state, "role", None) or "analyst"}


# ---- Connections --------------------------------------------------------
@app.get("/api/ws/connections")
def ws_list_connections(request: Request):
    _ws_guard()
    return _sanitize_json(ws_db.list_connections(get_current_user(request)))


@app.post("/api/ws/connections")
async def ws_create_connection(request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    body = await request.json()
    cid = ws_db.save_connection(get_current_user(request), body["name"],
                                body["source_type"], body.get("config", {}),
                                conn_id=body.get("id"))
    return {"id": cid}


@app.get("/api/ws/connections/{conn_id}")
def ws_get_connection(conn_id: int, request: Request):
    _ws_guard()
    rec = ws_db.get_connection(conn_id, get_current_user(request))
    if not rec:
        raise HTTPException(404, "Connection not found.")
    # never echo secrets back verbatim
    cfg = dict(rec.get("config") or {})
    for secret in ("password", "aws_secret_access_key"):
        if cfg.get(secret):
            cfg[secret] = "********"
    rec["config"] = cfg
    return _sanitize_json(rec)


@app.put("/api/ws/connections/{conn_id}")
async def ws_update_connection(conn_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    body = await request.json()
    ws_db.save_connection(get_current_user(request), body["name"],
                          body["source_type"], body.get("config", {}),
                          conn_id=conn_id)
    return {"id": conn_id}


@app.delete("/api/ws/connections/{conn_id}")
def ws_delete_connection(conn_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    ws_db.delete_connection(conn_id, get_current_user(request))
    return {"deleted": True}


@app.post("/api/ws/connections/{conn_id}/test")
def ws_test_connection(conn_id: int, request: Request):
    _ws_guard()
    rec = ws_db.get_connection(conn_id, get_current_user(request))
    if not rec:
        raise HTTPException(404, "Connection not found.")
    try:
        connector = BaseConnector.from_type(rec["source_type"], rec["config"])
        return {"ok": bool(connector.test_connection())}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/ws/connections/{conn_id}/preview")
def ws_preview_connection(conn_id: int, request: Request):
    _ws_guard()
    rec = ws_db.get_connection(conn_id, get_current_user(request))
    if not rec:
        raise HTTPException(404, "Connection not found.")
    try:
        connector = BaseConnector.from_type(rec["source_type"], rec["config"])
        df = connector.fetch().head(100)
        return _sanitize_json({"columns": list(df.columns),
                               "rows": df.astype(str).values.tolist()})
    except Exception as e:
        raise HTTPException(400, f"Preview failed: {e}")


# ---- Rulesets -----------------------------------------------------------
@app.get("/api/ws/rulesets")
def ws_list_rulesets(request: Request):
    _ws_guard()
    return _sanitize_json(ws_db.list_rulesets(get_current_user(request)))


@app.post("/api/ws/rulesets")
async def ws_create_ruleset(request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    body = await request.json()
    rid = ws_db.save_ruleset(get_current_user(request), body["name"],
                             body.get("description", ""), body.get("rules", []),
                             rs_id=body.get("id"))
    return {"id": rid}


@app.get("/api/ws/rulesets/{rs_id}")
def ws_get_ruleset(rs_id: int, request: Request):
    _ws_guard()
    rec = ws_db.get_ruleset(rs_id, get_current_user(request))
    if not rec:
        raise HTTPException(404, "Ruleset not found.")
    return _sanitize_json(rec)


@app.put("/api/ws/rulesets/{rs_id}")
async def ws_update_ruleset(rs_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    body = await request.json()
    ws_db.save_ruleset(get_current_user(request), body["name"],
                       body.get("description", ""), body.get("rules", []),
                       rs_id=rs_id)
    return {"id": rs_id}


@app.delete("/api/ws/rulesets/{rs_id}")
def ws_delete_ruleset(rs_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    ws_db.delete_ruleset(rs_id, get_current_user(request))
    return {"deleted": True}


# ---- Jobs ---------------------------------------------------------------
@app.get("/api/ws/jobs")
def ws_list_jobs(request: Request):
    _ws_guard()
    return _sanitize_json(ws_db.list_jobs(get_current_user(request)))


@app.post("/api/ws/jobs")
async def ws_create_job(request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    user = get_current_user(request)
    body = await request.json()
    jid = ws_db.save_job(
        user, body["name"], body["action"],
        source_conn_id=body.get("source_conn_id"),
        conn_a_id=body.get("conn_a_id"), conn_b_id=body.get("conn_b_id"),
        key_columns=body.get("key_columns"), exclude_columns=body.get("exclude_columns"),
        ruleset_id=body.get("ruleset_id"), schedule_cron=body.get("schedule_cron"),
        from_email=body.get("from_email"), notify_email=body.get("notify_email"),
        job_id=body.get("id"),
    )
    if body.get("schedule_cron"):
        try:
            _scheduler_mod.schedule_job(jid, user, body["schedule_cron"])
        except Exception as e:
            return {"id": jid, "schedule_warning": str(e)}
    return {"id": jid}


@app.get("/api/ws/jobs/{job_id}")
def ws_get_job(job_id: int, request: Request):
    _ws_guard()
    rec = ws_db.get_job(job_id, get_current_user(request))
    if not rec:
        raise HTTPException(404, "Job not found.")
    return _sanitize_json(rec)


@app.put("/api/ws/jobs/{job_id}")
async def ws_update_job(job_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    user = get_current_user(request)
    body = await request.json()
    ws_db.save_job(
        user, body["name"], body["action"],
        source_conn_id=body.get("source_conn_id"),
        conn_a_id=body.get("conn_a_id"), conn_b_id=body.get("conn_b_id"),
        key_columns=body.get("key_columns"), exclude_columns=body.get("exclude_columns"),
        ruleset_id=body.get("ruleset_id"), schedule_cron=body.get("schedule_cron"),
        from_email=body.get("from_email"), notify_email=body.get("notify_email"),
        job_id=job_id,
    )
    if body.get("schedule_cron"):
        _scheduler_mod.schedule_job(job_id, user, body["schedule_cron"])
    else:
        _scheduler_mod.unregister_job(job_id)
    return {"id": job_id}


@app.delete("/api/ws/jobs/{job_id}")
def ws_delete_job(job_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    _scheduler_mod.unregister_job(job_id)
    ws_db.delete_job(job_id, get_current_user(request))
    return {"deleted": True}


@app.post("/api/ws/jobs/{job_id}/run")
def ws_run_job(job_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    run_id = _scheduler_mod.trigger_job_now(job_id, get_current_user(request))
    return {"run_id": run_id}


@app.get("/api/ws/jobs/{job_id}/runs")
def ws_job_runs(job_id: int, request: Request):
    _ws_guard()
    return _sanitize_json(ws_db.list_runs(get_current_user(request), job_id=job_id))


# ---- Saved runs ---------------------------------------------------------
@app.get("/api/ws/saved-runs")
def ws_list_saved_runs(request: Request):
    _ws_guard()
    return _sanitize_json(ws_db.list_saved_runs(get_current_user(request)))


@app.post("/api/ws/saved-runs")
async def ws_save_run(request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    body = await request.json()
    session_id = body.get("session_id", "")
    ctx = _results_store.get(session_id, {})
    summary = ctx.get("counts") or ctx.get("dimensions") or {}
    rid = ws_db.save_manual_run(
        get_current_user(request), body.get("name", "saved run"),
        ctx.get("action", "compare"), body.get("sources", {}),
        session_id, summary, key_columns=",".join(ctx.get("keys", [])),
    )
    return {"id": rid}


@app.delete("/api/ws/saved-runs/{run_id}")
def ws_delete_saved_run(run_id: int, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin", "analyst")
    ws_db.delete_saved_run(run_id, get_current_user(request))
    return {"deleted": True}


# ---- User role management (admin only) ------------------------------------
@app.get("/api/ws/users")
def ws_list_users(request: Request):
    _ws_guard()
    _ws_require_role(request, "admin")
    return _sanitize_json(ws_db.list_users())


@app.put("/api/ws/users/{username}/role")
async def ws_set_user_role(username: str, request: Request):
    _ws_guard()
    _ws_require_role(request, "admin")
    body = await request.json()
    role = body.get("role")
    if role not in ("admin", "analyst", "readonly"):
        raise HTTPException(400, "role must be one of: admin, analyst, readonly")
    ws_db.set_user_role(username, role)
    return {"username": username, "role": role}


# ---- DQ score history -----------------------------------------------------
# NOTE: these live under /api/ws so WorkspaceAuthMiddleware resolves identity
# for them (it only runs for paths under that prefix).
@app.get("/api/ws/dq/history/{file_name}")
def dq_history(file_name: str, request: Request, days: int = 30):
    _ws_guard()
    return _sanitize_json(ws_db.get_dq_history(file_name, get_current_user(request), days=days))


@app.get("/api/ws/dq/baseline/{file_name}")
def dq_baseline(file_name: str, request: Request):
    _ws_guard()
    baseline = ws_db.get_dq_baseline(file_name, get_current_user(request))
    if not baseline:
        raise HTTPException(404, "No DQ history recorded for this file yet.")
    return _sanitize_json(baseline)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
