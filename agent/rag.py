"""
Reference-document retrieval -- hybrid semantic + keyword search over the
reference material uploaded with a session (data dictionaries, business rules,
mapping specs, and free-text PDF/prose).

Semantic ranking uses a LOCAL ONNX embedding model (fastembed) so reference
text never leaves the server -- consistent with the app's data-governance model
(see DATA_GOVERNANCE.md). It degrades gracefully: if fastembed isn't installed
or the model can't load (e.g. constrained memory), it falls back to the original
keyword / term-frequency search, so the feature never breaks a deploy.

Env vars:
  EMBED_MODEL       -- fastembed model id (default BAAI/bge-small-en-v1.5, ~small ONNX)
  FASTEMBED_CACHE   -- dir to cache the downloaded model (point at the /data volume
                       on Railway so it isn't re-downloaded on every deploy)
"""
import os
import re
import hashlib

_WORD_RX = re.compile(r"[a-z0-9]+")
_SNIPPET_RADIUS = 200  # chars of context on each side of a keyword hit
_DOC_CHUNK = 400       # chars per document passage for semantic matching
_DOC_CHUNK_OVERLAP = 80
_MIN_SEM_SCORE = 0.30  # drop semantic matches below this cosine similarity


def _terms(text: str) -> list[str]:
    return [t for t in _WORD_RX.findall(text.lower()) if len(t) > 2]


def _score_snippets(text: str, query_terms: list[str], max_snippets: int = 3) -> list[str]:
    """Find the max_snippets windows in `text` with the most query-term hits."""
    lower = text.lower()
    positions = []
    for term in query_terms:
        start = 0
        while True:
            idx = lower.find(term, start)
            if idx == -1:
                break
            positions.append(idx)
            start = idx + len(term)
    if not positions:
        return []
    positions.sort()
    windows = []
    for pos in positions:
        lo, hi = max(0, pos - _SNIPPET_RADIUS), min(len(text), pos + _SNIPPET_RADIUS)
        hits = sum(1 for p in positions if lo <= p <= hi)
        windows.append((hits, lo, hi))
    windows.sort(key=lambda w: -w[0])
    seen_ranges, out = [], []
    for hits, lo, hi in windows:
        if any(lo < e and s < hi for s, e in seen_ranges):
            continue
        seen_ranges.append((lo, hi))
        snippet = text[lo:hi].strip().replace("\n", " ")
        out.append(("…" if lo > 0 else "") + snippet + ("…" if hi < len(text) else ""))
        if len(out) >= max_snippets:
            break
    return out


# ---------------------------------------------------------------------------
# Keyword search (fallback + high-precision half of the hybrid)
# ---------------------------------------------------------------------------
def _keyword_search(ref_docs: dict, query: str, top_k: int) -> list[dict]:
    q = query.lower().strip()
    matches: list[dict] = []
    for col, defn in (ref_docs.get("data_dict") or {}).items():
        if q in col.lower() or q in str(defn).lower():
            matches.append({"type": "data_dict", "column": col, "definition": defn})
    for rule in (ref_docs.get("rules") or []):
        if q in str(rule).lower():
            matches.append({"type": "rule", "rule": rule})
    for m in (ref_docs.get("mapping") or []):
        if q in str(m).lower():
            matches.append({"type": "mapping", "spec": m})
    query_terms = _terms(q)
    for doc_name, text in (ref_docs.get("documents") or {}).items():
        for snippet in _score_snippets(text, query_terms):
            matches.append({"type": "document", "source": doc_name, "snippet": snippet})
    return matches[:top_k]


# ---------------------------------------------------------------------------
# Semantic layer -- local ONNX embeddings, in-memory cosine index
# ---------------------------------------------------------------------------
_EMBED_MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-small-en-v1.5")
_embedder = None
_embedder_failed = False
_emb_cache: dict = {}        # passages-hash -> (passages, normalized embeddings ndarray)
_EMB_CACHE_MAX = 8


def _get_embedder():
    """Lazily load the local embedding model once. Any failure (not installed,
    OOM, no network for first download) disables semantic search permanently
    for this process and the caller falls back to keyword search."""
    global _embedder, _embedder_failed
    if _embedder is not None or _embedder_failed:
        return _embedder
    try:
        from fastembed import TextEmbedding
        cache_dir = os.getenv("FASTEMBED_CACHE") or os.getenv("EMBED_CACHE_DIR")
        kwargs = {"cache_dir": cache_dir} if cache_dir else {}
        _embedder = TextEmbedding(model_name=_EMBED_MODEL_NAME, **kwargs)
    except Exception:
        _embedder_failed = True
        _embedder = None
    return _embedder


def _embed(texts: list[str]):
    """Return L2-normalized embeddings (ndarray) for `texts`, or None on failure."""
    emb = _get_embedder()
    if emb is None:
        return None
    try:
        import numpy as np
        vecs = np.array(list(emb.embed(list(texts))), dtype="float32")
        if vecs.ndim != 2 or vecs.shape[0] == 0:
            return None
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return vecs / norms
    except Exception:
        return None


def _chunk_text(text: str) -> list[str]:
    text = text or ""
    if not text.strip():
        return []
    if len(text) <= _DOC_CHUNK:
        return [text]
    step = _DOC_CHUNK - _DOC_CHUNK_OVERLAP
    return [text[i:i + _DOC_CHUNK] for i in range(0, len(text), step) if text[i:i + _DOC_CHUNK].strip()]


def _build_passages(ref_docs: dict) -> list[tuple]:
    """Every searchable unit as (embed_text, match_dict)."""
    passages: list[tuple] = []
    for col, defn in (ref_docs.get("data_dict") or {}).items():
        passages.append((f"{col}: {defn}", {"type": "data_dict", "column": col, "definition": defn}))
    for rule in (ref_docs.get("rules") or []):
        passages.append((str(rule), {"type": "rule", "rule": rule}))
    for m in (ref_docs.get("mapping") or []):
        passages.append((str(m), {"type": "mapping", "spec": m}))
    for doc_name, text in (ref_docs.get("documents") or {}).items():
        for chunk in _chunk_text(text):
            snippet = chunk.strip().replace("\n", " ")
            passages.append((chunk, {"type": "document", "source": doc_name, "snippet": snippet}))
    return passages


def _passages_hash(passages: list[tuple]) -> str:
    h = hashlib.sha1()
    for text, _ in passages:
        h.update(text.encode("utf-8", "ignore"))
        h.update(b"\x00")
    return h.hexdigest()


def _semantic_search(ref_docs: dict, query: str, top_k: int):
    """Return ranked match dicts (with cosine `score`), or None to signal the
    caller to fall back to keyword search."""
    passages = _build_passages(ref_docs)
    if not passages:
        return None
    key = _passages_hash(passages)
    cached = _emb_cache.get(key)
    if cached is None:
        embs = _embed([t for t, _ in passages])
        if embs is None:
            return None
        cached = (passages, embs)
        _emb_cache[key] = cached
        if len(_emb_cache) > _EMB_CACHE_MAX:
            _emb_cache.pop(next(iter(_emb_cache)))
    passages, embs = cached
    qv = _embed([query])
    if qv is None:
        return None
    try:
        import numpy as np
        sims = embs @ qv[0]
        order = np.argsort(-sims)[:top_k]
    except Exception:
        return None
    out = []
    for idx in order:
        score = float(sims[int(idx)])
        if score < _MIN_SEM_SCORE:
            continue
        md = dict(passages[int(idx)][1])
        md["score"] = round(score, 3)
        out.append(md)
    return out


def _sig(m: dict) -> tuple:
    """Dedup signature so a hit found by both semantic and keyword appears once."""
    t = m.get("type")
    if t == "data_dict":
        return ("data_dict", m.get("column"))
    if t == "rule":
        return ("rule", str(m.get("rule"))[:120])
    if t == "mapping":
        return ("mapping", str(m.get("spec"))[:120])
    return ("document", m.get("source"), (m.get("snippet") or "")[:80])


def invalidate_store():
    """Drop cached embeddings (e.g. when a session's reference docs change)."""
    _emb_cache.clear()


def search(ref_docs: dict, query: str, top_k: int = 15) -> dict:
    if not ref_docs:
        return {"matches": [], "note": "No reference documents were uploaded with this session."}
    q = (query or "").strip()
    if not q:
        return {"matches": [], "note": "Empty query."}

    kw = _keyword_search(ref_docs, q, top_k)
    sem = _semantic_search(ref_docs, q, top_k)

    if sem is None:
        # Semantic unavailable (fastembed missing / model couldn't load) -- keyword only.
        return {"matches": kw[:top_k], "total_matches": len(kw), "method": "keyword"}

    # Hybrid: semantic matches first (handle synonyms/paraphrases), then any
    # exact keyword hits not already covered (high precision).
    merged, seen = [], set()
    for m in sem + kw:
        s = _sig(m)
        if s in seen:
            continue
        seen.add(s)
        merged.append(m)
    return {"matches": merged[:top_k], "total_matches": len(merged), "method": "semantic+keyword"}
