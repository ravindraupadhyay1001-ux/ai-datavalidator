"""
Reference document retrieval — keyword-frequency search over whatever was
extracted from uploaded reference docs: structured data (data dictionary
entries, business rules, mapping specs) via substring match, plus free
text extracted from PDF reference docs via term-frequency-scored snippet
extraction. Not embedding/vector-based: no vector DB is set up, and for
per-session, per-dataset reference material this size, keyword scoring
gets a useful answer without one.
"""
import re

_WORD_RX = re.compile(r"[a-z0-9]+")
_SNIPPET_RADIUS = 200  # chars of context on each side of a hit


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
    # merge nearby hit positions into windows, scoring by hit density
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


def search(ref_docs: dict, query: str, top_k: int = 15) -> dict:
    if not ref_docs:
        return {"matches": [], "note": "No reference documents were uploaded with this session."}
    q = (query or "").lower().strip()
    if not q:
        return {"matches": [], "note": "Empty query."}

    matches = []
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
        snippets = _score_snippets(text, query_terms)
        for snippet in snippets:
            matches.append({"type": "document", "source": doc_name, "snippet": snippet})

    return {"matches": matches[:top_k], "total_matches": len(matches)}
