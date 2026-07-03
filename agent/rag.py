"""
Reference document retrieval — keyword-frequency search over whatever was
already extracted from uploaded reference docs (data dictionary entries,
business rules, mapping specs). Not embedding/vector-based: the reference
material here is small (a handful of columns/rules per dataset), so a
simple substring/keyword match is enough and needs no vector DB.
"""


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

    return {"matches": matches[:top_k], "total_matches": len(matches)}
