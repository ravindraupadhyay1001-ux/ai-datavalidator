"""
Dataset-aware persistent feedback store.

Rules are keyed first by username, then by a dataset fingerprint derived from
the sorted column names of both uploaded files. The same dataset re-uploaded
by the same user will resolve to the same fingerprint and reload its saved
rules. Rules saved by one user are never visible to another user.

Rules saved before per-user isolation existed (flat, unattributed) are kept
under a special "__legacy__" bucket so no historical data is lost. They act
as a read-only fallback: visible to any user until that user first edits a
rule for that schema, at which point the entry is copied ("forked") into
their own bucket and further edits are private to them from then on.

Storage: feedback_rules.json in the project root.
Thread safety: module-level RLock serialises every read-modify-write operation.
"""

import copy
import hashlib
import json
import os
import threading
from datetime import date
from typing import Optional

_STORE_PATH = os.getenv("FEEDBACK_STORE_PATH") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "feedback_rules.json",
)

_LEGACY_BUCKET = "__legacy__"

_cache: Optional[dict] = None
_LOCK = threading.RLock()


def _is_legacy_format(store: dict) -> bool:
    """True if `store` is the old flat {fingerprint: entry} shape rather than
    the new {username: {fingerprint: entry}} shape."""
    for v in store.values():
        return isinstance(v, dict) and "rules" in v
    return False


def _migrate(store: dict) -> dict:
    if store and _is_legacy_format(store):
        store = {_LEGACY_BUCKET: store}
    return store


def _load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    store = {}
    if os.path.exists(_STORE_PATH):
        try:
            with open(_STORE_PATH, "r", encoding="utf-8") as fh:
                store = json.load(fh)
        except Exception:
            store = {}
    _cache = _migrate(store)
    return _cache


def _save(store: dict) -> None:
    global _cache
    _cache = store
    with open(_STORE_PATH, "w", encoding="utf-8") as fh:
        json.dump(store, fh, indent=2, ensure_ascii=False)


def _norm_col(name: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]", "", name.lower().strip())


def compute_fingerprint(cols1: list, cols2: list) -> str:
    half1 = "|".join(sorted(_norm_col(c) for c in cols1))
    half2 = "|".join(sorted(_norm_col(c) for c in cols2))
    key = "||".join(sorted([half1, half2]))
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


def _jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    return len(set_a & set_b) / len(union) if union else 0.0


def _norm_name(name: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]", "", name.lower().strip())


def _fuzzy_lookup_in(bucket: dict, cols1: list, cols2: list,
                      threshold: float = 0.7,
                      file_names: Optional[list] = None) -> Optional[str]:
    if not bucket:
        return None
    current = {_norm_col(c) for c in cols1} | {_norm_col(c) for c in cols2}
    best_fp, best_score = None, 0.0
    for fp, entry in bucket.items():
        saved_cols = set(entry.get("cols", []))
        if not saved_cols:
            continue
        score = _jaccard(current, saved_cols)
        if score > best_score:
            best_score = score
            best_fp = fp
    if best_fp and best_score >= threshold:
        return best_fp
    # Fall back to matching by file name when the column-based match is
    # inconclusive (e.g. very few shared/renamed columns) -- the same
    # source files being re-uploaded is a strong signal on its own.
    if file_names:
        current_names = {_norm_name(n) for n in file_names if n}
        if current_names:
            for fp, entry in bucket.items():
                saved_names = {_norm_name(n) for n in entry.get("file_names", [])}
                if saved_names and current_names & saved_names:
                    return fp
    return None


def fuzzy_lookup_fingerprint(username: str, cols1: list, cols2: list,
                             threshold: float = 0.7,
                             file_names: Optional[list] = None) -> Optional[str]:
    with _LOCK:
        store = _load()
        fp = _fuzzy_lookup_in(store.get(username, {}), cols1, cols2, threshold, file_names)
        if fp:
            return fp
        return _fuzzy_lookup_in(store.get(_LEGACY_BUCKET, {}), cols1, cols2, threshold, file_names)


def save_rule(username, fingerprint, rule_text, category="general",
              dataset_label=None, cols1=None, cols2=None, file_names=None):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        bucket = store.setdefault(username, {})
        if fingerprint not in bucket:
            legacy_entry = store.get(_LEGACY_BUCKET, {}).get(fingerprint)
            bucket[fingerprint] = copy.deepcopy(legacy_entry) if legacy_entry else {
                "label": dataset_label or "",
                "created": str(date.today()),
                "updated": str(date.today()),
                "rules": [],
            }
        entry = bucket[fingerprint]
        entry["updated"] = str(date.today())
        if dataset_label and not entry.get("label"):
            entry["label"] = dataset_label
        if cols1 is not None or cols2 is not None:
            all_cols = list(
                {_norm_col(c) for c in (cols1 or [])}
                | {_norm_col(c) for c in (cols2 or [])}
            )
            entry["cols"] = all_cols
        if file_names:
            entry["file_names"] = list({*entry.get("file_names", []), *file_names})
        for existing in entry["rules"]:
            if existing["rule"].strip().lower() == rule_text.strip().lower():
                _save(store)
                return entry["rules"].index(existing) + 1, queued
        entry["rules"].append({"rule": rule_text, "category": category})
        _save(store)
        return len(entry["rules"]), queued
    finally:
        _LOCK.release()


def resolve_fingerprint(username, fingerprint, cols1=None, cols2=None, file_names=None):
    with _LOCK:
        store = _load()
        if fingerprint in store.get(username, {}):
            return fingerprint
        if fingerprint in store.get(_LEGACY_BUCKET, {}):
            return fingerprint
        if cols1 is not None or cols2 is not None or file_names:
            best_fp = fuzzy_lookup_fingerprint(username, cols1 or [], cols2 or [], file_names=file_names)
            if best_fp:
                return best_fp
        return fingerprint


def _rule_matches_module(category: str, module: str) -> bool:
    # Rules explicitly namespaced to a module (dc_{module}_*, Dataset Controls)
    # only apply within that module. Recon-specific categories only apply to
    # compare/lineage-family modules. Everything else (general, manually
    # saved rules) is shared across all modules for this dataset.
    if category.startswith("dc_"):
        return category.startswith(f"dc_{module}_")
    if category in ("recon_hints", "recon_rule"):
        return module in ("compare", "lineage", "quality_ai", "profile_ai", "governance_ai")
    return True


def get_rules(username, fingerprint, cols1=None, cols2=None, file_names=None, module=None):
    with _LOCK:
        store = _load()
        resolved = resolve_fingerprint(username, fingerprint, cols1, cols2, file_names)
        entry = store.get(username, {}).get(resolved)
        if entry is None:
            entry = store.get(_LEGACY_BUCKET, {}).get(resolved, {})
        rules = entry.get("rules", [])
        if module:
            rules = [r for r in rules if _rule_matches_module(r.get("category", ""), module)]
        return rules


def get_rules_as_text(username, fingerprint, cols1=None, cols2=None, file_names=None, module=None):
    rules = get_rules(username, fingerprint, cols1=cols1, cols2=cols2, file_names=file_names, module=module)
    if not rules:
        return ""
    lines = [f"  [{r['category'].upper()}] {r['rule']}" for r in rules]
    return ("\n\nDataset-specific rules saved by the user "
            "(ALWAYS apply these):\n" + "\n".join(lines))


def _fork_if_legacy_only(store: dict, username: str, fingerprint: str) -> dict:
    """Ensure the user has their own writable copy of `fingerprint`'s entry,
    forking it from the legacy bucket on first write if needed. Returns the
    user's bucket dict."""
    bucket = store.setdefault(username, {})
    if fingerprint not in bucket:
        legacy_entry = store.get(_LEGACY_BUCKET, {}).get(fingerprint)
        if legacy_entry:
            bucket[fingerprint] = copy.deepcopy(legacy_entry)
    return bucket


def delete_rule(username, fingerprint, rule_index):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        bucket = _fork_if_legacy_only(store, username, fingerprint)
        rules = bucket.get(fingerprint, {}).get("rules", [])
        idx = rule_index - 1
        if 0 <= idx < len(rules):
            rules.pop(idx)
            bucket[fingerprint]["updated"] = str(date.today())
            _save(store)
            return True, queued
        return False, queued
    finally:
        _LOCK.release()


def update_rule(username, fingerprint, rule_index, rule_text=None,
                category=None, direction=None):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        bucket = _fork_if_legacy_only(store, username, fingerprint)
        rules = bucket.get(fingerprint, {}).get("rules", [])
        i = rule_index - 1
        if direction in ("up", "down"):
            j = i - 1 if direction == "up" else i + 1
            if 0 <= i < len(rules) and 0 <= j < len(rules):
                rules[i], rules[j] = rules[j], rules[i]
                bucket[fingerprint]["updated"] = str(date.today())
                _save(store)
                return True, queued
        else:
            if rule_text and 0 <= i < len(rules):
                rules[i]["rule"] = rule_text
                if category:
                    rules[i]["category"] = category
                bucket[fingerprint]["updated"] = str(date.today())
                _save(store)
                return True, queued
        return False, queued
    finally:
        _LOCK.release()


def copy_rules(username, from_fingerprint, to_fingerprint, categories=None) -> int:
    """Copy every rule from one schema's saved set (this user's own, falling
    back to the shared legacy set) onto another schema for the same user --
    a rule-set "template" applied to a different schema. Reuses save_rule()'s
    own dedup logic, so re-applying the same template twice is a no-op the
    second time. Returns the number of rules actually copied (excludes ones
    already present)."""
    with _LOCK:
        store = _load()
        source_rules = list(store.get(username, {}).get(from_fingerprint, {}).get("rules", []))
        if not source_rules:
            source_rules = list(store.get(_LEGACY_BUCKET, {}).get(from_fingerprint, {}).get("rules", []))
    if not source_rules:
        return 0
    copied = 0
    for r in source_rules:
        if categories and r.get("category") not in categories:
            continue
        with _LOCK:
            before = len(_load().get(username, {}).get(to_fingerprint, {}).get("rules", []))
        save_rule(username, to_fingerprint, r["rule"], category=r.get("category", "general"))
        with _LOCK:
            after = len(_load().get(username, {}).get(to_fingerprint, {}).get("rules", []))
        if after > before:
            copied += 1
    return copied


def list_all_datasets(username):
    with _LOCK:
        store = _load()
        merged = {**store.get(_LEGACY_BUCKET, {}), **store.get(username, {})}
        return [
            {
                "fingerprint": fp,
                "label": v.get("label", ""),
                "rule_count": len(v.get("rules", [])),
                "updated": v.get("updated", ""),
            }
            for fp, v in merged.items()
        ]


def get_dataset_label(username, fingerprint):
    with _LOCK:
        store = _load()
        entry = store.get(username, {}).get(fingerprint)
        if entry is None:
            entry = store.get(_LEGACY_BUCKET, {}).get(fingerprint, {})
        return entry.get("label", "") if entry else ""


def delete_user_data(username):
    """Remove a user's entire Dataset Memory bucket (used when an admin
    deletes that user's account). The shared __legacy__ bucket is untouched."""
    with _LOCK:
        store = _load()
        if username in store:
            del store[username]
            _save(store)


def invalidate_cache():
    global _cache
    with _LOCK:
        _cache = None
