"""
Dataset-aware persistent feedback store.

Rules are keyed by a dataset fingerprint derived from the sorted column names
of both uploaded files. The same dataset re-uploaded will resolve to the same
fingerprint and reload its saved rules.

Storage: feedback_rules.json in the project root.
Thread safety: module-level RLock serialises every read-modify-write operation.
"""

import hashlib
import json
import os
import threading
from datetime import date
from typing import Optional

_STORE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "feedback_rules.json",
)

_cache: Optional[dict] = None
_LOCK = threading.RLock()


def _load() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    if os.path.exists(_STORE_PATH):
        try:
            with open(_STORE_PATH, "r", encoding="utf-8") as fh:
                _cache = json.load(fh)
                return _cache
        except Exception:
            pass
    _cache = {}
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


def fuzzy_lookup_fingerprint(cols1: list, cols2: list,
                             threshold: float = 0.7) -> Optional[str]:
    with _LOCK:
        store = _load()
        if not store:
            return None
        current = {_norm_col(c) for c in cols1} | {_norm_col(c) for c in cols2}
        best_fp, best_score = None, 0.0
        for fp, entry in store.items():
            saved_cols = set(entry.get("cols", []))
            if not saved_cols:
                continue
            score = _jaccard(current, saved_cols)
            if score > best_score:
                best_score = score
                best_fp = fp
        return best_fp if best_score >= threshold else None


def save_rule(fingerprint, rule_text, category="general",
              dataset_label=None, cols1=None, cols2=None):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        if fingerprint not in store:
            store[fingerprint] = {
                "label": dataset_label or "",
                "created": str(date.today()),
                "updated": str(date.today()),
                "rules": [],
            }
        entry = store[fingerprint]
        entry["updated"] = str(date.today())
        if dataset_label and not entry.get("label"):
            entry["label"] = dataset_label
        if cols1 is not None or cols2 is not None:
            all_cols = list(
                {_norm_col(c) for c in (cols1 or [])}
                | {_norm_col(c) for c in (cols2 or [])}
            )
            entry["cols"] = all_cols
        for existing in entry["rules"]:
            if existing["rule"].strip().lower() == rule_text.strip().lower():
                _save(store)
                return entry["rules"].index(existing) + 1, queued
        entry["rules"].append({"rule": rule_text, "category": category})
        _save(store)
        return len(entry["rules"]), queued
    finally:
        _LOCK.release()


def resolve_fingerprint(fingerprint, cols1=None, cols2=None):
    with _LOCK:
        store = _load()
        if fingerprint in store:
            return fingerprint
        if cols1 is not None or cols2 is not None:
            best_fp = fuzzy_lookup_fingerprint(cols1 or [], cols2 or [])
            if best_fp and best_fp in store:
                return best_fp
        return fingerprint


def get_rules(fingerprint, cols1=None, cols2=None):
    with _LOCK:
        store = _load()
        resolved = resolve_fingerprint(fingerprint, cols1, cols2)
        return store.get(resolved, {}).get("rules", [])


def get_rules_as_text(fingerprint):
    rules = get_rules(fingerprint)
    if not rules:
        return ""
    lines = [f"  [{r['category'].upper()}] {r['rule']}" for r in rules]
    return ("\n\nDataset-specific rules saved by the user "
            "(ALWAYS apply these):\n" + "\n".join(lines))


def delete_rule(fingerprint, rule_index):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        rules = store.get(fingerprint, {}).get("rules", [])
        idx = rule_index - 1
        if 0 <= idx < len(rules):
            rules.pop(idx)
            store[fingerprint]["updated"] = str(date.today())
            _save(store)
            return True, queued
        return False, queued
    finally:
        _LOCK.release()


def update_rule(fingerprint, rule_index, rule_text=None,
                category=None, direction=None):
    queued = not _LOCK.acquire(blocking=False)
    if queued:
        _LOCK.acquire(blocking=True)
    try:
        store = _load()
        rules = store.get(fingerprint, {}).get("rules", [])
        i = rule_index - 1
        if direction in ("up", "down"):
            j = i - 1 if direction == "up" else i + 1
            if 0 <= i < len(rules) and 0 <= j < len(rules):
                rules[i], rules[j] = rules[j], rules[i]
                store[fingerprint]["updated"] = str(date.today())
                _save(store)
                return True, queued
        else:
            if rule_text and 0 <= i < len(rules):
                rules[i]["rule"] = rule_text
                if category:
                    rules[i]["category"] = category
                store[fingerprint]["updated"] = str(date.today())
                _save(store)
                return True, queued
        return False, queued
    finally:
        _LOCK.release()


def list_all_datasets():
    with _LOCK:
        store = _load()
        return [
            {
                "fingerprint": fp,
                "label": v.get("label", ""),
                "rule_count": len(v.get("rules", [])),
                "updated": v.get("updated", ""),
            }
            for fp, v in store.items()
        ]


def get_dataset_label(fingerprint):
    with _LOCK:
        return _load().get(fingerprint, {}).get("label", "")


def invalidate_cache():
    global _cache
    with _LOCK:
        _cache = None
