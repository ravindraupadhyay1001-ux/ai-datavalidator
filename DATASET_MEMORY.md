# Dataset Memory — How It Works

## Overview

Dataset memory is a dual-layer persistence system. It combines:

1. **Conversation Memory** — per-session chat history for the AI agent
2. **Dataset Rule Memory** — persistent, cross-session validation rules keyed by dataset schema fingerprint

These two layers work together so the agent remembers both *what you said* in a session and *what rules you've saved* for a given dataset schema.

## Layer 1: Conversation Memory

**File:** `agent/memory.py`

Each browser session gets its own isolated chat history, held in-process (cleared on server restart — there is no cross-restart persistence layer; if that's needed later, swap `ChatMemory`'s dict-backed store for Redis/DynamoDB without touching call sites).

| Function | Purpose |
|---|---|
| `ChatMemory.get(session_id)` | Retrieve or create a session's message list |
| `ChatMemory.append(session_id, role, content)` | Append a turn; trims to the last 24 messages (12 human + 12 AI) |
| `ChatMemory.clear(session_id)` | Wipe history for a session |

## Layer 2: Dataset Rule Memory (Persistent)

**File:** `agent/feedback_store.py`

Rules are stored in `feedback_rules.json` and survive server restarts. They are keyed by a **schema fingerprint** — a 12-character MD5 hash of the dataset's sorted, normalized column names. This means rules follow the schema, not the filename.

### Schema Fingerprinting

```python
compute_fingerprint(cols1, cols2)  # -> 12-char hex, e.g. "a3f7c9d21e04"
```

- Column names are normalized (lowercased, non-alphanumeric stripped) and sorted before hashing.
- Two datasets with the same columns but different file names, renames, or column order produce the **same** fingerprint.

### Three-Tier Lookup

When a dataset is uploaded, `resolve_fingerprint()` tries three strategies in order:

| Priority | Strategy | Condition |
|---|---|---|
| 1 | **Exact fingerprint match** | Uploaded file's computed fingerprint matches a saved entry exactly |
| 2 | **Fuzzy Jaccard match** | `fuzzy_lookup_fingerprint()` — Jaccard similarity ≥ 0.70 on normalized column-name sets |
| 3 | **File-name fallback** | If column-based matching is inconclusive, match by previously-seen file names |

This ensures rules are found even when files are renamed, a few columns are added/removed, or column order changes.

### Module Isolation

Rules are tagged with a `category` that namespaces them by module, so a rule saved in one module never bleeds into another:

| Category prefix | Module |
|---|---|
| `recon_hints`, `recon_rule` | Reconciliation / Cross Reference / AI-driven Quality-Profile-Governance |
| `dc_quality_*` | Data Quality (Dataset Controls) |
| `dc_governance_*` | Data Governance |
| `dc_profile_*` | Data Profile |
| `dc_compare_*` (dc_-prefixed, any module) | Namespaced to that exact module only |
| `general` (default) | Shared across all modules for that dataset |

`_rule_matches_module(category, module)` enforces this at read time.

### Rule CRUD API (`agent/feedback_store.py`)

| Function | Purpose |
|---|---|
| `save_rule(fp, rule_text, category, ...)` | Save a rule; auto-deduplicates exact matches (returns the existing rule's 1-based index instead of creating a duplicate) |
| `get_rules(fp, ...)` / `get_rules_as_text(fp, ...)` | Retrieve rules as structured list or formatted LLM prompt text |
| `update_rule(fp, index, rule_text=..., category=..., direction=...)` | Edit rule text/category, or reorder via `direction="up"/"down"` |
| `delete_rule(fp, index)` | Remove by 1-based index |
| `list_all_datasets()` | Enumerate every schema with saved rules — `[{fingerprint, label, rule_count, updated}, ...]` |
| `copy_rules(from_fp, to_fp, categories=None)` | **Rule Templates** — copy one schema's saved rules onto another, skipping duplicates. Powers the "Rule Templates" panel in the AI Copilot so a rule set tuned for one file pair can be reused on a differently-named pair of the same shape. |

Thread safety is managed via a module-level `threading.RLock` for all read-modify-write operations.

### REST Endpoints for Rule Management

| Endpoint | Method | Purpose |
|---|---|---|
| `/rules/{session_id}` | GET | Get rules for the current session's dataset |
| `/rules/{session_id}/save` | POST | Save a rule |
| `/rules/{session_id}/delete` | POST | Delete by 1-based index |
| `/rules/{session_id}/update` | POST | Edit or reorder a rule |
| `/rules/{session_id}/export` | GET | Download rules as JSON |
| `/rules/{session_id}/import` | POST | Import rules (merge + dedupe) |
| `/dataset-controls/{session_id}/rules` | GET | Get rules (Dataset Controls panel) |
| `/dataset-controls/{session_id}/apply` | POST | Save a rule from a plain-English instruction |
| `/api/dq/rules/{fingerprint}` | GET/POST | Save/apply DQ column rules by fingerprint (LLM-interpreted from English) |
| `/api/recon/rule-templates` | GET | Browse every schema with saved rules (template library) |
| `/api/recon/rule-templates/apply` | POST | Copy one schema's rules onto another |

## How the Agent Uses Memory

**File:** `agent/executor.py`

Every time the agent runs, it:

1. Loads conversation history for the session
2. Resolves the dataset's schema fingerprint
3. Retrieves saved rules via `get_rules_as_text(fingerprint, module=...)`
4. Builds the LLM prompt: system context (fingerprint + saved rules) + prior conversation turns + the new user message
5. Executes the agent loop (tool calls, if in Agent mode)
6. Persists the new turn back into conversation memory

Saved rules are injected into the system prompt on every call, so the LLM always respects user-defined preferences without being re-told.

## End-to-End Walkthrough

1. User uploads `sales.csv` + `forecast.csv` → `compute_fingerprint(cols1, cols2)` = `"a3f7c9d21e04"`, stored in `results_store[session_id]["dataset_fingerprint"]`
2. User chats: *"amount must be non-null"* → agent responds and (if user confirms) calls `save_rule()` → rule written to `feedback_rules.json` under `"a3f7c9d21e04"`
3. Next week, user uploads the same schema (possibly renamed files, possibly reordered columns) → fingerprint matches → `get_rules_as_text()` injects the prior rule into the agent prompt automatically → **no re-entry needed**

## Key Design Decisions

| Decision | Reason |
|---|---|
| Fingerprint by column names, not file identity | Rules follow schema evolution and renames gracefully |
| Module-namespaced categories | Prevents rules from one module bleeding into another |
| 3-tier lookup with fuzzy fallback | Handles schema drift (a few columns added/removed/renamed) without losing rule reuse |
| JSON file storage for rules | Human-readable, directly editable, portable, no DB dependency |
| `threading.RLock` around all read-modify-write ops | Handles concurrent requests safely |
| Rule Templates via `copy_rules()` | Rules were scoped to one schema only — copying an existing tuned rule set onto a new, differently-shaped file pair avoids re-teaching the same preferences from scratch |
