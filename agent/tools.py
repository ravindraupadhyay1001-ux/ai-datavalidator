"""
Tool definitions for the AI Copilot agent loop.

Each tool operates on the CURRENT analysis session's already-computed
results (main.py's _results_store) plus any reference documents uploaded
alongside it — not by re-reading raw uploaded files, which the app doesn't
retain past the initial request. So these are "inspect this session's
results" tools, not "kick off a brand-new analysis" tools.

TOOL_SCHEMAS is OpenAI-compatible function-calling JSON schema, consumed by
agent.llm.ask_with_tools() (works for Groq and OpenAI, which share that
wire format).
"""

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "compare_files",
            "description": "Get the reconciliation/compare results for this session (matched/only-in-A/only-in-B/modified counts, join keys used).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_data_quality",
            "description": "Get the data quality results for this session (score, grade, per-dimension breakdown, worst columns).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "map_columns",
            "description": "Get the column mapping results for this session (matched source->target pairs, methods, confidence, unmatched columns).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_governance_check",
            "description": "Get the governance/PII scan results for this session (sensitivity classification, PII columns, regulatory flags).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_lineage_analysis",
            "description": "Get the lineage reconciliation results for this session (transform-adjusted compare, per-mapped-pair breakdown).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_reference_docs",
            "description": "Search the reference documents uploaded with this session (data dictionary, business rules, mapping spec) for a term or column name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Term, column name, or question to search for."}
                },
                "required": ["query"],
            },
        },
    },
]

_TOOL_TO_ACTION = {
    "compare_files": "compare",
    "check_data_quality": "quality",
    "map_columns": "mapping",
    "run_governance_check": "governance",
    "run_lineage_analysis": "lineage",
}


def _summarize_result(result: dict) -> dict:
    """Trim a stored analysis result down to what's useful/affordable to
    hand back to the LLM (drop large row-level payloads)."""
    if not result:
        return {"error": "No result available."}
    keep = {}
    for key in ("counts", "dimensions", "score", "grade", "rows", "columns",
                "keys", "method", "duplicate_rows", "type_mismatches",
                "near_key_columns", "correlations", "matched", "unmatched_file1",
                "unmatched_file2", "overall_classification", "pii_columns",
                "regulatory_flags", "schema_changes"):
        if key in result:
            keep[key] = result[key]
    if "columns_detail" in result:
        keep["columns_detail_sample"] = result["columns_detail"][:5]
    if "field_specs" in result:
        keep["field_specs_sample"] = result["field_specs"][:10]
    return keep or {"note": "Result present but had no summarizable fields."}


def make_tool_dispatch(session_results: dict, ref_docs: dict):
    """Returns {tool_name: callable(arguments_dict) -> JSON-serializable dict}
    bound to one session's data. main.py builds this per chat request."""
    from agent import rag

    def dispatch(name, arguments):
        if name == "query_reference_docs":
            return rag.search(ref_docs, arguments.get("query", ""))
        action = _TOOL_TO_ACTION.get(name)
        if not action:
            return {"error": f"Unknown tool '{name}'."}
        if session_results.get("action") != action:
            return {"error": f"No {action} results in this session yet. "
                             f"The user needs to run the {action.title()} tab first."}
        return _summarize_result(session_results)

    return dispatch
