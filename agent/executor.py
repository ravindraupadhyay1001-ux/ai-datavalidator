"""
Agent tool-calling loop: bind the 6 tools, invoke the LLM, execute
whichever tools it calls, feed results back, repeat until it gives a
final text answer or hits the iteration cap.
"""
import json

from agent import llm, prompts
from agent.tools import TOOL_SCHEMAS

MAX_ITERATIONS = 8


def run_agent(user_message: str, history: list[dict], tool_dispatch, extra_rules: str = "") -> str:
    """
    history: prior [{"role","content"}] turns (plain, no tool-call framing).
    tool_dispatch: callable(name, arguments_dict) -> JSON-serializable dict,
        e.g. from agent.tools.make_tool_dispatch(session_results, ref_docs).
    Returns the final assistant text.
    """
    if not llm.TOOL_CALLING_CONFIGURED:
        return llm.ask_safe(history + [{"role": "user", "content": user_message}],
                            system=prompts.build_system_prompt(extra_rules))

    system = prompts.build_system_prompt(extra_rules)
    messages = list(history) + [{"role": "user", "content": user_message}]

    for _ in range(MAX_ITERATIONS):
        try:
            result = llm.ask_with_tools(messages, TOOL_SCHEMAS, system=system)
        except Exception as e:
            return f"[AI unavailable: {e}]"

        tool_calls = result.get("tool_calls") or []
        if not tool_calls:
            return result.get("content") or "[No response from the agent.]"

        messages.append({
            "role": "assistant",
            "content": result.get("content"),
            "tool_calls": [
                {"id": tc["id"], "type": "function",
                 "function": {"name": tc["name"], "arguments": tc["arguments"]}}
                for tc in tool_calls
            ],
        })
        for tc in tool_calls:
            try:
                args = json.loads(tc["arguments"]) if tc["arguments"] else {}
            except Exception:
                args = {}
            try:
                tool_result = tool_dispatch(tc["name"], args)
            except Exception as e:
                tool_result = {"error": str(e)}
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": json.dumps(tool_result, default=str),
            })

    return "[Agent stopped after 8 tool-call rounds without a final answer. Try a more specific question.]"
