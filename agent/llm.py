"""
Multi-provider LLM factory — Bedrock / Groq / Gemini / OpenAI, with
automatic fallback. Groq is the default primary since it's free-tier and
doesn't need paid AWS Bedrock credits.

Two call shapes:
  ask(messages, system) / ask_safe(...)  -> plain text completion
  ask_with_tools(messages, tools, system) -> raw assistant message dict,
      possibly containing tool_calls, for the agent's tool-calling loop.
      Only Groq and OpenAI support native tool-calling here (both speak
      the same OpenAI-compatible wire format).
"""
import os

MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq").lower()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
# Accept either name -- the Settings page writes GEMINI_API_KEY while env-var
# setups often use GOOGLE_API_KEY; read both so Gemini works via either path.
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY", "")
# gemini-1.5-flash is retired (404 on v1beta); gemini-2.0-flash is the current
# free-tier fast model.
GEMINI_MODEL = os.getenv("GEMINI_MODEL") or os.getenv("GEMINI_MODEL_ID", "gemini-2.0-flash")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL_ID", "claude-opus-4-8")
# OpenRouter -- OpenAI-compatible gateway with a free tier; a good fallback
# when the Groq free quota is exhausted. Free model availability changes over
# time (some ":free" slugs become paid) -- override OPENROUTER_MODEL with any
# current free model from https://openrouter.ai/models?max_price=0 if needed.
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.2-3b-instruct:free")

AI_CONFIGURED = bool(MODEL_ID or GROQ_API_KEY or GOOGLE_API_KEY or OPENAI_API_KEY or ANTHROPIC_API_KEY or OPENROUTER_API_KEY)
TOOL_CALLING_CONFIGURED = bool(GROQ_API_KEY or OPENAI_API_KEY or OPENROUTER_API_KEY)


def _get_bedrock_client():
    import boto3
    session = boto3.Session(
        profile_name=os.getenv("AWS_PROFILE") or None,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    return session.client("bedrock-runtime")


def _ask_bedrock(messages, system=None):
    if not MODEL_ID:
        raise RuntimeError("BEDROCK_MODEL_ID not configured.")
    client = _get_bedrock_client()
    kwargs = {
        "modelId": MODEL_ID,
        "messages": [{"role": m["role"], "content": [{"text": m["content"]}]}
                     for m in messages],
        "inferenceConfig": {"maxTokens": 2000, "temperature": 0.2},
    }
    if system:
        kwargs["system"] = [{"text": system}]
    resp = client.converse(**kwargs)
    return resp["output"]["message"]["content"][0]["text"]


def _groq_client():
    if not GROQ_API_KEY:
        return None
    from openai import OpenAI
    return OpenAI(api_key=GROQ_API_KEY, base_url="https://api.groq.com/openai/v1"), GROQ_MODEL


def _openai_client():
    if not OPENAI_API_KEY:
        return None
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY), OPENAI_MODEL


def _openrouter_client():
    if not OPENROUTER_API_KEY:
        return None
    from openai import OpenAI
    # Optional ranking headers OpenRouter recommends; harmless if generic.
    return OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1",
        default_headers={"HTTP-Referer": "https://ai-datavalidator.com", "X-Title": "AI DataValidator"},
    ), OPENROUTER_MODEL


def _ask_groq(messages, system=None):
    """Groq's API is OpenAI-compatible, so the `openai` SDK works pointed
    at Groq's base URL — free tier, no separate SDK dependency needed."""
    info = _groq_client()
    if not info:
        raise RuntimeError("GROQ_API_KEY not configured.")
    client, model = info
    full = ([{"role": "system", "content": system}] if system else []) + messages
    resp = client.chat.completions.create(
        model=model, messages=full, max_tokens=2000, temperature=0.2)
    return resp.choices[0].message.content


def _ask_gemini(messages, system=None):
    if not GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not configured.")
    convo = "\n\n".join(f"{m['role'].upper()}: {m['content']}" for m in messages)
    # Prefer the current `google-genai` SDK; fall back to the deprecated
    # `google-generativeai` if only that is installed, so Gemini keeps working
    # either way during the dependency transition.
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=GOOGLE_API_KEY)
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=convo,
            config=types.GenerateContentConfig(
                system_instruction=system or None,
                max_output_tokens=2000,
                temperature=0.2,
            ),
        )
        return resp.text
    except ImportError:
        import google.generativeai as genai
        genai.configure(api_key=GOOGLE_API_KEY)
        model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=system or None)
        resp = model.generate_content(
            convo, generation_config={"max_output_tokens": 2000, "temperature": 0.2})
        return resp.text


def _ask_openai(messages, system=None):
    info = _openai_client()
    if not info:
        raise RuntimeError("OPENAI_API_KEY not configured.")
    client, model = info
    full = ([{"role": "system", "content": system}] if system else []) + messages
    resp = client.chat.completions.create(
        model=model, messages=full, max_tokens=2000, temperature=0.2)
    return resp.choices[0].message.content


def _ask_openrouter(messages, system=None):
    """OpenRouter is OpenAI-compatible, so the `openai` SDK works pointed at
    its base URL. Used as a free fallback when the Groq quota is exhausted."""
    info = _openrouter_client()
    if not info:
        raise RuntimeError("OPENROUTER_API_KEY not configured.")
    client, model = info
    full = ([{"role": "system", "content": system}] if system else []) + messages
    resp = client.chat.completions.create(
        model=model, messages=full, max_tokens=2000, temperature=0.2)
    return resp.choices[0].message.content


def _ask_anthropic(messages, system=None):
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not configured.")
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=ANTHROPIC_MODEL, max_tokens=2000, temperature=0.2,
        system=system or "",
        messages=[{"role": m["role"], "content": m["content"]} for m in messages],
    )
    return resp.content[0].text


_PROVIDERS = {
    "bedrock": _ask_bedrock,
    "groq": _ask_groq,
    "openrouter": _ask_openrouter,
    "gemini": _ask_gemini,
    "openai": _ask_openai,
    "anthropic": _ask_anthropic,
}
# OpenRouter right after Groq -- the free fallback when Groq's quota runs out.
_FALLBACK_ORDER = ["groq", "openrouter", "gemini", "bedrock", "openai", "anthropic"]

# Providers usable under a signed data-processing agreement (DPA) / zero-data-
# retention terms. The free tiers (groq / openrouter / gemini) are excluded
# because they may retain prompts or use them for model training. Bedrock runs
# inside your own AWS tenancy; OpenAI/Anthropic offer enterprise DPAs with
# zero-retention. Set DPA_ONLY=true for regulated (BFSI) data so only these
# providers are ever contacted, regardless of what LLM_PROVIDER / the Settings
# page selects. See DATA_GOVERNANCE.md.
_DPA_PROVIDERS = {"bedrock", "openai", "anthropic"}
DPA_ONLY = os.getenv("DPA_ONLY", "").strip().lower() in ("1", "true", "yes", "on")


def _governed(order):
    """Restrict a provider order to DPA-capable providers when DPA_ONLY is on,
    so masked schema samples can never reach a free / no-DPA provider even if
    one is configured or explicitly selected."""
    if not DPA_ONLY:
        return order
    return [p for p in order if p in _DPA_PROVIDERS]


def ask(messages, system=None):
    """Call the configured primary provider; auto-fallback through any other
    provider that has credentials configured if the primary fails."""
    order = _governed([LLM_PROVIDER] + [p for p in _FALLBACK_ORDER if p != LLM_PROVIDER])
    if not order:
        raise RuntimeError(
            "DPA_ONLY is enabled but no data-processing-agreement provider "
            "(Bedrock, OpenAI or Anthropic) is configured. Set one up in "
            "Settings, or unset DPA_ONLY for non-regulated data.")
    last_err = RuntimeError("No LLM provider configured.")
    for provider in order:
        fn = _PROVIDERS.get(provider)
        if not fn:
            continue
        try:
            return fn(messages, system)
        except Exception as e:
            last_err = e
    raise last_err


def ask_safe(messages, system=None):
    """Like ask() but returns an error string instead of raising."""
    try:
        return ask(messages, system)
    except Exception as e:
        return f"[AI unavailable: {e}]"


def ask_with_tools(messages, tools, system=None):
    """Returns {"content": str|None, "tool_calls": [{"id","name","arguments"}]}
    from whichever tool-calling-capable provider (groq/openai) is configured."""
    order = _governed([p for p in ([LLM_PROVIDER] + _FALLBACK_ORDER) if p in ("groq", "openai", "openrouter")])
    seen = set()
    full = ([{"role": "system", "content": system}] if system else []) + messages
    last_err = RuntimeError("No tool-calling-capable provider configured (need GROQ_API_KEY, OPENROUTER_API_KEY or OPENAI_API_KEY).")
    for provider in order:
        if provider in seen:
            continue
        seen.add(provider)
        info = _groq_client() if provider == "groq" else (
            _openrouter_client() if provider == "openrouter" else _openai_client())
        if not info:
            continue
        client, model = info
        try:
            resp = client.chat.completions.create(
                model=model, messages=full, tools=tools, tool_choice="auto",
                max_tokens=2000, temperature=0.2)
            msg = resp.choices[0].message
            return {
                "content": msg.content,
                "tool_calls": [
                    {"id": tc.id, "name": tc.function.name, "arguments": tc.function.arguments}
                    for tc in (msg.tool_calls or [])
                ],
            }
        except Exception as e:
            last_err = e
    raise last_err
