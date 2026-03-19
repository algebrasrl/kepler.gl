from __future__ import annotations

from typing import Any

from .message_text import _extract_prompt_from_messages
from .models import AgentConfig
from .tool_calls import _extract_request_tool_names


def _should_skip_agent_for_payload(agent: AgentConfig, payload: dict[str, Any]) -> str | None:
    provider = str(getattr(agent, "provider", "") or "").lower()
    model = str(getattr(agent, "model", "") or "").strip().lower()
    if provider != "openai":
        return None
    # Legacy gpt-4 variants have an 8k context window and cannot fit this q-map tool schema.
    if model in {"gpt-4", "gpt-4-0613", "gpt-4-0314"} and _extract_request_tool_names(payload):
        return "context-likely-exceeded-with-tools"
    return None


def _extract_explicit_tool_choice(payload: dict[str, Any]) -> str | None:
    """
    Resolve explicit tool command from last user message.
    Accepts forms like: listQCumberDatasets, <listQCumberDatasets, /listQCumberDatasets
    """
    if not isinstance(payload, dict):
        return None

    tools = payload.get("tools")
    if not isinstance(tools, list):
        return None

    tool_names: list[str] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        if not isinstance(fn, dict):
            continue
        name = fn.get("name")
        if isinstance(name, str) and name.strip():
            tool_names.append(name.strip())
    if not tool_names:
        return None

    raw_text = _extract_prompt_from_messages(payload.get("messages"))
    if not raw_text:
        return None

    candidate = raw_text.strip()
    candidate = candidate.lstrip("/<` ").rstrip(">` ")
    candidate = candidate.rstrip(" ,.;:!?")
    if not candidate or any(ch.isspace() for ch in candidate):
        return None

    by_lower = {name.lower(): name for name in tool_names}
    return by_lower.get(candidate.lower())


def _maybe_force_tool_choice(payload: dict[str, Any], *, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return dict(payload or {})

    base_payload = dict(payload or {})
    forced_tool_name = _extract_explicit_tool_choice(base_payload)
    if not forced_tool_name:
        return base_payload

    current_tool_choice = base_payload.get("tool_choice")
    if current_tool_choice in (None, "auto") or (
        isinstance(current_tool_choice, dict)
        and str(current_tool_choice.get("type") or "").lower() in {"", "auto"}
    ):
        base_payload["tool_choice"] = {"type": "function", "function": {"name": forced_tool_name}}
    return base_payload
