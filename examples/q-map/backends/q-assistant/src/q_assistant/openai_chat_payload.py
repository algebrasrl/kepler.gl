from __future__ import annotations

from typing import Any

from .message_text import _extract_message_text
from .openai_tool_schema import _coerce_openai_tools
from .request_coercion import _repair_message_tool_call_arguments
from .request_markers import _strip_request_id_markers_from_messages


def _coerce_openai_chat_payload(raw_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw_payload or {})
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        candidate: list[dict[str, Any]] | None = None
        message = payload.get("message")
        if isinstance(message, dict):
            role = str(message.get("role") or "user").strip().lower() or "user"
            text = _extract_message_text(message)
            if text:
                candidate = [{"role": role, "content": text}]
        elif isinstance(message, str) and message.strip():
            candidate = [{"role": "user", "content": message.strip()}]
        if candidate is None:
            prompt = payload.get("prompt")
            if isinstance(prompt, str) and prompt.strip():
                candidate = [{"role": "user", "content": prompt.strip()}]
        if candidate is not None:
            payload["messages"] = candidate
    payload["messages"] = _strip_request_id_markers_from_messages(payload.get("messages"))
    payload["messages"] = _repair_message_tool_call_arguments(payload.get("messages"))
    instructions = payload.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        msgs = payload.get("messages")
        if isinstance(msgs, list):
            has_system = any(
                isinstance(msg, dict) and str(msg.get("role") or "").strip().lower() == "system" for msg in msgs
            )
            if not has_system:
                payload["messages"] = [{"role": "system", "content": instructions.strip()}, *msgs]
        else:
            payload["messages"] = [{"role": "system", "content": instructions.strip()}]
    payload.pop("message", None)
    payload.pop("instructions", None)
    if "tools" in payload:
        payload["tools"] = _coerce_openai_tools(payload.get("tools"))
    # Force sequential tool calls: the model must emit one tool call per
    # response so it can observe each result before planning the next step.
    # This prevents batch-planned tool chains with stale/guessed dataset IDs.
    if payload.get("tools"):
        payload["parallel_tool_calls"] = False
    return payload
