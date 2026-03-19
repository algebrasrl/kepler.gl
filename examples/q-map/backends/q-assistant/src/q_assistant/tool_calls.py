from __future__ import annotations

import json
from typing import Any

from .request_coercion import _normalize_tool_argument_object


def _parse_tool_arguments(raw_arguments: Any, *, none_on_failure: bool = False) -> dict[str, Any] | None:
    if isinstance(raw_arguments, dict):
        return _normalize_tool_argument_object(raw_arguments)
    if not isinstance(raw_arguments, str):
        return None if none_on_failure else {}
    text = raw_arguments.strip()
    if not text:
        return None if none_on_failure else {}
    try:
        parsed = json.loads(text)
    except Exception:
        return None if none_on_failure else {}
    if isinstance(parsed, dict):
        return _normalize_tool_argument_object(parsed)
    return None if none_on_failure else {}


def _extract_request_tool_names(payload: dict[str, Any]) -> list[str]:
    if not isinstance(payload, dict):
        return []
    tools = payload.get("tools")
    if not isinstance(tools, list):
        return []
    names: list[str] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        if not isinstance(fn, dict):
            continue
        name = fn.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names


def _extract_tool_calls_from_assistant_message(message: Any) -> list[dict[str, Any]]:
    if not isinstance(message, dict):
        return []
    role = str(message.get("role") or "").strip().lower()
    if role and role != "assistant":
        return []
    tool_calls = message.get("tool_calls")
    if not isinstance(tool_calls, list):
        return []

    out: list[dict[str, Any]] = []
    for call in tool_calls:
        if not isinstance(call, dict):
            continue
        fn = call.get("function")
        fn_dict = fn if isinstance(fn, dict) else {}
        name = str(fn_dict.get("name") or "").strip() or None
        out.append(
            {
                "id": str(call.get("id") or "").strip() or None,
                "name": name,
                "function": fn_dict,
            }
        )
    return out


def _extract_response_tool_calls(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return []
    calls: list[str] = []
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message")
        for call in _extract_tool_calls_from_assistant_message(message):
            name = call.get("name")
            if isinstance(name, str) and name.strip():
                calls.append(name.strip())
    return calls


def _extract_assistant_tool_calls(messages: Any, *, max_items: int = 48) -> list[dict[str, Any]]:
    if not isinstance(messages, list):
        return []
    calls: list[dict[str, Any]] = []
    for message in messages:
        for call in _extract_tool_calls_from_assistant_message(message):
            name = str(call.get("name") or "").strip()
            if not name:
                continue
            fn = call.get("function")
            fn_dict = fn if isinstance(fn, dict) else {}
            calls.append(
                {
                    "id": str(call.get("id") or "").strip() or None,
                    "name": name,
                    "args": _parse_tool_arguments(fn_dict.get("parsedArguments", fn_dict.get("arguments"))),
                }
            )
            if len(calls) >= max_items:
                return calls[-max_items:]
    return calls[-max_items:]
