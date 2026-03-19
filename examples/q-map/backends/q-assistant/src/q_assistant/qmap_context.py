from __future__ import annotations

import json
from typing import Any


def _sanitize_qmap_context_payload(payload: Any) -> Any:
    blocked_keys = {
        "api_key",
        "apikey",
        "token",
        "authorization",
        "password",
        "secret",
        "email",
    }

    if isinstance(payload, dict):
        out: dict[str, Any] = {}
        for key, value in payload.items():
            key_text = str(key or "")
            if key_text.lower() in blocked_keys:
                continue
            out[key_text] = _sanitize_qmap_context_payload(value)
        return out

    if isinstance(payload, list):
        return [_sanitize_qmap_context_payload(item) for item in payload[:200]]

    if isinstance(payload, str):
        text = payload.strip()
        return text[:320]

    return payload


def _inject_qmap_context_message(
    payload: dict[str, Any],
    qmap_context_header: str | None,
    *,
    enabled: bool,
    max_chars: int,
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    if not enabled:
        return outgoing
    raw_header = (qmap_context_header or "").strip()
    if not raw_header:
        return outgoing

    try:
        parsed = json.loads(raw_header)
    except Exception:
        return outgoing

    sanitized = _sanitize_qmap_context_payload(parsed)
    text = json.dumps(sanitized, ensure_ascii=False)
    if max_chars > 0 and len(text) > max_chars:
        if max_chars < 128:
            text = '{"truncated":true}'
        else:
            keep = max_chars - len('{"truncated":true,"preview":""}') - 8
            preview = text[: max(0, keep)]
            text = json.dumps({"truncated": True, "preview": preview}, ensure_ascii=False)

    messages = outgoing.get("messages")
    if not isinstance(messages, list):
        return outgoing

    system_message = {
        "role": "system",
        "content": (
            "Runtime q-map context (datasets/layers/active filters). "
            "Use this as authoritative UI state: " + text
        ),
    }
    outgoing["messages"] = [system_message, *messages]
    return outgoing
