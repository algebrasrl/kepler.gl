from __future__ import annotations

import re
from typing import Any


_REQUEST_ID_MARKER_RE = re.compile(r"(?im)^\s*\[requestId:\s*[^\]]+\]\s*$")


def _strip_request_id_markers_from_text(content: Any) -> Any:
    if not isinstance(content, str):
        return content
    text = content.strip()
    if not text:
        return text
    lines = [line for line in text.splitlines() if not _REQUEST_ID_MARKER_RE.match(line)]
    return "\n".join(lines).strip()


def _strip_request_id_markers_from_messages(messages: Any) -> Any:
    if not isinstance(messages, list):
        return messages
    sanitized: list[Any] = []
    for raw_msg in messages:
        if not isinstance(raw_msg, dict):
            sanitized.append(raw_msg)
            continue
        msg = dict(raw_msg)
        role = str(msg.get("role") or "").strip().lower()
        if role != "assistant":
            sanitized.append(msg)
            continue
        content = msg.get("content")
        if isinstance(content, str):
            msg["content"] = _strip_request_id_markers_from_text(content)
        elif isinstance(content, list):
            next_content: list[Any] = []
            for part in content:
                if not isinstance(part, dict):
                    next_content.append(part)
                    continue
                next_part = dict(part)
                if isinstance(next_part.get("text"), str):
                    next_part["text"] = _strip_request_id_markers_from_text(next_part.get("text"))
                next_content.append(next_part)
            msg["content"] = next_content
        sanitized.append(msg)
    return sanitized
