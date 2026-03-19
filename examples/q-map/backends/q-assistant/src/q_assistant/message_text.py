from __future__ import annotations

import re
from typing import Any


def _text_from_message_content(content: Any) -> str:
    def _sanitize_message_text(text: str) -> str:
        # Remove control bytes that can leak from upstream providers and pollute final narrative.
        sanitized = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", str(text or ""))
        return sanitized.strip()

    if isinstance(content, str):
        return _sanitize_message_text(content)
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    cleaned = _sanitize_message_text(text)
                    if cleaned:
                        parts.append(cleaned)
        return "\n".join(parts).strip()
    return ""


def _text_from_message_parts(parts: Any) -> str:
    if not isinstance(parts, list):
        return ""
    texts: list[str] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if not isinstance(text, str) or not text.strip():
            text = part.get("content")
        if not isinstance(text, str) or not text.strip():
            text = part.get("value")
        if isinstance(text, str):
            cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text).strip()
            if cleaned:
                texts.append(cleaned)
    return "\n".join(texts).strip()


def _extract_message_text(message: Any) -> str:
    if not isinstance(message, dict):
        if isinstance(message, str):
            return message.strip()
        return ""
    content = _text_from_message_content(message.get("content"))
    if content:
        return content
    return _text_from_message_parts(message.get("parts"))


def _is_control_finalize_prompt(text: str) -> bool:
    normalized = " ".join(str(text or "").strip().lower().split())
    if not normalized:
        return False
    markers = (
        "tool execution complete.",
        "provide a concise final answer in plain text without calling tools.",
    )
    return all(marker in normalized for marker in markers)


def _extract_prompt_from_messages(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""

    # Prefer the most recent user message.
    latest_user_prompt = ""
    for item in reversed(messages):
        if not isinstance(item, dict):
            continue
        if str(item.get("role") or "").lower() != "user":
            continue
        text = _text_from_message_content(item.get("content"))
        if not text:
            continue
        if not latest_user_prompt:
            latest_user_prompt = text
        if _is_control_finalize_prompt(text):
            continue
        return text

    if latest_user_prompt:
        return latest_user_prompt

    # Fallback: any most recent textual message.
    for item in reversed(messages):
        if not isinstance(item, dict):
            continue
        text = _text_from_message_content(item.get("content"))
        if text:
            return text

    return ""


def _extract_prompt_from_single_message(message: Any) -> str:
    if not isinstance(message, dict):
        if isinstance(message, str):
            return message.strip()
        return ""

    role = str(message.get("role") or "").lower()
    content = _extract_message_text(message)
    if role == "user" and content:
        return content

    # Fallback: accept textual content even if role is missing/unexpected.
    return content
