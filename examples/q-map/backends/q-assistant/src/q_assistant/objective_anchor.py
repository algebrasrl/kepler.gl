from __future__ import annotations

import re
from typing import Any

from .objective_intent import (
    _extract_objective_required_focus_phrases,
    _objective_requests_dataset_discovery,
)
from .message_text import _extract_prompt_from_messages
from .objective_focus import _extract_objective_focus_terms, _normalize_focus_token
from .tool_calls import _extract_tool_calls_from_assistant_message


_OBJECTIVE_ANCHOR_PREFIX = "[OBJECTIVE_ANCHOR]"
_OBJECTIVE_CRITERIA_PREFIX = "[OBJECTIVE_CRITERIA]"


def _build_objective_focus_terms(objective_text: str, *, max_terms: int = 8) -> list[str]:
    if max_terms <= 0:
        return []
    focus_terms = _extract_objective_focus_terms(objective_text, max_terms=min(3, max_terms))
    required_focus_phrases = _extract_objective_required_focus_phrases(objective_text)
    if not required_focus_phrases:
        return focus_terms[:max_terms]

    merged_terms: list[str] = []
    seen_terms: set[str] = set()
    for term in [*required_focus_phrases, *focus_terms]:
        key = _normalize_focus_token(term)
        if not key or key in seen_terms:
            continue
        seen_terms.add(key)
        merged_terms.append(term)
    return merged_terms[:max_terms]


def _normalize_assistant_final_text_content(content: str, *, focus_terms: list[str]) -> tuple[str, bool]:
    text = str(content or "")
    if not text:
        return text, False

    changed = False
    kept_lines: list[str] = []
    has_coverage_line = False
    has_user_facing_content = False
    for raw_line in text.splitlines():
        stripped = str(raw_line or "").strip()
        if not stripped:
            kept_lines.append("")
            continue
        normalized = " ".join(stripped.lower().split())
        if (
            normalized.startswith("[progress]")
            or normalized.startswith("[executionsummary]")
            or normalized.startswith("[requestid:")
            or normalized.startswith("[guardrail]")
        ):
            changed = True
            continue
        if stripped.startswith(_OBJECTIVE_ANCHOR_PREFIX) or stripped.startswith(_OBJECTIVE_CRITERIA_PREFIX):
            changed = True
            continue
        if "include one explicit line" in normalized and "copertura obiettivo" in normalized:
            changed = True
            continue
        if "use these terms only in final narrative" in normalized:
            changed = True
            continue
        if "do not call extra tools only to satisfy lexical coverage" in normalized:
            changed = True
            continue
        if normalized.startswith("copertura obiettivo:"):
            if has_coverage_line:
                changed = True
                continue
            has_coverage_line = True
        else:
            has_user_facing_content = True
        kept_lines.append(stripped)

    if focus_terms and not has_coverage_line and has_user_facing_content:
        kept_lines.append(f"Copertura obiettivo: {', '.join(focus_terms)}.")
        changed = True

    normalized_text = "\n".join(kept_lines).strip()
    return normalized_text, changed or normalized_text != text.strip()


def _normalize_openai_response_final_text(payload: Any, *, objective_text: str) -> Any:
    if not isinstance(payload, dict):
        return payload
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return payload

    focus_terms = _build_objective_focus_terms(objective_text, max_terms=8)
    outgoing = dict(payload)
    changed = False
    normalized_choices: list[Any] = []
    for choice in choices:
        if not isinstance(choice, dict):
            normalized_choices.append(choice)
            continue
        next_choice = dict(choice)
        message = next_choice.get("message")
        if isinstance(message, dict):
            next_message = dict(message)
            if _extract_tool_calls_from_assistant_message(next_message):
                normalized_choices.append(next_choice)
                continue
            content = next_message.get("content")
            if isinstance(content, str):
                normalized_content, content_changed = _normalize_assistant_final_text_content(
                    content,
                    focus_terms=focus_terms,
                )
                if content_changed:
                    changed = True
                    next_message["content"] = normalized_content
                    next_choice["message"] = next_message
        normalized_choices.append(next_choice)
    if changed:
        outgoing["choices"] = normalized_choices
    return outgoing


def _inject_objective_anchor_message(payload: dict[str, Any], *, max_chars: int = 420) -> dict[str, Any]:
    """
    Keep an explicit short objective in system context so multi-turn/tool-heavy chats
    remain goal-oriented even when message history is compacted.
    """
    outgoing = dict(payload or {})
    messages = outgoing.get("messages")
    if not isinstance(messages, list):
        return outgoing

    objective_raw = _extract_prompt_from_messages(messages)
    objective = re.sub(r"\s+", " ", str(objective_raw or "")).strip()
    if not objective:
        return outgoing
    if max_chars > 0 and len(objective) > max_chars:
        objective = objective[: max_chars - 3].rstrip() + "..."

    anchor_line = f"{_OBJECTIVE_ANCHOR_PREFIX} Active user goal: {objective}"
    criteria_line = (
        f"{_OBJECTIVE_CRITERIA_PREFIX} Complete the requested map workflow with the minimum necessary tool calls; "
        "when objective is satisfied stop calling tools and return concise final success/failure with key stats."
    )

    if not _objective_requests_dataset_discovery(objective):
        criteria_line += (
            " Do not call listQMapDatasets as a default first step: use it only when dataset inventory/discovery is"
            " explicitly requested or when a prior step failed due to missing snapshot/dataset-not-found."
        )
    focus_terms = _build_objective_focus_terms(objective, max_terms=8)
    if focus_terms:
        criteria_line += (
            " In final text include one explicit line `Copertura obiettivo: ...` reusing these exact terms: "
            + ", ".join(focus_terms)
            + "."
        )
        criteria_line += " Use these terms only in final narrative; do not call extra tools only to satisfy lexical coverage."

    cleaned_messages: list[dict[str, Any]] = []
    for raw_msg in messages:
        if not isinstance(raw_msg, dict):
            continue
        msg = dict(raw_msg)
        role = str(msg.get("role") or "").strip().lower()
        if role != "system":
            cleaned_messages.append(msg)
            continue
        content = msg.get("content")
        if not isinstance(content, str):
            cleaned_messages.append(msg)
            continue
        lines = [
            line
            for line in content.splitlines()
            if not line.strip().startswith(_OBJECTIVE_ANCHOR_PREFIX)
            and not line.strip().startswith(_OBJECTIVE_CRITERIA_PREFIX)
        ]
        msg["content"] = "\n".join(lines).strip()
        cleaned_messages.append(msg)

    for idx, msg in enumerate(cleaned_messages):
        role = str(msg.get("role") or "").strip().lower()
        if role != "system":
            continue
        content = str(msg.get("content") or "").strip()
        next_content = "\n".join([part for part in [content, anchor_line, criteria_line] if part]).strip()
        next_msg = dict(msg)
        next_msg["content"] = next_content
        cleaned_messages[idx] = next_msg
        outgoing["messages"] = cleaned_messages
        return outgoing

    outgoing["messages"] = [{"role": "system", "content": f"{anchor_line}\n{criteria_line}"}, *cleaned_messages]
    return outgoing
