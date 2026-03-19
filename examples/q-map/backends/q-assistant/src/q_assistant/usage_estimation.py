from __future__ import annotations

import json
from typing import Any


def _as_int_if_possible(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _estimate_tokens_from_text(text: str, *, model_hint: str | None = None) -> tuple[int | None, str]:
    body = str(text or "")
    if not body:
        return 0, "empty"

    # Use tokenizer-backed estimates when available; otherwise use char-ratio approximation.
    try:
        import tiktoken  # type: ignore

        hint = str(model_hint or "").strip()
        if "/" in hint:
            hint = hint.split("/")[-1].strip()
        if hint:
            try:
                encoding = tiktoken.encoding_for_model(hint)
                return len(encoding.encode(body)), f"tiktoken:{hint}"
            except Exception:
                pass
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(body)), "tiktoken:cl100k_base"
    except Exception:
        # Fallback: ratio calibrated from OpenRouter/Gemini audit history (~5.5 chars/token).
        # Model-specific ratios from production observations; default 4.0 for unknown models.
        _MODEL_CHAR_RATIOS: dict[str, float] = {
            "gemini": 5.5,
            "gemma": 5.5,
        }
        hint_lower = str(model_hint or "").lower()
        ratio = next((r for k, r in _MODEL_CHAR_RATIOS.items() if k in hint_lower), 4.0)
        estimated = max(1, int(len(body) / ratio))
        return estimated, f"approx:chars_div_{ratio}"


def _estimate_payload_token_usage(payload: Any, *, model_hint: str | None = None) -> dict[str, Any]:
    try:
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        serialized = str(payload)

    estimated_tokens, method = _estimate_tokens_from_text(serialized, model_hint=model_hint)
    out: dict[str, Any] = {
        "estimatedPromptTokens": int(estimated_tokens) if estimated_tokens is not None else None,
        "serializedChars": len(serialized),
        "method": method,
    }
    if isinstance(payload, dict):
        messages = payload.get("messages")
        if isinstance(messages, list):
            out["messageCount"] = len(messages)
            out["toolMessageCount"] = sum(
                1 for msg in messages if isinstance(msg, dict) and str(msg.get("role") or "").strip().lower() == "tool"
            )
        tools = payload.get("tools")
        if isinstance(tools, list):
            out["toolCount"] = len(tools)
    return out


def _normalize_upstream_usage(usage: Any) -> dict[str, int] | None:
    if not isinstance(usage, dict):
        return None

    prompt_tokens = _as_int_if_possible(
        usage.get("prompt_tokens", usage.get("input_tokens", usage.get("promptTokenCount")))
    )
    completion_tokens = _as_int_if_possible(
        usage.get("completion_tokens", usage.get("output_tokens", usage.get("candidates_token_count")))
    )
    total_tokens = _as_int_if_possible(usage.get("total_tokens", usage.get("totalTokenCount")))

    if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
        total_tokens = prompt_tokens + completion_tokens

    if prompt_tokens is None and completion_tokens is None and total_tokens is None:
        return None

    normalized: dict[str, int] = {}
    if prompt_tokens is not None:
        normalized["promptTokens"] = int(prompt_tokens)
    if completion_tokens is not None:
        normalized["completionTokens"] = int(completion_tokens)
    if total_tokens is not None:
        normalized["totalTokens"] = int(total_tokens)
    return normalized


def _extract_upstream_usage(payload: Any) -> dict[str, int] | None:
    if not isinstance(payload, dict):
        return None

    direct = _normalize_upstream_usage(payload.get("usage"))
    if direct:
        return direct

    # Gemini native / OpenRouter proxied Gemini: usageMetadata at top level.
    usage_meta = payload.get("usageMetadata")
    if isinstance(usage_meta, dict):
        meta_usage = _normalize_upstream_usage(usage_meta)
        if meta_usage:
            return meta_usage

    choices = payload.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            usage = _normalize_upstream_usage(choice.get("usage"))
            if usage:
                return usage
    return None
