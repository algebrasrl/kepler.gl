from __future__ import annotations

import json
import os
import random
from typing import Any


def _provider_api_key(provider: str, default_api_key: str) -> str:
    global_api_key = os.getenv("Q_ASSISTANT_API_KEY", "").strip()
    if global_api_key:
        return global_api_key
    return {
        "openai": os.getenv("OPENAI_API_KEY", ""),
        "openrouter": os.getenv("OPENROUTER_API_KEY", ""),
        "ollama": "",
    }.get(provider, "") or default_api_key


def _openrouter_optional_headers(provider: str) -> dict[str, str]:
    if str(provider or "").lower() != "openrouter":
        return {}
    headers: dict[str, str] = {}
    referer = os.getenv("OPENROUTER_HTTP_REFERER", "").strip()
    title = os.getenv("OPENROUTER_X_TITLE", "").strip()
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title
    return headers


def _extract_error_message(body: Any) -> str:
    if not isinstance(body, dict):
        return str(body or "")
    message = body.get("message")
    if isinstance(message, str) and message:
        return message
    error = body.get("error")
    if isinstance(error, dict):
        nested = error.get("message")
        if isinstance(nested, str) and nested:
            return nested
        return json.dumps(error)
    if isinstance(error, str) and error:
        return error
    return json.dumps(body)


def _is_retryable_status(status_code: int) -> bool:
    return status_code in {408, 409, 429, 500, 502, 503, 504}


def _compute_retry_delay(
    attempt: int,
    *,
    base_delay_seconds: float,
    max_delay_seconds: float,
    jitter_ratio: float = 0.0,
) -> float:
    delay = min(base_delay_seconds * (2 ** max(attempt - 1, 0)), max_delay_seconds)
    jitter = max(0.0, min(1.0, float(jitter_ratio)))
    if jitter <= 0 or delay <= 0:
        return delay
    factor = 1.0 + random.uniform(-jitter, jitter)
    return max(0.0, min(max_delay_seconds, delay * factor))
