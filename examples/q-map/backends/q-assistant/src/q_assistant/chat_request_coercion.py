from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from .models import ChatRequest


def _coerce_chat_request(raw_payload: dict[str, Any] | None) -> ChatRequest:
    payload = raw_payload or {}

    # Strict canonical shape for /chat.
    if "prompt" not in payload:
        raise HTTPException(
            status_code=422,
            detail='Missing "prompt" in request body for /chat',
        )
    return ChatRequest.model_validate(payload)
