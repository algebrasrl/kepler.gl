from __future__ import annotations

from typing import Any


def _normalize(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("answer", "message", "text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        # Ollama native /api/chat response shape:
        # {"message": {"role": "assistant", "content": "..."}}
        message = payload.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()

        choices = payload.get("choices")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            candidate = choices[0].get("message", {}).get("content")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        content = payload.get("content")
        if isinstance(content, list):
            texts = [item.get("text", "") for item in content if isinstance(item, dict)]
            joined = "\n".join([t for t in texts if isinstance(t, str) and t]).strip()
            if joined:
                return joined

        candidates = payload.get("candidates")
        if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict):
            parts = candidates[0].get("content", {}).get("parts", [])
            if isinstance(parts, list):
                joined = "\n".join(
                    [part.get("text", "") for part in parts if isinstance(part, dict)]
                ).strip()
                if joined:
                    return joined

    return "Nessuna risposta valida ricevuta dal provider AI."
