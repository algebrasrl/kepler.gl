from __future__ import annotations


def _normalize_ollama_base(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith("/api"):
        return base[:-4]
    if base.endswith("/v1"):
        return base[:-3]
    return base


def _ollama_chat_url(base_url: str) -> str:
    """
    Ollama native chat endpoint.
    """
    return f"{_normalize_ollama_base(base_url)}/api/chat"


def _ollama_openai_chat_completions_url(base_url: str) -> str:
    """
    Ollama OpenAI-compatible endpoint.
    """
    return f"{_normalize_ollama_base(base_url)}/v1/chat/completions"
