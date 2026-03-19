from __future__ import annotations

from fastapi import HTTPException

from .config import Settings
from .models import AgentConfig
from .provider_retry import _provider_api_key

PROVIDER_DEFAULT_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "openrouter": "https://openrouter.ai/api/v1",
    "ollama": "http://localhost:11434",
}
SUPPORTED_PROVIDERS = {"openai", "openrouter", "ollama"}


def _build_chain_agents(
    settings: Settings,
    *,
    incoming: AgentConfig | None,
    openai_compatible_only: bool = False,
) -> list[AgentConfig]:
    openai_compatible_providers = {"openai", "openrouter", "ollama"}

    # If caller explicitly sets provider/model/baseUrl, keep single-agent behavior.
    if incoming and any(
        (
            bool((incoming.provider or "").strip()),
            bool((incoming.model or "").strip()),
            bool((incoming.baseUrl or "").strip()),
        )
    ):
        merged = _merge_agent(settings, incoming)
        if openai_compatible_only and (merged.provider or "").lower() not in openai_compatible_providers:
            return []
        return [merged]

    chain: list[AgentConfig] = []
    for entry in settings.agent_chain:
        provider = str(entry.provider or "").lower()
        if openai_compatible_only and provider not in openai_compatible_providers:
            continue
        if provider not in SUPPORTED_PROVIDERS:
            continue
        chain.append(
            AgentConfig(
                provider=provider,
                model=entry.model,
                baseUrl=entry.base_url,
                apiKey=_provider_api_key(provider, settings.default_api_key),
                temperature=settings.default_temperature,
                topP=settings.default_top_p,
            )
        )

    if not chain and not openai_compatible_only:
        chain.append(_merge_agent(settings, incoming))
    return chain


def _merge_agent(settings: Settings, incoming: AgentConfig | None) -> AgentConfig:
    provider = (incoming.provider if incoming and incoming.provider else settings.default_provider).lower()
    if provider not in SUPPORTED_PROVIDERS:
        supported = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise HTTPException(status_code=400, detail=f"Unsupported provider '{provider}'. Supported: {supported}")

    model = incoming.model if incoming and incoming.model else settings.default_model
    base_url = incoming.baseUrl if incoming and incoming.baseUrl else (
        settings.default_base_url or PROVIDER_DEFAULT_BASE_URLS.get(provider, "")
    )
    api_key = incoming.apiKey if incoming and incoming.apiKey else settings.default_api_key
    temperature = incoming.temperature if incoming and incoming.temperature is not None else settings.default_temperature
    top_p = incoming.topP if incoming and incoming.topP is not None else settings.default_top_p

    return AgentConfig(
        provider=provider,
        model=model,
        baseUrl=base_url,
        apiKey=api_key,
        temperature=temperature,
        topP=top_p,
    )
