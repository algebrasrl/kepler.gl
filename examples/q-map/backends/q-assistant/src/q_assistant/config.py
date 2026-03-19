from __future__ import annotations

import os
from dataclasses import dataclass

from q_backends_shared.config_utils import parse_bool, parse_origins

SUPPORTED_PROVIDERS = {"openai", "openrouter", "ollama"}

_Q_ASSISTANT_DEFAULT_ORIGINS = ["http://localhost:8081"]


def _parse_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


@dataclass(frozen=True)
class AgentChainEntry:
    provider: str
    model: str
    base_url: str


def _parse_agent_chain(
    raw: str | None,
    *,
    default_provider: str,
    default_model: str,
    default_base_url: str,
) -> list[AgentChainEntry]:
    """
    Parse chain entries from env.
    Format:
      Q_ASSISTANT_AGENT_CHAIN=provider|model|baseUrl,provider|model|baseUrl
    baseUrl is optional per entry.
    """
    if not raw or not raw.strip():
        return [
            AgentChainEntry(
                provider=default_provider,
                model=default_model,
                base_url=default_base_url,
            )
        ]

    provider_default_base_url = {
        "openai": "https://api.openai.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
        "ollama": "http://localhost:11434",
    }

    entries: list[AgentChainEntry] = []
    for item in raw.split(","):
        token = item.strip()
        if not token:
            continue
        parts = [p.strip() for p in token.split("|")]
        if len(parts) < 2:
            continue
        provider = parts[0].lower()
        model = parts[1]
        if provider not in SUPPORTED_PROVIDERS or not model:
            continue
        base_url = (
            parts[2]
            if len(parts) >= 3 and parts[2]
            else provider_default_base_url.get(provider, default_base_url)
        )
        entries.append(
            AgentChainEntry(
                provider=provider,
                model=model,
                base_url=base_url,
            )
        )

    if not entries:
        entries.append(
            AgentChainEntry(
                provider=default_provider,
                model=default_model,
                base_url=default_base_url,
            )
        )
    return entries


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    cors_origins: list[str]
    default_provider: str
    default_model: str
    default_base_url: str
    default_api_key: str
    allow_caller_api_key_fallback: bool
    default_temperature: float | None
    default_top_p: float | None
    agent_chain: list[AgentChainEntry]
    request_timeout_seconds: float
    upstream_retry_attempts: int
    upstream_retry_base_delay_seconds: float
    upstream_retry_max_delay_seconds: float
    upstream_retry_jitter_ratio: float
    upstream_retry_timeout_increment_seconds: float
    qcumber_cloud_api_base: str
    qcumber_cloud_token: str
    qcumber_cloud_timeout_seconds: float
    qstorage_cloud_api_base: str
    qstorage_cloud_token: str
    qstorage_cloud_timeout_seconds: float
    profile_name: str
    profile_email: str
    profile_registered_at: str
    profile_country: str
    explicit_tool_routing_enabled: bool
    qmap_context_enabled: bool
    qmap_context_max_chars: int
    chat_audit_enabled: bool
    chat_audit_log_path: str
    chat_audit_max_chars: int
    chat_audit_max_list_items: int
    chat_audit_max_string_chars: int
    chat_audit_max_files: int
    chat_audit_max_age_days: int
    chat_audit_include_payloads: bool
    chat_audit_include_context: bool
    chat_audit_stdout_enabled: bool
    token_budget_enabled: bool
    token_budget_context_limit_tokens: int
    token_budget_default_context_limit_tokens: int
    token_budget_reserved_output_tokens: int
    token_budget_warn_ratio: float
    token_budget_compact_ratio: float
    token_budget_hard_ratio: float


def load_settings() -> Settings:
    default_provider = os.getenv("Q_ASSISTANT_PROVIDER", "openrouter").lower()
    if default_provider not in SUPPORTED_PROVIDERS:
        default_provider = "openrouter"

    provider_default_base_url = {
        "openai": "https://api.openai.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
        "ollama": "http://localhost:11434",
    }.get(default_provider, "https://openrouter.ai/api/v1")

    default_api_key = _first_non_empty(
        os.getenv("Q_ASSISTANT_API_KEY"),
        # Provider-specific fallback for convenience in docker/.env setups.
        os.getenv("OPENAI_API_KEY") if default_provider == "openai" else None,
        os.getenv("OPENROUTER_API_KEY") if default_provider == "openrouter" else None,
    )

    default_model = os.getenv("Q_ASSISTANT_MODEL", "").strip()
    if not default_model:
        default_model = {
            "openai": "gpt-4o-mini",
            "openrouter": "google/gemini-3-flash-preview",
            "ollama": "llama3.1",
        }.get(default_provider, "google/gemini-3-flash-preview")

    default_base_url = _first_non_empty(os.getenv("Q_ASSISTANT_BASE_URL"), provider_default_base_url)
    agent_chain = _parse_agent_chain(
        os.getenv("Q_ASSISTANT_AGENT_CHAIN"),
        default_provider=default_provider,
        default_model=default_model,
        default_base_url=default_base_url,
    )

    chat_audit_max_chars = _parse_int(os.getenv("Q_ASSISTANT_CHAT_AUDIT_MAX_CHARS"))
    chat_audit_max_list_items = _parse_int(os.getenv("Q_ASSISTANT_CHAT_AUDIT_MAX_LIST_ITEMS"))
    chat_audit_max_string_chars = _parse_int(os.getenv("Q_ASSISTANT_CHAT_AUDIT_MAX_STRING_CHARS"))
    chat_audit_max_files = _parse_int(os.getenv("Q_ASSISTANT_CHAT_AUDIT_MAX_FILES"))
    chat_audit_max_age_days = _parse_int(os.getenv("Q_ASSISTANT_CHAT_AUDIT_MAX_AGE_DAYS"))
    token_budget_context_limit_tokens = _parse_int(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT"))
    token_budget_default_context_limit_tokens = (
        _parse_int(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_DEFAULT_CONTEXT_LIMIT")) or 128000
    )
    token_budget_reserved_output_tokens = (
        _parse_int(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS")) or 4096
    )
    token_budget_warn_ratio = _parse_float(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_WARN_RATIO"))
    token_budget_compact_ratio = _parse_float(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_COMPACT_RATIO"))
    token_budget_hard_ratio = _parse_float(os.getenv("Q_ASSISTANT_TOKEN_BUDGET_HARD_RATIO"))
    warn_ratio = max(0.1, min(0.95, float(token_budget_warn_ratio or 0.6)))
    compact_ratio = max(warn_ratio, min(0.98, float(token_budget_compact_ratio or 0.75)))
    hard_ratio = max(compact_ratio, min(0.995, float(token_budget_hard_ratio or 0.94)))

    return Settings(
        host=os.getenv("Q_ASSISTANT_HOST", "0.0.0.0"),
        port=int(os.getenv("Q_ASSISTANT_PORT", "3004")),
        cors_origins=parse_origins(os.getenv("Q_ASSISTANT_CORS_ORIGINS"), default=_Q_ASSISTANT_DEFAULT_ORIGINS),
        default_provider=default_provider,
        default_model=default_model,
        default_base_url=default_base_url,
        default_api_key=default_api_key,
        allow_caller_api_key_fallback=parse_bool(
            os.getenv("Q_ASSISTANT_ALLOW_CALLER_API_KEY_FALLBACK"), default=False
        ),
        default_temperature=_parse_float(os.getenv("Q_ASSISTANT_TEMPERATURE")),
        default_top_p=_parse_float(os.getenv("Q_ASSISTANT_TOP_P")),
        agent_chain=agent_chain,
        request_timeout_seconds=float(os.getenv("Q_ASSISTANT_TIMEOUT", "45")),
        upstream_retry_attempts=_parse_int(os.getenv("Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS")) or 2,
        upstream_retry_base_delay_seconds=float(
            os.getenv("Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY", "1.0")
        ),
        upstream_retry_max_delay_seconds=float(
            os.getenv("Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY", "8")
        ),
        upstream_retry_jitter_ratio=max(
            0.0,
            min(1.0, float(os.getenv("Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO", "0.2"))),
        ),
        upstream_retry_timeout_increment_seconds=max(
            0.0, float(os.getenv("Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT", "5"))
        ),
        qcumber_cloud_api_base=os.getenv(
            "Q_ASSISTANT_QCUMBER_CLOUD_API_BASE", "http://127.0.0.1:3001"
        ).rstrip("/"),
        qcumber_cloud_token=os.getenv("Q_ASSISTANT_QCUMBER_CLOUD_TOKEN", ""),
        qcumber_cloud_timeout_seconds=float(os.getenv("Q_ASSISTANT_QCUMBER_CLOUD_TIMEOUT", "20")),
        qstorage_cloud_api_base=os.getenv(
            "Q_ASSISTANT_QSTORAGE_CLOUD_API_BASE", "http://127.0.0.1:3005"
        ).rstrip("/"),
        qstorage_cloud_token=os.getenv("Q_ASSISTANT_QSTORAGE_CLOUD_TOKEN", ""),
        qstorage_cloud_timeout_seconds=float(os.getenv("Q_ASSISTANT_QSTORAGE_CLOUD_TIMEOUT", "20")),
        profile_name=os.getenv("Q_ASSISTANT_PROFILE_NAME", "Q-hive User"),
        profile_email=os.getenv("Q_ASSISTANT_PROFILE_EMAIL", "user@q-hive.local"),
        profile_registered_at=os.getenv("Q_ASSISTANT_PROFILE_REGISTERED_AT", "2025-01-01"),
        profile_country=os.getenv("Q_ASSISTANT_PROFILE_COUNTRY", "IT"),
        explicit_tool_routing_enabled=parse_bool(
            os.getenv("Q_ASSISTANT_EXPLICIT_TOOL_ROUTING"), default=True
        ),
        qmap_context_enabled=parse_bool(
            os.getenv("Q_ASSISTANT_ENABLE_QMAP_CONTEXT"), default=True
        ),
        qmap_context_max_chars=_parse_int(os.getenv("Q_ASSISTANT_QMAP_CONTEXT_MAX_CHARS")) or 12000,
        chat_audit_enabled=parse_bool(
            os.getenv("Q_ASSISTANT_CHAT_AUDIT_ENABLED"), default=True
        ),
        chat_audit_log_path=os.getenv(
            "Q_ASSISTANT_CHAT_AUDIT_LOG_PATH", "/tmp/q-assistant-chat-audit"
        ).strip()
        or "/tmp/q-assistant-chat-audit",
        # 0 disables compact/truncate and keeps full serialized event payload.
        chat_audit_max_chars=0 if chat_audit_max_chars is None else max(0, int(chat_audit_max_chars)),
        # 0 disables list slicing during sanitization.
        chat_audit_max_list_items=0
        if chat_audit_max_list_items is None
        else max(0, int(chat_audit_max_list_items)),
        # 0 disables string truncation during sanitization.
        chat_audit_max_string_chars=0
        if chat_audit_max_string_chars is None
        else max(0, int(chat_audit_max_string_chars)),
        # 0 disables retention by file-count.
        chat_audit_max_files=500 if chat_audit_max_files is None else max(0, int(chat_audit_max_files)),
        # 0 disables retention by age.
        chat_audit_max_age_days=30
        if chat_audit_max_age_days is None
        else max(0, int(chat_audit_max_age_days)),
        chat_audit_include_payloads=parse_bool(
            os.getenv("Q_ASSISTANT_CHAT_AUDIT_INCLUDE_PAYLOADS"), default=True
        ),
        chat_audit_include_context=parse_bool(
            os.getenv("Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT"), default=False
        ),
        chat_audit_stdout_enabled=parse_bool(
            os.getenv("Q_ASSISTANT_CHAT_AUDIT_STDOUT_ENABLED"), default=False
        ),
        token_budget_enabled=parse_bool(
            os.getenv("Q_ASSISTANT_TOKEN_BUDGET_ENABLED"), default=True
        ),
        token_budget_context_limit_tokens=max(0, int(token_budget_context_limit_tokens or 0)),
        token_budget_default_context_limit_tokens=max(16000, int(token_budget_default_context_limit_tokens)),
        token_budget_reserved_output_tokens=max(256, int(token_budget_reserved_output_tokens)),
        token_budget_warn_ratio=warn_ratio,
        token_budget_compact_ratio=compact_ratio,
        token_budget_hard_ratio=hard_ratio,
    )
