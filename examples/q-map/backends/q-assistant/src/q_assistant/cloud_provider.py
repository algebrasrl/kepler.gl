from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from .config import Settings


def _normalize_cloud_provider(raw_provider: str | None) -> str:
    normalized = str(raw_provider or "").strip().lower()
    if not normalized:
        return "q-storage-backend"
    if normalized not in {"q-storage-backend", "q-cumber-backend"}:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported cloud provider '{normalized}'. "
                "Use q-storage-backend or q-cumber-backend."
            ),
        )
    return normalized


def _resolve_cloud_provider_config(settings: Settings, raw_provider: str | None) -> dict[str, Any]:
    provider = _normalize_cloud_provider(raw_provider)
    if provider == "q-cumber-backend":
        return {
            "provider": provider,
            "base": settings.qcumber_cloud_api_base,
            "token": settings.qcumber_cloud_token,
            "timeout_seconds": settings.qcumber_cloud_timeout_seconds,
            "config_error": (
                "Q-cumber cloud API not configured. "
                "Set Q_ASSISTANT_QCUMBER_CLOUD_API_BASE."
            ),
        }
    if provider == "q-storage-backend":
        return {
            "provider": provider,
            "base": settings.qstorage_cloud_api_base,
            "token": settings.qstorage_cloud_token,
            "timeout_seconds": settings.qstorage_cloud_timeout_seconds,
            "config_error": (
                "Q-storage cloud API not configured. "
                "Set Q_ASSISTANT_QSTORAGE_CLOUD_API_BASE."
            ),
        }
    raise HTTPException(status_code=500, detail=f"Unhandled cloud provider '{provider}'.")
