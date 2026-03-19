"""FastAPI routes that proxy q-cumber requests.

Mounted on the q-assistant app so the frontend calls q-assistant,
which forwards to q-cumber server-side.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from .qcumber_client import QCumberClient

logger = logging.getLogger("q_assistant.qcumber_proxy")

router = APIRouter(prefix="/qcumber", tags=["qcumber-proxy"])

# Singleton client — set by mount_qcumber_proxy()
_client: QCumberClient | None = None


def mount_qcumber_proxy(app: Any, *, base_url: str, token: str, timeout: float) -> None:
    """Call once at startup to wire the router into the FastAPI app."""
    global _client
    _client = QCumberClient(base_url=base_url, token=token, timeout_seconds=timeout)
    app.include_router(router)
    logger.info("q-cumber proxy mounted → %s", base_url)


def _get_client() -> QCumberClient:
    if _client is None:
        raise HTTPException(503, "q-cumber proxy not configured")
    return _client


# ─── Routes ───────────────────────────────────────────────────────────────────


def _caller_token(request: Request) -> str:
    """Extract bearer token from the incoming request, if present."""
    auth = (request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


@router.get("/providers")
async def list_providers(request: Request, flat: bool = False):
    try:
        return await _get_client().list_providers(flat=flat, caller_token=_caller_token(request))
    except Exception as exc:
        logger.warning("q-cumber /providers failed: %s", exc)
        raise HTTPException(502, f"q-cumber upstream error: {exc}") from exc


@router.get("/providers/{provider_id}/datasets")
async def list_datasets(request: Request, provider_id: str):
    try:
        return await _get_client().list_datasets(provider_id, caller_token=_caller_token(request))
    except Exception as exc:
        logger.warning("q-cumber /datasets failed: %s", exc)
        raise HTTPException(502, f"q-cumber upstream error: {exc}") from exc


@router.get("/providers/{provider_id}/datasets/{dataset_id}")
async def get_dataset_help(request: Request, provider_id: str, dataset_id: str):
    try:
        return await _get_client().get_dataset_help(provider_id, dataset_id, caller_token=_caller_token(request))
    except Exception as exc:
        logger.warning("q-cumber /dataset-help failed: %s", exc)
        raise HTTPException(502, f"q-cumber upstream error: {exc}") from exc


class QueryBody(BaseModel):
    class Config:
        extra = "allow"


@router.post("/query")
async def query_dataset(request: Request, body: QueryBody):
    try:
        return await _get_client().query(body.model_dump(), caller_token=_caller_token(request))
    except Exception as exc:
        logger.warning("q-cumber /query failed: %s", exc)
        raise HTTPException(502, f"q-cumber upstream error: {exc}") from exc
