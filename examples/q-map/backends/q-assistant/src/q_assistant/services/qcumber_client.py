"""HTTP client for q-cumber REST API.

Proxies q-cumber requests from the frontend through q-assistant so that:
- q-cumber auth tokens are not exposed to the browser
- Network round-trips are reduced (backend-to-backend is faster)
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger("q_assistant.qcumber_client")


class QCumberClient:
    """Thin async HTTP wrapper around the q-cumber REST API."""

    def __init__(
        self,
        base_url: str,
        token: str = "",
        timeout_seconds: float = 20.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._token = token.strip()
        self._timeout = timeout_seconds

    def _headers(self, caller_token: str = "") -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        token = caller_token or self._token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _get(self, path: str, *, caller_token: str = "") -> Any:
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(url, headers=self._headers(caller_token))
            resp.raise_for_status()
            return resp.json()

    async def _post(self, path: str, body: dict[str, Any], *, caller_token: str = "") -> Any:
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(url, json=body, headers=self._headers(caller_token))
            resp.raise_for_status()
            return resp.json()

    # ─── Public API ───────────────────────────────────────────────────────

    async def list_providers(self, flat: bool = False, *, caller_token: str = "") -> Any:
        suffix = "?flat=true" if flat else ""
        return await self._get(f"/providers{suffix}", caller_token=caller_token)

    async def list_datasets(self, provider_id: str, *, caller_token: str = "") -> Any:
        return await self._get(
            f"/providers/{httpx.URL(provider_id).raw_path.decode() if '/' in provider_id else provider_id}/datasets",
            caller_token=caller_token,
        )

    async def get_dataset_help(
        self, provider_id: str, dataset_id: str, *, caller_token: str = ""
    ) -> Any:
        return await self._get(
            f"/providers/{provider_id}/datasets/{dataset_id}/help",
            caller_token=caller_token,
        )

    async def query(self, body: dict[str, Any], *, caller_token: str = "") -> Any:
        return await self._post("/datasets/query", body, caller_token=caller_token)
