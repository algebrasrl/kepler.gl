from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import ProviderCatalogItem, ProviderDatasetItem


class ProviderRegistry:
    def __init__(self, providers_root: Path):
        self._providers_root = providers_root

    def list_locales(self) -> list[str]:
        if not self._providers_root.exists():
            return []
        locales = [
            entry.name.strip()
            for entry in self._providers_root.iterdir()
            if entry.is_dir() and not entry.name.startswith(".") and entry.name != "__kedro__"
        ]
        return sorted([name for name in locales if name])

    def list_providers(self, locale: str | None = None) -> list[ProviderCatalogItem]:
        entries = self._load_all(locale=locale)
        return sorted(entries, key=lambda item: (item.locale.lower(), item.name.lower()))

    def get_provider(self, provider_id: str) -> ProviderCatalogItem | None:
        needle = provider_id.strip().lower()
        if not needle:
            return None
        for item in self._load_all(locale=None):
            if item.id.lower() == needle:
                return item
        return None

    def get_provider_descriptor(self, provider_id: str) -> dict[str, Any] | None:
        needle = provider_id.strip().lower()
        if not needle:
            return None
        for file_path, _locale_name in self._iter_provider_files(locale=None):
            raw = self._read_json(file_path)
            if not isinstance(raw, dict):
                continue
            current_id = str(raw.get("id") or file_path.stem).strip().lower()
            if current_id == needle:
                return raw
        return None

    def list_provider_datasets(self, provider_id: str) -> list[ProviderDatasetItem]:
        provider = self.get_provider(provider_id)
        if not provider:
            return []

        descriptor = self.get_provider_descriptor(provider_id)
        if not descriptor:
            return []

        provider_id_value = str(descriptor.get("id") or provider_id).strip()
        datasets_raw = descriptor.get("datasets")
        if not isinstance(datasets_raw, list):
            return []

        out: list[ProviderDatasetItem] = []
        for index, raw in enumerate(datasets_raw):
            if not isinstance(raw, dict):
                continue
            dataset_id = str(raw.get("id") or f"dataset-{index + 1}").strip()
            if not dataset_id:
                continue
            name = str(raw.get("name") or dataset_id).strip()
            if not name:
                continue
            resolved_url: str | None = None
            url = raw.get("url")
            if isinstance(url, str) and url.strip():
                resolved_url = url.strip()
            tags = raw.get("tags")
            out.append(
                ProviderDatasetItem(
                    id=dataset_id,
                    providerId=provider_id_value,
                    name=name,
                    description=str(raw.get("description")) if raw.get("description") else None,
                    url=resolved_url,
                    source=raw.get("source") if isinstance(raw.get("source"), dict) else None,
                    format=str(raw.get("format")) if raw.get("format") else None,
                    tags=[str(tag).strip() for tag in tags if str(tag).strip()]
                    if isinstance(tags, list)
                    else [],
                    ai=raw.get("ai") if isinstance(raw.get("ai"), dict) else None,
                )
            )
        return out

    def _load_all(self, locale: str | None = None) -> list[ProviderCatalogItem]:
        if not self._providers_root.exists():
            return []
        out: list[ProviderCatalogItem] = []
        for file_path, locale_name in self._iter_provider_files(locale=locale):
            item = self._load_file(file_path, locale_name)
            if item:
                out.append(item)
        return out

    def _iter_provider_files(self, locale: str | None = None) -> list[tuple[Path, str]]:
        if not self._providers_root.exists():
            return []
        locales = [locale] if locale else self.list_locales()
        out: list[tuple[Path, str]] = []
        for locale_name in locales:
            locale_dir = self._providers_root / locale_name
            if not locale_dir.exists() or not locale_dir.is_dir():
                continue
            for file_path in sorted(locale_dir.glob("*.json")):
                out.append((file_path, locale_name))
        return out

    def _load_file(self, file_path: Path, locale_name: str) -> ProviderCatalogItem | None:
        raw = self._read_json(file_path)
        if not isinstance(raw, dict):
            return None

        provider_id = str(raw.get("id") or file_path.stem).strip()
        name = str(raw.get("name") or provider_id).strip()
        if not provider_id or not name:
            return None

        api = raw.get("api") if isinstance(raw.get("api"), dict) else {}

        def _as_list(value: Any) -> list[str]:
            if not isinstance(value, list):
                return []
            return [str(v).strip() for v in value if str(v).strip()]

        return ProviderCatalogItem(
            id=provider_id,
            name=name,
            locale=str(raw.get("locale") or locale_name),
            category=str(raw.get("category") or "environmental"),
            organizationType=str(raw.get("organizationType") or "agency"),
            region=str(raw.get("region")) if raw.get("region") else None,
            country=str(raw.get("country")) if raw.get("country") else None,
            portalUrl=str(raw.get("portalUrl")) if raw.get("portalUrl") else None,
            apiType=str(api.get("type")) if api.get("type") else None,
            apiBaseUrl=str(api.get("baseUrl")) if api.get("baseUrl") else None,
            capabilities=_as_list(raw.get("capabilities")),
            formats=_as_list(raw.get("formats")),
            tags=_as_list(raw.get("tags")),
            notes=str(raw.get("notes")) if raw.get("notes") else None,
        )

    def _read_json(self, file_path: Path) -> Any:
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return None
