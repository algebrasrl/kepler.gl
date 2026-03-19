from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


class DatasetAdapterRegistry:
    """Dataset-specific normalization/enrichment hooks outside of query core."""

    def __init__(self, *, data_dir: Path) -> None:
        self._clc = _Clc2018Adapter(data_dir=data_dir)
        self._kontur = _KonturBoundariesAdapter()

    def get_virtual_fields(self, *, table: str) -> set[str]:
        return self._clc.virtual_fields(table=table)

    def normalize_filters(
        self,
        *,
        table: str,
        filters: list[Any] | None,
        filter_get: Callable[[Any, str, Any], Any],
    ) -> list[Any]:
        normalized = self._clc.normalize_filters(table=table, filters=filters, filter_get=filter_get)
        return self._kontur.normalize_filters(table=table, filters=normalized, filter_get=filter_get)

    def adjust_select_columns(
        self,
        *,
        table: str,
        requested_select: list[str],
        selected_non_geom: list[str],
        non_geom_set: set[str],
    ) -> list[str]:
        return self._clc.adjust_select_columns(
            table=table,
            requested_select=requested_select,
            selected_non_geom=selected_non_geom,
            non_geom_set=non_geom_set,
        )

    def enrich_rows(self, *, table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._clc.enrich_rows(table=table, rows=rows)

    def field_hint_overrides(self, *, table: str) -> dict[str, dict[str, Any]]:
        return self._clc.field_hint_overrides(table=table)

    def ai_profile_overrides(self, *, table: str) -> dict[str, Any]:
        return self._clc.ai_profile_overrides(table=table)


class _Clc2018Adapter:
    _TARGET_TABLE = "clc_2018"
    _VIRTUAL_FIELDS = {"clc_name_it", "clc_name_en"}

    def __init__(self, *, data_dir: Path) -> None:
        self._mapping_path = data_dir / "reference" / "clc_code_18_labels.json"
        self._mapping_cache: dict[str, dict[str, str]] | None = None

    def _is_target(self, *, table: str) -> bool:
        return str(table or "").strip().lower() == self._TARGET_TABLE

    def _load_mapping(self) -> dict[str, dict[str, str]]:
        if isinstance(self._mapping_cache, dict):
            return self._mapping_cache
        if not self._mapping_path.exists() or not self._mapping_path.is_file():
            self._mapping_cache = {}
            return self._mapping_cache
        try:
            parsed = json.loads(self._mapping_path.read_text(encoding="utf-8"))
        except Exception:
            self._mapping_cache = {}
            return self._mapping_cache

        out: dict[str, dict[str, str]] = {}
        if isinstance(parsed, dict):
            for raw_code, raw_item in parsed.items():
                code = str(raw_code).strip()
                if not code or not isinstance(raw_item, dict):
                    continue
                name_en = str(raw_item.get("name_en") or "").strip()
                name_it = str(raw_item.get("name_it") or "").strip()
                if not name_en and not name_it:
                    continue
                out[code] = {"name_en": name_en, "name_it": name_it}
        self._mapping_cache = out
        return self._mapping_cache

    def _sorted_codes(self) -> list[str]:
        mapping = self._load_mapping()
        return sorted(mapping.keys(), key=lambda value: (len(value), value))

    def _level_values(self, codes: list[str], width: int) -> list[str]:
        out = sorted({code[:width] for code in codes if len(code) >= width and code[:width].isdigit()})
        return out

    def virtual_fields(self, *, table: str) -> set[str]:
        if not self._is_target(table=table):
            return set()
        return set(self._VIRTUAL_FIELDS) if self._load_mapping() else set()

    def _match_codes(
        self,
        *,
        filter_op: str,
        filter_value: Any,
        filter_values: Any,
        lang_key: str,
        mapping: dict[str, dict[str, str]],
    ) -> list[str]:
        op = str(filter_op or "eq").strip().lower()
        labels_by_code: dict[str, str] = {
            code: str(item.get(lang_key) or "").strip()
            for code, item in mapping.items()
            if isinstance(item, dict)
        }
        labels_by_code = {code: label for code, label in labels_by_code.items() if label}
        if not labels_by_code:
            return []

        def _normalize(text: Any) -> str:
            return str(text or "").strip().lower()

        if op == "eq":
            needle = _normalize(filter_value)
            return [code for code, label in labels_by_code.items() if _normalize(label) == needle]
        if op == "contains":
            needle = _normalize(filter_value)
            return [code for code, label in labels_by_code.items() if needle in _normalize(label)]
        if op == "startswith":
            needle = _normalize(filter_value)
            return [code for code, label in labels_by_code.items() if _normalize(label).startswith(needle)]
        if op == "endswith":
            needle = _normalize(filter_value)
            return [code for code, label in labels_by_code.items() if _normalize(label).endswith(needle)]
        if op == "in":
            raw_values = filter_values if isinstance(filter_values, list) else [filter_value]
            needles = {_normalize(item) for item in raw_values if _normalize(item)}
            return [code for code, label in labels_by_code.items() if _normalize(label) in needles]
        return []

    def normalize_filters(
        self,
        *,
        table: str,
        filters: list[Any] | None,
        filter_get: Callable[[Any, str, Any], Any],
    ) -> list[Any]:
        if not self._is_target(table=table):
            return list(filters or [])

        mapping = self._load_mapping()
        if not mapping:
            return list(filters or [])

        normalized: list[Any] = []
        for filter_item in (filters or []):
            field = str(filter_get(filter_item, "field", "") or "").strip()
            op = str(filter_get(filter_item, "op", "eq") or "eq").strip().lower()
            if field in self._VIRTUAL_FIELDS:
                lang_key = "name_it" if field == "clc_name_it" else "name_en"
                codes = self._match_codes(
                    filter_op=op,
                    filter_value=filter_get(filter_item, "value", None),
                    filter_values=filter_get(filter_item, "values", None),
                    lang_key=lang_key,
                    mapping=mapping,
                )
                normalized.append({"field": "code_18", "op": "in", "values": codes})
                continue
            if field.lower() == "code_18":
                canonical_field = "code_18"
                if op == "in":
                    raw_values = filter_get(filter_item, "values", None)
                    if not isinstance(raw_values, list):
                        raw_values = [filter_get(filter_item, "value", None)]
                    normalized.append(
                        {
                            "field": canonical_field,
                            "op": op,
                            "values": [str(item).strip() for item in raw_values if item is not None],
                        }
                    )
                else:
                    raw_value = filter_get(filter_item, "value", None)
                    normalized.append(
                        {
                            "field": canonical_field,
                            "op": op,
                            "value": None if raw_value is None else str(raw_value).strip(),
                        }
                    )
                continue
            normalized.append(filter_item)
        return normalized

    def adjust_select_columns(
        self,
        *,
        table: str,
        requested_select: list[str],
        selected_non_geom: list[str],
        non_geom_set: set[str],
    ) -> list[str]:
        if not self._is_target(table=table):
            return selected_non_geom
        if "code_18" not in non_geom_set:
            return selected_non_geom
        if not any(field in self._VIRTUAL_FIELDS for field in requested_select):
            return selected_non_geom
        if "code_18" in selected_non_geom:
            return selected_non_geom
        return [*selected_non_geom, "code_18"]

    def enrich_rows(self, *, table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows or not self._is_target(table=table):
            return rows
        mapping = self._load_mapping()
        if not mapping:
            return rows

        first = next((row for row in rows if isinstance(row, dict) and row), None)
        if not first:
            return rows
        code_field = next(
            (field for field in first.keys() if isinstance(field, str) and field.strip().lower() == "code_18"),
            None,
        )
        if not code_field:
            return rows

        for row in rows:
            raw_code = row.get(code_field)
            if raw_code is None:
                continue
            code = str(raw_code).strip()
            if not code:
                continue
            item = mapping.get(code)
            if not item:
                continue
            name_en = item.get("name_en")
            name_it = item.get("name_it")
            if name_en:
                row.setdefault("clc_name_en", name_en)
            if name_it:
                row.setdefault("clc_name_it", name_it)
        return rows

    def field_hint_overrides(self, *, table: str) -> dict[str, dict[str, Any]]:
        if not self._is_target(table=table):
            return {}
        codes = self._sorted_codes()
        if not codes:
            return {}
        return {
            "code_18": {
                "enumValues": codes,
                "examples": codes[: min(20, len(codes))],
                "example": codes[0],
            }
        }

    def ai_profile_overrides(self, *, table: str) -> dict[str, Any]:
        if not self._is_target(table=table):
            return {}
        codes = self._sorted_codes()
        if not codes:
            return {}

        level_1 = self._level_values(codes, 1)
        level_2 = self._level_values(codes, 2)
        level_3 = [code for code in codes if len(code) == 3 and code.isdigit()]

        return {
            "thematicCodeHierarchy": {
                "codeField": "code_18",
                "levels": [
                    {"level": 1, "width": 1, "values": level_1},
                    {"level": 2, "width": 2, "values": level_2},
                    {"level": 3, "width": 3, "values": level_3},
                ],
                "allCodes": level_3,
            }
        }


class _KonturBoundariesAdapter:
    _TARGET_TABLE = "kontur_boundaries"
    _FIELD_ALIASES = {
        "level": "lv",
        "livello": "lv",
    }

    def _is_target(self, *, table: str) -> bool:
        return str(table or "").strip().lower() == self._TARGET_TABLE

    def normalize_filters(
        self,
        *,
        table: str,
        filters: list[Any] | None,
        filter_get: Callable[[Any, str, Any], Any],
    ) -> list[Any]:
        if not self._is_target(table=table):
            return list(filters or [])

        normalized: list[Any] = []
        for filter_item in (filters or []):
            field = str(filter_get(filter_item, "field", "") or "").strip()
            mapped_field = self._FIELD_ALIASES.get(field.lower(), field)

            if isinstance(filter_item, dict):
                next_item = dict(filter_item)
                next_item["field"] = mapped_field
                normalized.append(next_item)
            else:
                normalized.append(
                    {
                        "field": mapped_field,
                        "op": str(filter_get(filter_item, "op", "eq") or "eq").strip().lower(),
                        "value": filter_get(filter_item, "value", None),
                        "values": filter_get(filter_item, "values", None),
                    }
                )
        return normalized
