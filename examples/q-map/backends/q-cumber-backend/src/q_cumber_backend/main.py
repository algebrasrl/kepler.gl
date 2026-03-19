from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import re
from typing import Any
import unicodedata

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import psycopg_pool
import uvicorn

from .config import Settings, load_settings
from .dataset_adapters import DatasetAdapterRegistry
from .jwt_auth import JwtValidationError, decode_and_validate_jwt, extract_roles
from .sources import CKANSource, PostGISSource
from .sources.base import DataSource, SourceResult
from .models import (
    CloudUser,
    DatasetHelpResponse,
    DatasetQueryRequest,
    DatasetQueryResponse,
    DownloadMapResponse,
    KeplerDatasetData,
    KeplerDatasetField,
    KeplerDatasetInfo,
    KeplerDatasetPayload,
    MapListResponse,
    ProviderCatalogItem,
    ProviderDatasetItem,
    ProviderDatasetListResponse,
    ProviderListResponse,
    ProviderLocalesResponse,
)
from .provider_registry import ProviderRegistry
from .storage import MapStore


POSTGIS_SOURCE_TYPES = {"postgis", "postgres", "postgresql"}
CKAN_SOURCE_TYPES = {"ckan"}
ALL_SOURCE_TYPES = POSTGIS_SOURCE_TYPES | CKAN_SOURCE_TYPES
DEFAULT_FILTER_OPS_BY_TYPE = {
    "number": ["eq", "ne", "gt", "gte", "lt", "lte", "in", "is_null", "not_null"],
    "boolean": ["eq", "ne", "in", "is_null", "not_null"],
    "geojson": ["is_null", "not_null"],
    "string": ["eq", "ne", "in", "contains", "startswith", "endswith", "is_null", "not_null"],
}
DEFAULT_SUPPORTED_FILTER_OPS = [
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "not_null",
]


@dataclass(frozen=True)
class AuthContext:
    name: str
    email: str
    roles: tuple[str, ...] = ()
    subject: str = ""


def _build_postgis_conninfo(s: Settings) -> str:
    if s.postgis_dsn:
        return s.postgis_dsn
    return make_conninfo(
        host=s.postgis_host,
        port=s.postgis_port,
        dbname=s.postgis_db,
        user=s.postgis_user,
        password=s.postgis_password,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or load_settings()
    store = MapStore(app_settings.data_dir)
    provider_registry = ProviderRegistry(providers_root=app_settings.providers_dir)
    auth_scheme = HTTPBearer(auto_error=False)

    dataset_hints_cache: dict[str, dict[str, Any]] = {}
    dataset_hints_cache_ttl_seconds = max(60, int(app_settings.ai_hints_cache_ttl_seconds))
    dataset_adapters = DatasetAdapterRegistry(data_dir=app_settings.data_dir)

    _conninfo = _build_postgis_conninfo(app_settings)
    db_pool = psycopg_pool.ConnectionPool(
        conninfo=_conninfo,
        min_size=app_settings.postgis_pool_min_size,
        max_size=app_settings.postgis_pool_max_size,
        kwargs={"row_factory": dict_row},
        open=False,
    )

    # -- Source backends ----------------------------------------------------
    postgis_source = PostGISSource(db_pool)
    ckan_source = CKANSource(
        timeout=30.0,
        default_api_key=app_settings.ckan_api_key,
    )

    def _get_source(source_type: str) -> DataSource:
        if source_type in POSTGIS_SOURCE_TYPES:
            return postgis_source
        if source_type in CKAN_SOURCE_TYPES:
            return ckan_source
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported source.type '{source_type}'. Supported: {', '.join(sorted(ALL_SOURCE_TYPES))}",
        )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        db_pool.open(wait=True)
        try:
            yield
        finally:
            db_pool.close()

    app = FastAPI(title="q-map Q-cumber cloud backend", version="0.2.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def _dataset_cache_key(provider_id: str, dataset_id: str) -> str:
        return f"{provider_id.strip().lower()}::{dataset_id.strip().lower()}"

    def _is_number_like(text: str) -> bool:
        candidate = text.strip().replace(",", ".")
        if not candidate:
            return False
        try:
            float(candidate)
            return True
        except Exception:
            return False

    def _infer_column_profile(column_values: list[Any]) -> dict[str, Any]:
        def _compact_sample_value(value: Any) -> Any:
            if isinstance(value, dict):
                if isinstance(value.get("type"), str) and value.get("coordinates") is not None:
                    return {
                        "type": str(value.get("type") or "Geometry"),
                        "summary": "[geojson omitted]",
                    }
                return "[object omitted]"
            if isinstance(value, list):
                return "[array omitted]"
            if isinstance(value, str):
                stripped = value.strip()
                if len(stripped) > 160:
                    return f"{stripped[:157]}..."
                return stripped
            return value

        non_null = [value for value in column_values if value is not None]
        if not non_null:
            return {"type": "string", "nullRatio": 1.0}

        numeric_count = 0
        boolean_count = 0
        geojson_count = 0

        for value in non_null:
            if isinstance(value, bool):
                boolean_count += 1
                continue
            if isinstance(value, (int, float)):
                numeric_count += 1
                continue
            if isinstance(value, dict) and isinstance(value.get("type"), str) and value.get("coordinates") is not None:
                geojson_count += 1
                continue
            if isinstance(value, str) and _is_number_like(value):
                numeric_count += 1

        total = len(non_null)
        inferred = "string"
        if geojson_count / total >= 0.8:
            inferred = "geojson"
        elif numeric_count / total >= 0.8:
            inferred = "number"
        elif boolean_count / total >= 0.8:
            inferred = "boolean"

        return {
            "type": inferred,
            "nullRatio": round((len(column_values) - total) / max(1, len(column_values)), 4),
            "sampleValues": [_compact_sample_value(non_null[idx]) for idx in range(min(3, len(non_null)))],
        }

    def _infer_dataset_hints_profile(dataset_name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        fields = sorted({key for row in rows for key in row.keys() if isinstance(key, str) and key})
        column_profiles: dict[str, Any] = {}
        for field in fields:
            values = [row.get(field) for row in rows]
            column_profiles[field] = _infer_column_profile(values)

        geometry_fields = [
            field for field, profile in column_profiles.items() if profile.get("type") == "geojson"
        ]
        numeric_fields = [
            field for field, profile in column_profiles.items() if profile.get("type") == "number"
        ]

        return {
            "datasetName": dataset_name,
            "rowCount": len(rows),
            "fieldCount": len(fields),
            "fields": fields,
            "columnProfiles": column_profiles,
            "geometryFields": geometry_fields,
            "numericFields": numeric_fields,
            "suggestedOps": [
                op
                for op in [
                    "filter",
                    "aggregate",
                    "spatial_join" if geometry_fields else None,
                    "thematic_styling" if numeric_fields else None,
                ]
                if op
            ],
        }

    def _descriptor_field_hints(dataset_item: ProviderDatasetItem) -> dict[str, dict[str, Any]]:
        source = dataset_item.source if isinstance(dataset_item.source, dict) else {}
        table_name = str(source.get("table") or "").strip()
        ai_block = dataset_item.ai if isinstance(dataset_item.ai, dict) else {}
        raw_field_hints = ai_block.get("fieldHints")
        if not isinstance(raw_field_hints, dict):
            raw_field_hints = {}

        normalized: dict[str, dict[str, Any]] = {}
        for raw_field, raw_hint in raw_field_hints.items():
            field_name = str(raw_field or "").strip()
            if not field_name or not isinstance(raw_hint, dict):
                continue
            hint: dict[str, Any] = {}
            for key in [
                "description",
                "unit",
                "semanticRole",
                "type",
                "virtual",
                "sortable",
                "rankable",
                "adminLevel",
                "example",
                "examples",
                "enumValues",
            ]:
                if key in raw_hint:
                    hint[key] = raw_hint[key]
            aliases = raw_hint.get("aliases")
            if isinstance(aliases, list):
                hint["aliases"] = [str(alias).strip() for alias in aliases if str(alias).strip()]
            filter_ops = raw_hint.get("filterOps")
            if isinstance(filter_ops, list):
                hint["filterOps"] = [str(op).strip().lower() for op in filter_ops if str(op).strip()]
            normalized[field_name] = hint

        dynamic_hints = dataset_adapters.field_hint_overrides(table=table_name)
        if isinstance(dynamic_hints, dict):
            for raw_field, raw_hint in dynamic_hints.items():
                field_name = str(raw_field or "").strip()
                if not field_name or not isinstance(raw_hint, dict):
                    continue
                current = normalized.get(field_name, {})
                merged = {**current}
                for key in ["description", "unit", "semanticRole", "type", "virtual", "sortable", "rankable", "adminLevel", "example", "examples", "enumValues", "aliases", "filterOps"]:
                    if key in raw_hint:
                        merged[key] = raw_hint[key]
                normalized[field_name] = merged
        return normalized

    def _sanitize_ai_profile(value: Any, *, depth: int = 0) -> Any:
        if depth > 6:
            return None
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            out_list = []
            for item in value[:100]:
                sanitized_item = _sanitize_ai_profile(item, depth=depth + 1)
                if sanitized_item is not None:
                    out_list.append(sanitized_item)
            return out_list
        if isinstance(value, dict):
            out_dict: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key = str(raw_key or "").strip()
                if not key:
                    continue
                sanitized_value = _sanitize_ai_profile(raw_value, depth=depth + 1)
                if sanitized_value is not None:
                    out_dict[key] = sanitized_value
            return out_dict
        return str(value)

    def _descriptor_ai_profile(dataset_item: ProviderDatasetItem) -> dict[str, Any] | None:
        source = dataset_item.source if isinstance(dataset_item.source, dict) else {}
        table_name = str(source.get("table") or "").strip()
        ai_block = dataset_item.ai if isinstance(dataset_item.ai, dict) else {}
        raw_profile = ai_block.get("profile")
        base_profile = raw_profile if isinstance(raw_profile, dict) else {}
        dynamic_profile = dataset_adapters.ai_profile_overrides(table=table_name)
        merged_profile: dict[str, Any] = {}
        if isinstance(base_profile, dict):
            merged_profile.update(base_profile)
        if isinstance(dynamic_profile, dict):
            for key, value in dynamic_profile.items():
                if key in merged_profile and isinstance(merged_profile.get(key), dict) and isinstance(value, dict):
                    merged_profile[key] = {**merged_profile[key], **value}
                else:
                    merged_profile[key] = value
        sanitized = _sanitize_ai_profile(merged_profile)
        return sanitized if isinstance(sanitized, dict) and sanitized else None

    def _build_field_catalog(
        dataset_profile: dict[str, Any] | None,
        dataset_item: ProviderDatasetItem,
    ) -> list[dict[str, Any]]:
        descriptor_hints = _descriptor_field_hints(dataset_item)
        profile_fields = _field_names_from_dataset_profile(dataset_profile)
        field_order = [*profile_fields, *[field for field in descriptor_hints.keys() if field not in profile_fields]]
        column_profiles = dataset_profile.get("columnProfiles", {}) if isinstance(dataset_profile, dict) else {}
        out: list[dict[str, Any]] = []

        for field_name in field_order:
            descriptor_hint = descriptor_hints.get(field_name, {})
            profile_hint = column_profiles.get(field_name, {}) if isinstance(column_profiles, dict) else {}
            field_type = (
                str(descriptor_hint.get("type") or "").strip().lower()
                or str(profile_hint.get("type") or "").strip().lower()
                or "string"
            )
            if field_type not in DEFAULT_FILTER_OPS_BY_TYPE:
                field_type = "string"
            filter_ops = descriptor_hint.get("filterOps")
            if not isinstance(filter_ops, list) or not filter_ops:
                filter_ops = DEFAULT_FILTER_OPS_BY_TYPE.get(field_type, DEFAULT_FILTER_OPS_BY_TYPE["string"])
            sortable_default = field_type != "geojson"
            rankable_default = field_type in {"number", "boolean", "string"}
            item: dict[str, Any] = {
                "name": field_name,
                "type": field_type,
                "filterOps": filter_ops,
                "sortable": bool(descriptor_hint.get("sortable", sortable_default)),
                "rankable": bool(descriptor_hint.get("rankable", rankable_default)),
            }
            if "description" in descriptor_hint:
                item["description"] = descriptor_hint["description"]
            if "unit" in descriptor_hint:
                item["unit"] = descriptor_hint["unit"]
            if "semanticRole" in descriptor_hint:
                item["semanticRole"] = descriptor_hint["semanticRole"]
            if "adminLevel" in descriptor_hint:
                item["adminLevel"] = descriptor_hint["adminLevel"]
            if "virtual" in descriptor_hint:
                item["virtual"] = bool(descriptor_hint["virtual"])
            if "aliases" in descriptor_hint:
                item["aliases"] = descriptor_hint["aliases"]
            if "example" in descriptor_hint:
                item["example"] = descriptor_hint["example"]
            if "examples" in descriptor_hint:
                item["examples"] = descriptor_hint["examples"]
            if "enumValues" in descriptor_hint:
                item["enumValues"] = descriptor_hint["enumValues"]
            if isinstance(profile_hint, dict):
                if "nullRatio" in profile_hint:
                    item["nullRatio"] = profile_hint["nullRatio"]
                sample_values = profile_hint.get("sampleValues")
                if isinstance(sample_values, list):
                    item["sampleValues"] = sample_values
            out.append(item)
        return out

    def _build_ai_hints_from_dataset_profile(
        dataset_profile: dict[str, Any] | None,
        dataset_item: ProviderDatasetItem,
    ) -> dict[str, Any] | None:
        field_catalog = _build_field_catalog(dataset_profile, dataset_item)
        ai_profile = _descriptor_ai_profile(dataset_item)
        if not isinstance(dataset_profile, dict) and not field_catalog and not ai_profile:
            return None

        geometry_fields = dataset_profile.get("geometryFields", []) if isinstance(dataset_profile, dict) else []
        numeric_fields = dataset_profile.get("numericFields", []) if isinstance(dataset_profile, dict) else []
        if not isinstance(geometry_fields, list):
            geometry_fields = []
        if not isinstance(numeric_fields, list):
            numeric_fields = []
        if field_catalog:
            if not geometry_fields:
                geometry_fields = [
                    str(field.get("name"))
                    for field in field_catalog
                    if str(field.get("type") or "").strip().lower() == "geojson"
                ]
            if not numeric_fields:
                numeric_fields = [
                    str(field.get("name"))
                    for field in field_catalog
                    if str(field.get("type") or "").strip().lower() == "number"
                ]

        def _is_identifier_like_field(name: str) -> bool:
            lowered = str(name or "").strip().lower()
            if not lowered:
                return False
            if lowered in {"id", "gid", "fid", "pk", "uuid"}:
                return True
            if lowered.endswith("_id") or "__id" in lowered:
                return True
            return "hasc" in lowered

        scored_order_candidates: list[tuple[int, int, str]] = []
        seen_order_names: set[str] = set()
        for idx, field in enumerate(field_catalog):
            name = str(field.get("name") or "").strip()
            if not name:
                continue
            lowered_name = name.lower()
            if lowered_name in seen_order_names:
                continue
            field_type = str(field.get("type") or "").strip().lower()
            if not field.get("sortable") or field_type == "geojson":
                continue

            semantic_role = str(field.get("semanticRole") or "").strip().lower()
            score = 0
            if field_type in {"number", "integer", "real"}:
                score += 40
            if field.get("rankable"):
                score += 10
            if semantic_role in {"population", "area", "metric", "value", "density"}:
                score += 45
            elif semantic_role in {"name"}:
                score += 5
            if _is_identifier_like_field(name):
                score -= 50
            if semantic_role in {"id", "admin_parent_id"}:
                score -= 40
            if field_type == "string":
                score -= 5

            seen_order_names.add(lowered_name)
            scored_order_candidates.append((score, idx, name))

        order_by_candidates = [name for _score, _idx, name in sorted(scored_order_candidates, key=lambda item: (-item[0], item[1]))]
        dataset_ops = dataset_profile.get("suggestedOps", []) if isinstance(dataset_profile, dict) else []
        if not isinstance(dataset_ops, list):
            dataset_ops = []
        if not dataset_ops:
            dataset_ops = [
                op
                for op in [
                    "filter",
                    "aggregate",
                    "spatial_join" if geometry_fields else None,
                    "thematic_styling" if numeric_fields else None,
                ]
                if op
            ]

        return {
            "rowCount": dataset_profile.get("rowCount", 0) if isinstance(dataset_profile, dict) else 0,
            "geometryFields": geometry_fields,
            "numericFields": numeric_fields,
            "suggestedOps": dataset_ops,
            "supportedFilterOps": DEFAULT_SUPPORTED_FILTER_OPS,
            "orderByCandidates": order_by_candidates,
            "fieldCatalog": field_catalog,
            "aiProfile": ai_profile,
        }

    def _normalize_text_token(value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        normalized = unicodedata.normalize("NFD", raw)
        normalized = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
        return re.sub(r"[^a-z0-9]+", " ", normalized).strip()

    def _field_names_from_dataset_profile(dataset_profile: dict[str, Any] | None) -> list[str]:
        if not isinstance(dataset_profile, dict):
            return []
        raw_fields = dataset_profile.get("fields")
        if not isinstance(raw_fields, list):
            return []
        return [str(field).strip() for field in raw_fields if str(field).strip()]

    def _as_string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def _build_dataset_metric_profile(
        dataset_item: ProviderDatasetItem,
        *,
        dataset_class: str,
        has_admin_signals: bool,
        fields: list[str],
        ai_hints: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        ai_profile = (
            ai_hints.get("aiProfile") if isinstance(ai_hints, dict) and isinstance(ai_hints.get("aiProfile"), dict) else {}
        )
        declared = ai_profile.get("analysisMetrics") if isinstance(ai_profile.get("analysisMetrics"), dict) else {}
        if declared:
            profile = dict(declared)
            profile.setdefault("source", "descriptor_profile")
            profile.setdefault("confidence", "high")
            if isinstance(profile.get("recommendedDerivedMetrics"), list):
                profile["recommendedDerivedMetrics"] = [
                    item for item in profile.get("recommendedDerivedMetrics", []) if isinstance(item, dict)
                ]
            else:
                profile["recommendedDerivedMetrics"] = []
            if isinstance(profile.get("analysisCaveats"), list):
                profile["analysisCaveats"] = [
                    str(item).strip() for item in profile.get("analysisCaveats", []) if str(item).strip()
                ]
            else:
                profile["analysisCaveats"] = []
            if isinstance(profile.get("numeratorFieldCandidates"), list):
                profile["numeratorFieldCandidates"] = [
                    str(item).strip() for item in profile.get("numeratorFieldCandidates", []) if str(item).strip()
                ]
            else:
                profile["numeratorFieldCandidates"] = []
            if isinstance(profile.get("denominatorFieldCandidates"), list):
                profile["denominatorFieldCandidates"] = [
                    str(item).strip() for item in profile.get("denominatorFieldCandidates", []) if str(item).strip()
                ]
            else:
                profile["denominatorFieldCandidates"] = []
            if isinstance(profile.get("preferredRankingFieldCandidates"), list):
                profile["preferredRankingFieldCandidates"] = [
                    str(item).strip() for item in profile.get("preferredRankingFieldCandidates", []) if str(item).strip()
                ]
            else:
                profile["preferredRankingFieldCandidates"] = []
            return profile

        lower_fields = {str(field).strip().lower() for field in (fields or []) if str(field).strip()}
        text = (
            f"{dataset_item.id} {dataset_item.name} {dataset_item.description or ''} "
            + " ".join(str(tag or "") for tag in (dataset_item.tags or []))
        ).lower()
        is_clc_like = (
            dataset_class == "land_cover"
            or "clc" in text
            or "land cover" in text
            or "land-cover" in text
            or "code_18" in lower_fields
        )
        if is_clc_like:
            return {
                "source": "inferred_land_cover_proxy",
                "confidence": "medium",
                "metricSemantic": "proxy_environmental_pressure",
                "biasRisk": "absolute_only_bias",
                "numeratorFieldCandidates": ["area_ha", "zonal_value", "sum", "sum_area_m2"],
                "denominatorFieldCandidates": ["area_region_m2", "population"],
                "preferredRankingFieldCandidates": [
                    "pressure_pct_area",
                    "pressure_ha_per_100k",
                    "zonal_value",
                    "area_ha",
                ],
                "recommendedDerivedMetrics": [
                    {
                        "name": "pressure_pct_area",
                        "description": "Share of regional area classified as pressure proxy.",
                        "formulaHint": "numerator_m2 / area_region_m2",
                        "unit": "ratio",
                    },
                    {
                        "name": "pressure_ha_per_100k",
                        "description": "Pressure hectares per 100k residents.",
                        "formulaHint": "(numerator_ha / population) * 100000",
                        "unit": "ha_per_100k",
                    },
                ],
                "analysisCaveats": [
                    "Proxy based on CLC classes and not a direct measure of sanitary/clinical risk.",
                    "Absolute area rankings should be paired with a normalized metric when denominator fields are available.",
                ],
            }
        if has_admin_signals:
            return {
                "source": "inferred_administrative_basemap",
                "confidence": "low",
                "metricSemantic": "administrative_reference",
                "biasRisk": "none",
                "numeratorFieldCandidates": [],
                "denominatorFieldCandidates": [],
                "preferredRankingFieldCandidates": [],
                "recommendedDerivedMetrics": [],
                "analysisCaveats": [],
            }
        return None

    def _build_dataset_routing(
        provider: ProviderCatalogItem,
        dataset_item: ProviderDatasetItem,
        ai_hints: dict[str, Any] | None,
    ) -> dict[str, Any]:
        def _as_bool(value: Any, default: bool = False) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized in {"1", "true", "yes", "y", "on"}:
                    return True
                if normalized in {"0", "false", "no", "n", "off"}:
                    return False
            return default

        supported_query_tools = {
            "queryQCumberTerritorialUnits",
            "queryQCumberDatasetSpatial",
            "queryQCumberDataset",
        }

        fields: list[str] = []
        if isinstance(ai_hints, dict):
            raw_field_catalog = ai_hints.get("fieldCatalog")
            if isinstance(raw_field_catalog, list):
                for item in raw_field_catalog:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or "").strip()
                    if name and name not in fields:
                        fields.append(name)
        lower_fields = [field.lower() for field in fields]
        geometry_fields = _as_string_list(ai_hints.get("geometryFields")) if isinstance(ai_hints, dict) else []
        numeric_fields = _as_string_list(ai_hints.get("numericFields")) if isinstance(ai_hints, dict) else []
        if not numeric_fields and isinstance(ai_hints, dict):
            raw_field_catalog = ai_hints.get("fieldCatalog")
            if isinstance(raw_field_catalog, list):
                numeric_fields = [
                    str(item.get("name") or "").strip()
                    for item in raw_field_catalog
                    if isinstance(item, dict) and str(item.get("type") or "").strip().lower() == "number"
                ]
        suggested_ops = _as_string_list(ai_hints.get("suggestedOps")) if isinstance(ai_hints, dict) else []

        level_field_candidates = [
            field
            for field, lowered in zip(fields, lower_fields)
            if lowered == "lv" or lowered.endswith("__lv") or lowered.endswith("_lv")
        ]
        parent_id_field_candidates = [
            field
            for field, lowered in zip(fields, lower_fields)
            if (("__lv" in lowered and lowered.endswith("_id")) or lowered == "hasc" or lowered.endswith("_hasc"))
        ]
        name_field_candidates = [
            field
            for field, lowered in zip(fields, lower_fields)
            if lowered in {"name", "name_en"} or lowered.endswith("_name")
        ]

        tags = [str(tag).strip().lower() for tag in dataset_item.tags if str(tag).strip()]
        text = " ".join(
            [
                _normalize_text_token(dataset_item.id),
                _normalize_text_token(dataset_item.name),
                _normalize_text_token(dataset_item.description or ""),
                _normalize_text_token(" ".join(tags)),
                _normalize_text_token(" ".join(lower_fields)),
            ]
        )

        has_admin_signals = bool(level_field_candidates or parent_id_field_candidates) or any(
            marker in text
            for marker in [
                "administrative",
                "boundar",
                "confini",
                "province",
                "provincia",
                "municipalit",
                "comune",
                "region",
                "regione",
                "kontur",
            ]
        )

        dataset_class = "other"
        if has_admin_signals:
            dataset_class = "administrative"
        elif "land cover" in text or "land cover" in _normalize_text_token(dataset_item.name) or "clc" in text:
            dataset_class = "land_cover"
        elif "event" in text:
            dataset_class = "events"
        elif "feature" in text:
            dataset_class = "features"

        ai_profile = ai_hints.get("aiProfile") if isinstance(ai_hints, dict) and isinstance(ai_hints.get("aiProfile"), dict) else {}
        query_routing = ai_profile.get("queryRouting") if isinstance(ai_profile.get("queryRouting"), dict) else {}
        profile_preferred_tool_raw = str(query_routing.get("preferredTool") or "").strip()
        profile_preferred_tool = profile_preferred_tool_raw if profile_preferred_tool_raw in supported_query_tools else ""

        inferred_preferred_query_tool = (
            "queryQCumberTerritorialUnits"
            if has_admin_signals
            else "queryQCumberDatasetSpatial"
            if geometry_fields
            else "queryQCumberDataset"
        )
        preferred_query_tool = profile_preferred_tool or inferred_preferred_query_tool
        requires_spatial_bbox = _as_bool(
            query_routing.get("requiresSpatialBbox"),
            default=(preferred_query_tool == "queryQCumberDatasetSpatial" and not has_admin_signals),
        )
        expected_admin_type_supported = _as_bool(
            query_routing.get("expectedAdminTypeSupported"),
            default=has_admin_signals,
        )
        forbidden_admin_constraints = [
            str(item).strip()
            for item in query_routing.get("forbiddenAdminConstraints", [])
            if str(item).strip()
        ] if isinstance(query_routing.get("forbiddenAdminConstraints"), list) else []

        if profile_preferred_tool:
            query_tool_hint_reason = (
                "Preferred tool provided by dataset descriptor profile "
                f"(ai.profile.queryRouting.preferredTool={profile_preferred_tool})."
            )
            query_tool_hint_source = "descriptor_profile"
            query_tool_hint_confidence = "high"
        elif has_admin_signals:
            query_tool_hint_reason = "Administrative signals detected from metadata fields/tags/text."
            query_tool_hint_source = "inferred_admin_signals"
            query_tool_hint_confidence = "medium"
        elif geometry_fields:
            query_tool_hint_reason = "Geometry fields detected in dataset profile."
            query_tool_hint_source = "inferred_geometry"
            query_tool_hint_confidence = "medium"
        else:
            query_tool_hint_reason = "No administrative/spatial signal; using generic dataset query tool."
            query_tool_hint_source = "inferred_fallback"
            query_tool_hint_confidence = "low"

        metric_profile = _build_dataset_metric_profile(
            dataset_item,
            dataset_class=dataset_class,
            has_admin_signals=has_admin_signals,
            fields=fields,
            ai_hints=ai_hints,
        )

        return {
            "providerId": provider.id,
            "datasetId": dataset_item.id,
            "datasetClass": dataset_class,
            "isAdministrative": has_admin_signals,
            "queryToolHint": {
                "preferredTool": preferred_query_tool,
                "confidence": query_tool_hint_confidence,
                "source": query_tool_hint_source,
                "reason": query_tool_hint_reason,
                "requiresSpatialBbox": requires_spatial_bbox,
                "expectedAdminTypeSupported": expected_admin_type_supported,
                "forbiddenAdminConstraints": forbidden_admin_constraints,
            },
            "levelFieldCandidates": level_field_candidates,
            "parentIdFieldCandidates": parent_id_field_candidates,
            "nameFieldCandidates": name_field_candidates,
            "geometryFields": geometry_fields,
            "numericFields": numeric_fields,
            "suggestedOps": suggested_ops,
            "orderByCandidates": _as_string_list(ai_hints.get("orderByCandidates")) if isinstance(ai_hints, dict) else [],
            "metricProfile": metric_profile,
            "metadataSource": "ai_hints" if isinstance(ai_hints, dict) else "descriptor",
        }

    def _resolve_provider_and_dataset(provider_id: str, dataset_id: str) -> tuple[ProviderCatalogItem, ProviderDatasetItem]:
        provider = provider_registry.get_provider(provider_id)
        if not provider:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found")

        datasets = provider_registry.list_provider_datasets(provider.id)
        dataset_item = next(
            (item for item in datasets if item.id.strip().lower() == dataset_id.strip().lower()),
            None,
        )
        if not dataset_item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset '{dataset_id}' not found for provider '{provider.id}'",
            )
        return provider, dataset_item

    def _resolve_dataset_source(
        provider_id: str,
        dataset_id: str,
    ) -> tuple[str, str, dict[str, Any], DataSource]:
        """Resolve provider+dataset and return (label, id, source_cfg, source_backend)."""
        provider, target = _resolve_provider_and_dataset(provider_id, dataset_id)

        source = target.source if isinstance(target.source, dict) else {}
        source_type = str(source.get("type") or "").strip().lower()
        if source_type not in ALL_SOURCE_TYPES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Dataset '{target.id}' uses unsupported source.type '{source_type}'. "
                    f"Supported: {', '.join(sorted(ALL_SOURCE_TYPES))}."
                ),
            )

        # Validate source-specific required fields.
        if source_type in POSTGIS_SOURCE_TYPES:
            table = str(source.get("table") or "").strip()
            if not table:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Dataset '{target.id}' must declare source.table for PostGIS querying.",
                )
        elif source_type in CKAN_SOURCE_TYPES:
            if not str(source.get("resourceId") or "").strip():
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Dataset '{target.id}' must declare source.resourceId for CKAN querying.",
                )
            if not str(source.get("baseUrl") or "").strip():
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Dataset '{target.id}' must declare source.baseUrl for CKAN querying.",
                )

        backend = _get_source(source_type)
        return target.name, target.id, source, backend

    # Keep a backward-compatible alias used by _get_dataset_hints_profile.
    def _resolve_dataset_sql_source(
        provider_id: str,
        dataset_id: str,
    ) -> tuple[str, str, str, str, str]:
        """Legacy helper — returns PostGIS-specific tuple for hints profiling."""
        label, did, source_cfg, backend = _resolve_dataset_source(provider_id, dataset_id)
        schema = str(source_cfg.get("schema") or "qvt").strip()
        table = str(source_cfg.get("table") or "").strip()
        geometry_column = str(source_cfg.get("geometryColumn") or "geom").strip()
        return label, did, schema, table, geometry_column

    def _fetch_table_columns(schema: str, table: str) -> list[str]:
        """Fetch columns via PostGIS source (backward compat for hints profiling)."""
        return postgis_source.fetch_columns({"schema": schema, "table": table})

    def _filter_attr(filter_item: Any, key: str, default: Any = None) -> Any:
        if isinstance(filter_item, dict):
            return filter_item.get(key, default)
        return getattr(filter_item, key, default)

    def _run_source_query(
        provider_id: str,
        dataset_id: str,
        *,
        select_fields: list[str] | None,
        filters: list[Any] | None,
        spatial_bbox: tuple[float, float, float, float] | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> tuple[str, list[str], list[dict[str, Any]], int]:
        """Dispatch query to the appropriate source backend."""
        dataset_label, resolved_id, source_cfg, backend = _resolve_dataset_source(provider_id, dataset_id)
        source_type = str(source_cfg.get("type") or "").strip().lower()

        # Dataset adapter integration (virtual fields, filter normalization).
        table = str(source_cfg.get("table") or "").strip()
        requested_select = _normalize_select_fields(select_fields)
        normalized_filters = dataset_adapters.normalize_filters(
            table=table,
            filters=filters,
            filter_get=_filter_attr,
        )

        # For PostGIS sources: handle virtual fields and column adjustment.
        effective_select: list[str] | None = requested_select or None
        if source_type in POSTGIS_SOURCE_TYPES and requested_select:
            columns = backend.fetch_columns(source_cfg)
            geometry_column = str(source_cfg.get("geometryColumn") or "geom").strip()
            non_geom_set = {c for c in columns if c != geometry_column}
            virtual_fields = dataset_adapters.get_virtual_fields(table=table)
            selected_non_geom = [f for f in requested_select if f in non_geom_set]
            selected_virtual = [f for f in requested_select if f in virtual_fields]
            include_geojson = "_geojson" in requested_select
            selected_non_geom = dataset_adapters.adjust_select_columns(
                table=table,
                requested_select=requested_select,
                selected_non_geom=selected_non_geom,
                non_geom_set=non_geom_set,
            )
            if not selected_non_geom and not include_geojson and not selected_virtual:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="None of the requested select fields are available in dataset columns.",
                )
            effective_select = list(selected_non_geom)
            if include_geojson:
                effective_select.append("_geojson")

        result: SourceResult = backend.query(
            source_cfg,
            dataset_label=dataset_label,
            select_fields=effective_select,
            filters=normalized_filters,
            spatial_bbox=spatial_bbox,
            order_by=order_by,
            order_direction=order_direction,
            limit=limit,
            offset=offset,
        )

        # Post-query enrichment (dataset adapters).
        rows = dataset_adapters.enrich_rows(table=table, rows=result.rows)
        fields_out = list(result.fields)
        if rows:
            fields_out = sorted({k for item in rows for k in item.keys() if isinstance(k, str) and k})
        if requested_select:
            wanted = [f for f in requested_select if f in fields_out]
            if wanted:
                fields_out = wanted
        return dataset_label, fields_out, rows, result.total_matched

    async def _get_dataset_hints_profile(
        provider_id: str,
        dataset_id: str,
        fetch_if_missing: bool = True,
    ) -> dict[str, Any] | None:
        key = _dataset_cache_key(provider_id, dataset_id)
        cached = dataset_hints_cache.get(key)
        now_ms = int(__import__("time").time() * 1000)
        if isinstance(cached, dict):
            if now_ms - int(cached.get("ts", 0)) <= dataset_hints_cache_ttl_seconds * 1000:
                profile = cached.get("profile")
                if isinstance(profile, dict):
                    return profile
        if not fetch_if_missing:
            return None

        dataset_label, _resolved_id, source_cfg, backend = _resolve_dataset_source(provider_id, dataset_id)
        columns = backend.fetch_columns(source_cfg)
        geometry_column = str(source_cfg.get("geometryColumn") or source_cfg.get("geometryField") or "geom").strip()
        non_geom_columns = [c for c in columns if c != geometry_column]
        sample_fields: list[str] = [*non_geom_columns, "_geojson"]
        _label, _fields, sample_rows, total_matched = _run_source_query(
            provider_id,
            dataset_id,
            select_fields=sample_fields,
            filters=None,
            spatial_bbox=None,
            order_by=None,
            order_direction="asc",
            limit=2000,
            offset=0,
        )
        profile = _infer_dataset_hints_profile(dataset_label, sample_rows)
        profile["rowCount"] = total_matched
        dataset_hints_cache[key] = {"ts": now_ms, "profile": profile}
        return profile

    def _normalize_bbox(raw_bbox: list[float] | None) -> tuple[float, float, float, float] | None:
        if not isinstance(raw_bbox, list) or len(raw_bbox) != 4:
            return None
        try:
            min_x = float(raw_bbox[0])
            min_y = float(raw_bbox[1])
            max_x = float(raw_bbox[2])
            max_y = float(raw_bbox[3])
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid spatialBbox. Expected [minLon, minLat, maxLon, maxLat].",
            )
        if min_x > max_x or min_y > max_y:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid spatialBbox ordering. Expected min <= max for both axes.",
            )
        return (min_x, min_y, max_x, max_y)

    def _normalize_select_fields(select: list[str] | None) -> list[str]:
        if not isinstance(select, list):
            return []
        out: list[str] = []
        seen: set[str] = set()
        for field in select:
            if not isinstance(field, str):
                continue
            name = field.strip()
            if not name or name == "*" or name in seen:
                continue
            seen.add(name)
            out.append(name)
        return out

    def _infer_kepler_field_type(values: list[Any]) -> str:
        sample = next((value for value in values if value is not None), None)
        if sample is None:
            return "string"
        if isinstance(sample, dict):
            if isinstance(sample.get("type"), str) and sample.get("coordinates") is not None:
                return "geojson"
        if isinstance(sample, bool):
            return "boolean"
        if isinstance(sample, int) and not isinstance(sample, bool):
            return "integer"
        if isinstance(sample, float):
            return "real"
        return "string"

    def _build_kepler_dataset_payload(
        provider_id: str,
        dataset_id: str,
        dataset_label: str,
        fields: list[str],
        rows: list[dict[str, Any]],
    ) -> KeplerDatasetPayload:
        data_fields = [
            KeplerDatasetField(name=field, type=_infer_kepler_field_type([row.get(field) for row in rows]))
            for field in fields
        ]
        data_rows = [[row.get(field) for field in fields] for row in rows]
        return KeplerDatasetPayload(
            info=KeplerDatasetInfo(id=f"{provider_id}-{dataset_id}-query", label=f"{dataset_label} (query)"),
            data=KeplerDatasetData(fields=data_fields, rows=data_rows),
        )

    def _resolve_jwt_context(credentials: HTTPAuthorizationCredentials | None) -> AuthContext:
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
        try:
            claims = decode_and_validate_jwt(
                credentials.credentials,
                hs256_secrets=app_settings.jwt_auth.hs256_secrets,
                allowed_issuers=app_settings.jwt_auth.allowed_issuers,
                allowed_audiences=app_settings.jwt_auth.allowed_audiences,
                require_audience=app_settings.jwt_auth.require_audience,
                allowed_subjects=app_settings.jwt_auth.allowed_subjects,
            )
        except JwtValidationError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

        subject = str(claims.get("sub") or "").strip()
        return AuthContext(
            name=str(claims.get("name") or claims.get("preferred_username") or app_settings.user_name),
            email=str(claims.get("email") or app_settings.user_email),
            roles=extract_roles(claims, app_settings.jwt_auth.roles_claim_paths),
            subject=subject,
        )

    def resolve_auth(credentials: HTTPAuthorizationCredentials | None = Depends(auth_scheme)) -> AuthContext:
        if app_settings.jwt_auth.enabled:
            return _resolve_jwt_context(credentials)
        if not app_settings.api_token:
            return AuthContext(name=app_settings.user_name, email=app_settings.user_email)
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
        if credentials.credentials != app_settings.api_token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token")
        return AuthContext(name=app_settings.user_name, email=app_settings.user_email)

    def require_read_access(auth: AuthContext = Depends(resolve_auth)) -> AuthContext:
        required = app_settings.jwt_auth.read_roles
        if app_settings.jwt_auth.enabled and required and not set(auth.roles).intersection(required):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role for read. Required one of: {', '.join(required)}",
            )
        return auth

    @app.get("/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/me", response_model=CloudUser)
    async def me(auth: AuthContext = Depends(require_read_access)) -> CloudUser:
        return CloudUser(name=auth.name, email=auth.email)

    @app.get("/maps", response_model=MapListResponse)
    async def list_maps(_: AuthContext = Depends(require_read_access)) -> MapListResponse:
        return MapListResponse(items=store.list_maps())

    @app.get("/maps/{map_id}", response_model=DownloadMapResponse)
    async def download_map(map_id: str, _: AuthContext = Depends(require_read_access)) -> DownloadMapResponse:
        stored = store.get_map(map_id)
        if not stored:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
        return DownloadMapResponse(id=stored.id, map=stored.map, format=stored.format)

    def _readonly_write_error() -> None:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="q-cumber-backend is read-only. Use q-storage-backend to save maps.",
        )

    @app.post("/maps")
    async def create_map(payload: dict[str, Any], _: AuthContext = Depends(require_read_access)) -> dict[str, Any]:
        _readonly_write_error()
        return payload

    @app.put("/maps/{map_id}")
    async def update_map(
        map_id: str,
        payload: dict[str, Any],
        _: AuthContext = Depends(require_read_access),
    ) -> dict[str, Any]:
        _readonly_write_error()
        return {"map_id": map_id, "payload": payload}

    @app.get("/providers/locales", response_model=ProviderLocalesResponse)
    async def list_provider_locales(_: AuthContext = Depends(require_read_access)) -> ProviderLocalesResponse:
        return ProviderLocalesResponse(locales=provider_registry.list_locales())

    @app.get("/providers", response_model=ProviderListResponse)
    async def list_providers(
        locale: str | None = Query(default=None),
        _: AuthContext = Depends(require_read_access),
    ) -> ProviderListResponse:
        return ProviderListResponse(items=provider_registry.list_providers(locale=locale))

    @app.get("/providers/{provider_id}", response_model=ProviderCatalogItem)
    async def get_provider(provider_id: str, _: AuthContext = Depends(require_read_access)) -> ProviderCatalogItem:
        provider = provider_registry.get_provider(provider_id)
        if not provider:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found")
        return provider

    @app.get("/providers/{provider_id}/datasets", response_model=ProviderDatasetListResponse)
    async def list_provider_datasets(
        provider_id: str,
        _: AuthContext = Depends(require_read_access),
    ) -> ProviderDatasetListResponse:
        provider = provider_registry.get_provider(provider_id)
        if not provider:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found")
        raw_items = provider_registry.list_provider_datasets(provider.id)

        enriched_items: list[ProviderDatasetItem] = []
        for item in raw_items:
            dataset_profile = await _get_dataset_hints_profile(provider.id, item.id, fetch_if_missing=False)
            ai_hints = _build_ai_hints_from_dataset_profile(dataset_profile, item)
            enriched_items.append(
                ProviderDatasetItem(
                    id=item.id,
                    providerId=item.providerId,
                    name=item.name,
                    description=item.description,
                    url=item.url,
                    source=item.source,
                    format=item.format,
                    tags=item.tags,
                    ai=item.ai,
                    aiHints=ai_hints,
                    routing=_build_dataset_routing(provider, item, ai_hints),
                )
            )

        return ProviderDatasetListResponse(providerId=provider.id, items=enriched_items)

    @app.get("/providers/{provider_id}/datasets/{dataset_id}/help", response_model=DatasetHelpResponse)
    async def get_provider_dataset_help(
        provider_id: str,
        dataset_id: str,
        _: AuthContext = Depends(require_read_access),
    ) -> DatasetHelpResponse:
        provider, dataset_item = _resolve_provider_and_dataset(provider_id, dataset_id)
        dataset_profile = await _get_dataset_hints_profile(provider.id, dataset_item.id)
        ai_hints = _build_ai_hints_from_dataset_profile(dataset_profile, dataset_item)
        routing = _build_dataset_routing(provider, dataset_item, ai_hints)
        return DatasetHelpResponse(
            providerId=provider.id,
            datasetId=dataset_item.id,
            datasetName=dataset_item.name,
            aiHints=ai_hints,
            routing=routing,
        )

    @app.post("/datasets/query", response_model=DatasetQueryResponse)
    async def query_dataset(
        payload: DatasetQueryRequest,
        _: AuthContext = Depends(require_read_access),
    ) -> DatasetQueryResponse:
        query_bbox = _normalize_bbox(payload.spatialBbox)
        dataset_label, selected_fields, rows_out, total_matched = _run_source_query(
            payload.providerId,
            payload.datasetId,
            select_fields=payload.select,
            filters=payload.filters,
            spatial_bbox=query_bbox,
            order_by=payload.orderBy,
            order_direction=payload.orderDirection,
            limit=payload.limit,
            offset=payload.offset,
        )
        window_rows = [{field: row.get(field) for field in selected_fields} for row in rows_out]

        dataset_payload = _build_kepler_dataset_payload(
            provider_id=payload.providerId,
            dataset_id=payload.datasetId,
            dataset_label=dataset_label,
            fields=selected_fields,
            rows=window_rows,
        )

        provider, dataset_item = _resolve_provider_and_dataset(payload.providerId, payload.datasetId)
        dataset_profile = await _get_dataset_hints_profile(provider.id, dataset_item.id)
        ai_hints = _build_ai_hints_from_dataset_profile(dataset_profile, dataset_item)
        routing = _build_dataset_routing(provider, dataset_item, ai_hints)
        return DatasetQueryResponse(
            providerId=payload.providerId,
            datasetId=payload.datasetId,
            totalMatched=total_matched,
            returned=len(window_rows),
            fields=selected_fields,
            rows=window_rows,
            dataset=dataset_payload,
            aiHints=ai_hints,
            routing=routing,
        )

    return app


app = create_app()


def run() -> None:
    settings = load_settings()
    workers = settings.workers
    uvicorn.run(
        "q_cumber_backend.main:app",
        host="0.0.0.0",
        port=3001,
        workers=workers,
        reload=workers == 1,
    )


if __name__ == "__main__":
    run()
