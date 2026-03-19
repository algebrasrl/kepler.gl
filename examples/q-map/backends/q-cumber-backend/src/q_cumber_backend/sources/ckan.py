"""CKAN data source — queries external CKAN datastore APIs."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status
import httpx

from .base import SourceResult

log = logging.getLogger(__name__)

# Operators that CKAN datastore_search supports natively via ``filters``.
_CKAN_NATIVE_FILTER_OPS = {"eq", "in"}

# Mapping q-cumber filter operators to SQL comparisons for datastore_search_sql.
_OP_TO_SQL = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}


class CKANSource:
    """Queries an external CKAN instance via datastore_search / datastore_search_sql."""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        default_api_key: str = "",
    ) -> None:
        self._timeout = timeout
        self._default_api_key = default_api_key
        self._client = httpx.Client(timeout=timeout)
        self._columns_cache: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # DataSource protocol
    # ------------------------------------------------------------------

    def fetch_columns(self, source_cfg: dict[str, Any]) -> list[str]:
        base_url = self._base_url(source_cfg)
        resource_id = self._resource_id(source_cfg)
        cache_key = f"{base_url}::{resource_id}"
        cached = self._columns_cache.get(cache_key)
        if isinstance(cached, list) and cached:
            return cached

        # Fetch one row to discover columns.
        data = self._datastore_search(base_url, resource_id, source_cfg, limit=0)
        fields_raw = data.get("fields", [])
        columns = [
            str(f.get("id") or "").strip()
            for f in fields_raw
            if isinstance(f, dict)
            and str(f.get("id") or "").strip()
            and str(f.get("id") or "").strip() != "_id"
        ]
        if not columns:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"CKAN resource '{resource_id}' returned no fields.",
            )
        self._columns_cache[cache_key] = columns
        return columns

    def query(
        self,
        source_cfg: dict[str, Any],
        *,
        dataset_label: str,
        select_fields: list[str] | None,
        filters: list[Any] | None,
        spatial_bbox: tuple[float, float, float, float] | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> SourceResult:
        base_url = self._base_url(source_cfg)
        resource_id = self._resource_id(source_cfg)
        geometry_field = str(source_cfg.get("geometryField") or "").strip()
        columns = self.fetch_columns(source_cfg)

        needs_sql = self._needs_sql_mode(filters, spatial_bbox)

        if needs_sql:
            return self._query_via_sql(
                base_url=base_url,
                resource_id=resource_id,
                source_cfg=source_cfg,
                dataset_label=dataset_label,
                columns=columns,
                geometry_field=geometry_field,
                select_fields=select_fields,
                filters=filters,
                spatial_bbox=spatial_bbox,
                order_by=order_by,
                order_direction=order_direction,
                limit=limit,
                offset=offset,
            )

        return self._query_via_search(
            base_url=base_url,
            resource_id=resource_id,
            source_cfg=source_cfg,
            dataset_label=dataset_label,
            columns=columns,
            geometry_field=geometry_field,
            select_fields=select_fields,
            filters=filters,
            order_by=order_by,
            order_direction=order_direction,
            limit=limit,
            offset=offset,
        )

    # ------------------------------------------------------------------
    # datastore_search (simple filters: eq / in only)
    # ------------------------------------------------------------------

    def _query_via_search(
        self,
        *,
        base_url: str,
        resource_id: str,
        source_cfg: dict[str, Any],
        dataset_label: str,
        columns: list[str],
        geometry_field: str,
        select_fields: list[str] | None,
        filters: list[Any] | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> SourceResult:
        params: dict[str, Any] = {}

        # Filters → CKAN native format: {"field": value} or {"field": [v1,v2]}
        if filters:
            ckan_filters: dict[str, Any] = {}
            for f in filters:
                field, op, value, values = self._unpack_filter(f)
                if op == "eq":
                    ckan_filters[field] = value
                elif op == "in":
                    ckan_filters[field] = values if isinstance(values, list) else [value]
            if ckan_filters:
                params["filters"] = ckan_filters

        # Field selection
        if select_fields:
            valid = [f for f in select_fields if f in columns or f == "_geojson"]
            if valid:
                params["fields"] = ",".join(f for f in valid if f != "_geojson")

        if order_by and order_by in columns:
            params["sort"] = f"{order_by} {order_direction}"

        data = self._datastore_search(
            base_url, resource_id, source_cfg, limit=limit, offset=offset, **params
        )
        records = data.get("records", [])
        total = int(data.get("total", len(records)))

        rows = self._normalize_rows(records, columns, geometry_field)
        fields_out = self._resolve_fields(rows, select_fields, columns)

        return SourceResult(
            dataset_label=dataset_label,
            fields=fields_out,
            rows=rows,
            total_matched=total,
            columns=[c for c in columns if c != geometry_field],
        )

    # ------------------------------------------------------------------
    # datastore_search_sql (advanced filters, spatial)
    # ------------------------------------------------------------------

    def _query_via_sql(
        self,
        *,
        base_url: str,
        resource_id: str,
        source_cfg: dict[str, Any],
        dataset_label: str,
        columns: list[str],
        geometry_field: str,
        select_fields: list[str] | None,
        filters: list[Any] | None,
        spatial_bbox: tuple[float, float, float, float] | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> SourceResult:
        # Build SELECT clause
        if select_fields:
            sql_fields = [
                f'"{f}"' for f in select_fields if f in columns and f != "_geojson"
            ]
            if not sql_fields:
                sql_fields = [f'"{c}"' for c in columns if c != geometry_field]
        else:
            sql_fields = [f'"{c}"' for c in columns if c != geometry_field]

        if geometry_field and (not select_fields or "_geojson" in (select_fields or [])):
            sql_fields.append(f'ST_AsGeoJSON("{geometry_field}") AS _geojson')

        select_clause = ", ".join(sql_fields)

        # Build WHERE clause
        where_parts: list[str] = []
        if filters:
            for f in filters:
                clause = self._filter_to_sql_clause(f, set(columns))
                if clause:
                    where_parts.append(clause)

        if spatial_bbox and geometry_field:
            min_x, min_y, max_x, max_y = spatial_bbox
            where_parts.append(
                f'ST_Intersects("{geometry_field}", '
                f"ST_MakeEnvelope({min_x}, {min_y}, {max_x}, {max_y}, 4326))"
            )

        where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # ORDER BY
        order_clause = ""
        if order_by and order_by in columns:
            direction = "DESC" if str(order_direction).lower() == "desc" else "ASC"
            order_clause = f' ORDER BY "{order_by}" {direction}'

        # COUNT query
        count_sql = f'SELECT COUNT(*) AS total FROM "{resource_id}"{where_clause}'
        count_data = self._datastore_search_sql(base_url, count_sql, source_cfg)
        count_records = count_data.get("records", [])
        total = int(count_records[0].get("total", 0)) if count_records else 0

        # Data query
        data_sql = (
            f"SELECT {select_clause} "
            f'FROM "{resource_id}"{where_clause}{order_clause} '
            f"LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        data = self._datastore_search_sql(base_url, data_sql, source_cfg)
        records = data.get("records", [])

        rows = self._normalize_rows(records, columns, geometry_field)
        fields_out = self._resolve_fields(rows, select_fields, columns)

        return SourceResult(
            dataset_label=dataset_label,
            fields=fields_out,
            rows=rows,
            total_matched=total,
            columns=[c for c in columns if c != geometry_field],
        )

    # ------------------------------------------------------------------
    # CKAN API calls
    # ------------------------------------------------------------------

    def _datastore_search(
        self,
        base_url: str,
        resource_id: str,
        source_cfg: dict[str, Any],
        *,
        limit: int = 100,
        offset: int = 0,
        **extra: Any,
    ) -> dict[str, Any]:
        url = f"{base_url}/api/3/action/datastore_search"
        payload: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
            **extra,
        }
        return self._post(url, payload, source_cfg)

    def _datastore_search_sql(
        self,
        base_url: str,
        sql_query: str,
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        url = f"{base_url}/api/3/action/datastore_search_sql"
        return self._post(url, {"sql": sql_query}, source_cfg)

    def _post(
        self,
        url: str,
        payload: dict[str, Any],
        source_cfg: dict[str, Any],
    ) -> dict[str, Any]:
        api_key = str(source_cfg.get("apiKey") or self._default_api_key or "").strip()
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = api_key

        try:
            resp = self._client.post(url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            log.error("CKAN request failed: %s %s", url, exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"CKAN upstream error: {exc}",
            ) from exc

        if resp.status_code != 200:
            log.error("CKAN %s returned %s: %s", url, resp.status_code, resp.text[:500])
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"CKAN returned HTTP {resp.status_code}",
            )

        body = resp.json()
        if not body.get("success"):
            error = body.get("error", {})
            log.error("CKAN action failed: %s", error)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"CKAN action error: {error.get('message', error)}",
            )
        return body.get("result", {})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _base_url(source_cfg: dict[str, Any]) -> str:
        url = str(source_cfg.get("baseUrl") or "").strip().rstrip("/")
        if not url:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="CKAN source requires 'baseUrl' in provider descriptor.",
            )
        return url

    @staticmethod
    def _resource_id(source_cfg: dict[str, Any]) -> str:
        rid = str(source_cfg.get("resourceId") or "").strip()
        if not rid:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="CKAN source requires 'resourceId' in dataset descriptor.",
            )
        return rid

    @staticmethod
    def _unpack_filter(f: Any) -> tuple[str, str, Any, Any]:
        if isinstance(f, dict):
            return (
                str(f.get("field") or "").strip(),
                str(f.get("op") or "eq").strip().lower(),
                f.get("value"),
                f.get("values"),
            )
        return (
            str(getattr(f, "field", "") or "").strip(),
            str(getattr(f, "op", "eq") or "eq").strip().lower(),
            getattr(f, "value", None),
            getattr(f, "values", None),
        )

    @staticmethod
    def _needs_sql_mode(
        filters: list[Any] | None,
        spatial_bbox: tuple[float, float, float, float] | None,
    ) -> bool:
        if spatial_bbox:
            return True
        if not filters:
            return False
        for f in filters:
            op = str(
                f.get("op", "eq") if isinstance(f, dict) else getattr(f, "op", "eq")
            ).strip().lower()
            if op not in _CKAN_NATIVE_FILTER_OPS:
                return True
        return False

    @staticmethod
    def _filter_to_sql_clause(f: Any, valid_columns: set[str]) -> str:
        field, op, value, values = CKANSource._unpack_filter(f)
        if not field or field not in valid_columns:
            return ""

        quoted = f'"{field}"'

        if op == "is_null":
            return f"{quoted} IS NULL"
        if op == "not_null":
            return f"{quoted} IS NOT NULL"
        if op == "contains":
            escaped = str(value or "").replace("'", "''")
            return f"{quoted}::text ILIKE '%{escaped}%'"
        if op == "startswith":
            escaped = str(value or "").replace("'", "''")
            return f"{quoted}::text ILIKE '{escaped}%'"
        if op == "endswith":
            escaped = str(value or "").replace("'", "''")
            return f"{quoted}::text ILIKE '%{escaped}'"
        if op == "in":
            raw_vals = values if isinstance(values, list) else [value]
            literals = ", ".join(
                f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in raw_vals
            )
            return f"{quoted} IN ({literals})"

        sql_op = _OP_TO_SQL.get(op)
        if not sql_op:
            return ""

        if isinstance(value, (int, float)):
            return f"{quoted} {sql_op} {value}"
        escaped = str(value or "").replace("'", "''")
        return f"{quoted} {sql_op} '{escaped}'"

    @staticmethod
    def _normalize_rows(
        records: list[dict[str, Any]],
        columns: list[str],
        geometry_field: str,
    ) -> list[dict[str, Any]]:
        """Remove CKAN internal ``_id`` and parse _geojson strings."""
        import json as _json

        rows: list[dict[str, Any]] = []
        for rec in records:
            row = {k: v for k, v in rec.items() if k != "_id"}
            geo = row.get("_geojson")
            if isinstance(geo, str):
                try:
                    row["_geojson"] = _json.loads(geo)
                except Exception:
                    row["_geojson"] = None
            rows.append(row)
        return rows

    @staticmethod
    def _resolve_fields(
        rows: list[dict[str, Any]],
        select_fields: list[str] | None,
        columns: list[str],
    ) -> list[str]:
        if rows:
            fields = sorted(
                {k for r in rows for k in r.keys() if isinstance(k, str) and k}
            )
        else:
            fields = list(columns)
        if select_fields:
            wanted = [f for f in select_fields if f in fields]
            if wanted:
                return wanted
        return fields
