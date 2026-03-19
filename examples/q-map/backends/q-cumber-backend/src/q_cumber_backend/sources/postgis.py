"""PostGIS data source — extracted from main.py query logic."""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, status
from psycopg import sql
import psycopg_pool

from .base import DataSource, SourceResult


class PostGISSource:
    """Queries a PostGIS-backed table."""

    def __init__(self, pool: psycopg_pool.ConnectionPool) -> None:
        self._pool = pool
        self._table_columns_cache: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # DataSource protocol
    # ------------------------------------------------------------------

    def fetch_columns(self, source_cfg: dict[str, Any]) -> list[str]:
        schema = str(source_cfg.get("schema") or "qvt").strip()
        table = str(source_cfg.get("table") or "").strip()
        return self._fetch_table_columns(schema, table)

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
        schema = str(source_cfg.get("schema") or "qvt").strip()
        table = str(source_cfg.get("table") or "").strip()
        geometry_column = str(source_cfg.get("geometryColumn") or "geom").strip()

        columns = self._fetch_table_columns(schema, table)
        if geometry_column not in columns:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Geometry column '{geometry_column}' not found on '{schema}.{table}'.",
            )
        non_geom_columns = [c for c in columns if c != geometry_column]
        non_geom_set = set(non_geom_columns)

        # Determine projection ------------------------------------------------
        include_geojson: bool
        selected_non_geom: list[str]
        if select_fields:
            selected_non_geom = [f for f in select_fields if f in non_geom_set]
            include_geojson = "_geojson" in select_fields
            if not selected_non_geom and not include_geojson:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="None of the requested select fields are available in dataset columns.",
                )
        else:
            selected_non_geom = non_geom_columns
            include_geojson = True

        projected_sql_fields: list[sql.SQL] = [
            sql.SQL("{}.{}").format(sql.Identifier(table), sql.Identifier(col))
            for col in selected_non_geom
        ]
        if include_geojson:
            projected_sql_fields.append(
                sql.SQL("ST_AsGeoJSON({}.{})::jsonb AS _geojson").format(
                    sql.Identifier(table),
                    sql.Identifier(geometry_column),
                )
            )
        projected_fields = list(selected_non_geom)
        if include_geojson:
            projected_fields.append("_geojson")

        # Filters --------------------------------------------------------------
        where_clauses, params = build_sql_filter_clause(filters, non_geom_set)
        if spatial_bbox is not None:
            min_x, min_y, max_x, max_y = spatial_bbox
            where_clauses.append(
                sql.SQL(
                    "ST_Intersects({table}.{geom}, ST_MakeEnvelope(%s, %s, %s, %s, 4326))"
                ).format(
                    table=sql.Identifier(table),
                    geom=sql.Identifier(geometry_column),
                )
            )
            params.extend([min_x, min_y, max_x, max_y])

        where_sql = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)
            if where_clauses
            else sql.SQL("")
        )

        order_sql = sql.SQL("")
        if order_by and order_by in non_geom_set:
            direction = "DESC" if str(order_direction).lower() == "desc" else "ASC"
            order_sql = sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(order_by), sql.SQL(direction)
            )

        # Execute --------------------------------------------------------------
        with self._pool.connection() as conn:
            total_row = conn.execute(
                sql.SQL("SELECT COUNT(*) AS total FROM {}.{}{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    where_sql,
                ),
                params,
            ).fetchone()
            total_matched = int((total_row or {}).get("total") or 0)

            rows_raw = conn.execute(
                sql.SQL("SELECT {} FROM {}.{}{}{} LIMIT %s OFFSET %s").format(
                    sql.SQL(", ").join(projected_sql_fields),
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    where_sql,
                    order_sql,
                ),
                [*params, int(limit), int(offset)],
            ).fetchall()

        rows: list[dict[str, Any]] = []
        for row in rows_raw:
            current = dict(row)
            geo = current.get("_geojson")
            if isinstance(geo, str):
                try:
                    current["_geojson"] = json.loads(geo)
                except Exception:
                    current["_geojson"] = None
            rows.append(current)

        fields_out = list(projected_fields)
        if rows:
            fields_out = sorted(
                {k for item in rows for k in item.keys() if isinstance(k, str) and k}
            )
        if select_fields:
            wanted = [f for f in select_fields if f in fields_out]
            if wanted:
                fields_out = wanted

        return SourceResult(
            dataset_label=dataset_label,
            fields=fields_out,
            rows=rows,
            total_matched=total_matched,
            columns=non_geom_columns,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_table_columns(self, schema: str, table: str) -> list[str]:
        cache_key = f"{schema.lower()}.{table.lower()}"
        cached = self._table_columns_cache.get(cache_key)
        if isinstance(cached, list) and cached:
            return cached
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            ).fetchall()
        columns = [
            str(r.get("column_name") or "").strip()
            for r in rows
            if str(r.get("column_name") or "").strip()
        ]
        if not columns:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"PostGIS table '{schema}.{table}' not found or has no columns.",
            )
        self._table_columns_cache[cache_key] = columns
        return columns


# ------------------------------------------------------------------
# SQL filter builder (reusable, extracted from main.py)
# ------------------------------------------------------------------

def build_sql_filter_clause(
    filters: list[Any] | None,
    non_geom_columns: set[str],
) -> tuple[list[sql.SQL], list[Any]]:
    where_clauses: list[sql.SQL] = []
    params: list[Any] = []
    if not filters:
        return where_clauses, params

    def _attr(item: Any, key: str, default: Any = None) -> Any:
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    for f in filters:
        field = str(_attr(f, "field", "") or "").strip()
        op = str(_attr(f, "op", "eq") or "eq").strip().lower()
        if field not in non_geom_columns:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Filter field '{field}' is not available in dataset table columns.",
            )
        ident = sql.Identifier(field)
        if op == "is_null":
            where_clauses.append(sql.SQL("{} IS NULL").format(ident))
            continue
        if op == "not_null":
            where_clauses.append(sql.SQL("{} IS NOT NULL").format(ident))
            continue

        value = _attr(f, "value", None)
        values = _attr(f, "values", None)
        if op == "contains":
            where_clauses.append(sql.SQL("{}::text ILIKE %s").format(ident))
            params.append(f"%{'' if value is None else str(value)}%")
            continue
        if op == "startswith":
            where_clauses.append(sql.SQL("{}::text ILIKE %s").format(ident))
            params.append(f"{'' if value is None else str(value)}%")
            continue
        if op == "endswith":
            where_clauses.append(sql.SQL("{}::text ILIKE %s").format(ident))
            params.append(f"%{'' if value is None else str(value)}")
            continue
        if op == "in":
            raw_values = values if isinstance(values, list) else [value]
            if not raw_values:
                where_clauses.append(sql.SQL("FALSE"))
                continue
            placeholders = sql.SQL(",").join(sql.SQL("%s") for _ in raw_values)
            where_clauses.append(sql.SQL("{} IN ({})").format(ident, placeholders))
            params.extend(raw_values)
            continue

        op_sql = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}.get(op)
        if not op_sql:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unsupported filter operator '{op}'.",
            )
        where_clauses.append(sql.SQL("{} {} %s").format(ident, sql.SQL(op_sql)))
        params.append(value)
    return where_clauses, params
