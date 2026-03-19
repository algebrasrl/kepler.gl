#!/usr/bin/env python3
"""
ETL: OPAS ISPRA → PostGIS

Syncs air quality monitoring stations and latest measurements from
the OPAS API (https://opas.isprambiente.it) into PostGIS tables
queryable by q-cumber-backend.

Usage:
    python -m etl.opas_sync --stations          # sync stations only (fast)
    python -m etl.opas_sync --measurements      # sync latest measurements
    python -m etl.opas_sync --all               # both
    python -m etl.opas_sync --init              # create tables + full sync

Environment variables:
    OPAS_EMAIL          login email
    OPAS_PASSWORD       login password
    OPAS_API_BASE       API base URL (default: https://opas.isprambiente.it/api/v1)
    QCUMBER_POSTGIS_DSN full DSN, or use individual vars below:
    QCUMBER_POSTGIS_HOST, QCUMBER_POSTGIS_PORT, QCUMBER_POSTGIS_DB,
    QCUMBER_POSTGIS_USER, QCUMBER_POSTGIS_PASSWORD
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg
from psycopg.conninfo import make_conninfo

log = logging.getLogger("opas_sync")

# Key air quality parameters (ISPRA parameter IDs and name patterns)
# These are the main pollutants tracked by the Italian RMQA network.
# Main air quality pollutants only — keep this list tight to avoid
# exploding the number of API calls (each match = 1 HTTP request).
KEY_PARAM_EXACT = {
    "PM10", "PM2.5", "PM2,5",
    "NO2", "O3", "SO2", "CO",
    "Benzene",
}


def _build_conninfo() -> str:
    dsn = os.getenv("QCUMBER_POSTGIS_DSN", "").strip()
    if dsn:
        return dsn
    return make_conninfo(
        host=os.getenv("QCUMBER_POSTGIS_HOST", "localhost"),
        port=int(os.getenv("QCUMBER_POSTGIS_PORT", "5432")),
        dbname=os.getenv("QCUMBER_POSTGIS_DB", "qvt"),
        user=os.getenv("QCUMBER_POSTGIS_USER", "qvt"),
        password=os.getenv("QCUMBER_POSTGIS_PASSWORD", "qvt"),
    )


class OPASClient:
    """Thin client for the OPAS REST API."""

    def __init__(self, base_url: str, email: str, password: str) -> None:
        self._base = base_url.rstrip("/")
        self._email = email
        self._password = password
        self._http = httpx.Client(timeout=60.0)
        self._token: str = ""

    def _ensure_token(self) -> None:
        if self._token:
            return
        resp = self._http.post(
            f"{self._base}/login",
            json={"email": self._email, "password": self._password},
        )
        resp.raise_for_status()
        self._token = resp.json()["token"]
        log.info("OPAS login OK")

    def _get(self, path: str) -> dict:
        self._ensure_token()
        resp = self._http.get(
            f"{self._base}{path}",
            headers={"Authorization": f"Bearer {self._token}"},
        )
        if resp.status_code == 401:
            # Token expired — re-login once.
            self._token = ""
            self._ensure_token()
            resp = self._http.get(
                f"{self._base}{path}",
                headers={"Authorization": f"Bearer {self._token}"},
            )
        resp.raise_for_status()
        return resp.json()

    def stations(self) -> list[dict]:
        return self._get("/stations").get("stations", [])

    def series_for_station(self, station_id: int) -> list[dict]:
        return self._get(f"/series/{station_id}").get("series", [])

    def series_data(self, series_id: int, hours: int = 48) -> dict:
        return self._get(f"/series-data/{series_id}/{hours}").get("data", {})

    def series_data_range(self, series_id: int, start: str, end: str) -> dict:
        """Fetch hourly data for a date range (ISO 8601 format)."""
        return self._get(f"/series-data/{series_id}/{start}/{end}").get("data", {})


def _is_key_param(name: str) -> bool:
    """Match only exact primary pollutant names (case-insensitive)."""
    cleaned = (name or "").strip()
    return cleaned in KEY_PARAM_EXACT or cleaned.upper() in {p.upper() for p in KEY_PARAM_EXACT}


def init_tables(conn: psycopg.Connection) -> None:
    ddl_path = Path(__file__).parent / "opas_ddl.sql"
    ddl = ddl_path.read_text(encoding="utf-8")
    conn.execute(ddl)
    conn.commit()
    log.info("Tables created/verified")


def sync_stations(client: OPASClient, conn: psycopg.Connection) -> int:
    stations = client.stations()
    log.info("Fetched %d stations from OPAS", len(stations))

    conn.execute("DELETE FROM qvt.opas_stations")
    count = 0
    for s in stations:
        lat = s.get("lat_wgs84")
        lon = s.get("lon_wgs84")
        if lat is None or lon is None:
            continue
        conn.execute(
            """
            INSERT INTO qvt.opas_stations (
                id, eu_code, name, area_class, active, typology, network,
                measure_type, cadence,
                municipality, municipality_istat,
                province, province_istat, province_code,
                region, region_istat, altitude,
                startup_date, dismiss_date,
                geom, synced_at
            ) VALUES (
                %(id)s, %(eu_code)s, %(name)s, %(area_class)s, %(active)s,
                %(typology)s, %(network)s, %(measure_type)s, %(cadence)s,
                %(municipality)s, %(municipality_istat)s,
                %(province)s, %(province_istat)s, %(province_code)s,
                %(region)s, %(region_istat)s, %(altitude)s,
                %(startup_date)s, %(dismiss_date)s,
                ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
                now()
            )
            ON CONFLICT (id) DO UPDATE SET
                eu_code = EXCLUDED.eu_code,
                name = EXCLUDED.name,
                area_class = EXCLUDED.area_class,
                active = EXCLUDED.active,
                typology = EXCLUDED.typology,
                network = EXCLUDED.network,
                measure_type = EXCLUDED.measure_type,
                cadence = EXCLUDED.cadence,
                municipality = EXCLUDED.municipality,
                municipality_istat = EXCLUDED.municipality_istat,
                province = EXCLUDED.province,
                province_istat = EXCLUDED.province_istat,
                province_code = EXCLUDED.province_code,
                region = EXCLUDED.region,
                region_istat = EXCLUDED.region_istat,
                altitude = EXCLUDED.altitude,
                startup_date = EXCLUDED.startup_date,
                dismiss_date = EXCLUDED.dismiss_date,
                geom = EXCLUDED.geom,
                synced_at = now()
            """,
            {
                "id": s["id"],
                "eu_code": s.get("eu_code"),
                "name": s.get("name"),
                "area_class": s.get("area_classification"),
                "active": s.get("active"),
                "typology": s.get("typology_desc"),
                "network": s.get("network_type_desc"),
                "measure_type": s.get("measure_type_desc"),
                "cadence": s.get("cadence_type_desc"),
                "municipality": s.get("municipality_name"),
                "municipality_istat": s.get("municipality_istat_code"),
                "province": s.get("province_name"),
                "province_istat": s.get("province_istat_code"),
                "province_code": s.get("province_code"),
                "region": s.get("region_name"),
                "region_istat": s.get("region_istat_code"),
                "altitude": s.get("altitude"),
                "startup_date": s.get("startup_date"),
                "dismiss_date": s.get("dismiss_date"),
                "lat": lat,
                "lon": lon,
            },
        )
        count += 1

    conn.commit()
    log.info("Synced %d stations to qvt.opas_stations", count)
    return count


def sync_measurements(client: OPASClient, conn: psycopg.Connection) -> int:
    """Fetch latest measurement for key parameters per active station."""

    # Get active stations with coordinates.
    rows = conn.execute(
        "SELECT id, name, region, region_istat, ST_X(geom) AS lon, ST_Y(geom) AS lat "
        "FROM qvt.opas_stations WHERE active = true"
    ).fetchall()
    log.info("Processing measurements for %d active stations", len(rows))

    station_map = {r["id"]: r for r in rows}
    total = 0
    errors = 0

    for idx, (station_id, sinfo) in enumerate(station_map.items()):
        if idx % 50 == 0 and idx > 0:
            log.info("  progress: %d/%d stations", idx, len(station_map))
            conn.commit()

        try:
            series_list = client.series_for_station(station_id)
        except Exception as exc:
            log.warning("Failed to fetch series for station %d: %s", station_id, exc)
            errors += 1
            continue

        # Filter to key air quality parameters, one series per parameter_id.
        seen_params: set[int] = set()
        key_series: list[dict] = []
        for s in series_list:
            pid = s.get("parameter_id")
            if pid in seen_params:
                continue
            if _is_key_param(s.get("parameter_name", "")):
                seen_params.add(pid)
                key_series.append(s)

        for series in key_series:
            series_id = series.get("series_id")
            if not series_id:
                continue
            try:
                data = client.series_data(series_id, hours=48)
            except Exception as exc:
                log.debug("Failed series-data %d: %s", series_id, exc)
                errors += 1
                continue

            measurements = data.get("series_data") or []
            if not measurements:
                continue

            # Take the latest non-null measurement.
            latest = None
            for m in reversed(measurements):
                if m.get("measure_value") is not None:
                    latest = m
                    break
            if not latest:
                continue

            conn.execute(
                """
                INSERT INTO qvt.opas_measurements (
                    station_id, station_name, region, region_istat,
                    parameter_id, parameter_name, parameter_unit,
                    series_id, measure_value, measure_time, quality_code,
                    geom, synced_at
                ) VALUES (
                    %(station_id)s, %(station_name)s, %(region)s, %(region_istat)s,
                    %(parameter_id)s, %(parameter_name)s, %(parameter_unit)s,
                    %(series_id)s, %(measure_value)s, %(measure_time)s, %(quality_code)s,
                    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
                    now()
                )
                ON CONFLICT (station_id, parameter_id) DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    parameter_name = EXCLUDED.parameter_name,
                    parameter_unit = EXCLUDED.parameter_unit,
                    series_id = EXCLUDED.series_id,
                    measure_value = EXCLUDED.measure_value,
                    measure_time = EXCLUDED.measure_time,
                    quality_code = EXCLUDED.quality_code,
                    geom = EXCLUDED.geom,
                    synced_at = now()
                """,
                {
                    "station_id": station_id,
                    "station_name": sinfo["name"],
                    "region": sinfo["region"],
                    "region_istat": sinfo["region_istat"],
                    "parameter_id": series.get("parameter_id"),
                    "parameter_name": data.get("parameter_name") or series.get("parameter_name"),
                    "parameter_unit": data.get("parameter_unit") or series.get("parameter_unit"),
                    "series_id": series_id,
                    "measure_value": latest["measure_value"],
                    "measure_time": latest.get("measure_date_time"),
                    "quality_code": latest.get("measure_code"),
                    "lon": sinfo["lon"],
                    "lat": sinfo["lat"],
                },
            )
            total += 1

        # Small delay to avoid hammering the API.
        time.sleep(0.1)

    conn.commit()
    log.info("Synced %d measurements to qvt.opas_measurements (%d errors)", total, errors)
    return total


def sync_history(
    client: OPASClient,
    conn: psycopg.Connection,
    since: str,
    until: str,
) -> int:
    """Backfill hourly time-series data for key parameters.

    Args:
        since: ISO date start (e.g. "2025-01-01")
        until: ISO date end (e.g. "2026-03-19")
    """
    start_iso = f"{since}T00:00:00"
    end_iso = f"{until}T23:59:59"

    rows = conn.execute(
        "SELECT id, name, region, region_istat, ST_X(geom) AS lon, ST_Y(geom) AS lat "
        "FROM qvt.opas_stations WHERE active = true"
    ).fetchall()
    log.info(
        "History backfill: %d active stations, %s → %s",
        len(rows), since, until,
    )

    station_map = {r["id"]: r for r in rows}
    total_rows = 0
    total_series = 0
    errors = 0
    batch_size = 1000

    insert_sql = """
        INSERT INTO qvt.opas_hourly (
            station_id, parameter_id, measure_time,
            station_name, region, region_istat,
            parameter_name, parameter_unit, series_id,
            measure_value, validity_code, geom
        ) VALUES (
            %(station_id)s, %(parameter_id)s, %(measure_time)s,
            %(station_name)s, %(region)s, %(region_istat)s,
            %(parameter_name)s, %(parameter_unit)s, %(series_id)s,
            %(measure_value)s, %(validity_code)s,
            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
        )
        ON CONFLICT (station_id, parameter_id, measure_time) DO UPDATE SET
            measure_value = EXCLUDED.measure_value,
            validity_code = EXCLUDED.validity_code
    """

    pending: list[dict] = []

    def _flush() -> None:
        nonlocal total_rows
        if not pending:
            return
        with conn.cursor() as cur:
            for p in pending:
                cur.execute(insert_sql, p)
        conn.commit()
        total_rows += len(pending)
        pending.clear()

    for idx, (station_id, sinfo) in enumerate(station_map.items()):
        if idx % 25 == 0:
            log.info(
                "  history progress: %d/%d stations (%d series, %d rows)",
                idx, len(station_map), total_series, total_rows + len(pending),
            )

        try:
            series_list = client.series_for_station(station_id)
        except Exception as exc:
            log.warning("Failed series list for station %d: %s", station_id, exc)
            errors += 1
            continue

        # Deduplicate: one series per parameter_id.
        seen_params: set[int] = set()
        key_series: list[dict] = []
        for s in series_list:
            pid = s.get("parameter_id")
            if pid in seen_params:
                continue
            if _is_key_param(s.get("parameter_name", "")):
                seen_params.add(pid)
                key_series.append(s)

        for series in key_series:
            series_id = series.get("series_id")
            if not series_id:
                continue
            try:
                data = client.series_data_range(series_id, start_iso, end_iso)
            except Exception as exc:
                log.debug("Failed series-data-range %d: %s", series_id, exc)
                errors += 1
                continue

            measurements = data.get("series_data") or []
            if not measurements:
                continue

            param_name = data.get("parameter_name") or series.get("parameter_name")
            param_unit = data.get("parameter_unit") or series.get("parameter_unit")
            param_id = series.get("parameter_id")
            total_series += 1

            for m in measurements:
                val = m.get("measure_value")
                if val is None:
                    continue
                pending.append({
                    "station_id": station_id,
                    "parameter_id": param_id,
                    "measure_time": m["measure_date_time"],
                    "station_name": sinfo["name"],
                    "region": sinfo["region"],
                    "region_istat": sinfo["region_istat"],
                    "parameter_name": param_name,
                    "parameter_unit": param_unit,
                    "series_id": series_id,
                    "measure_value": val,
                    "validity_code": m.get("final_validity_code"),
                    "lon": sinfo["lon"],
                    "lat": sinfo["lat"],
                })
                if len(pending) >= batch_size:
                    _flush()

        # Small delay per station.
        time.sleep(0.05)

    _flush()
    log.info(
        "History backfill complete: %d rows, %d series, %d errors",
        total_rows, total_series, errors,
    )
    return total_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="OPAS ISPRA → PostGIS ETL")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stations", action="store_true", help="Sync stations only")
    group.add_argument("--measurements", action="store_true", help="Sync latest measurements")
    group.add_argument("--all", action="store_true", help="Sync stations + measurements")
    group.add_argument("--init", action="store_true", help="Create tables + full sync")
    group.add_argument("--history", action="store_true", help="Backfill hourly history")
    parser.add_argument("--since", type=str, default="2025-01-01", help="History start date (default: 2025-01-01)")
    parser.add_argument("--until", type=str, default=None, help="History end date (default: today)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    email = os.environ.get("OPAS_EMAIL", "")
    password = os.environ.get("OPAS_PASSWORD", "")
    if not email or not password:
        log.error("Set OPAS_EMAIL and OPAS_PASSWORD environment variables")
        sys.exit(1)

    api_base = os.environ.get("OPAS_API_BASE", "https://opas.isprambiente.it/api/v1")
    client = OPASClient(api_base, email, password)
    conninfo = _build_conninfo()

    if args.until is None:
        from datetime import date
        args.until = date.today().isoformat()

    with psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row) as conn:
        if args.init:
            init_tables(conn)
            sync_stations(client, conn)
            sync_measurements(client, conn)
        elif args.stations:
            sync_stations(client, conn)
        elif args.measurements:
            sync_measurements(client, conn)
        elif args.all:
            sync_stations(client, conn)
            sync_measurements(client, conn)
        elif args.history:
            init_tables(conn)
            sync_history(client, conn, since=args.since, until=args.until)

    log.info("Done")


if __name__ == "__main__":
    main()
