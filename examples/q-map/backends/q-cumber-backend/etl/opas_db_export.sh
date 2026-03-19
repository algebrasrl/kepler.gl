#!/usr/bin/env bash
set -euo pipefail

# OPAS PostGIS export/import helper
#
# Export (from local dev):
#   ./etl/opas_db_export.sh dump
#   ./etl/opas_db_export.sh dump --sql    # human-readable SQL instead of binary
#
# Import (to production):
#   PGHOST=prod-host PGUSER=prod-user PGDATABASE=prod-db \
#     ./etl/opas_db_export.sh restore opas_dump.pgdump
#
# The dump includes DDL (tables + partitions + indexes) and data for all
# qvt.opas_* tables (stations, measurements, hourly).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINER="${OPAS_PG_CONTAINER:-q-map-q-cumber-postgis}"
PG_USER="${PGUSER:-qvt}"
PG_DB="${PGDATABASE:-qvt}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%S)"
DUMPS_DIR="${SCRIPT_DIR}/../../q-cumber-postgis/dumps"

usage() {
    echo "Usage:"
    echo "  $0 dump [--sql] [output_file]    Export from local Docker container"
    echo "  $0 restore <dump_file>           Import into target database"
    echo "  $0 info <dump_file>              Show dump contents"
    echo ""
    echo "Environment variables:"
    echo "  OPAS_PG_CONTAINER  Docker container name (default: q-map-q-cumber-postgis)"
    echo "  PGHOST, PGUSER, PGDATABASE, PGPASSWORD  Target DB for restore"
    exit 1
}

cmd_dump() {
    local format="-Fc"
    local ext="pgdump"
    local output=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --sql) format="--inserts"; ext="sql"; shift ;;
            *) output="$1"; shift ;;
        esac
    done

    if [[ -z "$output" ]]; then
        mkdir -p "$DUMPS_DIR"
        output="${DUMPS_DIR}/opas_dump_${TIMESTAMP}.${ext}"
    fi

    echo "Dumping qvt.opas_* from container ${CONTAINER}..."

    if [[ "$format" == "--inserts" ]]; then
        docker exec "$CONTAINER" pg_dump -U "$PG_USER" -d "$PG_DB" \
            --schema=qvt \
            -t 'qvt.opas_*' \
            --no-owner --no-privileges \
            $format \
            > "$output"
    else
        docker exec "$CONTAINER" pg_dump -U "$PG_USER" -d "$PG_DB" \
            --schema=qvt \
            -t 'qvt.opas_*' \
            --no-owner --no-privileges \
            $format \
            > "$output"
    fi

    local size
    size=$(du -h "$output" | cut -f1)
    echo "Dump saved: ${output} (${size})"

    if [[ "$format" == "-Fc" ]]; then
        echo ""
        echo "To inspect:  $0 info ${output}"
        echo "To restore:  PGHOST=host PGUSER=user PGDATABASE=db $0 restore ${output}"
    fi
}

cmd_restore() {
    local dump_file="${1:?Missing dump file}"

    if [[ ! -f "$dump_file" ]]; then
        echo "Error: file not found: ${dump_file}" >&2
        exit 1
    fi

    local host="${PGHOST:?Set PGHOST}"
    local user="${PGUSER:-qvt}"
    local db="${PGDATABASE:-qvt}"

    echo "Target: ${user}@${host}/${db}"
    echo "Creating tables (DDL)..."
    psql -h "$host" -U "$user" -d "$db" -f "${SCRIPT_DIR}/opas_ddl.sql"

    echo "Restoring data from ${dump_file}..."

    if [[ "$dump_file" == *.sql ]]; then
        psql -h "$host" -U "$user" -d "$db" -f "$dump_file"
    else
        pg_restore -h "$host" -U "$user" -d "$db" \
            --no-owner --no-privileges \
            --data-only \
            -j 4 \
            "$dump_file" || true  # partitioned tables may warn, that's OK
    fi

    echo "Verifying..."
    psql -h "$host" -U "$user" -d "$db" -c "
        SELECT 'opas_stations' AS tbl, count(*) AS rows FROM qvt.opas_stations
        UNION ALL
        SELECT 'opas_measurements', count(*) FROM qvt.opas_measurements
        UNION ALL
        SELECT 'opas_hourly', count(*) FROM qvt.opas_hourly;
    "
    echo "Done."
}

cmd_info() {
    local dump_file="${1:?Missing dump file}"
    pg_restore --list "$dump_file" | head -40
    echo "..."
    echo ""
    echo "Total entries: $(pg_restore --list "$dump_file" | wc -l)"
}

case "${1:-}" in
    dump)    shift; cmd_dump "$@" ;;
    restore) shift; cmd_restore "$@" ;;
    info)    shift; cmd_info "$@" ;;
    *)       usage ;;
esac
