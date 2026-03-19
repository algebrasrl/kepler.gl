#!/bin/bash
set -Eeuo pipefail

# Per-dump idempotent restore for PostGIS pgdump files.
#
# Uses marker files in PGDATA/.dump-markers/ to track which dumps
# have been restored. Safe to call on every container start — already
# restored dumps are skipped.

DUMPS_DIR="${QCUMBER_DUMPS_DIR:-/dumps}"
PGHOST="${PGHOST:-/var/run/postgresql}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${POSTGRES_DB:-qvt}"
PGUSER="${POSTGRES_USER:-qvt}"
export PGHOST PGPORT PGDATABASE PGUSER

MARKER_DIR="${PGDATA:-/var/lib/postgresql/data}/.dump-markers"
mkdir -p "$MARKER_DIR"

if [ ! -d "$DUMPS_DIR" ] || [ -z "$(ls -A "$DUMPS_DIR"/*.pgdump 2>/dev/null)" ]; then
    echo "[restore-dumps] No dump files in ${DUMPS_DIR}/ — nothing to do."
    exit 0
fi

restored=0
for dump_file in "$DUMPS_DIR"/*.pgdump; do
    fname="$(basename "$dump_file")"
    marker="$MARKER_DIR/${fname}.done"

    if [ -f "$marker" ]; then
        echo "[restore-dumps] ${fname} already restored — skipping."
        continue
    fi

    echo "[restore-dumps] Restoring ${fname}..."
    pg_restore \
        -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        --no-owner --no-privileges \
        --clean --if-exists \
        "$dump_file" || {
            echo "[restore-dumps] Warning: some errors during restore of ${fname} (usually safe)."
        }
    touch "$marker"
    echo "[restore-dumps] Restored ${fname}."
    restored=$((restored + 1))
done

if [ "$restored" -gt 0 ]; then
    echo "[restore-dumps] Running ANALYZE on qvt schema..."
    psql -c "SELECT 'ANALYZE ' || schemaname || '.' || tablename || ';'
             FROM pg_tables WHERE schemaname = 'qvt'" -tA | psql
    echo "[restore-dumps] Done — restored ${restored} dump(s)."
else
    echo "[restore-dumps] All dumps already restored — nothing to do."
fi
