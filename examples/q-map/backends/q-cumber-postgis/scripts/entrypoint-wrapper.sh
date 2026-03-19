#!/bin/bash
set -e

# Wrapper around the official postgres entrypoint.
#
# On restart (PGDATA already initialized), runs incremental dump restore
# in the background after postgres accepts connections.
# On first init, docker-entrypoint-initdb.d scripts handle the restore.

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ -s "${PGDATA}/PG_VERSION" ]; then
    # DB already initialized — check for new dumps after postgres is ready
    (
        until pg_isready -h /var/run/postgresql \
              -U "${POSTGRES_USER:-qvt}" \
              -d "${POSTGRES_DB:-qvt}" 2>/dev/null; do
            sleep 2
        done
        /usr/local/bin/restore-dumps.sh
    ) &
fi

# Run the official postgres entrypoint (PID 1 for signal handling)
exec docker-entrypoint.sh "$@"
