#!/bin/bash
set -Eeuo pipefail

# First-init restore: delegates to the shared per-dump restore script.
# On subsequent restarts, entrypoint-wrapper.sh handles incremental restores.

/usr/local/bin/restore-dumps.sh
