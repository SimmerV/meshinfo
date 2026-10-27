#!/usr/bin/env bash
# Host/cron entry point: verified local or SSH-streamed PostgreSQL snapshot.
# Requires Python 3.11+ and Docker. Configuration and legacy environment
# overrides are parsed without eval by backup_database.py; see STORAGE.md.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$REPO_ROOT/scripts/backup_database.py" "$@"
