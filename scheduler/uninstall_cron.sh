#!/usr/bin/env bash
# uninstall_cron.sh — removes the NEPSE cron block installed by install_cron.sh

set -euo pipefail

MARKER_START="# ── NEPSE scheduler (managed by install_cron.sh) ────────────────"
MARKER_END="# ── end NEPSE scheduler ─────────────────────────────────────────"

EXISTING=$(crontab -l 2>/dev/null || true)

CLEANED=$(echo "$EXISTING" | awk "
    /$MARKER_START/,/$MARKER_END/ { next }
    { print }
")

echo "$CLEANED" | crontab -
echo "NEPSE cron jobs removed."