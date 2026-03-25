#!/usr/bin/env bash
# install_cron.sh
# ───────────────────────────────────────────────────────────────────────────────
# Installs two cron entries:
#
#   1. Market-hours poller  — runs every 4 minutes, Sun–Thu, 10:55–15:05 NPT
#      (The collector itself exits instantly outside 11:00–15:00, so the
#       10:55 start just ensures the first poll is captured promptly.)
#
#   2. End-of-day backfill — runs once at 15:10 NPT every day to catch any
#      gap if the machine was off during the day.
#
# Nepal Standard Time = UTC+5:45.  All cron times are in UTC.
#   11:00 NPT = 05:15 UTC
#   15:00 NPT = 09:15 UTC
#   15:10 NPT = 09:25 UTC
#   10:55 NPT = 05:10 UTC
#
# Cron runs in UTC by default on most Linux distros.
# ───────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/nepse_runner.sh"
LOG="$SCRIPT_DIR/nepse_data/cron.log"

# Make sure the runner is executable
chmod +x "$RUNNER"

# ── Build cron lines ──────────────────────────────────────────────────────────
# Cron day-of-week: 0=Sun 1=Mon 2=Tue 3=Wed 4=Thu 5=Fri 6=Sat
# Nepal market is open Sun–Thu.

# Poll every 4 min from 05:10 UTC to 09:15 UTC (10:55–15:00 NPT), Sun–Thu
POLL_ENTRY="*/4 5-9 * * 0-4   $RUNNER >> $LOG 2>&1"

# End-of-day / missed-day backfill at 09:25 UTC (15:10 NPT) every day
EOD_ENTRY="25 9 * * *          $RUNNER >> $LOG 2>&1"

# ── Inject into crontab (idempotent) ─────────────────────────────────────────
# Read existing crontab (ignore error if empty)
EXISTING=$(crontab -l 2>/dev/null || true)

MARKER_START="# ── NEPSE scheduler (managed by install_cron.sh) ────────────────"
MARKER_END="# ── end NEPSE scheduler ─────────────────────────────────────────"

# Strip old NEPSE block if present
CLEANED=$(echo "$EXISTING" | awk "
    /$MARKER_START/,/$MARKER_END/ { next }
    { print }
")

NEW_BLOCK=$(cat <<EOF
$MARKER_START
$POLL_ENTRY
$EOD_ENTRY
$MARKER_END
EOF
)

(echo "$CLEANED"; echo "$NEW_BLOCK") | crontab -

echo "────────────────────────────────────────────────────────────────"
echo "  NEPSE cron jobs installed successfully."
echo ""
echo "  Active entries:"
crontab -l | grep -A5 "NEPSE"
echo ""
echo "  Cron log → $LOG"
echo "  Collector log → $SCRIPT_DIR/nepse_data/scheduler.log"
echo ""
echo "  To remove: run uninstall_cron.sh  (or edit crontab -e manually)"
echo "────────────────────────────────────────────────────────────────"

