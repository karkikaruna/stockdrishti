#!/usr/bin/env bash
# nepse_runner.sh
# ───────────────
# Wrapper called by cron. Runs backfill first, then the live collector.
# Set NEPSE_DIR to wherever you cloned/placed the scripts.

set -euo pipefail

# ── Resolve script location ───────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Python interpreter ────────────────────────────────────────────────────────
# Prefer a venv if it exists alongside the scripts, else fall back to system python3
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"
if [[ -x "$VENV_PYTHON" ]]; then
    PYTHON="$VENV_PYTHON"
else
    PYTHON="$(command -v python3)"
fi

# ── Run ───────────────────────────────────────────────────────────────────────
"$PYTHON" "$SCRIPT_DIR/nepse_backfill.py"
"$PYTHON" "$SCRIPT_DIR/nepse_collector.py"