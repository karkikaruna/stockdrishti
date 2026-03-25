"""
stock_dashboard/scheduler/report_schedule.py
=============================================
Report scheduler — waits for market close then triggers
report/pdf_generator.py to build the PDF and send the email.

Two modes:
  1. Auto-trigger: called by fetch_schedule.py once market closes at 15:00
  2. Cron fallback at 15:15 NPT (in case fetch_schedule wasn't running):
        crontab -e
        15 15 * * 0-4 cd /path/to/stock_dashboard && python3 -m scheduler.report_schedule >> scheduler/logs/report_cron.log 2>&1

Manual run (from project root):
    python3 -m scheduler.report_schedule
  OR:
    python3 scheduler/report_schedule.py
"""

import os
import sys
import time
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

NPT = ZoneInfo("Asia/Kathmandu")

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCHEDULER_DIR    = os.path.dirname(os.path.abspath(__file__))   # .../scheduler/
PROJECT_ROOT     = os.path.dirname(SCHEDULER_DIR)               # .../stock_dashboard/
PDF_GENERATOR    = os.path.join(PROJECT_ROOT, "report", "pdf_generator.py")
LOG_DIR          = os.path.join(SCHEDULER_DIR, "logs")
LOG_PATH         = os.path.join(LOG_DIR, "report_schedule.log")

# ── Market close time ─────────────────────────────────────────────────────────
MARKET_CLOSE     = (15,  0)
WAIT_AFTER_CLOSE = 5   # minutes to wait after 15:00 before generating report
                       # gives the final candle time to be written to CSV


def now_npt() -> datetime:
    return datetime.now(NPT)

def log(msg: str):
    ts   = now_npt().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}]  {msg}"
    print(line, flush=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def market_closed() -> bool:
    n = now_npt()
    return (n.hour, n.minute) >= MARKET_CLOSE


def wait_for_close():
    """
    Block until MARKET_CLOSE + WAIT_AFTER_CLOSE minutes.
    If we're already past that time, return immediately.
    """
    n          = now_npt()
    close_h, close_m = MARKET_CLOSE
    target_m   = close_m + WAIT_AFTER_CLOSE
    target_h   = close_h + target_m // 60
    target_m   = target_m % 60

    target = n.replace(hour=target_h, minute=target_m, second=0, microsecond=0)

    if n >= target:
        log(f"Already past {target_h:02d}:{target_m:02d} NPT — proceeding immediately.")
        return

    wait_s = int((target - n).total_seconds())
    log(f"Market closes at 15:00 NPT. Will generate report at "
        f"{target_h:02d}:{target_m:02d} NPT "
        f"(in {wait_s//60}m {wait_s%60}s) …")

    while True:
        remaining = int((target - now_npt()).total_seconds())
        if remaining <= 0:
            break
        # Log a reminder every 5 minutes
        if remaining % 300 == 0 and remaining > 0:
            log(f"  Waiting … {remaining//60}m {remaining%60}s until report generation.")
        time.sleep(min(30, remaining))

    log("Wait complete. Triggering report generation …")


def run_pdf_generator() -> bool:
    """
    Invoke report/pdf_generator.py as a subprocess.
    Returns True on success (exit code 0), False otherwise.
    """
    if not os.path.exists(PDF_GENERATOR):
        log(f"ERROR — pdf_generator.py not found at: {PDF_GENERATOR}")
        return False

    log(f"Running: python3 {PDF_GENERATOR}")
    try:
        result = subprocess.run(
            [sys.executable, PDF_GENERATOR],
            cwd=PROJECT_ROOT,
            capture_output=False,   # let output stream live to terminal/cron log
            timeout=300,            # 5-minute hard timeout
        )
        if result.returncode == 0:
            log("pdf_generator.py completed successfully (exit 0).")
            return True
        else:
            log(f"pdf_generator.py exited with code {result.returncode}.")
            return False
    except subprocess.TimeoutExpired:
        log("ERROR — pdf_generator.py timed out after 5 minutes.")
        return False
    except Exception as e:
        log(f"ERROR — failed to run pdf_generator.py: {e}")
        return False


def run():
    log("=" * 60)
    log("report_schedule started")
    log(f"PDF generator → {PDF_GENERATOR}")
    log("=" * 60)

    # Wait until 15:05 NPT (or proceed immediately if already past)
    wait_for_close()

    # Trigger the report
    success = run_pdf_generator()

    if success:
        log("Report schedule complete. PDF generated and email sent.")
    else:
        log("Report schedule finished with errors — check logs above.")

    log("=" * 60)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    run()