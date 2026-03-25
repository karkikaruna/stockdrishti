"""
nepse_backfill.py
─────────────────
On startup, checks the CSV for any trading days that were missed (e.g.
machine was off) and fetches end-of-day closing data for those dates.

Strategy:
  • Find the last date recorded in live_feed.csv.
  • Walk forward day by day up to (but NOT including) today.
  • For any trading day with no data, fetch from merolagani and append.

Run:
    python nepse_backfill.py
"""

import csv
import os
import sys
from datetime import datetime, date, timedelta, UTC
from zoneinfo import ZoneInfo

import requests

# ── Re-use config from collector ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "nepse_data")
CSV_PATH = os.path.join(DATA_DIR, "live_feed.csv")
LOG_PATH = os.path.join(DATA_DIR, "scheduler.log")

WATCHLIST = ["NABIL", "ADBL", "NTC", "SCB", "NICA"]
NPT       = ZoneInfo("Asia/Kathmandu")

CSV_HEADERS = [
    "fetched_at", "symbol", "date",
    "open", "high", "low", "close",
    "volume", "prev_close", "pct_change",
]

NEPALI_HOLIDAYS = {
    "2025-01-14","2025-02-19","2025-03-28","2025-04-02","2025-04-14",
    "2025-05-12","2025-05-29","2025-07-06","2025-08-07","2025-08-08",
    "2025-08-16","2025-09-22","2025-10-01","2025-10-02","2025-10-08",
    "2025-10-09","2025-10-10","2025-10-20","2025-10-21","2025-10-22",
    "2025-10-23","2025-11-05","2025-12-29",
    "2026-01-14","2026-02-07","2026-02-26","2026-03-04","2026-04-14",
    "2026-05-01","2026-05-31",
}

# How many days back to look for a gap (safety cap)
MAX_LOOKBACK_DAYS = 30

CHART_URL = (
    "https://www.merolagani.com/handlers/TechnicalChartHandler.ashx"
    "?type=get_advanced_chart&symbol={sym}&resolution=1D"
    "&rangeStartDate={fr}&rangeEndDate={to}"
    "&from=&isAdjust=1&currencyCode=NPR"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Origin": "https://www.merolagani.com",
    "Referer": "https://www.merolagani.com/",
})

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_npt() -> datetime:
    return datetime.now(NPT)

def log(msg: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    ts   = now_npt().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}]  {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def to_unix(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").timestamp())

def is_trading_day(d: date) -> bool:
    if d.isoweekday() in (5, 6):
        return False
    if d.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS:
        return False
    return True

def ensure_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def append_rows(rows: list[dict]):
    if not rows:
        return
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writerows(rows)

# ── CSV inspection ────────────────────────────────────────────────────────────

def dates_in_csv() -> set[str]:
    """Return the set of date strings already present in the CSV."""
    if not os.path.exists(CSV_PATH):
        return set()
    dates: set[str] = set()
    try:
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d = row.get("date", "").strip()
                if d:
                    dates.add(d)
    except Exception as e:
        log(f"  Warning reading CSV: {e}")
    return dates

def last_recorded_date(recorded: set[str]) -> date | None:
    if not recorded:
        return None
    return max(datetime.strptime(d, "%Y-%m-%d").date() for d in recorded)

# ── Fetching historical candle for a specific date ────────────────────────────

def fetch_ohlcv_for_date(symbol: str, target_date: str) -> dict | None:
    """
    Fetch the candle for `target_date` specifically.
    We ask for a 15-day window around that date so the API returns enough history.
    """
    td   = datetime.strptime(target_date, "%Y-%m-%d")
    fr   = (td - timedelta(days=15)).strftime("%Y-%m-%d")
    to_  = (td + timedelta(days=2)).strftime("%Y-%m-%d")
    url  = CHART_URL.format(sym=symbol, fr=to_unix(fr), to=to_unix(to_) + 86400)

    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log(f"    FETCH ERROR {symbol} ({target_date}): {e}")
        return None

    if data.get("s") != "ok" or not data.get("t"):
        return None

    # Find the index whose timestamp matches target_date
    for i, ts in enumerate(data["t"]):
        day = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")
        if day == target_date:
            try:
                close = float(data["c"][i])
                prev  = float(data["c"][i - 1]) if i > 0 else None
                pct   = round((close - prev) / prev * 100, 2) if prev and prev > 0 else None
                return {
                    "fetched_at": now_npt().strftime("%Y-%m-%d %H:%M:%S") + " [backfill]",
                    "symbol":     symbol,
                    "date":       day,
                    "open":       round(float(data["o"][i]), 2),
                    "high":       round(float(data["h"][i]), 2),
                    "low":        round(float(data["l"][i]), 2),
                    "close":      round(close, 2),
                    "volume":     int(data["v"][i]),
                    "prev_close": round(prev, 2) if prev else "",
                    "pct_change": pct if pct is not None else "",
                }
            except Exception as e:
                log(f"    PARSE ERROR {symbol} ({target_date}): {e}")
                return None

    log(f"    {symbol:<10}  no candle found for {target_date}")
    return None

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    ensure_csv()
    log("── backfill check ─────────────────────────────────────────────")

    recorded = dates_in_csv()
    today    = now_npt().date()

    # Build list of missed trading days (exclusive of today — today is live)
    if recorded:
        last  = last_recorded_date(recorded)
        start = last + timedelta(days=1)
    else:
        # No data at all — look back MAX_LOOKBACK_DAYS
        start = today - timedelta(days=MAX_LOOKBACK_DAYS)

    missed: list[str] = []
    d = start
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if is_trading_day(d) and ds not in recorded:
            missed.append(ds)
        d += timedelta(days=1)

    if not missed:
        log("No missed trading days found. Nothing to backfill.")
        log("── backfill done ──────────────────────────────────────────────")
        return

    log(f"Found {len(missed)} missed trading day(s): {', '.join(missed)}")

    for target_date in missed:
        log(f"  Backfilling {target_date} …")
        batch = []
        for sym in WATCHLIST:
            row = fetch_ohlcv_for_date(sym, target_date)
            if row:
                batch.append(row)
                log(f"    {sym:<10}  close={row['close']}  pct={row['pct_change']}%")
            else:
                log(f"    {sym:<10}  no data for {target_date}")
        append_rows(batch)
        log(f"    → {len(batch)} rows backfilled for {target_date}")

    log("── backfill done ──────────────────────────────────────────────")


if __name__ == "__main__":
    run()