"""
stock_dashboard/scheduler/fetch_schedule.py
=============================================
Automated data-fetch scheduler — polls merolagani every 3-5 min
during market hours and writes rows to data/nepse_data/live_feed.csv.

Cron setup (run from project root, 10:50 AM NPT daily):
    crontab -e
    50 10 * * * cd /path/to/stock_dashboard && python3 -m scheduler.fetch_schedule >> scheduler/logs/fetch_cron.log 2>&1

Manual run (from project root):
    python3 -m scheduler.fetch_schedule
  OR:
    python3 scheduler/fetch_schedule.py
"""

import csv
import os
import sys
import random
import time
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

# ── Resolve paths relative to project root ────────────────────────────────────
# This file lives at stock_dashboard/scheduler/fetch_schedule.py
# Project root  =  stock_dashboard/
SCHEDULER_DIR = os.path.dirname(os.path.abspath(__file__))   # .../scheduler/
PROJECT_ROOT  = os.path.dirname(SCHEDULER_DIR)               # .../stock_dashboard/

DATA_DIR  = os.path.join(PROJECT_ROOT, "data", "nepse_data")
CSV_PATH  = os.path.join(DATA_DIR, "live_feed.csv")
LOG_DIR   = os.path.join(SCHEDULER_DIR, "logs")
LOG_PATH  = os.path.join(LOG_DIR, "fetch_schedule.log")

# ── Config ────────────────────────────────────────────────────────────────────
WATCHLIST = ["NABIL", "ADBL", "NTC", "SCB", "NICA"]

NPT          = ZoneInfo("Asia/Kathmandu")
MARKET_OPEN  = (11,  0)
MARKET_CLOSE = (15,  0)
POLL_MIN     = 180   # 3 min
POLL_MAX     = 300   # 5 min

CSV_HEADERS = [
    "fetched_at", "symbol", "date",
    "open", "high", "low", "close",
    "volume", "prev_close", "pct_change",
]

CIRCUIT_BREAKERS = [
    (10.0,  0, True ),
    ( 6.0, 40, False),
    ( 4.0, 20, False),
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

CHART_URL = (
    "https://www.merolagani.com/handlers/TechnicalChartHandler.ashx"
    "?type=get_advanced_chart&symbol={sym}&resolution=1D"
    "&rangeStartDate={fr}&rangeEndDate={to}"
    "&from=&isAdjust=1&currencyCode=NPR"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Origin": "https://www.merolagani.com",
    "Referer": "https://www.merolagani.com/",
})


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def now_npt() -> datetime:
    return datetime.now(NPT)

def to_unix(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").timestamp())

def is_trading_day(d: date | None = None) -> bool:
    if d is None:
        d = now_npt().date()
    if d.isoweekday() in (5, 6):
        return False
    if d.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS:
        return False
    return True

def market_status() -> str:
    n = now_npt()
    t = (n.hour, n.minute)
    if t < MARKET_OPEN:
        return "before"
    if t >= MARKET_CLOSE:
        return "after"
    return "open"

def log(msg: str):
    ts   = now_npt().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}]  {msg}"
    print(line, flush=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CSV
# ─────────────────────────────────────────────────────────────────────────────

def ensure_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()
        log(f"Created CSV → {CSV_PATH}")

def append_rows(rows: list[dict]):
    if not rows:
        return
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        w.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str) -> dict | None:
    n   = now_npt()
    td  = n.strftime("%Y-%m-%d")
    yd  = (n.date() - timedelta(days=10)).strftime("%Y-%m-%d")
    url = CHART_URL.format(sym=symbol, fr=to_unix(yd), to=to_unix(td)+86400)

    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log(f"  FETCH ERROR {symbol}: {e}")
        return None

    if data.get("s") != "ok" or not data.get("t"):
        return None

    try:
        idx   = len(data["c"]) - 1
        close = float(data["c"][idx])
        prev  = float(data["c"][idx-1]) if idx > 0 else None
        pct   = round((close - prev) / prev * 100, 2) if prev and prev > 0 else None
        ts    = data["t"][idx]
        day   = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

        today = now_npt().strftime("%Y-%m-%d")
        if day != today:
            log(f"  {symbol:<10}  latest candle is {day}, not today ({today}) — skipping")
            return None

        return {
            "fetched_at": now_npt().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol":     symbol,
            "date":       day,
            "open":       round(float(data["o"][idx]), 2),
            "high":       round(float(data["h"][idx]), 2),
            "low":        round(float(data["l"][idx]), 2),
            "close":      round(close, 2),
            "volume":     int(data["v"][idx]),
            "prev_close": round(prev, 2) if prev else "",
            "pct_change": pct if pct is not None else "",
        }
    except Exception as e:
        log(f"  PARSE ERROR {symbol}: {e}")
        return None

def fetch_nepse_pct() -> float | None:
    result = fetch_ohlcv("NEPSE")
    if result and result.get("pct_change") != "":
        try:
            return float(result["pct_change"])
        except Exception:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main scheduler loop
# ─────────────────────────────────────────────────────────────────────────────

def run():
    ensure_csv()
    log("=" * 60)
    log(f"fetch_schedule started  |  watchlist: {', '.join(WATCHLIST)}")
    log(f"CSV  → {CSV_PATH}")
    log(f"Log  → {LOG_PATH}")
    log("=" * 60)

    if not is_trading_day():
        n   = now_npt()
        msg = "holiday" if n.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS else "weekend"
        log(f"Today is a {msg}. Nothing to do. Exiting.")
        sys.exit(0)

    if market_status() == "before":
        n = now_npt()
        open_time = n.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1],
                               second=0, microsecond=0)
        wait_s = max(0, int((open_time - n).total_seconds()))
        if wait_s > 0:
            log(f"Market not open yet. Waiting {wait_s//60}m {wait_s%60}s "
                f"until 11:00 AM NPT …")
            time.sleep(wait_s)

    if market_status() == "after":
        log("Market already closed for today. Exiting.")
        sys.exit(0)

    halt_until = None
    day_closed = False
    poll       = 0

    while True:
        if market_status() == "after" or day_closed:
            log("Market closed. fetch_schedule done for today.")
            break

        now = now_npt()
        if halt_until and now < halt_until:
            rem = int((halt_until - now).total_seconds())
            log(f"  Circuit breaker active — resuming in {rem//60}m {rem%60}s")
            time.sleep(min(60, rem))
            continue
        elif halt_until and now >= halt_until:
            log("  Circuit breaker lifted. Resuming normal polling.")
            halt_until = None

        poll += 1
        log(f"Poll #{poll}  |  fetching {len(WATCHLIST)} symbols …")
        batch = []
        for sym in WATCHLIST:
            row = fetch_ohlcv(sym)
            if row:
                batch.append(row)
                log(f"  {sym:<10}  close={row['close']}  "
                    f"pct={row['pct_change']}%  vol={row['volume']:,}")
            else:
                log(f"  {sym:<10}  no data")

        append_rows(batch)
        log(f"  → {len(batch)} rows appended to {CSV_PATH}")

        idx_pct = fetch_nepse_pct()
        if idx_pct is not None:
            for threshold, halt_min, closes_day in CIRCUIT_BREAKERS:
                if abs(idx_pct) >= threshold:
                    if closes_day:
                        log(f" NEPSE {idx_pct:+.2f}% — ±10% breaker. "
                            "Market closed for today.")
                        day_closed = True
                    else:
                        halt_until = now_npt() + timedelta(minutes=halt_min)
                        log(f" NEPSE {idx_pct:+.2f}% — ±{threshold:.0f}% breaker. "
                            f"{halt_min}-min halt until "
                            f"{halt_until.strftime('%H:%M')} NPT")
                    break

        if day_closed:
            break

        interval = random.randint(POLL_MIN, POLL_MAX)
        log(f"  Next poll in {interval//60}m {interval%60}s")
        for _ in range(interval):
            if market_status() == "after":
                log("  Market closed mid-sleep — stopping early.")
                break
            time.sleep(1)

    log("fetch_schedule exiting.")


if __name__ == "__main__":
    run()