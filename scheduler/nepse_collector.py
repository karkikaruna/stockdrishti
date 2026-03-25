"""
nepse_collector.py
──────────────────
Fetches OHLCV data for every symbol in WATCHLIST for TODAY and appends
rows to live_feed.csv.  Designed to be called by cron / systemd timers
repeatedly during market hours (e.g. every 4 minutes via cron).

It exits immediately (with a 0 exit code and a log message) when:
  • today is not a trading day
  • current NPT time is outside 11:00–15:00
  • a ±10 % circuit breaker is in effect (day-close flag file present)

Circuit-breaker halt files:
  DATA_DIR/halt_<date>.txt   — created with resume timestamp inside
  DATA_DIR/closed_<date>.txt — created when ±10 % breaker fires
"""

import csv
import os
import sys
from datetime import datetime, date, timedelta, UTC
from zoneinfo import ZoneInfo

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

WATCHLIST = ["NABIL", "ADBL", "NTC", "SCB", "NICA"]

NPT          = ZoneInfo("Asia/Kathmandu")
MARKET_OPEN  = (11, 0)
MARKET_CLOSE = (15, 0)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "nepse_data")
CSV_PATH = os.path.join(DATA_DIR, "live_feed.csv")
LOG_PATH = os.path.join(DATA_DIR, "scheduler.log")

CSV_HEADERS = [
    "fetched_at", "symbol", "date",
    "open", "high", "low", "close",
    "volume", "prev_close", "pct_change",
]

CIRCUIT_BREAKERS = [
    (10.0,   0, True ),   # ±10 % → close for day
    ( 6.0,  40, False),   # ±6  % → 40-min halt
    ( 4.0,  20, False),   # ±4  % → 20-min halt
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Origin": "https://www.merolagani.com",
    "Referer": "https://www.merolagani.com/",
})

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_npt() -> datetime:
    return datetime.now(NPT)

def today_str() -> str:
    return now_npt().strftime("%Y-%m-%d")

def to_unix(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").timestamp())

def is_trading_day(d: date | None = None) -> bool:
    if d is None:
        d = now_npt().date()
    if d.isoweekday() in (5, 6):          # Fri/Sat = weekend in Nepal
        return False
    if d.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS:
        return False
    return True

def market_open_now() -> bool:
    n = now_npt()
    t = (n.hour, n.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE

def log(msg: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    ts   = now_npt().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}]  {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

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

# ── Circuit-breaker flag files ────────────────────────────────────────────────

def halt_file(d: str) -> str:
    return os.path.join(DATA_DIR, f"halt_{d}.txt")

def closed_file(d: str) -> str:
    return os.path.join(DATA_DIR, f"closed_{d}.txt")

def is_day_closed(d: str) -> bool:
    return os.path.exists(closed_file(d))

def is_halted(d: str) -> bool:
    """Returns True if a halt is active right now."""
    path = halt_file(d)
    if not os.path.exists(path):
        return False
    try:
        with open(path) as f:
            resume_ts = f.read().strip()
        resume = datetime.fromisoformat(resume_ts)
        if now_npt() < resume:
            rem = int((resume - now_npt()).total_seconds())
            log(f"  Circuit-breaker halt active — {rem // 60}m {rem % 60}s remaining "
                f"(resumes {resume.strftime('%H:%M')} NPT). Skipping this poll.")
            return True
        else:
            os.remove(path)   # halt expired
            log("  Circuit-breaker halt lifted.")
            return False
    except Exception:
        return False

def set_halt(d: str, minutes: int, threshold: float, pct: float):
    resume = now_npt() + timedelta(minutes=minutes)
    with open(halt_file(d), "w") as f:
        f.write(resume.isoformat())
    log(f"  NEPSE {pct:+.2f}% — ±{threshold:.0f}% breaker → "
        f"{minutes}-min halt until {resume.strftime('%H:%M')} NPT")

def set_closed(d: str, pct: float):
    with open(closed_file(d), "w") as f:
        f.write(f"closed at {now_npt().isoformat()} due to ±10% circuit breaker ({pct:+.2f}%)\n")
    log(f"  NEPSE {pct:+.2f}% — ±10% breaker → market closed for today.")

# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str, target_date: str | None = None) -> dict | None:
    """
    Fetch the latest daily candle for `symbol`.
    If target_date is given, only return data if the candle matches that date.
    """
    n  = now_npt()
    td = n.strftime("%Y-%m-%d")
    yd = (n.date() - timedelta(days=10)).strftime("%Y-%m-%d")
    url = CHART_URL.format(sym=symbol, fr=to_unix(yd), to=to_unix(td) + 86400)

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
        prev  = float(data["c"][idx - 1]) if idx > 0 else None
        pct   = round((close - prev) / prev * 100, 2) if prev and prev > 0 else None
        ts    = data["t"][idx]
        day   = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")

        check_date = target_date or today_str()
        if day != check_date:
            log(f"  {symbol:<10}  latest candle is {day}, expected {check_date} — skipping")
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

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    ensure_csv()
    log("── collector invoked ──────────────────────────────────────────")

    # 1. Is today a trading day?
    if not is_trading_day():
        n   = now_npt()
        msg = "holiday" if n.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS else "weekend"
        log(f"Today is a {msg}. Nothing to do.")
        sys.exit(0)

    today = today_str()

    # 2. Is market currently open?
    if not market_open_now():
        log("Outside market hours (11:00–15:00 NPT). Nothing to do.")
        sys.exit(0)

    # 3. Check circuit-breaker flags
    if is_day_closed(today):
        log("Market closed today by ±10% circuit breaker. Nothing to do.")
        sys.exit(0)

    if is_halted(today):
        sys.exit(0)

    # 4. Fetch all symbols
    log(f"Fetching {len(WATCHLIST)} symbols …")
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
    log(f"  → {len(batch)} rows appended")

    # 5. Check NEPSE index for circuit breaker
    idx_pct = fetch_nepse_pct()
    if idx_pct is not None:
        for threshold, halt_min, closes_day in CIRCUIT_BREAKERS:
            if abs(idx_pct) >= threshold:
                if closes_day:
                    set_closed(today, idx_pct)
                else:
                    set_halt(today, halt_min, threshold, idx_pct)
                break

    log("── collector done ─────────────────────────────────────────────")


if __name__ == "__main__":
    run()