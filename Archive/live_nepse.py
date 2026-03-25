"""
NEPSE Live ETL  ─  Terminal Edition
=====================================
Polls merolagani chart API every 3-5 minutes (matching their cache refresh).
Handles Nepali holidays, trading hours (11:00-15:00 NPT, Sun-Thu),
and all three NEPSE circuit breaker tiers.

Usage:
    python nepse_live.py
"""

import os
import sys
import time
import random
import signal
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

# ── ANSI colours ──────────────────────────────────────────────────────────────
R   = "\033[0;31m"
G   = "\033[0;32m"
Y   = "\033[0;33m"
B   = "\033[0;34m"
C   = "\033[0;36m"
W   = "\033[1;37m"
DIM = "\033[2m"
RST = "\033[0m"
CLR = "\033[2J\033[H"

NPT = ZoneInfo("Asia/Kathmandu")

# ── Market constants ──────────────────────────────────────────────────────────
MARKET_OPEN  = (11,  0)   # 11:00 AM NPT
MARKET_CLOSE = (15,  0)   # 03:00 PM NPT
POLL_MIN     = 180        # 3 minutes (merolagani cache lower bound)
POLL_MAX     = 300        # 5 minutes (merolagani cache upper bound)

# ── Circuit breaker tiers (NEPSE official rules) ─────────────────────────────
# (threshold_pct, halt_minutes, day_close)
CIRCUIT_BREAKERS = [
    (10.0,  0,   True),   # ±10% → market closed for the day
    ( 6.0, 40,  False),   # ±6%  → 40-minute halt
    ( 4.0, 20,  False),   # ±4%  → 20-minute halt
]

# ── Nepali public holidays 2025-2026 (AD dates) ──────────────────────────────
# Source: Nepal Stock Exchange official circular + GON public holidays
NEPALI_HOLIDAYS = {
    "2026-01-14",  # Maghe Sankranti
    "2026-02-07",  # Sonam Losar (Tamang New Year)
    "2026-02-26",  # Maha Shivaratri (approx)
    "2026-03-04",  # Fagu Purnima
    "2026-04-14",  # Nepali New Year (Baisakh 1, 2083)
    "2026-05-01",  # Labour Day
    "2026-05-31",  # Buddha Jayanti
    "2026-05-01",  # Labor Day / Buddha Jayanti
    "2026-05-29",  # Republic Day
    "2026-08-28",  # Janai Purnima
    "2026-09-04",  # Krishna Janmashtami
    "2026-09-19",  # Constitution Day
    "2026-10-11",  # Ghatasthapana (Dashain)
    "2026-10-17",  # Phulpati (Dashain)
    "2026-10-19",  # Maha Astami (Dashain)
    "2026-10-20",  # Maha Nawami (Dashain)
    "2026-10-21",  # Vijaya Dashami (Dashain)
    "2026-10-22",  # Ekadashi (Dashain holiday)
    "2026-10-23",  # Duwadashi (Dashain holiday)
    "2026-11-08",  # Laxmi Puja (Tihar)
    "2026-11-09",  # Govardhan Puja (Tihar)
    "2026-11-10",  # Bhai Tika (Tihar)
    "2026-11-15",  # Chhath Parva
    "2026-12-30",  # Tamu Lhosar
}

# ── API ───────────────────────────────────────────────────────────────────────
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

_stop = False

def now_npt() -> datetime:
    return datetime.now(NPT)

def to_unix(d: str) -> int:
    return int(datetime.strptime(d, "%Y-%m-%d").timestamp())

def is_trading_day(d: date | None = None) -> bool:
    if d is None:
        d = now_npt().date()
    # NEPSE trades Sun–Thu  (isoweekday: Sun=7, Mon=1 … Thu=4, Fri=5, Sat=6)
    if d.isoweekday() in (5, 6):      # Friday, Saturday → closed
        return False
    if d.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS:
        return False
    return True

def market_status() -> str:
    """Return 'before', 'open', or 'after'."""
    n = now_npt()
    t = (n.hour, n.minute)
    if t < MARKET_OPEN:
        return "before"
    if t >= MARKET_CLOSE:
        return "after"
    return "open"

def next_trading_open() -> datetime:
    """Datetime of the next market open (NPT)."""
    n = now_npt()
    d = n.date()
    # If today is trading day and market hasn't opened yet
    if is_trading_day(d) and market_status() == "before":
        return datetime(d.year, d.month, d.day,
                        MARKET_OPEN[0], MARKET_OPEN[1], tzinfo=NPT)
    # Otherwise find next trading day
    d += timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return datetime(d.year, d.month, d.day,
                    MARKET_OPEN[0], MARKET_OPEN[1], tzinfo=NPT)

def countdown_str(target: datetime) -> str:
    delta = int((target - now_npt()).total_seconds())
    if delta < 0:
        return "now"
    h, r = divmod(delta, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m:02d}m {s:02d}s"


def fetch_ohlcv(symbol: str) -> dict | None:
    """Fetch latest candle + previous close for a symbol."""
    n   = now_npt()
    td  = n.strftime("%Y-%m-%d")
    yd  = (n.date() - timedelta(days=10)).strftime("%Y-%m-%d")
    fr  = to_unix(yd)
    to  = to_unix(td) + 86400
    url = CHART_URL.format(sym=symbol, fr=fr, to=to)

    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"symbol": symbol, "error": str(e)[:60]}

    if data.get("s") != "ok" or not data.get("t"):
        return {"symbol": symbol, "error": "no data"}

    try:
        c_list = data["c"]
        last   = len(c_list) - 1
        close  = float(c_list[last])
        prev   = float(c_list[last - 1]) if last > 0 else None
        pct    = round((close - prev) / prev * 100, 2) if prev and prev > 0 else None

        ts  = data["t"][last]
        day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

        return {
            "symbol":   symbol,
            "date":     day,
            "open":     float(data["o"][last]),
            "high":     float(data["h"][last]),
            "low":      float(data["l"][last]),
            "close":    close,
            "volume":   int(data["v"][last]),
            "prev":     prev,
            "pct":      pct,
            "at":       now_npt().strftime("%H:%M:%S"),
        }
    except Exception as e:
        return {"symbol": symbol, "error": f"parse: {e}"}


def fetch_nepse_pct() -> float | None:
    """Fetch today's NEPSE index % change for circuit breaker detection."""
    result = fetch_ohlcv("NEPSE")
    if result and "pct" in result:
        return result["pct"]
    return None


def clr():
    sys.stdout.write(CLR)
    sys.stdout.flush()

def banner():
    print(f"""{C}
 ███╗   ██╗███████╗██████╗ ███████╗███████╗  ██╗     ██╗██╗   ██╗███████╗
 ████╗  ██║██╔════╝██╔══██╗██╔════╝██╔════╝  ██║     ██║██║   ██║██╔════╝
 ██╔██╗ ██║█████╗  ██████╔╝███████╗█████╗    ██║     ██║██║   ██║█████╗
 ██║╚██╗██║██╔══╝  ██╔═══╝ ╚════██║██╔══╝    ██║     ██║╚██╗ ██╔╝██╔══╝
 ██║ ╚████║███████╗██║     ███████║███████╗  ███████╗██║ ╚████╔╝ ███████╗
 ╚═╝  ╚═══╝╚══════╝╚═╝     ╚══════╝╚══════╝  ╚══════╝╚═╝  ╚═══╝ ╚══════╝
{RST}{DIM}  Terminal ETL  ·  merolagani chart API  ·  3-5 min cache-matched polling{RST}
""")

def fmt_pct(pct: float | None) -> str:
    if pct is None:
        return f"{DIM}    —   {RST}"
    if pct > 0:
        return f"{G}▲ +{pct:5.2f}%{RST}"
    if pct < 0:
        return f"{R}▼ {pct:6.2f}%{RST}"
    return f"{DIM}  {pct:5.2f}% {RST}"

def render(results: dict, poll: int, next_at: datetime,
           halt_msg: str | None, day_closed: bool):
    clr()
    n  = now_npt()
    st = market_status()

    status_tag = {
        "before": f"{Y}⏳ PRE-MARKET{RST}",
        "open":   f"{G}● LIVE{RST}",
        "after":  f"{DIM}■ CLOSED{RST}",
    }[st]

    print(f"\n  {C}NEPSE Live ETL{RST}  ·  "
          f"{W}{n.strftime('%a %d %b %Y  %H:%M:%S')} NPT{RST}  ·  {status_tag}")

    if day_closed:
        print(f"  {R}⛔  MARKET CLOSED FOR TODAY  (±10% circuit breaker triggered){RST}")
    elif halt_msg:
        print(f"  {R}⚠   CIRCUIT BREAKER HALT  —  {halt_msg}{RST}")
    else:
        cd = countdown_str(next_at)
        print(f"  {DIM}Poll #{poll}  ·  next fetch in {cd}  ·  Ctrl+C to quit{RST}")

    # ── Table ─────────────────────────────────────────────────────────────────
    W_SYM, W_DATE, W_NUM, W_VOL, W_PCT = 10, 12, 9, 12, 12
    sep = "  " + "─" * (W_SYM + W_DATE + W_NUM*4 + W_VOL + W_PCT + 14)
    hdrs = ["SYMBOL", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "CHANGE"]
    widths = [W_SYM, W_DATE, W_NUM, W_NUM, W_NUM, W_NUM, W_VOL, W_PCT]

    print(f"\n{sep}")
    print("  " + "  ".join(f"{W}{h:<{w}}{RST}" for h, w in zip(hdrs, widths)))
    print(sep)

    if not results:
        print(f"  {DIM}  Waiting for first fetch …{RST}")
    else:
        for sym, d in results.items():
            if d is None:
                print(f"  {Y}{sym:<{W_SYM}}{RST}  {DIM}—{RST}")
                continue
            if "error" in d:
                print(f"  {R}{sym:<{W_SYM}}{RST}  {DIM}{d['error']}{RST}")
                continue

            pct = d.get("pct")
            cc  = G if (pct or 0) > 0 else (R if (pct or 0) < 0 else W)

            print(
                f"  {W}{d['symbol']:<{W_SYM}}{RST}"
                f"  {DIM}{d['date']:<{W_DATE}}{RST}"
                f"  {d['open']:<{W_NUM}.2f}"
                f"  {d['high']:<{W_NUM}.2f}"
                f"  {d['low']:<{W_NUM}.2f}"
                f"  {cc}{d['close']:<{W_NUM}.2f}{RST}"
                f"  {d['volume']:<{W_VOL},}"
                f"  {fmt_pct(pct)}"
            )

    print(sep)
    if results:
        last_at = next(
            (v["at"] for v in results.values() if v and "at" in v), "—"
        )
        print(f"  {DIM}Last fetched: {last_at} NPT{RST}\n")



def ask_symbols() -> list[str]:
    clr()
    banner()
    print(f"{W}  ── Watchlist Setup ──────────────────────────────────{RST}")
    print(f"{DIM}  Enter stock symbols separated by commas or spaces.")
    print(f"  Examples:  NABIL, ADBL, SCB, NTC, NICA, UPPER{RST}\n")

    while True:
        raw = input(f"  {C}Symbols >{RST} ").strip()
        if not raw:
            print(f"  {Y}  Please enter at least one symbol.{RST}")
            continue

        symbols = [s.strip().upper()
                   for s in raw.replace(",", " ").split() if s.strip()]
        symbols = list(dict.fromkeys(symbols))   # dedupe, preserve order

        print(f"\n  {DIM}Watchlist — {len(symbols)} symbol(s):{RST}")
        for s in symbols:
            print(f"    {C}·{RST} {s}")

        yn = input(f"\n  {W}Start ETL? [Y/n]:{RST} ").strip().lower()
        if yn in ("", "y", "yes"):
            return symbols
        print()



def wait_screen():
    """Show a live countdown until next market open."""
    global _stop
    while not _stop:
        n    = now_npt()
        nxt  = next_trading_open()
        cd   = countdown_str(nxt)
        td   = is_trading_day()
        st   = market_status()

        clr()
        banner()
        if not td:
            day_name = n.strftime("%A")
            print(f"  {Y}Today ({day_name}) is not a trading day.{RST}")
            if n.strftime("%Y-%m-%d") in NEPALI_HOLIDAYS:
                print(f"  {DIM}  (public holiday){RST}")
        elif st == "before":
            print(f"  {Y}Market opens at 11:00 AM NPT today.{RST}")
        else:
            print(f"  {DIM}Market closed for today.{RST}")

        print(f"\n  {W}Next market open:{RST}  "
              f"{C}{nxt.strftime('%a %d %b %Y  11:00 AM NPT')}{RST}")
        print(f"  {DIM}Opens in: {cd}{RST}")
        print(f"\n  {DIM}Press Ctrl+C to quit, or wait …{RST}")

        # Check every second; break as soon as market opens
        for _ in range(60):
            if _stop:
                return
            if is_trading_day() and market_status() == "open":
                return
            time.sleep(1)



def run_etl(symbols: list[str]):
    global _stop

    def handle_exit(sig, frame):
        global _stop
        _stop = True

    signal.signal(signal.SIGINT, handle_exit)

    results    : dict        = {}
    poll        = 0
    halt_until  = None       # datetime when halt ends
    halt_msg    = None       # string shown in header
    day_closed  = False      # ±10% → closed for day

    while not _stop:
        # ── Check if we should be running ────────────────────────────────────
        if not is_trading_day():
            wait_screen()
            continue
        if market_status() == "before":
            wait_screen()
            continue
        if market_status() == "after" or day_closed:
            clr()
            banner()
            if day_closed:
                print(f"  {R}⛔  Market closed for today due to ±10% circuit breaker.{RST}")
            else:
                print(f"  {DIM}Market has closed for today (15:00 NPT).{RST}")
            nxt = next_trading_open()
            print(f"\n  {W}Next open:{RST} {C}{nxt.strftime('%a %d %b — 11:00 AM NPT')}{RST}")
            print(f"  {DIM}Opens in: {countdown_str(nxt)}{RST}\n")
            print(f"  {DIM}Press Ctrl+C to quit.{RST}")
            time.sleep(5)
            continue

        # ── Circuit breaker: still halted? ───────────────────────────────────
        now = now_npt()
        if halt_until and now < halt_until:
            rem_s = int((halt_until - now).total_seconds())
            m, s  = divmod(rem_s, 60)
            render(results, poll, halt_until, halt_msg, day_closed)
            print(f"  {R}  Resuming in {m:02d}m {s:02d}s …{RST}")
            time.sleep(1)
            continue
        elif halt_until and now >= halt_until:
            halt_until = None
            halt_msg   = None

        # ── Fetch ─────────────────────────────────────────────────────────────
        poll += 1
        for sym in symbols:
            if _stop:
                break
            results[sym] = fetch_ohlcv(sym)

        # ── Circuit breaker check ─────────────────────────────────────────────
        idx_pct = fetch_nepse_pct()
        if idx_pct is not None:
            for threshold, halt_min, closes_day in CIRCUIT_BREAKERS:
                if abs(idx_pct) >= threshold:
                    if closes_day:
                        day_closed = True
                        halt_msg   = None
                    else:
                        halt_until = now_npt() + timedelta(minutes=halt_min)
                        halt_msg   = (
                            f"NEPSE {idx_pct:+.2f}%  →  {halt_min}-min halt  "
                            f"(resumes ~{halt_until.strftime('%H:%M')} NPT)"
                        )
                    break

        # ── Decide next poll time (3–5 min jitter) ────────────────────────────
        interval = random.randint(POLL_MIN, POLL_MAX)
        next_at  = now_npt() + timedelta(seconds=interval)

        render(results, poll, next_at, halt_msg, day_closed)

        # ── Countdown wait ────────────────────────────────────────────────────
        for i in range(interval):
            if _stop:
                break
            # Re-check market close every tick
            if market_status() == "after":
                break
            remaining = interval - i
            sys.stdout.write(
                f"\r  {DIM}Next fetch in {remaining:3d}s  "
                f"·  poll #{poll}  ·  Ctrl+C to quit{RST}   "
            )
            sys.stdout.flush()
            time.sleep(1)

    print(f"\n\n  {C}ETL session ended.{RST}  {DIM}Goodbye.{RST}\n")



def main():
    try:
        symbols = ask_symbols()
        run_etl(symbols)
    except KeyboardInterrupt:
        print(f"\n\n  {DIM}Interrupted. Goodbye.{RST}\n")
        sys.exit(0)

if __name__ == "__main__":
    main()