"""
NEPSE Daily Report Generator
==============================
Reads live_feed.csv, generates PDF reports, emails them,
and manages CSV data lifecycle around market open/close.

THREE MODES
-----------
1. prev   (or auto-detect before 10:55 AM NPT)
   Generates a report from YESTERDAY's data still in live_feed.csv,
   emails it, then exits.  Run this before market opens.

2. cleanup  (or auto at 10:55–11:05 AM)
   Archives yesterday's CSV rows to nepse_data/archive/YYYY-MM-DD.csv,
   then wipes live_feed.csv clean (header only) ready for today's fetch.

3. today  (default — called by Task Scheduler at 3:50 PM)
   Generates today's report from live_feed.csv and emails it.

TASK SCHEDULER SETUP (Windows)
--------------------------------
  10:40 AM  →  python report.py prev      # send yesterday's PDF
  10:58 AM  →  python report.py cleanup   # clear CSV for today
  03:50 PM  →  python report.py today     # send today's PDF

Manual usage:
    python report.py              # auto-mode (picks mode by NPT time)
    python report.py prev
    python report.py cleanup
    python report.py today

.env file (place in same directory as report.py):
    EMAIL_SENDER=your_email@gmail.com
    EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
    EMAIL_RECEIVER=receiver@example.com
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
"""

import os
import sys
import smtplib
import argparse
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta, date as date_cls
from zoneinfo import ZoneInfo

# ── Load .env ─────────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

# ── Email Configuration (from .env) ───────────────────────────────────────────
EMAIL_SENDER   = os.getenv("EMAIL_SENDER",   "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "")
SMTP_HOST      = os.getenv("SMTP_HOST",      "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT",  "587"))

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Image as RLImage, HRFlowable, PageBreak, Table, TableStyle
)

# ── Paths ─────────────────────────────────────────────────────────────────────
NPT         = ZoneInfo("Asia/Kathmandu")
BASE_DIR    = r"C:\Codes\final_etl"
DATA_DIR    = os.path.join(BASE_DIR, "nepse_data")
CSV_PATH    = os.path.join(DATA_DIR, "live_feed.csv")
OUT_DIR     = os.path.join(DATA_DIR, "reports")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")

# Header line written when resetting the CSV
CSV_HEADERS = "fetched_at,symbol,date,open,high,low,close,volume,prev_close,pct_change\n"

# ── Market hours ──────────────────────────────────────────────────────────────
MARKET_OPEN  = (11, 0)
MARKET_CLOSE = (15, 0)

# ── Colours ───────────────────────────────────────────────────────────────────
BRAND_DARK  = colors.HexColor("#0d1b2a")
BRAND_BLUE  = colors.HexColor("#1565c0")
BRAND_LIGHT = colors.HexColor("#e3f2fd")
GREEN       = colors.HexColor("#2e7d32")
RED         = colors.HexColor("#c62828")
GREY        = colors.HexColor("#607d8b")

CIRCUIT_BREAKERS = [
    (10.0,  0,   True),
    ( 6.0, 40,  False),
    ( 4.0, 20,  False),
]


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.now(NPT).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]  {msg}", flush=True)
    log_path = os.path.join(DATA_DIR, "scheduler.log")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}]  {msg}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_mode() -> str:
    """
    Auto-detect mode by current NPT time:
      Before 10:55 AM  →  'prev'
      10:55–11:05 AM   →  'cleanup'
      After  11:05 AM  →  'today'
    """
    n = datetime.now(NPT)
    t = (n.hour, n.minute)
    if t < (10, 55):
        return "prev"
    if t < (11, 5):
        return "cleanup"
    return "today"


# ─────────────────────────────────────────────────────────────────────────────
# 1. EXTRACT & TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def load_for_date(target_date: str, csv_path: str) -> "pd.DataFrame | None":
    """Load and clean rows from csv_path that match target_date (YYYY-MM-DD)."""
    if not os.path.exists(csv_path):
        log(f"CSV not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        log("CSV is empty.")
        return None

    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    bad_ts = int(df["fetched_at"].isna().sum())
    if bad_ts:
        log(f"Dropped {bad_ts} row(s) with invalid timestamps.")
        df = df[df["fetched_at"].notna()].copy()

    df = df[df["fetched_at"].dt.strftime("%Y-%m-%d") == target_date].copy()
    if df.empty:
        log(f"No rows for {target_date} in {csv_path}.")
        return None

    for col in ["open", "high", "low", "close", "volume", "prev_close", "pct_change"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.dropna(subset=["close"], inplace=True)
    df.sort_values(["symbol", "fetched_at"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    log(f"Loaded {len(df)} rows for {df['symbol'].nunique()} symbol(s) — {target_date}")
    return df


def load_from_archive(date_str: str) -> "pd.DataFrame | None":
    """Load a specific date's rows from nepse_data/archive/YYYY-MM-DD.csv."""
    archive_path = os.path.join(ARCHIVE_DIR, f"{date_str}.csv")
    if not os.path.exists(archive_path):
        log(f"Archive not found: {archive_path}")
        return None
    return load_for_date(date_str, archive_path)


def compute_summary(sym_df: pd.DataFrame) -> dict:
    first_close = sym_df.iloc[0]["close"]
    last_close  = sym_df.iloc[-1]["close"]
    change      = round(last_close - first_close, 2)
    change_pct  = round((change / first_close) * 100, 2) if first_close else 0
    high        = sym_df["high"].max()
    low         = sym_df["low"].min()
    total_vol   = sym_df["volume"].iloc[-1]
    first_vol   = sym_df["volume"].iloc[0]

    # FIX: Use the correct columns (high/low) to find the time of actual intraday high/low
    high_time = sym_df.loc[sym_df["high"].idxmax(), "fetched_at"].strftime("%H:%M")
    low_time  = sym_df.loc[sym_df["low"].idxmin(),  "fetched_at"].strftime("%H:%M")

    closes = sym_df["close"].values
    if len(closes) >= 3:
        fh = closes[:len(closes)//2]
        sh = closes[len(closes)//2:]
        if fh[-1] > fh[0] and sh[-1] < sh[0]:
            trend = "rose in the first half of the session then pulled back toward close"
        elif fh[-1] < fh[0] and sh[-1] > sh[0]:
            trend = "fell early in the session then recovered toward close"
        elif closes[-1] > closes[0]:
            trend = "moved gradually higher through the session"
        elif closes[-1] < closes[0]:
            trend = "drifted lower through the session"
        else:
            trend = "traded flat through the session"
    else:
        trend = "had limited trading data today"

    if total_vol > 100000:
        vol_comment = "Volume was high, indicating strong trader interest."
    elif total_vol > 50000:
        vol_comment = "Volume was moderate."
    else:
        vol_comment = "Volume was light, suggesting limited activity."

    # FIX: Use direct column access instead of .get() which doesn't exist on pd.Series
    raw_pct    = sym_df.iloc[-1]["pct_change"]
    pct_change = None if pd.isna(raw_pct) else float(raw_pct)

    cb_status = "No circuit breaker was triggered."
    if pct_change is not None:
        pct = abs(pct_change)
        if pct >= 10:
            cb_status = "The 10% circuit breaker was triggered. Market closed early."
        elif pct >= 6:
            cb_status = "A 6% circuit breaker halt was triggered (40-minute halt)."
        elif pct >= 4:
            cb_status = "A 4% circuit breaker halt was triggered (20-minute halt)."

    return {
        "first_close": first_close,
        "last_close":  last_close,
        "change":      change,
        "change_pct":  change_pct,
        "high":        high,
        "low":         low,
        "volume":      total_vol,
        "vol_added":   total_vol - first_vol,
        "polls":       len(sym_df),
        "high_time":   high_time,
        "low_time":    low_time,
        "trend":       trend,
        "vol_comment": vol_comment,
        "cb_status":   cb_status,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. PLOT
# ─────────────────────────────────────────────────────────────────────────────

def plot_symbol(sym: str, sym_df: pd.DataFrame, summary: dict, out_path: str):
    fig, ax = plt.subplots(figsize=(9, 3.2))

    x     = sym_df["fetched_at"].values
    y     = sym_df["close"].values
    color = "#2e7d32" if summary["change"] >= 0 else "#c62828"

    ax.plot(x, y, color=color, linewidth=1.5, marker="o",
            markersize=4, markerfacecolor=color)
    ax.fill_between(x, y, min(y) * 0.999, alpha=0.08, color=color)

    ax.annotate(f"{y[0]:.1f}", xy=(x[0], y[0]),
                xytext=(6, 6), textcoords="offset points",
                fontsize=8, color="#555555")
    ax.annotate(f"{y[-1]:.1f}", xy=(x[-1], y[-1]),
                xytext=(-38, 6), textcoords="offset points",
                fontsize=8, fontweight="bold", color=color)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.tick_params(labelsize=8)
    ax.grid(True, color="#eeeeee", linewidth=0.6)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for spine in ["bottom", "left"]:
        ax.spines[spine].set_color("#cccccc")

    ax.set_xlabel("Time (NPT)", fontsize=8, color="#555555")
    ax.set_ylabel("Close Price (NPR)", fontsize=8, color="#555555")

    plt.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    log(f"  Chart saved -> {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. BUILD PDF
# ─────────────────────────────────────────────────────────────────────────────

def build_pdf(df: pd.DataFrame, chart_dir: str, pdf_path: str,
              report_date_str: str, label: str = ""):
    """
    Assemble the full multi-company PDF.
      report_date_str  — human-readable date shown in the header
      label            — optional banner e.g. "Previous Day Report"
    """
    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4,
        leftMargin=1.8*cm, rightMargin=1.8*cm,
        topMargin=1.5*cm,  bottomMargin=1.5*cm,
    )

    styles = getSampleStyleSheet()
    story  = []

    title_style = ParagraphStyle(
        "ReportTitle", parent=styles["Title"],
        fontSize=22, textColor=BRAND_DARK,
        spaceAfter=4, alignment=TA_CENTER,
    )
    sub_style = ParagraphStyle(
        "SubTitle", parent=styles["Normal"],
        fontSize=10, textColor=GREY,
        spaceAfter=2, alignment=TA_CENTER,
    )
    label_style = ParagraphStyle(
        "LabelBanner", parent=styles["Normal"],
        fontSize=9, textColor=colors.HexColor("#7b5ea7"),
        spaceAfter=4, alignment=TA_CENTER,
        fontName="Helvetica-BoldOblique",
    )
    company_style = ParagraphStyle(
        "CompanyName", parent=styles["Heading1"],
        fontSize=16, textColor=BRAND_BLUE,
        spaceBefore=10, spaceAfter=4,
    )

    generated_at = datetime.now(NPT).strftime("%H:%M NPT")

    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("NEPSE Daily Market Report", title_style))
    story.append(Paragraph(f"{report_date_str}  ·  Generated {generated_at}", sub_style))
    if label:
        story.append(Paragraph(f"[ {label} ]", label_style))
    story.append(HRFlowable(width="100%", thickness=1.5,
                            color=BRAND_BLUE, spaceAfter=12))

    symbols = df["symbol"].unique()

    for i, sym in enumerate(symbols):
        sym_df  = df[df["symbol"] == sym].copy()
        summary = compute_summary(sym_df)

        story.append(Paragraph(sym, company_style))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=BRAND_LIGHT, spaceAfter=6))

        change_color = GREEN if summary["change"] >= 0 else RED
        sign         = "▲" if summary["change"] >= 0 else "▼"

        data = [
            ["Open", "High", "Low", "Last Close", "Day Change", "Volume", "Polls"],
            [
                f"NPR {summary['first_close']:.2f}",
                f"NPR {summary['high']:.2f}",
                f"NPR {summary['low']:.2f}",
                f"NPR {summary['last_close']:.2f}",
                f"{sign} {summary['change']:+.2f} ({summary['change_pct']:+.2f}%)",
                f"{summary['volume']:,}",
                str(summary['polls']),
            ],
        ]

        col_w = [(A4[0] - 3.6*cm) / 7] * 7
        tbl   = Table(data, colWidths=col_w)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",     (0, 0), (-1, 0),  BRAND_DARK),
            ("TEXTCOLOR",      (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",       (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",       (0, 0), (-1, 0),  8),
            ("ALIGN",          (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND",     (0, 1), (-1, 1),  BRAND_LIGHT),
            ("FONTNAME",       (0, 1), (-1, 1),  "Helvetica-Bold"),
            ("FONTSIZE",       (0, 1), (-1, 1),  9),
            ("TEXTCOLOR",      (4, 1), (4, 1),   change_color),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT]),
            ("GRID",           (0, 0), (-1, -1), 0.4, colors.white),
            ("TOPPADDING",     (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 5),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.3*cm))

        chart_path = os.path.join(chart_dir, f"{sym}_chart.png")
        if os.path.exists(chart_path):
            page_w = A4[0] - 3.6*cm
            story.append(RLImage(chart_path, width=page_w, height=page_w * 0.38))

        story.append(Spacer(1, 0.3*cm))

        # Day summary
        direction   = "gained" if summary["change"] >= 0 else "lost"
        day_summary = (
            f"{sym} opened at NPR {summary['first_close']:.2f} and closed at "
            f"NPR {summary['last_close']:.2f}, {direction} NPR {abs(summary['change']):.2f} "
            f"({abs(summary['change_pct']):.2f}%) on the day. "
            f"The stock {summary['trend']}. "
            f"It reached its intraday high of NPR {summary['high']:.2f} around "
            f"{summary['high_time']} and its intraday low of NPR {summary['low']:.2f} around "
            f"{summary['low_time']}."
        )
        story.append(Paragraph("<b>Day Summary</b>", styles["Heading2"]))
        story.append(Paragraph(day_summary, styles["Normal"]))
        story.append(Spacer(1, 0.2*cm))

        # Volume
        vol_text = (
            f"Total volume recorded was {summary['volume']:,} shares. "
            f"{summary['vol_comment']}"
        )
        story.append(Paragraph("<b>Volume</b>", styles["Heading2"]))
        story.append(Paragraph(vol_text, styles["Normal"]))
        story.append(Spacer(1, 0.2*cm))

        # Circuit breaker
        story.append(Paragraph("<b>Circuit Breaker</b>", styles["Heading2"]))
        story.append(Paragraph(summary["cb_status"], styles["Normal"]))

        if i < len(symbols) - 1:
            story.append(PageBreak())

    # Footer
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GREY))
    story.append(Spacer(1, 0.2*cm))
    footer_style = ParagraphStyle("footer", parent=styles["Normal"],
                                  fontSize=7, textColor=GREY, alignment=TA_CENTER)
    story.append(Paragraph(
        f"Data sourced from merolagani.com  ·  For personal use only  ·  "
        f"Report generated {report_date_str} at {generated_at}",
        footer_style
    ))

    doc.build(story)
    log(f"PDF saved → {pdf_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def send_email(pdf_path: str, subject: str, body: str,
               receiver: str = EMAIL_RECEIVER):
    """Send a PDF report as an email attachment via Gmail SMTP."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not receiver:
        log("EMAIL SKIPPED — EMAIL_SENDER, EMAIL_PASSWORD, or EMAIL_RECEIVER "
            "not set in .env file.")
        return

    log(f"  Preparing email to {receiver} ...")
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    log(f"  Attaching: {os.path.basename(pdf_path)} ...")
    try:
        with open(pdf_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition",
                        f"attachment; filename={os.path.basename(pdf_path)}")
        msg.attach(part)
        log("  PDF attached.")
    except Exception as e:
        log(f"  FAILED to attach PDF: {e}")
        return

    log(f"  Connecting to {SMTP_HOST}:{SMTP_PORT} ...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            log(f"  Login OK as {EMAIL_SENDER}")
            server.sendmail(EMAIL_SENDER, receiver, msg.as_string())
            log(f"  Email sent successfully to {receiver}")
    except smtplib.SMTPAuthenticationError as e:
        log(f"  SMTP AUTH FAILED: {e}")
        log("  → Check EMAIL_SENDER and EMAIL_PASSWORD in your .env file")
        log("  → Make sure you used an App Password, not your Gmail password")
    except smtplib.SMTPException as e:
        log(f"  SMTP ERROR: {e}")
    except Exception as e:
        log(f"  EMAIL FAILED (unexpected): {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. CSV LIFECYCLE  —  archive + wipe
# ─────────────────────────────────────────────────────────────────────────────

def archive_and_wipe_csv():
    """
    1. Read every row in live_feed.csv.
    2. Group by date and write each date to nepse_data/archive/YYYY-MM-DD.csv
       (appends to existing archive files and deduplicates).
    3. Wipe live_feed.csv back to header-only for the new trading day.
    """
    if not os.path.exists(CSV_PATH):
        log("CLEANUP — live_feed.csv not found, nothing to archive.")
        _reset_csv()
        return

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        log("CLEANUP — live_feed.csv is empty, nothing to archive.")
        _reset_csv()
        return

    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    df = df[df["fetched_at"].notna()].copy()

    if df.empty:
        log("CLEANUP — no valid rows after timestamp parse.")
        _reset_csv()
        return

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    dates_saved = []

    for date_str, group in df.groupby(df["fetched_at"].dt.strftime("%Y-%m-%d")):
        archive_path = os.path.join(ARCHIVE_DIR, f"{date_str}.csv")
        if os.path.exists(archive_path):
            existing = pd.read_csv(archive_path)
            combined = pd.concat([existing, group], ignore_index=True).drop_duplicates()
            combined.to_csv(archive_path, index=False)
            log(f"  ARCHIVE — appended {len(group)} rows → {archive_path}")
        else:
            group.to_csv(archive_path, index=False)
            log(f"  ARCHIVE — created {archive_path}  ({len(group)} rows)")
        dates_saved.append(date_str)

    _reset_csv()
    log(f"CLEANUP DONE — archived dates: {dates_saved}")
    log(f"               live_feed.csv reset and ready for today's data.")


def _reset_csv():
    """Write just the header row to live_feed.csv (clears all data rows)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CSV_PATH, "w", encoding="utf-8") as f:
        f.write(CSV_HEADERS)
    log(f"  live_feed.csv reset → {CSV_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# SHARED REPORT PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def _date_label(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' → 'Wednesday, 25 March 2026'."""
    # FIX: Use already-imported date_cls instead of re-importing inside the function
    d = date_cls.fromisoformat(date_str)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_   = ["January", "February", "March", "April", "May", "June",
                 "July", "August", "September", "October", "November", "December"]
    return f"{day_names[d.weekday()]}, {d.day:02d} {months_[d.month - 1]} {d.year}"


def _run_report(df: pd.DataFrame, date_str: str,
                report_date_label: str, pdf_label: str,
                email_subject_prefix: str) -> "str | None":
    """
    Shared pipeline used by all three modes:
      generate charts → build PDF → send email.
    Returns the pdf_path on success, None on failure.
    """
    os.makedirs(OUT_DIR, exist_ok=True)
    chart_dir = os.path.join(OUT_DIR, "charts", date_str)
    os.makedirs(chart_dir, exist_ok=True)

    # Step A — charts
    log("Generating charts ...")
    for sym in df["symbol"].unique():
        try:
            sym_df  = df[df["symbol"] == sym].copy()
            summary = compute_summary(sym_df)
            chart_p = os.path.join(chart_dir, f"{sym}_chart.png")
            plot_symbol(sym, sym_df, summary, chart_p)
        except Exception as e:
            log(f"  Chart FAILED for {sym}: {e}")
            log(traceback.format_exc())

    # Step B — PDF
    pdf_name = f"NEPSE_Report_{date_str}.pdf"
    pdf_path = os.path.join(OUT_DIR, pdf_name)
    log(f"Building PDF → {pdf_path}")
    try:
        build_pdf(df, chart_dir, pdf_path, report_date_label, label=pdf_label)
    except Exception as e:
        log(f"PDF build FAILED: {e}")
        log(traceback.format_exc())
        return None

    # Step C — email
    generated_at = datetime.now(NPT).strftime("%H:%M NPT")
    subject = f"{email_subject_prefix} — {report_date_label}"
    body    = (
        f"Hi,\n\n"
        f"Please find attached the NEPSE market report for {report_date_label}.\n"
        f"Generated at {generated_at}.\n\n"
        f"The report includes price charts, OHLCV summary, day change, "
        f"volume analysis, and circuit breaker status for all tracked symbols.\n\n"
        f"— NEPSE ETL (StockDrishti)"
    )
    log("Sending email ...")
    try:
        send_email(pdf_path, subject, body)
    except Exception as e:
        log(f"Email step FAILED: {e}")
        log(traceback.format_exc())

    return pdf_path


# ─────────────────────────────────────────────────────────────────────────────
# MODE RUNNERS
# ─────────────────────────────────────────────────────────────────────────────

def run_previous_day_report():
    """
    Generate a PDF from yesterday's data (still in live_feed.csv or archive).
    Intended to run at ~10:40 AM, BEFORE --cleanup wipes the CSV.

    Data lookup order:
      1. live_feed.csv  (yesterday's rows still present)
      2. archive/YYYY-MM-DD.csv  (if cleanup already ran)
    """
    log("=" * 60)
    log("MODE: Previous Day Report")
    log("=" * 60)

    n         = datetime.now(NPT)
    yesterday = (n - timedelta(days=1)).strftime("%Y-%m-%d")
    log(f"Target date: {yesterday}")

    # Try live CSV first, then fall back to archive
    df = load_for_date(yesterday, CSV_PATH)
    if df is None:
        log(f"Not in live_feed.csv — checking archive ...")
        df = load_from_archive(yesterday)

    if df is None:
        log(f"No data found for {yesterday}. Cannot generate previous-day report.")
        log("  Run prev BEFORE cleanup, or verify the archive folder.")
        sys.exit(0)

    _run_report(
        df=df,
        date_str=yesterday,
        report_date_label=_date_label(yesterday),
        pdf_label="Previous Day Report",
        email_subject_prefix="NEPSE Previous Day Report",
    )
    log("Previous day report complete.")


def run_cleanup():
    """
    Archive all rows in live_feed.csv then reset it to header-only.
    Intended to run at ~10:58 AM, just before market opens at 11:00 AM.
    """
    log("=" * 60)
    log("MODE: CSV Cleanup  (archive + wipe)")
    log("=" * 60)
    archive_and_wipe_csv()
    log("Cleanup complete — live_feed.csv is ready for today's fetch.")


def run_today_report():
    """
    Generate today's PDF from live_feed.csv.
    Intended to run at ~3:50 PM, after market closes at 3:00 PM.
    """
    log("=" * 60)
    log("MODE: Today's Report")
    log(f"  CSV_PATH : {CSV_PATH}")
    log(f"  OUT_DIR  : {OUT_DIR}")
    log("=" * 60)

    os.makedirs(OUT_DIR, exist_ok=True)

    today_str = datetime.now(NPT).strftime("%Y-%m-%d")
    log(f"Loading today's data ({today_str}) ...")

    try:
        df = load_for_date(today_str, CSV_PATH)
    except Exception as e:
        log(f"Load FAILED: {e}")
        log(traceback.format_exc())
        sys.exit(1)

    if df is None:
        log("No data for today. Nothing to report. Exiting.")
        log(f"  Check that fetcher.py ran today and {CSV_PATH} is populated.")
        sys.exit(0)

    log(f"Loaded {len(df)} rows, {df['symbol'].nunique()} symbol(s): "
        f"{list(df['symbol'].unique())}")

    _run_report(
        df=df,
        date_str=today_str,
        report_date_label=_date_label(today_str),
        pdf_label="",
        email_subject_prefix="NEPSE Daily Report",
    )

    log("=" * 60)
    log("Today's report complete.")
    log("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="NEPSE Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  auto     detect mode from current NPT time (default)
  prev     generate previous day's report from live_feed.csv / archive
  cleanup  archive yesterday's CSV rows, wipe live_feed.csv for today
  today    generate today's report from live_feed.csv

Task Scheduler / cron:
  10:40 AM  python report.py prev
  10:58 AM  python report.py cleanup
  03:50 PM  python report.py today
        """
    )
    # FIX: Removed "--prev", "--cleanup", "--today" from choices — argparse
    # positional args never receive "--" prefixed strings, so including them
    # caused valid input to be rejected before lstrip("-") could normalise it.
    parser.add_argument(
        "mode", nargs="?", default="auto",
        choices=["auto", "prev", "cleanup", "today"],
        help="Which mode to run (default: auto)"
    )
    args = parser.parse_args()

    mode = args.mode.lstrip("-")
    if mode == "auto":
        mode = detect_mode()
        log(f"Auto-detected mode: [{mode}]")

    dispatch = {
        "prev":    run_previous_day_report,
        "cleanup": run_cleanup,
        "today":   run_today_report,
    }
    if mode not in dispatch:
        log(f"Unknown mode: {mode}")
        sys.exit(1)

    dispatch[mode]()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        NPT_ = ZoneInfo("Asia/Kathmandu")
        ts   = datetime.now(NPT_).strftime("%Y-%m-%d %H:%M:%S")
        log_path = os.path.join(BASE_DIR, "nepse_data", "scheduler.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}]  FATAL CRASH in report.py:\n")
            f.write(traceback.format_exc())
        raise