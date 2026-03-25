"""
NEPSE Daily Report Generator
==============================
Reads today's live_feed.csv, transforms the data,
plots a closing-price line graph per company,
builds a single PDF with all companies,
shows a Windows toast notification, and
emails the PDF via Gmail at completion.

Called automatically by fetcher.py after 3:00 PM,
OR scheduled via Task Scheduler at 7:00 PM as a fallback.

Usage:
    python report.py

Gmail setup (one-time):
    1. Enable 2-Step Verification: https://myaccount.google.com/security
    2. Create an App Password:     https://myaccount.google.com/apppasswords
    3. Fill GMAIL_FROM and GMAIL_APP_PASSWORD below.
"""

import os
import sys
import ssl
import smtplib
import subprocess
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from zoneinfo import ZoneInfo

import pandas as pd
import matplotlib
matplotlib.use("Agg")           # headless — no display needed
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
NPT      = ZoneInfo("Asia/Kathmandu")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "nepse_data")
CSV_PATH = os.path.join(DATA_DIR, "live_feed.csv")
OUT_DIR  = os.path.join(BASE_DIR, "nepse_data", "reports")

# ── Colours ───────────────────────────────────────────────────────────────────
BRAND_DARK  = colors.HexColor("#0d1b2a")
BRAND_BLUE  = colors.HexColor("#1565c0")
BRAND_LIGHT = colors.HexColor("#e3f2fd")
GREEN       = colors.HexColor("#2e7d32")
RED         = colors.HexColor("#c62828")
GREY        = colors.HexColor("#607d8b")

PLOT_BG     = "#0d1b2a"
PLOT_LINE   = "#29b6f6"
PLOT_FILL   = "#1565c0"
PLOT_GRID   = "#1e3a5f"



def log(msg: str):
    ts = datetime.now(NPT).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]  {msg}", flush=True)
    log_path = os.path.join(DATA_DIR, "scheduler.log")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}]  {msg}\n")


# ─────────────────────────────────────────────────────────────────────────────
# 1. EXTRACT & TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def load_today(csv_path: str) -> pd.DataFrame | None:
    """Load and clean today's rows from live_feed.csv."""
    if not os.path.exists(csv_path):
        log(f"CSV not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path)

    if df.empty:
        log("CSV is empty.")
        return None

    df["fetched_at"] = pd.to_datetime(df["fetched_at"])

    today_str = datetime.now(NPT).strftime("%Y-%m-%d")
    df = df[df["fetched_at"].dt.strftime("%Y-%m-%d") == today_str].copy()

    if df.empty:
        log(f"No rows for today ({today_str}) in CSV.")
        return None

    for col in ["open", "high", "low", "close", "volume", "prev_close", "pct_change"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.dropna(subset=["close"], inplace=True)
    df.sort_values(["symbol", "fetched_at"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    log(f"Loaded {len(df)} rows for {df['symbol'].nunique()} symbol(s) — {today_str}")
    return df


def compute_summary(sym_df: pd.DataFrame) -> dict:
    """Compute per-symbol stats: first close, last close, total change."""
    first_close = sym_df.iloc[0]["close"]
    last_close  = sym_df.iloc[-1]["close"]
    change      = round(last_close - first_close, 2)
    change_pct  = round((change / first_close) * 100, 2) if first_close else 0
    high        = sym_df["high"].max()
    low         = sym_df["low"].min()
    total_vol   = sym_df["volume"].iloc[-1]

    return {
        "first_close": first_close,
        "last_close":  last_close,
        "change":      change,
        "change_pct":  change_pct,
        "high":        high,
        "low":         low,
        "volume":      total_vol,
        "polls":       len(sym_df),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. PLOT
# ─────────────────────────────────────────────────────────────────────────────

def plot_symbol(sym: str, sym_df: pd.DataFrame, summary: dict,
                out_path: str):
    """Render a dark-themed line chart and save as PNG."""
    fig, ax = plt.subplots(figsize=(10, 3.8), facecolor=PLOT_BG)
    ax.set_facecolor(PLOT_BG)

    x = sym_df["fetched_at"].values
    y = sym_df["close"].values

    ax.plot(x, y, color=PLOT_LINE, linewidth=2.2, zorder=3)
    ax.fill_between(x, y, y.min() * 0.999,
                    alpha=0.25, color=PLOT_FILL, zorder=2)

    ax.annotate(f"{y[0]:.2f}",
                xy=(x[0], y[0]), xytext=(8, 6), textcoords="offset points",
                color="#90caf9", fontsize=8)
    ax.annotate(f"{y[-1]:.2f}",
                xy=(x[-1], y[-1]), xytext=(-45, 6), textcoords="offset points",
                color=PLOT_LINE, fontsize=9, fontweight="bold")

    ax.grid(True, color=PLOT_GRID, linewidth=0.6, linestyle="--", zorder=1)
    for spine in ax.spines.values():
        spine.set_edgecolor(PLOT_GRID)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.tick_params(colors="#90caf9", labelsize=8)
    plt.setp(ax.get_xticklabels(), rotation=0)

    change_sign = "▲" if summary["change"] >= 0 else "▼"
    change_col  = "#66bb6a" if summary["change"] >= 0 else "#ef5350"
    ax.set_title(
        f"{sym}  ·  {change_sign} {summary['change']:+.2f}  "
        f"({summary['change_pct']:+.2f}%)  ·  "
        f"Vol {summary['volume']:,}",
        color=change_col, fontsize=10, fontweight="bold", pad=8
    )
    ax.set_xlabel("Time (NPT)", color="#90caf9", fontsize=8)
    ax.set_ylabel("Close (NPR)", color="#90caf9", fontsize=8)

    plt.tight_layout()
    fig.savefig(out_path, dpi=130, bbox_inches="tight",
                facecolor=PLOT_BG, edgecolor="none")
    plt.close(fig)
    log(f"  Chart saved → {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. BUILD PDF
# ─────────────────────────────────────────────────────────────────────────────

def build_pdf(df: pd.DataFrame, chart_dir: str, pdf_path: str):
    """Assemble the full multi-company PDF report."""
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=1.8*cm, rightMargin=1.8*cm,
        topMargin=1.5*cm,  bottomMargin=1.5*cm,
    )

    styles = getSampleStyleSheet()
    story  = []

    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=22, textColor=BRAND_DARK,
        spaceAfter=4, alignment=TA_CENTER,
    )
    sub_style = ParagraphStyle(
        "SubTitle",
        parent=styles["Normal"],
        fontSize=10, textColor=GREY,
        spaceAfter=2, alignment=TA_CENTER,
    )
    company_style = ParagraphStyle(
        "CompanyName",
        parent=styles["Heading1"],
        fontSize=16, textColor=BRAND_BLUE,
        spaceBefore=10, spaceAfter=4,
    )

    today_str    = datetime.now(NPT).strftime("%A, %d %B %Y")
    generated_at = datetime.now(NPT).strftime("%H:%M NPT")

    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("NEPSE Daily Market Report", title_style))
    story.append(Paragraph(f"{today_str}  ·  Generated {generated_at}", sub_style))
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
            ("BACKGROUND",   (0, 0), (-1, 0), BRAND_DARK),
            ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
            ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, 0), 8),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND",   (0, 1), (-1, 1), BRAND_LIGHT),
            ("FONTNAME",     (0, 1), (-1, 1), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 1), (-1, 1), 9),
            ("TEXTCOLOR",    (4, 1), (4, 1),  change_color),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT]),
            ("GRID",         (0, 0), (-1, -1), 0.4, colors.white),
            ("TOPPADDING",   (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.3*cm))

        chart_path = os.path.join(chart_dir, f"{sym}_chart.png")
        if os.path.exists(chart_path):
            page_w = A4[0] - 3.6*cm
            story.append(RLImage(chart_path, width=page_w, height=page_w * 0.38))

        story.append(Spacer(1, 0.3*cm))

        direction = "gained" if summary["change"] >= 0 else "lost"
        summary_text = (
            f"<b>{sym}</b> {direction} <b>NPR {abs(summary['change']):.2f}</b> "
            f"({abs(summary['change_pct']):.2f}%) today, closing at "
            f"<b>NPR {summary['last_close']:.2f}</b> from an opening of "
            f"<b>NPR {summary['first_close']:.2f}</b>."
        )
        story.append(Paragraph(summary_text, styles["Normal"]))

        if i < len(symbols) - 1:
            story.append(PageBreak())

    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GREY))
    story.append(Spacer(1, 0.2*cm))
    footer = ParagraphStyle("footer", parent=styles["Normal"],
                            fontSize=7, textColor=GREY, alignment=TA_CENTER)
    story.append(Paragraph(
        "Data sourced from merolagani.com  ·  For personal use only  ·  "
        f"Report generated {today_str} at {generated_at}",
        footer
    ))

    doc.build(story)
    log(f"PDF saved → {pdf_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. WINDOWS TOAST NOTIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def show_notification(pdf_path: str):
    """
    Show a Windows notification. Tries three methods in order:
      1. Windows 10/11 Toast via PowerShell
      2. VBScript msg-box popup (fallback)
      3. Opens the PDF directly as a last resort
    """
    title   = "NEPSE Daily Report Ready"

    toast_ps = f"""
try {{
    [Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime] | Out-Null
    [Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom,ContentType=WindowsRuntime] | Out-Null
    $template = [Windows.UI.Notifications.ToastTemplateType]::ToastText02
    $xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent($template)
    $xml.GetElementsByTagName('text')[0].AppendChild($xml.CreateTextNode('{title}')) | Out-Null
    $xml.GetElementsByTagName('text')[1].AppendChild($xml.CreateTextNode('Report saved. Click to open PDF.')) | Out-Null
    $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
    [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('NEPSE ETL').Show($toast)
    Write-Output "TOAST_OK"
}} catch {{
    Write-Output "TOAST_FAIL: $_"
}}
"""
    toast_ok = False
    try:
        result = subprocess.run(
            ["powershell", "-WindowStyle", "Hidden",
             "-NonInteractive", "-Command", toast_ps],
            capture_output=True, text=True, timeout=15
        )
        output = (result.stdout or "").strip()
        errors = (result.stderr or "").strip()
        log(f"  Toast stdout: {output}")
        if errors:
            log(f"  Toast stderr: {errors}")
        if "TOAST_OK" in output:
            toast_ok = True
            log("Notification sent via Windows Toast.")
    except Exception as e:
        log(f"  Toast method exception: {e}")

    if not toast_ok:
        log("Falling back to VBScript popup ...")
        vbs_content = (
            f'Set objShell = CreateObject("WScript.Shell")\n'
            f'objShell.Popup "{title} - {pdf_path}", 30, "{title}", 64\n'
        )
        vbs_path = os.path.join(DATA_DIR, "_notify.vbs")
        try:
            with open(vbs_path, "w") as f:
                f.write(vbs_content)
            result = subprocess.run(
                ["cscript", "//Nologo", vbs_path],
                capture_output=True, text=True, timeout=35
            )
            log(f"  VBScript stdout: {(result.stdout or '').strip()}")
            if result.stderr:
                log(f"  VBScript stderr: {result.stderr.strip()}")
            log("Notification sent via VBScript popup.")
        except Exception as e:
            log(f"  VBScript method exception: {e}")

    try:
        os.startfile(pdf_path)
        log("PDF opened in default viewer.")
    except Exception as e:
        log(f"Could not open PDF automatically: {e}")
        log(f"  → Open manually: {pdf_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. GMAIL EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def _build_email(pdf_path: str) -> MIMEMultipart:
    """Compose the HTML email with the PDF attached."""
    today_str    = datetime.now(NPT).strftime("%A, %d %B %Y")
    generated_at = datetime.now(NPT).strftime("%H:%M NPT")
    pdf_filename = os.path.basename(pdf_path)

    msg = MIMEMultipart("mixed")
    msg["From"]    = GMAIL_FROM
    msg["To"]      = ", ".join(EMAIL_TO)
    if EMAIL_CC:
        msg["Cc"]  = ", ".join(EMAIL_CC)
    msg["Subject"] = f"NEPSE Daily Market Report — {today_str}"

    html_body = f"""
    <html><body style="font-family:Arial,sans-serif;color:#212121;max-width:600px;margin:auto;">
      <div style="background:#0d1b2a;padding:24px 32px;border-radius:8px 8px 0 0;">
        <h2 style="color:#29b6f6;margin:0;">&#128200; NEPSE Daily Market Report</h2>
        <p style="color:#90caf9;margin:6px 0 0;">{today_str}&nbsp;&middot;&nbsp;Generated {generated_at}</p>
      </div>
      <div style="background:#f5f5f5;padding:24px 32px;border-radius:0 0 8px 8px;">
        <p>Hi,</p>
        <p>Your NEPSE daily market report is attached as a PDF.<br>
           The report includes closing-price charts and key stats
           for all tracked symbols.</p>
        <hr style="border:none;border-top:1px solid #ddd;margin:20px 0;">
        <p style="font-size:12px;color:#888;">
          Data sourced from merolagani.com &nbsp;&middot;&nbsp; For personal use only
        </p>
      </div>
    </body></html>
    """
    msg.attach(MIMEText(html_body, "html"))

    with open(pdf_path, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="pdf")
    part.add_header("Content-Disposition", "attachment", filename=pdf_filename)
    msg.attach(part)

    return msg


def send_email(pdf_path: str) -> bool:
    """
    Send the PDF report via Gmail (App Password).
    Returns True on success, False on failure.
    Error is logged but never raised — won't crash the parent process.
    """
    if not os.path.exists(pdf_path):
        log(f"EMAIL SKIPPED — PDF not found: {pdf_path}")
        return False

    all_recipients = EMAIL_TO + EMAIL_CC
    log(f"Sending report email to: {', '.join(all_recipients)} …")

    try:
        msg     = _build_email(pdf_path)
        context = ssl.create_default_context()

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(GMAIL_FROM, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_FROM, all_recipients, msg.as_string())

        log(f"Email sent successfully to: {', '.join(all_recipients)}")
        return True

    except smtplib.SMTPAuthenticationError:
        log("EMAIL ERROR — Authentication failed. "
            "Check GMAIL_FROM and GMAIL_APP_PASSWORD.")
    except smtplib.SMTPConnectError as e:
        log(f"EMAIL ERROR — Could not connect to smtp.gmail.com: {e}")
    except smtplib.SMTPRecipientsRefused as e:
        log(f"EMAIL ERROR — Recipient refused: {e}")
    except Exception as e:
        log(f"EMAIL ERROR — {type(e).__name__}: {e}")

    return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    log("=" * 60)
    log("Report generator started")
    log("=" * 60)

    os.makedirs(OUT_DIR, exist_ok=True)

    # 1. Load & transform
    df = load_today(CSV_PATH)
    if df is None:
        log("Nothing to report today. Exiting.")
        sys.exit(0)

    # 2. Generate charts (one PNG per symbol)
    chart_dir = os.path.join(OUT_DIR, "charts")
    os.makedirs(chart_dir, exist_ok=True)

    for sym in df["symbol"].unique():
        sym_df  = df[df["symbol"] == sym].copy()
        summary = compute_summary(sym_df)
        chart_p = os.path.join(chart_dir, f"{sym}_chart.png")
        plot_symbol(sym, sym_df, summary, chart_p)

    # 3. Build PDF
    today_str = datetime.now(NPT).strftime("%Y-%m-%d")
    pdf_name  = f"NEPSE_Report_{today_str}.pdf"
    pdf_path  = os.path.join(OUT_DIR, pdf_name)
    build_pdf(df, chart_dir, pdf_path)

    # 4. Windows toast notification
    show_notification(pdf_path)

    # 5. Email the PDF via Gmail
    send_email(pdf_path)

    log("Report generation complete.")
    log("=" * 60)


if __name__ == "__main__":
    run()