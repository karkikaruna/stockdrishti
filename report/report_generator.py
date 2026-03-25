import os
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from datetime import datetime
import smtplib
from email.message import EmailMessage
import holidays

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet

from dotenv import load_dotenv

# ===============================
# LOAD ENV (FROM ROOT)
# ===============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

load_dotenv(dotenv_path=ENV_PATH)

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

# ===============================
# PATHS
# ===============================
DATA_PATH = os.path.join(BASE_DIR, "raw.csv")
OUTPUT_PDF = os.path.join(BASE_DIR, "daily_report.pdf")


# ===============================
# 1. CHECK TRADING DAY
# ===============================
def is_trading_day():
    today = datetime.now().date()

    # Skip Friday (4) & Saturday (5)
    if today.weekday() in [4, 5]:
        return False

    np_holidays = holidays.country_holidays('NP')

    if today in np_holidays:
        return False

    return True


# ===============================
# 2. GENERATE CHARTS
# ===============================
def generate_charts(df):
    image_paths = []

    for sym in df['symbol'].unique():
        sub = df[df['symbol'] == sym].copy()

        # ---- Line Chart ----
        plt.figure()
        plt.plot(sub['close'])
        plt.title(f"{sym} Close Price")
        plt.xlabel("Index")
        plt.ylabel("Close")

        line_path = os.path.join(BASE_DIR, f"{sym}_line.png")
        plt.savefig(line_path)
        plt.close()

        # ---- Candlestick ----
        sub2 = sub[['date', 'open', 'high', 'low', 'close']].copy()
        sub2['date'] = pd.to_datetime(sub2['date'])
        sub2.set_index('date', inplace=True)

        candle_path = os.path.join(BASE_DIR, f"{sym}_candle.png")
        mpf.plot(sub2, type='candle', savefig=candle_path)

        image_paths.append((sym, line_path, candle_path))

    return image_paths


# ===============================
# 3. CREATE PDF
# ===============================
def create_pdf(image_paths):
    doc = SimpleDocTemplate(OUTPUT_PDF)
    styles = getSampleStyleSheet()

    elements = []
    elements.append(Paragraph("NEPSE Daily Performance Report", styles['Title']))
    elements.append(Spacer(1, 12))

    for sym, line, candle in image_paths:
        elements.append(Paragraph(sym, styles['Heading2']))
        elements.append(Spacer(1, 8))

        elements.append(Image(line, width=400, height=200))
        elements.append(Spacer(1, 8))

        elements.append(Image(candle, width=400, height=200))
        elements.append(Spacer(1, 12))

    doc.build(elements)


# ===============================
# 4. SEND EMAIL
# ===============================
def send_email():
    msg = EmailMessage()
    msg['Subject'] = 'NEPSE Daily Report'
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    msg.set_content("Attached is today's NEPSE performance report.")

    with open(OUTPUT_PDF, 'rb') as f:
        msg.add_attachment(
            f.read(),
            maintype='application',
            subtype='pdf',
            filename="daily_report.pdf"
        )

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
        smtp.send_message(msg)


# ===============================
# 5. MAIN PIPELINE
# ===============================
def run_pipeline():
    if not is_trading_day():
        print("Market closed today. Skipping...")
        return

    print("Running ETL Report Pipeline...")

    df = pd.read_csv(DATA_PATH)
    df.columns = [c.strip() for c in df.columns]

    image_paths = generate_charts(df)
    create_pdf(image_paths)
    send_email()

    print("Report sent successfully!")


if __name__ == "__main__":
    run_pipeline()