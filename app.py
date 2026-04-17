import os
import json
import csv
import time
import queue
import threading
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================
# CONFIG
# =========================
PORT = int(os.getenv("PORT", "10000"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)

ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"
CSV_LOG = os.getenv("CSV_LOG", "alerts_log.csv")

ROLLING_WINDOW_MINUTES = 30
SUMMARY_INTERVAL_SECONDS = 300
TOP_N = 5

NY_TZ = ZoneInfo("America/New_York")

# Rolling activity scanner state
scanner_scores = {}

# Quality scanner state
quality_scores = {}

last_summary_sent = 0
email_queue = queue.Queue()

# =========================
# TIME HELPERS
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ny_now() -> datetime:
    return datetime.now(NY_TZ)

def utc_now_iso() -> str:
    return utc_now().isoformat()

def ny_now_str() -> str:
    return ny_now().strftime("%Y-%m-%d %I:%M:%S %p %Z")

def iso_utc_to_ny_str(iso_text: str) -> str:
    """
    Convert incoming ISO-like UTC time text to New York display time.
    If conversion fails, return original string.
    """
    if not iso_text or iso_text == "UNKNOWN":
        return iso_text
    try:
        cleaned = iso_text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(NY_TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return iso_text

# =========================
# GENERAL HELPERS
# =========================
def validate_env():
    if ENABLE_EMAIL and (not EMAIL_ADDRESS or not EMAIL_PASSWORD):
        raise RuntimeError("Missing EMAIL credentials")

def ensure_csv():
    if not os.path.exists(CSV_LOG):
        with open(CSV_LOG, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "received_time_ny",
                "received_time_utc",
                "symbol",
                "type",
                "price",
                "alert_time_ny",
                "status",
                "raw_json"
            ])

def log_alert(symbol, alert_type, price, alert_time_raw, status, raw):
    ensure_csv()
    with open(CSV_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            ny_now_str(),
            utc_now_iso(),
            symbol,
            alert_type,
            price,
            iso_utc_to_ny_str(alert_time_raw),
            status,
            json.dumps(raw, separators=(",", ":"))
        ])

def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)

def email_worker():
    while True:
        job = email_queue.get()
        try:
            if ENABLE_EMAIL:
                send_email(job["subject"], job["body"])
                print("[EMAIL SENT]", job["subject"])
            else:
                print("[EMAIL DISABLED]", job["subject"])
        except Exception as e:
            print("[EMAIL ERROR]", e)
        finally:
            email_queue.task_done()

threading.Thread(target=email_worker, daemon=True).start()

def enqueue_email(subject, body):
    email_queue.put({"subject": subject, "body": body})

# =========================
# ROLLING TOP 5 SCANNER
# =========================
def prune_scanner_hits():
    cutoff = utc_now() - timedelta(minutes=ROLLING_WINDOW_MINUTES)

    remove = []
    for sym, data in scanner_scores.items():
        data["hits"] = [h for h in data["hits"] if h >= cutoff]
        data["score"] = len(data["hits"])
        if not data["hits"]:
            remove.append(sym)

    for r in remove:
        del scanner_scores[r]

def update_scanner_score(symbol, price):
    now = utc_now()

    if symbol not in scanner_scores:
        scanner_scores[symbol] = {
            "hits": [now],
            "score": 1,
            "price": price,
            "last": now.isoformat()
        }
    else:
        scanner_scores[symbol]["hits"].append(now)
        scanner_scores[symbol]["price"] = price
        scanner_scores[symbol]["last"] = now.isoformat()

    prune_scanner_hits()

def get_top_symbols():
    prune_scanner_hits()
    ranked = sorted(
        scanner_scores.items(),
        key=lambda x: (x[1]["score"], x[1]["last"]),
        reverse=True
    )[:TOP_N]
    return [s for s, _ in ranked]

def in_top(symbol):
    return symbol in get_top_symbols()

# =========================
# QUALITY SCANNER
# =========================
def update_quality_score(symbol, quality_type, price, raw):
    """
    Because Pine alertcondition messages can't easily carry dynamic numeric scores
    in your current setup, we treat QUALITY_TOP_STOCK > QUALITY_WATCH_STOCK.
    """
    now = utc_now()

    score_value = 2 if quality_type == "QUALITY_TOP_STOCK" else 1

    quality_scores[symbol] = {
        "quality_label": quality_type,
        "quality_score": score_value,
        "price": price,
        "last": now.isoformat(),
        "raw": raw
    }

def get_quality_ranked_symbols():
    ranked = sorted(
        quality_scores.items(),
        key=lambda x: (x[1]["quality_score"], x[1]["last"]),
        reverse=True
    )
    return ranked

# =========================
# SUMMARY EMAILS
# =========================
def send_combined_summary_if_due():
    global last_summary_sent

    now = time.time()
    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return

    prune_scanner_hits()

    top_activity = sorted(
        scanner_scores.items(),
        key=lambda x: (x[1]["score"], x[1]["last"]),
        reverse=True
    )[:TOP_N]

    top_quality = get_quality_ranked_symbols()[:TOP_N]

    lines = [
        f"Hybrid Scanner Summary",
        f"Generated: {ny_now_str()}",
        "",
        f"Top {TOP_N} Active Stocks (last {ROLLING_WINDOW_MINUTES} minutes):",
        ""
    ]

    if top_activity:
        for i, (s, d) in enumerate(top_activity, 1):
            lines.append(f"{i}. {s} | Activity Score: {d['score']} | Price: {d['price']}")
    else:
        lines.append("No active scanner symbols yet.")

    lines.extend(["", f"Top {TOP_N} Quality Stocks:", ""])

    if top_quality:
        for i, (s, d) in enumerate(top_quality, 1):
            lines.append(f"{i}. {s} | {d['quality_label']} | Quality Score: {d['quality_score']} | Price: {d['price']}")
    else:
        lines.append("No quality scanner symbols yet.")

    lines.extend([
        "",
        "Interpretation:",
        "- Activity Score = scanner hits in last 30 minutes",
        "- Quality Score = 2 for QUALITY_TOP_STOCK, 1 for QUALITY_WATCH_STOCK",
        "- Entry alerts are filtered so only current Top 5 activity names can pass"
    ])

    enqueue_email("Hybrid Scanner Summary", "\n".join(lines))
    last_summary_sent = now

# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return "running"

@app.route("/health")
def health():
    return {
        "time_ny": ny_now_str(),
        "top_activity_symbols": get_top_symbols(),
        "quality_symbols": [s for s, _ in get_quality_ranked_symbols()[:TOP_N]],
        "scanner_tracked": len(scanner_scores),
        "quality_tracked": len(quality_scores),
    }

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()

        symbol = data.get("symbol", "UNK")
        alert_type = data.get("type", "UNK")
        price = str(data.get("price", "0"))
        alert_time_raw = str(data.get("time", "UNKNOWN"))
        alert_time_ny = iso_utc_to_ny_str(alert_time_raw)

        print("RECV:", data)

        log_alert(symbol, alert_type, price, alert_time_raw, "ok", data)

        # =========================
        # ACTIVITY SCANNER
        # =========================
        if alert_type == "SCANNER_TOP_STOCK":
            update_scanner_score(symbol, price)
            send_combined_summary_if_due()

        # =========================
        # QUALITY SCANNER
        # =========================
        elif alert_type in ["QUALITY_TOP_STOCK", "QUALITY_WATCH_STOCK"]:
            update_quality_score(symbol, alert_type, price, data)
            send_combined_summary_if_due()

        # =========================
        # ENTRY FILTER
        # Only allow entry alerts if current symbol is in Top 5 activity
        # =========================
        elif alert_type == "ENTRY_READY":
            if in_top(symbol):
                enqueue_email(
                    f"ENTRY READY - {symbol}",
                    f"ENTRY READY\n\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Alert Time (NY): {alert_time_ny}\n"
                    f"Received Time (NY): {ny_now_str()}\n\n"
                    f"This symbol is currently in the Top {TOP_N} activity scanner."
                )
            else:
                print("FILTERED ENTRY_READY:", symbol)

        elif alert_type == "ENTRY_LONG":
            if in_top(symbol):
                enqueue_email(
                    f"ENTRY LONG - {symbol}",
                    f"ENTRY LONG\n\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Alert Time (NY): {alert_time_ny}\n"
                    f"Received Time (NY): {ny_now_str()}\n\n"
                    f"This symbol is currently in the Top {TOP_N} activity scanner."
                )
            else:
                print("FILTERED ENTRY_LONG:", symbol)

        # =========================
        # MANAGEMENT ALERTS
        # Always pass through
        # =========================
        elif alert_type == "HOLD_OK":
            enqueue_email(
                f"HOLD - {symbol}",
                f"HOLD\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "TARGET_CHECKPOINT_HIT":
            enqueue_email(
                f"TARGET CHECKPOINT - {symbol}",
                f"TARGET CHECKPOINT HIT\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "SELL_INITIAL_STOP":
            enqueue_email(
                f"SELL INITIAL STOP - {symbol}",
                f"SELL INITIAL STOP\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "SELL_TRAILING_STOP":
            enqueue_email(
                f"SELL TRAILING STOP - {symbol}",
                f"SELL TRAILING STOP\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "SELL_TREND_WEAKENING":
            enqueue_email(
                f"SELL TREND WEAKENING - {symbol}",
                f"SELL TREND WEAKENING\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "SELL_END_OF_DAY":
            enqueue_email(
                f"SELL EOD - {symbol}",
                f"SELL END OF DAY\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "HOLD_OVERNIGHT":
            enqueue_email(
                f"HOLD OVERNIGHT - {symbol}",
                f"HOLD OVERNIGHT\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "HOLD_OVER_WEEKEND":
            enqueue_email(
                f"HOLD OVER WEEKEND - {symbol}",
                f"HOLD OVER WEEKEND\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        elif alert_type == "SELL_BEFORE_WEEKEND":
            enqueue_email(
                f"SELL BEFORE WEEKEND - {symbol}",
                f"SELL BEFORE WEEKEND\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n"
            )

        # =========================
        # FALLBACK
        # =========================
        else:
            enqueue_email(
                f"{alert_type} - {symbol}",
                f"GENERAL ALERT\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n\n"
                f"Raw Payload:\n{json.dumps(data, indent=2)}"
            )

        return {"ok": True}

    except Exception as e:
        print("ERROR:", e)
        return {"error": str(e)}, 500

if __name__ == "__main__":
    validate_env()
    ensure_csv()
    app.run(host="0.0.0.0", port=PORT)
