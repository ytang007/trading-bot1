import os
import json
import csv
import time
import queue
import threading
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================
# Config
# =========================
PORT = int(os.getenv("PORT", "10000"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail app password
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)
ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"

CSV_LOG = os.getenv("CSV_LOG", "alerts_log.csv")

# Scanner state
scanner_scores = {}
last_summary_sent = 0
SUMMARY_INTERVAL_SECONDS = 300  # 5 minutes
ROLLING_WINDOW_MINUTES = 30

# Background email queue
email_queue = queue.Queue()

# =========================
# Helpers
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def validate_env() -> None:
    missing = []
    if ENABLE_EMAIL:
        if not EMAIL_ADDRESS:
            missing.append("EMAIL_ADDRESS")
        if not EMAIL_PASSWORD:
            missing.append("EMAIL_PASSWORD")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def ensure_csv_exists() -> None:
    if not os.path.exists(CSV_LOG):
        with open(CSV_LOG, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "received_at_utc",
                "symbol",
                "type",
                "price",
                "time",
                "status",
                "raw_json"
            ])


def log_alert(symbol: str, alert_type: str, price: str, alert_time: str, status: str, raw_data: dict) -> None:
    ensure_csv_exists()
    with open(CSV_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            utc_now_iso(),
            symbol,
            alert_type,
            price,
            alert_time,
            status,
            json.dumps(raw_data, separators=(",", ":"))
        ])


def send_email(subject: str, body: str) -> None:
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)


def email_worker() -> None:
    while True:
        job = email_queue.get()
        try:
            if ENABLE_EMAIL:
                send_email(job["subject"], job["body"])
                print(f"[EMAIL SENT] {job['subject']}")
            else:
                print(f"[EMAIL DISABLED] {job['subject']}")
        except Exception as e:
            print(f"[EMAIL ERROR] {e}")
        finally:
            email_queue.task_done()


def enqueue_email(subject: str, body: str) -> None:
    email_queue.put({"subject": subject, "body": body})


def prune_old_hits() -> None:
    """Keep only scanner hits from the rolling window."""
    cutoff = utc_now() - timedelta(minutes=ROLLING_WINDOW_MINUTES)
    symbols_to_delete = []

    for symbol, data in scanner_scores.items():
        kept_hits = [hit for hit in data["hits"] if hit >= cutoff]
        data["hits"] = kept_hits
        data["score"] = len(kept_hits)

        if not kept_hits:
            symbols_to_delete.append(symbol)

    for symbol in symbols_to_delete:
        del scanner_scores[symbol]


def update_scanner_score(symbol: str, price: str) -> None:
    now_dt = utc_now()

    if symbol not in scanner_scores:
        scanner_scores[symbol] = {
            "hits": [now_dt],
            "score": 1,
            "price": price,
            "last_update": now_dt.isoformat()
        }
    else:
        scanner_scores[symbol]["hits"].append(now_dt)
        scanner_scores[symbol]["price"] = price
        scanner_scores[symbol]["last_update"] = now_dt.isoformat()

    prune_old_hits()


def send_scanner_summary_if_due() -> None:
    global last_summary_sent

    now = time.time()
    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return

    prune_old_hits()

    if not scanner_scores:
        return

    # Sort by rolling-window score first, then recency
    top5 = sorted(
        scanner_scores.items(),
        key=lambda item: (item[1]["score"], item[1]["last_update"]),
        reverse=True
    )[:5]

    lines = [
        f"TOP 5 SCANNER (LAST {ROLLING_WINDOW_MINUTES} MINUTES)",
        "",
        "Ranking basis:",
        f"- Scanner hits in the last {ROLLING_WINDOW_MINUTES} minutes",
        "- More recent updates rank higher when scores tie",
        ""
    ]

    for i, (symbol, data) in enumerate(top5, 1):
        lines.append(f"{i}. {symbol} | Score: {data['score']}")
        lines.append(f"   Price: {data['price']}")
        lines.append(f"   Last update: {data['last_update']}")
        lines.append("")

    enqueue_email("Top 5 Scanner Stocks", "\n".join(lines))
    last_summary_sent = now


def build_general_email(symbol: str, alert_type: str, price: str, alert_time: str) -> tuple[str, str]:
    subject = f"{alert_type} - {symbol}"
    body = (
        f"Trading Alert\n\n"
        f"Symbol: {symbol}\n"
        f"Type: {alert_type}\n"
        f"Price: {price}\n"
        f"Time: {alert_time}\n"
    )
    return subject, body


# Start background email thread
worker_thread = threading.Thread(target=email_worker, daemon=True)
worker_thread.start()

# =========================
# Routes
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Trading bot is running", 200


@app.route("/health", methods=["GET"])
def health():
    prune_old_hits()
    return jsonify({
        "status": "ok",
        "time_utc": utc_now_iso(),
        "email_enabled": ENABLE_EMAIL,
        "scanner_symbols_tracked": len(scanner_scores),
        "rolling_window_minutes": ROLLING_WINDOW_MINUTES
    }), 200


@app.route("/test-email", methods=["GET"])
def test_email():
    enqueue_email(
        "Trading Bot Test",
        "This is a test email from your Render trading bot."
    )
    return jsonify({"status": "ok", "message": "Test email queued"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    start = time.time()

    try:
        if not request.is_json:
            return jsonify({"status": "error", "message": "Expected JSON body"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"status": "error", "message": "Invalid JSON object"}), 400

        symbol = str(data.get("symbol", "UNKNOWN"))
        alert_type = str(data.get("type", "UNKNOWN"))
        price = str(data.get("price", "0"))
        alert_time = str(data.get("time", "UNKNOWN"))

        print("[WEBHOOK RECEIVED]")
        print(json.dumps(data, indent=2))

        log_alert(
            symbol=symbol,
            alert_type=alert_type,
            price=price,
            alert_time=alert_time,
            status="received",
            raw_data=data
        )

        # =========================
        # Scanner Engine alerts
        # =========================
        if alert_type == "SCANNER_TOP_STOCK":
            update_scanner_score(symbol, price)
            send_scanner_summary_if_due()

        elif alert_type == "ENTRY_READY":
            subject = f"ENTRY READY - {symbol}"
            body = (
                f"ENTRY READY\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"This symbol passed the scanner and is considered entry-ready.\n"
            )
            enqueue_email(subject, body)

        # =========================
        # Entry Engine alerts
        # =========================
        elif alert_type == "ENTRY_LONG":
            subject = f"ENTRY LONG - {symbol}"
            body = (
                f"ENTRY LONG\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"This is a buy setup alert from the Entry Engine.\n"
            )
            enqueue_email(subject, body)

        # =========================
        # Management Engine alerts
        # =========================
        elif alert_type == "HOLD_OK":
            subject = f"HOLD - {symbol}"
            body = (
                f"HOLD\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"Trend remains intact according to the Management Engine.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "TARGET_CHECKPOINT_HIT":
            subject = f"TARGET CHECKPOINT - {symbol}"
            body = (
                f"TARGET CHECKPOINT HIT\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"The trade has reached the target checkpoint.\n"
                f"Use trailing-stop / trend rules from the Management Engine.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "SELL_INITIAL_STOP":
            subject = f"SELL INITIAL STOP - {symbol}"
            body = (
                f"SELL INITIAL STOP\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"The initial stop-loss condition was triggered.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "SELL_TRAILING_STOP":
            subject = f"SELL TRAILING STOP - {symbol}"
            body = (
                f"SELL TRAILING STOP\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"The trailing stop condition was triggered.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "SELL_TREND_WEAKENING":
            subject = f"SELL TREND WEAKENING - {symbol}"
            body = (
                f"SELL TREND WEAKENING\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"The trend is weakening according to the Management Engine.\n"
            )
            enqueue_email(subject, body)

        elif alert_type == "SELL_END_OF_DAY":
            subject = f"SELL END OF DAY - {symbol}"
            body = (
                f"SELL END OF DAY\n\n"
                f"Symbol: {symbol}\n"
                f"Current price: {price}\n"
                f"Time: {alert_time}\n\n"
                f"End-of-day exit signal triggered.\n"
            )
            enqueue_email(subject, body)

        else:
            subject, body = build_general_email(symbol, alert_type, price, alert_time)
            enqueue_email(subject, body)

        elapsed_ms = int((time.time() - start) * 1000)
        return jsonify({
            "status": "ok",
            "message": "Webhook received",
            "elapsed_ms": elapsed_ms
        }), 200

    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")

        try:
            raw_data = request.get_json(silent=True) or {}
            log_alert(
                symbol=str(raw_data.get("symbol", "UNKNOWN")),
                alert_type=str(raw_data.get("type", "UNKNOWN")),
                price=str(raw_data.get("price", "0")),
                alert_time=str(raw_data.get("time", "UNKNOWN")),
                status=f"error: {e}",
                raw_data=raw_data if isinstance(raw_data, dict) else {}
            )
        except Exception:
            pass

        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    validate_env()
    ensure_csv_exists()
    app.run(host="0.0.0.0", port=PORT)
