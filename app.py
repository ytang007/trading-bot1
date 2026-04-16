import os
import json
import csv
import time
import queue
import threading
import smtplib
from datetime import datetime, timezone
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

# scanner state
scanner_scores = {}
last_summary_sent = 0
SUMMARY_INTERVAL_SECONDS = 300  # 5 minutes

# queue email so webhook returns fast
email_queue: queue.Queue[dict] = queue.Queue()


# =========================
# Helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_env() -> None:
    missing = []
    if ENABLE_EMAIL:
        if not EMAIL_ADDRESS:
            missing.append("EMAIL_ADDRESS")
        if not EMAIL_PASSWORD:
            missing.append("EMAIL_PASSWORD")
    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))


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


def send_scanner_summary_if_due() -> None:
    global last_summary_sent
    now = time.time()
    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return
    if not scanner_scores:
        return

    top5 = sorted(scanner_scores.items(), key=lambda x: x[1]["score"], reverse=True)[:5]

    lines = ["TOP 5 RIGHT NOW", ""]
    for i, (symbol, data) in enumerate(top5, 1):
        lines.append(f"{i}. {symbol} — {data['score']}/9")
        lines.append(f"   Intraday: {data['intraday']}  Pre: {data['pre']}")
        lines.append(f"   Price: {data['price']}")
        lines.append("")

    enqueue_email("Scanner Top 5", "\n".join(lines))
    last_summary_sent = now


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
    return jsonify({"status": "ok", "time_utc": utc_now_iso(), "email_enabled": ENABLE_EMAIL}), 200


@app.route("/test-email", methods=["GET"])
def test_email():
    enqueue_email("Trading Bot Test", "This is a test email from your Render bot.")
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

        log_alert(symbol, alert_type, price, alert_time, "received", data)

        # Scanner alerts
        if alert_type == "SCANNER_TOP_STOCK":
            score = int(float(data.get("score", 0)))
            intraday = int(float(data.get("intraday", 0)))
            pre = int(float(data.get("pre", 0)))

            scanner_scores[symbol] = {
                "score": score,
                "intraday": intraday,
                "pre": pre,
                "price": price,
                "last_update": utc_now_iso()
            }
            send_scanner_summary_if_due()

        elif alert_type == "ENTRY_READY":
            score = data.get("score", "?")
            intraday = data.get("intraday", "?")
            pre = data.get("pre", "?")
            stop = data.get("stop", "?")
            target = data.get("target", "?")

            subject = f"ENTRY READY - {symbol}"
            body = (
                f"ENTRY READY\n\n"
                f"Symbol: {symbol}\n"
                f"Score: {score}/9\n"
                f"Intraday: {intraday}\n"
                f"Pre: {pre}\n"
                f"Current price: {price}\n"
                f"Suggested stop: {stop}\n"
                f"Suggested target: {target}\n"
            )
            enqueue_email(subject, body)

        # Management alerts
        else:
            subject = f"{alert_type} - {symbol}"
            body = (
                f"Trading Alert\n\n"
                f"Symbol: {symbol}\n"
                f"Type: {alert_type}\n"
                f"Price: {price}\n"
                f"Time: {alert_time}\n"
            )
            enqueue_email(subject, body)

        elapsed_ms = int((time.time() - start) * 1000)
        return jsonify({"status": "ok", "elapsed_ms": elapsed_ms}), 200

    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    validate_env()
    ensure_csv_exists()
    app.run(host="0.0.0.0", port=PORT)
