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
# Environment / Config
# =========================
PORT = int(os.getenv("PORT", "10000"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail app password
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)

CSV_LOG = os.getenv("CSV_LOG", "alerts_log.csv")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # optional
ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"

# Email queue so webhook returns fast
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

    # Gmail SSL SMTP
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
                print(f"[EMAIL SKIPPED] {job['subject']}")
        except Exception as e:
            print(f"[EMAIL ERROR] {e}")
        finally:
            email_queue.task_done()


def format_email(data: dict) -> tuple[str, str]:
    symbol = str(data.get("symbol", "UNKNOWN"))
    alert_type = str(data.get("type", "UNKNOWN"))
    price = str(data.get("price", "0"))
    alert_time = str(data.get("time", "UNKNOWN"))

    subject = f"{alert_type} - {symbol}"
    body = (
        f"TradingView Alert Received\n\n"
        f"Symbol: {symbol}\n"
        f"Type: {alert_type}\n"
        f"Price: {price}\n"
        f"Time: {alert_time}\n\n"
        f"Raw JSON:\n{json.dumps(data, indent=2)}\n"
    )
    return subject, body


# Start background email thread once
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
    return jsonify({
        "status": "ok",
        "time_utc": utc_now_iso(),
        "email_enabled": ENABLE_EMAIL
    }), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    start = time.time()

    try:
        # Optional secret protection
        if WEBHOOK_SECRET:
            incoming_secret = request.headers.get("X-Webhook-Secret", "")
            if incoming_secret != WEBHOOK_SECRET:
                return jsonify({"status": "error", "message": "Unauthorized"}), 401

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

        # Log immediately
        log_alert(
            symbol=symbol,
            alert_type=alert_type,
            price=price,
            alert_time=alert_time,
            status="received",
            raw_data=data
        )

        # Queue email so the webhook can return fast
        subject, body = format_email(data)
        email_queue.put({
            "subject": subject,
            "body": body
        })

        elapsed_ms = int((time.time() - start) * 1000)
        return jsonify({
            "status": "ok",
            "message": "Webhook received",
            "elapsed_ms": elapsed_ms
        }), 200

    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        try:
            raw = request.get_json(silent=True) or {}
            log_alert(
                symbol=str(raw.get("symbol", "UNKNOWN")),
                alert_type=str(raw.get("type", "UNKNOWN")),
                price=str(raw.get("price", "0")),
                alert_time=str(raw.get("time", "UNKNOWN")),
                status=f"error: {e}",
                raw_data=raw if isinstance(raw, dict) else {}
            )
        except Exception:
            pass

        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/test-email", methods=["GET"])
def test_email():
    if not ENABLE_EMAIL:
        return jsonify({"status": "ok", "message": "Email disabled"}), 200

    subject = "Trading Bot Test Email"
    body = "This is a test email from your Render trading bot."
    email_queue.put({"subject": subject, "body": body})
    return jsonify({"status": "ok", "message": "Test email queued"}), 200


if __name__ == "__main__":
    validate_env()
    ensure_csv_exists()
    app.run(host="0.0.0.0", port=PORT)
