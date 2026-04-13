import os
import json
import smtplib
from email.mime.text import MIMEText
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================
# Environment variables
# =========================
EMAIL_ADDRESS = os.getenv("ytang06@gmail.com")
EMAIL_PASSWORD = os.getenv("rnjw lxii qdwx djmt")  # Gmail App Password
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)
PORT = int(os.getenv("PORT", "10000"))


def validate_env() -> None:
    """Make sure required environment variables are present."""
    missing = []

    if not EMAIL_ADDRESS:
        missing.append("EMAIL_ADDRESS")
    if not EMAIL_PASSWORD:
        missing.append("EMAIL_PASSWORD")

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def send_email(subject: str, body: str) -> None:
    """Send an email using Gmail SMTP over SSL."""
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)


@app.route("/", methods=["GET"])
def home():
    return "Trading bot is running", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if not request.is_json:
            return jsonify({"status": "error", "message": "Expected JSON body"}), 400

        data = request.get_json()

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
            f"Time: {alert_time}\n"
        )

        print("Received webhook:")
        print(json.dumps(data, indent=2))

        send_email(subject, body)

        return jsonify({"status": "ok", "message": "Email sent"}), 200

    except Exception as e:
        print(f"Webhook error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    validate_env()
    app.run(host="0.0.0.0", port=PORT)