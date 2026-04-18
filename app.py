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

# Unified scanner state
unified_scanner = {}

# Email timing
last_summary_sent = 0

# Background email queue
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
    Convert incoming ISO-like UTC text to New York time string.
    If parsing fails, return original text.
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
# UNIFIED SCANNER
# =========================
def prune_unified_scanner_hits():
    cutoff = utc_now() - timedelta(minutes=ROLLING_WINDOW_MINUTES)

    remove = []
    for sym, data in unified_scanner.items():
        data["hits"] = [h for h in data["hits"] if h >= cutoff]
        data["activity_score"] = len(data["hits"])

        if not data["hits"]:
            remove.append(sym)

    for r in remove:
        del unified_scanner[r]


def update_unified_scanner(symbol, alert_type, price):
    """
    SCANNER_TOP_STOCK  -> quality_score = 1
    SCANNER_ELITE_STOCK -> quality_score = 2
    """
    now = utc_now()
    quality_score = 2 if alert_type == "SCANNER_ELITE_STOCK" else 1

    if symbol not in unified_scanner:
        unified_scanner[symbol] = {
            "hits": [now],
            "activity_score": 1,
            "quality_score": quality_score,
            "tier": alert_type,
            "price": price,
            "last": now.isoformat()
        }
    else:
        unified_scanner[symbol]["hits"].append(now)
        unified_scanner[symbol]["activity_score"] = len(unified_scanner[symbol]["hits"])
        unified_scanner[symbol]["quality_score"] = max(
            unified_scanner[symbol]["quality_score"],
            quality_score
        )
        if alert_type == "SCANNER_ELITE_STOCK":
            unified_scanner[symbol]["tier"] = alert_type
        unified_scanner[symbol]["price"] = price
        unified_scanner[symbol]["last"] = now.isoformat()

    prune_unified_scanner_hits()


def get_unified_ranked():
    prune_unified_scanner_hits()
    return sorted(
        unified_scanner.items(),
        key=lambda x: (
            x[1]["quality_score"],   # elite first
            x[1]["activity_score"],  # then most active
            x[1]["last"]             # then most recent
        ),
        reverse=True
    )


def get_unified_top_symbols():
    ranked = get_unified_ranked()[:TOP_N]
    return [symbol for symbol, _ in ranked]


def in_unified_top(symbol):
    return symbol in get_unified_top_symbols()

# =========================
# SUMMARY EMAIL
# =========================
def send_unified_scanner_summary_if_due():
    global last_summary_sent

    now = time.time()
    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return

    ranked = get_unified_ranked()

    if not ranked:
        return

    elite = [(s, d) for s, d in ranked if d["quality_score"] == 2][:TOP_N]
    top_only = [(s, d) for s, d in ranked if d["quality_score"] == 1][:TOP_N]

    lines = [
        "Unified Scanner Summary",
        f"Generated: {ny_now_str()}",
        "",
        "Elite Picks"
    ]

    if elite:
        for i, (s, d) in enumerate(elite, 1):
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}")
    else:
        lines.append("None")

    lines.extend(["", "Top Stocks"])

    if top_only:
        for i, (s, d) in enumerate(top_only, 1):
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}")
    else:
        lines.append("None")

    lines.extend([
        "",
        "How to use this:",
        "- Elite Picks = focus first",
        "- Top Stocks = secondary watchlist",
        "- Wait for ENTRY_READY or ENTRY_LONG before buying",
        "",
        "Scoring notes:",
        f"- Activity Score = hits in last {ROLLING_WINDOW_MINUTES} minutes",
        "- Elite outranks Top when all else is equal"
    ])

    enqueue_email("Unified Scanner Summary", "\n".join(lines))
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
        "top_symbols": get_unified_top_symbols(),
        "tracked": len(unified_scanner)
    }


@app.route("/test-email")
def test_email():
    enqueue_email(
        "Trading Bot Test",
        f"This is a test email.\nGenerated: {ny_now_str()}"
    )
    return {"ok": True, "message": "test email queued"}


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()

        if not isinstance(data, dict):
            return {"error": "invalid json"}, 400

        symbol = data.get("symbol", "UNK")
        alert_type = data.get("type", "UNK")
        price = str(data.get("price", "0"))
        alert_time_raw = str(data.get("time", "UNKNOWN"))
        alert_time_ny = iso_utc_to_ny_str(alert_time_raw)

        print("RECV:", data)

        log_alert(symbol, alert_type, price, alert_time_raw, "ok", data)

        # =========================
        # UNIFIED SCANNER
        # =========================
        if alert_type in ["SCANNER_TOP_STOCK", "SCANNER_ELITE_STOCK"]:
            update_unified_scanner(symbol, alert_type, price)
            send_unified_scanner_summary_if_due()

        # =========================
        # ENTRY FILTER
        # Only allow entry alerts if symbol is in unified Top 5
        # =========================
        elif alert_type == "ENTRY_READY":
            if in_unified_top(symbol):
                enqueue_email(
                    f"ENTRY READY - {symbol}",
                    f"ENTRY READY\n\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Alert Time (NY): {alert_time_ny}\n"
                    f"Received Time (NY): {ny_now_str()}\n\n"
                    f"This symbol is currently in the unified scanner Top {TOP_N}."
                )
            else:
                print("FILTERED ENTRY_READY:", symbol)

        elif alert_type == "ENTRY_LONG":
            if in_unified_top(symbol):
                enqueue_email(
                    f"ENTRY LONG - {symbol}",
                    f"ENTRY LONG\n\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Alert Time (NY): {alert_time_ny}\n"
                    f"Received Time (NY): {ny_now_str()}\n\n"
                    f"This symbol is currently in the unified scanner Top {TOP_N}."
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
