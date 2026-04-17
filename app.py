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

scanner_scores = {}
last_summary_sent = 0

email_queue = queue.Queue()

# =========================
# HELPERS
# =========================
def utc_now():
    return datetime.now(timezone.utc)

def validate_env():
    if ENABLE_EMAIL and (not EMAIL_ADDRESS or not EMAIL_PASSWORD):
        raise RuntimeError("Missing EMAIL credentials")

def ensure_csv():
    if not os.path.exists(CSV_LOG):
        with open(CSV_LOG, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","symbol","type","price","status","raw"])

def log(symbol, t, price, status, raw):
    ensure_csv()
    with open(CSV_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([utc_now().isoformat(), symbol, t, price, status, json.dumps(raw)])

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
        except Exception as e:
            print("[EMAIL ERROR]", e)
        email_queue.task_done()

threading.Thread(target=email_worker, daemon=True).start()

def enqueue_email(subject, body):
    email_queue.put({"subject": subject, "body": body})

# =========================
# SCANNER ENGINE
# =========================
def prune_hits():
    cutoff = utc_now() - timedelta(minutes=ROLLING_WINDOW_MINUTES)

    remove = []
    for sym, data in scanner_scores.items():
        data["hits"] = [h for h in data["hits"] if h >= cutoff]
        data["score"] = len(data["hits"])
        if not data["hits"]:
            remove.append(sym)

    for r in remove:
        del scanner_scores[r]

def update_score(symbol, price):
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

    prune_hits()

def get_top_symbols():
    prune_hits()
    ranked = sorted(
        scanner_scores.items(),
        key=lambda x: (x[1]["score"], x[1]["last"]),
        reverse=True
    )[:TOP_N]
    return [s for s,_ in ranked]

def in_top(symbol):
    return symbol in get_top_symbols()

def send_summary():
    global last_summary_sent

    now = time.time()
    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return

    top = sorted(scanner_scores.items(),
        key=lambda x: (x[1]["score"], x[1]["last"]),
        reverse=True
    )[:TOP_N]

    lines = ["Top Scanner (30m):\n"]

    for i,(s,d) in enumerate(top,1):
        lines.append(f"{i}. {s} score {d['score']}")

    enqueue_email("Top 5 Scanner", "\n".join(lines))
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
        "top": get_top_symbols(),
        "tracked": len(scanner_scores)
    }

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()

        symbol = data.get("symbol","UNK")
        typ = data.get("type","UNK")
        price = str(data.get("price","0"))

        print("RECV:", data)

        log(symbol, typ, price, "ok", data)

        # ===== SCANNER =====
        if typ == "SCANNER_TOP_STOCK":
            update_score(symbol, price)
            send_summary()

        # ===== ENTRY FILTER =====
        elif typ in ["ENTRY_READY","ENTRY_LONG"]:
            if in_top(symbol):
                enqueue_email(f"{typ} {symbol}",
                              f"{symbol} {typ} at {price}")
            else:
                print("FILTERED:", symbol)

        # ===== MANAGEMENT =====
        elif typ == "HOLD_OK":
            enqueue_email(f"HOLD {symbol}", f"{symbol} holding")

        elif typ == "HOLD_OVERNIGHT":
            enqueue_email(f"HOLD OVERNIGHT {symbol}", f"{symbol}")

        elif typ == "HOLD_OVER_WEEKEND":
            enqueue_email(f"HOLD WEEKEND {symbol}", f"{symbol}")

        elif typ == "SELL_INITIAL_STOP":
            enqueue_email(f"SELL STOP {symbol}", f"{symbol}")

        elif typ == "SELL_TRAILING_STOP":
            enqueue_email(f"SELL TRAIL {symbol}", f"{symbol}")

        elif typ == "SELL_TREND_WEAKENING":
            enqueue_email(f"SELL TREND {symbol}", f"{symbol}")

        elif typ == "SELL_END_OF_DAY":
            enqueue_email(f"SELL EOD {symbol}", f"{symbol}")

        elif typ == "SELL_BEFORE_WEEKEND":
            enqueue_email(f"SELL WEEKEND {symbol}", f"{symbol}")

        else:
            enqueue_email(f"{typ} {symbol}", json.dumps(data))

        return {"ok": True}

    except Exception as e:
        print("ERROR:", e)
        return {"error": str(e)}, 500

if __name__ == "__main__":
    validate_env()
    ensure_csv()
    app.run(host="0.0.0.0", port=PORT)
