import os
import json
import csv
import time
import queue
import threading
import smtplib
from collections import deque
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo
from flask import Flask, request

app = Flask(__name__)

PORT = int(os.getenv("PORT", "10000"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)

ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"
CSV_LOG = os.getenv("CSV_LOG", "alerts_log.csv")

ROLLING_WINDOW_MINUTES = 30
SCANNER_SUMMARY_INTERVAL_SECONDS = int(os.getenv("SCANNER_SUMMARY_INTERVAL_SECONDS", "60"))
SWING_SUMMARY_INTERVAL_SECONDS = int(os.getenv("SWING_SUMMARY_INTERVAL_SECONDS", "600"))
TOP_N = 10

NY_TZ = ZoneInfo("America/New_York")

scanner_state = {}
scanner_lock = threading.Lock()
last_scanner_summary_sent = 0

swing_state = {}
swing_lock = threading.Lock()
last_swing_summary_sent = 0

cached_ranked = []
cached_top_symbols = []
cached_top_symbol_set = set()

email_queue = queue.Queue()
event_queue = queue.Queue()

csv_ready = False
csv_lock = threading.Lock()


def utc_now():
    return datetime.now(timezone.utc)


def ny_now():
    return datetime.now(NY_TZ)


def ny_now_str():
    return ny_now().strftime("%Y-%m-%d %I:%M:%S %p %Z")


def utc_now_iso():
    return utc_now().isoformat()


def convert_alert_time_to_ny(time_text):
    if not time_text:
        return "UNKNOWN"

    try:
        text = str(time_text).strip()

        if text.isdigit():
            dt = datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc)
            return dt.astimezone(NY_TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")

        cleaned = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(NY_TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    except Exception:
        return str(time_text)


def validate_env():
    if ENABLE_EMAIL and (not EMAIL_ADDRESS or not EMAIL_PASSWORD):
        raise RuntimeError("Missing EMAIL_ADDRESS or EMAIL_PASSWORD")


def ensure_csv():
    global csv_ready

    if csv_ready:
        return

    with csv_lock:
        if csv_ready:
            return

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
                    "raw_json",
                ])

        csv_ready = True


def log_event(event):
    ensure_csv()

    with csv_lock:
        with open(CSV_LOG, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                ny_now_str(),
                utc_now_iso(),
                event["symbol"],
                event["type"],
                event["price"],
                event["alert_time_ny"],
                json.dumps(event["raw"], separators=(",", ":")),
            ])


def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)


def enqueue_email(subject, body):
    email_queue.put({"subject": subject, "body": body})


def email_worker():
    while True:
        job = email_queue.get()

        try:
            if ENABLE_EMAIL:
                send_email(job["subject"], job["body"])
                print("[EMAIL SENT]", job["subject"], flush=True)
            else:
                print("[EMAIL DISABLED]", job["subject"], flush=True)

        except Exception as e:
            print("[EMAIL ERROR]", repr(e), flush=True)

        finally:
            email_queue.task_done()


def normalize_event(data):
    symbol = str(data.get("symbol", "UNK")).upper().strip()
    alert_type = str(data.get("type", "UNK")).upper().strip()
    price = str(data.get("price", "0"))
    alert_time_raw = str(data.get("time", "UNKNOWN"))

    if alert_type == "SWING_MASTER":
        code = str(data.get("signal_code", "0")).strip()

        if code in ("1", "1.0"):
            alert_type = "SWING_BUY"
        elif code in ("2", "2.0"):
            alert_type = "SWING_BREAKOUT"
        elif code in ("3", "3.0"):
            alert_type = "SWING_WARNING"
        elif code in ("4", "4.0"):
            alert_type = "SWING_EXIT"
        else:
            alert_type = "SWING_UNKNOWN"

    return {
        "symbol": symbol,
        "type": alert_type,
        "price": price,
        "alert_time_raw": alert_time_raw,
        "alert_time_ny": convert_alert_time_to_ny(alert_time_raw),
        "received_time_ny": ny_now_str(),
        "raw": data,
    }


# =========================
# SCANNER STATE + RANKING
# =========================

def _new_scanner_record(price, now):
    return {
        "hits": deque(),
        "activity_score": 0,
        "quality_score": 0,
        "premarket_score": 0,
        "premarket_tier": "",
        "early_leader_score": 0,
        "price": price,
        "last": now.isoformat(),
    }


def scanner_score(rec):
    return (
        rec.get("premarket_score", 0) * 30
        + rec.get("quality_score", 0) * 25
        + rec.get("early_leader_score", 0) * 20
        + rec.get("activity_score", 0) * 5
    )


def refresh_rankings():
    global cached_ranked, cached_top_symbols, cached_top_symbol_set

    cached_ranked = sorted(
        scanner_state.items(),
        key=lambda x: (
            scanner_score(x[1]),
            x[1]["last"],
        ),
        reverse=True,
    )

    cached_top_symbols = [symbol for symbol, _ in cached_ranked[:TOP_N]]
    cached_top_symbol_set = set(cached_top_symbols)


def prune_scanner_state(now=None):
    now = now or utc_now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW_MINUTES)
    changed = False
    remove = []

    for symbol, rec in list(scanner_state.items()):
        while rec["hits"] and rec["hits"][0] < cutoff:
            rec["hits"].popleft()
            changed = True

        new_activity = len(rec["hits"])
        if rec["activity_score"] != new_activity:
            rec["activity_score"] = new_activity
            changed = True

        if (
            rec["activity_score"] == 0
            and rec["quality_score"] == 0
            and rec["premarket_score"] == 0
            and rec["early_leader_score"] == 0
        ):
            remove.append(symbol)

    for symbol in remove:
        del scanner_state[symbol]
        changed = True

    if changed:
        refresh_rankings()


def get_ranked():
    with scanner_lock:
        prune_scanner_state()
        return list(cached_ranked)


def get_top_symbols():
    with scanner_lock:
        prune_scanner_state()
        return list(cached_top_symbols)


def in_top(symbol):
    with scanner_lock:
        prune_scanner_state()
        return symbol in cached_top_symbol_set


def update_scanner_state(event):
    now = utc_now()
    symbol = event["symbol"]
    alert_type = event["type"]
    price = event["price"]

    with scanner_lock:
        prune_scanner_state(now)

        if symbol not in scanner_state:
            scanner_state[symbol] = _new_scanner_record(price, now)

        rec = scanner_state[symbol]
        rec["price"] = price
        rec["last"] = now.isoformat()

        if alert_type == "SCANNER_TOP_STOCK":
            rec["hits"].append(now)
            rec["activity_score"] = len(rec["hits"])
            rec["quality_score"] = max(rec["quality_score"], 1)

        elif alert_type == "SCANNER_ELITE_STOCK":
            rec["hits"].append(now)
            rec["activity_score"] = len(rec["hits"])
            rec["quality_score"] = max(rec["quality_score"], 2)

        elif alert_type == "PREMARKET_TOP":
            rec["premarket_score"] = max(rec["premarket_score"], 1)
            if not rec["premarket_tier"]:
                rec["premarket_tier"] = "PREMARKET_TOP"

        elif alert_type == "PREMARKET_ELITE":
            rec["premarket_score"] = max(rec["premarket_score"], 2)
            rec["premarket_tier"] = "PREMARKET_ELITE"

        elif alert_type == "EARLY_LEADER":
            rec["early_leader_score"] = 1

        refresh_rankings()


def send_scanner_summary_if_due(force=False):
    global last_scanner_summary_sent

    now = time.time()

    if not force and now - last_scanner_summary_sent < SCANNER_SUMMARY_INTERVAL_SECONDS:
        return

    ranked = get_ranked()

    if not ranked:
        return

    lines = [
        "Scanner Ranked Summary",
        f"Generated: {ny_now_str()}",
        "",
        "Top Ranked Stocks",
    ]

    for i, (symbol, rec) in enumerate(ranked[:TOP_N], 1):
        tags = []

        if rec.get("premarket_tier"):
            tags.append(rec["premarket_tier"])

        if rec.get("quality_score", 0) == 2:
            tags.append("ELITE")
        elif rec.get("quality_score", 0) == 1:
            tags.append("TOP")

        if rec.get("early_leader_score", 0) == 1:
            tags.append("EARLY_LEADER")

        tag_text = ", ".join(tags) if tags else "TRACKED"

        lines.append(
            f"{i}. {symbol} | Score: {scanner_score(rec)} | {tag_text} | "
            f"Activity: {rec['activity_score']} | Price: {rec['price']}"
        )

    lines.extend([
        "",
        "Score model:",
        "Premarket Elite/Top + Scanner Elite/Top + Early Leader + Activity",
        "",
        "Use this as ranking/watchlist guidance, not automatic buy.",
    ])

    enqueue_email("Scanner Ranked Summary", "\n".join(lines))
    last_scanner_summary_sent = now


# =========================
# SWING STATE + RANKING
# =========================

def swing_signal_score(alert_type):
    if alert_type == "SWING_BREAKOUT":
        return 90
    if alert_type == "SWING_BUY":
        return 85
    if alert_type == "SWING_WARNING":
        return 45
    if alert_type == "SWING_EXIT":
        return 10
    return 0


def swing_status(alert_type):
    if alert_type == "SWING_BREAKOUT":
        return "BREAKOUT"
    if alert_type == "SWING_BUY":
        return "BUY ZONE"
    if alert_type == "SWING_WARNING":
        return "WARNING"
    if alert_type == "SWING_EXIT":
        return "EXIT"
    return "UNKNOWN"


def update_swing_state(event):
    symbol = event["symbol"]
    score = swing_signal_score(event["type"])

    with swing_lock:
        old = swing_state.get(symbol, {})

        swing_state[symbol] = {
            "symbol": symbol,
            "last_signal": event["type"],
            "status": swing_status(event["type"]),
            "score": score,
            "price": event["price"],
            "alert_time_ny": event["alert_time_ny"],
            "received_time_ny": event["received_time_ny"],
            "count": int(old.get("count", 0)) + 1,
            "raw": event["raw"],
        }


def get_swing_state_snapshot():
    with swing_lock:
        return dict(swing_state)


def get_ranked_swing():
    with swing_lock:
        return sorted(
            swing_state.values(),
            key=lambda x: (
                x.get("score", 0),
                x.get("count", 0),
                x.get("received_time_ny", ""),
            ),
            reverse=True,
        )


def send_swing_summary_if_due(force=False):
    global last_swing_summary_sent

    now = time.time()

    if not force and now - last_swing_summary_sent < SWING_SUMMARY_INTERVAL_SECONDS:
        return

    ranked = get_ranked_swing()

    if not ranked:
        return

    lines = [
        "Swing Trading Summary",
        f"Generated: {ny_now_str()}",
        "",
        "Top Swing Candidates / Alerts",
    ]

    for i, rec in enumerate(ranked[:10], 1):
        lines.append(
            f"{i}. {rec['symbol']} | {rec['status']} | Score: {rec['score']} | "
            f"Price: {rec['price']} | Count: {rec['count']} | Last: {rec['received_time_ny']}"
        )

    lines.extend([
        "",
        "Score meaning:",
        "90 = breakout strength",
        "85 = pullback/bounce buy zone",
        "45 = warning / tighten risk",
        "10 = exit condition",
        "",
        "Use this as swing watchlist ranking, not automatic buy/sell.",
    ])

    enqueue_email("Swing Trading Summary", "\n".join(lines))
    last_swing_summary_sent = now


# =========================
# EMAIL TEMPLATES
# =========================

def email_for_event(event):
    t = event["type"]
    s = event["symbol"]
    p = event["price"]
    at = event["alert_time_ny"]
    rt = event["received_time_ny"]

    return (
        f"{t.replace('_', ' ')} - {s}",
        f"{t}\n\n"
        f"Symbol: {s}\n"
        f"Price: {p}\n"
        f"Alert Time: {at}\n"
        f"Received: {rt}\n\n"
        f"Raw:\n{json.dumps(event['raw'], indent=2)}"
    )


# =========================
# ROUTING
# =========================

SCANNER_TYPES = {
    "PREMARKET_TOP",
    "PREMARKET_ELITE",
    "SCANNER_TOP_STOCK",
    "SCANNER_ELITE_STOCK",
    "EARLY_LEADER",
}

ENTRY_TYPES = {
    "ENTRY_READY",
    "ENTRY_LONG",
    "ELITE_EARLY",
}

MANAGEMENT_TYPES = {
    "HOLD_OK",
    "TARGET_CHECKPOINT_HIT",
    "SELL_INITIAL_STOP",
    "SELL_TRAILING_STOP",
    "SELL_TREND_WEAKENING",
    "SELL_END_OF_DAY",
    "HOLD_OVERNIGHT",
    "HOLD_OVER_WEEKEND",
    "SELL_BEFORE_WEEKEND",
    "LATE_DAY_RISK_OFF",
}

SWING_TYPES = {
    "SWING_BUY",
    "SWING_BREAKOUT",
    "SWING_WARNING",
    "SWING_EXIT",
    "SWING_UNKNOWN",
}


def handle_scanner_event(event):
    update_scanner_state(event)
    send_scanner_summary_if_due(force=False)


def handle_entry_event(event):
    if in_top(event["symbol"]):
        subject, body = email_for_event(event)
        enqueue_email(subject, body)
    else:
        print(f"[FILTERED {event['type']}] {event['symbol']} not in Top {TOP_N}", flush=True)


def handle_management_event(event):
    subject, body = email_for_event(event)
    enqueue_email(subject, body)


def handle_swing_event(event):
    update_swing_state(event)
    send_swing_summary_if_due(force=False)


def route_event(event):
    t = event["type"]

    if t in SCANNER_TYPES:
        handle_scanner_event(event)
    elif t in ENTRY_TYPES:
        handle_entry_event(event)
    elif t in MANAGEMENT_TYPES:
        handle_management_event(event)
    elif t in SWING_TYPES:
        handle_swing_event(event)
    else:
        subject, body = email_for_event(event)
        enqueue_email(subject, body)


def event_worker():
    while True:
        event = event_queue.get()

        try:
            print(f"[PROCESS] {event['symbol']} {event['type']} {event['price']}", flush=True)
            log_event(event)
            route_event(event)

        except Exception as e:
            print("[EVENT WORKER ERROR]", repr(e), flush=True)
            print("[EVENT RAW]", json.dumps(event.get("raw", {}), indent=2), flush=True)

        finally:
            event_queue.task_done()


threading.Thread(target=email_worker, daemon=True).start()
threading.Thread(target=event_worker, daemon=True).start()


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
        "top_symbols": get_top_symbols(),
        "tracked_scanner": len(get_ranked()),
        "tracked_swing": len(get_swing_state_snapshot()),
        "event_queue_size": event_queue.qsize(),
        "email_queue_size": email_queue.qsize(),
        "scanner_summary_interval_seconds": SCANNER_SUMMARY_INTERVAL_SECONDS,
        "swing_summary_interval_seconds": SWING_SUMMARY_INTERVAL_SECONDS,
    }


@app.route("/scanner-state")
def scanner_state_route():
    return {symbol: rec for symbol, rec in get_ranked()}


@app.route("/scanner-summary")
def scanner_summary_route():
    send_scanner_summary_if_due(force=True)
    return {"ok": True, "message": "scanner summary queued"}


@app.route("/swing-state")
def swing_state_route():
    return get_swing_state_snapshot()


@app.route("/swing-summary")
def swing_summary_route():
    send_swing_summary_if_due(force=True)
    return {"ok": True, "message": "swing summary queued"}


@app.route("/test-email")
def test_email():
    try:
        subject = "Trading Bot Test"
        body = f"This is a direct test email.\nGenerated: {ny_now_str()}"

        if ENABLE_EMAIL:
            send_email(subject, body)
            return {"ok": True, "message": "test email sent directly"}
        else:
            return {"ok": False, "message": "ENABLE_EMAIL is false"}

    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw_body = request.data.decode("utf-8", errors="ignore").strip()
        print("[WEBHOOK HIT]", raw_body, flush=True)

        data = request.get_json(silent=True)

        if data is None:
            data = json.loads(raw_body)

        if not isinstance(data, dict):
            print("[BAD PAYLOAD]", raw_body, flush=True)
            return {"ok": False, "error": "invalid json"}, 200

        event = normalize_event(data)

        print(f"[RECV QUEUED] {event['symbol']} {event['type']} {event['price']}", flush=True)

        event_queue.put(event)

        return {"ok": True}, 200

    except Exception as e:
        print("[WEBHOOK ERROR]", repr(e), flush=True)
        print("[REQUEST DATA]", request.data.decode("utf-8", errors="ignore"), flush=True)

        try:
            send_email(
                "WEBHOOK ERROR",
                f"Error:\n{repr(e)}\n\nRaw:\n{request.data.decode('utf-8', errors='ignore')}"
            )
        except Exception as email_error:
            print("[WEBHOOK ERROR EMAIL FAILED]", repr(email_error), flush=True)

        return {"ok": False, "error": str(e)}, 200


if __name__ == "__main__":
    validate_env()
    ensure_csv()
    app.run(host="0.0.0.0", port=PORT)
