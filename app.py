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
SUMMARY_INTERVAL_SECONDS = 900
TOP_N = 5

NY_TZ = ZoneInfo("America/New_York")

scanner_state = {}
scanner_lock = threading.Lock()

cached_ranked = []
cached_top_symbols = []
cached_top_symbol_set = set()

email_queue = queue.Queue()
event_queue = queue.Queue()

csv_ready = False
csv_lock = threading.Lock()

last_summary_sent = 0


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
                    "raw_json"
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
                json.dumps(event["raw"], separators=(",", ":"))
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
            print("[EMAIL ERROR]", repr(e))

        finally:
            email_queue.task_done()


def enqueue_email(subject, body):
    email_queue.put({"subject": subject, "body": body})


def normalize_event(data):
    symbol = str(data.get("symbol", "UNK"))
    alert_type = str(data.get("type", "UNK"))
    price = str(data.get("price", "0"))
    alert_time_raw = str(data.get("time", "UNKNOWN"))

    return {
        "symbol": symbol,
        "type": alert_type,
        "price": price,
        "alert_time_raw": alert_time_raw,
        "alert_time_ny": convert_alert_time_to_ny(alert_time_raw),
        "received_time_ny": ny_now_str(),
        "raw": data,
    }


def _new_state_record(price, now):
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


def refresh_rankings():
    global cached_ranked, cached_top_symbols, cached_top_symbol_set

    cached_ranked = sorted(
        scanner_state.items(),
        key=lambda x: (
            x[1]["premarket_score"],
            x[1]["quality_score"],
            x[1]["early_leader_score"],
            x[1]["activity_score"],
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
            scanner_state[symbol] = _new_state_record(price, now)

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


def send_scanner_summary_if_due():
    global last_summary_sent

    now = time.time()

    if now - last_summary_sent < SUMMARY_INTERVAL_SECONDS:
        return

    ranked = get_ranked()

    if not ranked:
        return

    premarket_elite = [(s, d) for s, d in ranked if d["premarket_score"] == 2][:TOP_N]
    premarket_top = [(s, d) for s, d in ranked if d["premarket_score"] == 1][:TOP_N]
    elite = [(s, d) for s, d in ranked if d["quality_score"] == 2][:TOP_N]
    top_only = [(s, d) for s, d in ranked if d["quality_score"] == 1][:TOP_N]

    lines = [
        "Unified Scanner Summary",
        f"Generated: {ny_now_str()}",
        "",
        "Premarket Elite"
    ]

    if premarket_elite:
        for i, (s, d) in enumerate(premarket_elite, 1):
            early = " | Early Leader" if d["early_leader_score"] == 1 else ""
            lines.append(f"{i}. {s} | Price: {d['price']}{early}")
    else:
        lines.append("None")

    lines.extend(["", "Premarket Top"])

    if premarket_top:
        for i, (s, d) in enumerate(premarket_top, 1):
            early = " | Early Leader" if d["early_leader_score"] == 1 else ""
            lines.append(f"{i}. {s} | Price: {d['price']}{early}")
    else:
        lines.append("None")

    lines.extend(["", "Elite Picks"])

    if elite:
        for i, (s, d) in enumerate(elite, 1):
            pm = f" | {d['premarket_tier']}" if d["premarket_tier"] else ""
            early = " | Early Leader" if d["early_leader_score"] == 1 else ""
            lines.append(
                f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm}{early}"
            )
    else:
        lines.append("None")

    lines.extend(["", "Top Stocks"])

    if top_only:
        for i, (s, d) in enumerate(top_only, 1):
            pm = f" | {d['premarket_tier']}" if d["premarket_tier"] else ""
            early = " | Early Leader" if d["early_leader_score"] == 1 else ""
            lines.append(
                f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm}{early}"
            )
    else:
        lines.append("None")

    lines.extend([
        "",
        "How to use this:",
        "- Premarket Elite = strongest names before 9:30",
        "- Premarket Top = secondary premarket watchlist",
        "- Early Leader = pressure building after the open",
        "- Elite Picks = strongest confirmed intraday names",
        "- Wait for ENTRY_LONG to execute",
        "- ELITE_EARLY = starter position only",
    ])

    enqueue_email("Unified Scanner Summary", "\n".join(lines))
    last_summary_sent = now


def email_for_event(event):
    t = event["type"]
    s = event["symbol"]
    p = event["price"]
    at = event["alert_time_ny"]
    rt = event["received_time_ny"]

    if t in ("PREMARKET_TOP", "PREMARKET_ELITE"):
        return (
            f"{t.replace('_', ' ')} - {s}",
            f"{t}\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This stock is ranking highly in the premarket scan."
        )

    if t == "EARLY_LEADER":
        return (
            f"EARLY LEADER - {s}",
            f"EARLY LEADER\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This stock is showing early leadership characteristics.\n"
            f"Watch for ELITE_EARLY or ENTRY_LONG."
        )

    if t in ("ENTRY_READY", "ENTRY_LONG", "ELITE_EARLY"):
        return (
            f"{t.replace('_', ' ')} - {s}",
            f"{t}\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This symbol is currently in the unified scanner Top {TOP_N}."
        )

    if t == "LATE_DAY_RISK_OFF":
        return (
            f"LATE-DAY RISK OFF - {s}",
            f"LATE-DAY RISK OFF\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"Late-day weakness is developing.\n"
            f"Review whether to reduce exposure or exit before the close."
        )

    return (
        f"{t.replace('_', ' ')} - {s}",
        f"{t}\n\n"
        f"Symbol: {s}\n"
        f"Price: {p}\n"
        f"Alert Time (NY): {at}\n"
        f"Received Time (NY): {rt}\n\n"
        f"Raw Payload:\n{json.dumps(event['raw'], indent=2)}"
    )


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


def handle_scanner_event(event):
    update_scanner_state(event)

    if event["type"] in {"PREMARKET_TOP", "PREMARKET_ELITE", "EARLY_LEADER"}:
        subject, body = email_for_event(event)
        enqueue_email(subject, body)

    send_scanner_summary_if_due()


def handle_entry_event(event):
    if in_top(event["symbol"]):
        subject, body = email_for_event(event)
        enqueue_email(subject, body)
    else:
        print(f"[FILTERED {event['type']}] {event['symbol']} not in Top {TOP_N}")


def handle_management_event(event):
    subject, body = email_for_event(event)
    enqueue_email(subject, body)


def route_event(event):
    t = event["type"]

    if t in SCANNER_TYPES:
        handle_scanner_event(event)
    elif t in ENTRY_TYPES:
        handle_entry_event(event)
    elif t in MANAGEMENT_TYPES:
        handle_management_event(event)
    else:
        subject, body = email_for_event(event)
        enqueue_email(subject, body)


def event_worker():
    while True:
        event = event_queue.get()

        try:
            print(f"[PROCESS] {event['symbol']} {event['type']} {event['price']}")
            log_event(event)
            route_event(event)

        except Exception as e:
            print("[EVENT WORKER ERROR]", repr(e))
            print("[EVENT RAW]", json.dumps(event.get("raw", {}), indent=2))

        finally:
            event_queue.task_done()


threading.Thread(target=email_worker, daemon=True).start()
threading.Thread(target=event_worker, daemon=True).start()


@app.route("/")
def home():
    return "running"


@app.route("/health")
def health():
    return {
        "time_ny": ny_now_str(),
        "top_symbols": get_top_symbols(),
        "tracked": len(get_ranked()),
        "event_queue_size": event_queue.qsize(),
        "email_queue_size": email_queue.qsize(),
    }


@app.route("/test-email")
def test_email():
    try:
        subject = "Trading Bot Test"
        body = f"This is a direct test email.\nGenerated: {ny_now_str()}"

        if ENABLE_EMAIL:
            send_email(subject, body)
            print("[TEST EMAIL SENT DIRECTLY]")
            return {"ok": True, "message": "test email sent directly"}
        else:
            print("[TEST EMAIL DISABLED]")
            return {"ok": False, "message": "ENABLE_EMAIL is false"}

    except Exception as e:
        print("[TEST EMAIL ERROR]", repr(e))
        return {"ok": False, "error": str(e)}, 500


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(silent=True)

        if data is None:
            raw_body = request.data.decode("utf-8", errors="ignore").strip()
            print("[RAW BODY]", raw_body)
            data = json.loads(raw_body)

        if not isinstance(data, dict):
            print("[BAD PAYLOAD]", data)
            return {"ok": False, "error": "invalid json"}, 200

        event = normalize_event(data)

        print(f"[RECV QUEUED] {event['symbol']} {event['type']} {event['price']}")

        event_queue.put(event)

        return {"ok": True}, 200

    except Exception as e:
        print("[WEBHOOK PARSE ERROR]", repr(e))
        print("[REQUEST DATA]", request.data.decode("utf-8", errors="ignore"))
        return {"ok": False, "error": str(e)}, 200


if __name__ == "__main__":
    validate_env()
    ensure_csv()
    app.run(host="0.0.0.0", port=PORT)
