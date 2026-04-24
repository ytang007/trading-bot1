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

# =====================================================
# CONFIG
# =====================================================
PORT = int(os.getenv("PORT", "10000"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL", EMAIL_ADDRESS)

ENABLE_EMAIL = os.getenv("ENABLE_EMAIL", "true").lower() == "true"
CSV_LOG = os.getenv("CSV_LOG", "alerts_log.csv")

ROLLING_WINDOW_MINUTES = 30
SUMMARY_INTERVAL_SECONDS = 900   # 15 minutes
TOP_N = 5

NY_TZ = ZoneInfo("America/New_York")

# =====================================================
# GLOBAL STATE
# =====================================================
scanner_state = {}
scanner_lock = threading.Lock()

cached_ranked = []
cached_top_symbols = []
cached_top_symbol_set = set()

email_queue = queue.Queue()
csv_ready = False
csv_lock = threading.Lock()

last_summary_sent = 0

# =====================================================
# TIME HELPERS
# =====================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ny_now() -> datetime:
    return datetime.now(NY_TZ)

def ny_now_str() -> str:
    return ny_now().strftime("%Y-%m-%d %I:%M:%S %p %Z")

def utc_now_iso() -> str:
    return utc_now().isoformat()

def iso_utc_to_ny_str(iso_text: str) -> str:
    if not iso_text:
        return "UNKNOWN"
    try:
        cleaned = iso_text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(NY_TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return iso_text

# =====================================================
# CSV / EMAIL
# =====================================================
def validate_env():
    if ENABLE_EMAIL and (not EMAIL_ADDRESS or not EMAIL_PASSWORD):
        raise RuntimeError("Missing EMAIL credentials")

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

def log_event(event: dict):
    ensure_csv()
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

def send_email(subject: str, body: str):
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

def enqueue_email(subject: str, body: str):
    email_queue.put({"subject": subject, "body": body})

# =====================================================
# EVENT NORMALIZATION
# =====================================================
def normalize_event(data: dict) -> dict:
    symbol = str(data.get("symbol", "UNK"))
    alert_type = str(data.get("type", "UNK"))
    price = str(data.get("price", "0"))
    alert_time_raw = str(data.get("time", "UNKNOWN"))

    return {
        "symbol": symbol,
        "type": alert_type,
        "price": price,
        "alert_time_raw": alert_time_raw,
        "alert_time_ny": iso_utc_to_ny_str(alert_time_raw),
        "received_time_ny": ny_now_str(),
        "raw": data,
    }

# =====================================================
# SCANNER STATE
# =====================================================
def _new_state_record(price: str, now: datetime):
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

def refresh_rankings():
    global cached_ranked, cached_top_symbols, cached_top_symbol_set

    cached_ranked = sorted(
        scanner_state.items(),
        key=lambda x: (
            x[1]["premarket_score"],
            x[1]["quality_score"],
            x[1]["early_leader_score"],
            x[1]["activity_score"],
            x[1]["last"]
        ),
        reverse=True
    )

    cached_top_symbols = [symbol for symbol, _ in cached_ranked[:TOP_N]]
    cached_top_symbol_set = set(cached_top_symbols)

def get_ranked():
    with scanner_lock:
        prune_scanner_state()
        return list(cached_ranked)

def get_top_symbols():
    with scanner_lock:
        prune_scanner_state()
        return list(cached_top_symbols)

def in_top(symbol: str) -> bool:
    with scanner_lock:
        prune_scanner_state()
        return symbol in cached_top_symbol_set

def update_scanner_state(event: dict):
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

# =====================================================
# EMAIL TEMPLATES
# =====================================================
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
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm}{early}")
    else:
        lines.append("None")

    lines.extend(["", "Top Stocks"])
    if top_only:
        for i, (s, d) in enumerate(top_only, 1):
            pm = f" | {d['premarket_tier']}" if d["premarket_tier"] else ""
            early = " | Early Leader" if d["early_leader_score"] == 1 else ""
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm}{early}")
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
        "- ELITE_EARLY = starter position only"
    ])

    enqueue_email("Unified Scanner Summary", "\n".join(lines))
    last_summary_sent = now

def email_for_event(event: dict):
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

    if t == "ENTRY_READY":
        return (
            f"ENTRY READY - {s}",
            f"ENTRY READY\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This symbol is currently in the unified scanner Top {TOP_N}."
        )

    if t == "ENTRY_LONG":
        return (
            f"ENTRY LONG - {s}",
            f"ENTRY LONG\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This symbol is currently in the unified scanner Top {TOP_N}."
        )

    if t == "ELITE_EARLY":
        return (
            f"ELITE EARLY - {s}",
            f"ELITE EARLY\n\n"
            f"Symbol: {s}\n"
            f"Price: {p}\n"
            f"Alert Time (NY): {at}\n"
            f"Received Time (NY): {rt}\n\n"
            f"This symbol is currently in the unified scanner Top {TOP_N}."
        )

    if t == "HOLD_OK":
        return (f"HOLD - {s}", f"HOLD\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "TARGET_CHECKPOINT_HIT":
        return (f"TARGET CHECKPOINT - {s}", f"TARGET CHECKPOINT HIT\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "SELL_INITIAL_STOP":
        return (f"SELL INITIAL STOP - {s}", f"SELL INITIAL STOP\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "SELL_TRAILING_STOP":
        return (f"SELL TRAILING STOP - {s}", f"SELL TRAILING STOP\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "SELL_TREND_WEAKENING":
        return (f"SELL TREND WEAKENING - {s}", f"SELL TREND WEAKENING\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "SELL_END_OF_DAY":
        return (f"SELL EOD - {s}", f"SELL END OF DAY\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "HOLD_OVERNIGHT":
        return (f"HOLD OVERNIGHT - {s}", f"HOLD OVERNIGHT\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "HOLD_OVER_WEEKEND":
        return (f"HOLD OVER WEEKEND - {s}", f"HOLD OVER WEEKEND\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

    if t == "SELL_BEFORE_WEEKEND":
        return (f"SELL BEFORE WEEKEND - {s}", f"SELL BEFORE WEEKEND\n\nSymbol: {s}\nPrice: {p}\nAlert Time (NY): {at}\nReceived Time (NY): {rt}\n")

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
        f"{t} - {s}",
        f"GENERAL ALERT\n\n"
        f"Symbol: {s}\n"
        f"Price: {p}\n"
        f"Alert Time (NY): {at}\n"
        f"Received Time (NY): {rt}\n\n"
        f"Raw Payload:\n{json.dumps(event['raw'], indent=2)}"
    )

# =====================================================
# ROUTING
# =====================================================
SCANNER_TYPES = {
    "PREMARKET_TOP",
    "PREMARKET_ELITE",
    "SCANNER_TOP_STOCK",
    "SCANNER_ELITE_STOCK",
    "EARLY_LEADER",
}

ENTRY_TYPES = {"ENTRY_READY", "ENTRY_LONG", "ELITE_EARLY"}

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

def handle_scanner_event(event: dict):
    update_scanner_state(event)

    if event["type"] in {"PREMARKET_TOP", "PREMARKET_ELITE", "EARLY_LEADER"}:
        subject, body = email_for_event(event)
        enqueue_email(subject, body)

    send_scanner_summary_if_due()

def handle_entry_event(event: dict):
    if in_top(event["symbol"]):
        subject, body = email_for_event(event)
        enqueue_email(subject, body)
    else:
        print(f"[FILTERED {event['type']}] {event['symbol']} not in Top {TOP_N}")

def handle_management_event(event: dict):
    subject, body = email_for_event(event)
    enqueue_email(subject, body)

def route_event(event: dict):
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

# =====================================================
# ROUTES
# =====================================================
@app.route("/")
def home():
    return "running"

@app.route("/health")
def health():
    return {
        "time_ny": ny_now_str(),
        "top_symbols": get_top_symbols(),
        "tracked": len(get_ranked())
    }

@app.route("/test-email")
def test_email():
    enqueue_email("Trading Bot Test", f"This is a test email.\nGenerated: {ny_now_str()}")
    return {"ok": True, "message": "test email queued"}

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()

        if not isinstance(data, dict):
            return {"error": "invalid json"}, 400

        event = normalize_event(data)
        print(f"[RECV] {event['symbol']} {event['type']} {event['price']}")

        log_event(event)
        route_event(event)

        return {"ok": True}
    except Exception as e:
        print("[ERROR]", e)
        return {"error": str(e)}, 500

if __name__ == "__main__":
    validate_env()
    ensure_csv()
    app.run(host="0.0.0.0", port=PORT)
