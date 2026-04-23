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
SUMMARY_INTERVAL_SECONDS = 1800   # 30 minutes
TOP_N = 5

NY_TZ = ZoneInfo("America/New_York")

# Unified scanner state
unified_scanner = {}
unified_scanner_lock = threading.Lock()
cached_unified_ranked = []
cached_top_symbols = []
cached_top_symbol_set = set()

last_summary_sent = 0

email_queue = queue.Queue()
csv_ready = False
csv_lock = threading.Lock()

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
                    "status",
                    "raw_json"
                ])
        csv_ready = True

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
# UNIFIED SCANNER STATE / RANKING
# =========================
def refresh_unified_scanner_rankings():
    global cached_unified_ranked, cached_top_symbols, cached_top_symbol_set

    cached_unified_ranked = sorted(
        unified_scanner.items(),
        key=lambda x: (
            x[1].get("premarket_score", 0),      # premarket first
            x[1]["quality_score"],               # elite/top intraday
            x[1].get("early_leader_score", 0),   # early leader boost
            x[1]["activity_score"],              # rolling activity
            x[1]["last"]                         # recency
        ),
        reverse=True
    )
    cached_top_symbols = [symbol for symbol, _ in cached_unified_ranked[:TOP_N]]
    cached_top_symbol_set = set(cached_top_symbols)

def prune_unified_scanner_hits(now=None):
    now = now or utc_now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW_MINUTES)
    changed = False

    remove = []
    for sym, data in list(unified_scanner.items()):
        hits = data["hits"]

        while hits and hits[0] < cutoff:
            hits.popleft()
            changed = True

        new_activity_score = len(hits)
        if data["activity_score"] != new_activity_score:
            data["activity_score"] = new_activity_score
            changed = True

        # Keep symbols if they still have useful non-activity metadata
        if (
            not hits
            and data.get("quality_score", 0) == 0
            and data.get("early_leader_score", 0) == 0
            and data.get("premarket_score", 0) == 0
        ):
            remove.append(sym)

    for r in remove:
        del unified_scanner[r]
        changed = True

    if changed:
        refresh_unified_scanner_rankings()

def update_unified_scanner(symbol, alert_type, price):
    now = utc_now()
    quality_score = 2 if alert_type == "SCANNER_ELITE_STOCK" else 1

    with unified_scanner_lock:
        prune_unified_scanner_hits(now)

        if symbol not in unified_scanner:
            unified_scanner[symbol] = {
                "hits": deque([now]),
                "activity_score": 1,
                "quality_score": quality_score,
                "early_leader_score": 0,
                "premarket_score": 0,
                "premarket_tier": "",
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

        refresh_unified_scanner_rankings()

def update_early_leader(symbol, price):
    now = utc_now()

    with unified_scanner_lock:
        prune_unified_scanner_hits(now)

        if symbol not in unified_scanner:
            unified_scanner[symbol] = {
                "hits": deque(),
                "activity_score": 0,
                "quality_score": 0,
                "early_leader_score": 1,
                "premarket_score": 0,
                "premarket_tier": "",
                "tier": "EARLY_LEADER",
                "price": price,
                "last": now.isoformat()
            }
        else:
            unified_scanner[symbol]["early_leader_score"] = 1
            unified_scanner[symbol]["price"] = price
            unified_scanner[symbol]["last"] = now.isoformat()

        refresh_unified_scanner_rankings()

def update_premarket_symbol(symbol, alert_type, price):
    now = utc_now()
    premarket_score = 2 if alert_type == "PREMARKET_ELITE" else 1

    with unified_scanner_lock:
        prune_unified_scanner_hits(now)

        if symbol not in unified_scanner:
            unified_scanner[symbol] = {
                "hits": deque(),
                "activity_score": 0,
                "quality_score": 0,
                "early_leader_score": 0,
                "premarket_score": premarket_score,
                "premarket_tier": alert_type,
                "tier": alert_type,
                "price": price,
                "last": now.isoformat()
            }
        else:
            unified_scanner[symbol]["premarket_score"] = max(
                unified_scanner[symbol].get("premarket_score", 0),
                premarket_score
            )
            if alert_type == "PREMARKET_ELITE":
                unified_scanner[symbol]["premarket_tier"] = alert_type
            elif not unified_scanner[symbol].get("premarket_tier"):
                unified_scanner[symbol]["premarket_tier"] = alert_type

            unified_scanner[symbol]["price"] = price
            unified_scanner[symbol]["last"] = now.isoformat()

        refresh_unified_scanner_rankings()

def get_unified_ranked():
    with unified_scanner_lock:
        prune_unified_scanner_hits()
        return list(cached_unified_ranked)

def get_unified_top_symbols():
    with unified_scanner_lock:
        prune_unified_scanner_hits()
        return list(cached_top_symbols)

def in_unified_top(symbol):
    with unified_scanner_lock:
        prune_unified_scanner_hits()
        return symbol in cached_top_symbol_set

def get_unified_tracked_count():
    with unified_scanner_lock:
        prune_unified_scanner_hits()
        return len(unified_scanner)

def is_elite(symbol):
    with unified_scanner_lock:
        return symbol in unified_scanner and unified_scanner[symbol].get("quality_score", 0) == 2

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

    premarket_elite = [(s, d) for s, d in ranked if d.get("premarket_score", 0) == 2][:TOP_N]
    premarket_top = [(s, d) for s, d in ranked if d.get("premarket_score", 0) == 1][:TOP_N]
    elite = [(s, d) for s, d in ranked if d.get("quality_score", 0) == 2][:TOP_N]
    top_only = [(s, d) for s, d in ranked if d.get("quality_score", 0) == 1][:TOP_N]

    lines = [
        "Unified Scanner Summary",
        f"Generated: {ny_now_str()}",
        ""
    ]

    lines.append("Premarket Elite")
    if premarket_elite:
        for i, (s, d) in enumerate(premarket_elite, 1):
            early_tag = " | Early Leader" if d.get("early_leader_score", 0) == 1 else ""
            lines.append(f"{i}. {s} | Price: {d['price']}{early_tag}")
    else:
        lines.append("None")

    lines.extend(["", "Premarket Top"])
    if premarket_top:
        for i, (s, d) in enumerate(premarket_top, 1):
            early_tag = " | Early Leader" if d.get("early_leader_score", 0) == 1 else ""
            lines.append(f"{i}. {s} | Price: {d['price']}{early_tag}")
    else:
        lines.append("None")

    lines.extend(["", "Elite Picks"])
    if elite:
        for i, (s, d) in enumerate(elite, 1):
            early_tag = " | Early Leader" if d.get("early_leader_score", 0) == 1 else ""
            pm_tag = f" | {d.get('premarket_tier')}" if d.get("premarket_tier") else ""
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm_tag}{early_tag}")
    else:
        lines.append("None")

    lines.extend(["", "Top Stocks"])
    if top_only:
        for i, (s, d) in enumerate(top_only, 1):
            early_tag = " | Early Leader" if d.get("early_leader_score", 0) == 1 else ""
            pm_tag = f" | {d.get('premarket_tier')}" if d.get("premarket_tier") else ""
            lines.append(f"{i}. {s} | Activity Score: {d['activity_score']} | Price: {d['price']}{pm_tag}{early_tag}")
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
        "tracked": get_unified_tracked_count()
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
        # PREMARKET
        # =========================
        if alert_type in ["PREMARKET_TOP", "PREMARKET_ELITE"]:
            update_premarket_symbol(symbol, alert_type, price)
            enqueue_email(
                f"{alert_type.replace('_', ' ')} - {symbol}",
                f"{alert_type}\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n\n"
                f"This stock is ranking highly in the premarket scan."
            )
            send_unified_scanner_summary_if_due()

        # =========================
        # INTRADAY SCANNER
        # =========================
        elif alert_type in ["SCANNER_TOP_STOCK", "SCANNER_ELITE_STOCK"]:
            update_unified_scanner(symbol, alert_type, price)
            send_unified_scanner_summary_if_due()

        elif alert_type == "EARLY_LEADER":
            update_early_leader(symbol, price)
            enqueue_email(
                f"EARLY LEADER - {symbol}",
                f"EARLY LEADER\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n\n"
                f"This stock is showing early leadership characteristics.\n"
                f"Watch for ELITE_EARLY or ENTRY_LONG."
            )
            send_unified_scanner_summary_if_due()

        # =========================
        # ENTRY FILTER
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

        elif alert_type == "ELITE_EARLY":
            if in_unified_top(symbol):
                enqueue_email(
                    f"ELITE EARLY - {symbol}",
                    f"ELITE EARLY\n\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Alert Time (NY): {alert_time_ny}\n"
                    f"Received Time (NY): {ny_now_str()}\n\n"
                    f"This symbol is currently in the unified scanner Top {TOP_N}."
                )
            else:
                print("FILTERED ELITE_EARLY:", symbol)

        # =========================
        # MANAGEMENT ALERTS
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

        elif alert_type == "LATE_DAY_RISK_OFF":
            enqueue_email(
                f"LATE-DAY RISK OFF - {symbol}",
                f"LATE-DAY RISK OFF\n\n"
                f"Symbol: {symbol}\n"
                f"Price: {price}\n"
                f"Alert Time (NY): {alert_time_ny}\n"
                f"Received Time (NY): {ny_now_str()}\n\n"
                f"Late-day weakness is developing.\n"
                f"Review whether to reduce exposure or exit before the close."
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
