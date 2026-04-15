import os
import json
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = int(os.getenv("PORT", "10000"))

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
        print("Received webhook:")
        print(json.dumps(data, indent=2))

        return jsonify({"status": "ok", "message": "Webhook received"}), 200
    except Exception as e:
        print(f"Webhook error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
