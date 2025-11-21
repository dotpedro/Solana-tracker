from flask import Flask, request, jsonify
from flask_cors import CORS
from collections import defaultdict
from datetime import datetime, UTC
import json

app = Flask(__name__)
CORS(app)

# In-memory cluster tracker
name_clusters = defaultdict(list)

def normalize_name(name: str) -> str:
    return ''.join(c.lower() for c in name if c.isalnum())

@app.route("/token_webhook", methods=["POST"])
def receive_token_mint():
    try:
        events = request.json

        # Debug: Print entire payload
        print("\n[Webhook Triggered] Raw Payload:")
        print(json.dumps(events, indent=2))

        if isinstance(events, dict):
            events = [events]  # wrap in list if single event

        for data in events:
            content = data.get("data", {}).get("content", {})
            metadata = content.get("metadata", {})
            mint_address = data.get("data", {}).get("id", "")
            name = metadata.get("name", "").strip()
            symbol = metadata.get("symbol", "").strip()

            if name and symbol and mint_address:
                norm_name = normalize_name(name)
                name_clusters[norm_name].append({
                    "mint": mint_address,
                    "name": name,
                    "symbol": symbol,
                    "time": datetime.now(UTC).isoformat()
                })

                print(f"\n🆕 Token detected: {name} ({symbol}) — {mint_address}")
                print(f"📊 Cluster '{norm_name.upper()}': {len(name_clusters[norm_name])} tokens")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ Error processing webhook:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 400

if __name__ == "__main__":
    app.run(port=5001)
