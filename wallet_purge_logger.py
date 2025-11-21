import os
import json
import time
from typing import List, Dict
from dotenv import load_dotenv
import requests

# Load Telegram credentials from .env
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOG_FILE = os.path.join("logs", "wallet_purge_log.json")
os.makedirs("logs", exist_ok=True)

def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[!] Telegram credentials missing.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        r = requests.post(url, data=payload, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[✗] Telegram send error: {e}")
        return False

def log_and_alert_wallet_purge(reason: str, purged_wallets: List[Dict[str, float]]):
    if not purged_wallets:
        return

    ts = int(time.time())
    entry = {
        "timestamp": ts,
        "reason": reason,
        "wallets": purged_wallets,
    }

    # Save to JSON log
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []

        logs.append(entry)

        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2)

        print(f"[✓] Logged {len(purged_wallets)} purged wallets to log file.")
    except Exception as e:
        print(f"[✗] Wallet purge logging error: {e}")

    # Build Telegram alert
    top = purged_wallets[:5]
    summary = "\n".join([f"- {w['wallet'][:4]}… (P: ${w['profit']:.1f})" for w in top])
    msg = (
        f"🧹 Wallet Tracker Update\n\n"
        f"{len(purged_wallets)} wallets removed ({reason})\n\n"
        f"Top purged wallets:\n{summary}\n\n"
        f"📦 Logged to: wallet_purge_log.json"
    )

    send_telegram(msg)
