# --- START OF FILE watchdog_usage.py ---

import os
import json
import time
import requests # For Telegram
import traceback
from datetime import datetime, date
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

# Read config from .env or use defaults matching api_usage_limiter.py
USAGE_FILE = os.getenv('API_USAGE_FILE', "api_daily_usage.json")
DAILY_API_CAP = int(os.getenv('DAILY_API_CAP', 322580))
API_BLOCKED_FLAG_FILE = os.getenv('API_BLOCKED_FLAG_FILE', ".api_blocked_flag")
CHECK_INTERVAL_SECONDS = int(os.getenv('WATCHDOG_CHECK_INTERVAL_SECONDS', 5 * 60)) # Check every 5 minutes default
ALERT_THRESHOLD_PERCENT = float(os.getenv('API_ALERT_THRESHOLD_PERCENT', 0.90)) # Re-check threshold here too

# Use dedicated Telegram vars if set, otherwise fallback to general ones
TELEGRAM_TOKEN_WATCHDOG = os.getenv("TELEGRAM_BOT_TOKEN_WATCHDOG", os.getenv("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID_WATCHDOG = os.getenv("TELEGRAM_CHAT_ID_WATCHDOG", os.getenv("TELEGRAM_CHAT_ID"))

# State to avoid sending duplicate alerts within a short period from the watchdog itself
_watchdog_alert_state = {
    "last_threshold_alert_day": None,
    "last_block_alert_day": None,
}

def send_telegram_alert_watchdog(message: str):
    """Sends alert specifically from the watchdog."""
    token = TELEGRAM_TOKEN_WATCHDOG
    chat_id = TELEGRAM_CHAT_ID_WATCHDOG
    if not token or not chat_id:
        print(f"WATCHDOG ALERT (Telegram Not Configured): {message}")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Using a different icon/header for watchdog alerts
    payload = {"chat_id": chat_id, "text": f"👁️ API Watchdog Alert 👁️\n\n{message}"}
    success = False
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        print(f"    [✓] Watchdog Alert Sent: {message.splitlines()[0]}...")
        success = True
    except Exception as e:
        print(f"    [✗] Failed to send Watchdog Telegram alert: {e}")
    return success

def check_usage_and_alert():
    """Reads usage file, checks status, and sends alerts if necessary."""
    global _watchdog_alert_state
    print(f"\n--- Watchdog Check @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    today_str = str(date.today())

    # Reset alert state if day has rolled over
    if _watchdog_alert_state["last_threshold_alert_day"] != today_str:
        _watchdog_alert_state["last_threshold_alert_day"] = None
    if _watchdog_alert_state["last_block_alert_day"] != today_str:
        _watchdog_alert_state["last_block_alert_day"] = None

    # --- Check Block Flag File ---
    # This is the most definitive indicator of being blocked by the limiter
    is_blocked_by_flag = os.path.exists(API_BLOCKED_FLAG_FILE)
    if is_blocked_by_flag and _watchdog_alert_state["last_block_alert_day"] != today_str:
        print("[!] Watchdog: Detected BLOCK FLAG file.")
        # Read usage file for context, even if blocked
        usage_data = {}
        if os.path.exists(USAGE_FILE):
             try:
                 with open(USAGE_FILE, "r", encoding="utf-8") as f: usage_data = json.load(f)
             except Exception: pass # Ignore errors reading file when blocked

        total = usage_data.get('total_calls_today', 'N/A')
        msg = ( f"API IS BLOCKED! (.api_blocked_flag exists)\n"
                f"Check logs and usage file.\n"
                f"Last reported total: {total}/{DAILY_API_CAP}")
        if send_telegram_alert_watchdog(msg):
            _watchdog_alert_state["last_block_alert_day"] = today_str
        return # Don't check thresholds if blocked by flag

    # --- Read Usage File ---
    if not os.path.exists(USAGE_FILE):
        print("Watchdog: Usage file not found yet.")
        return

    try:
        with open(USAGE_FILE, "r", encoding="utf-8") as f:
            usage_data = json.load(f)
    except Exception as e:
        print(f"Watchdog: Error reading usage file '{USAGE_FILE}': {e}")
        return

    # --- Validate Usage Data ---
    if usage_data.get("date") != today_str:
        print(f"Watchdog: Usage data date ({usage_data.get('date')}) doesn't match today ({today_str}). Skipping checks.")
        return

    total_calls = usage_data.get("total_calls_today", 0)
    helius_calls = usage_data.get("helius_calls_today", 0)
    birdeye_calls = usage_data.get("birdeye_calls_today", 0)
    other_calls = usage_data.get("other_calls_today", 0)
    # milestones_alerted = usage_data.get("milestones_alerted_today", []) # Use internal state instead

    print(f"Watchdog: Current Usage: Total={total_calls}, Helius={helius_calls}, Birdeye={birdeye_calls}, Other={other_calls} (Cap: {DAILY_API_CAP})")

    # --- Check Cap Numerically (Fallback if flag file failed) ---
    if total_calls >= DAILY_API_CAP and _watchdog_alert_state["last_block_alert_day"] != today_str:
         print("[!] Watchdog: Detected Usage >= Daily Cap.")
         msg = ( f"🚫 DAILY API CAP ({DAILY_API_CAP}) REACHED! 🚫\n"
                 f"Total: {total_calls}\n"
                 f"Helius: {helius_calls}\n"
                 f"Birdeye: {birdeye_calls}\n"
                 f"Other: {other_calls}\n"
                 "API calls should be blocked by limiter.")
         if send_telegram_alert_watchdog(msg):
             _watchdog_alert_state["last_block_alert_day"] = today_str
         return # Don't check threshold if cap is met

    # --- Check Threshold (e.g., 90%) ---
    threshold_limit = DAILY_API_CAP * ALERT_THRESHOLD_PERCENT
    if total_calls >= threshold_limit and _watchdog_alert_state["last_threshold_alert_day"] != today_str:
        print(f"[!] Watchdog: Detected Usage >= {ALERT_THRESHOLD_PERCENT*100:.0f}% Threshold.")
        msg = ( f"{ALERT_THRESHOLD_PERCENT*100:.0f}% of Daily API Cap Reached.\n"
                f"Total: {total_calls} / {DAILY_API_CAP}\n"
                f"Helius: {helius_calls}\n"
                f"Birdeye: {birdeye_calls}\n"
                f"Other: {other_calls}")
        if send_telegram_alert_watchdog(msg):
             _watchdog_alert_state["last_threshold_alert_day"] = today_str
    elif total_calls < threshold_limit:
        # Reset threshold alert state if usage drops below threshold (e.g., after daily reset)
         _watchdog_alert_state["last_threshold_alert_day"] = None

    print("Watchdog: Check complete.")


# ============================================================================
# Main Execution Loop for Watchdog
# ============================================================================
if __name__ == "__main__":
    print(f"Starting API Usage Watchdog...")
    print(f"Checking usage file: '{USAGE_FILE}'")
    print(f"Daily Cap set to: {DAILY_API_CAP}")
    print(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
    if not TELEGRAM_TOKEN_WATCHDOG or not TELEGRAM_CHAT_ID_WATCHDOG:
        print("Watchdog: WARNING - Telegram token/chat ID not set. Alerts will be console-only.")

    while True:
        try:
            check_usage_and_alert()
            # print(f"Watchdog: Sleeping for {CHECK_INTERVAL_SECONDS} seconds...") # Less verbose sleep message
            time.sleep(CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nWatchdog stopped by user.")
            break
        except Exception as e:
            print(f"[!!!] Watchdog: Unhandled error in main loop: {e}")
            traceback.print_exc()
            print("Watchdog: Retrying in 60 seconds...")
            time.sleep(60)

# --- END OF FILE watchdog_usage.py ---8