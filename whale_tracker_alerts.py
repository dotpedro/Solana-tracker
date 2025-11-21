# --- START OF FILE whale_tracker_alerts.py ---

#!/usr/bin/env python3
"""
Monitors top holders (whales) of previously alerted tokens
and sends alerts when significant sales are detected.
Reads ENRICHED reports from the 'processed' directory and
uses alert_history.json to identify target tokens.
Version: 1.1 - Added missing sleep definition
"""

import os
import json
import glob
import time
import requests
import datetime
import traceback
from datetime import timezone
from decimal import Decimal, getcontext # Use Decimal for precision with raw amounts
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# --- ENV & IMPORTS ---
load_dotenv()
TELEGRAM_TOKEN_WHALE   = os.getenv("TELEGRAM_BOT_TOKEN_WHALE", os.getenv("TELEGRAM_BOT_TOKEN")) # Allow specific token/chat
TELEGRAM_CHAT_ID_WHALE = os.getenv("TELEGRAM_CHAT_ID_WHALE", os.getenv("TELEGRAM_CHAT_ID"))

# Set Decimal precision
getcontext().prec = 30 # Set precision for Decimal calculations (adjust if needed)

if not (TELEGRAM_TOKEN_WHALE and TELEGRAM_CHAT_ID_WHALE):
    print("[!] WARNING: Whale Tracker Telegram credentials missing. Alerts will be console-only.")

# --- CONFIG ---
REPORTS_ROOT   = os.path.join("reports", "holdings")
PROCESSED_REPORTS_DIR = os.path.join(REPORTS_ROOT, "processed")
ALERT_HISTORY_FILE   = os.path.join(REPORTS_ROOT, "alert_history.json") # Read-only for this script
WHALE_STATE_FILE   = os.path.join(REPORTS_ROOT, "whale_tracker_state.json") # Maintains whale holdings

# Whale Tracking Parameters
TOP_N_WHALES_TO_TRACK = 10
SALE_THRESHOLD_PERCENT = 0.10
MIN_HOLDING_TO_TRACK_RAW = Decimal('1000000') # Example: Ignore if holding less than 1 raw unit (adjust as needed)

# Script Timing
CHECK_INTERVAL_SECONDS = 60
WHALE_ALERT_COOLDOWN_SECONDS = 60 * 60
SLEEP_BETWEEN_ALERTS_SECONDS = 2 # *** ADDED MISSING DEFINITION ***

os.makedirs(REPORTS_ROOT, exist_ok=True)

# --- UTILS ---
def send_telegram_whale(text: str) -> bool:
    token = TELEGRAM_TOKEN_WHALE
    chat_id = TELEGRAM_CHAT_ID_WHALE
    if not token or not chat_id:
        print(f"--- WHALE CONSOLE ALERT ---\n{text}\n--------------------")
        return True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": f"🚨🐋 Whale Sale Alert 🐋🚨\n\n{text}", "parse_mode": "Markdown", "disable_web_page_preview": "true"}
    try:
        r = requests.post(url, data=payload, timeout=20)
        r.raise_for_status()
        print(f"    [✓] Whale Alert Sent: {text.splitlines()[0]}")
        return True
    except requests.exceptions.HTTPError as e:
        error_content = e.response.text[:500]
        print(f"    [✗] Whale Alert Telegram HTTP error: {e.response.status_code} - {error_content}")
        return False
    except Exception as e:
        print(f"    [✗] Whale Alert Telegram general error: {e}")
        return False

def load_json_data(path: str, is_state_file: bool = False) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if is_state_file: print(f"    [i] State file not found: {path}. Will be created on first save.")
        else: print(f"    [!] Data file not found: {path}")
    except json.JSONDecodeError as e: print(f"    [!] Failed to decode JSON from {path}: {e}")
    except Exception as e: print(f"    [!] Failed to load {path}: {e}")
    return {}

def save_json_data(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: print(f"    [✗] Failed to save state data to {path}: {e}")

def latest_report_path() -> Optional[str]:
    processed_full_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_report_*.json")
    processed_delta_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_delta_*.json")
    all_files = glob.glob(processed_full_pattern) + glob.glob(processed_delta_pattern)
    if not all_files: return None
    try: return max(all_files, key=os.path.getmtime)
    except Exception as e: print(f"    [!] Error finding latest report path: {e}"); return None

def _format_compact_whale(value: Optional[Any], prefix: str = "", suffix: str = "", precision: int = 2) -> str:
    if value is None or value == "N/A": return "N/A"
    try:
        val = Decimal(value)
        abs_val = abs(val)
        if abs_val >= Decimal('1e12'): return f"{prefix}{val/Decimal('1e12'):.{precision}f}T{suffix}"
        if abs_val >= Decimal('1e9'): return f"{prefix}{val/Decimal('1e9'):.{precision}f}B{suffix}"
        if abs_val >= Decimal('1e6'): return f"{prefix}{val/Decimal('1e6'):.{precision}f}M{suffix}"
        if abs_val >= Decimal('1e3'): return f"{prefix}{val/Decimal('1e3'):.{precision}f}K{suffix}"
        if abs_val < 100: return f"{prefix}{val:.{min(precision, 4)}f}{suffix}"
        return f"{prefix}{val:,.0f}{suffix}"
    except Exception: return str(value) if value is not None else "N/A"

def format_whale_sale_alert(token_symbol: str, token_mint: str, whale_address: str,
                            amount_sold_ui: Decimal, sale_percent: float,
                            remaining_balance_ui: Decimal, decimals: int) -> str:
    whale_snippet = f"{whale_address[:5]}...{whale_address[-5:]}"
    amount_sold_str = _format_compact_whale(amount_sold_ui, suffix=f" {token_symbol}", precision=2)
    remaining_str = _format_compact_whale(remaining_balance_ui, suffix=f" {token_symbol}", precision=2)
    solscan_link = f"https://solscan.io/account/{whale_address}"
    dex_link = f"https://dexscreener.com/solana/{token_mint}"
    message = (
        f"*{token_symbol}* Sale Detected!\n"
        f"🐳 Whale: `{whale_snippet}`\n"
        f"📉 Sold: *{amount_sold_str}* ({sale_percent:.1%})\n"
        f"💰 Remaining: {remaining_str}\n\n"
        f"[Whale Wallet]({solscan_link}) | [DexScreener]({dex_link})"
    )
    return message

def process_report_for_whale_sales(report_path: str, alerted_tokens: set, whale_state: Dict[str, Any]) -> int:
    print(f"[*] Checking for whale sales in report: {os.path.basename(report_path)}")
    report_content = load_json_data(report_path)
    if not report_content: return 0

    processed_items_list: List[Dict[str, Any]] = []
    items_source_key = None
    if "correlated_holdings" in report_content and isinstance(report_content.get("correlated_holdings"), list) and report_content["correlated_holdings"]:
        processed_items_list = report_content["correlated_holdings"]
        items_source_key = "correlated_holdings"
    elif "correlated_holdings_snapshot" in report_content and isinstance(report_content.get("correlated_holdings_snapshot"), list) and report_content["correlated_holdings_snapshot"]:
        processed_items_list = report_content["correlated_holdings_snapshot"]
        items_source_key = "correlated_holdings_snapshot"

    if not processed_items_list:
        print(f"    [i] No items found using keys 'correlated_holdings' or 'correlated_holdings_snapshot'."); return 0

    print(f"    [i] Processing {len(processed_items_list)} items from key '{items_source_key}'.")
    alerts_sent_this_run = 0
    current_timestamp = int(time.time())

    if not isinstance(whale_state.get('tokens'), dict): whale_state['tokens'] = {}

    for item in processed_items_list:
        mint = item.get("token_mint")
        symbol = item.get("symbol", "N/A")
        if not mint or mint not in alerted_tokens: continue

        holder_data = item.get("bds_holder_data")
        if not isinstance(holder_data, list) or not holder_data: continue

        decimals = (item.get("bds_metadata") or {}).get("decimals")
        if decimals is None: decimals = (item.get("bds_market_data") or {}).get("decimals")
        if decimals is None or not isinstance(decimals, int) or decimals < 0:
            print(f"    [!] Invalid decimals for {symbol} ({mint[:6]}...). Skipping whale check."); continue

        if mint not in whale_state['tokens']: whale_state['tokens'][mint] = {'whales': {}}
        token_whale_state = whale_state['tokens'][mint]['whales']
        tracked_whales_in_report = set()

        for i, holder in enumerate(holder_data[:TOP_N_WHALES_TO_TRACK]):
            whale_address = holder.get("owner")
            current_amount_raw_str = holder.get("amount")
            current_ui_amount_str = holder.get("ui_amount")
            if not whale_address or current_amount_raw_str is None: continue

            try:
                current_amount_raw = Decimal(current_amount_raw_str)
                current_ui_amount = Decimal(current_ui_amount_str) if current_ui_amount_str is not None else current_amount_raw / (Decimal(10) ** decimals)
            except Exception as e:
                print(f"    [!] Error converting amount for whale {whale_address[:6]} of {symbol}: {e}. Skipping."); continue

            tracked_whales_in_report.add(whale_address)
            previous_whale_data = token_whale_state.get(whale_address)
            last_alert_ts = (previous_whale_data or {}).get("last_alert_timestamp", 0)
            time_since_last_whale_alert = current_timestamp - last_alert_ts

            if previous_whale_data:
                prev_amount_raw_str = previous_whale_data.get("last_seen_amount_raw")
                if prev_amount_raw_str is not None:
                    try:
                        prev_amount_raw = Decimal(prev_amount_raw_str)
                        sale_amount_raw = prev_amount_raw - current_amount_raw
                        if sale_amount_raw > 0:
                            min_holding_raw_adj = MIN_HOLDING_TO_TRACK_RAW * (Decimal(10) ** decimals) # Calculate threshold based on decimals
                            if prev_amount_raw >= min_holding_raw_adj:
                                sale_percent = float(sale_amount_raw / prev_amount_raw) if prev_amount_raw > 0 else 0.0
                                print(f"    Sale Detected: {symbol} Whale: {whale_address[:6]} Sold Raw: {sale_amount_raw} ({sale_percent:.2%}) Prev: {prev_amount_raw} Curr: {current_amount_raw}")
                                if sale_percent >= SALE_THRESHOLD_PERCENT:
                                    if time_since_last_whale_alert >= WHALE_ALERT_COOLDOWN_SECONDS:
                                        amount_sold_ui = sale_amount_raw / (Decimal(10) ** decimals)
                                        alert_msg = format_whale_sale_alert(symbol, mint, whale_address, amount_sold_ui, sale_percent, current_ui_amount, decimals)
                                        if send_telegram_whale(alert_msg):
                                            alerts_sent_this_run += 1
                                            token_whale_state[whale_address] = token_whale_state.get(whale_address, {})
                                            token_whale_state[whale_address]["last_alert_timestamp"] = current_timestamp
                                            time.sleep(SLEEP_BETWEEN_ALERTS_SECONDS) # *** Corrected reference ***
                                        else: print(f"    [✗] Failed to send whale sale alert for {whale_address[:6]} / {symbol}.")
                                    else: print(f"    Cooldown active for whale {whale_address[:6]} on {symbol}. Sale %: {sale_percent:.1%}. Skipping alert.")
                    except Exception as e_calc: print(f"    [!] Error calculating sale for whale {whale_address[:6]} of {symbol}: {e_calc}")

            # Always update state
            token_whale_state[whale_address] = token_whale_state.get(whale_address, {})
            token_whale_state[whale_address].update({
                "last_seen_amount_raw": str(current_amount_raw),
                "last_seen_ui_amount": str(current_ui_amount),
                "last_seen_report": os.path.basename(report_path),
                "last_seen_timestamp": current_timestamp
            })

        current_token_whales = set(token_whale_state.keys())
        whales_to_remove = current_token_whales - tracked_whales_in_report
        if whales_to_remove:
            print(f"    [i] Pruning {len(whales_to_remove)} whales no longer in top {TOP_N_WHALES_TO_TRACK} for {symbol} ({mint[:6]}...).")
            for whale_addr_remove in whales_to_remove: del token_whale_state[whale_addr_remove]

    return alerts_sent_this_run

def main_whale_loop():
    start_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"--- Starting Whale Tracker Alerts (v1.1) ({start_time_str}) ---") # Updated version
    print(f"Watching for processed reports in: {PROCESSED_REPORTS_DIR}")
    print(f"Tracking Top {TOP_N_WHALES_TO_TRACK} whales selling >= {SALE_THRESHOLD_PERCENT:.1%}")
    print(f"Alert History File (Read): {ALERT_HISTORY_FILE}")
    print(f"Whale State File (Read/Write): {WHALE_STATE_FILE}")

    latest_processed_report_path_handled: Optional[str] = None
    whale_tracker_state = load_json_data(WHALE_STATE_FILE, is_state_file=True)
    if 'tokens' not in whale_tracker_state: whale_tracker_state['tokens'] = {}

    while True:
        try:
            alert_history = load_json_data(ALERT_HISTORY_FILE)
            tokens_to_track = set(alert_history.keys())
            if not tokens_to_track:
                print(f"    No tokens found in alert history ({ALERT_HISTORY_FILE}). Waiting...")
                time.sleep(CHECK_INTERVAL_SECONDS * 2); continue

            current_latest_report_in_dir = latest_report_path()

            if not current_latest_report_in_dir: pass
            elif current_latest_report_in_dir != latest_processed_report_path_handled:
                print(f"\n[*] New latest processed report detected: {os.path.basename(current_latest_report_in_dir)}")
                state_before_processing = json.dumps(whale_tracker_state, sort_keys=True)
                alerts_triggered = process_report_for_whale_sales(current_latest_report_in_dir, tokens_to_track, whale_tracker_state)
                state_after_processing = json.dumps(whale_tracker_state, sort_keys=True)

                if state_before_processing != state_after_processing:
                    save_json_data(WHALE_STATE_FILE, whale_tracker_state)
                    print(f"    [i] Whale tracker state saved.")

                current_time_str_log = datetime.datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                if alerts_triggered > 0: print(f"    [✓] {alerts_triggered} WHALE SALE alerts sent based on '{os.path.basename(current_latest_report_in_dir)}' @ {current_time_str_log}")
                else: print(f"    [•] No significant whale sales detected in '{os.path.basename(current_latest_report_in_dir)}' @ {current_time_str_log}")

                latest_processed_report_path_handled = current_latest_report_in_dir

            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n--- Whale Tracker stopping due to user interrupt ---"); save_json_data(WHALE_STATE_FILE, whale_tracker_state); print("    Whale tracker state saved."); break
        except Exception as e:
            print(f"\n[!!!] Unhandled error in Whale Tracker main loop: {e}"); traceback.print_exc()
            print("    Attempting to save whale state before retrying..."); save_json_data(WHALE_STATE_FILE, whale_tracker_state)
            error_message_tg = f"🚨 Whale Tracker Main Loop Error 🚨\n\n`{type(e).__name__}: {str(e)}`\n\nSee console/logs. Will retry."
            send_telegram_whale(error_message_tg); print(f"    Retrying in {CHECK_INTERVAL_SECONDS*2} seconds..."); time.sleep(CHECK_INTERVAL_SECONDS*2)

if __name__ == "__main__":
    main_whale_loop()

# --- END OF FILE whale_tracker_alerts.py ---