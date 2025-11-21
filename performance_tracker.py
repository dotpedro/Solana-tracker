# --- START OF FILE performance_tracker.py ---

import os
import time
import json
import traceback
# import requests # <-- REMOVED: No longer needed directly
import math
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple, Set
from dotenv import load_dotenv

# --- Required Imports ---
load_dotenv()

# --- NEW: Import the Birdeye API gateway ---
try:
    import birdeye_api
    print("[✓] performance_tracker: Imported birdeye_api gateway.")
except ImportError:
    print("CRITICAL: birdeye_api.py not found. Exiting.")
    exit()
# --- END NEW Import ---

# --- REMOVED Birdeye specific import from helius_analyzer ---
# Keep helius_analyzer import *if* format_timestamp is still needed (or remove if not)
try:
    # from helius_analyzer import get_birdeye_token_overview # <-- REMOVED
    from helius_analyzer import format_timestamp # <-- Keep ONLY if used elsewhere, otherwise remove whole try/except
    print("[✓] performance_tracker: Imported format_timestamp from helius_analyzer.")
except ImportError:
    # Decide if format_timestamp is critical or can be replaced/removed
    print("WARNING: helius_analyzer.py not found. format_timestamp (if needed) will fail.")
    # Define a fallback format_timestamp if necessary
    def format_timestamp(ts: Optional[int]) -> Optional[str]:
        if ts is None: return None
        try: return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except (TypeError, ValueError): return None
# --- END REMOVED Import ---

try:
    import api_usage_limiter # Still needed indirectly by birdeye_api
except ImportError:
    print("CRITICAL: api_usage_limiter.py not found (required by birdeye_api). Exiting.")
    exit()

# --- NEW: Import the Centralized Telegram Router ---
try:
    import telegram_alert_router
    print("[✓] performance_tracker: Imported telegram_alert_router.")
except ImportError:
    print("CRITICAL: telegram_alert_router.py not found. Telegram alerts will fail. Exiting.")
    exit()
# --- END NEW Import ---


# --- REMOVED Telegram Function and Constants ---
# TELEGRAM_TOKEN_PERFORMANCE = os.getenv("TELEGRAM_BOT_TOKEN_PERFORMANCE", os.getenv("TELEGRAM_BOT_TOKEN")) # <-- REMOVED
# TELEGRAM_CHAT_ID_PERFORMANCE = os.getenv("TELEGRAM_CHAT_ID_PERFORMANCE", os.getenv("TELEGRAM_CHAT_ID")) # <-- REMOVED
# def send_telegram_performance(text: str, is_summary: bool = False) -> bool: # <-- REMOVED
    # ... (implementation removed) ...
# --- End REMOVED Telegram ---


# --- CONFIG (remains the same) ---
TRACKING_STATE_FILE = "performance_tracking_state.json"
ALERT_HISTORY_FILE = os.path.join("reports", "holdings", "alert_history.json") # Assuming this is correct path
CHECK_INTERVAL_HOURS = 1
SUMMARY_INTERVAL_HOURS = 8 # How often to send the summary report
PRICE_INCREASE_ALERT_THRESHOLD = 0.15 # 15% hourly increase
TOTAL_GAIN_MILESTONES_ALERT = [0.5, 1.0, 2.0, 5.0, 10.0]
SUMMARY_GAIN_THRESHOLDS = {
    5.0: "📈 > 500%", 2.0: "📈 > 200%", 1.0: "📈 > 100%",
    0.5: "📈 > 50%", 0.1: "📈 > 10%",
}
DECREASE_HOURS_TO_PAUSE = 2
PAUSE_DURATION_HOURS = 12
DISCARD_HOURS_WITHOUT_GAIN = 24
MIN_GAIN_TO_AVOID_DISCARD = 0.05

# --- Helper Functions (remain the same) ---
def load_state() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(TRACKING_STATE_FILE):
        try:
            with open(TRACKING_STATE_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except Exception as e: print(f"[!] Error loading tracking state: {e}")
    return {}

def save_state(state: Dict[str, Dict[str, Any]]):
    try:
        with open(TRACKING_STATE_FILE, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)
    except Exception as e: print(f"[✗] Error saving tracking state: {e}")

def load_alert_history() -> Dict[str, Any]:
     # Ensure the directory exists before trying to load
     alert_history_dir = os.path.dirname(ALERT_HISTORY_FILE)
     if alert_history_dir and not os.path.exists(alert_history_dir):
         try: os.makedirs(alert_history_dir, exist_ok=True)
         except OSError as e: print(f"[!] Error creating directory for alert history '{alert_history_dir}': {e}")

     if os.path.exists(ALERT_HISTORY_FILE):
        try:
            with open(ALERT_HISTORY_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except Exception as e: print(f"[!] Error loading alert history: {e}")
     return {}

def calculate_price_change(old: Optional[float], new: Optional[float]) -> Optional[float]:
    if old is None or new is None: return None
    if old < 1e-12: return float('inf') if (new is not None and new > 0) else 0.0
    try:
        return (new - old) / old
    except ZeroDivisionError:
        return float('inf') if (new is not None and new > 0) else 0.0
    except Exception:
        return None

# --- Main Tracking Logic (Modified to use telegram_alert_router) ---
def check_performance_and_alert():
    print(f"\n--- Running Performance Check @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")
    tracking_state = load_state()
    alert_history = load_alert_history() # Load from defined path
    current_ts = int(time.time())
    paused_count = 0; alerts_sent = 0; newly_added = 0

    # 1. Add newly alerted tokens (unchanged logic)
    print("[*] Checking alert history for new tokens to track...")
    for mint, history_data in alert_history.items():
        if mint not in tracking_state or tracking_state.get(mint, {}).get("status") == "discarded":
            initial_price = history_data.get("price")
            initial_mc = history_data.get("market_cap")
            initial_alert_ts = history_data.get("last_alert_timestamp")
            if initial_alert_ts is None or initial_price is None or initial_price <= 0:
                 print(f"    Skipping adding {mint[:6]}... from alert history: Missing/invalid timestamp or initial price ({initial_price}).")
                 continue
            print(f"    Adding/Re-tracking token: {mint[:6]}...")
            tracking_state[mint] = {
                "symbol": history_data.get("symbol", "N/A"),
                "name": history_data.get("name", "N/A"),
                "status": "tracking",
                "initial_alert_ts": initial_alert_ts,
                "initial_price_usd": initial_price,
                "initial_mc_usd": initial_mc,
                "last_check_ts": current_ts,
                "last_price_usd": initial_price,
                "consecutive_decreases": 0,
                "paused_until_ts": None,
                "total_gain_milestones_hit": [],
                "peak_price_usd": initial_price,
                "peak_gain_percentage": 0.0,
                "initial_metrics": history_data.copy()
            }
            newly_added += 1
    if newly_added > 0: print(f"[+] Added {newly_added} new tokens to tracking state.")

    # 2. Iterate through tracked tokens (Modified API call & Alerting)
    print("[*] Checking performance of tracked tokens...")
    mints_to_discard = []
    active_tracking_count = 0

    for mint, state_data in tracking_state.items():
        status = state_data.get("status")
        if status == "discarded": continue

        initial_price = state_data.get("initial_price_usd")
        if initial_price is None or initial_price <= 0:
            print(f"    [!] Skipping {mint[:6]}: Invalid initial price in state ({initial_price}). Marking discarded.")
            state_data["status"] = "discarded"; mints_to_discard.append(mint); continue

        paused_until = state_data.get("paused_until_ts")
        if status == "paused":
            if paused_until is not None and current_ts < paused_until:
                 paused_count += 1; continue
            else: # Pause ended
                 print(f"    Token {state_data.get('symbol', mint[:6])} pause ended. Resuming.")
                 state_data["status"] = "tracking"; state_data["paused_until_ts"] = None
                 state_data["consecutive_decreases"] = 0; state_data["last_check_ts"] = current_ts
                 # --- MODIFIED: Use birdeye_api.token_overview on resume ---
                 overview_resume = birdeye_api.token_overview(mint)
                 if overview_resume and overview_resume.get("price") is not None:
                     state_data["last_price_usd"] = overview_resume.get("price")
                     print(f"        Resetting last price for {state_data.get('symbol', mint[:6])} to current: ${state_data['last_price_usd']:.6f}")
                 else:
                      print(f"        Could not fetch current price for {state_data.get('symbol', mint[:6])} on resume via gateway.")
                 time.sleep(0.1)

        # --- If tracking, fetch overview using birdeye_api ---
        active_tracking_count += 1
        # --- MODIFIED: Use birdeye_api.token_overview ---
        current_overview = birdeye_api.token_overview(mint)
        time.sleep(0.1)

        if current_overview is None:
            print(f"    [!] Failed to get overview for {state_data.get('symbol', mint[:6])} via gateway. Skipping update.")
            state_data["last_check_ts"] = current_ts
            continue

        # --- Rest of the logic is mostly unchanged, using data from current_overview ---
        current_price = current_overview.get("price")
        current_mc = current_overview.get('marketCap', current_overview.get('mc'))
        last_price = state_data.get("last_price_usd")

        state_data["symbol"] = current_overview.get("symbol", state_data.get("symbol", "N/A"))
        state_data["name"] = current_overview.get("name", state_data.get("name", "N/A"))

        # Update Peak Performance (unchanged logic)
        if current_price is not None:
            if state_data.get("peak_price_usd") is None: state_data["peak_price_usd"] = initial_price
            if current_price > state_data["peak_price_usd"]:
                print(f"    Updating peak price for {state_data.get('symbol', mint[:6])} to ${current_price:.6f}")
                state_data["peak_price_usd"] = current_price
                peak_gain = calculate_price_change(initial_price, state_data["peak_price_usd"])
                if peak_gain is not None: state_data["peak_gain_percentage"] = peak_gain

        # Check for Stagnation/Discard (unchanged logic)
        time_since_alert = current_ts - state_data.get("initial_alert_ts", current_ts)
        if time_since_alert > DISCARD_HOURS_WITHOUT_GAIN * 3600:
             current_peak_gain = state_data.get("peak_gain_percentage", 0.0)
             if current_peak_gain < MIN_GAIN_TO_AVOID_DISCARD:
                 print(f"    Discarding {state_data.get('symbol','N/A')} ({mint[:6]}): Peak gain ({current_peak_gain*100:.1f}%) < threshold ({MIN_GAIN_TO_AVOID_DISCARD*100:.1f}%) after {DISCARD_HOURS_WITHOUT_GAIN}h.")
                 state_data["status"] = "discarded"; mints_to_discard.append(mint); continue

        # Check Price Changes (unchanged logic, modified alert sending)
        price_change_hourly = calculate_price_change(last_price, current_price)
        alert_sent_this_cycle_for_token = False

        if price_change_hourly is not None and not math.isinf(price_change_hourly):
            if price_change_hourly >= PRICE_INCREASE_ALERT_THRESHOLD:
                print(f"    [+] Hourly Alert Triggered for {state_data.get('symbol','N/A')} ({mint[:6]}): {price_change_hourly*100:+.1f}%")
                price_change_total = calculate_price_change(initial_price, current_price)
                total_change_str = f"{price_change_total*100:+.1f}%" if price_change_total is not None else "N/A"
                mc_str = f"${current_mc:,.0f}" if isinstance(current_mc, (int, float)) else "N/A"
                current_price_str = f"${current_price:.6f}" if current_price is not None else "N/A"
                # --- FORMATTING REMAINS for Legacy Markdown (uses *) ---
                alert_text = ( f"*{state_data.get('symbol','N/A')}* ({mint[:4]}...{mint[-4:]})\n"
                               f"📈 Price Up: {price_change_hourly*100:+.1f}% (last hour)\n"
                               f"🚀 Since Alert: {total_change_str}\n"
                               f"💲 Current Price: {current_price_str}\n"
                               f"💰 Current MC: {mc_str}" )
                # --- MODIFIED: Use telegram_alert_router ---
                alert_prefix = "🚀 Token Performance Update 🚀"
                log_text_snippet = alert_text.splitlines()[0] # For console log
                if telegram_alert_router.send_alert(
                    message=alert_text,
                    category="performance", # Corresponds to _PERFORMANCE env vars
                    prefix=alert_prefix,
                    parse_mode="Markdown", # Keep legacy mode as text uses *...*
                    escape_markdown=False  # Text already contains markdown
                ):
                    alerts_sent += 1
                    alert_sent_this_cycle_for_token = True
                    print(f"    [✓] Performance Alert Sent via Router: {log_text_snippet}...")
                else:
                    print(f"    [✗] Failed to send Performance Alert via Router: {log_text_snippet}...")
                # --- END MODIFIED ---
                state_data["consecutive_decreases"] = 0
            elif price_change_hourly < -0.01: # Decrease check
                state_data["consecutive_decreases"] = state_data.get("consecutive_decreases", 0) + 1
                print(f"    [-] Price decreased for {state_data.get('symbol','N/A')} ({mint[:6]}): {price_change_hourly*100:.1f}%. Consecutive: {state_data['consecutive_decreases']}")
                if state_data["consecutive_decreases"] >= DECREASE_HOURS_TO_PAUSE:
                    print(f"    ⏸️ Pausing tracking for {state_data.get('symbol','N/A')} ({mint[:6]}): Decreased {DECREASE_HOURS_TO_PAUSE} consecutive hours.")
                    state_data["status"] = "paused"
                    state_data["paused_until_ts"] = current_ts + PAUSE_DURATION_HOURS * 3600
            else: state_data["consecutive_decreases"] = 0 # Stable/minor gain
        elif price_change_hourly is not None and math.isinf(price_change_hourly): # Infinite change
            print(f"    [?] Hourly price change inf for {state_data.get('symbol','N/A')} ({mint[:6]}) (last: {last_price}, current: {current_price}). Resetting decrease count.")
            state_data["consecutive_decreases"] = 0
        else: # Calculation failed
             print(f"    [?] Hourly price change calc failed for {state_data.get('symbol','N/A')} ({mint[:6]}) (last: {last_price}, current: {current_price}). Resetting decrease count.")
             state_data["consecutive_decreases"] = 0

        # Check Total Gain Milestones (unchanged logic, modified alert sending)
        if not alert_sent_this_cycle_for_token:
            price_change_total = calculate_price_change(initial_price, current_price)
            if price_change_total is not None:
                milestones_hit_list = state_data.setdefault("total_gain_milestones_hit", [])
                for milestone_alert_threshold in TOTAL_GAIN_MILESTONES_ALERT:
                    if price_change_total >= milestone_alert_threshold and milestone_alert_threshold not in milestones_hit_list:
                        print(f"    [++] TOTAL GAIN Milestone Alert for {state_data.get('symbol','N/A')} ({mint[:6]}): Reached {milestone_alert_threshold*100:.0f}% ({price_change_total*100:+.1f}%)")
                        mc_str = f"${current_mc:,.0f}" if isinstance(current_mc, (int, float)) else "N/A"
                        current_price_str = f"${current_price:.6f}" if current_price is not None else "N/A"
                         # --- FORMATTING REMAINS for Legacy Markdown (uses *) ---
                        alert_text = ( f"*{state_data.get('symbol','N/A')}* ({mint[:4]}...{mint[-4:]})\n"
                                       f"🎯 TOTAL Gain Milestone: {price_change_total*100:+.1f}% since alert! ({milestone_alert_threshold*100:.0f}% Target)\n"
                                       f"💲 Current Price: {current_price_str}\n"
                                       f"💰 Current MC: {mc_str}" )
                        # --- MODIFIED: Use telegram_alert_router ---
                        alert_prefix = "🚀 Token Performance Update 🚀" # Same prefix for milestones
                        log_text_snippet = alert_text.splitlines()[0] # For console log
                        if telegram_alert_router.send_alert(
                            message=alert_text,
                            category="performance",
                            prefix=alert_prefix,
                            parse_mode="Markdown",
                            escape_markdown=False
                        ):
                            alerts_sent += 1
                            milestones_hit_list.append(milestone_alert_threshold)
                            print(f"    [✓] Milestone Alert Sent via Router: {log_text_snippet}...")
                        else:
                             print(f"    [✗] Failed to send Milestone Alert via Router: {log_text_snippet}...")
                        # --- END MODIFIED ---


        # Update last known stats (unchanged)
        state_data["last_check_ts"] = current_ts
        if current_price is not None: state_data["last_price_usd"] = current_price
        # --- End Loop for Token ---

    # --- Final State Update & Cleanup (unchanged) ---
    if mints_to_discard:
        print(f"[*] Cleaning up {len(mints_to_discard)} discarded tokens from state.")
        for mint in mints_to_discard:
            if mint in tracking_state: tracking_state[mint]["status"] = "discarded"
    save_state(tracking_state)
    print(f"[*] Performance check complete. Actively Tracking: {active_tracking_count}, Paused: {paused_count}, Discarded This Cycle: {len(mints_to_discard)}, Alerts Sent: {alerts_sent}")

# --- SUMMARY FUNCTION (Modified alert sending) ---
def generate_and_send_performance_summary(tracking_state: Dict[str, Dict[str, Any]]):
    print("\n--- Generating Performance Summary ---")
    summary_counts = {label: 0 for label in SUMMARY_GAIN_THRESHOLDS.values()}
    summary_counts["📉 < 10% (Or Neg)"] = 0
    total_analyzed = 0; discarded_count = 0
    sorted_thresholds = sorted(SUMMARY_GAIN_THRESHOLDS.items(), key=lambda item: item[0], reverse=True)

    for mint, state_data in tracking_state.items():
        status = state_data.get("status")
        if status == "discarded": discarded_count += 1; continue

        total_analyzed += 1
        peak_gain = state_data.get("peak_gain_percentage", 0.0)
        initial_price = state_data.get("initial_price_usd")
        if peak_gain is None or initial_price is None or initial_price <= 0:
            print(f"    [!] Skipping {mint[:6]} from summary: Invalid peak gain or initial price.")
            continue

        categorized = False
        for threshold_value, label in sorted_thresholds:
            if peak_gain >= threshold_value:
                summary_counts[label] += 1; categorized = True; break
        if not categorized: summary_counts["📉 < 10% (Or Neg)"] += 1

    summary_lines = [f"Summary of Peak Performance Since Initial Alert ({total_analyzed} tokens analyzed):"]
    for label, count in summary_counts.items():
        if count > 0: summary_lines.append(f"  • {label}: {count}")
    summary_lines.append(f"\n(Discarded/Stagnant Tokens: {discarded_count})")
    summary_text = "\n".join(summary_lines)
    print(summary_text)

    # --- MODIFIED: Use telegram_alert_router for summary ---
    summary_prefix = "📊 Performance Summary 📊"
    if telegram_alert_router.send_alert(
        message=summary_text,
        category="performance", # Use performance category
        prefix=summary_prefix,
        parse_mode="Markdown", # Keep legacy mode as text doesn't use complex formatting
        escape_markdown=False, # Text doesn't contain markdown
        is_status_update=True # Indicate it's a summary/status
    ):
        print("    [✓] Performance Summary Sent via Router.")
    else:
        print("    [✗] Failed to send Performance Summary via Router.")
    # --- END MODIFIED ---


# --- Main Loop (unchanged) ---
if __name__ == "__main__":
    print("Starting Performance Tracker (v1.2.0 - Router Integration)...") # <-- Version Bump
    tracker_state_on_start = load_state()
    last_summary_ts = tracker_state_on_start.get("_system", {}).get("last_summary_sent_ts", 0)
    if last_summary_ts == 0: print("[i] No previous summary timestamp found.")
    else: print(f"[i] Last summary sent at: {datetime.fromtimestamp(last_summary_ts, timezone.utc):%Y-%m-%d %H:%M:%S UTC}")

    check_performance_and_alert() # Initial check
    current_ts_main = int(time.time())
    if current_ts_main - last_summary_ts >= SUMMARY_INTERVAL_HOURS * 3600:
        print("[*] Summary interval elapsed since last recorded summary. Generating now...")
        generate_and_send_performance_summary(load_state())
        last_summary_ts = current_ts_main
        tracker_state_updated = load_state()
        tracker_state_updated.setdefault("_system", {})["last_summary_sent_ts"] = last_summary_ts
        save_state(tracker_state_updated)

    while True:
        try:
            next_check_time = datetime.now(timezone.utc) + timedelta(hours=CHECK_INTERVAL_HOURS)
            sleep_duration = max((next_check_time - datetime.now(timezone.utc)).total_seconds(), 10)
            print(f"\n--- Sleeping for ~{sleep_duration / 3600:.1f} hours until next performance check ({next_check_time:%Y-%m-%d %H:%M:%S UTC}) ---")
            time.sleep(sleep_duration)

            check_performance_and_alert()
            current_ts_main = int(time.time())

            if current_ts_main - last_summary_ts >= SUMMARY_INTERVAL_HOURS * 3600:
                 print("[*] Summary interval elapsed. Generating summary...")
                 generate_and_send_performance_summary(load_state())
                 last_summary_ts = current_ts_main
                 tracker_state_updated = load_state()
                 tracker_state_updated.setdefault("_system", {})["last_summary_sent_ts"] = last_summary_ts
                 save_state(tracker_state_updated)

        except KeyboardInterrupt: print("\nPerformance Tracker stopped by user."); break
        except Exception as e:
            print(f"\n[!!!] Unhandled error in Performance Tracker main loop: {e}"); traceback.print_exc()
            print("    Attempting to save state before retrying...");
            try: save_state(load_state())
            except Exception as save_err: print(f"    [!!!] CRITICAL: Failed to save state during error handling: {save_err}")
            print("    Retrying after 5 minutes..."); time.sleep(5 * 60)

# --- END OF FILE performance_tracker.py ---