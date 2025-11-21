# --- START OF FILE monitor_alerts.py ---

#!/usr/bin/env python3
"""
Smarter Telegram alerts for token-correlation reports.
Reads ENRICHED and VALIDATED reports from the 'processed' directory.
Uses centralized telegram_alert_router for sending.
Version: 1.6 - Integrated telegram_alert_router
"""

import os
import json
import glob
import time
import datetime
import traceback
from datetime import timezone # Explicit import
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# --- Centralized Alerting ---
try:
    from telegram_alert_router import send_alert as router_send_alert
    print("[✓] monitor_alerts: Imported send_alert from telegram_alert_router.")
    # Define a wrapper function to maintain similar call signature if needed,
    # or directly use router_send_alert where send_telegram was called.
    # We will call it directly for simplicity here.
    USE_ROUTER = True
except ImportError:
    print("[!] WARNING: telegram_alert_router not found. Falling back to console-only alerts.")
    USE_ROUTER = False
    # Define a dummy function for console fallback if router fails to import
    def router_send_alert(**kwargs) -> bool:
        message = kwargs.get("message", "No message provided.")
        category = kwargs.get("category", "default")
        print(f"\n--- CONSOLE ALERT ({category} - Router Fallback) ---")
        print(message)
        print("--- (telegram_alert_router not available) ---\n")
        return True # Simulate success for console print

# --- ENV & IMPORTS ---
load_dotenv() # Load .env variables for potential use elsewhere or by the router

# NOTE: TELEGRAM_TOKEN and TELEGRAM_CHAT_ID are no longer directly used here.
# The router handles credential loading based on its own logic (.env variables).

# --- Timestamp Formatting ---
# Fallback format_timestamp (Seems robust enough)
def default_format_timestamp(ts: Optional[Any]) -> Optional[str]:
    if ts is None: return "N/A"
    try:
        if isinstance(ts, str):
            try: # ISO format check
                dt_obj = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
                return dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
            except ValueError: # Try float conversion
                try:
                    ts_float = float(ts)
                    # Handle potential milliseconds
                    if ts_float > 1e12 and ts_float < 1e13: ts_float /= 1000
                    return datetime.datetime.fromtimestamp(ts_float, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                except ValueError: return str(ts) # Give up, return original string
        elif isinstance(ts, (int, float)):
            # Handle potential milliseconds
            if ts > 1e12 and ts < 1e13: ts /= 1000
            return datetime.datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        return str(ts) # Return string representation if not handled
    except Exception: return str(ts) # Catch-all for safety

# Attempt to import specialized formatter
try:
    from helius_analyzer import format_timestamp as ha_format_timestamp
    print("[✓] monitor_alerts: Imported format_timestamp from helius_analyzer.")
    def format_timestamp_for_alert(ts: Optional[Any]) -> Optional[str]:
        if ts is None: return "N/A"
        try:
            # Handle potential milliseconds before passing to helius formatter
            if isinstance(ts, (int, float)):
                 if ts > 1e12 and ts < 1e13: ts /= 1000
                 # Ensure it's an int for helius formatter if it expects one
                 formatted_ts = ha_format_timestamp(int(ts))
                 # Fallback if helius formatter returns None or empty
                 return formatted_ts if formatted_ts else default_format_timestamp(ts)
            elif isinstance(ts, str):
                # Let default handle string parsing first
                return default_format_timestamp(ts)
            # Fallback for other types
            return default_format_timestamp(ts)
        except Exception:
            # Fallback on any error during specialized formatting
            return default_format_timestamp(ts)
except ImportError:
    print("[!] monitor_alerts: Could not import format_timestamp from helius_analyzer. Using basic formatter.")
    format_timestamp_for_alert = default_format_timestamp

# --- CONFIG ---
REPORTS_ROOT   = os.path.join("reports", "holdings")
PROCESSED_REPORTS_DIR = os.path.join(REPORTS_ROOT, "processed")
HISTORY_FILE   = os.path.join(REPORTS_ROOT, "alert_history.json")

MC_THRESHOLD_PERCENT_CHANGE   = 0.20
VOL_THRESHOLD_PERCENT_CHANGE  = 0.30
MIN_HOLDERS_FOR_ALERT = 1
SLEEP_BETWEEN_ALERTS_SECONDS = 2 # Router might have internal delays, but this adds explicit spacing
CHECK_INTERVAL_SECONDS = 45
MIN_ALERT_INTERVAL_SECONDS = 30 * 60

os.makedirs(REPORTS_ROOT, exist_ok=True)
os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)

# --- UTILS ---

# REMOVED: send_telegram function - Replaced by router_send_alert call

def load_json_data(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if path.endswith("alert_history.json"): print(f"    [i] Alert history file not found: {path}. Will be created.")
        else: print(f"    [!] Report file not found: {path}")
    except json.JSONDecodeError as e: print(f"    [!] Failed to decode JSON from {path}: {e}")
    except Exception as e: print(f"    [!] Failed to load {path}: {e}")
    return {}

def save_json_data(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: print(f"    [✗] Failed to save data to {path}: {e}")

def calculate_percent_change(old: Optional[Any], new: Optional[Any]) -> Optional[float]:
    if old is None or new is None: return None
    try: old_f = float(old); new_f = float(new)
    except (ValueError, TypeError): return None
    if old_f == 0: return float('inf') if new_f > 0 else (float('-inf') if new_f < 0 else 0.0)
    try: return (new_f - old_f) / abs(old_f)
    except Exception: return None

def latest_report_path() -> Optional[str]:
    processed_full_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_report_*.json")
    processed_delta_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_delta_*.json")
    all_files = glob.glob(processed_full_pattern) + glob.glob(processed_delta_pattern)
    if not all_files: return None
    try: return max(all_files, key=os.path.getmtime)
    except Exception as e: print(f"    [!] Error finding latest report path: {e}"); return None

def _format_compact(value: Optional[Any], prefix: str = "$", precision: int = 1) -> str:
    if value is None or value == "N/A": return "N/A"
    try:
        val = float(value)
        if abs(val) >= 1_000_000_000: return f"{prefix}{val/1_000_000_000:.{precision}f}B"
        if abs(val) >= 1_000_000: return f"{prefix}{val/1_000_000:.{precision}f}M"
        if abs(val) >= 1_000: return f"{prefix}{val/1_000:.{precision}f}K"
        # Ensure commas for values below 1000 if prefix is $
        if prefix == "$" and abs(val) < 1000: return f"{prefix}{val:,.2f}" # Show cents for prices < 1k
        return f"{prefix}{val:,.0f}" # Default formatting for others
    except (ValueError, TypeError): return str(value)

def _format_pct(value: Optional[Any]) -> str:
    if value is None or value == "N/A": return "N/A"
    try: return f"{float(value):+.1f}%"
    except (ValueError, TypeError): return str(value)

# --- Updated Message Formatting (Remains Largely the Same) ---
def format_alert_message(processed_item: Dict[str, Any], an_history: Optional[Dict[str, Any]] = None) -> str:
    # ... (Keep the entire existing format_alert_message function content) ...
    # ... (It correctly generates the Markdown string needed) ...
    # --- START OF COPIED format_alert_message ---
    mint = processed_item.get("token_mint", "N/A")
    symbol = processed_item.get("symbol", "N/A")
    name = processed_item.get("name", "N/A")

    original_data = processed_item.get("original_correlation_data", {})
    holders_in_set = original_data.get("total_holders_in_set", 0)

    market_data = processed_item.get("bds_market_data") or {}
    trade_data = processed_item.get("bds_trade_data") or {}
    bds_security = processed_item.get("bds_security_data") or {}
    gmgn_security = processed_item.get("gmgn_security_info") or {}

    # Market Data
    price = market_data.get("price")
    liquidity = market_data.get("liquidity")
    mc = market_data.get("market_cap")
    fdv = market_data.get("fdv")
    top_holder_pct_val = market_data.get("top_holder_pct")

    # Trade Data
    volume_24h = trade_data.get("volume_24h") or (trade_data.get("volume", {}) or {}).get("h24")
    total_holders_bds = trade_data.get("holder")
    price_change_1h_pct = trade_data.get("price_change_1h_percent")
    price_change_6h_pct = trade_data.get("price_change_6h_percent")
    price_change_24h_pct = trade_data.get("price_change_24h_percent")

    # Security Data
    bds_mutable_meta = bds_security.get('mutableMetadata')
    bds_freezeable = bds_security.get('freezeable')
    bds_freeze_auth = bds_security.get('freezeAuthority')
    bds_creator_addr = bds_security.get('creatorAddress')
    bds_update_auth = bds_security.get('metaplexUpdateAuthority')
    bds_top10_pct = bds_security.get('top10HolderPercent')

    gmgn_honeypot = gmgn_security.get('is_honeypot')
    gmgn_mintable = gmgn_security.get('is_mintable')
    gmgn_rug_pull = gmgn_security.get('is_rug_pull')

    # --- Age Calculation ---
    raw_creation_ts_sources = [
        original_data.get("first_event_timestamp_utc"),
        (processed_item.get("bds_metadata", {}) or {}).get("created_timestamp"),
        (bds_security or {}).get("creationTime"),
        (bds_security or {}).get("creation_timestamp_ms")
    ]
    creation_dt: Optional[datetime.datetime] = None
    for ts_source in raw_creation_ts_sources:
        if ts_source is None: continue
        formatted_ts = default_format_timestamp(ts_source) # Use the basic one for reliable parsing here
        if formatted_ts != "N/A" and formatted_ts != str(ts_source):
            try:
                 # Ensure it has timezone info for correct comparison
                 creation_dt = datetime.datetime.strptime(formatted_ts, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                 break
            except ValueError: continue
    age_str_part = "N/A"
    if creation_dt:
        try:
            age_delta = datetime.datetime.now(timezone.utc) - creation_dt
            total_seconds = age_delta.total_seconds()
            if total_seconds < 0: age_str_part = "Future?"
            elif total_seconds < 60: age_str_part = f"{int(total_seconds)}s"
            elif total_seconds < 3600: age_str_part = f"{int(total_seconds // 60)}m {int(total_seconds % 60)}s"
            elif total_seconds < 86400: age_str_part = f"{int(total_seconds // 3600)}h {int((total_seconds % 3600) // 60)}m"
            else: age_str_part = f"{int(total_seconds // 86400)}d {int((total_seconds % 86400) // 3600)}h"
            age_str_part += " old"
        except Exception as e_age: age_str_part = f"Error ({e_age})" # Include error for debugging
    # --- End Age ---

    # Formatting values
    liquidity_str = _format_compact(liquidity, prefix="$", precision=1)
    mc_str = _format_compact(mc, prefix="$", precision=1)
    fdv_str = _format_compact(fdv, prefix="$", precision=1)
    volume_24h_str = _format_compact(volume_24h, prefix="$", precision=1)
    price_str = f"${float(price):.5g}" if isinstance(price, (int, float)) else ("N/A" if price is None else str(price))
    holders_bds_str = f"{total_holders_bds:,}" if isinstance(total_holders_bds, int) else ("N/A" if total_holders_bds is None else str(total_holders_bds))

    mc_display_str = mc_str if mc_str != "N/A" else (f"N/A (FDV: {fdv_str})" if fdv_str != "N/A" else "N/A")

    dex_link = f"https://dexscreener.com/solana/{mint}" if mint != "N/A" else "N/A"
    gmgn_link = f"https://gmgn.ai/sol/token/{mint}" if mint != "N/A" else "N/A"

    alert_reason = processed_item.get("_alert_reason", "Update")

    # --- Build Message Lines ---
    title_name_part = f" ({name})" if name and name != "N/A" and name != symbol and symbol != "N/A" else ""
    mint_snippet = f"({mint[:4]}...{mint[-4:]})" if mint != "N/A" else ""
    message_lines = [f"*{symbol}{title_name_part}* {mint_snippet} - *{alert_reason}*"]

    message_lines.append("\n*Market & Price*") # Section Header
    message_lines.append(f"💰 MC: {mc_display_str} | 💧 Liq: {liquidity_str}")
    message_lines.append(f"💲 Price: {price_str} | 📈 Vol 24h: {volume_24h_str}")

    price_change_parts = []
    if price_change_1h_pct is not None: price_change_parts.append(f"1h: {_format_pct(price_change_1h_pct)}")
    if price_change_6h_pct is not None: price_change_parts.append(f"6h: {_format_pct(price_change_6h_pct)}")
    if price_change_24h_pct is not None: price_change_parts.append(f"24h: {_format_pct(price_change_24h_pct)}")
    if price_change_parts: message_lines.append(f"Δ Price: {' | '.join(price_change_parts)}")

    message_lines.append("\n*Holders & Age*") # Section Header
    message_lines.append(f"👥 Monitored: {holders_in_set} | Total Holders: {holders_bds_str}")
    message_lines.append(f"⏱️ Age: {age_str_part}")

    # Changes since last alert
    if an_history:
        mc_change_val = calculate_percent_change(an_history.get("market_cap"), mc)
        vol_change_val = calculate_percent_change(an_history.get("volume24h"), volume_24h)
        holder_change_val = holders_in_set - an_history.get("total_holders_in_set", holders_in_set)
        change_parts = []
        if mc_change_val is not None and abs(mc_change_val) >= 0.001: change_parts.append(f"MC {(mc_change_val * 100):+.1f}%")
        if vol_change_val is not None and abs(vol_change_val) >= 0.001: change_parts.append(f"Vol {(vol_change_val * 100):+.1f}%")
        if holder_change_val != 0: change_parts.append(f"Mon. {holder_change_val:+.0f}")
        if change_parts: message_lines.append(f"📊 Changes (Since Alert): {' | '.join(change_parts)}")

    # Security Flags
    sec_flags_list = []
    if bds_mutable_meta == True: sec_flags_list.append("Mutable")
    if bds_freezeable == True and bds_freeze_auth and \
       bds_freeze_auth not in ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]:
        sec_flags_list.append("FreezeAuth")
    if gmgn_honeypot == True: sec_flags_list.append("GMGN Pot")
    if gmgn_mintable == True: sec_flags_list.append("GMGN Mintable")
    if gmgn_rug_pull == True: sec_flags_list.append("GMGN Rug")
    if isinstance(top_holder_pct_val, float): sec_flags_list.append(f"Top1 {top_holder_pct_val*100:.0f}%")
    if isinstance(bds_top10_pct, float): sec_flags_list.append(f"Top10(BDS) {bds_top10_pct*100:.0f}%")
    # Simplified auth checks slightly
    generic_auths = ["11111111111111111111111111111111", "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM", None, ""]
    creator_disp = None
    if bds_creator_addr and bds_creator_addr not in generic_auths:
        creator_disp = f"Creator: {bds_creator_addr[:4]}.."
        sec_flags_list.append(creator_disp)
    if bds_update_auth and bds_update_auth not in generic_auths and bds_update_auth != bds_creator_addr:
         sec_flags_list.append(f"UpdAuth: {bds_update_auth[:4]}..")
    elif bds_update_auth and bds_update_auth != bds_creator_addr and creator_disp:
        # If creator was displayed and update auth exists but is generic/same, don't add another line
        pass
    elif bds_update_auth and bds_update_auth not in generic_auths and not creator_disp:
        # If creator wasn't displayed but update auth is non-generic
        sec_flags_list.append(f"UpdAuth: {bds_update_auth[:4]}..")


    if sec_flags_list:
        message_lines.append("\n🔒 *Security Flags*") # Section Header
        flags_line = ", ".join(sec_flags_list)
        if len(flags_line) > 60: # Heuristic for wrapping
             flags_line = ",\n   ".join(sec_flags_list) # Indent wrapped lines
        message_lines.append(f"   {flags_line}")

    # Ensure links are properly formatted for Markdown (already done in v1.5)
    message_lines.append(f"\n[DexScreener]({dex_link}) | [GMGN]({gmgn_link})")
    return "\n".join(message_lines)
    # --- END OF COPIED format_alert_message ---


def process_report_for_alerts(report_path: str, alert_history: Dict[str, Any]) -> int:
    print(f"[*] Processing report for alerts: {os.path.basename(report_path)}")
    report_content = load_json_data(report_path)
    if not report_content: return 0

    processed_items_list: List[Dict[str, Any]] = []
    items_source_key = None
    # Check for primary key first
    if "correlated_holdings" in report_content and isinstance(report_content.get("correlated_holdings"), list) and report_content["correlated_holdings"]:
        processed_items_list = report_content["correlated_holdings"]
        items_source_key = "correlated_holdings"
    # Fallback to snapshot key
    elif "correlated_holdings_snapshot" in report_content and isinstance(report_content.get("correlated_holdings_snapshot"), list) and report_content["correlated_holdings_snapshot"]:
        processed_items_list = report_content["correlated_holdings_snapshot"]
        items_source_key = "correlated_holdings_snapshot"

    if not processed_items_list:
        print(f"    [i] No items found using keys 'correlated_holdings' or 'correlated_holdings_snapshot' for report: {os.path.basename(report_path)}"); return 0

    print(f"    [i] Found {len(processed_items_list)} items using key '{items_source_key}' in report: {os.path.basename(report_path)}")
    alerts_sent_this_run = 0

    for processed_item in processed_items_list:
        mint_address = processed_item.get("token_mint")
        validation_flags = processed_item.get("validation_flags", {})
        original_data = processed_item.get("original_correlation_data", {})
        market_data = processed_item.get("bds_market_data") or {}
        trade_data = processed_item.get("bds_trade_data") or {} # Corrected key usage here

        if not mint_address: continue
        # Ensure only validated tokens proceed
        if not validation_flags.get("passes_validation", False): continue

        current_monitored_holders = original_data.get("total_holders_in_set", 0)
        if current_monitored_holders < MIN_HOLDERS_FOR_ALERT: continue

        token_alert_history = alert_history.get(mint_address, {})
        should_send_alert = False
        alert_reason = "Update"

        # Determine if alert is needed (logic remains same as v1.5)
        if not token_alert_history:
            should_send_alert = True
            alert_reason = "✨ New Validated Listing"
            print(f"    Alert reason for {mint_address[:6]}...: {alert_reason}")
        else:
            holders_now = current_monitored_holders
            holders_prev = token_alert_history.get("total_holders_in_set", 0)
            mc_now = market_data.get("market_cap")
            mc_prev = token_alert_history.get("market_cap")
            # Use correct key 'volume_24h' from trade_data
            vol_now = trade_data.get("volume_24h") or (trade_data.get("volume", {}) or {}).get("h24")
            # Use correct history key 'volume24h'
            vol_prev = token_alert_history.get("volume24h")

            mc_change_pct = calculate_percent_change(mc_prev, mc_now)
            vol_change_pct = calculate_percent_change(vol_prev, vol_now)

            # Threshold checks
            if mc_change_pct is not None and mc_change_pct >= MC_THRESHOLD_PERCENT_CHANGE:
                should_send_alert = True; alert_reason = f"MC ↑{mc_change_pct*100:.0f}%"
            elif vol_change_pct is not None and vol_change_pct >= VOL_THRESHOLD_PERCENT_CHANGE:
                should_send_alert = True; alert_reason = f"Vol ↑{vol_change_pct*100:.0f}%"
            elif holders_now > holders_prev:
                should_send_alert = True; alert_reason = f"Mon.Holders +{holders_now - holders_prev}"

            if should_send_alert:
                 mc_prev_fmt = _format_compact(mc_prev); mc_now_fmt = _format_compact(mc_now)
                 vol_prev_fmt = _format_compact(vol_prev); vol_now_fmt = _format_compact(vol_now)
                 print(f"    Alert reason for {mint_address[:6]}...: {alert_reason} (MC: {mc_prev_fmt} -> {mc_now_fmt} | Vol: {vol_prev_fmt} -> {vol_now_fmt} | Holders: {holders_prev} -> {holders_now})")

        if should_send_alert:
            last_alert_ts = token_alert_history.get("last_alert_timestamp", 0)
            time_since_last_alert = int(time.time()) - last_alert_ts
            is_truly_first_alert = not token_alert_history

            if not is_truly_first_alert and time_since_last_alert < MIN_ALERT_INTERVAL_SECONDS:
                print(f"    Cooldown active for {mint_address[:6]}... ({alert_reason}). Time since last: {time_since_last_alert}s. Skipping."); continue

            # Prepare and send alert using the router
            processed_item["_alert_reason"] = alert_reason
            message_text = format_alert_message(processed_item, token_alert_history if not is_truly_first_alert else None)

            print(f"    Attempting to send alert for {mint_address[:6]}... via router ({alert_reason})")
            # --- INTEGRATION POINT ---
            # Call the router's send_alert function
            # We want to use the original Markdown formatting, so parse_mode="Markdown"
            # And the message is already formatted, so escape_markdown=False
            success = router_send_alert(
                message=message_text,
                parse_mode="Markdown",      # Use legacy Markdown as message formatter creates this
                escape_markdown=False,     # Message already contains Markdown characters (*, _, etc.)
                disable_web_page_preview=True, # Maintain previous behavior
                # category=None            # Use default category resolution in router
                                             # (Looks for _DEFAULT or _<CATEGORY> env vars)
                # prefix=None              # Use default alert prefix from router
                is_status_update=False   # This is an alert, not a status update
            )
            # --- END INTEGRATION POINT ---

            if success:
                # Router logs success/failure internally, but we can add confirmation here
                print(f"    [✓] Router reported success sending alert for {mint_address[:6]}...")
                alerts_sent_this_run += 1
                current_time_int = int(time.time())
                # Update history (logic remains same as v1.5)
                history_update_data = {
                    "last_alert_timestamp": current_time_int,
                    "first_alert_timestamp": token_alert_history.get("first_alert_timestamp", current_time_int),
                    "last_alert_report_path": os.path.basename(report_path),
                    "total_holders_in_set": current_monitored_holders,
                    "market_cap": market_data.get("market_cap"),
                    "fdv": market_data.get("fdv"),
                    "volume24h": trade_data.get("volume_24h") or (trade_data.get("volume", {}) or {}).get("h24"),
                    "price": market_data.get("price"),
                    "symbol": processed_item.get("symbol", "N/A"),
                    "name": processed_item.get("name", "N/A"),
                    "last_alert_reason": alert_reason
                }
                # Create a new dict for the history entry to avoid modifying the loop variable inplace if it's complex
                final_history_entry = token_alert_history.copy()
                # Update with new data, ensuring None values don't overwrite existing keys unless intended
                final_history_entry.update({k: v for k, v in history_update_data.items() if v is not None})
                # Update if value changed or key is new
                final_history_entry = {**token_alert_history, **history_update_data}


                alert_history[mint_address] = final_history_entry

                # Rate limiting / flood control
                if alerts_sent_this_run >= 15: # Increased limit slightly
                    print(f"    [!] Reached max alerts for this run ({alerts_sent_this_run}). Stopping for this report."); break
                time.sleep(SLEEP_BETWEEN_ALERTS_SECONDS) # Keep explicit sleep between alerts
            else:
                # Router handles logging the failure reason
                print(f"    [✗] Router reported failure sending alert for {mint_address[:6]}... History not updated.")
                # Consider if you need different behavior on send failure

    return alerts_sent_this_run


def main_alert_loop():
    start_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"--- Starting Monitor Alerts (v1.6 - Router Integration) ({start_time_str}) ---")
    print(f"Watching for processed reports in: {PROCESSED_REPORTS_DIR}")
    print(f"Alert History File: {HISTORY_FILE}")
    if USE_ROUTER:
        print(f"Alerting via: telegram_alert_router")
    else:
        print(f"Alerting via: Console Only (telegram_alert_router not found)")
    print(f"Config: MC Change >= {MC_THRESHOLD_PERCENT_CHANGE*100:.0f}%, Vol Change >= {VOL_THRESHOLD_PERCENT_CHANGE*100:.0f}%, Min Mon. Holders = {MIN_HOLDERS_FOR_ALERT}, Cooldown = {MIN_ALERT_INTERVAL_SECONDS // 60} mins")

    latest_processed_report_path_handled: Optional[str] = None
    alert_history_data = load_json_data(HISTORY_FILE)

    while True:
        try:
            current_latest_report_in_dir = latest_report_path()

            if not current_latest_report_in_dir:
                # print(".", end="", flush=True) # Optional: indicate checking activity
                pass # No reports yet
            elif current_latest_report_in_dir != latest_processed_report_path_handled:
                print(f"\n[*] New latest processed report detected: {os.path.basename(current_latest_report_in_dir)}")
                alerts_triggered = process_report_for_alerts(current_latest_report_in_dir, alert_history_data)

                # Save history if alerts were sent OR if the file didn't exist and we loaded/created data
                if alerts_triggered > 0 or (not os.path.exists(HISTORY_FILE) and alert_history_data):
                    save_json_data(HISTORY_FILE, alert_history_data)
                    if alerts_triggered > 0: print(f"    [i] Saved alert history updates.")

                current_time_str_log = datetime.datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                if alerts_triggered > 0:
                    print(f"    [✓] {alerts_triggered} alerts sent based on '{os.path.basename(current_latest_report_in_dir)}' @ {current_time_str_log}")
                else:
                    print(f"    [•] No new alert-worthy changes in '{os.path.basename(current_latest_report_in_dir)}' @ {current_time_str_log}")

                latest_processed_report_path_handled = current_latest_report_in_dir
            # else: # Optional: indicate checking when no new report is found
                # print(".", end="", flush=True)

            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n--- Monitor Alerts stopping due to user interrupt ---")
            save_json_data(HISTORY_FILE, alert_history_data)
            print("    Alert history saved.")
            break
        except Exception as e:
            print(f"\n[!!!] Unhandled error in Monitor Alerts main loop: {e}")
            traceback.print_exc()
            print("    Attempting to save history before retrying...")
            save_json_data(HISTORY_FILE, alert_history_data)
            # Send error alert via router if possible
            error_message_tg = f"🚨 Monitor Alerts Main Loop Error 🚨\n\n`{type(e).__name__}: {str(e)}`\n\nSee console/logs. Will attempt to save history and retry."
            # Use router_send_alert directly here as well
            router_send_alert(
                message=error_message_tg,
                category="ERROR", # Use a specific category for errors if configured in router
                parse_mode="Markdown", # Simple markdown for the error message
                escape_markdown=False # Message contains backticks
            )
            print(f"    Retrying in {CHECK_INTERVAL_SECONDS*2} seconds...")
            time.sleep(CHECK_INTERVAL_SECONDS*2)

if __name__ == "__main__":
    main_alert_loop()

# --- END OF FILE monitor_alerts.py ---