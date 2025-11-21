# --- START OF FILE api_usage_limiter.py ---

import os
import json
import time
import requests
import traceback
from datetime import datetime, date
from typing import Optional, Dict, Any, Callable, List
from dotenv import load_dotenv

# --- User-defined Modules ---
try:
    import cache_manager
    CACHE_ENABLED = True
    print("[✓] api_usage_limiter: cache_manager imported.")
    cache_manager.init_db()
except ImportError:
    print("[!] api_usage_limiter: cache_manager.py not found. Caching will be disabled.")
    CACHE_ENABLED = False
    cache_manager = None

# --- Configuration ---
load_dotenv()

# --- API Caps ---
# Assuming 10M/month each (~322k/day)
HELIUS_DAILY_CAP = int(os.getenv('HELIUS_DAILY_CAP', 322580)) # This is now a credit cap
BIRDEYE_DAILY_CAP = int(os.getenv('BIRDEYE_DAILY_CAP', 322580)) # This is a credit cap

# --- Helius Credit Costs (per endpoint_name) ---
HELIUS_COST_DEFAULT = int(os.getenv('HELIUS_COST_DEFAULT', 1)) # Fallback cost for unlisted Helius endpoints

HELIUS_CREDIT_COST_MAP = {
    "getBalance": 1,
    "parseTransaction": 100,
    "getAsset": 10, # Note: was previously HELIUS_COST_GET_ASSET (default 1 via env)
    "getAssetBatch": 10, # Consistent with existing HELIUS_COST_GET_ASSET_BATCH
    "getAssetProof": 10,
    "getAssetsByOwner": 10,
    "getAssetsByAuthority": 10,
    "getAssetsByCreator": 10,
    "getAssetsByGroup": 10,
    "searchAssets": 10,
    "getSignaturesForAsset": 10,
    "getTokenAccounts": 10,
    "getNFTEditions": 10,
    "getValidityProofs": 100,
    "getTransaction": 10,
    "getBlock": 10,
    "getBlocks": 10,
    "getInflationReward": 10,
    "getSignaturesForAddress": 10,
    "getBlockTime": 10,
    "simulateTransaction": 105,
    "getWalletTokenList": 100,
    "getWalletTxList": 150,
    # Any other Helius RPC endpoint not listed here will use HELIUS_COST_DEFAULT (1)
}

# --- Birdeye Configuration ---
BIRDEYE_FALLBACK_CREDIT_COST = int(os.getenv('BIRDEYE_FALLBACK_CREDIT_COST', 1)) # Cost used if endpoint_name not in map
BIRDEYE_COST_BATCH_OPERATION = int(os.getenv('BIRDEYE_COST_BATCH_OPERATION', 50)) # Cost for "batch" type operations

BIRDEYE_CREDIT_COST_MAP = {
    "defi_networks": 1,
    "legacy_price": 10,
    "defi_multi_price": BIRDEYE_COST_BATCH_OPERATION, # batch
    "defi_history_price": 60,
    "defi_historical_price_unix": 10,
    "defi_price_volume_single": 15,
    "defi_price_volume_multi": BIRDEYE_COST_BATCH_OPERATION, # batch
    "defi_txs_token": 10,
    "defi_txs_token_seek_by_time": 15,
    "defi_txs_pair": 10,
    "defi_txs_pair_seek_by_time": 15,
    "defi_ohlcv": 30,
    "defi_ohlcv_pair": 30,
    "defi_ohlcv_base_quote": 30,
    "legacy_tokenlist": 30,
    "v3_token_list": 100,
    "v3_token_list_scroll": 500,
    "v2_tokens_new_listing": 80,
    "legacy_token_security": 50,
    "defi_token_trending": 50,
    "legacy_token_overview": 30,
    "v2_markets": 50,
    "legacy_token_creation_info": 50,
    "v2_tokens_top_traders": 30,
    "v3_metadata_single": 5,
    "v3_metadata_multiple": BIRDEYE_COST_BATCH_OPERATION, # batch
    "v3_market_data": 15,
    "v3_trade_data": 15,
    "v3_holders": 50,
    "trader_gainers_losers": 30,
    "trader_txs": 15,
    "v3_search": 100,
    "v3_pair_overview_single": 20,
    "v3_pair_overview_multiple": BIRDEYE_COST_BATCH_OPERATION, # batch
}


# --- File Paths & Flags ---
USAGE_FILE = os.getenv('API_USAGE_FILE', "api_daily_usage.json")
DETAILED_LOG_FILE = os.getenv('API_DETAILED_LOG_FILE', "api_call_details.log")
API_HELIUS_BLOCKED_FLAG_FILE = os.getenv('API_HELIUS_BLOCKED_FLAG_FILE', ".api_helius_blocked_flag")
API_BIRDEYE_BLOCKED_FLAG_FILE = os.getenv('API_BIRDEYE_BLOCKED_FLAG_FILE', ".api_birdeye_blocked_flag")
DRY_RUN_MODE = os.getenv("API_DRY_RUN", "false").lower() == "true"
BIRDEYE_OVERRIDE = os.getenv("API_BIRDEYE_OVERRIDE", "false").lower() == "true" # <-- NEW: Birdeye Override Flag
print(f"\n\n    [[[ CRITICAL DEBUG ]]] api_usage_limiter.py: MODULE-LEVEL BIRDEYE_OVERRIDE IS: {BIRDEYE_OVERRIDE}    [[[ CRITICAL DEBUG ]]]\n\n")

# --- Telegram ---
TELEGRAM_TOKEN_USAGE_ALERT = os.getenv("TELEGRAM_BOT_TOKEN_USAGE_ALERT", os.getenv("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID_USAGE_ALERT = os.getenv("TELEGRAM_CHAT_ID_USAGE_ALERT", os.getenv("TELEGRAM_CHAT_ID"))

# --- Global State (loaded from file) ---
_usage_data = {
    "date": str(date.today()),
    "helius_credits_today": 0,
    "birdeye_credits_today": 0,
    "gmgn_calls_today": 0,
    "other_calls_today": 0,
    "helius_milestones_alerted": [],
    "birdeye_milestones_alerted": [],
    "helius_alert_sent_for_block": False,
    "birdeye_alert_sent_for_block": False,
}
API_HELIUS_BLOCKED = False
API_BIRDEYE_BLOCKED = False

# --- Helper Functions ---

def _send_telegram_alert(message: str):
    token = TELEGRAM_TOKEN_USAGE_ALERT
    chat_id = TELEGRAM_CHAT_ID_USAGE_ALERT
    if not token or not chat_id: print(f"ALERT (Usage Telegram Not Configured): {message}"); return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"; payload = {"chat_id": chat_id, "text": f"📈 API Usage Update 📈\n\n{message}"}
    success = False
    try: r = requests.post(url, data=payload, timeout=10); r.raise_for_status(); print(f"    [✓] Usage Alert Sent: {message.splitlines()[0]}..."); success = True
    except Exception as e: print(f"    [✗] Failed to send Usage Telegram alert: {e}")
    return success

def _load_usage():
    global _usage_data, API_HELIUS_BLOCKED, API_BIRDEYE_BLOCKED
    # Helius blocking (no override for this one currently)
    if os.path.exists(API_HELIUS_BLOCKED_FLAG_FILE):
        API_HELIUS_BLOCKED = True
        print("[!] API Limiter: HELIUS calls are currently BLOCKED (flag file exists).")
    else:
        API_HELIUS_BLOCKED = False # Ensure it's reset if flag is removed

    # Birdeye blocking - now respects BIRDEYE_OVERRIDE for the flag file
    if os.path.exists(API_BIRDEYE_BLOCKED_FLAG_FILE):
        if BIRDEYE_OVERRIDE:
            print("[*] API Limiter: Birdeye block flag file exists, BUT API_BIRDEYE_OVERRIDE is active. Ignoring flag for initial block state.")
            API_BIRDEYE_BLOCKED = False # Override is on, so don't consider the service blocked by the mere presence of the flag
            # Optional: You could also remove the flag file here if the override is on
            # try:
            #     os.remove(API_BIRDEYE_BLOCKED_FLAG_FILE)
            #     print(f"[*] API Limiter: Removed Birdeye block flag file '{API_BIRDEYE_BLOCKED_FLAG_FILE}' due to active override.")
            # except OSError as e:
            #     print(f"[!] API Limiter: Could not remove Birdeye block flag file '{API_BIRDEYE_BLOCKED_FLAG_FILE}' during override: {e}")
        else:
            API_BIRDEYE_BLOCKED = True # Override is OFF, flag exists, so it's blocked.
            print("[!] API Limiter: BIRDEYE calls are currently BLOCKED (flag file exists, override is OFF).")
    else:
        API_BIRDEYE_BLOCKED = False # No flag file, so not blocked initially.

    loaded_successfully = False
    if os.path.exists(USAGE_FILE):
        try:
            with open(USAGE_FILE, "r", encoding="utf-8") as f: data = json.load(f)
            required_keys = ["date", "helius_credits_today", "birdeye_credits_today"]
            if all(k in data for k in required_keys):
                 data.setdefault("gmgn_calls_today", 0)
                 data.setdefault("other_calls_today", 0)
                 data.setdefault("helius_milestones_alerted", [])
                 data.setdefault("birdeye_milestones_alerted", [])
                 data.setdefault("helius_alert_sent_for_block", False)
                 data.setdefault("birdeye_alert_sent_for_block", False)
                 _usage_data = data
                 loaded_successfully = True
            else: print(f"[!] API Limiter: Invalid format in {USAGE_FILE} (missing required keys). Resetting.")
        except (json.JSONDecodeError, IOError) as e:
            print(f"[!] API Limiter: Error reading {USAGE_FILE}: {e}. Resetting usage.")

    if not loaded_successfully:
        _reset_daily_usage_if_new_day(force_reset=True) # This also resets API_HELIUS_BLOCKED and API_BIRDEYE_BLOCKED if a reset occurs
    else:
        _reset_daily_usage_if_new_day()

    # Re-evaluate API_BIRDEYE_BLOCKED if _reset_daily_usage_if_new_day reset it.
    # This ensures that if a new day started and cleared the flag, the override behavior is consistent.
    # This part might be redundant if _reset_daily_usage_if_new_day correctly sets API_BIRDEYE_BLOCKED to False.
    # However, it's safer to re-check the flag status in case of complex interactions.
    if not API_BIRDEYE_BLOCKED: # If it wasn't set to True by the Helius check or Birdeye flag + no override
        if os.path.exists(API_BIRDEYE_BLOCKED_FLAG_FILE):
            if BIRDEYE_OVERRIDE:
                # API_BIRDEYE_BLOCKED remains False
                pass
            else:
                API_BIRDEYE_BLOCKED = True
                # print an additional warning if it gets re-blocked here, though ideally covered above.

    print(f"[*] API Limiter Initialized: Date={_usage_data['date']}, Helius Credits={_usage_data['helius_credits_today']}, Birdeye Credits={_usage_data['birdeye_credits_today']}, GMGN Calls={_usage_data['gmgn_calls_today']}, H_Blocked={API_HELIUS_BLOCKED}, B_Blocked={API_BIRDEYE_BLOCKED} (Override: {BIRDEYE_OVERRIDE})")

def _save_usage():
    global _usage_data
    try:
        if "last_known_remaining_credits" in _usage_data: # Defensive removal if old key still exists
            del _usage_data["last_known_remaining_credits"]
        with open(USAGE_FILE, "w", encoding="utf-8") as f: json.dump(_usage_data, f, indent=2)
    except IOError as e: print(f"[!] API Limiter: CRITICAL - Could not save usage data to {USAGE_FILE}: {e}")

def _reset_daily_usage_if_new_day(force_reset=False):
    global _usage_data, API_HELIUS_BLOCKED, API_BIRDEYE_BLOCKED
    today_str = str(date.today())
    current_data_date = _usage_data.get("date") if isinstance(_usage_data, dict) else None

    if force_reset or current_data_date != today_str:
        print(f"[*] API Limiter: Resetting daily usage counters for {today_str}.")
        _usage_data = {
            "date": today_str,
            "helius_credits_today": 0,
            "birdeye_credits_today": 0,
            "gmgn_calls_today": 0,
            "other_calls_today": 0,
            "helius_milestones_alerted": [],
            "birdeye_milestones_alerted": [],
            "helius_alert_sent_for_block": False,
            "birdeye_alert_sent_for_block": False,
        }
        API_HELIUS_BLOCKED = False
        API_BIRDEYE_BLOCKED = False
        for flag_file in [API_HELIUS_BLOCKED_FLAG_FILE, API_BIRDEYE_BLOCKED_FLAG_FILE]:
            if os.path.exists(flag_file):
                try: os.remove(flag_file); print(f"[*] API Limiter: Removed block flag file '{flag_file}'.")
                except OSError as e: print(f"[!] API Limiter: Could not remove block flag file '{flag_file}': {e}")
        _save_usage()

def _log_detailed_call(api_name: str, endpoint: str, calling_script: str, cache_hit: bool, blocked: bool, credits_used: Optional[int] = None):
    try:
        is_helius = api_name.lower() == "helius"
        is_birdeye = api_name.lower() == "birdeye"
        is_gmgn = api_name.lower() == "gmgn"
        
        current_helius_credits = _usage_data.get("helius_credits_today", 0) if is_helius else "N/A"
        current_birdeye_credits = _usage_data.get("birdeye_credits_today", 0) if is_birdeye else "N/A"
        current_gmgn_calls = _usage_data.get("gmgn_calls_today", 0) if is_gmgn else "N/A"

        log_entry = {
            "timestamp": datetime.now().isoformat(), "script": calling_script, "api": api_name,
            "endpoint": endpoint, "cache_hit": cache_hit, "blocked": blocked, "dry_run": DRY_RUN_MODE,
            "credits_used": credits_used if not cache_hit and not blocked else ("N/A" if (cache_hit or blocked) else 0), 
            "helius_credits_after_call": current_helius_credits,
            "birdeye_credits_after_call": current_birdeye_credits,
            "gmgn_calls_after_call": current_gmgn_calls
        }
        with open(DETAILED_LOG_FILE, "a", encoding="utf-8") as f: f.write(json.dumps(log_entry) + "\n")
    except Exception as e: print(f"[!] API Limiter: Error writing detailed log {DETAILED_LOG_FILE}: {e}")

def _increment_usage(api_name: str, credits_used: int):
    global _usage_data
    api_name_lower = api_name.lower()

    if api_name_lower == "helius":
        _usage_data["helius_credits_today"] = _usage_data.get("helius_credits_today", 0) + credits_used
    elif api_name_lower == "birdeye":
        _usage_data["birdeye_credits_today"] = _usage_data.get("birdeye_credits_today", 0) + credits_used
    elif api_name_lower == "gmgn": 
        _usage_data["gmgn_calls_today"] = _usage_data.get("gmgn_calls_today", 0) + credits_used
    else: 
        _usage_data["other_calls_today"] = _usage_data.get("other_calls_today", 0) + credits_used
    _save_usage()

# --- MODIFIED FUNCTION ---
def _check_api_specific_alerts_and_block(api_name_lower: str) -> bool:
    global _usage_data, API_HELIUS_BLOCKED, API_BIRDEYE_BLOCKED # BIRDEYE_OVERRIDE is a global constant read at startup

    if api_name_lower == "helius":
        current_credits = _usage_data.get("helius_credits_today", 0)
        daily_cap = HELIUS_DAILY_CAP
        milestone_key = "helius_milestones_alerted"
        block_alert_key = "helius_alert_sent_for_block"
        block_flag_file = API_HELIUS_BLOCKED_FLAG_FILE
        is_blocked_global = API_HELIUS_BLOCKED
    elif api_name_lower == "birdeye":
        current_credits = _usage_data.get("birdeye_credits_today", 0)
        daily_cap = BIRDEYE_DAILY_CAP
        milestone_key = "birdeye_milestones_alerted"
        block_alert_key = "birdeye_alert_sent_for_block"
        block_flag_file = API_BIRDEYE_BLOCKED_FLAG_FILE
        is_blocked_global = API_BIRDEYE_BLOCKED
    else:
        return False # Not an API we manage blocking for

    milestones_alerted = _usage_data.get(milestone_key, [])

    if is_blocked_global:  # Already blocked by a flag file (e.g., from a previous run without override)
        return True

    cap_reached = current_credits >= daily_cap

    if cap_reached:
        # Cap is reached. Check if an override applies for Birdeye.
        if api_name_lower == "birdeye" and BIRDEYE_OVERRIDE:
            print(f"[*] API Limiter: Birdeye daily credit cap ({daily_cap}) reached, but API_BIRDEYE_OVERRIDE is active. Call will NOT be blocked by cap.")
            # Block is overridden. The function will proceed to milestone checks and then return False.
        else:
            # No override for this API, or not Birdeye. Proceed with standard blocking.
            if not _usage_data.get(block_alert_key, False): # Check if block alert has already been sent today
                alert_msg = (
                    f"🚫 {api_name_lower.upper()} DAILY CREDIT CAP ({daily_cap}) REACHED! 🚫\n"
                    f"Credits Today: {current_credits}\n"
                    f"Further {api_name_lower.upper()} calls will be BLOCKED today."
                )
                _send_telegram_alert(alert_msg)
                print(f"CRITICAL: {alert_msg}")
                try:
                    with open(block_flag_file, "w", encoding="utf-8") as f:
                        f.write(f"{api_name_lower} Blocked on {_usage_data['date']} at {datetime.now().isoformat()} with {current_credits} credits.")
                except IOError as e:
                    print(f"[!] API Limiter: CRITICAL - Could not create block flag file '{block_flag_file}': {e}")
                _usage_data[block_alert_key] = True
                _save_usage()  # Save because block_alert_key was updated

            if api_name_lower == "helius":
                API_HELIUS_BLOCKED = True
            elif api_name_lower == "birdeye": # This will only be hit if BIRDEYE_OVERRIDE is false
                API_BIRDEYE_BLOCKED = True
            return True  # API call should be blocked

    # If we reach here, the API call is not hard-blocked by daily cap (either cap not reached, or it was overridden for Birdeye).
    # Check for percentage milestones.
    # Standard milestones up to 90%
    for percent_milestone in range(10, 100, 10):
        milestone_credit_count = int(daily_cap * (percent_milestone / 100.0))
        if current_credits >= milestone_credit_count and percent_milestone not in milestones_alerted:
            alert_msg = (
                f"{percent_milestone}% of {api_name_lower.upper()} Daily Credit Cap Reached.\n"
                f"Credits Today: {current_credits} / {daily_cap}"
            )
            _send_telegram_alert(alert_msg)
            print(f"INFO: {alert_msg}")
            _usage_data[milestone_key].append(percent_milestone)
            _save_usage() # Save because milestones_alerted was updated
            
    # Special 100% milestone alert if Birdeye cap is reached AND override is active
    # This replaces the "BLOCKED" alert for Birdeye when overridden.
    if api_name_lower == "birdeye" and cap_reached and BIRDEYE_OVERRIDE:
        percent_milestone_100 = 100
        if percent_milestone_100 not in milestones_alerted: # Check if 100% milestone alert has been sent
            alert_msg = (
                f"INFO: 100% of Birdeye Daily Credit Cap Reached (API_BIRDEYE_OVERRIDE is Active).\n"
                f"Credits Today: {current_credits} / {daily_cap}"
            )
            _send_telegram_alert(alert_msg)
            print(f"INFO: {alert_msg}")
            _usage_data[milestone_key].append(percent_milestone_100) # Log 100% milestone
            _save_usage() # Save because milestones_alerted was updated

    return False # Not blocked
# --- END OF MODIFIED FUNCTION ---

# --- Public Function ---
def request_api(
    method: Callable, api_name: str, endpoint_name: str,
    calling_script: Optional[str] = None, cache_key: Optional[str] = None,
    cache_ttl_seconds: Optional[int] = 3600, force_refresh: bool = False,
    *args, **kwargs
) -> Optional[Any]:
    global API_HELIUS_BLOCKED, API_BIRDEYE_BLOCKED, _usage_data

    _reset_daily_usage_if_new_day()

    if calling_script is None:
        try: frame = traceback.extract_stack(limit=3)[0]; calling_script = os.path.basename(frame.filename) if frame.filename else "unknown_script"
        except Exception: calling_script = "unknown_caller"

    api_name_lower = api_name.lower()

    # --- 1. Check Cache ---
    if CACHE_ENABLED and cache_manager and cache_key and not force_refresh:
        try:
            cached_response = cache_manager.get_cache(cache_key, max_age_seconds=cache_ttl_seconds)
            if cached_response is not None:
                _log_detailed_call(api_name, endpoint_name, calling_script, cache_hit=True, blocked=False, credits_used=0)
                return cached_response
        except Exception as e: print(f"[!] API Limiter: Cache read error for key '{cache_key}': {e}")

    # --- 2. Check Specific API Blocking (Initial Check) ---
    is_currently_blocked = False
    if api_name_lower == "helius": is_currently_blocked = API_HELIUS_BLOCKED or _check_api_specific_alerts_and_block("helius")
    elif api_name_lower == "birdeye": is_currently_blocked = API_BIRDEYE_BLOCKED or _check_api_specific_alerts_and_block("birdeye")
    
    if is_currently_blocked:
        print(f"    [API BLOCKED] Call to {api_name}::{endpoint_name} by {calling_script} denied (Cap Reached/Blocked).")
        _log_detailed_call(api_name, endpoint_name, calling_script, cache_hit=False, blocked=True)
        return None

    # --- 3. Handle Dry Run ---
    if DRY_RUN_MODE:
        print(f"    [DRY RUN] Skipping call: {api_name}::{endpoint_name} by {calling_script}.")
        _log_detailed_call(api_name, endpoint_name, calling_script, cache_hit=False, blocked=False, credits_used=0)
        return None

    # --- Pre-Call Logging (using estimated cost from maps) ---
    estimated_cost = 1 
    usage_str = ""
    if api_name_lower == "helius":
        estimated_cost = HELIUS_CREDIT_COST_MAP.get(endpoint_name, HELIUS_COST_DEFAULT)
        usage_str = f"H Credits: {_usage_data.get('helius_credits_today', 0)}+{estimated_cost}/{HELIUS_DAILY_CAP}"
    elif api_name_lower == "birdeye":
        estimated_cost = BIRDEYE_CREDIT_COST_MAP.get(endpoint_name, BIRDEYE_FALLBACK_CREDIT_COST)
        usage_str = f"B Credits: {_usage_data.get('birdeye_credits_today', 0)}+{estimated_cost}/{BIRDEYE_DAILY_CAP}"
    elif api_name_lower == "gmgn":
        usage_str = f"G Calls: {_usage_data.get('gmgn_calls_today', 0)}+1"
    else:
        usage_str = f"Other Calls: {_usage_data.get('other_calls_today', 0)}+1"
    print(f"    [API Call Attempt] {api_name}::{endpoint_name} by {calling_script}. Current {usage_str}")


    # --- 4. Make the Actual API Call ---
    response_data = None
    api_response_object = None
    actual_credits_used = 0 

    try:
        is_requests_call = hasattr(method, '__module__') and 'requests' in method.__module__
        if is_requests_call and 'timeout' not in kwargs: kwargs['timeout'] = 45
        
        api_response_object = method(*args, **kwargs) 

        if api_response_object is not None:
            status_code = getattr(api_response_object, 'status_code', None)
            headers = getattr(api_response_object, 'headers', {}) 
            content_type = headers.get('Content-Type', '').lower()
            is_success = status_code is not None and 200 <= status_code < 300

            if is_success:
                if 'application/json' in content_type:
                    try: response_data = api_response_object.json()
                    except json.JSONDecodeError as json_err:
                        print(f"    [✗] API Limiter: JSON Decode Error for {api_name}::{endpoint_name}: {json_err}")
                        try: print(f"        Response Text: {api_response_object.text[:500]}")
                        except: pass
                        response_data = None 
                # else: response_data is already None if not json
            else:
                print(f"    [✗] API Limiter: Received non-2xx status ({status_code}) for {api_name}::{endpoint_name}.")
                try: print(f"        Response Text: {api_response_object.text[:500]}") 
                except: pass
                response_data = None

            if api_name_lower == "helius":
                actual_credits_used = HELIUS_CREDIT_COST_MAP.get(endpoint_name, HELIUS_COST_DEFAULT)
                if endpoint_name not in HELIUS_CREDIT_COST_MAP:
                     print(f"    [!] CREDIT MAP MISSING: Helius endpoint '{endpoint_name}' not in HELIUS_CREDIT_COST_MAP. Using default cost: {HELIUS_COST_DEFAULT}.")
            elif api_name_lower == "birdeye":
                actual_credits_used = BIRDEYE_CREDIT_COST_MAP.get(endpoint_name, BIRDEYE_FALLBACK_CREDIT_COST)
                if endpoint_name not in BIRDEYE_CREDIT_COST_MAP:
                    print(f"    [!] CREDIT MAP MISSING: Birdeye endpoint '{endpoint_name}' not in BIRDEYE_CREDIT_COST_MAP. Using fallback cost: {BIRDEYE_FALLBACK_CREDIT_COST}.")
            
            elif api_name_lower == "gmgn" or api_name_lower == "other":
                 actual_credits_used = 1 
        else: 
            print(f"    [✗] API Limiter: API method for {api_name}::{endpoint_name} returned None or did not produce a response object.")
            if api_name_lower == "helius":
                actual_credits_used = HELIUS_CREDIT_COST_MAP.get(endpoint_name, HELIUS_COST_DEFAULT)
                if endpoint_name not in HELIUS_CREDIT_COST_MAP:
                     print(f"    [!] CREDIT MAP MISSING: Helius endpoint '{endpoint_name}' not in HELIUS_CREDIT_COST_MAP (phantom call). Using default cost: {HELIUS_COST_DEFAULT}.")
            elif api_name_lower == "birdeye":
                actual_credits_used = BIRDEYE_CREDIT_COST_MAP.get(endpoint_name, BIRDEYE_FALLBACK_CREDIT_COST)
                if endpoint_name not in BIRDEYE_CREDIT_COST_MAP:
                    print(f"    [!] CREDIT MAP MISSING: Birdeye endpoint '{endpoint_name}' not in BIRDEYE_CREDIT_COST_MAP (phantom call). Using fallback cost: {BIRDEYE_FALLBACK_CREDIT_COST}.")
            else: actual_credits_used = 1
    except Exception as call_err:
        print(f"    [✗] API Limiter: Error during call to {api_name}::{endpoint_name}: {call_err}")
        traceback.print_exc(limit=2)
        response_data = None 
        if api_name_lower == "helius":
            actual_credits_used = HELIUS_CREDIT_COST_MAP.get(endpoint_name, HELIUS_COST_DEFAULT)
            if endpoint_name not in HELIUS_CREDIT_COST_MAP:
                print(f"    [!] CREDIT MAP MISSING: Helius endpoint '{endpoint_name}' not in HELIUS_CREDIT_COST_MAP (exception). Using default cost: {HELIUS_COST_DEFAULT}.")
        elif api_name_lower == "birdeye":
            actual_credits_used = BIRDEYE_CREDIT_COST_MAP.get(endpoint_name, BIRDEYE_FALLBACK_CREDIT_COST)
            if endpoint_name not in BIRDEYE_CREDIT_COST_MAP:
                print(f"    [!] CREDIT MAP MISSING: Birdeye endpoint '{endpoint_name}' not in BIRDEYE_CREDIT_COST_MAP (exception). Using fallback cost: {BIRDEYE_FALLBACK_CREDIT_COST}.")
        else: actual_credits_used = 1

    _increment_usage(api_name, actual_credits_used)
    _log_detailed_call(api_name, endpoint_name, calling_script, cache_hit=False, blocked=False, credits_used=actual_credits_used)

    final_block_check = False
    if api_name_lower == "helius": final_block_check = _check_api_specific_alerts_and_block("helius")
    elif api_name_lower == "birdeye": final_block_check = _check_api_specific_alerts_and_block("birdeye")

    if final_block_check and not is_currently_blocked : 
        print(f"    [API NOW BLOCKED] Call to {api_name}::{endpoint_name} by {calling_script} was the one that triggered the block.")

    if response_data is not None and CACHE_ENABLED and cache_manager and cache_key:
        try: cache_manager.set_cache(cache_key, response_data)
        except Exception as cache_err: print(f"[!] API Limiter: Cache write error for key '{cache_key}': {cache_err}")

    return response_data

# --- Initial Load ---
_load_usage()
# --- MODIFIED PRINT ---
print(f"[*] API Limiter Config: Helius Cap={HELIUS_DAILY_CAP}, Birdeye Cap={BIRDEYE_DAILY_CAP}, Alerts=10%, DryRun={DRY_RUN_MODE}, Cache={CACHE_ENABLED}, BirdeyeOverride={BIRDEYE_OVERRIDE}")
print(f"    Helius default cost: {HELIUS_COST_DEFAULT}, getAsset (map): {HELIUS_CREDIT_COST_MAP.get('getAsset', 'N/A')}, parseTransaction (map): {HELIUS_CREDIT_COST_MAP.get('parseTransaction', 'N/A')}")
print(f"    Birdeye fallback cost: {BIRDEYE_FALLBACK_CREDIT_COST}, batch operation cost: {BIRDEYE_COST_BATCH_OPERATION}")


if __name__ == "__main__":
    print("\n--- API Usage Limiter Status ---")
    print(f"Date: {_usage_data['date']}")
    print(f"Helius Credits Today: {_usage_data['helius_credits_today']}/{HELIUS_DAILY_CAP}")
    print(f"Birdeye Credits Today: {_usage_data['birdeye_credits_today']}/{BIRDEYE_DAILY_CAP}")
    print(f"GMGN Calls Today: {_usage_data.get('gmgn_calls_today', 0)}")
    print(f"Other Calls Today: {_usage_data.get('other_calls_today', 0)}")
    print(f"Helius Milestones Alerted: {_usage_data.get('helius_milestones_alerted', [])}")
    print(f"Birdeye Milestones Alerted: {_usage_data.get('birdeye_milestones_alerted', [])}")
    print(f"Helius Block Alert Sent: {_usage_data.get('helius_alert_sent_for_block', False)}")
    print(f"Birdeye Block Alert Sent: {_usage_data.get('birdeye_alert_sent_for_block', False)}")
    print(f"Helius API Currently Blocked: {API_HELIUS_BLOCKED}")
    print(f"Birdeye API Currently Blocked: {API_BIRDEYE_BLOCKED}")
    # --- MODIFIED PRINT SECTION ---
    print(f"Dry Run Mode: {DRY_RUN_MODE}")
    print(f"Birdeye Override Mode: {BIRDEYE_OVERRIDE}") # <-- NEW
    print(f"Cache Enabled: {CACHE_ENABLED}")
    # --- END OF MODIFIED PRINT SECTION ---
    print(f"Usage File: {os.path.abspath(USAGE_FILE)}")
    print(f"Log File: {os.path.abspath(DETAILED_LOG_FILE)}")
    print(f"Helius Block Flag File: {os.path.abspath(API_HELIUS_BLOCKED_FLAG_FILE)}")
    print(f"Birdeye Block Flag File: {os.path.abspath(API_BIRDEYE_BLOCKED_FLAG_FILE)}")

# --- END OF FILE api_usage_limiter.py ---