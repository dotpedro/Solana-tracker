# --- START OF FILE runner_analyzer.py ---

#!/usr/bin/env python3
"""
Runner Analyzer - KOL Activity Tracker & Bridge
Correlates KOL signals (buys & sells) with tokens from processed reports.
Tracks and alerts on aggregated KOL activity for matched tokens.
Calculates KOL-specific price volatility scores.
Version: 5.6 - Consumes signals from ai_xtn, KOLWalletAlert, and _soleyes. Tracks KOL volatility.
"""

import os
import json
import glob
import time
# import requests # No longer needed for Telegram sending
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple, Set
from decimal import Decimal, getcontext, InvalidOperation
import re
import copy

from dotenv import load_dotenv

# --- Import the centralized Birdeye API gateway ---
BIRDEYE_API_LOAD_ERROR = None
try:
    import birdeye_api
    print("[✓] Runner Analyzer: Successfully imported birdeye_api module.")
except ImportError as e:
    BIRDEYE_API_LOAD_ERROR = f"Failed to import birdeye_api: {e}. Birdeye calls WILL FAIL."
    print(f"[CRITICAL] Runner Analyzer: {BIRDEYE_API_LOAD_ERROR}")
except ValueError as e: 
     BIRDEYE_API_LOAD_ERROR = f"Error initializing birdeye_api (likely missing API key): {e}. Birdeye calls WILL FAIL."
     print(f"[CRITICAL] Runner Analyzer: {BIRDEYE_API_LOAD_ERROR}")
except Exception as e:
    BIRDEYE_API_LOAD_ERROR = f"Unexpected error importing/initializing birdeye_api: {e}. Birdeye calls WILL FAIL."
    print(f"[CRITICAL] Runner Analyzer: {BIRDEYE_API_LOAD_ERROR}")

# --- Import the centralized Telegram Alert Router ---
TELEGRAM_ROUTER_LOAD_ERROR = None
try:
    import telegram_alert_router
    from telegram_alert_router import escape_markdown_legacy
    print("[✓] Runner Analyzer: Successfully imported telegram_alert_router module.")
except ImportError as e:
    TELEGRAM_ROUTER_LOAD_ERROR = f"Failed to import telegram_alert_router: {e}. Telegram alerts WILL FAIL."
    print(f"[CRITICAL] Runner Analyzer: {TELEGRAM_ROUTER_LOAD_ERROR}")
    def escape_markdown_legacy(text: Optional[Any]) -> str: return str(text or '')
except Exception as e:
    TELEGRAM_ROUTER_LOAD_ERROR = f"Unexpected error importing telegram_alert_router: {e}. Telegram alerts WILL FAIL."
    print(f"[CRITICAL] Runner Analyzer: {TELEGRAM_ROUTER_LOAD_ERROR}")
    def escape_markdown_legacy(text: Optional[Any]) -> str: return str(text or '')

# --- Dependency Check for api_usage_limiter (Optional Info) ---
try:
    import api_usage_limiter 
    print("[✓] Runner Analyzer: Found api_usage_limiter (dependency for birdeye_api).")
except ImportError:
    print("[!] Runner Analyzer: api_usage_limiter not found. birdeye_api module will likely use its internal STUB for API calls.")


load_dotenv()
getcontext().prec = 18 

# --- Configuration ---
# <<< CHANGE: Added SOLEYES_SIGNALS_FILE >>>
INFLUENCER_SIGNALS_FILE = "influencer_structured_signals.json" # From original ai_xtn scraper
KOL_WALLET_ALERT_SIGNALS_FILE = "kol_wallet_alert_structured_signals.json" # From KOLWalletAlert scraper
SOLEYES_SIGNALS_FILE = "soleyes_structured_signals.json" # <<< NEW: From _soleyes scraper

PROCESSED_REPORTS_DIR = os.path.join("reports", "holdings", "processed")
RUNNER_ANALYZER_STATE_FILE = "runner_analyzer_kol_activity_state.json"
LOG_FILE = "runner_analyzer_kol_activity.log"
CHECK_INTERVAL_SECONDS = int(os.getenv("RUNNER_KOL_CHECK_INTERVAL_SECONDS", "90"))

ALERT_CATEGORY_MAIN = "RUNNER"
ALERT_CATEGORY_ERROR = "RUNNER_ERROR"

KOL_SIGNAL_MAX_AGE_HOURS = int(os.getenv("RUNNER_KOL_SIGNAL_MAX_AGE_HOURS", "12"))
MIN_LIQUIDITY_FOR_ALERT = Decimal(os.getenv("RUNNER_KOL_MIN_LIQUIDITY", "2000"))
MIN_MCAP_FOR_ALERT = Decimal(os.getenv("RUNNER_KOL_MIN_MCAP", "3000"))
MAX_MCAP_FOR_ALERT = Decimal(os.getenv("RUNNER_KOL_MAX_MCAP", "3000000"))
MIN_VOLUME_24H_FOR_ALERT = Decimal(os.getenv("RUNNER_KOL_MIN_VOLUME_24H", "5000"))
MIN_ALERT_INTERVAL_PER_TOKEN_SECONDS = int(os.getenv("RUNNER_KOL_MIN_ALERT_INTERVAL_TOKEN_SECONDS", 1 * 60 * 60))
TOKEN_ACTIVITY_PROFILE_RETENTION_DAYS = int(os.getenv("RUNNER_KOL_ACTIVITY_RETENTION_DAYS", "3"))

NUM_LATEST_REPORTS_TO_ANALYZE = int(os.getenv("RUNNER_KOL_NUM_REPORTS_TO_ANALYZE", "5"))
MAX_ALERTS_PER_CYCLE = 10

KOL_VOLATILITY_WINDOW_SECONDS = int(os.getenv("RUNNER_KOL_VOLATILITY_WINDOW_SECONDS", 1 * 60 * 60)) 
KOL_MIN_SIGNALS_FOR_SCORE_DISPLAY = int(os.getenv("RUNNER_KOL_MIN_SIGNALS_FOR_SCORE_DISPLAY", "3"))
KOL_PERFORMANCE_METRICS_RETENTION_DAYS = int(os.getenv("RUNNER_KOL_PERFORMANCE_METRICS_RETENTION_DAYS", "30"))


os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)

# --- Utility Functions (Logging, JSON Handling, Formatting, Time) ---
# ... (These functions: log_message, load_json_data, save_json_data, load_kol_activity_state, save_kol_activity_state, 
# _format_compact, _format_percent, get_token_creation_timestamp, calculate_age_string remain unchanged from your provided code) ...
def log_message(message: str, is_error: bool = False, to_telegram_error_channel: bool = False):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir and not os.path.exists(log_dir):
            try: os.makedirs(log_dir, exist_ok=True)
            except OSError: pass
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(log_entry + "\n")
    except IOError: print(f"[!] Critical: Could not write to log file {LOG_FILE}")

    if is_error and to_telegram_error_channel and not TELEGRAM_ROUTER_LOAD_ERROR:
        escaped_message = escape_markdown_legacy(message[:1000])
        title = "🚨 Runner Analyzer (KOL Activity) Error 🚨"
        full_telegram_message = f"{title}\n{escaped_message}"
        telegram_alert_router.send_alert(
            full_telegram_message, category=ALERT_CATEGORY_ERROR,
            parse_mode="Markdown", escape_markdown=False, prefix=None
        )
    elif is_error and to_telegram_error_channel and TELEGRAM_ROUTER_LOAD_ERROR:
         print(f"[!] Telegram Router failed to load, cannot send error to Telegram: {TELEGRAM_ROUTER_LOAD_ERROR}")

def load_json_data(path: str, is_state_file: bool = False) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if is_state_file: log_message(f"    [i] State file not found: {path}. Will create.")
    except json.JSONDecodeError as e: log_message(f"    [!] Failed to decode JSON from {path}: {e}")
    except Exception as e: log_message(f"    [!] Failed to load {path}: {e}")
    return None

def save_json_data(path: str, data: Dict[str, Any]): 
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: log_message(f"    [✗] Failed to save data to {path}: {e}", is_error=True, to_telegram_error_channel=True)


def load_kol_activity_state() -> Dict[str, Any]:
    state_data = load_json_data(RUNNER_ANALYZER_STATE_FILE, is_state_file=True)
    if state_data is None: state_data = {}
    
    state_data.setdefault("token_kol_activity", {})
    if "token_kol_activity" in state_data and isinstance(state_data["token_kol_activity"], dict):
        for mint, profile_data in state_data["token_kol_activity"].items():
            profile_data["reported_by_kols"] = set(profile_data.get("reported_by_kols", []))
            profile_data.setdefault("latest_known_action_by_kol", {})
            profile_data.setdefault("kol_actions", [])
            profile_data.setdefault("first_kol_signal_ts", 0)
            profile_data.setdefault("last_kol_signal_ts", 0)
            profile_data.setdefault("last_alert_ts_for_this_token", 0)
            profile_data.setdefault("symbol_for_alert", mint[:6] if mint else "N/A")
            profile_data.setdefault("mint", mint)

    state_data.setdefault("kol_performance_metrics", {})
    if "kol_performance_metrics" in state_data and isinstance(state_data["kol_performance_metrics"], dict):
        for kol_name, metrics in state_data["kol_performance_metrics"].items():
            try:
                metrics["cumulative_abs_price_change_pct"] = Decimal(str(metrics.get("cumulative_abs_price_change_pct", "0.0")))
                metrics["average_abs_price_change_pct"] = Decimal(str(metrics.get("average_abs_price_change_pct", "0.0")))
            except InvalidOperation: 
                log_message(f"    [!] Invalid Decimal string for KOL {kol_name} metrics. Resetting to 0.", is_error=True)
                metrics["cumulative_abs_price_change_pct"] = Decimal("0.0")
                metrics["average_abs_price_change_pct"] = Decimal("0.0")
            metrics["signals_processed_for_volatility"] = int(metrics.get("signals_processed_for_volatility", 0))
            metrics.setdefault("pending_volatility_checks", [])
            for check in metrics["pending_volatility_checks"]:
                if "price_at_processing_time" in check:
                     check["price_at_processing_time"] = str(check["price_at_processing_time"])
    return state_data

def save_kol_activity_state(state: Dict[str, Any]):
    state_to_serialize = copy.deepcopy(state)
    if "token_kol_activity" in state_to_serialize and isinstance(state_to_serialize["token_kol_activity"], dict):
        for mint, profile in state_to_serialize["token_kol_activity"].items():
            if "reported_by_kols" in profile and isinstance(profile["reported_by_kols"], set):
                profile["reported_by_kols"] = sorted(list(profile["reported_by_kols"]))
    
    if "kol_performance_metrics" in state_to_serialize and isinstance(state_to_serialize["kol_performance_metrics"], dict):
        for kol_name, metrics in state_to_serialize["kol_performance_metrics"].items():
            if "cumulative_abs_price_change_pct" in metrics and isinstance(metrics.get("cumulative_abs_price_change_pct"), Decimal):
                metrics["cumulative_abs_price_change_pct"] = str(metrics["cumulative_abs_price_change_pct"])
            if "average_abs_price_change_pct" in metrics and isinstance(metrics.get("average_abs_price_change_pct"), Decimal):
                metrics["average_abs_price_change_pct"] = str(metrics["average_abs_price_change_pct"])
    try:
        with open(RUNNER_ANALYZER_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state_to_serialize, f, indent=2)
        log_message(f"    [i] Saved KOL activity state to {RUNNER_ANALYZER_STATE_FILE}")
    except TypeError as te:
        log_message(f"    [✗] Failed to serialize state for saving: {te}. State may contain non-serializable types.", is_error=True, to_telegram_error_channel=True)
        log_message(f"    [✗] Problematic state sample: {str(state_to_serialize)[:1000]}", is_error=True) 
    except Exception as e:
        log_message(f"    [✗] Failed to save data to {RUNNER_ANALYZER_STATE_FILE}: {e}", is_error=True, to_telegram_error_channel=True)

def _format_compact(value: Optional[Any], prefix: str = "$", precision: int = 1, default_val: str = "N/A") -> str:
    if value is None: return default_val
    try: val_decimal = Decimal(str(value))
    except: return str(value) if value is not None else default_val
    if abs(val_decimal) >= Decimal("1000000000"): return f"{prefix}{val_decimal/Decimal('1000000000'):.{precision}f}B"
    if abs(val_decimal) >= Decimal("1000000"): return f"{prefix}{val_decimal/Decimal('1000000'):.{precision}f}M"
    if abs(val_decimal) >= Decimal("1000"): return f"{prefix}{val_decimal/Decimal('1000'):.{precision}f}K"
    precision_for_small = 6 if val_decimal != Decimal(0) and abs(val_decimal) < Decimal(1) else (precision if abs(val_decimal) > 0 else 0)
    return f"{prefix}{val_decimal:,.{precision_for_small}f}"

def _format_percent(value: Optional[Any], default_val: str = "N/A") -> str:
    if value is None: return default_val
    try: val_decimal = Decimal(str(value))
    except: return str(value) if value is not None else default_val
    return f"{val_decimal:+.1f}%"

def get_token_creation_timestamp(live_creation_info: Optional[Dict[str, Any]]) -> Optional[float]:
    block_time = live_creation_info.get("blockUnixTime") if live_creation_info else None
    if isinstance(block_time, (int, float)): return float(block_time)
    return None

def calculate_age_string(creation_unix_timestamp: Optional[float]) -> str:
    if creation_unix_timestamp is None: return "Unknown"
    try:
        creation_dt = datetime.fromtimestamp(creation_unix_timestamp, tz=timezone.utc)
        age_delta = datetime.now(timezone.utc) - creation_dt
        total_seconds = age_delta.total_seconds()
        if total_seconds < 0: return "Future?"
        days = int(total_seconds // 86400); total_seconds %= 86400
        hours = int(total_seconds // 3600); total_seconds %= 3600
        minutes = int(total_seconds // 60); seconds = int(total_seconds % 60)
        if days > 0: return f"{days}d {hours}h"
        if hours > 0: return f"{hours}h {minutes}m"
        if minutes > 0: return f"{minutes}m {seconds}s"
        return f"{seconds}s"
    except Exception: return "Error"

# --- KOL Performance Metrics Functions ---
# ... (_ensure_kol_metrics_exist, process_pending_volatility_checks remain unchanged) ...
def _ensure_kol_metrics_exist(state: Dict[str, Any], kol_name: str) -> Dict[str, Any]:
    kol_metrics_all = state.setdefault("kol_performance_metrics", {})
    kol_specific_metrics = kol_metrics_all.setdefault(kol_name, {})
    
    if not isinstance(kol_specific_metrics.get("cumulative_abs_price_change_pct"), Decimal):
        kol_specific_metrics["cumulative_abs_price_change_pct"] = Decimal(str(kol_specific_metrics.get("cumulative_abs_price_change_pct", "0.0")))
    if not isinstance(kol_specific_metrics.get("average_abs_price_change_pct"), Decimal):
        kol_specific_metrics["average_abs_price_change_pct"] = Decimal(str(kol_specific_metrics.get("average_abs_price_change_pct", "0.0")))
    
    kol_specific_metrics.setdefault("signals_processed_for_volatility", 0)
    kol_specific_metrics.setdefault("pending_volatility_checks", [])
    return kol_specific_metrics

def process_pending_volatility_checks(state: Dict[str, Any], current_time_unix: int) -> bool:
    if BIRDEYE_API_LOAD_ERROR: return False
    
    log_message("    [*] Processing pending KOL volatility checks...")
    updated_kols = False
    kol_performance_metrics = state.get("kol_performance_metrics", {})

    for kol_name, kol_metrics in kol_performance_metrics.items():
        pending_checks = kol_metrics.get("pending_volatility_checks", [])
        new_pending_checks = []
        processed_this_kol = False

        for check_item in pending_checks:
            if current_time_unix >= check_item.get("measurement_due_ts", float('inf')):
                token_mint = check_item.get("token_mint")
                price_at_processing_str = check_item.get("price_at_processing_time")
                signal_tweet_id = check_item.get("signal_tweet_id", "N/A")
                log_message(f"        Measuring volatility for KOL {kol_name}, token {token_mint[:6]}, signal {signal_tweet_id[:10]}...")
                
                time.sleep(0.1) 
                current_market_data = birdeye_api.market_data(token_mint)
                current_price_decimal: Optional[Decimal] = None

                if current_market_data and current_market_data.get("price") is not None:
                    try:
                        current_price_decimal = Decimal(str(current_market_data["price"]))
                    except InvalidOperation:
                        log_message(f"        [!] Invalid current price for {token_mint} from Birdeye: {current_market_data['price']}", is_error=True)
                        new_pending_checks.append(check_item) 
                        continue
                else: 
                    log_message(f"        [!] Could not fetch current price for {token_mint}. Assuming rug (100% abs volatility).")
                    current_price_decimal = Decimal(0) 

                try:
                    price_at_processing_dec = Decimal(price_at_processing_str)
                except InvalidOperation:
                    log_message(f"        [!] Invalid stored price_at_processing_time for {token_mint}: {price_at_processing_str}. Skipping this check.", is_error=True)
                    continue

                price_change_pct: Decimal
                if price_at_processing_dec == Decimal(0):
                    if current_price_decimal == Decimal(0): price_change_pct = Decimal(0)
                    else: price_change_pct = Decimal(1000) 
                else:
                    price_change_pct = ((current_price_decimal - price_at_processing_dec) / price_at_processing_dec) * Decimal(100)
                
                abs_price_change_pct = abs(price_change_pct)
                
                kol_metrics["cumulative_abs_price_change_pct"] = kol_metrics.get("cumulative_abs_price_change_pct", Decimal("0.0")) + abs_price_change_pct
                kol_metrics["signals_processed_for_volatility"] = kol_metrics.get("signals_processed_for_volatility", 0) + 1
                
                if kol_metrics["signals_processed_for_volatility"] > 0:
                    kol_metrics["average_abs_price_change_pct"] = kol_metrics["cumulative_abs_price_change_pct"] / Decimal(kol_metrics["signals_processed_for_volatility"])
                else: 
                    kol_metrics["average_abs_price_change_pct"] = Decimal("0.0")

                log_message(f"        KOL {kol_name} volatility for {token_mint[:6]}: {price_change_pct:+.1f}%. New Avg |ΔP|: {kol_metrics['average_abs_price_change_pct']:.1f}% ({kol_metrics['signals_processed_for_volatility']} signals)")
                updated_kols = True
                processed_this_kol = True
            else:
                new_pending_checks.append(check_item) 
        
        if processed_this_kol: 
             kol_metrics["pending_volatility_checks"] = new_pending_checks

    if updated_kols:
        log_message("    [*] Finished processing KOL volatility checks. Some scores updated.")
    else:
        log_message("    [*] No KOL volatility checks were due or processed this cycle.")
    return updated_kols

# --- Main Processing Logic ---
def process_kol_activity_and_alert(state: Dict[str, Any]):
    log_message("[*] Starting KOL Activity Tracking & Alerting cycle...")
    current_time_unix = int(time.time())
    state_updated_this_cycle = False 

    if BIRDEYE_API_LOAD_ERROR:
         log_message(f"[CRITICAL] Cannot proceed with cycle: {BIRDEYE_API_LOAD_ERROR}", is_error=True, to_telegram_error_channel=True)
         return
    if TELEGRAM_ROUTER_LOAD_ERROR:
        log_message(f"[CRITICAL] Telegram router failed to load: {TELEGRAM_ROUTER_LOAD_ERROR}. Alerts will not be sent.", is_error=True)

    if process_pending_volatility_checks(state, current_time_unix):
        state_updated_this_cycle = True

    # 1. Load Influencer Signals from all sources
    all_kol_signals_raw: List[Dict[str, Any]] = []
    
    # Source 1: ai_xtn (original)
    signals_ai_xtn_raw = load_json_data(INFLUENCER_SIGNALS_FILE)
    if signals_ai_xtn_raw and isinstance(signals_ai_xtn_raw, list):
        all_kol_signals_raw.extend(signals_ai_xtn_raw)
        log_message(f"    [i] Loaded {len(signals_ai_xtn_raw)} signals from {INFLUENCER_SIGNALS_FILE} (ai_xtn source).")
    else:
        log_message(f"    [!] No signals found or file invalid for {INFLUENCER_SIGNALS_FILE} (ai_xtn source).")

    # Source 2: KOLWalletAlert
    signals_kolwallet_raw = load_json_data(KOL_WALLET_ALERT_SIGNALS_FILE)
    if signals_kolwallet_raw and isinstance(signals_kolwallet_raw, list):
        all_kol_signals_raw.extend(signals_kolwallet_raw)
        log_message(f"    [i] Loaded {len(signals_kolwallet_raw)} signals from {KOL_WALLET_ALERT_SIGNALS_FILE} (KOLWalletAlert source).")
    else:
        log_message(f"    [!] No signals found or file invalid for {KOL_WALLET_ALERT_SIGNALS_FILE} (KOLWalletAlert source).")

    # <<< CHANGE: Added loading for _soleyes signals >>>
    # Source 3: _soleyes
    signals_soleyes_raw = load_json_data(SOLEYES_SIGNALS_FILE)
    if signals_soleyes_raw and isinstance(signals_soleyes_raw, list):
        all_kol_signals_raw.extend(signals_soleyes_raw)
        log_message(f"    [i] Loaded {len(signals_soleyes_raw)} signals from {SOLEYES_SIGNALS_FILE} (_soleyes source).")
    else:
        log_message(f"    [!] No signals found or file invalid for {SOLEYES_SIGNALS_FILE} (_soleyes source).")


    if not all_kol_signals_raw:
        log_message("    [!] No influencer signals found from any source. Proceeding with existing state.")
    else:
        log_message(f"    [i] Total raw signals from all sources: {len(all_kol_signals_raw)} before deduplication.")

    deduplicated_signals_map: Dict[str, Dict[str, Any]] = {}
    for signal_item in all_kol_signals_raw:
        if isinstance(signal_item, dict) and "tweet_id" in signal_item:
            # Prioritize signals with more complete information if duplicate tweet_ids occur across files (unlikely but possible)
            # For now, simple "first-seen" or "last-seen" based on list order.
            # If `signal_item` comes from a more trusted source or has more fields, could prioritize.
            # Current behavior: last one in all_kol_signals_raw for a given tweet_id wins.
            deduplicated_signals_map[signal_item["tweet_id"]] = signal_item 
    
    all_kol_signals_processed = list(deduplicated_signals_map.values())
    if all_kol_signals_raw:
        log_message(f"    [i] Total signals after deduplication by tweet_id: {len(all_kol_signals_processed)}.")

    recent_kol_signals: List[Dict[str, Any]] = []
    kol_signal_cutoff_ts = current_time_unix - (KOL_SIGNAL_MAX_AGE_HOURS * 3600)
    for signal in all_kol_signals_processed:
        if isinstance(signal, dict) and \
           isinstance(signal.get("tweet_actual_timestamp"), int) and \
           signal["tweet_actual_timestamp"] >= kol_signal_cutoff_ts and \
           signal.get("primary_symbol") and \
           isinstance(signal.get("primary_symbol"), str) and \
           not re.match(r"\$[0-9\.,]+[KkMmBb]?", signal.get("primary_symbol", "")) and \
           signal.get("contract_address") and \
           signal.get("action") in ["buy", "sell"]:
            recent_kol_signals.append(signal)

    if not recent_kol_signals and all_kol_signals_processed :
        log_message(f"    [i] No recent KOL 'buy' or 'sell' signals (with valid primary_symbol & CA) from any source within the last {KOL_SIGNAL_MAX_AGE_HOURS} hours.")
    elif recent_kol_signals:
        log_message(f"    [i] Found {len(recent_kol_signals)} recent & valid KOL signals from all sources to process.")
    recent_kol_signals.sort(key=lambda s: s["tweet_actual_timestamp"])


    # --- Steps 2, 3, 4, 5 (Scan Reports, Process Signals, Alerting, Cleanup) remain largely unchanged in logic, ---
    # --- but will now operate on the combined set of signals from all three sources. ---
    # --- No structural changes needed in those sections due to adding a new source, as long as the signal format is consistent. ---

    # 2. Scan Processed Reports
    report_files_with_mtime: List[Tuple[str, float]] = []
    for pattern in [os.path.join(PROCESSED_REPORTS_DIR, "holding_report_*.json"), os.path.join(PROCESSED_REPORTS_DIR, "holding_delta_*.json")]:
        for report_file_path in glob.glob(pattern):
            try: report_files_with_mtime.append((report_file_path, os.path.getmtime(report_file_path)))
            except FileNotFoundError: continue

    if not report_files_with_mtime: log_message("    [!] No processed reports found. Cannot cross-reference KOL signals effectively for alerts.")
    report_files_with_mtime.sort(key=lambda x: x[1], reverse=True)
    latest_n_report_paths = [path for path, _ in report_files_with_mtime[:NUM_LATEST_REPORTS_TO_ANALYZE]]

    all_report_tokens_map: Dict[str, Dict[str, Any]] = {}
    if latest_n_report_paths:
        log_message(f"    [i] Will load tokens from up to {len(latest_n_report_paths)} latest reports.")
        for report_path in latest_n_report_paths:
            report_data = load_json_data(report_path)
            if report_data:
                report_items_list = []
                if "correlated_holdings" in report_data and isinstance(report_data.get("correlated_holdings"), list): report_items_list = report_data["correlated_holdings"]
                elif "correlated_holdings_snapshot" in report_data and isinstance(report_data.get("correlated_holdings_snapshot"), list): report_items_list = report_data["correlated_holdings_snapshot"]
                elif "holdings" in report_data and isinstance(report_data.get("holdings"), list): report_items_list = report_data["holdings"]
                for token_data in report_items_list:
                     if isinstance(token_data, dict):
                        mint = token_data.get("token_mint") or token_data.get("mint")
                        if mint and isinstance(mint, str) and mint not in all_report_tokens_map: all_report_tokens_map[mint] = token_data
        log_message(f"    [i] Loaded {len(all_report_tokens_map)} unique tokens from reports.")
    else: log_message("    [i] No reports found to cross-reference with KOL signals. Alerts will rely on prior alert status.")

    token_kol_activity = state.get("token_kol_activity", {})
    alerts_this_cycle_count = 0
    updated_token_profiles_this_cycle = set()

    # 3. Process each KOL signal and update token profiles
    for kol_signal in recent_kol_signals:
        kol_token_mint = kol_signal.get("contract_address")
        kol_tweet_id = kol_signal.get("tweet_id")
        kol_name = kol_signal.get("kol_wallet", "Unknown KOL")
        kol_action = kol_signal.get("action")
        kol_symbol = kol_signal.get("primary_symbol")
        kol_tweet_ts = kol_signal.get("tweet_actual_timestamp")
        # source_account = kol_signal.get("source_account", "N/A") # Available if scraper adds it

        if not all([kol_token_mint, kol_tweet_id, kol_name, kol_action, kol_symbol, kol_tweet_ts]):
            log_message(f"    [!] Skipping incomplete KOL signal: {str(kol_signal)[:200]}...")
            continue

        token_profile = token_kol_activity.get(kol_token_mint)
        if token_profile is None:
            token_profile = {
                "mint": kol_token_mint, "symbol_for_alert": kol_symbol, "first_kol_signal_ts": kol_tweet_ts,
                "last_kol_signal_ts": kol_tweet_ts, "last_alert_ts_for_this_token": 0, "kol_actions": [],
                "reported_by_kols": set(), "latest_known_action_by_kol": {}
            }
            token_kol_activity[kol_token_mint] = token_profile
            state_updated_this_cycle = True 
        else: 
            if not isinstance(token_profile.get("reported_by_kols"), set):
                 token_profile["reported_by_kols"] = set(token_profile.get("reported_by_kols", []))

        if token_profile["symbol_for_alert"] == kol_token_mint[:6] and kol_symbol != kol_token_mint[:6]:
             token_profile["symbol_for_alert"] = kol_symbol 
             state_updated_this_cycle = True

        action_already_recorded = any(a.get("tweet_id") == kol_tweet_id for a in token_profile.get("kol_actions", []))
        if action_already_recorded: continue

        new_action_record = {"kol": kol_name, "action": kol_action, "tweet_id": kol_tweet_id, "tweet_ts": kol_tweet_ts, "symbol": kol_symbol}
        token_profile.setdefault("kol_actions", []).append(new_action_record)
        token_profile["kol_actions"].sort(key=lambda x: x.get("tweet_ts", 0))

        token_profile["last_kol_signal_ts"] = max(token_profile.get("last_kol_signal_ts", 0), kol_tweet_ts)
        token_profile.setdefault("reported_by_kols", set()).add(kol_name)
        token_profile.setdefault("latest_known_action_by_kol", {})[kol_name] = kol_action
        state_updated_this_cycle = True 

        updated_token_profiles_this_cycle.add(kol_token_mint)
        log_message(f"    Updated KOL activity for {kol_symbol} ({kol_token_mint[:6]}): {kol_name} {kol_action} (Tweet ID: {kol_tweet_id})")

        if not BIRDEYE_API_LOAD_ERROR and KOL_VOLATILITY_WINDOW_SECONDS > 0:
            kol_metrics = _ensure_kol_metrics_exist(state, kol_name)
            is_duplicate_pending = any(p.get("signal_tweet_id") == kol_tweet_id for p in kol_metrics["pending_volatility_checks"])
            
            if not is_duplicate_pending:
                time.sleep(0.1) 
                price_data = birdeye_api.market_data(kol_token_mint)
                if price_data and price_data.get("price") is not None:
                    try:
                        price_at_processing = Decimal(str(price_data["price"]))
                        kol_metrics["pending_volatility_checks"].append({
                            "token_mint": kol_token_mint,
                            "signal_tweet_id": kol_tweet_id,
                            "price_at_processing_time": str(price_at_processing),
                            "processing_timestamp": current_time_unix,
                            "signal_action": kol_action, 
                            "measurement_due_ts": current_time_unix + KOL_VOLATILITY_WINDOW_SECONDS
                        })
                        log_message(f"        Added pending volatility check for KOL {kol_name}, token {kol_token_mint[:6]} (price: {price_at_processing}). Due in {KOL_VOLATILITY_WINDOW_SECONDS // 60} mins.")
                        state_updated_this_cycle = True
                    except InvalidOperation:
                        log_message(f"        [!] Could not parse price {price_data['price']} for {kol_token_mint} to Decimal. Volatility check not added.", is_error=True)
                else:
                    log_message(f"        [!] Could not fetch price for {kol_token_mint} to add volatility check.")

    # 4. Alerting based on updated profiles
    if updated_token_profiles_this_cycle:
        log_message(f"    [*] Evaluating {len(updated_token_profiles_this_cycle)} updated token profiles for alerts...")
    for token_mint_to_alert in updated_token_profiles_this_cycle:
        if alerts_this_cycle_count >= MAX_ALERTS_PER_CYCLE:
            log_message(f"    [!] Reached max alerts ({MAX_ALERTS_PER_CYCLE}) for this script run. Remaining will be processed next time.")
            break

        token_profile = token_kol_activity.get(token_mint_to_alert)
        if not token_profile:
            log_message(f"    [!] Error: Profile for {token_mint_to_alert} not found during alert phase.")
            continue

        alert_primary_symbol = token_profile.get("symbol_for_alert", token_mint_to_alert[:6])
        is_in_report = token_mint_to_alert in all_report_tokens_map
        has_been_alerted_on_previously = token_profile.get("last_alert_ts_for_this_token", 0) > 0

        if not is_in_report and not has_been_alerted_on_previously:
            log_message(f"    [i] Token {alert_primary_symbol} ({token_mint_to_alert[:6]}) received KOL signal(s) but is NOT in the latest {NUM_LATEST_REPORTS_TO_ANALYZE} reports AND has NOT been alerted on previously. No alert this time.")
            continue
        elif not is_in_report and has_been_alerted_on_previously:
            log_message(f"    [i] Token {alert_primary_symbol} ({token_mint_to_alert[:6]}) is NOT in current reports, but WAS alerted previously. Proceeding with alert check.")

        if current_time_unix - token_profile.get("last_alert_ts_for_this_token", 0) < MIN_ALERT_INTERVAL_PER_TOKEN_SECONDS:
            last_alert_dt = datetime.fromtimestamp(token_profile.get('last_alert_ts_for_this_token', 0), tz=timezone.utc)
            log_message(f"    [i] Alert for {alert_primary_symbol} ({token_mint_to_alert[:6]}) on cooldown ({MIN_ALERT_INTERVAL_PER_TOKEN_SECONDS}s). Last alert: {last_alert_dt.isoformat()}. Skipping.")
            continue

        log_message(f"    [*] Evaluating alert conditions for token: {alert_primary_symbol} ({token_mint_to_alert[:6]})")

        time.sleep(0.1); live_market_data = birdeye_api.market_data(token_mint_to_alert)
        time.sleep(0.2); live_trade_data = birdeye_api.trade_data(token_mint_to_alert)
        time.sleep(0.2); live_creation_info = birdeye_api.token_creation_info(token_mint_to_alert)

        validation_warnings = []
        live_liquidity, live_mcap, live_fdv, live_volume_24h, live_price = Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0)
        txns_24h, price_1h_pct_live, price_6h_pct_live, price_24h_pct_live = 0, None, None, None
        live_total_supply_str = "N/A"

        if not live_market_data: validation_warnings.append("Failed to fetch live Birdeye Market data.")
        if not live_trade_data: validation_warnings.append("Failed to fetch live Birdeye Trade data.")
        if not live_creation_info: log_message(f"    [i] Failed to fetch creation info for {alert_primary_symbol} ({token_mint_to_alert[:6]}). Age will be unknown.")

        try:
            if live_market_data:
                live_liquidity = Decimal(str(live_market_data.get("liquidity", "0")))
                live_mcap = Decimal(str(live_market_data.get("marketCap", "0")))
                live_fdv = Decimal(str(live_market_data.get("fdv", "0")))
                live_price = Decimal(str(live_market_data.get("price", "0")))
                raw_total_supply = live_market_data.get("totalSupply")
                if raw_total_supply is not None: live_total_supply_str = _format_compact(raw_total_supply, prefix="", precision=0)
            if live_trade_data:
                live_volume_24h = Decimal(str(live_trade_data.get("volume24h", "0")))
                txns_24h = live_trade_data.get("txnCount24h", 0)
                price_1h_pct_live = live_trade_data.get("priceChange1hPercent")
                price_6h_pct_live = live_trade_data.get("priceChange6hPercent")
                price_24h_pct_live = live_trade_data.get("priceChange24hPercent")
        except Exception as dec_ex:
            log_message(f"    [!] Error processing live data for {alert_primary_symbol}: {dec_ex}")
            validation_warnings.append(f"Error processing live data fields.")

        effective_mcap_for_check = live_mcap
        mcap_display_str = _format_compact(live_mcap)
        if live_mcap == Decimal(0) and live_fdv > Decimal(0):
            effective_mcap_for_check = live_fdv
            mcap_display_str = f"$0 (FDV: {_format_compact(live_fdv)})"
        elif live_mcap == Decimal(0) and live_fdv == Decimal(0):
            mcap_display_str = "$0 (FDV N/A or $0"
            if live_total_supply_str != "N/A": mcap_display_str += f", Supply: {live_total_supply_str})"
            else: mcap_display_str += ")"

        if live_liquidity < MIN_LIQUIDITY_FOR_ALERT: validation_warnings.append(f"Low Liq: {_format_compact(live_liquidity)} (Min: {_format_compact(MIN_LIQUIDITY_FOR_ALERT)})")
        if live_market_data and not (MIN_MCAP_FOR_ALERT <= effective_mcap_for_check <= MAX_MCAP_FOR_ALERT):
            validation_warnings.append(f"MC/FDV OOR: {mcap_display_str} (Range: {_format_compact(MIN_MCAP_FOR_ALERT)} - {_format_compact(MAX_MCAP_FOR_ALERT)})")
        if live_trade_data and live_volume_24h < MIN_VOLUME_24H_FOR_ALERT:
            txn_disp = f"({txns_24h} txns)" if txns_24h is not None and txns_24h > 0 else ("(0 txns)" if txns_24h == 0 else "")
            validation_warnings.append(f"Low Vol: {_format_compact(live_volume_24h)} {txn_disp} (Min: {_format_compact(MIN_VOLUME_24H_FOR_ALERT)})")

        report_token_data = all_report_tokens_map.get(token_mint_to_alert, {})
        token_name_for_alert = report_token_data.get("name")
        if not token_name_for_alert or token_name_for_alert.lower() == alert_primary_symbol.lstrip('$').lower():
            token_name_for_alert = alert_primary_symbol.lstrip('$')

        alert_lines = []
        last_action = token_profile["kol_actions"][-1]["action"] if token_profile.get("kol_actions") else "unknown"
        title_emoji = "⚠️" if validation_warnings else ("📈" if last_action == "buy" else "🔥")
        alert_lines.append(f"{title_emoji} *KOL Activity: {escape_markdown_legacy(alert_primary_symbol)}* {title_emoji}")
        if token_name_for_alert and token_name_for_alert.lower() != alert_primary_symbol.lstrip('$').lower():
            alert_lines.append(f"Name: {escape_markdown_legacy(token_name_for_alert)}")
        alert_lines.append(f"CA: `{escape_markdown_legacy(token_mint_to_alert)}`")

        if is_in_report: alert_lines.append(f"_Token found in recent general reports._")
        elif has_been_alerted_on_previously: alert_lines.append(f"_Token previously alerted, continuing to track._")

        alert_lines.append(f"\n*Recent KOL Actions ({len(token_profile['kol_actions'])}):*")
        for action_item in token_profile["kol_actions"][-5:]: # Show last 5 actions
            action_verb = "Bought" if action_item.get('action') == 'buy' else "Sold"
            action_time_str = "N/A"
            if action_item.get('tweet_ts'):
                action_time = datetime.fromtimestamp(action_item['tweet_ts'], tz=timezone.utc)
                action_time_str = action_time.strftime('%b %d, %H:%M UTC')
            
            kol_name_for_score = action_item.get('kol', 'Unknown')
            kol_name_esc = escape_markdown_legacy(kol_name_for_score)
            action_symbol_esc = escape_markdown_legacy(action_item.get('symbol', '?SYM?'))
            
            score_str_display = ""
            kol_perf_metrics = state.get("kol_performance_metrics", {}).get(kol_name_for_score)
            if kol_perf_metrics and \
               isinstance(kol_perf_metrics.get("average_abs_price_change_pct"), Decimal) and \
               kol_perf_metrics.get("signals_processed_for_volatility", 0) >= KOL_MIN_SIGNALS_FOR_SCORE_DISPLAY:
                avg_vol_pct = kol_perf_metrics["average_abs_price_change_pct"]
                score_str_display = f" (Avg |ΔP| {avg_vol_pct:.0f}%)" # Integer percentage

            alert_lines.append(f"  - *{kol_name_esc}*{escape_markdown_legacy(score_str_display)} {action_verb} {action_symbol_esc} ({action_time_str})")

        kols_holding = sorted([kol for kol, act in token_profile.get("latest_known_action_by_kol", {}).items() if act == "buy"])
        kols_sold = sorted([kol for kol, act in token_profile.get("latest_known_action_by_kol", {}).items() if act == "sell"])
        if kols_holding: alert_lines.append(f"KOLs Holding: {escape_markdown_legacy(', '.join(kols_holding[:3]))}{'...' if len(kols_holding) > 3 else ''}")
        if kols_sold: alert_lines.append(f"KOLs Exited: {escape_markdown_legacy(', '.join(kols_sold[:3]))}{'...' if len(kols_sold) > 3 else ''}")

        alert_lines.append(f"\n*Live Metrics:*")
        mc_liq_line = f"💰 MC: {escape_markdown_legacy(mcap_display_str)} | 💧 Liq: {escape_markdown_legacy(_format_compact(live_liquidity))}"
        if live_mcap == Decimal(0) and live_total_supply_str != "N/A" and "(FDV N/A or $0" not in mcap_display_str and "FDV:" not in mcap_display_str:
            mc_liq_line += f" (Supply: {escape_markdown_legacy(live_total_supply_str)})"
        alert_lines.append(mc_liq_line)
        alert_lines.append(f"💲 Price: {escape_markdown_legacy(_format_compact(live_price, '$', 8))}")
        vol_text = escape_markdown_legacy(_format_compact(live_volume_24h))
        txns_text = f"({txns_24h} txns)" if txns_24h is not None and txns_24h > 0 else ("(0 txns)" if txns_24h == 0 and live_volume_24h == Decimal(0) else "")
        alert_lines.append(f"📈 Vol 24h: {vol_text} {escape_markdown_legacy(txns_text)}")

        creation_ts = get_token_creation_timestamp(live_creation_info)
        age_str = calculate_age_string(creation_ts)
        alert_lines.append(f"⏱️ Age: {escape_markdown_legacy(age_str)}")

        price_changes_str_parts = []
        if price_1h_pct_live is not None: price_changes_str_parts.append(f"1h: {_format_percent(price_1h_pct_live)}")
        if price_6h_pct_live is not None: price_changes_str_parts.append(f"6h: {_format_percent(price_6h_pct_live)}")
        if price_24h_pct_live is not None: price_changes_str_parts.append(f"24h: {_format_percent(price_24h_pct_live)}")
        if price_changes_str_parts: alert_lines.append(f"Δ Price: {escape_markdown_legacy(' | '.join(price_changes_str_parts))}")

        if validation_warnings:
            alert_lines.append(f"\n*Validation Warnings:*")
            for warning in validation_warnings: alert_lines.append(f"  - {escape_markdown_legacy(warning)}")

        dex_link = f"https://dexscreener.com/solana/{token_mint_to_alert}"
        gmgn_link = f"https://gmgn.ai/sol/token/{token_mint_to_alert}"
        alert_lines.append(f"\n[{escape_markdown_legacy('DexScreener')}]({dex_link}) | [{escape_markdown_legacy('GMGN')}]({gmgn_link})")

        alert_message = "\n".join(alert_lines)
        alert_sent = False
        if not TELEGRAM_ROUTER_LOAD_ERROR:
             alert_sent = telegram_alert_router.send_alert(
                 alert_message, category=ALERT_CATEGORY_MAIN,
                 parse_mode="Markdown", escape_markdown=False, prefix=None
             )
        else:
             log_message("[!] Cannot send Telegram alert - Router module not loaded.", is_error=True)
             print(f"--- RUNNER KOL ACTIVITY CONSOLE ALERT ---\n{alert_message}\n--------------------")

        if alert_sent:
            token_profile["last_alert_ts_for_this_token"] = current_time_unix
            alerts_this_cycle_count += 1
            state_updated_this_cycle = True 
            log_message(f"    [🎉] Alert sent for KOL activity on {alert_primary_symbol} ({token_mint_to_alert[:6]}) via Telegram Router.")
        else:
            log_message(f"    [✗] Failed to send Telegram alert for {alert_primary_symbol} ({token_mint_to_alert[:6]}) using Telegram Router. Last alert timestamp NOT updated.")

    # 5. Profile & Metrics Cleanup Logic
    profile_cleanup_cutoff_ts = current_time_unix - (TOKEN_ACTIVITY_PROFILE_RETENTION_DAYS * 24 * 3600)
    mints_to_delete_profile = [
        mint for mint, profile_data in token_kol_activity.items()
        if profile_data.get("last_kol_signal_ts", 0) < profile_cleanup_cutoff_ts and \
           profile_data.get("last_alert_ts_for_this_token", 0) < profile_cleanup_cutoff_ts
    ]
    if mints_to_delete_profile:
        deleted_count = 0
        for mint_to_del in mints_to_delete_profile:
            if mint_to_del in token_kol_activity:
                del token_kol_activity[mint_to_del]
                deleted_count +=1
        if deleted_count > 0:
            log_message(f"    [i] Cleaned up {deleted_count} old/inactive token activity profiles from state (older than {TOKEN_ACTIVITY_PROFILE_RETENTION_DAYS} days).")
            state_updated_this_cycle = True
    
    kol_metrics_cleanup_cutoff_ts = current_time_unix - (KOL_PERFORMANCE_METRICS_RETENTION_DAYS * 24 * 3600)
    if "kol_performance_metrics" in state:
        cleaned_pending_checks_count = 0
        for kol_name, metrics in state["kol_performance_metrics"].items():
            original_pending_count = len(metrics.get("pending_volatility_checks", []))
            valid_pending_checks = [
                pc for pc in metrics.get("pending_volatility_checks", [])
                if pc.get("processing_timestamp", 0) >= kol_metrics_cleanup_cutoff_ts
            ]
            if len(valid_pending_checks) < original_pending_count:
                metrics["pending_volatility_checks"] = valid_pending_checks
                cleaned_pending_checks_count += (original_pending_count - len(valid_pending_checks))
                state_updated_this_cycle = True
        if cleaned_pending_checks_count > 0:
            log_message(f"    [i] Cleaned up {cleaned_pending_checks_count} old pending volatility checks (older than {KOL_PERFORMANCE_METRICS_RETENTION_DAYS} days).")

    if state_updated_this_cycle:
        log_message(f"    [*] Saving KOL activity state (updates occurred this cycle).")
        save_kol_activity_state(state)
    else:
         log_message(f"    [*] No state changes occurred this cycle. State not saved.")

    log_message(f"[*] KOL Activity Tracking cycle finished. {alerts_this_cycle_count} alerts sent.")


# --- Main Execution Loop ---
if __name__ == "__main__":
    log_message("--- Starting Runner Analyzer (KOL Activity Tracker v5.6 - Multi-Source Signals & Volatility Score) ---")

    if BIRDEYE_API_LOAD_ERROR:
        startup_error_msg = f"CRITICAL: {BIRDEYE_API_LOAD_ERROR}. The script cannot function without birdeye_api. Exiting."
        log_message(startup_error_msg, is_error=True, to_telegram_error_channel=True)
        if not TELEGRAM_ROUTER_LOAD_ERROR:
             telegram_alert_router.send_alert(
                 f"🚨 Runner Analyzer (KOL) STARTUP FAILED 🚨\n\n{escape_markdown_legacy(BIRDEYE_API_LOAD_ERROR)}\n\nScript will exit.",
                 category=ALERT_CATEGORY_ERROR, parse_mode="Markdown", escape_markdown=False
             )
        exit(1)

    if TELEGRAM_ROUTER_LOAD_ERROR:
         log_message(f"[WARNING] {TELEGRAM_ROUTER_LOAD_ERROR}. Telegram alerts disabled.", is_error=True)
    else:
         log_message("[✓] Telegram Alert Router module loaded. Alerts will be sent if configured.")

    log_message(f"[i] Check interval: {CHECK_INTERVAL_SECONDS}s")
    # <<< CHANGE: Updated log to show all three signal sources >>>
    log_message(f"[i] KOL Signal Source (ai_xtn): {INFLUENCER_SIGNALS_FILE}")
    log_message(f"[i] KOL Signal Source (KOLWallet): {KOL_WALLET_ALERT_SIGNALS_FILE}")
    log_message(f"[i] KOL Signal Source (_soleyes): {SOLEYES_SIGNALS_FILE}")
    log_message(f"[i] KOL Signal Max Age: {KOL_SIGNAL_MAX_AGE_HOURS}h")
    log_message(f"[i] Alert Thresholds: Liq>{_format_compact(MIN_LIQUIDITY_FOR_ALERT)}, Vol>{_format_compact(MIN_VOLUME_24H_FOR_ALERT)}, MC={_format_compact(MIN_MCAP_FOR_ALERT)}-{_format_compact(MAX_MCAP_FOR_ALERT)}")
    log_message(f"[i] Alert Cooldown per Token: {MIN_ALERT_INTERVAL_PER_TOKEN_SECONDS}s")
    log_message(f"[i] Max Alerts per Cycle: {MAX_ALERTS_PER_CYCLE}")
    log_message(f"[i] State Profile Retention: {TOKEN_ACTIVITY_PROFILE_RETENTION_DAYS} days")
    log_message(f"[i] Analyzing latest {NUM_LATEST_REPORTS_TO_ANALYZE} reports.")
    log_message(f"[i] KOL Volatility Window: {KOL_VOLATILITY_WINDOW_SECONDS // 60} minutes")
    log_message(f"[i] KOL Min Signals for Score Display: {KOL_MIN_SIGNALS_FOR_SCORE_DISPLAY}")
    log_message(f"[i] KOL Performance Metrics Pending Check Retention: {KOL_PERFORMANCE_METRICS_RETENTION_DAYS} days")
    log_message(f"[i] Main alerts category: '{ALERT_CATEGORY_MAIN}', Error alerts category: '{ALERT_CATEGORY_ERROR}'")


    while True:
        try:
            current_state = load_kol_activity_state()
            process_kol_activity_and_alert(current_state) 
            log_message(f"Next check in {CHECK_INTERVAL_SECONDS} seconds...")
            time.sleep(CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log_message("Runner Analyzer (KOL Activity) stopped by user.")
            break
        except Exception as e:
            full_traceback = traceback.format_exc()
            log_message(f"Unhandled CRASH in main_loop: {type(e).__name__}: {e}\n{full_traceback}", is_error=True, to_telegram_error_channel=False) 
            
            error_type_safe = escape_markdown_legacy(type(e).__name__)
            error_msg_safe = escape_markdown_legacy(str(e)[:200]) 
            tb_lines = full_traceback.splitlines()
            relevant_tb_line = ""
            if len(tb_lines) > 2 : 
                relevant_tb_line = escape_markdown_legacy(f"...{tb_lines[-3][-80:]}\n...{tb_lines[-2][-80:]}\n...{tb_lines[-1][-80:]}")

            telegram_error_message = (
                f"🚨 *KOL Activity Analyzer CRASH* 🚨\n\n"
                f"Error: `{error_type_safe}: {error_msg_safe}`\n"
                f"Traceback (sample):\n```\n{relevant_tb_line}\n```\n"
                f"The script encountered an unhandled error. Check local logs for full details. Retrying soon."
            )
            if not TELEGRAM_ROUTER_LOAD_ERROR:
                 telegram_alert_router.send_alert(
                     telegram_error_message, category=ALERT_CATEGORY_ERROR,
                     parse_mode="Markdown", escape_markdown=False
                 )
            else:
                 log_message("[!] Cannot send CRASH alert to Telegram - Router module not loaded.", is_error=True)
            
            retry_delay = CHECK_INTERVAL_SECONDS * 2 
            log_message(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

# --- END OF FILE runner_analyzer.py ---