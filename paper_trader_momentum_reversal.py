# --- START OF FILE paper_trader_flexible_dip.py ---

#!/usr/bin/env python3
"""
Paper Trading Simulator - FLEXIBLE DIP BUYING STRATEGY.
Version: 1.0.1 - Corrected priceChangePercent parsing.
Based on Paper Trader Momentum Reversal v1.0.0 logic.
"""

import os
import json
import glob
import time
import requests
import datetime
import traceback
import math
import csv
import hashlib
import urllib.parse
from pathlib import Path
from datetime import timezone, timedelta
from decimal import Decimal, getcontext
from typing import Dict, Any, Optional, List, Tuple, Set
from dotenv import load_dotenv

# --- ENV & IMPORTS ---
load_dotenv()
TELEGRAM_TOKEN_TRADER   = os.getenv("TELEGRAM_BOT_TOKEN_TRADER", os.getenv("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID_TRADER = os.getenv("TELEGRAM_CHAT_ID_TRADER", os.getenv("TELEGRAM_CHAT_ID"))
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")

getcontext().prec = 18 # Standard precision

if not (TELEGRAM_TOKEN_TRADER and TELEGRAM_CHAT_ID_TRADER): print("[!] WARNING: [Flexible Dip] Trader Telegram credentials missing.")
if not BIRDEYE_API_KEY: print("[!] CRITICAL: [Flexible Dip] BIRDEYE_API_KEY missing."); exit()

try: import api_usage_limiter; print("[✓] paper_trader_flexible_dip: Imported api_usage_limiter.")
except ImportError: print("CRITICAL: api_usage_limiter.py not found!"); exit()
try: from helius_analyzer import format_timestamp as default_format_timestamp; print("[✓] paper_trader_flexible_dip: Imported format_timestamp.")
except ImportError:
    print("[!] paper_trader_flexible_dip: Could not import format_timestamp. Using basic internal version.")
    def default_format_timestamp(ts: Optional[Any]) -> Optional[str]:
        if ts is None: return "N/A"
        try: return datetime.datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except: return str(ts)

# --- CONFIG (Flexible Dip Specific) ---
VERBOSE_LOGGING = True # previous value was True (no change for API usage)
REPORTS_ROOT   = os.path.join("reports", "holdings")
PROCESSED_REPORTS_DIR = os.path.join(REPORTS_ROOT, "processed")
TRADER_STATE_FILE   = os.path.join(REPORTS_ROOT, "paper_trader_flexible_dip_state.json")
TRADE_LOG_CSV_FILE = os.path.join(REPORTS_ROOT, "paper_trade_flexible_dip_log.csv")
INITIAL_CAPITAL_USD = Decimal("1000.00") # previous value was Decimal("1000.00")
MAX_OPEN_POSITIONS = 2 # previous value was 5
STOP_LOSS_PCT = Decimal("0.20"); TRAILING_STOP_LOSS_PCT = Decimal("0.15"); DAILY_LOSS_LIMIT_PCT = Decimal("0.10") # previous values were Decimal("0.20"), Decimal("0.15"), Decimal("0.10")
RISK_LEVEL_0_ALLOCATION_PCT = Decimal("0.05"); RISK_LEVEL_0_TP_PCT = Decimal("0.40") # Standard TP
RISK_LEVEL_1_ALLOCATION_PCT = Decimal("0.07"); RISK_LEVEL_1_TP_PCT = Decimal("1.00"); RISK_LEVEL_1_ENTRY_THRESHOLD = Decimal("1.5")
RISK_LEVEL_2_ALLOCATION_PCT = Decimal("0.10"); RISK_LEVEL_2_TP_PCT = Decimal("2.50"); RISK_LEVEL_2_ENTRY_THRESHOLD = Decimal("2.0")
SIMULATED_FEE_PCT = Decimal("0.001"); SIMULATED_SLIPPAGE_PCT = Decimal("0.005") # previous values were Decimal("0.001"), Decimal("0.005")
TRADING_START_HOUR_UTC = 0; TRADING_END_HOUR_UTC = 24 # previous value was 0; 24
STOP_LOSS_COOLDOWN_SECONDS = 15 * 60 # previous value was 15 * 60
KILL_SWITCH_LOSS_PCT = Decimal("0.50"); KILL_SWITCH_DURATION_SECONDS = 2 * 60 * 60 # previous values were Decimal("0.50"), 2 * 60 * 60
PENDING_ENTRY_WINDOW_SECONDS = 5 * 60 # previous value was 3 * 60
PENDING_ENTRY_PRICE_DEVIATION_PCT = Decimal("0.05") # previous value was Decimal("0.05")
NUM_REPORTS_TO_SCAN = 5 # previous value was 10
RECENTLY_ADDED_PENDING_WINDOW_SECONDS = 10 * 60 # previous value was 10 * 60
PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR = Decimal("1.10") # previous value was Decimal("1.10")

# --- General Quality Filters (Loosened for more candidates) ---
FLEX_STRICT_MIN_AGE_MINUTES = 10 # previous value was 5
FLEX_STRICT_MIN_TOTAL_HOLDERS = 50 # previous value was 20
FLEX_STRICT_MAX_TOP1_HOLDER_PCT = 0.65 # previous value was 0.70 # More lenient
FLEX_REQUIRE_SOCIALS = False # previous value was False
FLEX_STRICT_MIN_MONITORED_HOLDERS = 0 # previous value was 0 # Allow tokens not held by our monitored wallets
FLEX_STRICT_MIN_LIQUIDITY_USD = 3000 # previous value was 2000
FLEX_FILTER_MAX_24H_LOSS_PCT = Decimal("-90.0") # previous value was Decimal("-95.0") # Standard rug pull filter

# --- DIP SCENARIOS DEFINITIONS ---
# Each scenario:
#   name: Descriptive name for logging/scoring
#   timeframes_dip: Dict where keys are timeframes ("m5", "h1", "h6", "h24")
#                   and values are (min_dip_pct, max_dip_pct) e.g. (-50.0, -20.0) -> must be between -50% and -20%
#                   ALL conditions in timeframes_dip MUST be met.
#   timeframes_confirmation (optional): Dict for positive confirmation on other timeframes. (min_gain_pct, max_gain_pct)
#   timeframes_avoid_strong_negative (optional): Dict to avoid if other timeframes are *too* negative. (min_allowed_pct, max_allowed_pct) e.g. {"m5": (-100, -5.0)} means m5 should not be worse than -5%
#   priority: Integer (lower is higher priority if multiple scenarios match)
#   score_bonus: Decimal score bonus for matching this scenario
DIP_SCENARIOS = [
    {
        "name": "Deep 6H Dip, Recent Bounce",
        "timeframes_dip": {"h6": (-70.0, -25.0)},
        "timeframes_confirmation": {"m5": (1.0, 100.0)}, # Must have a 5m bounce
        "timeframes_avoid_strong_negative": {"h1": (-100.0, -20.0)}, # 1H should not be actively crashing hard
        "priority": 1, "score_bonus": Decimal("2.5")
    },
    {
        "name": "Sharp 1H Drop, Stabilizing",
        "timeframes_dip": {"h1": (-50.0, -15.0)},
        "timeframes_confirmation": {"m5": (0.5, 100.0)},
        "timeframes_avoid_strong_negative": {"h6": (-100.0, -10.0)}, # 6H shouldn't be in total freefall
        "priority": 2, "score_bonus": Decimal("2.0")
    },
    {
        "name": "Multi-TF Pullback (24H, 6H Dip)",
        "timeframes_dip": {"h24": (-60.0, -10.0), "h6": (-40.0, -5.0)},
        # No specific bounce required, just a general sustained dip
        "priority": 3, "score_bonus": Decimal("1.5")
    },
    {
        "name": "Minor 1H Pullback in Uptrend",
        "timeframes_dip": {"h1": (-25.0, -5.0)},
        "timeframes_confirmation": {"h6": (5.0, 200.0)}, # 6H should be positive
        "priority": 4, "score_bonus": Decimal("1.0")
    },
    {
        "name": "Quick M5 Dip", # For very short-term dips
        "timeframes_dip": {"m5": (-30.0, -5.0)},
        "timeframes_avoid_strong_negative": {"h1": (-100.0, -10.0)}, # H1 shouldn't be crashing
        "priority": 5, "score_bonus": Decimal("0.75")
    }
]

# --- Scoring Weights (Flexible Dip Specific) ---
SCORE_WEIGHTS = {
    # Scenario bonus is added separately
    "liquidity": 1.0,                 # General liquidity score
    "vol_liq_ratio": 0.8,
    "price_change_positive_short_term": 0.5, # Small bonus for m5/h1 if positive after dip
    "total_holders": 0.4,
    "monitored_holders": 0.5,         # Lowered as general filter is also lower
    "top1_holder_pct": -0.7,
    "age_score": 0.3,
    "socials": 0.1                    # Very low weight as not required
}
# --- Timing & API Endpoints (Standard) ---
CHECK_INTERVAL_SECONDS = 60 # previous value was 30
SUMMARY_INTERVAL_4HR_SECONDS = 4 * 60 * 60 # previous value was 4 * 60 * 60
SUMMARY_INTERVAL_DAILY_SECONDS = 24 * 60 * 60 # previous value was 24 * 60 * 60
BDS_BASE_V3_URL = "https://public-api.birdeye.so/defi/v3/token"; BDS_BASE_LEGACY_URL = "https://public-api.birdeye.so/defi"; BDS_MARKET_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/market-data"; BDS_TRADE_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/trade-data/single"; BDS_METADATA_MULTIPLE_ENDPOINT = f"{BDS_BASE_V3_URL}/meta-data/multiple"; BDS_TOKEN_SECURITY_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/token_security"

os.makedirs(REPORTS_ROOT, exist_ok=True); os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)

# --- UTILS (Mostly Standard) ---
def escape_markdown_original(text: Optional[Any]) -> str:
    if text is None: return ''
    if not isinstance(text, str): text = str(text)
    escape_chars = r'_*`['
    for char_to_escape in escape_chars: text = text.replace(char_to_escape, '\\' + char_to_escape)
    return text

def send_trader_telegram(text: str) -> bool:
    """ Sends telegram message with [Flexible Dip] prefix as PLAIN TEXT """
    token = TELEGRAM_TOKEN_TRADER; chat_id = TELEGRAM_CHAT_ID_TRADER
    if not token or not chat_id: print(f"--- FLEXIBLE DIP CONSOLE ---\n{text}\n--------------------"); return True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": f"💧 Flexible Dip 💧\n\n{text}", "disable_web_page_preview": "true"}
    try: r = requests.post(url, data=payload, timeout=20); r.raise_for_status(); return True
    except requests.exceptions.HTTPError as e: error_content = e.response.text[:500] if e.response else str(e); print(f"    [✗] [Flexible Dip] Trader Telegram HTTP error: {e.response.status_code if e.response else 'N/A'} - {error_content}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False
    except Exception as e: print(f"    [✗] [Flexible Dip] Trader Telegram general error: {e}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False

# (load/save json, calculate_percent_change, get_latest_report_paths - standard, with updated prefixes)
def load_json_data(path: str, is_state_file: bool = False) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if is_state_file: print(f"    [i] [Flexible Dip] State file not found: {path}.")
        else: print(f"    [!] [Flexible Dip] Data file not found: {path}")
    except json.JSONDecodeError as e: print(f"    [!] [Flexible Dip] Failed to decode JSON from {path}: {e}")
    except Exception as e: print(f"    [!] [Flexible Dip] Failed to load {path}: {e}")
    return {}
def save_json_data(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: print(f"    [✗] [Flexible Dip] Failed to save data to {path}: {e}")
def load_trader_state() -> Dict[str, Any]: return load_json_data(TRADER_STATE_FILE, is_state_file=True)
def save_trader_state(state: Dict[str, Any]): save_json_data(TRADER_STATE_FILE, state)
def calculate_percent_change(old: Optional[Any], new: Optional[Any]) -> Optional[float]:
    if old is None or new is None: return None;
    try: old_f = float(old); new_f = float(new)
    except (ValueError, TypeError): return None
    if old_f == 0: return float('inf') if new_f > 0 else (float('-inf') if new_f < 0 else 0.0)
    try: return (new_f - old_f) / abs(old_f)
    except Exception: return None
def get_latest_report_paths(n: int = 10) -> List[str]:
    processed_full_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_report_*.json")
    processed_delta_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_delta_*.json")
    all_files = glob.glob(processed_full_pattern) + glob.glob(processed_delta_pattern)
    if not all_files: return []
    try: all_files.sort(key=os.path.getmtime, reverse=True); return all_files[:n]
    except Exception as e: print(f"    [!] [Flexible Dip] Error finding latest report paths: {e}"); return []

def _format_compact(value: Optional[Any], prefix: str = "$", precision: int = 1) -> str: # Standard
    if value is None or value == "N/A": return "N/A";
    try:
        val = float(value)
        if abs(val) >= 1_000_000_000: return f"{prefix}{val/1_000_000_000:.{precision}f}B"
        if abs(val) >= 1_000_000: return f"{prefix}{val/1_000_000:.{precision}f}M"
        if abs(val) >= 1_000: return f"{prefix}{val/1_000:.{precision}f}K"
        formatted_num = f"{val:,.{precision}f}" if precision >= 0 else f"{val:,.0f}"
        return f"{prefix}{formatted_num}"
    except (ValueError, TypeError): return str(value)
def _format_pct(value: Optional[Any]) -> str: # Standard
    if value is None or value == "N/A": return "N/A";
    try: return f"{float(value):+.1f}%"
    except (ValueError, TypeError): return str(value)

# (API Call functions: get_current_price, pt_get_bds_..., get_enriched_data... - standard, with updated prefixes and calling_script)
def get_current_price(mint_address: str) -> Optional[Decimal]: # Updated calling_script
    if not mint_address or not BIRDEYE_API_KEY: return None
    price_cache_ttl = 45; cache_key = f"birdeye_price_{mint_address}"; url = f"https://public-api.birdeye.so/defi/price?address={mint_address}"; headers = {"X-API-KEY": BIRDEYE_API_KEY}
    data = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="price", calling_script="paper_trader_flex_dip.get_current_price", cache_key=cache_key, cache_ttl_seconds=price_cache_ttl, url=url, headers=headers, timeout=10)
    if isinstance(data, dict) and data.get("success") == True and isinstance(data.get("data"), dict):
        price_value = data["data"].get("value");
        if price_value is not None:
            try: return Decimal(str(price_value))
            except Exception as e: print(f"    [!] [Flexible Dip] Error converting price {price_value} to Decimal for {mint_address}: {e}"); return None
    elif isinstance(data, dict) and data.get("success") == False:
         msg = data.get('message', 'Unknown error')
         if "Not found" not in msg and "Request failed" not in msg: print(f"    [!] [Flexible Dip] Birdeye price API failed for {mint_address}: {msg}")
    return None

def pt_get_bds_market_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_MARKET_DATA_ENDPOINT}?address={mint}&chain={chain}"
    cache_key = f"bds_v3_market_{mint}_{chain}"
    market_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_market_data", calling_script="paper_trader_flex_dip.pt_get_bds_market_data", cache_key=cache_key, cache_ttl_seconds=45, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(market_data_response, dict) and market_data_response.get("success"): data_payload = market_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_trade_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TRADE_DATA_ENDPOINT}?address={mint}&chain={chain}"
    cache_key = f"bds_v3_trade_{mint}_{chain}"
    trade_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_trade_data", calling_script="paper_trader_flex_dip.pt_get_bds_trade_data", cache_key=cache_key, cache_ttl_seconds=60, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(trade_data_response, dict) and trade_data_response.get("success"): data_payload = trade_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_metadata_single(mint: str, chain: str = "solana") -> Optional[Dict]:
    mint_list = [mint]; mints_param_value = urllib.parse.quote(",".join(mint_list)); cache_key_hash = hashlib.sha256(mints_param_value.encode()).hexdigest(); cache_key = f"bds_v3_meta_multi_{chain}_{cache_key_hash}"
    url = f"{BDS_METADATA_MULTIPLE_ENDPOINT}?list_address={mints_param_value}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    metadata_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_metadata_multiple", calling_script="paper_trader_flex_dip.pt_get_bds_metadata_single", cache_key=cache_key, cache_ttl_seconds=60*60, url=url, headers=headers)
    if isinstance(metadata_response, dict) and metadata_response.get("success"): data_payload = metadata_response.get("data"); return data_payload.get(mint) if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_token_security(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TOKEN_SECURITY_ENDPOINT}?address={mint}"; cache_key = f"bds_security_{mint}_{chain}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    security_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="token_security", calling_script="paper_trader_flex_dip.pt_get_bds_token_security", cache_key=cache_key, cache_ttl_seconds=5*60*60, url=url, headers=headers)
    if isinstance(security_response, dict) and security_response.get("success"): data_payload = security_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None

def get_enriched_data_for_pending_candidate(mint_address: str) -> Optional[Dict]:
    if VERBOSE_LOGGING: print(f"        [FD Verbose] Fetching enriched data for {mint_address[:6]}...")
    enriched_data = {"token_mint": mint_address, "bds_metadata": None, "bds_market_data": None, "bds_holder_data": None, "bds_trade_data": None, "bds_security_data": None, "gmgn_security_info": None, "original_correlation_data": {"token_mint": mint_address}}
    enriched_data["bds_metadata"] = pt_get_bds_metadata_single(mint_address); time.sleep(0.05)
    enriched_data["bds_market_data"] = pt_get_bds_market_data(mint_address); time.sleep(0.05)
    enriched_data["bds_trade_data"] = pt_get_bds_trade_data(mint_address); time.sleep(0.05)
    enriched_data["bds_security_data"] = pt_get_bds_token_security(mint_address); time.sleep(0.05)
    if not enriched_data["bds_market_data"] or not enriched_data["bds_trade_data"] or not enriched_data["bds_security_data"]:
        if VERBOSE_LOGGING: print(f"        [FD Verbose] Failed essential enriched data fetch for {mint_address[:6]}. Market={bool(enriched_data['bds_market_data'])}, Trade={bool(enriched_data['bds_trade_data'])}, Security={bool(enriched_data['bds_security_data'])}")
        print(f"        [!] [Flexible Dip] Failed to fetch essential enriched data for {mint_address[:6]}."); return None
    if enriched_data["bds_metadata"]: enriched_data["symbol"] = enriched_data["bds_metadata"].get("symbol", "N/A"); enriched_data["name"] = enriched_data["bds_metadata"].get("name", "N/A")
    else: enriched_data["symbol"] = "N/A"; enriched_data["name"] = "N/A"
    if VERBOSE_LOGGING: print(f"        [FD Verbose] Enriched data fetched for {mint_address[:6]}. Symbol: {enriched_data['symbol']}")
    print(f"        [✓] [Flexible Dip] Enriched data snapshot fetched for {mint_address[:6]}."); return enriched_data

CSV_HEADER = ["exit_timestamp", "mint", "symbol", "entry_timestamp", "entry_price", "exit_price", "position_size_usd", "position_size_tokens", "pnl_usd", "pnl_pct", "exit_reason", "risk_level", "dip_scenario"]
def log_trade_to_csv(trade_data: Dict[str, Any]): # Added dip_scenario
    file_exists = Path(TRADE_LOG_CSV_FILE).is_file()
    try:
        with open(TRADE_LOG_CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADER)
            if not file_exists: writer.writeheader()
            row = {
                "exit_timestamp": datetime.datetime.fromtimestamp(trade_data["exit_timestamp"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "mint": trade_data["mint"], "symbol": trade_data["symbol"],
                "entry_timestamp": datetime.datetime.fromtimestamp(trade_data["entry_timestamp"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "entry_price": trade_data["entry_price"], "exit_price": trade_data["exit_price"],
                "position_size_usd": trade_data["position_size_usd"], "position_size_tokens": trade_data["position_size_tokens"],
                "pnl_usd": trade_data["pnl_usd"],
                "pnl_pct": f"{(Decimal(trade_data['pnl_usd']) / Decimal(trade_data['position_size_usd']) * 100 if Decimal(trade_data['position_size_usd']) > 0 else 0):.2f}",
                "exit_reason": trade_data["exit_reason"], "risk_level": trade_data["risk_level_at_entry"],
                "dip_scenario": trade_data.get("dip_scenario_at_entry", "N/A") # New field
            }
            writer.writerow(row)
    except IOError as e: print(f"    [✗] [Flexible Dip] Error writing to trade log CSV: {e}")
    except Exception as e_csv: print(f"    [✗] [Flexible Dip] Unexpected error logging trade to CSV: {e_csv}")

def _calculate_metrics_from_trades(trades_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    default_metrics = {"total_trades": 0, "win_rate_pct": "0.0", "avg_win_pct": "0.0", "avg_loss_pct": "0.0", "total_realized_pnl_usd_for_list": Decimal("0.0")}
    if not trades_list: return default_metrics
    processed_trades_count = 0; wins = 0; losses = 0; win_pnl_pct_sum = Decimal("0.0"); loss_pnl_pct_sum = Decimal("0.0"); current_list_pnl_usd = Decimal("0.0")
    for trade in trades_list:
        try:
            pnl_usd = Decimal(str(trade['pnl_usd'])); entry_usd = Decimal(str(trade['position_size_usd']))
            if entry_usd == Decimal("0"): continue
            processed_trades_count += 1; current_list_pnl_usd += pnl_usd; pnl_pct_for_trade = (pnl_usd / entry_usd) * 100
            if pnl_usd > 0: wins += 1; win_pnl_pct_sum += pnl_pct_for_trade
            else: losses += 1; loss_pnl_pct_sum += pnl_pct_for_trade
        except (TypeError, ValueError) as ve: print(f"    [!] [Flexible Dip] Invalid data for metrics: {ve}. PNL: '{trade.get('pnl_usd')}', Entry: '{trade.get('position_size_usd')}'")
        except Exception as e: print(f"    [!] [Flexible Dip] Error processing trade for metrics: {trade.get('mint')} - {e}")
    win_rate = (wins / processed_trades_count) * 100 if processed_trades_count > 0 else 0; avg_win_pct = float(win_pnl_pct_sum / wins) if wins > 0 else 0.0; avg_loss_pct = float(loss_pnl_pct_sum / losses) if losses > 0 else 0.0
    return {"total_trades": processed_trades_count, "win_rate_pct": f"{win_rate:.1f}", "avg_win_pct": f"{avg_win_pct:.1f}", "avg_loss_pct": f"{avg_loss_pct:.1f}", "total_realized_pnl_usd_for_list": current_list_pnl_usd}
def calculate_performance_metrics(state: Dict[str, Any]):
    history = state.get("trade_history", []); default_metrics = {"total_trades": 0, "win_rate_pct": "0.0", "avg_win_pct": "0.0", "avg_loss_pct": "0.0", "total_realized_pnl_usd": state.get('realized_pnl_usd', "0.0")}
    if not history: state['performance_metrics'] = default_metrics; return
    overall_metrics_cal = _calculate_metrics_from_trades(history)
    state['performance_metrics'] = {"total_trades": overall_metrics_cal['total_trades'], "win_rate_pct": overall_metrics_cal['win_rate_pct'], "avg_win_pct": overall_metrics_cal['avg_win_pct'], "avg_loss_pct": overall_metrics_cal['avg_loss_pct'], "total_realized_pnl_usd": state['realized_pnl_usd']}


# --- Updated Filter Function (Flexible Dip) ---
def passes_strict_filters(item: Dict[str, Any], current_time: int, is_revalidation: bool = False) -> Tuple[bool, Optional[str], Optional[str], Optional[Decimal]]:
    mint_addr = item.get("token_mint", "UNKNOWN_MINT"); symbol = item.get("symbol", "N/A")
    if VERBOSE_LOGGING: print(f"    [FD Filter] START Filtering for {symbol} ({mint_addr[:6]}) (Reval: {is_revalidation})")

    market_data = item.get("bds_market_data") or {}
    trade_data = item.get("bds_trade_data") or {}

    # --- CORRECTED PRICE CHANGE PARSING ---
    price_changes: Dict[str, Decimal] = {}
    timeframe_map = {
        "m1": "price_change_1m_percent",
        "m5": "price_change_5m_percent",
        "m30": "price_change_30m_percent",
        "h1": "price_change_1h_percent",
        "h2": "price_change_2h_percent",
        "h4": "price_change_4h_percent",
        "h6": "price_change_6h_percent",
        "h8": "price_change_8h_percent",
        "h12": "price_change_12h_percent",
        "h24": "price_change_24h_percent",
    }
    for scenario_tf_key, actual_data_key in timeframe_map.items():
        val = trade_data.get(actual_data_key) # This is a percentage like 10.5 for +10.5%
        if val is not None:
            try:
                # Birdeye API returns percentages like 10.5, not fractions like 0.105
                price_changes[scenario_tf_key] = Decimal(str(val))
            except Exception as e:
                if VERBOSE_LOGGING: print(f"        [FD Filter WARN] Could not convert {actual_data_key} value '{val}' to Decimal for {symbol}: {e}")
    # --- END OF CORRECTED PRICE CHANGE PARSING ---

    if VERBOSE_LOGGING and not price_changes and not is_revalidation: # Only warn verbosely if it's initial scan and data is missing
        print(f"        [FD Filter WARN] No Birdeye price change data found in bds_trade_data for {symbol}. Trade data keys: {list(trade_data.keys())}")

    metadata_raw = item.get("bds_metadata"); metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
    bds_security = item.get("bds_security_data") or {}; gmgn_security = item.get("gmgn_security_info") or {}
    original_data = item.get("original_correlation_data", {})

    matched_scenario_name: Optional[str] = None
    scenario_score_bonus: Optional[Decimal] = Decimal("0.0")

    # --- 1. Check Dip Scenarios ---
    possible_matches = []
    for scenario in DIP_SCENARIOS:
        scenario_match = True
        log_scenario_check = f"        [FD Filter Scenario] Checking '{scenario['name']}' for {symbol}:"

        for tf, (min_dip, max_dip) in scenario["timeframes_dip"].items():
            current_pct_change = price_changes.get(tf)
            if current_pct_change is None:
                scenario_match = False
                log_scenario_check += f" TF_{tf}:MISSING;"
                break
            if not (Decimal(str(min_dip)) <= current_pct_change <= Decimal(str(max_dip))): # Compare Decimals
                scenario_match = False
                log_scenario_check += (
                    f" TF_{tf}:{current_pct_change:.1f}% not in ({min_dip}%,{max_dip}%);"
                )
                break
            log_scenario_check += f" TF_{tf}:{current_pct_change:.1f}% OK;"

        if not scenario_match:
            if VERBOSE_LOGGING:
                print(log_scenario_check + " FAILED_DIP_TF")
            continue

        if "timeframes_confirmation" in scenario:
            for tf, (min_gain, max_gain) in scenario["timeframes_confirmation"].items():
                current_pct_change = price_changes.get(tf)
                if current_pct_change is None:
                    scenario_match = False
                    log_scenario_check += f" ConfTF_{tf}:MISSING;"
                    break
                if not (Decimal(str(min_gain)) <= current_pct_change <= Decimal(str(max_gain))): # Compare Decimals
                    scenario_match = False
                    log_scenario_check += (
                        f" ConfTF_{tf}:{current_pct_change:.1f}% not in ({min_gain}%,{max_gain}%);"
                    )
                    break
                log_scenario_check += f" ConfTF_{tf}:{current_pct_change:.1f}% OK;"

        if not scenario_match:
            if VERBOSE_LOGGING:
                print(log_scenario_check + " FAILED_CONF_TF")
            continue

        if "timeframes_avoid_strong_negative" in scenario:
            for tf, (min_allowed, max_allowed) in scenario["timeframes_avoid_strong_negative"].items():
                current_pct_change = price_changes.get(tf)
                if current_pct_change is None:
                    # If data for avoidance check is missing, we might allow it or disallow based on strictness.
                    # For now, let's assume if avoidance data is missing, it doesn't trigger avoidance.
                    log_scenario_check += f" AvoidNTF_{tf}:MISSING (Skipped Avoid);"
                    continue # Or scenario_match = False; break; if very strict

                # Ensure current_pct_change is actually negative before checking against negative avoid range
                # e.g. if min_allowed is -100 and max_allowed is -20, we want to avoid anything MORE negative than -20
                # So, if current_pct_change is -30, it's avoided. If it's -10, it's NOT avoided.
                # The original logic `not (min_allowed <= current_pct_change <= max_allowed)` is correct for this.
                if not (Decimal(str(min_allowed)) <= current_pct_change <= Decimal(str(max_allowed))):
                    scenario_match = False
                    log_scenario_check += (
                        f" AvoidNTF_{tf}:{current_pct_change:.1f}% not in ({min_allowed}%,{max_allowed}%);"
                    )
                    break
                log_scenario_check += f" AvoidNTF_{tf}:{current_pct_change:.1f}% OK;"

        if not scenario_match:
            if VERBOSE_LOGGING:
                print(log_scenario_check + " FAILED_AVOID_NEG_TF")
            continue

        if scenario_match:
            if VERBOSE_LOGGING: print(log_scenario_check + " MATCHED!")
            possible_matches.append(scenario)

    if not possible_matches:
        if VERBOSE_LOGGING: print(f"    [FD Filter Fail] {symbol}: Did not match any DIP_SCENARIOS.")
        return False, "No Dip Scenario Matched", None, None

    possible_matches.sort(key=lambda s: s["priority"])
    best_match_scenario = possible_matches[0]
    matched_scenario_name = best_match_scenario["name"]
    scenario_score_bonus = best_match_scenario["score_bonus"]
    if VERBOSE_LOGGING: print(f"    [FD Filter] {symbol}: Matched Scenario '{matched_scenario_name}' (Bonus: {scenario_score_bonus}). Proceeding to general filters.")
    item['_matched_dip_scenario_name'] = matched_scenario_name
    item['_scenario_score_bonus'] = str(scenario_score_bonus)

    def log_filter_check(param_name, val, condition_op, threshold, passed):
        if VERBOSE_LOGGING: print(f"        [FD Filter Gen] {param_name}: {val} {condition_op} {threshold} -> {'PASS' if passed else 'FAIL'}")

    liquidity_val = market_data.get("liquidity")
    if not isinstance(liquidity_val, (int, float)): log_filter_check("Liquidity", liquidity_val, "is", "Numeric", False); return False, f"Invalid Liquidity Type ({liquidity_val})", matched_scenario_name, scenario_score_bonus
    pass_liq = liquidity_val >= FLEX_STRICT_MIN_LIQUIDITY_USD
    log_filter_check("Liquidity", _format_compact(liquidity_val,'$',0) , ">=", _format_compact(FLEX_STRICT_MIN_LIQUIDITY_USD,'$',0), pass_liq)
    if not pass_liq: return False, f"Low Liquidity ({_format_compact(liquidity_val,'$',0)} < {_format_compact(FLEX_STRICT_MIN_LIQUIDITY_USD,'$',0)})", matched_scenario_name, scenario_score_bonus

    creation_dt = None; age_delta_seconds = None
    raw_creation_ts_sources = [metadata.get("created_timestamp"), bds_security.get("creationTime"), bds_security.get("creation_timestamp_ms")]
    if original_data: raw_creation_ts_sources.insert(0, original_data.get("first_event_timestamp_utc"))
    for ts_source in raw_creation_ts_sources:
         if ts_source is None: continue
         try:
             formatted_ts = default_format_timestamp(ts_source)
             if formatted_ts != "N/A" and formatted_ts != str(ts_source):
                 try: creation_dt = datetime.datetime.strptime(formatted_ts, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc); break
                 except ValueError: continue
         except Exception: continue
    if creation_dt: age_delta_seconds = (datetime.datetime.now(timezone.utc) - creation_dt).total_seconds()
    if age_delta_seconds is None: log_filter_check("Age", "N/A", "is", "Determined", False); return False, "Cannot Determine Age", matched_scenario_name, scenario_score_bonus
    age_minutes = age_delta_seconds / 60
    pass_age = age_minutes >= FLEX_STRICT_MIN_AGE_MINUTES
    log_filter_check("Age (min)", f"{age_minutes:.1f}", ">=", FLEX_STRICT_MIN_AGE_MINUTES, pass_age)
    if not pass_age: return False, f"Too New ({age_minutes:.1f} min < {FLEX_STRICT_MIN_AGE_MINUTES} min)", matched_scenario_name, scenario_score_bonus

    total_holders_bds = trade_data.get("holder")
    if not isinstance(total_holders_bds, int): log_filter_check("Total Holders", total_holders_bds, "is", "Integer", False); return False, f"Invalid Total Holders Type ({total_holders_bds})", matched_scenario_name, scenario_score_bonus
    pass_total_holders = total_holders_bds >= FLEX_STRICT_MIN_TOTAL_HOLDERS
    log_filter_check("Total Holders", total_holders_bds, ">=", FLEX_STRICT_MIN_TOTAL_HOLDERS, pass_total_holders)
    if not pass_total_holders: return False, f"Low Total Holders ({total_holders_bds} < {FLEX_STRICT_MIN_TOTAL_HOLDERS})", matched_scenario_name, scenario_score_bonus

    if (not is_revalidation or (is_revalidation and original_data and "total_holders_in_set" in original_data)) and FLEX_STRICT_MIN_MONITORED_HOLDERS > 0 :
        monitored_holders_count = original_data.get("total_holders_in_set", 0)
        if not isinstance(monitored_holders_count, int): log_filter_check("Monitored Holders", monitored_holders_count, "is", "Integer", False); return False, "Invalid Monitored Holders Type", matched_scenario_name, scenario_score_bonus
        pass_monitored_holders = monitored_holders_count >= FLEX_STRICT_MIN_MONITORED_HOLDERS
        log_filter_check("Monitored Holders", monitored_holders_count, ">=", FLEX_STRICT_MIN_MONITORED_HOLDERS, pass_monitored_holders)
        if not pass_monitored_holders: return False, f"Low Monitored Holders ({monitored_holders_count} < {FLEX_STRICT_MIN_MONITORED_HOLDERS})", matched_scenario_name, scenario_score_bonus
    elif VERBOSE_LOGGING and FLEX_STRICT_MIN_MONITORED_HOLDERS > 0: print(f"        [FD Filter Gen] Monitored Holders: SKIPPED (Reval: {is_revalidation}, Data Present: {bool(original_data and 'total_holders_in_set' in original_data)})")

    top_holder_pct_val = market_data.get("top_holder_pct")
    if top_holder_pct_val is not None:
        if not isinstance(top_holder_pct_val, float): log_filter_check("Top1 Holder Pct", top_holder_pct_val, "is", "Float", False); return False, "Invalid Top1 Holder Pct Type", matched_scenario_name, scenario_score_bonus
        pass_top1 = top_holder_pct_val < FLEX_STRICT_MAX_TOP1_HOLDER_PCT
        log_filter_check("Top1 Holder Pct", f"{top_holder_pct_val*100:.1f}%", "<", f"{FLEX_STRICT_MAX_TOP1_HOLDER_PCT*100:.1f}%", pass_top1)
        if not pass_top1: return False, f"High Top1 Holder Pct ({(top_holder_pct_val*100):.0f}% >= {FLEX_STRICT_MAX_TOP1_HOLDER_PCT*100:.0f}%)", matched_scenario_name, scenario_score_bonus
    elif VERBOSE_LOGGING: print(f"        [FD Filter Gen] Top1 Holder Pct: SKIPPED (None)")

    if FLEX_REQUIRE_SOCIALS:
        extensions_raw = metadata.get("extensions"); extensions = extensions_raw if isinstance(extensions_raw, dict) else {}
        has_social = any(k in extensions for k in ["website", "twitter", "telegram"] if extensions.get(k))
        log_filter_check("Socials", "Present" if has_social else "Missing", "==", "Present", has_social)
        if not has_social: return False, "Missing Social Links", matched_scenario_name, scenario_score_bonus

    price_24h_val = price_changes.get("h24")
    if price_24h_val is not None:
        pass_max_loss = price_24h_val >= FLEX_FILTER_MAX_24H_LOSS_PCT
        log_filter_check("Max 24H Loss", f"{price_24h_val:.1f}%", ">=", f"{FLEX_FILTER_MAX_24H_LOSS_PCT:.1f}%", pass_max_loss)
        if not pass_max_loss: return False, f"Excessive 24h Loss ({price_24h_val:.1f}%)", matched_scenario_name, scenario_score_bonus
    elif VERBOSE_LOGGING: print(f"        [FD Filter Gen] Max 24H Loss: SKIPPED (24h data None for {symbol})")

    if bds_security.get('mutableMetadata') == True: log_filter_check("Mutable Metadata", True, "==", False, False); return False, "Mutable Metadata", matched_scenario_name, scenario_score_bonus
    if VERBOSE_LOGGING: log_filter_check("Mutable Metadata", bds_security.get('mutableMetadata', 'N/A'), "==", False, bds_security.get('mutableMetadata') != True)

    freeze_auth = bds_security.get('freezeAuthority')
    is_freezeable_active = bds_security.get('freezeable') == True and freeze_auth and freeze_auth not in ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
    if is_freezeable_active: log_filter_check("Freeze Authority", "Active", "==", "Inactive", False); return False, "Freeze Authority Active", matched_scenario_name, scenario_score_bonus
    if VERBOSE_LOGGING: log_filter_check("Freeze Authority", "Active" if is_freezeable_active else "Inactive", "==", "Inactive", not is_freezeable_active)

    if gmgn_security and gmgn_security.get('is_honeypot') == True: log_filter_check("GMGN Honeypot", True, "==", False, False); return False, "GMGN Honeypot", matched_scenario_name, scenario_score_bonus
    if VERBOSE_LOGGING: log_filter_check("GMGN Honeypot", gmgn_security.get('is_honeypot', 'N/A'), "==", False, gmgn_security.get('is_honeypot') != True)

    if gmgn_security and gmgn_security.get('is_mintable') == True: log_filter_check("GMGN Mintable", True, "==", False, False); return False, "GMGN Mintable", matched_scenario_name, scenario_score_bonus
    if VERBOSE_LOGGING: log_filter_check("GMGN Mintable", gmgn_security.get('is_mintable', 'N/A'), "==", False, gmgn_security.get('is_mintable') != True)

    if VERBOSE_LOGGING: print(f"    [FD Filter Pass] {symbol} ({mint_addr[:6]}): Passed all filters. Matched Scenario: '{matched_scenario_name}'")
    return True, None, matched_scenario_name, scenario_score_bonus


# --- Updated Ranking Function (Flexible Dip) ---
def rank_trade_candidates(candidate_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scored_candidates = []
    if not candidate_items: return []
    print(f"    [*] [Flexible Dip] Ranking {len(candidate_items)} candidates...")
    for item in candidate_items:
        score = Decimal("0.0"); reasons = []; score_details = {}
        market_data = item.get("bds_market_data") or {}
        trade_data = item.get("bds_trade_data") or {}

        # --- CORRECTED PRICE CHANGE PARSING ---
        price_changes: Dict[str, Decimal] = {}
        timeframe_map = {
            "m1": "price_change_1m_percent",
            "m5": "price_change_5m_percent",
            "m30": "price_change_30m_percent",
            "h1": "price_change_1h_percent",
            "h2": "price_change_2h_percent",
            "h4": "price_change_4h_percent",
            "h6": "price_change_6h_percent",
            "h8": "price_change_8h_percent",
            "h12": "price_change_12h_percent",
            "h24": "price_change_24h_percent",
        }
        for scenario_tf_key, actual_data_key in timeframe_map.items():
            val = trade_data.get(actual_data_key)
            if val is not None:
                try:
                    price_changes[scenario_tf_key] = Decimal(str(val))
                except Exception as e:
                     if VERBOSE_LOGGING: print(f"        [FD Rank WARN] Could not convert {actual_data_key} value '{val}' to Decimal for {item.get('symbol','N/A')}: {e}")
        # --- END OF CORRECTED PRICE CHANGE PARSING ---

        metadata_raw = item.get("bds_metadata"); metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
        original_data = item.get("original_correlation_data", {})

        # Add scenario bonus first
        scenario_bonus_str = item.get('_scenario_score_bonus', "0.0")
        scenario_bonus = Decimal(scenario_bonus_str)
        if scenario_bonus > 0:
            score += scenario_bonus
            reasons.append(f"Scenario:{item.get('_matched_dip_scenario_name', 'Unknown')}(+{scenario_bonus_str})")
            score_details[f"Scenario:{item.get('_matched_dip_scenario_name', 'Unknown')}"] = scenario_bonus

        # Liquidity Score
        liq = market_data.get("liquidity")
        liq_score = Decimal("0.0")
        if isinstance(liq, (int, float)):
            if liq > 50000: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.5")
            elif liq > 10000: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.0")
            elif liq >= FLEX_STRICT_MIN_LIQUIDITY_USD: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("0.5")
        if liq_score != Decimal("0.0"): # Explicit compare with Decimal(0)
            score += liq_score
            reasons.append(f"Liq:{_format_compact(liq,'$',0)}({liq_score:+.2f})")
            score_details["Liquidity"] = liq_score

        # Volume / Liquidity Ratio
        vol24h = trade_data.get("volume24h")
        vol_liq_score = Decimal("0.0")
        ratio_val_display = "N/A"
        if isinstance(liq, (int, float)) and isinstance(vol24h, (int, float)) and liq > 1000:
            liq_dec = Decimal(str(liq)); ratio_val = Decimal(str(vol24h)) / (liq_dec if liq_dec != Decimal(0) else Decimal('inf'))
            ratio_val_display = f"{ratio_val:.1f}x"
            if ratio_val > 10: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.5")
            elif ratio_val > 5: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.0")
            elif ratio_val > 2: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("0.5")
        if vol_liq_score != Decimal("0.0"):
            score += vol_liq_score
            reasons.append(f"V/L:{ratio_val_display}({vol_liq_score:+.2f})")
            score_details["Vol/Liq Ratio"] = vol_liq_score

        # Price Change Positive Short Term (after dip)
        positive_st_score = Decimal("0.0")
        m5_change = price_changes.get("m5")
        h1_change = price_changes.get("h1")
        if m5_change is not None and m5_change > 0:
            positive_st_score += Decimal(str(SCORE_WEIGHTS["price_change_positive_short_term"])) * min(m5_change / Decimal("5.0"), Decimal("1.0"))
        if h1_change is not None and h1_change > 0:
             positive_st_score += Decimal(str(SCORE_WEIGHTS["price_change_positive_short_term"])) * min(h1_change / Decimal("10.0"), Decimal("1.0"))
        if positive_st_score != Decimal("0.0"):
            score += positive_st_score
            reasons.append(f"STPos({positive_st_score:+.2f})")
            score_details["ShortTermPositive"] = positive_st_score

        # Total Holders
        total_holders_val = trade_data.get("holder")
        th_score = Decimal("0.0")
        if isinstance(total_holders_val, int):
            if total_holders_val > 1000: th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("1.0")
            elif total_holders_val > 250: th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("0.5")
            elif total_holders_val >= FLEX_STRICT_MIN_TOTAL_HOLDERS : th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("0.2")
        if th_score != Decimal("0.0"):
            score += th_score
            reasons.append(f"Hdrs:{total_holders_val}({th_score:+.2f})")
            score_details["TotalHolders"] = th_score

        # Monitored Holders
        monitored_holders_val = original_data.get("total_holders_in_set", 0)
        mh_score = Decimal("0.0")
        if FLEX_STRICT_MIN_MONITORED_HOLDERS == 0 or monitored_holders_val >= FLEX_STRICT_MIN_MONITORED_HOLDERS :
            if monitored_holders_val > 2: mh_score = Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("1.0")
            elif monitored_holders_val >= 1 : mh_score = Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("0.5")
        if mh_score != Decimal("0.0"):
            score += mh_score
            reasons.append(f"MonHdrs:{monitored_holders_val}({mh_score:+.2f})")
            score_details["MonitoredHolders"] = mh_score

        # Top1 Holder Pct
        top1_pct_float = market_data.get("top_holder_pct")
        top1_score = Decimal("0.0")
        if isinstance(top1_pct_float, float):
            top1_pct_dec = Decimal(str(top1_pct_float))
            if top1_pct_dec < Decimal("0.30"): top1_score = Decimal(str(abs(float(SCORE_WEIGHTS["top1_holder_pct"])))) * Decimal("0.5")
            elif top1_pct_dec >= Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)):
                penalty_denominator = (Decimal("1.0") - Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)))
                penalty_factor_numerator = (top1_pct_dec - Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)))
                penalty_factor = penalty_factor_numerator / (penalty_denominator if penalty_denominator != Decimal(0) else Decimal('inf'))
                top1_score = Decimal(str(SCORE_WEIGHTS["top1_holder_pct"])) * penalty_factor
        if top1_score != Decimal("0.0"):
            score += top1_score
            reasons.append(f"Top1:{top1_pct_float*100:.0f}%({top1_score:+.2f})")
            score_details["Top1HolderPct"] = top1_score

        # Age Score
        creation_dt = None
        raw_creation_ts_sources = [metadata.get("created_timestamp"), (item.get("bds_security_data", {}) or {}).get("creationTime"), (item.get("bds_security_data", {}) or {}).get("creation_timestamp_ms")]
        if original_data: raw_creation_ts_sources.insert(0, original_data.get("first_event_timestamp_utc"))
        for ts_source in raw_creation_ts_sources:
            if ts_source is None: continue
            try: formatted_ts = default_format_timestamp(ts_source)
            except Exception: continue
            if formatted_ts != "N/A" and formatted_ts != str(ts_source):
                try: creation_dt = datetime.datetime.strptime(formatted_ts, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc); break
                except ValueError: continue
        age_score_val = Decimal("0.0")
        if creation_dt:
            age_delta_seconds = (datetime.datetime.now(timezone.utc) - creation_dt).total_seconds()
            if age_delta_seconds >= (60 * 60) and age_delta_seconds < (24 * 60 * 60) : age_score_val = Decimal(str(SCORE_WEIGHTS["age_score"])) * Decimal("1.0")
            elif age_delta_seconds >= (FLEX_STRICT_MIN_AGE_MINUTES * 60): age_score_val = Decimal(str(SCORE_WEIGHTS["age_score"])) * Decimal("0.5")
        if age_score_val !=Decimal("0.0"):
            score += age_score_val
            reasons.append(f"Age({age_score_val:+.2f})")
            score_details["AgeScore"] = age_score_val

        # Socials Score
        extensions_raw = metadata.get("extensions"); extensions = extensions_raw if isinstance(extensions_raw, dict) else {}
        has_social = any(k in extensions for k in ["website", "twitter", "telegram"] if extensions.get(k))
        socials_score = Decimal("0.0")
        if has_social: socials_score = Decimal(str(SCORE_WEIGHTS["socials"]))
        if socials_score != Decimal("0.0"):
            score += socials_score
            reasons.append(f"Socials({socials_score:+.2f})")
            score_details["SocialsScore"] = socials_score

        item['_score'] = score; item['_score_reasons'] = reasons; item['_score_details'] = score_details
        scored_candidates.append(item)

    scored_candidates.sort(key=lambda x: x['_score'], reverse=True)
    print(f"    [*] [Flexible Dip] Ranking Complete. Top {min(3, len(scored_candidates))} candidates:")
    for i, item_ranked in enumerate(scored_candidates[:3]):
        print(f"        {i+1}. {item_ranked.get('symbol','???')} ({item_ranked.get('token_mint')[:6]}...) Score: {item_ranked['_score']:.2f} (Reasons: {', '.join(item_ranked.get('_score_reasons',[]))})")
        if VERBOSE_LOGGING and '_score_details' in item_ranked:
            detail_str_parts = []
            for k,v_dec in item_ranked['_score_details'].items():
                if isinstance(v_dec, Decimal):
                    v_fmt = f"{v_dec:+.2f}"
                else:
                    v_fmt = str(v_dec)
                detail_str_parts.append(f"{k}:{v_fmt}")
            detail_str = ", ".join(detail_str_parts)
            print(f"           Score Details: {detail_str}")
    return scored_candidates

def enter_paper_trade(mint: str, symbol: str, name: str, entry_price: Decimal, state: Dict[str, Any], matched_dip_scenario: Optional[str]) -> bool:
    current_balance = Decimal(state['current_balance_usd']); initial_capital = Decimal(state['initial_capital_usd']); risk_level = state['risk_level']
    if risk_level == 0: allocation_pct = RISK_LEVEL_0_ALLOCATION_PCT; capital_base = initial_capital; tp_pct = RISK_LEVEL_0_TP_PCT
    elif risk_level == 1: allocation_pct = RISK_LEVEL_1_ALLOCATION_PCT; capital_base = current_balance; tp_pct = RISK_LEVEL_1_TP_PCT
    else: allocation_pct = RISK_LEVEL_2_ALLOCATION_PCT; capital_base = current_balance; tp_pct = RISK_LEVEL_2_TP_PCT
    position_size_usd = capital_base * allocation_pct; position_size_usd_after_fee = position_size_usd * (Decimal("1") - SIMULATED_FEE_PCT); simulated_entry_price = entry_price * (Decimal("1") + SIMULATED_SLIPPAGE_PCT)
    if simulated_entry_price <= 0: print(f"    [!] [Flexible Dip] Invalid simulated entry price for {symbol}."); return False
    position_size_tokens = position_size_usd_after_fee / simulated_entry_price; initial_stop_loss_price = simulated_entry_price * (Decimal("1") - STOP_LOSS_PCT); take_profit_price = simulated_entry_price * (Decimal("1") + tp_pct)

    state['open_positions'][mint] = {
        "symbol": symbol, "name": name, "entry_price": str(simulated_entry_price),
        "entry_timestamp": int(time.time()), "position_size_usd": str(position_size_usd),
        "position_size_tokens": str(position_size_tokens), "initial_stop_loss_price": str(initial_stop_loss_price),
        "take_profit_price": str(take_profit_price), "risk_level": risk_level, "status": "OPEN",
        "highest_price_seen": str(simulated_entry_price),
        "dip_scenario_at_entry": matched_dip_scenario
    }
    log_entry_price_fmt = _format_compact(simulated_entry_price, '$', 6); log_sl_fmt = _format_compact(initial_stop_loss_price, '$', 6); log_tp_fmt = _format_compact(take_profit_price, '$', 6)
    print(f"    [+] [Flexible Dip] Entered PAPER TRADE for {symbol} ({mint[:6]}...) Scenario: {matched_dip_scenario}"); print(f"        Size: {position_size_usd:.2f} USD | Entry: ~{log_entry_price_fmt} | SL: {log_sl_fmt} | TP: {log_tp_fmt} | Risk: {risk_level}")
    symbol_safe = escape_markdown_original(symbol); tg_entry_price_fmt = _format_compact(simulated_entry_price, '$', 5); tg_sl_fmt = _format_compact(initial_stop_loss_price, '$', 5); tg_tp_fmt = _format_compact(take_profit_price, '$', 5); size_fmt = f"${position_size_usd:.2f}"
    scenario_text_tg = f"\nScenario: {escape_markdown_original(matched_dip_scenario)}" if matched_dip_scenario else ""
    alert_text = (f"➡️ Entered Paper Trade: *{symbol_safe}*\nEntry Price: {tg_entry_price_fmt}\nSize: {size_fmt}\nSL: {tg_sl_fmt} | TP: {tg_tp_fmt}\nRisk Level: {risk_level}{scenario_text_tg}")
    send_trader_telegram(alert_text)
    return True

def manage_pending_entries(state: Dict[str, Any]):
    if 'pending_entry' not in state or not state['pending_entry']: return
    if VERBOSE_LOGGING: print(f"    [FD Verbose] Managing {len(state['pending_entry'])} pending entries...")
    else: print(f"    [*] [Flexible Dip] Managing {len(state['pending_entry'])} pending entries...")

    mints_to_process = list(state['pending_entry'].keys()); current_time = time.time()
    for mint in mints_to_process:
        if mint not in state['pending_entry']: continue
        pending_data = state['pending_entry'][mint]; symbol = pending_data.get('symbol', mint[:6])
        original_matched_scenario = pending_data.get('matched_dip_scenario_name', 'N/A_pending')

        if VERBOSE_LOGGING: print(f"    [FD Verbose] Checking pending: {symbol} ({mint[:6]}). Scenario: {original_matched_scenario}")

        monitoring_start_time = pending_data.get('monitoring_start_time', 0)
        entry_window_seconds = pending_data.get('entry_window_seconds', PENDING_ENTRY_WINDOW_SECONDS)
        if current_time > monitoring_start_time + entry_window_seconds:
            print(f"    [-] [Flexible Dip] Pending entry window expired for {symbol}. Removing."); del state['pending_entry'][mint]; continue

        op_halt_active = (state['daily_quota_achieved'] or
                          state.get('daily_loss_limit_hit', False) or
                          len(state['open_positions']) >= MAX_OPEN_POSITIONS or
                          current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS or
                          current_time < state.get('kill_switch_active_until_ts', 0))
        if op_halt_active:
            if VERBOSE_LOGGING: print(f"        [FD Verbose] Operational halt active for {symbol}, skipping pending check.");
            continue

        current_price_quick = get_current_price(mint); time.sleep(0.05)
        if current_price_quick is None: print(f"    [!] [Flexible Dip] Could not get quick price for pending entry {symbol}."); continue

        price_condition_met = False; signal_price_str = pending_data.get('signal_price')
        if signal_price_str and signal_price_str != 'N/A':
            try:
                signal_price = Decimal(signal_price_str)
                lower_bound = signal_price * (Decimal(1) - PENDING_ENTRY_PRICE_DEVIATION_PCT)
                upper_bound = signal_price * (Decimal(1) + PENDING_ENTRY_PRICE_DEVIATION_PCT)
                price_condition_met = (lower_bound <= current_price_quick <= upper_bound)
                if VERBOSE_LOGGING: print(f"        [FD Verbose] {symbol} Price Check: QuickPrice={_format_compact(current_price_quick, '$', 6)}, Signal={_format_compact(signal_price, '$', 6)}, Lower={_format_compact(lower_bound, '$', 6)}, Upper={_format_compact(upper_bound, '$', 6)} -> Met: {price_condition_met}")
            except Exception as e: print(f"    [!] [Flexible Dip] Error checking price deviation for {symbol}: {e}"); continue
            if not price_condition_met:
                if VERBOSE_LOGGING: print(f"        [FD Verbose] {symbol} price {_format_compact(current_price_quick, '$', 6)} outside signal deviation.")
                continue
        else:
            price_condition_met = True
            if VERBOSE_LOGGING: print(f"        [FD Verbose] {symbol} no signal price, proceeding to revalidation.")

        if not price_condition_met: continue

        print(f"    [>] [Flexible Dip] Price condition met for {symbol}. Fetching full data for re-validation...")
        fresh_enriched_data = get_enriched_data_for_pending_candidate(mint)
        if not fresh_enriched_data: print(f"    [!] [Flexible Dip] Failed to fetch fresh enriched data for {symbol} for re-validation."); continue

        if 'original_correlation_data' in pending_data:
            fresh_enriched_data['original_correlation_data'] = pending_data['original_correlation_data']

        passes_final_check, reason, reval_matched_scenario, _ = passes_strict_filters(fresh_enriched_data, int(current_time), is_revalidation=True)

        if not passes_final_check:
            print(f"    [!] [Flexible Dip] {symbol} ({mint[:6]}) failed final re-validation: {reason}. Removing from pending."); del state['pending_entry'][mint]; continue

        if VERBOSE_LOGGING and original_matched_scenario != reval_matched_scenario:
            print(f"        [FD Verbose] {symbol} scenario changed on reval: '{original_matched_scenario}' -> '{reval_matched_scenario}'")

        print(f"    [✓] [Flexible Dip] {symbol} ({mint[:6]}) passed final re-validation. Matched Scenario: '{reval_matched_scenario}'")
        current_price_from_market = (fresh_enriched_data.get("bds_market_data") or {}).get("price")
        price_to_enter = current_price_quick
        if current_price_from_market is not None:
            try: price_to_enter = Decimal(str(current_price_from_market))
            except Exception: pass

        trade_name = fresh_enriched_data.get("name", pending_data.get('name', 'N/A'))
        if enter_paper_trade(mint, symbol, trade_name, price_to_enter, state, reval_matched_scenario):
            print(f"    [>>>] [Flexible Dip] Successfully executed paper trade for {symbol}."); del state['pending_entry'][mint]
        else: print(f"    [!] [Flexible Dip] Failed to execute paper trade for {symbol} despite passing re-validation.")

def manage_portfolio(state: Dict[str, Any]):
    if 'open_positions' in state and state['open_positions']:
        if VERBOSE_LOGGING: print(f"    [FD Verbose] Managing {len(state['open_positions'])} open positions...")
        else: print(f"    [*] [Flexible Dip] Managing {len(state['open_positions'])} open positions...")
    else: return
    mints_to_check = list(state['open_positions'].keys()); closed_positions_this_cycle = 0; current_time = int(time.time())
    for mint in mints_to_check:
        position = state['open_positions'].get(mint)
        if not position or position.get('status') != 'OPEN': continue
        symbol = position.get('symbol', mint[:6]); symbol_safe = escape_markdown_original(symbol); mint_short_safe = escape_markdown_original(mint[:4]+"..")
        dip_scenario_at_entry = position.get("dip_scenario_at_entry", "N/A_open")
        current_price = get_current_price(mint); time.sleep(0.1)
        if current_price is None: print(f"    [!] [Flexible Dip] Could not get current price for open position {symbol}. Skipping checks."); continue
        entry_price = Decimal(position['entry_price']); initial_stop_loss_price = Decimal(position['initial_stop_loss_price']); take_profit_price = Decimal(position['take_profit_price'])
        position_size_tokens = Decimal(position['position_size_tokens']); position_size_usd_entry = Decimal(position['position_size_usd']); entry_timestamp = position['entry_timestamp']
        highest_price_seen = Decimal(position.get('highest_price_seen', position['entry_price'])); highest_price_seen = max(highest_price_seen, current_price); position['highest_price_seen'] = str(highest_price_seen)
        trailing_stop_price = highest_price_seen * (Decimal("1") - TRAILING_STOP_LOSS_PCT); effective_stop_loss_price = max(initial_stop_loss_price, trailing_stop_price); position['effective_stop_loss_price'] = str(effective_stop_loss_price)
        exit_reason = None; exit_price = None; current_price_fmt_log = _format_compact(current_price, '$', 6); effective_sl_fmt_log = _format_compact(effective_stop_loss_price, '$', 6); tp_price_fmt_log = _format_compact(take_profit_price, '$', 6)
        if current_price <= effective_stop_loss_price: exit_reason = "STOPPED_OUT"; exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT); print(f"    [!] [Flexible Dip] STOP LOSS for {symbol} at {current_price_fmt_log} (SL: {effective_sl_fmt_log})"); state['last_stop_loss_timestamp'] = current_time
        elif current_price >= take_profit_price: exit_reason = "TAKE_PROFIT"; exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT); print(f"    [+] [Flexible Dip] TAKE PROFIT for {symbol} at {current_price_fmt_log} (TP: {tp_price_fmt_log})")
        if exit_reason and exit_price is not None:
            closed_positions_this_cycle += 1; exit_value_usd = position_size_tokens * exit_price; exit_value_usd_after_fee = exit_value_usd * (Decimal("1") - SIMULATED_FEE_PCT); pnl_usd = exit_value_usd_after_fee - position_size_usd_entry; pnl_pct = (pnl_usd / position_size_usd_entry) * 100 if position_size_usd_entry > 0 else Decimal("0")
            state['current_balance_usd'] = str(Decimal(state['current_balance_usd']) + pnl_usd); state['realized_pnl_usd'] = str(Decimal(state['realized_pnl_usd']) + pnl_usd)
            pnl_usd_fmt = f"{pnl_usd:+.2f}"; pnl_pct_fmt = f"{pnl_pct:+.1f}"; entry_price_fmt = _format_compact(entry_price, '$', 6); exit_price_fmt = _format_compact(exit_price, '$', 6); portfolio_fmt = _format_compact(state['current_balance_usd'], '$', 2)
            if exit_reason == "STOPPED_OUT" and abs(pnl_pct / 100) >= KILL_SWITCH_LOSS_PCT:
                 kill_switch_until = current_time + KILL_SWITCH_DURATION_SECONDS; state['kill_switch_active_until_ts'] = kill_switch_until; kill_switch_msg = f"🚨 *[Flexible Dip] KILL SWITCH!* 🚨\nTrade: {symbol_safe}\nLoss: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\nHalting for {KILL_SWITCH_DURATION_SECONDS // 60} mins."; print(f"    [!!!] [Flexible Dip] KILL SWITCH by {symbol} loss ({pnl_pct:.1f}%)."); send_trader_telegram(kill_switch_msg)
            closed_trade_data = {
                "mint": mint, "symbol": symbol, "entry_timestamp": entry_timestamp, "exit_timestamp": current_time,
                "entry_price": str(entry_price), "exit_price": str(exit_price),
                "position_size_usd": str(position_size_usd_entry), "position_size_tokens": str(position_size_tokens),
                "pnl_usd": str(pnl_usd), "exit_reason": exit_reason, "risk_level_at_entry": position.get("risk_level"),
                "pnl_pct": f"{pnl_pct:.2f}", "dip_scenario_at_entry": dip_scenario_at_entry
            }
            if 'trade_history' not in state: state['trade_history'] = []
            state['trade_history'].append(closed_trade_data); log_trade_to_csv(closed_trade_data); del state['open_positions'][mint]
            scenario_text_tg_closed = f"\nScenario: {escape_markdown_original(dip_scenario_at_entry)}" if dip_scenario_at_entry and dip_scenario_at_entry != "N/A_open" else ""
            alert_text = (f"Closed {symbol_safe} ({mint_short_safe})\nReason: {exit_reason}\nPnL: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\nEntry: {entry_price_fmt} | Exit: {exit_price_fmt}{scenario_text_tg_closed}\nPortfolio: {portfolio_fmt} USD")
            send_trader_telegram(alert_text)
            if pnl_usd > 0: check_and_update_risk_level(state)
    if closed_positions_this_cycle > 0: print(f"    [*] [Flexible Dip] Closed {closed_positions_this_cycle} position(s) this cycle."); calculate_performance_metrics(state)

def check_and_update_risk_level(state: Dict[str, Any]):
    current_balance = Decimal(state['current_balance_usd']); initial_capital = Decimal(state['initial_capital_usd'])
    current_risk_level = state['risk_level']; daily_quota_achieved = state['daily_quota_achieved']; new_risk_level = current_risk_level; new_quota_status = daily_quota_achieved
    balance_fmt = _format_compact(current_balance, '$', 2)
    if current_risk_level == 0 and current_balance >= initial_capital * RISK_LEVEL_1_ENTRY_THRESHOLD:
        new_risk_level = 1; msg = f"🚀 Risk Level Increased: 0 -> 1\nBalance: {balance_fmt} USD"; print(f"    [!] [Flexible Dip] Risk Level Increased: 0 -> 1"); send_trader_telegram(msg)
    if current_risk_level == 1 and current_balance >= initial_capital * RISK_LEVEL_2_ENTRY_THRESHOLD:
        new_risk_level = 2; new_quota_status = True; msg = f"🏆 Daily Quota Achieved! 🏆\nRisk Level: 1 -> 2\nBalance: {balance_fmt} USD\nNo new trades today."; print(f"    [!] [Flexible Dip] DAILY QUOTA ACHIEVED! Risk Level: 1 -> 2"); send_trader_telegram(msg)
    state['risk_level'] = new_risk_level; state['daily_quota_achieved'] = new_quota_status
def generate_and_send_performance_summary_md_original(state: Dict[str, Any], period_hours: int, period_name: str, current_time: int):
    since_timestamp = current_time - (period_hours * 60 * 60); history = state.get("trade_history", []); period_trades = [t for t in history if t.get("exit_timestamp") is not None and int(t["exit_timestamp"]) >= since_timestamp]
    summary_msg = f"📊 *{period_name} Performance Summary* 📊\n\n"; summary_msg += f"Period: Last {period_hours} hours\n"
    if not period_trades: print(f"    [i] [Flexible Dip] No trades in the last {period_hours} hours."); summary_msg += f"No trades closed in this period.\n\n"
    else:
        period_metrics_cal = _calculate_metrics_from_trades(period_trades); pnl_period_fmt = f"{period_metrics_cal['total_realized_pnl_usd_for_list']:.2f}"; wr_period_fmt = period_metrics_cal['win_rate_pct']; avg_win_fmt = period_metrics_cal['avg_win_pct']; avg_loss_fmt = period_metrics_cal['avg_loss_pct']
        summary_msg += f"Trades Closed: {period_metrics_cal['total_trades']}\n"; summary_msg += f"Realized PnL (Period): {pnl_period_fmt} USD\n"; summary_msg += f"Win Rate (Period): {wr_period_fmt}%\n"; summary_msg += f"Avg Win (Period): {avg_win_fmt}%\n"; summary_msg += f"Avg Loss (Period): {avg_loss_fmt}%\n\n"
    summary_msg += f"---Portfolio Status---\n"; current_balance_fmt = _format_compact(state['current_balance_usd'], '$', 2); total_pnl_fmt = f"{Decimal(state.get('performance_metrics', {}).get('total_realized_pnl_usd', '0.00')):.2f}"
    summary_msg += f"Current Balance: {current_balance_fmt} USD\n"; summary_msg += f"Total Realized PnL (All Time): {total_pnl_fmt} USD"
    send_trader_telegram(summary_msg)
    if period_hours == 4: state['last_4hr_summary_ts'] = current_time
    elif period_hours == 24: state['last_daily_summary_ts'] = current_time
    save_trader_state(state)


# --- Main Loop (Flexible Dip) ---
def main_trading_loop():
    start_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"--- Starting Paper Trader [FLEXIBLE DIP] (v1.0.1) ({start_time_str}) ---") # Version Update
    print(f"Processed Reports Dir: {PROCESSED_REPORTS_DIR}; State File: {TRADER_STATE_FILE}")
    print(f"VERBOSE LOGGING: {VERBOSE_LOGGING}")
    print(f"Dip Scenarios Count: {len(DIP_SCENARIOS)}")
    if VERBOSE_LOGGING:
        for i, scn in enumerate(DIP_SCENARIOS): print(f"  Scenario {i+1}: {scn['name']} (Priority: {scn['priority']}, Bonus: {scn['score_bonus']})")
    print(f"General Filters: Age>={FLEX_STRICT_MIN_AGE_MINUTES}m, Liq>={_format_compact(FLEX_STRICT_MIN_LIQUIDITY_USD,'$',0)}, Holders>={FLEX_STRICT_MIN_TOTAL_HOLDERS}, Top1<{FLEX_STRICT_MAX_TOP1_HOLDER_PCT*100:.1f}%") # Adjusted Top1 format

    state = load_trader_state()
    if not state:
        print("[*] [Flexible Dip] Initializing new trader state.")
        state = {"initial_capital_usd": str(INITIAL_CAPITAL_USD),"current_balance_usd": str(INITIAL_CAPITAL_USD),"capital_at_quota_start": str(INITIAL_CAPITAL_USD),"realized_pnl_usd": "0.0","risk_level": 0,"daily_quota_achieved": False,"last_quota_reset_timestamp": int(time.time()),"open_positions": {},"trade_history": [],"last_stop_loss_timestamp": 0,"kill_switch_active_until_ts": 0,"pending_entry": {},"performance_metrics": {},"recently_pending": {},"last_4hr_summary_ts": 0,"last_daily_summary_ts": 0,"prev_day_total_realized_pnl_at_reset": "0.0"}
        calculate_performance_metrics(state); save_trader_state(state)
    else: # Ensure all keys exist
        state.setdefault("initial_capital_usd", str(INITIAL_CAPITAL_USD)); state.setdefault("current_balance_usd", state.get("initial_capital_usd", str(INITIAL_CAPITAL_USD))); state.setdefault("capital_at_quota_start", state.get("current_balance_usd", str(INITIAL_CAPITAL_USD))); state.setdefault("realized_pnl_usd", "0.0"); state.setdefault("risk_level", 0); state.setdefault("daily_quota_achieved", False); state.setdefault("last_quota_reset_timestamp", 0); state.setdefault("open_positions", {}); state.setdefault("trade_history", []); state.setdefault("last_stop_loss_timestamp", 0); state.setdefault("kill_switch_active_until_ts", 0); state.setdefault("pending_entry", {}); state.setdefault("performance_metrics", {}); state.setdefault("recently_pending", {}); state.setdefault("last_4hr_summary_ts", 0); state.setdefault("last_daily_summary_ts", 0); state.setdefault("prev_day_total_realized_pnl_at_reset", "0.0")
        state['initial_capital_usd'] = str(Decimal(state['initial_capital_usd'])); state['current_balance_usd'] = str(Decimal(state['current_balance_usd'])); state['capital_at_quota_start'] = str(Decimal(state['capital_at_quota_start'])); state['realized_pnl_usd'] = str(Decimal(state['realized_pnl_usd']))
        if not state.get('performance_metrics') or 'total_realized_pnl_usd' not in state['performance_metrics']: calculate_performance_metrics(state)

    print(f"[*] [Flexible Dip] Current State: Balance=${Decimal(state['current_balance_usd']):.2f}, Risk={state['risk_level']}, Quota Met={state['daily_quota_achieved']}")
    print(f"[*] [Flexible Dip] Tracking {len(state.get('open_positions', {}))} open, {len(state.get('pending_entry', {}))} pending.")
    last_report_scan_time = 0

    while True:
        current_time = int(time.time()); current_dt_utc = datetime.datetime.now(timezone.utc); current_hour_utc = current_dt_utc.hour
        print(f"\n--- [Flexible Dip] Loop Start: {default_format_timestamp(current_time)} ---")
        try:
            today_start_utc = current_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0); today_start_ts = int(today_start_utc.timestamp())
            if state.get('last_quota_reset_timestamp', 0) < today_start_ts:
                print(f"\n--- [Flexible Dip] New Day [{today_start_utc.strftime('%Y-%m-%d')}] Resetting Daily Quota & Risk Level ---")
                state['daily_quota_achieved'] = False; state['risk_level'] = 0; state.pop('daily_loss_limit_hit', None); state['capital_at_quota_start'] = state['current_balance_usd']; state['last_quota_reset_timestamp'] = current_time
                prev_day_pnl_usd = Decimal(state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")) - Decimal(state.get('prev_day_total_realized_pnl_at_reset', "0.0"))
                calculate_performance_metrics(state); state['prev_day_total_realized_pnl_at_reset'] = state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")
                start_balance_fmt = _format_compact(state['capital_at_quota_start'], '$', 2); prev_day_pnl_fmt = _format_compact(prev_day_pnl_usd, '$', 2)
                send_trader_telegram(f"🌅 New Day! Daily quota reset. Risk level to 0.\nStart Balance: {start_balance_fmt} USD\nPrev Day PnL: {prev_day_pnl_fmt} USD")
                save_trader_state(state)

            manage_portfolio(state);
            manage_pending_entries(state)

            if VERBOSE_LOGGING: print("    [FD Verbose] Checking operational halts...")
            else: print("    [*] [Flexible Dip] Checking operational halts...")
            current_balance_dec = Decimal(state['current_balance_usd']); capital_start_dec = Decimal(state['capital_at_quota_start']); unrealized_pnl = Decimal("0.0")
            if state['open_positions']:
                 if VERBOSE_LOGGING: print(f"        [FD Verbose] Calculating unrealized PnL for {len(state['open_positions'])} positions...")
                 for mint_pos, pos_data in state['open_positions'].items():
                     current_price_for_pnl = get_current_price(mint_pos)
                     if current_price_for_pnl:
                         try: entry_val = Decimal(pos_data['position_size_usd']); current_val = Decimal(pos_data['position_size_tokens']) * current_price_for_pnl; unrealized_pnl += (current_val - entry_val)
                         except Exception as dec_err: print(f"        [!] [Flexible Dip] Error calculating PNL for {mint_pos}: {dec_err}")
                     time.sleep(0.05)
                 if VERBOSE_LOGGING: print(f"        [FD Verbose] Unrealized PnL: {unrealized_pnl:+.2f} USD")
            effective_balance = current_balance_dec + unrealized_pnl; daily_drawdown = (capital_start_dec - effective_balance) / capital_start_dec if capital_start_dec > 0 else Decimal("0.0")
            if daily_drawdown >= DAILY_LOSS_LIMIT_PCT:
                if not state.get('daily_loss_limit_hit', False):
                    eff_balance_fmt = _format_compact(str(effective_balance), '$', 2); daily_drawdown_fmt = f"{daily_drawdown:.1%}"
                    print(f"    [!!!] [Flexible Dip] DAILY LOSS LIMIT HIT! Drawdown: {daily_drawdown_fmt}. Halting."); send_trader_telegram(f"🛑 Daily Loss Limit Hit ({daily_drawdown_fmt})! 🛑\nNo new trades.\nEffective Balance: {eff_balance_fmt} USD"); state['daily_loss_limit_hit'] = True
            else: state['daily_loss_limit_hit'] = False
            is_trading_hours = TRADING_START_HOUR_UTC <= current_hour_utc < TRADING_END_HOUR_UTC; is_sl_cooldown = current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS; is_kill_switch_active = current_time < state.get('kill_switch_active_until_ts', 0)
            can_consider_new_trade = (not state['daily_quota_achieved'] and not state.get('daily_loss_limit_hit', False) and is_trading_hours and not is_sl_cooldown and not is_kill_switch_active)
            log_msg_halts = f"        [Flexible Dip] Halts: Quota={state['daily_quota_achieved']}, LossLimit={state.get('daily_loss_limit_hit', False)}, Hours={is_trading_hours}, SL Cool={is_sl_cooldown}, Kill={is_kill_switch_active}"
            log_msg_can_trade = f"        [Flexible Dip] --> Can consider new trade? {can_consider_new_trade}"
            if VERBOSE_LOGGING: print(log_msg_halts); print(log_msg_can_trade)
            elif not can_consider_new_trade: print(log_msg_halts); print(log_msg_can_trade)

            should_scan_reports = can_consider_new_trade and current_time > last_report_scan_time + CHECK_INTERVAL_SECONDS * 2
            if VERBOSE_LOGGING: print(f"    [FD Verbose] Should scan reports? {should_scan_reports} (CanTrade:{can_consider_new_trade}, TimeOK:{current_time > last_report_scan_time + CHECK_INTERVAL_SECONDS * 2})")

            if should_scan_reports:
                print(f"\n[*] [Flexible Dip] Scanning last {NUM_REPORTS_TO_SCAN} reports for new dip candidates...")
                last_report_scan_time = current_time; latest_report_files = get_latest_report_paths(NUM_REPORTS_TO_SCAN); potential_candidates_dict: Dict[str, Dict] = {}

                if not latest_report_files: print("    [!] [Flexible Dip] No processed reports found to scan.")
                else:
                    if VERBOSE_LOGGING: print(f"    [FD Verbose] Found {len(latest_report_files)} report files to scan.")
                    total_items_processed_from_reports = 0
                    unique_mints_passed_filters = 0

                    for report_idx, report_path in enumerate(latest_report_files):
                        report_name = os.path.basename(report_path)
                        if VERBOSE_LOGGING: print(f"    [FD Verbose] Scanning Report {report_idx+1}/{len(latest_report_files)}: {report_name}")
                        report_data = load_json_data(report_path);
                        if not report_data: print(f"        [!] [Flexible Dip] Failed to load report {report_name}"); continue

                        items_list = report_data.get("correlated_holdings", report_data.get("correlated_holdings_snapshot", []))
                        if not items_list or not isinstance(items_list, list):
                            if VERBOSE_LOGGING: print(f"        [FD Verbose] No relevant items list in {report_name}");
                            continue
                        
                        if VERBOSE_LOGGING: print(f"        [FD Verbose] Report {report_name} contains {len(items_list)} items. Filtering...")
                        total_items_processed_from_reports += len(items_list)

                        for item_data in items_list:
                            mint_cand = item_data.get("token_mint")
                            if not mint_cand or not isinstance(item_data, dict): continue
                            
                            passed_strict, fail_reason, matched_scenario, scenario_bonus = passes_strict_filters(item_data, current_time, is_revalidation=False)
                            
                            if passed_strict and matched_scenario and scenario_bonus is not None:
                                if mint_cand not in potential_candidates_dict:
                                    unique_mints_passed_filters += 1
                                item_data['_matched_dip_scenario_name'] = matched_scenario
                                item_data['_scenario_score_bonus'] = str(scenario_bonus)
                                potential_candidates_dict[mint_cand] = item_data
                            elif VERBOSE_LOGGING and fail_reason:
                                print(f"        [FD Filter Fail Detail] {item_data.get('symbol','N/A')} ({mint_cand[:6]}): {fail_reason}")


                    print(f"    [Flexible Dip] Scanned {len(latest_report_files)} reports, {total_items_processed_from_reports} total items processed.")
                    print(f"    [Flexible Dip] Found {len(potential_candidates_dict)} unique candidates passing all filters and matching a dip scenario.")

                    if potential_candidates_dict:
                        items_to_rank = list(potential_candidates_dict.values())
                        ranked_candidates = rank_trade_candidates(items_to_rank)
                        
                        state['recently_pending'] = {m: ts for m, ts in state.get('recently_pending', {}).items() if current_time - ts < RECENTLY_ADDED_PENDING_WINDOW_SECONDS}
                        added_to_pending_count = 0; replaced_in_pending_count = 0
                        if VERBOSE_LOGGING: print(f"    [FD Verbose] Processing {len(ranked_candidates)} ranked candidates for pending list. Recently pending: {len(state['recently_pending'])}.")
                        else: print(f"    [*] [Flexible Dip] Processing top {len(ranked_candidates)} ranked for pending list...")
                        
                        for i, ranked_item_cand in enumerate(ranked_candidates):
                            pending_mint_cand = ranked_item_cand.get("token_mint")
                            if not pending_mint_cand: continue
                            new_candidate_score = ranked_item_cand.get('_score', Decimal('-1')); symbol_for_log_cand = ranked_item_cand.get('symbol', 'N/A')
                            matched_scenario_for_pending = ranked_item_cand.get('_matched_dip_scenario_name', "N/A_ranked")
                            
                            if VERBOSE_LOGGING: print(f"        [FD Verbose] Considering candidate #{i+1}: {symbol_for_log_cand} ({pending_mint_cand[:6]}) Score: {new_candidate_score:.2f}, Scenario: {matched_scenario_for_pending}")

                            if pending_mint_cand in state['open_positions']:
                                if VERBOSE_LOGGING: print(f"            [FD Verbose] Skipping {symbol_for_log_cand}: Already in open positions.");
                                continue
                            if pending_mint_cand in state.get('recently_pending', {}):
                                if VERBOSE_LOGGING: print(f"            [FD Verbose] Skipping {symbol_for_log_cand}: In recently_pending cooldown.");
                                continue
                            
                            num_open_plus_pending = len(state['open_positions']) + len(state.get('pending_entry', {}))
                            has_free_slot = num_open_plus_pending < MAX_OPEN_POSITIONS
                            pending_entry_map = state.setdefault('pending_entry', {})
                            
                            new_pending_data = {
                                "symbol": symbol_for_log_cand, "name": ranked_item_cand.get("name", "N/A"),
                                "signal_timestamp": int(time.time()),
                                "signal_price": str((ranked_item_cand.get("bds_market_data") or {}).get("price") or 'N/A'),
                                "monitoring_start_time": int(time.time()),
                                "entry_window_seconds": PENDING_ENTRY_WINDOW_SECONDS,
                                "score": str(new_candidate_score), "risk_level": state['risk_level'],
                                "matched_dip_scenario_name": matched_scenario_for_pending,
                                "original_correlation_data": ranked_item_cand.get("original_correlation_data", {})
                            }

                            if pending_mint_cand in pending_entry_map:
                                existing_pending_score = Decimal(pending_entry_map[pending_mint_cand].get('score', '-1'))
                                if new_candidate_score > existing_pending_score * PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR:
                                    print(f"            [FD] Updating PENDING {symbol_for_log_cand} (NewScore:{new_candidate_score:.2f} > OldScore:{existing_pending_score:.2f})")
                                    pending_entry_map[pending_mint_cand] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint_cand] = current_time; replaced_in_pending_count +=1
                                elif VERBOSE_LOGGING: print(f"            [FD Verbose] Skipping {symbol_for_log_cand}: Already pending, new score not significantly better.")
                            elif has_free_slot:
                                print(f"            -> [FD] Added {symbol_for_log_cand} to PENDING. Score: {new_candidate_score:.2f}, Scenario: {matched_scenario_for_pending}")
                                pending_entry_map[pending_mint_cand] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint_cand] = current_time; added_to_pending_count += 1
                            elif pending_entry_map:
                                lowest_score_pending_mint_cand = None; lowest_score_val = new_candidate_score
                                for mint_in_pending, p_data in pending_entry_map.items():
                                    if mint_in_pending == pending_mint_cand: continue
                                    current_pending_score = Decimal(p_data.get('score', '-1'))
                                    if current_pending_score < lowest_score_val: lowest_score_val = current_pending_score; lowest_score_pending_mint_cand = mint_in_pending
                                if lowest_score_pending_mint_cand and new_candidate_score > lowest_score_val:
                                    removed_symbol_cand = pending_entry_map[lowest_score_pending_mint_cand].get('symbol','Old')
                                    print(f"            -> [FD] Replacing PENDING {removed_symbol_cand} (Score:{lowest_score_val:.2f}) with {symbol_for_log_cand} (Score:{new_candidate_score:.2f})")
                                    pending_entry_map.pop(lowest_score_pending_mint_cand); pending_entry_map[pending_mint_cand] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint_cand] = current_time; replaced_in_pending_count += 1
                                elif VERBOSE_LOGGING: print(f"            [FD Verbose] Skipping {symbol_for_log_cand}: No free slots and not better than existing pending.")
                            else:
                                 if VERBOSE_LOGGING: print(f"            [FD Verbose] Skipping {symbol_for_log_cand}: No free slots and no pending entries to compare against.")


                            if len(state['open_positions']) + len(state.get('pending_entry', {})) >= MAX_OPEN_POSITIONS:
                                 if added_to_pending_count > 0 or replaced_in_pending_count > 0 :
                                     print(f"        [Flexible Dip] Max open/pending positions ({MAX_OPEN_POSITIONS}) reached. Stopping candidate processing for this cycle.")
                                 break

                        if added_to_pending_count > 0 or replaced_in_pending_count > 0:
                            print(f"    [*] [Flexible Dip] Finished candidate scan. Added: {added_to_pending_count}, Replaced: {replaced_in_pending_count}. Current Pending: {len(state.get('pending_entry',{}))}")
                        elif VERBOSE_LOGGING: print(f"    [FD Verbose] Finished candidate scan. No new pending entries added or replaced.")
                    elif VERBOSE_LOGGING: print(f"    [FD Verbose] No candidates passed filters in reports or none to rank.")


            if current_time >= state.get('last_4hr_summary_ts', 0) + SUMMARY_INTERVAL_4HR_SECONDS:
                print(f"\n[*] [Flexible Dip] Generating 4-Hour Performance Summary...")
                calculate_performance_metrics(state); generate_and_send_performance_summary_md_original(state, 4, "4-Hour [Flexible Dip]", current_time)
            elif current_time >= state.get('last_daily_summary_ts', 0) + SUMMARY_INTERVAL_DAILY_SECONDS:
                print(f"\n[*] [Flexible Dip] Generating Daily Performance Summary...")
                calculate_performance_metrics(state); generate_and_send_performance_summary_md_original(state, 24, "Daily [Flexible Dip]", current_time)
            else:
                save_trader_state(state)

            print(f"--- [Flexible Dip] Loop End: {default_format_timestamp(time.time())} ---")
            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt: print("\n--- Flexible Dip Trader stopping ---"); save_trader_state(state); print("    Flexible Dip state saved."); break
        except Exception as e:
            print(f"\n[!!!] [Flexible Dip] Unhandled error in main loop: {e}"); traceback.print_exc()
            print("    [Flexible Dip] Attempting to save state..."); save_trader_state(state)
            error_type_safe = escape_markdown_original(type(e).__name__); error_msg_safe = escape_markdown_original(str(e))
            telegram_error_message = f"🚨 *Flexible Dip Main Loop Error* 🚨\n\n`{error_type_safe}: {error_msg_safe}`\n\nSee console/logs. Will retry."
            send_trader_telegram(telegram_error_message); print(f"    [Flexible Dip] Retrying in {CHECK_INTERVAL_SECONDS*2} seconds..."); time.sleep(CHECK_INTERVAL_SECONDS * 2)


if __name__ == "__main__":
    main_trading_loop()

# --- END OF FILE paper_trader_flexible_dip.py ---