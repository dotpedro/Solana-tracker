# --- START OF FILE paper_trader_advanced_bottom_detector.py ---

#!/usr/bin/env python3
"""
Paper Trading Simulator - ADVANCED LOCAL BOTTOM DETECTOR STRATEGY.
Version: 1.2.1
- Revised near-miss logic:
  - `passes_strict_filters` now populates proximity for all checks.
  - `calculate_nearness_score` scores based on overall closeness,
    not eliminating for initial MC/dip scenario fails within the score func.
  - Main loop collects all failed candidates (post-initial MC check) for near-miss scoring.
- Added MIN_MARKET_CAP_USD filter ($10,000) to exclude low market cap tokens
  from analysis for both trading and near-miss reporting.
- Ensured market cap filter is applied early in candidate processing and
  within passes_strict_filters and calculate_nearness_score.
- Corrected OHLCV API parameter back to 'type' as per specific endpoint docs.
- Switched get_recent_swap_transactions to use v3 all-time-trades endpoint.
- Added adapter for v3 trade data to legacy format for analysis functions.
Based on Flexible Dip Buyer v1.0.2 logic.
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

if not (TELEGRAM_TOKEN_TRADER and TELEGRAM_CHAT_ID_TRADER): print("[!] WARNING: [AdvBottom] Trader Telegram credentials missing.")
if not BIRDEYE_API_KEY: print("[!] CRITICAL: [AdvBottom] BIRDEYE_API_KEY missing."); exit()

try: import api_usage_limiter; print("[✓] paper_trader_adv_bottom: Imported api_usage_limiter.")
except ImportError: print("CRITICAL: api_usage_limiter.py not found!"); exit()
try: from helius_analyzer import format_timestamp as default_format_timestamp; print("[✓] paper_trader_adv_bottom: Imported format_timestamp.")
except ImportError:
    print("[!] paper_trader_adv_bottom: Could not import format_timestamp. Using basic internal version.")
    def default_format_timestamp(ts: Optional[Any]) -> Optional[str]:
        if ts is None: return "N/A"
        try: return datetime.datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except: return str(ts)

# --- CONFIG (Flexible Dip Specific - Retained) ---
VERBOSE_LOGGING = True # previous value was True (no change for API usage)
REPORTS_ROOT   = os.path.join("reports", "holdings")
PROCESSED_REPORTS_DIR = os.path.join(REPORTS_ROOT, "processed")
TRADER_STATE_FILE   = os.path.join(REPORTS_ROOT, "paper_trader_adv_bottom_state.json")
TRADE_LOG_CSV_FILE = os.path.join(REPORTS_ROOT, "paper_trade_adv_bottom_log.csv")
INITIAL_CAPITAL_USD = Decimal("1000.00") # previous value was Decimal("1000.00")
MAX_OPEN_POSITIONS = 1 # previous value was 5
DAILY_LOSS_LIMIT_PCT = Decimal("0.10") # previous value was Decimal("0.10")
RISK_LEVEL_0_ALLOCATION_PCT = Decimal("0.05"); RISK_LEVEL_0_TP_PCT = Decimal("0.40")
RISK_LEVEL_1_ALLOCATION_PCT = Decimal("0.07"); RISK_LEVEL_1_TP_PCT = Decimal("1.00"); RISK_LEVEL_1_ENTRY_THRESHOLD = Decimal("1.5")
RISK_LEVEL_2_ALLOCATION_PCT = Decimal("0.10"); RISK_LEVEL_2_TP_PCT = Decimal("2.50"); RISK_LEVEL_2_ENTRY_THRESHOLD = Decimal("2.0")
SIMULATED_FEE_PCT = Decimal("0.001"); SIMULATED_SLIPPAGE_PCT = Decimal("0.005")
TRADING_START_HOUR_UTC = 0; TRADING_END_HOUR_UTC = 24 # previous value was 0; 24
STOP_LOSS_COOLDOWN_SECONDS = 15 * 60 # previous value was 15 * 60
KILL_SWITCH_LOSS_PCT = Decimal("0.50"); KILL_SWITCH_DURATION_SECONDS = 2 * 60 * 60
PENDING_ENTRY_WINDOW_SECONDS = 5 * 60 # previous value was 3 * 60
PENDING_ENTRY_PRICE_DEVIATION_PCT = Decimal("0.05") # previous value was Decimal("0.05")
NUM_REPORTS_TO_SCAN = 2 # previous value was 10
RECENTLY_ADDED_PENDING_WINDOW_SECONDS = 10 * 60 # previous value was 10 * 60
PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR = Decimal("1.10") # previous value was Decimal("1.10")
TOKEN_RESCAN_COOLDOWN_SECONDS = 4 * 60 * 60  # previous value was 60 * 60 (1 hour for full re-analysis of a token)
MAX_NEAR_MISSES_TO_REPORT = 5 # previous value was 10
NEAR_MISS_REPORT_INTERVAL_SECONDS = 60 * 60 # previous value was 15 * 60 (Report near misses every 1 hour)
# --- Add this near your other URL constants or in the CONFIG section ---
DEXSCREENER_BASE_URL_SOLANA = "https://dexscreener.com/solana/"

# --- NEW CONFIG: Minimum Market Cap ---
MIN_MARKET_CAP_USD = Decimal("10000.00") # For actual trading consideration
# For near-misses, we might allow slightly lower MCs to be *scored*,
# but the main loop will filter very low MCs before full analysis.
# Let's define a very low threshold for MC just to avoid processing complete dust for near-misses.
MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION = Decimal("1000.00")


# --- General Quality Filters (Retained) ---
FLEX_STRICT_MIN_AGE_MINUTES = 10 # previous value was 5
FLEX_STRICT_MIN_TOTAL_HOLDERS = 75 # previous value was 20
FLEX_STRICT_MAX_TOP1_HOLDER_PCT = 0.60 # previous value was 0.70
FLEX_REQUIRE_SOCIALS = False # previous value was False
FLEX_STRICT_MIN_MONITORED_HOLDERS = 0 # previous value was 0
FLEX_STRICT_MIN_LIQUIDITY_USD = 7500 # previous value was 2000
FLEX_FILTER_MAX_24H_LOSS_PCT = Decimal("-90.0") # previous value was Decimal("-95.0")

# --- DIP SCENARIOS DEFINITIONS (Retained & Extended) ---
DIP_SCENARIOS = [
    {"name": "Deep 6H Dip, Recent Bounce",
     "timeframes_dip": {"h6": (-70.0, -25.0)},
     "timeframes_confirmation": {"m5": (1.0, 100.0)},
     "timeframes_avoid_strong_negative": {"h1": (-100.0, -20.0)},
     "priority": 1, "score_bonus": Decimal("2.5")},

    {"name": "Sharp 1H Drop, Stabilizing",
     "timeframes_dip": {"h1": (-50.0, -15.0)},
     "timeframes_confirmation": {"m5": (0.5, 100.0)},
     "timeframes_avoid_strong_negative": {"h6": (-100.0, -10.0)},
     "priority": 2, "score_bonus": Decimal("2.0")},

    {"name": "Multi-TF Pullback (24H, 6H Dip)",
     "timeframes_dip": {"h24": (-60.0, -10.0), "h6": (-40.0, -5.0)},
     "priority": 3, "score_bonus": Decimal("1.5")},

    {"name": "1H Positive Turnaround (Capped Bounce)",
     "timeframes_dip": {"h6": (-90.0, -5.0), "h24": (-90.0, -5.0)},
     "timeframes_confirmation": {"h1": (0.1, 20.0)},
     "timeframes_avoid_strong_negative": {"m5": (-10.0, 100.0)},
     "priority": 4, "score_bonus": Decimal("1.2")},

    {"name": "1H & 5M Positive Confirmation (Capped Bounce)",
     "timeframes_dip": {"h1": (0.1, 20.0)},
     "timeframes_confirmation": {"m5": (0.1, 15.0)},
     "timeframes_avoid_strong_negative": {"h6": (-80.0, 30.0)},
     "priority": 5, "score_bonus": Decimal("1.4")},

    {"name": "Minor 1H Pullback in Uptrend",
     "timeframes_dip": {"h1": (-25.0, -5.0)},
     "timeframes_confirmation": {"h6": (5.0, 200.0)},
     "priority": 6, "score_bonus": Decimal("1.0")},

    {"name": "Quick M5 Dip",
     "timeframes_dip": {"m5": (-30.0, -5.0)},
     "timeframes_avoid_strong_negative": {"h1": (-100.0, -10.0)},
     "priority": 7, "score_bonus": Decimal("0.75")}
]

# --- Scoring Weights (Retained & Extended) ---
SCORE_WEIGHTS = {
    "liquidity": 1.0, "vol_liq_ratio": 0.8, "price_change_positive_short_term": 0.5,
    "total_holders": 0.4, "monitored_holders": 0.5, "top1_holder_pct": -0.7,
    "age_score": 0.3, "socials": 0.1, "dip_buy_pressure": 0.7,
    "atr_dip_confirmation_bonus": Decimal("0.5"),
    "whale_buy_confirmation_bonus": Decimal("0.6"),
}

# --- Buy Pressure Analysis (Retained) ---
FETCH_RECENT_TX_COUNT = 15 # previous value was 50
MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS = 5 # previous value was 10
ENABLE_DIP_BUY_PRESSURE_FILTER = True # previous value was True
DIP_BUY_PRESSURE_MIN_NET_BUYS = 2 # previous value was 3
DIP_BUY_PRESSURE_RECENT_TX_MAX_AGE_MINUTES = 10 # previous value was 30

# --- NEW: Configuration for Advanced Bottom Detection ---
USDC_MINT_ADDRESS_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ATR_TIMEFRAME_OHLCV = "4H" # previous value was "1H" (Reduces granularity but fewer distinct data points to consider over time)
ATR_CANDLE_COUNT = 1 # previous value was 2
ATR_ENTRY_MULTIPLIER = Decimal("1.0") # previous value was Decimal("1.0")
ATR_STOP_LOSS_MULTIPLIER = Decimal("1.0") # previous value was Decimal("1.0")
ATR_TRAILING_ENABLED = True # previous value was True

WHALE_TRADER_COUNT = 2 # previous value was 5
WHALE_BUY_LOOKBACK_MINUTES = 30 # previous value was 60
WHALE_SELL_USD_THRESHOLD = Decimal("1000.00") # previous value was Decimal("1000.00")
WHALE_SELL_LOOKBACK_MINUTES = 5 # previous value was 10

MINT_BURN_CHECK_INTERVAL_SECONDS = 30 * 60 # previous value was 5 * 60 (300s -> 1800s)
MINT_EVENT_LOOKBACK_MINUTES = 20 # previous value was 10

SELL_PRESSURE_LOOKBACK_MINUTES = 5 # previous value was 10
SELL_PRESSURE_RATIO_THRESHOLD = Decimal("2.0") # previous value was Decimal("2.0")
SELL_PRESSURE_MIN_SELL_USD_THRESHOLD = Decimal("5000.00") # previous value was Decimal("5000.00")

ORDER_BOOK_SPREAD_MULTIPLIER_THRESHOLD = Decimal("2.0") # previous value was Decimal("2.0")
ORDER_BOOK_MIN_BID_DEPTH_USD = Decimal("2000.00") # previous value was Decimal("2000.00")
ORDER_BOOK_CHECK_INTERVAL_SECONDS = 15 * 60 # previous value was 2 * 60 (120s -> 900s)

# --- Timing & API Endpoints (Standard & Extended) ---
CHECK_INTERVAL_SECONDS = 120 # previous value was 30
SUMMARY_INTERVAL_4HR_SECONDS = 4 * 60 * 60
SUMMARY_INTERVAL_DAILY_SECONDS = 24 * 60 * 60
BDS_BASE_V3_URL = "https://public-api.birdeye.so/defi/v3"
BDS_BASE_V2_URL = "https://public-api.birdeye.so/defi/v2"
BDS_BASE_LEGACY_URL = "https://public-api.birdeye.so/defi"
BDS_MARKET_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/token/market-data"
BDS_TRADE_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/token/trade-data/single"
BDS_METADATA_MULTIPLE_ENDPOINT = f"{BDS_BASE_V3_URL}/token/meta-data/multiple"
BDS_TOKEN_SECURITY_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/token_security"
BDS_TOKEN_TX_LIST_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/txs/token"

BDS_OHLCV_BASE_QUOTE_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/ohlcv/base_quote"
BDS_TOKEN_TOP_TRADERS_ENDPOINT = f"{BDS_BASE_V2_URL}/tokens/top_traders"
BDS_TOKEN_MINT_BURN_ENDPOINT = f"{BDS_BASE_V3_URL}/token/mint-burn-txs"
BDS_TRADER_TXS_SEEK_BY_TIME_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/trader/txs/seek_by_time"
BDS_MARKET_DATA_MULTIPLE_ENDPOINT = f"{BDS_BASE_V3_URL}/token/market-data/multiple"

BDS_ALL_TIME_TRADES_SINGLE_ENDPOINT = f"{BDS_BASE_V3_URL}/all-time/trades/single"

os.makedirs(REPORTS_ROOT, exist_ok=True); os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)

try:
    from gmgn_api import gmgn
    GMGN_ENABLED = True
    gmgn_analyzer = gmgn()
    print("[✓] paper_trader_adv_bottom: GMGN API enabled and analyzer initialized.")
except ImportError:
    print("[!] paper_trader_adv_bottom: gmgn_api.py not found or failed to init. GMGN features disabled.")
    GMGN_ENABLED = False
    gmgn_analyzer = None

# --- UTILS ---
def escape_markdown_original(text: Optional[Any]) -> str:
    if text is None: return ''
    if not isinstance(text, str): text = str(text)
    escape_chars = r'_*`['
    for char_to_escape in escape_chars: text = text.replace(char_to_escape, '\\' + char_to_escape)
    return text

def send_trader_telegram(text: str) -> bool:
    token = TELEGRAM_TOKEN_TRADER; chat_id = TELEGRAM_CHAT_ID_TRADER
    prefix = "🤖 AdvBottom 🤖"
    if not token or not chat_id: print(f"--- ADV BOTTOM CONSOLE ---\n{text}\n--------------------"); return True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": f"{prefix}\n\n{text}", "disable_web_page_preview": "true"}
    try: r = requests.post(url, data=payload, timeout=20); r.raise_for_status(); return True
    except requests.exceptions.HTTPError as e: error_content = e.response.text[:500] if e.response else str(e); print(f"    [✗] [AdvBottom] Trader Telegram HTTP error: {e.response.status_code if e.response else 'N/A'} - {error_content}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False
    except Exception as e: print(f"    [✗] [AdvBottom] Trader Telegram general error: {e}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False

def load_json_data(path: str, is_state_file: bool = False) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if is_state_file: print(f"    [i] [AdvBottom] State file not found: {path}.")
        else: print(f"    [!] [AdvBottom] Data file not found: {path}")
    except json.JSONDecodeError as e: print(f"    [!] [AdvBottom] Failed to decode JSON from {path}: {e}")
    except Exception as e: print(f"    [!] [AdvBottom] Failed to load {path}: {e}")
    return {}
def save_json_data(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: print(f"    [✗] [AdvBottom] Failed to save data to {path}: {e}")
def load_trader_state() -> Dict[str, Any]: return load_json_data(TRADER_STATE_FILE, is_state_file=True)
def save_trader_state(state: Dict[str, Any]): save_json_data(TRADER_STATE_FILE, state)
def calculate_percent_change(old: Optional[Any], new: Optional[Any]) -> Optional[Decimal]:
    if old is None or new is None: return None
    try: old_d = Decimal(str(old)); new_d = Decimal(str(new))
    except: return None
    if old_d == Decimal(0): return Decimal('inf') if new_d > 0 else (Decimal('-inf') if new_d < 0 else Decimal(0))
    try: return (new_d - old_d) / abs(old_d)
    except Exception: return None
def get_latest_report_paths(n: int = 10) -> List[str]:
    processed_full_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_report_*.json")
    processed_delta_pattern = os.path.join(PROCESSED_REPORTS_DIR, "holding_delta_*.json")
    all_files = glob.glob(processed_full_pattern) + glob.glob(processed_delta_pattern)
    if not all_files: return []
    try: all_files.sort(key=os.path.getmtime, reverse=True); return all_files[:n]
    except Exception as e: print(f"    [!] [AdvBottom] Error finding latest report paths: {e}"); return []

def _format_compact(value: Optional[Any], prefix: str = "$", precision: int = 1) -> str:
    if value is None or value == "N/A": return "N/A";
    try:
        val = float(value)
        if abs(val) >= 1_000_000_000: return f"{prefix}{val/1_000_000_000:.{precision}f}B"
        if abs(val) >= 1_000_000: return f"{prefix}{val/1_000_000:.{precision}f}M"
        if abs(val) >= 1_000: return f"{prefix}{val/1_000:.{precision}f}K"
        formatted_num = f"{val:,.{precision}f}" if precision >= 0 else f"{val:,.0f}"
        return f"{prefix}{formatted_num}"
    except (ValueError, TypeError): return str(value)
def _format_pct(value: Optional[Any]) -> str:
    if value is None or value == "N/A": return "N/A";
    try: return f"{float(value):+.1f}%"
    except (ValueError, TypeError): return str(value)

def get_current_price(mint_address: str) -> Optional[Decimal]:
    if not mint_address or not BIRDEYE_API_KEY: return None
    price_cache_ttl = 45; cache_key = f"birdeye_price_{mint_address}"; url = f"https://public-api.birdeye.so/defi/price?address={mint_address}"; headers = {"X-API-KEY": BIRDEYE_API_KEY}
    data = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="price", calling_script="paper_trader_adv_bottom.get_current_price", cache_key=cache_key, cache_ttl_seconds=price_cache_ttl, url=url, headers=headers, timeout=10)
    if isinstance(data, dict) and data.get("success") == True and isinstance(data.get("data"), dict):
        price_value = data["data"].get("value");
        if price_value is not None:
            try: return Decimal(str(price_value))
            except Exception as e: print(f"    [!] [AdvBottom] Error converting price {price_value} to Decimal for {mint_address}: {e}"); return None
    elif isinstance(data, dict) and data.get("success") == False:
         msg = data.get('message', 'Unknown error')
         if "Not found" not in msg and "Request failed" not in msg: print(f"    [!] [AdvBottom] Birdeye price API failed for {mint_address}: {msg}")
    return None

def get_ohlcv_interval_seconds(ohlcv_type_str: str) -> Optional[int]:
    """Helper to get interval duration in seconds from OHLCV type string."""
    ohlcv_type_str = ohlcv_type_str.lower()
    if 'm' in ohlcv_type_str:
        try: return int(ohlcv_type_str.replace('m', '')) * 60
        except ValueError: return None
    elif 'h' in ohlcv_type_str:
        try: return int(ohlcv_type_str.replace('h', '')) * 3600
        except ValueError: return None
    elif 'd' in ohlcv_type_str:
        try: return int(ohlcv_type_str.replace('d', '')) * 86400
        except ValueError: return None
    return None

def pt_get_bds_ohlcv_base_quote(base_address: str, quote_address: str, ohlcv_type: str, time_from: int, time_to: int, chain: str = "solana") -> Optional[List[Dict]]:
    """Retrieve OHLCV data for a base/quote pair. Parameter is 'type'."""
    if not all([base_address, quote_address, ohlcv_type]):
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Validate] Missing required address/type params. Base: {base_address}, Quote: {quote_address}, Type: {ohlcv_type}")
        return None
    if not (isinstance(time_from, int) and isinstance(time_to, int) and 0 < time_from < 2_000_000_000 and 0 < time_to < 2_000_000_000):
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Validate] Invalid timestamps (range or type). From: {time_from}, To: {time_to}")
        return None
    if time_from >= time_to:
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Validate] time_from ({time_from}) must be less than time_to ({time_to}).")
        return None

    MAX_CANDLES_PER_REQUEST = 1000
    interval_seconds = get_ohlcv_interval_seconds(ohlcv_type)
    if interval_seconds is None:
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Validate] Invalid ohlcv_type for interval calculation: {ohlcv_type}")
        return None

    requested_duration_seconds = time_to - time_from
    num_candles_requested = math.ceil(requested_duration_seconds / interval_seconds)

    if num_candles_requested > MAX_CANDLES_PER_REQUEST:
        if VERBOSE_LOGGING:
            print(f"    [!] [AdvBottom OHLCV Validate] Requested candle count ({num_candles_requested}) for type '{ohlcv_type}' "
                  f"over period {time_from}-{time_to} exceeds max ({MAX_CANDLES_PER_REQUEST}). Aborting.")
        return None

    params = {
        "base_address": base_address,
        "quote_address": quote_address,
        "type": ohlcv_type,
        "time_from": time_from,
        "time_to": time_to
    }
    url = f"{BDS_OHLCV_BASE_QUOTE_ENDPOINT}?{urllib.parse.urlencode(params)}"
    cache_key = f"bds_ohlcv_{base_address}_{quote_address}_{ohlcv_type}_{time_from}_{time_to}_{chain}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain, "accept": "application/json"}

    response = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="ohlcv_base_quote",
        calling_script="paper_trader_adv_bottom.pt_get_bds_ohlcv_base_quote",
        cache_key=cache_key, cache_ttl_seconds=15 * 60,
        url=url, headers=headers, timeout=15
    )

    if isinstance(response, dict) and response.get("success") and isinstance(response.get("data"), dict):
        items = response["data"].get("items")
        if isinstance(items, list):
            return items
        else:
            if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Fetch Error] 'data.items' field is not a list for {base_address[:6]}. Response: {str(response)[:200]}")
            return None
    elif isinstance(response, dict) and not response.get("success"):
        error_message = response.get('message', 'Unknown error from Birdeye OHLCV')
        status_code = response.get('status_code')
        birdeye_details = response.get('details')

        log_entry = f"    [!] [AdvBottom OHLCV API Error] For {base_address[:6]}/{quote_address[:6]} (type: {ohlcv_type}, from: {time_from}, to: {time_to}):"
        if status_code: log_entry += f" Status: {status_code}."
        log_entry += f" Message: '{error_message}'."
        if birdeye_details: log_entry += f" Birdeye Details: {birdeye_details}."

        if VERBOSE_LOGGING or "Invalid value" in error_message or (isinstance(status_code, int) and 400 <= status_code < 500):
            log_entry += f" Full Params Sent: {params}"

        print(log_entry)
        return None
    elif response is None:
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Fetch Error] API limiter returned None for {base_address[:6]}. Likely rate-limited or pre-API call failure.")
        return None
    else:
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom OHLCV Fetch Error] Received unexpected response type from API limiter for {base_address[:6]}. Type: {type(response)}, Response: {str(response)[:200]}")
        return None

def pt_get_bds_all_time_trades_multiple(
    token_mint_addresses: List[str],
    time_frame: str = "1h",
    limit_per_token: int = 100,
    chain: str = "solana"
) -> Optional[Dict[str, List[Dict]]]:
    if not BIRDEYE_API_KEY: return None
    if not token_mint_addresses: return {}

    if len(token_mint_addresses) > 1:
        if VERBOSE_LOGGING:
            print(f"    [!] [AdvBottom AllTimeTrades] WARNING: pt_get_bds_all_time_trades_multiple called with {len(token_mint_addresses)} tokens. "
                  f"Will only process the first token ({token_mint_addresses[0][:6]}...) using the single-token endpoint due to API plan constraints.")

    token_mint_address = token_mint_addresses[0]

    params = {
        "address": token_mint_address,
        "time_frame": time_frame,
        "limit": limit_per_token
    }
    url = f"{BDS_ALL_TIME_TRADES_SINGLE_ENDPOINT}?{urllib.parse.urlencode(params)}"

    headers = {"X-API-KEY": BIRDEYE_API_KEY, "accept": "application/json", "x-chain": chain}

    cache_key = f"bds_all_time_trades_single_v3_{chain}_{token_mint_address}_{time_frame}_{limit_per_token}"
    cache_ttl = 5 * 60

    response_data = api_usage_limiter.request_api(
        method=requests.get,
        api_name="birdeye",
        endpoint_name="all_time_trades_single_v3",
        calling_script="paper_trader_adv_bottom.pt_get_bds_all_time_trades_multiple (via single)",
        cache_key=cache_key,
        cache_ttl_seconds=cache_ttl,
        url=url,
        headers=headers,
        timeout=20
    )

    if isinstance(response_data, dict) and response_data.get("success"):
        data_payload = response_data.get("data")
        actual_trades_list = []

        if isinstance(data_payload, list):
            for item in data_payload:
                if isinstance(item, dict) and "blockUnixTime" in item and "txHash" in item:
                    actual_trades_list.append(item)
                elif VERBOSE_LOGGING:
                    print(f"    [!] [AdvBottom AllTimeTrades] Encountered non-trade object in data list for {token_mint_address}: {str(item)[:100]}")
            if not data_payload:
                 if VERBOSE_LOGGING: print(f"    [i] [AdvBottom AllTimeTrades] Received empty data list for {token_mint_address}.")
            elif not actual_trades_list and data_payload:
                 if VERBOSE_LOGGING: print(f"    [!] [AdvBottom AllTimeTrades] Data list for {token_mint_address} did not contain recognizable trade objects. Original data: {str(data_payload)[:200]}")
            return {token_mint_address: actual_trades_list}
        elif isinstance(data_payload, dict) and "items" in data_payload and isinstance(data_payload.get("items"), list):
            for item in data_payload["items"]:
                if isinstance(item, dict) and "blockUnixTime" in item and "txHash" in item:
                    actual_trades_list.append(item)
                elif VERBOSE_LOGGING:
                    print(f"    [!] [AdvBottom AllTimeTrades] Encountered non-trade object in data.items list for {token_mint_address}: {str(item)[:100]}")
            if not data_payload["items"]:
                 if VERBOSE_LOGGING: print(f"    [i] [AdvBottom AllTimeTrades] Received empty data.items list for {token_mint_address}.")
            elif not actual_trades_list and data_payload["items"]:
                 if VERBOSE_LOGGING: print(f"    [!] [AdvBottom AllTimeTrades] Data.items list for {token_mint_address} did not contain recognizable trade objects. Original data: {str(data_payload['items'])[:200]}")
            return {token_mint_address: actual_trades_list}
        else:
            if VERBOSE_LOGGING:
                print(f"    [!] [AdvBottom AllTimeTrades] 'data' field from single endpoint is neither a direct list nor contains a valid 'items' list for {token_mint_address}. Response: {str(response_data)[:200]}")
            return {token_mint_address: []}
    else: # Handle non-success or unexpected response_data type
        if VERBOSE_LOGGING:
            err_msg = (response_data or {}).get("message", "Unknown error or non-dict response")
            print(f"    [!] [AdvBottom AllTimeTrades] API call failed or returned non-success for {token_mint_address}. Error: {err_msg}. Full response: {str(response_data)[:200]}")
        return {token_mint_address: []}

def pt_get_bds_token_top_traders(token_address: str, time_frame: str = "1h", limit: int = 5, chain: str = "solana") -> Optional[List[Dict]]:
    params = {"address": token_address, "time_frame": time_frame, "sort_type": "desc", "sort_by": "volume", "offset": 0, "limit": limit}
    url = f"{BDS_TOKEN_TOP_TRADERS_ENDPOINT}?{urllib.parse.urlencode(params)}"
    cache_key = f"bds_top_traders_{token_address}_{time_frame}_{limit}_{chain}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    response = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="token_top_traders",
        calling_script="paper_trader_adv_bottom.pt_get_bds_token_top_traders",
        cache_key=cache_key, cache_ttl_seconds=10 * 60, url=url, headers=headers, timeout=15 )
    if isinstance(response, dict) and response.get("success") and isinstance(response.get("data"), dict):
        return response["data"].get("items")
    return None

def pt_get_bds_token_mint_burn_txs(token_address: str, limit: int = 20, chain: str = "solana", after_time: Optional[int] = None) -> Optional[List[Dict]]:
    params = {"address": token_address, "sort_by": "block_time", "sort_type": "desc", "type": "all", "offset": 0, "limit": limit}
    if after_time: params["after_time"] = after_time
    url = f"{BDS_TOKEN_MINT_BURN_ENDPOINT}?{urllib.parse.urlencode(params)}"
    cache_key = f"bds_mint_burn_{token_address}_{limit}_{after_time or 0}_{chain}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    response = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="token_mint_burn",
        calling_script="paper_trader_adv_bottom.pt_get_bds_token_mint_burn_txs",
        cache_key=cache_key, cache_ttl_seconds=3 * 60, url=url, headers=headers, timeout=15 )
    if isinstance(response, dict) and response.get("success") and isinstance(response.get("data"), dict):
        return response["data"].get("items")
    return None

def pt_get_bds_token_tx_list(
    token_address: str,
    offset: int = 0,
    limit: int = 50,
    tx_type: str = "swap",
    sort_type: str = "desc",
    chain: str = "solana"
) -> Optional[List[Dict]]:
    if not BIRDEYE_API_KEY or not token_address:
        return None

    params = {
        "address": token_address,
        "offset": offset,
        "limit": limit,
        "tx_type": tx_type,
        "sort_type": sort_type
    }
    url = f"{BDS_TOKEN_TX_LIST_ENDPOINT}?{urllib.parse.urlencode(params)}"

    headers = {"X-API-KEY": BIRDEYE_API_KEY, "accept": "application/json", "x-chain": chain}
    cache_key = f"bds_token_tx_list_{chain}_{token_address}_{tx_type}_o{offset}_l{limit}_{sort_type}"

    response_from_limiter = api_usage_limiter.request_api(
        method=requests.get,
        api_name="birdeye",
        endpoint_name="token_txs_list",
        calling_script="paper_trader_adv_bottom.pt_get_bds_token_tx_list",
        cache_key=cache_key,
        cache_ttl_seconds=2 * 60,
        url=url,
        headers=headers,
        timeout=20
    )

    if response_from_limiter is None:
        if VERBOSE_LOGGING:
            print(f"    [!] [AdvBottom TokenTxs] Received None from API limiter for {token_address[:6]}. Call failed, blocked, or dry run.")
        return None

    if isinstance(response_from_limiter, dict):
        if response_from_limiter.get("success") is True:
            data_payload = response_from_limiter.get("data")
            if isinstance(data_payload, dict):
                items = data_payload.get("items")
                if isinstance(items, list):
                    return items
                else:
                    if VERBOSE_LOGGING: print(f"    [!] [AdvBottom TokenTxs] Successful API response for {token_address[:6]}, but 'data.items' is not a list. Payload: {str(data_payload)[:200]}")
            else:
                if VERBOSE_LOGGING: print(f"    [!] [AdvBottom TokenTxs] Successful API response for {token_address[:6]}, but 'data' field is not a dict. Payload: {str(data_payload)[:200]}")

        elif response_from_limiter.get("success") is False:
            error_message = response_from_limiter.get('message', f'Birdeye logical error for {token_address[:6]} token_txs')
            details = response_from_limiter.get('details', 'No details provided.')
            if VERBOSE_LOGGING:
                print(f"    [!] [AdvBottom TokenTxs] API call HTTP 2xx but Birdeye logical error for {token_address[:6]}. Message: {error_message}")
                print(f"        Full error payload/details: {str(details)[:500]}")
        else:
            if VERBOSE_LOGGING: print(f"    [!] [AdvBottom TokenTxs] Received unexpected dict structure from API limiter for {token_address[:6]}: {str(response_from_limiter)[:200]}")
    else:
        if VERBOSE_LOGGING: print(f"    [!] [AdvBottom TokenTxs] Received unexpected response type from API limiter for {token_address[:6]}. Type: {type(response_from_limiter)}, Resp: {str(response_from_limiter)[:200]}")

    return None

def pt_get_bds_trader_txs_seek_by_time(trader_address: str, before_time: int, after_time: int, limit: int = 50, chain: str = "solana") -> Optional[List[Dict]]:
    params = {"address": trader_address, "before_time": before_time, "after_time": after_time, "offset": 0, "limit": limit}
    url = f"{BDS_TRADER_TXS_SEEK_BY_TIME_ENDPOINT}?{urllib.parse.urlencode(params)}"
    cache_key = f"bds_trader_txs_{trader_address}_{before_time}_{after_time}_{limit}_{chain}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    response = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="trader_txs_seek_by_time",
        calling_script="paper_trader_adv_bottom.pt_get_bds_trader_txs_seek_by_time",
        cache_key=cache_key, cache_ttl_seconds=5 * 60, url=url, headers=headers, timeout=15 )
    if isinstance(response, dict) and response.get("success") and isinstance(response.get("data"), dict):
        data_payload = response.get("data", {})
        return data_payload.get("items") if isinstance(data_payload, dict) else (data_payload if isinstance(data_payload, list) else None)
    return None

def pt_get_bds_market_data_multiple(mint_addresses: List[str], chain: str = "solana") -> Optional[Dict[str, Dict]]:
    if not mint_addresses: return None
    list_address_param = ",".join(mint_addresses)
    url = f"{BDS_MARKET_DATA_MULTIPLE_ENDPOINT}?list_address={urllib.parse.quote(list_address_param)}"
    cache_key = f"bds_market_multi_{chain}_{hashlib.sha256(list_address_param.encode()).hexdigest()}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    response = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="market_data_multiple",
        calling_script="paper_trader_adv_bottom.pt_get_bds_market_data_multiple",
        cache_key=cache_key, cache_ttl_seconds=60, url=url, headers=headers, timeout=15 )
    if isinstance(response, dict) and response.get("success") and isinstance(response.get("data"), dict):
        return response["data"]
    return None

# --- ATR Calculation ---
def calculate_atr_from_ohlcv(ohlcv_data: List[Dict], period: int = 1) -> Optional[Tuple[Decimal, Decimal]]:
    if not ohlcv_data or len(ohlcv_data) < 2:
        if VERBOSE_LOGGING: print("        [ATR Calc] Not enough OHLCV data for ATR (need at least 2 candles).")
        return None

    candle_now = ohlcv_data[0]
    candle_prev = ohlcv_data[1]

    try:
        high_now = Decimal(str(candle_now['h']))
        low_now = Decimal(str(candle_now['l']))
        close_now = Decimal(str(candle_now['c']))
        close_prev = Decimal(str(candle_prev['c']))
    except (KeyError, ValueError, TypeError) as e:
        if VERBOSE_LOGGING: print(f"        [ATR Calc] Invalid OHLCV data format: {e} in {candle_now} or {candle_prev}")
        return None

    tr1 = high_now - low_now
    tr2 = abs(high_now - close_prev)
    tr3 = abs(low_now - close_prev)

    true_range = max(tr1, tr2, tr3)

    current_atr_value = true_range

    current_atr_percent = Decimal("0")
    if close_now > 0:
        current_atr_percent = (current_atr_value / close_now) * Decimal("100")

    if VERBOSE_LOGGING:
        print(f"        [ATR Calc] H:{high_now:.4f} L:{low_now:.4f} C:{close_now:.4f} PrevC:{close_prev:.4f} -> TR/ATR(1): {current_atr_value:.4f} ({current_atr_percent:.2f}%)")

    return current_atr_value, current_atr_percent

def _adapt_v3_trade_to_legacy_format(tx_data: Dict, target_mint: str) -> Optional[Dict]:
    if not isinstance(tx_data, dict):
        if VERBOSE_LOGGING: print(f"        [Tx Adapt] Input tx_data is not a dict: {type(tx_data)}")
        return None

    if not all(key in tx_data for key in ["blockUnixTime", "txHash", "from", "to", "txType"]):
        if VERBOSE_LOGGING:
            print(f"        [Tx Adapt] Input tx_data missing essential fields from /txs/token endpoint. Object: {str(tx_data)[:150]}")
        return None

    if tx_data.get("txType") != "swap":
        if VERBOSE_LOGGING: print(f"        [Tx Adapt] Non-swap transaction passed to adapter: {tx_data.get('txType')}")
        return None

    from_data = tx_data.get("from")
    to_data = tx_data.get("to")

    if not isinstance(from_data, dict) or not isinstance(to_data, dict) or \
       not from_data.get("address") or not to_data.get("address"):
        if VERBOSE_LOGGING: print(f"        [Tx Adapt] Missing 'from' or 'to' data or their addresses in tx_data. Object: {str(tx_data)[:150]}")
        return None

    adapted_trade = {
        "block_timestamp": tx_data.get("blockUnixTime"),
        "blockUnixTime": tx_data.get("blockUnixTime"),
        "success": True,
        "transaction_type": "Swap",
        "tx_hash": tx_data.get("txHash"),
        "source": tx_data.get("source"),
        "owner": tx_data.get("owner"),
    }

    adapted_trade["from_token_address"] = from_data.get("address")
    adapted_trade["from_token_amount_ui"] = from_data.get("uiAmount")

    adapted_trade["to_token_address"] = to_data.get("address")
    adapted_trade["to_token_amount_ui"] = to_data.get("uiAmount")

    from_value_usd_str = None
    to_value_usd_str = None

    try:
        if adapted_trade["from_token_address"] == target_mint:
            from_ui_amount = from_data.get("uiAmount")
            from_nearest_price = from_data.get("nearestPrice")
            if from_ui_amount is not None and from_nearest_price is not None:
                from_value_usd_str = str(Decimal(str(from_ui_amount)) * Decimal(str(from_nearest_price)))
        elif adapted_trade["to_token_address"] == target_mint:
            to_ui_amount = to_data.get("uiAmount")
            to_nearest_price = to_data.get("nearestPrice")
            if to_ui_amount is not None and to_nearest_price is not None:
                to_value_usd_str = str(Decimal(str(to_ui_amount)) * Decimal(str(to_nearest_price)))
    except Exception as e:
        if VERBOSE_LOGGING: print(f"        [Tx Adapt] Error calculating USD value for {target_mint[:6]} in tx {tx_data.get('txHash')}: {e}")

    adapted_trade["from_token_value_usd"] = from_value_usd_str
    adapted_trade["to_token_value_usd"] = to_value_usd_str

    return adapted_trade


def get_recent_swap_transactions(mint_address: str, limit: int = 50, max_age_minutes: Optional[int] = None) -> Optional[List[Dict]]:
    if not mint_address or not BIRDEYE_API_KEY:
        return None

    raw_transactions = pt_get_bds_token_tx_list(
        token_address=mint_address,
        limit=FETCH_RECENT_TX_COUNT,
        tx_type="swap",
        sort_type="desc"
    )
    time.sleep(0.05)

    if raw_transactions is None:
        if VERBOSE_LOGGING:
            print(f"    [!] [AdvBottom get_recent_swap_transactions] Failed to fetch transactions for {mint_address[:6]} using /txs/token endpoint.")
        return None

    if not isinstance(raw_transactions, list):
        if VERBOSE_LOGGING:
            print(f"    [!] [AdvBottom get_recent_swap_transactions] Expected a list of transactions for {mint_address[:6]}, got {type(raw_transactions)}.")
        return None

    adapted_swaps = []
    if VERBOSE_LOGGING:
        print(f"        [AdvBottom TokenTxs] Received {len(raw_transactions)} raw transactions for {mint_address[:6]} from /txs/token. Adapting...")

    for tx_data in raw_transactions:
        adapted = _adapt_v3_trade_to_legacy_format(tx_data, mint_address)
        if adapted:
            adapted_swaps.append(adapted)

    if not adapted_swaps and VERBOSE_LOGGING:
        print(f"    [i] [AdvBottom get_recent_swap_transactions] No trades adapted from {len(raw_transactions)} raw transactions for {mint_address[:6]}.")

    if max_age_minutes is not None and adapted_swaps:
        filtered_swaps = []
        current_timestamp_utc = int(time.time())
        min_timestamp_utc = current_timestamp_utc - (max_age_minutes * 60)

        for swap in adapted_swaps:
            tx_timestamp = swap.get("block_timestamp")
            if isinstance(tx_timestamp, int) and tx_timestamp >= min_timestamp_utc:
                filtered_swaps.append(swap)

        if VERBOSE_LOGGING and len(adapted_swaps) != len(filtered_swaps):
            print(f"        [AdvBottom TokenTxs Filter] Filtered {len(adapted_swaps)} adapted swaps to {len(filtered_swaps)} for {mint_address[:6]} within last {max_age_minutes}m.")
        return filtered_swaps

    return adapted_swaps if adapted_swaps else None


def analyze_transaction_value_pressure(transactions: List[Dict], target_mint_address: str) -> Dict[str, Any]:
    buy_usd_volume = Decimal("0.0"); sell_usd_volume = Decimal("0.0"); buy_count = 0; sell_count = 0
    if not transactions: return {"buy_usd_volume": buy_usd_volume, "sell_usd_volume": sell_usd_volume, "buy_count": 0, "sell_count": 0, "total_swaps_analyzed": 0}
    for tx in transactions:
        if not isinstance(tx, dict): continue
        from_token_addr = tx.get("from_token_address"); to_token_addr = tx.get("to_token_address")

        from_value_usd = None
        if tx.get("from_token_value_usd") is not None:
            try: from_value_usd = Decimal(str(tx["from_token_value_usd"]))
            except: pass

        to_value_usd = None
        if tx.get("to_token_value_usd") is not None:
            try: to_value_usd = Decimal(str(tx["to_token_value_usd"]))
            except: pass

        if from_token_addr == target_mint_address and from_value_usd is not None:
            sell_usd_volume += from_value_usd; sell_count += 1
        elif to_token_addr == target_mint_address and to_value_usd is not None:
            buy_usd_volume += to_value_usd; buy_count += 1
    return {"buy_usd_volume": buy_usd_volume, "sell_usd_volume": sell_usd_volume, "buy_count": buy_count, "sell_count": sell_count, "total_swaps_analyzed": buy_count + sell_count}

def analyze_transaction_pressure(transactions: List[Dict], target_mint_address: str) -> Dict[str, Any]:
    buy_count = 0; sell_count = 0; other_count = 0
    if not transactions: return {"buy_count": 0, "sell_count": 0, "net_buys": 0, "total_swaps_analyzed": 0}
    for tx in transactions:
        from_token = tx.get("from_token_address"); to_token = tx.get("to_token_address")
        if from_token == target_mint_address: sell_count += 1
        elif to_token == target_mint_address: buy_count += 1
        else:
            other_count +=1
    net_buys = buy_count - sell_count; total_relevant_swaps = buy_count + sell_count
    if VERBOSE_LOGGING and other_count > 0: print(f"        [AdvBottom Pressure Detail] For {target_mint_address[:6]}: Others={other_count} in {len(transactions)} raw txs. Relevant swaps: {total_relevant_swaps}")
    return {"buy_count": buy_count, "sell_count": sell_count, "net_buys": net_buys, "total_swaps_analyzed": total_relevant_swaps}

def pt_get_bds_market_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_MARKET_DATA_ENDPOINT}?address={mint}&chain={chain}"; cache_key = f"bds_v3_market_{mint}_{chain}"
    market_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_market_data", calling_script="paper_trader_adv_bottom.pt_get_bds_market_data", cache_key=cache_key, cache_ttl_seconds=45, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(market_data_response, dict) and market_data_response.get("success"): data_payload = market_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_trade_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TRADE_DATA_ENDPOINT}?address={mint}&chain={chain}"; cache_key = f"bds_v3_trade_{mint}_{chain}"
    trade_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_trade_data", calling_script="paper_trader_adv_bottom.pt_get_bds_trade_data", cache_key=cache_key, cache_ttl_seconds=60, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(trade_data_response, dict) and trade_data_response.get("success"): data_payload = trade_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_metadata_single(mint: str, chain: str = "solana") -> Optional[Dict]:
    mint_list = [mint]; mints_param_value = urllib.parse.quote(",".join(mint_list)); cache_key_hash = hashlib.sha256(mints_param_value.encode()).hexdigest(); cache_key = f"bds_v3_meta_multi_{chain}_{cache_key_hash}"
    url = f"{BDS_METADATA_MULTIPLE_ENDPOINT}?list_address={mints_param_value}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    metadata_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_metadata_multiple", calling_script="paper_trader_adv_bottom.pt_get_bds_metadata_single", cache_key=cache_key, cache_ttl_seconds=60*60, url=url, headers=headers)
    if isinstance(metadata_response, dict) and metadata_response.get("success"): data_payload = metadata_response.get("data"); return data_payload.get(mint) if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_token_security(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TOKEN_SECURITY_ENDPOINT}?address={mint}"; cache_key = f"bds_security_{mint}_{chain}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    security_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="token_security", calling_script="paper_trader_adv_bottom.pt_get_bds_token_security", cache_key=cache_key, cache_ttl_seconds=5*60*60, url=url, headers=headers)
    if isinstance(security_response, dict) and security_response.get("success"): data_payload = security_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None

def get_enriched_data_for_pending_candidate(mint_address: str) -> Optional[Dict]:
    if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Fetching enriched data for {mint_address[:6]}...")
    enriched_data = {"token_mint": mint_address, "bds_metadata": None, "bds_market_data": None, "bds_holder_data": None, "bds_trade_data": None, "bds_security_data": None, "gmgn_security_info": None, "original_correlation_data": {"token_mint": mint_address}}
    enriched_data["bds_metadata"] = pt_get_bds_metadata_single(mint_address); time.sleep(0.05)
    enriched_data["bds_market_data"] = pt_get_bds_market_data(mint_address); time.sleep(0.05)
    enriched_data["bds_trade_data"] = pt_get_bds_trade_data(mint_address); time.sleep(0.05)
    enriched_data["bds_security_data"] = pt_get_bds_token_security(mint_address); time.sleep(0.05)
    if not enriched_data["bds_market_data"] or not enriched_data["bds_trade_data"] or not enriched_data["bds_security_data"]:
        if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Failed essential enriched data fetch for {mint_address[:6]}. Market={bool(enriched_data['bds_market_data'])}, Trade={bool(enriched_data['bds_trade_data'])}, Security={bool(enriched_data['bds_security_data'])}")
        print(f"        [!] [AdvBottom] Failed to fetch essential enriched data for {mint_address[:6]}."); return None
    if enriched_data["bds_metadata"]: enriched_data["symbol"] = enriched_data["bds_metadata"].get("symbol", "N/A"); enriched_data["name"] = enriched_data["bds_metadata"].get("name", "N/A")
    else: enriched_data["symbol"] = "N/A"; enriched_data["name"] = "N/A"
    if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Enriched data fetched for {mint_address[:6]}. Symbol: {enriched_data['symbol']}")
    print(f"        [✓] [AdvBottom] Enriched data snapshot fetched for {mint_address[:6]}."); return enriched_data

CSV_HEADER = ["exit_timestamp", "mint", "symbol", "entry_timestamp", "entry_price", "exit_price", "position_size_usd", "position_size_tokens", "pnl_usd", "pnl_pct", "exit_reason", "risk_level", "dip_scenario", "entry_atr_value", "entry_atr_pct"]
def log_trade_to_csv(trade_data: Dict[str, Any]):
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
                "dip_scenario": trade_data.get("dip_scenario_at_entry", "N/A"),
                "entry_atr_value": trade_data.get("entry_atr_value_at_entry", "N/A"),
                "entry_atr_pct": trade_data.get("entry_atr_pct_at_entry", "N/A")
            }
            writer.writerow(row)
    except IOError as e: print(f"    [✗] [AdvBottom] Error writing to trade log CSV: {e}")
    except Exception as e_csv: print(f"    [✗] [AdvBottom] Unexpected error logging trade to CSV: {e_csv}")

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
        except (TypeError, ValueError) as ve: print(f"    [!] [AdvBottom] Invalid data for metrics: {ve}. PNL: '{trade.get('pnl_usd')}', Entry: '{trade.get('position_size_usd')}'")
        except Exception as e: print(f"    [!] [AdvBottom] Error processing trade for metrics: {trade.get('mint')} - {e}")
    win_rate = (wins / processed_trades_count) * 100 if processed_trades_count > 0 else 0; avg_win_pct = float(win_pnl_pct_sum / wins) if wins > 0 else 0.0; avg_loss_pct = float(loss_pnl_pct_sum / losses) if losses > 0 else 0.0
    return {"total_trades": processed_trades_count, "win_rate_pct": f"{win_rate:.1f}", "avg_win_pct": f"{avg_win_pct:.1f}", "avg_loss_pct": f"{avg_loss_pct:.1f}", "total_realized_pnl_usd_for_list": current_list_pnl_usd}
def calculate_performance_metrics(state: Dict[str, Any]):
    history = state.get("trade_history", []); default_metrics = {"total_trades": 0, "win_rate_pct": "0.0", "avg_win_pct": "0.0", "avg_loss_pct": "0.0", "total_realized_pnl_usd": state.get('realized_pnl_usd', "0.0")}
    if not history: state['performance_metrics'] = default_metrics; return
    overall_metrics_cal = _calculate_metrics_from_trades(history)
    state['performance_metrics'] = {"total_trades": overall_metrics_cal['total_trades'], "win_rate_pct": overall_metrics_cal['win_rate_pct'], "avg_win_pct": overall_metrics_cal['avg_win_pct'], "avg_loss_pct": overall_metrics_cal['avg_loss_pct'], "total_realized_pnl_usd": state['realized_pnl_usd']}

# --- Updated Filter Function (AdvBottom) ---
def passes_strict_filters(
    item: Dict[str, Any],
    current_time: int,
    is_revalidation: bool = False
) -> Tuple[bool, Optional[str], Optional[str], Optional[Decimal], Dict[str, Any]]:
    mint_addr = item.get("token_mint", "UNKNOWN_MINT")
    symbol = item.get("symbol", "N/A")
    name = item.get("name", "N/A")

    if VERBOSE_LOGGING: print(f"    [AdvBottom Filter] START Filtering for {symbol} ({mint_addr[:6]}) (Reval: {is_revalidation})")

    proximity_details: Dict[str, Any] = {}
    final_fail_reason: Optional[str] = None
    passes_all_filters = True # Assume passes until a check fails

    market_data = item.get("bds_market_data") or {}
    trade_data = item.get("bds_trade_data") or {}
    metadata = item.get("bds_metadata") or {}
    bds_security = item.get("bds_security_data") or {}
    gmgn_security = item.get("gmgn_security_info") or {}
    original_data = item.get("original_correlation_data", {})

    price_changes: Dict[str, Decimal] = {}
    timeframe_map = {
        "m5": "m5", "m15": "m15", "m30": "m30", "h1": "h1", "h6": "h6", "h24": "h24"
    } # Birdeye v3 priceChange keys

    if trade_data:
        for scenario_tf_key, bds_v3_key in timeframe_map.items():
            val = trade_data.get("priceChange", {}).get(bds_v3_key)
            if val is not None:
                try: price_changes[scenario_tf_key] = Decimal(str(val))
                except Exception as e:
                    if VERBOSE_LOGGING: print(f"        [AdvBottom Filter WARN] Could not convert priceChange.{bds_v3_key} value '{val}' to Decimal for {symbol}: {e}")
                    proximity_details[f"price_change_{scenario_tf_key}_error"] = str(val) # Record error for proximity
    else: # No trade data
        passes_all_filters = False; final_fail_reason = final_fail_reason or "Missing Fresh Trade Data"
        proximity_details["essential_data"] = "Missing Trade Data"


    if not market_data:
        passes_all_filters = False; final_fail_reason = final_fail_reason or "Missing Fresh Market Data"
        proximity_details["essential_data"] = (proximity_details.get("essential_data","") + " Missing Market Data").strip()


    # --- Market Cap Filter ---
    market_cap_val = market_data.get("mc")
    if market_cap_val is not None:
        try:
            market_cap_dec = Decimal(str(market_cap_val))
            proximity_details["market_cap_current_usd"] = float(market_cap_dec)
            proximity_details["market_cap_target_usd"] = float(MIN_MARKET_CAP_USD)
            if market_cap_dec < MIN_MARKET_CAP_USD:
                passes_all_filters = False
                final_fail_reason = final_fail_reason or f"Market Cap Too Low (<{_format_compact(MIN_MARKET_CAP_USD, '$', 0)})"
                proximity_details["market_cap_shortfall_usd"] = float(MIN_MARKET_CAP_USD - market_cap_dec)
            else: # Passed MC check
                proximity_details["market_cap_shortfall_usd"] = 0.0 # Explicitly zero shortfall
        except Exception as e_mc_filter:
            if VERBOSE_LOGGING: print(f"        [AdvBottom Filter WARN] Could not convert market_cap value '{market_cap_val}' to Decimal for {symbol}: {e_mc_filter}")
            passes_all_filters = False; final_fail_reason = final_fail_reason or "Invalid Market Cap Type"
            proximity_details["market_cap"] = "Invalid Type"
    else: # Market cap data missing
        passes_all_filters = False; final_fail_reason = final_fail_reason or "Market Cap Missing"
        proximity_details["market_cap"] = "Missing"
        proximity_details["market_cap_current_usd"] = 0.0 # For scoring nearness
        proximity_details["market_cap_target_usd"] = float(MIN_MARKET_CAP_USD)
        proximity_details["market_cap_shortfall_usd"] = float(MIN_MARKET_CAP_USD)


    # --- Dip Scenario Matching ---
    matched_scenario_name: Optional[str] = None
    scenario_score_bonus: Optional[Decimal] = Decimal("0.0")
    best_match_scenario_obj: Optional[Dict[str, Any]] = None
    possible_matches = []

    for scenario in DIP_SCENARIOS:
        scenario_match_this_one = True # Check this specific scenario
        temp_proximity_dip_this_scenario = {}

        for tf, (min_dip, max_dip) in scenario["timeframes_dip"].items():
            current_pct_change = price_changes.get(tf)
            temp_proximity_dip_this_scenario[f"dip_{tf}_target_min"] = float(min_dip)
            temp_proximity_dip_this_scenario[f"dip_{tf}_target_max"] = float(max_dip)
            if current_pct_change is None:
                scenario_match_this_one = False; temp_proximity_dip_this_scenario[f"dip_{tf}_missing"] = True; break
            temp_proximity_dip_this_scenario[f"dip_{tf}_current_pct"] = float(current_pct_change)
            if not (Decimal(str(min_dip)) <= current_pct_change <= Decimal(str(max_dip))):
                scenario_match_this_one = False; shortfall = 0.0
                if current_pct_change > Decimal(str(max_dip)): shortfall = float(current_pct_change - Decimal(str(max_dip)))
                elif current_pct_change < Decimal(str(min_dip)): shortfall = float(Decimal(str(min_dip)) - current_pct_change)
                temp_proximity_dip_this_scenario[f"dip_{tf}_shortfall_pct"] = shortfall
        if not scenario_match_this_one:
            proximity_details[f"scenario_{scenario['name'].replace(' ','_')}_dip_fail"] = temp_proximity_dip_this_scenario # Store why this scenario failed
            continue

        if "timeframes_confirmation" in scenario:
            for tf, (min_gain, max_gain) in scenario["timeframes_confirmation"].items():
                current_pct_change = price_changes.get(tf)
                temp_proximity_dip_this_scenario[f"conf_{tf}_target_min"] = float(min_gain)
                temp_proximity_dip_this_scenario[f"conf_{tf}_target_max"] = float(max_gain)
                if current_pct_change is None: scenario_match_this_one = False; temp_proximity_dip_this_scenario[f"conf_{tf}_missing"] = True; break
                temp_proximity_dip_this_scenario[f"conf_{tf}_current_pct"] = float(current_pct_change)
                if not (Decimal(str(min_gain)) <= current_pct_change <= Decimal(str(max_gain))):
                    scenario_match_this_one = False; shortfall = 0.0
                    if current_pct_change < Decimal(str(min_gain)): shortfall = float(Decimal(str(min_gain)) - current_pct_change)
                    # Could also penalize overshooting max_gain if desired
                    temp_proximity_dip_this_scenario[f"conf_{tf}_shortfall_pct"] = shortfall
        if not scenario_match_this_one:
            proximity_details[f"scenario_{scenario['name'].replace(' ','_')}_conf_fail"] = temp_proximity_dip_this_scenario
            continue

        if "timeframes_avoid_strong_negative" in scenario:
            for tf, (min_allowed, max_allowed) in scenario["timeframes_avoid_strong_negative"].items():
                current_pct_change = price_changes.get(tf)
                temp_proximity_dip_this_scenario[f"avoid_{tf}_target_min"] = float(min_allowed)
                temp_proximity_dip_this_scenario[f"avoid_{tf}_target_max"] = float(max_allowed)
                if current_pct_change is None: continue # If data missing, can't fail this check
                temp_proximity_dip_this_scenario[f"avoid_{tf}_current_pct"] = float(current_pct_change)
                if not (Decimal(str(min_allowed)) <= current_pct_change <= Decimal(str(max_allowed))):
                    scenario_match_this_one = False; temp_proximity_dip_this_scenario[f"avoid_{tf}_failed"] = True
        if not scenario_match_this_one:
            proximity_details[f"scenario_{scenario['name'].replace(' ','_')}_avoid_fail"] = temp_proximity_dip_this_scenario
            continue

        if scenario_match_this_one:
            possible_matches.append({"scenario_obj": scenario, "proximity_dip_details_for_match": temp_proximity_dip_this_scenario})

    if not possible_matches:
        passes_all_filters = False; final_fail_reason = final_fail_reason or "No Dip Scenario Matched"
        proximity_details["dip_scenario_match"] = "Failed all scenarios"
    else:
        possible_matches.sort(key=lambda s: s["scenario_obj"]["priority"])
        best_match_scenario_data = possible_matches[0]
        best_match_scenario_obj = best_match_scenario_data["scenario_obj"]
        matched_scenario_name = best_match_scenario_obj["name"]
        scenario_score_bonus = best_match_scenario_obj["score_bonus"]
        # Add details of the *matched* scenario to proximity_details
        proximity_details.update(best_match_scenario_data.get("proximity_dip_details_for_match", {}))
        proximity_details["matched_dip_scenario_name"] = matched_scenario_name # For nearness score later
        item['_matched_dip_scenario_name'] = matched_scenario_name
        item['_scenario_score_bonus'] = str(scenario_score_bonus)
        if VERBOSE_LOGGING: print(f"    [AdvBottom Filter] {symbol}: Matched Scenario '{matched_scenario_name}' (Bonus: {scenario_score_bonus}).")


    # --- General Quality Filters (Continued) ---
    # Each filter now updates passes_all_filters, final_fail_reason, and proximity_details

    liquidity_val = market_data.get("liquidity")
    if not isinstance(liquidity_val, (int, float, Decimal)):
        if passes_all_filters: final_fail_reason = final_fail_reason or "Invalid Liquidity Type"
        passes_all_filters = False; proximity_details["liquidity"] = "Invalid Type"
        proximity_details["liquidity_current_usd"] = 0.0
        proximity_details["liquidity_target_usd"] = float(FLEX_STRICT_MIN_LIQUIDITY_USD)
        proximity_details["liquidity_shortfall_usd"] = float(FLEX_STRICT_MIN_LIQUIDITY_USD)
    else:
        liquidity_val_dec = Decimal(str(liquidity_val))
        proximity_details["liquidity_current_usd"] = float(liquidity_val_dec)
        proximity_details["liquidity_target_usd"] = float(FLEX_STRICT_MIN_LIQUIDITY_USD)
        if liquidity_val_dec < FLEX_STRICT_MIN_LIQUIDITY_USD:
            if passes_all_filters: final_fail_reason = final_fail_reason or "Low Liquidity"
            passes_all_filters = False
            proximity_details["liquidity_shortfall_usd"] = float(FLEX_STRICT_MIN_LIQUIDITY_USD - liquidity_val_dec)
        else:
            proximity_details["liquidity_shortfall_usd"] = 0.0

    creation_dt = None; age_delta_seconds = None
    # ... (age calculation logic remains, updates proximity_details similarly) ...
    ts_sources_list = [ (metadata or {}).get("created_timestamp"), (bds_security or {}).get("creationTime"), (bds_security or {}).get("creation_timestamp_ms"), (original_data or {}).get("first_event_timestamp_utc") ]
    for ts_source in ts_sources_list:
         if ts_source is None: continue
         try: formatted_ts = default_format_timestamp(ts_source)
         except Exception: continue
         if formatted_ts and formatted_ts != "N/A" and formatted_ts != str(ts_source):
             try: creation_dt = datetime.datetime.strptime(formatted_ts, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc); break
             except ValueError: continue
    if creation_dt: age_delta_seconds = (datetime.datetime.now(timezone.utc) - creation_dt).total_seconds()

    if age_delta_seconds is None:
        if passes_all_filters: final_fail_reason = final_fail_reason or "Cannot Determine Age"
        passes_all_filters = False; proximity_details["age"] = "Undetermined"
        proximity_details["age_current_minutes"] = 0.0
        proximity_details["age_target_minutes"] = float(FLEX_STRICT_MIN_AGE_MINUTES)
        proximity_details["age_shortfall_minutes"] = float(FLEX_STRICT_MIN_AGE_MINUTES)
    else:
        age_minutes = age_delta_seconds / 60
        proximity_details["age_current_minutes"] = float(age_minutes)
        proximity_details["age_target_minutes"] = float(FLEX_STRICT_MIN_AGE_MINUTES)
        if age_minutes < FLEX_STRICT_MIN_AGE_MINUTES:
            if passes_all_filters: final_fail_reason = final_fail_reason or "Too New"
            passes_all_filters = False
            proximity_details["age_shortfall_minutes"] = float(FLEX_STRICT_MIN_AGE_MINUTES - age_minutes)
        else:
            proximity_details["age_shortfall_minutes"] = 0.0

    total_holders_bds = trade_data.get("holderCount") if trade_data else None
    if total_holders_bds is None and trade_data: total_holders_bds = trade_data.get("holder")

    if not isinstance(total_holders_bds, int):
        if passes_all_filters: final_fail_reason = final_fail_reason or "Invalid Total Holders Type"
        passes_all_filters = False; proximity_details["total_holders"] = "Invalid Type"
        proximity_details["total_holders_current"] = 0
        proximity_details["total_holders_target"] = FLEX_STRICT_MIN_TOTAL_HOLDERS
        proximity_details["total_holders_shortfall"] = FLEX_STRICT_MIN_TOTAL_HOLDERS
    elif total_holders_bds < FLEX_STRICT_MIN_TOTAL_HOLDERS:
        if passes_all_filters: final_fail_reason = final_fail_reason or "Low Total Holders"
        passes_all_filters = False
        proximity_details["total_holders_shortfall"] = FLEX_STRICT_MIN_TOTAL_HOLDERS - total_holders_bds
        proximity_details["total_holders_current"] = total_holders_bds
        proximity_details["total_holders_target"] = FLEX_STRICT_MIN_TOTAL_HOLDERS
    else:
        proximity_details["total_holders_current"] = total_holders_bds
        proximity_details["total_holders_target"] = FLEX_STRICT_MIN_TOTAL_HOLDERS
        proximity_details["total_holders_shortfall"] = 0

    top_holder_pct_val = market_data.get("topHolderPercentage")
    if top_holder_pct_val is None: top_holder_pct_val = market_data.get("top_holder_pct")

    if top_holder_pct_val is not None:
        try:
            top_holder_pct_float = float(top_holder_pct_val)
            proximity_details["top1_holder_current_pct"] = top_holder_pct_float * 100
            proximity_details["top1_holder_target_max_pct"] = FLEX_STRICT_MAX_TOP1_HOLDER_PCT * 100
            if top_holder_pct_float > FLEX_STRICT_MAX_TOP1_HOLDER_PCT:
                if passes_all_filters: final_fail_reason = final_fail_reason or "High Top1 Holder Pct"
                passes_all_filters = False
                proximity_details["top1_holder_excess_pct"] = (top_holder_pct_float - FLEX_STRICT_MAX_TOP1_HOLDER_PCT) * 100
            else:
                proximity_details["top1_holder_excess_pct"] = 0.0
        except (ValueError, TypeError):
            if passes_all_filters: final_fail_reason = final_fail_reason or "Invalid Top1 Holder Pct Type"
            passes_all_filters = False; proximity_details["top1_holder_pct"] = "Invalid Type"
            proximity_details["top1_holder_current_pct"] = 0.0 # or some indicator for invalid
            proximity_details["top1_holder_target_max_pct"] = FLEX_STRICT_MAX_TOP1_HOLDER_PCT * 100
            proximity_details["top1_holder_excess_pct"] = 100.0 # Max penalty


    if FLEX_REQUIRE_SOCIALS:
        extensions = (metadata or {}).get("extensions", {})
        has_social = any(extensions.get(k) for k in ["website", "twitter", "telegram"])
        proximity_details["socials_present"] = has_social
        if not has_social:
            if passes_all_filters: final_fail_reason = final_fail_reason or "Missing Social Links"
            passes_all_filters = False; proximity_details["socials"] = "Missing"

    price_24h_val = price_changes.get("h24")
    if price_24h_val is not None:
        proximity_details["max_24h_loss_current_pct"] = float(price_24h_val)
        proximity_details["max_24h_loss_threshold_pct"] = float(FLEX_FILTER_MAX_24H_LOSS_PCT)
        if price_24h_val < FLEX_FILTER_MAX_24H_LOSS_PCT:
            if passes_all_filters: final_fail_reason = final_fail_reason or "Excessive 24h Loss"
            passes_all_filters = False

    # Security Flags
    if (bds_security or {}).get('mutableMetadata') is True:
        if passes_all_filters: final_fail_reason = final_fail_reason or "Mutable Metadata"
        passes_all_filters = False; proximity_details["security_mutable_metadata"] = True
    else: proximity_details["security_mutable_metadata"] = False

    freeze_auth = (bds_security or {}).get('freezeAuthority')
    is_freezeable_active = (bds_security or {}).get('freezeable') is True and freeze_auth and freeze_auth not in ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
    if is_freezeable_active:
        if passes_all_filters: final_fail_reason = final_fail_reason or "Freeze Authority Active"
        passes_all_filters = False; proximity_details["security_freeze_authority"] = True
    else: proximity_details["security_freeze_authority"] = False

    if (gmgn_security or {}).get('is_honeypot') is True:
        if passes_all_filters: final_fail_reason = final_fail_reason or "GMGN Honeypot"
        passes_all_filters = False; proximity_details["security_gmgn_honeypot"] = True
    else: proximity_details["security_gmgn_honeypot"] = False
    if (gmgn_security or {}).get('is_mintable') is True:
        if passes_all_filters: final_fail_reason = final_fail_reason or "GMGN Mintable"
        passes_all_filters = False; proximity_details["security_gmgn_mintable"] = True
    else: proximity_details["security_gmgn_mintable"] = False


    # --- Dip Buy Pressure Filter ---
    # Populate proximity details even if filter is off, for scoring if weight is non-zero
    recent_txs_pressure = get_recent_swap_transactions(mint_addr, FETCH_RECENT_TX_COUNT, DIP_BUY_PRESSURE_RECENT_TX_MAX_AGE_MINUTES)
    if recent_txs_pressure is None or not recent_txs_pressure:
        proximity_details["dip_pressure_tx_data"] = "Missing/None"
        proximity_details["dip_pressure_tx_count_current"] = 0
        proximity_details["dip_pressure_tx_count_target"] = MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS
        proximity_details["dip_pressure_current_net_buys"] = 0
        proximity_details["dip_pressure_target_net_buys"] = DIP_BUY_PRESSURE_MIN_NET_BUYS
        proximity_details["dip_pressure_net_buys_shortfall"] = DIP_BUY_PRESSURE_MIN_NET_BUYS
        if ENABLE_DIP_BUY_PRESSURE_FILTER:
            if passes_all_filters: final_fail_reason = final_fail_reason or "Dip Pressure Check Failed (No Txs)"
            passes_all_filters = False
    else:
        pressure_analysis = analyze_transaction_pressure(recent_txs_pressure, mint_addr)
        item['_dip_buy_count'] = pressure_analysis['buy_count']; item['_dip_sell_count'] = pressure_analysis['sell_count']
        item['_dip_net_buys'] = pressure_analysis['net_buys']; item['_dip_tx_analyzed_count'] = pressure_analysis['total_swaps_analyzed']
        proximity_details["dip_pressure_tx_count_current"] = pressure_analysis['total_swaps_analyzed']
        proximity_details["dip_pressure_tx_count_target"] = MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS
        proximity_details["dip_pressure_current_net_buys"] = pressure_analysis['net_buys']
        proximity_details["dip_pressure_target_net_buys"] = DIP_BUY_PRESSURE_MIN_NET_BUYS

        if pressure_analysis['total_swaps_analyzed'] < MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS:
             proximity_details["dip_pressure_tx_count_shortfall"] = MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS - pressure_analysis['total_swaps_analyzed']
             if ENABLE_DIP_BUY_PRESSURE_FILTER: # Only fail if filter is enabled and tx count is low (can be debated)
                # if passes_all_filters: final_fail_reason = final_fail_reason or "Dip Pressure Tx Count Low" # Optional: Make this a hard fail
                # passes_all_filters = False # Optional
                if VERBOSE_LOGGING: print(f"            [AdvBottom Filter Pressure] {symbol}: Insufficient tx for pressure filter. Scoring will reflect.")
        else:
            proximity_details["dip_pressure_tx_count_shortfall"] = 0

        if pressure_analysis['net_buys'] < DIP_BUY_PRESSURE_MIN_NET_BUYS:
            proximity_details["dip_pressure_net_buys_shortfall"] = DIP_BUY_PRESSURE_MIN_NET_BUYS - pressure_analysis['net_buys']
            if ENABLE_DIP_BUY_PRESSURE_FILTER:
                if passes_all_filters: final_fail_reason = final_fail_reason or "Low Dip Buy Pressure"
                passes_all_filters = False
        else:
             proximity_details["dip_pressure_net_buys_shortfall"] = 0


    # --- ATR and Whale Confirmation (only if not is_revalidation for hard fail, but always populate proximity) ---
    time_to_ohlcv = current_time; timeframe_duration_seconds = 3600
    ohlcv_tf_str = ATR_TIMEFRAME_OHLCV
    interval_s = get_ohlcv_interval_seconds(ohlcv_tf_str)
    if interval_s: timeframe_duration_seconds = interval_s

    time_from_ohlcv = time_to_ohlcv - ( (ATR_CANDLE_COUNT + 2) * timeframe_duration_seconds )
    ohlcv_data = pt_get_bds_ohlcv_base_quote(mint_addr, USDC_MINT_ADDRESS_SOLANA, ohlcv_tf_str, time_from_ohlcv, time_to_ohlcv)
    time.sleep(0.05)
    current_atr_value, current_atr_percent = None, None

    if ohlcv_data and len(ohlcv_data) >= ATR_CANDLE_COUNT:
        atr_result = calculate_atr_from_ohlcv(ohlcv_data, ATR_CANDLE_COUNT)
        if atr_result:
            current_atr_value, current_atr_percent = atr_result
            item['_current_atr_value'] = str(current_atr_value); item['_current_atr_percent'] = str(current_atr_percent)
            proximity_details["atr_value"] = float(current_atr_value)
            proximity_details["atr_percent"] = float(current_atr_percent)

    if current_atr_percent is None or current_atr_percent <= Decimal("0"):
        proximity_details["atr_calculation"] = "Failed or Zero"
        if not is_revalidation:
            if passes_all_filters: final_fail_reason = final_fail_reason or "ATR Calculation Failed"
            passes_all_filters = False
    else:
        dip_percent_to_check_abs = Decimal("0.0")
        if best_match_scenario_obj: # Only if a scenario was matched earlier
            dip_tfs_for_scenario = list(best_match_scenario_obj.get("timeframes_dip", {}).keys())
            primary_dip_tf_for_atr = dip_tfs_for_scenario[0] if dip_tfs_for_scenario else None
            if primary_dip_tf_for_atr:
                dip_value_for_atr = price_changes.get(primary_dip_tf_for_atr)
                if dip_value_for_atr is not None and dip_value_for_atr < 0:
                    dip_percent_to_check_abs = abs(dip_value_for_atr)
            if dip_percent_to_check_abs == Decimal("0.0"):
                for tf_key_atr in dip_tfs_for_scenario:
                    tf_dip_atr = price_changes.get(tf_key_atr)
                    if tf_dip_atr is not None and tf_dip_atr < 0:
                        dip_percent_to_check_abs = max(dip_percent_to_check_abs, abs(tf_dip_atr))

        proximity_details["atr_dip_current_dip_pct"] = float(dip_percent_to_check_abs)
        if dip_percent_to_check_abs == Decimal("0.0"):
            proximity_details["atr_dip_value"] = "No valid dip found in matched scenario TFs"
            if not is_revalidation:
                if passes_all_filters: final_fail_reason = final_fail_reason or f"No valid dip % for ATR check"
                passes_all_filters = False
        else:
            required_atr_dip_pct = ATR_ENTRY_MULTIPLIER * current_atr_percent
            proximity_details["atr_dip_target_requirement_pct"] = float(required_atr_dip_pct)
            if not (dip_percent_to_check_abs > required_atr_dip_pct):
                shortfall_val = float(required_atr_dip_pct - dip_percent_to_check_abs)
                proximity_details["atr_dip_confirmation_shortfall_pct"] = shortfall_val if shortfall_val > 0 else 0.0
                if not is_revalidation:
                    if passes_all_filters: final_fail_reason = final_fail_reason or "ATR Dip Confirmation Failed"
                    passes_all_filters = False
            else:
                proximity_details["atr_dip_confirmation_shortfall_pct"] = 0.0


    top_traders = pt_get_bds_token_top_traders(mint_addr, time_frame=f"{WHALE_BUY_LOOKBACK_MINUTES}m", limit=WHALE_TRADER_COUNT); time.sleep(0.05)
    whale_buy_confirmed_flag = False
    if top_traders:
        for trader in top_traders:
            if trader.get("tradeBuy", 0) > 0 and trader.get("volumeBuy", 0) > 0:
                whale_buy_confirmed_flag = True; item['_whale_buy_confirmed'] = True; break
    proximity_details["whale_buy_confirmed"] = whale_buy_confirmed_flag
    if not whale_buy_confirmed_flag and not is_revalidation:
        if passes_all_filters: final_fail_reason = final_fail_reason or "No Whale Buy Confirmation"
        passes_all_filters = False; # proximity_details["whale_buy_confirmation"] is already set to False (Missing)


    if VERBOSE_LOGGING:
        if passes_all_filters:
            print(f"    [AdvBottom Filter Pass] {symbol} ({mint_addr[:6]}): Passed all filters. Matched Scenario: '{matched_scenario_name}'")
        else:
            print(f"    [AdvBottom Filter Fail] {symbol} ({mint_addr[:6]}): Did not pass all. Reason: {final_fail_reason}. Matched Scenario: '{matched_scenario_name}' (if any)")

    return passes_all_filters, final_fail_reason, matched_scenario_name, scenario_score_bonus, proximity_details

# --- Ranking Function (AdvBottom) ---
def rank_trade_candidates(candidate_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scored_candidates = []
    if not candidate_items: return []
    print(f"    [*] [AdvBottom] Ranking {len(candidate_items)} candidates...")
    for item in candidate_items:
        score = Decimal("0.0"); reasons = []; score_details = {}
        market_data = item.get("bds_market_data") or {}
        trade_data = item.get("bds_trade_data") or {}
        price_changes: Dict[str, Decimal] = {}
        timeframe_map_rank = {
            "m1": "m1", "m5": "m5", "m15": "m15", "m30": "m30",
            "h1": "h1", "h2": "h2", "h4": "h4", "h6": "h6",
            "h8": "h8", "h12": "h12", "h24": "h24"
        }
        if trade_data:
            for scenario_tf_key, bds_v3_key in timeframe_map_rank.items():
                val = trade_data.get("priceChange", {}).get(bds_v3_key)
                if val is not None:
                    try: price_changes[scenario_tf_key] = Decimal(str(val))
                    except Exception as e:
                        if VERBOSE_LOGGING: print(f"        [AdvBottom Rank WARN] Could not convert priceChange.{bds_v3_key} value '{val}' to Decimal for {item.get('symbol','N/A')}: {e}")

        metadata_raw = item.get("bds_metadata"); metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
        original_data = item.get("original_correlation_data", {})
        scenario_bonus_str = item.get('_scenario_score_bonus', "0.0"); scenario_bonus = Decimal(scenario_bonus_str)
        if scenario_bonus > 0: score += scenario_bonus; reasons.append(f"Scenario:{item.get('_matched_dip_scenario_name', 'Unknown')}(+{scenario_bonus_str})"); score_details[f"Scenario:{item.get('_matched_dip_scenario_name', 'Unknown')}"] = scenario_bonus

        liq = market_data.get("liquidity"); liq_score = Decimal("0.0")
        if isinstance(liq, (int, float, Decimal)): # Allow Decimal
            liq_dec_val = Decimal(str(liq))
            if liq_dec_val > 50000: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.5")
            elif liq_dec_val > 10000: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.0")
            elif liq_dec_val >= FLEX_STRICT_MIN_LIQUIDITY_USD: liq_score = Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("0.5")
        if liq_score != Decimal("0.0"): score += liq_score; reasons.append(f"Liq:{_format_compact(liq,'$',0)}({liq_score:+.2f})"); score_details["Liquidity"] = liq_score

        vol24h = trade_data.get("volume", {}).get("h24") if trade_data else None # v3: data.volume.h24
        vol_liq_score = Decimal("0.0"); ratio_val_display = "N/A"
        if isinstance(liq, (int, float, Decimal)) and isinstance(vol24h, (int, float, Decimal)) and Decimal(str(liq)) > 1000:
            liq_dec = Decimal(str(liq)); vol24h_dec = Decimal(str(vol24h))
            ratio_val = vol24h_dec / (liq_dec if liq_dec != Decimal(0) else Decimal('inf')); ratio_val_display = f"{ratio_val:.1f}x"
            if ratio_val > 10: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.5")
            elif ratio_val > 5: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.0")
            elif ratio_val > 2: vol_liq_score = Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("0.5")
        if vol_liq_score != Decimal("0.0"): score += vol_liq_score; reasons.append(f"V/L:{ratio_val_display}({vol_liq_score:+.2f})"); score_details["Vol/Liq Ratio"] = vol_liq_score

        positive_st_score = Decimal("0.0"); m5_change = price_changes.get("m5"); h1_change = price_changes.get("h1")
        if m5_change is not None and m5_change > 0: positive_st_score += Decimal(str(SCORE_WEIGHTS["price_change_positive_short_term"])) * min(m5_change / Decimal("5.0"), Decimal("1.0"))
        if h1_change is not None and h1_change > 0: positive_st_score += Decimal(str(SCORE_WEIGHTS["price_change_positive_short_term"])) * min(h1_change / Decimal("10.0"), Decimal("1.0"))
        if positive_st_score != Decimal("0.0"): score += positive_st_score; reasons.append(f"STPos({positive_st_score:+.2f})"); score_details["ShortTermPositive"] = positive_st_score

        total_holders_val = trade_data.get("holderCount") if trade_data else None; th_score = Decimal("0.0") # v3: data.holderCount
        if isinstance(total_holders_val, int):
            if total_holders_val > 1000: th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("1.0")
            elif total_holders_val > 250: th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("0.5")
            elif total_holders_val >= FLEX_STRICT_MIN_TOTAL_HOLDERS : th_score = Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("0.2")
        if th_score != Decimal("0.0"): score += th_score; reasons.append(f"Hdrs:{total_holders_val}({th_score:+.2f})"); score_details["TotalHolders"] = th_score

        monitored_holders_val = original_data.get("total_holders_in_set", 0); mh_score = Decimal("0.0")
        if FLEX_STRICT_MIN_MONITORED_HOLDERS == 0 or monitored_holders_val >= FLEX_STRICT_MIN_MONITORED_HOLDERS :
            if monitored_holders_val > 2: mh_score = Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("1.0")
            elif monitored_holders_val >= 1 : mh_score = Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("0.5")
        if mh_score != Decimal("0.0"): score += mh_score; reasons.append(f"MonHdrs:{monitored_holders_val}({mh_score:+.2f})"); score_details["MonitoredHolders"] = mh_score

        top1_pct_val = market_data.get("topHolderPercentage"); top1_score = Decimal("0.0") # v3: data.topHolderPercentage (0 to 1)
        if isinstance(top1_pct_val, (float, Decimal)): # Allow Decimal
            top1_pct_dec = Decimal(str(top1_pct_val))
            if top1_pct_dec < Decimal("0.30"): top1_score = Decimal(str(abs(float(SCORE_WEIGHTS["top1_holder_pct"])))) * Decimal("0.5")
            elif top1_pct_dec >= Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)):
                penalty_denominator = (Decimal("1.0") - Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)));
                penalty_factor_numerator = (top1_pct_dec - Decimal(str(FLEX_STRICT_MAX_TOP1_HOLDER_PCT)))
                penalty_factor = min(penalty_factor_numerator / (penalty_denominator if penalty_denominator != Decimal(0) else Decimal('inf')), Decimal("1.0"))
                top1_score = Decimal(str(SCORE_WEIGHTS["top1_holder_pct"])) * penalty_factor
        if top1_score != Decimal("0.0"): score += top1_score; reasons.append(f"Top1:{float(top1_pct_val)*100 if top1_pct_val is not None else 'N/A'}:.0f%({top1_score:+.2f})"); score_details["Top1HolderPct"] = top1_score

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
        if age_score_val !=Decimal("0.0"): score += age_score_val; reasons.append(f"Age({age_score_val:+.2f})"); score_details["AgeScore"] = age_score_val

        extensions_raw = metadata.get("extensions"); extensions = extensions_raw if isinstance(extensions_raw, dict) else {}
        has_social = any(k in extensions for k in ["website", "twitter", "telegram"] if extensions.get(k)); socials_score = Decimal("0.0")
        if has_social: socials_score = Decimal(str(SCORE_WEIGHTS["socials"]))
        if socials_score != Decimal("0.0"): score += socials_score; reasons.append(f"Socials({socials_score:+.2f})"); score_details["SocialsScore"] = socials_score

        dip_buy_pressure_score_val = Decimal("0.0")
        weight_for_pressure = Decimal(str(SCORE_WEIGHTS.get("dip_buy_pressure", "0.0")))
        if weight_for_pressure != Decimal("0.0"):
            net_buys = item.get('_dip_net_buys', 0); tx_analyzed = item.get('_dip_tx_analyzed_count', 0)
            if tx_analyzed >= MIN_TRANSACTIONS_FOR_PRESSURE_ANALYSIS:
                if net_buys > 0 and tx_analyzed > 0:
                    score_factor = min(Decimal(str(net_buys)) / (Decimal(str(tx_analyzed)) / Decimal("2.0")), Decimal("1.5"))
                    dip_buy_pressure_score_val = weight_for_pressure * score_factor
            if dip_buy_pressure_score_val != Decimal("0.0"):
                score += dip_buy_pressure_score_val
                reasons.append(f"Pressure(N:{net_buys}/T:{tx_analyzed})({dip_buy_pressure_score_val:+.2f})")
                score_details["DipBuyPressure"] = dip_buy_pressure_score_val

        if item.get('_current_atr_value') and SCORE_WEIGHTS.get("atr_dip_confirmation_bonus", Decimal("0.0")) > 0:
            atr_bonus = SCORE_WEIGHTS.get("atr_dip_confirmation_bonus", Decimal("0.0"))
            score += atr_bonus
            reasons.append(f"ATRConf({atr_bonus:+.2f})")
            score_details["ATRConfirmationBonus"] = atr_bonus
        if item.get('_whale_buy_confirmed') and SCORE_WEIGHTS.get("whale_buy_confirmation_bonus", Decimal("0.0")) > 0:
            whale_bonus = SCORE_WEIGHTS.get("whale_buy_confirmation_bonus", Decimal("0.0"))
            score += whale_bonus
            reasons.append(f"WhaleBuy({whale_bonus:+.2f})")
            score_details["WhaleBuyConfirmationBonus"] = whale_bonus

        item['_score'] = score; item['_score_reasons'] = reasons; item['_score_details'] = score_details
        scored_candidates.append(item)

    scored_candidates.sort(key=lambda x: x['_score'], reverse=True)
    print(f"    [*] [AdvBottom] Ranking Complete. Top {min(3, len(scored_candidates))} candidates:")
    for i, item_ranked in enumerate(scored_candidates[:3]):
        print(f"        {i+1}. {item_ranked.get('symbol','???')} ({item_ranked.get('token_mint')[:6]}...) Score: {item_ranked['_score']:.2f} (Reasons: {', '.join(item_ranked.get('_score_reasons',[]))})")
        if VERBOSE_LOGGING and '_score_details' in item_ranked:
            detail_str_parts = []
            for k,v_dec in item_ranked['_score_details'].items():
                if isinstance(v_dec, Decimal): v_fmt = f"{v_dec:+.2f}"
                else: v_fmt = str(v_dec)
                detail_str_parts.append(f"{k}:{v_fmt}")
            detail_str = ", ".join(detail_str_parts); print(f"           Score Details: {detail_str}")
    return scored_candidates

# --- Trade Entry, Portfolio Management, etc. (AdvBottom) ---
def enter_paper_trade(mint: str, symbol: str, name: str, entry_price: Decimal, state: Dict[str, Any], matched_dip_scenario: Optional[str], candidate_item: Dict[str, Any]) -> bool:
    current_balance = Decimal(state['current_balance_usd'])
    initial_capital = Decimal(state['initial_capital_usd'])
    risk_level = state['risk_level']

    allocation_pct: Decimal; capital_base: Decimal; tp_pct: Decimal
    if risk_level == 0:
        allocation_pct = RISK_LEVEL_0_ALLOCATION_PCT
        capital_base = initial_capital
        tp_pct = RISK_LEVEL_0_TP_PCT
    elif risk_level == 1:
        allocation_pct = RISK_LEVEL_1_ALLOCATION_PCT
        capital_base = current_balance
        tp_pct = RISK_LEVEL_1_TP_PCT
    else: # risk_level == 2
        allocation_pct = RISK_LEVEL_2_ALLOCATION_PCT
        capital_base = current_balance
        tp_pct = RISK_LEVEL_2_TP_PCT

    position_size_usd = capital_base * allocation_pct
    position_size_usd_after_fee = position_size_usd * (Decimal("1") - SIMULATED_FEE_PCT)
    simulated_entry_price = entry_price * (Decimal("1") + SIMULATED_SLIPPAGE_PCT)

    if simulated_entry_price <= 0:
        print(f"    [!] [AdvBottom] Invalid simulated entry price for {symbol}.")
        return False

    position_size_tokens = position_size_usd_after_fee / simulated_entry_price
    entry_atr_value, entry_atr_percent = None, None

    if candidate_item.get('_current_atr_value') and candidate_item.get('_current_atr_percent'):
        try:
            entry_atr_value = Decimal(candidate_item['_current_atr_value'])
            entry_atr_percent = Decimal(candidate_item['_current_atr_percent'])
        except Exception:
            if VERBOSE_LOGGING:
                print(f"    [!] [AdvBottom] Error converting stored ATR from candidate_item to Decimal for {symbol}.")

    if entry_atr_value is None or entry_atr_value <= Decimal("0"):
        if VERBOSE_LOGGING:
            print(f"    [AdvBottom] ATR from candidate_item invalid or missing for {symbol}. Fetching fresh ATR for entry SL.")
        current_time_entry = int(time.time())
        time_to_ohlcv = current_time_entry
        timeframe_duration_seconds_entry = 3600

        interval_s_entry = get_ohlcv_interval_seconds(ATR_TIMEFRAME_OHLCV)
        if interval_s_entry: timeframe_duration_seconds_entry = interval_s_entry

        time_from_ohlcv = time_to_ohlcv - ( (ATR_CANDLE_COUNT + 2) * timeframe_duration_seconds_entry )

        ohlcv_data = pt_get_bds_ohlcv_base_quote(mint, USDC_MINT_ADDRESS_SOLANA, ATR_TIMEFRAME_OHLCV, time_from_ohlcv, time_to_ohlcv)
        if ohlcv_data and len(ohlcv_data) >= ATR_CANDLE_COUNT:
            atr_result = calculate_atr_from_ohlcv(ohlcv_data, ATR_CANDLE_COUNT)
            if atr_result:
                entry_atr_value, entry_atr_percent = atr_result

    if entry_atr_value is None or entry_atr_value <= Decimal("0"):
        print(f"    [!] [AdvBottom] Could not determine valid ATR for {symbol} at entry. Aborting trade.")
        return False

    initial_stop_loss_price = simulated_entry_price - (entry_atr_value * ATR_STOP_LOSS_MULTIPLIER)
    if initial_stop_loss_price >= simulated_entry_price:
        print(f"    [!] [AdvBottom] ATR based SL ({_format_compact(initial_stop_loss_price, '$', 6)}) is not below entry price ({_format_compact(simulated_entry_price, '$', 6)}). Defaulting SL. ATR Val: {entry_atr_value:.4f}")
        initial_stop_loss_price = simulated_entry_price * (Decimal("1") - Decimal("0.10"))
        if initial_stop_loss_price >= simulated_entry_price:
             print(f"    [!] [AdvBottom] Fallback SL also invalid. Aborting trade.")
             return False

    take_profit_price = simulated_entry_price * (Decimal("1") + tp_pct)

    state['open_positions'][mint] = {
        "symbol": symbol, "name": name, "entry_price": str(simulated_entry_price),
        "entry_timestamp": int(time.time()), "position_size_usd": str(position_size_usd),
        "position_size_tokens": str(position_size_tokens), "initial_stop_loss_price": str(initial_stop_loss_price),
        "take_profit_price": str(take_profit_price), "risk_level": risk_level, "status": "OPEN",
        "highest_price_seen": str(simulated_entry_price), "dip_scenario_at_entry": matched_dip_scenario,
        "entry_atr_value_at_entry": str(entry_atr_value),
        "entry_atr_pct_at_entry": str(entry_atr_percent if entry_atr_percent is not None else "N/A"),
        "current_atr_value": str(entry_atr_value), "last_mint_burn_check_ts": 0,
        "last_order_book_check_ts": 0, "median_spread_initial": None,
        "last_whale_sell_check_ts": 0, "last_sell_pressure_check_ts":0,
        "last_atr_update_ts": int(time.time())
    }

    log_entry_price_fmt = _format_compact(simulated_entry_price, '$', 6)
    log_sl_fmt = _format_compact(initial_stop_loss_price, '$', 6)
    log_tp_fmt = _format_compact(take_profit_price, '$', 6)

    print(f"    [+] [AdvBottom] Entered PAPER TRADE for {symbol} ({mint[:6]}...) Scenario: {matched_dip_scenario}")
    print(f"        Size: {position_size_usd:.2f} USD | Entry: ~{log_entry_price_fmt} | ATR SL: {log_sl_fmt} (ATR Val: {entry_atr_value:.4f}) | TP: {log_tp_fmt} | Risk: {risk_level}")

    symbol_safe = escape_markdown_original(symbol)
    tg_entry_price_fmt = _format_compact(simulated_entry_price, '$', 5)
    tg_sl_fmt = _format_compact(initial_stop_loss_price, '$', 5)
    tg_tp_fmt = _format_compact(take_profit_price, '$', 5)
    size_fmt = f"${position_size_usd:.2f}"
    dex_screener_link = f"{DEXSCREENER_BASE_URL_SOLANA}{mint}"

    scenario_text_tg = f"\nScenario: {escape_markdown_original(matched_dip_scenario)}" if matched_dip_scenario else ""
    # Using Markdown link syntax [Text](URL). Ensure your send_trader_telegram handles MarkdownV2 or equivalent.
    # If not, just append dex_screener_link as a raw string.
    alert_text = (f"➡️ Entered Paper Trade: *{symbol_safe}*\n"
                  f"Mint: `{escape_markdown_original(mint)}`\n"
                  f"Entry Price: {tg_entry_price_fmt}\n"
                  f"Size: {size_fmt}\n"
                  f"ATR SL: {tg_sl_fmt} | TP: {tg_tp_fmt}\n"
                  f"Risk Level: {risk_level}{scenario_text_tg}\n"
                  f"Chart: {dex_screener_link}") # Appending raw link
    send_trader_telegram(alert_text)
    return True


def manage_pending_entries(state: Dict[str, Any]):
    if 'pending_entry' not in state or not state['pending_entry']: return
    if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Managing {len(state['pending_entry'])} pending entries...")
    else: print(f"    [*] [AdvBottom] Managing {len(state['pending_entry'])} pending entries...")

    mints_to_process = list(state['pending_entry'].keys()); current_time = time.time()
    for mint in mints_to_process:
        if mint not in state['pending_entry']: continue
        pending_data = state['pending_entry'][mint]; symbol = pending_data.get('symbol', mint[:6])
        original_matched_scenario = pending_data.get('matched_dip_scenario_name', 'N/A_pending')
        if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Checking pending: {symbol} ({mint[:6]}). Scenario: {original_matched_scenario}")
        monitoring_start_time = pending_data.get('monitoring_start_time', 0)
        entry_window_seconds = pending_data.get('entry_window_seconds', PENDING_ENTRY_WINDOW_SECONDS)
        if current_time > monitoring_start_time + entry_window_seconds:
            print(f"    [-] [AdvBottom] Pending entry window expired for {symbol}. Removing."); del state['pending_entry'][mint]; continue
        op_halt_active = (state['daily_quota_achieved'] or state.get('daily_loss_limit_hit', False) or len(state['open_positions']) >= MAX_OPEN_POSITIONS or current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS or current_time < state.get('kill_switch_active_until_ts', 0))
        if op_halt_active:
            if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Operational halt active for {symbol}, skipping pending check.");
            continue
        current_price_quick = get_current_price(mint); time.sleep(0.05)
        if current_price_quick is None: print(f"    [!] [AdvBottom] Could not get quick price for pending entry {symbol}."); continue
        price_condition_met = False; signal_price_str = pending_data.get('signal_price')
        if signal_price_str and signal_price_str != 'N/A':
            try:
                signal_price = Decimal(signal_price_str); lower_bound = signal_price * (Decimal(1) - PENDING_ENTRY_PRICE_DEVIATION_PCT); upper_bound = signal_price * (Decimal(1) + PENDING_ENTRY_PRICE_DEVIATION_PCT)
                price_condition_met = (lower_bound <= current_price_quick <= upper_bound)
                if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] {symbol} Price Check: QuickPrice={_format_compact(current_price_quick, '$', 6)}, Signal={_format_compact(signal_price, '$', 6)}, Lower={_format_compact(lower_bound, '$', 6)}, Upper={_format_compact(upper_bound, '$', 6)} -> Met: {price_condition_met}")
            except Exception as e: print(f"    [!] [AdvBottom] Error checking price deviation for {symbol}: {e}"); continue
            if not price_condition_met:
                if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] {symbol} price {_format_compact(current_price_quick, '$', 6)} outside signal deviation.")
                continue
        else: price_condition_met = True;
        if VERBOSE_LOGGING and price_condition_met and not (signal_price_str and signal_price_str != 'N/A'): print(f"        [AdvBottom Verbose] {symbol} no signal price, proceeding to revalidation.")
        if not price_condition_met: continue

        print(f"    [>] [AdvBottom] Price condition met for {symbol}. Fetching full data for re-validation...")
        fresh_enriched_data = get_enriched_data_for_pending_candidate(mint)
        if not fresh_enriched_data: print(f"    [!] [AdvBottom] Failed to fetch fresh enriched data for {symbol} for re-validation."); continue

        if 'original_correlation_data' in pending_data: fresh_enriched_data['original_correlation_data'] = pending_data['original_correlation_data']
        if pending_data.get("_current_atr_value"): fresh_enriched_data["_current_atr_value"] = pending_data["_current_atr_value"]
        if pending_data.get("_current_atr_percent"): fresh_enriched_data["_current_atr_percent"] = pending_data["_current_atr_percent"]

        passes_final_check, reason, reval_matched_scenario, _, prox_reval = passes_strict_filters(fresh_enriched_data, int(current_time), is_revalidation=True)

        if not passes_final_check:
            print(f"    [!] [AdvBottom] {symbol} ({mint[:6]}) failed final re-validation: {reason}. Removing from pending."); del state['pending_entry'][mint]; continue

        if VERBOSE_LOGGING and original_matched_scenario != reval_matched_scenario:
            print(f"        [AdvBottom Verbose] {symbol} scenario changed on reval: '{original_matched_scenario}' -> '{reval_matched_scenario}'")
        print(f"    [✓] [AdvBottom] {symbol} ({mint[:6]}) passed final re-validation. Matched Scenario: '{reval_matched_scenario}'")

        current_price_from_market = (fresh_enriched_data.get("bds_market_data") or {}).get("price"); price_to_enter = current_price_quick
        if current_price_from_market is not None:
            try: price_to_enter = Decimal(str(current_price_from_market))
            except Exception: pass

        trade_name = fresh_enriched_data.get("name", pending_data.get('name', 'N/A'))

        if enter_paper_trade(mint, symbol, trade_name, price_to_enter, state, reval_matched_scenario, fresh_enriched_data):
            print(f"    [>>>] [AdvBottom] Successfully executed paper trade for {symbol}."); del state['pending_entry'][mint]
        else: print(f"    [!] [AdvBottom] Failed to execute paper trade for {symbol} despite passing re-validation.")

def manage_portfolio(state: Dict[str, Any]):
    if 'open_positions' in state and state['open_positions']:
        if VERBOSE_LOGGING:
            print(f"    [AdvBottom Verbose] Managing {len(state['open_positions'])} open positions...")
        else:
            print(f"    [*] [AdvBottom] Managing {len(state['open_positions'])} open positions...")
    else:
        return

    mints_to_check = list(state['open_positions'].keys())
    closed_positions_this_cycle = 0
    current_time = int(time.time())

    open_mints = [m for m in mints_to_check if state['open_positions'].get(m) and state['open_positions'][m].get('status') == 'OPEN']
    current_prices_map = {}
    if open_mints:
        for m_addr in open_mints:
            cp = get_current_price(m_addr)
            if cp is not None:
                current_prices_map[m_addr] = cp
            time.sleep(0.05)

    for mint in mints_to_check:
        position = state['open_positions'].get(mint)
        if not position or position.get('status') != 'OPEN':
            continue

        symbol = position.get('symbol', mint[:6])
        symbol_safe = escape_markdown_original(symbol)
        mint_short_safe = escape_markdown_original(mint[:4]+"..") # Used for display if full mint isn't shown

        dip_scenario_at_entry = position.get("dip_scenario_at_entry", "N/A_open")

        entry_price = Decimal(position['entry_price'])
        take_profit_price = Decimal(position['take_profit_price'])

        position_size_tokens = Decimal(position['position_size_tokens'])
        position_size_usd_entry = Decimal(position['position_size_usd'])

        entry_timestamp = position['entry_timestamp']
        highest_price_seen = Decimal(position.get('highest_price_seen', position['entry_price']))

        current_price = current_prices_map.get(mint)
        if current_price is None:
            if VERBOSE_LOGGING:
                print(f"    [!] [AdvBottom Portfolio] Could not get current price for open position {symbol} from batched fetch. Skipping checks for this cycle.")
            continue

        highest_price_seen = max(highest_price_seen, current_price)
        position['highest_price_seen'] = str(highest_price_seen)

        current_atr_value_for_sl = Decimal(position.get("current_atr_value", position.get("entry_atr_value_at_entry", "0")))
        atr_update_interval = 60 * 60
        if current_time - position.get('last_atr_update_ts', 0) > atr_update_interval:
            ohlcv_time_to = current_time
            timeframe_duration_seconds_update = 3600

            interval_s_update = get_ohlcv_interval_seconds(ATR_TIMEFRAME_OHLCV)
            if interval_s_update: timeframe_duration_seconds_update = interval_s_update

            ohlcv_time_from = ohlcv_time_to - ( (ATR_CANDLE_COUNT + 2) * timeframe_duration_seconds_update )

            ohlcv_data_update = pt_get_bds_ohlcv_base_quote(mint, USDC_MINT_ADDRESS_SOLANA, ATR_TIMEFRAME_OHLCV, ohlcv_time_from, ohlcv_time_to)
            time.sleep(0.05)
            if ohlcv_data_update and len(ohlcv_data_update) >= ATR_CANDLE_COUNT:
                atr_res_update = calculate_atr_from_ohlcv(ohlcv_data_update, ATR_CANDLE_COUNT)
                if atr_res_update and atr_res_update[0] > 0:
                    current_atr_value_for_sl = atr_res_update[0]
                    position['current_atr_value'] = str(current_atr_value_for_sl)
                    position['last_atr_update_ts'] = current_time
                    if VERBOSE_LOGGING:
                        print(f"        [AdvBottom ATR Update] {symbol} ATR updated to: {current_atr_value_for_sl:.4f}")

        initial_sl_price_fixed = Decimal(position["initial_stop_loss_price"])

        effective_stop_loss_price = initial_sl_price_fixed
        if ATR_TRAILING_ENABLED and current_atr_value_for_sl > 0:
            trailing_stop_price_candidate = highest_price_seen - (current_atr_value_for_sl * ATR_STOP_LOSS_MULTIPLIER)
            effective_stop_loss_price = max(initial_sl_price_fixed, trailing_stop_price_candidate)

        position['effective_stop_loss_price'] = str(effective_stop_loss_price)

        exit_reason = None
        exit_price = None

        current_price_fmt_log = _format_compact(current_price, '$', 6)
        effective_sl_fmt_log = _format_compact(effective_stop_loss_price, '$', 6)
        tp_price_fmt_log = _format_compact(take_profit_price, '$', 6)

        if current_price <= effective_stop_loss_price:
            exit_reason = "ATR_STOP_LOSS"
            exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
            print(f"    [!] [AdvBottom] ATR STOP LOSS for {symbol} at {current_price_fmt_log} (SL: {effective_sl_fmt_log})")
            state['last_stop_loss_timestamp'] = current_time
        elif current_price >= take_profit_price:
            exit_reason = "TAKE_PROFIT"
            exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
            print(f"    [+] [AdvBottom] TAKE PROFIT for {symbol} at {current_price_fmt_log} (TP: {tp_price_fmt_log})")

        if not exit_reason and current_time - position.get('last_whale_sell_check_ts', 0) > (WHALE_SELL_LOOKBACK_MINUTES * 60 / 2):
            position['last_whale_sell_check_ts'] = current_time
            top_traders = pt_get_bds_token_top_traders(mint, time_frame=f"{WHALE_SELL_LOOKBACK_MINUTES}m", limit=WHALE_TRADER_COUNT)
            time.sleep(0.05)
            if top_traders:
                whale_trader_addresses = [t.get('owner') for t in top_traders if t.get('owner')]
                for trader_addr in whale_trader_addresses:
                    time_before_trader_txs = current_time
                    time_after_trader_txs = current_time - (WHALE_SELL_LOOKBACK_MINUTES * 60)

                    trader_txs = pt_get_bds_trader_txs_seek_by_time(trader_addr, time_before_trader_txs, time_after_trader_txs, limit=20)
                    time.sleep(0.05)
                    if trader_txs:
                        for tx in trader_txs:
                            if tx.get("tx_type") == "swap":
                                sold_our_token = False
                                sold_value_usd = Decimal("0")

                                base_tx_info = tx.get("base", {})
                                quote_tx_info = tx.get("quote", {})

                                if base_tx_info.get("address") == mint and Decimal(str(base_tx_info.get("ui_change_amount","0"))) < 0:
                                    sold_our_token = True
                                    amount_sold_ui_our_token = abs(Decimal(str(base_tx_info.get("ui_change_amount","0"))))
                                    if quote_tx_info.get("address") == USDC_MINT_ADDRESS_SOLANA:
                                        amount_stable_received = abs(Decimal(str(quote_tx_info.get("ui_change_amount","0"))))
                                        sold_value_usd = amount_stable_received
                                    else:
                                        sold_value_usd = amount_sold_ui_our_token * current_price
                                elif quote_tx_info.get("address") == mint and Decimal(str(quote_tx_info.get("ui_change_amount","0"))) < 0:
                                    sold_our_token = True
                                    amount_sold_ui_our_token = abs(Decimal(str(quote_tx_info.get("ui_change_amount","0"))))
                                    if base_tx_info.get("address") == USDC_MINT_ADDRESS_SOLANA:
                                        amount_stable_received = abs(Decimal(str(base_tx_info.get("ui_change_amount","0"))))
                                        sold_value_usd = amount_stable_received
                                    else:
                                        sold_value_usd = amount_sold_ui_our_token * current_price

                                if sold_our_token and sold_value_usd >= WHALE_SELL_USD_THRESHOLD:
                                    exit_reason = "WHALE_SELL_DETECTED"
                                    exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
                                    print(f"    [!] [AdvBottom] WHALE SELL by {trader_addr[:6]} for {symbol} (Approx Val: ${_format_compact(sold_value_usd, '', 0)}). Exiting.")
                                    break
                    if exit_reason:
                        break

        if not exit_reason and current_time - position.get('last_mint_burn_check_ts', 0) > MINT_BURN_CHECK_INTERVAL_SECONDS:
            position['last_mint_burn_check_ts'] = current_time
            mint_burn_after_time = max(entry_timestamp, current_time - (MINT_EVENT_LOOKBACK_MINUTES * 60))
            mint_burn_txs = pt_get_bds_token_mint_burn_txs(mint, limit=5, after_time=mint_burn_after_time)
            time.sleep(0.05)
            if mint_burn_txs:
                for tx in mint_burn_txs:
                    if tx.get("common_type") == "mint" and tx.get("block_time", 0) >= entry_timestamp:
                        exit_reason = "MINT_EVENT_DETECTED"
                        exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
                        print(f"    [!] [AdvBottom] MINT EVENT for {symbol} (Amount: {tx.get('ui_amount_string','N/A')}). Exiting.")
                        break

        if not exit_reason and current_time - position.get('last_sell_pressure_check_ts', 0) > (SELL_PRESSURE_LOOKBACK_MINUTES * 60 / 2) :
            position['last_sell_pressure_check_ts'] = current_time
            recent_token_txs = get_recent_swap_transactions(mint, limit=100, max_age_minutes=SELL_PRESSURE_LOOKBACK_MINUTES)
            time.sleep(0.05)
            if recent_token_txs:
                value_pressure = analyze_transaction_value_pressure(recent_token_txs, mint)
                s_usd = value_pressure['sell_usd_volume']
                b_usd = value_pressure['buy_usd_volume']
                if VERBOSE_LOGGING:
                    print(f"        [AdvBottom SellPressure] {symbol}: SellVol=${s_usd:.2f}, BuyVol=${b_usd:.2f} (in last {SELL_PRESSURE_LOOKBACK_MINUTES}m)")
                if s_usd > b_usd * SELL_PRESSURE_RATIO_THRESHOLD and s_usd > SELL_PRESSURE_MIN_SELL_USD_THRESHOLD :
                    exit_reason = "HIGH_SELL_PRESSURE_USD"
                    exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
                    print(f"    [!] [AdvBottom] HIGH SELL PRESSURE for {symbol} (Sell USD: {_format_compact(s_usd,'$',0)} vs Buy USD: {_format_compact(b_usd,'$',0)}). Exiting.")

        if not exit_reason and current_time - position.get('last_order_book_check_ts', 0) > ORDER_BOOK_CHECK_INTERVAL_SECONDS:
            position['last_order_book_check_ts'] = current_time
            market_snapshot = pt_get_bds_market_data(mint)
            time.sleep(0.05)
            if market_snapshot:
                bid_price_str = market_snapshot.get("bidPrice")
                ask_price_str = market_snapshot.get("askPrice")
                bid_qty_str = market_snapshot.get("bidQuantity")

                if bid_price_str and ask_price_str:
                    try:
                        bid_p = Decimal(str(bid_price_str))
                        ask_p = Decimal(str(ask_price_str))
                        if bid_p > 0 and ask_p > bid_p :
                            current_spread_pct = ((ask_p - bid_p) / bid_p) * 100
                            if position.get("median_spread_initial") is None:
                                position["median_spread_initial"] = str(current_spread_pct)

                            initial_spread_pct_str = position.get("median_spread_initial", "inf")
                            if initial_spread_pct_str != "inf":
                                initial_spread_pct = Decimal(initial_spread_pct_str)
                                if current_spread_pct > ORDER_BOOK_SPREAD_MULTIPLIER_THRESHOLD * initial_spread_pct:
                                     exit_reason = "SPREAD_WIDENED"
                                     exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
                                     print(f"    [!] [AdvBottom] SPREAD WIDENED for {symbol} (Curr: {current_spread_pct:.2f}%, Init: {initial_spread_pct:.2f}%). Exiting.")
                    except Exception as e_spread:
                        if VERBOSE_LOGGING:
                            print(f"        [AdvBottom SpreadCheck] Error calculating spread for {symbol}: {e_spread}")

                if not exit_reason and bid_qty_str and bid_price_str :
                    try:
                        bid_depth_tokens = Decimal(str(bid_qty_str))
                        bid_p_for_depth = Decimal(str(bid_price_str))
                        bid_depth_usd = bid_depth_tokens * bid_p_for_depth
                        if VERBOSE_LOGGING:
                            print(f"        [AdvBottom DepthCheck] {symbol} Top Bid Depth: {_format_compact(bid_depth_usd,'$',0)}")
                        if bid_depth_usd < ORDER_BOOK_MIN_BID_DEPTH_USD:
                            exit_reason = "LOW_BID_DEPTH"
                            exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT)
                            print(f"    [!] [AdvBottom] LOW BID DEPTH for {symbol} ({_format_compact(bid_depth_usd,'$',0)} < Threshold). Exiting.")
                    except Exception as e_depth:
                         if VERBOSE_LOGGING:
                            print(f"        [AdvBottom DepthCheck] Error calculating bid depth for {symbol}: {e_depth}")

        if exit_reason and exit_price is not None:
            closed_positions_this_cycle += 1
            if exit_price <= Decimal("0"):
                print(f"    [!] [AdvBottom Portfolio] Invalid exit price ({exit_price}) for {symbol}. Using entry price for PNL to avoid errors, but this is a problem.")
                exit_price = entry_price

            exit_value_usd = position_size_tokens * exit_price
            exit_value_usd_after_fee = exit_value_usd * (Decimal("1") - SIMULATED_FEE_PCT)
            pnl_usd = exit_value_usd_after_fee - position_size_usd_entry
            pnl_pct = (pnl_usd / position_size_usd_entry) * 100 if position_size_usd_entry > 0 else Decimal("0")

            state['current_balance_usd'] = str(Decimal(state['current_balance_usd']) + pnl_usd)
            state['realized_pnl_usd'] = str(Decimal(state['realized_pnl_usd']) + pnl_usd)

            pnl_usd_fmt = f"{pnl_usd:+.2f}"
            pnl_pct_fmt = f"{pnl_pct:+.1f}"
            entry_price_fmt = _format_compact(entry_price, '$', 6)
            exit_price_fmt = _format_compact(exit_price, '$', 6)
            portfolio_fmt = _format_compact(state['current_balance_usd'], '$', 2)
            dex_screener_link = f"{DEXSCREENER_BASE_URL_SOLANA}{mint}"


            if (exit_reason == "ATR_STOP_LOSS" or "SELL_DETECTED" in exit_reason or "PRESSURE" in exit_reason or "MINT_EVENT" in exit_reason or "SPREAD_WIDENED" in exit_reason or "LOW_BID_DEPTH" in exit_reason) \
               and pnl_pct < Decimal("0") and abs(pnl_pct / 100) >= KILL_SWITCH_LOSS_PCT :
                 kill_switch_until = current_time + KILL_SWITCH_DURATION_SECONDS
                 state['kill_switch_active_until_ts'] = kill_switch_until
                 kill_switch_msg = f"🚨 *[AdvBottom] KILL SWITCH!* 🚨\nTrade: {symbol_safe}\nLoss: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\nReason: {exit_reason}\nHalting for {KILL_SWITCH_DURATION_SECONDS // 60} mins."
                 print(f"    [!!!] [AdvBottom] KILL SWITCH by {symbol} loss ({pnl_pct:.1f}%). Reason: {exit_reason}.")
                 send_trader_telegram(kill_switch_msg)

            closed_trade_data = {
                "mint": mint, "symbol": symbol, "entry_timestamp": entry_timestamp,
                "exit_timestamp": current_time, "entry_price": str(entry_price),
                "exit_price": str(exit_price), "position_size_usd": str(position_size_usd_entry),
                "position_size_tokens": str(position_size_tokens), "pnl_usd": str(pnl_usd),
                "exit_reason": exit_reason, "risk_level_at_entry": position.get("risk_level"),
                "pnl_pct": f"{pnl_pct:.2f}", "dip_scenario_at_entry": dip_scenario_at_entry,
                "entry_atr_value_at_entry": position.get("entry_atr_value_at_entry", "N/A"),
                "entry_atr_pct_at_entry": position.get("entry_atr_pct_at_entry", "N/A")
            }
            if 'trade_history' not in state:
                state['trade_history'] = []
            state['trade_history'].append(closed_trade_data)
            log_trade_to_csv(closed_trade_data)
            del state['open_positions'][mint]

            scenario_text_tg_closed = f"\nScenario: {escape_markdown_original(dip_scenario_at_entry)}" if dip_scenario_at_entry and dip_scenario_at_entry != "N/A_open" else ""
            alert_text = (f"Closed {symbol_safe} (`{escape_markdown_original(mint)}`)\n"
                          f"Reason: {exit_reason}\n"
                          f"PnL: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\n"
                          f"Entry: {entry_price_fmt} | Exit: {exit_price_fmt}{scenario_text_tg_closed}\n"
                          f"Portfolio: {portfolio_fmt} USD\n"
                          f"Chart: {dex_screener_link}") # Appending raw link
            send_trader_telegram(alert_text)

            if pnl_usd > 0:
                check_and_update_risk_level(state)

    if closed_positions_this_cycle > 0:
        print(f"    [*] [AdvBottom] Closed {closed_positions_this_cycle} position(s) this cycle.")
        calculate_performance_metrics(state)

# --- Risk Level & Summary (Retained, prefix updated) ---
def check_and_update_risk_level(state: Dict[str, Any]):
    current_balance = Decimal(state['current_balance_usd'])
    initial_capital = Decimal(state['initial_capital_usd'])
    current_risk_level = state['risk_level']
    daily_quota_achieved = state['daily_quota_achieved']
    new_risk_level = current_risk_level
    new_quota_status = daily_quota_achieved

    balance_fmt = _format_compact(current_balance, '$', 2)
    if current_risk_level == 0 and current_balance >= initial_capital * RISK_LEVEL_1_ENTRY_THRESHOLD:
        new_risk_level = 1
        msg = f"🚀 Risk Level Increased: 0 -> 1\nBalance: {balance_fmt} USD"
        print("    [!] [AdvBottom] Risk Level Increased: 0 -> 1")
        send_trader_telegram(msg)
    elif current_risk_level == 1 and current_balance >= initial_capital * RISK_LEVEL_2_ENTRY_THRESHOLD:
        new_risk_level = 2
        new_quota_status = True
        msg = f"🏆 Daily Quota Achieved! 🏆\nRisk Level: 1 -> 2\nBalance: {balance_fmt} USD\nNo new trades today."
        print("    [!] [AdvBottom] DAILY QUOTA ACHIEVED! Risk Level: 1 -> 2")
        send_trader_telegram(msg)

    state['risk_level'] = new_risk_level
    state['daily_quota_achieved'] = new_quota_status


def generate_and_send_performance_summary_md_original(state: Dict[str, Any], period_hours: int, period_name: str, current_time: int):
    since_timestamp = current_time - (period_hours * 60 * 60); history = state.get("trade_history", []); period_trades = [t for t in history if t.get("exit_timestamp") is not None and int(t["exit_timestamp"]) >= since_timestamp]
    summary_msg = f"📊 *{period_name} Performance Summary* 📊\n\n"; summary_msg += f"Period: Last {period_hours} hours\n"
    if not period_trades: print(f"    [i] [AdvBottom] No trades in the last {period_hours} hours."); summary_msg += f"No trades closed in this period.\n\n"
    else:
        period_metrics_cal = _calculate_metrics_from_trades(period_trades); pnl_period_fmt = f"{period_metrics_cal['total_realized_pnl_usd_for_list']:.2f}"; wr_period_fmt = period_metrics_cal['win_rate_pct']; avg_win_fmt = period_metrics_cal['avg_win_pct']; avg_loss_fmt = period_metrics_cal['avg_loss_pct']
        summary_msg += f"Trades Closed: {period_metrics_cal['total_trades']}\n"; summary_msg += f"Realized PnL (Period): {pnl_period_fmt} USD\n"; summary_msg += f"Win Rate (Period): {wr_period_fmt}%\n"; summary_msg += f"Avg Win (Period): {avg_win_fmt}%\n"; summary_msg += f"Avg Loss (Period): {avg_loss_fmt}%\n\n"
    summary_msg += f"---Portfolio Status---\n"; current_balance_fmt = _format_compact(state['current_balance_usd'], '$', 2);
    total_pnl_all_time_str = state.get('performance_metrics', {}).get('total_realized_pnl_usd', '0.00')
    try: total_pnl_fmt = f"{Decimal(total_pnl_all_time_str):.2f}"
    except: total_pnl_fmt = "Error"

    summary_msg += f"Current Balance: {current_balance_fmt} USD\n"; summary_msg += f"Total Realized PnL (All Time): {total_pnl_fmt} USD"
    send_trader_telegram(summary_msg)
    if period_hours == 4: state['last_4hr_summary_ts'] = current_time
    elif period_hours == 24: state['last_daily_summary_ts'] = current_time
    save_trader_state(state)

def calculate_nearness_score(proximity_info: Dict[str, Any], matched_scenario_name_from_filter: Optional[str]) -> float:
    """
    Calculates a score for "near miss" candidates. Lower is better (closer).
    Scores based on deviation from ideal criteria.
    """
    if not proximity_info: return float('inf') # Should not happen if called correctly

    base_score = 0.0
    # --- Market Cap Score ---
    mc_shortfall = proximity_info.get("market_cap_shortfall_usd", float(MIN_MARKET_CAP_USD)) # Default to full target if missing
    mc_target = proximity_info.get("market_cap_target_usd", float(MIN_MARKET_CAP_USD))
    if mc_target > 0 and mc_shortfall > 0:
        # Higher penalty for being further below the *trading* threshold MIN_MARKET_CAP_USD
        base_score += (mc_shortfall / mc_target) * 500 # Weight for MC shortfall
    elif proximity_info.get("market_cap") == "Missing" or proximity_info.get("market_cap") == "Invalid Type":
        base_score += 1000 # Heavy penalty for missing/invalid MC data

    # --- Dip Scenario Score ---
    # If no dip scenario was matched by passes_strict_filters, this is a large penalty.
    # matched_scenario_name_from_filter comes from passes_strict_filters return
    if not matched_scenario_name_from_filter and proximity_info.get("dip_scenario_match") == "Failed all scenarios":
        base_score += 2000 # Very high penalty if no scenario even remotely matched
    else:
        # If a scenario was matched, but other things failed, the base_score additions below will cover it.
        # If a scenario was *almost* matched, proximity_info might have shortfall details for specific scenarios.
        # For simplicity now, we rely on the "Failed all scenarios" flag. More granular scoring could be added.
        # Example: iterate scenario fail details in proximity_info and sum up shortfalls.
        pass


    # --- Liquidity Score ---
    liq_shortfall = proximity_info.get("liquidity_shortfall_usd")
    if liq_shortfall is not None and liq_shortfall > 0:
        liq_target = proximity_info.get("liquidity_target_usd", float(FLEX_STRICT_MIN_LIQUIDITY_USD))
        if liq_target > 0: base_score += (liq_shortfall / liq_target) * 200
    elif proximity_info.get("liquidity") == "Invalid Type": base_score += 500

    # --- Dip Pressure Score ---
    dp_net_shortfall = proximity_info.get("dip_pressure_net_buys_shortfall")
    if dp_net_shortfall is not None and dp_net_shortfall > 0 : base_score += dp_net_shortfall * 50
    dp_tx_shortfall = proximity_info.get("dip_pressure_tx_count_shortfall")
    if dp_tx_shortfall is not None and dp_tx_shortfall > 0: base_score += dp_tx_shortfall * 10
    if proximity_info.get("dip_pressure_tx_data") == "Missing/None": base_score += 100

    # --- ATR Dip Confirmation Score ---
    atr_dip_shortfall = proximity_info.get("atr_dip_confirmation_shortfall_pct")
    if atr_dip_shortfall is not None and atr_dip_shortfall > 0: base_score += atr_dip_shortfall * 30
    if proximity_info.get("atr_calculation") == "Failed or Zero": base_score += 300
    if proximity_info.get("atr_dip_value") == "No valid dip found in matched scenario TFs": base_score += 200

    # --- Whale Buy Confirmation Score ---
    if proximity_info.get("whale_buy_confirmed") is False: # Check for explicit False
        base_score += 150

    # --- Security Flags Score ---
    if proximity_info.get("security_mutable_metadata") is True: base_score += 50
    if proximity_info.get("security_freeze_authority") is True: base_score += 50
    if proximity_info.get("security_gmgn_honeypot") is True: base_score += 1000
    if proximity_info.get("security_gmgn_mintable") is True: base_score += 200

    # Other general filters could be scored here too (age, holders, top1_holder_pct)
    age_shortfall = proximity_info.get("age_shortfall_minutes")
    if age_shortfall is not None and age_shortfall > 0: base_score += age_shortfall # 1 point per minute short
    holders_shortfall = proximity_info.get("total_holders_shortfall")
    if holders_shortfall is not None and holders_shortfall > 0: base_score += holders_shortfall * 2 # 2 points per holder short

    return base_score

# --- Main Loop (AdvBottom) ---
# --- Main Loop (AdvBottom) ---
def main_trading_loop():
    start_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    last_near_miss_report_time = 0

    # Assuming DEXSCREENER_BASE_URL_SOLANA is defined globally, e.g.,
    # DEXSCREENER_BASE_URL_SOLANA = "https://dexscreener.com/solana/"

    print(f"--- Starting Paper Trader [ADVANCED BOTTOM DETECTOR] (v1.2.1 - Revised Near Miss + DexScreener) ({start_time_str}) ---") # Updated version for clarity
    print(f"Processed Reports Dir: {PROCESSED_REPORTS_DIR}; State File: {TRADER_STATE_FILE}")
    print(f"VERBOSE LOGGING: {VERBOSE_LOGGING}")
    print(f"Minimum Market Cap (Trading): ${_format_compact(MIN_MARKET_CAP_USD, '', 0)}")
    print(f"Minimum Market Cap (Near Miss Consideration): ${_format_compact(MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION, '', 0)}")
    print(f"ATR: Resolution={ATR_TIMEFRAME_OHLCV}, EntryMult={ATR_ENTRY_MULTIPLIER}, SLMult={ATR_STOP_LOSS_MULTIPLIER}, Trailing={ATR_TRAILING_ENABLED}")
    print(f"Whale: Top={WHALE_TRADER_COUNT}, BuyLookback={WHALE_BUY_LOOKBACK_MINUTES}m, SellUSD={WHALE_SELL_USD_THRESHOLD}, SellLookback={WHALE_SELL_LOOKBACK_MINUTES}m")
    print(f"Mint/Burn: CheckInterval={MINT_BURN_CHECK_INTERVAL_SECONDS}s, Lookback={MINT_EVENT_LOOKBACK_MINUTES}m")
    print(f"Sell Pressure: Lookback={SELL_PRESSURE_LOOKBACK_MINUTES}m, Ratio={SELL_PRESSURE_RATIO_THRESHOLD}, MinSellUSD={SELL_PRESSURE_MIN_SELL_USD_THRESHOLD}")
    print(f"Order Book: SpreadMult={ORDER_BOOK_SPREAD_MULTIPLIER_THRESHOLD}, MinBidDepth={ORDER_BOOK_MIN_BID_DEPTH_USD}")
    print(f"Token Rescan Cooldown: {TOKEN_RESCAN_COOLDOWN_SECONDS // 60} minutes")
    print(f"Near Miss Reporting: Top {MAX_NEAR_MISSES_TO_REPORT} every {NEAR_MISS_REPORT_INTERVAL_SECONDS // 60} minutes")


    state = load_trader_state()
    default_state_structure = {
        "initial_capital_usd": str(INITIAL_CAPITAL_USD), "current_balance_usd": str(INITIAL_CAPITAL_USD),
        "capital_at_quota_start": str(INITIAL_CAPITAL_USD), "realized_pnl_usd": "0.0", "risk_level": 0,
        "daily_quota_achieved": False, "last_quota_reset_timestamp": 0, "open_positions": {},
        "trade_history": [], "last_stop_loss_timestamp": 0, "kill_switch_active_until_ts": 0,
        "pending_entry": {}, "performance_metrics": {}, "recently_pending": {},
        "last_4hr_summary_ts": 0, "last_daily_summary_ts": 0, "prev_day_total_realized_pnl_at_reset": "0.0",
        "daily_loss_limit_hit": False,
        "recently_analyzed_full_timestamps": {},
        "last_near_miss_report_time": 0
    }
    if not state:
        print("[*] [AdvBottom] Initializing new trader state.")
        state = default_state_structure.copy()
        state['last_quota_reset_timestamp'] = int(time.time())
        calculate_performance_metrics(state)
    else:
        for key, default_value in default_state_structure.items():
            state.setdefault(key, default_value)
        for key_str_decimal in ["initial_capital_usd", "current_balance_usd", "capital_at_quota_start", "realized_pnl_usd", "prev_day_total_realized_pnl_at_reset"]:
            try:
                state[key_str_decimal] = str(Decimal(state[key_str_decimal]))
            except Exception:
                state[key_str_decimal] = default_state_structure[key_str_decimal]
        if not state.get('performance_metrics') or 'total_realized_pnl_usd' not in state['performance_metrics']:
            calculate_performance_metrics(state)

    last_near_miss_report_time = state.get('last_near_miss_report_time', 0)

    print(f"[*] [AdvBottom] Current State: Balance=${Decimal(state['current_balance_usd']):.2f}, Risk={state['risk_level']}, Quota Met={state['daily_quota_achieved']}")
    print(f"[*] [AdvBottom] Tracking {len(state.get('open_positions', {}))} open, {len(state.get('pending_entry', {}))} pending.")
    last_report_scan_time = 0

    while True:
        current_time = int(time.time())
        current_dt_utc = datetime.datetime.now(timezone.utc)
        current_hour_utc = current_dt_utc.hour

        print(f"\n--- [AdvBottom] Loop Start: {default_format_timestamp(current_time)} ---")
        try:
            today_start_utc = current_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ts = int(today_start_utc.timestamp())

            if state.get('last_quota_reset_timestamp', 0) < today_start_ts:
                print(f"\n--- [AdvBottom] New Day [{today_start_utc.strftime('%Y-%m-%d')}] Resetting Daily Quota & Risk Level ---")
                state['daily_quota_achieved'] = False
                state['risk_level'] = 0
                state['daily_loss_limit_hit'] = False
                state['capital_at_quota_start'] = state['current_balance_usd']
                state['last_quota_reset_timestamp'] = current_time
                state['last_near_miss_report_time'] = 0

                prev_day_pnl_usd = Decimal(state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")) - Decimal(state.get('prev_day_total_realized_pnl_at_reset', "0.0"))
                state['prev_day_total_realized_pnl_at_reset'] = state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")

                calculate_performance_metrics(state)
                start_balance_fmt = _format_compact(state['capital_at_quota_start'], '$', 2)
                prev_day_pnl_fmt = _format_compact(prev_day_pnl_usd, '$', 2)
                send_trader_telegram(f"🌅 New Day! Daily quota reset. Risk level to 0.\nStart Balance: {start_balance_fmt} USD\nPrev Day PnL: {prev_day_pnl_fmt} USD")

            manage_portfolio(state)
            manage_pending_entries(state)

            if VERBOSE_LOGGING: print("    [AdvBottom Verbose] Checking operational halts...")
            else: print("    [*] [AdvBottom] Checking operational halts...")

            current_balance_dec = Decimal(state['current_balance_usd'])
            capital_start_dec = Decimal(state['capital_at_quota_start'])
            unrealized_pnl = Decimal("0.0")

            if state['open_positions']:
                 if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Calculating unrealized PnL for {len(state['open_positions'])} positions...")
                 open_mints_for_pnl = list(state['open_positions'].keys())
                 pnl_prices_map = {}
                 for m_addr_pnl in open_mints_for_pnl:
                     cp_pnl = get_current_price(m_addr_pnl)
                     if cp_pnl: pnl_prices_map[m_addr_pnl] = cp_pnl
                     time.sleep(0.05)
                 for mint_pos, pos_data in state['open_positions'].items():
                     current_price_for_pnl = pnl_prices_map.get(mint_pos)
                     if current_price_for_pnl:
                         try:
                            entry_val_str = pos_data['position_size_usd']; tokens_str = pos_data['position_size_tokens']
                            if entry_val_str and tokens_str:
                                entry_val = Decimal(entry_val_str); current_val = Decimal(tokens_str) * current_price_for_pnl
                                unrealized_pnl += (current_val - entry_val)
                            elif VERBOSE_LOGGING: print(f"        [!] [AdvBottom PNL Calc] Missing position_size_usd or position_size_tokens for {mint_pos}")
                         except Exception as dec_err: print(f"        [!] [AdvBottom PNL Calc] Error calculating PNL for {mint_pos}: {dec_err}")
                 if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Unrealized PnL: {unrealized_pnl:+.2f} USD")

            effective_balance = current_balance_dec + unrealized_pnl; daily_drawdown = Decimal("0.0")
            if capital_start_dec > 0: daily_drawdown = (capital_start_dec - effective_balance) / capital_start_dec
            if daily_drawdown >= DAILY_LOSS_LIMIT_PCT:
                if not state.get('daily_loss_limit_hit', False):
                    eff_balance_fmt = _format_compact(str(effective_balance), '$', 2); daily_drawdown_fmt = f"{daily_drawdown:.1%}"
                    print(f"    [!!!] [AdvBottom] DAILY LOSS LIMIT HIT! Drawdown: {daily_drawdown_fmt}. Halting."); send_trader_telegram(f"🛑 Daily Loss Limit Hit ({daily_drawdown_fmt})! 🛑\nNo new trades.\nEffective Balance: {eff_balance_fmt} USD"); state['daily_loss_limit_hit'] = True

            is_trading_hours = TRADING_START_HOUR_UTC <= current_hour_utc < TRADING_END_HOUR_UTC
            is_sl_cooldown = current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS
            is_kill_switch_active = current_time < state.get('kill_switch_active_until_ts', 0)
            can_consider_new_trade = (not state['daily_quota_achieved'] and not state.get('daily_loss_limit_hit', False) and is_trading_hours and not is_sl_cooldown and not is_kill_switch_active)
            log_msg_halts = f"        [AdvBottom] Halts: Quota={state['daily_quota_achieved']}, LossLimit={state.get('daily_loss_limit_hit', False)}, Hours={is_trading_hours}, SL Cool={is_sl_cooldown}, Kill={is_kill_switch_active}"
            log_msg_can_trade = f"        [AdvBottom] --> Can consider new trade? {can_consider_new_trade}"
            if VERBOSE_LOGGING: print(log_msg_halts); print(log_msg_can_trade)
            elif not can_consider_new_trade: print(log_msg_halts); print(log_msg_can_trade)

            slots_available = len(state['open_positions']) + len(state.get('pending_entry', {})) < MAX_OPEN_POSITIONS
            time_for_scan = current_time > last_report_scan_time + (CHECK_INTERVAL_SECONDS * 2) # Ensure this is CHECK_INTERVAL_SECONDS * 2 or more
            should_scan_reports = can_consider_new_trade and slots_available and time_for_scan

            if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Should scan reports? {should_scan_reports} (CanTrade:{can_consider_new_trade}, SlotsOK: {slots_available}, TimeOK:{time_for_scan})")

            if should_scan_reports:
                print(f"\n[*] [AdvBottom] Scanning last {NUM_REPORTS_TO_SCAN} reports for new dip candidates...")
                last_report_scan_time = current_time
                latest_report_files = get_latest_report_paths(NUM_REPORTS_TO_SCAN)
                potential_candidates_dict: Dict[str, Dict] = {}
                total_items_processed_from_reports = 0
                near_miss_candidates_this_cycle: List[Dict[str, Any]] = []

                state.setdefault('recently_analyzed_full_timestamps', {})
                state['recently_analyzed_full_timestamps'] = {
                    mint_key: ts_val for mint_key, ts_val in state['recently_analyzed_full_timestamps'].items()
                    if current_time - ts_val < TOKEN_RESCAN_COOLDOWN_SECONDS
                }

                if not latest_report_files:
                    print("    [!] [AdvBottom] No processed reports found to scan.")
                else:
                    if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Found {len(latest_report_files)} report files to scan.")

                    for report_idx, report_path in enumerate(latest_report_files):
                        report_name = os.path.basename(report_path)
                        if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Scanning Report {report_idx+1}/{len(latest_report_files)}: {report_name}")
                        report_data = load_json_data(report_path)
                        if not report_data: print(f"        [!] [AdvBottom] Failed to load report {report_name}"); continue

                        items_list = report_data.get("correlated_holdings", report_data.get("correlated_holdings_snapshot", []))
                        if not items_list or not isinstance(items_list, list):
                            if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] No relevant items list in {report_name}"); continue

                        if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Report {report_name} contains {len(items_list)} items. Fetching fresh data & filtering...")
                        total_items_processed_from_reports += len(items_list)

                        for item_data_from_report in items_list:
                            mint_cand = item_data_from_report.get("token_mint")
                            original_corr_data = item_data_from_report.get("original_correlation_data", item_data_from_report.copy())

                            if not mint_cand or not isinstance(mint_cand, str):
                                if VERBOSE_LOGGING: print(f"        [AdvBottom Scan] Skipping item, missing or invalid 'token_mint': {item_data_from_report}")
                                continue

                            last_analyzed_ts = state['recently_analyzed_full_timestamps'].get(mint_cand)
                            if last_analyzed_ts and (current_time - last_analyzed_ts < TOKEN_RESCAN_COOLDOWN_SECONDS):
                                if VERBOSE_LOGGING: print(f"    [i] Skipping {mint_cand[:6]} from report {report_name}, recently fully analyzed at {default_format_timestamp(last_analyzed_ts)}. Cooldown: {TOKEN_RESCAN_COOLDOWN_SECONDS // 60} min.")
                                continue

                            if VERBOSE_LOGGING: print(f"    [*] Evaluating candidate {mint_cand[:6]} from report {report_name} by fetching fresh data...")
                            fresh_candidate_item = {
                                "token_mint": mint_cand, "original_correlation_data": original_corr_data,
                                "symbol": item_data_from_report.get("symbol", "N/A"), "name": item_data_from_report.get("name", "N/A"),
                                "bds_metadata": None, "bds_market_data": None, "bds_trade_data": None,
                                "bds_security_data": None, "gmgn_security_info": None
                            }
                            essential_data_fetched = True

                            if VERBOSE_LOGGING: print(f"        Fetching fresh metadata for {mint_cand[:6]}...")
                            fresh_metadata = pt_get_bds_metadata_single(mint_cand); time.sleep(0.05)
                            if fresh_metadata:
                                fresh_candidate_item["bds_metadata"] = fresh_metadata
                                fresh_candidate_item["symbol"] = fresh_metadata.get("symbol", fresh_candidate_item["symbol"])
                                fresh_candidate_item["name"] = fresh_metadata.get("name", fresh_candidate_item["name"])
                            elif VERBOSE_LOGGING: print(f"        [!] Warning: Failed to fetch fresh metadata for {mint_cand[:6]}.")

                            if VERBOSE_LOGGING: print(f"        Fetching fresh market data for {mint_cand[:6]}...")
                            fresh_market_data = pt_get_bds_market_data(mint_cand); time.sleep(0.05)
                            if fresh_market_data:
                                fresh_candidate_item["bds_market_data"] = fresh_market_data
                                market_cap_usd_val_nm = fresh_market_data.get("mc")
                                if market_cap_usd_val_nm is not None:
                                    try:
                                        market_cap_usd_dec_nm = Decimal(str(market_cap_usd_val_nm))
                                        if market_cap_usd_dec_nm < MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION:
                                            if VERBOSE_LOGGING:
                                                print(f"        [AdvBottom Scan NM Filter] Skipping {mint_cand[:6]} ({fresh_candidate_item.get('symbol', 'N/A')}): MC ${_format_compact(market_cap_usd_dec_nm, '', 0)} too low for near-miss consideration (Threshold: ${_format_compact(MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION, '', 0)})")
                                            state['recently_analyzed_full_timestamps'][mint_cand] = current_time
                                            essential_data_fetched = False
                                    except Exception as e_mc_nm:
                                        if VERBOSE_LOGGING: print(f"        [AdvBottom Scan NM Filter] Warning: Could not parse MC '{market_cap_usd_val_nm}' for {mint_cand[:6]}: {e_mc_nm}")
                                elif VERBOSE_LOGGING:
                                     print(f"        [AdvBottom Scan NM Filter] Warning: MC ('mc') not found for {mint_cand[:6]}. Cannot apply near-miss MC pre-filter.")
                            else:
                                print(f"        [!] CRITICAL: Failed to fetch fresh market data for {mint_cand[:6]}. Skipping.");
                                state['recently_analyzed_full_timestamps'][mint_cand] = current_time;
                                essential_data_fetched = False

                            if not essential_data_fetched: continue

                            if VERBOSE_LOGGING: print(f"        Fetching fresh trade data for {mint_cand[:6]}...")
                            fresh_trade_data = pt_get_bds_trade_data(mint_cand); time.sleep(0.05)
                            if fresh_trade_data: fresh_candidate_item["bds_trade_data"] = fresh_trade_data
                            else: print(f"        [!] CRITICAL: Failed to fetch fresh trade data for {mint_cand[:6]}. Skipping."); state['recently_analyzed_full_timestamps'][mint_cand] = current_time; essential_data_fetched = False

                            if not essential_data_fetched: continue

                            if VERBOSE_LOGGING: print(f"        Fetching fresh security data for {mint_cand[:6]}...")
                            fresh_security_data = pt_get_bds_token_security(mint_cand); time.sleep(0.05)
                            if fresh_security_data: fresh_candidate_item["bds_security_data"] = fresh_security_data
                            elif VERBOSE_LOGGING: print(f"        [!] Warning: Failed to fetch fresh security data for {mint_cand[:6]}.")

                            if GMGN_ENABLED and gmgn_analyzer:
                                if VERBOSE_LOGGING: print(f"        Fetching GMGN security for {mint_cand[:6]} (main loop)...")
                                try:
                                    gmgn_sec_info = gmgn_analyzer.getSecurityInfo(mint_cand); time.sleep(0.1)
                                    fresh_candidate_item["gmgn_security_info"] = gmgn_sec_info or {}
                                except Exception as e_gmgn_main:
                                    if VERBOSE_LOGGING: print(f"            [AdvBottom MainLoop GMGN] Error fetching GMGN for {mint_cand[:6]}: {e_gmgn_main}")
                                    fresh_candidate_item["gmgn_security_info"] = {"error": str(e_gmgn_main)}

                            state['recently_analyzed_full_timestamps'][mint_cand] = current_time

                            filter_output = passes_strict_filters(fresh_candidate_item, current_time, is_revalidation=False)
                            passes_strict, fail_reason, matched_scenario_name, scenario_bonus, proximity_info = filter_output

                            if passes_strict and matched_scenario_name and scenario_bonus is not None:
                                if mint_cand not in potential_candidates_dict:
                                   potential_candidates_dict[mint_cand] = fresh_candidate_item
                            else:
                                nearness_score_val = calculate_nearness_score(proximity_info, matched_scenario_name)
                                near_miss_candidates_this_cycle.append({
                                    "mint": mint_cand, "symbol": fresh_candidate_item.get("symbol", "N/A"),
                                    "name": fresh_candidate_item.get("name", "N/A"), "proximity": proximity_info,
                                    "matched_scenario": matched_scenario_name,
                                    "fail_reason_for_trade": fail_reason,
                                    "nearness_score": nearness_score_val
                                })
                                if VERBOSE_LOGGING and fail_reason: # fail_reason might be None if it just didn't meet criteria but not a hard fail
                                    print(f"        [AdvBottom Filter Detail -> Near Miss List] {fresh_candidate_item.get('symbol','N/A')} ({mint_cand[:6]}): {fail_reason if fail_reason else 'Criteria not fully met'}. Nearness score: {nearness_score_val:.2f}")


                    print(f"    [AdvBottom] Scanned {len(latest_report_files)} reports, {total_items_processed_from_reports} total items initially considered.")
                    print(f"    [AdvBottom] Found {len(potential_candidates_dict)} unique candidates after passing all filters for trading.")
                    print(f"    [AdvBottom] Identified {len(near_miss_candidates_this_cycle)} candidates for near-miss scoring.")

                    # --- Process Near Misses ---
                    if near_miss_candidates_this_cycle and (current_time - last_near_miss_report_time > NEAR_MISS_REPORT_INTERVAL_SECONDS):
                        near_miss_candidates_this_cycle.sort(key=lambda x: x.get("nearness_score", float('inf')))
                        message = "🔎 Top Near Miss Token Candidates:\n\n"
                        count = 0
                        for cand_miss in near_miss_candidates_this_cycle:
                            if count >= MAX_NEAR_MISSES_TO_REPORT: break
                            prox_details_str_parts = []
                            prox_info_cand_miss = cand_miss.get("proximity", {})

                            mc_curr = prox_info_cand_miss.get('market_cap_current_usd')
                            mc_targ = prox_info_cand_miss.get('market_cap_target_usd', float(MIN_MARKET_CAP_USD))
                            if mc_curr is not None:
                                mc_short = prox_info_cand_miss.get('market_cap_shortfall_usd', mc_targ - mc_curr if mc_curr < mc_targ else 0)
                                if mc_short > 0 : prox_details_str_parts.append(f"MC Short: ${_format_compact(mc_short,'',0)} (Curr: ${_format_compact(mc_curr,'',0)})")

                            if not cand_miss.get('matched_scenario') and prox_info_cand_miss.get("dip_scenario_match") == "Failed all scenarios":
                                prox_details_str_parts.append("No Dip Scenario")

                            if prox_info_cand_miss.get('liquidity_shortfall_usd', 0) > 0 : prox_details_str_parts.append(f"Liq Short: ${_format_compact(prox_info_cand_miss['liquidity_shortfall_usd'],'',0)} (Curr: ${_format_compact(prox_info_cand_miss.get('liquidity_current_usd'),'',0)})")
                            if prox_info_cand_miss.get('dip_pressure_net_buys_shortfall', 0) > 0 : prox_details_str_parts.append(f"NetBuys Short: {prox_info_cand_miss['dip_pressure_net_buys_shortfall']} (Curr: {prox_info_cand_miss.get('dip_pressure_current_net_buys')})")
                            if prox_info_cand_miss.get('atr_dip_confirmation_shortfall_pct', 0) > 0:
                                current_dip_str = f"{prox_info_cand_miss.get('atr_dip_current_dip_pct', 'N/A')}"
                                if isinstance(prox_info_cand_miss.get('atr_dip_current_dip_pct'), (float, Decimal)): current_dip_str = f"{float(prox_info_cand_miss['atr_dip_current_dip_pct']):.1f}%"
                                prox_details_str_parts.append(f"ATR Dip Short: {prox_info_cand_miss['atr_dip_confirmation_shortfall_pct']:.1f}% (Dip: {current_dip_str})")
                            if prox_info_cand_miss.get("whale_buy_confirmed") is False: prox_details_str_parts.append("No Whale Buy")

                            near_miss_mint = cand_miss['mint']
                            dex_screener_link = f"{DEXSCREENER_BASE_URL_SOLANA}{near_miss_mint}" # Ensure this constant is defined

                            message += f"{count+1}. *{escape_markdown_original(cand_miss['symbol'])}* (`{escape_markdown_original(near_miss_mint)}`)\n"
                            message += f"   Score: {cand_miss['nearness_score']:.0f} (Lower is closer)\n"
                            message += f"   Trade Fail: {escape_markdown_original(cand_miss.get('fail_reason_for_trade', 'Multiple criteria'))}\n"
                            if cand_miss.get('matched_scenario'): message += f"   Matched Scenario (filter): {escape_markdown_original(cand_miss['matched_scenario'])}\n"
                            if prox_details_str_parts: message += f"   Deviations: {'; '.join(prox_details_str_parts)}\n"
                            message += f"   Chart: {dex_screener_link}\n"
                            message += "\n"
                            count += 1
                        if count > 0: send_trader_telegram(message); last_near_miss_report_time = current_time; state['last_near_miss_report_time'] = last_near_miss_report_time
                        elif VERBOSE_LOGGING: print("    [AdvBottom Verbose] No near-miss candidates met criteria for reporting this interval.")

                    # --- Process Passed Candidates for Trading ---
                    if potential_candidates_dict:
                        items_to_rank = list(potential_candidates_dict.values())
                        ranked_candidates = rank_trade_candidates(items_to_rank)
                        current_recently_pending = state.setdefault('recently_pending', {})
                        valid_recently_pending = { m: ts for m, ts in current_recently_pending.items() if current_time - ts < RECENTLY_ADDED_PENDING_WINDOW_SECONDS }
                        state['recently_pending'] = valid_recently_pending
                        added_to_pending_count = 0; replaced_in_pending_count = 0
                        if VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Processing {len(ranked_candidates)} ranked candidates for pending list. Recently pending: {len(state['recently_pending'])}.")
                        else: print(f"    [*] [AdvBottom] Processing top {min(len(ranked_candidates), MAX_OPEN_POSITIONS)} ranked for pending list...")
                        pending_entry_map = state.setdefault('pending_entry', {})
                        for i, ranked_item_cand in enumerate(ranked_candidates):
                            pending_mint_cand = ranked_item_cand.get("token_mint");
                            if not pending_mint_cand: continue
                            new_candidate_score = ranked_item_cand.get('_score', Decimal('-1')); symbol_for_log_cand = ranked_item_cand.get('symbol', 'N/A')
                            matched_scenario_for_pending = ranked_item_cand.get('_matched_dip_scenario_name', "N/A_ranked")
                            if VERBOSE_LOGGING: print(f"        [AdvBottom Verbose] Considering candidate #{i+1}: {symbol_for_log_cand} ({pending_mint_cand[:6]}) Score: {new_candidate_score:.2f}, Scenario: {matched_scenario_for_pending}")
                            if pending_mint_cand in state['open_positions']:
                                if VERBOSE_LOGGING: print(f"            [AdvBottom Verbose] Skipping {symbol_for_log_cand}: Already in open positions."); continue
                            if pending_mint_cand in state['recently_pending']:
                                if VERBOSE_LOGGING: print(f"            [AdvBottom Verbose] Skipping {symbol_for_log_cand}: In 'recently_pending' (added to pending list) cooldown."); continue
                            num_open_plus_pending = len(state['open_positions']) + len(pending_entry_map); has_free_slot_for_new = num_open_plus_pending < MAX_OPEN_POSITIONS
                            new_pending_data = { "symbol": symbol_for_log_cand, "name": ranked_item_cand.get("name", "N/A"), "signal_timestamp": int(time.time()),
                                "signal_price": str((ranked_item_cand.get("bds_market_data") or {}).get("price") or 'N/A'), "monitoring_start_time": int(time.time()),
                                "entry_window_seconds": PENDING_ENTRY_WINDOW_SECONDS, "score": str(new_candidate_score), "risk_level": state['risk_level'],
                                "matched_dip_scenario_name": matched_scenario_for_pending, "original_correlation_data": ranked_item_cand.get("original_correlation_data", {}),
                                "_current_atr_value": ranked_item_cand.get("_current_atr_value"), "_current_atr_percent": ranked_item_cand.get("_current_atr_percent")}
                            if pending_mint_cand in pending_entry_map:
                                existing_pending_score = Decimal(pending_entry_map[pending_mint_cand].get('score', '-1'))
                                if new_candidate_score > existing_pending_score * PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR:
                                    print(f"            [AdvBottom] Updating PENDING {symbol_for_log_cand} (NewScore:{new_candidate_score:.2f} > OldScore:{existing_pending_score:.2f} * {PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR})")
                                    pending_entry_map[pending_mint_cand] = new_pending_data; state['recently_pending'][pending_mint_cand] = current_time; replaced_in_pending_count +=1
                                elif VERBOSE_LOGGING: print(f"            [AdvBottom Verbose] Skipping {symbol_for_log_cand}: Already pending, new score not significantly better.")
                            elif has_free_slot_for_new:
                                print(f"            -> [AdvBottom] Added {symbol_for_log_cand} to PENDING. Score: {new_candidate_score:.2f}, Scenario: {matched_scenario_for_pending}")
                                pending_entry_map[pending_mint_cand] = new_pending_data; state['recently_pending'][pending_mint_cand] = current_time; added_to_pending_count += 1
                            elif pending_entry_map:
                                lowest_score_pending_mint_cand = None; worst_score_in_pending = Decimal('Infinity') ; candidate_to_oust = None
                                for mint_in_p, p_data in pending_entry_map.items():
                                    current_p_score = Decimal(p_data.get('score', '-1'))
                                    if current_p_score < worst_score_in_pending: worst_score_in_pending = current_p_score; candidate_to_oust = mint_in_p
                                if candidate_to_oust and new_candidate_score > worst_score_in_pending:
                                    removed_symbol_cand = pending_entry_map[candidate_to_oust].get('symbol','OldSymbol')
                                    print(f"            -> [AdvBottom] Replacing PENDING {removed_symbol_cand} (Score:{worst_score_in_pending:.2f}) with {symbol_for_log_cand} (Score:{new_candidate_score:.2f})")
                                    pending_entry_map.pop(candidate_to_oust); pending_entry_map[pending_mint_cand] = new_pending_data
                                    state['recently_pending'][pending_mint_cand] = current_time; state['recently_pending'][candidate_to_oust] = current_time ; replaced_in_pending_count += 1
                                elif VERBOSE_LOGGING: print(f"            [AdvBottom Verbose] Skipping {symbol_for_log_cand}: No free slots and not better score than any existing pending items.")
                            if len(state['open_positions']) + len(pending_entry_map) >= MAX_OPEN_POSITIONS:
                                 if added_to_pending_count > 0 or replaced_in_pending_count > 0 : print(f"        [AdvBottom] Max open/pending positions ({MAX_OPEN_POSITIONS}) reached. Stopping further candidate processing for this cycle.")
                                 break
                        if added_to_pending_count > 0 or replaced_in_pending_count > 0: print(f"    [*] [AdvBottom] Finished candidate scan. Added: {added_to_pending_count}, Replaced: {replaced_in_pending_count}. Current Pending: {len(pending_entry_map)}")
                        elif VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] Finished candidate scan. No new pending entries added or replaced.")
                    elif VERBOSE_LOGGING: print(f"    [AdvBottom Verbose] No candidates passed filters in reports or none to rank after fetching fresh data.")

            # --- Performance Summaries & State Saving ---
            if current_time >= state.get('last_4hr_summary_ts', 0) + SUMMARY_INTERVAL_4HR_SECONDS:
                print(f"\n[*] [AdvBottom] Generating 4-Hour Performance Summary...")
                calculate_performance_metrics(state)
                generate_and_send_performance_summary_md_original(state, 4, "4-Hour [AdvBottom]", current_time)
            elif current_time >= state.get('last_daily_summary_ts', 0) + SUMMARY_INTERVAL_DAILY_SECONDS:
                print(f"\n[*] [AdvBottom] Generating Daily Performance Summary...")
                calculate_performance_metrics(state)
                generate_and_send_performance_summary_md_original(state, 24, "Daily [AdvBottom]", current_time)
            else:
                save_trader_state(state)

            print(f"--- [AdvBottom] Loop End: {default_format_timestamp(time.time())} ---")
            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n--- AdvBottom Trader stopping ---")
            if 'state' in locals(): save_trader_state(state)
            print("    AdvBottom state saved.")
            break
        except Exception as e:
            print(f"\n[!!!] [AdvBottom] Unhandled error in main loop: {e}")
            traceback.print_exc()
            if 'state' in locals():
                print("    [AdvBottom] Attempting to save state due to error...")
                save_trader_state(state)
            error_type_safe = escape_markdown_original(type(e).__name__)
            error_msg_safe = escape_markdown_original(str(e))
            telegram_error_message = f"🚨 *AdvBottom Main Loop Error* 🚨\n\n`{error_type_safe}: {error_msg_safe}`\n\nSee console/logs. Will retry."
            send_trader_telegram(telegram_error_message)
            print(f"    [AdvBottom] Retrying in {CHECK_INTERVAL_SECONDS*2} seconds...")
            time.sleep(CHECK_INTERVAL_SECONDS * 2)
            
# --- This should be at the very end of your script ---
if __name__ == "__main__":
    if 'TOKEN_RESCAN_COOLDOWN_SECONDS' not in globals():
        print("[!] CRITICAL: TOKEN_RESCAN_COOLDOWN_SECONDS is not defined globally. Please define it in the CONFIG section.")
        TOKEN_RESCAN_COOLDOWN_SECONDS = 3600
    if 'MIN_MARKET_CAP_USD' not in globals():
        print("[!] CRITICAL: MIN_MARKET_CAP_USD is not defined globally. Please define it in the CONFIG section.")
        MIN_MARKET_CAP_USD = Decimal("10000.00")
    if 'MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION' not in globals():
        print("[!] CRITICAL: MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION is not defined globally. Please define it in the CONFIG section.")
        MIN_MC_FOR_NEAR_MISS_SCORING_CONSIDERATION = Decimal("1000.00")
    main_trading_loop()
# --- END OF FILE paper_trader_advanced_bottom_detector.py ---