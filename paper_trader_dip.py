# --- START OF FILE paper_trader_dip.py ---

#!/usr/bin/env python3
"""
Paper Trading Simulator - DIP BUYING STRATEGY.
Version: 1.1.1 - Re-enabled PnL logging, confirms dip logic.
Based on Paper Trader v1.9.5 logic.
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

getcontext().prec = 18 

if not (TELEGRAM_TOKEN_TRADER and TELEGRAM_CHAT_ID_TRADER): print("[!] WARNING: [Dip Buyer] Trader Telegram credentials missing.")
if not BIRDEYE_API_KEY: print("[!] CRITICAL: [Dip Buyer] BIRDEYE_API_KEY missing."); exit()

try: import api_usage_limiter; print("[✓] paper_trader_dip: Imported api_usage_limiter.")
except ImportError: print("CRITICAL: api_usage_limiter.py not found!"); exit()
try: from helius_analyzer import format_timestamp as default_format_timestamp; print("[✓] paper_trader_dip: Imported format_timestamp.")
except ImportError:
    print("[!] paper_trader_dip: Could not import format_timestamp. Using basic internal version.")
    def default_format_timestamp(ts: Optional[Any]) -> Optional[str]:
        if ts is None: return "N/A"
        try: return datetime.datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except: return str(ts)

# --- CONFIG ---
REPORTS_ROOT   = os.path.join("reports", "holdings")
PROCESSED_REPORTS_DIR = os.path.join(REPORTS_ROOT, "processed") 
TRADER_STATE_FILE   = os.path.join(REPORTS_ROOT, "paper_trader_dip_state.json") 
TRADE_LOG_CSV_FILE = os.path.join(REPORTS_ROOT, "paper_trade_dip_log.csv")     
INITIAL_CAPITAL_USD = Decimal("1000.00")
MAX_OPEN_POSITIONS = 5
STOP_LOSS_PCT = Decimal("0.20"); TRAILING_STOP_LOSS_PCT = Decimal("0.15"); DAILY_LOSS_LIMIT_PCT = Decimal("0.10")
RISK_LEVEL_0_ALLOCATION_PCT = Decimal("0.05"); RISK_LEVEL_0_TP_PCT = Decimal("0.40")
RISK_LEVEL_1_ALLOCATION_PCT = Decimal("0.07"); RISK_LEVEL_1_TP_PCT = Decimal("1.00"); RISK_LEVEL_1_ENTRY_THRESHOLD = Decimal("1.5")
RISK_LEVEL_2_ALLOCATION_PCT = Decimal("0.10"); RISK_LEVEL_2_TP_PCT = Decimal("2.50"); RISK_LEVEL_2_ENTRY_THRESHOLD = Decimal("2.0")
SIMULATED_FEE_PCT = Decimal("0.001"); SIMULATED_SLIPPAGE_PCT = Decimal("0.005")
TRADING_START_HOUR_UTC = 0; TRADING_END_HOUR_UTC = 24
STOP_LOSS_COOLDOWN_SECONDS = 15 * 60
KILL_SWITCH_LOSS_PCT = Decimal("0.50"); KILL_SWITCH_DURATION_SECONDS = 2 * 60 * 60
PENDING_ENTRY_WINDOW_SECONDS = 3 * 60; PENDING_ENTRY_PRICE_DEVIATION_PCT = Decimal("0.05")
NUM_REPORTS_TO_SCAN = 10 
RECENTLY_ADDED_PENDING_WINDOW_SECONDS = 10 * 60
PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR = Decimal("1.10") 
# --- Strict Filters (DIP BUYING SPECIFIC) ---
STRICT_MIN_LIQUIDITY_USD = 5000
STRICT_MAX_TOP1_HOLDER_PCT = 0.50
STRICT_MIN_AGE_MINUTES = 15          
STRICT_MIN_TOTAL_HOLDERS = 50        
REQUIRE_SOCIALS = True
STRICT_REQUIRE_POSITIVE_1H_CHANGE = False 
REQUIRE_NEGATIVE_6H_CHANGE = True        
FILTER_MAX_24H_LOSS_PCT = Decimal("-90.0") 
STRICT_MIN_MONITORED_HOLDERS = 2 
# --- Scoring Weights ---
SCORE_WEIGHTS = { "liquidity": 1.5, "vol_liq_ratio": 1.0, "price_change_1h": 1.0, "price_change_6h_neg": 1.5, "total_holders": 0.5, "monitored_holders": 1.2, "top1_holder_pct": -1.0, "age_score": 0.5, "socials": 0.3 }
# --- Timing & API Endpoints ---
CHECK_INTERVAL_SECONDS = 30 
SUMMARY_INTERVAL_4HR_SECONDS = 4 * 60 * 60
SUMMARY_INTERVAL_DAILY_SECONDS = 24 * 60 * 60
BDS_BASE_V3_URL = "https://public-api.birdeye.so/defi/v3/token"; BDS_BASE_LEGACY_URL = "https://public-api.birdeye.so/defi"; BDS_MARKET_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/market-data"; BDS_TRADE_DATA_ENDPOINT = f"{BDS_BASE_V3_URL}/trade-data/single"; BDS_METADATA_MULTIPLE_ENDPOINT = f"{BDS_BASE_V3_URL}/meta-data/multiple"; BDS_TOKEN_SECURITY_ENDPOINT = f"{BDS_BASE_LEGACY_URL}/token_security"

os.makedirs(REPORTS_ROOT, exist_ok=True); os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)

# --- UTILS ---
def escape_markdown_original(text: Optional[Any]) -> str: 
    if text is None: return '' 
    if not isinstance(text, str): text = str(text)
    escape_chars = r'_*`[' 
    for char_to_escape in escape_chars: text = text.replace(char_to_escape, '\\' + char_to_escape)
    return text

def send_trader_telegram(text: str) -> bool:
    """ Sends telegram message with [Dip Buyer] prefix as PLAIN TEXT """ # Updated docstring
    token = TELEGRAM_TOKEN_TRADER; chat_id = TELEGRAM_CHAT_ID_TRADER
    if not token or not chat_id: print(f"--- DIP BUYER CONSOLE ---\n{text}\n--------------------"); return True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # --- REMOVED parse_mode ---
    payload = {"chat_id": chat_id, "text": f"📉 Dip Buyer 📉\n\n{text}", "disable_web_page_preview": "true"} 
    # --- END REMOVAL ---
    try: r = requests.post(url, data=payload, timeout=20); r.raise_for_status(); return True
    except requests.exceptions.HTTPError as e: error_content = e.response.text[:500] if e.response else str(e); print(f"    [✗] [Dip Buyer] Trader Telegram HTTP error: {e.response.status_code if e.response else 'N/A'} - {error_content}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False
    except Exception as e: print(f"    [✗] [Dip Buyer] Trader Telegram general error: {e}"); print(f"        Problematic text (first 100 chars): {text[:100]}"); return False

def load_json_data(path: str, is_state_file: bool = False) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        if is_state_file: print(f"    [i] [Dip Buyer] State file not found: {path}. Will be created on first save.")
        else: print(f"    [!] [Dip Buyer] Data file not found: {path}") 
    except json.JSONDecodeError as e: print(f"    [!] [Dip Buyer] Failed to decode JSON from {path}: {e}")
    except Exception as e: print(f"    [!] [Dip Buyer] Failed to load {path}: {e}")
    return {}
def save_json_data(path: str, data: Dict[str, Any]):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except Exception as e: print(f"    [✗] [Dip Buyer] Failed to save data to {path}: {e}")

def load_trader_state() -> Dict[str, Any]: return load_json_data(TRADER_STATE_FILE, is_state_file=True)
def save_trader_state(state: Dict[str, Any]): save_json_data(TRADER_STATE_FILE, state)

def calculate_percent_change(old: Optional[Any], new: Optional[Any]) -> Optional[float]:
    if old is None or new is None: return None
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
    except Exception as e: print(f"    [!] [Dip Buyer] Error finding latest report paths: {e}"); return []

def _format_compact(value: Optional[Any], prefix: str = "$", precision: int = 1) -> str:
    if value is None or value == "N/A": return "N/A"
    try:
        val = float(value)
        if abs(val) >= 1_000_000_000: return f"{prefix}{val/1_000_000_000:.{precision}f}B"
        if abs(val) >= 1_000_000: return f"{prefix}{val/1_000_000:.{precision}f}M"
        if abs(val) >= 1_000: return f"{prefix}{val/1_000:.{precision}f}K"
        formatted_num = f"{val:,.{precision}f}" if precision >= 0 else f"{val:,.0f}"
        return f"{prefix}{formatted_num}"
    except (ValueError, TypeError): return str(value) 
def _format_pct(value: Optional[Any]) -> str:
    if value is None or value == "N/A": return "N/A"
    try: return f"{float(value):+.1f}%"
    except (ValueError, TypeError): return str(value)

def get_current_price(mint_address: str) -> Optional[Decimal]:
    if not mint_address or not BIRDEYE_API_KEY: return None
    price_cache_ttl = 45; cache_key = f"birdeye_price_{mint_address}"; url = f"https://public-api.birdeye.so/defi/price?address={mint_address}"; headers = {"X-API-KEY": BIRDEYE_API_KEY}
    data = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="price", calling_script="paper_trader_dip.get_current_price", cache_key=cache_key, cache_ttl_seconds=price_cache_ttl, url=url, headers=headers, timeout=10) 
    if isinstance(data, dict) and data.get("success") == True and isinstance(data.get("data"), dict):
        price_value = data["data"].get("value");
        if price_value is not None:
            try: return Decimal(str(price_value))
            except Exception as e: print(f"    [!] [Dip Buyer] Error converting price {price_value} to Decimal for {mint_address}: {e}"); return None
    elif isinstance(data, dict) and data.get("success") == False:
         msg = data.get('message', 'Unknown error')
         if "Not found" not in msg and "Request failed" not in msg: print(f"    [!] [Dip Buyer] Birdeye price API failed for {mint_address}: {msg}")
    return None
def pt_get_bds_market_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_MARKET_DATA_ENDPOINT}?address={mint}&chain={chain}"
    cache_key = f"bds_v3_market_{mint}_{chain}"
    market_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_market_data", calling_script="paper_trader_dip.pt_get_bds_market_data", cache_key=cache_key, cache_ttl_seconds=45, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(market_data_response, dict) and market_data_response.get("success"): data_payload = market_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_trade_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TRADE_DATA_ENDPOINT}?address={mint}&chain={chain}"
    cache_key = f"bds_v3_trade_{mint}_{chain}"
    trade_data_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_trade_data", calling_script="paper_trader_dip.pt_get_bds_trade_data", cache_key=cache_key, cache_ttl_seconds=60, url=url, headers={"X-API-KEY": BIRDEYE_API_KEY})
    if isinstance(trade_data_response, dict) and trade_data_response.get("success"): data_payload = trade_data_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_metadata_single(mint: str, chain: str = "solana") -> Optional[Dict]:
    mint_list = [mint]; mints_param_value = urllib.parse.quote(",".join(mint_list)); cache_key_hash = hashlib.sha256(mints_param_value.encode()).hexdigest(); cache_key = f"bds_v3_meta_multi_{chain}_{cache_key_hash}"
    url = f"{BDS_METADATA_MULTIPLE_ENDPOINT}?list_address={mints_param_value}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    metadata_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="v3_metadata_multiple", calling_script="paper_trader_dip.pt_get_bds_metadata_single", cache_key=cache_key, cache_ttl_seconds=60*60, url=url, headers=headers)
    if isinstance(metadata_response, dict) and metadata_response.get("success"): data_payload = metadata_response.get("data"); return data_payload.get(mint) if isinstance(data_payload, dict) else None
    return None
def pt_get_bds_token_security(mint: str, chain: str = "solana") -> Optional[Dict]:
    url = f"{BDS_TOKEN_SECURITY_ENDPOINT}?address={mint}"; cache_key = f"bds_security_{mint}_{chain}"; headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": chain}
    security_response = api_usage_limiter.request_api(method=requests.get, api_name="birdeye", endpoint_name="token_security", calling_script="paper_trader_dip.pt_get_bds_token_security", cache_key=cache_key, cache_ttl_seconds=5*60*60, url=url, headers=headers)
    if isinstance(security_response, dict) and security_response.get("success"): data_payload = security_response.get("data"); return data_payload if isinstance(data_payload, dict) else None
    return None
def get_enriched_data_for_pending_candidate(mint_address: str) -> Optional[Dict]:
    print(f"        [Dip Buyer] Fetching enriched data snapshot for {mint_address[:6]}...") 
    enriched_data = {"token_mint": mint_address, "bds_metadata": None, "bds_market_data": None, "bds_holder_data": None, "bds_trade_data": None, "bds_security_data": None, "gmgn_security_info": None, "original_correlation_data": {"token_mint": mint_address}}
    enriched_data["bds_metadata"] = pt_get_bds_metadata_single(mint_address); time.sleep(0.05)
    enriched_data["bds_market_data"] = pt_get_bds_market_data(mint_address); time.sleep(0.05)
    enriched_data["bds_trade_data"] = pt_get_bds_trade_data(mint_address); time.sleep(0.05)
    enriched_data["bds_security_data"] = pt_get_bds_token_security(mint_address); time.sleep(0.05)
    if not enriched_data["bds_market_data"] or not enriched_data["bds_trade_data"] or not enriched_data["bds_security_data"]: print(f"        [!] [Dip Buyer] Failed to fetch essential enriched data for {mint_address[:6]}. Market={bool(enriched_data['bds_market_data'])}, Trade={bool(enriched_data['bds_trade_data'])}, Security={bool(enriched_data['bds_security_data'])}"); return None
    if enriched_data["bds_metadata"]: enriched_data["symbol"] = enriched_data["bds_metadata"].get("symbol", "N/A"); enriched_data["name"] = enriched_data["bds_metadata"].get("name", "N/A")
    else: enriched_data["symbol"] = "N/A"; enriched_data["name"] = "N/A"
    print(f"        [✓] [Dip Buyer] Enriched data snapshot fetched for {mint_address[:6]}."); return enriched_data

# (log_trade_to_csv, _calculate_metrics_from_trades, calculate_performance_metrics remain the same, use new log file name)
CSV_HEADER = ["exit_timestamp", "mint", "symbol", "entry_timestamp", "entry_price", "exit_price", "position_size_usd", "position_size_tokens", "pnl_usd", "pnl_pct", "exit_reason", "risk_level"]
def log_trade_to_csv(trade_data: Dict[str, Any]):
    file_exists = Path(TRADE_LOG_CSV_FILE).is_file() # Uses the dip buyer log file path
    try:
        with open(TRADE_LOG_CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADER)
            if not file_exists: writer.writeheader()
            row = {"exit_timestamp": datetime.datetime.fromtimestamp(trade_data["exit_timestamp"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),"mint": trade_data["mint"],"symbol": trade_data["symbol"],"entry_timestamp": datetime.datetime.fromtimestamp(trade_data["entry_timestamp"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),"entry_price": trade_data["entry_price"],"exit_price": trade_data["exit_price"],"position_size_usd": trade_data["position_size_usd"],"position_size_tokens": trade_data["position_size_tokens"],"pnl_usd": trade_data["pnl_usd"],"pnl_pct": f"{(Decimal(trade_data['pnl_usd']) / Decimal(trade_data['position_size_usd']) * 100 if Decimal(trade_data['position_size_usd']) > 0 else 0):.2f}","exit_reason": trade_data["exit_reason"],"risk_level": trade_data["risk_level_at_entry"]}
            writer.writerow(row)
    except IOError as e: print(f"    [✗] [Dip Buyer] Error writing to trade log CSV: {e}")
    except Exception as e_csv: print(f"    [✗] [Dip Buyer] Unexpected error logging trade to CSV: {e_csv}")
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
        except (TypeError, ValueError) as ve: print(f"    [!] [Dip Buyer] Invalid data for metrics calculation in trade {trade.get('mint')}: {ve}. PNL USD: '{trade.get('pnl_usd')}', Entry USD: '{trade.get('position_size_usd')}'")
        except Exception as e: print(f"    [!] [Dip Buyer] Error processing trade for metrics calculation: {trade.get('mint')} - {e}")
    win_rate = (wins / processed_trades_count) * 100 if processed_trades_count > 0 else 0; avg_win_pct = float(win_pnl_pct_sum / wins) if wins > 0 else 0.0; avg_loss_pct = float(loss_pnl_pct_sum / losses) if losses > 0 else 0.0
    return {"total_trades": processed_trades_count, "win_rate_pct": f"{win_rate:.1f}", "avg_win_pct": f"{avg_win_pct:.1f}", "avg_loss_pct": f"{avg_loss_pct:.1f}", "total_realized_pnl_usd_for_list": current_list_pnl_usd}
def calculate_performance_metrics(state: Dict[str, Any]): 
    history = state.get("trade_history", []); default_metrics = {"total_trades": 0, "win_rate_pct": "0.0", "avg_win_pct": "0.0", "avg_loss_pct": "0.0", "total_realized_pnl_usd": state.get('realized_pnl_usd', "0.0")}
    if not history: state['performance_metrics'] = default_metrics; return
    overall_metrics_cal = _calculate_metrics_from_trades(history)
    state['performance_metrics'] = {"total_trades": overall_metrics_cal['total_trades'], "win_rate_pct": overall_metrics_cal['win_rate_pct'], "avg_win_pct": overall_metrics_cal['avg_win_pct'], "avg_loss_pct": overall_metrics_cal['avg_loss_pct'], "total_realized_pnl_usd": state['realized_pnl_usd']}

# --- Updated Filter Function ---
def passes_strict_filters(item: Dict[str, Any], current_time: int) -> Tuple[bool, Optional[str]]:
    """Checks if an enriched item passes the stricter filters. Includes monitored holder count."""
    mint_addr = item.get("token_mint", "UNKNOWN_MINT")
    symbol = item.get("symbol", "N/A")
    
    market_data = item.get("bds_market_data") or {}
    trade_data = item.get("bds_trade_data") or {}
    metadata_raw = item.get("bds_metadata"); metadata = metadata_raw if isinstance(metadata_raw, dict) else {} 
    bds_security = item.get("bds_security_data") or {}; gmgn_security = item.get("gmgn_security_info") or {}
    original_data = item.get("original_correlation_data", {}) 

    # --- Monitored Holders Filter ---
    if original_data and "total_holders_in_set" in original_data: 
        monitored_holders_count = original_data.get("total_holders_in_set", 0)
        if not isinstance(monitored_holders_count, int) or monitored_holders_count < STRICT_MIN_MONITORED_HOLDERS:
            print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Monitored Holders ({monitored_holders_count} < {STRICT_MIN_MONITORED_HOLDERS})")
            return False, f"Low Monitored Holders ({monitored_holders_count} < {STRICT_MIN_MONITORED_HOLDERS})"
        
    # --- Age Filter ---
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
    if age_delta_seconds is None: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Age (Cannot Determine)"); return False, "Cannot Determine Age"
    if age_delta_seconds < (STRICT_MIN_AGE_MINUTES * 60): print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Age ({age_delta_seconds/60:.1f} min < {STRICT_MIN_AGE_MINUTES})"); return False, f"Too New (< {STRICT_MIN_AGE_MINUTES} min)"
        
    # --- Total Holders Filter ---
    total_holders_bds = trade_data.get("holder")
    if not isinstance(total_holders_bds, int) or total_holders_bds < STRICT_MIN_TOTAL_HOLDERS: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Total Holders ({total_holders_bds} < {STRICT_MIN_TOTAL_HOLDERS})"); return False, f"Low Total Holders ({total_holders_bds} < {STRICT_MIN_TOTAL_HOLDERS})"
        
    # --- Liquidity Filter ---
    liquidity = market_data.get("liquidity")
    if not isinstance(liquidity, (int, float)) or liquidity < STRICT_MIN_LIQUIDITY_USD: 
        liq_fmt = _format_compact(liquidity,'$',0); min_liq_fmt = _format_compact(STRICT_MIN_LIQUIDITY_USD,'$',0)
        print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Liquidity ({liq_fmt} < {min_liq_fmt})")
        return False, f"Low Liquidity ({liq_fmt} < {min_liq_fmt})"
        
    # --- Top Holder % Filter ---
    top_holder_pct_val = market_data.get("top_holder_pct") 
    if top_holder_pct_val is not None and (not isinstance(top_holder_pct_val, float) or top_holder_pct_val >= STRICT_MAX_TOP1_HOLDER_PCT):
        pct_disp = f"{top_holder_pct_val*100:.0f}%" if isinstance(top_holder_pct_val, float) else "N/A"
        print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Top Holder % ({pct_disp} >= {STRICT_MAX_TOP1_HOLDER_PCT*100:.0f}%)")
        return False, f"High Top1 Holder % ({pct_disp} >= {STRICT_MAX_TOP1_HOLDER_PCT*100:.0f}%)"
        
    # --- Socials Filter ---
    if REQUIRE_SOCIALS:
        extensions_raw = metadata.get("extensions"); extensions = extensions_raw if isinstance(extensions_raw, dict) else {}
        has_social = any(k in extensions for k in ["website", "twitter", "telegram"] if extensions.get(k))
        if not has_social: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Socials Check"); return False, "Missing Social Links"
            
    # --- Security Filters ---
    if bds_security.get('mutableMetadata') == True: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Security (Mutable Metadata)"); return False, "Mutable Metadata"
    if bds_security.get('freezeable') == True and bds_security.get('freezeAuthority') and bds_security['freezeAuthority'] not in ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Security (Freeze Authority Active)"); return False, "Freeze Authority Active"
    if gmgn_security and gmgn_security.get('is_honeypot') == True: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Security (GMGN Honeypot)"); return False, "GMGN Honeypot"
    if gmgn_security and gmgn_security.get('is_mintable') == True: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Security (GMGN Mintable)"); return False, "GMGN Mintable"
        
    # --- Optional 1H Positive Filter ---
    if STRICT_REQUIRE_POSITIVE_1H_CHANGE:
        price_change_1h_pct_raw = (trade_data.get("priceChangePercent") or {}).get("h1") # Check correct key
        if price_change_1h_pct_raw is None: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Missing 1H Price Change Data"); return False, "Missing 1h Price Change Data"
        try: price_change_1h_pct = float(price_change_1h_pct_raw)
        except (ValueError, TypeError): print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Invalid 1H Price Change Data Type ({type(price_change_1h_pct_raw)})"); return False, "Invalid 1h Price Change Data Type"
        if price_change_1h_pct < 0: pct_fmt = _format_pct(price_change_1h_pct); print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Price Change 1H ({pct_fmt})"); return False, f"Negative 1h Price Change ({pct_fmt})" 
            
    # --- Optional 6H Negative Filter ---
    if REQUIRE_NEGATIVE_6H_CHANGE:
        price_change_6h_pct_raw = (trade_data.get("priceChangePercent") or {}).get("h6") # Use correct key
        if price_change_6h_pct_raw is None: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Missing 6H Price Change Data"); return False, "Missing 6h Price Change Data"
        try: price_change_6h_pct = float(price_change_6h_pct_raw)
        except (ValueError, TypeError): print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Invalid 6H Price Change Data Type ({type(price_change_6h_pct_raw)})"); return False, "Invalid 6h Price Change Data Type"
        if price_change_6h_pct >= 0: pct_fmt = _format_pct(price_change_6h_pct); print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Price Change 6H ({pct_fmt} is not negative)"); return False, f"Non-Negative 6h Price Change ({pct_fmt})"
            
    # --- Optional 24H Max Loss Filter ---
    if FILTER_MAX_24H_LOSS_PCT is not None:
        price_change_24h_pct_raw = (trade_data.get("priceChangePercent") or {}).get("h24") # Use correct key
        if price_change_24h_pct_raw is None: print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Missing 24H Price Change Data"); return False, "Missing 24h Price Change Data"
        try: price_change_24h_pct = float(price_change_24h_pct_raw)
        except (ValueError, TypeError): print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Invalid 24H Price Change Data Type ({type(price_change_24h_pct_raw)})"); return False, "Invalid 24h Price Change Data Type"
        if price_change_24h_pct < float(FILTER_MAX_24H_LOSS_PCT): # Compare floats
            pct_fmt = _format_pct(price_change_24h_pct)
            print(f"    [Dip Buyer Filter Fail] {symbol} ({mint_addr[:6]}): Failed Max Loss 24H ({pct_fmt} < {float(FILTER_MAX_24H_LOSS_PCT):.1f}%)")
            return False, f"Excessive 24h Loss ({pct_fmt})"

    return True, None

# --- Updated Ranking Function ---
def rank_trade_candidates(candidate_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scored_candidates = []
    if not candidate_items: return []
    print(f"    [*] [Dip Buyer] Ranking {len(candidate_items)} candidates...") 
    for item in candidate_items:
        score = Decimal("0.0"); reasons = []; market_data = item.get("bds_market_data") or {}; trade_data = item.get("bds_trade_data") or {}
        metadata_raw = item.get("bds_metadata"); metadata = metadata_raw if isinstance(metadata_raw, dict) else {}; original_data = item.get("original_correlation_data", {})
        
        # --- Standard Scoring ---
        liq = market_data.get("liquidity")
        if isinstance(liq, (int, float)):
            if liq > 50000: score += Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.5"); reasons.append("Liq>50k")
            elif liq > 20000: score += Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("1.0"); reasons.append("Liq>20k")
            elif liq > STRICT_MIN_LIQUIDITY_USD: score += Decimal(str(SCORE_WEIGHTS["liquidity"])) * Decimal("0.5"); reasons.append(f"Liq>{STRICT_MIN_LIQUIDITY_USD/1000:.0f}k")
        vol24h = trade_data.get("volume24h")
        if isinstance(liq, (int, float)) and isinstance(vol24h, (int, float)) and liq > 1000: 
            liq_dec = Decimal(str(liq)); ratio = Decimal(str(vol24h)) / (liq_dec if liq_dec != Decimal(0) else Decimal('inf'))
            if ratio > 10: score += Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.5"); reasons.append("V/L>10")
            elif ratio > 5: score += Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("1.0"); reasons.append("V/L>5")
            elif ratio > 2: score += Decimal(str(SCORE_WEIGHTS["vol_liq_ratio"])) * Decimal("0.5"); reasons.append("V/L>2")
            
        # Score 1h price change (positive or negative contributes)
        price_1h_raw = (trade_data.get("priceChangePercent") or {}).get("h1") 
        if price_1h_raw is not None: 
            try:
                price_1h = float(price_1h_raw)
                # Use absolute value for momentum scoring? Or keep directional? Let's keep directional but less weight.
                momentum_score_1h = max(Decimal(-1.0), min(Decimal(price_1h) / Decimal("20.0"), Decimal("1.5"))) # Allow negative contribution capped at -1.0
                score += Decimal(str(SCORE_WEIGHTS["price_change_1h"])) * momentum_score_1h
                reasons.append(f"1h:{price_1h:+.1f}%")
            except (ValueError, TypeError): pass 
            
        total_holders = trade_data.get("holder")
        if isinstance(total_holders, int):
            if total_holders > 1000: score += Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("1.0"); reasons.append("Hd>1k")
            elif total_holders > 250: score += Decimal(str(SCORE_WEIGHTS["total_holders"])) * Decimal("0.5"); reasons.append("Hd>250")
        monitored_holders = original_data.get("total_holders_in_set", 0) 
        if monitored_holders > 5: score += Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("1.0"); reasons.append(f"Mon>{monitored_holders}") 
        elif monitored_holders >= STRICT_MIN_MONITORED_HOLDERS: score += Decimal(str(SCORE_WEIGHTS["monitored_holders"])) * Decimal("0.5"); reasons.append(f"Mon>{monitored_holders}")
        
        top1_pct = market_data.get("top_holder_pct")
        if isinstance(top1_pct, float):
            if top1_pct < 0.30: score += Decimal(str(abs(float(SCORE_WEIGHTS["top1_holder_pct"])))) * Decimal("0.5") 
            elif top1_pct < STRICT_MAX_TOP1_HOLDER_PCT: score += Decimal("0.0")
            else: penalty_denominator = (Decimal("1.0") - Decimal(str(STRICT_MAX_TOP1_HOLDER_PCT))); penalty_factor = (Decimal(str(top1_pct)) - Decimal(str(STRICT_MAX_TOP1_HOLDER_PCT))) / (penalty_denominator if penalty_denominator != Decimal(0) else Decimal('inf')); score += Decimal(str(SCORE_WEIGHTS["top1_holder_pct"])) * penalty_factor; reasons.append(f"Top1 {top1_pct*100:.0f}%")
        creation_dt = None; raw_creation_ts_sources = [metadata.get("created_timestamp"), (item.get("bds_security_data", {}) or {}).get("creationTime"), (item.get("bds_security_data", {}) or {}).get("creation_timestamp_ms")]
        if original_data: raw_creation_ts_sources.insert(0, original_data.get("first_event_timestamp_utc"))
        for ts_source in raw_creation_ts_sources:
            if ts_source is None: continue
            try: formatted_ts = default_format_timestamp(ts_source)
            except Exception: continue
            if formatted_ts != "N/A" and formatted_ts != str(ts_source):
                try: creation_dt = datetime.datetime.strptime(formatted_ts, '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc); break
                except ValueError: continue
        if creation_dt:
            age_delta_seconds = (datetime.datetime.now(timezone.utc) - creation_dt).total_seconds()
            if age_delta_seconds >= 3600 and age_delta_seconds < 86400: score += Decimal(str(SCORE_WEIGHTS["age_score"])) * Decimal("1.0"); reasons.append("Age 1h-24h")
            elif age_delta_seconds >= STRICT_MIN_AGE_MINUTES * 60: score += Decimal(str(SCORE_WEIGHTS["age_score"])) * Decimal("0.5"); reasons.append(f"Age >{STRICT_MIN_AGE_MINUTES}m")
        extensions_raw = metadata.get("extensions"); extensions = extensions_raw if isinstance(extensions_raw, dict) else {}
        has_social = any(k in extensions for k in ["website", "twitter", "telegram"] if extensions.get(k))
        if has_social: score += Decimal(str(SCORE_WEIGHTS["socials"])); reasons.append("Socials")

        # --- Score based on 6H Dip Depth ---
        price_change_6h_pct_raw = (trade_data.get("priceChangePercent") or {}).get("h6")
        if price_change_6h_pct_raw is not None:
            try:
                price_change_6h_pct = float(price_change_6h_pct_raw)
                if price_change_6h_pct < 0: # Only score if it's actually negative
                    # Scale score: -50% = score factor 1.0. Max score factor 1.5
                    dip_score_factor = min(abs(Decimal(price_change_6h_pct)) / Decimal("50.0"), Decimal("1.5")) 
                    dip_score_contribution = dip_score_factor * Decimal(str(SCORE_WEIGHTS["price_change_6h_neg"]))
                    score += dip_score_contribution
                    reasons.append(f"Dip6h:{price_change_6h_pct:.1f}%")
            except (ValueError, TypeError): pass # Ignore if not valid number
        # --- End Dip Scoring ---
        
        item['_score'] = score; item['_score_reasons'] = reasons; scored_candidates.append(item)
        
    scored_candidates.sort(key=lambda x: x['_score'], reverse=True)
    print(f"    [*] [Dip Buyer] Ranking Complete. Top {min(3, len(scored_candidates))} candidates:")
    for i, item in enumerate(scored_candidates[:3]): print(f"        {i+1}. {item.get('symbol','???')} ({item.get('token_mint')[:6]}...) Score: {item['_score']:.2f} (Reasons: {', '.join(item.get('_score_reasons',[]))})")
    return scored_candidates

# (manage_portfolio uses original MD now)
def manage_portfolio(state: Dict[str, Any]):
    if 'open_positions' in state and state['open_positions']: print(f"    [*] [Dip Buyer] Managing {len(state['open_positions'])} open positions...")
    else: return 
    mints_to_check = list(state['open_positions'].keys()); closed_positions_this_cycle = 0; current_time = int(time.time())
    for mint in mints_to_check:
        position = state['open_positions'].get(mint)
        if not position or position.get('status') != 'OPEN': continue
        symbol = position.get('symbol', mint[:6]); symbol_safe = escape_markdown_original(symbol); mint_short_safe = escape_markdown_original(mint[:4]+"..")
        current_price = get_current_price(mint); time.sleep(0.1) 
        if current_price is None: print(f"    [!] [Dip Buyer] Could not get current price for open position {symbol}. Skipping checks."); continue
        entry_price = Decimal(position['entry_price']); initial_stop_loss_price = Decimal(position['initial_stop_loss_price']); take_profit_price = Decimal(position['take_profit_price'])
        position_size_tokens = Decimal(position['position_size_tokens']); position_size_usd_entry = Decimal(position['position_size_usd']); entry_timestamp = position['entry_timestamp']
        highest_price_seen = Decimal(position.get('highest_price_seen', position['entry_price'])); highest_price_seen = max(highest_price_seen, current_price); position['highest_price_seen'] = str(highest_price_seen)
        trailing_stop_price = highest_price_seen * (Decimal("1") - TRAILING_STOP_LOSS_PCT); effective_stop_loss_price = max(initial_stop_loss_price, trailing_stop_price); position['effective_stop_loss_price'] = str(effective_stop_loss_price)
        exit_reason = None; exit_price = None; current_price_fmt_log = _format_compact(current_price, '$', 6); effective_sl_fmt_log = _format_compact(effective_stop_loss_price, '$', 6); trailing_sl_fmt_log = _format_compact(trailing_stop_price, '$', 6); tp_price_fmt_log = _format_compact(take_profit_price, '$', 6)
        if current_price <= effective_stop_loss_price: exit_reason = "STOPPED_OUT"; exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT); print(f"    [!] [Dip Buyer] STOP LOSS triggered for {symbol} at price {current_price_fmt_log} (Effective SL: {effective_sl_fmt_log}, Trail: {trailing_sl_fmt_log})"); state['last_stop_loss_timestamp'] = current_time
        elif current_price >= take_profit_price: exit_reason = "TAKE_PROFIT"; exit_price = current_price * (Decimal("1") - SIMULATED_SLIPPAGE_PCT); print(f"    [+] [Dip Buyer] TAKE PROFIT triggered for {symbol} at price {current_price_fmt_log} (TP: {tp_price_fmt_log})")
        if exit_reason and exit_price is not None: 
            closed_positions_this_cycle += 1; exit_value_usd = position_size_tokens * exit_price; exit_value_usd_after_fee = exit_value_usd * (Decimal("1") - SIMULATED_FEE_PCT); pnl_usd = exit_value_usd_after_fee - position_size_usd_entry; pnl_pct = (pnl_usd / position_size_usd_entry) * 100 if position_size_usd_entry > 0 else Decimal("0")
            state['current_balance_usd'] = str(Decimal(state['current_balance_usd']) + pnl_usd); state['realized_pnl_usd'] = str(Decimal(state['realized_pnl_usd']) + pnl_usd)
            pnl_usd_fmt = f"{pnl_usd:+.2f}"; pnl_pct_fmt = f"{pnl_pct:+.1f}"; entry_price_fmt = _format_compact(entry_price, '$', 6); exit_price_fmt = _format_compact(exit_price, '$', 6); portfolio_fmt = _format_compact(state['current_balance_usd'], '$', 2) 
            if exit_reason == "STOPPED_OUT" and abs(pnl_pct / 100) >= KILL_SWITCH_LOSS_PCT:
                 kill_switch_until = current_time + KILL_SWITCH_DURATION_SECONDS; state['kill_switch_active_until_ts'] = kill_switch_until; kill_switch_msg = f"🚨 *[Dip Buyer] KILL SWITCH TRIGGERED!* 🚨\nTrade: {symbol_safe}\nLoss: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\nHalting new trades for {KILL_SWITCH_DURATION_SECONDS // 60} mins."; print(f"    [!!!] [Dip Buyer] KILL SWITCH TRIGGERED by {symbol} loss ({pnl_pct:.1f}%). Halting new trades until {default_format_timestamp(kill_switch_until)}."); send_trader_telegram(kill_switch_msg)
            closed_trade_data = {"mint": mint, "symbol": symbol, "entry_timestamp": entry_timestamp, "exit_timestamp": current_time, "entry_price": str(entry_price), "exit_price": str(exit_price), "position_size_usd": str(position_size_usd_entry), "position_size_tokens": str(position_size_tokens), "pnl_usd": str(pnl_usd), "exit_reason": exit_reason, "risk_level_at_entry": position.get("risk_level"), "pnl_pct": f"{pnl_pct:.2f}"}
            if 'trade_history' not in state: state['trade_history'] = []
            state['trade_history'].append(closed_trade_data); log_trade_to_csv(closed_trade_data); del state['open_positions'][mint]
            alert_text = (f"Closed {symbol_safe} ({mint_short_safe})\nReason: {exit_reason}\nPnL: {pnl_usd_fmt} USD ({pnl_pct_fmt}%)\nEntry: {entry_price_fmt} | Exit: {exit_price_fmt}\nPortfolio: {portfolio_fmt} USD")
            send_trader_telegram(alert_text) 
            if pnl_usd > 0: check_and_update_risk_level(state) 
    if closed_positions_this_cycle > 0: print(f"    [*] [Dip Buyer] Closed {closed_positions_this_cycle} position(s) this cycle."); calculate_performance_metrics(state) 

# (check_and_update_risk_level uses original MD now)
def check_and_update_risk_level(state: Dict[str, Any]):
    current_balance = Decimal(state['current_balance_usd']); initial_capital = Decimal(state['initial_capital_usd'])
    current_risk_level = state['risk_level']; daily_quota_achieved = state['daily_quota_achieved']; new_risk_level = current_risk_level; new_quota_status = daily_quota_achieved
    balance_fmt = _format_compact(current_balance, '$', 2) 
    if current_risk_level == 0 and current_balance >= initial_capital * RISK_LEVEL_1_ENTRY_THRESHOLD:
        new_risk_level = 1; msg = f"🚀 Risk Level Increased: 0 -> 1\nBalance: {balance_fmt} USD"; print(f"    [!] [Dip Buyer] Risk Level Increased: 0 -> 1 (Balance {current_balance:.2f} >= {initial_capital * RISK_LEVEL_1_ENTRY_THRESHOLD:.2f})"); send_trader_telegram(msg)
    if current_risk_level == 1 and current_balance >= initial_capital * RISK_LEVEL_2_ENTRY_THRESHOLD: 
        new_risk_level = 2; new_quota_status = True; msg = f"🏆 Daily Quota Achieved! 🏆\nRisk Level: 1 -> 2\nBalance: {balance_fmt} USD\nNo new trades today."; print(f"    [!] [Dip Buyer] DAILY QUOTA ACHIEVED! Risk Level: 1 -> 2 (Balance {current_balance:.2f} >= {initial_capital * RISK_LEVEL_2_ENTRY_THRESHOLD:.2f})"); send_trader_telegram(msg)
    state['risk_level'] = new_risk_level; state['daily_quota_achieved'] = new_quota_status

# (enter_paper_trade uses original MD now)
def enter_paper_trade(mint: str, symbol: str, name: str, entry_price: Decimal, state: Dict[str, Any]) -> bool:
    current_balance = Decimal(state['current_balance_usd']); initial_capital = Decimal(state['initial_capital_usd']); risk_level = state['risk_level']
    if risk_level == 0: allocation_pct = RISK_LEVEL_0_ALLOCATION_PCT; capital_base = initial_capital; tp_pct = RISK_LEVEL_0_TP_PCT
    elif risk_level == 1: allocation_pct = RISK_LEVEL_1_ALLOCATION_PCT; capital_base = current_balance; tp_pct = RISK_LEVEL_1_TP_PCT
    else: allocation_pct = RISK_LEVEL_2_ALLOCATION_PCT; capital_base = current_balance; tp_pct = RISK_LEVEL_2_TP_PCT
    position_size_usd = capital_base * allocation_pct; position_size_usd_after_fee = position_size_usd * (Decimal("1") - SIMULATED_FEE_PCT); simulated_entry_price = entry_price * (Decimal("1") + SIMULATED_SLIPPAGE_PCT)
    if simulated_entry_price <= 0: print(f"    [!] [Dip Buyer] Invalid simulated entry price ({_format_compact(simulated_entry_price, '$', 8)}) for {symbol}."); return False
    position_size_tokens = position_size_usd_after_fee / simulated_entry_price; initial_stop_loss_price = simulated_entry_price * (Decimal("1") - STOP_LOSS_PCT); take_profit_price = simulated_entry_price * (Decimal("1") + tp_pct)
    state['open_positions'][mint] = {"symbol": symbol, "name": name, "entry_price": str(simulated_entry_price), "entry_timestamp": int(time.time()), "position_size_usd": str(position_size_usd), "position_size_tokens": str(position_size_tokens), "initial_stop_loss_price": str(initial_stop_loss_price), "take_profit_price": str(take_profit_price), "risk_level": risk_level, "status": "OPEN", "highest_price_seen": str(simulated_entry_price) }
    log_entry_price_fmt = _format_compact(simulated_entry_price, '$', 6); log_target_price_fmt = _format_compact(entry_price, '$', 6); log_sl_fmt = _format_compact(initial_stop_loss_price, '$', 6); log_tp_fmt = _format_compact(take_profit_price, '$', 6)
    print(f"    [+] [Dip Buyer] Entered PAPER TRADE for {symbol} ({mint[:6]}...)"); print(f"        Size: {position_size_usd:.2f} USD ({position_size_tokens:.4f} {symbol})"); print(f"        Entry: ~{log_entry_price_fmt} (Target: {log_target_price_fmt})"); print(f"        Initial SL: {log_sl_fmt} (-{STOP_LOSS_PCT:.0%})"); print(f"        TP: {log_tp_fmt} (+{tp_pct:.0%})"); print(f"        Risk Level: {risk_level}")
    symbol_safe = escape_markdown_original(symbol); tg_entry_price_fmt = _format_compact(simulated_entry_price, '$', 5); tg_sl_fmt = _format_compact(initial_stop_loss_price, '$', 5); tg_tp_fmt = _format_compact(take_profit_price, '$', 5); size_fmt = f"${position_size_usd:.2f}"
    alert_text = (f"➡️ Entered Paper Trade: *{symbol_safe}*\nEntry Price: {tg_entry_price_fmt}\nSize: {size_fmt}\nSL: {tg_sl_fmt} | TP: {tg_tp_fmt}\nRisk Level: {risk_level}")
    send_trader_telegram(alert_text) # Uses dip buyer prefix
    return True

# (manage_pending_entries remains the same logic, uses updated passes_strict_filters)
def manage_pending_entries(state: Dict[str, Any]):
    if 'pending_entry' not in state or not state['pending_entry']: return
    print(f"    [*] [Dip Buyer] Managing {len(state['pending_entry'])} pending entries...")
    mints_to_process = list(state['pending_entry'].keys()); current_time = time.time()
    for mint in mints_to_process:
        if mint not in state['pending_entry']: continue 
        pending_data = state['pending_entry'][mint]; symbol = pending_data.get('symbol', mint[:6]); monitoring_start_time = pending_data.get('monitoring_start_time', 0); entry_window_seconds = pending_data.get('entry_window_seconds', PENDING_ENTRY_WINDOW_SECONDS); signal_price_str = pending_data.get('signal_price')
        if current_time > monitoring_start_time + entry_window_seconds: print(f"    [-] [Dip Buyer] Pending entry window expired for {symbol} ({mint[:6]}). Removing."); del state['pending_entry'][mint]; continue
        if (state['daily_quota_achieved'] or state.get('daily_loss_limit_hit', False) or len(state['open_positions']) >= MAX_OPEN_POSITIONS or current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS or current_time < state.get('kill_switch_active_until_ts', 0)): continue
        current_price_quick = get_current_price(mint); time.sleep(0.05) 
        if current_price_quick is None: print(f"    [!] [Dip Buyer] Could not get quick price for pending entry {symbol}. Retrying next cycle."); continue
        price_condition_met = False
        if signal_price_str and signal_price_str != 'N/A': 
            try: signal_price = Decimal(signal_price_str); lower_bound = signal_price * (Decimal(1) - PENDING_ENTRY_PRICE_DEVIATION_PCT); upper_bound = signal_price * (Decimal(1) + PENDING_ENTRY_PRICE_DEVIATION_PCT); price_condition_met = (lower_bound <= current_price_quick <= upper_bound)
            except Exception as e: print(f"    [!] [Dip Buyer] Error checking price deviation for {symbol}: {e}"); continue 
            if not price_condition_met: print(f"        [Dip Buyer] Price {_format_compact(current_price_quick, '$', 6)} for {symbol} outside deviation of {_format_compact(signal_price, '$', 6)}.")
        else: print(f"    [?] [Dip Buyer] Signal price missing/invalid for {symbol}, considering price condition met for re-validation."); price_condition_met = True 
        if not price_condition_met: continue
        print(f"    [>] [Dip Buyer] Price condition met for {symbol}. Fetching full data for re-validation...")
        fresh_enriched_data = get_enriched_data_for_pending_candidate(mint)
        if not fresh_enriched_data: print(f"    [!] [Dip Buyer] Failed to fetch fresh enriched data for {symbol}. Skipping entry attempt this cycle."); continue
        passes_final_check, reason = passes_strict_filters(fresh_enriched_data, int(current_time)) # Will use new filters
        if not passes_final_check: print(f"    [!] [Dip Buyer] {symbol} ({mint[:6]}) failed final strict filter re-validation before entry: {reason}. Removing from pending."); del state['pending_entry'][mint]; continue
        print(f"    [✓] [Dip Buyer] {symbol} ({mint[:6]}) passed final re-validation.")
        current_price_from_market = (fresh_enriched_data.get("bds_market_data") or {}).get("price")
        if current_price_from_market is not None:
            try: price_to_enter = Decimal(str(current_price_from_market)); print(f"        [Dip Buyer] Using entry price from fresh market data: {_format_compact(price_to_enter, '$', 6)}")
            except Exception as e: print(f"        [!] [Dip Buyer] Error converting market data price {current_price_from_market} to Decimal. Using quick price. Error: {e}"); price_to_enter = current_price_quick
        else: print(f"        [!] [Dip Buyer] Price missing in fresh market data for {symbol}. Using quick price."); price_to_enter = current_price_quick
        trade_name = fresh_enriched_data.get("name", pending_data.get('name', 'N/A'))
        print(f"    [*] [Dip Buyer] Attempting to execute paper trade for {symbol}...") 
        if enter_paper_trade(mint, symbol, trade_name, price_to_enter, state): print(f"    [>>>] [Dip Buyer] Successfully executed paper trade entry for {symbol} after re-validation."); del state['pending_entry'][mint] 
        else: print(f"    [!] [Dip Buyer] Failed to execute paper trade entry for {symbol} despite passing re-validation.")

# (Summary function uses original MD now)
def generate_and_send_performance_summary_md_original(state: Dict[str, Any], period_hours: int, period_name: str, current_time: int):
    since_timestamp = current_time - (period_hours * 60 * 60); history = state.get("trade_history", []); period_trades = [t for t in history if t.get("exit_timestamp") is not None and int(t["exit_timestamp"]) >= since_timestamp]
    summary_msg = f"📊 *{period_name} Performance Summary* 📊\n\n"; summary_msg += f"Period: Last {period_hours} hours\n"
    if not period_trades: print(f"    [i] [Dip Buyer] No trades in the last {period_hours} hours to summarize."); summary_msg += f"No trades closed in this period.\n\n" 
    else:
        period_metrics_cal = _calculate_metrics_from_trades(period_trades); pnl_period_fmt = f"{period_metrics_cal['total_realized_pnl_usd_for_list']:.2f}"; wr_period_fmt = period_metrics_cal['win_rate_pct']; avg_win_fmt = period_metrics_cal['avg_win_pct']; avg_loss_fmt = period_metrics_cal['avg_loss_pct']
        summary_msg += f"Trades Closed: {period_metrics_cal['total_trades']}\n"; summary_msg += f"Realized PnL (Period): {pnl_period_fmt} USD\n"; summary_msg += f"Win Rate (Period): {wr_period_fmt}%\n"; summary_msg += f"Avg Win (Period): {avg_win_fmt}%\n"; summary_msg += f"Avg Loss (Period): {avg_loss_fmt}%\n\n"
    summary_msg += f"---Portfolio Status---\n"; current_balance_fmt = _format_compact(state['current_balance_usd'], '$', 2); total_pnl_fmt = f"{Decimal(state.get('performance_metrics', {}).get('total_realized_pnl_usd', '0.00')):.2f}"
    summary_msg += f"Current Balance: {current_balance_fmt} USD\n"; summary_msg += f"Total Realized PnL (All Time): {total_pnl_fmt} USD" 
    send_trader_telegram(summary_msg) # Uses dip buyer prefix
    if period_hours == 4: state['last_4hr_summary_ts'] = current_time
    elif period_hours == 24: state['last_daily_summary_ts'] = current_time
    save_trader_state(state) 

# --- Main Loop ---
def main_trading_loop():
    start_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"--- Starting Paper Trader [DIP BUYER] (v1.1.1 - Fix PnL Log) ({start_time_str}) ---") # Version updated
    print(f"Reading last {NUM_REPORTS_TO_SCAN} processed reports from: {PROCESSED_REPORTS_DIR}")
    print(f"Trader State File: {TRADER_STATE_FILE}") 
    print(f"Trading Hours (UTC): {TRADING_START_HOUR_UTC:02d}:00 - {TRADING_END_HOUR_UTC:02d}:00")
    print(f"Strict Filter: Min Monitored Holders = {STRICT_MIN_MONITORED_HOLDERS}") 
    print(f"Strict Filter: Require Positive 1H Change = {STRICT_REQUIRE_POSITIVE_1H_CHANGE}") 
    print(f"Strict Filter: Require Negative 6H Change = {REQUIRE_NEGATIVE_6H_CHANGE}") 
    print(f"Strict Filter: Max 24H Loss = {FILTER_MAX_24H_LOSS_PCT}%" if FILTER_MAX_24H_LOSS_PCT else "Strict Filter: Max 24H Loss = Disabled") 
    print(f"Score Weight: Monitored Holders = {SCORE_WEIGHTS['monitored_holders']}") 
    print(f"Score Weight: Negative 6H Change = {SCORE_WEIGHTS['price_change_6h_neg']}") 

    state = load_trader_state() # Loads dip buyer state
    if not state: # Initialize state
        print("[*] [Dip Buyer] Initializing new trader state.")
        state = {"initial_capital_usd": str(INITIAL_CAPITAL_USD),"current_balance_usd": str(INITIAL_CAPITAL_USD),"capital_at_quota_start": str(INITIAL_CAPITAL_USD),"realized_pnl_usd": "0.0","risk_level": 0,"daily_quota_achieved": False,"last_quota_reset_timestamp": int(time.time()),"open_positions": {},"trade_history": [],"last_stop_loss_timestamp": 0,"kill_switch_active_until_ts": 0,"pending_entry": {},"performance_metrics": {},"recently_pending": {},"last_4hr_summary_ts": 0,"last_daily_summary_ts": 0,"prev_day_total_realized_pnl_at_reset": "0.0"}
        calculate_performance_metrics(state); save_trader_state(state)
    else: # Load existing state & ensure all keys
        state.setdefault("initial_capital_usd", str(INITIAL_CAPITAL_USD)); state.setdefault("current_balance_usd", state.get("initial_capital_usd", str(INITIAL_CAPITAL_USD))); state.setdefault("capital_at_quota_start", state.get("current_balance_usd", str(INITIAL_CAPITAL_USD))); state.setdefault("realized_pnl_usd", "0.0"); state.setdefault("risk_level", 0); state.setdefault("daily_quota_achieved", False); state.setdefault("last_quota_reset_timestamp", 0); state.setdefault("open_positions", {}); state.setdefault("trade_history", []); state.setdefault("last_stop_loss_timestamp", 0); state.setdefault("kill_switch_active_until_ts", 0); state.setdefault("pending_entry", {}); state.setdefault("performance_metrics", {}); state.setdefault("recently_pending", {}); state.setdefault("last_4hr_summary_ts", 0); state.setdefault("last_daily_summary_ts", 0); state.setdefault("prev_day_total_realized_pnl_at_reset", "0.0")
        state['initial_capital_usd'] = str(Decimal(state['initial_capital_usd'])); state['current_balance_usd'] = str(Decimal(state['current_balance_usd'])); state['capital_at_quota_start'] = str(Decimal(state['capital_at_quota_start'])); state['realized_pnl_usd'] = str(Decimal(state['realized_pnl_usd']))
        if not state.get('performance_metrics') or 'total_realized_pnl_usd' not in state['performance_metrics']: calculate_performance_metrics(state)

    print(f"[*] [Dip Buyer] Current State: Balance=${Decimal(state['current_balance_usd']):.2f}, Risk Level={state['risk_level']}, Quota Met={state['daily_quota_achieved']}")
    print(f"[*] [Dip Buyer] Tracking {len(state.get('open_positions', {}))} open positions, {len(state.get('pending_entry', {}))} pending entries.")

    last_report_scan_time = 0
    
    while True:
        current_time = int(time.time()); current_dt_utc = datetime.datetime.now(timezone.utc); current_hour_utc = current_dt_utc.hour
        print(f"\n--- [Dip Buyer] Loop Start: {default_format_timestamp(current_time)} ---") 
        try:
            # --- Daily Reset Check ---
            today_start_utc = current_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0); today_start_ts = int(today_start_utc.timestamp())
            if state.get('last_quota_reset_timestamp', 0) < today_start_ts:
                print(f"\n--- [Dip Buyer] New Day [{today_start_utc.strftime('%Y-%m-%d')}] Resetting Daily Quota & Risk Level ---")
                state['daily_quota_achieved'] = False; state['risk_level'] = 0; state.pop('daily_loss_limit_hit', None); state['capital_at_quota_start'] = state['current_balance_usd']; state['last_quota_reset_timestamp'] = current_time
                prev_day_pnl_usd = Decimal(state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")) - Decimal(state.get('prev_day_total_realized_pnl_at_reset', "0.0"))
                calculate_performance_metrics(state); state['prev_day_total_realized_pnl_at_reset'] = state.get('performance_metrics',{}).get('total_realized_pnl_usd', "0.0")
                start_balance_fmt = _format_compact(state['capital_at_quota_start'], '$', 2); prev_day_pnl_fmt = _format_compact(prev_day_pnl_usd, '$', 2) 
                send_trader_telegram(f"🌅 New Day! Daily quota reset. Risk level set to 0.\nStart Balance: {start_balance_fmt} USD\nPrev Day PnL: {prev_day_pnl_fmt} USD")
                save_trader_state(state) 

            # --- Manage Positions & Pending ---
            manage_portfolio(state); 
            manage_pending_entries(state) 

            # --- Check Operational Halts ---
            print("    [*] [Dip Buyer] Checking operational halts...")
            current_balance_dec = Decimal(state['current_balance_usd']); capital_start_dec = Decimal(state['capital_at_quota_start']); unrealized_pnl = Decimal("0.0")
            if state['open_positions']:
                 # --- FIX: Uncomment PnL Logging ---
                 print(f"        Calculating unrealized PnL for {len(state['open_positions'])} positions...") 
                 # --- END FIX ---
                 for mint_pos, pos_data in state['open_positions'].items(): 
                     current_price_for_pnl = get_current_price(mint_pos) 
                     if current_price_for_pnl:
                         try: entry_val = Decimal(pos_data['position_size_usd']); current_val = Decimal(pos_data['position_size_tokens']) * current_price_for_pnl; unrealized_pnl += (current_val - entry_val)
                         except Exception as dec_err: print(f"        [!] [Dip Buyer] Error calculating unrealized PNL for {mint_pos}: {dec_err}")
                     time.sleep(0.05)
                 # --- FIX: Uncomment PnL Logging ---
                 print(f"        Unrealized PnL: {unrealized_pnl:+.2f} USD") 
                 # --- END FIX ---
            effective_balance = current_balance_dec + unrealized_pnl; daily_drawdown = (capital_start_dec - effective_balance) / capital_start_dec if capital_start_dec > 0 else Decimal("0.0")
            if daily_drawdown >= DAILY_LOSS_LIMIT_PCT:
                if not state.get('daily_loss_limit_hit', False):
                    eff_balance_fmt = _format_compact(str(effective_balance), '$', 2); daily_drawdown_fmt = f"{daily_drawdown:.1%}"
                    print(f"    [!!!] [Dip Buyer] DAILY LOSS LIMIT HIT! Drawdown: {daily_drawdown:.2%} >= {DAILY_LOSS_LIMIT_PCT:.1%}. Halting new entries."); send_trader_telegram(f"🛑 Daily Loss Limit Hit ({daily_drawdown_fmt})! 🛑\nNo new trades today.\nEffective Balance: {eff_balance_fmt} USD"); state['daily_loss_limit_hit'] = True
            else: state['daily_loss_limit_hit'] = False 
            is_trading_hours = TRADING_START_HOUR_UTC <= current_hour_utc < TRADING_END_HOUR_UTC; is_sl_cooldown = current_time < state.get('last_stop_loss_timestamp', 0) + STOP_LOSS_COOLDOWN_SECONDS; is_kill_switch_active = current_time < state.get('kill_switch_active_until_ts', 0)
            num_open_plus_pending = len(state['open_positions']) + len(state.get('pending_entry', {}))
            can_consider_new_trade = (not state['daily_quota_achieved'] and not state.get('daily_loss_limit_hit', False) and is_trading_hours and not is_sl_cooldown and not is_kill_switch_active)
            print(f"        [Dip Buyer] Daily Quota: {state['daily_quota_achieved']}, Loss Limit: {state.get('daily_loss_limit_hit', False)}, Trading Hours: {is_trading_hours}, SL Cooldown: {is_sl_cooldown}, Kill Switch: {is_kill_switch_active}")
            print(f"        [Dip Buyer] --> Can consider new trade? {can_consider_new_trade}")

            # --- Look for New Trade Candidates ---
            should_scan_reports = can_consider_new_trade and current_time > last_report_scan_time + CHECK_INTERVAL_SECONDS * 2
            
            if should_scan_reports: 
                print(f"\n[*] [Dip Buyer] Scanning last {NUM_REPORTS_TO_SCAN} reports for new candidates...")
                last_report_scan_time = current_time; latest_report_files = get_latest_report_paths(NUM_REPORTS_TO_SCAN); potential_candidates: Dict[str, Dict] = {}
                print(f"    [Dip Buyer] Found {len(latest_report_files)} report files to scan.")
                if not latest_report_files: print("    [!] [Dip Buyer] No processed reports found to scan.")
                else:
                    processed_item_count = 0; passed_filter_count = 0
                    for report_path in latest_report_files:
                        report_name = os.path.basename(report_path); report_data = load_json_data(report_path);
                        if not report_data: print(f"        [!] [Dip Buyer] Failed to load report {report_name}"); continue
                        items_list = [];
                        if "correlated_holdings" in report_data and isinstance(report_data["correlated_holdings"], list): items_list = report_data["correlated_holdings"]
                        elif "correlated_holdings_snapshot" in report_data and isinstance(report_data["correlated_holdings_snapshot"], list): items_list = report_data["correlated_holdings_snapshot"]
                        if not items_list: continue
                        processed_item_count += len(items_list)
                        for item in items_list:
                            mint = item.get("token_mint");
                            if not mint or not isinstance(item, dict): continue 
                            passed_strict, fail_reason = passes_strict_filters(item, current_time) # Uses dip buyer filters now
                            if passed_strict: 
                                if mint not in potential_candidates: passed_filter_count += 1
                                potential_candidates[mint] = item 
                    print(f"    [Dip Buyer] Scanned {len(latest_report_files)} reports, processed {processed_item_count} items total.")
                    print(f"    [Dip Buyer] Found {len(potential_candidates)} unique candidates passing strict filters.")

                    if potential_candidates:
                        items_to_rank = list(potential_candidates.values())
                        ranked_candidates = rank_trade_candidates(items_to_rank) # Uses dip buyer scoring 
                        state['recently_pending'] = {m: ts for m, ts in state.get('recently_pending', {}).items() if current_time - ts < RECENTLY_ADDED_PENDING_WINDOW_SECONDS}
                        added_to_pending_count = 0; replaced_in_pending_count = 0
                        print(f"    [*] [Dip Buyer] Processing top {len(ranked_candidates)} ranked candidates for pending list...")
                        
                        for i, ranked_item in enumerate(ranked_candidates):
                            pending_mint = ranked_item.get("token_mint");
                            if not pending_mint: continue
                            new_candidate_score = ranked_item.get('_score', Decimal('-1')); symbol_for_log = ranked_item.get('symbol', 'N/A')
                            # print(f"        Considering #{i+1}: {symbol_for_log} ({pending_mint[:6]}) Score: {new_candidate_score:.2f}") 
                            if pending_mint in state['open_positions']: print(f"            Skipping: {symbol_for_log} Already in open positions."); continue 
                            if pending_mint in state.get('recently_pending', {}): print(f"            Skipping: {symbol_for_log} In recently_pending cooldown."); continue 
                            num_open_plus_pending = len(state['open_positions']) + len(state.get('pending_entry', {})) 
                            has_free_slot = num_open_plus_pending < MAX_OPEN_POSITIONS; pending_entry_map = state.setdefault('pending_entry', {})
                            new_pending_data = {"symbol": symbol_for_log, "name": ranked_item.get("name", "N/A"), "signal_timestamp": int(time.time()), "signal_price": str((ranked_item.get("bds_market_data") or {}).get("price") or 'N/A'), "monitoring_start_time": int(time.time()), "entry_window_seconds": PENDING_ENTRY_WINDOW_SECONDS, "score": str(new_candidate_score), "risk_level": state['risk_level']}
                            if pending_mint in pending_entry_map:
                                existing_pending_score = Decimal(pending_entry_map[pending_mint].get('score', '-1'))
                                if new_candidate_score > existing_pending_score * PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR:
                                    print(f"            [Dip Buyer] Updating PENDING {symbol_for_log} ({pending_mint[:6]}) with better score ({new_candidate_score:.2f} > {existing_pending_score:.2f} * {PENDING_REPLACE_SCORE_IMPROVEMENT_FACTOR:.2f})")
                                    pending_entry_map[pending_mint] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint] = current_time; replaced_in_pending_count +=1
                            elif has_free_slot:
                                print(f"            -> [Dip Buyer] Added {symbol_for_log} ({pending_mint[:6]}) to PENDING (slot available). Score: {new_candidate_score:.2f}")
                                pending_entry_map[pending_mint] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint] = current_time; added_to_pending_count += 1
                            elif pending_entry_map:
                                lowest_score_pending_mint = None; lowest_score = new_candidate_score
                                for mint_in_pending, p_data in pending_entry_map.items():
                                    if mint_in_pending == pending_mint: continue 
                                    current_pending_score = Decimal(p_data.get('score', '-1'))
                                    if current_pending_score < lowest_score: lowest_score = current_pending_score; lowest_score_pending_mint = mint_in_pending
                                if lowest_score_pending_mint and new_candidate_score > lowest_score: 
                                    removed_symbol = pending_entry_map[lowest_score_pending_mint].get('symbol','Old')
                                    print(f"            -> [Dip Buyer] Replacing PENDING {removed_symbol} ({lowest_score_pending_mint[:6]}) (Score: {lowest_score:.2f}) with {symbol_for_log} ({pending_mint[:6]}) (Score: {new_candidate_score:.2f}).")
                                    pending_entry_map.pop(lowest_score_pending_mint); pending_entry_map[pending_mint] = new_pending_data; state.setdefault('recently_pending', {})[pending_mint] = current_time; replaced_in_pending_count += 1
                            if len(state['open_positions']) + len(state.get('pending_entry', {})) >= MAX_OPEN_POSITIONS and added_to_pending_count == 0 and replaced_in_pending_count == 0: 
                                print(f"        [Dip Buyer] Max positions ({MAX_OPEN_POSITIONS}) reached. Stopping candidate processing."); break 
                        if added_to_pending_count > 0 or replaced_in_pending_count > 0: print(f"    [*] [Dip Buyer] Finished candidate scan. Added: {added_to_pending_count}, Replaced: {replaced_in_pending_count}. Current Pending: {len(state.get('pending_entry',{}))}")
                        else: print(f"    [*] [Dip Buyer] Finished candidate scan. No new pending entries added or replaced.")
                    else: print(f"    [*] [Dip Buyer] No candidates passed strict filters in the scanned reports.")

            # --- Periodic Performance Summaries ---
            if current_time >= state.get('last_4hr_summary_ts', 0) + SUMMARY_INTERVAL_4HR_SECONDS:
                print(f"\n[*] [Dip Buyer] Generating 4-Hour Performance Summary...")
                calculate_performance_metrics(state); generate_and_send_performance_summary_md_original(state, 4, "4-Hour [Dip Buyer]", current_time) 
            elif current_time >= state.get('last_daily_summary_ts', 0) + SUMMARY_INTERVAL_DAILY_SECONDS:
                print(f"\n[*] [Dip Buyer] Generating Daily Performance Summary...")
                calculate_performance_metrics(state); generate_and_send_performance_summary_md_original(state, 24, "Daily [Dip Buyer]", current_time) 
            else: 
                save_trader_state(state) 
                
            print(f"--- [Dip Buyer] Loop End: {default_format_timestamp(time.time())} ---") 
            time.sleep(CHECK_INTERVAL_SECONDS)
            
        except KeyboardInterrupt: print("\n--- Dip Buyer stopping ---"); save_trader_state(state); print("    Dip Buyer state saved."); break
        except Exception as e:
            print(f"\n[!!!] [Dip Buyer] Unhandled error in main loop: {e}"); traceback.print_exc()
            print("    [Dip Buyer] Attempting to save state..."); save_trader_state(state)
            error_type_safe = escape_markdown_original(type(e).__name__); error_msg_safe = escape_markdown_original(str(e))
            telegram_error_message = f"🚨 *Dip Buyer Main Loop Error* 🚨\n\n`{error_type_safe}: {error_msg_safe}`\n\nSee console/logs. Will retry."
            send_trader_telegram(telegram_error_message); print(f"    [Dip Buyer] Retrying in {CHECK_INTERVAL_SECONDS*2} seconds..."); time.sleep(CHECK_INTERVAL_SECONDS * 2)


if __name__ == "__main__":
    main_trading_loop()

# --- END OF FILE paper_trader_dip.py ---