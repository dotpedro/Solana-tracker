# --- START OF FILE report_processor.py ---

import os
import time
import json
import traceback
import requests # Needed for Telegram
import glob
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple, Set
from dotenv import load_dotenv

load_dotenv()

try:
    import birdeye_api
    print("[✓] report_processor: Imported birdeye_api gateway.")
except ImportError:
    print("CRITICAL: birdeye_api.py not found. Exiting.")
    exit()

try:
    from helius_analyzer import format_timestamp, get_birdeye_token_overview
except ImportError:
    print("CRITICAL: helius_analyzer.py not found. Exiting.")
    exit()

try:
    from gmgn_api import gmgn
    GMGN_ENABLED = True
    gmgn_analyzer = gmgn()
    print("[✓] report_processor: GMGN API enabled.")
except ImportError:
    print("[!] report_processor: gmgn_api.py not found. GMGN checks disabled.")
    GMGN_ENABLED = False
    gmgn_analyzer = None

try:
    import api_usage_limiter
except ImportError:
    print("CRITICAL: api_usage_limiter.py not found (required by birdeye_api). Exiting.")
    exit()

try:
    from eco_mode_config import is_eco_mode
    print("[✓] report_processor: Imported eco_mode_config.")
except ImportError:
    print("[!] report_processor: eco_mode_config.py not found. Eco Mode checks will default to OFF.")
    def is_eco_mode() -> bool:
        print("    [Fallback] eco_mode_config.is_eco_mode() returning False.")
        return False

TELEGRAM_TOKEN_PROCESSOR = os.getenv("TELEGRAM_BOT_TOKEN_PROCESSOR", os.getenv("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID_PROCESSOR = os.getenv("TELEGRAM_CHAT_ID_PROCESSOR", os.getenv("TELEGRAM_CHAT_ID"))

def send_telegram_processor(text: str) -> bool:
    token = TELEGRAM_TOKEN_PROCESSOR
    chat_id = TELEGRAM_CHAT_ID_PROCESSOR
    if not token or not chat_id:
        print(f"PROCESSOR ALERT (Telegram Not Configured): {text}")
        return True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": f"🔍 Report Processor Alert 🔍\n\n{text}", "parse_mode": "Markdown"}
    success = False
    try:
        r = requests.post(url, data=payload, timeout=15)
        r.raise_for_status()
        print(f"    [✓] Processor Alert Sent: {text.splitlines()[0]}...")
        success = True
    except Exception as e:
        print(f"    [✗] Failed to send Processor Telegram alert: {e}")
    return success

PENDING_REPORTS_DIR = os.path.join("reports", "holdings", "pending")
PROCESSED_REPORTS_DIR = os.path.join("reports", "holdings", "processed")
ARCHIVE_REPORTS_DIR = os.path.join("reports", "holdings", "archive_raw")
PROCESSOR_STATE_FILE = "report_processor_state.json"
CHECK_INTERVAL_SECONDS = 5
REPORT_STALE_MINUTES = 60
MIN_LIQUIDITY_THRESHOLD = 2000
HOLDER_CONCENTRATION_THRESHOLD = 0.7
TOP_N_HOLDERS_TO_FETCH = 20
MIN_BUY_SELL_RATIO = 0.5

ALERT_HISTORY_FILE = os.getenv("ALERT_HISTORY_FILE_PATH", os.path.join("reports", "holdings", "alert_history.json"))
PROCESSED_CACHE_FILE = os.path.join("reports", "holdings", "processed_cache.json")
PROCESSED_CACHE_PRUNE_DAYS = 7
ECO_MODE_COOLDOWN_SECONDS = int(os.getenv("ECO_MODE_COOLDOWN_HOURS", "6")) * 60 * 60
NORMAL_MODE_COOLDOWN_SECONDS = int(os.getenv("NORMAL_MODE_COOLDOWN_HOURS", "2")) * 60 * 60

os.makedirs(PENDING_REPORTS_DIR, exist_ok=True)
os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)
os.makedirs(ARCHIVE_REPORTS_DIR, exist_ok=True)

def _format_currency(value: Optional[float], precision: int = 0) -> str:
    if value is None: return "N/A"
    try: return f"${value:,.{precision}f}"
    except (ValueError, TypeError): return "N/A"

def _format_percent(value: Optional[float], plus_sign: bool = True) -> str:
    if value is None: return "N/A"
    try:
        sign = "+" if plus_sign and value > 0 else ""
        if value == float('inf'): return f"{sign}∞%"
        return f"{sign}{value * 100:.1f}%"
    except (ValueError, TypeError): return "N/A"

def _format_int(value: Optional[int]) -> str:
    if value is None: return "N/A"
    try: return f"{value:,}"
    except (ValueError, TypeError): return "N/A"

def load_processor_state() -> Dict[str, int]:
    if os.path.exists(PROCESSOR_STATE_FILE):
        try:
            with open(PROCESSOR_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                return state if isinstance(state, dict) else {}
        except Exception as e:
            print(f"[!] Error loading processor state: {e}")
    return {}

def save_processor_state(state: Dict[str, int]):
    try:
        with open(PROCESSOR_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[✗] Error saving processor state: {e}")

def load_json_data(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] Failed to load {path}: {e}")
        return None

def save_processed_report(filename_base: str, data: Dict[str, Any]):
    try:
        output_filename = os.path.join(PROCESSED_REPORTS_DIR, filename_base)
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"    [✓] Saved processed report: {output_filename}")
    except Exception as e:
        print(f"[✗] Failed to save processed report {filename_base}: {e}")

def archive_raw_report(report_path: str):
    try:
        filename = os.path.basename(report_path)
        archive_path = os.path.join(ARCHIVE_REPORTS_DIR, filename)
        if os.path.exists(report_path):
            os.rename(report_path, archive_path)
            print(f"    Archived raw report to: {archive_path}")
        else:
            print(f"    [!] Raw report {report_path} not found for archiving.")
    except Exception as e:
        print(f"    [!] Failed to archive raw report {report_path}: {e}")

def load_alert_history() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(ALERT_HISTORY_FILE):
        try:
            with open(ALERT_HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
                return history if isinstance(history, dict) else {}
        except json.JSONDecodeError:
            print(f"[!] Error decoding JSON from {ALERT_HISTORY_FILE}. Returning empty history.")
            return {}
        except Exception as e:
            print(f"[!] Error loading alert history from {ALERT_HISTORY_FILE}: {e}")
    else:
        print(f"[i] Alert history file not found: {ALERT_HISTORY_FILE}. Assuming empty history.")
    return {}

def _get_total_supply_from_market_data(market_data: Optional[Dict[str, Any]]) -> Optional[str]:
    if not market_data or not isinstance(market_data, dict): return None
    supply_keys_to_try = ["total_supply", "totalSupply", "supply"]
    for key in supply_keys_to_try:
        supply_value = market_data.get(key)
        if supply_value is not None:
            if isinstance(supply_value, (str, int, float)): return str(supply_value)
    return None

def load_processed_cache() -> Dict[str, Dict[str, Any]]:
    cache_data = {}
    if os.path.exists(PROCESSED_CACHE_FILE):
        try:
            with open(PROCESSED_CACHE_FILE, "r", encoding="utf-8") as f:
                loaded_cache = json.load(f)
                if not isinstance(loaded_cache, dict):
                    print(f"[!] {PROCESSED_CACHE_FILE} not a valid dict. Reinitializing.")
                    return {}
                current_ts = int(time.time())
                prune_threshold_ts = current_ts - (PROCESSED_CACHE_PRUNE_DAYS * 24 * 60 * 60)
                pruned_count = 0
                for mint, data in list(loaded_cache.items()):
                    if data.get("last_processed_timestamp", 0) < prune_threshold_ts:
                        del loaded_cache[mint]
                        pruned_count += 1
                if pruned_count > 0:
                    print(f"    [i] Pruned {pruned_count} old entries from {PROCESSED_CACHE_FILE}.")
                cache_data = loaded_cache
        except json.JSONDecodeError:
            print(f"[!] Error decoding JSON from {PROCESSED_CACHE_FILE}. Returning empty.")
        except Exception as e:
            print(f"[!] Error loading processed cache from {PROCESSED_CACHE_FILE}: {e}")
    else:
        print(f"[i] Processed cache file not found: {PROCESSED_CACHE_FILE}. Assuming empty.")
    return cache_data

def save_processed_cache(cache_data: Dict[str, Dict[str, Any]]):
    try:
        with open(PROCESSED_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"[✗] Error saving processed cache to {PROCESSED_CACHE_FILE}: {e}")

def process_report_file(report_path: str, processor_state: Dict[str, int]) -> bool:
    """
    Main entry-point for handling a single raw Birdeye correlation report.

    Args:
        report_path:  absolute path to the raw JSON report produced by the on-chain crawler
        processor_state:  dict persisted by the runner that remembers which files were processed

    Returns:
        True  – report was handled (even if no alert was raised)
        False – report could not be processed (corrupted JSON, etc.)
    """
    filename_base = os.path.basename(report_path)
    current_ts = int(time.time())

    print(f"--- Processing report: {filename_base} ---")

    eco_active = is_eco_mode()
    cooldown_duration = ECO_MODE_COOLDOWN_SECONDS if eco_active else NORMAL_MODE_COOLDOWN_SECONDS

    alert_history   = load_alert_history()
    processed_cache = load_processed_cache()

    print(f"    [i] Eco Mode: {'ON' if eco_active else 'OFF'}. "
          f"Effective Cooldown: {cooldown_duration // 3600} hours.")

    # ---------------------------------------------------------------------
    # 1.  Load the raw report
    # ---------------------------------------------------------------------
    report_content = load_json_data(report_path)
    if not report_content:
        print(f"    [!] Failed to load content for {filename_base}. Archiving.")
        archive_raw_report(report_path)
        processor_state[filename_base] = current_ts
        return False

    force_recheck_flag = report_content.pop("force_recheck", False)
    if force_recheck_flag:
        print(f"    [!] 'force_recheck' flag for {filename_base}. Full re-evaluation.")

    # ---------------------------------------------------------------------
    # 2.  Pull the list of correlated items (may be live or snapshot key)
    # ---------------------------------------------------------------------
    correlated_items: List[Dict[str, Any]] = []
    if "correlated_holdings" in report_content:
        correlated_items = report_content.get("correlated_holdings", [])
    elif "correlated_holdings_snapshot" in report_content:
        correlated_items = report_content.get("correlated_holdings_snapshot", [])
    else:
        print(f"    [!] No 'correlated_holdings' or 'snapshot' in {filename_base}.")

    # ---------------------------------------------------------------------
    # 3.  Short-circuit if the report is actually empty
    # ---------------------------------------------------------------------
    if not correlated_items:
        print(f"    [i] No correlated items in {filename_base}. Archiving as processed.")
        processed_data_empty = report_content.copy()
        processed_data_empty["processor_run_timestamp"]  = current_ts
        processed_data_empty["processor_schema_version"] = "1.4.1_birdeye_vol+security"
        if ("correlated_holdings" not in processed_data_empty and
                "correlated_holdings_snapshot" not in processed_data_empty):
            processed_data_empty["correlated_holdings_enriched"] = []  # ensure the key
        save_processed_report(filename_base, processed_data_empty)
        archive_raw_report(report_path)
        processor_state[filename_base] = current_ts
        return True

    # ---------------------------------------------------------------------
    # 4.  Decide which mints we actually need to call the API for
    # ---------------------------------------------------------------------
    mints_to_fetch: Set[str] = set()
    if force_recheck_flag:
        for item in correlated_items:
            mint_address = item.get("original_correlation_data", item).get("token_mint")
            if mint_address and isinstance(mint_address, str):
                mints_to_fetch.add(mint_address)
    else:
        for item_meta in correlated_items:
            mint_address_meta = item_meta.get("original_correlation_data",
                                              item_meta).get("token_mint")
            if not mint_address_meta or not isinstance(mint_address_meta, str):
                continue

            skip_meta   = False
            if (mint_address_meta in alert_history and
                    (current_ts - alert_history[mint_address_meta]
                     .get("last_alert_timestamp", 0) < cooldown_duration)):
                skip_meta = True
            elif (mint_address_meta in processed_cache and
                    (current_ts - processed_cache[mint_address_meta]
                     .get("last_processed_timestamp", 0) < cooldown_duration)):
                skip_meta = True

            if not skip_meta:
                mints_to_fetch.add(mint_address_meta)

    # ---------------------------------------------------------------------
    # 5.  Batch-fetch metadata where possible
    # ---------------------------------------------------------------------
    mint_list     = list(mints_to_fetch)
    metadata_map  = {}
    if mint_list:
        print(f"    [i] Attempting batch metadata for {len(mint_list)} tokens...")
        metadata_response = birdeye_api.metadata_batch(mint_list)
        if metadata_response:
            metadata_map = metadata_response
            print(f"    [✓] Fetched metadata for {len(metadata_map)}/{len(mint_list)} tokens.")
        else:
            print("    [!] Failed to fetch batch metadata.")
        time.sleep(0.1)
    else:
        print("    [i] No mints for metadata batch "
              "(all might be in cool-down or invalid).")

    # ---------------------------------------------------------------------
    # 6.  Per-item enrichment / validation loop
    # ---------------------------------------------------------------------
    enriched_correlated_items          = []
    items_failed_validation_this_run   = {}
    items_passed_validation_this_run   = set()  # to clear from processed_cache

    print(f"    Processing {len(correlated_items)} total correlated items...")
    for item_index, item in enumerate(correlated_items):
        original_data = item.get("original_correlation_data", item)
        mint_address  = original_data.get("token_mint")

        # --------------------------------------------------------------
        # Basic sanity for the mint address
        # --------------------------------------------------------------
        if not mint_address or not isinstance(mint_address, str):
            print(f"        Skipping Item {item_index + 1}: Invalid mint.")
            enriched_correlated_items.append({
                "original_correlation_data": item.copy(),
                "token_mint": mint_address,
                "symbol": "N/A",
                "name": "N/A",
                "processing_status": "skipped_invalid_mint",
                "is_update_candidate": False,
                "validation_flags": {
                    "passes_validation": False,
                    "reasons": ["Invalid Mint Data"]
                }
            })
            continue

        # --------------------------------------------------------------
        # Cool-down checks …
        # (all unchanged)
        # --------------------------------------------------------------
        is_update_candidate = False
        processing_status   = "new_candidate"
        skip_this_item      = False
        cached_symbol       = "N/A"
        cached_name         = "N/A"

        if not force_recheck_flag:
            if mint_address in alert_history:
                last_alert_ts = alert_history[mint_address].get("last_alert_timestamp", 0)
                if current_ts - last_alert_ts < cooldown_duration:
                    skip_this_item  = True
                    processing_status = "skipped_cool_down"
                    time_since = current_ts - last_alert_ts
                    cached_symbol = alert_history[mint_address].get("symbol",
                                                                    "N/A (alerted)")
                    cached_name   = alert_history[mint_address].get("name",
                                                                    "N/A (alerted)")
                    print(f"        [i] Mint {mint_address[:6]} "
                          f"({item_index + 1}): Recently alerted "
                          f"({time_since // 60}m ago). Skipping API calls.")
                    enriched_item_skipped = {
                        "original_correlation_data": item.copy(),
                        "token_mint": mint_address,
                        "symbol": cached_symbol,
                        "name": cached_name,
                        "bds_metadata": {
                            "status": processing_status,
                            "cached_symbol": cached_symbol,
                            "cached_name": cached_name,
                            "note": f"Alerted {time_since // 60}m ago."
                        },
                        "processing_status": processing_status,
                        "is_update_candidate": False,
                        "last_alert_timestamp_from_history": last_alert_ts,
                        "validation_flags": {
                            "passes_validation": False,
                            "reasons": ["Skipped (Alert Cool-down)"]
                        }
                    }
                else:
                    is_update_candidate = True
                    processing_status   = "update_candidate"
                    print(f"        [i] Mint {mint_address[:6]} "
                          f"({item_index + 1}): Alerted >"
                          f"{cooldown_duration // 3600}h ago. "
                          "Processing as update.")
            elif mint_address in processed_cache:
                last_processed_ts = processed_cache[mint_address].get(
                    "last_processed_timestamp", 0)
                if current_ts - last_processed_ts < cooldown_duration:
                    skip_this_item  = True
                    processing_status = "skipped_recent_failure"
                    time_since = current_ts - last_processed_ts
                    cached_symbol = processed_cache[mint_address].get(
                        "symbol", "N/A (failed cache)")
                    cached_name   = processed_cache[mint_address].get(
                        "name", "N/A (failed cache)")
                    print(f"        [i] Mint {mint_address[:6]} "
                          f"({item_index + 1}): Failed validation "
                          f"({time_since // 60}m ago). Skipping API calls.")
                    enriched_item_skipped = {
                        "original_correlation_data": item.copy(),
                        "token_mint": mint_address,
                        "symbol": cached_symbol,
                        "name": cached_name,
                        "bds_metadata": {
                            "status": processing_status,
                            "cached_symbol": cached_symbol,
                            "cached_name": cached_name,
                            "note": f"Failed validation {time_since // 60}m ago."
                        },
                        "processing_status": processing_status,
                        "is_update_candidate": False,
                        "last_processed_timestamp_from_cache": last_processed_ts,
                        "validation_flags": {
                            "passes_validation": False,
                            "reasons": ["Skipped (Recent Failure Cool-down)"]
                        }
                    }
                else:
                    print(f"        [i] Mint {mint_address[:6]} "
                          f"({item_index + 1}): Failed validation >"
                          f"{cooldown_duration // 3600}h ago. Re-processing.")

        # --------------------------------------------------------------
        # If cool-down tells us to skip the item entirely
        # --------------------------------------------------------------
        if skip_this_item:
            enriched_correlated_items.append(enriched_item_skipped)
            continue

        # --------------------------------------------------------------
        # Determine final processing status after cool-down logic
        # --------------------------------------------------------------
        if force_recheck_flag:
            print(f"        [!] Mint {mint_address[:6]} "
                  f"({item_index + 1}): Processing due to 'force_recheck'.")
            if mint_address in alert_history:
                processing_status = "update_candidate_forced"
                is_update_candidate = True
            elif mint_address in processed_cache:
                processing_status = "new_candidate_forced_after_fail"
            else:
                processing_status = "new_candidate_forced"
        elif processing_status == "new_candidate":
            print(f"        [i] Mint {mint_address[:6]} "
                  f"({item_index + 1}): New candidate for processing.")

        # --------------------------------------------------------------
        # Create the working structure for this item
        # --------------------------------------------------------------
        print(f"    Processing Item {item_index + 1}/{len(correlated_items)} "
              f"Mint: {mint_address[:6]}... (Status: {processing_status})")
        enriched_item = {
            "original_correlation_data": item.copy(),
            "token_mint": mint_address,
            "bds_metadata": metadata_map.get(mint_address, {}),
            "bds_market_data": None,
            "bds_holder_data": None,
            "bds_trade_data": None,
            "bds_security_data": None,
            "gmgn_security_info": None,
            "processing_status": processing_status,
            "is_update_candidate": is_update_candidate,
            "validation_flags": {"passes_validation": True, "reasons": []}
        }

        # --------------------------------------------------------------
        # API calls (unchanged)
        # --------------------------------------------------------------
        market_data      = birdeye_api.market_data(mint_address)
        time.sleep(0.05)
        holders_response = birdeye_api.holders(
            mint_address, limit=TOP_N_HOLDERS_TO_FETCH)
        time.sleep(0.05)
        trade_data       = birdeye_api.trade_data(mint_address)
        time.sleep(0.05)
        # ---------- NEW: flatten Birdeye-v3 volume fields ----------
        # USD-denominated 24 h volume lives in trade_data["volume"]["h24"].
        total_vol_usd_24h = (trade_data.get("volume", {}) or {}).get("h24")
        if total_vol_usd_24h is not None:
            trade_data["volume_24h"] = total_vol_usd_24h      # keep old flat key

        # Optional: token-denominated volume (can be useful later)
        trade_data["volume_token_24h"] = (
            trade_data.get("volume_token", {}) or {}
        ).get("h24")
        # -----------------------------------------------------------

        bds_security_data = birdeye_api.token_security(mint_address)
        time.sleep(0.05)

        holders_list: Optional[List[Dict]] = None
        if holders_response and isinstance(holders_response.get("items"), list):
            holders_list = holders_response["items"]

        enriched_item["bds_market_data"]  = market_data
        enriched_item["bds_holder_data"]  = holders_list
        enriched_item["bds_trade_data"]   = trade_data
        enriched_item["bds_security_data"] = bds_security_data

        # --------------------------------------------------------------
        # Surface API failures
        # --------------------------------------------------------------
        if not market_data:
            enriched_item["validation_flags"]["reasons"].append(
                "BDS Market Data Failed")
            enriched_item["validation_flags"]["passes_validation"] = False
            print(f"        [!] BDS Market Data Fetch Failed for "
                  f"{mint_address[:6]}")
        if holders_list is None:
            enriched_item["validation_flags"]["reasons"].append(
                "BDS Holder Data Failed")
            enriched_item["validation_flags"]["passes_validation"] = False
            print(f"        [!] BDS Holder Data Fetch Failed for "
                  f"{mint_address[:6]}")
        if not trade_data:
            enriched_item["validation_flags"]["reasons"].append(
                "BDS Trade Data Failed")
            enriched_item["validation_flags"]["passes_validation"] = False
            print(f"        [!] BDS Trade Data Fetch Failed for "
                  f"{mint_address[:6]}")

        # --------------------------------------------------------------
        # Pull symbol / name / decimals from the batched metadata, with
        # fallback to token_overview & other heuristics (unchanged)
        # --------------------------------------------------------------
        token_metadata_from_batch = enriched_item["bds_metadata"]
        original_report_symbol    = original_data.get("symbol", "N/A")
        original_report_name      = original_data.get("name",   "N/A")

        meta_symbol_from_batch = token_metadata_from_batch.get("symbol")
        meta_name_from_batch   = token_metadata_from_batch.get("name")

        enriched_item["symbol"] = (meta_symbol_from_batch
                                   if meta_symbol_from_batch
                                   else original_report_symbol)
        enriched_item["name"]   = (meta_name_from_batch
                                   if meta_name_from_batch
                                   else original_report_name)

        symbol_source   = ("metadata_batch" if meta_symbol_from_batch
                           else ("original_report"
                                 if original_report_symbol != "N/A" else "missing"))
        name_source     = ("metadata_batch" if meta_name_from_batch
                           else ("original_report"
                                 if original_report_name != "N/A" else "missing"))
        current_decimals = token_metadata_from_batch.get("decimals")
        decimals_source  = ("metadata_batch" if current_decimals is not None
                            else "missing_initial")

        # fallback to token_overview, holders API, etc.  (unchanged)
        # --------------------------------------------------------------
        needs_overview_fallback = (
            (not enriched_item["symbol"] or enriched_item["symbol"] == "N/A") or
            (not enriched_item["name"] or enriched_item["name"]   == "N/A") or
            current_decimals is None
        ) and mint_address

        if needs_overview_fallback:
            print(f"        Attempting Birdeye token_overview fallback for "
                  f"{mint_address[:6]}...")
            try:
                overview_data_fetched = get_birdeye_token_overview(mint_address)
                time.sleep(0.05)
                if overview_data_fetched:
                    print(f"            [+] Overview data for "
                          f"{mint_address[:6]}")
                    if ((not enriched_item["symbol"] or
                         enriched_item["symbol"] == "N/A") and
                            overview_data_fetched.get("symbol")):
                        enriched_item["symbol"] = overview_data_fetched["symbol"]
                        symbol_source = "overview_api"
                    if ((not enriched_item["name"] or
                         enriched_item["name"] == "N/A") and
                            overview_data_fetched.get("name")):
                        enriched_item["name"] = overview_data_fetched["name"]
                        name_source = "overview_api"
                    if (current_decimals is None and
                            isinstance(overview_data_fetched.get("decimals"), int)):
                        current_decimals = overview_data_fetched["decimals"]
                        decimals_source  = "overview_api"
            except Exception as e_ov:
                print(f"        [!] Error token_overview fallback: {e_ov}")

        if (current_decimals is None and market_data and
                isinstance(market_data.get("decimals"), int)):
            current_decimals = market_data["decimals"]
            decimals_source  = "market_data_api"
        if (current_decimals is None and holders_list and
                len(holders_list) > 0 and
                isinstance(holders_list[0].get("decimals"), int)):
            holder_decimals = holders_list[0]["decimals"]
            if 0 <= holder_decimals <= 18:
                current_decimals = holder_decimals
                decimals_source  = "holder_data_api"

        print(f"        [i] Symbol: '{enriched_item['symbol']}' "
              f"(src: {symbol_source}), Name: '{enriched_item['name']}' "
              f"(src: {name_source}), Decimals: "
              f"{current_decimals if current_decimals is not None else 'Unk'} "
              f"(src: {decimals_source})")

        # --------------------------------------------------------------
        # Liquidity threshold check (unchanged)
        # --------------------------------------------------------------
        if market_data:
            liquidity = market_data.get("liquidity")
            if liquidity is None or not isinstance(liquidity,
                                                   (int, float)):
                if enriched_item["validation_flags"]["passes_validation"]:
                    enriched_item["validation_flags"]["passes_validation"] = False
                enriched_item["validation_flags"]["reasons"].append(
                    "Missing Liquidity")
                print("        [!] Missing Liquidity")
            elif liquidity < MIN_LIQUIDITY_THRESHOLD:
                if enriched_item["validation_flags"]["passes_validation"]:
                    enriched_item["validation_flags"]["passes_validation"] = False
                enriched_item["validation_flags"]["reasons"].append(
                    f"Low Liquidity ({_format_currency(liquidity)} "
                    f"< {_format_currency(MIN_LIQUIDITY_THRESHOLD)})")
                print(f"        [!] Low Liquidity: "
                      f"{_format_currency(liquidity)}")
            else:
                print(f"        [✓] Liquidity OK: "
                      f"{_format_currency(liquidity)}")

        # --------------------------------------------------------------
        # Holder concentration check (unchanged)
        # --------------------------------------------------------------
        if (holders_list is not None and market_data and
                isinstance(current_decimals, int) and current_decimals >= 0):
            token_supply_str = _get_total_supply_from_market_data(market_data)
            if token_supply_str:
                try:
                    token_supply_float = float(token_supply_str)
                    if token_supply_float > 0 and holders_list:
                        top_holder_amount_str = holders_list[0].get('amount')
                        if top_holder_amount_str:
                            top_holder_amount_float = float(top_holder_amount_str)
                            supply_raw = token_supply_float * (10 ** current_decimals)
                            if supply_raw > 0:
                                top_holder_pct = top_holder_amount_float / supply_raw
                                if (market_data and
                                        "top_holder_pct" not in market_data):
                                    market_data["top_holder_pct"] = None
                                if market_data:
                                    market_data["top_holder_pct"] = top_holder_pct
                                if top_holder_pct > HOLDER_CONCENTRATION_THRESHOLD:
                                    if enriched_item["validation_flags"]["passes_validation"]:
                                        enriched_item["validation_flags"]["passes_validation"] = False
                                    enriched_item["validation_flags"]["reasons"].append(
                                        f"High Holder Conc ({top_holder_pct * 100:.1f}%)")
                                    print(f"        [!] High Holder Conc: "
                                          f"{top_holder_pct * 100:.1f}%")
                                else:
                                    print(f"        [✓] Holder Conc OK: "
                                          f"{top_holder_pct * 100:.1f}%")
                            else:
                                print("        [!] Holder Conc: Invalid raw supply "
                                      f"({supply_raw}).")
                        else:
                            print("        [!] Holder Conc: Top holder amount missing.")
                    elif not holders_list:
                        print("        [i] Holder Conc: Holder list empty.")
                    else:
                        print("        [!] Holder Conc: Invalid total supply "
                              f"({token_supply_float}).")
                except Exception as e:
                    print(f"        [!] Holder Conc Error: {e}.")
            else:
                print("        [!] Holder Conc: Missing total_supply. "
                      f"Keys: {list(market_data.keys()) if market_data else 'None'}")
        elif holders_list is None:
            print("        [!] Holder Conc Skipped (No Holder Data)")
        elif not market_data:
            print("        [!] Holder Conc Skipped (No Market Data)")
        elif not isinstance(current_decimals, int) or current_decimals < 0:
            print(f"        [!] Holder Conc Skipped "
                  f"(Invalid Decimals: {current_decimals})")

        # --------------------------------------------------------------
        #  ***  V3 Trade-Data volume & Buy/Sell-ratio  ***
        # --------------------------------------------------------------
        if trade_data:
            # --- Corrected keys for V3 Trade-Data (USD first, fallback second) ---
            # 1.  Grab USD fields that Birdeye v3 gives directly
            buy_vol_usd   = trade_data.get("volume_buy_24h_usd")
            sell_vol_usd  = trade_data.get("volume_sell_24h_usd")
            total_vol_usd = trade_data.get("volume_24h_usd")

            # 2.  Legacy fallback (if any are missing)
            if buy_vol_usd is None:
                buy_vol_usd = trade_data.get("volume_buy_24h")       # legacy USD
            if sell_vol_usd is None:
                sell_vol_usd = trade_data.get("volume_sell_24h")

            if total_vol_usd is None:
                raw_token_vol = (trade_data.get("volume", {}) or {}).get("h24")
                if raw_token_vol and current_decimals is not None and market_data:
                    try:
                        price_for_calc = (market_data.get("price") or
                                          market_data.get("priceUsd"))
                        total_vol_usd = (float(raw_token_vol) /
                                         (10 ** current_decimals) *
                                         float(price_for_calc))
                    except Exception:
                        total_vol_usd = None

            # ---- display (safe even if None) ----
            if total_vol_usd is not None:
                print(f"        [i] 24h Vol (USD): "
                      f"{_format_currency(total_vol_usd)}")
            else:
                print("        [!] Could not determine 24h USD volume.")

            # ---- Buy / Sell ratio check (same threshold logic) ----
            if (buy_vol_usd is not None and sell_vol_usd is not None and
                    sell_vol_usd > 1000):
                ratio = buy_vol_usd / sell_vol_usd if sell_vol_usd else float('inf')
                if ratio < MIN_BUY_SELL_RATIO:
                    if enriched_item["validation_flags"]["passes_validation"]:
                        enriched_item["validation_flags"]["passes_validation"] = False
                    enriched_item["validation_flags"]["reasons"].append(
                        f"High Sell Pressure (Ratio {ratio:.2f} "
                        f"< {MIN_BUY_SELL_RATIO:.1f})")
                    print(f"        [!] High Sell Pressure detected "
                          f"(Ratio: {ratio:.2f}).")
                else:
                    print(f"        [✓] Buy/Sell Ratio OK: {ratio:.2f}")
            else:
                print("        [i] Buy/Sell Ratio check skipped "
                      "(Sell USD ≤ $1 000 or data missing)")
        # --------------------------------------------------------------

        # --------------------------------------------------------------
        #  Birdeye security flags
        # --------------------------------------------------------------
        if bds_security_data:
            if bds_security_data.get('mutableMetadata'):
                enriched_item["validation_flags"]["passes_validation"] = False
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS Mutable Metadata")
                print("        [!] BDS: Mutable Metadata")

            if (bds_security_data.get('freezeable') and
                    bds_security_data.get('freezeAuthority')):
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS Freeze Auth Exists")
                print("        [!] BDS: Freeze Auth Exists")

            # --- NEW security flags (Birdeye token_security) ---
            if bds_security_data.get("fakeToken"):
                enriched_item["validation_flags"]["passes_validation"] = False
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS Fake Token")
                print("        [!] BDS Flag: Fake Token")

            if bds_security_data.get("nonTransferable"):
                enriched_item["validation_flags"]["passes_validation"] = False
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS Non-Transferable")
                print("        [!] BDS Flag: Token is Non-Transferable")

            if bds_security_data.get("transferFeeEnable"):
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS Transfer Fee Enabled")
                print("        [i] BDS: Transfer Fee Enabled (info)")

            if not bds_security_data.get("lockInfo"):
                enriched_item["validation_flags"]["reasons"].append(
                    "BDS No LockInfo (Mintable?)")
                print("        [i] BDS: No LockInfo – token may still be mintable")

            if bds_security_data.get("jupStrictList") is False:
                enriched_item["validation_flags"]["reasons"].append(
                    "Not in Jupiter Strict List")
                print("        [i] BDS: Token NOT in Jupiter strict list")

            creator_pct = bds_security_data.get("creatorPercentage")
            if isinstance(creator_pct, float) and creator_pct > 0.10:   # 10 %
                enriched_item["validation_flags"]["reasons"].append(
                    f"Creator Holds {creator_pct:.1%}")
                print(f"        [i] BDS: Creator large share ({creator_pct:.1%})")

        # --------------------------------------------------------------
        # GMGN / honeypot checks (unchanged)
        # --------------------------------------------------------------
        if GMGN_ENABLED and gmgn_analyzer:
            print("        Fetching GMGN security...")
            try:
                security_info = gmgn_analyzer.getSecurityInfo(mint_address)
                time.sleep(0.1)
                enriched_item["gmgn_security_info"] = security_info or {}
                if security_info:
                    if security_info.get('is_honeypot'):
                        if enriched_item["validation_flags"]["passes_validation"]:
                            enriched_item["validation_flags"]["passes_validation"] = False
                        enriched_item["validation_flags"]["reasons"].append(
                            "GMGN Honeypot")
                        print("        [!] GMGN: Honeypot!")
                    if security_info.get('is_mintable'):
                        if enriched_item["validation_flags"]["passes_validation"]:
                            enriched_item["validation_flags"]["passes_validation"] = False
                        enriched_item["validation_flags"]["reasons"].append(
                            "GMGN Mintable")
                        print("        [!] GMGN: Mintable!")
                    if (not security_info.get('is_honeypot') and
                            not security_info.get('is_mintable')):
                        print("        [✓] GMGN OK.")
                else:
                    print("        [i] GMGN info not found.")
            except Exception as e:
                print(f"        [!] Error GMGN: {e}")
                enriched_item["gmgn_security_info"] = {"error": str(e)}
                enriched_item["validation_flags"]["reasons"].append(
                    "GMGN Check Failed")

        # --------------------------------------------------------------
        # Final outcome for this item
        # --------------------------------------------------------------
        if enriched_item["validation_flags"]["passes_validation"]:
            print(f"    [✓] Final Validation for {mint_address[:6]}: PASSED "
                  f"(Status: {processing_status})")
            items_passed_validation_this_run.add(mint_address)
        else:
            print(f"    [!] Final Validation for {mint_address[:6]}: FAILED. "
                  f"(Status: {processing_status}) Reasons: "
                  f"{enriched_item['validation_flags']['reasons']}")
            items_failed_validation_this_run[mint_address] = {
                "last_processed_timestamp": current_ts,
                "validation_passed": False,
                "symbol": enriched_item.get("symbol", "N/A"),
                "name": enriched_item.get("name", "N/A"),
                "reasons": enriched_item['validation_flags']['reasons'][:3]
            }

        enriched_correlated_items.append(enriched_item)

    # ---------------------------------------------------------------------
    # 7.  Update processed_cache with failures & clear those that passed
    # ---------------------------------------------------------------------
    cache_updated = False
    if items_failed_validation_this_run:
        processed_cache.update(items_failed_validation_this_run)
        cache_updated = True
        print(f"    [i] Updated processed_cache with "
              f"{len(items_failed_validation_this_run)} items that failed "
              "validation.")

    cleared_from_cache_count = 0
    for mint_passed in items_passed_validation_this_run:
        if mint_passed in processed_cache:
            del processed_cache[mint_passed]
            cleared_from_cache_count += 1
            cache_updated = True
    if cleared_from_cache_count > 0:
        print(f"    [i] Cleared {cleared_from_cache_count} items from "
              "processed_cache as they passed validation.")

    if cache_updated:
        save_processed_cache(processed_cache)

    # ---------------------------------------------------------------------
    # 8.  Persist enriched report
    # ---------------------------------------------------------------------
    processed_report_data = report_content.copy()
    if "correlated_holdings" in processed_report_data:
        processed_report_data["correlated_holdings"] = enriched_correlated_items
    elif "correlated_holdings_snapshot" in processed_report_data:
        processed_report_data["correlated_holdings_snapshot"] = (
            enriched_correlated_items)
    else:
        processed_report_data["correlated_holdings_enriched"] = (
            enriched_correlated_items)

    processed_report_data["processor_run_timestamp"]  = current_ts
    processed_report_data["processor_schema_version"] = (
        "1.4.1_birdeye_vol+security")

    save_processed_report(filename_base, processed_report_data)
    processor_state[filename_base] = current_ts
    archive_raw_report(report_path)

    print(f"--- Finished processing: {filename_base} ---")
    return True


if __name__ == "__main__":
    SCRIPT_SCHEMA_VERSION = "1.5.3_cache_refinements"
    print(f"Starting Report Processor (v{SCRIPT_SCHEMA_VERSION})...")
    print(f"Alert History: {ALERT_HISTORY_FILE}")
    print(f"Processed (Failed) Cache: {PROCESSED_CACHE_FILE} (Prune >{PROCESSED_CACHE_PRUNE_DAYS}d)")
    print(f"Eco Cooldown: {ECO_MODE_COOLDOWN_SECONDS // 3600}h, Normal Cooldown: {NORMAL_MODE_COOLDOWN_SECONDS // 3600}h")

    try:
        last_known_eco_status = is_eco_mode()
        print(f"Eco Mode at Startup: {'ON' if last_known_eco_status else 'OFF'}")
    except Exception as e:
        print(f"[!] Warn: Eco status check fail at startup: {e}"); last_known_eco_status = False

    initial_state = load_processor_state(); save_processor_state(initial_state)

    while True:
        files_processed_or_skipped_this_cycle = set()
        try:
            current_eco_status_in_loop = is_eco_mode()
            if current_eco_status_in_loop != last_known_eco_status:
                ts_now = datetime.now().strftime("%H:%M:%S")
                print(f"    [{ts_now}] Eco Mode changed to: {'ON' if current_eco_status_in_loop else 'OFF'}")
                last_known_eco_status = current_eco_status_in_loop

            processed_state = load_processor_state(); current_ts = int(time.time())
            patterns = [os.path.join(PENDING_REPORTS_DIR, "*.json")] # Simpler glob for all json
            pending_files_all = []; [pending_files_all.extend(glob.glob(p)) for p in patterns]
            pending_files_all = sorted(list(set(f for f in pending_files_all if f.endswith('.json'))))
            
            files_to_process = []; stale_thresh_ts = current_ts - (REPORT_STALE_MINUTES * 60)

            for rp in pending_files_all: # Changed var name for clarity
                fn = os.path.basename(rp)
                # Simplified processed check: if in state, assume recently handled unless very old
                if fn in processed_state and processed_state[fn] > (current_ts - (REPORT_STALE_MINUTES * 60 * 12)): # Wider window
                    continue
                try:
                    mod_time = os.path.getmtime(rp)
                except FileNotFoundError:
                    print(f"    [!] {fn} disappeared."); 
                    if fn not in processed_state: processed_state[fn] = current_ts
                    files_processed_or_skipped_this_cycle.add(fn)
                    continue
                if mod_time < stale_thresh_ts:
                    print(f"    [!] Stale: {fn}"); 
                    if fn not in processed_state: processed_state[fn] = current_ts
                    files_processed_or_skipped_this_cycle.add(fn); archive_raw_report(rp)
                else:
                    files_to_process.append(rp)
            
            files_to_process.sort(key=lambda pth: os.path.getmtime(pth) if os.path.exists(pth) else float('inf'))

            if files_to_process:
                print(f"\n    [*] {len(files_to_process)} reports to process (Eco: {'ON' if current_eco_status_in_loop else 'OFF'})")
                for rp in files_to_process:
                    fn = os.path.basename(rp)
                    if fn in processed_state and processed_state[fn] > (current_ts - (REPORT_STALE_MINUTES * 60 * 12)):
                        continue
                    if os.path.exists(rp):
                         if not process_report_file(rp, processed_state):
                              print(f"    Marking {fn} processed due to error/skip.")
                              if fn not in processed_state: processed_state[fn] = current_ts
                         files_processed_or_skipped_this_cycle.add(fn)
                    else:
                         print(f"    {fn} disappeared before processing."); 
                         if fn not in processed_state: processed_state[fn] = current_ts
                         files_processed_or_skipped_this_cycle.add(fn)
            elif not files_processed_or_skipped_this_cycle and pending_files_all:
                 print(f"    All pending reports stale/processed (Eco: {'ON' if current_eco_status_in_loop else 'OFF'})")

            if not pending_files_all and not files_to_process and not files_processed_or_skipped_this_cycle:
                 ts_now_idle = datetime.now().strftime("%H:%M:%S")
                 print(f"    [{ts_now_idle}] No new reports. Eco: {'ON' if current_eco_status_in_loop else 'OFF'}. Sleep {CHECK_INTERVAL_SECONDS}s...")

            if files_processed_or_skipped_this_cycle:
                save_processor_state(processed_state)
            
            time.sleep(CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nReport Processor stopped."); break
        except Exception as e_main:
            print(f"\n[!!!] Main Loop Error: {e_main}"); err_details = traceback.format_exc(); print(err_details)
            send_telegram_processor(f"RP Main Loop Error: {str(e_main)}\n\n{err_details[-1000:]}"); 
            print("    Retrying after 60s..."); time.sleep(60)
        finally:
             if 'processed_state' in locals() and files_processed_or_skipped_this_cycle: # Check if var exists
                if not isinstance(processed_state, dict): processed_state = {}
                save_processor_state(processed_state)
# --- END OF FILE report_processor.py ---