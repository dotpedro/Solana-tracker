# --- START OF FILE telegram_manual_checker.py ---

import os
import json
import asyncio
import re
from datetime import datetime, timezone
from dotenv import load_dotenv
import time
import traceback
import logging
from typing import Any, List, Dict, Tuple

# --- Load Environment Variables (Needed for dependencies like API keys if used here) ---
# Keep load_dotenv() here if this module might load other secrets for its dependencies.
# If ONLY the bot token was used, this could technically be removed, but it's safer
# to keep it in case Birdeye/Helius/etc. keys are added to .env later.
load_dotenv()
# --- End Environment Variables ---

# Configure logging for this module
logger = logging.getLogger(__name__)

# --- Project-Specific Imports ---
# These are the dependencies for the core logic functions in *this* file.
try:
    import birdeye_api
    from helius_analyzer import format_timestamp as ha_format_timestamp
    from report_processor import (
        _format_currency, _format_percent, _format_int
    )
    from gmgn_api import gmgn
    # This specific import is still used by functions in this file
    from telegram_alert_router import escape_markdown_v2 as router_escape_markdown_v2

    KOL_SIGNALS_FILE = "influencer_structured_signals.json"
    TRACKED_TOKENS_FILE = "tracked_tokens.json"
    ALERT_HISTORY_FILE = "tracker_alert_history.json"
    TRACKER_STATE_FILE = "token_tracker_states.json"

    import api_usage_limiter
    import cache_manager
    logger.info("[✓] telegram_manual_checker: Successfully imported dependencies (birdeye, helius, report_proc, gmgn, alert_router, etc.).")
except ImportError as e:
    logger.critical(f"[!] telegram_manual_checker: CRITICAL IMPORT ERROR: {e}. Core logic may fail.")
    # Define dummy escape function if router failed
    if 'router_escape_markdown_v2' not in globals():
        def router_escape_markdown_v2(text: str) -> str: return text
    # Define dummy formatters if report_processor failed
    if '_format_currency' not in globals(): _format_currency = lambda x, **kwargs: str(x)
    if '_format_percent' not in globals(): _format_percent = lambda x, **kwargs: str(x)
    if '_format_int' not in globals(): _format_int = lambda x, **kwargs: str(x)
    # Allow script to load but functionality will be broken

# --- Constants ---
MIN_LIQUIDITY_THRESHOLD_USD = 5000
MAX_HOLDER_CONCENTRATION_PCT = 0.30
MIN_BUY_SELL_RATIO_24H = 0.7
MIN_TOKEN_AGE_HOURS = 1
KOL_LOOKBACK_HOURS = 72

# --- Initializations needed for core logic ---
GMGN_ENABLED = True
gmgn_analyzer_instance = None
try:
    gmgn_analyzer_instance = gmgn()
    logger.info("[✓] telegram_manual_checker: GMGN Analyzer initialized.")
except Exception as e:
    GMGN_ENABLED = False
    logger.warning(f"[!] telegram_manual_checker: GMGN Analyzer init failed: {e}")

try:
    if 'cache_manager' in globals() and cache_manager:
        cache_manager.init_db()
        logger.info("[✓] telegram_manual_checker: Cache Manager DB initialized.")
    else:
        logger.warning("[!] telegram_manual_checker: Cache Manager not available for DB init.")
except Exception as e:
    logger.error(f"[!] telegram_manual_checker: Cache Manager DB init failed: {e}")

# --- Helper Functions ---

def robust_format_timestamp(ts_input: Any) -> str:
    """Robustly formats various timestamp inputs to a standard string."""
    if ts_input is None: return "N/A"
    ts_val = None
    # Attempt to parse numeric inputs (Unix timestamp in s or ms)
    if isinstance(ts_input, (int, float)):
        ts_val = ts_input
    elif isinstance(ts_input, str):
        try: ts_val = float(ts_input)
        except ValueError: pass

    if ts_val is not None:
        # Handle potential millisecond timestamps
        if 1e12 <= ts_val < 1e14: ts_val /= 1000
        # Basic sanity check for plausible Unix timestamp range
        elif ts_val > 1e14 or ts_val < -1e12 : return router_escape_markdown_v2(str(ts_input)) # Unlikely timestamp, return original
        try:
            # Prefer helius_analyzer formatting if available and works
            if 'ha_format_timestamp' in globals() and callable(ha_format_timestamp):
                formatted_ts = ha_format_timestamp(int(ts_val))
                # Check if formatting returned a valid string
                if isinstance(formatted_ts, str) and formatted_ts != "None":
                    return router_escape_markdown_v2(formatted_ts)
                else: # Fallback if ha_format_timestamp fails or returns None
                    dt_obj = datetime.fromtimestamp(int(ts_val), tz=timezone.utc)
                    return router_escape_markdown_v2(dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC'))
            else: # Fallback if helius_analyzer not available
                 dt_obj = datetime.fromtimestamp(int(ts_val), tz=timezone.utc)
                 return router_escape_markdown_v2(dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC'))
        except (ValueError, TypeError, OSError): pass # Error during conversion, try next method

    # Attempt to parse ISO format strings
    if isinstance(ts_input, str):
        try:
            # Handle timezone info correctly
            dt_obj = datetime.fromisoformat(ts_input.replace("Z", "+00:00")).astimezone(timezone.utc)
            return router_escape_markdown_v2(dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC'))
        except ValueError: pass # Not a valid ISO string

    # If all parsing fails, return the original input, escaped
    logger.warning(f"Could not parse timestamp: {ts_input}. Returning raw.")
    return router_escape_markdown_v2(str(ts_input))


def _load_json_file_manual_checker(filepath: str, default_value: Any = None) -> Any:
    """Loads JSON data from a file, with error handling."""
    # Use list as default for token tracking files, dict otherwise
    if default_value is None:
        if "tracked_tokens" in filepath: default_value = []
        else: default_value = {}

    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: data = json.load(f)
            logger.debug(f"Loaded data from {filepath}")
            return data
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"[JSON Load MC - ERROR] Error loading {filepath}: {e}. Using default.")
            return default_value # Return the appropriate default type
    else:
        logger.warning(f"File not found: {filepath}. Returning default.")
        return default_value


def _save_json_file_manual_checker(filepath: str, data: Any):
    """Saves data to a JSON file, with error handling."""
    try:
        with open(filepath, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
        logger.debug(f"Saved data to {filepath}")
    except IOError as e: logger.error(f"[JSON Save MC - ERROR] Error saving {filepath}: {e}")
    except TypeError as e: logger.error(f"[JSON Save MC - ERROR] Data is not JSON serializable for {filepath}: {e}")


# --- Core Logic Functions ---

async def perform_safety_audit(contract_address: str) -> Tuple[str, str, str]:
    """Performs safety audit using Birdeye, GMGN, etc. Returns (report_string, symbol, name)."""
    escaped_ca_display = router_escape_markdown_v2(contract_address)
    report_lines = [f"🛡️ *Safety Audit for CA:* `{escaped_ca_display}` 🛡️\n"]
    results = {} # For debugging in case of errors
    token_symbol_raw, token_name_raw = "N/A", "N/A"
    token_symbol_md, token_name_md = "N/A", "N/A"
    decimals_val = None

    # Define links
    dexscreener_link = f"https://dexscreener.com/solana/{contract_address}"
    birdeye_link = f"https://birdeye.so/token/{contract_address}?chain=solana"

    # Get the current event loop or create one if necessary
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("No running event loop, getting default event loop.")
        loop = asyncio.get_event_loop()

    # Validate CA format
    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", contract_address):
        logger.warning(f"Invalid CA format received: {contract_address}")
        return f"❌ Invalid CA format: `{escaped_ca_display}`", "N/A", "N/A"

    try:
        # --- Fetch Data (using run_in_executor for potentially blocking calls) ---
        logger.info(f"Starting safety audit for {contract_address}")

        # Metadata
        metadata_api_response = await loop.run_in_executor(None, birdeye_api.metadata_batch, [contract_address])
        token_specific_metadata = {}
        results["metadata_api_response"] = metadata_api_response # Store raw response for debugging
        if metadata_api_response:
            if isinstance(metadata_api_response, dict):
                # Handle both potential response structures
                if "items" in metadata_api_response and isinstance(metadata_api_response.get("items"), dict):
                    token_specific_metadata = metadata_api_response["items"].get(contract_address, {})
                else: # Direct response or different structure
                    token_specific_metadata = metadata_api_response.get(contract_address, {})
        if token_specific_metadata:
            token_symbol_raw = token_specific_metadata.get("symbol", "N/A")
            token_name_raw = token_specific_metadata.get("name", "N/A")
            decimals_val = token_specific_metadata.get("decimals")
            logger.debug(f"Metadata for {contract_address}: Symbol={token_symbol_raw}, Name={token_name_raw}, Decimals={decimals_val}")

        # Market Data (Birdeye's token overview)
        market_data_raw = await loop.run_in_executor(None, birdeye_api.market_data, contract_address)
        results["market_data_raw"] = market_data_raw
        if market_data_raw:
            # Fallback if metadata failed
            if token_symbol_raw == "N/A": token_symbol_raw = market_data_raw.get("symbol", "N/A")
            if token_name_raw == "N/A": token_name_raw = market_data_raw.get("name", "N/A")
            if decimals_val is None: decimals_val = market_data_raw.get("decimals")
            logger.debug(f"Market data found for {contract_address}. Symbol={token_symbol_raw}, Name={token_name_raw}, Decimals={decimals_val}")
        else:
             logger.warning(f"No market data found from Birdeye for {contract_address}")


        # Prepare display names (escape markdown)
        if token_symbol_raw != "N/A" and token_symbol_raw is not None: token_symbol_md = router_escape_markdown_v2(token_symbol_raw)
        else: token_symbol_raw = "N/A"; token_symbol_md = "N/A" # Ensure raw is also 'N/A'
        if token_name_raw != "N/A" and token_name_raw is not None: token_name_md = router_escape_markdown_v2(token_name_raw)
        else: token_name_raw = "N/A"; token_name_md = "N/A" # Ensure raw is also 'N/A'

        # Add Token Name/Symbol line
        if token_symbol_raw != "N/A" and token_name_raw != "N/A": report_lines.append(f"*Token:* ${token_symbol_md} \\({token_name_md}\\)")
        elif token_symbol_raw != "N/A": report_lines.append(f"*Token:* ${token_symbol_md} \\(Name N/A\\)")
        else: report_lines.append(f"*Token:* Symbol/Name N/A \\(Meta failed or not found\\)")
        report_lines.append(f"🔗 [Birdeye]({birdeye_link}) \\| [DexScreener]({dexscreener_link})\n")

        # Trade Data (Prices, Volume etc.)
        trade_data_raw = await loop.run_in_executor(None, birdeye_api.trade_data, contract_address)
        results["trade_data_raw"] = trade_data_raw
        if not trade_data_raw: logger.warning(f"No trade data found from Birdeye for {contract_address}")

        # Security Data (Birdeye specific)
        bds_security_raw = await loop.run_in_executor(None, birdeye_api.token_security, contract_address)
        results["bds_security_raw"] = bds_security_raw
        if not bds_security_raw: logger.warning(f"No security data found from Birdeye for {contract_address}")

        # Holder Data
        holders_response = await loop.run_in_executor(None, birdeye_api.holders, contract_address, 3) # Get top 3
        holders_data_raw = []
        if holders_response and isinstance(holders_response.get("items"), list):
             holders_data_raw = holders_response["items"]
        results["holders_response"] = holders_response # Store raw response
        if not holders_data_raw: logger.warning(f"No holder data found from Birdeye for {contract_address}")


        # --- Process & Format Data ---
        price_str, liquidity_str, mc_display_str, volume_24h_str = "N/A", "N/A", "N/A", "N/A"
        age_str_part, total_holders_bds_str = "N/A", "N/A"
        price_changes_str = "N/A"; holder_conc_display_str = "N/A"

        # Process Market Data
        if market_data_raw:
            price_val = market_data_raw.get("price")
            liquidity_val = market_data_raw.get("liquidity")
            mc_api_val = market_data_raw.get("marketCap") or market_data_raw.get("mc")
            fdv_api_val = market_data_raw.get("fdv")

            # Determine Market Cap/FDV string
            if mc_api_val is not None:
                mc_display_str = router_escape_markdown_v2(_format_currency(mc_api_val))
            elif fdv_api_val is not None:
                mc_display_str = f"N/A \\(FDV: {router_escape_markdown_v2(_format_currency(fdv_api_val))}\\)"
            else: # Try calculating MC if possible
                total_supply_calc_str = market_data_raw.get("supply") or market_data_raw.get("total_supply")
                if price_val is not None and total_supply_calc_str is not None:
                    try:
                        calculated_mc = float(price_val) * float(total_supply_calc_str)
                        mc_display_str = f"{router_escape_markdown_v2(_format_currency(calculated_mc))} \\(calc\\.\\)"
                    except (ValueError, TypeError): mc_display_str = "N/A \\(Calc Error\\)"
                else: mc_display_str = "N/A"

            # Format Price with dynamic precision
            precision_price = 4 # Default precision
            if price_val:
                current_decimals_for_price = decimals_val # Prefer metadata decimals
                if current_decimals_for_price is None: current_decimals_for_price = market_data_raw.get('decimals') # Fallback to market data

                if current_decimals_for_price is not None:
                    # Adjust precision based on price magnitude and known decimals
                    if price_val < 0.000001 and current_decimals_for_price > 6: precision_price = current_decimals_for_price
                    elif price_val < 0.0001 and current_decimals_for_price >=6 : precision_price = 6
                    elif price_val < 0.01 and current_decimals_for_price >=5 : precision_price = 5
                else: # Estimate precision if decimals unknown
                    if price_val < 0.000001 : precision_price = 8
                    elif price_val < 0.0001 : precision_price = 6
                    elif price_val < 0.01 : precision_price = 5

            price_str = router_escape_markdown_v2(_format_currency(price_val, precision=precision_price))

            # Format Liquidity and add warning if low
            liquidity_str_raw = _format_currency(liquidity_val)
            liquidity_str = router_escape_markdown_v2(liquidity_str_raw)
            if liquidity_val is None or liquidity_val < MIN_LIQUIDITY_THRESHOLD_USD:
                liquidity_str += f" \\(🚩 *Low\\!*\\)"

            # Determine Token Age
            # Try multiple sources for creation timestamp
            created_timestamp_s = market_data_raw.get("createTimestamp")
            if created_timestamp_s is None and trade_data_raw: created_timestamp_s = trade_data_raw.get("tokenCreateTime")
            if created_timestamp_s is None and bds_security_raw: created_timestamp_s = bds_security_raw.get("creationTime")

            if created_timestamp_s:
                try:
                    # Ensure it's a valid positive number before converting
                    if isinstance(created_timestamp_s, (int, float)) and created_timestamp_s > 0:
                        creation_dt = datetime.fromtimestamp(int(created_timestamp_s), tz=timezone.utc)
                        age_delta = datetime.now(timezone.utc) - creation_dt
                        # Format age nicely
                        days, rem_secs = divmod(age_delta.total_seconds(), 86400); hours, rem_secs = divmod(rem_secs, 3600); mins, secs = divmod(rem_secs, 60)
                        days, hours, mins = int(days), int(hours), int(mins)
                        raw_age_str = (f"{days}d {hours}h" if days > 0 else (f"{hours}h {mins}m" if hours > 0 else f"{mins}m {int(secs)}s")) + " old"
                        age_str_part = router_escape_markdown_v2(raw_age_str)
                        # Add warning if token is young
                        if age_delta.total_seconds() / 3600 < MIN_TOKEN_AGE_HOURS:
                             age_str_part += f" \\(🚩 *Young\\!*\\)"
                    else:
                        age_str_part = f"N/A \\(Invalid TS: {router_escape_markdown_v2(str(created_timestamp_s))}\\)"
                except Exception as age_e:
                    logger.error(f"Error calculating age for {contract_address} from TS {created_timestamp_s}: {age_e}")
                    age_str_part = "N/A \\(TS Error\\)"
            else:
                age_str_part = "N/A \\(No TS\\)"

        # Process Trade Data
        if trade_data_raw:
            # Volume
            volume_24h_val = (trade_data_raw.get("volume", {}) or {}).get("h24") or trade_data_raw.get("v24hUSD")
            volume_24h_str = router_escape_markdown_v2(_format_currency(volume_24h_val))

            # Holder Count
            total_holders_bds_val = trade_data_raw.get("holderCount") or trade_data_raw.get("holder")
            total_holders_bds_str = router_escape_markdown_v2(_format_int(total_holders_bds_val)) if total_holders_bds_val is not None else "N/A"

            # Price Changes
            price_change_percent_dict = trade_data_raw.get("priceChangePercent", {}) or {} # Ensure it's a dict
            pc1h_v3 = price_change_percent_dict.get("h1"); pc6h_v3 = price_change_percent_dict.get("h6"); pc24h_v3 = price_change_percent_dict.get("h24")
            # Fallbacks to older API fields if new ones missing
            pc1h = pc1h_v3 if pc1h_v3 is not None else trade_data_raw.get("priceChange1h")
            pc24h = pc24h_v3 if pc24h_v3 is not None else trade_data_raw.get("priceChange24h")
            # Try 6h first, fallback to 4h for medium term
            pc_medium_term_val, pc_medium_term_label = (pc6h_v3, "6h") if pc6h_v3 is not None else (trade_data_raw.get("priceChange4h"), "4h")

            pc_parts = []
            if pc1h is not None: pc_parts.append(f"1h: {router_escape_markdown_v2(_format_percent(pc1h / 100.0))}")
            if pc_medium_term_val is not None and pc_medium_term_label: pc_parts.append(f"{pc_medium_term_label}: {router_escape_markdown_v2(_format_percent(pc_medium_term_val / 100.0))}")
            if pc24h is not None: pc_parts.append(f"24h: {router_escape_markdown_v2(_format_percent(pc24h / 100.0))}")
            price_changes_str = " \\| ".join(pc_parts) if pc_parts else "N/A"

        # Add Basic Info to Report
        report_lines.append(f"💧 *Liquidity:* {liquidity_str}")
        report_lines.append(f"💰 *Market Cap/FDV:* {mc_display_str}")
        # Only show volume if it's significant
        if volume_24h_str != "N/A" and volume_24h_str != router_escape_markdown_v2("$0"):
             report_lines.append(f"📊 *Volume \\(24h\\):* {volume_24h_str}")
        report_lines.append(f"💲 *Price:* {price_str}")
        if price_changes_str != "N/A" and price_changes_str:
            report_lines.append(f"Δ *Price %:* {price_changes_str}")
        report_lines.append(f"⏳ *Age:* {age_str_part}")
        # Only show holder count if significant
        if total_holders_bds_str != "N/A" and total_holders_bds_str != router_escape_markdown_v2("0"):
            report_lines.append(f"👥 *Total Holders \\(BDS\\):* {total_holders_bds_str}")

        # --- Security & Health Flags Section ---
        report_lines.append("\n🚩 *SECURITY & HEALTH FLAGS:*")

        # Process Birdeye Security Data
        if bds_security_raw:
            mutable = bds_security_raw.get('mutableMetadata')
            freeze_auth_present_flag = bds_security_raw.get('freezeAuthority') is not None
            is_freezeable = bds_security_raw.get('freezeable', False) # Default to False if missing
            top10_holder_pct_bds_sec = bds_security_raw.get('top10HolderPercent')

            mutable_display = "*Yes* 🚩" if isinstance(mutable, bool) and mutable else ("No" if isinstance(mutable, bool) else "N/A")
            # Freeze check: Needs both freeze authority AND freezeable flag to be dangerous
            freeze_display = "*Yes* 🚩" if freeze_auth_present_flag and is_freezeable else ("No" if isinstance(is_freezeable, bool) or (bds_security_raw and not freeze_auth_present_flag) else "N/A")

            report_lines.append(f"  \\- BDS Mutable Metadata: {mutable_display}")
            report_lines.append(f"  \\- BDS Freeze Authority: {freeze_display}")

            # Holder Concentration from BDS Security (if available)
            if top10_holder_pct_bds_sec is not None:
                 try:
                     holder_conc_display_str_raw = f"{float(top10_holder_pct_bds_sec) * 100.0:.1f}% (BDS Sec Top10)"
                     holder_conc_display_str = router_escape_markdown_v2(holder_conc_display_str_raw)
                     if float(top10_holder_pct_bds_sec) > MAX_HOLDER_CONCENTRATION_PCT:
                         holder_conc_display_str += f" \\(🚩 *High\\!*\\)"
                 except (ValueError, TypeError):
                     logger.warning(f"Could not parse top10HolderPercent: {top10_holder_pct_bds_sec}")
                     holder_conc_display_str = "N/A (Parse Error)"
        else:
            # Report failure if BDS security fetch didn't work
            report_lines.extend([f"  \\- BDS Security: Fetch Failed", f"  \\- BDS Mutable Metadata: N/A", f"  \\- BDS Freeze Authority: N/A"])

        # Holder Concentration Calculation (Fallback if BDS Security didn't provide it)
        # Requires holders list, market data (for supply), and decimals
        if holder_conc_display_str == "N/A" and holders_data_raw and market_data_raw and isinstance(decimals_val, int):
            total_supply_for_calc_str = market_data_raw.get("supply")
            # Ensure we have supply and holder data
            if total_supply_for_calc_str and holders_data_raw and (holders_data_raw[0].get('uiAmountString') or holders_data_raw[0].get('uiAmount')):
                try:
                    total_supply_for_calc_float = float(total_supply_for_calc_str)
                    # Get top holder amount (prefer uiAmountString for precision)
                    top_holder_ui_amount_str = holders_data_raw[0].get('uiAmountString') or str(holders_data_raw[0].get('uiAmount', '0'))
                    if top_holder_ui_amount_str:
                        top_holder_ui_amount_float = float(top_holder_ui_amount_str)
                        # Calculate percentage if supply > 0
                        if total_supply_for_calc_float > 0:
                            top1_calc_pct = top_holder_ui_amount_float / total_supply_for_calc_float
                            holder_conc_display_str_raw = f"{top1_calc_pct * 100.0:.1f}% (#1 from list)"
                            holder_conc_display_str = router_escape_markdown_v2(holder_conc_display_str_raw)
                            # Add flag if concentration is high
                            if top1_calc_pct > MAX_HOLDER_CONCENTRATION_PCT:
                                holder_conc_display_str += f" \\(🚩 *High\\!*\\)"
                        else: holder_conc_display_str = "N/A (Supply=0)" # Cannot calculate % if supply is zero
                except (ValueError, TypeError, IndexError) as hc_err:
                    logger.warning(f"Error calculating holder concentration: {hc_err}")
                    holder_conc_display_str = "N/A (Calc Error)"
        # Add holder concentration line if calculated or from BDS Security
        if holder_conc_display_str != "N/A":
            report_lines.append(f"  \\- Holder Conc\\.: {holder_conc_display_str}")

        # Process GMGN Security Data (if enabled and initialized)
        if GMGN_ENABLED and gmgn_analyzer_instance:
            gmgn_sec = await loop.run_in_executor(None, gmgn_analyzer_instance.getSecurityInfo, contract_address)
            results["gmgn_security"] = gmgn_sec
            if gmgn_sec:
                hp = "*Yes* 🚩" if gmgn_sec.get('is_honeypot') else "No"
                mint_val = "*Yes* 🚩" if gmgn_sec.get('is_mintable') else "No"
                report_lines.extend([f"  \\- GMGN Honeypot: {hp}", f"  \\- GMGN Mintable: {mint_val}"])
                if gmgn_sec.get('is_rug'): report_lines.append(f"  \\- GMGN RUG Flag: *Yes* 🚩")
            else: report_lines.append("  \\- GMGN Security: Failed/N/A")
        else: report_lines.append("  \\- GMGN Security: Disabled")

        # List Top Holders (from Birdeye holders list)
        if holders_data_raw:
            temp_top_holders_lines = []
            valid_holder_info_found_for_list = False
            for holder_item_idx, holder_item in enumerate(holders_data_raw):
                owner = holder_item.get('owner')
                # Use uiAmountString first, fallback to uiAmount
                ui_amount_str_val = holder_item.get('uiAmountString') or holder_item.get('uiAmount')
                if owner and ui_amount_str_val is not None:
                     try:
                         ui_amount_val_float = float(str(ui_amount_str_val))
                         # Only list holders with > 0 balance
                         if ui_amount_val_float > 0 :
                             amount_display = router_escape_markdown_v2(f"{_format_int(ui_amount_val_float)} tokens")
                             # Format owner address nicely
                             temp_top_holders_lines.append(f"    \\- `{owner[:4]}...{owner[-4:]}`: {amount_display}")
                             valid_holder_info_found_for_list = True
                     except ValueError:
                         # Handle case where amount is not a number
                         temp_top_holders_lines.append(f"    \\- `{owner[:4]}...{owner[-4:]}`: Amount N/A \\(Parse Error\\)")
            # Only add the section if valid holder info was found
            if valid_holder_info_found_for_list:
                report_lines.extend(["  \\- Top Holders \\(BDS List\\):", *temp_top_holders_lines])

        # Buy/Sell Ratio (from Birdeye trade data)
        if trade_data_raw:
            # Try new v2 fields first, fallback to v1
            buy_vol_usd_obj = (trade_data_raw.get("buys", {}) or {}).get("h24", {}).get("usdAmount")
            sell_vol_usd_obj = (trade_data_raw.get("sells", {}) or {}).get("h24", {}).get("usdAmount")

            try:
                 # Ensure float conversion, default to 0.0 if missing/invalid
                 buy_vol_usd = float(buy_vol_usd_obj if buy_vol_usd_obj is not None else trade_data_raw.get("v24hBuyVolume", 0.0) or 0.0)
                 sell_vol_usd = float(sell_vol_usd_obj if sell_vol_usd_obj is not None else trade_data_raw.get("v24hSellVolume", 0.0) or 0.0)

                 # Only calculate ratio if there's volume
                 if buy_vol_usd > 0 or sell_vol_usd > 0:
                    ratio_str_raw, flag = ("N/A", "")
                    if sell_vol_usd > 0:
                        ratio = buy_vol_usd / sell_vol_usd
                        ratio_str_raw = f"{ratio:.2f}"
                        # Add flag if ratio is below threshold AND sell volume is significant
                        if ratio < MIN_BUY_SELL_RATIO_24H and sell_vol_usd > 1000 :
                            flag = f" \\(🚩 <{router_escape_markdown_v2(str(MIN_BUY_SELL_RATIO_24H))}\\)"
                    elif buy_vol_usd > 0: # Only buys
                        ratio_str_raw = "∞ (Only Buys)"

                    ratio_str = router_escape_markdown_v2(ratio_str_raw)
                    buy_vol_disp = router_escape_markdown_v2(_format_currency(buy_vol_usd))
                    sell_vol_disp = router_escape_markdown_v2(_format_currency(sell_vol_usd))
                    report_lines.append(f"  \\- Buy/Sell Ratio \\(24h\\): {ratio_str}{flag} \\({buy_vol_disp}/{sell_vol_disp}\\)")
            except (ValueError, TypeError) as vol_err:
                 logger.warning(f"Could not parse buy/sell volume for ratio calc: {vol_err}")


        # --- KOL Activity Section ---
        report_lines.append("\n*🗣️ KOL Activity \\(Recent\\):*")
        try:
            if os.path.exists(KOL_SIGNALS_FILE):
                # Use run_in_executor for file I/O
                def read_kol_sync():
                    logger.debug(f"Reading KOL file: {KOL_SIGNALS_FILE}")
                    # Load with robust loading function
                    return _load_json_file_manual_checker(KOL_SIGNALS_FILE, []) # Default to empty list

                kol_signals_all = await loop.run_in_executor(None, read_kol_sync)

                if not isinstance(kol_signals_all, list):
                     logger.error(f"KOL signals file ({KOL_SIGNALS_FILE}) did not contain a list.")
                     report_lines.append(f"  \\- Error reading KOL signals file \\(invalid format\\)\\.")
                else:
                    relevant_signals = []
                    target_symbol_lower = (f"${token_symbol_raw}").lower() if token_symbol_raw and token_symbol_raw != "N/A" else ""
                    now_ts = int(time.time())
                    kol_lookback_ts = now_ts - (KOL_LOOKBACK_HOURS * 3600)

                    # Filter signals for relevance and recency
                    for signal in kol_signals_all:
                        # Check timestamp first for efficiency
                        if signal.get("tweet_actual_timestamp", 0) < kol_lookback_ts: continue
                        signal_ca = signal.get("contract_address")
                        signal_primary_symbol = (signal.get("primary_symbol") or "").lower()

                        match_ca = signal_ca and signal_ca.lower() == contract_address.lower()
                        match_symbol = bool(target_symbol_lower) and signal_primary_symbol == target_symbol_lower

                        if match_ca or match_symbol:
                            relevant_signals.append(signal)

                    # Sort relevant signals by time (most recent first)
                    relevant_signals.sort(key=lambda x: x.get("tweet_actual_timestamp", 0), reverse=True)

                    if not relevant_signals:
                        report_lines.append(f"  \\- No recent KOL activity found for this token in last {KOL_LOOKBACK_HOURS}h\\.")
                    else:
                        logger.info(f"Found {len(relevant_signals)} relevant KOL signals for {contract_address}.")
                        # Determine KOL buy/sell actions
                        kols_actions = {}
                        for s in sorted(relevant_signals, key=lambda x: x.get("tweet_actual_timestamp", 0)): # Sort oldest to newest for action tracking
                            kol_name = s.get("kol_wallet", "Unknown KOL")
                            action = s.get("action")
                            if action in ["buy", "sell"]:
                                kols_actions[kol_name] = {"action": action, "timestamp": s.get("tweet_actual_timestamp")}

                        # Display up to 10 recent signals
                        displayed_signals = 0
                        for signal in relevant_signals:
                            if displayed_signals >= 10:
                                report_lines.append(f"    \\.\\.\\. and {len(relevant_signals) - displayed_signals} more mentions\\.")
                                break
                            action = router_escape_markdown_v2(signal.get("action", "mention").capitalize())
                            kol_name_md = router_escape_markdown_v2(signal.get("kol_wallet", "Unknown KOL"))
                            tweet_dt_str = robust_format_timestamp(signal.get("tweet_actual_timestamp")) # Use robust formatter
                            tweet_id = signal.get("tweet_id")
                            tweet_link_text = router_escape_markdown_v2(f"Tweet {tweet_id}" if tweet_id else "Link")
                            tweet_link_url = f"https://twitter.com/i/web/status/{tweet_id}" if tweet_id else "N/A"

                            report_lines.append(f"  \\- *{action}* by *{kol_name_md}* on {tweet_dt_str}")
                            if tweet_link_url != "N/A":
                                report_lines.append(f"    Tweet: [{tweet_link_text}]({tweet_link_url})")
                            # else: Don't add a line if link is N/A
                            displayed_signals += 1

                        # Summarize KOL holding status based on latest buy/sell signal
                        still_holding_kols_md = [router_escape_markdown_v2(kol) for kol, data in kols_actions.items() if data["action"] == "buy"]
                        sold_kols_md = [router_escape_markdown_v2(kol) for kol, data in kols_actions.items() if data["action"] == "sell"]

                        if still_holding_kols_md: report_lines.append(f"\n  ➡️ *KOLs presumed still holding:* {', '.join(still_holding_kols_md)}")
                        if sold_kols_md: report_lines.append(f"  ➡️ *KOLs who signaled sell:* {', '.join(sold_kols_md)}")
                        if not still_holding_kols_md and not sold_kols_md and relevant_signals:
                             report_lines.append(f"\n  \\- General mentions found, no clear buy/sell signals from known KOLs\\.") # Clarified message
            else:
                report_lines.append(f"  \\- KOL signals file \\('{router_escape_markdown_v2(KOL_SIGNALS_FILE)}'\\) not found\\.")
        except Exception as e_kol:
            logger.error(f"Error checking KOL activity for {contract_address}: {e_kol}", exc_info=True)
            # traceback.print_exc() # Handled by logger
            report_lines.append(f"  \\- Error checking KOL activity: {router_escape_markdown_v2(str(e_kol)[:50])}\\.")

        # --- Final Touches ---
        report_lines.append("\n*Disclaimer: DYOR\\. Data from public APIs may have delays or inaccuracies\\.*")
        final_report = "\n".join(report_lines)
        logger.info(f"Safety audit complete for {contract_address}. Report length: {len(final_report)}")
        return final_report, token_symbol_raw, token_name_raw

    except api_usage_limiter.APIBlockException as abe:
        logger.warning(f"API Blocked during audit for {contract_address}: {abe}")
        return f"❌ API Blocked: {router_escape_markdown_v2(abe.api_name)} \\- {router_escape_markdown_v2(abe.endpoint_name)}\\. Try later\\.", "N/A", "N/A"
    except Exception as e:
        logger.error(f"Unhandled error in audit for {contract_address}: {e}", exc_info=True)
        # traceback.print_exc() # Handled by logger
        # Attempt to dump debug data
        results_dump_file = f"error_audit_{contract_address}_{int(time.time())}.json"
        try:
            with open(results_dump_file, "w") as f: json.dump(results, f, indent=2, default=str) # Use default=str for non-serializable items
            logger.info(f"Debug data dumped to {results_dump_file}")
        except Exception as dump_e: logger.error(f"Could not dump debug data: {dump_e}")
        return f"❌ Unexpected error during audit: {router_escape_markdown_v2(str(e)[:100])}\\. Check server logs\\.", "N/A", "N/A"


def load_tracked_tokens() -> List[Dict]:
    """Loads the list of tracked tokens from the JSON file."""
    logger.debug(f"Loading tracked tokens from {TRACKED_TOKENS_FILE}")
    return _load_json_file_manual_checker(TRACKED_TOKENS_FILE, default_value=[])

def save_tracked_tokens(tokens_list: List[Dict]) -> None:
    """Saves the list of tracked tokens to the JSON file."""
    logger.debug(f"Saving {len(tokens_list)} tracked tokens to {TRACKED_TOKENS_FILE}")
    _save_json_file_manual_checker(TRACKED_TOKENS_FILE, tokens_list)

def add_token_to_tracking(ca: str, chat_id: Any, symbol: str, name: str) -> bool:
    """Adds a token to the tracking list for a specific chat ID. Returns True if added, False if already exists."""
    tokens = load_tracked_tokens()
    chat_id_str = str(chat_id) # Ensure consistency
    # Check if this exact CA and chat_id combination already exists
    if any(t.get('ca') == ca and str(t.get('chat_id')) == chat_id_str for t in tokens):
        logger.info(f"Token {ca} already tracked for chat {chat_id_str}. No action needed.")
        return False
    else:
        # Add new token entry
        new_token_entry = {
            "ca": ca,
            "chat_id": chat_id_str,
            "symbol": symbol if symbol else "N/A", # Ensure defaults
            "name": name if name else "N/A",       # Ensure defaults
            "added_timestamp": int(time.time())
        }
        tokens.append(new_token_entry)
        save_tracked_tokens(tokens)
        logger.info(f"Added token {ca} (Symbol: {symbol}, Name: {name}) for tracking for chat {chat_id_str}.")
        return True

def remove_token_from_tracking(ca_to_remove: str, chat_id: Any) -> bool:
    """Removes a specific token for a specific chat ID. Returns True if removed, False otherwise."""
    chat_id_str = str(chat_id)
    tokens = load_tracked_tokens()
    initial_len = len(tokens)
    # Filter out the token to remove
    tokens_after = [t for t in tokens if not (t.get('ca') == ca_to_remove and str(t.get('chat_id')) == chat_id_str)]

    if len(tokens_after) < initial_len:
        save_tracked_tokens(tokens_after)
        logger.info(f"Removed token {ca_to_remove} from tracking for chat {chat_id_str}.")
        # --- Clean up related state/history ---
        # Alert History
        alert_history = _load_json_file_manual_checker(ALERT_HISTORY_FILE, default_value={})
        keys_to_del_alerts = [k for k in alert_history if k.startswith(f"{ca_to_remove}_{chat_id_str}_")]
        if keys_to_del_alerts:
            logger.debug(f"Removing {len(keys_to_del_alerts)} alert history keys for {ca_to_remove}_{chat_id_str}")
            for k in keys_to_del_alerts: alert_history.pop(k, None)
            _save_json_file_manual_checker(ALERT_HISTORY_FILE, alert_history)
        # Tracker State
        tracker_states = _load_json_file_manual_checker(TRACKER_STATE_FILE, default_value={})
        state_key_to_del = f"{ca_to_remove}_{chat_id_str}"
        if state_key_to_del in tracker_states:
            logger.debug(f"Removing tracker state key: {state_key_to_del}")
            del tracker_states[state_key_to_del]
            _save_json_file_manual_checker(TRACKER_STATE_FILE, tracker_states)
        # --- End cleanup ---
        return True
    else:
        logger.info(f"Attempted to remove {ca_to_remove} for chat {chat_id_str}, but it wasn't tracked.")
        return False

def remove_all_tokens_for_chat(chat_id: Any) -> int:
    """Removes all tracked tokens for a specific chat ID. Returns the number of tokens removed."""
    chat_id_str = str(chat_id)
    tokens = load_tracked_tokens()
    # Identify tokens belonging to this chat
    removed_tokens_info = [t for t in tokens if str(t.get('chat_id')) == chat_id_str]
    count_removed = len(removed_tokens_info)

    if count_removed == 0:
        logger.info(f"Attempted to remove all tokens for chat {chat_id_str}, but none were tracked.")
        return 0

    # Filter out tokens for this chat
    tokens_after = [t for t in tokens if str(t.get('chat_id')) != chat_id_str]
    save_tracked_tokens(tokens_after)
    logger.info(f"Removed all {count_removed} tracked tokens for chat {chat_id_str}.")

    # --- Clean up related state/history for all removed tokens ---
    alert_history = _load_json_file_manual_checker(ALERT_HISTORY_FILE, default_value={})
    tracker_states = _load_json_file_manual_checker(TRACKER_STATE_FILE, default_value={})
    alert_hist_changed = False
    states_changed = False

    for removed_token in removed_tokens_info:
        ca = removed_token.get('ca')
        if not ca: continue # Skip if CA missing for some reason

        # Alert History cleanup
        keys_to_del_alerts = [k for k in alert_history if k.startswith(f"{ca}_{chat_id_str}_")]
        if keys_to_del_alerts:
            logger.debug(f"Removing {len(keys_to_del_alerts)} alert history keys for {ca}_{chat_id_str}")
            for k in keys_to_del_alerts: alert_history.pop(k, None)
            alert_hist_changed = True

        # Tracker State cleanup
        state_key_to_del = f"{ca}_{chat_id_str}"
        if state_key_to_del in tracker_states:
            logger.debug(f"Removing tracker state key: {state_key_to_del}")
            del tracker_states[state_key_to_del]
            states_changed = True

    # Save files only if changes were made
    if alert_hist_changed: _save_json_file_manual_checker(ALERT_HISTORY_FILE, alert_history)
    if states_changed: _save_json_file_manual_checker(TRACKER_STATE_FILE, tracker_states)
    # --- End cleanup ---

    return count_removed

# --- END OF FILE telegram_manual_checker.py ---