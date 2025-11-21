# --- START OF FILE token_tracker_service.py ---

# token_tracker_service.py

import os
import json
import asyncio
import time
from datetime import datetime, timezone # timedelta removed
from decimal import Decimal, getcontext # ROUND_DOWN removed
from dotenv import load_dotenv
import traceback
from typing import Dict, Any, Optional, List # Tuple, Set removed

# --- Project-Specific Imports ---
try:
    import birdeye_api
    from gmgn_api import gmgn
    from report_processor import _format_currency, _format_percent, _format_int
    from telegram_alert_router import send_alert_async, escape_markdown_v2 as router_escape_markdown_v2
    print("[✓] token_tracker_service: Successfully imported base modules (including birdeye_api and telegram_alert_router).")
except (ImportError, ValueError) as e:
    print(f"[!] token_tracker_service: CRITICAL STARTUP ERROR: {e}. Exiting.")
    exit()

# --- Load Environment Variables & Config ---
load_dotenv()
getcontext().prec = 18

TRACKED_TOKENS_FILE = "tracked_tokens.json"
ALERT_HISTORY_FILE = "tracker_alert_history.json"
TRACKER_STATE_FILE = "token_tracker_states.json"

# --- Config Constants for Tracking Logic ---
TOP_N_HOLDERS_TO_MONITOR = 5
MIN_SELL_USD_TO_ALERT_TOP_HOLDER = Decimal("500.00")
MIN_BUY_USD_TO_ALERT_TOP_HOLDER = Decimal("500.00")
TOP_HOLDER_TX_LOOKBACK_MINUTES = 60

MIN_LIQUIDITY_FOR_BUY_SIGNAL_USD = Decimal("10000")
MIN_MCAP_FOR_BUY_SIGNAL_USD = Decimal("20000")
DIP_SCENARIOS_FOR_BUY_ALERT = [
    {"name": "Sharp 1H Drop, Stabilizing", "h1_dip_min": Decimal("-50.0"), "h1_dip_max": Decimal("-15.0"), "m5_confirm_min": Decimal("0.5")},
    {"name": "Deep 6H Dip, Recent Bounce", "h6_dip_min": Decimal("-70.0"), "h6_dip_max": Decimal("-25.0"), "m5_confirm_min": Decimal("1.0")},
]

PROFIT_TARGETS_PCT = [Decimal("0.25"), Decimal("0.50"), Decimal("1.00"), Decimal("2.00")]

CHECK_INTERVAL_HOLDER_ACTIVITY_SECONDS = 2 * 60
CHECK_INTERVAL_BUY_SIGNAL_SECONDS = 2 * 60
CHECK_INTERVAL_PRICE_TARGET_SECONDS = 60
MAIN_LOOP_POLL_TRACKED_LIST_SECONDS = 30

ALERT_COOLDOWN_SECONDS = 30
TARGET_ALERT_COOLDOWN_SECONDS = 30
INITIAL_REPORT_COOLDOWN_SECONDS = 23 * 60 * 60 # Almost 24 hours

alert_history: Dict[str, int] = {}
tracker_states: Dict[str, Any] = {}

gmgn_analyzer_instance = None
if birdeye_api.BIRDEYE_API_KEY:
    try:
        gmgn_analyzer_instance = gmgn()
        print("[✓] token_tracker_service: GMGN Analyzer initialized.")
    except Exception as e:
        print(f"[!] token_tracker_service: GMGN Analyzer init failed: {e}")
        gmgn_analyzer_instance = None

USDC_MINT_ADDRESS_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# JSON load/save helpers
def _load_json_file(filepath: str, default_value: Any = None) -> Any:
    if default_value is None: default_value = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[!] Error loading {filepath}: {e}. Using default.")
    return default_value

def _save_json_file(filepath: str, data: Any):
    try:
        with open(filepath, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
    except IOError as e: print(f"[!] Error saving {filepath}: {e}")

def load_alert_history(): global alert_history; alert_history = _load_json_file(ALERT_HISTORY_FILE, {})
def save_alert_history(): _save_json_file(ALERT_HISTORY_FILE, alert_history)
def load_tracker_states(): global tracker_states; tracker_states = _load_json_file(TRACKER_STATE_FILE, {})
def save_tracker_states(): _save_json_file(TRACKER_STATE_FILE, tracker_states)

# Alert cooldown logic
def can_send_alert(ca: str, chat_id: str, event_type: str, unique_event_id_part: str = "", cooldown_override: Optional[int] = None, send_once_if_event_part_exists: bool = False) -> bool:
    event_key = f"{ca}_{chat_id}_{event_type}_{unique_event_id_part}"
    last_alert_time = alert_history.get(event_key, 0)

    if send_once_if_event_part_exists:
        # If a unique_event_id_part is provided (e.g., for a specific transaction hash)
        # and this event_key has been recorded before (last_alert_time > 0),
        # then this specific event should not be alerted again.
        if unique_event_id_part and last_alert_time > 0:
            return False  # Event already sent, do not send again

        # If it's the first time for this unique event (last_alert_time == 0),
        # or if there's no specific unique_event_id_part to make it "send-once",
        # it's eligible to be sent (this time).
        # No further cooldown check is applied here for send-once type;
        # it's assumed that if it's send-once, the first time is always allowed.
        return True

    # Default behavior: cooldown based (for alerts that can repeat after a cooldown)
    cooldown = cooldown_override if cooldown_override is not None else ALERT_COOLDOWN_SECONDS
    return time.time() - last_alert_time > cooldown

def record_alert_sent(ca: str, chat_id: str, event_type: str, unique_event_id_part: str = ""):
    event_key = f"{ca}_{chat_id}_{event_type}_{unique_event_id_part}"
    alert_history[event_key] = int(time.time())
    save_alert_history()

# --- Symbol & Name Resolution Helper ---
async def ensure_token_symbol_and_name(token_info: Dict[str, Any], chat_id_str: str):
    """
    Ensures token symbol and name are available in token_info.
    Fetches from Birdeye if missing/N/A and persists them to tracked_tokens.json.
    `chat_id_str` must be the string representation of the chat_id.
    Modifies `token_info` in place.
    """
    ca = token_info["ca"]
    current_symbol = token_info.get("symbol")
    current_name = token_info.get("name")
    chain = token_info.get("chain", "solana")

    needs_resolution = False
    if not current_symbol or current_symbol == "N/A" or not current_symbol.strip():
        needs_resolution = True
    if not current_name or current_name == "N/A" or not current_name.strip():
        needs_resolution = True

    if not needs_resolution:
        return # Already have valid symbol and name

    # print(f"    [Symbol/Name] Attempting to resolve for {router_escape_markdown_v2(ca)} (Chat: {chat_id_str})...")
    
    resolved_symbol = "N/A"
    resolved_name = "N/A"

    # Try birdeye_api.metadata_batch first
    try:
        metadata_response = await asyncio.to_thread(birdeye_api.metadata_batch, [ca])
        await asyncio.sleep(0.05) 
        if isinstance(metadata_response, dict) and ca in metadata_response:
            md = metadata_response[ca]
            resolved_symbol = md.get("symbol", "N/A").strip()
            resolved_name = md.get("name", "N/A").strip()
    except Exception as e:
        print(f"        [!] Error fetching metadata_batch for {ca}: {e}")

    # Fallback to market_data if still N/A or empty
    symbol_from_md_is_valid = resolved_symbol and resolved_symbol != "N/A" and resolved_symbol.strip()
    name_from_md_is_valid = resolved_name and resolved_name != "N/A" and resolved_name.strip()

    if not symbol_from_md_is_valid or not name_from_md_is_valid:
        try:
            market_data = await bds_get_market_data(ca, chain=chain)
            await asyncio.sleep(0.05)
            if market_data:
                if not symbol_from_md_is_valid:
                    s = market_data.get("symbol", "N/A").strip()
                    if s and s != "N/A": resolved_symbol = s
                if not name_from_md_is_valid:
                    n = market_data.get("name", "N/A").strip()
                    if n and n != "N/A": resolved_name = n
        except Exception as e:
            print(f"        [!] Error fetching market_data for fallback for {ca}: {e}")
    
    final_symbol = resolved_symbol if resolved_symbol and resolved_symbol != "N/A" and resolved_symbol.strip() else ca[:6]
    final_name = resolved_name if resolved_name and resolved_name != "N/A" and resolved_name.strip() else "N/A" # Keep N/A if truly not found

    if token_info.get("symbol") != final_symbol or token_info.get("name") != final_name:
        print(f"        [Symbol/Name] Resolved: {router_escape_markdown_v2(ca)} -> Symbol: '{router_escape_markdown_v2(final_symbol)}', Name: '{router_escape_markdown_v2(final_name)}' (Chat: {chat_id_str})")
        token_info["symbol"] = final_symbol
        token_info["name"] = final_name

        # Persist to tracked_tokens.json
        tracked_list = _load_json_file(TRACKED_TOKENS_FILE, [])
        updated_in_file = False
        for t_item in tracked_list:
            if t_item.get("ca") == ca and str(t_item.get("chat_id")) == chat_id_str:
                if t_item.get("symbol") != final_symbol or t_item.get("name") != final_name:
                    t_item["symbol"] = final_symbol
                    t_item["name"] = final_name
                    updated_in_file = True
                break
        
        if updated_in_file:
            _save_json_file(TRACKED_TOKENS_FILE, tracked_list)
            # print(f"        [Symbol/Name] Persisted symbol and name for {router_escape_markdown_v2(ca)} in {TRACKED_TOKENS_FILE}")

def get_token_display_details(token_info: Dict[str, Any]) -> tuple[str, str, str]:
    # token_info should have 'symbol' and 'name' populated by ensure_token_symbol_and_name
    symbol = token_info.get("symbol", token_info.get("ca", "N/A")[:6]) 
    name = token_info.get("name", "N/A")
    
    symbol_esc = router_escape_markdown_v2(symbol)
    name_esc = router_escape_markdown_v2(name)

    if name and name != "N/A" and name.strip():
        name_display_esc = f"{symbol_esc} ({name_esc})"
    else:
        name_display_esc = symbol_esc
    
    return name_display_esc, symbol_esc, name_esc

# --- BIRDEYE DATA FETCHERS ---
async def bds_get_market_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    return await asyncio.to_thread(birdeye_api.market_data, mint_address=mint, chain=chain)
async def bds_get_trade_data(mint: str, chain: str = "solana") -> Optional[Dict]:
    return await asyncio.to_thread(birdeye_api.trade_data, mint_address=mint, chain=chain)
async def bds_get_token_holders(mint_address: str, limit: int = 20, offset: int = 0, chain: str = "solana") -> Optional[Dict]:
    return await asyncio.to_thread(birdeye_api.holders, mint_address=mint_address, limit=limit, offset=offset, chain=chain)
async def bds_get_token_security(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    return await asyncio.to_thread(birdeye_api.token_security, mint_address=mint_address, chain=chain)
async def bds_get_trader_txs_for_token(trader_address: str, token_address:str, time_before: int, time_after: int, limit: int = 50, chain: str = "solana") -> List[Dict]:
    all_txs_data = await asyncio.to_thread(birdeye_api.trader_txs, trader_address=trader_address, after_time=time_after, limit=limit, offset=0, chain=chain)
    if not all_txs_data or not isinstance(all_txs_data.get("items"), list): return []
    token_related_txs = []
    for tx in all_txs_data.get("items", []):
        tx_block_time = tx.get("block_unix_time")
        if tx_block_time and tx_block_time < time_after: continue 
        if tx_block_time and tx_block_time > time_before: continue 

        if tx.get("tx_type") == "swap":
            base_addr = (tx.get("base", {}) or {}).get("address")
            quote_addr = (tx.get("quote", {}) or {}).get("address")
            if token_address in [base_addr, quote_addr]: token_related_txs.append(tx)
    return token_related_txs

# --- Formatting utilities ---
def _format_currency_value(value: Optional[Any], prefix: str = "$", precision: int = 2) -> str:
    return _format_currency(value, precision=precision) 

def robust_format_timestamp_value(ts_input: Any) -> str:
    if ts_input is None: return "N/A"
    ts_val = None
    if isinstance(ts_input, (int, float)): ts_val = ts_input
    elif isinstance(ts_input, str):
        try: ts_val = float(ts_input)
        except ValueError: pass
    if ts_val is not None:
        if 1e12 <= ts_val < 1e14: 
             ts_val /= 1000
        elif ts_val > 1e14 or ts_val < -1e12 : 
            return str(ts_input) 
        try: return datetime.fromtimestamp(int(ts_val), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except (ValueError, OSError): pass 
    if isinstance(ts_input, str):
        try:
            dt_obj = datetime.fromisoformat(ts_input.replace("Z", "+00:00"))
            if dt_obj.tzinfo is None:
                 dt_obj = dt_obj.replace(tzinfo=timezone.utc) 
            else:
                 dt_obj = dt_obj.astimezone(timezone.utc)
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
        except ValueError: pass 
    return str(ts_input)

# --- MINI AUDIT FUNCTION ---
async def generate_mini_audit_report(ca: str, chain: str = "solana") -> List[str]:
    lines = []
    market_data = await bds_get_market_data(ca, chain=chain)
    trade_data = await bds_get_trade_data(ca, chain=chain)

    if market_data:
        price = market_data.get("price")
        liq = market_data.get("liquidity")
        mc_raw = market_data.get("market_cap") or market_data.get("marketCap") or market_data.get("mc")
        lines.append(f"*Price:* {router_escape_markdown_v2(_format_currency_value(price, precision=8 if price and Decimal(str(price)) < Decimal('0.0001') else 4))}")
        lines.append(f"*Liq:* {router_escape_markdown_v2(_format_currency_value(liq))}")
        lines.append(f"*MCap:* {router_escape_markdown_v2(_format_currency_value(mc_raw))}")
    else: lines.append("*Market Data:* N/A")

    if trade_data:
        vol24h = (trade_data.get("volume", {}) or {}).get("h24") or trade_data.get("v24hUSD")
        lines.append(f"*Vol (24h):* {router_escape_markdown_v2(_format_currency_value(vol24h))}")
        pc = trade_data.get("priceChangePercent", {})
        pc1h = pc.get("h1"); pc24h = pc.get("h24")
        change_parts = []
        if pc1h is not None: change_parts.append(f"1h: {router_escape_markdown_v2(_format_percent(Decimal(str(pc1h))/100))}")
        if pc24h is not None: change_parts.append(f"24h: {router_escape_markdown_v2(_format_percent(Decimal(str(pc24h))/100))}")
        if change_parts: lines.append(f"*Change %:* {' \\| '.join(change_parts)}")
    else: lines.append("*Trade Data:* N/A")
    return lines


# --- CORE LOGIC FUNCTIONS ---
# --- START OF REPLACEMENT for analyze_holder_activity ---
async def analyze_holder_activity(ca: str, chat_id: str, token_info: Dict):
    await ensure_token_symbol_and_name(token_info, chat_id)
    name_display_esc, symbol_esc, _ = get_token_display_details(token_info)
    chain = token_info.get("chain", "solana")
    state_key = f"{ca}_{chat_id}" # For fetching this token's specific state

    # print(f"    [Holder Activity] Analyzing {name_display_esc} ({router_escape_markdown_v2(ca)}) on {chain} for chat {chat_id}")
    
    holders_data = await bds_get_token_holders(ca, limit=TOP_N_HOLDERS_TO_MONITOR + 5, chain=chain)
    if not holders_data or not isinstance(holders_data.get("items"), list) or not holders_data["items"]:
        # print(f"        [!] Could not fetch holders for {name_display_esc} (Chain: {chain}).");
        return
    
    current_price_data = await bds_get_market_data(ca, chain=chain)
    current_price = Decimal(str(current_price_data.get("price", "0"))) if current_price_data else Decimal("0")
    if current_price == Decimal("0"):
        # print(f"        [!] Could not fetch current price for {name_display_esc} (Chain: {chain}).")
        # We can still proceed if transactions involve USDC for value calculation.
        pass

    now_ts = int(time.time())
    
    # Determine the effective start time for looking for new transactions for alerts.
    token_specific_tracker_state = tracker_states.get(state_key, {})
    _last_initial_report_ts = token_specific_tracker_state.get("last_initial_report_ts", 0)
    
    standard_alert_lookback_start_ts = now_ts - (TOP_HOLDER_TX_LOOKBACK_MINUTES * 60)
    
    if _last_initial_report_ts > 0 and _last_initial_report_ts > standard_alert_lookback_start_ts :
        effective_alert_time_after = _last_initial_report_ts
    else:
        effective_alert_time_after = standard_alert_lookback_start_ts
    
    if effective_alert_time_after >= now_ts:
        # print(f"        [Holder Activity] effective_alert_time_after ({effective_alert_time_after}) is too close to now_ts ({now_ts}). Skipping analysis for {name_display_esc}.")
        return

    for holder in holders_data["items"][:TOP_N_HOLDERS_TO_MONITOR]:
        owner = holder.get("owner")
        if not owner:
            continue
        
        trader_token_txs = await bds_get_trader_txs_for_token(
            owner,
            ca,
            time_before=now_ts,
            time_after=effective_alert_time_after,
            limit=20, 
            chain=chain
        )
        await asyncio.sleep(0.15) 
        
        if not trader_token_txs:
            continue

        for tx in trader_token_txs:
            tx_hash, tx_block_time = tx.get("tx_hash", "N/A"), tx.get("block_unix_time")
            
            if tx_block_time and tx_block_time < effective_alert_time_after:
                continue

            sold_amount_ui, bought_amount_ui, tx_value_usd = Decimal("0"), Decimal("0"), Decimal("0")
            base_info, quote_info = tx.get("base", {}), tx.get("quote", {})

            base_addr = base_info.get("address") if base_info else None
            quote_addr = quote_info.get("address") if quote_info else None
            base_change = Decimal(str(base_info.get("ui_change_amount", "0"))) if base_info else Decimal("0")
            quote_change = Decimal(str(quote_info.get("ui_change_amount", "0"))) if quote_info else Decimal("0")

            if base_addr == ca: # Token is base
                if base_change < 0: sold_amount_ui = abs(base_change)
                else: bought_amount_ui = base_change
                
                if quote_addr == USDC_MINT_ADDRESS_SOLANA: tx_value_usd = abs(quote_change)
                elif current_price > 0: tx_value_usd = abs(base_change) * current_price
            
            elif quote_addr == ca: # Token is quote
                if quote_change < 0: sold_amount_ui = abs(quote_change)
                else: bought_amount_ui = quote_change
                
                if base_addr == USDC_MINT_ADDRESS_SOLANA: tx_value_usd = abs(base_change)
                elif current_price > 0: tx_value_usd = abs(quote_change) * current_price

            if sold_amount_ui > 0 and tx_value_usd >= MIN_SELL_USD_TO_ALERT_TOP_HOLDER:
                event_id_part = f"sell_holder_{owner[:6]}_{tx_hash[:8]}"
                # Call can_send_alert with send_once_if_event_part_exists=True
                if can_send_alert(ca, chat_id, "holder_sell", event_id_part, send_once_if_event_part_exists=True):
                    ca_md = router_escape_markdown_v2(ca)
                    owner_md = router_escape_markdown_v2(owner)
                    sold_amount_md = router_escape_markdown_v2(_format_int(sold_amount_ui))
                    tx_value_usd_md = router_escape_markdown_v2(_format_currency_value(tx_value_usd))
                    tx_time_md = router_escape_markdown_v2(robust_format_timestamp_value(tx_block_time))
                    # tx_hash_esc = router_escape_markdown_v2(tx_hash) # Raw tx_hash is fine for URL

                    msg_parts = [
                        f"🚨 *Major Holder Sell Alert*",
                        f"Token: *{name_display_esc}* (`{ca_md}`)",
                        f"Wallet `{owner_md}` sold ~{sold_amount_md} ${symbol_esc} (~{tx_value_usd_md})",
                        f"🕒 {tx_time_md}",
                        f"[View on Solana Explorer](https://solscan.io/tx/{tx_hash})" if tx_hash != "N/A" else "Tx: N/A",
                        f"Consider reviewing your position\\."
                    ]
                    mini_audit_lines = await generate_mini_audit_report(ca, chain)
                    if mini_audit_lines: msg_parts.extend(["\n📊 *Snapshot:*", *mini_audit_lines])
                    if await send_alert_async(message="\n".join(msg_parts), chat_id=chat_id, category="tracker", parse_mode="MarkdownV2"):
                        record_alert_sent(ca, chat_id, "holder_sell", event_id_part)

            elif bought_amount_ui > 0 and tx_value_usd >= MIN_BUY_USD_TO_ALERT_TOP_HOLDER:
                event_id_part = f"buy_holder_{owner[:6]}_{tx_hash[:8]}"
                # Call can_send_alert with send_once_if_event_part_exists=True
                if can_send_alert(ca, chat_id, "holder_buy", event_id_part, send_once_if_event_part_exists=True):
                    ca_md = router_escape_markdown_v2(ca)
                    owner_md = router_escape_markdown_v2(owner)
                    bought_amount_md = router_escape_markdown_v2(_format_int(bought_amount_ui))
                    tx_value_usd_md = router_escape_markdown_v2(_format_currency_value(tx_value_usd))
                    tx_time_md = router_escape_markdown_v2(robust_format_timestamp_value(tx_block_time))
                    # tx_hash_esc = router_escape_markdown_v2(tx_hash) # Raw tx_hash is fine for URL

                    msg_parts = [
                        f"💰 *Major Holder Buy Alert*",
                        f"Token: *{name_display_esc}* (`{ca_md}`)",
                        f"Wallet `{owner_md}` bought ~{bought_amount_md} ${symbol_esc} (~{tx_value_usd_md})",
                        f"🕒 {tx_time_md}",
                        f"[View on Solana Explorer](https://solscan.io/tx/{tx_hash})" if tx_hash != "N/A" else "Tx: N/A",
                        f"This could be a positive sign\\."
                    ]
                    mini_audit_lines = await generate_mini_audit_report(ca, chain)
                    if mini_audit_lines: msg_parts.extend(["\n📊 *Snapshot:*", *mini_audit_lines])
                    if await send_alert_async(message="\n".join(msg_parts), chat_id=chat_id, category="tracker", parse_mode="MarkdownV2"):
                        record_alert_sent(ca, chat_id, "holder_buy", event_id_part)

async def analyze_buy_opportunity(ca: str, chat_id: str, token_info: Dict):
    await ensure_token_symbol_and_name(token_info, chat_id)
    name_display_esc, _, _ = get_token_display_details(token_info)
    chain = token_info.get("chain", "solana")

    # print(f"    [Buy Opportunity] Analyzing {name_display_esc} ({router_escape_markdown_v2(ca)}) on {chain} for chat {chat_id}")
    market_data = await bds_get_market_data(ca, chain=chain)
    trade_data = await bds_get_trade_data(ca, chain=chain)
    await asyncio.sleep(0.05) 
    if not market_data or not trade_data: return

    liquidity = Decimal(str(market_data.get("liquidity", "0")))
    mcap_raw = market_data.get("market_cap") or market_data.get("marketCap") or market_data.get("mc")
    mcap = Decimal(str(mcap_raw if mcap_raw is not None else "0"))

    if liquidity < MIN_LIQUIDITY_FOR_BUY_SIGNAL_USD or mcap < MIN_MCAP_FOR_BUY_SIGNAL_USD: return

    price_changes = trade_data.get("priceChangePercent", {}) or trade_data 
    matched_scenario_name = None

    h1_str = str(price_changes.get("h1", "0.0") or "0.0")
    m5_str = str(price_changes.get("m5", "0.0") or "0.0")
    h6_str = str(price_changes.get("h6", "0.0") or "0.0")

    try:
        h1 = Decimal(h1_str)
        m5 = Decimal(m5_str)
        h6 = Decimal(h6_str)
    except Exception as e:
        # print(f"        [!] Could not parse price changes for {name_display_esc} ({router_escape_markdown_v2(ca)}): {e}")
        return 

    for scenario in DIP_SCENARIOS_FOR_BUY_ALERT:
        try:
            if scenario["name"] == "Sharp 1H Drop, Stabilizing" and scenario["h1_dip_min"] <= h1 <= scenario["h1_dip_max"] and m5 >= scenario["m5_confirm_min"]:
                matched_scenario_name = scenario["name"]; break
            elif scenario["name"] == "Deep 6H Dip, Recent Bounce" and scenario["h6_dip_min"] <= h6 <= scenario["h6_dip_max"] and m5 >= scenario["m5_confirm_min"]:
                matched_scenario_name = scenario["name"]; break
        except TypeError as e:
            #  print(f"        [!] Type error comparing price changes for {name_display_esc}: {e}. Data: h1={h1}, m5={m5}, h6={h6}")
             continue 

    if matched_scenario_name:
        event_id_part = f"buy_dip_{matched_scenario_name.replace(' ','_')}"
        if can_send_alert(ca, chat_id, "buy_signal", event_id_part):
            ca_md = router_escape_markdown_v2(ca)
            scenario_md = router_escape_markdown_v2(matched_scenario_name)
            price_md = router_escape_markdown_v2(_format_currency_value(market_data.get('price'), precision=8))
            
            alert_icon = "📈" if "Bounce" in matched_scenario_name or "Stabilizing" in matched_scenario_name else "📉"
            msg_parts = [
                f"{alert_icon} *Potential Buy Opportunity*",
                f"Token: *{name_display_esc}* (`{ca_md}`)",
                f"Scenario: *{scenario_md}* detected.",
                f"Current Price: ~{price_md}",
                f"Consider evaluating for entry\\. DYOR & manage risk\\!"
            ]
            mini_audit_lines = await generate_mini_audit_report(ca, chain)
            if mini_audit_lines: msg_parts.extend(["\n📊 *Snapshot:*", *mini_audit_lines])
            if await send_alert_async(message="\n".join(msg_parts), chat_id=chat_id, category="tracker", parse_mode="MarkdownV2"):
                record_alert_sent(ca, chat_id, "buy_signal", event_id_part)


async def check_profit_targets(ca: str, chat_id: str, token_info: Dict):
    await ensure_token_symbol_and_name(token_info, chat_id)
    name_display_esc, _, _ = get_token_display_details(token_info)
    chain = token_info.get("chain", "solana")
    state_key = f"{ca}_{chat_id}"; token_specific_state = tracker_states.get(state_key)

    if not token_specific_state or "entry_price" not in token_specific_state or Decimal(str(token_specific_state.get("entry_price","0"))) <= 0:
        market_data = await bds_get_market_data(ca, chain=chain)
        current_price_str = str(market_data.get("price", "0")) if market_data else "0"
        current_price_dec = Decimal(current_price_str)

        if current_price_dec > 0:
            tracker_states[state_key] = {
                "entry_price": current_price_str,
                "targets_hit": [],
                "last_seen_price": current_price_str,
                "last_initial_report_ts": tracker_states.get(state_key, {}).get("last_initial_report_ts", 0) 
            }
            save_tracker_states()
            token_specific_state = tracker_states[state_key]
            # print(f"    [Profit Target] Initialized/Reset entry price for {name_display_esc} ({router_escape_markdown_v2(ca)}) to {current_price_str}")
        else:
            # print(f"    [Profit Target] Cannot initialize entry price for {name_display_esc} ({router_escape_markdown_v2(ca)}) - zero price.")
            return 

    entry_price = Decimal(token_specific_state["entry_price"])
    targets_hit_previously = set(token_specific_state.get("targets_hit", []))

    market_data = await bds_get_market_data(ca, chain=chain)
    if not market_data or market_data.get("price") is None:
        # print(f"    [Profit Target] Could not fetch current price for {name_display_esc} ({router_escape_markdown_v2(ca)})")
        return 

    current_price = Decimal(str(market_data.get("price","0")))
    token_specific_state["last_seen_price"] = str(current_price) 

    if current_price <= 0:
        # print(f"    [Profit Target] Current price is zero for {name_display_esc} ({router_escape_markdown_v2(ca)}), skipping target check.")
        save_tracker_states() 
        return

    state_changed = False
    for target_pct in PROFIT_TARGETS_PCT:
        target_price = entry_price * (Decimal("1") + target_pct)
        target_id_str = str(int(target_pct * 100)) 

        if current_price >= target_price and target_id_str not in targets_hit_previously:
            event_id_part = f"target_{target_id_str}pct"
            if can_send_alert(ca, chat_id, "profit_target", event_id_part, cooldown_override=TARGET_ALERT_COOLDOWN_SECONDS):
                ca_md = router_escape_markdown_v2(ca)
                target_id_str_md = router_escape_markdown_v2(target_id_str)
                prec = 8 if entry_price < Decimal('0.0001') else 6
                target_price_fmt_md = router_escape_markdown_v2(_format_currency_value(target_price, precision=prec))
                entry_price_fmt_md = router_escape_markdown_v2(_format_currency_value(entry_price, precision=prec))
                current_price_fmt_md = router_escape_markdown_v2(_format_currency_value(current_price, precision=prec))
                
                msg_parts = [
                    f"🎯 *Profit Target Hit*",
                    f"Token: *{name_display_esc}* (`{ca_md}`)",
                    f"Target: *\\+{target_id_str_md}%* reached (Target Price: ~{target_price_fmt_md})",
                    f"Entry Price: ~{entry_price_fmt_md}",
                    f"Current Price: ~{current_price_fmt_md}",
                    f"Consider your strategy \\(e\\.g\\., take partial profits, adjust stop\\-loss\\)\\. DYOR\\!"
                ]
                mini_audit_lines = await generate_mini_audit_report(ca, chain)
                if mini_audit_lines: msg_parts.extend(["\n📊 *Snapshot:*", *mini_audit_lines])

                if await send_alert_async(message="\n".join(msg_parts), chat_id=chat_id, category="tracker", parse_mode="MarkdownV2"):
                    token_specific_state.setdefault("targets_hit", []).append(target_id_str)
                    record_alert_sent(ca, chat_id, "profit_target", event_id_part)
                    state_changed = True 

    if state_changed: 
        save_tracker_states()
    # Save if last_seen_price was updated, even if no targets hit
    elif tracker_states.get(state_key, {}).get("last_seen_price") != str(current_price) :
        save_tracker_states()


async def send_initial_tracking_report(ca: str, chat_id: str, token_info: Dict):
    await ensure_token_symbol_and_name(token_info, chat_id) # Ensures token_info has symbol and name
    name_display_esc, _, _ = get_token_display_details(token_info)
    chain = token_info.get("chain", "solana")
    state_key = f"{ca}_{chat_id}"

    last_report_ts = tracker_states.get(state_key, {}).get("last_initial_report_ts", 0)
    if time.time() - last_report_ts > INITIAL_REPORT_COOLDOWN_SECONDS:
        # print(f"    [Initial Report] Generating for {name_display_esc} ({router_escape_markdown_v2(ca)}) for chat {chat_id}")

        market_data = await bds_get_market_data(ca, chain=chain)
        security_data = await bds_get_token_security(ca, chain=chain)
        await asyncio.sleep(0.1) 

        ca_md = router_escape_markdown_v2(ca)
        chain_md = router_escape_markdown_v2(chain)

        if not market_data:
            await send_alert_async(
                message=f"Could not fetch initial market data for {name_display_esc} (`{ca_md}`). Will keep trying to track.",
                chat_id=chat_id, category="tracker", is_status_update=True, parse_mode="MarkdownV2"
            )
            tracker_states.setdefault(state_key, {}).update({"last_initial_report_ts": int(time.time())}) # Record attempt
            save_tracker_states()
            return

        report_lines = [f"Now tracking *{name_display_esc}* (`{ca_md}`) on {chain_md}\\.\n", "📊 *Initial Snapshot:*"]
        report_lines.extend(await generate_mini_audit_report(ca, chain)) 

        if security_data:
            mutable = security_data.get('mutableMetadata')
            freeze_auth = security_data.get('freezeAuthority') is not None and security_data.get('freezeable', False)
            report_lines.append(f"*Mutable Metadata:* {'Yes 🚩' if mutable else 'No'}")
            report_lines.append(f"*Freeze Authority:* {'Yes 🚩' if freeze_auth else 'No'}")
        
        if gmgn_analyzer_instance:
            try:
                gmgn_sec = await asyncio.to_thread(gmgn_analyzer_instance.getSecurityInfo, ca)
                if gmgn_sec:
                    hp = "*Yes* 🚩" if gmgn_sec.get('is_honeypot') else "No"
                    mint_val = "*Yes* 🚩" if gmgn_sec.get('is_mintable') else "No"
                    report_lines.extend([f"*GMGN Honeypot:* {hp}", f"*GMGN Mintable:* {mint_val}"])
                else:
                     report_lines.append("*GMGN Security:* N/A")
            except Exception as e:
                # print(f"      [!] Error fetching GMGN data for {ca_md}: {e}")
                report_lines.append("*GMGN Security:* Error")
        
        # Holder activity summary
        holder_activity_summary_lines = []
        lookback_seconds_initial = TOP_HOLDER_TX_LOOKBACK_MINUTES * 60
        current_ts_initial = int(time.time())
        lookback_ts_initial = current_ts_initial - lookback_seconds_initial

        top_holders_initial = await bds_get_token_holders(ca, limit=TOP_N_HOLDERS_TO_MONITOR, chain=chain)
        await asyncio.sleep(0.05) 

        if top_holders_initial and isinstance(top_holders_initial.get("items"), list):
            for holder_item in top_holders_initial["items"][:TOP_N_HOLDERS_TO_MONITOR]:
                owner = holder_item.get("owner")
                if not owner: continue
                
                trader_txs_initial = await bds_get_trader_txs_for_token(
                    owner, ca, time_before=current_ts_initial, time_after=lookback_ts_initial, limit=50, chain=chain
                )
                await asyncio.sleep(0.15)

                total_sold_ui, total_bought_ui = Decimal("0"), Decimal("0")
                if trader_txs_initial:
                    for tx in trader_txs_initial:
                        base_info, quote_info = tx.get("base", {}), tx.get("quote", {})
                        token_is_base = (base_info.get("address") == ca)
                        token_is_quote = (quote_info.get("address") == ca)
                        change_ui = Decimal("0")
                        if token_is_base: change_ui = Decimal(str(base_info.get("ui_change_amount", "0")))
                        elif token_is_quote: change_ui = Decimal(str(quote_info.get("ui_change_amount", "0")))
                        else: continue 
                        if change_ui < 0: total_sold_ui += abs(change_ui)
                        elif change_ui > 0: total_bought_ui += change_ui
                
                if total_sold_ui > 0 or total_bought_ui > 0:
                    owner_display_md = router_escape_markdown_v2(owner[:6] + "..." + owner[-4:])
                    if total_sold_ui > 0:
                        sells_display_md = router_escape_markdown_v2(_format_int(total_sold_ui))
                        holder_activity_summary_lines.append(f"  \\- Top Holder `{owner_display_md}` sold: {sells_display_md}")
                    if total_bought_ui > 0:
                        buys_display_md = router_escape_markdown_v2(_format_int(total_bought_ui))
                        holder_activity_summary_lines.append(f"  \\- Top Holder `{owner_display_md}` bought: {buys_display_md}")
        
        if holder_activity_summary_lines:
            report_lines.append(f"\n*Holder Activity (last {TOP_HOLDER_TX_LOOKBACK_MINUTES} min):*")
            report_lines.extend(holder_activity_summary_lines)
        
        full_report_msg = "\n".join(report_lines)

        if await send_alert_async(message=full_report_msg, chat_id=chat_id, category="tracker", is_status_update=True, parse_mode="MarkdownV2"):
            tracker_states.setdefault(state_key, {}).update({"last_initial_report_ts": int(time.time())})
            save_tracker_states()


async def tracker_task_for_token(tracked_item: Dict[str, Any], task_run_timestamps: Dict[str, float]):
    ca, chat_id_str = tracked_item["ca"], str(tracked_item["chat_id"])
    current_time = time.time(); state_key = f"{ca}_{chat_id_str}"

    # Ensure symbol and name are populated for the first time if needed.
    # Subsequent calls within specific analyzers will re-check if still "N/A" or empty.
    await ensure_token_symbol_and_name(tracked_item, chat_id_str)
    
    await send_initial_tracking_report(ca, chat_id_str, tracked_item)

    holder_key, buy_key, target_key = f"{state_key}_holder", f"{state_key}_buy", f"{state_key}_target"

    if current_time - task_run_timestamps.get(holder_key, 0) > CHECK_INTERVAL_HOLDER_ACTIVITY_SECONDS:
        # print(f" -> Running Holder Activity Check for {ca}/{chat_id_str}")
        try: await analyze_holder_activity(ca, chat_id_str, tracked_item)
        except Exception as e: print(f"[!] Error in analyze_holder_activity for {ca}/{chat_id_str}: {e}"); traceback.print_exc()
        finally: task_run_timestamps[holder_key] = current_time; await asyncio.sleep(0.2) 

    if current_time - task_run_timestamps.get(buy_key, 0) > CHECK_INTERVAL_BUY_SIGNAL_SECONDS:
        # print(f" -> Running Buy Opportunity Check for {ca}/{chat_id_str}")
        try: await analyze_buy_opportunity(ca, chat_id_str, tracked_item)
        except Exception as e: print(f"[!] Error in analyze_buy_opportunity for {ca}/{chat_id_str}: {e}"); traceback.print_exc()
        finally: task_run_timestamps[buy_key] = current_time; await asyncio.sleep(0.2) 

    if current_time - task_run_timestamps.get(target_key, 0) > CHECK_INTERVAL_PRICE_TARGET_SECONDS:
        # print(f" -> Running Profit Target Check for {ca}/{chat_id_str}")
        try: await check_profit_targets(ca, chat_id_str, tracked_item)
        except Exception as e: print(f"[!] Error in check_profit_targets for {ca}/{chat_id_str}: {e}"); traceback.print_exc()
        finally: task_run_timestamps[target_key] = current_time; await asyncio.sleep(0.2) 


async def main_tracker_loop():
    load_alert_history(); load_tracker_states()
    task_run_timestamps: Dict[str, float] = {} 
    print(f"[*] Token Tracker Service Started (v_refactor_name_symbol_alerts_md) using router for alerts.")

    try:
        while True:
            loop_start_time = time.time()
            try:
                tracked_list = _load_json_file(TRACKED_TOKENS_FILE, [])

                if not tracked_list:
                    # print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] [Tracker] No tokens found in {TRACKED_TOKENS_FILE}. Sleeping {MAIN_LOOP_POLL_TRACKED_LIST_SECONDS}s...")
                    await asyncio.sleep(MAIN_LOOP_POLL_TRACKED_LIST_SECONDS); continue

                # print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] [Tracker] Processing {len(tracked_list)} tracked items...")

                active_task_keys, active_state_keys = set(), set()
                for item in tracked_list:
                    ca_l, chat_id_l_str = item.get('ca'), str(item.get('chat_id'))
                    if not ca_l or not chat_id_l_str: continue 
                    state_key_l = f"{ca_l}_{chat_id_l_str}"
                    active_task_keys.add(f"{state_key_l}_holder")
                    active_task_keys.add(f"{state_key_l}_buy")
                    active_task_keys.add(f"{state_key_l}_target")
                    active_state_keys.add(state_key_l) 

                for key_ts in list(task_run_timestamps.keys()):
                    if key_ts not in active_task_keys:
                        # print(f"    [Cleanup] Removing stale task timestamp: {key_ts}")
                        del task_run_timestamps[key_ts]

                changed_states = False
                for key_s in list(tracker_states.keys()):
                    if key_s not in active_state_keys:
                        # print(f"    [Cleanup] Removing stale tracker state: {key_s}")
                        del tracker_states[key_s]; changed_states = True
                if changed_states: save_tracker_states()

                semaphore = asyncio.Semaphore(5) 

                async def worker(item_to_track):
                    if 'ca' not in item_to_track or 'chat_id' not in item_to_track:
                        #  print(f"[!] Skipping invalid tracked item: {item_to_track}")
                         return
                    async with semaphore:
                        item_to_track.setdefault('symbol', None) 
                        item_to_track.setdefault('name', None) 
                        await tracker_task_for_token(item_to_track, task_run_timestamps)

                valid_items = [item for item in tracked_list if 'ca' in item and 'chat_id' in item]
                tasks_to_run = [worker(item) for item in valid_items]
                results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

                for i, res in enumerate(results):
                    if isinstance(res, Exception):
                        item_info_str = str(valid_items[i] if i < len(valid_items) else "Unknown item")
                        # print(f"[!!!] Error in worker task for {item_info_str}: {res}") # Reduced verbosity
                        # traceback.print_exception(type(res), res, res.__traceback__)

                loop_end_time = time.time()
                duration = loop_end_time - loop_start_time
                # print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] [Tracker] Cycle complete ({duration:.2f}s). Sleeping {MAIN_LOOP_POLL_TRACKED_LIST_SECONDS}s...")
                await asyncio.sleep(max(0, MAIN_LOOP_POLL_TRACKED_LIST_SECONDS - duration)) 

            except asyncio.CancelledError:
                print("[Tracker] Main loop task cancelled."); raise 
            except FileNotFoundError:
                print(f"[!] {TRACKED_TOKENS_FILE} not found. Please create it. Sleeping {MAIN_LOOP_POLL_TRACKED_LIST_SECONDS}s...")
                await asyncio.sleep(MAIN_LOOP_POLL_TRACKED_LIST_SECONDS)
            except Exception as e_inner:
                print(f"[!!!] Unhandled error in token_tracker_service main loop's inner try: {e_inner}")
                traceback.print_exc()
                print(f"[Tracker] Sleeping 60s after error...")
                await asyncio.sleep(60) 

    except KeyboardInterrupt:
        print("[Tracker] KeyboardInterrupt: Stopping...")
    except asyncio.CancelledError:
        print("[Tracker] Main tracker loop cancelled externally.")
    finally:
        print("[Tracker] Finalizing...")
        save_alert_history() 
        save_tracker_states() 
        print("[Tracker] Exited.")

if __name__ == "__main__":
    # Reduce console output for routine operations by commenting out print statements in loops
    # or by using a more sophisticated logging setup. For now, some prints are commented.
    # Example: prints inside loops or frequent checks. Error prints are kept.
    print_interval = 5 * 60 # Print status every 5 minutes
    last_print_time = 0

    async def periodically_print_status(num_tracked):
        global last_print_time
        now = time.time()
        if now - last_print_time > print_interval or num_tracked == 0:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] [Tracker] Status: Processing {num_tracked} items. Next status in ~{print_interval/60:.0f} min.")
            last_print_time = now
    
    # Monkey patch main_tracker_loop to include periodic status (simplified for this example)
    original_main_tracker_loop = main_tracker_loop
    async def new_main_tracker_loop():
        load_alert_history(); load_tracker_states()
        task_run_timestamps: Dict[str, float] = {} 
        print(f"[*] Token Tracker Service Started (v_refactor_name_symbol_alerts_md_quiet) using router for alerts.")
        global last_print_time
        last_print_time = time.time() # Initialize
        
        try:
            while True:
                loop_start_time = time.time()
                try:
                    tracked_list = _load_json_file(TRACKED_TOKENS_FILE, [])
                    await periodically_print_status(len(tracked_list))

                    if not tracked_list:
                        await asyncio.sleep(MAIN_LOOP_POLL_TRACKED_LIST_SECONDS); continue

                    active_task_keys, active_state_keys = set(), set()
                    for item in tracked_list:
                        ca_l, chat_id_l_str = item.get('ca'), str(item.get('chat_id'))
                        if not ca_l or not chat_id_l_str: continue 
                        state_key_l = f"{ca_l}_{chat_id_l_str}"
                        active_task_keys.add(f"{state_key_l}_holder"); active_task_keys.add(f"{state_key_l}_buy"); active_task_keys.add(f"{state_key_l}_target")
                        active_state_keys.add(state_key_l) 

                    for key_ts in list(task_run_timestamps.keys()):
                        if key_ts not in active_task_keys: del task_run_timestamps[key_ts]
                    
                    changed_states = False
                    for key_s in list(tracker_states.keys()):
                        if key_s not in active_state_keys: del tracker_states[key_s]; changed_states = True
                    if changed_states: save_tracker_states()

                    semaphore = asyncio.Semaphore(5) 
                    async def worker(item_to_track):
                        if 'ca' not in item_to_track or 'chat_id' not in item_to_track: return
                        async with semaphore:
                            item_to_track.setdefault('symbol', None); item_to_track.setdefault('name', None) 
                            await tracker_task_for_token(item_to_track, task_run_timestamps)

                    valid_items = [item for item in tracked_list if 'ca' in item and 'chat_id' in item]
                    tasks_to_run = [worker(item) for item in valid_items]
                    results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

                    for i, res in enumerate(results):
                        if isinstance(res, Exception):
                            item_info_str = str(valid_items[i] if i < len(valid_items) else "Unknown item")
                            print(f"[!!!] Error in worker task for {item_info_str}: {res}")
                            # traceback.print_exception(type(res), res, res.__traceback__) # Can be verbose

                    loop_end_time = time.time()
                    duration = loop_end_time - loop_start_time
                    await asyncio.sleep(max(0, MAIN_LOOP_POLL_TRACKED_LIST_SECONDS - duration)) 

                except asyncio.CancelledError: print("[Tracker] Main loop task cancelled."); raise 
                except FileNotFoundError: print(f"[!] {TRACKED_TOKENS_FILE} not found. Sleeping..."); await asyncio.sleep(MAIN_LOOP_POLL_TRACKED_LIST_SECONDS)
                except Exception as e_inner:
                    print(f"[!!!] Unhandled error in token_tracker_service main loop: {e_inner}"); traceback.print_exc()
                    await asyncio.sleep(60) 
        finally:
            print("[Tracker] Finalizing..."); save_alert_history(); save_tracker_states(); print("[Tracker] Exited.")

    try:
        asyncio.run(new_main_tracker_loop()) # Use the modified loop
    except KeyboardInterrupt:
        print("[Tracker Main] Application stopped by user.")
    except asyncio.CancelledError:
        print("[Tracker Main] Application tasks cancelled.")
    except Exception as e_main:
        print(f"[!!!] Critical error launching tracker: {e_main}")
        traceback.print_exc()

# --- END OF FILE token_tracker_service.py ---