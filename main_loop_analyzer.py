# --- START OF FILE main_loop_analyzer.py ---

import os
import time
import json
import random # For dynamic parameters
import math
import traceback # For error reporting
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Set

# --- Eco Mode Configuration ---
try:
    # Add is_eco_mode here
    from eco_mode_config import load_current_params, is_eco_mode
    print("[✓] Eco Mode configuration module loaded.")
except ImportError:
    print("[!] Warning: eco_mode_config.py not found. Eco mode disabled. Using default parameters.")
    # Define dummy functions if import fails to prevent crashes later
    def load_current_params(): return {} # Return empty dict to signal failure
    def is_eco_mode(): return False # Default to False
# --- End Eco Mode ---

# Assuming wallet_purge_logger.py is in the same directory or Python path
try:
    from wallet_purge_logger import log_and_alert_wallet_purge
except ImportError:
    print("[!] Warning: wallet_purge_logger.py not found. Purge alerts disabled.")
    def log_and_alert_wallet_purge(reason: str, removed_list: List[Dict]):
        print(f"[!] Wallet Purge Alert Stub (reason: {reason}, count: {len(removed_list)})")

# Assuming solo_tracker.py is in the same directory or Python path
try:
    from solo_tracker import track_token
    print("[✓] Successfully imported track_token from solo_tracker.py")
    CAN_TRACK_SOLO = True
except ImportError as e:
    print(f"[!] Could not import track_token from solo_tracker.py: {e}. Solo tracking will be disabled.")
    CAN_TRACK_SOLO = False
    def track_token(*args, **kwargs): # Stub if not available
        pass


# --- Define BASE directory first ---
REPORTS_BASE_DIR = "reports"
HOLDING_REPORTS_SUBDIR = os.path.join(REPORTS_BASE_DIR, "holdings")

# --- Define directories for report stages ---
PENDING_REPORTS_DIR = os.path.join(HOLDING_REPORTS_SUBDIR, "pending")     # Raw reports saved here
PROCESSED_REPORTS_DIR = os.path.join(HOLDING_REPORTS_SUBDIR, "processed") # Where processor writes
ARCHIVE_REPORTS_DIR = os.path.join(HOLDING_REPORTS_SUBDIR, "archive_raw") # Where processor archives

# Keep old dirs defined in case other code (or user) expects them, but ensure new ones exist
HOLDING_FULL_REPORTS_DIR = os.path.join(HOLDING_REPORTS_SUBDIR, "full")
HOLDING_DELTA_REPORTS_DIR = os.path.join(HOLDING_REPORTS_SUBDIR, "delta")

# --- Ensure all necessary directories exist ---
os.makedirs(HOLDING_REPORTS_SUBDIR, exist_ok=True)
os.makedirs(PENDING_REPORTS_DIR, exist_ok=True)   # <<< CREATE PENDING DIR
os.makedirs(PROCESSED_REPORTS_DIR, exist_ok=True)
os.makedirs(ARCHIVE_REPORTS_DIR, exist_ok=True)
os.makedirs(HOLDING_FULL_REPORTS_DIR, exist_ok=True) # Keep creating these for now
os.makedirs(HOLDING_DELTA_REPORTS_DIR, exist_ok=True)

# --- Configuration for Holding Monitor State (Store in holdings subdir) ---
HOLDING_MONITOR_STATE_FILE = os.path.join(HOLDING_REPORTS_SUBDIR, "main_loop_holding_state.json")

# --- Cache Manager Import & Init ---
try:
    import cache_manager
    print("[✓] Importing cache_manager for main_loop_analyzer...")
    cache_manager.init_db() # Initialize DB on script start
except ImportError as e:
    print(f"ERROR: Could not import 'cache_manager.py': {e}. Caching disabled.")
    cache_manager = None
# --- End Cache Import ---

# ------------------------------------------------------------------
#  Wallet store: keep top wallets across runs (Fixed Constants)
# ------------------------------------------------------------------
WALLET_STORE_FILE = "wallet_store.json"
MAX_WALLETS_KEPT  = 300 # Fixed: Max capacity of the wallet store
INACTIVITY_WINDOW_SECONDS = 24 * 60 * 60 # Fixed: Time before a wallet is pruned for inactivity

def load_wallet_store() -> Dict[str, Dict[str, Any]]:
    """Loads the wallet store from JSON file."""
    if os.path.exists(WALLET_STORE_FILE):
        try:
            with open(WALLET_STORE_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except Exception as e: print(f"[!] Wallet-store load error: {e}")
    return {}

def save_wallet_store(store: Dict[str, Dict[str, Any]]):
    """Saves the wallet store to JSON file."""
    try:
        with open(WALLET_STORE_FILE, "w", encoding="utf-8") as f: json.dump(store, f, indent=2)
    except IOError as e: print(f"[✗] Wallet-store save error: {e}")

# --- Helius Analyzer Import ---
try:
    from helius_analyzer import (
        get_candidate_tokens,
        find_early_receivers_detailed,
        format_timestamp,
        get_wallet_balances,
        _fetch_parsed_history_page_v0,
        # parse_transactions_helius_v0, # Not explicitly used here currently
    )
    print("[✓] Successfully imported required functions from helius_analyzer.py")
except ImportError as e:
    print(f"ERROR: Could not import from 'helius_analyzer.py': {e}")
    print("       Make sure 'helius_analyzer.py' exists and is named correctly.")
    exit(1) # Exit if core dependency is missing
# --- End Helius Import ---

# --- GMGN API Import ---
try:
    from gmgn_api import gmgn
    print("[✓] Successfully imported gmgn class from gmgn_api.py")
except ImportError as e:
    print(f"ERROR: Could not import from 'gmgn_api.py': {e}")
    print("       GMGN functionality will be disabled.")
    gmgn = None
# --- End GMGN Import ---

load_dotenv()
print("[*] Loaded environment variables for main_loop_analyzer.")

# --- Configuration for Analysis History (Fixed Constants) ---
ANALYSIS_HISTORY_FILE = "analysis_history.json"
# Fixed: Minimum time before re-analyzing the same token
MIN_REANALYSIS_INTERVAL_SECONDS = 4 * 60 * 60
# Fixed: How long analysis history entries are kept before cleanup
HISTORY_CLEANUP_AGE_SECONDS = 14 * 24 * 60 * 60
# --- End History Config ---


# --- Swap Extraction Logic (Core Functionality - Unchanged) ---
def extract_swaps_from_tx(tx_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts swap details from a parsed Helius transaction if it's a known DEX swap."""
    swaps = []
    tx_type = tx_details.get("type")
    source = tx_details.get("source")
    timestamp = tx_details.get("timestamp")
    signature = tx_details.get("signature")

    # Check if it's a SWAP type or from a known DEX source
    if tx_type == "SWAP" or source in ["JUPITER_V6", "RAYDIUM", "ORCA", "METEORA", "PUMP"]:
        events = tx_details.get("events", {})
        swap_event = events.get("swap")

        if swap_event:
            token_in_list = swap_event.get("tokenInputs", [])
            token_out_list = swap_event.get("tokenOutputs", [])

            # Handle simple 1-to-1 swaps
            if len(token_in_list) == 1 and len(token_out_list) == 1:
                token_in = token_in_list[0]
                token_out = token_out_list[0]

                # Try to determine the user wallet involved
                user_wallet = tx_details.get("feePayer") or swap_event.get("taker") or swap_event.get("maker")

                if token_in.get("mint") and token_out.get("mint") and user_wallet:
                    swaps.append({
                        "wallet": user_wallet,
                        "signature": signature,
                        "timestamp": timestamp,
                        "datetime_utc": format_timestamp(timestamp), # Assumes format_timestamp exists
                        "dex_source": source,
                        "token_in_mint": token_in.get("mint"),
                        "token_in_amount": token_in.get("amount"), # Raw amount
                        "token_out_mint": token_out.get("mint"),
                        "token_out_amount": token_out.get("amount") # Raw amount
                    })
    return swaps

# --- Helper: adjust_search_parameters (Core Functionality - Unchanged) ---
def adjust_search_parameters(
    base_min_vol: int, base_max_liq: int, base_max_mc: int
) -> tuple[int, int, int]:
    """Applies random adjustments to search parameters for variety."""
    # Adjust min volume (tend towards lower, but not too low)
    min_vol_adj = base_min_vol * random.uniform(0.30, 0.90)
    new_min_vol = max(500, int(min_vol_adj)) # Ensure a minimum floor

    # Adjust max liquidity (allow wider range)
    max_liq_adj = base_max_liq * random.uniform(0.40, 2.00)
    new_max_liq = max(10_000, int(max_liq_adj)) # Ensure a minimum floor

    # Adjust max market cap (allow slightly wider range)
    max_mc_adj = base_max_mc * random.uniform(0.50, 1.20)
    new_max_mc = max(50_000, int(max_mc_adj)) # Ensure a minimum floor

    print(f"    [*] Dynamic Params Used: Min Vol=${new_min_vol:,.0f}, Max Liq=${new_max_liq:,.0f}, Max MC=${new_max_mc:,.0f}")
    return new_min_vol, new_max_liq, new_max_mc

# --- Analysis History Load/Save/Cleanup Functions (Core Functionality - Corrected) ---
def load_analysis_history() -> Dict[str, int]:
    """Loads the analysis history from JSON file."""
    if os.path.exists(ANALYSIS_HISTORY_FILE):
        try:
            with open(ANALYSIS_HISTORY_FILE, 'r', encoding="utf-8") as f:
                data = json.load(f)
            # Ensure data is dict and values are integers
            return {k: int(v) for k, v in data.items() if isinstance(k, str) and isinstance(v, (int, float))}
        except (IOError, json.JSONDecodeError, ValueError) as e:
            print(f"[!] Error loading history file '{ANALYSIS_HISTORY_FILE}': {e}. Starting fresh.")
    return {}

def save_analysis_history(history: Dict[str, int]):
    """Saves the analysis history to JSON file."""
    try:
        with open(ANALYSIS_HISTORY_FILE, 'w', encoding="utf-8") as f:
            json.dump(history, f, indent=2)
    except IOError as e:
        print(f"[✗] Error saving analysis history to '{ANALYSIS_HISTORY_FILE}': {e}")

# <<< CORRECTION: Ensure definition accepts the age parameter >>>
def cleanup_analysis_history(history: Dict[str, int], history_cleanup_age_seconds: int) -> Dict[str, int]:
     """Removes entries from history older than the specified age."""
     current_time = int(time.time())
     # Use the passed-in age threshold
     cleaned_history = {
         mint: timestamp for mint, timestamp in history.items()
         if current_time - timestamp < history_cleanup_age_seconds
     }
     removed_count = len(history) - len(cleaned_history)
     if removed_count > 0:
         days = history_cleanup_age_seconds / (24 * 60 * 60)
         print(f"[History Cleanup] Removed {removed_count} entries older than {days:.1f} days.")
     return cleaned_history

# --- Holding Monitor State Load/Save Functions (Core Functionality - Unchanged) ---
def load_holding_monitor_state() -> Dict[str, Dict[str, Any]]:
    """Loads the previous holding monitor state from JSON file."""
    if os.path.exists(HOLDING_MONITOR_STATE_FILE):
        try:
            with open(HOLDING_MONITOR_STATE_FILE, "r", encoding="utf-8") as f:
                loaded_state = json.load(f)
            # Basic validation
            return loaded_state if isinstance(loaded_state, dict) else {}
        except Exception as e:
            print(f"[!] Error loading holding monitor state from '{HOLDING_MONITOR_STATE_FILE}': {e}. Starting fresh.")
    return {}

def save_holding_monitor_state(current_state: Dict[str, Dict[str, Any]]):
    """Saves the current holding monitor state to JSON file."""
    try:
        with open(HOLDING_MONITOR_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(current_state, f, indent=2)
    except IOError as e:
        print(f"[✗] Error saving holding monitor state to '{HOLDING_MONITOR_STATE_FILE}': {e}")

# ============================================================================
# Main token-analysis phase (Uses parameters passed from main loop)
# ============================================================================
# <<< CORRECTION: Add monitor_bf_max_mc to definition >>>
def run_analysis_phase(
    gmgn_analyzer: Optional[Any], analysis_history: Dict[str, int],
    # --- Parameters passed in ---
    base_min_volume: int, base_max_liquidity: int,
    max_tokens_to_process: int, initial_helius_limit: int, top_wallets_to_keep: int,
    max_trades_filter: int, recent_history_limit: int, swaps_to_report: int,
    min_correlation_swaps: int, earliest_sigs: int, lone_wolf_check: bool,
    fetch_gmgn: bool, monitor_bf_max_mc: int # Added parameter for wide pass
    # --- End Parameters ---
) -> Dict[str, Any]:
    global wallet_store # Use global wallet_store defined outside
    print("\n" + "=" * 20 + " Starting Analysis Phase " + "=" * 20)
    current_time_analysis_start = time.time()
    analysis_results: Dict[str, Any] = {}

    # --- Candidate Fetching Logic (Uses passed-in parameters) ---
    max_birdeye_attempts = 2
    attempt_count = 0
    candidate_tokens: list[dict] = []

    while attempt_count < max_birdeye_attempts and not candidate_tokens:
        attempt_count += 1
        print(f"[*] Analysis Stage 1 (Attempt {attempt_count}/{max_birdeye_attempts}): Fetching candidates (Birdeye)…")

        # Use parameters passed into the function for dynamic fetching attempts
        if attempt_count == 1:
            # Use base parameters but widen slightly for first pass
            current_min_vol = max(500, int(base_min_volume * 0.20))
            current_max_liq = int(base_max_liquidity * 2.5)
            # Use the MONITOR max MC here for the wide pass
            current_max_mc = monitor_bf_max_mc
            print(f"    [*] Wide Pass Used: Vol≥${current_min_vol:,.0f}, Liq≤${current_max_liq:,.0f}, FDV≤${current_max_mc:,.0f}")
        else:
            # Relax further for the second attempt if needed
            current_min_vol = 300
            current_max_liq = int(base_max_liquidity * 6.0)
            current_max_mc = int(monitor_bf_max_mc * 1.5) # Slightly higher than monitor max
            print(f"    [*] Relaxed Pass Used: Vol≥${current_min_vol:,.0f}, Liq≤${current_max_liq:,.0f}, FDV≤${current_max_mc:,.0f}")

        # Note: Fixed parameters limit=250, min_marketcap=10k, max_pages_to_try=15 are used here
        # as they were in the original script and not part of the eco_mode config.
        fetched_candidates = get_candidate_tokens(
            limit=250,
            min_volume=current_min_vol,
            max_liquidity=current_max_liq,
            min_marketcap=10_000,
            max_marketcap=current_max_mc,
            max_pages_to_try=15
        )

        if fetched_candidates:
            # Filter by MC again just to be sure API adhered to max_marketcap
            fetched_candidates = [tok for tok in fetched_candidates if float(tok.get("mc", 0)) < current_max_mc]
            print(f"    [*] Filtering {len(fetched_candidates)} candidates against analysis history…")
            temp_candidate_tokens = []
            for tok in fetched_candidates:
                mint = tok.get("address")
                last_ts = analysis_history.get(mint)
                # Use global constant MIN_REANALYSIS_INTERVAL_SECONDS
                if last_ts and (current_time_analysis_start - last_ts < MIN_REANALYSIS_INTERVAL_SECONDS):
                    continue
                temp_candidate_tokens.append(tok)

            filtered_out_count = len(fetched_candidates) - len(temp_candidate_tokens)
            if filtered_out_count > 0:
                print(f"        Filtered out {filtered_out_count} recently analysed (within {MIN_REANALYSIS_INTERVAL_SECONDS // 3600} hours).")

            if temp_candidate_tokens:
                candidate_tokens = temp_candidate_tokens
                print(f"    [✓] {len(candidate_tokens)} fresh candidates remain for analysis.")
                break # Found candidates, exit the while loop
            elif attempt_count < max_birdeye_attempts:
                print("    [!] All candidates recently analyzed. Retrying with wider parameters...")
                time.sleep(2)
        else:
            if attempt_count < max_birdeye_attempts:
                print("    [!] Birdeye returned 0 candidates. Retrying...")
                time.sleep(2)

    if not candidate_tokens:
        print("[!] No suitable tokens found after attempts. Skipping analysis phase.")
        return {}

    # Use parameter max_tokens_to_process
    tokens_to_process = candidate_tokens[:max_tokens_to_process]
    print(f"[✓] Analysis Stage 1 Complete: Processing top {len(tokens_to_process)} of {len(candidate_tokens)} candidates.")
    # --- End Candidate Fetching ---

    processed_count = 0
    for token_info in tokens_to_process:
        processed_count += 1
        symbol = token_info.get("symbol", "N/A")
        mint = token_info.get("address")
        volume = float(token_info.get("v24hUSD", 0))
        liquidity = float(token_info.get("liquidity", 0))

        print(f"\n  ({processed_count}/{len(tokens_to_process)}) Analysing {symbol} ({mint})")
        if not mint:
            print("    [✗] Skipping – missing mint address in candidate data.")
            continue

        # Add to history immediately before processing
        ts_analysis = int(time.time())
        analysis_history[mint] = ts_analysis
        save_analysis_history(analysis_history) # Save history after each addition
        print(f"    [*] Added/Updated {symbol} in analysis_history at {format_timestamp(ts_analysis)}")

        try:
            # --- Helius Early Receivers (Uses passed-in parameters) ---
            helius_receiver_data = find_early_receivers_detailed(
                mint,
                limit=initial_helius_limit,     # Use parameter
                target_tx_count=earliest_sigs,  # Use parameter
                check_lone_wolf=lone_wolf_check # Use parameter
            )
            # --- End Helius Early Receivers ---

            # --- GMGN Logic (Uses passed-in parameters) ---
            gmgn_sec_info = None
            gmgn_fetched_profiles = {}
            top_wallets_filtered = []
            if fetch_gmgn and gmgn_analyzer: # Use parameter fetch_gmgn
                print(f"    [*] Fetching GMGN security info for {symbol}...")
                gmgn_sec_info = gmgn_analyzer.getSecurityInfo(mint)
                time.sleep(0.3) # Be nice to unofficial API

                # Extract unique wallets from Helius data for GMGN profile fetching
                unique_wallets_for_gmgn = list({
                    e["wallet"] for e in helius_receiver_data if e.get("wallet")
                })[:initial_helius_limit] # Limit based on Helius initial fetch

                if unique_wallets_for_gmgn:
                    print(f"    [*] Fetching GMGN profiles for top {len(unique_wallets_for_gmgn)} early receivers...")
                    for w_addr in unique_wallets_for_gmgn:
                        profile = gmgn_analyzer.getWalletInfo(walletAddress=w_addr)
                        gmgn_fetched_profiles[w_addr] = profile if profile else {"error": "fetch failed"}
                        time.sleep(0.4) # Be nice

                    # Filter and sort wallets based on GMGN profit/trades
                    valid_profiles = []
                    print(f"    [*] Filtering {len(gmgn_fetched_profiles)} GMGN profiles...")
                    for w, p in gmgn_fetched_profiles.items():
                        if p and not p.get("error"):
                            trades = float(p.get("buy_7d", 0) or 0) + float(p.get("sell_7d", 0) or 0)
                            # Use parameter max_trades_filter
                            if trades > max_trades_filter:
                                print(f"        Wallet {w[:6]} skipped (Trades: {trades:.0f} > {max_trades_filter})")
                                continue
                            # Prioritize realized profit, fallback to PNL
                            profit = float(p.get("realized_profit_7d") or p.get("pnl_7d") or -float("inf"))
                            valid_profiles.append({"wallet": w, "profit": profit, "profile": p})

                    valid_profiles.sort(key=lambda x: x["profit"], reverse=True)
                    # Use parameter top_wallets_to_keep
                    top_wallets_filtered = valid_profiles[:top_wallets_to_keep]
                    print(f"        Kept top {len(top_wallets_filtered)} wallets after filtering.")

                    # Update global wallet_store with latest profit data
                    for item in top_wallets_filtered:
                        w_addr = item["wallet"]
                        profit = item["profit"]
                        # Ensure profit is a valid number for JSON
                        if not math.isfinite(profit): profit = 0.0
                        wallet_store[w_addr] = {"latest_profit": profit, "last_seen_ts": int(time.time())}

                    # Prune wallet_store if it exceeds max size (using global constant)
                    if len(wallet_store) > MAX_WALLETS_KEPT:
                        print(f"    [*] Pruning wallet store (size {len(wallet_store)} > max {MAX_WALLETS_KEPT})...")
                        sorted_wallets = sorted(wallet_store.items(), key=lambda kv: kv[1].get("latest_profit", 0), reverse=True)
                        keep_n = MAX_WALLETS_KEPT // 2 # Keep the best half
                        removed_wallets_list = sorted_wallets[keep_n:]
                        wallet_store = dict(sorted_wallets[:keep_n]) # Update global store
                        # Log the purge
                        log_and_alert_wallet_purge(
                            "profit-prune",
                            [{"wallet": w, "profit": meta.get("latest_profit", 0)} for w, meta in removed_wallets_list]
                        )
                        print(f"        Wallet store trimmed to {len(wallet_store)} best wallets by profit.")
                        save_wallet_store(wallet_store) # Save after pruning
            # --- End GMGN Logic ---

            top_wallets_swaps: Dict[str, list] = {}
            top_wallet_addresses = [item["wallet"] for item in top_wallets_filtered]

            # --- Fetch recent history for top wallets (Uses passed-in parameters) ---
            if top_wallet_addresses:
                print(f"    [*] Fetching recent transaction history for {len(top_wallet_addresses)} profitable wallets...")
                for w_addr in top_wallet_addresses:
                    print(f"        Fetching Helius history page for wallet {w_addr[:6]}...")
                    # Use parameter recent_history_limit
                    recent_tx_data = _fetch_parsed_history_page_v0(w_addr, limit=recent_history_limit)
                    wallet_swaps = []

                    if recent_tx_data: # Check if the list is not None and not empty
                        print(f"        Found {len(recent_tx_data)} recent parsed txs for {w_addr[:6]}. Extracting swaps...")
                        for tx_detail in recent_tx_data:
                            # Extract swaps specific to this wallet
                            wallet_swaps.extend([
                                s for s in extract_swaps_from_tx(tx_detail) if s.get("wallet") == w_addr
                            ])
                    else:
                        print(f"        No recent transaction data found or fetch failed for wallet {w_addr[:6]}.")

                    # Sort swaps newest first and limit the number reported
                    wallet_swaps.sort(key=lambda x: x["timestamp"], reverse=True)
                    # Use parameter swaps_to_report
                    top_wallets_swaps[w_addr] = wallet_swaps[:swaps_to_report]
                    print(f"        Extracted {len(top_wallets_swaps[w_addr])} most recent swaps for {w_addr[:6]}.")
                    time.sleep(0.1) # Small delay between Helius calls
            # --- End Recent History Fetch ---

            # --- Correlated Buys Logic (Uses passed-in parameter) ---
            recent_buyers_map: Dict[str, set] = {}
            all_recent_swaps = [s for swaps_list in top_wallets_swaps.values() for s in swaps_list]
            for swap_info in all_recent_swaps:
                token_out = swap_info.get("token_out_mint") # Token the wallet received (bought)
                wallet_address = swap_info.get("wallet")
                if token_out and wallet_address:
                    recent_buyers_map.setdefault(token_out, set()).add(wallet_address)

            correlated_buys_list = []
            print(f"    [*] Checking for correlated buys among {len(all_recent_swaps)} recent swaps...")
            for mint_addr, buyers_set in recent_buyers_map.items():
                # Use parameter min_correlation_swaps
                if len(buyers_set) >= min_correlation_swaps:
                    # Collect trade details for this correlated buy event
                    trades_for_mint = [
                        s for s in all_recent_swaps
                        if s.get("token_out_mint") == mint_addr and s.get("wallet") in buyers_set
                    ]
                    correlated_buys_list.append({
                        "token_mint": mint_addr,
                        "total_wallets_bought": len(buyers_set),
                        "buying_wallets": sorted(list(buyers_set)), # List of wallets involved
                        "trade_details": sorted(trades_for_mint, key=lambda x: x["timestamp"]) # Chronological
                    })
                    print(f"        Found correlated buy: {mint_addr[:6]}... ({len(buyers_set)} wallets)")
            # --- End Correlated Buys ---

            # --- Final Result Assembly (Core structure unchanged) ---
            analysis_results[symbol] = {
                "analysis_timestamp": ts_analysis,
                "analysis_datetime_utc": format_timestamp(ts_analysis),
                "mint": mint,
                "symbol": symbol, # Include symbol here too
                "liquidity_usd": liquidity,
                "mc": token_info.get("mc"),
                "volume_24h_usd": volume,
                "helius_early_receiver_events": helius_receiver_data,
                "gmgn_security_info": gmgn_sec_info or {}, # Use fetched or empty dict
                "top_profitable_wallets": top_wallet_addresses, # List of wallet addresses
                "top_profitable_wallet_profiles": { # Dict of wallet: profile
                    w_item["wallet"]: w_item["profile"] for w_item in top_wallets_filtered
                },
                "top_wallets_recent_swaps": top_wallets_swaps, # Dict of wallet: [swaps]
                "recent_correlated_buys": correlated_buys_list, # List of correlated buy events
            }
            print(f"    [✓] Analysis successful for {symbol}.")
        except Exception as e:
            print(f"    [✗] ERROR analysing {symbol} ({mint}): {e}")
            traceback.print_exc()
        time.sleep(0.5) # Keep sleep after full token analysis cycle

    # --- Save Snapshot (Core functionality unchanged) ---
    if analysis_results:
        output_filename = "hybrid_snapshot_output.json"
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(analysis_results, f, indent=2)
            print(f"[✓] Analysis snapshot saved to '{output_filename}'")
        except IOError as e:
            print(f"[✗] Error saving analysis snapshot: {e}")

    print("\n" + "=" * 20 + " Analysis Phase Complete " + "=" * 20)
    return analysis_results

# ============================================================================
# Monitoring Phase Function (Saves RAW reports to PENDING dir) - Uses Params
# ============================================================================
def run_holding_monitor_cycle(
    wallets_to_track: Set[str],
    birdeye_filter_mints: Set[str],
    min_correlation_holdings: int, # Parameter passed in
    previous_holdings_state: Dict[str, Dict[str, Any]],
    current_wallet_store: Dict[str, Dict[str, Any]] # Passed for solo_tracker
) -> Dict[str, Dict[str, Any]]:
    """
    Monitors holdings of specified wallets, saves raw reports, and calculates delta state.
    Uses parameters passed from the main loop.
    """
    print(f"\n--- Running Holding Monitor Cycle: {format_timestamp(int(time.time()))} ---")
    current_cycle_timestamp = int(time.time())

    if not wallets_to_track:
        print("[!] Monitor: No wallets provided for tracking. Skipping cycle.")
        return previous_holdings_state # Return previous state if no wallets

    # 1. Fetch Balances & Filter (Core logic unchanged)
    current_wallet_holdings_details: Dict[str, Dict[str, Any]] = {} # Stores {wallet: {mint: details}}
    print(f"[*] Monitor: Fetching holdings for {len(wallets_to_track)} wallets (using cache)...")
    wallets_processed = 0
    for wallet in wallets_to_track:
        wallets_processed += 1
        if wallets_processed % 20 == 0: # Print progress periodically
             print(f"    Processed {wallets_processed}/{len(wallets_to_track)} wallets...")
        balances_data = get_wallet_balances(wallet) # Uses helius_analyzer (cached)
        time.sleep(0.1) # Small delay between calls

        if balances_data is None: # Handle API error or empty wallet case
            current_wallet_holdings_details[wallet] = {}
            continue

        filtered_for_wallet: Dict[str, Any] = {}
        for token_acc in balances_data.get("tokens", []):
            mint = token_acc.get("mint")
            amount_raw = token_acc.get("amount", 0)
            # Filter out tokens with zero balance
            if mint and amount_raw > 0:
                 # Filter based on provided birdeye_filter_mints set
                if not birdeye_filter_mints or mint in birdeye_filter_mints:
                    filtered_for_wallet[mint] = {
                        "mint": mint,
                        "ui_amount": token_acc.get("uiAmount", 0.0),
                        "amount_raw": amount_raw,
                        "decimals": token_acc.get("decimals", 0)
                    }
        current_wallet_holdings_details[wallet] = filtered_for_wallet
    print(f"    Finished fetching for {len(wallets_to_track)} wallets.")


    # 2. Call track_token (Optional, Core logic unchanged)
    if CAN_TRACK_SOLO:
        print("[*] Monitor: Running solo_tracker.track_token for filtered holdings...")
        tracked_count = 0
        for wallet_addr, held_mints_details_map in current_wallet_holdings_details.items():
            if held_mints_details_map:
                mints_held_list = list(held_mints_details_map.keys())
                if mints_held_list:
                    # Assumes track_token handles its own logic/filtering
                    track_token(wallet_addr, mints_held_list, current_wallet_store)
                    tracked_count += 1
        print(f"    Called track_token for {tracked_count} wallets with holdings.")

    # 3. Aggregate & Correlate Holdings (Core logic unchanged)
    print("[*] Monitor: Aggregating and correlating filtered holdings...")
    token_holders: Dict[str, Set[str]] = {} # Stores {mint: {wallet1, wallet2, ...}}
    for wallet, holdings_map in current_wallet_holdings_details.items():
        for mint_addr in holdings_map.keys():
            token_holders.setdefault(mint_addr, set()).add(wallet)

    # 4. Build RAW Correlated Holdings List (Uses parameter min_correlation_holdings)
    raw_correlated_holdings_list: List[Dict[str, Any]] = []
    print(f"[*] Monitor: Building raw correlated list (min holders: {min_correlation_holdings})...")
    for mint_addr, holders_set in token_holders.items():
        # Check correlation threshold AND ensure the mint was in the Birdeye filter
        if len(holders_set) >= min_correlation_holdings and mint_addr in birdeye_filter_mints:
            # Get UI amounts for reporting
            holder_balances_info = {
                h_wallet: current_wallet_holdings_details[h_wallet].get(mint_addr, {}).get("ui_amount", "N/A")
                for h_wallet in holders_set
            }
            raw_correlated_holdings_list.append({
                "token_mint": mint_addr,
                "total_holders_in_set": len(holders_set),
                "holding_wallets_in_set": sorted(list(holders_set)),
                "holder_balances_uiAmount": holder_balances_info,
            })

    # Sort by number of holders, descending
    raw_correlated_holdings_list.sort(key=lambda x: x["total_holders_in_set"], reverse=True)
    print(f"[✓] Monitor: Found {len(raw_correlated_holdings_list)} raw commonly-held tokens meeting criteria.")

    # 5. Prepare and Save RAW Reports to PENDING Directory (Uses parameter)
    report_parameters = {
        "min_holders_for_correlation": min_correlation_holdings, # Record parameter used
        "birdeye_filter_mints_count": len(birdeye_filter_mints),
        "wallets_monitored_count": len(wallets_to_track)
    }

    # --- Raw Full Report Data ---
    full_raw_report_data = {
        "report_timestamp": current_cycle_timestamp,
        "report_datetime_utc": format_timestamp(current_cycle_timestamp),
        "parameters": report_parameters,
        "correlated_holdings": raw_correlated_holdings_list, # Includes only correlated ones
        "all_wallet_filtered_holdings": current_wallet_holdings_details # Includes all holdings passing filter
    }
    # Save Raw Full Report to PENDING directory
    full_raw_report_filename = os.path.join(PENDING_REPORTS_DIR, f"holding_report_{current_cycle_timestamp}.json")
    try:
        with open(full_raw_report_filename, "w", encoding="utf-8") as f:
            json.dump(full_raw_report_data, f, indent=2)
        print(f"[✓] Raw full holding report saved to PENDING: '{os.path.basename(full_raw_report_filename)}'")
    except IOError as e:
        print(f"[✗] Error saving raw full holding report: {e}")


    # --- Calculate Delta (Uses raw amounts, core logic unchanged) ---
    # Create a simple state {wallet: {mint: raw_amount}} for delta comparison
    current_state_for_delta: Dict[str, Dict[str, int]] = {
        w: {m: d["amount_raw"] for m, d in h.items()}
        for w, h in current_wallet_holdings_details.items()
    }
    changed_wallets_for_delta: Dict[str, Any] = {}

    # Compare current state with previous state
    for wallet, current_mints_map in current_state_for_delta.items():
        prev_mints_map = previous_holdings_state.get(wallet, {})
        if current_mints_map != prev_mints_map:
            changed_wallets_for_delta[wallet] = {"current": current_mints_map, "previous": prev_mints_map}

    # Check for wallets that were previously tracked but are now empty/gone
    for wallet in previous_holdings_state.keys():
        if wallet not in current_state_for_delta and wallet not in changed_wallets_for_delta:
             changed_wallets_for_delta[wallet] = {"current": {}, "previous": previous_holdings_state[wallet]}


    # --- Save Raw Delta Report to PENDING directory (if changes occurred) ---
    # Check if there were changes or if this is the first delta report
    delta_dir_has_files = any(f.is_file() and f.name.startswith("holding_delta_") for f in os.scandir(PENDING_REPORTS_DIR))

    if changed_wallets_for_delta or not delta_dir_has_files:
        delta_raw_report_data = {
            "report_timestamp": current_cycle_timestamp,
            "report_datetime_utc": format_timestamp(current_cycle_timestamp),
            "parameters": report_parameters, # Same parameters as full report
            "changed_wallets_balance_state": changed_wallets_for_delta,
            "correlated_holdings_snapshot": raw_correlated_holdings_list # Include snapshot for context
        }
        delta_raw_report_filename = os.path.join(PENDING_REPORTS_DIR, f"holding_delta_{current_cycle_timestamp}.json")
        try:
            with open(delta_raw_report_filename, "w", encoding="utf-8") as f:
                json.dump(delta_raw_report_data, f, indent=2)
            print(f"[✓] Raw delta holding report saved to PENDING: '{os.path.basename(delta_raw_report_filename)}'")
        except IOError as e:
            print(f"[✗] Error saving raw delta holding report: {e}")
    else:
        print("[*] Monitor: No changes detected for delta report.")

    # --- Console summary (Uses parameter, core logic unchanged) ---
    print("\n--- Holding Monitor Summary (Raw Correlated) ---")
    if raw_correlated_holdings_list:
        # Use parameter min_correlation_holdings in the print statement
        print(f"** Top Tokens (from Birdeye filter) held by ≥ {min_correlation_holdings} monitored wallets:**")
        for i, data in enumerate(raw_correlated_holdings_list[:10]): # Show top 10
            print(f"  • {data['token_mint']} (Holders: {data['total_holders_in_set']})")
        if len(raw_correlated_holdings_list) > 10:
            print(f"   ... and {len(raw_correlated_holdings_list) - 10} more.")
    else:
        print("No commonly-held tokens (matching Birdeye filter and correlation threshold) found.")
    print("--- Holding Monitor Cycle Complete ---")

    # Return the state needed for the *next* delta calculation
    return current_state_for_delta


# ============================================================================
# Main Execution Loop
# ============================================================================
if __name__ == "__main__":

    # --- Load INITIAL Parameters from Eco Mode Config ---
    print("[*] Loading INITIAL script parameters based on Eco Mode setting...")
    # Keep track of the initially loaded state
    initial_eco_on = is_eco_mode()
    current_active_eco_mode = initial_eco_on # Store the mode currently in use by the script
    print(f"[*] Initial Eco Mode detected as: {'ON' if initial_eco_on else 'OFF'}")
    params = load_current_params() # Load initial params

    if not params: # Check if initial loading failed (returned {} )
        print("[!] CRITICAL: Failed to load initial parameters from eco_mode_config. Using hardcoded defaults.")
        # Define default fallbacks here (matching 'eco_off' values from JSON)
        # (You MUST define ALL parameters here as fallbacks if loading fails)
        ANALYSIS_INTERVAL_SECONDS = 300
        HOLDING_MONITOR_INTERVAL_SECONDS = 300
        CACHE_CLEANUP_INTERVAL_SECONDS = 3600
        LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS = 86400
        MAX_SLEEP_SECONDS = 120
        BASE_MIN_VOLUME_USD = 15000
        BASE_MAX_LIQUIDITY_USD = 300000
        MONITOR_BF_MIN_VOLUME_USD = 5000
        MONITOR_BF_MAX_LIQUIDITY_USD = 500000
        MONITOR_BF_MIN_MC_USD = 2000
        MONITOR_BF_MAX_MC_USD = 2500000
        MONITOR_BF_LIMIT = 500
        MONITOR_BF_MAX_PAGES_TO_TRY = 15
        TOKENS_TO_ANALYZE = 20
        INITIAL_HELIUS_LIMIT = 25
        TOP_WALLETS_TO_KEEP = 20
        MAX_TRADES_FILTER_7D = 1000
        RECENT_HISTORY_LIMIT = 30
        SWAPS_TO_REPORT = 4
        MIN_CORRELATION_HOLDINGS = 2
        MIN_CORRELATION_SWAPS = 2
        EARLIEST_SIGS = 250
        LONE_WOLF_CHECK = False
        FETCH_GMGN = True # Default assumes GMGN available if config fails
    else:
        print(f"[✓] Initial parameters loaded. Eco Mode is currently {'ON' if initial_eco_on else 'OFF'}.")
        mode_key = "eco_on" if initial_eco_on else "eco_off"
        # --- Assign initial parameter values ---
        # Assign ALL parameters using .get() with a default fallback
        # Use the 'eco_off' values as reasonable defaults if a key is missing
        ANALYSIS_INTERVAL_SECONDS = params.get("ANALYSIS_INTERVAL_SECONDS", 300)
        HOLDING_MONITOR_INTERVAL_SECONDS = params.get("HOLDING_MONITOR_INTERVAL_SECONDS", 300)
        CACHE_CLEANUP_INTERVAL_SECONDS = params.get("CACHE_CLEANUP_INTERVAL_SECONDS", 3600)
        LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS = params.get("HISTORY_CLEANUP_INTERVAL_SECONDS", 86400)
        MAX_SLEEP_SECONDS = params.get("MAX_SLEEP_SECONDS", 120)
        BASE_MIN_VOLUME_USD = params.get("BASE_MIN_VOLUME_USD", 15000)
        BASE_MAX_LIQUIDITY_USD = params.get("BASE_MAX_LIQUIDITY_USD", 300000)
        MONITOR_BF_MIN_VOLUME_USD = params.get("MONITOR_BF_MIN_VOLUME_USD", 5000)
        MONITOR_BF_MAX_LIQUIDITY_USD = params.get("MONITOR_BF_MAX_LIQUIDITY_USD", 500000)
        MONITOR_BF_MIN_MC_USD = params.get("MONITOR_BF_MIN_MC_USD", 2000)
        MONITOR_BF_MAX_MC_USD = params.get("MONITOR_BF_MAX_MC_USD", 2500000)
        MONITOR_BF_LIMIT = params.get("MONITOR_BF_LIMIT", 500)
        MONITOR_BF_MAX_PAGES_TO_TRY = params.get("MONITOR_BF_MAX_PAGES_TO_TRY", 15)
        TOKENS_TO_ANALYZE = params.get("TOKENS_TO_ANALYZE", 20)
        INITIAL_HELIUS_LIMIT = params.get("INITIAL_HELIUS_LIMIT", 25)
        TOP_WALLETS_TO_KEEP = params.get("TOP_WALLETS_TO_KEEP", 20)
        MAX_TRADES_FILTER_7D = params.get("MAX_TRADES_FILTER_7D", 1000)
        RECENT_HISTORY_LIMIT = params.get("RECENT_HISTORY_LIMIT", 30)
        SWAPS_TO_REPORT = params.get("SWAPS_TO_REPORT", 4)
        MIN_CORRELATION_HOLDINGS = params.get("MIN_CORRELATION_HOLDINGS", 2)
        MIN_CORRELATION_SWAPS = params.get("MIN_CORRELATION_SWAPS", 2)
        EARLIEST_SIGS = params.get("EARLIEST_SIGS", 250)
        LONE_WOLF_CHECK = params.get("LONE_WOLF_CHECK", False)
        FETCH_GMGN = params.get("FETCH_GMGN", True)
    # --- End Parameter Loading ---

    # --- Function to Reload Parameters Dynamically ---
    def reload_parameters_if_needed():
        """Checks eco mode status and reloads parameters if it changed."""
        # Declare relevant variables as global to modify them
        global current_active_eco_mode, main_gmgn_analyzer
        global ANALYSIS_INTERVAL_SECONDS, HOLDING_MONITOR_INTERVAL_SECONDS, CACHE_CLEANUP_INTERVAL_SECONDS
        global LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS, MAX_SLEEP_SECONDS, BASE_MIN_VOLUME_USD
        global BASE_MAX_LIQUIDITY_USD, MONITOR_BF_MIN_VOLUME_USD, MONITOR_BF_MAX_LIQUIDITY_USD
        global MONITOR_BF_MIN_MC_USD, MONITOR_BF_MAX_MC_USD, MONITOR_BF_LIMIT, MONITOR_BF_MAX_PAGES_TO_TRY
        global TOKENS_TO_ANALYZE, INITIAL_HELIUS_LIMIT, TOP_WALLETS_TO_KEEP, MAX_TRADES_FILTER_7D
        global RECENT_HISTORY_LIMIT, SWAPS_TO_REPORT, MIN_CORRELATION_HOLDINGS, MIN_CORRELATION_SWAPS
        global EARLIEST_SIGS, LONE_WOLF_CHECK, FETCH_GMGN

        desired_eco_mode = is_eco_mode()

        if desired_eco_mode != current_active_eco_mode:
            mode_text = "ON" if desired_eco_mode else "OFF"
            print(f"\n{'='*15} ECO MODE CHANGE DETECTED! Attempting to switch to {mode_text} {'='*15}")
            new_params = load_current_params()

            if not new_params:
                print(f"[!] FAILED to reload parameters for Eco Mode {mode_text}. Keeping previous settings.")
                return # Exit the function, don't change anything

            print(f"[✓] Successfully reloaded parameters for Eco Mode {mode_text}.")
            old_fetch_gmgn = FETCH_GMGN # Store old value for comparison

            # --- Re-assign ALL parameter variables ---
            # Use .get() with defaults matching the initial loading defaults
            ANALYSIS_INTERVAL_SECONDS = new_params.get("ANALYSIS_INTERVAL_SECONDS", 300)
            HOLDING_MONITOR_INTERVAL_SECONDS = new_params.get("HOLDING_MONITOR_INTERVAL_SECONDS", 300)
            CACHE_CLEANUP_INTERVAL_SECONDS = new_params.get("CACHE_CLEANUP_INTERVAL_SECONDS", 3600)
            LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS = new_params.get("HISTORY_CLEANUP_INTERVAL_SECONDS", 86400)
            MAX_SLEEP_SECONDS = new_params.get("MAX_SLEEP_SECONDS", 120)
            BASE_MIN_VOLUME_USD = new_params.get("BASE_MIN_VOLUME_USD", 15000)
            BASE_MAX_LIQUIDITY_USD = new_params.get("BASE_MAX_LIQUIDITY_USD", 300000)
            MONITOR_BF_MIN_VOLUME_USD = new_params.get("MONITOR_BF_MIN_VOLUME_USD", 5000)
            MONITOR_BF_MAX_LIQUIDITY_USD = new_params.get("MONITOR_BF_MAX_LIQUIDITY_USD", 500000)
            MONITOR_BF_MIN_MC_USD = new_params.get("MONITOR_BF_MIN_MC_USD", 2000)
            MONITOR_BF_MAX_MC_USD = new_params.get("MONITOR_BF_MAX_MC_USD", 2500000)
            MONITOR_BF_LIMIT = new_params.get("MONITOR_BF_LIMIT", 500)
            MONITOR_BF_MAX_PAGES_TO_TRY = new_params.get("MONITOR_BF_MAX_PAGES_TO_TRY", 15)
            TOKENS_TO_ANALYZE = new_params.get("TOKENS_TO_ANALYZE", 20)
            INITIAL_HELIUS_LIMIT = new_params.get("INITIAL_HELIUS_LIMIT", 25)
            TOP_WALLETS_TO_KEEP = new_params.get("TOP_WALLETS_TO_KEEP", 20)
            MAX_TRADES_FILTER_7D = new_params.get("MAX_TRADES_FILTER_7D", 1000)
            RECENT_HISTORY_LIMIT = new_params.get("RECENT_HISTORY_LIMIT", 30)
            SWAPS_TO_REPORT = new_params.get("SWAPS_TO_REPORT", 4)
            MIN_CORRELATION_HOLDINGS = new_params.get("MIN_CORRELATION_HOLDINGS", 2)
            MIN_CORRELATION_SWAPS = new_params.get("MIN_CORRELATION_SWAPS", 2)
            EARLIEST_SIGS = new_params.get("EARLIEST_SIGS", 250)
            LONE_WOLF_CHECK = new_params.get("LONE_WOLF_CHECK", False)
            FETCH_GMGN = new_params.get("FETCH_GMGN", True)

            # --- Handle GMGN Analyzer State Change ---
            if FETCH_GMGN != old_fetch_gmgn:
                if FETCH_GMGN:
                    if gmgn: # Check if import was successful
                        print("[*] Eco Mode Change: Initializing GMGN analyzer instance...")
                        try:
                            main_gmgn_analyzer = gmgn() # Re-initialize
                            print("[✓] GMGN analyzer initialized.")
                        except Exception as e:
                            print(f"[✗] Failed GMGN initialization during mode change: {e}. Disabling GMGN.")
                            FETCH_GMGN = False # Force disable if init fails now
                            main_gmgn_analyzer = None
                    else:
                        print("[!] Eco Mode Change: Cannot enable GMGN as import failed previously.")
                        FETCH_GMGN = False # Ensure it stays false if import failed
                        main_gmgn_analyzer = None
                else:
                    print("[*] Eco Mode Change: Disabling GMGN analyzer instance.")
                    main_gmgn_analyzer = None # Set to None

            # --- Update the script's active mode status ---
            current_active_eco_mode = desired_eco_mode
            print(f"--- Parameter Example Check ---")
            print(f"    ANALYSIS_INTERVAL_SECONDS changed to: {ANALYSIS_INTERVAL_SECONDS}")
            print(f"    TOKENS_TO_ANALYZE changed to: {TOKENS_TO_ANALYZE}")
            print(f"    FETCH_GMGN changed to: {FETCH_GMGN}")
            print(f"{'='*50}\n")
        # else:
            # print("[.] Eco mode unchanged since last check.") # Optional: uncomment for verbose logging


    # --- Initialization ---
    # Check FETCH_GMGN parameter *after* initial loading
    main_gmgn_analyzer = None # Initialize as None
    if FETCH_GMGN:
        if not gmgn: # Check if import failed
            print("[!] WARNING: Initial FETCH_GMGN is True but gmgn_api import failed. Disabling GMGN.")
            FETCH_GMGN = False # Correct the flag if import failed
        else:
            print("="*60+"\n[!] WARNING: GMGN Fetching is ENABLED via parameter.\n"
                  "    This uses an unofficial API. Use responsibly.\n"+"="*60)
            time.sleep(1)
            print("[*] Initializing GMGN analyzer instance...")
            try:
                main_gmgn_analyzer = gmgn() # Initial instance
                print("[✓] GMGN analyzer initialized.")
            except Exception as e:
                print(f"[✗] Failed initial GMGN initialization: {e}. Disabling GMGN.")
                FETCH_GMGN = False # Disable if init fails
                main_gmgn_analyzer = None


    print("[*] Loading analysis history state...")
    analysis_history_state = load_analysis_history()
    print(f"[✓] Loaded analysis_history for {len(analysis_history_state)} tokens.")

    print("[*] Loading wallet store...")
    wallet_store = load_wallet_store() # wallet_store is global
    print(f"[✓] Wallet store loaded with {len(wallet_store)} wallets (Max capacity: {MAX_WALLETS_KEPT}).")

    print("[*] Loading holding monitor state for delta calculation...")
    current_holdings_state_for_delta = load_holding_monitor_state()
    print(f"[✓] Loaded previous holding state for {len(current_holdings_state_for_delta)} wallets.")

    # Load last analysis snapshot if available (for context, not direct use in loop)
    last_analysis_results = {}
    snapshot_filename = "hybrid_snapshot_output.json"
    if os.path.exists(snapshot_filename):
        try:
            with open(snapshot_filename, 'r', encoding="utf-8") as f:
                last_analysis_results = json.load(f)
            print(f"[✓] Loaded previous analysis snapshot from '{snapshot_filename}'.")
        except Exception as e:
            print(f"[!] Failed to load previous snapshot '{snapshot_filename}': {e}")

    # GMGN Analyzer instance is now handled above based on FETCH_GMGN flag

    # Track last run times for scheduling
    last_cache_cleanup_time = 0
    last_holding_check_time = 0
    last_analysis_run_time = 0
    last_history_cleanup_time = 0
    last_wallet_cleanup_time = 0

    # --- Print Initial Configuration ---
    print("\n[*] Initializing Main Loop with configuration:")
    print(f"    - Initial Eco Mode Active: {current_active_eco_mode}") # Show the mode we started with
    print(f"    - Analysis Interval: {ANALYSIS_INTERVAL_SECONDS}s")
    print(f"    - Monitor Interval: {HOLDING_MONITOR_INTERVAL_SECONDS}s")
    # ... (print other initial parameters as before) ...
    print(f"    - Fetch GMGN Enabled: {FETCH_GMGN}")
    print("-" * 40)

    print("\nStarting Main Analyzer Loop (Press Ctrl+C to stop)...")
    # --- End Initialization ---

    # --- Main Loop ---
    try:
        while True:
            # <<< --- CALL THE RELOAD FUNCTION AT THE START OF EACH LOOP --- >>>
            reload_parameters_if_needed()
            # <<< --------------------------------------------------------- >>>

            current_time_loop_start = time.time()

            # --- Calculate next run times using potentially updated parameters ---
            # These calculations now use the global variables which might have been updated
            next_analysis_time = last_analysis_run_time + ANALYSIS_INTERVAL_SECONDS
            next_monitor_time = last_holding_check_time + HOLDING_MONITOR_INTERVAL_SECONDS
            next_cache_cleanup_time = last_cache_cleanup_time + CACHE_CLEANUP_INTERVAL_SECONDS
            next_history_cleanup_time = last_history_cleanup_time + LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS
            next_wallet_cleanup_time = last_wallet_cleanup_time + INACTIVITY_WINDOW_SECONDS

            # --- Periodic Tasks ---
            # (No changes needed here, they use the updated global interval variables)
            # Cache Cleanup
            if cache_manager and current_time_loop_start >= next_cache_cleanup_time:
                print("\n" + "~"*20 + " Running Periodic API Cache Cleanup " + "~"*20)
                cache_manager.cleanup_cache()
                last_cache_cleanup_time = time.time()
                print("~"*20 + " API Cache Cleanup Complete " + "~"*20)

            # Analysis History Cleanup
            if current_time_loop_start >= next_history_cleanup_time:
                 print("\n" + "~"*20 + " Running Analysis History Cleanup " + "~"*20)
                 analysis_history_state = cleanup_analysis_history(analysis_history_state, HISTORY_CLEANUP_AGE_SECONDS)
                 save_analysis_history(analysis_history_state)
                 last_history_cleanup_time = time.time()
                 print("~"*20 + " Analysis History Cleanup Complete " + "~"*20)

            # Wallet Store Cleanup
            if current_time_loop_start >= next_wallet_cleanup_time:
                 print("\n" + "~"*20 + " Running Wallet Store Cleanup " + "~"*20)
                 stale = [w for w, meta in wallet_store.items()
                          if current_time_loop_start - meta.get("last_seen_ts", 0) > INACTIVITY_WINDOW_SECONDS]
                 if stale:
                     print(f"    Found {len(stale)} inactive wallets to prune.")
                     log_and_alert_wallet_purge(
                         "inactivity-prune",
                         [{"wallet": w, "profit": wallet_store[w].get("latest_profit", 0)} for w in stale]
                     )
                     for w_addr in stale: wallet_store.pop(w_addr, None)
                     print(f"    Pruned {len(stale)} inactive wallets. Current size: {len(wallet_store)}")
                     save_wallet_store(wallet_store)
                 else: print("    No inactive wallets found.")
                 last_wallet_cleanup_time = time.time()
                 print("~"*20 + " Wallet Store Cleanup Complete " + "~"*20)
            # --- End Periodic Tasks ---


            # --- Analysis Phase ---
            # (No changes needed here, it uses the updated global parameter variables passed to the function)
            if current_time_loop_start >= next_analysis_time:
                print("\n" + "*"*20 + f" Triggering Analysis Phase (Interval: {ANALYSIS_INTERVAL_SECONDS}s) " + "*"*20)
                # The variables passed here (like TOKENS_TO_ANALYZE) are the potentially updated global ones
                analysis_output = run_analysis_phase(
                    main_gmgn_analyzer, analysis_history_state,
                    BASE_MIN_VOLUME_USD, BASE_MAX_LIQUIDITY_USD,
                    TOKENS_TO_ANALYZE, INITIAL_HELIUS_LIMIT, TOP_WALLETS_TO_KEEP,
                    MAX_TRADES_FILTER_7D, RECENT_HISTORY_LIMIT, SWAPS_TO_REPORT,
                    MIN_CORRELATION_SWAPS, EARLIEST_SIGS, LONE_WOLF_CHECK,
                    FETCH_GMGN, MONITOR_BF_MAX_MC_USD
                )
                if analysis_output: last_analysis_results = analysis_output
                last_analysis_run_time = time.time()
                print("*"*20 + " Analysis Phase Finished " + "*"*20)
            # --- End Analysis Phase ---


            # --- Monitor Phase ---
            # (No changes needed here, it uses the updated global parameter variables passed to the function)
            if current_time_loop_start >= next_monitor_time:
                print("\n" + "*"*20 + f" Triggering Monitor Phase (Interval: {HOLDING_MONITOR_INTERVAL_SECONDS}s) " + "*"*20)
                wallets_to_monitor_live = set(wallet_store.keys())
                if wallets_to_monitor_live:
                    print(f"[*] Monitor Phase: Preparing to monitor {len(wallets_to_monitor_live)} wallets.")
                    print("[*] Monitor Phase: Fetching Birdeye filter list for holdings...")
                    # Uses potentially updated MONITOR_BF_* variables
                    bf_tokens_list = get_candidate_tokens(
                        MONITOR_BF_LIMIT, MONITOR_BF_MIN_VOLUME_USD, MONITOR_BF_MAX_LIQUIDITY_USD,
                        MONITOR_BF_MIN_MC_USD, MONITOR_BF_MAX_MC_USD, MONITOR_BF_MAX_PAGES_TO_TRY
                    )
                    bf_mints_set = {t.get('address') for t in bf_tokens_list if t.get('address')}
                    print(f"[✓] Monitor Phase: Using {len(bf_mints_set)} tokens for Birdeye holding filter.")
                    if bf_mints_set: print(f"    Sample bf_mints: {list(bf_mints_set)[:3]}...")

                    # Passes potentially updated MIN_CORRELATION_HOLDINGS
                    new_holdings_state = run_holding_monitor_cycle(
                        wallets_to_track=wallets_to_monitor_live,
                        birdeye_filter_mints=bf_mints_set,
                        min_correlation_holdings=MIN_CORRELATION_HOLDINGS,
                        previous_holdings_state=current_holdings_state_for_delta,
                        current_wallet_store=wallet_store
                    )
                    current_holdings_state_for_delta = new_holdings_state
                    save_holding_monitor_state(current_holdings_state_for_delta)
                else:
                    print("\n[*] Monitor Phase: No wallets currently in wallet_store to monitor.")
                last_holding_check_time = time.time()
                print("*"*20 + " Monitor Phase Finished " + "*"*20)
            # --- End Monitor Phase ---


            # --- Sleep Logic ---
            # (No changes needed here, it uses the updated global interval/sleep variables)
            time_before_sleep_calc = time.time()
            effective_next_times = []
            tasks_schedule = [
                ("Analysis", last_analysis_run_time, ANALYSIS_INTERVAL_SECONDS),
                ("Monitor", last_holding_check_time, HOLDING_MONITOR_INTERVAL_SECONDS),
                ("CacheClean", last_cache_cleanup_time if cache_manager else 0, CACHE_CLEANUP_INTERVAL_SECONDS if cache_manager else float('inf')),
                ("HistoryClean", last_history_cleanup_time, LOOP_HISTORY_CLEANUP_INTERVAL_SECONDS),
                ("WalletPrune", last_wallet_cleanup_time, INACTIVITY_WINDOW_SECONDS)
            ]

            next_task_name = "MaxSleep"
            min_next_time = time_before_sleep_calc + MAX_SLEEP_SECONDS

            print("\n--- Calculating Sleep Time ---")
            for name, last_run, interval in tasks_schedule:
                if interval == float('inf'): continue
                next_run_scheduled = last_run + interval
                print(f"    Task '{name}': Last run @ {format_timestamp(int(last_run)) if last_run > 0 else 'Never'}, Interval={interval}s, Next run due @ {format_timestamp(int(next_run_scheduled))}")
                if next_run_scheduled > time_before_sleep_calc:
                    if next_run_scheduled < min_next_time:
                        min_next_time = next_run_scheduled
                        next_task_name = name
                else:
                    min_next_time = time_before_sleep_calc + 1
                    next_task_name = name
                    print(f"    Task '{name}' is overdue or due now.")
                    # break # Optional: Uncomment if you want to always prioritize the first overdue task

            sleep_duration = max(0, min_next_time - time_before_sleep_calc)
            sleep_duration = min(max(1, sleep_duration), MAX_SLEEP_SECONDS) # Ensure 1 <= sleep <= MAX_SLEEP

            print(f"--- Next task due: '{next_task_name}'")
            print(f"--- Main Loop: Sleeping for {sleep_duration:.1f} seconds (max {MAX_SLEEP_SECONDS}s)... ---")
            time.sleep(sleep_duration)
            # --- End Sleep Logic ---

    except KeyboardInterrupt:
        print("\n[!] Analyzer loop stopped by user (KeyboardInterrupt).")
    except Exception as e:
        print(f"\n[!!!] UNHANDLED EXCEPTION IN MAIN LOOP: {e}")
        traceback.print_exc()
        print("[!] Waiting 60 seconds before potentially restarting or exiting...")
        time.sleep(60)
    finally:
        # Attempt to save state on exit or error
        print("[*] Attempting to save final holding monitor state...")
        if 'current_holdings_state_for_delta' in locals():
            save_holding_monitor_state(current_holdings_state_for_delta)
            print("[✓] Holding monitor state saved.")
        else:
            print("[!] Holding monitor state variable not found, could not save.")
        print("[*] Exiting main_loop_analyzer.")

# --- END OF FILE main_loop_analyzer.py ---