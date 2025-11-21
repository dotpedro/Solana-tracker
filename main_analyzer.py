# --- START OF FILE main_analyzer.py ---

import os
import time
import json
import random # Added for dynamic parameters
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Set

# --- Import functions and constants from your first script ---
# --- It MUST be named 'helius_analyzer.py' for this import to work ---
try:
    from helius_analyzer import (
        get_candidate_tokens,
        find_early_receivers_detailed,
        format_timestamp,
        _fetch_signatures_page, # Needed for recent history fetch
        parse_transactions_helius_v0, # Needed for recent history fetch
        # Import constants if needed, or redefine below
        # We need API keys if helius_analyzer functions don't load them internally
        BIRDEYE_API_KEY as HELIUS_ANALYZER_BIRDEYE_KEY, # Rename to avoid potential clashes
        HELIUS_API_KEY as HELIUS_ANALYZER_HELIUS_KEY
    )
    print("[✓] Successfully imported from helius_analyzer.py")
except ImportError as e:
    print(f"ERROR: Could not import from 'helius_analyzer.py': {e}")
    print("       Make sure your original script file is RENAMED to 'helius_analyzer.py' and is in the same directory.")
    exit()
# --- End Helius Import ---

# --- Import the gmgn class ---
try:
    from gmgn_api import gmgn
    print("[✓] Successfully imported gmgn class from gmgn_api.py")
except ImportError as e:
    print(f"ERROR: Could not import from 'gmgn_api.py': {e}")
    print("       Make sure 'gmgn_api.py' exists in the same directory.")
    exit()
# --- End gmgn Import ---

load_dotenv()
print("[*] Loaded environment variables for main_analyzer.")
# Re-check keys used by this script directly if any
# Example: If Birdeye key was needed for price history later
# BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
# if not BIRDEYE_API_KEY: raise ValueError("BIRDEYE_API_KEY needed but not found")


# --- ADD Swap Extraction Logic ---
def extract_swaps_from_tx(tx_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parses a Helius transaction object and extracts swap details."""
    swaps = []
    tx_type = tx_details.get("type"); source = tx_details.get("source")
    timestamp = tx_details.get("timestamp"); signature = tx_details.get("signature")
    if tx_type == "SWAP" or source in ["JUPITER_V6", "RAYDIUM", "ORCA", "METEORA", "PUMP"]:
        events = tx_details.get("events", {}); swap_event = events.get("swap")
        if swap_event:
            token_in_list = swap_event.get("tokenInputs", [])
            token_out_list = swap_event.get("tokenOutputs", [])
            token_in = token_in_list[0] if token_in_list else {}
            token_out = token_out_list[0] if token_out_list else {}
            # Best effort to find the user wallet initiating the swap
            user_wallet = tx_details.get("feePayer") or swap_event.get("taker") or swap_event.get("maker")
            if token_in.get("mint") and token_out.get("mint") and user_wallet:
                 swaps.append({
                     "wallet": user_wallet, "signature": signature, "timestamp": timestamp,
                     "datetime_utc": format_timestamp(timestamp), # Use imported formatter
                     "dex_source": source,
                     "token_in_mint": token_in.get("mint"),
                     "token_in_amount": token_in.get("amount"),
                     "token_out_mint": token_out.get("mint"),
                     "token_out_amount": token_out.get("amount")
                 })
    return swaps
# --- END Swap Extraction ---

# ============================================================================
# Dynamic Parameter Adjustment Function
# ============================================================================
def adjust_search_parameters(base_min_vol, base_max_liq):
    """Adjusts search parameters randomly within defined bounds."""
    # Adjust Min Volume (+/- 30% around base, ensure minimum of 1000)
    min_vol_adj = base_min_vol * random.uniform(0.7, 1.3)
    new_min_vol = max(1000, int(min_vol_adj)) # Ensure it doesn't go too low

    # Adjust Max Liquidity (+/- 50% around base, ensure minimum of 10000)
    max_liq_adj = base_max_liq * random.uniform(0.5, 1.5)
    new_max_liq = max(10000, int(max_liq_adj)) # Ensure it doesn't go too low

    print(f"    [*] Dynamic Params: New Min Volume=${new_min_vol:,.0f}, New Max Liquidity=${new_max_liq:,.0f}")
    return new_min_vol, new_max_liq

# ============================================================================
# MAIN Orchestration Function
# ============================================================================
def main(max_tokens_to_process: int = 3,
         # --- Base parameters for dynamic adjustment ---
         base_min_volume: int = 20000,
         base_max_liquidity: int = 150000,
         # --- Other parameters ---
         initial_helius_receiver_limit: int = 50,
         top_profitable_wallets_to_keep: int = 5,
         max_trades_filter: int = 500, # GMGN 7d buy+sell trades
         recent_history_limit: int = 75, # Helius TXs for recent activity
         swaps_to_report_per_wallet: int = 5, # Latest swaps to keep
         min_buyers_for_correlation: int = 2, # For recent swaps correlation
         earliest_sigs_to_analyze: int = 1000, # Helius earliest sigs for token
         perform_lone_wolf_check: bool = True, # Helius lone wolf check
         fetch_gmgn_data: bool = True): # Master GMGN switch

    print("-" * 60)
    print(f"Starting Hybrid Token Analysis (Recent Activity Snapshot):")
    print(f"  - Base Min Volume: ${base_min_volume:,.0f}, Base Max Liquidity: ${base_max_liquidity:,.0f}")
    print(f"  - Max Tokens Per Run: {max_tokens_to_process}, Initial Helius Receivers: {initial_helius_receiver_limit}")
    print(f"  - Top Wallets To Keep: {top_profitable_wallets_to_keep}, Max Trades Filter: {max_trades_filter}")
    print(f"  - Recent History Limit: {recent_history_limit}, Swaps To Report: {swaps_to_report_per_wallet}")
    print(f"  - Min Correlation: {min_buyers_for_correlation}")
    print(f"  - Helius Earliest Sigs: {earliest_sigs_to_analyze}, Lone Wolf Check: {perform_lone_wolf_check}")
    print(f"  - Fetch GMGN Data: {fetch_gmgn_data}")
    print("-" * 60)

    # --- Initialize GMGN Analyzer ---
    gmgn_analyzer = None
    if fetch_gmgn_data:
        print("[*] Initializing GMGN analyzer...")
        try: gmgn_analyzer = gmgn(); print("[✓] GMGN analyzer initialized.")
        except Exception as e: print(f"[✗] Failed GMGN init: {e}. GMGN disabled."); fetch_gmgn_data = False

    # --- Dynamic Parameter Adjustment ---
    current_min_volume, current_max_liquidity = adjust_search_parameters(base_min_volume, base_max_liquidity)

    # 1. Get Candidate Tokens (using adjusted parameters)
    print("[*] Stage 1: Fetching candidate tokens (Birdeye via helius_analyzer)...")
    # Calls the function imported from helius_analyzer.py
    candidate_tokens = get_candidate_tokens(
        limit=max_tokens_to_process * 4,
        min_volume=current_min_volume,   # Use adjusted value
        max_liquidity=current_max_liquidity # Use adjusted value
    )
    if not candidate_tokens: print("\n[!] No suitable tokens found for current parameters. Exiting run."); return

    tokens_to_process = candidate_tokens[:max_tokens_to_process]
    print(f"\n[✓] Stage 1 Complete: Processing Top {len(tokens_to_process)} tokens found.")

    final_output = {}
    processed_count = 0

    # --- Main Loop Processing Each Token ---
    for token_info in tokens_to_process:
        processed_count += 1
        symbol = token_info.get("symbol", "N/A"); mint = token_info.get("address")
        volume = float(token_info.get("v24hUSD", 0)); liquidity = float(token_info.get("liquidity", 0))
        print(f"\n({processed_count}/{len(tokens_to_process)}) Processing Token: {symbol} ({mint})")
        print(f"    Data -> Volume: ${volume:,.2f}, Liquidity: ${liquidity:,.2f}")
        if not mint: print("    [✗] Skipping token - missing mint."); continue

        # 2. Get Early Receivers via Helius
        print("[*] Stage 2: Fetching Helius early receiver events (via helius_analyzer)...")
        # Calls the function imported from helius_analyzer.py
        helius_receiver_data = find_early_receivers_detailed(
            mint,
            limit=initial_helius_receiver_limit,
            target_sig_count=earliest_sigs_to_analyze,
            check_lone_wolf=perform_lone_wolf_check
        )
        print(f"    [✓] Stage 2 Complete: Found {len(helius_receiver_data)} Helius early receiver events.")

        # 3. Fetch and Filter GMGN Data (if enabled)
        gmgn_security_info = None; gmgn_all_fetched_profiles = {}; top_profitable_filtered_wallets = []
        if fetch_gmgn_data and gmgn_analyzer:
            print("[*] Stage 3: Fetching & Filtering GMGN Data (via gmgn_api)...")
            # Fetch Security Info
            print(f"        Fetching GMGN security info..."); security_info = gmgn_analyzer.getSecurityInfo(mint)
            if security_info: gmgn_security_info = security_info; print(f"        [✓] Security fetched.")
            else: print(f"        [!] Failed security fetch.")
            time.sleep(0.8)
            # Fetch Profiles
            unique_helius_wallets = {e['wallet'] for e in helius_receiver_data}
            wallets_to_query = list(unique_helius_wallets)[:initial_helius_receiver_limit]
            if wallets_to_query:
                print(f"        Fetching {len(wallets_to_query)} GMGN profiles...")
                for idx, wallet_addr in enumerate(wallets_to_query):
                    # print(f"          ({idx+1}/{len(wallets_to_query)}) Fetching profile for {wallet_addr[:6]}...") # Verbose
                    profile = gmgn_analyzer.getWalletInfo(walletAddress=wallet_addr, period="7d")
                    gmgn_all_fetched_profiles[wallet_addr] = profile if profile else {"error":"fetch failed"}
                    time.sleep(1.0) # Sleep between GMGN calls
                print(f"        [✓] Profile fetching complete.")
                # Filter & Rank
                print(f"        Filtering & ranking profiles...")
                valid_profiles = []
                for wallet, p_data in gmgn_all_fetched_profiles.items():
                    if p_data and not p_data.get("error"):
                        try: # Add try-except for robust float conversion
                            buy_7d = float(p_data.get('buy_7d', 0) or 0)
                            sell_7d = float(p_data.get('sell_7d', 0) or 0)
                            profit = float(p_data.get('realized_profit_7d') or p_data.get('pnl_7d') or -float('inf'))
                        except (ValueError, TypeError):
                            print(f"            [!] Warning: Invalid numeric data in GMGN profile for {wallet[:6]}")
                            continue # Skip if data isn't numeric
                        if (buy_7d + sell_7d) > max_trades_filter: print(f"            [Filter] Skipping {wallet[:6]} (Bot Trades={(buy_7d + sell_7d)} > {max_trades_filter})"); continue
                        valid_profiles.append({"wallet": wallet, "profit": profit, "profile": p_data})
                valid_profiles.sort(key=lambda x: x['profit'], reverse=True)
                top_profitable_filtered_wallets = valid_profiles[:top_profitable_wallets_to_keep]
                print(f"        [✓] Selected Top {len(top_profitable_filtered_wallets)} wallets.")
            else: print("        [*] No Helius wallets to query GMGN for.")
            print(f"    [✓] Stage 3 Complete.")
        else: print("    [*] Skipping Stage 3: GMGN fetching disabled or failed.")

        # 4. Fetch Recent Activity for Top Wallets
        print("[*] Stage 4: Fetching recent swap history for top wallets (Helius)...")
        top_wallets_recent_swaps: Dict[str, List[Dict]] = {}
        top_wallet_list = [item['wallet'] for item in top_profitable_filtered_wallets] # Get addresses from filtered list
        if not top_wallet_list: print("    [*] No top wallets selected.")
        else:
            wallet_count = 0
            for wallet_addr in top_wallet_list:
                wallet_count += 1
                print(f"        ({wallet_count}/{len(top_wallet_list)}) Fetching recent TXs for {wallet_addr[:6]}... (limit {recent_history_limit})")
                # Use _fetch_signatures_page directly (imported from helius_analyzer)
                latest_sigs_data = _fetch_signatures_page(wallet_addr, limit=recent_history_limit)
                wallet_swaps = []
                if latest_sigs_data and isinstance(latest_sigs_data, list):
                    signatures = [item['signature'] for item in latest_sigs_data if item.get('signature')]
                    if signatures:
                        # Use parse_transactions_helius_v0 (imported from helius_analyzer)
                        recent_txs = parse_transactions_helius_v0(signatures)
                        for tx in recent_txs:
                            # Use extract_swaps_from_tx defined in *this* file
                            swaps = extract_swaps_from_tx(tx)
                            # Filter for swaps initiated by the specific wallet we are checking
                            wallet_swaps.extend([s for s in swaps if s.get("wallet") == wallet_addr])
                # Sort all found swaps for this wallet by time, descending
                wallet_swaps.sort(key=lambda x: x['timestamp'], reverse=True)
                # Store only the latest N swaps specified by parameter
                top_wallets_recent_swaps[wallet_addr] = wallet_swaps[:swaps_to_report_per_wallet]
                print(f"            [✓] Found {len(wallet_swaps)} total recent swaps, reporting latest {len(top_wallets_recent_swaps.get(wallet_addr,[]))}.")
                time.sleep(1.0) # Sleep between wallets
        print(f"    [✓] Stage 4 Complete.")

        # 5. Correlate Recent Swaps
        print("[*] Stage 5: Correlating recent swaps...")
        recent_buyers: Dict[str, Set[str]] = {}
        all_recent_swaps_flat: List[Dict] = [s for swaps in top_wallets_recent_swaps.values() for s in swaps]
        for swap in all_recent_swaps_flat:
            token = swap.get("token_out_mint"); w = swap.get("wallet")
            if token and w: recent_buyers.setdefault(token, set()).add(w)

        correlated_buys = []
        for mint_addr, buyers in recent_buyers.items():
            if len(buyers) >= min_buyers_for_correlation:
                 trades = [s for s in all_recent_swaps_flat if s.get("token_out_mint")==mint_addr and s.get("wallet") in buyers]
                 correlated_buys.append({"token_mint": mint_addr, "total_wallets_bought": len(buyers), "buying_wallets": sorted(list(buyers)), "trade_details": sorted(trades, key=lambda x: x['timestamp'])})
        print(f"    [✓] Stage 5 Complete: Found {len(correlated_buys)} recently correlated buys.")

        # 6. Combine final output
        final_output[symbol] = {
            "mint": mint, "liquidity_usd": liquidity, "mc": token_info.get("mc"),
            "volume_24h_usd": volume, "helius_early_receiver_events": helius_receiver_data,
            "gmgn_security_info": gmgn_security_info if gmgn_security_info is not None else {},
            "top_profitable_wallets": top_wallet_list, # Use the actual list of top wallets
            "top_profitable_wallet_profiles": {w['wallet']: w['profile'] for w in top_profitable_filtered_wallets},
            "top_wallets_recent_swaps": top_wallets_recent_swaps,
            "recent_correlated_buys": correlated_buys
        }

        # Adjust sleep
        base_sleep = 1.8;
        if fetch_gmgn_data: base_sleep += 1.0
        if perform_lone_wolf_check: base_sleep += 0.5
        print(f"    [*] Sleeping for {base_sleep:.1f}s...")
        time.sleep(base_sleep)
    # --- End Token Loop ---

    # 7. Save results
    output_filename = "hybrid_snapshot_output.json"
    try:
        with open(output_filename, "w") as f: json.dump(final_output, f, indent=2)
        print(f"\n✅ Done. Saved detailed snapshot results for {len(final_output)} tokens to {output_filename}")
    except IOError as e: print(f"\n[✗] Error saving results: {e}")

if __name__ == "__main__":
    # --- Parameters to Tune ---
    BASE_MIN_VOLUME_USD = 20000
    BASE_MAX_LIQUIDITY_USD = 150000
    TOKENS_TO_ANALYZE = 3
    INITIAL_HELIUS_LIMIT = 50
    TOP_WALLETS_TO_KEEP = 5
    MAX_TRADES_FILTER_7D = 500
    RECENT_HISTORY_LIMIT = 75
    SWAPS_TO_REPORT = 5
    MIN_CORRELATION = 2
    EARLIEST_SIGS = 1000
    LONE_WOLF_CHECK = True
    FETCH_GMGN = True
    # --- End Parameters ---

    if FETCH_GMGN: print("="*60+"\nWARNING: GMGN Fetching is ENABLED (Unofficial API).\n"+"="*60); time.sleep(2)

    # Call the main function in *this* file
    main(
        max_tokens_to_process=TOKENS_TO_ANALYZE,
        base_min_volume=BASE_MIN_VOLUME_USD,       # Pass base values
        base_max_liquidity=BASE_MAX_LIQUIDITY_USD, # Pass base values
        initial_helius_receiver_limit=INITIAL_HELIUS_LIMIT,
        top_profitable_wallets_to_keep=TOP_WALLETS_TO_KEEP,
        max_trades_filter=MAX_TRADES_FILTER_7D,
        recent_history_limit=RECENT_HISTORY_LIMIT,
        swaps_to_report_per_wallet=SWAPS_TO_REPORT,
        min_buyers_for_correlation=MIN_CORRELATION,
        earliest_sigs_to_analyze=EARLIEST_SIGS,
        perform_lone_wolf_check=LONE_WOLF_CHECK,
        fetch_gmgn_data=FETCH_GMGN
    )

# --- END OF FILE main_analyzer.py ---