import asyncio
import time
from typing import List, Dict, Optional, Any, TypedDict
from datetime import datetime, timezone
import logging

# Assuming birdeye_api is in the same directory or Python path
import birdeye_api
# For Markdown escaping, useful for constructing the report
import telegram_alert_router # Assuming this is accessible for escape_markdown_v2

# Basic logging setup
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Configuration Constants ---
DEFAULT_CONFIG = {
    "MIN_WALLETS_PER_BUNDLE": 3,
    "TIME_WINDOW_SEC": 60,  # Max time span for a cluster of buys
    "ENABLE_FUNDER_TRACE": True,
    "MAX_TRADES_TO_FETCH_FOR_BUNDLES": 200, # For get_token_trades (Birdeye limit is often 250)
    "MAX_TXS_TO_FETCH_FOR_FUNDING": 10,    # For birdeye_api.trader_txs per wallet
    "MIN_SOL_FOR_FUNDING_TX": 0.01,        # Minimum SOL amount to consider as a funding transaction
    "MAX_DAYS_FOR_FUNDING_TRACE": 7,       # How far back to look for funding txs
}

# Solana Mint Addresses
NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"
# Add other common quote mints if needed, e.g., USDC, USDT

# --- Data Structures ---
class ParsedBuy(TypedDict):
    tx_hash: str
    timestamp: int
    buyer_address: str
    sol_spent: float
    token_received: float
    token_mint: str

class Bundle(TypedDict):
    start_time: int
    end_time: int
    wallets: List[str]  # Unique buyer wallets
    buy_transactions: List[ParsedBuy]
    total_sol_spent: float
    total_token_received: float
    avg_sol_spent: float
    funder_summary: Optional[Dict[str, int]]  # funder_wallet -> count_of_funded_wallets_in_bundle

# --- Helper Functions ---

def _format_timestamp_pretty(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M:%S UTC')

def _escape_md(text: Any) -> str:
    return telegram_alert_router.escape_markdown_v2(str(text))

async def _parse_birdeye_tx_to_buy(tx_data: Dict, target_mint: str) -> Optional[ParsedBuy]:
    """
    Parses a raw transaction from Birdeye's /public/txs/token endpoint
    to extract buy information for the target_mint.
    Focuses on Jupiter swap events if available.
    """
    try:
        tx_hash = tx_data.get("tx_hash")
        timestamp = tx_data.get("block_timestamp")
        events = tx_data.get("events")

        if not (tx_hash and timestamp):
            return None

        if events and isinstance(events, list):
            for event in events:
                if isinstance(event, dict) and event.get("type") == "jupiterSwapEvent":
                    event_data = event.get("data", {})
                    input_mint = event_data.get("inputMint")
                    output_mint = event_data.get("outputMint")
                    user_wallet = event_data.get("user") # This is the buyer

                    if user_wallet and output_mint == target_mint and input_mint == NATIVE_SOL_MINT:
                        sol_spent_lamports = event_data.get("inputAmount", 0)
                        token_received_units = event_data.get("outputAmount", 0)
                        
                        # Assuming SOL has 9 decimals and token might have different decimals
                        # For simplicity, we'll rely on token_overview for decimals later if needed for display,
                        # but for calculation, raw amounts are fine if compared consistently.
                        # Here, we convert SOL to float. Token amount is kept as is.
                        sol_spent = sol_spent_lamports / (10**9)
                        
                        return ParsedBuy(
                            tx_hash=tx_hash,
                            timestamp=timestamp,
                            buyer_address=user_wallet,
                            sol_spent=sol_spent,
                            token_received=float(token_received_units), # Convert to float
                            token_mint=target_mint
                        )
        
        # Fallback: If no Jupiter event, try to parse instructions and token balance changes
        # This is more complex and less reliable; for now, we focus on Jupiter events.
        # If you need broader DEX support, this section would need significant expansion.
        # logger.debug(f"No Jupiter swap event for {target_mint} found in tx {tx_hash}. Implement fallback if needed.")

    except Exception as e:
        logger.error(f"Error parsing Birdeye tx {tx_data.get('tx_hash', 'N/A')} for buys: {e}", exc_info=False) # exc_info=False for brevity
    return None


async def _fetch_and_parse_buys(token_mint: str, config: Dict) -> List[ParsedBuy]:
    """
    Fetches recent transactions in pages and parses them into a list of ParsedBuy objects.
    Handles Birdeye's limit of 50 trades per request for the /defi/txs/token endpoint.
    """
    logger.info(f"Fetching up to {config['MAX_TRADES_TO_FETCH_FOR_BUNDLES']} trades for {token_mint} (in pages)...")
    
    all_parsed_buys: List[ParsedBuy] = []
    current_offset = 0
    trades_per_page = 50 # Birdeye's max limit for this endpoint
    total_trades_to_fetch = config["MAX_TRADES_TO_FETCH_FOR_BUNDLES"]
    pages_fetched = 0
    max_pages = (total_trades_to_fetch + trades_per_page - 1) // trades_per_page # Calculate max pages needed

    while len(all_parsed_buys) < total_trades_to_fetch and pages_fetched < max_pages:
        logger.info(f"Fetching page {pages_fetched + 1}/{max_pages}, offset {current_offset}, limit {trades_per_page} for {token_mint}")
        
        raw_txs_data_page = await asyncio.to_thread(
            birdeye_api.get_token_trades,
            mint_address=token_mint,
            limit=trades_per_page,
            offset=current_offset,
            # sort_type="desc" # Default in birdeye_api is 'desc' (newest first)
                              # If you need oldest first directly, change to "asc" and adjust logic,
                              # or sort at the end like currently done.
        )

        if not raw_txs_data_page or not isinstance(raw_txs_data_page, dict) or "items" not in raw_txs_data_page:
            logger.warning(f"No transaction data or unexpected format on page {pages_fetched + 1} for {token_mint}. Stopping pagination.")
            if raw_txs_data_page: logger.debug(f"Raw data received on page: {str(raw_txs_data_page)[:500]}")
            break # Stop if a page fails or has no items

        raw_tx_list_page = raw_txs_data_page.get("items", [])
        if not isinstance(raw_tx_list_page, list):
            logger.warning(f"Expected a list of transactions in 'items' on page {pages_fetched + 1}, got {type(raw_tx_list_page)}. Stopping pagination.")
            break
        
        if not raw_tx_list_page: # No more items returned by the API
            logger.info(f"No more trades found on page {pages_fetched + 1} for {token_mint}. End of results.")
            break

        page_parsed_buys: List[ParsedBuy] = []
        for tx_data in raw_tx_list_page:
            if isinstance(tx_data, dict):
                buy = await _parse_birdeye_tx_to_buy(tx_data, token_mint)
                if buy:
                    page_parsed_buys.append(buy)
        
        all_parsed_buys.extend(page_parsed_buys)
        
        # Check if Birdeye indicates there are more pages (optional, but good practice if API provides it)
        # For /defi/txs/token, it provides "hasNext"
        if not raw_txs_data_page.get("hasNext", False) and len(raw_tx_list_page) < trades_per_page:
             logger.info(f"Birdeye indicated no more pages or returned less than limit on page {pages_fetched + 1}. Stopping.")
             break

        current_offset += trades_per_page
        pages_fetched += 1
        
        # Add a small delay to be respectful to the API, especially if fetching many pages
        if pages_fetched < max_pages and len(all_parsed_buys) < total_trades_to_fetch :
             await asyncio.sleep(0.2) # 200ms delay between paged requests

    # Sort by timestamp, oldest first (important for bundle logic)
    # API returns newest first by default (sort_type="desc")
    all_parsed_buys.sort(key=lambda b: b["timestamp"]) 
    
    # If you fetched more than requested due to page sizes, truncate
    if len(all_parsed_buys) > total_trades_to_fetch:
        all_parsed_buys = all_parsed_buys[:total_trades_to_fetch]

    logger.info(f"Fetched {pages_fetched} page(s). Found {len(all_parsed_buys)} potential buy transactions for {token_mint} (after parsing).")
    return all_parsed_buys


async def _identify_bundles(buy_txs: List[ParsedBuy], config: Dict) -> List[Bundle]:
    """Identifies buy bundles from a list of parsed buy transactions."""
    if not buy_txs:
        return []

    bundles: List[Bundle] = []
    current_bundle_buys: List[ParsedBuy] = []
    
    for buy in buy_txs:
        if not current_bundle_buys:
            current_bundle_buys.append(buy)
        else:
            last_buy_in_bundle_time = current_bundle_buys[-1]["timestamp"]
            if (buy["timestamp"] - last_buy_in_bundle_time) <= config["TIME_WINDOW_SEC"]:
                # Add to current bundle if wallet is unique for this bundle
                if buy["buyer_address"] not in [b["buyer_address"] for b in current_bundle_buys]:
                    current_bundle_buys.append(buy)
            else:
                # Time window exceeded, finalize previous bundle if it meets criteria
                if len(current_bundle_buys) >= config["MIN_WALLETS_PER_BUNDLE"]:
                    bundle_wallets = list(set(b["buyer_address"] for b in current_bundle_buys)) # Should already be unique
                    total_sol = sum(b["sol_spent"] for b in current_bundle_buys)
                    bundles.append(Bundle(
                        start_time=current_bundle_buys[0]["timestamp"],
                        end_time=current_bundle_buys[-1]["timestamp"],
                        wallets=bundle_wallets,
                        buy_transactions=list(current_bundle_buys), # Make a copy
                        total_sol_spent=total_sol,
                        total_token_received=sum(b["token_received"] for b in current_bundle_buys),
                        avg_sol_spent=total_sol / len(bundle_wallets) if bundle_wallets else 0,
                        funder_summary=None # To be filled later
                    ))
                # Start a new bundle
                current_bundle_buys = [buy]

    # Check the last bundle
    if len(current_bundle_buys) >= config["MIN_WALLETTS_PER_BUNDLE"]:
        bundle_wallets = list(set(b["buyer_address"] for b in current_bundle_buys))
        total_sol = sum(b["sol_spent"] for b in current_bundle_buys)
        bundles.append(Bundle(
            start_time=current_bundle_buys[0]["timestamp"],
            end_time=current_bundle_buys[-1]["timestamp"],
            wallets=bundle_wallets,
            buy_transactions=list(current_bundle_buys),
            total_sol_spent=total_sol,
            total_token_received=sum(b["token_received"] for b in current_bundle_buys),
            avg_sol_spent=total_sol / len(bundle_wallets) if bundle_wallets else 0,
            funder_summary=None
        ))
    
    logger.info(f"Identified {len(bundles)} bundles meeting criteria.")
    return bundles


async def _trace_funders_for_bundle(bundle: Bundle, config: Dict) -> None:
    """Traces funding sources for wallets in a single bundle and updates bundle in-place."""
    if not config["ENABLE_FUNDER_TRACE"]:
        return

    logger.info(f"Tracing funders for bundle starting at {_format_timestamp_pretty(bundle['start_time'])} with {len(bundle['wallets'])} wallets.")
    
    # We need transactions *before* the first buy in the bundle.
    # Look back up to MAX_DAYS_FOR_FUNDING_TRACE
    trace_before_time = bundle['start_time']
    trace_after_time = trace_before_time - (config["MAX_DAYS_FOR_FUNDING_TRACE"] * 24 * 60 * 60)

    potential_funders: Dict[str, List[str]] = {} # funder_wallet -> list of funded_wallets_in_bundle

    for wallet_address in bundle["wallets"]:
        # Find the timestamp of this wallet's buy within the bundle
        wallet_buy_time = trace_before_time # Default to bundle start time
        for bt_tx in bundle["buy_transactions"]:
            if bt_tx["buyer_address"] == wallet_address:
                wallet_buy_time = bt_tx["timestamp"]
                break
        
        # Fetch transactions for this wallet *before* its specific buy time (or bundle start time)
        # but *after* the general MAX_DAYS_FOR_FUNDING_TRACE limit
        trader_txs_data = await asyncio.to_thread(
            birdeye_api.trader_txs,
            trader_address=wallet_address,
            before_time=wallet_buy_time, # Only txs before this wallet's buy
            after_time=trace_after_time, # But not older than N days
            limit=config["MAX_TXS_TO_FETCH_FOR_FUNDING"]
        )

        if trader_txs_data and isinstance(trader_txs_data.get("items"), list):
            wallet_txs = trader_txs_data["items"]
            # Sort by time, newest first, to find the most recent funding.
            wallet_txs.sort(key=lambda tx: tx.get("block_time", 0), reverse=True)

            for tx in wallet_txs:
                is_sol_transfer = tx.get("token_address") == NATIVE_SOL_MINT
                amount = tx.get("token_amount", 0.0)
                
                # Identify SOL received by the wallet_address
                # In Birdeye's trader_txs, 'trader_address' is the one we query.
                # We need to check if this tx represents SOL coming *into* trader_address.
                # This depends on Birdeye's schema for representing transfers.
                # Assuming 'token_amount' is positive if received by 'trader_address',
                # and 'counter_party_address' is the sender (funder).
                # Or, if 'destination_address' == wallet_address and 'source_address' is funder.
                # The current birdeye_api.trader_txs output description isn't detailed enough.
                # For now, let's assume positive 'token_amount' means received by 'trader_address'
                # and 'counter_party_address' is the funder if present. This is a GUESS.
                # A more robust check would involve looking at 'source_address' vs 'destination_address'
                # if those fields are reliably present for SOL transfers in trader_txs.

                # Simplified logic for this example:
                # If it's a SOL transfer, and amount is significant,
                # and there's a 'counter_party_address' that isn't the wallet itself.
                funder = tx.get("counter_party_address")
                if is_sol_transfer and amount >= config["MIN_SOL_FOR_FUNDING_TX"] and funder and funder != wallet_address:
                    if funder not in potential_funders:
                        potential_funders[funder] = []
                    if wallet_address not in potential_funders[funder]: # Ensure unique add
                        potential_funders[funder].append(wallet_address)
                    logger.debug(f"Wallet {wallet_address[:6]} potentially funded by {funder[:6]} ({amount} SOL)")
                    break # Found most recent significant SOL deposit for this wallet
        await asyncio.sleep(0.1) # Small delay between API calls

    funder_summary = {funder: len(wallets) for funder, wallets in potential_funders.items() if len(wallets) > 1}
    bundle["funder_summary"] = funder_summary
    logger.info(f"Funder trace for bundle complete. Summary: {funder_summary}")


async def _format_report(token_mint: str, bundles: List[Bundle], token_info: Optional[Dict]) -> str:
    """Formats the bundle analysis into a MarkdownV2 string for Telegram."""
    if not bundles:
        return f"No significant buy bundles detected for token `{_escape_md(token_mint)}`."

    token_name = token_info.get("name", token_mint) if token_info else token_mint
    token_symbol = f"${token_info.get('symbol', '')}" if token_info and token_info.get('symbol') else ""
    
    report_lines = [
        f"🚨 *Bundle Activity Detected for {token_symbol} ({_escape_md(token_name)})*",
        f"Token: `{_escape_md(token_mint)}`\n"
    ]

    for i, bundle in enumerate(bundles):
        report_lines.append(f"🧠 *Cluster {i + 1}*:")
        report_lines.append(f"  • Time Window: {_format_timestamp_pretty(bundle['start_time'])} \\- {_format_timestamp_pretty(bundle['end_time'])}")
        report_lines.append(f"  • Unique Buyers: {len(bundle['wallets'])}")
        report_lines.append(f"  • Total Buys: {len(bundle['buy_transactions'])}")
        report_lines.append(f"  • Total SOL Spent: {_escape_md(f'{bundle["total_sol_spent"]:.2f}')}")
        report_lines.append(f"  • Avg SOL / Buyer: {_escape_md(f'{bundle["avg_sol_spent"]:.2f}')}")
        
        if bundle["funder_summary"]:
            funders_str_parts = []
            # Sort by number of wallets funded, then by funder address
            sorted_funders = sorted(bundle["funder_summary"].items(), key=lambda item: (-item[1], item[0]))
            
            for funder, count in sorted_funders:
                funders_str_parts.append(f"{_escape_md(funder[:4])}\\.\\.\\.{_escape_md(funder[-4:])} ({count} wallets)")
            if funders_str_parts:
                 report_lines.append(f"  • Shared Funder Matches: {', '.join(funders_str_parts)}")
            else:
                report_lines.append("  • No significant shared funders detected for this cluster.")
        elif DEFAULT_CONFIG["ENABLE_FUNDER_TRACE"]: # To differentiate from disabled trace
             report_lines.append("  • No significant shared funders detected for this cluster.")
        report_lines.append("") # Newline for readability

    return "\n".join(report_lines)

# --- Main Analysis Function ---
async def analyze_token_for_bundles(token_mint: str, config_override: Optional[Dict] = None) -> str:
    """
    Analyzes a token for buy bundle activity.
    Fetches trades, identifies clusters, optionally traces funding, and returns a formatted report.
    """
    current_config = DEFAULT_CONFIG.copy()
    if config_override:
        current_config.update(config_override)

    logger.info(f"Starting bundle analysis for token: {token_mint}")

    # Fetch token overview for name/symbol
    token_info = await asyncio.to_thread(birdeye_api.token_overview, mint_address=token_mint)
    if not token_info:
        logger.warning(f"Could not fetch token overview for {token_mint}. Proceeding without it.")

    # 1. Fetch and parse buy transactions
    buy_txs = await _fetch_and_parse_buys(token_mint, current_config)
    if not buy_txs:
        return f"No buy transactions found or parsed for token `{_escape_md(token_mint)}`."

    # 2. Identify bundles
    bundles = await _identify_bundles(buy_txs, current_config)
    if not bundles:
        return f"No buy bundles meeting criteria (min {current_config['MIN_WALLETS_PER_BUNDLE']} wallets within {current_config['TIME_WINDOW_SEC']}s) found for `{_escape_md(token_mint)}`."

    # 3. (Optional) Trace funders for each bundle
    if current_config["ENABLE_FUNDER_TRACE"]:
        # Create a list of tasks for tracing funders for each bundle concurrently
        funder_trace_tasks = [_trace_funders_for_bundle(bundle, current_config) for bundle in bundles]
        await asyncio.gather(*funder_trace_tasks)
    
    # 4. Format and return report
    report = await _format_report(token_mint, bundles, token_info)
    logger.info(f"Bundle analysis complete for {token_mint}.")
    return report


# --- Example Usage (for testing) ---
if __name__ == "__main__":
    async def main_test():
        # Replace with a token mint that has recent activity for testing
        # test_token_mint = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzL7xeQu2d25qg" # PYTH (Example)
        test_token_mint = "FGB1Z9BB7wdFmNsfqnPyHLXpYLbBmFBjbHWv2c9epump" # BONK (Example)
        # test_token_mint = "DUSTawucrTsGU8hcqRdHDCX Yd9aूणYgXEBLtjbxG4DT" # DUST (Example with non-ASCII - ensure env is UTF-8)
        # test_token_mint = input("Enter token mint address for bundle analysis: ") # Interactive

        print(f"Analyzing {test_token_mint} for bundle activity...")
        
        # Test with funder trace disabled
        # report_no_funder = await analyze_token_for_bundles(test_token_mint, {"ENABLE_FUNDER_TRACE": False})
        # print("\n--- REPORT (FUNDER TRACE DISABLED) ---")
        # print(report_no_funder)

        # print("\n--- Testing with funder trace enabled (default) ---")
        report = await analyze_token_for_bundles(test_token_mint)
        print("\n--- FINAL REPORT ---")
        print(report)

    # Ensure BIRDEYE_API_KEY is set in your .env file for birdeye_api to work
    from dotenv import load_dotenv
    load_dotenv()
    if not birdeye_api.BIRDEYE_API_KEY:
        print("CRITICAL: BIRDEYE_API_KEY not found. Please set it in your .env file.")
    else:
        asyncio.run(main_test())