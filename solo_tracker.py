# --- START OF FILE solo_tracker.py ---

import os
import json
import time
# import requests # No longer needed directly
from typing import Dict, List, Optional, Any, Tuple # <<< MODIFIED THIS LINE
from dotenv import load_dotenv

# ── Load environment ────────────────────────────────────────────
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# BIRDEYE_API_KEY is no longer needed directly here, helius_analyzer uses it

SOLO_WATCHLIST_FILE = "solo_token_watchlist.json"
TOP_WALLETS_LIMIT = 20

# --- Import necessary function from helius_analyzer ---
# This function already uses the api_usage_limiter and cache_manager
try:
    from helius_analyzer import get_birdeye_token_overview
    print("[✓] solo_tracker: Successfully imported get_birdeye_token_overview.")
except ImportError:
    print("[✗] solo_tracker: Failed to import get_birdeye_token_overview from helius_analyzer. Token info in alerts will be basic.")
    # Provide a stub if import fails
    def get_birdeye_token_overview(mint_address: str) -> Optional[Dict[str, Any]]:
        return None

# --- Need requests for Telegram still ---
import requests

# ── Telegram Alert ──────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[!] Telegram credentials missing for solo_tracker.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        # Note: Using Markdown requires escaping special chars in token symbols/names if needed
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        r = requests.post(url, data=payload, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[✗] Telegram send error (solo_tracker): {e}")
        return False

# ── Load / Save JSON Watchlist ──────────────────────────────────
def load_watchlist() -> Dict[str, List[str]]:
    if os.path.exists(SOLO_WATCHLIST_FILE):
        try:
            with open(SOLO_WATCHLIST_FILE, "r", encoding="utf-8") as f:
                content = json.load(f)
                # Basic validation: ensure it's a dict where values are lists
                if isinstance(content, dict) and all(isinstance(v, list) for v in content.values()):
                    return content
                else:
                    print(f"[!] Invalid format in {SOLO_WATCHLIST_FILE}. Starting fresh.")
                    return {}
        except Exception as e:
            print(f"[!] Failed to load solo watchlist: {e}")
    return {}

def save_watchlist(data: Dict[str, List[str]]):
    try:
        with open(SOLO_WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[✗] Failed to save solo watchlist: {e}")

# ── Removed local fetch_token_info, using helius_analyzer version ──

# ── Main Entry ──────────────────────────────────────────────────
def track_token(wallet_address: str, token_mints: List[str], wallet_store: Dict[str, Dict]):
    """
    Tracks tokens held by top wallets and alerts when a token is seen
    held by multiple top wallets over time.
    Uses helius_analyzer.get_birdeye_token_overview for token info.
    """
    # --- Wallet Qualification (Unchanged) ---
    # Ensure wallet_store is a dict before proceeding
    if not isinstance(wallet_store, dict):
         print("[!] solo_tracker: Invalid wallet_store provided. Skipping.")
         return

    try:
        # Sort safely, handling potential missing 'latest_profit'
        sorted_wallets = sorted(
            wallet_store.items(),
            key=lambda x: x[1].get("latest_profit", -float('inf')) if isinstance(x[1], dict) else -float('inf'),
            reverse=True
        )
    except Exception as e:
        print(f"[!] solo_tracker: Error sorting wallet_store: {e}")
        return # Cannot determine top wallets

    top_wallets = {w[0] for w in sorted_wallets[:TOP_WALLETS_LIMIT]}
    if wallet_address not in top_wallets:
        # This wallet isn't currently in the top N, don't process its holdings for *this* tracker
        return

    # --- Watchlist Tracking & Alerting ---
    watchlist = load_watchlist()
    alerts_to_send: List[Tuple[str, List[str]]] = [] # Use Tuple type hint

    for token_mint in token_mints:
        if not isinstance(token_mint, str) or not token_mint: # Basic validation
            continue

        touched_by = watchlist.get(token_mint, [])

        # Ensure touched_by is a list (handling potential file corruption)
        if not isinstance(touched_by, list):
             print(f"[!] solo_tracker: Corrupted watchlist entry for {token_mint[:6]}... Resetting entry.")
             touched_by = []

        if wallet_address not in touched_by:
            # Add current qualified wallet to the list for this token
            touched_by.append(wallet_address)
            watchlist[token_mint] = touched_by # Update watchlist dict

            # Check if this token has now been seen by >= 2 unique top wallets
            if len(touched_by) >= 2:
                # Check if we have *already* alerted for this specific token reaching >=2 holders
                # We need state for this, or a simpler approach: only alert the *first* time it hits 2.
                # Let's go with the simpler approach: Alert only when count becomes exactly 2.
                if len(touched_by) == 2:
                    print(f"    [*] Solo Tracker: Token {token_mint[:6]}... now seen by {len(touched_by)} top wallets. Adding to alert queue.")
                    alerts_to_send.append((token_mint, touched_by))
                # else: # Already alerted when it hit 2 previously
                #    print(f"    [*] Solo Tracker: Token {token_mint[:6]}... already seen by {len(touched_by)} top wallets.")

    # Save the updated watchlist regardless of alerts
    save_watchlist(watchlist)

    # --- Send Alerts ---
    if not alerts_to_send:
        return # No alerts to send this time

    print(f"[*] Solo Tracker: Sending {len(alerts_to_send)} alerts...")
    for token_mint, wallets_list in alerts_to_send:
        # Fetch token info using the cached/limited function
        print(f"    Fetching Birdeye info for alert: {token_mint[:6]}...")
        overview_data = get_birdeye_token_overview(token_mint) # From helius_analyzer
        time.sleep(0.1) # Small delay even if cached

        token_symbol = "N/A"
        price_str = "N/A"
        if overview_data:
            token_symbol = overview_data.get("symbol", "N/A")
            price = overview_data.get("price")
            price_str = f"${float(price):.6f}" if isinstance(price, (int, float)) else "N/A"

        # Format wallet list
        wallet_summary = "\n".join([f"- {w[:4]}...{w[-4:]}" for w in wallets_list])
        # Construct message
        msg = (
            f"👀 *Solo Token Alert* 👀\n\n"
            f"🪙 *Token:* {token_symbol} ({token_mint[:4]}...{token_mint[-4:]})\n"
            f"💲 *Price:* {price_str}\n\n"
            f"👥 Now seen held by *{len(wallets_list)} top wallets*:\n{wallet_summary}\n\n"
            f"🔗 [DexScreener](https://dexscreener.com/solana/{token_mint})"
        )
        send_telegram(msg)
        # Add delay between sending multiple alerts
        time.sleep(0.5)

# --- END OF FILE solo_tracker.py ---