import os
import json
import time
from typing import Dict, List
from collections import defaultdict
from operator import itemgetter
from datetime import datetime

from birdeye_api import tokenlist_page, trade_data, market_data, token_creation_info

# === CONFIG ===
LIMIT_TOKENS = 50
MIN_CLUSTER_SIZE = 2
SAVE_OUTPUT = True
CREATION_TIME_WINDOW_SEC = 6 * 3600  # Only include tokens created in last 6 hours
OUTPUT_FILE = f"cluster_trends_{int(time.time())}.json"
API_SLEEP_SEC = 0.2  # To respect rate limits

def normalize_name(name: str) -> str:
    return ''.join(c.lower() for c in name if c.isalnum())

def fetch_and_cluster():
    print("[*] Fetching token list...")
    page = tokenlist_page(offset=0, limit=LIMIT_TOKENS)
    tokens = page.get("tokens", []) if isinstance(page, dict) else []


    if not tokens:
        print("[!] Failed to fetch token list.")
        return

    now_unix = int(time.time())
    fresh_tokens = []

    print("[*] Enriching with creation times...")
    for token in tokens:
        mint = token["address"]
        creation_info = token_creation_info(mint) or {}
        creation_time = creation_info.get("creationTime", 0)
        token["creationTime"] = creation_time

        if creation_time > now_unix - CREATION_TIME_WINDOW_SEC:
            fresh_tokens.append(token)

        time.sleep(API_SLEEP_SEC)

    print(f"[*] Found {len(fresh_tokens)} tokens created in the last {CREATION_TIME_WINDOW_SEC // 3600}h")

    clusters: Dict[str, List[Dict]] = defaultdict(list)
    for token in fresh_tokens:
        norm_name = normalize_name(token.get("name", ""))
        if norm_name:
            clusters[norm_name].append({
                "name": token.get("name"),
                "symbol": token.get("symbol"),
                "mint": token.get("address"),
                "creationTime": token.get("creationTime"),
            })

    print(f"[*] Grouped into {len(clusters)} name clusters.")

    result = {}
    for name_key, token_list in clusters.items():
        if len(token_list) < MIN_CLUSTER_SIZE:
            continue

        print(f"\n[+] Cluster: {name_key.upper()} ({len(token_list)} tokens)")

        enriched = []
        for token in token_list:
            mint = token["mint"]
            trade = trade_data(mint) or {}
            market = market_data(mint) or {}
            enriched.append({
                "mint": mint,
                "name": token["name"],
                "symbol": token["symbol"],
                "volume_24h": trade.get("v24hUSD", 0),
                "txns_24h": trade.get("txns24h", 0),
                "liquidity": market.get("liquidity", 0),
                "market_cap": market.get("marketCap", 0),
                "creationTime": token["creationTime"]
            })
            time.sleep(API_SLEEP_SEC)

        sorted_group = sorted(enriched, key=itemgetter("volume_24h"), reverse=True)
        leader = sorted_group[0]
        print(f"    🏆 Leading: {leader['symbol']} — ${leader['volume_24h']} volume")

        result[name_key] = sorted_group

    if SAVE_OUTPUT:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n[✓] Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_and_cluster()
