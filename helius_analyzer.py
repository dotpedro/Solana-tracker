# --- START OF FILE helius_analyzer.py ---

import os
import requests
import time
import json
from dotenv import load_dotenv
from requests.exceptions import RequestException, Timeout, HTTPError
from typing import List, Dict, Any, Optional, Set, Union
from datetime import datetime, timezone
import urllib.parse  # Needed for adding query params

# --- Cache Manager Import ---
try:
    import cache_manager
    print("[✓] Importing cache_manager for helius_analyzer...")
except ImportError:
    print("ERROR: helius_analyzer.py - cache_manager.py not found.")
    cache_manager = None  # degrade gracefully

# --- API Usage Limiter Import ---
try:
    import api_usage_limiter
    print("[✓] Importing api_usage_limiter for helius_analyzer...")
except ImportError:
    print("CRITICAL: api_usage_limiter.py not found! API calls will not be managed.")
    class ApiUsageLimiterStub:
        CACHE_ENABLED = False
        def request_api(*args, **kwargs) -> None:
            print("    [STUB] api_usage_limiter.request_api called - LIMITER MISSING")
            return None
    api_usage_limiter = ApiUsageLimiterStub()  # type: ignore

load_dotenv()
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not BIRDEYE_API_KEY: raise ValueError("BIRDEYE_API_KEY missing...")
if not HELIUS_API_KEY: raise ValueError("HELIUS_API_KEY missing...")

# --- Helius URLs ---
HELIUS_API_BASE_URL = "https://api.helius.xyz"
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL") or f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# --- gTFA Controls (read from environment; your main script prints these) ---
USE_GTFA = os.getenv("USE_GTFA", "1") == "1"
GTFA_PAGE_LIMIT_FULL = int(os.getenv("GTFA_PAGE_LIMIT_FULL", "100"))   # full details per call
GTFA_PAGE_LIMIT_SIGS = int(os.getenv("GTFA_PAGE_LIMIT_SIGS", "1000"))  # signatures per call
GTFA_ORDER_EARLY = os.getenv("GTFA_ORDER_EARLY", "asc")                # earliest scans
GTFA_ORDER_RECENT = os.getenv("GTFA_ORDER_RECENT", "desc")             # recent wallet pages

print(f"[gTFA] helius_analyzer: USE_GTFA={USE_GTFA} FULL={GTFA_PAGE_LIMIT_FULL} SIGS={GTFA_PAGE_LIMIT_SIGS} "
      f"ORDER_EARLY={GTFA_ORDER_EARLY} ORDER_RECENT={GTFA_ORDER_RECENT}")

EXCLUDED_SYMBOLS = {"SOL", "WSOL", "USDC", "USDT", "BONK", "WIF", "JUP", "PYTH", "RAY", "SRM", "MSOL", "JITOSOL", "RENDER", "HNT"}
KNOWN_PROGRAM_ADDRESSES = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "11111111111111111111111111111111",
    "SysvarRent111111111111111111111111111111111", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "9qvG1zUp8xF1Bi4m6UdRNby1BAAuaZXdtQcmb5jVhYHA",
    "whirLbMiicVdio4qvUfM5KAg6Ct8EpTdqsQ4S556MJ2", "MeteoraagvWLeyJPDYzspRarXdy5S5GHh1GnhDkGJ",
    "pumpEuHsBQHAX3r5T6Low9ykQAumX6msj9f1M6JjUQBi"
}

def format_timestamp(ts: Optional[int]) -> Optional[str]:
    if ts is None: return None
    try: return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except (TypeError, ValueError): return None

# ---------------------------------------------------------------------------
# get_candidate_tokens - Unchanged
# ---------------------------------------------------------------------------
def get_candidate_tokens(
    limit: int = 50, min_volume: int = 1_000, max_liquidity: int = 250_000,
    min_marketcap: int = 10_000, max_marketcap: int = 100_000,
    max_pages_to_try: int = 20, birdeye_page_cache_ttl_seconds: int = 10 * 60
) -> List[Dict[str, Any]]:
    PAGE_SIZE = 50
    all_collected_raw_tokens: List[Dict[str, Any]] = []
    current_offset = 0
    pages_processed = 0
    print(f"[*] Fetching candidate tokens... (target: {limit} tokens)")
    estimated_raw_tokens_needed = limit * 3
    while pages_processed < max_pages_to_try and len(all_collected_raw_tokens) < estimated_raw_tokens_needed:
        page_cache_key = f"birdeye_tokenlist_rawpage_offset{current_offset}_limit{PAGE_SIZE}"
        url = f"https://public-api.birdeye.so/defi/tokenlist?offset={current_offset}&limit={PAGE_SIZE}"
        headers = {"X-API-KEY": BIRDEYE_API_KEY}
        page_token_list = api_usage_limiter.request_api(
            method=requests.get, api_name="birdeye", endpoint_name="tokenlist",
            calling_script="helius_analyzer.get_candidate_tokens",
            cache_key=page_cache_key, cache_ttl_seconds=birdeye_page_cache_ttl_seconds,
            url=url, headers=headers, timeout=45
        )
        raw_tokens_on_page: List[Dict] = []
        if isinstance(page_token_list, dict) and 'data' in page_token_list and isinstance(page_token_list['data'].get('tokens'), list):
            raw_tokens_on_page = page_token_list['data']['tokens']
        elif isinstance(page_token_list, list):
            raw_tokens_on_page = page_token_list
        elif page_token_list is None:
            print(f"    [!] Failed/Blocked Birdeye page {pages_processed + 1}. Stopping pagination.")
            break
        else:
            print(f"    [!] Unexpected data type from limiter for Birdeye page {pages_processed + 1}: {type(page_token_list)}. Stopping.")
            break
        if not raw_tokens_on_page:
            print(f"    [!] No more tokens on page {pages_processed + 1}. Stopping pagination.")
            break
        all_collected_raw_tokens.extend(raw_tokens_on_page)
        pages_processed += 1
        current_offset += PAGE_SIZE
        if len(raw_tokens_on_page) < PAGE_SIZE:
            print("    [!] Last page had fewer than PAGE_SIZE tokens. Assuming end of results.")
            break
    final_filtered_tokens: List[Dict[str, Any]] = []
    print(f"[*] Filtering {len(all_collected_raw_tokens)} raw tokens from {pages_processed} pages...")
    if not all_collected_raw_tokens:
        print("[!] No raw tokens collected from Birdeye.")
        return []
    for t in all_collected_raw_tokens:
        try:
            vol = float(t.get("v24hUSD", 0)); liq = float(t.get("liquidity", 0)); mc  = float(t.get("mc", 0) or 0)
            address = t.get("address"); symbol_raw = t.get("symbol"); symbol = str(symbol_raw or "").upper()
        except (ValueError, TypeError, AttributeError) as e:
            print(f"    [!] Skipping token due to parsing error: {e} - Data: {str(t)[:100]}"); continue
        if (address and symbol not in EXCLUDED_SYMBOLS and vol >= min_volume and 0 < liq <= max_liquidity and min_marketcap <= mc < max_marketcap):
            final_filtered_tokens.append(t)
    final_filtered_tokens.sort(key=lambda x: float(x.get("v24hUSD", 0)), reverse=True)
    print(f"[✓] Found {len(final_filtered_tokens)} tokens matching criteria (target was {limit}).")
    return final_filtered_tokens[:limit]

# ---------------------------------------------------------------------------
# get_birdeye_token_overview - Unchanged
# ---------------------------------------------------------------------------
def get_birdeye_token_overview(mint_address: str, cache_ttl_seconds: int = 3 * 60) -> Optional[Dict[str, Any]]:
    if not mint_address or not BIRDEYE_API_KEY: return None
    cache_key = f"birdeye_token_overview_{mint_address}"
    url = f"https://public-api.birdeye.so/defi/token_overview?address={mint_address}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    json_data = api_usage_limiter.request_api(
        method=requests.get, api_name="birdeye", endpoint_name="token_overview",
        calling_script="helius_analyzer.get_birdeye_token_overview",
        cache_key=cache_key, cache_ttl_seconds=cache_ttl_seconds,
        url=url, headers=headers, timeout=20
    )
    if json_data is None: return None
    token_data = json_data.get("data")
    if isinstance(token_data, dict): return token_data
    else:
        print(f"    [!] Birdeye Warning: 'data' field missing/invalid in overview for {mint_address}. Response: {str(json_data)[:200]}")
        return None

# ============================================================================
# Helius RPC helpers (gTFA + hydration)  --- NEW
# ============================================================================

class HeliusRPCError(RuntimeError): pass

def _helius_rpc(method: str, params: list, *, timeout: int = 45) -> Any:
    if not HELIUS_RPC_URL:
        raise HeliusRPCError("HELIUS_RPC_URL is not configured")
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(HELIUS_RPC_URL, json=body, timeout=timeout)
    if r.status_code != 200:
        raise HeliusRPCError(f"HTTP {r.status_code}: {r.text}")
    j = r.json()
    if "error" in j:
        raise HeliusRPCError(str(j["error"]))
    return j.get("result")

def _gTFA(
    address: str,
    *,
    limit: int = 100,
    order: str = "desc",                # "asc" | "desc"
    details: str = "signatures",        # "signatures" (<=1000) | "full" (<=100)
    status: Optional[str] = "succeeded",
    block_time: Optional[Dict[str, int]] = None,  # {"gte": unix, "lte": unix}
    slot: Optional[Dict[str, int]] = None,        # {"gte": slot, "lte": slot}
    pagination_token: Optional[str] = None,
    timeout: int = 45,
) -> Dict[str, Any]:
    assert order in ("asc", "desc")
    assert details in ("signatures", "full")
    params = [address, {
        "limit": limit,
        "order": order,
        "transactionDetails": details,
    }]
    filters: Dict[str, Any] = {}
    if status: filters["status"] = status
    if block_time: filters["blockTime"] = block_time
    if slot: filters["slot"] = slot
    if filters: params[1]["filters"] = filters
    if pagination_token:
        params[1]["paginationToken"] = pagination_token
    return _helius_rpc("getTransactionsForAddress", params, timeout=timeout)

def fetch_parsed_transactions(signatures: List[str], *, timeout=45) -> List[dict]:
    """
    Batch hydrate signatures into parsed transactions using Helius v0 parse endpoint.
    Accepts list[str] or list[dict] and normalizes to list[str].
    """
    if not signatures:
        return []

    # 1) Normalize to a clean list[str]
    norm: list[str] = []
    for s in signatures:
        if isinstance(s, str):
            norm.append(s)
        elif isinstance(s, dict) and "signature" in s and isinstance(s["signature"], str):
            norm.append(s["signature"])
    # Filter out anything funky (short strings etc.)
    norm = [s for s in norm if isinstance(s, str) and len(s) >= 44]

    if not norm:
        print("    [!] fetch_parsed_transactions: no valid signatures after normalization.")
        return []

    out: List[dict] = []
    CHUNK = 100
    url = f"{HELIUS_API_BASE_URL}/v0/transactions?api-key={HELIUS_API_KEY}"  # NOTE: no trailing slash
    headers = {'Content-Type': 'application/json'}

    for i in range(0, len(norm), CHUNK):
        chunk_sigs = norm[i:i+CHUNK]
        if not chunk_sigs:
            continue
        payload = json.dumps({"transactions": chunk_sigs})
        batch_result_list = api_usage_limiter.request_api(
            method=requests.post, api_name="helius", endpoint_name="parseTransactionsV0",
            calling_script="helius_analyzer.fetch_parsed_transactions",
            cache_key=None, cache_ttl_seconds=0,
            url=url, headers=headers, data=payload, timeout=timeout
        )
        if isinstance(batch_result_list, list):
            out.extend(tx for tx in batch_result_list if isinstance(tx, dict))
        elif batch_result_list is None:
            print("    [!] parseTransactions batch blocked/failed.")
        else:
            print(f"    [!] Unexpected parseTransactions return type: {type(batch_result_list)}")
        time.sleep(0.05)

    return out


# ============================================================================
# Helius v0 History API (fallback path) --- same as your current behavior
# ============================================================================

def _fetch_parsed_history_page_v0_fallback(
    address: str, limit: int = 100, before_sig: Optional[str] = None
) -> Optional[List[Dict[str, Any]]]:
    cache_key = f"helius_txHistoryV0_{address}_limit{limit}" + (f"_before{before_sig}" if before_sig else "")
    helius_history_cache_ttl = 2 * 60 * 60  # 2 hours
    base_url = f"{HELIUS_API_BASE_URL}/v0/addresses/{address}/transactions?api-key={HELIUS_API_KEY}"
    params = {"limit": limit}
    if before_sig: params["before"] = before_sig
    query_string = urllib.parse.urlencode(params)
    full_url = f"{base_url}&{query_string}"

    resp_json = api_usage_limiter.request_api(
        method=requests.get, api_name="helius", endpoint_name="getAddressTransactionsV0",
        calling_script="helius_analyzer._fetch_parsed_history_page_v0_fallback",
        cache_key=cache_key, cache_ttl_seconds=helius_history_cache_ttl,
        url=full_url, timeout=45
    )
    if resp_json is None: return None
    if isinstance(resp_json, list):
        if cache_manager and getattr(api_usage_limiter, "CACHE_ENABLED", False):
            try: cache_manager.set_cache(cache_key, resp_json)
            except Exception as e: print(f"[!] Cache write error for {cache_key}: {e}")
        return resp_json
    if isinstance(resp_json, dict) and resp_json.get("error"):
        print(f"    [*] Helius returned error in history fetch: {resp_json['error']}")
        return None
    print(f"    [!] Unexpected data type from limiter for Helius history fetch: {type(resp_json)}")
    return None

def get_earliest_parsed_transactions_v0_fallback(
    address: str, target_count: int = 1000, max_total_fetch: int = 10000, page_limit: int = 5
) -> List[Dict[str, Any]]:
    print(f"    [*] Fallback: Fetching earliest ~{target_count} parsed TXs for {address} via History API...")
    all_parsed_txs: List[Dict[str, Any]] = []
    last_sig: Optional[str] = None
    pages = 0
    while pages < page_limit and len(all_parsed_txs) < max_total_fetch:
        page_results = _fetch_parsed_history_page_v0_fallback(address, limit=100, before_sig=last_sig)
        if page_results is None:
            print(f"    [*] Pagination stopped: Error/timeout/blocked on page {pages + 1}."); break
        elif not page_results:
            print(f"    [*] Pagination stopped: Reached start (no more txs) on page {pages + 1}."); break
        else:
            all_parsed_txs.extend(page_results)
            if page_results[-1].get('signature'): last_sig = page_results[-1]['signature']
            else:
                print("    [!] Last transaction in page missing signature. Stopping pagination."); break
            pages += 1
            time.sleep(0.1)
    if not all_parsed_txs:
        print(f"    [!] No parsed TXs collected for {address} via History API."); return []
    print(f"    [*] Collected {len(all_parsed_txs)} parsed TXs total via pagination from {pages} pages.")
    oldest_data = all_parsed_txs[-target_count:]
    oldest_data.reverse()  # sort oldest first
    print(f"    [*] Returning {len(oldest_data)} oldest parsed TXs found (target was {target_count}).")
    return oldest_data

# ============================================================================
# Functions used by main loop (now gTFA-aware)
# ============================================================================

def _fetch_parsed_history_page_v0(
    address: str, limit: int = 100, before_sig: Optional[str] = None
) -> Optional[List[Dict[str, Any]]]:
    """
    Return a single page of parsed transactions (newest-first) for a wallet.
    - If USE_GTFA: fetch signatures via gTFA(order=GTFA_ORDER_RECENT), hydrate via parse endpoint.
    - Else: use your previous v0 History API (fallback).
    """
    if not USE_GTFA:
        return _fetch_parsed_history_page_v0_fallback(address, limit=limit, before_sig=before_sig)

    try:
        signatures: List[str] = []
        pagination = None

        while len(signatures) < limit:
            res = _gTFA(
                address,
                limit=min(GTFA_PAGE_LIMIT_SIGS, 1000),
                order=GTFA_ORDER_RECENT,          # newest-first by default
                details="signatures",
                status="succeeded",
                pagination_token=pagination,
            )

            page = res.get("data", [])
            if not page:
                break

            # --- NORMALIZE gTFA PAGE ITEMS TO PURE SIGNATURE STRINGS ---
            page_sigs: list[str] = []
            for item in page:
                if isinstance(item, str):
                    page_sigs.append(item)
                elif isinstance(item, dict) and isinstance(item.get("signature"), str):
                    page_sigs.append(item["signature"])

            if not page_sigs:
                break

            signatures.extend(page_sigs)
            pagination = res.get("paginationToken")
            if not pagination:
                break

        signatures = signatures[:limit]
        parsed = fetch_parsed_transactions(signatures)
        return parsed

    except Exception as e:
        print(f"    [!] gTFA recent history fetch failed for {address}: {e}. Falling back to v0 History API…")
        return _fetch_parsed_history_page_v0_fallback(address, limit=limit, before_sig=before_sig)

# Lone-wolf support (history-before check)
def _check_history_before_sig(address: str, before_sig: str) -> bool:
    """
    Returns True if ANY transactions exist for the address before the given signature.
    We keep the v0 History API here for simplicity because gTFA pagination cursors
    are slot-based, not signature-based.
    """
    results = _fetch_parsed_history_page_v0_fallback(address, limit=1, before_sig=before_sig)
    if results is None:
        print(f"    [!] Lone wolf check failed for {address} before {before_sig[:6]} (API error/timeout). Assuming NOT first.")
        return True
    return bool(results)

_WALLET_HISTORY_CACHE: Dict[str, bool] = {}
def is_potentially_first_transaction(wallet_address: str, transaction_signature_of_receipt: str) -> bool:
    """Checks if a transaction appears to be the first for a wallet."""
    if len(_WALLET_HISTORY_CACHE) > 5000: _WALLET_HISTORY_CACHE.clear()
    if not wallet_address or not transaction_signature_of_receipt: return False
    cache_key = f"lone_wolf_v0_{wallet_address}_{transaction_signature_of_receipt}"
    if cache_key in _WALLET_HISTORY_CACHE:
        return _WALLET_HISTORY_CACHE[cache_key]
    has_history_before = _check_history_before_sig(wallet_address, transaction_signature_of_receipt)
    is_first = not has_history_before
    _WALLET_HISTORY_CACHE[cache_key] = is_first
    return is_first

def find_early_receivers_detailed(
    token_mint_address: str, limit: int = 50,
    target_tx_count: int = 1000,  # number of earliest txs to consider
    check_lone_wolf: bool = True
) -> List[Dict[str, Any]]:
    """
    Finds early receivers.
    - If USE_GTFA: get earliest signatures via gTFA(order=GTFA_ORDER_EARLY), hydrate, then analyze.
    - Else: fallback to your v0 History API path (unchanged logic).
    """
    _WALLET_HISTORY_CACHE.clear()
    print(f"    [*] Finding early receivers for mint: {token_mint_address} (target_tx_count: {target_tx_count}, limit_wallets: {limit})")

    if USE_GTFA:
        # 1) gTFA: collect earliest signatures (order asc)
        try:
            signatures: List[str] = []
            pagination = None

            while len(signatures) < target_tx_count:
                res = _gTFA(
                    token_mint_address,
                    limit=min(GTFA_PAGE_LIMIT_SIGS, 1000),
                    order=GTFA_ORDER_EARLY,      # oldest-first by default
                    details="signatures",
                    status="succeeded",
                    pagination_token=pagination,
                )

                page = res.get("data", [])
                if not page:
                    break

                # --- NORMALIZE gTFA PAGE ITEMS TO PURE SIGNATURE STRINGS ---
                page_sigs: list[str] = []
                for item in page:
                    if isinstance(item, str):
                        page_sigs.append(item)
                    elif isinstance(item, dict) and isinstance(item.get("signature"), str):
                        page_sigs.append(item["signature"])

                if not page_sigs:
                    break

                signatures.extend(page_sigs)
                pagination = res.get("paginationToken")
                if not pagination:
                    break

            if len(signatures) > target_tx_count:
                signatures = signatures[:target_tx_count]

            # 2) Hydrate to parsed txs (keep your schema)
            transactions = fetch_parsed_transactions(signatures)

        except Exception as e:
            print(f"    [!] gTFA earliest fetch failed for {token_mint_address}: {e}. Falling back to v0 History API…")
            transactions = get_earliest_parsed_transactions_v0_fallback(
                token_mint_address, target_count=target_tx_count
            )
    else:
        # v0 fallback
        transactions = get_earliest_parsed_transactions_v0_fallback(
            token_mint_address, target_count=target_tx_count
        )

    if not transactions:
        print(f"    [!] No earliest transactions found for {token_mint_address}.")
        return []

    # Keep only valid parsed txs with timestamps
    valid_transactions = [
        t for t in transactions
        if isinstance(t, dict) and t.get('error') is None and t.get('timestamp') is not None
    ]
    if not valid_transactions:
        print(f"    [!] No valid transactions with timestamps found among fetched history for {token_mint_address}.")
        return []

    receiver_events: List[Dict[str, Any]] = []
    seen_wallets: Set[str] = set()

    # Ensure chronological order
    valid_transactions.sort(key=lambda x: x.get("timestamp", 0))

    print(f"    [*] Analyzing {len(valid_transactions)} earliest valid transactions for receivers...")
    for tx in valid_transactions:
        if len(seen_wallets) >= limit:
            break

        tx_timestamp = tx.get('timestamp')
        tx_signature = tx.get('signature')
        tx_datetime_utc = format_timestamp(tx_timestamp)
        token_transfers = tx.get("tokenTransfers")
        if not isinstance(token_transfers, list):
            continue

        for transfer in token_transfers:
            if not isinstance(transfer, dict):
                continue

            mint = transfer.get("mint")
            to_wallet = transfer.get("toUserAccount")
            from_wallet = transfer.get("fromUserAccount")  # can be None for mints

            if mint == token_mint_address and to_wallet:
                # skip programs/self
                if to_wallet in KNOWN_PROGRAM_ADDRESSES or from_wallet == to_wallet:
                    continue

                transfer_type = "initial_distribution_or_mint" if from_wallet is None else "transfer"

                if to_wallet not in seen_wallets:
                    if len(seen_wallets) >= limit:
                        break

                    is_lone_wolf = False
                    if check_lone_wolf and tx_signature:
                        is_lone_wolf = is_potentially_first_transaction(to_wallet, tx_signature)
                        status_str = "[LONE WOLF]" if is_lone_wolf else "[Existing Wallet]"
                        print(f"        -> Found new receiver: {to_wallet[:6]}... ({status_str}) in tx {tx_signature[:6]}...")

                    receiver_events.append({
                        "wallet": to_wallet,
                        "event_timestamp": tx_timestamp,
                        "event_datetime_utc": tx_datetime_utc,
                        "signature": tx_signature,
                        "from_account": from_wallet,
                        "type": transfer_type,
                        "is_lone_wolf": is_lone_wolf
                    })
                    seen_wallets.add(to_wallet)

            if len(seen_wallets) >= limit:
                break

    if len(seen_wallets) >= limit:
        print(f"    [✓] Reached limit of {limit} unique early wallets.")
    print(f"    [✓] Found {len(receiver_events)} events for {len(seen_wallets)} unique early wallets for {token_mint_address}.")
    return receiver_events


# --- UNMODIFIED: Balances API ---
def get_wallet_balances(wallet_address: str) -> Optional[Dict[str, Any]]:
    if not HELIUS_API_KEY or not wallet_address: return None
    cache_key = f"helius_getBalances_{wallet_address}"
    url = f"{HELIUS_API_BASE_URL}/v0/addresses/{wallet_address}/balances?api-key={HELIUS_API_KEY}"
    helius_balance_cache_ttl = 60 * 60
    data = api_usage_limiter.request_api(
        method=requests.get, api_name="helius", endpoint_name="getBalances",
        calling_script="helius_analyzer.get_wallet_balances",
        cache_key=cache_key, cache_ttl_seconds=helius_balance_cache_ttl,
        url=url, timeout=30
    )
    if data is None: return None
    if isinstance(data, dict) and "nativeBalance" in data and "tokens" in data: return data
    else:
        print(f"    [!] Unexpected balance data structure from limiter: {str(data)[:200]}")
        return None

# --- Kept for completeness (not used by gTFA path directly) ---
def parse_transactions_helius_v0(signatures: List[str]) -> List[Dict[str, Any]]:
    """Legacy parser (kept). Prefer fetch_parsed_transactions above."""
    if not signatures: return []
    parsed_txs: List[Dict[str, Any]] = []
    signatures_to_fetch: List[str] = []
    print(f"    [*] (parse_transactions_helius_v0) Parsing {len(signatures)} transactions (checking cache)...")
    for sig in signatures:
        cache_key = f"helius_parseTx_{sig}"
        cached_tx = None
        if getattr(api_usage_limiter, "CACHE_ENABLED", False) and cache_manager:
            try: cached_tx = cache_manager.get_cache(cache_key, max_age_seconds=7 * 24 * 60 * 60)
            except Exception as e: print(f"[!] Cache read error for {cache_key}: {e}")
        if cached_tx is not None and isinstance(cached_tx, dict):
            parsed_txs.append(cached_tx)
        else:
            signatures_to_fetch.append(sig)
    cache_hits = len(signatures) - len(signatures_to_fetch)
    print(f"        Cache hits: {cache_hits}, Need to fetch: {len(signatures_to_fetch)}")
    if not signatures_to_fetch: return parsed_txs
    url = f"{HELIUS_API_BASE_URL}/v0/transactions/?api-key={HELIUS_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    max_per_req = 100
    for i in range(0, len(signatures_to_fetch), max_per_req):
        batch_to_fetch = signatures_to_fetch[i:i+max_per_req]
        payload = json.dumps({"transactions": batch_to_fetch})
        batch_result_list = api_usage_limiter.request_api(
            method=requests.post, api_name="helius", endpoint_name="parseTransactionsV0",
            calling_script="helius_analyzer.parse_transactions_helius_v0",
            cache_key=None, cache_ttl_seconds=0,
            url=url, headers=headers, data=payload, timeout=60
        )
        if batch_result_list is None:
            print(f"    [!] Failed to parse batch {(i // max_per_req) + 1} via limiter or blocked.")
            continue
        if isinstance(batch_result_list, list):
            for tx_data in batch_result_list:
                sig = tx_data.get('signature')
                if isinstance(tx_data, dict) and sig:
                    if tx_data.get('error'):
                        print(f"        Parsed TX for sig {sig} had error: {tx_data['error']}")
                    else:
                        parsed_txs.append(tx_data)
                        if getattr(api_usage_limiter, "CACHE_ENABLED", False) and cache_manager:
                            individual_cache_key = f"helius_parseTx_{sig}"
                            try: cache_manager.set_cache(individual_cache_key, tx_data)
                            except Exception as e: print(f"[!] Cache write error for {individual_cache_key}: {e}")
                else:
                    print(f"        [!] Unexpected item in parsed batch result: {str(tx_data)[:100]}")
        else:
            print(f"    [!] Unexpected non-list result from limiter for parseTx batch: {type(batch_result_list)}")
        time.sleep(0.5)
    print(f"    [*] (parse_transactions_helius_v0) Finished parsing. Total results: {len(parsed_txs)}")
    return parsed_txs

# --- END OF FILE helius_analyzer.py ---
