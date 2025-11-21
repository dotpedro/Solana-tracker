# --- START OF FILE birdeye_api.py ---

import os
import requests
import time
import json
import hashlib
import urllib.parse
from typing import Dict, Any, Optional, List, Callable # <-- Added Callable here
from dotenv import load_dotenv

# --- Required User Modules ---
try:
    import api_usage_limiter
    import cache_manager
    CACHE_ENABLED = True
    print("[✓] birdeye_api: Successfully imported api_usage_limiter and cache_manager.")
    # Initialize cache database when this module is imported
    cache_manager.init_db()
except ImportError as e:
    print(f"[!] birdeye_api: CRITICAL IMPORT ERROR - {e}. Caching/Limiting might fail.")
    # Define stubs if necessary, or let it raise errors later
    CACHE_ENABLED = False
    class ApiUsageLimiterStub:
        def request_api(*args, **kwargs): print("    [STUB] api_usage_limiter missing!"); return None
    class CacheManagerStub:
        def get_cache(*args, **kwargs): return None
        def set_cache(*args, **kwargs): pass
    api_usage_limiter = ApiUsageLimiterStub()
    cache_manager = CacheManagerStub()

# --- Configuration ---
load_dotenv()
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
if not BIRDEYE_API_KEY:
    raise ValueError("[CRITICAL] birdeye_api: BIRDEYE_API_KEY not found in environment variables.")

# --- Birdeye API Base URLs ---
BDS_BASE_V3_URL = "https://public-api.birdeye.so/defi/v3"
BDS_BASE_LEGACY_URL = "https://public-api.birdeye.so/defi" # For /token_security, /token_creation_info, /price
BDS_BASE_TRADER_URL = "https://public-api.birdeye.so"    # For /trader/txs/seek_by_time

# Standard Headers
BASE_HEADERS = {"X-API-KEY": BIRDEYE_API_KEY}

# Default Cache TTLs (can be overridden per function)
DEFAULT_CACHE_TTL_SHORT = 2 * 60       # 2 minutes for frequently changing data
DEFAULT_CACHE_TTL_MEDIUM = 15 * 60     # 15 minutes for less volatile data
DEFAULT_CACHE_TTL_LONG = 6 * 60 * 60   # 6 hours for static data (like creation info)

# --- Internal Helper Function ---

# --- Internal Helper Function ---

def _make_birdeye_request(
    endpoint_path: str,
    api_base: str, # Specify which base URL to use (BDS_BASE_V3_URL, BDS_BASE_LEGACY_URL, etc.)
    params: Optional[Dict] = None,
    specific_endpoint_name: Optional[str] = None,
    http_method: Callable = requests.get, # <-- This line uses Callable
    calling_script_override: Optional[str] = None,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_MEDIUM,
    chain: str = "solana", # Default chain, used primarily for headers in legacy/trader APIs
    is_batch_metadata: bool = False # Special flag for metadata batch cache key
) -> Optional[Any]:
    """
    Internal function to handle Birdeye API requests via api_usage_limiter.
    Manages URL construction, headers, caching, and basic response validation.
    """
    url = f"{api_base}{endpoint_path}"
    query_params = {k: v for k, v in (params or {}).items() if v is not None}
    headers = BASE_HEADERS.copy()
    request_kwargs = {"timeout": 30, "headers": headers} # Base request args

    # Handle chain parameter placement (header vs query param)
    if api_base in [BDS_BASE_LEGACY_URL, BDS_BASE_TRADER_URL]:
        headers["x-chain"] = chain
        # Remove chain from query_params if it exists, as it's handled by header
        query_params.pop("chain", None)
    elif api_base == BDS_BASE_V3_URL:
        # V3 uses chain as a query parameter
        query_params["chain"] = chain

    # Build URL with query params for GET requests
    if http_method == requests.get and query_params:
        encoded_params = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote)
        url += f"?{encoded_params}"
    elif http_method == requests.post:
        request_kwargs["json"] = query_params # Send params as JSON body for POST

    # Determine calling script dynamically if not provided
    calling_script = calling_script_override
    if calling_script is None:
        try:
            import traceback
            frame = traceback.extract_stack(limit=3)[1] # Go back one more frame
            calling_script = os.path.basename(frame.filename) if frame.filename else "unknown_script"
        except Exception:
            calling_script = "unknown_caller"

    # Generate Cache Key
    cache_key = None
    if CACHE_ENABLED and cache_manager:
        param_str_parts = []
        # Special handling for metadata batch
        if is_batch_metadata and 'list_address' in query_params:
             # Hash the long list of addresses
             mints_hash = hashlib.sha256(query_params['list_address'].encode()).hexdigest()[:16]
             param_str_parts.append(f"list_address_hash={mints_hash}")
             # Include other params if any
             param_str_parts.extend(sorted([f"{k}={v}" for k, v in query_params.items() if k != 'list_address']))
        else:
            param_str_parts = sorted([f"{k}={v}" for k, v in query_params.items()])

        param_str = "&".join(param_str_parts)
        endpoint_name_safe = specific_endpoint_name or endpoint_path.replace('/', '_').strip('_')
        cache_key = f"bds_api_{endpoint_name_safe}_{param_str}_{chain}"


    # Make request via limiter
    response_data = api_usage_limiter.request_api(
        method=http_method,
        api_name="birdeye",
        endpoint_name=specific_endpoint_name or endpoint_path.replace('/', '_').strip('_'),
        calling_script=calling_script,
        cache_key=cache_key,
        cache_ttl_seconds=cache_ttl_seconds,
        url=url,
        **request_kwargs # Pass headers, timeout, json body etc.
    )

    # Validate and return data field
    if isinstance(response_data, dict) and response_data.get("success"):
        return response_data.get("data")
    elif isinstance(response_data, dict):
        # Limiter usually logs errors, but we can add more context here if needed
        error_msg = response_data.get('message', 'Unknown Birdeye error')
        status_code = response_data.get('statusCode', 'N/A')
        # Be less verbose for common 'not found' errors on specific endpoints
        if not (specific_endpoint_name == "trader_txs" and status_code == 404):
             print(f"    [!] birdeye_api ({specific_endpoint_name}): Request failed (Success=False, Status={status_code}, Msg='{error_msg}') for URL: {url}")
    elif response_data is not None:
         print(f"    [!] birdeye_api ({specific_endpoint_name}): Unexpected response type from limiter: {type(response_data)}")

    return None

# --- Public API Functions ---

# == V3 Endpoints ==

def market_data(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches live price, liquidity, market cap, etc. for a token."""
    return _make_birdeye_request(
        endpoint_path="/token/market-data",
        api_base=BDS_BASE_V3_URL,
        params={"address": mint_address},
        specific_endpoint_name="v3_market_data",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_SHORT,
        chain=chain
    )

def trade_data(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches 24h volume, transaction counts, and price change stats."""
    return _make_birdeye_request(
        endpoint_path="/token/trade-data/single",
        api_base=BDS_BASE_V3_URL,
        params={"address": mint_address},
        specific_endpoint_name="v3_trade_data",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_SHORT,
        chain=chain
    )

def holders(mint_address: str, limit: int = 20, offset: int = 0, chain: str = "solana") -> Optional[Dict]:
    """Fetches a list of top token holders."""
    return _make_birdeye_request(
        endpoint_path="/token/holder",
        api_base=BDS_BASE_V3_URL,
        params={"address": mint_address, "limit": limit, "offset": offset},
        specific_endpoint_name="v3_holders",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_MEDIUM,
        chain=chain
    )

def metadata_batch(mint_addresses: List[str], chain: str = "solana") -> Optional[Dict]:
    """Fetches metadata (symbol, name, decimals) for multiple tokens."""
    if not mint_addresses:
        return {}
    if len(mint_addresses) > 50:
        print(f"    [!] birdeye_api (metadata_batch): Requested {len(mint_addresses)} mints, exceeding Birdeye limit of 50. Truncating.")
        mint_addresses = mint_addresses[:50]

    # URL-encode each address individually before joining
    encoded_mints = [urllib.parse.quote(mint, safe="") for mint in mint_addresses]
    mints_param_value = ",".join(encoded_mints)

    return _make_birdeye_request(
        endpoint_path="/token/meta-data/multiple",
        api_base=BDS_BASE_V3_URL,
        params={"list_address": mints_param_value}, # Use the joined, encoded string
        specific_endpoint_name="v3_metadata_multiple",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_LONG,
        chain=chain, # Chain needed for V3 here too
        is_batch_metadata=True # Use special cache key logic
    )

# == Legacy Endpoints ==

def tokenlist_page(offset: int = 0, limit: int = 50) -> Optional[Dict]:
    """Fetches a page of tokens from the global list (often used for discovery)."""
    # Note: Tokenlist doesn't strictly need a chain, but Birdeye might default or prefer it.
    # We use the legacy base URL as per original scripts.
    return _make_birdeye_request(
        endpoint_path="/tokenlist",
        api_base=BDS_BASE_LEGACY_URL, # Uses /defi/tokenlist
        params={"offset": offset, "limit": limit},
        specific_endpoint_name="legacy_tokenlist",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_MEDIUM,
        chain="solana" # Provide default chain, though may not be strictly necessary here
    )

def new_listings_v2(limit: int = 100) -> Optional[List[Dict]]:
    """Fetches recently listed tokens across all supported DEXes using v2 endpoint."""
    return _make_birdeye_request(
        endpoint_path="/tokens/new_listing",
        api_base=BDS_BASE_V3_URL,  # Still using V3 base since it's under /defi/v2 but hosted on same domain
        params={"limit": limit},
        specific_endpoint_name="v2_new_listing",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_SHORT,
        chain="solana"
    )


def token_overview(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches a general overview including metadata (often a fallback)."""
    return _make_birdeye_request(
        endpoint_path="/token_overview",
        api_base=BDS_BASE_LEGACY_URL, # Uses /defi/token_overview
        params={"address": mint_address},
        specific_endpoint_name="legacy_token_overview",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_MEDIUM,
        chain=chain # Provide chain, legacy endpoint might use it
    )

def token_security(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches security flags like mutable metadata and freeze authority."""
    return _make_birdeye_request(
        endpoint_path="/token_security",
        api_base=BDS_BASE_LEGACY_URL, # Uses /defi/token_security
        params={"address": mint_address},
        specific_endpoint_name="legacy_token_security",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_LONG, # Security info changes less often
        chain=chain # Pass chain for x-chain header
    )

def token_creation_info(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches token creation timestamp and creator address."""
    return _make_birdeye_request(
        endpoint_path="/token_creation_info",
        api_base=BDS_BASE_LEGACY_URL, # Uses /defi/token_creation_info
        params={"address": mint_address},
        specific_endpoint_name="legacy_token_creation_info",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_LONG, # Creation info is static
        chain=chain # Pass chain for x-chain header
    )

def price(mint_address: str, chain: str = "solana") -> Optional[Dict]:
    """Fetches just the current price of a token."""
    return _make_birdeye_request(
        endpoint_path="/price",
        api_base=BDS_BASE_LEGACY_URL, # Uses /defi/price
        params={"address": mint_address},
        specific_endpoint_name="legacy_price",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_SHORT, # Price changes often
        chain=chain # Pass chain for x-chain header
    )

def get_token_trades(mint_address: str, limit: int = 50, offset: int = 0, sort_by: str = "block_timestamp", sort_type: str = "desc", chain: str = "solana") -> Optional[Any]:
    """
    Fetches a list of trades for a specified token using the /defi/txs/token endpoint.
    The _make_birdeye_request function returns the 'data' field from the Birdeye response.
    """
    # Based on documentation: https://public-api.birdeye.so/defi/txs/token
    # The mint address is passed as 'address' in params.
    # The chain is handled by the x-chain header for BDS_BASE_LEGACY_URL.
    return _make_birdeye_request(
        endpoint_path="/txs/token", # Corrected endpoint path
        api_base=BDS_BASE_LEGACY_URL, # Corrected base URL
        params={
            "address": mint_address, # Mint address as a parameter
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,   # e.g., "block_timestamp"
            "sort_type": sort_type # e.g., "desc" or "asc"
        },
        specific_endpoint_name="defi_token_txs", # More specific name
        cache_ttl_seconds=DEFAULT_CACHE_TTL_SHORT, # Trades change frequently
        chain=chain # x-chain header will be used
    )


# == Trader Endpoint (Top Level Base URL) ==

def trader_txs(
    trader_address: str,
    after_time: Optional[int] = None,
    before_time: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    chain: str = "solana"
) -> Optional[Dict]:
    """
    Fetches transaction history for a specific trader wallet, filterable by time.
    Note: Filtering by token address happens *after* fetching in consuming code.
    """
    params = {
        "address": trader_address,
        "limit": limit,
        "offset": offset
    }
    if after_time is not None:
        params["after_time"] = after_time
    if before_time is not None:
        params["before_time"] = before_time

    # This endpoint uses the top-level base URL and x-chain header
    return _make_birdeye_request(
        endpoint_path="/trader/txs/seek_by_time",
        api_base=BDS_BASE_TRADER_URL, # Uses base https://public-api.birdeye.so
        params=params,
        specific_endpoint_name="trader_txs",
        cache_ttl_seconds=DEFAULT_CACHE_TTL_MEDIUM, # Trader history can be cached moderately
        chain=chain # Pass chain for x-chain header
    )


# --- Example Usage / Testing ---
if __name__ == "__main__":
    print("\n--- Birdeye API Gateway Test ---")
    example_mint = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzL7xeQu2d25qg" # Example: PYTH
    example_trader = "3QtUb3Acif4ghNBYMCXBgXo4j1mCua9Rx4CwEuzHRCf9" # Example trader

    print(f"\n[*] Testing V3 Market Data for {example_mint[:6]}...")
    m_data = market_data(example_mint)
    if m_data:
        print(f"    Price: {m_data.get('price')}, Liq: {m_data.get('liquidity')}, MC: {m_data.get('marketCap')}")
    else:
        print("    Failed to fetch market data.")

    print(f"\n[*] Testing V3 Trade Data for {example_mint[:6]}...")
    t_data = trade_data(example_mint)
    if t_data:
        print(f"    Vol 24h: {t_data.get('v24hUSD', t_data.get('volume',{}).get('h24'))}, Txns 24h: {t_data.get('txns24h', t_data.get('txnCount24h'))}")
        print(f"    Price Change % (1h/6h/24h): {t_data.get('priceChangePercent', {}).get('h1')}% / {t_data.get('priceChangePercent', {}).get('h6')}% / {t_data.get('priceChangePercent', {}).get('h24')}%")
    else:
        print("    Failed to fetch trade data.")

    print(f"\n[*] Testing V3 Holders for {example_mint[:6]} (Limit 3)...")
    h_data = holders(example_mint, limit=3)
    if h_data and isinstance(h_data.get('items'), list):
        print(f"    Found {len(h_data['items'])} holders.")
        for i, holder_item in enumerate(h_data['items']):
            print(f"      {i+1}. Owner: {holder_item.get('owner')}, Amount: {holder_item.get('uiAmountString', 'N/A')}")
    else:
        print("    Failed to fetch holders.")

    print(f"\n[*] Testing Legacy Security for {example_mint[:6]}...")
    sec_data = token_security(example_mint)
    if sec_data:
        print(f"    Mutable: {sec_data.get('mutableMetadata')}, Freezeable: {sec_data.get('freezeable')}, Freeze Auth: {sec_data.get('freezeAuthority')}")
    else:
        print("    Failed to fetch security data.")

    print(f"\n[*] Testing Trader TXs for {example_trader[:6]}...")
    # Get timestamp for 1 day ago
    one_day_ago = int(time.time()) - (24 * 60 * 60)
    tr_txs_data = trader_txs(example_trader, after_time=one_day_ago, limit=5)
    if tr_txs_data and isinstance(tr_txs_data.get("items"), list):
        print(f"    Found {len(tr_txs_data['items'])} transactions for trader in the last 24h (limit 5).")
        # Further filtering by token would happen here in the calling script
    elif tr_txs_data and isinstance(tr_txs_data.get("items"), list) and not tr_txs_data["items"]:
         print("    No transactions found for trader in the last 24h.")
    else:
        print("    Failed to fetch trader transactions or API returned non-list.")

    print("\n--- Test Complete ---")


# --- END OF FILE birdeye_api.py ---