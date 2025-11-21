# --- START OF FILE gmgn_api.py ---

import os
import random
import time
import json
import requests # Needed for exception types, even if direct calls are removed
import tls_client # type: ignore
from fake_useragent import UserAgent # type: ignore
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv # Import dotenv

# --- Import Cache Manager ---
try:
    # Import is checked to set CACHE_ENABLED, even if module not used directly later.
    import cache_manager # pylint: disable=unused-import # noqa
    CACHE_ENABLED = True
    print("[✓] cache_manager imported (for fallback/info) for GMGN API.")
except ImportError:
    print("ERROR: cache_manager.py not found. GMGN API caching disabled if limiter fails.")
    CACHE_ENABLED = False
    cache_manager = None

# --- API Usage Limiter Import ---
try:
    import api_usage_limiter
    print("[✓] Importing api_usage_limiter for gmgn_api...")
except ImportError:
    print("CRITICAL: api_usage_limiter.py not found! GMGN API calls will not be managed.")
    class ApiUsageLimiterStub:
        def request_api(*args, **kwargs) -> None:
            print("    [STUB] api_usage_limiter.request_api called - LIMITER MISSING")
            return None
    api_usage_limiter = ApiUsageLimiterStub() # type: ignore
# --- End Limiter Import ---

class gmgn:
    BASE_URL = "https://gmgn.ai/defi/quotation"
    TTL_STATIC_INFO = 6 * 60 * 60
    TTL_WALLET_PROFILES = 3 * 60 * 60
    TTL_TRENDING_DYNAMIC = 15 * 60
    TTL_PRICE_GAS = 2 * 60

    def __init__(self):
        try:
            # Removed 'path=' argument for broader compatibility
            self.ua = UserAgent(fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
        except Exception as e: print(f"[!] Warning: Failed to initialize UserAgent: {e}. Using fallback."); self.ua = None
        self.sendRequest = tls_client.Session(random_tls_extension_order=True)

    def randomiseRequestHeaders(self):
        # ... (implementation unchanged) ...
        common_identifiers = ['chrome_124', 'chrome_123', 'chrome_120','firefox_125', 'firefox_124', 'firefox_120','safari_17_0', 'safari_16_5']
        self.identifier = random.choice(common_identifiers)
        self.sendRequest.client_identifier = self.identifier
        user_agent_str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        if self.ua:
            try:
                browser_name = self.identifier.split('_')[0]
                if browser_name == 'chrome': user_agent_str = self.ua.chrome
                elif browser_name == 'firefox': user_agent_str = self.ua.firefox
                elif browser_name == 'safari': user_agent_str = self.ua.safari
                else: user_agent_str = self.ua.random
            except Exception: user_agent_str = self.ua.random
        self.headers = {
            'Host': 'gmgn.ai','accept': 'application/json, text/plain, */*',
            'accept-language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.8', 'en;q=0.7']), 'dnt': '1',
            'priority': 'u=1, i', 'referer': 'https://gmgn.ai/?chain=sol',
            'sec-ch-ua': f'"{random.choice(["Not/A)Brand", "Chromium"])}";v="99", "Google Chrome";v="{self.identifier.split("_")[1].split(".")[0] if "chrome" in self.identifier else "124"}", "Microsoft Edge";v="{self.identifier.split("_")[1].split(".")[0] if "chrome" in self.identifier else "124"}"',
            'sec-ch-ua-mobile': '?0','sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors','sec-fetch-site': 'same-origin','user-agent': user_agent_str
        }


    # ---------------------------------------------------------------------------
    # _make_request - USES LIMITER
    # ---------------------------------------------------------------------------
    def _make_request(self, url: str, ttl_seconds: int) -> Optional[Dict]:
        """Internal helper uses api_usage_limiter for requests and caching."""
        cache_key = f"gmgn_{url}"
        endpoint_name = url.split(self.BASE_URL + "/", 1)[1] if self.BASE_URL in url else url.split('/')[-1]

        self.randomiseRequestHeaders()
        parsed_json = api_usage_limiter.request_api(
            method=self.sendRequest.get, api_name="gmgn", endpoint_name=endpoint_name,
            calling_script="gmgn_api._make_request", cache_key=cache_key,
            cache_ttl_seconds=ttl_seconds, url=url, headers=self.headers
        )

        if parsed_json is None: return None
        elif not isinstance(parsed_json, dict):
             print(f"    [!] GMGN Warning: Limiter returned non-dict ({type(parsed_json)}) for {endpoint_name}. Ignoring.")
             return None
        else: return parsed_json

    # --- Specific Endpoint Methods ---
    # (These remain unchanged as they call the modified _make_request)
    def getTokenInfo(self, contractAddress: str) -> Optional[Dict]:
        if not contractAddress: print("    [!] GMGN Error: Contract address needed."); return None
        url = f"{self.BASE_URL}/v1/tokens/sol/{contractAddress}"
        return self._make_request(url, ttl_seconds=self.TTL_STATIC_INFO)

    def getNewPairs(self, limit: int = 50) -> Optional[List[Dict]]:
        if limit > 50: print("    [!] GMGN Warning: Limit capped at 50."); limit = 50
        url = f"{self.BASE_URL}/v1/pairs/sol/new_pairs?limit={limit}&orderby=open_timestamp&direction=desc&filters[]=not_honeypot"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_TRENDING_DYNAMIC)
        return jsonResponse.get('data', {}).get('pairs') if isinstance(jsonResponse, dict) else None

    def getTrendingWallets(self, timeframe: str = "7d", walletTag: str = "smart_degen") -> Optional[List[Dict]]:
        valid_tags=["pump_smart","smart_degen","reowned","snipe_bot"]; valid_times=["1d","7d","30d"]
        if timeframe not in valid_times: timeframe = "7d"
        if walletTag not in valid_tags: walletTag = "smart_degen"
        url = f"{self.BASE_URL}/v1/rank/sol/wallets/{timeframe}?tag={walletTag}&orderby=pnl_{timeframe}&direction=desc"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_TRENDING_DYNAMIC)
        return jsonResponse.get('data', {}).get('rank') if isinstance(jsonResponse, dict) else None

    def getTrendingTokens(self, timeframe: str = "1h") -> Optional[List[Dict]]:
        valid_times=["1m","5m","1h","6h","24h"]; timeframe=timeframe if timeframe in valid_times else "1h"
        url = f"{self.BASE_URL}/v1/rank/sol/swaps/{timeframe}?orderby=swaps&direction=desc"
        if timeframe=="1m": url+="&limit=20"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_TRENDING_DYNAMIC)
        return jsonResponse.get('data', {}).get('rank') if isinstance(jsonResponse, dict) else None

    def getTokensByCompletion(self, limit: int = 50) -> Optional[List[Dict]]:
        if limit > 50: limit=50
        url = f"{self.BASE_URL}/v1/rank/sol/pump?limit={limit}&orderby=progress&direction=desc&pump=true"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_TRENDING_DYNAMIC)
        return jsonResponse.get('data', {}).get('rank') if isinstance(jsonResponse, dict) else None

    def findSnipedTokens(self, size: int = 10) -> Optional[List[Dict]]:
        if size > 39: size=39
        url = f"{self.BASE_URL}/v1/signals/sol/snipe_new?size={size}&is_show_alert=false&featured=false"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_TRENDING_DYNAMIC)
        return jsonResponse.get('data', {}).get('list') if isinstance(jsonResponse, dict) else None

    def getGasFee(self) -> Optional[Dict]:
        url = f"{self.BASE_URL}/v1/chains/sol/gas_price"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_PRICE_GAS)
        return jsonResponse.get('data') if isinstance(jsonResponse, dict) else None

    def getTokenUsdPrice(self, contractAddress: str) -> Optional[Dict]:
        if not contractAddress: print("    [!] GMGN Error: Contract address needed."); return None
        url = f"{self.BASE_URL}/v1/sol/tokens/realtime_token_price?address={contractAddress}"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_PRICE_GAS)
        return jsonResponse.get('data') if isinstance(jsonResponse, dict) else None

    def getTopBuyers(self, contractAddress: str) -> Optional[List[Dict]]:
        if not contractAddress: print("    [!] GMGN Error: Contract address needed."); return None
        url = f"{self.BASE_URL}/v1/tokens/top_buyers/sol/{contractAddress}"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_WALLET_PROFILES)
        return jsonResponse.get('data') if isinstance(jsonResponse, dict) and isinstance(jsonResponse.get('data'), list) else None

    def getSecurityInfo(self, contractAddress: str) -> Optional[Dict]:
        if not contractAddress: print("    [!] GMGN Error: Contract address needed."); return None
        url = f"{self.BASE_URL}/v1/tokens/security/sol/{contractAddress}"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_STATIC_INFO)
        if isinstance(jsonResponse, dict):
            data_field = jsonResponse.get('data')
            if isinstance(data_field, dict): return data_field
            elif 'is_honeypot' in jsonResponse or 'is_mintable' in jsonResponse: return jsonResponse
            else: print(f"    [!] GMGN Warning: Unexpected structure for getSecurityInfo response: {str(jsonResponse)[:100]}..."); return None
        return None

    def getWalletInfo(self, walletAddress: str, period: str = "7d") -> Optional[Dict]:
        periods = ["7d", "30d"];
        if not walletAddress: print("    [!] GMGN Error: Wallet address needed."); return None
        if period not in periods: period = "7d"
        url = f"{self.BASE_URL}/v1/smartmoney/sol/walletNew/{walletAddress}?period={period}"
        jsonResponse = self._make_request(url, ttl_seconds=self.TTL_WALLET_PROFILES)
        return jsonResponse.get('data') if isinstance(jsonResponse, dict) else None

# --- END OF FILE gmgn_api.py ---