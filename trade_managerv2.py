
# trade_manager_v2_nb.py
# Liquidity Geometry Trade Manager — Non‑blocking edition
# - Correct IO: SCENARIOS_IN -> SCENARIOS_EXEC_IN
# - Never hangs on API calls (strict timeouts + page caps + global budget)
# - Env toggles to skip heavy endpoints when rate-limited or offline
from __future__ import annotations

import os, json, time, statistics, uuid, threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import requests

# --------------------
# ENV / KNOBS
# --------------------
from dotenv import load_dotenv
load_dotenv(override=True)

SCENARIOS_IN      = os.getenv("SCENARIOS_IN", "scenarios.jsonl")                 # from Bottom Detector
SCENARIOS_EXEC_IN = os.getenv("SCENARIOS_EXEC_IN", "scenarios_exec_in.jsonl")    # to wallet_sizer
EXEC_CONTROL  = os.getenv("EXEC_CONTROL", "exec_control.jsonl")
OPEN_POS_DIR  = os.getenv("OPEN_POS_DIR", "open_positions")
DEADLIST_PATH = os.getenv("DEADLIST_PATH", "deadlist.json")
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))

# APIs
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
HELIUS_API_KEY  = os.getenv("HELIUS_API_KEY", "")
JUPITER_QUOTE_URL = os.getenv("JUPITER_QUOTE", "https://quote-api.jup.ag/v6/quote")

USDC_MINT_SOL = os.getenv("USDC_MINT_SOL", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
WSOL_MINT_SOL = os.getenv("WSOL_MINT_SOL", "So11111111111111111111111111111111111111112")
USDT_MINT_SOL = os.getenv("USDT_MINT_SOL", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")

# Geometry weights → Sustainability Score S
STRUCTURE_W = 0.35
DEPTH_W     = 0.35
DYNAMICS_W  = 0.20
EXIT_W      = 0.10

# Gating thresholds
GREEN_MIN_S = float(os.getenv("GREEN_MIN_S", "70"))
YELLOW_MIN_S = float(os.getenv("YELLOW_MIN_S", "45"))

# Route probe config
ROUTE_TESTS_USD = [float(x) for x in os.getenv("ROUTE_TESTS_USD", "25,50,100,200").split(",")]
ROUTE_SLIPPAGE_BPS = int(os.getenv("ROUTE_SLIPPAGE_BPS", "200"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "4.0"))  # hard timeout per request

# Dispatch sizing
BASE_POSITION_SIZE = float(os.getenv("BASE_POSITION_SIZE", "100"))
MAX_POSITION_SIZE  = float(os.getenv("MAX_POSITION_SIZE", "300"))
RR_SIZE_MULTIPLIER = float(os.getenv("RR_SIZE_MULTIPLIER", "1.5"))

# Misc heuristics
SELL_VOLUME_LOOKBACK_SEC = int(os.getenv("SELL_VOLUME_LOOKBACK_SEC", "3600"))
MIN_SELL_VOLUME_USD      = float(os.getenv("MIN_SELL_VOLUME_USD", "1.0"))

# Non-blocking toggles
BIRDEYE_SELLPRESS_ENABLED = os.getenv("BIRDEYE_SELLPRESS_ENABLED", "1") != "0"
BIRDEYE_SELLPRESS_MAX_PAGES = int(os.getenv("BIRDEYE_SELLPRESS_MAX_PAGES", "2"))  # 2*500 tx max
BIRDEYE_SELLPRESS_TIME_BUDGET = float(os.getenv("BIRDEYE_SELLPRESS_TIME_BUDGET", "2.0"))  # seconds total budget
JUPITER_ENABLED = os.getenv("JUPITER_ENABLED", "1") != "0"

LOOP_SLEEP_SEC = int(os.getenv("PM_LOOP_SLEEP_SEC", "4"))

# --------------------
# Utilities / Cache
# --------------------
def _now() -> int: return int(time.time())

def _touch(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8"):
            pass

for p in (SCENARIOS_IN, SCENARIOS_EXEC_IN, EXEC_CONTROL, DEADLIST_PATH):
    _touch(p)
os.makedirs(OPEN_POS_DIR, exist_ok=True)

class TTLCache:
    def __init__(self, ttl_sec: int = 180):
        self.ttl = ttl_sec
        self._d: Dict[str, Tuple[int, object]] = {}
        self._lock = threading.Lock()
    def get(self, key: str):
        with self._lock:
            it = self._d.get(key)
            if not it: return None
            ts, val = it
            if time.time() - ts > self.ttl:
                self._d.pop(key, None); return None
            return val
    def set(self, key: str, val):
        with self._lock: self._d[key] = (int(time.time()), val)

CACHE_DS = TTLCache(ttl_sec=180)
CACHE_BE = TTLCache(ttl_sec=120)
CACHE_HOLDERS = TTLCache(ttl_sec=600)

def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f: f.write(json.dumps(obj) + "\n")

# --------------------
# External APIs (non-blocking)
# --------------------
def _be_headers():
    return {"accept":"application/json","x-chain":"solana","X-API-KEY": BIRDEYE_API_KEY}

def ds_meta(mint: str) -> dict:
    """Dexscreener meta for best Solana pair."""
    key = f"ds:{mint}"
    c = CACHE_DS.get(key)
    if c is not None: return c
    out = {}
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=HTTP_TIMEOUT_SEC)
        if r.status_code == 200:
            pairs = (r.json() or {}).get("pairs") or []
            pairs = [p for p in pairs if p.get("chainId") == "solana"]
            if pairs:
                pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
                best = pairs[0]
                out = {
                    "pair": best.get("pairAddress"),
                    "quote": (best.get("quoteToken") or {}).get("address") or USDC_MINT_SOL,
                    "mc": float(best.get("marketCap") or 0.0),
                    "liq_usd": float((best.get("liquidity") or {}).get("usd") or 0.0),
                    "m5_buys": int((best.get("txns",{}).get("m5",{}).get("buys",0))),
                    "m5_sells": int((best.get("txns",{}).get("m5",{}).get("sells",0))),
                    "priceUsd": float(best.get("priceUsd") or 0.0),
                    "created_ms": int(best.get("pairCreatedAt") or 0),
                    "symbol": (best.get("baseToken") or {}).get("symbol") or mint[:6]
                }
    except Exception:
        out = {}
    CACHE_DS.set(key, out)
    return out

def be_spot_price(mint: str) -> Optional[float]:
    key = f"be:px:{mint}"
    c = CACHE_BE.get(key)
    if c is not None: return c
    try:
        if not BIRDEYE_API_KEY: return None
        r = requests.get("https://public-api.birdeye.so/defi/price", params={"address": mint}, headers=_be_headers(), timeout=HTTP_TIMEOUT_SEC)
        if r.status_code == 200:
            val = ((r.json() or {}).get("data") or {}).get("value")
            px = float(val) if val else None
            CACHE_BE.set(key, px)
            return px
    except Exception:
        return None
    return None

def fetch_top_holders(mint: str, limit: int = 20) -> List[Tuple[str, float]]:
    """(owner_wallet, ui_amount). Uses Birdeye v3 holder endpoint; returns at most 'limit'."""
    key = f"holders:{mint}:{limit}"
    c = CACHE_HOLDERS.get(key)
    if c is not None: return c
    out: List[Tuple[str, float]] = []
    if not BIRDEYE_API_KEY:
        CACHE_HOLDERS.set(key, out); return out
    try:
        offset = 0
        while len(out) < limit:
            page = min(100, limit - len(out))
            r = requests.get(
                "https://public-api.birdeye.so/defi/v3/token/holder",
                params={"address": mint, "offset": offset, "limit": page, "ui_amount_mode": "scaled"},
                headers=_be_headers(),
                timeout=HTTP_TIMEOUT_SEC
            )
            if r.status_code != 200: break
            items = ((r.json() or {}).get("data") or {}).get("items") or []
            if not items: break
            for it in items:
                owner = it.get("owner") or it.get("holder") or it.get("address")
                amt = float(it.get("uiAmount") or it.get("ui_amount") or it.get("amount") or 0.0)
                if owner and amt>0: out.append((owner, amt))
            offset += len(items)
            if len(items) < page: break
    except Exception:
        pass
    CACHE_HOLDERS.set(key, out[:limit])
    return out[:limit]

# --------------------
# Geometry metrics
# --------------------
def topk_share(holders: List[Tuple[str,float]], k: int) -> float:
    if not holders: return 1.0
    holders_sorted = sorted(holders, key=lambda x: x[1], reverse=True)
    total = sum(a for _,a in holders_sorted) or 1.0
    return sum(a for _,a in holders_sorted[:k]) / total

def hhi(holders: List[Tuple[str,float]], limit: int = 50) -> float:
    if not holders: return 1.0
    total = sum(a for _,a in holders) or 1.0
    shares = [(a/total) for _,a in holders[:limit]]
    return sum(s*s for s in shares)

def jupiter_slippage_estimate(mint: str, quote_mint: Optional[str], usd_sizes: List[float]) -> Tuple[List[float], List[float]]:
    """
    Query Jupiter quote for a set of USD notional buy sizes into the base mint.
    Returns (sizes, slippages as fractions 0..1). Best effort fallback based on liquidity.
    """
    slippages, sizes = [], []
    if not quote_mint: quote_mint = USDC_MINT_SOL
    px = be_spot_price(mint) or ds_meta(mint).get("priceUsd") or 0.0
    for usd in usd_sizes:
        try:
            if JUPITER_ENABLED and px > 0:
                amount = int(usd * (10**6))  # USDC precision
                params = {"inputMint": quote_mint, "outputMint": mint, "amount": amount, "slippageBps": int(ROUTE_SLIPPAGE_BPS)}
                r = requests.get(JUPITER_QUOTE_URL, params=params, timeout=HTTP_TIMEOUT_SEC)
                if r.status_code == 200:
                    data = r.json() or {}
                    impact = float(data.get("priceImpactPct") or 0.0)
                    slippages.append(max(0.0, min(1.0, abs(impact))))
                    sizes.append(usd); continue
        except Exception:
            pass
        # Fallback proxy from liquidity bucket
        liq_usd = ds_meta(mint).get("liq_usd") or 1.0
        proxy = min(0.99, max(0.005, usd / max(liq_usd, 1.0)))
        slippages.append(proxy); sizes.append(usd)
    return sizes, slippages

def curvature_from_slippage(sizes: List[float], slippages: List[float]) -> float:
    if len(sizes) < 3: return 0.0
    pairs = sorted(zip(sizes, slippages), key=lambda x: x[0])
    slip = [p[1] for p in pairs]
    curvs = []
    for i in range(1, len(slip)-1):
        curv = slip[i-1] - 2*slip[i] + slip[i+1]
        curvs.append(curv)
    return statistics.median(curvs) if curvs else 0.0

def sell_pressure_ratio_v3(mint: str) -> Tuple[float, int, str]:
    """
    Non-blocking sell pressure estimator. Respects page & time budgets.
    Returns (ratio, n_items, mode) where mode ∈ {"ok","fallback_no_api","fallback_budget","disabled"}.
    """
    if not BIRDEYE_SELLPRESS_ENABLED:
        return 0.5, 0, "disabled"
    if not BIRDEYE_API_KEY:
        return 0.5, 0, "fallback_no_api"

    base_url = "https://public-api.birdeye.so/defi/v3/token/txs-by-volume"
    after_time = _now() - SELL_VOLUME_LOOKBACK_SEC
    t0 = time.monotonic()

    def fetch(tx_type: str) -> Tuple[float,int]:
        total_usd=0.0; count=0; offset=0; pages=0
        while pages < BIRDEYE_SELLPRESS_MAX_PAGES:
            if time.monotonic() - t0 > BIRDEYE_SELLPRESS_TIME_BUDGET:
                break
            params = {
                "token_address": mint,
                "volume_type": "usd",
                "tx_type": tx_type,
                "min_volume": MIN_SELL_VOLUME_USD,
                "after_time": after_time,
                "sort_by": "block_unix_time",
                "sort_type": "desc",
                "offset": offset,
                "limit": 500,
                "ui_amount_mode": "scaled",
            }
            try:
                r = requests.get(base_url, params=params, headers=_be_headers(), timeout=HTTP_TIMEOUT_SEC)
                if r.status_code != 200: break
                data = (r.json() or {}).get("data") or {}
                items = data.get("items") or []
                if not items: break
                for it in items:
                    try:
                        total_usd += float(it.get("volume_usd") or 0.0); count += 1
                    except Exception: pass
                if len(items) < 500: break
                offset += len(items); pages += 1
            except Exception:
                break
        return total_usd, count

    sell_usd, _ = fetch("sell")
    tot_usd, n  = fetch("all")
    elapsed = time.monotonic() - t0
    if elapsed > BIRDEYE_SELLPRESS_TIME_BUDGET:
        mode = "fallback_budget"
    else:
        mode = "ok"
    if tot_usd <= 0:
        return 0.5, 0, mode
    return max(0.0, min(1.0, sell_usd/tot_usd)), n, mode

# --------------------
# Sustainability Score
# --------------------
@dataclass
class GeoFeatures:
    top10_share: float = 1.0
    hhi: float = 1.0
    depth_1_5: float = 0.0  # inverse slippage proxy near 1–5% band
    curvature: float = 0.0  # higher => worse
    asymmetry: float = 1.0  # buy depth vs sell depth proxy; 1=balanced, >1 buy-dominant
    lp_flow: float = 0.0    # approx via m5 buys - sells (scaled)
    sell_pressure: float = 0.5
    overhang_ratio: float = 0.8  # lower better

def score_structure(g: GeoFeatures) -> float:
    s_top10 = max(0.0, 1.0 - min(1.0, g.top10_share))
    norm_hhi = min(1.0, max(0.0, (0.1 - g.hhi) / 0.1))
    return 100.0 * (0.6*s_top10 + 0.4*norm_hhi)

def score_depth(g: GeoFeatures) -> float:
    inv_slip = max(0.0, min(1.0, g.depth_1_5))
    curv_pen = max(0.0, min(1.0, 1.0 - min(1.0, abs(g.curvature)*5)))
    asym = max(0.0, min(1.0, 1.0 - min(1.0, abs(g.asymmetry - 1.0))))
    return 100.0 * (0.5*inv_slip + 0.3*curv_pen + 0.2*asym)

def score_dynamics(g: GeoFeatures) -> float:
    flow = max(0.0, min(1.0, 0.5 + g.lp_flow/200.0))
    sell_ok = max(0.0, min(1.0, 1.0 - g.sell_pressure))
    return 100.0 * (0.6*flow + 0.4*sell_ok)

def score_exit(g: GeoFeatures) -> float:
    return 100.0 * max(0.0, min(1.0, 1.0 - min(1.0, g.overhang_ratio)))

def sustainability_score(g: GeoFeatures) -> float:
    s = STRUCTURE_W*score_structure(g) + DEPTH_W*score_depth(g) + DYNAMICS_W*score_dynamics(g) + EXIT_W*score_exit(g)
    return max(0.0, min(100.0, s))

# --------------------
# Feature extraction
# --------------------
def features_for_mint(mint: str) -> Tuple[GeoFeatures, dict]:
    meta = ds_meta(mint)
    quote = meta.get("quote") or USDC_MINT_SOL
    g = GeoFeatures()
    diag = {"warnings": []}

    # Holders geometry
    holders = fetch_top_holders(mint, limit=20)
    if holders:
        g.top10_share = topk_share(holders, 10)
        g.hhi = hhi(holders, limit=20)
    else:
        diag["warnings"].append("no_holders_data")

    # Depth & curvature via route probe (slippage as proxy)
    sizes, slips = jupiter_slippage_estimate(mint, quote, ROUTE_TESTS_USD)
    if slips:
        inv = 1.0 - max(0.0, min(1.0, slips[0]))
        g.depth_1_5 = inv
        g.curvature = curvature_from_slippage(sizes, slips)
    else:
        diag["warnings"].append("no_slippage_data")

    # Asymmetry + LP flow via Dexscreener m5
    m5_b = meta.get("m5_buys") or 0
    m5_s = meta.get("m5_sells") or 0
    g.asymmetry = (m5_b+1)/(m5_s+1)
    g.lp_flow = float(m5_b - m5_s)

    # Sell pressure via Birdeye v3 (non-blocking)
    sp, n, mode = sell_pressure_ratio_v3(mint)
    g.sell_pressure = sp
    if mode != "ok":
        diag["warnings"].append(f"sell_pressure:{mode}")

    # Exit overhang (approx)
    g.overhang_ratio = min(1.5, max(0.0, 0.5*g.top10_share + 0.8*g.sell_pressure))

    return g, {"meta": meta, "slippage_samples": {"sizes": sizes, "slippages": slips}, "holders_n": len(holders), **diag}

# --------------------
# Dispatch logic
# --------------------
def concurrency_ok() -> bool:
    try:
        n = sum(1 for fn in os.listdir(OPEN_POS_DIR) if fn.endswith('.json'))
        return n < MAX_CONCURRENT_TRADES
    except Exception:
        return True

def dynamic_size_from_score(S: float) -> float:
    mult = 1.0
    if S >= 85: mult = RR_SIZE_MULTIPLIER
    elif S >= 75: mult = 1.2
    return min(MAX_POSITION_SIZE, BASE_POSITION_SIZE * mult)

def should_dispatch(S: float, meta: dict) -> Tuple[bool, str]:
    liq = meta.get("liq_usd") or 0
    if S >= GREEN_MIN_S and liq >= 15000:
        return True, "green_ok"
    if S >= YELLOW_MIN_S and liq >= 30000:
        return True, "yellow_ok_high_liq"
    return False, "score_or_liq_insufficient"

# --------------------
# IO: scenarios tailer
# --------------------
class Tailer:
    def __init__(self, path: str):
        self.path = path; self.pos = 0; _touch(path)
    def read_new(self) -> List[Dict]:
        out = []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                f.seek(self.pos)
                for line in f:
                    s = line.strip()
                    if not s: continue
                    try: out.append(json.loads(s))
                    except json.JSONDecodeError: pass
                self.pos = f.tell()
        except FileNotFoundError:
            _touch(self.path); self.pos = 0
        return out

# --------------------
# Main loop
# --------------------
def handle_candidate(scn: dict):
    mint = scn.get("mint")
    if not mint: return

    g, diag = features_for_mint(mint)
    S = sustainability_score(g)
    meta = diag.get("meta") or {}

    ok, reason = should_dispatch(S, meta)
    if not concurrency_ok():
        reason = "capacity_reached"

    analysis = {
        "mint": mint,
        "symbol": scn.get("symbol") or meta.get("symbol") or mint[:6],
        "S": round(S,2),
        "features": g.__dict__,
        "ds_meta": meta,
        "diag": diag,
        "policy_reason": reason,
        "ts": _now(),
    }

    enriched = {
        "id": scn.get("id") or str(uuid.uuid4()),
        "mint": mint,
        "symbol": analysis["symbol"],
        "side": scn.get("side","buy"),
        "size_usd": scn.get("size_usd") or dynamic_size_from_score(S),
        "signal_ts": scn.get("signal_ts") or _now(),
        "analysis": analysis,
        "raw_scn": scn,
    }

    if ok and concurrency_ok():
        _append_jsonl(SCENARIOS_EXEC_IN, enriched)
        print(f"[DISPATCH->EXEC_IN] {analysis['symbol']} S={S:.1f} liq=${meta.get('liq_usd',0):.0f} reason={reason}")
    else:
        print(f"[SKIP] {analysis['symbol']} S={S:.1f} reason={reason}")

def main():
    print("[tm2-nb] Liquidity Geometry manager (non-blocking). IO: SCENARIOS_IN -> SCENARIOS_EXEC_IN")
    tail = Tailer(SCENARIOS_IN)
    while True:
        try:
            for scn in tail.read_new():
                handle_candidate(scn)
        except Exception as e:
            print(f"[loop] error: {e}")
        time.sleep(LOOP_SLEEP_SEC)

if __name__ == "__main__":
    main()
