# trade_manager.py
# Sits between Monitor and Executor.
# - Enforces MAX_CONCURRENT_TRADES.
# - Enriches candidates with fresh market data (BQ, RR, etc.).
# - Applies repeat entry policies and manages swaps.
# - Maintains a shared deadlist of tokens to ignore.
# --- MODIFIED with Backlog, Safe Eviction, and Watchlist Ingestion ---
# --- EXTENDED with Bagholder, Exhaustion, and Revival Gate Analysis (fully implemented) ---
# --- ENHANCED with PnL optimization features ---

from __future__ import annotations

import os, json, time, math, bisect, random, shutil, statistics, heapq, uuid, threading
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Set, Callable
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# =========================
# ENV / KNOBS
# =========================
# Queues & State
SCENARIOS_IN = os.getenv("SCENARIOS_IN", "scenarios.jsonl")
SCENARIOS_EXEC = os.getenv("SCENARIOS_EXEC", "scenarios_exec_in.jsonl")

EXEC_CONTROL = os.getenv("EXEC_CONTROL", "exec_control.jsonl")
OPEN_POS_DIR = os.getenv("OPEN_POS_DIR", "open_positions")
TRADES_OUT = os.getenv("TRADES_OUT", "trades.jsonl")


SELL_VOLUME_LOOKBACK_SEC = int(os.getenv("SELL_VOLUME_LOOKBACK_SEC", "3600"))  # 1 hour default
MIN_SELL_VOLUME_USD = float(os.getenv("MIN_SELL_VOLUME_USD", "1.0"))  # Ignore tiny sells

# Concurrency & Policy
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))

# Repeat Entry Policy
REPEAT_FILE = os.getenv("REPEAT_FILE", "entry_registry.json")
REPEAT_COOLDOWN_MIN = int(os.getenv("REPEAT_COOLDOWN_MIN", "45"))
REPEAT_MIN_ADVANTAGE = float(os.getenv("REPEAT_MIN_ADVANTAGE", "0.30"))
REPEAT_NEW_LOW_PCT = float(os.getenv("REPEAT_NEW_LOW_PCT", "4.0"))
REPEAT_DECAY_FACTOR = float(os.getenv("REPEAT_DECAY_FACTOR", "0.85"))

# Deadlist (shared)
DEADLIST_PATH = os.getenv("DEADLIST_PATH", "deadlist.json")
DEADLIST_DEFAULT_TTL_MIN = int(os.getenv("DEADLIST_DEFAULT_TTL_MIN", "4320"))


# Add these global variables near the top of the file (around line 50)
_BACKLOG_LAST_ANALYZED: Dict[str, int] = {}  # mint -> timestamp
BACKLOG_CANDIDATE_COOLDOWN_SEC = int(os.getenv("BACKLOG_CANDIDATE_COOLDOWN_SEC", "120"))


# Add lock at the top of the file (around line 50)
_capacity_lock = threading.Lock()

# Market Data (shared)
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")  # Needed for holder analysis
BIRDEYE_BARS_MIN = int(os.getenv("BIRDEYE_BARS_MIN", "240"))
USDC_MINT_SOL = os.getenv("USDC_MINT_SOL", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
WSOL_MINT_SOL = os.getenv("WSOL_MINT_SOL", "So11111111111111111111111111111111111111112")
USDT_MINT_SOL = os.getenv("USDT_MINT_SOL", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
PREFERRED_QUOTES = [USDC_MINT_SOL, WSOL_MINT_SOL, USDT_MINT_SOL]

JUPITER_QUOTE_URL = os.getenv("JUPITER_QUOTE", "https://quote-api.jup.ag/v6/quote")
ROUTE_TEST_USD = float(os.getenv("ROUTE_TEST_USD", "5"))
ROUTE_MAX_IMPACT_PCT = float(os.getenv("ROUTE_MAX_IMPACT_PCT", "30"))  # Reused for revival gate
ROUTE_SLIPPAGE_BPS = int(os.getenv("ROUTE_SLIPPAGE_BPS", "200"))


# RR Model (shared)
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "40.0"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "20.0"))
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "40"))
FEES_BPS = int(os.getenv("FEES_BPS", "30"))

# GPT Arbitration (optional)
GPT_SWAP_ON = int(os.getenv("GPT_SWAP_ON", "1"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")
GPT_HTTP_TIMEOUT_SEC = int(os.getenv("GPT_HTTP_TIMEOUT_SEC", "5"))

# Backlog (priority queue)
PM_BACKLOG_MAX = int(os.getenv("PM_BACKLOG_MAX", "50"))
PM_BACKLOG_TTL_SEC = int(os.getenv("PM_BACKLOG_TTL_SEC", "900"))  # 15 min
PM_BACKLOG_RESCORE_SEC = int(os.getenv("PM_BACKLOG_RESCORE_SEC", "60"))

# Watchlist ingestion
WATCH_QUEUE = os.getenv("WATCH_QUEUE", "watchlist.jsonl")
PM_READ_WATCHLIST = int(os.getenv("PM_READ_WATCHLIST", "1"))
PM_WATCH_SIZE_USD = float(os.getenv("PM_WATCH_SIZE_USD", "250.0"))
PM_WATCH_DEDUP_MIN = int(os.getenv("PM_WATCH_DEDUP_MIN", "10"))

# Eviction safety
EVICT_MAX_PNL_PCT = float(os.getenv("EVICT_MAX_PNL_PCT", "0.0"))  # 0.0 = never evict profitable (>0)
MIN_HOLD_SEC_BEFORE_EVICT = int(os.getenv("MIN_HOLD_SEC_BEFORE_EVICT", "600"))  # 10 minutes

# --- Freshness & drift gates (TM level) ---
SCN_MAX_STALENESS_MIN = int(os.getenv("SCN_MAX_STALENESS_MIN", "60"))     # max age since signal_ts to allow dispatch
MAX_PRICE_DRIFT_PCT   = float(os.getenv("MAX_PRICE_DRIFT_PCT", "80"))   # max % drift vs fresh reference before dispatch

# --- Replay / Re-read knobs (last-N tail) ---
SCN_REPLAY_LAST_N = int(os.getenv("SCN_REPLAY_LAST_N", "10"))            # how many recent lines to recheck
SCN_REPLAY_EVERY_SEC = int(os.getenv("SCN_REPLAY_EVERY_SEC", "60"))      # replay frequency
SCN_MIN_AGE_BEFORE_RECHECK_MIN = int(os.getenv("SCN_MIN_AGE_BEFORE_RECHECK_MIN", "3"))  # don't recheck until N minutes after signal
SCN_REPROCESS_COOLDOWN_MIN = int(os.getenv("SCN_REPROCESS_COOLDOWN_MIN", "10"))         # per-scenario cooldown
SCN_SEEN_FILE = os.getenv("SCN_SEEN_FILE", "scn_seen.json")              # registry for last reprocess time


# === NEW: Bagholder, Exhaustion & Revival Gate Knobs ===
# Bagholder Wall
BAGH_TOP_HOLDERS_LIMIT = int(os.getenv("BAGH_TOP_HOLDERS_LIMIT", "10"))
BAGH_MIN_LOSS_PCT = float(os.getenv("BAGH_MIN_LOSS_PCT", "-80.0"))
BAGH_MIN_SUPPLY_SHARE = float(os.getenv("BAGH_MIN_SUPPLY_SHARE", "0.001"))  # 0.1%
SELL_PRESSURE_MAX = float(os.getenv("SELL_PRESSURE_MAX", "0.30"))  # 25% max allowed

# Exhaustion & Stability
STABILITY_LOOKBACK_MIN = int(os.getenv("STABILITY_LOOKBACK_MIN", "60"))
STABILITY_MIN = float(os.getenv("STABILITY_MIN", "0.90"))
HOLDER_DORMANCY_HOURS = int(os.getenv("HOLDER_DORMANCY_HOURS", "48"))
HOLDER_ACTIVITY_DECLINE_MIN = float(os.getenv("HOLDER_ACTIVITY_DECLINE_MIN", "0.60"))

# Revival diagnostics / guard-rails
STABILITY_MIN_BARS = int(os.getenv("STABILITY_MIN_BARS", "5"))          # min 1m bars to enforce stability
HOLDER_DECLINE_MIN_EVAL = int(os.getenv("HOLDER_DECLINE_MIN_EVAL", "10"))# min wallets evaluated to enforce decline
LOG_REVIVAL_DETAILS = int(os.getenv("LOG_REVIVAL_DETAILS", "1"))          # extra logs & meta reason details


# Revival/Trigger Policy
REVIVAL_GATE_RR_PENALTY = float(os.getenv("REVIVAL_GATE_RR_PENALTY", "0.8"))  # Multiplier if gate fails
STRICT_REVIVAL_GATE = int(os.getenv("STRICT_REVIVAL_GATE", "0"))  # 1 = drop if gate fails
ENABLE_DS_SYNTHETIC = int(os.getenv("ENABLE_DS_SYNTHETIC", "1"))


# Controlled Liquidity Cadence (Executor Integration)
HEAVY_SELL_DETECT_PCT = float(os.getenv("HEAVY_SELL_DETECT_PCT", "-10.0"))  # PnL drop to trigger pause
REVIVAL_PAUSE_HOURS = int(os.getenv("REVIVAL_PAUSE_HOURS", "6"))

# System
LOOP_SLEEP_SEC = int(os.getenv("PM_LOOP_SLEEP_SEC", "5"))
_PAUSE_UNTIL_TS_BY_MINT: Dict[str, int] = {}  # In-memory state for pause commands

# =========================
# NEW: PnL Optimization Knobs
# =========================
POSITION_SIZE_BY_RR = int(os.getenv("POSITION_SIZE_BY_RR", "1"))
BASE_POSITION_SIZE = float(os.getenv("BASE_POSITION_SIZE", "100"))
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "300"))
RR_SIZE_MULTIPLIER = float(os.getenv("RR_SIZE_MULTIPLIER", "1.5"))
QUICK_PROFIT_PCT = float(os.getenv("QUICK_PROFIT_PCT", "15.0"))
SCALE_OUT_PCT = float(os.getenv("SCALE_OUT_PCT", "25.0"))
EARLY_EXIT_DROP_PCT = float(os.getenv("EARLY_EXIT_DROP_PCT", "5.0"))
MIN_HOLD_FOR_SWAP = int(os.getenv("MIN_HOLD_FOR_SWAP", "600"))  # 10 minutes


# Add to your env/knobs section
MIN_BARS_REQUIRED = int(os.getenv("MIN_BARS_REQUIRED", "2"))  # Minimum bars to analyze
MIN_ROUTE_M5 = int(os.getenv("MIN_ROUTE_M5", "5"))  # Lowered from 10
MIN_ROUTE_MC = float(os.getenv("MIN_ROUTE_MC", "30000"))  # Lowered from 100000
STABILITY_MIN_BARS = int(os.getenv("STABILITY_MIN_BARS", "5"))  # Lowered from 30
HOLDER_DECLINE_MIN_EVAL = int(os.getenv("HOLDER_DECLINE_MIN_EVAL", "5"))  # Lowered from 10

# =========================
# Small in-process TTL cache
# =========================
class TTLCache:
    def __init__(self, ttl_sec: int = 180):
        self.ttl = ttl_sec
        self._d: Dict[str, Tuple[int, object]] = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            ts, val = it
            if time.time() - ts > self.ttl:
                self._d.pop(key, None)
                return None
            return val

    def set(self, key: str, val):
        with self._lock:
            self._d[key] = (int(time.time()), val)

CACHE_DS = TTLCache(ttl_sec=180)         # Dexscreener meta
CACHE_BE_BARS = TTLCache(ttl_sec=90)     # Birdeye OHLCV slices
CACHE_HOLDERS = TTLCache(ttl_sec=300)    # Holders lists
CACHE_HEL_TX = TTLCache(ttl_sec=300)     # Helius tx history per wallet
CACHE_SUPPLY = TTLCache(ttl_sec=1800)    # token supply

# cache for resolving token-account -> owner wallet
CACHE_OWNER = TTLCache(ttl_sec=3600)


# =========================
# NEW: PnL Optimization Functions
# =========================
def calculate_dynamic_size(analysis: dict) -> float:
    """Scale position size based on RR and BQ conviction"""
    if not POSITION_SIZE_BY_RR:
        return BASE_POSITION_SIZE
    
    base_size = BASE_POSITION_SIZE
    rr = analysis.get("rr", 1.0)
    bq = analysis.get("bq", 0.5)
    
    # RR-based multiplier
    if rr > 2.0:
        size_mult = RR_SIZE_MULTIPLIER
    elif rr > 1.5:
        size_mult = 1.2
    else:
        size_mult = 1.0
    
    # BQ-based adjustment
    bq_boost = 1.0 + (bq * 0.5)  # Up to 50% boost for high quality bottoms
    
    dynamic_size = base_size * size_mult * bq_boost
    
    # Apply time-of-day awareness
    dynamic_size *= get_market_hour_penalty()
    
    return min(dynamic_size, MAX_POSITION_SIZE)

def get_market_hour_penalty() -> float:
    """Reduce position sizes during low-activity hours"""
    utc_hour = time.gmtime().tm_hour
    # Asian market hours (low crypto volume)
    if 0 <= utc_hour <= 8:
        return 0.7  # 30% size reduction
    # US market open
    elif 13 <= utc_hour <= 21:
        return 1.2  # 20% size increase
    return 1.0

def enhanced_rr_calculation(analysis: dict, bars: List[Bar]) -> float:
    """More robust RR calculation that handles edge cases"""
    try:
        base_rr = analysis.get("rr", 1.0)
        
        # Skip enhancement if we have very few bars
        if len(bars) < 5:
            print(f"[enhanced_rr] Warning: Only {len(bars)} bars, using base RR")
            return max(1.0, base_rr)  # Ensure minimum RR of 1.0
        
        # 1. Volume confirmation boost (more lenient)
        m5 = analysis.get("m5", 0)
        if m5 > 15:  # Lowered threshold
            base_rr *= 1.15
            print(f"[enhanced_rr] Volume boost: m5={m5}")
        elif m5 > 8:
            base_rr *= 1.05
            print(f"[enhanced_rr] Minor volume boost: m5={m5}")
        
        # 2. Trend alignment - only if we have enough bars
        if len(bars) >= 10:
            recent_prices = [b.p for b in bars]
            if len(recent_prices) >= 10:
                recent_avg = sum(recent_prices[-5:]) / 5
                older_avg = sum(recent_prices[-10:-5]) / 5
                if older_avg > 0:
                    trend = recent_avg / older_avg
                    if trend > 1.01:  # 1% uptrend
                        base_rr *= 1.1
                        print(f"[enhanced_rr] Trend boost: {trend:.3f}")
        
        # 3. Market cap efficiency - smaller caps have higher potential
        mc = analysis.get("mc", 0)
        if mc and mc > 0:
            if mc < 30000:  # < $30k MC
                base_rr *= 1.3
                print(f"[enhanced_rr] Small cap boost: MC=${mc:.0f}")
            elif mc < 60000:  # < $60k MC  
                base_rr *= 1.15
                print(f"[enhanced_rr] Medium cap boost: MC=${mc:.0f}")
            elif mc < 100000:  # < $100k MC
                base_rr *= 1.05
                print(f"[enhanced_rr] Minor cap boost: MC=${mc:.0f}")
        
        # 4. Revival gate bonus for strong fundamentals
        if analysis.get("revival_gate_ok") and analysis.get("price_stability", 0) > 0.8:
            base_rr *= 1.1
            print(f"[enhanced_rr] Revival gate bonus")
        
        # Ensure reasonable bounds
        final_rr = min(max(0.5, base_rr), 8.0)  # Cap between 0.5 and 8.0
        print(f"[enhanced_rr] Final RR: {final_rr:.2f} (from base: {analysis.get('rr', 1.0):.2f})")
        return final_rr
        
    except Exception as e:
        print(f"[enhanced_rr] Error in RR calculation: {e}")
        return max(1.0, analysis.get("rr", 1.0))  # Fallback to base RR

def backlog_priority_score(analysis: dict) -> float:
    """Better backlog prioritization beyond just RR"""
    rr = analysis.get("rr", 1.0)
    bq = analysis.get("bq", 0.5)
    m5 = analysis.get("m5", 0)
    mc = analysis.get("mc", 1000000)
    revival_ok = analysis.get("revival_gate_ok", False)
    
    # Base RR weight
    score = rr * 0.4
    
    # Bottom quality weight
    score += bq * 0.3
    
    # Activity and market cap efficiency
    activity_score = min(m5 / 10.0, 2.0)  # Cap activity boost
    mc_score = max(0.5, min(2.0, 100000 / max(mc, 50000)))  # Favor smaller caps
    
    score += activity_score * 0.15
    score += mc_score * 0.15
    
    # Revival gate bonus
    if revival_ok:
        score *= 1.2
    
    return score

def check_early_exit_signals():
    """Monitor open positions for early exit opportunities"""
    incumbents = get_incumbents_ctx()
    for pos in incumbents:
        mint = pos["mint"]
        
        # Check for heavy selling pressure
        current_price = _spot_price(mint)
        if current_price:
            # If price drops 5% below entry, consider early exit
            entry_px = float(pos.get("entry_px", 0))
            if entry_px > 0 and current_price < entry_px * (1 - EARLY_EXIT_DROP_PCT/100):
                print(f"[!] Early exit signal for {mint[:6]} -{EARLY_EXIT_DROP_PCT}% from entry")
                _append_jsonl(EXEC_CONTROL, {
                    "type": "FORCE_CLOSE", 
                    "ts": _now(), 
                    "mint": mint,
                    "reason": f"early_exit_{EARLY_EXIT_DROP_PCT}pct_drop"
                })
        
        # Check volume drying up (exit if activity disappears)
        _, m5, _, _, _, _ = _ds_meta(mint)
        if m5 and m5 < 3:  # Very low activity
            print(f"[!] Low activity exit for {mint[:6]} (m5={m5})")
            _append_jsonl(EXEC_CONTROL, {
                "type": "FORCE_CLOSE",
                "ts": _now(),
                "mint": mint, 
                "reason": "low_activity_exit"
            })

def check_profit_taking():
    """
    Trailing take-profit:
      - Arm once peak gain reaches TRAIL_ARM_PCT.
      - Trail at TRAIL_GIVEBACK_PCT of the peak price (never below break-even).
      - Requires a minimal holding time to avoid instant whipsaws.
    Tunables via ENV (with sensible defaults if unset):
      TRAIL_ARM_PCT (default 12.0)
      TRAIL_GIVEBACK_PCT (default 35.0)
      TRAIL_MIN_HOLD_SEC (default 60)
    """
    arm_pct        = float(os.getenv("TRAIL_ARM_PCT", "12.0"))          # % gain from entry to arm
    giveback_pct   = float(os.getenv("TRAIL_GIVEBACK_PCT", "35.0"))     # % drop from peak to stop
    min_hold_sec   = int(os.getenv("TRAIL_MIN_HOLD_SEC", "60"))         # min seconds before enforcing

    now_ts = _now()
    # Walk all open positions directly; we need entry/max/last fields from files
    for fn in os.listdir(OPEN_POS_DIR):
        if not fn.endswith(".json"):
            continue

        try:
            ppath = os.path.join(OPEN_POS_DIR, fn)
            with open(ppath, "r", encoding="utf-8") as f:
                pos = json.load(f)

            mint     = pos["mint"]
            entry_px = float(pos.get("entry_px") or 0.0)
            entry_ts = int(pos.get("entry_ts") or now_ts)
            note     = (pos.get("note") or {})
            last_px  = float(note.get("last_px") or 0.0)
            max_px   = float(pos.get("max_px") or entry_px)

            if entry_px <= 0 or last_px <= 0:
                continue

            held_sec = max(0, now_ts - entry_ts)
            peak_gain_pct = (max_px - entry_px) / entry_px * 100.0

            # Only arm once we’ve reached the configured threshold and a minimal hold time
            if peak_gain_pct < arm_pct or held_sec < min_hold_sec:
                continue

            # Compute a trailing stop:
            #  - Start from a giveback off the peak
            #  - Never let it go below break-even
            giveback = giveback_pct / 100.0
            trail_from_peak_px = max_px * (1.0 - giveback)
            be_px = entry_px  # break-even floor
            trail_stop_px = max(trail_from_peak_px, be_px)

            # Persist the suggested trail level for visibility (non-blocking best-effort)
            try:
                pos["trail_stop_px"] = float(trail_stop_px)
                with open(ppath, "w", encoding="utf-8") as fw:
                    json.dump(pos, fw)
            except Exception:
                pass

            # Trigger exit if price has given back to the trail or below
            if last_px <= trail_stop_px:
                print(f"[!] TRAIL STOP {mint[:6]} peak={peak_gain_pct:.1f}% "
                      f"giveback={giveback_pct:.0f}% hit: last={last_px:.6g} ≤ stop={trail_stop_px:.6g}")
                _append_jsonl(EXEC_CONTROL, {
                    "type": "FORCE_CLOSE",
                    "ts": _now(),
                    "mint": mint,
                    "reason": f"trail_stop_{giveback_pct:.0f}pct_from_peak"
                })

        except Exception as e:
            print(f"[trail] failed on {fn}: {e}")

# =========================
# Safe Utilities & Reset
# =========================
_LAST_WATCH_INGEST_TS_BY_MINT: Dict[str, int] = {}

def _now() -> int:
    return int(time.time())

def _touch(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8"):
            pass

_touch(SCENARIOS_IN); _touch(SCENARIOS_EXEC); _touch(EXEC_CONTROL)
_touch(REPEAT_FILE); _touch(DEADLIST_PATH); os.makedirs(OPEN_POS_DIR, exist_ok=True)

def adapt_watch_to_scn(wi: dict) -> dict:
    now = _now()
    mint = wi["mint"]
    symbol = wi.get("symbol", mint[:6])
    wi_meta = wi.get("meta", {})
    # Use dynamic sizing for watchlist items too
    size_usd = calculate_dynamic_size({"rr": 1.5, "bq": 0.7})  # Conservative estimate
    
    return {
        "id": wi.get("id", str(uuid.uuid4())), "mint": mint, "symbol": symbol, "side": "buy",
        "size_usd": size_usd, "signal_ts": int(wi.get("seed_ts") or now),
        "meta": {"reason": "watch_promote", "pair_addr": wi_meta.get("pair_addr"),
                 "dex_id": wi_meta.get("dex_id"), "m5": wi_meta.get("m5"),
                 "mc": wi_meta.get("mc_now"), "created_ms": wi_meta.get("created_ms"),
                 }
    }

def _watch_dedup_ok(mint: str, ts: int) -> bool:
    last = _LAST_WATCH_INGEST_TS_BY_MINT.get(mint, 0)
    if ts - last < PM_WATCH_DEDUP_MIN * 60:
        return False
    _LAST_WATCH_INGEST_TS_BY_MINT[mint] = ts
    return True

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
                    except json.JSONDecodeError: print(f"[tailer] WARN: Skipping malformed JSON line in {self.path}")
                self.pos = f.tell()
        except FileNotFoundError:
            _touch(self.path); self.pos = 0
        return out


def _load_json(path, default):
    try:
        return json.load(open(path, "r", encoding="utf-8"))
    except Exception:
        return default

def _save_json(path, obj):
    json.dump(obj, open(path, "w", encoding="utf-8"), indent=2)

def _ensure_file(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as _:
            pass

# --- Seen registry (to throttle reprocessing) ---
def seen_load():
    _ensure_file(SCN_SEEN_FILE)
    return _load_json(SCN_SEEN_FILE, {"by_id": {}, "by_mint": {}})

def seen_touch(seen: dict, scn: dict):
    now = _now()
    sid = scn.get("id")
    if sid:
        seen["by_id"][sid] = now
    mint = scn.get("mint")
    if mint:
        seen["by_mint"][mint] = now
    _save_json(SCN_SEEN_FILE, seen)

def seen_ok(seen: dict, scn: dict, cooldown_min: int) -> bool:
    now = _now()
    sid = scn.get("id")
    last = None
    if sid:
        last = seen["by_id"].get(sid)
    if last is None:
        last = seen["by_mint"].get(scn.get("mint"))
    if last is None:
        return True
    return (now - last) >= cooldown_min * 60

# --- Read the last N jsonl objects (robust to large files) ---
def read_last_n_jsonl(path: str, n: int) -> list[dict]:
    """
    Efficiently read the last n JSONL lines (best-effort; ignores malformed lines).
    """
    _ensure_file(path)
    out = []
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 4096
            data = b""
            pos = size
            while pos > 0 and len(out) < n:
                read_size = block if pos >= block else pos
                pos -= read_size
                f.seek(pos)
                data = f.read(read_size) + data
                lines = data.split(b"\n")
                # keep first partial for next iteration
                data = lines[0]
                for line in reversed(lines[1:]):
                    s = line.decode("utf-8", errors="ignore").strip()
                    if not s:
                        continue
                    try:
                        obj = json.loads(s)
                        out.append(obj)
                        if len(out) >= n:
                            break
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        pass
    return list(reversed(out))  # original order (oldest→newest among the last N)

def replay_candidates_last_n(path: str,
                             last_n: int,
                             min_age_min: int,
                             cooldown_min: int) -> list[dict]:
    """
    Pull the last N scenarios, return those:
    - whose signal_ts is at least min_age_min old (enough time passed),
    - and which have not been reprocessed within cooldown_min.
    """
    now = _now()
    cutoff_min_age = min_age_min * 60
    seen = seen_load()
    out = []
    for scn in read_last_n_jsonl(path, last_n):
        sig_ts = int(scn.get("signal_ts") or 0)
        if sig_ts and (now - sig_ts) < cutoff_min_age:
            continue
        if not seen_ok(seen, scn, cooldown_min):
            continue
        out.append(scn)
    return out


def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f: f.write(json.dumps(obj) + "\n")

class Backlog:
    def __init__(self, cap: int, ttl_sec: int):
        self.cap, self.ttl, self.heap, self.items = cap, ttl_sec, [], {}
    
    def put(self, analysis: dict):
        mint, ts = analysis["mint"], _now()
        # Use priority score instead of just RR
        priority_score = backlog_priority_score(analysis)
        
        if mint in self.items and priority_score <= backlog_priority_score(self.items[mint]["analysis"]): 
            return
        entry_ts = self.items[mint]["ts"] if mint in self.items else ts
        if mint not in self.items and len(self.items) >= self.cap:
            try:
                # Remove lowest priority instead of oldest
                worst_mint = min(self.items, key=lambda m: backlog_priority_score(self.items[m]["analysis"]))
                self.items.pop(worst_mint, None)
            except ValueError: 
                pass
        self.items[mint] = {"analysis": analysis, "ts": entry_ts, "scored_ts": ts}
        heapq.heappush(self.heap, (-priority_score, -entry_ts, mint))
    
    def pop_best(self) -> Optional[dict]:
        now = _now()
        while self.heap:
            _, _, mint = heapq.heappop(self.heap)
            rec = self.items.get(mint)
            if not rec: 
                continue
            if now - rec["ts"] > self.ttl:
                self.items.pop(mint, None)
                continue
            return self.items.pop(mint)["analysis"]
        return None
    
    def drop(self, mint: str): 
        self.items.pop(mint, None)
    
    def has(self, mint: str) -> bool: 
        return mint in self.items
    
    def top_n(self, n: int = 3) -> List[dict]:
        """Return top N candidates by priority score without removing them"""
        valid_items = []
        now = _now()
        
        for mint, rec in list(self.items.items()):
            if now - rec["ts"] > self.ttl:
                self.items.pop(mint, None)
                continue
            valid_items.append(rec["analysis"])
        
        # Sort by priority score (highest first) and return top N
        valid_items.sort(key=lambda x: backlog_priority_score(x), reverse=True)
        return valid_items[:n]
    
    def remove(self, candidate: dict):
        """Remove a candidate by mint"""
        mint = candidate["mint"]
        self.drop(mint)

    # NEW METHOD: Cleanup stale entries
    def cleanup_stale(self):
        """Remove entries older than TTL. Call at start of each cycle."""
        now = _now()
        expired = [
            mint for mint, rec in self.items.items() 
            if now - rec["ts"] > self.ttl
        ]
        for mint in expired:
            self.items.pop(mint, None)
            # Note: heap cleanup happens naturally in pop_best()
        if expired:
            print(f"[backlog] Cleaned {len(expired)} stale entries: {[m[:6] for m in expired]}")

# =========================
# Deadlist Module
# =========================
def deadlist_load(path: str = DEADLIST_PATH) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {"mints": {}}

def is_dead(mint: str, dl: dict, now_ts: int, default_ttl_min: int) -> bool:
    entry = dl.get("mints", {}).get(mint)
    if not entry: return False
    ttl_sec = (entry.get("ttl_min") or default_ttl_min) * 60
    return now_ts < (entry.get("ts", 0) + ttl_sec)

# =========================
# NEW: Holder & Cost Basis Utilities
# =========================
@dataclass
class Holder:
    address: str
    ui_amount: float
    supply_share: Optional[float] = None  # 0..1 if known

@dataclass
class CostBasis:
    address: str
    avg_cost_usd_per_token: float
    current_value_usd: float
    pnl_pct: float

# --- API helpers ---
def _be_headers():
    return {"accept": "application/json", "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY}

def _helius_rpc_url():
    # Helius JSON-RPC endpoint (works like Solana RPC)
    key = HELIUS_API_KEY
    return f"https://mainnet.helius-rpc.com/?api-key={key}" if key else None

def _helius_enhanced_url(wallet: str, limit: int = 50):
    return f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={HELIUS_API_KEY}&limit={limit}"

def _rpc_call(method: str, params: list, timeout: int = 8) -> Optional[dict]:
    url = _helius_rpc_url()
    if not url:
        return None
    try:
        r = requests.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[rpc] {method} error: {e}")
    return None

def _get_token_supply(mint: str) -> Optional[Tuple[int, int]]:
    """
    Solana RPC fallback for token supply.
    Returns (amount_raw:int, decimals:int) or None.
    """
    if not HELIUS_API_KEY:
        return None
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        payload = {"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":[mint]}
        r = requests.post(url, json=payload, timeout=8)
        if r.status_code != 200:
            return None
        result = (r.json() or {}).get("result") or {}
        value  = (result.get("value") or {})
        amount = value.get("amount")
        decimals = value.get("decimals")
        if amount is None or decimals is None:
            return None
        return int(amount), int(decimals)
    except Exception:
        return None


def _get_circulating_supply_tokens(mint: str) -> Optional[float]:
    """
    Returns circulating supply in *tokens* (not USD).
    Tries Birdeye token_overview.circulating_supply, falls back to token_meta.supply,
    then to RPC getTokenSupply (total supply).
    """
    if BIRDEYE_API_KEY:
        try:
            r = requests.get(
                "https://public-api.birdeye.so/defi/token_overview",
                params={"address": mint},
                headers={"accept":"application/json","x-chain":"solana","X-API-KEY":BIRDEYE_API_KEY},
                timeout=5,
            )
            if r.status_code == 200:
                data = (r.json() or {}).get("data") or {}
                circ = data.get("circulating_supply")
                if circ is not None:
                    circ_f = float(circ)
                    if circ_f > 0:
                        return circ_f
        except Exception:
            pass
        try:
            r = requests.get(
                "https://public-api.birdeye.so/defi/token_meta",
                params={"address": mint},
                headers={"accept":"application/json","x-chain":"solana","X-API-KEY":BIRDEYE_API_KEY},
                timeout=5,
            )
            if r.status_code == 200:
                data = (r.json() or {}).get("data") or {}
                sup = data.get("supply")
                if sup is not None:
                    sup_f = float(sup)
                    if sup_f > 0:
                        return sup_f  # treat total as circulating if nothing better
        except Exception:
            pass

    # RPC fallback (total supply)
    sup = _get_token_supply(mint)
    if sup:
        amount_raw, dec = sup
        total = float(amount_raw) / (10 ** int(dec))
        if total > 0:
            return total
    return None


def _resolve_owner_wallet(addr: str) -> str:
    """
    If addr is a token account, return its owner wallet.
    If it's already a wallet, return as-is.
    Uses RPC getAccountInfo parsed->info.owner and caches.
    """
    if not addr:
        return addr

    key = f"owner:{addr}"
    cached = CACHE_OWNER.get(key)
    if cached is not None:
        return cached

    # Try to see if this address looks like a token account by asking RPC
    owner = _get_token_account_owner(addr)
    if owner:
        CACHE_OWNER.set(key, owner)
        return owner

    # No parsed owner found -> assume it's already a wallet
    CACHE_OWNER.set(key, addr)
    return addr


def _get_token_largest_accounts(mint: str, limit: int) -> List[Tuple[str, int]]:
    """
    Returns list of (tokenAccount, amount) sorted desc.
    """
    res = _rpc_call("getTokenLargestAccounts", [mint, {"commitment":"confirmed"}])
    out = []
    if not res or "result" not in res:
        return out
    try:
        vals = (res["result"] or {}).get("value") or []
        for it in vals[:limit*2]:  # overfetch
            amt = int(it.get("amount") or "0")
            ta = it.get("address")
            if ta and amt > 0:
                out.append((ta, amt))
    except Exception:
        pass
    return out[:limit*2]

def _get_token_account_owner(token_account: str) -> Optional[str]:
    res = _rpc_call("getAccountInfo", [token_account, {"encoding":"jsonParsed","commitment":"confirmed"}])
    try:
        info = (res or {}).get("result", {}).get("value", {}).get("data", {}).get("parsed", {}).get("info", {})
        owner = info.get("owner")
        return owner
    except Exception:
        return None

def _lamports_to_ui(amount: int, decimals: int) -> float:
    if decimals <= 0:
        return float(amount)
    return float(amount) / (10 ** decimals)

def _ds_meta(mint: str) -> Tuple[Optional[float], Optional[int], Optional[int], Optional[str], Optional[str], Optional[float]]:
    """
    Returns (mc_usd, m5_total, created_ms, best_pair, best_quote, price_usd)
    cached for 180s
    """
    key = f"ds:{mint}"
    cached = CACHE_DS.get(key)
    if cached is not None:
        return cached
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=3)
        best_pair, best_quote, mc_usd, m5_total, created_ms, price_usd = None, None, None, None, None, None
        if r.status_code == 200:
            pairs = (r.json() or {}).get("pairs") or []
            pairs = [p for p in pairs if p.get("chainId") == "solana"]
            if pairs:
                pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
                best = pairs[0]
                mc_usd = float(best["marketCap"]) if best.get("marketCap") else None
                m5_total = int((best.get('txns', {}).get('m5', {}).get('buys', 0))) + int((best.get('txns', {}).get('m5', {}).get('sells', 0)))
                created_ms = int(best.get("pairCreatedAt") or 0) if best.get("pairCreatedAt") else None
                best_pair = best.get("pairAddress")
                best_quote = (best.get("quoteToken") or {}).get("address")
                # New: capture priceUsd for synthetic bars
                try:
                    price_usd = float(best.get("priceUsd") or 0.0)
                except Exception:
                    price_usd = None
        val = (mc_usd, m5_total, created_ms, best_pair, best_quote, price_usd)
        CACHE_DS.set(key, val)
        return val
    except Exception as e:
        print(f"[market] Dexscreener meta failed for {mint[:6]}: {e}")
        return None, None, None, None, None, None


def _spot_price(mint: str) -> Optional[float]:
    # Prefer Birdeye spot; fallback to Dexscreener best pair price
    px = _be_spot_price(mint)
    if px is not None:
        return px
    _, _, _, _, _, ds_px = _ds_meta(mint)
    return ds_px

def _drift_pct(ref: float, now_px: float) -> float:
    if not ref or not now_px:
        return 0.0
    try:
        return abs(now_px / ref - 1.0) * 100.0
    except Exception:
        return 0.0

def validate_and_enrich_candidate(candidate: dict, from_backlog: bool = False) -> Optional[dict]:
    """
    Unified validation pipeline for new and backlog candidates.
    
    Args:
        candidate: Raw scenario or analysis dict with 'raw_scn' key
        from_backlog: If True, skip age check (already fresh from just-in-time analysis)
    
    Returns:
        Enriched analysis dict or None if validation fails
    
    Validation Steps:
        1. Fresh market analysis (price, metrics, gates)
        2. Freshness/drift guard (age + price movement)
        3. Deadlist check
        4. Repeat policy (cooldown + RR multiplier)
    """
    # Extract raw scenario (handle both formats)
    raw_scn = candidate.get("raw_scn") or candidate
    mint = raw_scn.get("mint")
    symbol = raw_scn.get("symbol", mint[:6] if mint else "???")
    
    print(f"[validation] Checking {symbol} ({'backlog' if from_backlog else 'new'})")
    
    # Step 1: Fresh market analysis
    fresh = analyze_candidate(raw_scn)
    if not fresh:
        print(f"  [-] Fresh analysis failed (no bars, bad data, etc.)")
        return None
    
    # Step 2: Freshness/drift guard
    # For backlog candidates, we just analyzed them, so skip age check
    # For new candidates, enforce both age and drift limits
    ok, why = _fresh_enough_and_not_drifted(fresh, ignore_age=from_backlog)
    if not ok:
        print(f"  [-] Freshness check failed: {why}")
        return None
    
    # Step 3: Deadlist check
    dl = deadlist_load()
    now = _now()
    if is_dead(fresh["mint"], dl, now, DEADLIST_DEFAULT_TTL_MIN):
        print(f"  [-] Token is deadlisted")
        return None
    
    # Step 4: Repeat policy
    reg = read_repeat_registry()
    ok_rep, rr_mult, rep_why = apply_repeat_policy(fresh, reg)
    if not ok_rep:
        print(f"  [-] Repeat policy rejected: {rep_why}")
        return None
    
    # Apply repeat multiplier to RR
    fresh["rr"] *= rr_mult
    
    print(f"  [+] Validated: RR={fresh['rr']:.2f}, multiplier={rr_mult:.2f}, reason={rep_why}")
    return fresh

# trade_manager.py
# --- Helper: freshness & drift guard (with optional age ignore) ---
def _fresh_enough_and_not_drifted(fresh: dict, *, ignore_age: bool = False, max_stale_min: int | None = None):
    """
    Returns (ok: bool, why: str)

    - If ignore_age=True, we skip the staleness (age) check. This is used for backlog promotions
      because we re-analyze just-in-time; price drift protection still applies.
    - If ignore_age=False, we enforce age using SCN_MAX_STALENESS_MIN or an override.
    """
    now = _now()
    raw = (fresh.get("raw_scn") or {})
    sig_ts = int(raw.get("signal_ts") or 0)

    if not ignore_age:
        limit = (SCN_MAX_STALENESS_MIN if max_stale_min is None else max_stale_min)
        if sig_ts == 0 or (now - sig_ts) > limit * 60:
            age_min = (now - sig_ts) / 60 if sig_ts else 1e9
            return False, f"stale:{age_min:.1f}m>{limit}m"

    # Drift guard: if spot has drifted too far from our reference (low_p), reject.
    ref_px = float(fresh.get("low_p") or 0.0)
    if ref_px > 0:
        spot = _spot_price(fresh["mint"])
        if spot:
            drift = _drift_pct(ref_px, spot)
            if drift > MAX_PRICE_DRIFT_PCT:
                return False, f"drift:{drift:.2f}%>{MAX_PRICE_DRIFT_PCT:.2f}% (ref={ref_px:.8f} spot={spot:.8f})"

    return True, "ok"

# --- Holders: Birdeye v3 -> RPC fallback (owner resolution) ---
def fetch_top_holders(mint: str, limit: int) -> List[Holder]:
    """
    Fetch top holders from Birdeye v3 with pagination (limit <=100 per call).
    Prefer Birdeye's 'owner' if present; otherwise resolve token-account -> owner via RPC.
    Also compute supply_share when supply is known.
    """
    if not BIRDEYE_API_KEY:
        print("[holders] WARN: No Birdeye API key for holder analysis.")
        return []

    def _be_headers():
        return {"accept":"application/json","x-chain":"solana","X-API-KEY":BIRDEYE_API_KEY}

    # Try supply via Birdeye
    total_supply: Optional[float] = None
    supply_src = "birdeye"
    try:
        ov = requests.get(
            "https://public-api.birdeye.so/defi/token_overview",
            params={"address": mint},
            headers=_be_headers(),
            timeout=5,
        )
        if ov.status_code == 200:
            data = (ov.json() or {}).get("data") or {}
            total_supply = float(data.get("circulating_supply") or data.get("supply") or 0.0)
    except Exception:
        pass

    # Fallback supply via token_meta
    if not total_supply or total_supply <= 0:
        try:
            meta = requests.get(
                "https://public-api.birdeye.so/defi/token_meta",
                params={"address": mint},
                headers=_be_headers(),
                timeout=5,
            )
            if meta.status_code == 200:
                data = (meta.json() or {}).get("data") or {}
                total_supply = float(data.get("supply") or 0.0)
        except Exception:
            pass

    # Final fallback supply via RPC
    if (not total_supply or total_supply <= 0) and HELIUS_API_KEY:
        sup = _get_token_supply(mint)
        if sup:
            amount_raw, dec = sup
            total_supply = float(amount_raw) / (10 ** int(dec))
            supply_src = "rpc"

    holders: List[Holder] = []
    remaining = max(1, limit)
    offset = 0
    PAGE = 100

    while remaining > 0:
        page_size = min(PAGE, remaining)
        try:
            r = requests.get(
                "https://public-api.birdeye.so/defi/v3/token/holder",
                params={
                    "address": mint,
                    "offset": offset,
                    "limit": page_size,
                    "ui_amount_mode": "scaled"
                },
                headers=_be_headers(),
                timeout=8,
            )
            if r.status_code != 200:
                print(f"[holders] v3 holder API status {r.status_code} for {mint[:6]} (offset={offset}, limit={page_size})")
                break

            items = ((r.json() or {}).get("data") or {}).get("items") or []
            if not items:
                break

            for it in items:
                # Birdeye may return 'owner' (wallet) OR 'holder' (token-account) depending on endpoint version
                owner = it.get("owner")
                holder_addr = it.get("holder") or it.get("address")  # token-account or wallet
                ui_amount = float(it.get("uiAmount") or it.get("ui_amount") or it.get("amount") or 0.0)

                addr = owner if owner else _resolve_owner_wallet(holder_addr)
                holders.append(Holder(address=addr, ui_amount=ui_amount))

            got = len(items)
            remaining -= got
            offset += got
            if got < page_size:
                break

        except Exception as e:
            print(f"[holders] holders fetch error: {e}")
            break

    # Compute supply share
    if holders:
        if not total_supply or total_supply <= 0:
            total_supply = sum(h.ui_amount for h in holders) or None
            supply_src = "approx_top_sum" if total_supply else "unknown"

        for h in holders:
            try:
                h.supply_share = (h.ui_amount / total_supply) if total_supply and total_supply > 0 else None
            except Exception:
                h.supply_share = None

    print(f"[holders] Birdeye fetched {len(holders)} holders for {mint[:6]} (target={limit}, supply={'{:.0f}'.format(total_supply) if total_supply else 'unknown'} src={supply_src})")
    return holders[:limit]

# --- Price at time utilities for cost basis ---
@dataclass
class Bar:
    t: int
    p: float

def _be_ohlcv_pair(pair_addr: str, t0: int, t1: int) -> List[Bar]:
    key = f"be_pair:{pair_addr}:{t0}:{t1}"
    cached = CACHE_BE_BARS.get(key)
    if cached is not None:
        return cached
    try:
        url = "https://public-api.birdeye.so/defi/ohlcv"
        headers = _be_headers()
        r = requests.get(url, params={"address": pair_addr, "type": "1m", "time_from": t0, "time_to": t1}, headers=headers, timeout=5)
        items = ((r.json() or {}).get("data") or {}).get("items") or [] if r.status_code == 200 else []
        bars = [Bar(int(it["unixTime"]), float(it["close"])) for it in items if it.get("unixTime") and it.get("close")]
        bars.sort(key=lambda b: b.t)
        CACHE_BE_BARS.set(key, bars)
        return bars
    except Exception as e:
        print(f"[ohlcv] pair err: {e}")
        return []

def _be_ohlcv_bq(base: str, quote: str, t0: int, t1: int) -> List[Bar]:
    key = f"be_bq:{base}:{quote}:{t0}:{t1}"
    cached = CACHE_BE_BARS.get(key)
    if cached is not None:
        return cached
    try:
        url = "https://public-api.birdeye.so/defi/ohlcv/base_quote"
        headers = _be_headers()
        r = requests.get(url, params={"base_address": base, "quote_address": quote, "type": "1m", "time_from": t0, "time_to": t1}, headers=headers, timeout=5)
        items = ((r.json() or {}).get("data") or {}).get("items") or [] if r.status_code == 200 else []
        bars = [Bar(int(it["unixTime"]), float(it["close"])) for it in items if it.get("unixTime") and it.get("close")]
        bars.sort(key=lambda b: b.t)
        CACHE_BE_BARS.set(key, bars)
        return bars
    except Exception as e:
        print(f"[ohlcv] bq err: {e}")
        return []
    
def _be_trades_to_1m_bars(mint: str, t0: int, t1: int) -> List[Bar]:
    """
    Fallback: build 1-minute bars from Birdeye trades when OHLCV endpoints return 0 bars.
    - First try with sources='all'
    - If 404/429/etc., retry once without 'sources'
    - Light pagination to gather enough trades (up to MAX_PAGES)
    Returns a list of Bar(t, p) averaged by minute within [t0, t1].
    """
    try:
        headers = _be_headers()
        url = "https://public-api.birdeye.so/defi/token_trades"

        MAX_PAGES = int(os.getenv("TRADES_FALLBACK_MAX_PAGES", "5"))
        PAGE_SIZE = int(os.getenv("TRADES_FALLBACK_PAGE_SIZE", "1000"))
        RETRY_PAUSE_S = float(os.getenv("TRADES_FALLBACK_RETRY_PAUSE_S", "0.4"))

        buckets: Dict[int, List[float]] = {}
        seen_ids: Set[str] = set()

        def _ingest(items: List[dict]):
            added = 0
            for it in items:
                # Protect against payload shape differences
                try:
                    # Prefer explicit fields, fall back as needed
                    ts = int(it.get("blockUnixTime") or it.get("blockTime") or it.get("timestamp") or 0)
                    if ts <= 0 or ts < t0 or ts > t1:
                        continue
                    price = float(it.get("price") or it.get("priceUsd") or 0.0)
                    if price <= 0:
                        continue

                    # Optional unique id to dedupe (if present)
                    uid = it.get("signature") or it.get("txHash") or f"{ts}-{price}-{len(seen_ids)}"
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)

                    minute = ts - (ts % 60)
                    buckets.setdefault(minute, []).append(price)
                    added += 1
                except Exception:
                    continue
            return added

        # Try to fetch up to MAX_PAGES, first with sources='all'
        page = 1
        while page <= MAX_PAGES:
            params = {
                "address": mint,
                "from": t0,
                "to": t1,
                "page": page,
                "limit": PAGE_SIZE,
                "sources": "all",
            }
            r = requests.get(url, params=params, headers=headers, timeout=8)
            if r.status_code == 200:
                items = ((r.json() or {}).get("data") or {}).get("items") or []
                ing = _ingest(items)
                # If fewer than PAGE_SIZE trades, likely no more pages
                if len(items) < PAGE_SIZE:
                    break
                page += 1
                continue

            # Retry once without 'sources' on common transient errors
            if r.status_code in (404, 429, 500, 502, 503):
                try:
                    time.sleep(RETRY_PAUSE_S)
                except Exception:
                    pass
                params.pop("sources", None)
                r2 = requests.get(url, params=params, headers=headers, timeout=8)
                if r2.status_code == 200:
                    items = ((r2.json() or {}).get("data") or {}).get("items") or []
                    ing = _ingest(items)
                    if len(items) < PAGE_SIZE:
                        break
                    page += 1
                    continue

            # Any other hard failure -> stop pagination
            break

        if not buckets:
            print(f"[market] trades→bars fallback produced 0 bars for {mint[:6]}")
            return []

        bars = [Bar(t=m, p=(sum(ps) / len(ps))) for m, ps in sorted(buckets.items())]
        print(f"[market] trades→bars fallback produced {len(bars)} bars for {mint[:6]} (pages<= {MAX_PAGES})")
        return bars

    except Exception as e:
        print(f"[market] trades→bars error: {e}")
        return []


def _be_spot_price(mint: str) -> Optional[float]:
    try:
        url = "https://public-api.birdeye.so/defi/price"
        r = requests.get(url, params={"address": mint}, headers=_be_headers(), timeout=4)
        if r.status_code == 200:
            data = (r.json() or {}).get("data") or {}
            val = data.get("value")
            return float(val) if val else None
    except Exception as e:
        print(f"[price] spot err: {e}")
    return None

def _interp_price_at(bars: List[Bar], cutoff_ts: int) -> Optional[float]:
    if not bars:
        return None
    ts = [b.t for b in bars]; ps = [b.p for b in bars]
    if cutoff_ts <= ts[0]: return ps[0]
    if cutoff_ts >= ts[-1]: return ps[-1]
    idx = bisect.bisect_left(ts, cutoff_ts)
    if ts[idx] == cutoff_ts: return ps[idx]
    t0, p0 = ts[idx - 1], ps[idx - 1]; t1, p1 = ts[idx], ps[idx]
    w = (cutoff_ts - t0) / (t1 - t0) if (t1 - t0) else 0
    return p0 + w * (p1 - p0)

# --- Enhanced Tx parsing (buys/sells for mint) ---
def _helius_enhanced(wallet: str, limit: int = 60) -> List[dict]:
    """
    Fetch enhanced transactions for a wallet using Helius.
    Retries with smaller limits on 400/413 and logs succinct diagnostics.
    """
    if not HELIUS_API_KEY:
        return []

    base = "https://api.helius.xyz/v0/addresses"
    lims = [min(limit, 60), 40, 20]
    for lim in lims:
        url = f"{base}/{wallet}/transactions?api-key={HELIUS_API_KEY}&limit={lim}"
        try:
            r = requests.get(url, timeout=8)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    kinds = {d.get('type') for d in data[:5]}
                    has_swap_event = any(((d.get('events') or {}).get('swap')) for d in data[:5])
                    print(f"[helius] {wallet[:6]} txs={len(data)} types={kinds} swap_event_sample={has_swap_event}")
                    return data
                print(f"[helius] unexpected payload type={type(data)} for {wallet[:6]}")
                return []
            if r.status_code in (400, 413):  # too big
                print(f"[helius] {wallet[:6]} limit {lim} rejected ({r.status_code}); retrying smaller.")
                continue
            print(f"[helius] status {r.status_code} for {wallet[:6]}")
            return []
        except Exception as e:
            print(f"[helius] exception {e} for {wallet[:6]}")
            return []
    return []


def _tx_swap_events_for_mint(tx: dict, mint: str) -> List[Tuple[str, float, int]]:
    """
    Extract swap events for a specific mint from a Helius enhanced transaction.
    Returns a list of (side, qty_ui, timestamp).
    side: 'buy'  -> wallet RECEIVED the mint
          'sell' -> wallet SENT the mint
    """
    out: List[Tuple[str, float, int]] = []
    try:
        ts = int(tx.get("timestamp") or 0)
        events = tx.get("events") or {}
        swap = events.get("swap") or {}

        # Preferred shape: tokenInputs/tokenOutputs inside 'swap'
        token_in_list = swap.get("tokenInputs") or []
        token_out_list = swap.get("tokenOutputs") or []

        def _match_side(entries, side_label):
            for e in entries:
                try:
                    if (e.get("mint") == mint) and float(e.get("tokenAmount", 0)) > 0:
                        out.append((side_label, float(e.get("tokenAmount", 0)), ts))
                except Exception:
                    continue

        if token_in_list or token_out_list:
            # Inputs: wallet gave these away -> SELL
            _match_side(token_in_list, "sell")
            # Outputs: wallet received these -> BUY
            _match_side(token_out_list, "buy")
            return out

        # Fallback shape (rare): tokenTransfers list with sources/targets
        # Try to infer direction: if 'mint' matches and 'fromUserAccount' vs 'toUserAccount'
        tts = tx.get("tokenTransfers") or []
        if tts:
            for e in tts:
                try:
                    if e.get("mint") != mint:
                        continue
                    amt = float(e.get("tokenAmount", 0))
                    if amt <= 0:
                        continue
                    # Heuristics: if 'fromUserAccount' exists -> wallet sent -> SELL
                    # else if 'toUserAccount' exists -> wallet received -> BUY
                    if e.get("fromUserAccount"):
                        out.append(("sell", amt, ts))
                    elif e.get("toUserAccount"):
                        out.append(("buy", amt, ts))
                except Exception:
                    continue

        return out
    except Exception:
        return out


# --- Cost basis estimation per wallet ---
def estimate_cost_basis(wallet: str, mint: str, current_price: float) -> Optional[CostBasis]:
    """
    Size-weighted avg cost (USD/token) for this wallet on this mint.
    Faster + cheaper:
      - smaller default history
      - cap processed swap events
      - per-call price cache for repeated windows
      - optional CB time budget
    """
    if not HELIUS_API_KEY:
        return None

    CB_TIME_BUDGET = int(os.getenv("CB_TIME_BUDGET_SEC", "12"))
    cb_t0 = time.time()

    # tiny in-function cache for price lookups keyed by ("pair"/"bq", key, t0, t1)
    price_cache: Dict[Tuple[str, str, int, int], List[Bar]] = {}

    def _bars_cached(kind: str, key: str, t0: int, t1: int, fetch_fn: Callable[[], List[Bar]]) -> List[Bar]:
        k = (kind, key, t0, t1)
        bs = price_cache.get(k)
        if bs is not None:
            return bs
        bs = fetch_fn()
        price_cache[k] = bs
        return bs

    def _price_at(ts: int, best_pair: Optional[str], best_quote: Optional[str]) -> Optional[float]:
        t0 = max(0, ts - 180)  # wider window to improve hit rate
        t1 = ts + 120

        if best_pair:
            bars = _bars_cached("pair", best_pair, t0, t1, lambda: _be_ohlcv_pair(best_pair, t0, t1))
            p = _interp_price_at(bars, ts)
            if p: return p

        if best_quote:
            bars = _bars_cached("bq", f"{mint}:{best_quote}", t0, t1, lambda: _be_ohlcv_bq(mint, best_quote, t0, t1))
            p = _interp_price_at(bars, ts)
            if p: return p

        # USDC as final bq attempt
        bars = _bars_cached("bq", f"{mint}:{USDC_MINT_SOL}", t0, t1, lambda: _be_ohlcv_bq(mint, USDC_MINT_SOL, t0, t1))
        p = _interp_price_at(bars, ts)
        if p: return p

        return _be_spot_price(mint)

    def _cb_for_wallet(w: str) -> Optional[CostBasis]:
        # lighter history by default; escalate only if needed
        txs = _helius_enhanced(w, limit=int(os.getenv("CB_TX_LIMIT", "60")))
        if not txs:
            return None

        # best pair/quote once
        meta = _ds_meta(mint)
        try:
            _, _, _, best_pair, best_quote, _ = meta
        except ValueError:
            _, _, _, best_pair, best_quote = meta

        buys: List[Tuple[float, float]] = []
        sells_qty = 0.0
        processed_swaps = 0
        MAX_SWAPS = int(os.getenv("CB_MAX_SWAPS", "24"))

        for tx in txs:
            if CB_TIME_BUDGET and (time.time() - cb_t0) >= CB_TIME_BUDGET:
                # stop early; use what we have
                break
            for side, qty_ui, ts in _tx_swap_events_for_mint(tx, mint):
                if qty_ui <= 0:
                    continue
                processed_swaps += 1
                if processed_swaps > MAX_SWAPS:
                    break

                price = _price_at(ts, best_pair, best_quote)
                if not price or price <= 0:
                    continue

                if side == "buy":
                    buys.append((qty_ui, float(price)))
                else:
                    sells_qty += qty_ui
            if processed_swaps > MAX_SWAPS:
                break

        if not buys:
            return None

        total_qty = sum(q for q, _ in buys) - sells_qty
        if total_qty <= 0:
            total_qty = sum(q for q, _ in buys)

        wsum = sum(q * p for q, p in buys)
        qty_sum = sum(q for q, _ in buys)
        if qty_sum <= 0:
            return None

        avg_cost = wsum / qty_sum
        pnl_pct = ((current_price - avg_cost) / avg_cost) * 100.0 if avg_cost > 0 else 0.0
        curr_val = max(0.0, total_qty) * current_price

        return CostBasis(address=w,
                         avg_cost_usd_per_token=float(avg_cost),
                         current_value_usd=float(curr_val),
                         pnl_pct=float(pnl_pct))

    cb = _cb_for_wallet(wallet)
    if cb is not None:
        return cb

    owner = _resolve_owner_wallet(wallet)
    if owner and owner != wallet:
        return _cb_for_wallet(owner)
    return None


def compute_sell_pressure_ratio_v3(mint: str, current_price: float) -> Tuple[float, int]:
    """
    Use Birdeye V3 /defi/v3/token/txs-by-volume to estimate sell pressure:
        ratio = sell_volume_usd / total_volume_usd
    Returns (ratio in [0,1], sample_count) or (9.99, 0) on error/no data.
    """
    if not BIRDEYE_API_KEY:
        print("[sell_pressure_v3] No Birdeye API key.")
        return 9.99, 0
    if current_price <= 0:
        return 9.99, 0

    base_url = "https://public-api.birdeye.so/defi/v3/token/txs-by-volume"
    now = _now()
    after_time = now - SELL_VOLUME_LOOKBACK_SEC
    headers = _be_headers()  # already includes x-chain + X-API-KEY

    def fetch_totals(tx_type: str) -> Tuple[float, int]:
        total_usd = 0.0
        count = 0
        offset = 0
        has_next = True
        while has_next and offset < 10000:
            params = {
                "token_address": mint,
                "volume_type": "usd",
                "tx_type": tx_type,                 # "sell" or "all"
                "min_volume": MIN_SELL_VOLUME_USD,  # ignore dust
                "after_time": after_time,
                "sort_by": "block_unix_time",
                "sort_type": "desc",
                "offset": offset,
                "limit": 500,
                "ui_amount_mode": "scaled",
            }
            try:
                r = requests.get(base_url, params=params, headers=headers, timeout=8)
                if r.status_code != 200:
                    print(f"[sell_pressure_v3] API {tx_type} error {r.status_code}: {r.text[:140]}")
                    return total_usd, count
                data = (r.json() or {}).get("data") or {}
                items = data.get("items") or []
                if not items:
                    break
                for it in items:
                    try:
                        total_usd += float(it.get("volume_usd") or 0.0)
                        count += 1
                    except Exception:
                        continue
                has_next = bool(data.get("has_next"))
                offset += len(items)
                if len(items) < 500:
                    break
            except Exception as e:
                print(f"[sell_pressure_v3] Exception {tx_type}: {e}")
                return total_usd, count
        return total_usd, count

    sell_usd, sell_n = fetch_totals("sell")
    total_usd, tot_n = fetch_totals("all")

    if total_usd <= 0:
        return 9.99, 0

    ratio = sell_usd / total_usd
    ratio = max(0.0, min(1.0, ratio))
    return ratio, max(sell_n, tot_n)

# =========================
# Exhaustion & Stability Analysis
# =========================
def compute_price_stability(bars: List[Bar]) -> float:
    """Enhanced stability calculation that handles edge cases better"""
    if not bars:
        return 0.0
    
    # NEW: If we have very few bars, return a neutral stability score
    if len(bars) < 5:
        print(f"[stability] Warning: only {len(bars)} bars available, using neutral stability")
        return 0.7  # Neutral score instead of 0.0
    
    cutoff = bars[-1].t - STABILITY_LOOKBACK_MIN * 60
    window = [b for b in bars if b.t >= cutoff]
    
    # If window is too small, use all available bars but warn
    if len(window) < 5:
        window = bars[-min(10, len(bars)):]  # Use last 10 bars or whatever available
        print(f"[stability] Using {len(window)} available bars for stability calculation")
    
    if len(window) < 3:
        return 0.5  # Default neutral stability
    
    prices = [b.p for b in window if b.p > 0]
    if len(prices) < 3:
        return 0.5
    
    try:
        mean_p = statistics.mean(prices)
        if mean_p <= 0: 
            return 0.5
        
        price_range = max(prices) - min(prices)
        if price_range <= 0:
            return 1.0  # Perfect stability if no movement
        
        vol = price_range / mean_p
        stability = max(0.0, 1.0 - min(vol, 1.0))  # Cap volatility at 100%
        
        print(f"[stability] Price stability over {len(window)} bars: {stability:.3f}")
        return stability
    except Exception as e:
        print(f"[stability] Error calculating stability: {e}")
        return 0.5  # Neutral on error

def compute_holder_activity_decline(mint: str) -> Tuple[float, int]:
    """
    Calculates inactivity among top holders, but only over wallets we can evaluate.
    - A wallet is 'inactive' if sold out (~90%+) or dormant (no tx in HOLDER_DORMANCY_HOURS).
    - Wallets with no readable history are skipped (unknown), not counted against decline.
    Returns (decline_ratio, evaluated_count).
    """
    holders = fetch_top_holders(mint, BAGH_TOP_HOLDERS_LIMIT)
    if not holders:
        print("[activity_decline] No holder data; returning 0.0.")
        return 0.0, 0

    inactive = 0
    evaluated = 0
    now_ts = _now()

    for h in holders:
        txs = _helius_enhanced(h.address, limit=60) if HELIUS_API_KEY else []
        if not txs:
            continue  # skip unknowns

        evaluated += 1
        last_ts = 0
        sold_amount = 0.0

        for tx in txs:
            last_ts = max(last_ts, int(tx.get("timestamp") or 0))
            for side, qty_ui, ts in _tx_swap_events_for_mint(tx, mint):
                if side == "sell":
                    sold_amount += qty_ui

        sold_out = (sold_amount >= (h.ui_amount * 0.9)) if h.ui_amount > 0 else False
        dormant = (now_ts - last_ts) >= HOLDER_DORMANCY_HOURS * 3600 if last_ts > 0 else False

        if sold_out or dormant:
            inactive += 1

    if evaluated == 0:
        print("[activity_decline] No evaluable holders; returning 0.0.")
        return 0.0, 0

    decline = inactive / evaluated
    print(f"[activity_decline] Holder activity decline: {decline:.3f} (inactive={inactive}/{evaluated}, total={len(holders)})")
    return decline, evaluated

# =========================
# Market Data Module
# =========================
def fetch_birdeye_1m_bars(mint: str, minutes: int) -> List[Bar]:
    """
    Robust 1m bars with:
      - backoff retries for transient 4xx/5xx
      - multiple sources (pair -> base_quote(ds) -> preferred quotes -> mint)
      - trades→bars as last *data* fallback
      - 5m fallback when 1m is too sparse
      - only then, optional DS synthetic (if enabled)
    """
    try:
        enable_ds_synthetic = ENABLE_DS_SYNTHETIC
    except NameError:
        enable_ds_synthetic = int(os.getenv("ENABLE_DS_SYNTHETIC", "1"))

    if not BIRDEYE_API_KEY:
        print(f"[market] Birdeye fetch skipped for {mint[:6]}: No API key.")
        return []

    time_to   = int(time.time())
    time_from = time_to - minutes * 60
    headers   = _be_headers()

    # helper: GET with tiny retry/backoff
    def _get(url: str, params: dict, timeout=5, attempts=3, pause=0.35):
        last = None
        for i in range(attempts):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=timeout)
                if r.status_code == 200:
                    return r
                if r.status_code in (408, 425, 429, 500, 502, 503, 504):
                    time.sleep(pause * (i + 1))
                    last = r
                    continue
                return r
            except Exception as e:
                last = e
                time.sleep(pause * (i + 1))
        return last

    # discover best pair/quote + cheap price
    ds_pair = ds_quote = None
    ds_price = None
    try:
        mc_usd, m5_total, created_ms, ds_pair, ds_quote, ds_price = _ds_meta(mint)  # may return 6-tuple
    except Exception:
        try:
            mc_usd, m5_total, created_ms, ds_pair, ds_quote = _ds_meta(mint)        # back-compat 5-tuple
        except Exception:
            pass
    if ds_price is None:
        try:
            r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=3)
            if r.status_code == 200:
                pairs = (r.json() or {}).get("pairs") or []
                pairs = [p for p in pairs if p.get("chainId") == "solana"]
                if pairs:
                    pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0), reverse=True)
                    ds_price = float(pairs[0].get("priceUsd") or 0) or None
        except Exception:
            pass

    def _bars_from_items(items: List[dict]) -> List[Bar]:
        bars = [Bar(int(it["unixTime"]), float(it["close"])) for it in (items or []) if it.get("unixTime") and it.get("close")]
        bars.sort(key=lambda b: b.t)
        return bars

    # 1) pair level (1m)
    if ds_pair:
        r = _get("https://public-api.birdeye.so/defi/ohlcv",
                 {"address": ds_pair, "type": "1m", "time_from": time_from, "time_to": time_to})
        if hasattr(r, "status_code") and r.status_code == 200:
            items = ((r.json() or {}).get("data") or {}).get("items") or []
            bars = _bars_from_items(items)
            print(f"[market] pair OHLCV bars={len(bars)}")
            if len(bars) >= 2: return bars

    # 2) base_quote with DS quote (1m)
    if ds_quote:
        r = _get("https://public-api.birdeye.so/defi/ohlcv/base_quote",
                 {"base_address": mint, "quote_address": ds_quote, "type": "1m", "time_from": time_from, "time_to": time_to})
        if hasattr(r, "status_code") and r.status_code == 200:
            items = ((r.json() or {}).get("data") or {}).get("items") or []
            bars = _bars_from_items(items)
            print(f"[market] base_quote(DS quote) bars={len(bars)}")
            if len(bars) >= 2: return bars

    # 3) base_quote with preferred quotes (1m)
    for quote in PREFERRED_QUOTES:
        print(f"[market] trying base_quote {mint[:6]} / {quote[:6]}")
        r = _get("https://public-api.birdeye.so/defi/ohlcv/base_quote",
                 {"base_address": mint, "quote_address": quote, "type": "1m", "time_from": time_from, "time_to": time_to})
        if hasattr(r, "status_code") and r.status_code == 200:
            items = ((r.json() or {}).get("data") or {}).get("items") or []
            bars = _bars_from_items(items)
            print(f"[market] .OK {quote[:6]} bars={len(bars)}")
            if len(bars) >= 2: return bars

    # 4) mint-level (1m)
    print(f"[market] fallback mint OHLCV for {mint[:6]}")
    r = _get("https://public-api.birdeye.so/defi/ohlcv",
             {"address": mint, "type": "1m", "time_from": time_from, "time_to": time_to})
    if hasattr(r, "status_code") and r.status_code == 200:
        items = ((r.json() or {}).get("data") or {}).get("items") or []
        bars = _bars_from_items(items)
        print(f"[market] mint OHLCV bars={len(bars)}")
        if len(bars) >= 2: return bars

    # 4.5) trades→bars data fallback (still "real" data)
    try:
        bars = _be_trades_to_1m_bars(mint, time_from, time_to)
        if len(bars) >= 2:
            return bars
    except Exception as e:
        print(f"[market] trades→bars fallback error: {e}")

    # 5) 5-minute fallback (often available when 1m is too sparse)
    def _try_5m(url: str, params: dict) -> List[Bar]:
        pr = dict(params); pr["type"] = "5m"
        rr = _get(url, pr)
        if hasattr(rr, "status_code") and rr.status_code == 200:
            items = ((rr.json() or {}).get("data") or {}).get("items") or []
            bs = _bars_from_items(items)
            # expand each 5m close into 5 one-minute "steps" for downstream math to behave sanely
            out = []
            for b in bs:
                for i in range(5):
                    out.append(Bar(b.t - (4 - i)*60, b.p))
            return out
        return []

    # try 5m through the same preference order
    if ds_pair:
        bars = _try_5m("https://public-api.birdeye.so/defi/ohlcv",
                       {"address": ds_pair, "type": "5m", "time_from": time_from, "time_to": time_to})
        if len(bars) >= 2: 
            print(f"[market] pair OHLCV 5m→1m bars={len(bars)}")
            return bars
    if ds_quote:
        bars = _try_5m("https://public-api.birdeye.so/defi/ohlcv/base_quote",
                       {"base_address": mint, "quote_address": ds_quote, "type": "5m", "time_from": time_from, "time_to": time_to})
        if len(bars) >= 2:
            print(f"[market] base_quote(DS) 5m→1m bars={len(bars)}")
            return bars
    for quote in PREFERRED_QUOTES:
        bars = _try_5m("https://public-api.birdeye.so/defi/ohlcv/base_quote",
                       {"base_address": mint, "quote_address": quote, "type": "5m", "time_from": time_from, "time_to": time_to})
        if len(bars) >= 2:
            print(f"[market] base_quote {quote[:6]} 5m→1m bars={len(bars)}")
            return bars
    bars = _try_5m("https://public-api.birdeye.so/defi/ohlcv",
                   {"address": mint, "type": "5m", "time_from": time_from, "time_to": time_to})
    if len(bars) >= 2:
        print(f"[market] mint OHLCV 5m→1m bars={len(bars)}")
        return bars

    # 6) last resort synthetic (same as before)
    try:
        if enable_ds_synthetic and ds_price and ds_price > 0:
            now_t = int(time.time())
            bars = [Bar(now_t - 60, float(ds_price)), Bar(now_t, float(ds_price))]
            print(f"[market] synthetic bars=2 from DS price for {mint[:6]}")
            return bars
    except Exception as e:
        print(f"[market] synthetic bars error: {e}")

    print(f"[market] No reliable bars for {mint[:6]} across pair/base_quote/mint, trades, 5m, and DS synthetic.")
    return []


def fetch_dexscreener_meta(mint: str) -> Tuple[Optional[float], Optional[int], Optional[int]]:
    try:
        print(f"[market] Fetching Dexscreener meta for {mint[:6]}")
        r = requests.get(f'https://api.dexscreener.com/latest/dex/tokens/{mint}', timeout=3)
        if r.status_code != 200:
            print(f"[market] Dexscreener API error for {mint[:6]}: Status {r.status_code}")
            return None, None, None
        pairs = (r.json() or {}).get('pairs') or []
        if not pairs:
            print(f"[market] No pairs found on Dexscreener for {mint[:6]}")
            return None, None, None
        pairs.sort(key=lambda p: float((p.get('liquidity') or {}).get('usd') or 0.0), reverse=True)
        best = pairs[0]
        mc_usd = float(best['marketCap']) if best.get('marketCap') else None
        m5_total = int((best.get('txns', {}).get('m5', {}).get('buys', 0))) + int((best.get('txns', {}).get('m5', {}).get('sells', 0)))
        created_ms = int(best['pairCreatedAt']) if best.get('pairCreatedAt') else None
        print(f"[market] Dexscreener meta for {mint[:6]}: MC=${mc_usd}, m5={m5_total}")
        return mc_usd, m5_total, created_ms
    except Exception as e:
        print(f"[market] Dexscreener fetch exception for {mint[:6]}: {e}")
        return None, None, None

def jupiter_quote(input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Optional[dict]:
    try:
        r = requests.get(JUPITER_QUOTE_URL, params={
            "inputMint": input_mint, "outputMint": output_mint,
            "amount": amount, "slippageBps": slippage_bps,
        }, timeout=3)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

def route_health_ok(base_mint: str, token_mint: str) -> Tuple[bool, str, float, float]:
    """Enhanced route health with better error handling and logging"""
    try:
        buy_amt = int(ROUTE_TEST_USD * (10**6))
        q_buy = jupiter_quote(base_mint, token_mint, buy_amt, ROUTE_SLIPPAGE_BPS)
        if not q_buy: 
            return False, "no_buy_quote", 99.0, 99.0
        
        imp_buy = float(q_buy.get("priceImpactPct", 99.0)) * 100.0
        out_amt = int(q_buy.get("outAmount", 0))
        
        if out_amt <= 0: 
            return False, "buy_out_zero", imp_buy, 99.0
        
        q_sell = jupiter_quote(token_mint, base_mint, out_amt, ROUTE_SLIPPAGE_BPS)
        if not q_sell: 
            return False, "no_sell_quote", imp_buy, 99.0
        
        imp_sell = float(q_sell.get("priceImpactPct", 99.0)) * 100.0
        
        # More lenient impact thresholds for small caps
        effective_max_impact = ROUTE_MAX_IMPACT_PCT
        if imp_buy > 50.0 or imp_sell > 50.0:  # Extreme levels
            effective_max_impact = min(ROUTE_MAX_IMPACT_PCT * 1.5, 60.0)
        
        ok = imp_buy < effective_max_impact and imp_sell < effective_max_impact
        reason = f"buy={imp_buy:.1f}%_sell={imp_sell:.1f}%_max={effective_max_impact:.1f}%"
        
        return ok, reason, imp_buy, imp_sell
    except Exception as e:
        print(f"[route_health] Exception: {e}")
        return False, f"route_exception:{str(e)[:50]}", 99.0, 99.0

# =========================
# Price Analytics (existing)
# =========================
def pct_change(bars: List[Bar], minutes: int) -> Optional[float]:
    if not bars or len(bars) < 2: return None
    now_t = bars[-1].t
    cutoff = now_t - minutes * 60
    p_old = _interp_price_at(bars, cutoff)
    p_new = bars[-1].p
    if p_old is None or p_old <= 0: return None
    return (p_new - p_old) / p_old * 100.0

def refined_bottom(bars: List[Bar], lookback_min: int = 240, silence_min: int = 8) -> Tuple[Optional[int], Optional[float]]:
    if not bars: return None, None
    window = [b for b in bars if b.t >= bars[-1].t - lookback_min * 60]
    if not window: return None, None

    prices = [b.p for b in window]
    high_p = max(prices)
    high_idx = len(prices) - 1 - prices[::-1].index(high_p)
    search_window = window[high_idx:]
    if not search_window: return None, None

    low_p = min(b.p for b in search_window)
    low_bar = next(b for b in search_window if b.p == low_p)
    return low_bar.t, low_bar.p

def drawdown_from_recent_high(bars: List[Bar], lookback_min: int = 240) -> Optional[float]:
    if not bars: return None
    window = [b for b in bars if b.t >= bars[-1].t - lookback_min * 60]
    if not window: return None
    high_p = max(b.p for b in window)
    curr_p = bars[-1].p
    return ((high_p - curr_p) / high_p) * 100.0 if high_p > 0 else 0.0

# =========================
# BQ and RR Models (existing)
# =========================
def friction_pct() -> float:
    return (SLIPPAGE_BPS + FEES_BPS) / 100.0

# REVERTED: Previous version's simple RR calculation
def rr_from_here(curr_pnl_pct: float) -> float:
    # Simple fixed RR ratio regardless of current PnL
    return TAKE_PROFIT_PCT / STOP_LOSS_PCT

def bottom_quality(bars: List[Bar], low_t: int, low_p: float, m5: int, mc: Optional[float]) -> float:
    if not bars or not low_t or not low_p: return 0.0
    dd = drawdown_from_recent_high(bars, 240) or 0.0
    dd_score = min(dd / 60.0, 1.0)
    post_low_prices = [b.p for b in bars if b.t > low_t]
    if len(post_low_prices) > 5:
        returns = [(post_low_prices[i] / post_low_prices[i-1]) - 1 for i in range(1, len(post_low_prices))]
        vol = statistics.stdev(returns) if len(returns) > 1 else 0.0
        vol_score = 1.0 - min(vol / 0.02, 1.0)
    else:
        vol_score = 0.5
    pc5 = pct_change(bars, 5) or 0.0
    rec_score = 0.5 + (pc5 / 20.0)
    mc_score = 0.5
    if mc:
        if 10000 <= mc <= 60000: mc_score = 1.0
        elif mc < 10000: mc_score = 0.6 + 0.4 * (mc / 10000)
        else: mc_score = max(0.0, 0.9 - 0.9 * (mc - 60000) / 100000)
    bq = (dd_score * 0.45) + (vol_score * 0.15) + (rec_score * 0.20) + (mc_score * 0.20)
    activity_factor = min(1.0, (m5 or 0) / 8.0)
    bq = max(0.0, min(1.0, bq))
    return round(bq * activity_factor, 3)

# =========================
# Candidate Analysis
def analyze_candidate(scn: dict) -> Optional[dict]:
    """Enhanced candidate analysis with better error handling"""
    try:
        _STABILITY_MIN_BARS = STABILITY_MIN_BARS
    except NameError:
        _STABILITY_MIN_BARS = 5  # Lowered default
    
    try:
        _HOLDER_DECLINE_MIN_EVAL = HOLDER_DECLINE_MIN_EVAL
    except NameError:
        _HOLDER_DECLINE_MIN_EVAL = 5  # Lowered default
    
    try:
        _LOG_REVIVAL_DETAILS = LOG_REVIVAL_DETAILS
    except NameError:
        _LOG_REVIVAL_DETAILS = 1

    mint = scn["mint"]
    symbol = scn.get("symbol", mint[:6])
    print(f"[analysis] Analyzing candidate: {symbol} ({mint[:6]})")

    # Deadlist check
    dl = deadlist_load()
    if is_dead(mint, dl, _now(), DEADLIST_DEFAULT_TTL_MIN):
        print(f"[analysis] Dropping {symbol}: deadlisted.")
        return None

    # Get bars with fallback
    bars = fetch_birdeye_1m_bars(mint, BIRDEYE_BARS_MIN)
    
    # NEW: More lenient bar requirement
    if len(bars) < 2:
        print(f"[analysis] Dropping {symbol}: insufficient bar data ({len(bars)}<2)")
        return None
    
    current_price = bars[-1].p
    if current_price <= 0:
        print(f"[analysis] Dropping {symbol}: invalid current price")
        return None

    # Market data
    mc, m5, created_ms = fetch_dexscreener_meta(mint)
    low_t, low_p = refined_bottom(bars)
    
    # NEW: Better bottom detection fallback
    if not low_t or not low_p:
        print(f"[analysis] Using fallback bottom detection for {symbol}")
        if len(bars) >= 2:
            # Use the lowest point in available bars
            low_bar = min(bars, key=lambda b: b.p)
            low_t, low_p = low_bar.t, low_bar.p
        else:
            print(f"[analysis] Dropping {symbol}: could not determine bottom.")
            return None

    # Route health with adjusted thresholds
    MIN_ROUTE_M5 = int(os.getenv("MIN_ROUTE_M5", "5"))  # Lowered from 10
    MIN_ROUTE_MC = float(os.getenv("MIN_ROUTE_MC", "30000"))  # Lowered from 100000
    
    route_checked = False
    route_ok = True
    imp_buy = imp_sell = 0.0
    
    if ((m5 or 0) >= MIN_ROUTE_M5) and ((mc or 0.0) >= MIN_ROUTE_MC):
        route_checked = True
        route_ok, route_reason, imp_buy, imp_sell = route_health_ok(USDC_MINT_SOL, mint)
        print(f"[analysis] Route health for {symbol}: {route_ok} ({route_reason})")
    else:
        print(f"[analysis] Route health skipped for {symbol} (m5={m5}, mc={mc})")

    # Price changes
    pc5 = pct_change(bars, 5) or 0.0
    pc60 = pct_change(bars, 60) or 0.0
    bq = bottom_quality(bars, low_t, low_p, m5 or 0, mc)

    # Base RR calculation
    rr_entry = rr_from_here(0)
    rr_comp = rr_entry * (1 + (bq * 0.5) + (pc5 / 40.0))
    
    # Route penalty only if checked and failed
    if route_checked and not route_ok:
        rr_comp *= 0.7  # Reduced penalty
        print(f"[analysis] Applied route penalty: {rr_comp:.2f}")

    # Apply enhanced RR calculation
    rr_comp = enhanced_rr_calculation({"rr": rr_comp, "m5": m5, "mc": mc, "revival_gate_ok": False, "price_stability": 0.0}, bars)

    # Revival Gate with better handling of limited data
    sell_pressure, sell_n = compute_sell_pressure_ratio_v3(mint, current_price)
    price_stab = compute_price_stability(bars)
    holder_decline, holder_eval = compute_holder_activity_decline(mint)

    # Adjusted thresholds for limited data
    stability_points = len([b for b in bars if b.t >= (bars[-1].t - STABILITY_LOOKBACK_MIN * 60)])
    enforce_stability = stability_points >= _STABILITY_MIN_BARS
    enforce_decline = holder_eval >= _HOLDER_DECLINE_MIN_EVAL

    reasons = []
    revival_gate_fail_count = 0

    # Route check
    if route_checked and not route_ok:
        reasons.append("route_bad")
        revival_gate_fail_count += 1
    elif not route_checked:
        reasons.append("route_skipped(low_flow)")

    # Sell pressure check (more lenient)
    if sell_pressure < 9.0:
        if sell_pressure > SELL_PRESSURE_MAX:
            reasons.append(f"sell_pressure={sell_pressure:.2f}>{SELL_PRESSURE_MAX}")
            revival_gate_fail_count += 1
    else:
        reasons.append("sell_pressure_skipped(no_data)")

    # Stability check
    if enforce_stability:
        if price_stab < STABILITY_MIN:
            reasons.append(f"stability={price_stab:.2f}<{STABILITY_MIN}")
            revival_gate_fail_count += 1
    else:
        reasons.append(f"stability_skipped({stability_points}pts<{_STABILITY_MIN_BARS})")

    # Holder decline check
    if enforce_decline:
        if holder_decline < HOLDER_ACTIVITY_DECLINE_MIN:
            reasons.append(f"decline={holder_decline:.2f}<{HOLDER_ACTIVITY_DECLINE_MIN}")
            revival_gate_fail_count += 1
    else:
        reasons.append(f"decline_skipped(eval={holder_eval}<{_HOLDER_DECLINE_MIN_EVAL})")

    revival_gate_ok = (revival_gate_fail_count == 0)

    if _LOG_REVIVAL_DETAILS:
        print(f"[revival_gate] {symbol}: OK={revival_gate_ok}, fails={revival_gate_fail_count}/4")
        print(f"[revival_gate] Details: sell_p={sell_pressure:.2f}, stability={price_stab:.2f}, decline={holder_decline:.2f}")
        print(f"[revival_gate] Reasons: {', '.join(reasons)}")

    # Apply penalty only if multiple failures
    if not revival_gate_ok:
        penalty_severity = min(0.5 + (revival_gate_fail_count * 0.1), 0.8)  # 0.6-0.8 penalty
        rr_comp *= penalty_severity
        print(f"[revival_gate] Applied RR penalty {penalty_severity:.2f}. New RR: {rr_comp:.2f}")

    print(f"[analysis] Completed {symbol}: BQ={bq:.3f}, RR={rr_comp:.2f}, m5={m5}, mc=${mc}")

    return {
        "raw_scn": scn,
        "mint": mint,
        "symbol": symbol,
        "mc": mc,
        "m5": m5,
        "pc5": round(pc5, 1) if pc5 is not None else None,
        "pc60": round(pc60, 1) if pc60 is not None else None,
        "low_t": low_t,
        "low_p": low_p,
        "bq": float(bq),
        "rr": float(rr_comp),
        "route_ok": bool(route_ok),
        "imp_buy": float(imp_buy),
        "imp_sell": float(imp_sell),
        "sell_pressure_ratio": None if sell_pressure >= 9.0 else float(sell_pressure),
        "price_stability": float(price_stab),
        "holder_activity_decline": float(holder_decline),
        "revival_gate_ok": bool(revival_gate_ok),
    }

# =========================
# Repeat Entry & Swapping (existing + slight tweaks)
# =========================
def read_repeat_registry() -> dict:
    try: return json.load(open(REPEAT_FILE, "r", encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError): return {"mints": {}}

def write_repeat_registry(data: dict):
    with open(REPEAT_FILE, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)

def apply_repeat_policy(analysis: dict, reg: dict) -> Tuple[bool, float, str]:
    mint_data = reg.get("mints", {}).get(analysis["mint"])
    if not mint_data:
        print(f"[policy] Repeat policy for {analysis['symbol']}: First time entry.")
        return True, 1.0, "first_time"

    times = mint_data.get("times", 1)
    last_low = mint_data.get("last_low", 0)

    new_low_threshold = last_low * (1.0 - REPEAT_NEW_LOW_PCT / 100.0)
    if analysis["low_p"] < new_low_threshold:
        print(f"[policy] Repeat policy for {analysis['symbol']}: OK (new structural low).")
        return True, 1.0, "new_structural_low"

    last_ts = mint_data.get("last_ts", 0)
    if _now() - last_ts < REPEAT_COOLDOWN_MIN * 60:
        remaining = (REPEAT_COOLDOWN_MIN * 60 - (_now() - last_ts)) / 60
        print(f"[policy] Repeat policy for {analysis['symbol']}: DROP (cooldown {remaining:.0f}m remaining).")
        return False, 1.0, f"cooldown_{remaining:.0f}m"

    multiplier = REPEAT_DECAY_FACTOR ** min(times, 5)
    print(f"[policy] Repeat policy for {analysis['symbol']}: OK (repeat entry, multiplier={multiplier:.2f}).")
    return True, multiplier, "repeat_stricter"

def update_repeat_registry(reg: dict, analysis: dict):
    mint = analysis["mint"]
    entry = reg.get("mints", {}).get(mint, {})
    entry["last_ts"] = _now()
    entry["last_low"] = analysis["low_p"]
    entry["times"] = entry.get("times", 0) + 1
    reg.setdefault("mints", {})[mint] = entry
    write_repeat_registry(reg)

def get_incumbents_ctx() -> List[dict]:
    incumbents = []
    now_ts = _now()
    for fn in os.listdir(OPEN_POS_DIR):
        if not fn.endswith(".json"):
            continue
        try:
            pos = json.load(open(os.path.join(OPEN_POS_DIR, fn), "r", encoding="utf-8"))
            entry_px = float(pos.get("entry_px") or 0.0)
            entry_ts = int(pos.get("entry_ts") or now_ts)
            last_px = float((pos.get("note", {}) or {}).get("last_px") or pos.get("max_px") or entry_px or 0.0)
            if entry_px <= 0:
                continue
            pnl_pct = (last_px - entry_px) / entry_px * 100.0
            rr = rr_from_here(pnl_pct)
            incumbents.append({
                "mint": pos["mint"],
                "pnl": round(pnl_pct, 2),
                "rr": round(rr, 2),
                "held_sec": max(0, now_ts - entry_ts),
            })
        except Exception:
            continue
    incumbents.sort(key=lambda x: (x["rr"], x["pnl"]))  # worst first
    return incumbents

def gpt_swap(candidate: dict, incumbents: List[dict], threshold: float) -> Tuple[bool, Optional[str], str]:
    if not GPT_SWAP_ON or not OPENAI_API_KEY: return False, None, "gpt_disabled"

    system_prompt = (
        "Decide if a new candidate should replace an existing position. "
        "You will receive ONLY evictable incumbents (non-profitable under configured threshold and past hold-grace). "
        "Let worst_rr be the lowest rr among incumbents. "
        f"The advantage_threshold is {threshold:.2f}. "
        "Swap ONLY if candidate_rr >= worst_rr * (1 + advantage_threshold). "
        "Choose the single incumbent to close: the one with the LOWEST rr (tie-breaker: lowest pnl). "
        "Return strict JSON: {\"swap\":true|false, \"close_mint\":\"...|null\", \"reason\":\"...\"}"
    )

    user_payload = {
        "advantage_threshold": threshold,
        "candidate": {k: v for k, v in candidate.items() if k in ["mint", "symbol", "rr", "bq", "pc5", "route_ok", "revival_gate_ok"]},
        "incumbents": incumbents
    }

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    body = {
        "model": GPT_MODEL, "temperature": 0.1, "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": json.dumps(user_payload)}]
    }

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body, timeout=GPT_HTTP_TIMEOUT_SEC)
        if r.status_code == 200:
            data = r.json()["choices"][0]["message"]["content"]
            res = json.loads(data)
            if res.get("swap") and res.get("close_mint"):
                return True, res["close_mint"], res.get("reason", "gpt_swap")
    except Exception as e:
        return False, None, f"gpt_exception_{e}"

    return False, None, "gpt_no_swap"

def admit_or_swap(analysis: dict, repeat_registry: dict) -> dict:
    """
    Enhanced swap logic considering position age and PnL trajectory
    """
    rr_eff = float(analysis.get("rr") or 0.0)
    symbol = analysis.get("symbol") or analysis.get("mint", "")[:6]
    reason = "base"

    # Respect deadlist and trivial rejects handled upstream (analysis phase)
    # At this point, we only arbitrate capacity & advantage.

    incumbents = get_incumbents_ctx()  # reads /open_positions and computes rr/pnl per live position
    if len(incumbents) < MAX_CONCURRENT_TRADES:
        print(f"[decision] Admitting {symbol}: slot available.")
        return {"action": "admit", "rr": rr_eff, "why": f"slot_available/{reason}"}

    # All slots are full → evaluate evictable set
    # Enhanced evictability criteria with position age protection
    evictables = [
        inc for inc in incumbents
        if inc["pnl"] <= EVICT_MAX_PNL_PCT and inc["held_sec"] >= max(MIN_HOLD_SEC_BEFORE_EVICT, MIN_HOLD_FOR_SWAP)
    ]

    if not evictables:
        print(f"[decision] All slots full, but no incumbents are evictable.")
        return {"action": "drop", "why": f"no_evictable_incumbents/{reason}"}

    # NEW: Prefer swapping positions that are stagnant vs rising
    stagnant_positions = [p for p in evictables if abs(p["pnl"]) < 2.0]
    if stagnant_positions:
        # Swap stagnant positions first
        evictables = stagnant_positions

    # Compare RR vs worst evictable RR
    evictables.sort(key=lambda x: (x["rr"], x["pnl"]))  # worst first
    worst = evictables[0]
    worst_rr = float(worst["rr"])
    print(f"[decision] Candidate RR {rr_eff:.2f} vs worst evictable RR {worst_rr:.2f} (mint: {worst['mint'][:6]}).")

    threshold = worst_rr * (1.0 + REPEAT_MIN_ADVANTAGE)
    if rr_eff >= threshold:
        print(f"[decision] Heuristic SWAP is GO for {symbol}.")
        return {"action": "swap", "close_mint": worst["mint"], "rr": rr_eff, "why": f"heuristic_rr_advantage/{reason}"}

    # Optional GPT arbitration
    if GPT_SWAP_ON and OPENAI_API_KEY:
        print(f"[decision] Heuristic failed. Trying GPT arbitration for {symbol}.")
        do_swap, mint_to_close, gpt_reason = gpt_swap(analysis, evictables, REPEAT_MIN_ADVANTAGE)
        if do_swap and mint_to_close:
            # still honor the advantage condition against the chosen incumbent
            chosen = next((i for i in evictables if i["mint"] == mint_to_close), None)
            if chosen and rr_eff >= chosen["rr"] * (1.0 + REPEAT_MIN_ADVANTAGE):
                return {"action": "swap", "close_mint": mint_to_close, "rr": rr_eff, "why": f"gpt:{gpt_reason}/{reason}"}
            else:
                print(f"[decision] GPT swap rejected: RR does not meet required advantage over chosen incumbent.")

    return {"action": "drop", "why": f"no_advantage_or_protected/{reason}"}

def reevaluate_backlog_candidates(backlog: Backlog):
    """
    Re-analyze top backlog candidates when TE has free slots.
    NOW WITH FULL CAPACITY LOCK PROTECTION.
    """
    incumbents = get_incumbents_ctx()
    if len(incumbents) >= MAX_CONCURRENT_TRADES:
        return
    
    if len(backlog.items) == 0:
        return
    
    now = _now()
    print(f"[*] Free slot detected ({len(incumbents)}/{MAX_CONCURRENT_TRADES}). "
          f"Checking backlog ({len(backlog.items)} candidates)...")
    
    # Get top 3 candidates
    top_candidates = backlog.top_n(3)
    rechecked_candidates = []
    
    for i, candidate in enumerate(top_candidates, 1):
        mint = candidate.get("mint")
        symbol = candidate.get("symbol", mint[:6])
        
        # Per-candidate cooldown check
        last_analyzed = _BACKLOG_LAST_ANALYZED.get(mint, 0)
        cooldown_remaining = BACKLOG_CANDIDATE_COOLDOWN_SEC - (now - last_analyzed)
        if cooldown_remaining > 0:
            print(f"   → Skipping {symbol}: analyzed {int((now - last_analyzed))}s ago "
                  f"(cooldown: {int(cooldown_remaining)}s remaining)")
            continue
        
        print(f"   → Checking backlog candidate #{i}: {symbol} rr={candidate.get('rr', 0):.2f}")
        
        # Record analysis timestamp
        _BACKLOG_LAST_ANALYZED[mint] = now
        
        # Fresh analysis with current market data
        fresh_analysis = analyze_candidate(candidate["raw_scn"])
        if not fresh_analysis:
            print(f"     [x] Fresh analysis failed, removing from backlog.")
            backlog.remove(candidate)
            continue
        
        # Check price drift
        ref_price = float(fresh_analysis.get("low_p") or 0)
        if ref_price > 0:
            current_spot = _spot_price(fresh_analysis["mint"])
            if current_spot:
                drift_pct = _drift_pct(ref_price, current_spot)
                if drift_pct > MAX_PRICE_DRIFT_PCT:
                    print(f"     [x] Price drift too high ({drift_pct:.1f}% > {MAX_PRICE_DRIFT_PCT}%), skipping.")
                    continue
        
        # Check freshness/drift guard
        ok, why_fail = _fresh_enough_and_not_drifted(fresh_analysis)
        if not ok:
            print(f"     [x] Failed freshness check: {why_fail}")
            continue
        
        # Check deadlist
        dl = deadlist_load()
        if is_dead(fresh_analysis["mint"], dl, _now(), DEADLIST_DEFAULT_TTL_MIN):
            print(f"     [x] Now deadlisted, removing.")
            backlog.remove(candidate)
            continue
        
        # Check repeat policy
        repeat_reg = read_repeat_registry()
        ok_rep, rr_mult, rep_why = apply_repeat_policy(fresh_analysis, repeat_reg)
        if not ok_rep:
            print(f"     [x] Repeat policy rejected: {rep_why}")
            continue
        
        # Apply repeat multiplier
        fresh_analysis["rr"] *= rr_mult
        
        rechecked_candidates.append(fresh_analysis)
        print(f"     [✓] Valid candidate, fresh RR: {fresh_analysis['rr']:.2f}")
    
    if rechecked_candidates:
        # Pick the best one by RR
        best_candidate = max(rechecked_candidates, key=lambda x: x["rr"])
        symbol = best_candidate.get("symbol", best_candidate["mint"][:6])
        
        print(f"[+] Selected {symbol} as best revalidated candidate (rr={best_candidate['rr']:.2f})")
        
        # Calculate dynamic position size
        dynamic_size = calculate_dynamic_size(best_candidate)
        
        # Enrich and dispatch to executor
        meta_fields_to_add = {
            "bq", "rr", "pc5", "pc60", "m5", "mc", "low_t", "low_p", "route_ok",
            "imp_buy", "imp_sell", "sell_pressure_ratio", "price_stability",
            "holder_activity_decline", "revival_gate_ok"
        }
        
        enriched_scn = {
            **best_candidate["raw_scn"],
            "size_usd": dynamic_size,  # Apply dynamic sizing
            "meta": {
                **(best_candidate["raw_scn"].get("meta") or {}),
                "reason": f"backlog_recheck_dispatch",
                "pm_src": (best_candidate["raw_scn"].get("meta") or {}).get("reason", "unknown"),
                "dynamic_size": dynamic_size,
                **{k: best_candidate[k] for k in meta_fields_to_add if k in best_candidate}
            }
        }
        
        # ✅ CRITICAL FIX: Add capacity lock protection RIGHT before dispatch
        with _capacity_lock:
            # Last-second capacity re-check (race condition protection)
            incs = get_incumbents_ctx()
            if len(incs) >= MAX_CONCURRENT_TRADES:
                print(f"[*] Capacity filled during backlog recheck analysis; "
                      f"deferring {symbol} for next cycle")
                # Put candidate back in backlog with updated analysis
                backlog.put(best_candidate)
                return
            
            # Safe to dispatch - capacity still available
            _append_jsonl(SCENARIOS_EXEC, enriched_scn)
            backlog.remove(best_candidate)
            update_repeat_registry(read_repeat_registry(), best_candidate)
            print(f"[+] Dispatched {symbol} from backlog (revalidated, capacity-safe, size=${dynamic_size:.0f})")
    else:
        print("[*] No viable backlog candidate found after revalidation.")

# =========================
# Main Loop
# =========================
def main():
    print("🤖 Starting Enhanced Pre-Trade Manager with PnL Optimization.")
    tailer_scenarios = Tailer(SCENARIOS_IN)
    tailer_watch = Tailer(WATCH_QUEUE) if PM_READ_WATCHLIST else None
    backlog = Backlog(PM_BACKLOG_MAX, PM_BACKLOG_TTL_SEC)

    # periodic replay timer
    last_replay_ts = 0
    # NEW: Backlog recheck cooldown (avoid analyzing same tokens too frequently)
    last_backlog_recheck_ts = 0
    BACKLOG_RECHECK_COOLDOWN_SEC = 30  # Recheck backlog every 30 seconds max

    while True:
        try:  # ← ADDED: Global error handling wrapper
            print("\n--- PM Cycle Start ---")
            now = _now()
            
            # NEW: Cleanup stale backlog entries
            backlog.cleanup_stale()

            # NEW: Check for profit taking and early exits
            check_profit_taking()
            check_early_exit_signals()

            # 1) Ingest NEW candidates from Monitor/EM
            candidates = tailer_scenarios.read_new()

            # 2) Optional: ingest watchlist items and adapt to scenarios
            if tailer_watch:
                watch_items = tailer_watch.read_new()
                adapted = []
                if watch_items:
                    print(f"[*] Received {len(watch_items)} new item(s) from Watchlist.")
                for wi in watch_items:
                    try:
                        mint = wi.get("mint")
                        if not mint or not _watch_dedup_ok(mint, int(wi.get("seed_ts") or now)):
                            continue
                        adapted.append(adapt_watch_to_scn(wi))
                    except Exception as e:
                        print(f"[!] Error adapting watchlist item: {e}")
                candidates.extend(adapted)

            if not candidates:
                print("[*] No new candidates from Monitor/Watchlist.")
            else:
                print(f"[*] Received {len(candidates)} total new candidate(s).")

            # 3) REPLAY: periodically reread the last-N lines of scenarios.jsonl
            if SCN_REPLAY_LAST_N > 0 and (now - last_replay_ts) >= SCN_REPLAY_EVERY_SEC:
                replayed = replay_candidates_last_n(
                    SCENARIOS_IN,
                    last_n=SCN_REPLAY_LAST_N,
                    min_age_min=SCN_MIN_AGE_BEFORE_RECHECK_MIN,
                    cooldown_min=SCN_REPROCESS_COOLDOWN_MIN
                )
                if replayed:
                    print(f"[*] Replay pulled {len(replayed)} candidate(s) from last {SCN_REPLAY_LAST_N}.")
                    candidates.extend(replayed)
                last_replay_ts = now

            repeat_registry = read_repeat_registry()
            open_mints = {p["mint"] for p in get_incumbents_ctx()}

            # 4) Admit / Swap / Drop for NEW + REPLAYED candidates (REFACTORED)
            for scn in candidates:
                # throttle reprocessing of the same scenario/mint
                seen = seen_load()
                seen_touch(seen, scn)

                # REFACTORED: Use unified validation
                analysis = validate_and_enrich_candidate(scn, from_backlog=False)
                if not analysis:
                    print(f"[-] Validation failed for {scn.get('symbol', scn.get('mint', '???')[:6])}")
                    continue

                decision = admit_or_swap(analysis, repeat_registry)
                action = decision.get("action")
                print(f"[*] Decision for {analysis['symbol']}: {action.upper()} | RR: {analysis['rr']:.2f} | Reason: {decision['why']}")

                if action == "drop":
                    # Capacity/no-advantage style → backlog if still promising & not already open
                    is_capacity_drop = (
                        "no_advantage" in decision["why"]
                        or "no_evictable" in decision["why"]
                        or "bad_route_but_full" in decision["why"]
                    )
                    if is_capacity_drop and analysis.get("route_ok") and analysis["rr"] > 0.8 and analysis["mint"] not in open_mints:
                        print(f"[*] Adding {analysis['symbol']} to backlog.")
                        backlog.put(analysis)
                    continue

                # ✅ RESTORED: Freshness/drift guard right before dispatch
                ok, why_fail = _fresh_enough_and_not_drifted(analysis)
                if not ok:
                    print(f"[-] {analysis['symbol']} not dispatched (admit/swap): {why_fail}. Keeping candidate in backlog for a later pass.")
                    # ✅ RESTORED: Backlog logic for freshness failures
                    if analysis.get("route_ok") and analysis["rr"] > 0.8 and analysis["mint"] not in open_mints:
                        backlog.put(analysis)
                    continue

                # Calculate dynamic position size
                dynamic_size = calculate_dynamic_size(analysis)

                # Enrich for executor
                meta_fields_to_add = {
                    "bq", "rr", "pc5", "pc60", "m5", "mc", "low_t", "low_p", "route_ok",
                    "imp_buy", "imp_sell", "sell_pressure_ratio", "price_stability",
                    "holder_activity_decline", "revival_gate_ok"
                }
                enriched_scn = {
                    **analysis["raw_scn"],
                    "size_usd": dynamic_size,  # Apply dynamic sizing
                    "meta": {
                        **(analysis["raw_scn"].get("meta") or {}),
                        "reason": f"pm_{action}:{decision['why']}",
                        "pm_src": (analysis["raw_scn"].get("meta") or {}).get("reason", "unknown"),
                        "dynamic_size": dynamic_size,
                        **{k: analysis[k] for k in meta_fields_to_add if k in analysis}
                    }
                }

                if action == "swap":
                    close_mint = decision["close_mint"]
                    _append_jsonl(EXEC_CONTROL, {
                        "type": "FORCE_CLOSE", "ts": now, "mint": close_mint,
                        "reason": f"pm_swap:{decision['why']}"
                    })
                    print(f"[!] Sent FORCE_CLOSE for {close_mint[:6]}")

                # Capacity check RIGHT BEFORE dispatch with thread safety
                with _capacity_lock:
                    incs = get_incumbents_ctx()
                    if len(incs) >= MAX_CONCURRENT_TRADES:
                        print("[*] Capacity just filled; deferring dispatch and backlogging.")
                        backlog.put(analysis)
                        continue

                    _append_jsonl(SCENARIOS_EXEC, enriched_scn)
                    print(f"[+] Forwarded {analysis['symbol']} to executor (size=${dynamic_size:.0f}).")
                    update_repeat_registry(repeat_registry, analysis)
                    # ✅ RESTORED: Update open_mints after dispatch
                    open_mints = {p["mint"] for p in get_incumbents_ctx()}

            # 5) Promote from backlog — REFACTORED to use unified validation
            while True:
                incs = get_incumbents_ctx()
                if len(incs) >= MAX_CONCURRENT_TRADES:
                    break

                a = backlog.pop_best()
                if not a:
                    break

                print(f"[*] Attempting to promote {a['symbol']} from backlog (fresh analysis).")
                
                # REFACTORED: Use unified validation with backlog flag
                fresh = validate_and_enrich_candidate(a, from_backlog=True)
                if not fresh:
                    print(f"[-] Backlog promotion {a['symbol']}: validation failed.")
                    continue

                # Capacity re-check at the last moment with thread safety
                with _capacity_lock:
                    incs = get_incumbents_ctx()
                    if len(incs) >= MAX_CONCURRENT_TRADES:
                        print("[*] Capacity just filled; deferring backlog promotion.")
                        backlog.put(fresh)
                        break

                    # Calculate dynamic position size for backlog promotion
                    dynamic_size = calculate_dynamic_size(fresh)
                    
                    meta_fields_to_add_backlog = {
                        "bq", "rr", "pc5", "pc60", "m5", "mc", "low_t", "low_p", "route_ok",
                        "imp_buy", "imp_sell", "sell_pressure_ratio", "price_stability",
                        "holder_activity_decline", "revival_gate_ok"
                    }
                    enriched = {
                        **fresh["raw_scn"],
                        "size_usd": dynamic_size,  # Apply dynamic sizing
                        "meta": {
                            **(fresh["raw_scn"].get("meta") or {}),
                            "reason": f"pm_backlog_promote",
                            "pm_src": (fresh["raw_scn"].get("meta") or {}).get("reason", "unknown"),
                            "dynamic_size": dynamic_size,
                            **{k: fresh[k] for k in meta_fields_to_add_backlog if k in fresh}
                        }
                    }

                    _append_jsonl(SCENARIOS_EXEC, enriched)
                    update_repeat_registry(read_repeat_registry(), fresh)
                    print(f"[+] PROMOTED from backlog: {fresh['symbol']} rr={fresh['rr']:.2f} size=${dynamic_size:.0f}")
                    # ✅ RESTORED: Update open_mints after backlog promotion
                    open_mints = {p["mint"] for p in get_incumbents_ctx()}
                    time.sleep(1)  # gentle pacing for executor

            # NEW: 6) Smart backlog re-evaluation when slots are free (ADDED FUNCTIONALITY)
            if (now - last_backlog_recheck_ts) >= BACKLOG_RECHECK_COOLDOWN_SEC:
                reevaluate_backlog_candidates(backlog)
                last_backlog_recheck_ts = now

            # 7) Heavy-sell pause (unchanged)
            incs = get_incumbents_ctx()
            for pos in incs:
                mint, pnl = pos["mint"], pos["pnl"]
                if mint in _PAUSE_UNTIL_TS_BY_MINT and now < _PAUSE_UNTIL_TS_BY_MINT[mint]:
                    continue
                if pnl < HEAVY_SELL_DETECT_PCT:
                    print(f"[!] Detected heavy sell on {mint[:6]} (pnl={pnl:.2f}%). Sending PAUSE_BUYS.")
                    pause_until = now + REVIVAL_PAUSE_HOURS * 3600
                    _PAUSE_UNTIL_TS_BY_MINT[mint] = pause_until
                    _append_jsonl(EXEC_CONTROL, {
                        "type": "PAUSE_BUYS", "ts": now, "mint": mint,
                        "hours": REVIVAL_PAUSE_HOURS, "reason": f"heavy_sell_pnl_{pnl:.1f}%"
                    })

            print(f"--- Cycle End --- Sleeping for {LOOP_SLEEP_SEC} seconds...")
            time.sleep(LOOP_SLEEP_SEC)
            
        except KeyboardInterrupt:  # ← ADDED: Graceful exit for Ctrl+C
            print("\n[!] Keyboard interrupt - exiting gracefully.")
            raise
        except Exception as e:  # ← ADDED: Catch-all for any other exceptions
            print(f"[!] TM cycle error: {e}")
            import traceback
            traceback.print_exc()
            print(f"[!] Sleeping for {LOOP_SLEEP_SEC} seconds before retry...")
            time.sleep(LOOP_SLEEP_SEC)  # Prevent tight error loop
        
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Exiting Enhanced Pre-Trade Manager.")