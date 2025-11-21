# paper_trade.py
# Minimal-friction paper trader with optional GPT rationale, spam control, and safe Telegram.
# Adds: ANALYZE_LAST_N mode to evaluate last N processed reports and emit a CSV summary.

from __future__ import annotations
import os, json, time, math, bisect, hashlib, statistics, random
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Set

import requests
from dotenv import load_dotenv
load_dotenv(override=True)

from threading import Thread, Event
from queue import Queue, Empty
import ssl
# websocket-client
from websocket import WebSocketApp

# =========================
# ENV / KNOBS (safe defaults)
# =========================
PROCESSED_DIR = os.getenv('PROCESSED_DIR', 'reports/holdings/processed')
INCLUDE_PROCESSED_DELTAS = int(os.getenv('INCLUDE_PROCESSED_DELTAS', '0'))

BIRDEYE_API_KEY = os.getenv('BIRDEYE_API_KEY', '')
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY', '')
API_BIRDEYE_OVERRIDE = str(os.getenv('API_BIRDEYE_OVERRIDE', 'false')).lower() in ('1','true','yes')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

WALLET_HISTORY_LIMIT = int(os.getenv('WALLET_HISTORY_LIMIT', '40'))
BIRDEYE_BARS_LIMIT = int(os.getenv('BIRDEYE_BARS_LIMIT', '240'))

EXHAUSTION_ALERT_COOLDOWN_MIN = int(os.getenv('EXHAUSTION_ALERT_COOLDOWN_MIN', '20'))
TG_DEDUP_WINDOW_SEC = int(os.getenv('TG_DEDUP_WINDOW_SEC', '60'))
WHYNOT_COOLDOWN_MIN = int(os.getenv('WHYNOT_COOLDOWN_MIN', '15'))  # for white/grey "why not" notes

# Base 5m activity bar (kept from .env if present)
REBID_MARKET_TXN_5M_MIN = int(os.getenv('REBID_MARKET_TXN_5M_MIN', '8'))

USDC_MINT_SOL = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
WSOL_MINT_SOL = 'So11111111111111111111111111111111111111112'
USDT_MINT_SOL = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
PREFERRED_QUOTES = [USDC_MINT_SOL, WSOL_MINT_SOL, USDT_MINT_SOL]

# --- Route health (Jupiter) ---
JUPITER_QUOTE_URL = os.getenv('JUPITER_QUOTE', 'https://quote-api.jup.ag/v6/quote')
ROUTE_BASE_MINT = os.getenv('BASE_INPUT_MINT', USDC_MINT_SOL)
ROUTE_TEST_USD = float(os.getenv('ROUTE_TEST_USD', '25'))
ROUTE_MAX_IMPACT_PCT = float(os.getenv('ROUTE_MAX_IMPACT_PCT', '5.0'))
ROUTE_SLIPPAGE_BPS = int(os.getenv('ROUTE_SLIPPAGE_BPS', '150'))
USDC_DECIMALS = 6

# --- Market-cap + age environment knobs ---
MC_BID_ENV_LOW_USD = float(os.getenv('MC_BID_ENV_LOW_USD', '10000'))   # ~10k ⇒ strong
MC_BID_ENV_HIGH_USD = float(os.getenv('MC_BID_ENV_HIGH_USD', '30000'))  # ~30k ⇒ weak
AGE_YOUNG_MAX_MIN = int(os.getenv('AGE_YOUNG_MAX_MIN', '240'))        # ≤4h considered "youngish" and boosted

# --- Looser bidding defaults (work even if your .env lacks these keys) ---
REBID_M5_SOFTEN_FACTOR = float(os.getenv('REBID_M5_SOFTEN_FACTOR', '0.50'))  # 50% of baseline m5 when boosted
BOOST_MIN_BQ = float(os.getenv('BOOST_MIN_BQ', '0.40'))            # bottom_quality_score threshold
REBID_M5_FLOOR = int(os.getenv('REBID_M5_FLOOR', '3'))               # hard floor if bq very high (>= 0.70)

# --- Optional GPT gate / rationale ---
ENABLE_GPT_GATE = int(os.getenv('ENABLE_GPT_GATE', '0'))
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
GPT_MODEL = os.getenv('GPT_MODEL', 'gpt-4o-mini')
GPT_MIN_CONF = float(os.getenv('GPT_MIN_CONF', '0.55'))
WHYNOT_ON_NON_REBID = int(os.getenv('WHYNOT_ON_NON_REBID', '0'))   # white/grey narrations
EXPLAIN_EXHAUSTION = int(os.getenv('EXPLAIN_EXHAUSTION', '1'))    # gpt rationale on exhaustion alerts

# Analysis mode: set ANALYZE_LAST_N>0 to run last N reports instead of single latest
ANALYZE_LAST_N = int(os.getenv('ANALYZE_LAST_N', '3'))

# Debug noise controls
DEBUG_NORMAL_TICKS = int(os.getenv('DEBUG_NORMAL_TICKS', '0'))     # set 0 to avoid spam

# New: Simulator loop
RUN_FOREVER = int(os.getenv('RUN_FOREVER', '1'))
LOOP_SLEEP_SEC = int(os.getenv('LOOP_SLEEP_SEC', '10'))

# New: Simulation mode
SIMULATION_MODE = int(os.getenv('SIMULATION_MODE', '0'))

# New: Adaptive
RECENT_TOKENS_WINDOW = int(os.getenv('RECENT_TOKENS_WINDOW', '50'))
LAST_TRADES_N = int(os.getenv('LAST_TRADES_N', '20'))
DEPRESSED_1H_THRESHOLD = float(os.getenv('DEPRESSED_1H_THRESHOLD', '-5.0'))
HOT_1H_THRESHOLD = float(os.getenv('HOT_1H_THRESHOLD', '10.0'))
RECOVERY_ADJUST_PCT = float(os.getenv('RECOVERY_ADJUST_PCT', '25.0'))
WINRATE_LOW = float(os.getenv('WINRATE_LOW', '40.0'))
WINRATE_HIGH = float(os.getenv('WINRATE_HIGH', '60.0'))
BQ_ADJUST_STEP = float(os.getenv('BQ_ADJUST_STEP', '0.05'))

# --- Birdeye WebSocket (live price) ---
BIRDEYE_WS_ON        = int(os.getenv("BIRDEYE_WS_ON", "1"))
BIRDEYE_WS_URL       = os.getenv("BIRDEYE_WS_URL", "wss://public-api.birdeye.so/ws")
# optional key; WS works without it, but add if you have one
BIRDEYE_WS_API_KEY   = os.getenv("BIRDEYE_WS_API_KEY", "")  # alias to not clash with REST key
BIRDEYE_WS_PING_SEC  = int(os.getenv("BIRDEYE_WS_PING_SEC", "20"))
BIRDEYE_WS_MINT_CAP  = int(os.getenv("BIRDEYE_WS_MINT_CAP", "200"))  # safety cap
# only trigger fast re-check when price changes ≥ this fraction (0.005 = 0.5%)
BIRDEYE_WS_MIN_REL_MOVE = float(os.getenv("BIRDEYE_WS_MIN_REL_MOVE", "0.005"))


# --- Wait / exit behavior (hard-coded, no env) ---
MIN_HOLD_MIN = 12   # grace period before normal exits arm
DISASTER_SL_PCT = 35.0 # always-on kill switch (rug/wick protection)

BE_ACTIVATE_AT = 12.0 # run-up to arm break-even stop
BE_BUFFER_PCT = 1.0  # stop sits ~1% under entry

TRAIL_ACTIVATE_AT = 25.0 # run-up to arm trailing stop
TRAIL_GIVEBACK_PCT = 12.0 # trailing giveback from high

MAX_HOLD_MIN = 180  # absolute time stop
REENTRY_COOLDOWN_MIN = 20   # avoid immediate churn on same mint

# --- Market-cap realism guards ---
RECOVERY_MULT_CEIL = 4.0  # cap rebound multiple used in bottom-MC math
MC_NOW_PENALTY_START = 30000.0  # start taper confidence above this live MC
MC_NOW_PENALTY_MAXAT = 120000.0 # full taper by this MC
MC_NOW_PENALTY_MAX = 0.35     # up to -35% on mc/age score

REQUIRE_ROUTE_WHEN_MC_NOW = 60000.0  # if live MC >= this, boosted path requires route OK

# Globals for adaptive / performance / equity
recent_m5: List[int] = []
recent_1h: List[float] = []
global_pnl_history: List[float] = []  # list of pnl_pct for all trades
last_processed_path: Optional[str] = None
initial_equity = 100.0
_LAST_EXIT_TS_BY_MINT: Dict[str, int] = {}

# --- Incremental bar cache: process only new 1m candles ---
BAR_CACHE_PATH = os.path.join(PROCESSED_DIR, "bar_seen_cache.json")
try:
    _BAR_SEEN = json.load(open(BAR_CACHE_PATH, "r", encoding="utf-8"))
except Exception:
    _BAR_SEEN = {}  # { mint: last_bar_ts }

def _bar_seen_get(mint: str) -> int:
    try: return int(_BAR_SEEN.get(mint, 0))
    except: return 0

def _bar_seen_set(mint: str, ts: int):
    _BAR_SEEN[mint] = int(ts)
    try:
        json.dump(_BAR_SEEN, open(BAR_CACHE_PATH, "w", encoding="utf-8"))
    except Exception as e:
        print("[*] bar cache write error:", e)

# --- Persist open positions across loops ---
OPEN_POS_DIR = os.path.join(PROCESSED_DIR, "open_positions")
os.makedirs(OPEN_POS_DIR, exist_ok=True)

def _open_pos_path(mint: str) -> str:
    return os.path.join(OPEN_POS_DIR, f"{mint}.json")

def load_open_pos(mint: str):
    p = _open_pos_path(mint)
    if not os.path.isfile(p): return None
    try: return json.load(open(p, "r", encoding="utf-8"))
    except: return None

def save_open_pos(mint: str, state: dict):
    try: json.dump(state, open(_open_pos_path(mint), "w", encoding="utf-8"))
    except Exception as e: print("[*] save_open_pos error:", e)

def clear_open_pos(mint: str):
    try:
        p = _open_pos_path(mint)
        if os.path.isfile(p): os.remove(p)
    except Exception as e:
        print("[*] clear_open_pos error:", e)

# =========================
# Safe utilities
# =========================
def _now() -> int:
    return int(time.time())

def _sha(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))

# --- live price cache (updated by WS) ---
LIVE_PRICE: Dict[str, Dict] = {}  # mint -> {"price": float, "ts": int}

def record_live_price(mint: str, px: float, ts: Optional[int] = None):
    if px is None: 
        return
    LIVE_PRICE[mint] = {"price": float(px), "ts": ts or _now()}

def current_live_price(mint: str) -> Optional[float]:
    d = LIVE_PRICE.get(mint)
    return float(d["price"]) if d else None

# =========================
# Telegram (plain text; safe; dedup; truncation; no markdown)
# =========================
_TG_DEDUP: Dict[str, int] = {}
def _should_send(msg: str, window_sec: int) -> bool:
    h = _sha(msg.strip()[:256])
    now = _now()
    last = _TG_DEDUP.get(h, 0)
    if now - last < window_sec:
        return False
    _TG_DEDUP[h] = now
    return True

def tg_send(text: str, cooldown_sec: int = TG_DEDUP_WINDOW_SEC):
    # Plain text (no parse_mode) → avoids Telegram 400 "can't parse entities"
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('[tg] (dry)', text.replace('\\n', ' ')[:220])
        return
    if not _should_send(text, cooldown_sec):
        return
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text[:3900],               # safe truncation
        'disable_web_page_preview': True
    }
    try:
        r = requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json=payload, timeout=8
        )
        if r.status_code != 200:
            print('[tg] error:', r.status_code, r.text[:200])
    except Exception as e:
        print('[tg] exception:', repr(e))

class BirdeyeWS:
    """
    Minimal WS client for Birdeye:
      - connects to wss://public-api.birdeye.so/socket/solana?x-api-key=...
      - SUBSCRIBE_PRICE / UNSUBSCRIBE_PRICE per mint
      - emits (mint, price, ts) via out_q
    """
    def __init__(self, url: str, x_api_key: str = "", ping_sec: int = 20):
        # fix common misconfigs (https:// or /ws)
        if url.startswith("https://"):
            url = "wss://" + url[len("https://"):]
        if "/ws" in url:
            # switch to the correct socket path; keep api key if present
            base = "wss://public-api.birdeye.so/socket/solana"
            # prefer explicit key below if provided
            url = base
        # append api key if not present in url and we have one
        if x_api_key and "x-api-key=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}x-api-key={x_api_key}"

        self.url = url
        self.x_api_key = x_api_key
        self.ping_sec = ping_sec
        self.ws: Optional[WebSocketApp] = None
        self.thread: Optional[Thread] = None
        self.stop_ev = Event()
        self.subscribed: Set[str] = set()
        self.out_q: Queue = Queue()

    def _headers(self):
        # Birdeye requires these WS headers
        return [
            "Origin: ws://public-api.birdeye.so",
            "Sec-WebSocket-Origin: ws://public-api.birdeye.so",
            "Sec-WebSocket-Protocol: echo-protocol",
        ]

    def _on_open(self, ws):
        # resubscribe after reconnect
        for m in list(self.subscribed):
            try:
                ws.send(json.dumps({"type": "SUBSCRIBE_PRICE", "data": {"address": m}}))
            except Exception:
                pass

    def _on_message(self, ws, msg):
        try:
            data = json.loads(msg)
        except Exception:
            return
        # Expect messages like: {"type":"PRICE_DATA","data":{"address":"...","c":1.23,"unixTime":173...}}
        if isinstance(data, dict) and data.get("type") == "PRICE_DATA":
            d = data.get("data") or {}
            mint = d.get("address")
            price = d.get("c") or d.get("close")
            ts = int(d.get("unixTime") or d.get("t") or _now())
            if mint and price:
                self.out_q.put((mint, float(price), ts))

    def _on_error(self, ws, err):
        print("[birdeye-ws] error:", err)

    def _on_close(self, ws, code, reason):
        print("[birdeye-ws] closed:", code, reason)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_ev.clear()

        def run():
            while not self.stop_ev.is_set():
                try:
                    self.ws = WebSocketApp(
                        self.url,
                        header=self._headers(),
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.ping_sec)
                except Exception as e:
                    print("[birdeye-ws] run err:", e)
                time.sleep(2)

        self.thread = Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_ev.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def subscribe(self, mint: str):
        if not mint or len(self.subscribed) >= BIRDEYE_WS_MINT_CAP:
            return
        if mint in self.subscribed:
            return
        self.subscribed.add(mint)
        try:
            if self.ws:
                self.ws.send(json.dumps({"type": "SUBSCRIBE_PRICE", "data": {"address": mint}}))
        except Exception:
            pass

    def unsubscribe(self, mint: str):
        if mint not in self.subscribed:
            return
        self.subscribed.discard(mint)
        try:
            if self.ws:
                self.ws.send(json.dumps({"type": "UNSUBSCRIBE_PRICE", "data": {"address": mint}}))
        except Exception:
            pass

# =========================
# Minimal types
# =========================
@dataclass
class Bar:
    t: int
    p: float

@dataclass
class Swap:
    t: int
    side: str
    wallet: Optional[str]

@dataclass
class Trade:
    symbol: str
    mint: str
    entry_t: int
    entry_p: float
    exit_t: Optional[int] = None
    exit_p: Optional[float] = None
    reason: Optional[str] = None
    pnl_pct: Optional[float] = None

@dataclass
class Config:
    # Core bottom detection
    drawdown_threshold_pct: float = float(os.getenv('DRAWDOWN_THRESHOLD_PCT', '40'))
    lookback_high_min: int = int(os.getenv('LOOKBACK_HIGH_MIN', '240'))
    silence_minutes: int = int(os.getenv('SILENCE_MINUTES', '8'))
    # Loosen near-silence by default (more EXHAUSTION signals)
    max_silence_tx: int = int(os.getenv('MAX_SILENCE_TX', '5'))     # default 5
    # Rebid staging
    rebid_window_min: int = int(os.getenv('REBID_WINDOW_MIN', '5'))
    rebid_market_txn_5m_min: int = REBID_MARKET_TXN_5M_MIN
    min_recovery_from_low_pct: float = float(os.getenv('MIN_RECOVERY_FROM_LOW_PCT', '3.0'))
    # Exits
    take_profit_pct: float = float(os.getenv('TAKE_PROFIT_PCT', '40.0'))
    stop_loss_pct: float = float(os.getenv('STOP_LOSS_PCT', '20.0'))
    time_stop_min: int = int(os.getenv('TIME_STOP_MIN', '90'))
    # Boost loosening (works even if .env has nothing)
    rebid_m5_soften_factor: float = REBID_M5_SOFTEN_FACTOR
    boost_min_bq: float = BOOST_MIN_BQ
    rebid_m5_floor: int = REBID_M5_FLOOR

# =========================
# Optional helius imports (soft)
# =========================
try:
    from helius_analyzer import _fetch_parsed_history_page_v0, format_timestamp
    HELIUS_OK = True
except Exception:
    HELIUS_OK = False
    def _fetch_parsed_history_page_v0(*args, **kwargs): return []
    def format_timestamp(ts: int): return ts

# =========================
# Processed reports (inputs)
# =========================
def newest_processed_report_path() -> Optional[str]:
    if not os.path.isdir(PROCESSED_DIR):
        return None
    files = []
    for f in os.listdir(PROCESSED_DIR):
        if not f.endswith('.json'): continue
        if f.startswith('holding_report_'): files.append(os.path.join(PROCESSED_DIR, f))
        elif INCLUDE_PROCESSED_DELTAS and f.startswith('holding_delta_'): files.append(os.path.join(PROCESSED_DIR, f))
    if not files: return None
    files.sort()
    return files[-1]

def list_latest_reports(n: int) -> list[str]:
    """Return last n holding_report_*.json paths (lexical sort is chronological)."""
    if not os.path.isdir(PROCESSED_DIR):
        return []
    files = []
    for f in os.listdir(PROCESSED_DIR):
        if f.endswith('.json') and f.startswith('holding_report_'):
            files.append(os.path.join(PROCESSED_DIR, f))
    files.sort()
    return files[-n:] if n > 0 else []

def iter_valid_tokens_from_report(report: dict):
    items = report.get('processed_items') or report.get('items') or report.get('correlated_holdings') or []
    for it in items:
        if not isinstance(it, dict): continue
        flags = it.get('validation_flags') or {}
        status = str(it.get('processing_status', '')) or ''
        if not flags.get('passes_validation', False): continue
        if status.startswith('skipped_'): continue
        if status not in ('new_candidate', 'update_candidate'): continue
        md = it.get('bds_metadata') or {}
        ca = md.get('address') or it.get('token_mint') or it.get('address')
        if not ca: continue
        sym = it.get('symbol') or md.get('symbol') or ca[:6]
        name = it.get('name') or md.get('name') or sym
        yield (sym, name, ca, it)

def wallets_holding_mint_from_processed(report: dict, ca: str) -> Set[str]:
    holders: Set[str] = set()
    all_hold = report.get('all_wallet_filtered_holdings') or {}
    if isinstance(all_hold, dict) and all_hold:
        for w, mints_map in all_hold.items():
            if ca in (mints_map or {}):
                holders.add(w)
        return holders
    items = report.get('processed_items') or report.get('items') or report.get('correlated_holdings') or []
    for it in items:
        if it.get('token_mint') == ca or (it.get('bds_metadata') or {}).get('address') == ca:
            ocd = it.get('original_correlation_data') or {}
            for w in (ocd.get('holding_wallets_in_set') or []):
                holders.add(w)
    return holders

# =========================
# Swaps (from holders)
# =========================
def extract_swaps_from_tx(tx_details: Dict) -> List[Dict]:
    swaps = []
    tx_type = tx_details.get('type')
    source = tx_details.get('source')
    timestamp = tx_details.get('timestamp')
    signature = tx_details.get('signature')
    if tx_type == 'SWAP' or source in ['JUPITER_V6', 'RAYDIUM', 'ORCA', 'METEORA', 'PUMP']:
        events = tx_details.get('events', {})
        swap_event = events.get('swap')
        if swap_event:
            token_in_list = swap_event.get('tokenInputs', [])
            token_out_list = swap_event.get('tokenOutputs', [])
            if len(token_in_list) == 1 and len(token_out_list) == 1:
                token_in = token_in_list[0]; token_out = token_out_list[0]
                user_wallet = tx_details.get('feePayer') or swap_event.get('taker') or swap_event.get('maker')
                if token_in.get('mint') and token_out.get('mint') and user_wallet:
                    swaps.append({
                        'wallet': user_wallet,
                        'signature': signature,
                        'timestamp': timestamp,
                        'dex_source': source,
                        'token_in_mint': token_in.get('mint'),
                        'token_out_mint': token_out.get('mint'),
                    })
    return swaps

def mint_swaps_from_holders(holders: Set[str], mint: str, per_wallet_limit=WALLET_HISTORY_LIMIT,
                            window_start: Optional[int]=None, window_end: Optional[int]=None) -> List[Swap]:
    out: List[Swap] = []
    if not HELIUS_OK or not holders:
        return out
    for w in list(holders):
        try:
            txs = _fetch_parsed_history_page_v0(w, limit=per_wallet_limit) or []
            for tx in txs:
                ts = int(tx.get('timestamp') or 0)
                if window_start and ts < window_start: continue
                if window_end and ts > window_end: continue
                for s in extract_swaps_from_tx(tx):
                    if s.get('token_out_mint') == mint:
                        out.append(Swap(t=int(s['timestamp']), side='buy',  wallet=w))
                    elif s.get('token_in_mint') == mint:
                        out.append(Swap(t=int(s['timestamp']), side='sell', wallet=w))
        except Exception as e:
            print(f'[holder_swaps] wallet {w[:6]} err: {e}')
            continue
    out.sort(key=lambda x: x.t)
    return out

def holder_sold_share(holders: Set[str], swaps: List[Swap], low_t: int, window_end: int) -> float:
    if not holders: return 0.0
    sold_wallets: Set[str] = set()
    for s in swaps:
        if s.side == 'sell' and low_t < s.t <= window_end and s.wallet:
            sold_wallets.add(s.wallet)
    return round(len(sold_wallets)/max(len(holders),1)*100.0, 1)

# =========================
# Market data (Birdeye + Dexscreener)
# =========================
def _time_range_min(limit_minutes: int) -> tuple[int,int]:
    now = _now()
    return now - limit_minutes*60, now

def _birdeye_ohlcv_base_quote_1m(base_addr: str, quote_addr: str, limit_minutes: int) -> List[Bar]:
    if not BIRDEYE_API_KEY and not API_BIRDEYE_OVERRIDE:
        return []
    time_from, time_to = _time_range_min(limit_minutes)
    url = 'https://public-api.birdeye.so/defi/ohlcv/base_quote'
    params = {'base_address': base_addr, 'quote_address': quote_addr, 'type':'1m',
              'time_from': time_from, 'time_to': time_to}
    headers = {'accept':'application/json', 'x-chain':'solana'}
    if BIRDEYE_API_KEY:
        headers['X-API-KEY'] = BIRDEYE_API_KEY
    try:
        r = requests.get(url, params=params, headers=headers, timeout=2)
        if r.status_code != 200:
            return []
        items = ((r.json() or {}).get('data') or {}).get('items') or []
        bars: List[Bar] = []
        for it in items:
            ts = it.get('t') or it.get('time') or it.get('unixTime')
            cl = it.get('c') or it.get('close') or it.get('value')
            if ts is None or cl is None:
                continue
            ts = int(ts)
            if ts > 2_000_000_000: ts //= 1000
            price = float(cl)
            if price > 0:
                bars.append(Bar(ts, price))
        bars.sort(key=lambda b: b.t)
        return bars
    except Exception:
        return []

def fetch_birdeye_1m_bars(mint: str, limit: int) -> List[Bar]:
    for quote in PREFERRED_QUOTES:
        bars = _birdeye_ohlcv_base_quote_1m(mint, quote, limit)
        if len(bars) >= 30:
            return bars
    return []

def dexscreener_best_pair_and_m5(mint: str) -> Tuple[Optional[str], Optional[str], int, Optional[float], Optional[int]]:
    """
    Returns: (pair_addr, dex_id, m5_total, market_cap_usd, pair_created_at_ms)
    """
    try:
        r = requests.get(f'https://api.dexscreener.com/latest/dex/tokens/{mint}', timeout=2)
        if r.status_code != 200:
            return None, None, 0, None, None
        data = r.json() or {}
        pairs = data.get('pairs') or []
        if not pairs: return None, None, 0, None, None
        def liq_usd(p):
            liq = p.get('liquidity') or {}
            return float(liq.get('usd') or liq.get('usdValue') or 0.0)
        pairs.sort(key=liq_usd, reverse=True)
        best = pairs[0]
        txns = best.get('txns') or {}
        m5 = txns.get('m5') or {}
        m5_total = int(m5.get('buys') or 0) + int(m5.get('sells') or 0)
        mc_usd = best.get('marketCap')
        if mc_usd is not None:
            try: mc_usd = float(mc_usd)
            except: mc_usd = None
        created_ms = best.get('pairCreatedAt')
        if created_ms is not None:
            try: created_ms = int(created_ms)
            except: created_ms = None
        return best.get('pairAddress') or best.get('pairId'), (best.get('dexId') or '').upper(), m5_total, mc_usd, created_ms
    except Exception:
        return None, None, 0, None, None

def dexscreener_trades_last_10m(pair_addr: Optional[str]) -> Tuple[int, bool]:
    if not pair_addr: return 0, True
    cutoff = _now() - 600
    try:
        url = f'https://api.dexscreener.com/latest/dex/trades/pair/{pair_addr}?limit=200'
        r = requests.get(url, timeout=2)
        if r.status_code != 200:
            url = f'https://api.dexscreener.com/latest/dex/trades/{pair_addr}?limit=200'
            r = requests.get(url, timeout=2)
        if r.status_code == 200:
            j = r.json() or {}
            items = j.get('trades') or j.get('pairs') or []
            cnt = 0
            for it in items:
                ts = it.get('timestamp') or it.get('time') or it.get('t')
                if ts is None: continue
                ts = int(ts)
                if ts > 2_000_000_000_000: ts //= 1_000_000
                if ts > 2_000_000_000:     ts //= 1000
                if ts >= cutoff: cnt += 1
            return cnt, False
    except Exception:
        pass
    return 0, True

# =========================
# % change helpers (with interpolation)
# =========================
def _interp_price_at(bars: List[Bar], cutoff_ts: int) -> Optional[float]:
    if not bars: return None
    ts = [b.t for b in bars]; ps = [b.p for b in bars]
    if cutoff_ts <= ts[0]: return ps[0]
    if cutoff_ts >= ts[-1]: return ps[-1]
    idx = bisect.bisect_left(ts, cutoff_ts)
    if ts[idx] == cutoff_ts: return ps[idx]
    t0, p0 = ts[idx-1], ps[idx-1]; t1, p1 = ts[idx], ps[idx]
    if t1 == t0: return p0
    w = (cutoff_ts - t0) / (t1 - t0)
    return p0 + w*(p1 - p0)

def pct_change_linear(bars: List[Bar], minutes: int) -> Optional[float]:
    if not bars: return None
    now_t = bars[-1].t
    cutoff = now_t - minutes*60
    p_old = _interp_price_at(bars, cutoff)
    p_new = bars[-1].p
    if p_old is None or p_old <= 0: return None
    return (p_new - p_old) / p_old * 100.0

def fmt_pct(x: Optional[float]) -> str:
    return f"{x:+.2f}%" if x is not None else "n/a"

# =========================
# Confirmations
# =========================
def micro_structure_ok(bars: List[Bar], low_t: int, need_consecutive:int=1) -> Tuple[bool, str]:
    closes = [b.p for b in bars if b.t > low_t]
    if len(closes) < need_consecutive + 1:
        return False, "few_post_low_closes"
    consec = 0
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            consec += 1
            if consec >= need_consecutive:
                return True, f"consec_up={consec}"
        else:
            consec = 0
    return False, f"consec_up={consec}"

def jupiter_quote(input_mint: str, output_mint: str, amount_base_units: int, slippage_bps: int) -> Optional[dict]:
    try:
        r = requests.get(JUPITER_QUOTE_URL, params={
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount_base_units,
            "slippageBps": slippage_bps,
            "onlyDirectRoutes": "false"
        }, timeout=2)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def route_health_ok(base_mint: str, token_mint: str, test_usd: float = ROUTE_TEST_USD,
                    max_impact_pct: float = ROUTE_MAX_IMPACT_PCT, slippage_bps: int = ROUTE_SLIPPAGE_BPS) -> Tuple[bool, str]:
    try:
        base_dec = USDC_DECIMALS if base_mint == USDC_MINT_SOL else 6
        buy_amt = int(max(1, test_usd * (10 ** base_dec)))
        q_buy = jupiter_quote(base_mint, token_mint, buy_amt, slippage_bps)
        if not q_buy: return False, "no_buy_quote"
        out_amt = int(q_buy.get("outAmount") or 0)
        imp_buy = 100.0 * float(q_buy.get("priceImpactPct") or 0.0)
        if out_amt <= 0: return False, "buy_out_zero"
        q_sell = jupiter_quote(token_mint, base_mint, out_amt, slippage_bps)
        if not q_sell: return False, "no_sell_quote"
        imp_sell = 100.0 * float(q_sell.get("priceImpactPct") or 0.0)
        ok = (imp_buy < max_impact_pct) and (imp_sell < max_impact_pct)
        return ok, f"imp_buy={imp_buy:.2f}% imp_sell={imp_sell:.2f}%"
    except Exception as e:
        return False, f"route_err:{e}"

def holder_flow_bias_ok(holder_swaps: List[Swap], low_t: int, window_min:int=20) -> Tuple[bool, str]:
    hi = low_t + window_min*60
    buys  = sum(1 for s in holder_swaps if low_t < s.t <= hi and s.side=="buy")
    sells = sum(1 for s in holder_swaps if low_t < s.t <= hi and s.side=="sell")
    return (buys >= sells, f"holders_buys={buys} sells={sells}")

# =========================
# GPT helpers (backoff, guard, rationale)
# =========================
def _gpt_chat_json(system: str, user_json: dict, model: str, key: str, max_attempts: int = 3) -> Tuple[Optional[dict], str]:
    if not key:
        return None, "no_key"
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type":"application/json"}
    body = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type":"json_object"},
        "messages": [{"role":"system","content":system},{"role":"user","content":json.dumps(user_json, separators=(',',':'))}]
    }
    for attempt in range(1, max_attempts+1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=2)
            if r.status_code == 200:
                js = r.json()
                content = js["choices"][0]["message"]["content"]
                try:
                    data = json.loads(content)
                except Exception:
                    return None, "bad_json"
                return data, "ok"
            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(8, (2 ** attempt)) + random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue
            return None, f"http_{r.status_code}"
        except Exception:
            time.sleep(0.5 * attempt)
    return None, "timeout_or_retries"

def gpt_should_bid(payload: dict) -> Tuple[bool, float, str, str]:
    if not ENABLE_GPT_GATE:
        return True, 1.0, "gpt_gate_disabled", "disabled"
    sys = (
        "You are a crypto microcap reviewer. Decide GO/NOGO for a speculative REBID.\n"
        "Consider: token age (min), % moves (5m/1h/6h/24h), drawdown & recovery, minutes since silence,\n"
        "market txns (m5/10m), DEX id, route price impact (buy/sell), holder flow bias, and bottom_quality_score (0..1).\n"
        "Bias against: very young (<30m) with low m5, negative 5m momentum, high price impact (>2.5%), or no recovery.\n"
        "Prefer GO if bottom_quality_score is high, recovery is starting, and confirms align.\n"
        "Output strict JSON: {\"go\":true|false,\"confidence\":0..1,\"reason\":\"...\"}."
    )
    data, tag = _gpt_chat_json(sys, payload, GPT_MODEL, OPENAI_API_KEY)
    if not data:
        return False, 0.0, f"gpt_{tag}", tag
    go = bool(data.get("go"))
    conf = float(data.get("confidence") or 0.0)
    reason = str(data.get("reason") or "")
    if not go or conf < GPT_MIN_CONF:
        return False, conf, reason or "low_confidence", "ok"
    return True, conf, reason, "ok"

def gpt_explain_exhaustion(payload: dict) -> Tuple[str, str]:
    if not EXPLAIN_EXHAUSTION or not OPENAI_API_KEY:
        return "", "disabled"
    sys = (
        "Explain briefly (one sentence) why this looks like a potential exhaustion area in a memecoin context.\n"
        "Mention drawdown, silence, early recovery/activity hints, and (if given) the low market cap angle. No emojis."
        "Output as JSON: {\"why\": \"your explanation here\"}."
    )
    data, tag = _gpt_chat_json(sys, payload, GPT_MODEL, OPENAI_API_KEY)
    if not data:
        return f"gpt_{tag}", tag
    why = data.get("why") or data.get("explanation") or data.get("text") or ""
    why = str(why).strip()
    if not why:
        return "no_explanation", "ok"
    return why[:240], "ok"

def reevaluate_mint_once(cfg: Config, mint: str, symbol_hint: Optional[str] = None):
    """
    Lightweight re-check for a single mint on a live price tick.
    Uses your normal pipeline (bars + pair info) but skips heavy holder analysis for speed.
    """
    bars = fetch_birdeye_1m_bars(mint, limit=BIRDEYE_BARS_LIMIT)
    if len(bars) < 30:
        return
    bars = maybe_refetch_for_pct(bars, mint)

    pair_addr, dex_id, m5_total, mc_usd, created_ms = dexscreener_best_pair_and_m5(mint)
    exact_10m, approx = dexscreener_trades_last_10m(pair_addr)
    if approx and m5_total:
        exact_10m = m5_total * 2

    avg_m5 = statistics.mean(recent_m5) if recent_m5 else cfg.rebid_market_txn_5m_min
    adaptive_m5_min, soft_m5, adaptive_min_recovery = compute_adaptive_values(cfg, avg_m5)

    # Evaluate quickly without holder_swaps (the rest of your gates still apply: micro, route, m5, quality…)
    trader = PaperTrader(cfg, symbol_hint or mint[:6], mint, adaptive_m5_min, soft_m5, adaptive_min_recovery)
    trader.run(bars, [], (pair_addr, dex_id, m5_total, mc_usd, created_ms), exact_10m, approx)

# =========================
# Detector
# =========================
class BottomDetector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def exhaustion_score(self, info: Dict) -> float:
        """
        0..1 where higher favors rebid. Increases with deeper drawdown and *lower* post-low tx count.
        """
        dd = max(0.0, float(info.get("drawdown_pct", 0.0)))
        post_tx = max(0, int(info.get("post_tx", 0)))
        dd_f = min(dd, 100.0) / 100.0
        mx = max(1, self.cfg.max_silence_tx)
        sil_f = max(0.0, (mx - min(post_tx, mx)) / mx)
        exhaustion = 0.75 * dd_f + 0.25 * sil_f
        return round(_clamp(exhaustion, 0.0, 1.0), 3)

    def mc_age_score(self, current_price: float, low_price: float,
                     market_cap_usd: Optional[float],
                     age_min: Optional[int]) -> Tuple[float, Optional[float]]:
        """
        Score 0..1 based on estimated *bottom* market cap and age.
        - Estimate mcap at low: mc_low_est = mc_now * (low / current).
        - If mc_low_est ≤ MC_BID_ENV_LOW_USD ⇒ 1.0
          If ≥ MC_BID_ENV_HIGH_USD ⇒ 0.0
          Linear in-between.
        - Age multiplier: linearly decay from 1.0 at age=0 to 0.5 at age=AGE_YOUNG_MAX_MIN+, i.e. newer favoured.
        Returns (score, mc_low_est)
        """
        if not market_cap_usd or current_price <= 0 or low_price <= 0:
            return 0.0, None
        # Cap overly-large rebound multiple so mc_low_est doesn't look "too perfect"
        if current_price > 0 and low_price > 0:
            mult = current_price / low_price
            if mult > RECOVERY_MULT_CEIL:
                # Pretend rebound was capped at RECOVERY_MULT_CEIL
                mc_low_est = (market_cap_usd or 0.0) * (1.0 / RECOVERY_MULT_CEIL)
            else:
                mc_low_est = float(market_cap_usd) * (low_price / current_price)
        else:
            mc_low_est = float(market_cap_usd) * (low_price / current_price)

        # Age-sensitive ranges
        lo, hi = MC_BID_ENV_LOW_USD, MC_BID_ENV_HIGH_USD
        if age_min is not None:
            if age_min < 60:
                lo, hi = 10000, 20000
            elif 60 <= age_min <= 360:
                lo, hi = 20000, 50000
            else:
                lo, hi = 30000, 100000

        if hi <= lo: hi = lo + 1.0
        mid = (lo + hi) / 2.0
        span = (hi - lo) / 2.0
        def band_score(mc):
            if lo <= mc <= hi:
                x = (mc - mid) / max(span, 1.0)
                return 1.0 - 0.1 * (x ** 2)  # mid=1.0, edges≈0.9
            if mc < lo:
                return max(0.6, 0.6 + 0.3 * (mc / lo))  # Adjusted to 0.3 for continuity to 0.9
            # mc > hi
            decay_dist = max(1.0, 100000.0 - hi)
            return max(0.0, 0.9 - 0.9 * (mc - hi) / decay_dist)  # Adjusted for continuity from 0.9
        base = band_score(mc_low_est)
        # age multiplier (0.8..1.0)
        if age_min is None:
            age_mult = 1.0
        else:
            age_mult = 1.0 - _clamp(age_min / max(1, AGE_YOUNG_MAX_MIN), 0.0, 1.0) * 0.2
        # Soft penalty for large current market cap (damps overconfidence on big live caps)
        if market_cap_usd and market_cap_usd > MC_NOW_PENALTY_START:
            pen_lin = (market_cap_usd - MC_NOW_PENALTY_START) / max(1.0, MC_NOW_PENALTY_MAXAT - MC_NOW_PENALTY_START)
            pen = _clamp(pen_lin, 0.0, 1.0) * MC_NOW_PENALTY_MAX
            base *= (1.0 - pen)
        score = base * age_mult
        return round(_clamp(score, 0.0, 1.0), 3), mc_low_est

    def state_for_index(self, bars: List[Bar], holder_swaps: List[Swap], idx: int,
                        market_txn_5m: int,
                        market_cap_usd: Optional[float],
                        pair_created_at_ms: Optional[int],
                        pct5: Optional[float], pct60: Optional[float]) -> Tuple[str, Dict]:
        if idx < 1:
            return "NORMAL", {"reason":"few_bars"}
        start = max(0, idx - self.cfg.lookback_high_min)
        window = bars[start:idx+1]
        prices = [b.p for b in window if b.p > 0]
        if not prices:
            return "NORMAL", {"reason":"no_prices"}
        recent_high = max(prices); current_p = bars[idx].p
        if recent_high <= 0 or current_p <= 0:
            return "NORMAL", {"reason":"nonpos_price"}
        dd = (recent_high - current_p) / recent_high * 100.0
        if dd < self.cfg.drawdown_threshold_pct:
            return "NORMAL", {"drawdown_pct": round(dd,2)}

        low_price = min(prices)
        low_idx_rel = prices.index(low_price)
        low_idx = start + low_idx_rel
        low_t = bars[low_idx].t

        # age (minutes) from pair creation if available
        age_min = None
        if pair_created_at_ms:
            created_s = int(pair_created_at_ms // 1000)
            age_min = max(0, (bars[idx].t - created_s) // 60)

        # Silence window after low
        sil_end_t = low_t + 60*self.cfg.silence_minutes
        post = [s for s in holder_swaps if low_t < s.t <= sil_end_t]

        # scores
        info_base = {"drawdown_pct": round(dd,2), "post_tx": len(post), "low_t": low_t}
        ex_score = self.exhaustion_score(info_base)
        mc_age_s, mc_low_est = self.mc_age_score(current_p, low_price, market_cap_usd, age_min)

        # Dynamic bottom quality based on volatility
        vol = abs(pct5 or 0) / abs(pct60 or 1e-6) if pct60 is not None else 10.0  # High vol default
        exh_weight = 0.35
        mc_weight = 0.65
        if vol > 2:
            exh_weight, mc_weight = 0.5, 0.5
        elif vol < 1:
            exh_weight, mc_weight = 0.25, 0.75
        bottom_quality = round(_clamp(exh_weight*ex_score + mc_weight*mc_age_s, 0.0, 1.0), 3)

        # rb_end = bars[idx].t
        recovery = (current_p - low_price) / max(low_price,1e-12) * 100.0
        minutes_since_sil = max(0, (bars[idx].t - sil_end_t)//60)
        info_rebid = {
            "drawdown_pct": round(dd,2),
            "post_tx": len(post),
            "recovery_pct": round(recovery,2),
            "minutes_since_silence": minutes_since_sil,
            "market_txn_5m": market_txn_5m,
            "low_t": low_t,
            "exhaustion_score": ex_score,
            "mc_low_est_usd": mc_low_est,
            "age_min": age_min,
            "bottom_quality_score": bottom_quality
        }

        if len(post) <= self.cfg.max_silence_tx:
            if bars[idx].t >= sil_end_t:
                return "REBID", info_rebid
            else:
                return "EXHAUSTION", info_rebid
        else:
            return "REBID", info_rebid

# =========================
# Paper Trader
# =========================
class PaperTrader:
    def __init__(self, cfg: Config, symbol: str, mint: str, adaptive_m5_min: int, soft_m5: int, adaptive_min_recovery: float):
        self.cfg = cfg
        self.symbol = symbol
        self.mint = mint
        self.detector = BottomDetector(cfg)
        self.pos: Optional[Trade] = None
        self.trades: List[Trade] = []
        self._last_state: Optional[str] = None
        self._last_exhaustion_alert_ts: int = 0
        self._last_whynot_ts: int = 0
        # cache metrics per run
        self._pct5=self._pct60=self._pct360=self._pct1440=None
        self._mkt_txn_10m=None
        self._mkt_txn_10m_is_approx=True
        # Adaptive
        self.adaptive_m5_min = adaptive_m5_min
        self.soft_m5 = soft_m5
        self.adaptive_min_recovery = adaptive_min_recovery

    def run(self, bars: List[Bar], holder_swaps: List[Swap],
            pair_info: Tuple[Optional[str], Optional[str], int, Optional[float], Optional[int]],
            market_txn_10m: int, market_txn_10m_is_approx: bool):
        if not bars:
            return
        self._pct5    = pct_change_linear(bars, 5)
        self._pct60   = pct_change_linear(bars, 60)
        self._pct360  = pct_change_linear(bars, 360)
        self._pct1440 = pct_change_linear(bars, 1440)

        self._mkt_txn_10m = market_txn_10m
        self._mkt_txn_10m_is_approx = market_txn_10m_is_approx

        # --- Incremental processing: skip already-seen bars ---
        last_ts = _bar_seen_get(self.mint)
        new_idx_start = 0
        if last_ts > 0:
            for j, b in enumerate(bars):
                if b.t > last_ts:
                    new_idx_start = j
                    break
            else:
                # No new 1m candle yet; persist open state and return
                self._finalize(bars)
                return

        pair_addr, dex_id, m5_total, mc_usd, created_ms = pair_info
        for i in range(new_idx_start, len(bars)):
            self._on_bar(bars, holder_swaps, i, m5_total or 0, pair_addr, dex_id, mc_usd, created_ms)
        # remember latest processed candle time for this mint
        _bar_seen_set(self.mint, bars[-1].t)
        self._finalize(bars)

    def _on_bar(self, bars: List[Bar], holder_swaps: List[Swap], idx: int,
                market_txn_5m: int, pair_addr: Optional[str], dex_id: Optional[str],
                market_cap_usd: Optional[float], pair_created_at_ms: Optional[int]):
        now = _now()
        bar = bars[idx]
        approx_tag = " (~)" if self._mkt_txn_10m_is_approx else ""
        moves_line = (f"Δ5m {fmt_pct(self._pct5)} | Δ1h {fmt_pct(self._pct60)} | "
                      f"Δ6h {fmt_pct(self._pct360)} | Δ24h {fmt_pct(self._pct1440)}")

        # Manage open position exits
        if self.pos:
            entry_p  = self.pos.entry_p
            held_sec = bar.t - self.pos.entry_t
            held_min = held_sec / 60.0
            pnl_pct  = (bar.p - entry_p) / entry_p * 100.0

            # 7.1 Always-on disaster stop (active even during grace period)
            if pnl_pct <= -DISASTER_SL_PCT:
                self._close(bar, "DISASTER_SL", pnl_pct); return

            # 7.2 Update high-water
            if bar.p > self._max_price_since_entry:
                self._max_price_since_entry = bar.p

            runup_pct = (self._max_price_since_entry - entry_p) / entry_p * 100.0

            # 7.3 Arm break-even once enough progress
            if (not self._be_armed) and runup_pct >= BE_ACTIVATE_AT:
                self._be_armed = True
                self._be_stop_price = entry_p * (1.0 - BE_BUFFER_PCT/100.0)

            # 7.4 Arm trailing once further progress
            if (not self._trail_armed) and runup_pct >= TRAIL_ACTIVATE_AT:
                self._trail_armed = True
                gb = TRAIL_GIVEBACK_PCT/100.0
                self._trail_stop_price = self._max_price_since_entry * (1.0 - gb)

            # 7.5 Maintain ratcheting trail (only moves up)
            if self._trail_armed:
                gb = TRAIL_GIVEBACK_PCT/100.0
                candidate = self._max_price_since_entry * (1.0 - gb)
                if candidate > (self._trail_stop_price or 0.0):
                    self._trail_stop_price = candidate

            # 7.6 Respect minimum hold for normal exits
            normal_exits_allowed = (bar.t >= self._hold_armed_at)

            if normal_exits_allowed:
                # Trailing hit?
                if self._trail_armed and self._trail_stop_price and bar.p <= self._trail_stop_price:
                    self._close(bar, "TRAIL", pnl_pct); return
                # Break-even hit?
                if self._be_armed and self._be_stop_price and bar.p <= self._be_stop_price:
                    self._close(bar, "BE", pnl_pct); return
                # Regular TP / SL
                if pnl_pct >= self.cfg.take_profit_pct:
                    self._close(bar, "TP", pnl_pct); return
                if pnl_pct <= -self.cfg.stop_loss_pct:
                    self._close(bar, "SL", pnl_pct); return

            # 7.7 Absolute time stop (even if grace period passed and nothing hit)
            if held_min >= MAX_HOLD_MIN:
                self._close(bar, "TIME", pnl_pct); return

        # State detect
        state, info = self.detector.state_for_index(bars, holder_swaps, idx, market_txn_5m, market_cap_usd, pair_created_at_ms, self._pct5, self._pct60)

        # EXHAUSTION alert (rate-limited)
        if not self.pos and state == "EXHAUSTION":
            should_alert = (now - self._last_exhaustion_alert_ts >= EXHAUSTION_ALERT_COOLDOWN_MIN*60)
            if should_alert:
                why_exh = ""
                if EXPLAIN_EXHAUSTION:
                    payload = {
                        "symbol": self.symbol, "mint": self.mint,
                        "dd_pct": info.get("drawdown_pct", 0.0),
                        "post_tx": info.get("post_tx", 0),
                        "pct_5m": self._pct5, "pct_1h": self._pct60,
                        "m5": market_txn_5m, "m10": self._mkt_txn_10m,
                        "exhaustion_score": info.get("exhaustion_score", 0.0),
                        "mc_low_est_usd": info.get("mc_low_est_usd"),
                        "age_min": info.get("age_min"),
                        "bottom_quality_score": info.get("bottom_quality_score", 0.0)
                    }
                    why_exh, tag = gpt_explain_exhaustion(payload)
                    if why_exh and why_exh.startswith("gpt_"):
                        why_exh = f"(explain fail: {why_exh})"
                curr_mc_str = f"${int(market_cap_usd):,}" if market_cap_usd else "?"
                low_mc_str  = f"${int(info.get('mc_low_est_usd') or 0):,}"
                tg_send(
                    "🟡 Exhaustion "
                    f"{self.symbol} ({self.mint[:6]}…)\n"
                    f"DD {info.get('drawdown_pct','?')}% | Silence tx ≤ {info.get('post_tx',0)} in {self.cfg.silence_minutes}m\n"
                    f"Exhaust {info.get('exhaustion_score',0.0):.2f} | BottomQ {info.get('bottom_quality_score',0.0):.2f}\n"
                    f"MC now ≈ {curr_mc_str} | Est. bottom MC ≈ {low_mc_str}\n"
                    f"Age {info.get('age_min','?')}m\n"
                    f"Low @ {format_timestamp(info.get('low_t'))} | Pair {dex_id or '?'}:{(pair_addr[:6]+'…') if pair_addr else '?'}\n"
                    f"Market txns (5m) {market_txn_5m} | Market txns (10m) {self._mkt_txn_10m}{approx_tag}\n"
                    f"{moves_line}\n"
                    + (f"Why: {why_exh}\n" if why_exh else "") +
                    f"Dexscreener: https://dexscreener.com/solana/{pair_addr or self.mint}\n"
                    f"GMGN: https://gmgn.ai/sol/token/{self.mint}"
                )
                self._last_exhaustion_alert_ts = now

        # Early recovery bias for EXHAUSTION
        early_bid = False
        if state == "EXHAUSTION":
            recovery = float(info.get("recovery_pct", 0.0))
            dd = float(info.get("drawdown_pct", 0.0))
            if recovery >= 20 and dd >= self.cfg.drawdown_threshold_pct and market_txn_5m >= self.soft_m5:
                early_bid = True
                state = "REBID"  # Treat as REBID for bidding logic

        # REBID path (with confirmations + age/mcap boost and GPT gate)
        if not self.pos and state == "REBID":
            low_t = info.get("low_t", bars[idx].t)

            ok_micro, why_micro = micro_structure_ok(bars[:idx+1], low_t, need_consecutive=1)
            ok_route, why_route = route_health_ok(ROUTE_BASE_MINT, self.mint,
                                                 test_usd=ROUTE_TEST_USD,
                                                 max_impact_pct=ROUTE_MAX_IMPACT_PCT,
                                                 slippage_bps=ROUTE_SLIPPAGE_BPS)
            ok_flow,  why_flow  = holder_flow_bias_ok(holder_swaps, low_t, window_min=20)

            ex_score = float(info.get("exhaustion_score", 0.0))
            bq_score = float(info.get("bottom_quality_score", 0.0))
            recovery = float(info.get("recovery_pct", 0.0))
            mc_low_est = info.get("mc_low_est_usd")
            age_min = info.get("age_min")

            # Relax micro if slight negative pct5 but good recovery
            if not ok_micro and self._pct5 > -5.0 and recovery > self.adaptive_min_recovery:
                ok_micro = True
                why_micro += " | pct5_override"

            # Strengthened validation
            if mc_low_est is not None and mc_low_est > 60000:
                if not ok_route:
                    ok_route = False  # Require ok_route for high mcap
            if age_min is not None and age_min < 30:
                if not ok_flow:
                    ok_flow = False  # Require ok_flow for young tokens

            base_passed = (
                ok_micro and ok_route and (ok_flow or (market_txn_5m >= self.adaptive_m5_min))
            )

            must_require_route = (market_cap_usd or 0.0) >= REQUIRE_ROUTE_WHEN_MC_NOW

            boosted_passed = (
                (bq_score >= self.cfg.boost_min_bq) and
                ok_micro and
                (
                    (ok_route and must_require_route) or   # MC high -> route mandatory
                    ((ok_route or ok_flow) and not must_require_route)  # MC lower -> old rule
                ) and
                (market_txn_5m >= self.soft_m5) and
                (ex_score >= 0.6) and (recovery >= 10)
            )

            # quality override for strong bottoms
            quality_override = (
                (bq_score >= 0.50) and
                ok_micro and ok_route and
                (recovery >= self.adaptive_min_recovery * 0.75) and
                (market_txn_5m >= min(self.cfg.rebid_m5_floor, self.soft_m5))
            )

            low_mcap_passed = False
            if mc_low_est is not None:
                low_mcap_passed = (
                    mc_low_est < 25000 and
                    ex_score >= 0.50 and
                    bq_score >= 0.35 and
                    ok_micro and
                    (ok_route or ok_flow) and
                    market_txn_5m >= 3 and
                    recovery >= self.adaptive_min_recovery * 0.5
                )

            passed = base_passed or boosted_passed or quality_override or low_mcap_passed or early_bid

            debug  = (
                f"micro:{ok_micro}({why_micro}) | route:{ok_route}({why_route}) | "
                f"flow:{ok_flow}({why_flow}) | m5={market_txn_5m} | base={self.adaptive_m5_min} "
                f"| soft_m5={self.soft_m5} | ex={ex_score:.2f} | bq={bq_score:.2f}"
                + (" | BOOST" if (boosted_passed and not base_passed) else "")
                + (" | QUALITY_OVERRIDE" if (quality_override and not (base_passed or boosted_passed)) else "")
                + (" | LOW_MCAP" if (low_mcap_passed and not (base_passed or boosted_passed or quality_override)) else "")
                + (" | EARLY_BID" if early_bid else "")
            )

            if not passed:
                if WHYNOT_ON_NON_REBID and (_now() - self._last_whynot_ts) >= WHYNOT_COOLDOWN_MIN*60:
                    tg_send(f"⚪ REBID gated {self.symbol} ({self.mint[:6]}…) — {debug}", cooldown_sec=WHYNOT_COOLDOWN_MIN*60)
                    self._last_whynot_ts = _now()
                self._last_state = state
                return

            # Re-entry cooldown per mint
            last_exit_ts = _LAST_EXIT_TS_BY_MINT.get(self.mint, 0)
            if _now() - last_exit_ts < REENTRY_COOLDOWN_MIN * 60:
                self._last_state = state
                return

            # GPT veto / rationale (optional)
            age_print_min = info.get('age_min')
            if age_print_min is None:
                age_print_min = max(0, (bars[idx].t - bars[0].t)//60)
            payload = {
                "symbol": self.symbol, "mint": self.mint, "dex": dex_id or "?",
                "age_min": age_print_min,
                "m5_txn": market_txn_5m, "m10_txn": self._mkt_txn_10m,
                "pct_5m": self._pct5, "pct_1h": self._pct60, "pct_6h": self._pct360, "pct_24h": self._pct1440,
                "drawdown_pct": info.get("drawdown_pct", 0.0),
                "recovery_pct": info.get("recovery_pct", 0.0),
                "minutes_since_silence": info.get("minutes_since_silence", 0),
                "holder_flow_bias_ok": ok_flow,
                "route_impact_buy_sell": why_route,
                "exhaustion_score": ex_score,
                "bottom_quality_score": bq_score,
                "mc_low_est_usd": info.get("mc_low_est_usd")
            }
            go, conf, reason, gpt_tag = gpt_should_bid(payload)
            if not go:
                if (_now() - self._last_whynot_ts) >= WHYNOT_COOLDOWN_MIN*60:
                    tg_send(f"⚪ REBID veto — {self.symbol} ({self.mint[:6]}…) — conf {conf:.2f} — {reason} ({gpt_tag})",
                            cooldown_sec=WHYNOT_COOLDOWN_MIN*60)
                    self._last_whynot_ts = _now()
                self._last_state = state
                return

            # Open paper position
            self.pos = Trade(symbol=self.symbol, mint=self.mint, entry_t=bar.t, entry_p=bar.p)

            # Wait/exit trackers
            self._max_price_since_entry = bar.p
            self._be_armed   = False
            self._trail_armed = False
            self._hold_armed_at = bar.t + int(MIN_HOLD_MIN * 60)
            self._be_stop_price = None
            self._trail_stop_price = None

            holders_set = set(s.wallet for s in holder_swaps if s.wallet)
            sold_pct = holder_sold_share(holders_set, holder_swaps, low_t, bars[idx].t) if holder_swaps else 0.0

            curr_mc_str = f"${int(market_cap_usd):,}" if market_cap_usd else "?"
            low_mc_str  = f"${int(info.get('mc_low_est_usd') or 0):,}"
            tg_send(
                "🟢 REBID BUY (paper) "
                f"{self.symbol} ({self.mint[:6]}…)\n"
                f"Price {bar.p:.8f} | DD {info.get('drawdown_pct',0)}% | Rec {info.get('recovery_pct',0)}%\n"
                f"Since silence {info.get('minutes_since_silence',0)}m | m5 {info.get('market_txn_5m',0)} ≥ {self.adaptive_m5_min} | "
                f"m10 {self._mkt_txn_10m}{approx_tag} | Holders sold ≈ {sold_pct:.1f}%\n"
                f"MC now ≈ {curr_mc_str} | Est. bottom MC ≈ {low_mc_str}\n"
                f"Age {age_print_min}m\n"
                f"Ex={ex_score:.2f} | BottomQ={bq_score:.2f}\n"
                f"Confirm: {debug}\n"
                + (f"GPT: conf {conf:.2f} — {reason}\n" if ENABLE_GPT_GATE else "") +
                f"Pair {dex_id or '?'}:{(pair_addr[:6]+'…') if pair_addr else '?'}\n"
                f"{moves_line}\n"
                f"Dexscreener: https://dexscreener.com/solana/{pair_addr or self.mint}\n"
                f"GMGN: https://gmgn.ai/sol/token/{self.mint}"
            )

        elif DEBUG_NORMAL_TICKS and (self._last_state != state):
            dd = info.get("drawdown_pct", "?")
            tg_send(f"⚪ {self.symbol} ({self.mint[:6]}…) {state} — DD {dd}% | Rec {info.get('recovery_pct','?')}% | "
                    f"mins_since_sil {info.get('minutes_since_silence','?')} | m5 {info.get('market_txn_5m','?')}")

        self._last_state = state

    def _close(self, bar: Bar, reason: str, pnl_pct: float):
        self.pos.exit_t = bar.t
        self.pos.exit_p = bar.p
        self.pos.reason = reason
        self.pos.pnl_pct = pnl_pct
        tg_send(
            "🔴 EXIT (paper) "
            f"{self.pos.symbol} ({self.pos.mint[:6]}…)\n"
            f"{self.pos.entry_p:.8f} → {bar.p:.8f} | Reason {reason} | PnL {pnl_pct:.2f}%"
        )
        self.trades.append(self.pos)
        _LAST_EXIT_TS_BY_MINT[self.pos.mint] = _now()
        clear_open_pos(self.pos.mint)
        self.pos = None

    def _finalize(self, bars: List[Bar]):
        # Do NOT force-close. Persist open state; exits happen on future candles.
        if self.pos:
            state = {
                "entry_t": self.pos.entry_t,
                "entry_p": self.pos.entry_p,
                "max_price_since_entry": getattr(self, "_max_price_since_entry", self.pos.entry_p),
                "be_armed": getattr(self, "_be_armed", False),
                "trail_armed": getattr(self, "_trail_armed", False),
                "hold_armed_at": getattr(self, "_hold_armed_at", self.pos.entry_t + int(MIN_HOLD_MIN*60)),
                "be_stop_price": getattr(self, "_be_stop_price", None),
                "trail_stop_price": getattr(self, "_trail_stop_price", None),
            }
            save_open_pos(self.mint, state)
        else:
            clear_open_pos(self.mint)

    def report(self) -> Dict:
        if not self.trades:
            return {"trades":0,"winrate_pct":0.0,"avg_trade_pct":0.0,"pnl_sum_pct":0.0,"max_drawdown_pct":0.0}
        pnls = [t.pnl_pct for t in self.trades if t.pnl_pct is not None]
        wins = sum(1 for x in pnls if x>0); wr = wins/len(pnls)*100.0 if pnls else 0.0
        avg = statistics.mean(pnls) if pnls else 0.0; pnl_sum = sum(pnls)
        cum=0.0; peak=-1e9; maxdd=0.0
        for x in pnls:
            cum += x; peak = max(peak, cum); maxdd = max(maxdd, peak-cum)
        return {"trades":len(pnls),"winrate_pct":round(wr,1),"avg_trade_pct":round(avg,2),
                "pnl_sum_pct":round(pnl_sum,2),"max_drawdown_pct":round(maxdd,2)}

# =========================
# Orchestrator
# =========================
def maybe_refetch_for_pct(bars: List[Bar], mint: str) -> List[Bar]:
    need_6h  = pct_change_linear(bars, 360)  is None
    need_24h = pct_change_linear(bars, 1440) is None
    if not (need_6h or need_24h): return bars
    larger = fetch_birdeye_1m_bars(mint, limit=max(BIRDEYE_BARS_LIMIT, 1500))
    return larger if larger else bars

def adjust_cfg(cfg: Config) -> Config:
    if global_pnl_history:
        last_n = global_pnl_history[-min(LAST_TRADES_N, len(global_pnl_history)):]
        wins = sum(1 for p in last_n if p > 0)
        wr = wins / len(last_n) * 100 if len(last_n) > 0 else 0.0
        if wr < WINRATE_LOW:
            cfg.boost_min_bq += BQ_ADJUST_STEP
        elif wr > WINRATE_HIGH:
            cfg.boost_min_bq -= BQ_ADJUST_STEP
        cfg.boost_min_bq = _clamp(cfg.boost_min_bq, 0.4, 0.7)
    return cfg

def compute_adaptive_values(cfg: Config, avg_m5: float) -> Tuple[int, int, float]:
    """Compute adaptive gates in a robust way.
    - Use trimmed median of recent m5 to avoid outliers inflating the gate
    - Clamp adaptive_m5_min to [3..18]
    - Derive a soft_m5 by cfg.rebid_m5_soften_factor but keep >=2
    - Adjust recovery threshold by regime and clamp to [1.0..6.0]
    """
    # Rebuild an m5 sample if available in scope (fallback to provided avg_m5)
    try:
        m5_series = recent_m5[:]  # type: ignore[name-defined]
    except Exception:
        m5_series = []
    base = None
    if m5_series:
        m5_series = [x for x in m5_series if isinstance(x, (int, float)) and x >= 0]
        m5_series.sort()
        k = max(1, int(0.2 * len(m5_series)))  # trim 20% tails
        trimmed = m5_series[k:-k] if len(m5_series) > 2 * k else m5_series
        try:
            base = statistics.median(trimmed)
        except statistics.StatisticsError:
            base = statistics.median(m5_series)
    if base is None:
        base = avg_m5 if isinstance(avg_m5, (int, float)) and avg_m5 > 0 else cfg.rebid_market_txn_5m_min

    raw = int(base * 0.5)  # softer than mean*0.6
    adaptive_m5_min = max(3, min(raw, 18))  # clamp
    cfg.rebid_market_txn_5m_min = adaptive_m5_min

    soft_m5 = max(2, int(adaptive_m5_min * cfg.rebid_m5_soften_factor))

    # Regime-based recovery requirement
    try:
        median_1h = statistics.median(recent_1h)  # type: ignore[name-defined]
    except Exception:
        median_1h = 0.0
    adaptive_min_recovery = cfg.min_recovery_from_low_pct
    if isinstance(median_1h, (int, float)):
        if median_1h < -2.0:  # depressed
            adaptive_min_recovery *= 0.9
        elif median_1h > 2.0:  # hot
            adaptive_min_recovery *= 1.1
    adaptive_min_recovery = _clamp(adaptive_min_recovery, 1.0, 6.0)

    return adaptive_m5_min, soft_m5, adaptive_min_recovery


def pre_scan_for_cluster(cfg: Config, report: dict) -> int:
    cluster_activity = 0
    for symbol, name, ca, item in iter_valid_tokens_from_report(report):
        try:
            bars = fetch_birdeye_1m_bars(ca, limit=BIRDEYE_BARS_LIMIT)
            if len(bars) < 30: continue
            pair_addr, dex_id, m5_total, mc_usd, created_ms = dexscreener_best_pair_and_m5(ca)
            holders = wallets_holding_mint_from_processed(report, ca)
            window_start, window_end = bars[0].t, bars[-1].t
            holder_swaps = mint_swaps_from_holders(holders, ca, per_wallet_limit=WALLET_HISTORY_LIMIT,
                                                   window_start=window_start, window_end=window_end)
            pct5 = pct_change_linear(bars, 5)
            pct60 = pct_change_linear(bars, 60)
            state, info = BottomDetector(cfg).state_for_index(bars, holder_swaps, len(bars)-1, m5_total, mc_usd, created_ms, pct5, pct60)
            dd = info.get("drawdown_pct", 0.0)
            if dd > 50 and m5_total > 5:
                cluster_activity += 1
        except:
            pass
    return cluster_activity

def process_report_at_path(cfg: Config, path: str) -> list[dict]:
    global recent_m5, recent_1h, global_pnl_history
    """Run the normal flow for a single report file and return compact per-token summary rows."""
    try:
        report = json.load(open(path, "r", encoding="utf-8"))
    except Exception as e:
        print("[*] Could not read report:", path, e)
        return []

    tg_send(f"📦 Using processed report: {os.path.basename(path)}")

    # Pre-scan for cluster confidence
    cluster_activity = pre_scan_for_cluster(cfg, report)
    if cluster_activity >= 5:
        cfg.boost_min_bq -= 0.05
        cfg.boost_min_bq = _clamp(cfg.boost_min_bq, 0.3, 0.7)  # Allow more aggressive

    # Collect m5 for adaptive
    m5_list = []
    for symbol, name, ca, item in iter_valid_tokens_from_report(report):
        try:
            _, _, m5_total, _, _ = dexscreener_best_pair_and_m5(ca)
            if m5_total > 0:
                m5_list.append(m5_total)
        except:
            pass
    avg_m5 = statistics.mean(m5_list) if m5_list else cfg.rebid_market_txn_5m_min

    adaptive_m5_min, soft_m5, adaptive_min_recovery = compute_adaptive_values(cfg, avg_m5)

    out_rows = []
    any_traded = False

    for symbol, name, ca, item in iter_valid_tokens_from_report(report):
        try:
            bars = fetch_birdeye_1m_bars(ca, limit=BIRDEYE_BARS_LIMIT)
            if len(bars) < 30:
                tried = "/".join(x[:4] for x in PREFERRED_QUOTES)
                tg_send(f"[{os.path.basename(path)}] ⏭️ Skip {symbol}: insufficient 1m OHLCV for {ca[:6]}… (tried {tried})")
                continue

            bars = maybe_refetch_for_pct(bars, ca)

            holders = wallets_holding_mint_from_processed(report, ca)
            window_start, window_end = bars[0].t, bars[-1].t
            holder_swaps = mint_swaps_from_holders(holders, ca, per_wallet_limit=WALLET_HISTORY_LIMIT,
                                                   window_start=window_start, window_end=window_end)

            pair_addr, dex_id, m5_total, mc_usd, created_ms = dexscreener_best_pair_and_m5(ca)
            exact_10m, approx = dexscreener_trades_last_10m(pair_addr)
            if approx and m5_total:
                exact_10m = m5_total * 2

            trader = PaperTrader(cfg, symbol, ca, adaptive_m5_min, soft_m5, adaptive_min_recovery)
            # Restore any open position for this mint
            persist = load_open_pos(ca)
            if persist:
                trader.pos = Trade(symbol=symbol, mint=ca,
                                   entry_t=persist["entry_t"], entry_p=persist["entry_p"])
                trader._max_price_since_entry = persist.get("max_price_since_entry", trader.pos.entry_p)
                trader._be_armed = persist.get("be_armed", False)
                trader._trail_armed = persist.get("trail_armed", False)
                trader._hold_armed_at = persist.get("hold_armed_at", trader.pos.entry_t + int(MIN_HOLD_MIN*60))
                trader._be_stop_price = persist.get("be_stop_price")
                trader._trail_stop_price = persist.get("trail_stop_price")

            trader.run(bars, holder_swaps, (pair_addr, dex_id, m5_total, mc_usd, created_ms), exact_10m, approx)
            rep = trader.report()

            # Update globals
            for t in trader.trades:
                if t.pnl_pct is not None:
                    global_pnl_history.append(t.pnl_pct)
            if m5_total > 0:
                recent_m5.append(m5_total)
                recent_m5 = recent_m5[-RECENT_TOKENS_WINDOW:]
            ch60 = pct_change_linear(bars, 60)
            if ch60 is not None:
                recent_1h.append(ch60)
                recent_1h = recent_1h[-RECENT_TOKENS_WINDOW:]

            approx_tag = " (~)" if approx else ""
            ch5, ch60, ch360, ch1440 = (pct_change_linear(bars, m) for m in (5,60,360,1440))
            moves  = f"Moves: 5m {fmt_pct(ch5)} · 1h {fmt_pct(ch60)} · 6h {fmt_pct(ch360)} · 24h {fmt_pct(ch1440)}"
            mkt_ln = f"Pair {dex_id or '?'}:{(pair_addr[:6]+'…') if pair_addr else '?'} | Txns 5m {m5_total} | Txns 10m {exact_10m}{approx_tag}"

            tg_send(
                f"[{os.path.basename(path)}] 📈 {symbol} ({ca[:6]}…): Trades {rep['trades']} | Win {rep['winrate_pct']}% | "
                f"Avg {rep['avg_trade_pct']}% | Sum {rep['pnl_sum_pct']}% | MaxDD {rep['max_drawdown_pct']}%\n"
                f"{mkt_ln}\n{moves}\n"
                f"Dexscreener: https://dexscreener.com/solana/{pair_addr or ca}\n"
                f"GMGN: https://gmgn.ai/sol/token/{ca}"
            )

            out_rows.append({
                "report": os.path.basename(path),
                "symbol": symbol,
                "mint": ca,
                "trades": rep["trades"],
                "winrate_pct": rep["winrate_pct"],
                "avg_trade_pct": rep["avg_trade_pct"],
                "pnl_sum_pct": rep["pnl_sum_pct"],
                "max_drawdown_pct": rep["max_drawdown_pct"],
                "m5_txns": m5_total,
                "m10_txns": exact_10m,
            })

            any_traded = any_traded or (rep["trades"] > 0)

        except Exception as e:
            tg_send(f"[{os.path.basename(path)}] ⚠️ Error on {symbol} {ca[:6]}…: {e}")

    if not any_traded:
        tg_send(f"[{os.path.basename(path)}] ℹ️ No entries this run (conditions not met).")
    return out_rows

def process_newest_report(cfg: Config):
    global last_processed_path
    path = newest_processed_report_path()
    if not path or path == last_processed_path:
        return
    rows = process_report_at_path(cfg, path)
    if rows:
        last_processed_path = path
        update_equity_csv()

def update_equity_csv():
    if not global_pnl_history:
        return
    equity = initial_equity
    for p in global_pnl_history:
        equity *= (1 + p / 100.0)
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    total_trades = len(global_pnl_history)
    wins = sum(1 for p in global_pnl_history if p > 0)
    overall_wr = (wins / total_trades * 100) if total_trades > 0 else 0.0
    row = [now_str, round(equity, 2), total_trades, round(overall_wr, 1)]

    csv_path = os.path.join(PROCESSED_DIR, "equity_curve.csv")
    exists = os.path.exists(csv_path)
    import csv
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["timestamp", "equity", "total_trades", "overall_winrate_pct"])
        w.writerow(row)
    tg_send(f"📊 Updated equity curve: equity={round(equity,2)}, trades={total_trades}, wr={round(overall_wr,1)}%")

def simulate_paper_trades(cfg: Config, n_reports: int = 20):
    paths = list_latest_reports(ANALYZE_LAST_N)
    if not paths:
        print("[*] No reports for simulation.")
        return
    total_pnl = 0.0
    total_trades = 0
    all_rows = []
    for p in paths:
        rows = process_report_at_path(cfg, p)
        all_rows.extend(rows)
        for r in rows:
            total_pnl += r["pnl_sum_pct"]
            total_trades += r["trades"]
    print(f"Simulated {total_trades} trades over {len(paths)} reports, total PnL: {total_pnl:.2f}%")

def compute_global_stats() -> Dict:
    if not global_pnl_history:
        return {
            "trades": 0,
            "winrate_pct": 0.0,
            "avg_pnl_pct": 0.0,
            "total_pnl_sum_pct": 0.0,
            "equity": initial_equity,
            "max_dd_pct": 0.0
        }

    pnls = global_pnl_history
    trades = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    winrate = wins / trades * 100 if trades else 0.0
    avg_pnl = statistics.mean(pnls) if pnls else 0.0
    total_sum = sum(pnls)

    # Equity curve
    equity_curve = [initial_equity]
    for p in pnls:
        next_eq = equity_curve[-1] * (1 + p / 100.0)
        equity_curve.append(next_eq)
    current_equity = equity_curve[-1]

    # Max DD
    max_dd = 0.0
    peak = equity_curve[0]
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak * 100.0 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    return {
        "trades": trades,
        "winrate_pct": round(winrate, 1),
        "avg_pnl_pct": round(avg_pnl, 2),
        "total_pnl_sum_pct": round(total_sum, 2),
        "equity": round(current_equity, 2),
        "max_dd_pct": round(max_dd, 2)
    }

def send_global_pnl_summary(action_desc: str):
    stats = compute_global_stats()
    msg = (
        f"📊 Overall PnL Summary after {action_desc}:\n"
        f"Total Trades: {stats['trades']}\n"
        f"Win Rate: {stats['winrate_pct']}%\n"
        f"Average PnL per Trade: {stats['avg_pnl_pct']}%\n"
        f"Total PnL Sum: {stats['total_pnl_sum_pct']}%\n"
        f"Current Equity (from {initial_equity}): {stats['equity']}\n"
        f"Max Drawdown: {stats['max_dd_pct']}%\n"
        f"Recent Trades (last 5 PnLs): {', '.join(f'{p:.2f}%' for p in global_pnl_history[-5:]) if global_pnl_history else 'None'}"
    )
    tg_send(msg)

# =========================
# Runner
# =========================
def main():
    cfg = Config()
    cfg = adjust_cfg(cfg)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    be_ws = None

    # boot Birdeye WS if enabled
    if BIRDEYE_WS_ON:
        be_ws = BirdeyeWS(BIRDEYE_WS_URL, x_api_key=BIRDEYE_WS_API_KEY, ping_sec=BIRDEYE_WS_PING_SEC)
        be_ws.start()

    def seed_ws_from_report(path: Optional[str]):
        """Subscribe mints found in the latest processed report."""
        if not (BIRDEYE_WS_ON and be_ws and path):
            return
        try:
            report = json.load(open(path, "r", encoding="utf-8"))
            for sym, name, ca, item in iter_valid_tokens_from_report(report):
                be_ws.subscribe(ca)
        except Exception as e:
            print("[ws-seed] err:", e)

    if SIMULATION_MODE:
        tg_send("🧪 Simulation mode enabled")
        simulate_paper_trades(cfg, ANALYZE_LAST_N or 20)
        return

    tg_send("🤖 Paper trader — start")
    while True:
        prev_trades_count = len(global_pnl_history)
        if ANALYZE_LAST_N > 0:
            tg_send(f"🧪 Analyze last {ANALYZE_LAST_N} reports")
            paths = list_latest_reports(ANALYZE_LAST_N)
            if not paths:
                print("[*] No processed reports found.")
            else:
                all_rows: list[dict] = []
                for p in paths:
                    rows = process_report_at_path(cfg, p)
                    all_rows.extend(rows)

                # Write a tiny CSV summary: reports/holdings/analysis/last_<N>_summary.csv
                out_dir = os.path.join(PROCESSED_DIR, "..", "analysis")
                os.makedirs(out_dir, exist_ok=True)
                out_csv = os.path.join(out_dir, f"last_{ANALYZE_LAST_N}_summary.csv")
                try:
                    import csv
                    fieldnames = ["report","symbol","mint","trades","winrate_pct","avg_trade_pct","pnl_sum_pct","max_drawdown_pct","m5_txns","m10_txns"]
                    with open(out_csv, "w", newline="", encoding="utf-8") as f:
                        w = csv.DictWriter(f, fieldnames=fieldnames)
                        w.writeheader()
                        for r in all_rows:
                            w.writerow({k: r.get(k, "") for k in fieldnames})
                    tg_send(f"🧾 Wrote summary: {out_csv}")
                except Exception as e:
                    print("[*] Could not write CSV:", e)

            tg_send("✅ Analysis complete")
            send_global_pnl_summary("analysis")
        else:
            process_newest_report(cfg)
            if last_processed_path:
                seed_ws_from_report(last_processed_path)
            send_global_pnl_summary("processing newest report" if len(global_pnl_history) > prev_trades_count else "no new report")

        # Drain WS ticks & trigger quick re-evals
        if BIRDEYE_WS_ON and be_ws:
            drained = 0
            while drained < 200:  # guard against starving the loop
                try:
                    mint, px, ts = be_ws.out_q.get_nowait()
                except Empty:
                    break

                # look at the *previous* cached price
                prev_entry = LIVE_PRICE.get(mint)
                prev_px = prev_entry["price"] if prev_entry else None

                # decide if move is meaningful (default 0.5%)
                rel = 0.0 if prev_px is None else abs((px - prev_px) / max(prev_px, 1e-12))
                if (prev_px is None) or (rel >= BIRDEYE_WS_MIN_REL_MOVE):
                    reevaluate_mint_once(cfg, mint)

                # now update the cache
                record_live_price(mint, px, ts)

                drained += 1

        time.sleep(LOOP_SLEEP_SEC)
    tg_send("✅ Run complete")

if __name__ == "__main__":
    main()