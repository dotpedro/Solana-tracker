# Script B: Trade Executor (realistic paper trader)
# Consumes scenarios from JSONL queue, enters positions with latency/slippage/fees,
# manages exits with wall-clock rules, logs ENTRY/EXIT to trades.jsonl.
# --- MODIFIED TO WORK WITH TRADE MANAGER ---

from __future__ import annotations
import os, json, time, math, random, csv, statistics, shutil
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Set
import requests
from dotenv import load_dotenv
load_dotenv(override=True)
from threading import Thread, Event
from queue import Queue, Empty
import ssl
from websocket import WebSocketApp
import uuid

# =========================
# ENV / KNOBS (safe defaults)
# =========================
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

BIRDEYE_API_KEY = os.getenv('BIRDEYE_API_KEY', '')
BIRDEYE_WS_ON = int(os.getenv("BIRDEYE_WS_ON", "1"))
# Corrected WS URL format
BIRDEYE_WS_URL = os.getenv("BIRDEYE_WS_URL", "wss://public-api.birdeye.so/socket/solana")
BIRDEYE_WS_API_KEY = os.getenv("BIRDEYE_WS_API_KEY", "")
BIRDEYE_WS_PING_SEC = int(os.getenv("BIRDEYE_WS_PING_SEC", "20"))
BIRDEYE_WS_MINT_CAP = int(os.getenv("BIRDEYE_WS_MINT_CAP", "200"))
BIRDEYE_WS_MIN_REL_MOVE = float(os.getenv("BIRDEYE_WS_MIN_REL_MOVE", "0.005"))

ENTRY_LATENCY_SEC = int(os.getenv('ENTRY_LATENCY_SEC', '2'))
SLIPPAGE_BPS = int(os.getenv('SLIPPAGE_BPS', '40'))
FEES_BPS = int(os.getenv('FEES_BPS', '30'))
MIN_HOLD_SEC = int(os.getenv('MIN_HOLD_SEC', '60'))
TAKE_PROFIT_PCT = float(os.getenv('TAKE_PROFIT_PCT', '40.0'))
STOP_LOSS_PCT = float(os.getenv('STOP_LOSS_PCT', '20.0'))
DISASTER_SL_PCT = float(os.getenv('DISASTER_SL_PCT', '35.0'))
BE_ACTIVATE_AT = float(os.getenv('BE_ACTIVATE_AT', '12.0'))
BE_BUFFER_PCT = float(os.getenv('BE_BUFFER_PCT', '1.0'))
TRAIL_ACTIVATE_AT = float(os.getenv('TRAIL_ACTIVATE_AT', '25.0'))
TRAIL_GIVEBACK_PCT = float(os.getenv('TRAIL_GIVEBACK_PCT', '12.0'))
MAX_HOLD_MIN = int(os.getenv('MAX_HOLD_MIN', '180'))
REENTRY_COOLDOWN_MIN = int(os.getenv('REENTRY_COOLDOWN_MIN', '20'))
POLL_SEC = int(os.getenv('POLL_SEC', '5'))

# MODIFIED: Default input queue is now the output of Trade Manager
SIM_QUEUE = os.getenv('SIM_QUEUE', 'scenarios_exec.jsonl')
# NEW: Control channel for commands from Trade Manager
EXEC_CONTROL = os.getenv("EXEC_CONTROL", "exec_control.jsonl")
TRADES_OUT = os.getenv('TRADES_OUT', 'trades.jsonl')
OPEN_POS_DIR = os.getenv('OPEN_POS_DIR', 'open_positions')
os.makedirs(OPEN_POS_DIR, exist_ok=True)

# =========================
# Reset Helpers & Logic
# =========================
def _touch(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8"): pass

def _del(path: str):
    try:
        if os.path.isfile(path): os.remove(path)
    except Exception as e: print(f"[reset] Could not delete file {path}: {e}")

def _rmdir_contents(path: str):
    try:
        if os.path.isdir(path): shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)
    except Exception as e: print(f"[reset] Could not clear folder {path}: {e}")

AUTO_RESET_ON_START = int(os.getenv("AUTO_RESET_ON_START", "1"))
if AUTO_RESET_ON_START:
    print("[reset] Trade Executioner: starting clean…")
    _del(TRADES_OUT)
    _del(SIM_QUEUE)
    _del(os.path.join(os.path.dirname(TRADES_OUT), "equity_curve.csv"))
    _touch(SIM_QUEUE)
    _touch(EXEC_CONTROL)
    _rmdir_contents(OPEN_POS_DIR)

# =========================
# Script State & Config
# =========================
# Add SOL and USDC mint addresses
USDC_MINT_SOL = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
SOL_MINT_SOL = 'So11111111111111111111111111111111111111112'

# Update the PREFERRED_QUOTES list to include both
PREFERRED_QUOTES = [USDC_MINT_SOL, SOL_MINT_SOL]

# Live price cache (from WS or poll)
LIVE_PRICE: Dict[str, Dict] = {}  # mint -> {"price": float, "ts": int}
ENABLE_GPT_GATE = int(os.getenv('ENABLE_GPT_GATE', '0')) # For alert formatting
initial_equity = 100.0

# System health monitoring
_REST_FALLBACKS: List[int] = []
_LAST_HEALTH_ALERT_TS: Dict[str, int] = {}
QUEUE_BACKLOG_THRESHOLD = 10

def record_live_price(mint: str, px: float, ts: Optional[int] = None):
    if px is None: 
        return
    LIVE_PRICE[mint] = {"price": float(px), "ts": ts or _now()}

# =========================
# NEW & ROBUST PRICE FETCHING
# =========================

def fetch_birdeye_spot_price(mint: str) -> Optional[float]:
    """Layer 1: Fetch spot price from Birdeye."""
    if not BIRDEYE_API_KEY:
        return None
    url = "https://public-api.birdeye.so/defi/price"
    headers = {"accept":"application/json", "x-chain":"solana", "X-API-KEY": BIRDEYE_API_KEY}
    try:
        r = requests.get(url, params={"address": mint}, headers=headers, timeout=4)
        if r.status_code == 200:
            data = (r.json() or {}).get("data") or {}
            px = data.get("value")
            if px:
                print(f"[*] Fetched spot price for {mint[:6]} via Birdeye REST: ${px}")
                return float(px)
    except Exception as e:
        print(f"[!] Birdeye spot price exception for {mint[:6]}: {e}")
    return None

def fetch_dexscreener_price(mint: str) -> Optional[float]:
    """Layer 3: Final fallback to Dexscreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        r = requests.get(url, timeout=4)
        if r.status_code == 200:
            pairs = (r.json() or {}).get("pairs") or []
            if not pairs: return None
            # Sort by liquidity to get the most reliable price
            pairs.sort(key=lambda p: float(((p.get("liquidity") or {}).get("usd") or 0.0)), reverse=True)
            px_str = pairs[0].get("priceUsd")
            if px_str:
                px = float(px_str)
                print(f"[*] Fetched price for {mint[:6]} via Dexscreener REST: ${px}")
                return px
    except Exception as e:
        print(f"[!] Dexscreener fallback exception for {mint[:6]}: {e}")
    return None

def get_price(mint: str) -> Optional[float]:
    """
    Robust multi-layer price fetching function.
    Checks live WS cache, then falls back to Birdeye Spot, Birdeye OHLCV, and finally Dexscreener.
    """
    d = LIVE_PRICE.get(mint)
    if d and _now() - d["ts"] < 60:
        return d["price"]

    print(f"[!] Live price for {mint[:6]} stale or missing. Falling back to REST APIs.")
    _REST_FALLBACKS.append(_now())

    # 1. Try Birdeye Spot Price API
    px = fetch_birdeye_spot_price(mint)
    if px:
        record_live_price(mint, px, _now())
        return px

    # 2. Try Birdeye OHLCV with a wider 15-minute window
    bars = fetch_birdeye_1m_bars(mint, 15) # Use 15-min window for robustness
    if bars:
        price = bars[-1].p
        ts = bars[-1].t
        print(f"[*] Fetched price for {mint[:6]} via Birdeye OHLCV REST: ${price}")
        record_live_price(mint, price, ts)
        return price

    # 3. Final fallback to Dexscreener
    px = fetch_dexscreener_price(mint)
    if px:
        record_live_price(mint, px, _now())
        return px

    print(f"[!] ALL REST API fallbacks failed for {mint[:6]}. No price available.")
    return None


# =========================
# Safe utilities & Tailer
# =========================
def _now() -> int:
    return int(time.time())

class Tailer:
    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        _touch(path)

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
            _touch(self.path)
            self.pos = 0
        return out

# =========================
# Telegram
# =========================
def tg_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('[tg] (dry)', text.replace('\n', ' ')[:220])
        return
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text[:3900],
        'disable_web_page_preview': True
    }
    try:
        r = requests.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json=payload, timeout=8
        )
        if r.status_code != 200:
            print(f"[tg] error: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        print(f"[tg] exception: {e}")

# =========================
# PnL & Equity Reporting
# =========================
def get_all_trades_from_log() -> list[dict]:
    trades = []
    if not os.path.exists(TRADES_OUT):
        return []
    with open(TRADES_OUT, "r", encoding="utf-8") as f:
        for line in f:
            try:
                trade = json.loads(line)
                if trade.get("type") == "EXIT":
                    trades.append(trade)
            except:
                continue
    return trades

def compute_global_stats() -> Dict:
    all_exits = get_all_trades_from_log()
    if not all_exits:
        return {
            "trades": 0, "winrate_pct": 0.0, "avg_pnl_pct": 0.0,
            "total_pnl_sum_pct": 0.0, "equity": initial_equity, "max_dd_pct": 0.0
        }

    pnls = [float(t["pnl_pct"]) for t in all_exits if "pnl_pct" in t]
    if not pnls and all_exits: # Handle case with entries but no exits yet
         return {
            "trades": 0, "winrate_pct": 0.0, "avg_pnl_pct": 0.0,
            "total_pnl_sum_pct": 0.0, "equity": initial_equity, "max_dd_pct": 0.0
        }
    
    trades = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    winrate = wins / trades * 100 if trades else 0.0
    avg_pnl = statistics.mean(pnls) if pnls else 0.0
    total_sum = sum(pnls)

    equity_curve = [initial_equity]
    for p in pnls:
        equity_curve.append(equity_curve[-1] * (1 + p / 100.0))
    current_equity = equity_curve[-1]

    max_dd = 0.0
    peak = equity_curve[0]
    for eq in equity_curve:
        if eq > peak: peak = eq
        dd = (peak - eq) / peak * 100.0 if peak > 0 else 0.0
        if dd > max_dd: max_dd = dd

    return {
        "trades": trades, "winrate_pct": round(winrate, 1), "avg_pnl_pct": round(avg_pnl, 2),
        "total_pnl_sum_pct": round(total_sum, 2), "equity": round(current_equity, 2), "max_dd_pct": round(max_dd, 2)
    }

def send_global_pnl_summary():
    stats = compute_global_stats()
    all_exits = get_all_trades_from_log()
    last_5_pnls = [t.get('pnl_pct', 0.0) for t in all_exits[-5:]]
    msg = (
        f"📊 Overall PnL Summary:\n"
        f"Total Trades: {stats['trades']}\n"
        f"Win Rate: {stats['winrate_pct']}%\n"
        f"Average PnL per Trade: {stats['avg_pnl_pct']}%\n"
        f"Total PnL Sum: {stats['total_pnl_sum_pct']}%\n"
        f"Current Equity (from ${initial_equity:.2f}): ${stats['equity']:.2f}\n"
        f"Max Drawdown: {stats['max_dd_pct']}%\n"
        f"Recent Trades (last 5 PnLs): {', '.join(f'{p:.2f}%' for p in last_5_pnls) if last_5_pnls else 'None'}"
    )
    tg_send(msg)

def update_equity_csv():
    stats = compute_global_stats()
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    row = [now_str, stats['equity'], stats['trades'], stats['winrate_pct']]
    
    csv_path = os.path.join(os.path.dirname(TRADES_OUT), "equity_curve.csv")
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "equity", "total_trades", "overall_winrate_pct"])
        writer.writerow(row)

# =========================
# Birdeye WS
# =========================
class BirdeyeWS:
    def __init__(self, url: str, x_api_key: str = "", ping_sec: int = 20):
        if not x_api_key:
            raise ValueError("Birdeye WebSocket API key is required.")
        sep = "&" if "?" in url else "?"
        self.url = f"{url}{sep}x-api-key={x_api_key}"
        
        self.ping_sec = ping_sec
        self.ws: Optional[WebSocketApp] = None
        self.thread: Optional[Thread] = None
        self.stop_ev = Event()
        self.subscribed: Set[str] = set()
        self.out_q: Queue = Queue()

    def _headers(self):
        return [
            "Origin: https://birdeye.so",
            "Sec-WebSocket-Origin: https://birdeye.so",
            "Sec-WebSocket-Protocol: echo-protocol",
        ]

    def _on_open(self, ws):
        print("[ws] WebSocket connection opened.")
        for m in list(self.subscribed):
            try:
                print(f"[ws] Re-subscribing to {m[:6]}…")
                # Use more reliable base/quote subscription
                subscribe_msg = {
                    "type": "SUBSCRIBE_BASE_QUOTE_PRICE",
                    "data": {
                        "base_address": m,
                        "quote_address": USDC_MINT_SOL
                    }
                }
                ws.send(json.dumps(subscribe_msg))
            except Exception as e:
                print(f"[ws] Failed to re-subscribe on open: {e}")

    def _on_message(self, ws, msg):
        try:
            data = json.loads(msg)
        except:
            return
        # Note: Response format for SUBSCRIBE_BASE_QUOTE_PRICE is the same as SUBSCRIBE_PRICE
        if isinstance(data, dict) and data.get("type") == "PRICE_DATA":
            d = data.get("data") or {}
            mint = d.get("address") # This is the base_address (the token mint)
            price = d.get("c") or d.get("close")
            ts = int(d.get("unixTime") or d.get("t") or _now())
            if mint and price:
                self.out_q.put((mint, float(price), ts))

    def _on_error(self, ws, err):
        print(f"[ws] Error: {err}")

    def _on_close(self, ws, code, reason):
        print(f"[ws] Connection closed: {code} - {reason}")

    def _on_ping(self, ws, message):
        print("[ws] Ping received, sending pong.")
        self.ws.pong(message)

    def _on_pong(self, ws, message):
        print("[ws] Pong received.")

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_ev.clear()

        def run():
            while not self.stop_ev.is_set():
                try:
                    print(f"[ws] Connecting to {self.url}...")
                    self.ws = WebSocketApp(
                        self.url,
                        header=self._headers(),
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                        on_ping=self._on_ping,
                        on_pong=self._on_pong
                    )
                    self.ws.run_forever(
                        sslopt={"cert_reqs": ssl.CERT_NONE}, 
                        ping_interval=self.ping_sec,
                        ping_timeout=10
                    )
                except Exception as e:
                    print(f"[ws] Run error: {e}")
                
                if not self.stop_ev.is_set():
                    print("[ws] Reconnecting in 5 seconds...")
                    time.sleep(5)

        self.thread = Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_ev.set()
        if self.ws:
            self.ws.close()
        print("[ws] WebSocket stopped.")

    def subscribe(self, mint: str):
        if not mint or len(self.subscribed) >= BIRDEYE_WS_MINT_CAP:
            return
        if mint in self.subscribed:
            return
        print(f"[ws] Subscribing to price updates for {mint[:6]}…")
        self.subscribed.add(mint)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                # Use more reliable base/quote subscription
                subscribe_msg = {
                    "type": "SUBSCRIBE_BASE_QUOTE_PRICE",
                    "data": {
                        "base_address": mint,
                        "quote_address": USDC_MINT_SOL
                    }
                }
                self.ws.send(json.dumps(subscribe_msg))
            except Exception as e:
                print(f"[ws] Failed to send subscribe message: {e}")

    def unsubscribe(self, mint: str):
        if mint not in self.subscribed:
            return
        print(f"[ws] Unsubscribing from {mint[:6]}…")
        self.subscribed.discard(mint)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                unsubscribe_msg = {
                    "type": "UNSUBSCRIBE_BASE_QUOTE_PRICE", 
                    "data": {
                        "base_address": mint,
                        "quote_address": USDC_MINT_SOL
                    }
                }
                self.ws.send(json.dumps(unsubscribe_msg))
            except Exception as e:
                 print(f"[ws] Failed to send unsubscribe message: {e}")

# =========================
# Market data (OHLCV fallback)
# =========================
@dataclass
class Bar:
    t: int
    p: float

def fetch_birdeye_1m_bars(mint: str, minutes: int) -> List[Bar]:
    """Layer 2: Fetch OHLCV bars for a given number of minutes."""
    if not BIRDEYE_API_KEY:
        print("[!] Birdeye REST call skipped: BIRDEYE_API_KEY is not set.")
        return []

    for quote in PREFERRED_QUOTES:
        time_from, time_to = _now() - minutes*60, _now()
        url = 'https://public-api.birdeye.so/defi/ohlcv/base_quote'
        params = {'base_address': mint, 'quote_address': quote, 'type':'1m',
                  'time_from': time_from, 'time_to': time_to}
        headers = {'accept':'application/json', 'x-chain':'solana', 'X-API-KEY': BIRDEYE_API_KEY}
        
        try:
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if r.status_code == 200:
                items = ((r.json() or {}).get('data') or {}).get('items') or []
                bars: List[Bar] = []
                for it in items:
                    ts = it.get('unixTime') or 0
                    cl = it.get('close') or 0.0
                    if ts and cl > 0:
                        bars.append(Bar(int(ts), float(cl)))
                bars.sort(key=lambda b: b.t)
                if len(bars) >= 1:
                    return bars
            else:
                print(f"[!] Birdeye OHLCV Error for {mint[:6]}...: Status {r.status_code} - {r.text[:100]}")

        except Exception as e:
            print(f"[!] Birdeye OHLCV Exception for {mint[:6]}...: {e}")
            
    return []

# =========================
# Position Management
# =========================
@dataclass
class Position:
    id: str
    mint: str
    symbol: str
    entry_ts: int
    entry_px: float
    qty: float
    size_usd: float
    max_px: float
    be_armed: bool = False
    trail_armed: bool = False
    hold_armed_at: int = 0
    be_stop_px: Optional[float] = None
    trail_stop_px: Optional[float] = None
    note: Dict = field(default_factory=dict)
    time_warning_sent: bool = False

def _open_pos_path(mint: str) -> str:
    return os.path.join(OPEN_POS_DIR, f"{mint}.json")

def load_open_pos(mint: str) -> Optional[Dict]:
    p = _open_pos_path(mint)
    if not os.path.isfile(p): return None
    try: return json.load(open(p, "r", encoding="utf-8"))
    except: return None

def save_open_pos(pos: Position):
    state = {
        "id": pos.id, "mint": pos.mint, "symbol": pos.symbol, "entry_ts": pos.entry_ts,
        "entry_px": pos.entry_px, "qty": pos.qty, "size_usd": pos.size_usd, "max_px": pos.max_px,
        "be_armed": pos.be_armed, "trail_armed": pos.trail_armed, "hold_armed_at": pos.hold_armed_at,
        "be_stop_px": pos.be_stop_px, "trail_stop_px": pos.trail_stop_px, "note": pos.note,
        "time_warning_sent": pos.time_warning_sent,
    }
    try: json.dump(state, open(_open_pos_path(pos.mint), "w", encoding="utf-8"))
    except Exception as e:
        print(f"[!] Error saving open position for {pos.mint[:6]}: {e}")

def clear_open_pos(mint: str):
    try:
        p = _open_pos_path(mint)
        if os.path.isfile(p): os.remove(p)
    except:
        pass

# =========================
# Executor
# =========================
class TradeExecutor:
    def __init__(self, ws: Optional[BirdeyeWS]):
        self.ws = ws
        self.positions: Dict[str, Position] = {}
        self.scenario_tailer = Tailer(SIM_QUEUE)
        self.control_tailer = Tailer(EXEC_CONTROL)
        self.pending_scenarios: List[Dict] = []
        self._last_exit_ts_by_mint: Dict[str, int] = {}
        self._load_from_disk()

    def _load_from_disk(self):
        print("[*] Loading any existing open positions from disk...")
        loaded_count = 0
        for fn in os.listdir(OPEN_POS_DIR):
            if fn.endswith(".json"):
                mint = fn[:-5]
                state = load_open_pos(mint)
                if state:
                    pos = Position(**state)
                    self.positions[mint] = pos
                    if self.ws:
                        self.ws.subscribe(mint)
                    loaded_count += 1
                    print(f"[*] Loaded position: {pos.symbol} ({mint[:6]})")
        print(f"[*] Loaded {loaded_count} open position(s).")

    def run(self):
        while True:
            print(f"\n--- Executor Loop Start (Positions: {len(self.positions)}, Pending: {len(self.pending_scenarios)}) ---")
            self.check_system_health()
            
            print("[1a] Checking for control messages...")
            self.check_control_messages()

            print("[1b] Reading scenario queue...")
            new_scenarios = self.scenario_tailer.read_new()
            if new_scenarios:
                print(f"[*] Found {len(new_scenarios)} new scenario(s) in queue.")
                self.pending_scenarios.extend(new_scenarios)
            
            print("[2] Processing pending scenarios...")
            self.try_open_pending()
            
            print("[3] Stepping through open positions...")
            self.step_positions()
            
            if self.ws:
                print("[4] Draining WebSocket price updates...")
                self.drain_ws()
                
            print(f"--- Loop End (Positions: {len(self.positions)}) --- Sleeping for {POLL_SEC} seconds...")
            time.sleep(POLL_SEC)

    def check_control_messages(self):
        messages = self.control_tailer.read_new()
        for msg in messages:
            if msg.get("type") == "FORCE_CLOSE":
                mint = msg.get("mint")
                reason = msg.get("reason", "remote")
                print(f"[!] Received FORCE_CLOSE command for {mint[:6]}... Reason: {reason}")
                
                # CRITICAL FIX: Remove any pending scenarios for this mint and set immediate cooldown
                self._last_exit_ts_by_mint[mint] = _now()
                before_count = len(self.pending_scenarios)
                self.pending_scenarios = [s for s in self.pending_scenarios if s.get("mint") != mint]
                after_count = len(self.pending_scenarios)
                removed_count = before_count - after_count
                if removed_count > 0:
                    print(f"[!] Removed {removed_count} pending scenario(s) for {mint[:6]} due to FORCE_CLOSE")
                
                if mint in self.positions:
                    pos = self.positions[mint]
                    print(f"[!] Force closing position: {pos.symbol} ({mint[:6]})")
                    px = get_price(mint)
                    if px is None:
                        print(f"[!] Cannot force close {pos.symbol} ({mint[:6]}): No price available.")
                        continue
                    pnl_pct = (px - pos.entry_px) / pos.entry_px * 100.0 if pos.entry_px > 0 else 0.0
                    self._close(pos, px, f"FORCE_CLOSE:{reason}", pnl_pct)
                else:
                    print(f"[*] Ignoring FORCE_CLOSE for {mint[:6]}: not an open position.")

    def try_open_pending(self):
        now = _now()
        
        # NEW: Deduplicate scenarios - keep only the latest per mint
        mint_to_scenario = {}
        for scenario in self.pending_scenarios:
            mint = scenario["mint"]
            mint_to_scenario[mint] = scenario  # Last one wins
        self.pending_scenarios = list(mint_to_scenario.values())
        
        remaining = []
        if not self.pending_scenarios:
            print("[-] No pending scenarios to process.")
            return

        for scenario in self.pending_scenarios:
            mint = scenario["mint"]
            symbol = scenario["symbol"]
            
            if self.ws:
                self.ws.subscribe(mint)
            
            if now < scenario.get("signal_ts", 0) + ENTRY_LATENCY_SEC:
                print(f"[*] Scenario for {symbol} ({mint[:6]}) is too recent, will retry.")
                remaining.append(scenario)
                continue
            
            if mint in self.positions:
                print(f"[-] Skipping scenario for {symbol} ({mint[:6]}): position already open.")
                continue

            last_exit = self._last_exit_ts_by_mint.get(mint, 0)
            cooldown_remaining = (REENTRY_COOLDOWN_MIN * 60) - (now - last_exit)
            if now - last_exit < REENTRY_COOLDOWN_MIN * 60:
                print(f"[-] Skipping scenario for {symbol} ({mint[:6]}): on reentry cooldown ({cooldown_remaining//60}m {cooldown_remaining%60}s remaining).")
                continue

            print(f"[*] Attempting to open position for {symbol} ({mint[:6]}...).")
            px = get_price(mint)
            if px is None:
                print(f"[!] Could not fetch price for {symbol} ({mint[:6]}), will retry.")
                remaining.append(scenario)
                continue

            direction = 1 if scenario.get("side", "buy") == "buy" else -1
            slip = (SLIPPAGE_BPS / 10000.0) * direction
            fee = FEES_BPS / 10000.0
            fill_px = px * (1 + slip) * (1 + fee)
            qty = scenario["size_usd"] / fill_px if fill_px > 0 else 0.0

            pos = Position(
                id=scenario.get("id", str(uuid.uuid4())), mint=mint, symbol=symbol, entry_ts=now,
                entry_px=fill_px, qty=qty, size_usd=scenario["size_usd"],
                max_px=fill_px, hold_armed_at=now + MIN_HOLD_SEC,
                note=scenario.get("meta", {})
            )
            self.positions[mint] = pos
            save_open_pos(pos)
            
            if self.ws:
                self.ws.subscribe(mint)
            print(f"[+] Successfully opened position for {symbol} ({mint[:6]}) at ${fill_px:.8f}.")

            entry_rec = {
                "type": "ENTRY", "id": pos.id, "mint": mint, "symbol": pos.symbol, "ts": now,
                "px": fill_px, "qty": qty, "size_usd": scenario["size_usd"],
                "fees_bps": FEES_BPS, "slippage_bps": SLIPPAGE_BPS, "note": pos.note
            }
            with open(TRADES_OUT, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry_rec) + "\n")
            
            meta = pos.note
            tg_send(
                "🟢 ENTRY (paper) "
                f"{pos.symbol} ({mint[:6]}…)\n"
                f"Price {fill_px:.8f} | BQ: {meta.get('bq', '?')} | RR: {meta.get('rr', '?')}\n"
                f"Reason: {meta.get('reason', 'N/A')}\n"
                f"MC now ≈ ${int(meta.get('mc', 0)):,}\n"
                f"Dexscreener: https://dexscreener.com/solana/{meta.get('pair_addr') or mint}"
            )
        self.pending_scenarios = remaining

    def step_positions(self):
        now = _now()
        if not self.positions:
            print("[-] No open positions to manage.")
            return

        print(f"[*] Managing {len(self.positions)} open position(s)...")
        for mint, pos in list(self.positions.items()):
            px = get_price(mint)
            if px is None:
                print(f"[!] Cannot step position for {pos.symbol} ({mint[:6]}): No price available.")
                continue
            
            pos.max_px = max(pos.max_px, px)
            pnl_pct = (px - pos.entry_px) / pos.entry_px * 100.0 if pos.entry_px > 0 else 0.0
            
            print(f"[*] Checking {pos.symbol} ({mint[:6]}): Px={px:.8f}, PnL={pnl_pct:.2f}%, MaxPx={pos.max_px:.8f}")

            if pnl_pct <= -DISASTER_SL_PCT:
                print(f"[!] Disaster SL triggered for {pos.symbol} ({mint[:6]}) at {pnl_pct:.2f}%")
                self._close(pos, px, "DISASTER_SL", pnl_pct); continue

            runup_pct = (pos.max_px - pos.entry_px) / pos.entry_px * 100.0 if pos.entry_px > 0 else 0.0

            if not pos.be_armed and runup_pct >= BE_ACTIVATE_AT:
                pos.be_armed = True
                pos.be_stop_px = pos.entry_px * (1.0 - BE_BUFFER_PCT / 100.0)
                print(f"[+] Armed BE stop for {pos.symbol} ({mint[:6]}) at ${pos.be_stop_px:.8f}.")
                tg_send(f"🛡️ Break-Even Armed for {pos.symbol} ({pos.mint[:6]}…)\nRun-up hit {runup_pct:.2f}%.")

            if not pos.trail_armed and runup_pct >= TRAIL_ACTIVATE_AT:
                pos.trail_armed = True
                gb = TRAIL_GIVEBACK_PCT / 100.0
                pos.trail_stop_px = pos.max_px * (1.0 - gb)
                print(f"[+] Armed trailing stop for {pos.symbol} ({mint[:6]}) at ${pos.trail_stop_px:.8f}.")
                tg_send(f"📈 Trailing Stop Armed for {pos.symbol} ({pos.mint[:6]}…)\nRun-up hit {runup_pct:.2f}%.")

            if pos.trail_armed and pos.trail_stop_px:
                gb = TRAIL_GIVEBACK_PCT / 100.0
                candidate = pos.max_px * (1.0 - gb)
                if candidate > pos.trail_stop_px:
                    print(f"[*] Ratcheting trail for {pos.symbol} from ${pos.trail_stop_px:.8f} to ${candidate:.8f}.")
                    pos.trail_stop_px = candidate

            if now < pos.hold_armed_at:
                continue

            if pnl_pct >= TAKE_PROFIT_PCT: 
                print(f"[+] TP triggered for {pos.symbol} ({mint[:6]}) at {pnl_pct:.2f}%")
                self._close(pos, px, "TP", pnl_pct); continue
            if pnl_pct <= -STOP_LOSS_PCT: 
                print(f"[!] SL triggered for {pos.symbol} ({mint[:6]}) at {pnl_pct:.2f}%")
                self._close(pos, px, "SL", pnl_pct); continue
            if pos.be_armed and pos.be_stop_px and px <= pos.be_stop_px: 
                print(f"[!] BE stop triggered for {pos.symbol} ({mint[:6]})")
                self._close(pos, px, "BE", pnl_pct); continue
            if pos.trail_armed and pos.trail_stop_px and px <= pos.trail_stop_px: 
                print(f"[!] Trail stop triggered for {pos.symbol} ({mint[:6]})")
                self._close(pos, px, "TRAIL", pnl_pct); continue
            
            held_min = (now - pos.entry_ts) / 60.0
            if held_min >= MAX_HOLD_MIN: 
                print(f"[!] Time stop triggered for {pos.symbol} ({mint[:6]}) after {held_min:.1f}m")
                self._close(pos, px, "TIME", pnl_pct); continue
            
            if not pos.time_warning_sent and held_min >= (MAX_HOLD_MIN * 0.9):
                pos.time_warning_sent = True
                tg_send(f"⌛ Nearing Time Stop for {pos.symbol} ({pos.mint[:6]}…)\nOpen for {held_min:.1f}m. PnL {pnl_pct:.2f}%.")

            save_open_pos(pos)

    def _close(self, pos: Position, px: float, reason: str, pnl_pct: float):
        print(f"[!] Closing position: {pos.symbol} ({pos.mint[:6]}) - Reason: {reason}")
        
        direction = -1
        slip = (SLIPPAGE_BPS / 10000.0) * direction
        fee = FEES_BPS / 10000.0
        fill_px = px * (1 + slip) * (1 + fee)
        final_pnl_pct = (fill_px - pos.entry_px) / pos.entry_px * 100.0 if pos.entry_px > 0 else 0.0

        exit_rec = {
            "type": "EXIT", "id": pos.id, "mint": pos.mint, "symbol": pos.symbol, "ts": _now(),
            "entry_px": pos.entry_px, "exit_px": fill_px, "qty": pos.qty,
            "pnl_pct": final_pnl_pct, "reason": reason, "note": pos.note
        }
        with open(TRADES_OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(exit_rec) + "\n")
            
        tg_send(f"🔴 EXIT (paper) {pos.symbol} ({pos.mint[:6]}…)\n{pos.entry_px:.8f} → {fill_px:.8f} | {reason} | PnL {final_pnl_pct:.2f}%")
        
        self._last_exit_ts_by_mint[pos.mint] = _now()
        clear_open_pos(pos.mint)
        if self.ws:
            self.ws.unsubscribe(pos.mint)
        
        # Double-check position exists before deleting
        if pos.mint in self.positions:
            del self.positions[pos.mint]
            print(f"[+] Successfully removed {pos.symbol} ({pos.mint[:6]}) from positions dict")
        else:
            print(f"[!] Warning: {pos.symbol} ({pos.mint[:6]}) not found in positions dict during close")
        
        update_equity_csv()
        send_global_pnl_summary()

    def drain_ws(self):
        if not self.ws: return
        drained = 0
        while not self.ws.out_q.empty() and drained < 200:
            try:
                mint, px, ts = self.ws.out_q.get_nowait()
                record_live_price(mint, px, ts)
                drained += 1
            except Empty:
                break
        if drained > 0:
            print(f"[*] Drained {drained} price updates from WebSocket.")

    def check_system_health(self):
        now = _now()
        one_min_ago = now - 60
        recent_fallbacks = sum(1 for ts in _REST_FALLBACKS if ts > one_min_ago)
        if recent_fallbacks > 5 and (now - _LAST_HEALTH_ALERT_TS.get("ws", 0)) > 300:
            msg = f"🔌 Price Feed Warning: High REST API fallbacks ({recent_fallbacks}/60s). Check WS."
            print(f"[!] {msg}")
            tg_send(msg)
            _LAST_HEALTH_ALERT_TS["ws"] = now
        _REST_FALLBACKS[:] = [ts for ts in _REST_FALLBACKS if ts > one_min_ago]

        backlog_size = len(self.pending_scenarios)
        if backlog_size >= QUEUE_BACKLOG_THRESHOLD and (now - _LAST_HEALTH_ALERT_TS.get("queue", 0)) > 300:
            msg = f"📥 Queue Backlog Warning: {backlog_size} scenarios pending."
            print(f"[!] {msg}")
            tg_send(msg)
            _LAST_HEALTH_ALERT_TS["queue"] = now

def main():
    ws = None
    if BIRDEYE_WS_ON:
        if not BIRDEYE_WS_API_KEY:
            print("[!] BIRDEYE_WS_ON is 1, but BIRDEYE_WS_API_KEY is not set. WebSocket will not start.")
        else:
            ws = BirdeyeWS(BIRDEYE_WS_URL, BIRDEYE_WS_API_KEY, BIRDEYE_WS_PING_SEC)
            ws.start()
    
    executor = TradeExecutor(ws)
    print("🤖 Starting Trade Executor...")
    tg_send("🤖 Trade Executor — start")
    
    try:
        executor.run()
    except KeyboardInterrupt:
        print("\n[!] Keyboard interrupt detected. Shutting down...")
    finally:
        if ws:
            ws.stop()
        print("✅ Executor has stopped.")
        tg_send("✅ Trade Executor complete")

if __name__ == "__main__":
    main()