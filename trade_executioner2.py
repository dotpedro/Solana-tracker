# trade_executioner2.py
# Live executor with OG price engine (WS + REST fallbacks) and Jupiter swaps.
# - Entries: BUY USDC -> token via Jupiter.
# - Exits/partials: SELL token -> USDC via Jupiter.
# - Continuous price monitoring: Birdeye WS + REST fallbacks.
# - Safety: LIVE_MODE guard, SOL fee buffer, USDC clamp, quick confirm loop.

from __future__ import annotations
import os, json, time, base64, math, traceback, ssl, statistics, asyncio
import csv  # <-- ADD THIS (TE2 already imports statistics)

from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, Set
from threading import Thread, Event, Lock
from queue import Queue, Empty

#import fcntl  # File locking for POSIX systems

import requests
from websocket import WebSocketApp
from dotenv import load_dotenv
load_dotenv(override=True)


# === Token balance helpers & thresholds ===
DUST_UNITS = float(os.getenv("DUST_UNITS", "0.000001"))

# ===== Position file locks =====
_pos_locks: Dict[str, Lock] = {}

def _get_pos_lock(mint: str) -> Lock:
    """Get or create a lock for a specific mint's position file."""
    if mint not in _pos_locks:
        _pos_locks[mint] = Lock()
    return _pos_locks[mint]

def _read_position(mint: str) -> Optional[dict]:
    """Safely read position file with locking."""
    ppath = _open_pos_path(mint)
    if not os.path.exists(ppath):
        return None
    
    lock = _get_pos_lock(mint)
    with lock:
        try:
            with open(ppath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            tprint(f"[POS] read error {mint[:6]}: {e}")
            return None

def _write_position(mint: str, pos: dict) -> bool:
    """Safely write position file with locking and atomic write."""
    ppath = _open_pos_path(mint)
    lock = _get_pos_lock(mint)
    
    with lock:
        try:
            # Atomic write: write to temp file, then rename
            temp_path = f"{ppath}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(pos, f, indent=2)
            os.replace(temp_path, ppath)  # Atomic on POSIX
            return True
        except Exception as e:
            tprint(f"[POS] write error {mint[:6]}: {e}")
            return False

# ===== Telegram (alerts only) =====
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ON        = int(os.getenv("TELEGRAM_ON", "1"))
OPEN_POS_DIR = os.getenv("OPEN_POS_DIR", "open_positions")
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
ECO_FLAG = os.getenv("ECO_FLAG", "eco_on.flag")   # optional: shared flag file

# --- OG-style SL / Trail / BE / Time Stops (env-tunable) ---
DISASTER_SL_PCT    = float(os.getenv("DISASTER_SL_PCT",    "20.0"))   # hard emergency stop
STOP_LOSS_PCT      = float(os.getenv("STOP_LOSS_PCT",      "7.0"))   # regular stop loss
BREAK_EVEN_ARM_PCT = float(os.getenv("BREAK_EVEN_ARM_PCT", "10.0"))   # arm BE after this gain
TRAIL_ARM_PCT      = float(os.getenv("TRAIL_ARM_PCT",      "20.0"))   # start trailing after this gain
TRAIL_GIVEBACK_PCT = float(os.getenv("TRAIL_GIVEBACK_PCT", "35.0"))   # % giveback from peak
TIME_STOP_MIN      = float(os.getenv("TIME_STOP_MIN",      "90"))     # max hold (minutes)
TAKE_PROFIT_PCT    = float(os.getenv("TAKE_PROFIT_PCT",    "20.0"))  # optional static TP

# --- Retry constants (tweak to taste) ---
RETRY_MAX_DEVIATION_PCT = 2.0     # only retry if |current - trigger|/trigger * 100 <= 2%
RETRY_MIN_DELAY_SEC     = 8       # minimum gap between retries
RETRY_BACKOFF_MULT      = 1.6     # exponential backoff multiplier on each attempt
RETRY_MAX_ATTEMPTS      = 6       # cap retries so we don't spam forever

def _should_retry_now(cm: dict) -> bool:
    attempts = int(cm.get("attempts") or 0)
    last_ts  = float(cm.get("last_attempt_ts") or 0)
    required_gap = RETRY_MIN_DELAY_SEC * (RETRY_BACKOFF_MULT ** attempts)
    return (time.time() - last_ts) >= required_gap

def _price_within_deviation(trigger_px: float, current_px: float, max_dev_pct: float) -> bool:
    if trigger_px <= 0 or current_px <= 0:
        return False
    dev = abs(current_px - trigger_px) / trigger_px * 100.0
    return dev <= max_dev_pct

def retry_closing_positions():
    """
    Re-attempts closing if:
      - The position is marked closing=True,
      - The on-chain balance > 0,
      - Price is within ±2% of the original trigger price,
      - Backoff window passed,
      - Attempts < max.
    """
    for fn in os.listdir(OPEN_POS_DIR):
        if not fn.endswith(".json"):
            continue

        mint = fn[:-5]  # strip .json
        pos = _read_position(mint)
        if not pos:
            continue
        try:
            

            if not pos.get("closing"):
                continue

            mint = pos["mint"]
            note = pos.get("note") or {}
            last_px = float(note.get("last_px") or 0)
            cm = pos.get("closing_meta") or {}
            trigger_px = float(cm.get("trigger_px") or 0)
            attempts   = int(cm.get("attempts") or 0)

            # check balance
            try:
                bal = _get_token_balance_by_mint(RPC_URL, OWNER_PUBKEY, mint)
            except Exception as e:
                tprint(f"[RT] {mint[:6]} balance check failed: {e}")
                bal = None

            # finalize if empty
            if bal is not None and bal <= (globals().get("DUST_UNITS", 0.000001)):
                tprint(f"[RT] {mint[:6]} zero balance -> finalize close")
                _mark_closed(mint)
                continue

            if attempts >= RETRY_MAX_ATTEMPTS:
                tprint(f"[RT] {mint[:6]} max retry attempts reached ({attempts})")
                continue

            if not _should_retry_now(cm):
                continue

            if not _price_within_deviation(trigger_px, last_px, RETRY_MAX_DEVIATION_PCT):
                tprint(f"[RT] {mint[:6]} skip retry: price drift > {RETRY_MAX_DEVIATION_PCT}%")
                continue

            reason = f"retry_close_attempt_{attempts+1}"
            _append_jsonl(EXEC_CONTROL, {
                "type": "FORCE_CLOSE",
                "ts": _now(),
                "mint": mint,
                "reason": reason
            })

            cm["attempts"] = attempts + 1
            cm["last_attempt_ts"] = time.time()
            pos["closing_meta"] = cm
            _write_position(mint, pos)

            

            tprint(f"[RT] {mint[:6]} re-enqueued close (#{attempts+1}) "
                   f"@ ~{last_px:.8f} (trigger {trigger_px:.8f})")

        except Exception as e:
            tprint(f"[RT] error {fn}: {e}")


def _open_pos_path(mint: str) -> str:
    return os.path.join(OPEN_POS_DIR, f"{mint}.json")

def _mark_open(mint: str, symbol: str, size_usd: float, entry_px: float, note: Optional[dict] = None):
    """Thread-safe position creation."""
    try:
        meta = dict(note or {})
        meta.setdefault("last_px", float(entry_px))
        meta["last_ts"] = _now()
        pos = {
            "id": f"{mint}_{int(time.time())}",
            "mint": mint,
            "symbol": symbol,
            "entry_ts": int(time.time()),
            "entry_px": float(entry_px),
            "qty": 0.0,
            "size_usd": float(size_usd),
            "max_px": float(entry_px),
            "be_armed": False,
            "trail_armed": False,
            "hold_armed_at": 0,
            "be_stop_px": None,
            "trail_stop_px": None,
            "note": meta,
            "time_warning_sent": False,
        }
        _write_position(mint, pos)
    except Exception as e:
        tprint(f"[slots] failed to mark open for {symbol} ({mint[:6]}): {e}")


def _mark_closed(mint: str):
    """Free a slot by removing the json file for this mint."""
    try:
        p = _open_pos_path(mint)
        if os.path.exists(p):
            os.remove(p)
    except Exception as e:
        tprint(f"[slots] failed to mark closed for {mint[:6]}: {e}")


def _update_open_pos_price(mint: str, price: float):
    """Thread-safe price update using lock."""
    pos = _read_position(mint)
    if not pos:
        return
    
    note = pos.setdefault("note", {})
    note["last_px"] = float(price)
    note["last_ts"] = _now()
    pos["max_px"] = max(float(pos.get("max_px") or 0.0), float(price))
    
    _write_position(mint, pos)




def tg_send(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:3900], "disable_web_page_preview": True},
            timeout=8
        )
    except Exception:
        pass


# ===== PnL & Equity Reporting (OG-style) =====
initial_equity = 100.0  # same default as OG

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
    if not pnls and all_exits:
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

def send_global_pnl_summary(sync_wallet_sizer: bool = True):
    """
    Sends Telegram summary that shows LIVE wallet equity.
    Optionally writes it to bankroll_state.json for wallet_sizer to consume.
    """
    stats = compute_global_stats()             # historical stats from EXITs
    live_equity = compute_live_wallet_equity_usd()

    # Optional: persist for wallet_sizer
    if sync_wallet_sizer:
        try:
            path = os.getenv("BANKROLL_STATE_PATH", "bankroll_state.json")
            state = {}
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        state = json.load(f) or {}
                except Exception:
                    state = {}
            state["current_equity"] = round(live_equity, 6)
            state["updated_ts"] = _now()
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f)
        except Exception as e:
            tprint(f"[equity] bankroll_state write failed: {e}")

    # Build message
    all_exits = get_all_trades_from_log()
    last_5_pnls = [t.get('pnl_pct', 0.0) for t in all_exits[-5:]]

    msg = (
        "📊 Overall PnL Summary:\n"
        f"Total Trades: {stats['trades']}\n"
        f"Win Rate: {stats['winrate_pct']}%\n"
        f"Average PnL per Trade: {stats['avg_pnl_pct']}%\n"
        f"Total PnL Sum: {stats['total_pnl_sum_pct']}%\n"
        f"Current Equity (WALLET): ${live_equity:.2f}\n"
        f"Max Drawdown (from trade history): {stats['max_dd_pct']}%\n"
        f"Recent Trades (last 5 PnLs): "
        f"{', '.join(f'{p:.2f}%' for p in last_5_pnls) if last_5_pnls else 'None'}"
    )

    tg_send(msg)


def update_equity_csv():
    """
    Writes equity_curve.csv using LIVE wallet equity (USDC + MTM of opens).
    Keeps the rest of the stats from compute_global_stats() for context.
    """
    stats = compute_global_stats()  # still useful for trade counts & winrate
    live_equity = compute_live_wallet_equity_usd()

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    row = [now_str, round(live_equity, 4), stats['trades'], stats['winrate_pct']]

    csv_path = os.path.join(os.path.dirname(TRADES_OUT), "equity_curve.csv")
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "equity_live_usd", "total_trades", "overall_winrate_pct"])
        writer.writerow(row)



def _slots_full() -> bool:
    try:
        return len([f for f in os.listdir(OPEN_POS_DIR) if f.endswith(".json")]) >= MAX_CONCURRENT_TRADES
    except Exception:
        return False

_prev_slots_full = None
def eco_sync():
    """Detect slot status changes and trigger ECO notifications."""
    global _prev_slots_full
    now_full = _slots_full()
    if _prev_slots_full is None:
        _prev_slots_full = now_full
        if now_full and not os.path.exists(ECO_FLAG):
            open(ECO_FLAG, "w").close()
        elif not now_full and os.path.exists(ECO_FLAG):
            try: os.remove(ECO_FLAG)
            except: pass
        return
    if not _prev_slots_full and now_full:
        tg_send(f"⚠️ Slots full ({MAX_CONCURRENT_TRADES}). Entering ECO mode.")
        open(ECO_FLAG, "w").close()
    elif _prev_slots_full and not now_full:
        tg_send("✅ Slot freed. Exiting ECO mode.")
        try:
            if os.path.exists(ECO_FLAG):
                os.remove(ECO_FLAG)
        except: pass
    _prev_slots_full = now_full

# =====================
# ENV / FILES
# =====================
SCENARIOS_EXEC_OUT = os.getenv("SCENARIOS_EXEC", "scenarios_exec_sized.jsonl")
EXEC_CONTROL   = os.getenv("EXEC_CONTROL", "exec_control.jsonl")
TRADES_OUT     = os.getenv("TRADES_OUT", "trades.jsonl")

LIVE_MODE = os.getenv("LIVE_MODE", "0") == "1"


# Verbose logging toggle for TE2
TE2_VERBOSE = int(os.getenv("TE2_VERBOSE", "1"))
def tprint(*args, **kwargs):
    if TE2_VERBOSE:
        print("[TE2]", *args, **kwargs)


# Wallet / RPC
RPC_URL    = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
KEY_JSON   = os.getenv("PRIVATE_KEY_JSON", "wallet.json")   # path to JSON array file
KEY_B58    = os.getenv("PRIVATE_KEY_BASE58", "")            # OR base58 secret
PUBKEY_B58 = os.getenv("TRADER_PUBLIC_KEY", "")             # optional
OWNER_PUBKEY = PUBKEY_B58                        # cached string; may be empty

def get_owner_pubkey() -> str:
    """Return base58 owner pubkey; resolve lazily so import order doesn't matter."""
    global OWNER_PUBKEY
    if OWNER_PUBKEY:
        return OWNER_PUBKEY
    try:
        # _pubkey() is defined later; this only runs when first called
        OWNER_PUBKEY = str(_pubkey())
    except Exception:
        OWNER_PUBKEY = ""
    return OWNER_PUBKEY

# Jupiter
JUP_USE_PRO  = os.getenv("JUP_USE_PRO", "0") == "1"
JUP_BASE     = "https://api.jup.ag" if JUP_USE_PRO else "https://lite-api.jup.ag"
JUP_API_KEY  = os.getenv("JUP_API_KEY", "")
SLIPPAGE_BPS_LIVE = int(os.getenv("SLIPPAGE_BPS_LIVE", "50"))  # 0.50%
WRAP_UNWRAP_SOL   = os.getenv("WRAP_UNWRAP_SOL", "1") == "1"
DYNAMIC_CU_LIMIT  = os.getenv("DYNAMIC_CU_LIMIT", "1") == "1"
PRIO_LEVEL        = os.getenv("PRIO_LEVEL", "veryHigh")        # low|medium|high|veryHigh
PRIO_LAMPORTS_MAX = int(os.getenv("PRIO_LAMPORTS_MAX", "5000000"))

# Tokens / decimals
USDC_MINT = os.getenv("USDC_MINT_SOL", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
WSOL_MINT = os.getenv("WSOL_MINT_SOL", "So11111111111111111111111111111111111111112")
USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))

# Fee safety
SOL_FEE_BUFFER_SOL   = float(os.getenv("SOL_FEE_BUFFER_SOL", "0.01"))
SOL_MIN_REQUIRED_SOL = float(os.getenv("SOL_MIN_REQUIRED_SOL", "0.003"))

# Loop cadence
LOOP_SLEEP_SEC = float(os.getenv("TE2_LOOP_SLEEP_SEC", "0.20"))

# =====================
# OG Price Engine ENV
# =====================
BIRDEYE_API_KEY   = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_WS_ON     = int(os.getenv("BIRDEYE_WS_ON", "1"))
BIRDEYE_WS_URL    = os.getenv("BIRDEYE_WS_URL", "wss://public-api.birdeye.so/socket/solana")
BIRDEYE_WS_PING_SEC = int(os.getenv("BIRDEYE_WS_PING_SEC", "20"))

# Preferred quotes for OHLCV base/quote
USDC_MINT_SOL = USDC_MINT
SOL_MINT_SOL  = WSOL_MINT
PREFERRED_QUOTES = [USDC_MINT_SOL, SOL_MINT_SOL]

# Outlier/staleness knobs (light defaults; adjust if you want stricter)
MAX_PRICE_STALENESS_SECS = int(os.getenv("MAX_PRICE_STALENESS_SECS", "60"))
OUTLIER_PCT = float(os.getenv("OUTLIER_PCT", "0.15"))

# =====================
# LIVE PRICE CACHE
# =====================
LIVE_PRICE: Dict[str, Dict[str, float]] = {}  # mint -> {"price": float, "ts": int}
_REST_FALLBACKS: List[int] = []               # timestamps of REST fallback usage

def _now() -> int:
    return int(time.time())

def record_live_price(mint: str, px: float, ts: Optional[int] = None):
    if px is None:
        return
    LIVE_PRICE[mint] = {"price": float(px), "ts": float(ts or _now())}

# =====================
# Birdeye WebSocket
# =====================
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
        # permissive headers; Birdeye WS accepts plain connection
        return [
            "Accept: application/json",
            "Pragma: no-cache",
            "Cache-Control: no-cache",
        ]

    def _on_open(self, ws):
        print("[ws] Connected")
        if self.subscribed:
            subs = [{"address": m} for m in self.subscribed]
            msg = json.dumps({"type": "subscribeToken", "data": subs})
            ws.send(msg)
            print(f"[ws] Resubscribed {len(self.subscribed)} mints.")

    def _on_message(self, ws, message):
        """
        Accepts multiple Birdeye payload shapes, e.g.:
          { "type": "subscribeToken", "data": [ { "address": "...", "value": 0.123 } ] }
          { "type": "token", "data": { "address": "...", "value": 0.123 } }
          { "type": "PRICE_DATA", "data": { "address": "...", "c": 0.123, "unixTime": 1712345678 } }
        Writes each tick to out_q and updates open_positions via _update_open_pos_price().
        """
        try:
            msg = json.loads(message or "{}")
            mtype = msg.get("type")
            data = msg.get("data")
            ts = _now()

            def _px_from(obj: dict):
                if not isinstance(obj, dict):
                    return None
                for k in ("value", "price", "c", "close", "p"):
                    v = obj.get(k)
                    if v is None:
                        continue
                    try:
                        return float(v)
                    except Exception:
                        continue
                return None

            # Initial snapshot after (re)subscribe: list of {address, value}
            if mtype == "subscribeToken" and isinstance(data, list):
                for it in data:
                    addr = (it.get("address") or "").strip()
                    px = _px_from(it)
                    if addr and px is not None:
                        self.out_q.put((addr, px, ts))
                        _update_open_pos_price(addr, px)

            # Streaming updates (support several server labels)
            elif mtype in ("token", "PRICE_DATA", "tick", "update") and isinstance(data, dict):
                addr = (data.get("address") or "").strip()
                # Some PRICE_DATA frames include explicit timestamp fields
                ts = int(data.get("unixTime") or data.get("t") or ts)
                px = _px_from(data)
                if addr and px is not None:
                    self.out_q.put((addr, px, ts))
                    _update_open_pos_price(addr, px)

        except Exception as e:
            print(f"[ws] parse error: {e}")

    def _on_error(self, ws, error):
        print(f"[ws] Error: {error}")

    def _on_close(self, ws, code, reason):
        print(f"[ws] Closed: {code} {reason}")

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_ev.clear()

        def run():
            backoff = 1.0
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
                    self.ws.run_forever(
                        sslopt={"cert_reqs": ssl.CERT_NONE},
                        ping_interval=self.ping_sec,
                    )
                except Exception as e:
                    print(f"[ws] run_forever error: {e}")
                if self.stop_ev.is_set():
                    break
                time.sleep(backoff)
                backoff = min(30.0, backoff * 2)

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
        mint = (mint or "").strip()
        if not mint or mint in self.subscribed:
            return
        self.subscribed.add(mint)
        if self.ws:
            try:
                self.ws.send(json.dumps({"type": "subscribeToken", "data": [{"address": mint}]}))
            except Exception as e:
                print(f"[ws] subscribe defer: {e}")

    def unsubscribe(self, mint: str):
        # optional; Birdeye supports unsubscribe message types
        mint = (mint or "").strip()
        if not mint or mint not in self.subscribed:
            return
        self.subscribed.discard(mint)
        if self.ws:
            try:
                self.ws.send(json.dumps({"type": "unsubscribeToken", "data": [{"address": mint}]}))
            except Exception as e:
                print(f"[ws] unsubscribe defer: {e}")


# Drain WS queue into LIVE_PRICE
def drain_ws(ws: Optional[BirdeyeWS]):
    if not ws: return
    drained = 0
    while not ws.out_q.empty() and drained < 500:
        try:
            mint, px, ts = ws.out_q.get_nowait()
            record_live_price(mint, px, ts)
            drained += 1
        except Empty:
            break

# =====================
# REST Fallbacks
# =====================
@dataclass
class Bar:
    t: int
    p: float

def fetch_birdeye_spot_price(mint: str) -> Optional[float]:
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
                return float(px)
    except Exception:
        pass
    return None

def fetch_birdeye_1m_bars(mint: str, minutes: int) -> List[Bar]:
    if not BIRDEYE_API_KEY:
        return []
    bars: List[Bar] = []
    for quote in PREFERRED_QUOTES:
        time_from, time_to = _now() - minutes*60, _now()
        url = "https://public-api.birdeye.so/defi/ohlcv/base_quote"
        params = {"base_address": mint, "quote_address": quote, "type":"1m",
                  "time_from": time_from, "time_to": time_to}
        headers = {"accept":"application/json", "x-chain":"solana", "X-API-KEY": BIRDEYE_API_KEY}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if r.status_code == 200:
                items = ((r.json() or {}).get("data") or {}).get("items") or []
                for it in items:
                    ts = it.get("unixTime") or 0
                    cl = it.get("close") or 0.0
                    if ts and cl:
                        bars.append(Bar(int(ts), float(cl)))
        except Exception:
            pass
    bars.sort(key=lambda b: b.t)
    return bars

def fetch_dexscreener_price(mint: str) -> Optional[float]:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        r = requests.get(url, timeout=4)
        if r.status_code == 200:
            pairs = (r.json() or {}).get("pairs") or []
            if not pairs:
                return None
            pairs.sort(key=lambda p: float(((p.get("liquidity") or {}).get("usd") or 0.0)), reverse=True)
            px_str = pairs[0].get("priceUsd")
            if px_str:
                return float(px_str)
    except Exception:
        pass
    return None

def get_price(mint: str) -> Optional[float]:
    """WS → Birdeye Spot → Birdeye OHLCV → Dexscreener (and update cache)."""
    d = LIVE_PRICE.get(mint)
    if d and _now() - d["ts"] < MAX_PRICE_STALENESS_SECS:
        return float(d["price"])

    _REST_FALLBACKS.append(_now())

    # 1) Birdeye spot
    px = fetch_birdeye_spot_price(mint)
    if px:
        record_live_price(mint, px, _now())
        
        return px

    # 2) Birdeye OHLCV (use last close)
    bars = fetch_birdeye_1m_bars(mint, minutes=5)
    if bars:
        price = bars[-1].p
        ts    = bars[-1].t
        record_live_price(mint, price, ts)
        
        return price

    # 3) Dexscreener
    px = fetch_dexscreener_price(mint)
    if px:
        record_live_price(mint, px, _now())
        
        return px

    return None

# =====================
# IO helpers (tailers)
# =====================
def _touch(p: str):
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8"): pass

_touch(SCENARIOS_EXEC_OUT); _touch(EXEC_CONTROL); _touch(TRADES_OUT)

class Tailer:
    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        _touch(path)
    def read_new(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                f.seek(self.pos)
                for line in f:
                    s = line.strip()
                    if not s: continue
                    try: out.append(json.loads(s))
                    except json.JSONDecodeError:
                        print(f"[tailer] WARN: skipping malformed line in {self.path}")
                self.pos = f.tell()
        except FileNotFoundError:
            _touch(self.path); self.pos = 0
            if out:
                tprint(f"tailer: read {len(out)} new line(s) from {self.path}")
        return out

def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")




def step_positions():
    """
    Scan open positions, update arms, and enqueue exactly ONE FORCE_CLOSE per mint
    when a stop/TP condition hits by marking the position as 'closing'.
    """
    for fn in os.listdir(OPEN_POS_DIR):
        if not fn.endswith(".json"):
            continue

        mint = fn[:-5]  # strip .json
        pos = _read_position(mint)
        if not pos:
            continue

        try:
            

            # Skip if a close is already in-flight
            if pos.get("closing"):
                continue

            mint     = pos["mint"]
            note     = pos.get("note") or {}
            entry_px = float(pos.get("entry_px", 0) or 0)
            last_px  = float(note.get("last_px") or 0)
            max_px   = float(pos.get("max_px") or (entry_px or 0))
            age_min  = (time.time() - float(pos.get("entry_ts", time.time()))) / 60.0

            if entry_px <= 0 or last_px <= 0:
                continue

            pnl_pct = (last_px / entry_px - 1.0) * 100.0

            # Small helper to persist 'closing' and enqueue a single FORCE_CLOSE
            def _enqueue_close(reason: str):
                pos["closing"] = True
                cm = pos.setdefault("closing_meta", {})
                cm.setdefault("trigger_px", last_px) 
                cm.setdefault("attempts", 0) 
                cm.setdefault("last_attempt_ts", 0.0) 
                pos["closing_reason"] = reason
                
                if not _write_position(mint, pos):
                    tprint(f"[SL] {mint[:6]} failed to mark closing")
                    return
                
                _append_jsonl(EXEC_CONTROL, {
                    "type": "FORCE_CLOSE",
                    "ts": _now(),
                    "mint": mint,
                    "reason": reason
                })

            # === 1) DISASTER STOP ===
            if pnl_pct <= -DISASTER_SL_PCT:
                tprint(f"[SL] {mint[:6]} DISASTER STOP {pnl_pct:.1f}%")
                _enqueue_close(f"disaster_sl_{pnl_pct:.1f}%")
                continue

            # === 2) REGULAR STOP LOSS ===
            if pnl_pct <= -STOP_LOSS_PCT:
                tprint(f"[SL] {mint[:6]} STOP LOSS {pnl_pct:.1f}%")
                _enqueue_close(f"stop_loss_{pnl_pct:.1f}%")
                continue

            # === 3) BREAK EVEN ARM / STOP ===
            needs_write = False
            if pnl_pct >= BREAK_EVEN_ARM_PCT and not pos.get("be_armed"):
                pos["be_armed"] = True
                pos["be_stop_px"] = entry_px
                tprint(f"[SL] {mint[:6]} BE armed at {entry_px:.6f}")
                needs_write = True

            if pos.get("be_armed") and last_px <= float(pos.get("be_stop_px") or 0):
                tprint(f"[SL] {mint[:6]} BE STOP hit")
                _enqueue_close("break_even_stop")
                continue

            # === 4) TRAILING STOP ===
            if pnl_pct >= TRAIL_ARM_PCT and not pos.get("trail_armed"):
                pos["trail_armed"] = True
                tprint(f"[SL] {mint[:6]} Trail armed at +{pnl_pct:.1f}%")
                needs_write = True

            if pos.get("trail_armed"):
                # Update max from peak
                if last_px > max_px:
                    pos["max_px"] = last_px
                    max_px = last_px  # sync local
                    needs_write = True
                giveback = 100.0 * (1.0 - (last_px / max_px if max_px > 0 else 1.0))
                if giveback >= TRAIL_GIVEBACK_PCT:
                    tprint(f"[SL] {mint[:6]} Trail STOP {giveback:.1f}% from peak")
                    _enqueue_close(f"trail_stop_{giveback:.1f}%_from_peak")
                    continue

            # === 5) TIME STOP ===
            if age_min >= TIME_STOP_MIN:
                tprint(f"[SL] {mint[:6]} TIME STOP {age_min:.1f}m")
                _enqueue_close(f"time_stop_{age_min:.1f}m")
                continue

            # === 6) TAKE PROFIT ===
            if pnl_pct >= TAKE_PROFIT_PCT:
                tprint(f"[SL] {mint[:6]} TAKE PROFIT +{pnl_pct:.1f}%")
                _enqueue_close(f"take_profit_{pnl_pct:.1f}%")
                continue

            # Persist any arm updates that happened without triggering a stop
            if needs_write:
                _write_position(mint, pos)

        except Exception as e:
            tprint(f"[SL] error {fn}: {e}")


def _err_str(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"

# =====================
# Solana RPC / signing
# =====================
# Pylance may warn on Windows; runtime is fine with proper installs.
from solders.keypair import Keypair  # type: ignore
from solders.pubkey import Pubkey    # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore
from solders.signature import Signature  # type: ignore

from solana.rpc.api import Client  # type: ignore
from solana.rpc.types import TxOpts  # type: ignore
from solana.rpc.commitment import Confirmed  # type: ignore

_client: Optional[Client] = None
def _rpc() -> Client:
    global _client
    if _client is None:
        _client = Client(RPC_URL, timeout=20)
    return _client

def _load_keypair() -> Keypair:
    if KEY_B58.strip():
        return Keypair.from_base58_string(KEY_B58.strip())
    with open(KEY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return Keypair.from_bytes(bytes(data))
    sk = data.get("secretKey")
    if isinstance(sk, str):
        return Keypair.from_base58_string(sk)
    raise RuntimeError("Invalid PRIVATE_KEY_*; set PRIVATE_KEY_BASE58 or PRIVATE_KEY_JSON (array).")

def _pubkey() -> Pubkey:
    return _load_keypair().pubkey()

def _sol_balance_sol(owner: Pubkey) -> float:
    r = _rpc().get_balance(owner)
    return (r.value or 0)/1_000_000_000

def _token_balance_raw(owner: Pubkey, mint_b58: str) -> Tuple[int, int]:
    """
    FIXED: Use raw JSON-RPC to avoid solana-py API version issues.
    Matches wallet_sizer.py approach for consistent balance checks.
    """
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                str(owner),
                {"mint": mint_b58},
                {"encoding": "jsonParsed"}
            ]
        }
        r = requests.post(RPC_URL, json=payload, timeout=10)
        r.raise_for_status()
        result = r.json().get("result", {})
        
        total_raw, decimals = 0, 0
        for acc in result.get("value", []):
            try:
                info = acc["account"]["data"]["parsed"]["info"]
                ta = info["tokenAmount"]
                total_raw += int(ta["amount"])
                decimals = int(ta.get("decimals", decimals))
            except Exception:
                pass
        return total_raw, decimals
    except Exception as e:
        tprint(f"_token_balance_raw error for {mint_b58[:6]}: {e}")
        return 0, 0
    
def _get_token_balance_by_mint(rpc_url: str, owner_pubkey_b58: str, mint_b58: str) -> float:
    """
    Return UI token amount for owner & SPL mint by reusing _token_balance_raw().
    rpc_url is unused; kept for signature compatibility.
    """
    try:
        # If caller passed empty string, fall back to the loaded key
        owner = _pubkey() if not owner_pubkey_b58 else Pubkey.from_string(owner_pubkey_b58)
        raw, dec = _token_balance_raw(owner, mint_b58)
        if dec <= 0:
            return 0.0
        return raw / (10 ** dec)
    except Exception:
        return 0.0


def _usdc_balance(owner: Pubkey) -> float:
    raw, dec = _token_balance_raw(owner, USDC_MINT)
    if dec <= 0: 
        dec = USDC_DECIMALS
    return raw / (10 ** dec)

def compute_live_wallet_equity_usd() -> float:
    """
    Exact wallet portfolio value in USD (no TM involved):
      equity = USDC wallet balance
             + sum over open positions of mark-to-market value
      MTM for a position uses the entry cost ('size_usd') scaled by price ratio:
        value_usd = size_usd * (last_px / entry_px)
      This avoids needing on-chain token qty and stays consistent with your logs.
    """
    # 1) USDC in wallet
    owner = _pubkey()
    usdc_cash = _usdc_balance(owner)

    # 2) Open positions MTM
    mtm_total = 0.0
    try:
        for fn in os.listdir(OPEN_POS_DIR):
            if not fn.endswith(".json"):
                continue
            with open(os.path.join(OPEN_POS_DIR, fn), "r", encoding="utf-8") as f:
                pos = json.load(f)
            size_usd  = float(pos.get("size_usd") or 0.0)
            entry_px  = float(pos.get("entry_px") or 0.0)
            last_px   = float(((pos.get("note") or {}).get("last_px")) or 0.0)
            if size_usd > 0 and entry_px > 0 and last_px > 0:
                mtm_total += size_usd * (last_px / entry_px)
            elif size_usd > 0:
                # Fall back to cost if we don't have a fresh price yet
                mtm_total += size_usd
    except Exception as e:
        tprint(f"[equity] error computing MTM: {e}")

    return float(usdc_cash + mtm_total)


# =====================
# Jupiter client (/swap/v1)
# =====================
def _jup_headers() -> Dict[str, str]:
    h = {"Content-Type": "application/json"}
    # Pro uses Bearer; Lite needs nothing
    if JUP_USE_PRO and JUP_API_KEY:
        h["authorization"] = f"Bearer {JUP_API_KEY}"
    return h

def jup_quote(input_mint: str, output_mint: str, amount_raw: int, slippage_bps: int = SLIPPAGE_BPS_LIVE) -> dict:
    url = f"{JUP_BASE}/swap/v1/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount_raw,
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "asLegacyTransaction": "false",
        "restrictIntermediateTokens": "true"
    }
    r = requests.get(url, params=params, headers=_jup_headers(), timeout=15)
    r.raise_for_status()
    return r.json()

def jup_build_swap(user_pubkey: Pubkey, quote: dict) -> dict:
    """
    Build Jupiter /swap body. On Lite, omit fee fields entirely.
    On Pro, use numeric autoMultiplier + optional CU price cap.
    If a 422 happens, retry once with the most minimal body.
    """
    url = f"{JUP_BASE}/swap/v1/swap"

    # Common body
    body = {
        "userPublicKey": str(user_pubkey),
        "quoteResponse": quote,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "dynamicComputeUnitLimit": DYNAMIC_CU_LIMIT,
    }

    if JUP_USE_PRO:
        # Pro: use numeric autoMultiplier and optional CU price cap
        prio_multiplier = {"low": 0.5, "medium": 1.0, "high": 1.5, "veryHigh": 2.0}.get(PRIO_LEVEL, 1.0)
        body["prioritizationFeeLamports"] = {"autoMultiplier": prio_multiplier}
        body["computeUnitPriceMicroLamports"] = min(PRIO_LAMPORTS_MAX, 5_000_000)
    else:
        # Lite: DO NOT include fee fields; Lite rejects them
        pass

    # First attempt (preferred body)
    r = requests.post(url, json=body, headers=_jup_headers(), timeout=20)
    if r.status_code == 200:
        return r.json()

    # If Lite or Pro responds 422, retry once with minimal body (no fee fields at all)
    if r.status_code == 422:
        min_body = {
            "userPublicKey": str(user_pubkey),
            "quoteResponse": quote,
            "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
            "dynamicComputeUnitLimit": DYNAMIC_CU_LIMIT,
        }
        r2 = requests.post(url, json=min_body, headers=_jup_headers(), timeout=20)
        if r2.status_code == 200:
            return r2.json()
        # bubble up with the server's message for visibility
        try:
            msg = r2.text
        except Exception:
            msg = "422 from Jupiter on minimal swap body"
        raise RuntimeError(f"swap-build failed after retry: {r2.status_code} {msg}")

    # Any other non-200
    try:
        msg = r.text
    except Exception:
        msg = f"{r.status_code} from Jupiter"
    r.raise_for_status()  # will raise HTTPError with details
    return r.json()

def jup_sign_and_send(swap_resp: dict, keypair: Keypair) -> str:
    """
    Sign and send a Jupiter /swap v1 base64 transaction using solders only.
    """
    raw = base64.b64decode(swap_resp["swapTransaction"])
    unsigned = VersionedTransaction.from_bytes(raw)

    # Sign message bytes with our solders Keypair
    try:
        msg_bytes = unsigned.message.serialize()
    except AttributeError:
        # Fallback if serialize doesn't exist
        try:
            from solders.message import to_bytes_versioned  # type: ignore
            msg_bytes = to_bytes_versioned(unsigned.message)
        except ImportError:
            # Last resort: convert message to bytes directly
            msg_bytes = bytes(unsigned.message)

    sig = keypair.sign_message(msg_bytes)

    # Build signed v0 tx - use populate method instead of constructor
    signed = VersionedTransaction.populate(unsigned.message, [sig])

    # Send + quick confirm
    sig_obj = _rpc().send_raw_transaction(
        bytes(signed),
        opts=TxOpts(skip_preflight=False, max_retries=3, preflight_commitment=Confirmed),
    ).value
    sig_str = str(sig_obj)

    for _ in range(6):
        time.sleep(0.6)
        st = _rpc().get_signature_statuses([Signature.from_string(sig_str)]).value[0]
        if st and (st.confirmation_status is not None):
            break

    return sig_str


# =====================
# Guards & execution
# =====================
def _clamp_usdc_amount(ui_amount_usdc: float) -> float:
    owner = _pubkey()
    sol = _sol_balance_sol(owner)
    if sol < SOL_MIN_REQUIRED_SOL:
        raise RuntimeError("Insufficient SOL for fees.")
    if sol - SOL_FEE_BUFFER_SOL < 0:
        raise RuntimeError("SOL below fee buffer.")
    usdc = _usdc_balance(owner)
    return max(0.0, min(ui_amount_usdc, usdc))

def live_buy_usdc_to_token(mint: str, size_usd: float) -> str:
    """
    Execute a USDC -> TOKEN market buy via Jupiter.
    Returns the transaction signature on success, raises on failure.
    """
    try:
        owner = _pubkey()
        kp    = _load_keypair()

        # --- Visibility: wallet caps before clamping
        sol  = _sol_balance_sol(owner)
        usdc = _usdc_balance(owner)
        clamped = _clamp_usdc_amount(size_usd)

        tprint(
            f"cap check: requested=${size_usd:.2f} | "
            f"wallet usdc={usdc:.4f} | sol={sol:.6f} | "
            f"fee_buf={SOL_FEE_BUFFER_SOL} | min_sol={SOL_MIN_REQUIRED_SOL} | "
            f"clamped=${clamped:.4f}"
        )

        if clamped <= 0:
            # hard stop: insufficient USDC or SOL fee buffer
            raise RuntimeError("Clamped amount is zero (insufficient USDC or SOL for fees)")

        # --- Quote & swap build
        amount_raw = int(round(clamped * (10 ** USDC_DECIMALS)))
        tprint(f"quote {mint[:6]}: USDC→TOKEN amount_raw={amount_raw}")
        quote = jup_quote(USDC_MINT, mint, amount_raw, SLIPPAGE_BPS_LIVE)

        tprint(f"swap-build {mint[:6]}: calling Jupiter /swap")
        swap_resp = jup_build_swap(owner, quote)

        # --- Sign & send
        tprint(f"send {mint[:6]}: signing + sending")
        sig = jup_sign_and_send(swap_resp, kp)
        tprint(f"ok {mint[:6]}: signature {sig[:10]}…")

        return sig

    except Exception as e:
        # Bubble up after logging so caller can record it in trades.jsonl
        tprint(f"error {mint[:6]}: {type(e).__name__}: {e}")
        raise


def live_sell_token_to_usdc(symbol: str, token_mint: str, percent: float = 100.0) -> str:
    if not LIVE_MODE:
        raise RuntimeError("LIVE_MODE=1 not set – refusing to trade real funds.")
    kp = _load_keypair()
    owner = kp.pubkey()

    total_raw, decimals = _token_balance_raw(owner, token_mint)
    if total_raw <= 0:
        raise RuntimeError("No token balance to sell.")
    if decimals <= 0:
        decimals = 6

    sell_raw = int(total_raw * (percent / 100.0))
    if sell_raw <= 0:
        raise RuntimeError("Sell amount zero after percent clamp.")

    quote = jup_quote(token_mint, USDC_MINT, sell_raw, SLIPPAGE_BPS_LIVE)
    swap_resp = jup_build_swap(owner, quote)
    sig = jup_sign_and_send(swap_resp, kp)
    return sig

# =====================
# Logging helpers
# =====================
def _log_entry(mint: str, symbol: str, size_usd: float, sig: Optional[str], err: Optional[str] = None):
    if err:
        _append_jsonl(TRADES_OUT, {
            "type":"ERROR_ENTRY","ts":_now(),"mint":mint,"symbol":symbol,"size_usd":size_usd,"err":err
        })
    else:
        _append_jsonl(TRADES_OUT, {
            "type":"ENTRY","ts":_now(),"mint":mint,"symbol":symbol,"size_usd":size_usd,"mode":"live","sig":sig
        })

def _log_exit(mint: str, symbol: str, percent: float, sig: Optional[str], err: Optional[str] = None):
    if err:
        _append_jsonl(TRADES_OUT, {
            "type":"ERROR_EXIT","ts":_now(),"mint":mint,"symbol":symbol,"percent":percent,"err":err
        })
    else:
        _append_jsonl(TRADES_OUT, {
            "type":"EXIT","ts":_now(),"mint":mint,"symbol":symbol,"percent":percent,"mode":"live","sig":sig
        })

# =====================
# Entry/Exit handlers
# =====================
def process_entry_scenario(scn: Dict[str, Any], ws: Optional[BirdeyeWS]):
    side = (scn.get("side") or "buy").lower()
    mint   = scn.get("mint")
    symbol = scn.get("symbol") or (mint[:6] if mint else "UNK")
    size_usd = float(scn.get("size_usd") or 0.0)

    tprint(f"scenario: side={side} | {symbol} ({(mint or 'UNK')[:6]}) | size_usd={size_usd}")

    # Only buys are handled here
    if side != "buy":
        tprint(f"ignore {symbol}: non-buy side={side}")
        return

    # Must have a mint and positive size (sizer fills size_usd)
    if not mint or size_usd <= 0:
        tprint(f"ignore {symbol}: missing mint or non-positive size (mint={mint}, size_usd={size_usd})")
        return

    # --- HARD GUARDS: slots & duplicate-per-mint ---
    if _slots_full():
        tprint(f"skip {symbol}: slots full ({MAX_CONCURRENT_TRADES})")
        _append_jsonl(TRADES_OUT, {"type":"WARN","ts":_now(),"mint":mint,"symbol":symbol,"msg":"Slots full; skipping entry"})
        eco_sync()
        return

    if os.path.exists(_open_pos_path(mint)):
        tprint(f"skip {symbol}: already open (slot occupied)")
        _append_jsonl(TRADES_OUT, {"type":"WARN","ts":_now(),"mint":mint,"symbol":symbol,"msg":"Mint already open; skipping entry"})
        return

    # Subscribe to live price
    if ws:
        ws.subscribe(mint)
        tprint(f"ws subscribe → {mint[:6]}")

    # Get a usable price (for sanity/logs)
    px = get_price(mint)
    if px is None:
        _append_jsonl(TRADES_OUT, {"type":"WARN","ts":_now(),"mint":mint,"msg":"No price available; skipping entry"})
        tprint(f"skip {symbol}: no price available")
        return

    if not LIVE_MODE:
        _append_jsonl(TRADES_OUT, {"type":"GUARD","ts":_now(),"msg":"Entry ignored because LIVE_MODE=0","scn":scn})
        tprint(f"guard {symbol}: LIVE_MODE=0")
        return

    try:
        # Clamp against wallet (USDC & SOL buffers)
        owner = _pubkey()
        unclamped = size_usd
        clamped = _clamp_usdc_amount(size_usd)
        if clamped <= 0:
            tprint(f"abort {symbol}: clamped amount is zero (usdc/sol cap)")
            _log_entry(mint, symbol, size_usd, sig=None, err="Clamped amount zero")
            return
        if abs(clamped - unclamped) > 1e-9:
            tprint(f"clamp {symbol}: requested=${unclamped:.2f} → clamped=${clamped:.2f}")

        amount_raw = int(round(clamped * (10 ** USDC_DECIMALS)))
        tprint(f"quote {symbol}: USDC→TOKEN amount_raw={amount_raw}")
        quote = jup_quote(USDC_MINT, mint, amount_raw, SLIPPAGE_BPS_LIVE)

        tprint(f"swap-build {symbol}: calling Jupiter /swap")
        swap_resp = jup_build_swap(owner, quote)

        tprint(f"send {symbol}: signing + sending")
        sig = jup_sign_and_send(swap_resp, _load_keypair())
        tprint(f"ok {symbol}: signature {sig[:10]}…")

        # --- OCCUPY THE SLOT immediately on success ---
        meta = (scn.get("meta") or {})

        _mark_open(mint, symbol, clamped, entry_px=px, note=meta)  # <-- now TM can manage it

        eco_sync()

        _log_entry(mint, symbol, clamped, sig=sig)
        try:
            tg_send(f"🟢 ENTRY {symbol} ({mint[:6]}…) size ${clamped:.2f} — tx: {sig}")
        except Exception:
            pass

    except Exception as e:
        err = _err_str(e)
        tprint(f"error {symbol}: {err}")
        _log_entry(mint, symbol, size_usd, sig=None, err=err)



def process_control(cmd: Dict[str, Any]):
    ctype  = (cmd.get("type") or "").upper()
    mint   = cmd.get("mint")
    symbol = cmd.get("symbol") or (mint[:6] if mint else "UNK")
    if not ctype or not mint:
        return

    if not LIVE_MODE:
        _append_jsonl(TRADES_OUT, {"type":"GUARD","ts":_now(),"msg":"Control ignored because LIVE_MODE=0","cmd":cmd})
        return

    try:
        if ctype == "FORCE_CLOSE":
            # Execute full exit on-chain
            sig = live_sell_token_to_usdc(symbol, mint, percent=100.0)

            # --- Read position file to compute PnL% like OG TE ---
            entry_px = 0.0
            exit_px  = 0.0
            pos = _read_position(mint)
            if pos:
                entry_px = float(pos.get("entry_px") or 0.0)
                exit_px  = float(((pos.get("note") or {}).get("last_px")) or 0.0)

            pnl_pct = (exit_px - entry_px) / entry_px * 100.0 if entry_px > 0 and exit_px > 0 else 0.0

            # Log OG-style EXIT row to trades.jsonl (used by summaries)
            _append_jsonl(TRADES_OUT, {
                "type": "EXIT",
                "ts":   _now(),
                "mint": mint,
                "symbol": symbol,
                "entry_px": entry_px,
                "exit_px":  exit_px,
                "pnl_pct":  round(pnl_pct, 4),
                "reason":   "FORCE_CLOSE",
                "mode":     "live",
                "tx":       sig
            })

            # Telegram alert with PnL%
            try:
                tg_send(f"🔴 EXIT {symbol} ({mint[:6]}…) 100%  PnL {pnl_pct:.2f}% — tx: {sig}")
            except Exception:
                pass

            # Free slot + eco
            _mark_closed(mint)
            eco_sync()

            # OG-style equity + global summary
            try:
                update_equity_csv()
                send_global_pnl_summary()
            except Exception:
                pass

        elif ctype == "PARTIAL_CLOSE":
            pct = float(cmd.get("percent") or 50.0)

            # Execute partial exit on-chain
            sig = live_sell_token_to_usdc(symbol, mint, percent=pct)

            # Compute current %PnL for info (partials are not counted in OG summaries)
            entry_px = 0.0
            last_px  = 0.0
            pos = _read_position(mint)
            if pos:
                entry_px = float(pos.get("entry_px") or 0.0)
                last_px  = float(((pos.get("note") or {}).get("last_px")) or 0.0)
            pnl_pct = (last_px - entry_px) / entry_px * 100.0 if entry_px > 0 and last_px > 0 else 0.0

            # Log a PARTIAL row (EXITS summary only counts type=="EXIT")
            _append_jsonl(TRADES_OUT, {
                "type":    "PARTIAL",
                "ts":      _now(),
                "mint":    mint,
                "symbol":  symbol,
                "percent": pct,
                "entry_px": entry_px,
                "mark_px":  last_px,
                "pnl_pct":  round(pnl_pct, 4),
                "mode":     "live",
                "tx":       sig
            })

            # Telegram alert
            try:
                tg_send(f"🟠 PARTIAL {symbol} ({mint[:6]}…) {pct:.0f}%  PnL {pnl_pct:.2f}% — tx: {sig}")
            except Exception:
                pass

            # Keep slot occupied on partial; just sync eco flag
            eco_sync()

        elif ctype == "WITHDRAW":
            _append_jsonl(TRADES_OUT, {"type":"INFO","ts":_now(),"msg":"WITHDRAW not implemented in TE2","cmd":cmd})

        else:
            _append_jsonl(TRADES_OUT, {"type":"WARN","ts":_now(),"msg":"Unknown EXEC_CONTROL command","cmd":cmd})

    except Exception as e:
        _append_jsonl(TRADES_OUT, {
            "type":"ERROR_EXIT",
            "ts":_now(),
            "mint":mint,
            "symbol":symbol,
            "percent":float(cmd.get("percent") or 100.0),
            "err":_err_str(e)
        })

def reconcile_positions(rpc_url: str, owner_pubkey_b58: str):
    """
    Close local 'ghost' slots if wallet has ~0 balance for that mint.
    Adds:
      - PHANTOM_GRACE_SEC: skip phantom cleanup for very recent entries.
      - PHANTOM_CONSEC_ZEROS: require N consecutive zero-balance reads before closing.
    If closing=True and balance is ~0, finalize close immediately (post-exit).
    """
    PHANTOM_GRACE_SEC = int(os.getenv("PHANTOM_GRACE_SEC", "120"))     # default 2 minutes
    PHANTOM_CONSEC_ZEROS = int(os.getenv("PHANTOM_CONSEC_ZEROS", "2")) # require 2 consecutive zeros

    for fn in os.listdir(OPEN_POS_DIR):
        if not fn.endswith(".json"):
            continue
        mint = fn[:-5]  # strip .json
        pos = _read_position(mint)
        if not pos:
                continue
        try:

            # Ensure fields exist
            pos.setdefault("note", {})
            pos.setdefault("phantom_checks", 0)

            # Compute age since entry to apply grace
            entry_ts = int(pos.get("entry_ts") or time.time())
            age_sec = time.time() - entry_ts

            # On-chain balance
            bal = _get_token_balance_by_mint(rpc_url, owner_pubkey_b58, mint)

            # === Finalize real closes quickly ===
            if bal <= DUST_UNITS and pos.get("closing"):
                tprint(f"[RC] {mint[:6]} closing finalized (zero balance)")
                _mark_closed(mint)
                continue

            # === Phantom/empty protection ===
            if bal <= DUST_UNITS:
                # Within settlement grace: do NOT treat as phantom yet
                if age_sec < PHANTOM_GRACE_SEC and not pos.get("closing"):
                    tprint(f"[RC] {mint[:6]} zero bal but within grace ({age_sec:.1f}s) — defer phantom cleanup")
                    # heartbeat only
                    pos["note"]["reconciled_ts"] = time.time()
                    _write_position(mint, pos)
                    continue

                # Outside grace: require consecutive zero reads before closing
                pos["phantom_checks"] = int(pos.get("phantom_checks") or 0) + 1
                if pos["phantom_checks"] >= PHANTOM_CONSEC_ZEROS:
                    tprint(f"[RC] {mint[:6]} phantom/empty for {pos['phantom_checks']} checks → close local")
                    _mark_closed(mint)
                    continue
                else:
                    tprint(f"[RC] {mint[:6]} zero bal check {pos['phantom_checks']}/{PHANTOM_CONSEC_ZEROS} (waiting)")
                    pos["note"]["reconciled_ts"] = time.time()
                    _write_position(mint, pos)
                    continue

            # === Non-zero balance path ===
            # Reset phantom counter and write heartbeat
            if pos.get("phantom_checks"):
                pos["phantom_checks"] = 0
            pos["note"]["reconciled_ts"] = time.time()
            _write_position(mint, pos)

        except Exception as e:
            tprint(f"[RC] error {fn}: {e}")




# =====================
# MAIN
# =====================
def main():
    if not LIVE_MODE:
        print("[TE2] LIVE_MODE=0 -> hard guard enabled; refusing to trade real funds.")
        print("       Set LIVE_MODE=1 in your .env to enable live execution.")
    else:
        print("[TE2] LIVE_MODE=1 -> REAL MONEY MODE ENABLED")

    print(f"[TE2] Reading entries:  {SCENARIOS_EXEC_OUT}")
    print(f"[TE2] Reading controls: {EXEC_CONTROL}")
    print(f"[TE2] Writing logs:     {TRADES_OUT}")
    print(f"[TE2] RPC_URL: {RPC_URL} | Jupiter: {JUP_BASE}")

    # Start WS if enabled
    ws: Optional[BirdeyeWS] = None
    if BIRDEYE_WS_ON and BIRDEYE_API_KEY:
        try:
            ws = BirdeyeWS(BIRDEYE_WS_URL, BIRDEYE_API_KEY, BIRDEYE_WS_PING_SEC)
            ws.start()
            print("[TE2] Birdeye WS started.")
        except Exception as e:
            print(f"[TE2] Birdeye WS failed to start: {e}")

    # Preflight wallet/RPC (latest → recent fallback)
    try:
        pk = _pubkey()
        print(f"[TE2] Wallet loaded: {pk}")

        client = _rpc()
        try:
            client.get_latest_blockhash()
            print("[TE2] RPC latest blockhash OK")
        except Exception:
            client.get_recent_blockhash()
            print("[TE2] RPC blockhash OK (recent)")

        print(f"[TE2] Wallet: {pk}")
        eco_sync()

    except Exception:
        print("[TE2] WARNING: Could not preflight RPC/wallet. Check RPC_URL and PRIVATE_KEY_*.")

    # --- Start background price refresher (additive; non-breaking) ---
    # Uses get_price(mint) which already records LIVE_PRICE and calls _update_open_pos_price.
    # Refresh interval can be tuned via PRICE_REFRESH_SEC env (default 10s).
    os.makedirs(OPEN_POS_DIR, exist_ok=True)
    _price_refresher_stop = Event()

    def _price_refresher_loop():
        print("[TE2] Price refresher started")
        interval = int(os.getenv("PRICE_REFRESH_SEC", "10"))
        while not _price_refresher_stop.is_set():
            try:
                for fname in os.listdir(OPEN_POS_DIR):
                    if not fname.endswith(".json"):
                        continue
                    mint = fname[:-5]  # strip ".json"
                    px = get_price(mint)
                    if px is not None:
                        # get_price already updates LIVE_PRICE and open file via _update_open_pos_price
                        _update_open_pos_price(mint, px)
                        tprint(f"refreshed {mint[:6]}: {px}")
                time.sleep(interval)
            except Exception as e:
                print(f"[TE2] refresher error: {e}")
                time.sleep(5)

    try:
        Thread(target=_price_refresher_loop, daemon=True).start()
    except Exception as e:
        print(f"[TE2] failed to start price refresher: {e}")

    # Tailers (unchanged)
    scn_tail = Tailer(SCENARIOS_EXEC_OUT)
    ctl_tail = Tailer(EXEC_CONTROL)

    # Main loop (unchanged)
    while True:
        try:
            # Drain WS (update LIVE_PRICE)
            drain_ws(ws)

            reconcile_positions(RPC_URL, get_owner_pubkey())

            step_positions()

            retry_closing_positions()                   # price-aware retry within ±2%

            

            # Process entries
            for scn in scn_tail.read_new():
                process_entry_scenario(scn, ws)

            # Process controls
            for cmd in ctl_tail.read_new():
                process_control(cmd)

            time.sleep(LOOP_SLEEP_SEC)
        except KeyboardInterrupt:
            print("\n[TE2] Stopped by user.")
            _price_refresher_stop.set()
            break
        except Exception as e:
            _append_jsonl(TRADES_OUT, {
                "type":"FATAL","ts":_now(),"err":_err_str(e),"trace":traceback.format_exc()
            })
            time.sleep(1.0)



if __name__ == "__main__":
    main()
