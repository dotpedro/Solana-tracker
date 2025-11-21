# exhaustion_monitor.py
# Middle-layer watcher.
# - Consumes watchlist.jsonl (candidates from Bottom Detector).
# - Subscribes to live prices and computes micro-structure confirmations.
# - Promotes candidates to scenarios.jsonl when "GO" criteria are met.
# - Manages relief_registry.json to pause Detector's work on watched mints.

from __future__ import annotations
import os, json, time, uuid, requests, ssl, shutil
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Set
from threading import Thread, Event
from queue import Queue, Empty
from websocket import WebSocketApp
from dotenv import load_dotenv
load_dotenv(override=True)

# =========================
# ENV / KNOBS
# =========================
WATCH_QUEUE = os.getenv("WATCH_QUEUE", "watchlist.jsonl")
EXEC_QUEUE = os.getenv("SCENARIOS_IN", "scenarios.jsonl")
RELIEF_REGISTRY = os.getenv("RELIEF_REGISTRY", "relief_registry.json")
RELIEF_TTL_MIN = int(os.getenv('RELIEF_TTL_MIN', '30'))

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '')

BIRDEYE_API_KEY = os.getenv('BIRDEYE_API_KEY', '')
BIRDEYE_WS_ON = int(os.getenv("BIRDEYE_WS_ON", "1"))
BIRDEYE_WS_URL = os.getenv("BIRDEYE_WS_URL", "wss://public-api.birdeye.so/ws")
BIRDEYE_WS_API_KEY = os.getenv("BIRDEYE_WS_API_KEY", "")
BIRDEYE_WS_PING_SEC = int(os.getenv("BIRDEYE_WS_PING_SEC", "20"))
BIRDEYE_WS_MINT_CAP = int(os.getenv("BIRDEYE_WS_MINT_CAP", "200"))

LOOP_SLEEP_SEC = int(os.getenv("MONITOR_LOOP_SLEEP_SEC", "3"))
ECO_SLEEP_MULT = float(os.getenv("ECO_SLEEP_MULT", "3.0"))  # slow down when slots are full

# --- Capacity-aware eco mode ---
OPEN_POS_DIR = os.getenv("OPEN_POS_DIR", "open_positions")
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))

def _slots_full() -> bool:
    try:
        return len([f for f in os.listdir(OPEN_POS_DIR) if f.endswith(".json")]) >= MAX_CONCURRENT_TRADES
    except Exception:
        return False

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper() # INFO or DEBUG

# Promotion thresholds (tune these to control sensitivity)
MIN_RECOVERY_FROM_LOW_PCT = float(os.getenv("MIN_RECOVERY_FROM_LOW_PCT", "3.0"))
CONSEC_UP_TICKS_MIN = int(os.getenv("CONSEC_UP_TICKS_MIN", "2"))
REQUIRE_ROUTE_WHEN_MC_NOW = float(os.getenv("REQUIRE_ROUTE_WHEN_MC_NOW", "60000"))
ROUTE_TEST_USD = float(os.getenv("ROUTE_TEST_USD", "25"))
ROUTE_MAX_IMPACT_PCT = float(os.getenv("ROUTE_MAX_IMPACT_PCT", "5.0"))
ROUTE_SLIPPAGE_BPS = int(os.getenv("ROUTE_SLIPPAGE_BPS", "150"))

SIM_SIZE_USD = float(os.getenv('SIM_SIZE_USD', '250.0'))
USDC_MINT_SOL = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'

# =========================
# Reset Helpers & Logic
# =========================
def _touch_empty(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
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
    print("[reset] Exhaustion Monitor: starting clean…")
    _del(EXEC_QUEUE)
    _touch_empty(EXEC_QUEUE)
    _del(RELIEF_REGISTRY)
    try:
        json.dump({"mints": [], "updated_ts": 0}, open(RELIEF_REGISTRY, "w", encoding="utf-8"))
    except Exception as e:
        print(f"[reset] Could not initialize {RELIEF_REGISTRY}: {e}")
    _touch_empty(WATCH_QUEUE)

# =========================
# Logging and Utilities
# =========================
def _log(level: str, message: str):
    if level == 'DEBUG' and LOG_LEVEL != 'DEBUG':
        return
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} [{level}] {message}")

def _now() -> int: return int(time.time())

def tg_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        _log('INFO', f"[tg-dry] {text.replace(chr(10), ' ')}")
        return
    try:
        r = requests.post(f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3900],
                                "disable_web_page_preview": True}, timeout=8)
        if r.status_code != 200:
            _log("WARN", f"Telegram API error: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        _log("ERROR", f"Telegram send exception: {repr(e)}")

# --- Unified EM alert formatting (display-only) ---
UNIFIED_TG_FMT = int(os.getenv("UNIFIED_TG_FMT", "1"))

def _fmt_num(x, nd=2):
    try: return f"{float(x):.{nd}f}"
    except: return "n/a"

def em_promote(sym, mint, reason, rec=None, route=None, m5=None, mc=None):
    if not UNIFIED_TG_FMT: return
    tg_send(
        f"[EM|PROMOTE] {sym} ({mint[:6]}) → scenarios.jsonl"
        f" | reason={reason}"
        f"{f' | rec={_fmt_num(rec)}%' if rec is not None else ''}"
        f"{f' | route={route}' if route is not None else ''}"
        f"{f' | m5={m5}' if m5 is not None else ''}"
        f"{f' | mc=${_fmt_num(mc,0)}' if mc is not None else ''}"
    )

def em_drop(sym, mint, reason, rec=None, route=None):
    if not UNIFIED_TG_FMT: return
    tg_send(
        f"[EM|DROP] {sym} ({mint[:6]})"
        f" | reason={reason}"
        f"{f' | rec={_fmt_num(rec)}%' if rec is not None else ''}"
        f"{f' | route={route}' if route is not None else ''}"
    )

def em_relief(sym, mint, action):
    if not UNIFIED_TG_FMT: return
    tg_send(f"[EM|RELIEF] {sym} ({mint[:6]}) | {action}")

def em_error(sym, mint, err):
    if not UNIFIED_TG_FMT: return
    tg_send(f"[EM|ERROR] {sym} ({mint[:6]}) | {err}")


def read_json(path: str, default):
    try: return json.load(open(path, "r", encoding="utf-8"))
    except Exception as e:
        _log("WARN", f"Failed to read JSON from {path}: {e}")
        return default

def write_json(path: str, obj):
    try: json.dump(obj, open(path, "w", encoding="utf-8"), indent=2)
    except Exception as e: _log("ERROR", f"Failed to write JSON to {path}: {e}")

def relief_load() -> Set[str]:
    return set(read_json(RELIEF_REGISTRY, {"mints":[]}).get("mints", []))

def relief_save(mints: Set[str]):
    write_json(RELIEF_REGISTRY, {"mints": sorted(list(mints)), "updated_ts": _now()})

def relief_add(mint: str, symbol: str):
    s = relief_load(); s.add(mint); relief_save(s)
    em_relief(symbol, mint, "relief_added")

def relief_remove(mint: str, symbol: str):
    s = relief_load()
    if mint in s:
        s.remove(mint)
        relief_save(s)
        em_relief(symbol, mint, "relief_removed")

# =========================
# Birdeye WebSocket Client
# =========================
class BirdeyeWS:
    def __init__(self, url: str, x_api_key: str, ping_sec: int = 20):
        if x_api_key and "x-api-key=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}x-api-key={x_api_key}"
        self.url = url
        self.ws: Optional[WebSocketApp] = None
        self.thread: Optional[Thread] = None
        self.stop_ev = Event()
        self.subscribed: Set[str] = set()
        self.out_q: Queue = Queue()

    def _headers(self): return ["Origin: wss://public-api.birdeye.so", "Sec-WebSocket-Protocol: echo-protocol"]
    def _on_open(self, ws):
        _log("INFO", "WebSocket connection opened.")
        for m in list(self.subscribed): self.subscribe(m, force=True)
    def _on_close(self, ws, status_code, msg):
        _log("WARN", f"WebSocket connection closed. Code: {status_code}, Msg: {msg}")
    def _on_error(self, ws, error):
        _log("ERROR", f"WebSocket error: {error}")
    def _on_message(self, ws, msg):
        try: data = json.loads(msg)
        except: return
        if data.get("type") == "PRICE_DATA":
            d = data.get("data", {})
            mint, price, ts = d.get("address"), d.get("c"), d.get("unixTime", _now())
            if mint and price: self.out_q.put((mint, float(price), ts))

    def start(self):
        if self.thread and self.thread.is_alive(): return
        self.stop_ev.clear()
        def run():
            while not self.stop_ev.is_set():
                _log("INFO", f"Attempting to connect to WebSocket: {self.url}")
                self.ws = WebSocketApp(self.url, header=self._headers(), on_open=self._on_open,
                                       on_message=self._on_message, on_error=self._on_error, on_close=self._on_close)
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=BIRDEYE_WS_PING_SEC)
                if not self.stop_ev.is_set():
                    _log("INFO", "WebSocket disconnected. Reconnecting in 5 seconds...")
                    time.sleep(5)
        self.thread = Thread(target=run, daemon=True); self.thread.start()

    def subscribe(self, mint: str, force: bool = False):
        if not force and mint in self.subscribed: return
        if len(self.subscribed) >= BIRDEYE_WS_MINT_CAP:
            _log("WARN", f"WebSocket subscription limit ({BIRDEYE_WS_MINT_CAP}) reached. Cannot subscribe {mint[:8]}...")
            return
        self.subscribed.add(mint)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.send(json.dumps({"type": "SUBSCRIBE_PRICE", "data": {"address": mint}}))
                _log("DEBUG", f"Subscribed to price updates for {mint[:8]}...")
            except Exception as e:
                _log("ERROR", f"Failed to subscribe to {mint[:8]}: {e}")

    def unsubscribe(self, mint: str):
        if mint not in self.subscribed: return
        self.subscribed.discard(mint)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.send(json.dumps({"type": "UNSUBSCRIBE_PRICE", "data": {"address": mint}}))
                _log("DEBUG", f"Unsubscribed from price updates for {mint[:8]}...")
            except Exception as e:
                _log("ERROR", f"Failed to unsubscribe from {mint[:8]}: {e}")

# =========================
# Data Fetching & Confirmations
# =========================
def fetch_birdeye_price_and_low(mint: str, low_t: int) -> Tuple[Optional[float], Optional[float]]:
    if not BIRDEYE_API_KEY: return None, None
    now = _now()
    time_from = min(low_t - 60, now - 300)
    url = 'https://public-api.birdeye.so/defi/ohlcv'
    params = {'address': mint, 'type':'1m', 'time_from': time_from, 'time_to': now}
    headers = {'accept':'application/json', 'x-chain':'solana', 'X-API-KEY': BIRDEYE_API_KEY}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=4)
        if r.status_code != 200:
            _log("WARN", f"Birdeye API OHLCV request failed for {mint[:8]} with status {r.status_code}")
            return None, None
        items = ((r.json() or {}).get('data') or {}).get('items') or []
        if not items: return None, None
        
        current_price = float(items[-1]['c'])
        prices_after_low = [float(it['c']) for it in items if it.get('unixTime', 0) >= low_t and it.get('c') is not None]
        low_price = min(prices_after_low) if prices_after_low else None

        return current_price, low_price
    except Exception as e:
        _log("ERROR", f"Exception fetching Birdeye OHLCV for {mint[:8]}: {e}")
        em_error("N/A", mint, f"birdeye_fetch_err:{e}")
        return None, None

def jupiter_quote(input_mint: str, output_mint: str, amount_base_units: int, slippage_bps: int) -> Optional[dict]:
    try:
        r = requests.get("https://quote-api.jup.ag/v6/quote", params={
            "inputMint": input_mint, "outputMint": output_mint, "amount": amount_base_units,
            "slippageBps": slippage_bps, "onlyDirectRoutes": "false"}, timeout=3)
        return r.json() if r.status_code == 200 else None
    except Exception: return None

def route_health_ok(base_mint: str, token_mint: str) -> Tuple[bool, str]:
    try:
        buy_amt = int(max(1, ROUTE_TEST_USD * (10 ** 6)))
        q_buy = jupiter_quote(base_mint, token_mint, buy_amt, ROUTE_SLIPPAGE_BPS)
        if not q_buy: return False, "no_buy_quote"
        out_amt = int(q_buy.get("outAmount") or 0)
        imp_buy = 100.0 * float(q_buy.get("priceImpactPct") or 0.0)
        if out_amt <= 0: return False, "buy_out_zero"
        q_sell = jupiter_quote(token_mint, base_mint, out_amt, ROUTE_SLIPPAGE_BPS)
        if not q_sell: return False, "no_sell_quote"
        imp_sell = 100.0 * float(q_sell.get("priceImpactPct") or 0.0)
        ok = (imp_buy < ROUTE_MAX_IMPACT_PCT) and (imp_sell < ROUTE_MAX_IMPACT_PCT)
        return ok, f"imp_buy={imp_buy:.2f}%"
    except Exception as e:
        em_error("N/A", token_mint, f"route_health_err:{e}")
        return False, f"route_err:{e}"

# =========================
# Main Monitor Logic
# =========================
@dataclass
class MonitorState:
    mint: str
    symbol: str
    seed_ts: int
    low_t: int
    meta: Dict
    
    last_price: Optional[float] = None
    last_ts: int = 0
    consecutive_up: int = 0
    promoted: bool = False
    last_check_ts: int = 0

class ExhaustionMonitor:
    def __init__(self):
        self.ws = BirdeyeWS(BIRDEYE_WS_URL, BIRDEYE_WS_API_KEY) if BIRDEYE_WS_ON and BIRDEYE_WS_API_KEY else None
        self.tailer_pos = 0
        self.active: Dict[str, MonitorState] = {}

    def _tail_and_adopt(self):
        if not os.path.exists(WATCH_QUEUE):
            return
        adopted_count = 0
        with open(WATCH_QUEUE, "r", encoding="utf-8") as f:
            f.seek(self.tailer_pos)
            for line in f:
                try:
                    seed = json.loads(line)
                    mint = seed.get("mint")
                    if mint and mint not in self.active:
                        meta = seed.get("meta", {})
                        symbol = seed.get("symbol", mint[:6])
                        self.active[mint] = MonitorState(
                            mint=mint,
                            symbol=symbol,
                            seed_ts=seed.get("seed_ts", _now()),
                            low_t=seed.get("low_t", 0),
                            meta=meta,
                        )
                        if self.ws:
                            self.ws.subscribe(mint)
                        relief_add(mint, symbol)
                        mc_str = f"MC: ${int(meta.get('mc_now', 0)):,}" if meta.get("mc_now") else "MC: N/A"
                        bq_raw = meta.get("bottom_quality", None)
                        bq_str = f"BQ: {bq_raw:.2f}" if isinstance(bq_raw, (int, float)) else "BQ: N/A"
                        _log("INFO", f"Adopted {seed.get('symbol')} ({mint[:8]}...). {mc_str}, {bq_str}")
                        adopted_count += 1
                except json.JSONDecodeError:
                    _log("WARN", f"Could not decode JSON from watchlist: {line.strip()}")
                    continue
            self.tailer_pos = f.tell()
        if adopted_count > 0:
            _log("INFO", f"Adopted {adopted_count} new token(s) from watchlist.")

    def _drain_ws_prices(self):
        if not self.ws:
            return
        drained_count = 0
        while True:
            try:
                mint, px, ts = self.ws.out_q.get_nowait()
                drained_count += 1
                if mint in self.active:
                    st = self.active[mint]
                    _log("DEBUG", f"WS tick for {st.symbol[:10]} ({mint[:6]}): ${px:.6f}")
                    if st.last_price and px > st.last_price:
                        st.consecutive_up += 1
                    elif st.last_price and px < st.last_price:
                        st.consecutive_up = 0
                    st.last_price = px
                    st.last_ts = ts
            except Empty:
                break
        if drained_count > 0:
            _log("DEBUG", f"Drained {drained_count} price ticks from WebSocket queue.")

    def _promote(self, st: MonitorState, reason: str, **kwargs):
        st.promoted = True

        scenario = {
            "id": str(uuid.uuid4()),
            "mint": st.mint,
            "symbol": st.symbol,
            "side": "buy",
            "size_usd": SIM_SIZE_USD,
            "signal_ts": _now(),
            "meta": {
                **(st.meta or {}),
                "reason": f"monitor_promote:{reason}",
                "promoter_last_price": st.last_price,
            },
        }

        # append to executor queue
        with open(EXEC_QUEUE, "a", encoding="utf-8") as f:
            f.write(json.dumps(scenario) + "\n")

        em_promote(
            st.symbol,
            st.mint,
            reason=reason,
            rec=kwargs.get("rec"),
            route=kwargs.get("route"),
            m5=st.meta.get("m5") if st.meta else None,
            mc=st.meta.get("mc_now") if st.meta else None,
        )
        _log("INFO", f"PROMOTED {st.symbol} ({st.mint[:8]}...). Reason: {reason}, Price: ${st.last_price:.6f}")

        if st.mint in self.active:
            del self.active[st.mint]
        if self.ws:
            self.ws.unsubscribe(st.mint)
        relief_remove(st.mint, st.symbol)

    def _check_and_promote(self):
        now = _now()
        for mint, st in list(self.active.items()):
            if now - st.last_check_ts < 15:
                continue
            st.last_check_ts = now

            _log("DEBUG", f"Checking {st.symbol} ({mint[:8]}...). Last price: {st.last_price}")

            # Use live WS price if available, otherwise fetch via REST
            current_price = st.last_price
            low_price_from_rest = None

            if not current_price or now - (st.last_ts or 0) > 60:
                _log("DEBUG", f"Price for {st.symbol} is stale or missing, fetching via REST...")
                current_price, low_price_from_rest = fetch_birdeye_price_and_low(st.mint, st.low_t)
                if current_price:
                    st.last_price = current_price
                    st.last_ts = now

            if not current_price:
                _log("DEBUG", f"No valid price found for {st.symbol}, skipping check.")
                continue

            # --- Promotion Conditions ---
            # Cond 1: Minimum recovery from low (requires REST call for low_price)
            if low_price_from_rest:
                recovery_pct = ((current_price - low_price_from_rest) / low_price_from_rest) * 100 if low_price_from_rest > 0 else 0
                _log("DEBUG", f"{st.symbol}: Recovery check: current=${current_price:.6f}, low=${low_price_from_rest:.6f}, recovery={recovery_pct:.2f}% (Threshold: >{MIN_RECOVERY_FROM_LOW_PCT}%)")
                if recovery_pct > MIN_RECOVERY_FROM_LOW_PCT:
                    self._promote(st, "recovery_ok", rec=recovery_pct)
                    continue

            # Cond 2: Consecutive up-ticks from WebSocket
            _log("DEBUG", f"{st.symbol}: Consec ticks check: {st.consecutive_up} (Threshold: >={CONSEC_UP_TICKS_MIN})")
            if st.consecutive_up >= CONSEC_UP_TICKS_MIN:
                self._promote(st, "consecutive_ticks", rec=None)
                continue

            # Cond 3: Route health for high market cap tokens
            mc_now = st.meta.get("mc_now", 0) if st.meta else 0
            if mc_now >= REQUIRE_ROUTE_WHEN_MC_NOW:
                _log("DEBUG", f"{st.symbol}: High MC (${mc_now:,.0f}), checking route health...")
                ok, why = route_health_ok(USDC_MINT_SOL, st.mint)
                _log("DEBUG", f"{st.symbol}: Route health result: OK={ok}, Info={why}")
                if ok:
                    self._promote(st, "route_ok", route=why)
                    continue

    def _cleanup_stale(self):
        now = _now()
        stale_mints = [
            mint for mint, st in self.active.items()
            if now - st.seed_ts > RELIEF_TTL_MIN * 60
        ]
        for mint in stale_mints:
            st = self.active[mint]
            em_drop(st.symbol, mint, reason="timeout")
            _log("INFO", f"Timing out stale monitor for {st.symbol} ({mint[:8]}...).")
            del self.active[mint]
            if self.ws:
                self.ws.unsubscribe(mint)
            relief_remove(mint, st.symbol)

    def run(self):
        _log("INFO", "🤖 Starting Exhaustion Monitor...")
        tg_send("🤖 Exhaustion Monitor - start")
        if self.ws:
            self.ws.start()

        while True:
            # === ECO MODE: when slots are full, avoid adopting/promoting and keep things cheap ===
            if _slots_full():
                _log("INFO", "[eco] EM: slots full → pausing adopts/promotions and WS growth.")
                # keep it light: clean up any stale to avoid leaks
                self._cleanup_stale()
                # optionally still drain existing WS queue without adding new subs
                # self._drain_ws_prices()
                time.sleep(LOOP_SLEEP_SEC * ECO_SLEEP_MULT)
                continue

            _log("DEBUG", f"--- New Monitor Cycle | Active: {len(self.active)} ---")
            self._tail_and_adopt()       # may add new WS subs only when not full
            self._drain_ws_prices()
            self._check_and_promote()
            self._cleanup_stale()
            time.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    monitor = ExhaustionMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        _log("INFO", "Exiting Exhaustion Monitor.")
