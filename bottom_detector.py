# Script A: Bottom Detector (signals producer)
# Continuously scans the last 3 processed reports, detects EXHAUSTION/REBID bottoms,
# applies gates, and emits scenarios to a JSONL queue.

from __future__ import annotations
import os, json, time, math, bisect, hashlib, statistics, random, shutil, glob, pathlib, re
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Set
import requests
from dotenv import load_dotenv
load_dotenv(override=True)
import uuid

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
WHYNOT_COOLDOWN_MIN = int(os.getenv('WHYNOT_COOLDOWN_MIN', '15'))

REBID_MARKET_TXN_5M_MIN = int(os.getenv('REBID_MARKET_TXN_5M_MIN', '8'))

USDC_MINT_SOL = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
WSOL_MINT_SOL = 'So11111111111111111111111111111111111111112'
USDT_MINT_SOL = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
PREFERRED_QUOTES = [USDC_MINT_SOL, WSOL_MINT_SOL, USDT_MINT_SOL]

JUPITER_QUOTE_URL = os.getenv('JUPITER_QUOTE', 'https://quote-api.jup.ag/v6/quote')
ROUTE_BASE_MINT = os.getenv('BASE_INPUT_MINT', USDC_MINT_SOL)
ROUTE_TEST_USD = float(os.getenv('ROUTE_TEST_USD', '25'))
ROUTE_MAX_IMPACT_PCT = float(os.getenv('ROUTE_MAX_IMPACT_PCT', '5.0'))
ROUTE_SLIPPAGE_BPS = int(os.getenv('ROUTE_SLIPPAGE_BPS', '150'))
USDC_DECIMALS = 6

MC_BID_ENV_LOW_USD = float(os.getenv('MC_BID_ENV_LOW_USD', '10000'))
MC_BID_ENV_HIGH_USD = float(os.getenv('MC_BID_ENV_HIGH_USD', '30000'))
AGE_YOUNG_MAX_MIN = int(os.getenv('AGE_YOUNG_MAX_MIN', '240'))

REBID_M5_SOFTEN_FACTOR = float(os.getenv('REBID_M5_SOFTEN_FACTOR', '0.50'))
BOOST_MIN_BQ = float(os.getenv('BOOST_MIN_BQ', '0.40'))
REBID_M5_FLOOR = int(os.getenv('REBID_M5_FLOOR', '3'))

ENABLE_GPT_GATE = int(os.getenv('ENABLE_GPT_GATE', '0'))
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
GPT_MODEL = os.getenv('GPT_MODEL', 'gpt-4o-mini')
GPT_MIN_CONF = float(os.getenv('GPT_MIN_CONF', '0.55'))
WHYNOT_ON_NON_REBID = int(os.getenv('WHYNOT_ON_NON_REBID', '0'))
EXPLAIN_EXHAUSTION = int(os.getenv('EXPLAIN_EXHAUSTION', '1'))

RUN_FOREVER = int(os.getenv('RUN_FOREVER', '1'))
LOOP_SLEEP_SEC = int(os.getenv('LOOP_SLEEP_SEC', '10'))

# --- Capacity-aware eco mode ---
OPEN_POS_DIR = os.getenv('OPEN_POS_DIR', 'open_positions')  # executor writes one .json per open position
MAX_CONCURRENT_TRADES = int(os.getenv('MAX_CONCURRENT_TRADES', '3'))
ECO_SLEEP_MULT = float(os.getenv('ECO_SLEEP_MULT', '3.0'))  # slow down when slots are full

def _slots_full() -> bool:
    try:
        return len([f for f in os.listdir(OPEN_POS_DIR) if f.endswith('.json')]) >= MAX_CONCURRENT_TRADES
    except Exception:
        return False


RECENT_TOKENS_WINDOW = int(os.getenv('RECENT_TOKENS_WINDOW', '50'))
LAST_TRADES_N = int(os.getenv('LAST_TRADES_N', '20'))
DEPRESSED_1H_THRESHOLD = float(os.getenv('DEPRESSED_1H_THRESHOLD', '-5.0'))
HOT_1H_THRESHOLD = float(os.getenv('HOT_1H_THRESHOLD', '10.0'))
RECOVERY_ADJUST_PCT = float(os.getenv('RECOVERY_ADJUST_PCT', '25.0'))
WINRATE_LOW = float(os.getenv('WINRATE_LOW', '40.0'))
WINRATE_HIGH = float(os.getenv('WINRATE_HIGH', '60.0'))
BQ_ADJUST_STEP = float(os.getenv('BQ_ADJUST_STEP', '0.05'))

RECOVERY_MULT_CEIL = 4.0
MC_NOW_PENALTY_START = 30000.0
MC_NOW_PENALTY_MAXAT = 120000.0
MC_NOW_PENALTY_MAX = 0.35
REQUIRE_ROUTE_WHEN_MC_NOW = 60000.0

SIM_QUEUE = os.getenv('SIM_QUEUE', 'scenarios.jsonl')
SIM_SIZE_USD = float(os.getenv('SIM_SIZE_USD', '250.0'))
EMIT_COOLDOWN_MIN = int(os.getenv('EMIT_COOLDOWN_MIN', '5'))

WATCH_QUEUE = os.getenv("WATCH_QUEUE", "watchlist.jsonl")
RELIEF_REGISTRY = os.getenv("RELIEF_REGISTRY", "relief_registry.json")

# <<< NEW: Report selection and dedup settings
PICK_WINDOW = int(os.getenv("PICK_WINDOW","3"))
MAX_BACKFILL_AGE_MIN = int(os.getenv("MAX_BACKFILL_AGE_MIN","180"))
MIN_REPORT_ENTRIES = int(os.getenv("MIN_REPORT_ENTRIES","1"))
FILL_ORDER = os.getenv("FILL_ORDER","oldest")
REPORT_PATTERN = os.getenv("REPORT_PATTERN", "holding_report_*.json")
REPORTS_INDEX = os.getenv("REPORTS_INDEX", "reports_index.json")
SEEN_TUPLES = os.getenv("SEEN_TUPLES", "seen_tuples.json")
SEEN_MINTS = os.getenv("SEEN_MINTS", "seen_mints.json")
NOVEL_TTL_MIN = int(os.getenv("NOVEL_TTL_MIN","180"))
MINT_GLOBAL_COOLDOWN_MIN = int(os.getenv("MINT_GLOBAL_COOLDOWN_MIN","90"))

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
    print("[reset] Bottom Detector: starting clean…")
    _del(WATCH_QUEUE)
    _touch_empty(WATCH_QUEUE)
    _del(SIM_QUEUE)
    _touch_empty(SIM_QUEUE)
    _del(RELIEF_REGISTRY)
    _del(REPORTS_INDEX) # <<< NEW
    _del(SEEN_TUPLES)   # <<< NEW
    _del(SEEN_MINTS)    # <<< NEW
    try:
        json.dump({"mints": [], "updated_ts": 0}, open(RELIEF_REGISTRY, "w", encoding="utf-8"))
    except Exception as e:
        print(f"[reset] Could not initialize {RELIEF_REGISTRY}: {e}")

# =========================
# Script State
# =========================
_RELIEF_SET: Set[str] = set() # Cache for the current run
recent_m5: List[int] = []
recent_1h: List[float] = []
_LAST_EMIT_TS_BY_MINT: Dict[str, int] = {}
_LAST_WHYNOT_TS_BY_MINT: Dict[str, int] = {}

# =========================
# Safe utilities
# =========================
def _now() -> int:
    return int(time.time())

def _sha(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))

def _load_json(p, default):
    try: return json.load(open(p,'r',encoding='utf-8'))
    except: return default
def _save_json(p, obj): json.dump(obj, open(p,'w',encoding='utf-8'), indent=2)

def _relief_load():
    global _RELIEF_SET
    try:
        data = json.load(open(RELIEF_REGISTRY, "r", encoding="utf-8"))
        _RELIEF_SET = set(data.get("mints", []))
    except (FileNotFoundError, json.JSONDecodeError):
        _RELIEF_SET = set()

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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('[tg] (dry)', text.replace('\n', ' ')[:220])
        return
    if not _should_send(text, cooldown_sec):
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
            print('[tg] error:', r.status_code, r.text[:200])
    except Exception as e:
        print('[tg] exception:', repr(e))

UNIFIED_TG_FMT = int(os.getenv("UNIFIED_TG_FMT", "1"))

def _fmt_num(x, nd=2):
    try: return f"{float(x):.{nd}f}"
    except: return "n/a"

def bd_watch(sym, mint, reason, m5=None, mc=None, dex=None):
    if not UNIFIED_TG_FMT: return
    tg_send(
        f"[BD|WATCH] {sym} ({mint[:6]}) → watchlist.jsonl"
        f" | reason={reason}"
        f"{f' | m5={m5}' if m5 is not None else ''}"
        f"{f' | mc=${_fmt_num(mc,0)}' if mc is not None else ''}"
        f"{f' | dex={dex}' if dex else ''}"
    )

def bd_rebid(sym, mint, dd=None, rec=None, m5=None, mc=None, ex=None, bq=None):
    if not UNIFIED_TG_FMT: return
    tg_send(
        f"[BD|REBID] {sym} ({mint[:6]}) → scenarios.jsonl"
        f"{f' | dd={_fmt_num(dd)}%' if dd is not None else ''}"
        f"{f' | rec={_fmt_num(rec)}%' if rec is not None else ''}"
        f"{f' | m5={m5}' if m5 is not None else ''}"
        f"{f' | mc=${_fmt_num(mc,0)}' if mc is not None else ''}"
        f"{f' | ex={_fmt_num(ex)}' if ex is not None else ''}"
        f"{f' | bq={_fmt_num(bq)}' if bq is not None else ''}"
    )

def bd_gated(sym, mint, why):
    if not UNIFIED_TG_FMT: return
    tg_send(f"[BD|GATED] {sym} ({mint[:6]}) | {why}")

def bd_veto(sym, mint, conf, why):
    if not UNIFIED_TG_FMT: return
    tg_send(f"[BD|VETO] {sym} ({mint[:6]}) | conf={_fmt_num(conf)} | {why}")

def bd_error(sym, mint, err):
    if not UNIFIED_TG_FMT: return
    tg_send(f"[BD|ERROR] {sym} ({mint[:6]}) | {err}")


# =========================
# Optional helius imports (soft)
# =========================
try:
    from helius_analyzer import _fetch_parsed_history_page_v0, format_timestamp
    HELIUS_OK = True
except Exception:
    HELIUS_OK = False
    def _fetch_parsed_history_page_v0(*args, **kwargs): return []
    def format_timestamp(ts: int): return str(ts)

# =========================
# Processed reports (inputs)
# =========================
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
# <<< NEW: Report Selection & Dedup Logic
# =========================
def _list_reports_sorted_oldest_first(dir_path, pattern):
    return sorted(glob.glob(str(pathlib.Path(dir_path) / pattern)))

def _derive_created_ts(path):
    m = re.search(r'(\d{10})', os.path.basename(path))
    return int(m.group(1)) if m else int(os.stat(path).st_mtime)

def _within_window(created_ts, max_age_min):
    return (_now() - created_ts) <= max_age_min * 60

def _load_idx(): return _load_json(REPORTS_INDEX, {"reports":[]})
def _save_idx(idx): _save_json(REPORTS_INDEX, idx)

def _idx_upsert(idx, name, path, cts):
    for r in idx["reports"]:
        if r["name"] == name:
            r["path"]=path; r["created_ts"]=cts; return
    idx["reports"].append({"name":name, "path":path, "created_ts":cts, "entries":None, "last_checked_ts":None})

def _idx_set_entries(idx, name, entries):
    for r in idx["reports"]:
        if r["name"]==name:
            r["entries"]=int(entries); r["last_checked_ts"]=_now(); return

def _idx_lookup(idx, name):
    for r in idx["reports"]:
        if r["name"]==name: return r
    return None

def _load_seen_tuples():
    d = _load_json(SEEN_TUPLES, {"ttl_min": NOVEL_TTL_MIN, "seen":[]}); d["ttl_min"]=NOVEL_TTL_MIN; return d
def _save_seen_tuples(d): _save_json(SEEN_TUPLES, d)
def _prune_seen_tuples(d):
    cutoff = _now() - d["ttl_min"]*60
    d["seen"] = [x for x in d["seen"] if x.get("ts",0) >= cutoff]
def _seen_tuple_recently(d, report_name, mint):
    return any(x["report"]==report_name and x["mint"]==mint for x in d["seen"])
def _mark_seen_tuple(d, report_name, mint):
    d["seen"].append({"report":report_name,"mint":mint,"ts":_now()}); _save_seen_tuples(d)

def _load_seen_mints():
    d = _load_json(SEEN_MINTS, {"cooldown_min": MINT_GLOBAL_COOLDOWN_MIN, "mints":[]}); d["cooldown_min"]=MINT_GLOBAL_COOLDOWN_MIN; return d
def _save_seen_mints(d): _save_json(SEEN_MINTS, d)
def _prune_seen_mints(d):
    cutoff = _now() - d["cooldown_min"]*60
    d["mints"] = [x for x in d["mints"] if x.get("ts",0) >= cutoff]
def _seen_mint_recently(d, mint):
    return any(x["mint"]==mint for x in d["mints"])
def _mark_seen_mint(d, mint):
    d["mints"].append({"mint":mint,"ts":_now()}); _save_seen_mints(d)

def extract_valid_tokens_from_path(report_path: str) -> List[Dict]:
    try:
        report = json.load(open(report_path, "r", encoding="utf-8"))
        return [item for _, _, _, item in iter_valid_tokens_from_report(report)]
    except Exception:
        return []

def select_reports_for_cycle() -> list[tuple[str, list[dict]]]:
    idx = _load_idx()
    paths = _list_reports_sorted_oldest_first(PROCESSED_DIR, REPORT_PATTERN)
    if not paths: return []

    tailN_paths = list(reversed(paths))[:PICK_WINDOW]
    tailN_paths.reverse()
    head = []
    for rp in tailN_paths:
        name = os.path.basename(rp)
        cts  = _derive_created_ts(rp)
        _idx_upsert(idx, name, rp, cts)
        tokens = extract_valid_tokens_from_path(rp)
        _idx_set_entries(idx, name, len(tokens))
        head.append((_idx_lookup(idx, name), tokens))

    _save_idx(idx)

    picks = [(m["path"], toks) for (m, toks) in head if m and (len(toks) >= MIN_REPORT_ENTRIES)]
    missing = PICK_WINDOW - len(picks)
    if missing <= 0:
        return picks[:PICK_WINDOW]

    older_paths = [p for p in paths if p not in [m["path"] for (m,_) in head if m]]
    resolved = []
    for rp in older_paths:
        name = os.path.basename(rp)
        meta = _idx_lookup(idx, name)
        if not meta:
            cts = _derive_created_ts(rp)
            _idx_upsert(idx, name, rp, cts)
            meta = _idx_lookup(idx, name)
        if not _within_window(meta["created_ts"], MAX_BACKFILL_AGE_MIN):
            continue
        if meta.get("entries") is None:
            cnt = len(extract_valid_tokens_from_path(rp))
            _idx_set_entries(idx, name, cnt)
            _save_idx(idx)
            meta["entries"] = cnt
        if (meta.get("entries", 0)) >= MIN_REPORT_ENTRIES:
            resolved.append(meta)

    if FILL_ORDER == "oldest":
        resolved.sort(key=lambda r: r["created_ts"])
    else:
        resolved.sort(key=lambda r: r.get("created_ts", 0), reverse=True)

    fillers = []
    for r in resolved:
        if len(fillers) >= missing: break
        if any(r["path"] == p for (p, _) in picks):
            continue
        toks = extract_valid_tokens_from_path(r["path"])
        fillers.append((r["path"], toks))

    return picks + fillers

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

@dataclass
class Swap:
    t: int
    side: str
    wallet: Optional[str]

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
@dataclass
class Bar:
    t: int
    p: float

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

# =========================
# Detector
# =========================
@dataclass
class Config:
    drawdown_threshold_pct: float = float(os.getenv('DRAWDOWN_THRESHOLD_PCT', '40'))
    lookback_high_min: int = int(os.getenv('LOOKBACK_HIGH_MIN', '240'))
    silence_minutes: int = int(os.getenv('SILENCE_MINUTES', '8'))
    max_silence_tx: int = int(os.getenv('MAX_SILENCE_TX', '5'))
    rebid_window_min: int = int(os.getenv('REBID_WINDOW_MIN', '5'))
    rebid_market_txn_5m_min: int = REBID_MARKET_TXN_5M_MIN
    min_recovery_from_low_pct: float = float(os.getenv('MIN_RECOVERY_FROM_LOW_PCT', '3.0'))
    rebid_m5_soften_factor: float = REBID_M5_SOFTEN_FACTOR
    boost_min_bq: float = BOOST_MIN_BQ
    rebid_m5_floor: int = REBID_M5_FLOOR

class BottomDetector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def exhaustion_score(self, info: Dict) -> float:
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
        if not market_cap_usd or current_price <= 0 or low_price <= 0:
            return 0.0, None
        if current_price > 0 and low_price > 0:
            mult = current_price / low_price
            if mult > RECOVERY_MULT_CEIL:
                mc_low_est = (market_cap_usd or 0.0) * (1.0 / RECOVERY_MULT_CEIL)
            else:
                mc_low_est = float(market_cap_usd) * (low_price / current_price)
        else:
            mc_low_est = float(market_cap_usd) * (low_price / current_price)

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
                return 1.0 - 0.1 * (x ** 2)
            if mc < lo:
                return max(0.6, 0.6 + 0.3 * (mc / lo))
            decay_dist = max(1.0, 100000.0 - hi)
            return max(0.0, 0.9 - 0.9 * (mc - hi) / decay_dist)
        base = band_score(mc_low_est)
        if age_min is None:
            age_mult = 1.0
        else:
            age_mult = 1.0 - _clamp(age_min / max(1, AGE_YOUNG_MAX_MIN), 0.0, 1.0) * 0.2
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

        age_min = None
        if pair_created_at_ms:
            created_s = int(pair_created_at_ms // 1000)
            age_min = max(0, (bars[idx].t - created_s) // 60)

        sil_end_t = low_t + 60*self.cfg.silence_minutes
        post = [s for s in holder_swaps if low_t < s.t <= sil_end_t]

        info_base = {"drawdown_pct": round(dd,2), "post_tx": len(post), "low_t": low_t}
        ex_score = self.exhaustion_score(info_base)
        mc_age_s, mc_low_est = self.mc_age_score(current_p, low_price, market_cap_usd, age_min)

        vol = abs(pct5 or 0) / abs(pct60 or 1e-6) if pct60 is not None else 10.0
        exh_weight = 0.35
        mc_weight = 0.65
        if vol > 2:
            exh_weight, mc_weight = 0.5, 0.5
        elif vol < 1:
            exh_weight, mc_weight = 0.25, 0.75
        bottom_quality = round(_clamp(exh_weight*ex_score + mc_weight*mc_age_s, 0.0, 1.0), 3)

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
# Signal Emitter
# =========================
def emit_sim_scenario(mint: str, symbol: str, size_usd: float, info: Dict, pair_info: Tuple, holder_swaps: List[Swap],
                      reason: str, pct5: Optional[float], pct60: Optional[float], pct360: Optional[float], pct1440: Optional[float],
                      market_txn_10m: int, approx: bool, ok_micro: bool, why_micro: str, ok_route: bool, why_route: str, ok_flow: bool, why_flow: str,
                      ex_score: float, bq_score: float, conf: float, gpt_reason: str,
                      adaptive_m5_min: int, soft_m5: int, passed_conditions: str):
    now = _now()
    last_emit = _LAST_EMIT_TS_BY_MINT.get(mint, 0)
    if now - last_emit < EMIT_COOLDOWN_MIN * 60:
        return
    _LAST_EMIT_TS_BY_MINT[mint] = now

    pair_addr, dex_id, m5_total, mc_usd, created_ms = pair_info

    debug_line = (
        f"micro:{ok_micro}({why_micro}) | route:{ok_route}({why_route}) | "
        f"flow:{ok_flow}({why_flow}) | m5={m5_total} | base={adaptive_m5_min} "
        f"| soft_m5={soft_m5} | ex={ex_score:.2f} | bq={bq_score:.2f}{passed_conditions}"
    )

    scenario = {
        "id": str(uuid.uuid4()),
        "mint": mint,
        "symbol": symbol,
        "side": "buy",
        "size_usd": size_usd,
        "signal_ts": now,
        "meta": {
            "reason": reason,
            "low_t": info.get("low_t"),
            "dd_pct": info.get("drawdown_pct"),
            "recovery_pct": info.get("recovery_pct"),
            "exhaustion_score": ex_score,
            "bottom_quality": bq_score,
            "m5": m5_total,
            "m10": market_txn_10m,
            "route_ok": ok_route,
            "holders_buy": int(why_flow.split(" ")[0].split("=")[1]) if "buys" in why_flow else 0,
            "holders_sell": int(why_flow.split(" ")[1].split("=")[1]) if "sells" in why_flow else 0,
            "pair_addr": pair_addr,
            "dex_id": dex_id,
            "mc_now": mc_usd,
            "age_min": info.get("age_min"),
            "alert_debug_line": debug_line,
            "gpt_confidence": conf,
            "gpt_reason": gpt_reason,
        }
    }

    with open(SIM_QUEUE, "a", encoding="utf-8") as f:
        f.write(json.dumps(scenario) + "\n")

    bd_rebid(symbol, mint, dd=info.get('drawdown_pct'), rec=info.get('recovery_pct'),
             m5=m5_total, mc=mc_usd, ex=ex_score, bq=bq_score)

def emit_watch_scenario(mint: str, symbol: str, info: Dict, pair_info: Tuple):
    now = _now()
    if now - _LAST_EMIT_TS_BY_MINT.get(mint, 0) < EMIT_COOLDOWN_MIN * 60:
        return

    pair_addr, dex_id, m5_total, mc_usd, created_ms = pair_info

    watch_item = {
        "id": str(uuid.uuid4()),
        "mint": mint,
        "symbol": symbol,
        "seed_ts": now,
        "low_t": info.get("low_t"),
        "size_usd": SIM_SIZE_USD,
        "meta": {
            "reason": "EXHAUSTION_seed",
            "dd_pct": info.get("drawdown_pct"),
            "recovery_pct": info.get("recovery_pct"),
            "exhaustion_score": info.get("exhaustion_score"),
            "bottom_quality": info.get("bottom_quality_score"),
            "m5": m5_total,
            "pair_addr": pair_addr,
            "dex_id": dex_id,
            "mc_now": mc_usd,
            "mc_low_est_usd": info.get("mc_low_est_usd"),
            "age_min": info.get("age_min"),
        }
    }

    with open(WATCH_QUEUE, "a", encoding="utf-8") as f:
        f.write(json.dumps(watch_item) + "\n")

    bd_watch(symbol, mint, "exhaustion", m5=m5_total, mc=mc_usd, dex=dex_id)
    _LAST_EMIT_TS_BY_MINT[mint] = now


# =========================
# Orchestrator
# =========================
def maybe_refetch_for_pct(bars: List[Bar], mint: str) -> List[Bar]:
    need_6h  = pct_change_linear(bars, 360)  is None
    need_24h = pct_change_linear(bars, 1440) is None
    if not (need_6h or need_24h): return bars
    larger = fetch_birdeye_1m_bars(mint, limit=max(BIRDEYE_BARS_LIMIT, 1500))
    return larger if larger else bars

def compute_adaptive_values(cfg: Config, avg_m5: float) -> Tuple[int, int, float]:
    m5_series = recent_m5[:]
    base = None
    if m5_series:
        m5_series = [x for x in m5_series if isinstance(x, (int, float)) and x >= 0]
        m5_series.sort()
        k = max(1, int(0.2 * len(m5_series)))
        trimmed = m5_series[k:-k] if len(m5_series) > 2 * k else m5_series
        try:
            base = statistics.median(trimmed)
        except statistics.StatisticsError:
            base = statistics.median(m5_series)
    if base is None:
        base = avg_m5 if isinstance(avg_m5, (int, float)) and avg_m5 > 0 else cfg.rebid_market_txn_5m_min

    raw = int(base * 0.5)
    adaptive_m5_min = max(3, min(raw, 18))
    cfg.rebid_market_txn_5m_min = adaptive_m5_min

    soft_m5 = max(2, int(adaptive_m5_min * cfg.rebid_m5_soften_factor))

    median_1h = statistics.median(recent_1h) if recent_1h else 0.0
    adaptive_min_recovery = cfg.min_recovery_from_low_pct
    if median_1h < -2.0:
        adaptive_min_recovery *= 0.9
    elif median_1h > 2.0:
        adaptive_min_recovery *= 1.1
    adaptive_min_recovery = _clamp(adaptive_min_recovery, 1.0, 6.0)

    return adaptive_m5_min, soft_m5, adaptive_min_recovery

def process_report_and_tokens(cfg: Config, report: dict, novel_tokens: List[dict]) -> List[str]:
    report_name = os.path.basename(report.get("report_path", "unknown_report"))
    print(f"[*] Analyzing {len(novel_tokens)} novel tokens from report: {report_name}")
    
    emitted_mints = []

    # Pre-scan for adaptive values
    m5_list = []
    for item in novel_tokens:
        ca = (item.get('bds_metadata') or {}).get('address') or item.get('token_mint')
        if not ca: continue
        try:
            _, _, m5_total, _, _ = dexscreener_best_pair_and_m5(ca)
            if m5_total > 0:
                m5_list.append(m5_total)
        except Exception:
            pass
    avg_m5 = statistics.mean(m5_list) if m5_list else cfg.rebid_market_txn_5m_min
    adaptive_m5_min, soft_m5, adaptive_min_recovery = compute_adaptive_values(cfg, avg_m5)

    for item in novel_tokens:
        md = item.get('bds_metadata') or {}
        ca = md.get('address') or item.get('token_mint') or item.get('address')
        sym = item.get('symbol') or md.get('symbol') or ca[:6]

        if ca in _RELIEF_SET:
            print(f"[*] Skipping {sym} ({ca[:6]}…): under monitor relief.")
            continue

        try:
            bars = fetch_birdeye_1m_bars(ca, limit=BIRDEYE_BARS_LIMIT)
            if len(bars) < 30:
                print(f"[-] Skipping {sym} ({ca[:6]}…): Insufficient price data ({len(bars)} bars).")
                continue

            bars = maybe_refetch_for_pct(bars, ca)

            holders = wallets_holding_mint_from_processed(report, ca)
            window_start, window_end = bars[0].t, bars[-1].t
            holder_swaps = mint_swaps_from_holders(holders, ca, per_wallet_limit=WALLET_HISTORY_LIMIT,
                                                   window_start=window_start, window_end=window_end)

            pair_addr, dex_id, m5_total, mc_usd, created_ms = dexscreener_best_pair_and_m5(ca)
            pair_info = (pair_addr, dex_id, m5_total, mc_usd, created_ms)
            exact_10m, approx = dexscreener_trades_last_10m(pair_addr)
            if approx and m5_total:
                exact_10m = m5_total * 2

            pct5 = pct_change_linear(bars, 5)
            pct60 = pct_change_linear(bars, 60)
            pct360 = pct_change_linear(bars, 360)
            pct1440 = pct_change_linear(bars, 1440)

            state, info = BottomDetector(cfg).state_for_index(bars, holder_swaps, len(bars)-1, m5_total, mc_usd, created_ms, pct5, pct60)
            now = _now()

            if state == "EXHAUSTION":
                emit_watch_scenario(ca, sym, info, pair_info)
                emitted_mints.append(ca)
                continue

            if state != "REBID":
                continue

            low_t = info.get("low_t", bars[-1].t)
            ok_micro, why_micro = micro_structure_ok(bars, low_t, need_consecutive=1)
            ok_route, why_route = route_health_ok(ROUTE_BASE_MINT, ca)
            ok_flow, why_flow = holder_flow_bias_ok(holder_swaps, low_t, window_min=20)

            ex_score = float(info.get("exhaustion_score", 0.0))
            bq_score = float(info.get("bottom_quality_score", 0.0))
            recovery = float(info.get("recovery_pct", 0.0))
            mc_low_est = info.get("mc_low_est_usd")
            age_min = info.get("age_min")

            if not ok_micro and pct5 > -5.0 and recovery > adaptive_min_recovery:
                ok_micro = True; why_micro += " | pct5_override"

            base_passed = (ok_micro and ok_route and (ok_flow or (m5_total >= adaptive_m5_min)))
            must_require_route = (mc_usd or 0.0) >= REQUIRE_ROUTE_WHEN_MC_NOW
            boosted_passed = ( (bq_score >= cfg.boost_min_bq) and ok_micro and ((ok_route and must_require_route) or ((ok_route or ok_flow) and not must_require_route)) and (m5_total >= soft_m5) and (ex_score >= 0.6) and (recovery >= 10) )
            quality_override = ( (bq_score >= 0.50) and ok_micro and ok_route and (recovery >= adaptive_min_recovery * 0.75) and (m5_total >= min(cfg.rebid_m5_floor, soft_m5)) )
            low_mcap_passed = False
            if mc_low_est is not None:
                low_mcap_passed = ( mc_low_est < 25000 and ex_score >= 0.50 and bq_score >= 0.35 and ok_micro and (ok_route or ok_flow) and m5_total >= 3 and recovery >= adaptive_min_recovery * 0.5 )

            passed = base_passed or boosted_passed or quality_override or low_mcap_passed
            
            if not passed:
                if bq_score > 0.4:
                    emit_watch_scenario(ca, sym, info, pair_info)
                    emitted_mints.append(ca)
                elif WHYNOT_ON_NON_REBID and (_now() - _LAST_WHYNOT_TS_BY_MINT.get(ca, 0)) >= WHYNOT_COOLDOWN_MIN*60:
                    debug = f"micro:{ok_micro} | route:{ok_route} | flow:{ok_flow} | m5={m5_total} | bq={bq_score:.2f}"
                    bd_gated(sym, ca, debug)
                    _LAST_WHYNOT_TS_BY_MINT[ca] = _now()
                continue

            passed_conditions = ""
            if passed:
                if boosted_passed and not base_passed: passed_conditions += " | BOOST"
                if quality_override and not (base_passed or boosted_passed): passed_conditions += " | QUALITY_OVERRIDE"
                if low_mcap_passed and not (base_passed or boosted_passed or quality_override): passed_conditions += " | LOW_MCAP"

            payload = { "symbol": sym, "mint": ca, "dex": dex_id or "?", "age_min": info.get('age_min', max(0, (bars[-1].t - bars[0].t)//60)), "m5_txn": m5_total, "m10_txn": exact_10m, "pct_5m": pct5, "pct_1h": pct60, "pct_6h": pct360, "pct_24h": pct1440, "drawdown_pct": info.get("drawdown_pct", 0.0), "recovery_pct": info.get("recovery_pct", 0.0), "bottom_quality_score": bq_score }
            go, conf, gpt_reason, gpt_tag = gpt_should_bid(payload)
            if not go:
                if (_now() - _LAST_WHYNOT_TS_BY_MINT.get(ca, 0)) >= WHYNOT_COOLDOWN_MIN*60:
                    bd_veto(sym, ca, conf, f"{gpt_reason} ({gpt_tag})")
                    _LAST_WHYNOT_TS_BY_MINT[ca] = _now()
                continue

            emit_sim_scenario(ca, sym, SIM_SIZE_USD, info, pair_info, holder_swaps, "REBID_OK", pct5, pct60, pct360, pct1440, exact_10m, approx, ok_micro, why_micro, ok_route, why_route, ok_flow, why_flow, ex_score, bq_score, conf, gpt_reason, adaptive_m5_min, soft_m5, passed_conditions)
            emitted_mints.append(ca)

        except Exception as e:
            print(f"[!] Error processing {sym} ({ca[:6]}…): {e}")
            bd_error(sym, ca, str(e))
            
    return emitted_mints

def main():
    cfg = Config()

    print("🤖 Starting Bottom Detector (with report selection)...")
    tg_send("🤖 Bottom Detector — start (with report selection)")

    while RUN_FOREVER:
        # Eco mode: if all trade slots are occupied, skip expensive scans
        if _slots_full():
            print("[eco] Bottom Detector: slots full → skipping scan to reduce API usage.")
            time.sleep(LOOP_SLEEP_SEC * ECO_SLEEP_MULT)
            continue

        print("--- New Processing Cycle ---")
        _relief_load()
        print(f"[*] Loaded {_RELIEF_SET.__len__()} mints under monitor relief.")
        
        reports_to_process = select_reports_for_cycle()
        if not reports_to_process:
            print(f"[*] No valid reports found to process. Waiting...")
            time.sleep(LOOP_SLEEP_SEC)
            continue

        print(f"[*] Selected {len(reports_to_process)} reports for this cycle.")
        
        seen_tuples = _load_seen_tuples()
        _prune_seen_tuples(seen_tuples)
        seen_mints = _load_seen_mints()
        _prune_seen_mints(seen_mints)
        
        total_emitted_this_cycle = 0

        for report_path, all_tokens in reports_to_process:
            report_name = os.path.basename(report_path)

            novel_tokens = []
            for token_item in all_tokens:
                ca = (token_item.get('bds_metadata') or {}).get('address') or token_item.get('token_mint')
                if not ca:
                    continue
                if _seen_tuple_recently(seen_tuples, report_name, ca) or _seen_mint_recently(seen_mints, ca):
                    continue
                novel_tokens.append(token_item)

            if not novel_tokens:
                print(f"[*] No novel tokens to process in {report_name}.")
                continue

            try:
                full_report_json = json.load(open(report_path, "r", encoding="utf-8"))
                full_report_json["report_path"] = report_path
            except Exception as e:
                print(f"[!] Could not read report for processing: {report_path} - {e}")
                continue

            emitted_mints = process_report_and_tokens(cfg, full_report_json, novel_tokens)

            if emitted_mints:
                total_emitted_this_cycle += len(emitted_mints)
                for mint in emitted_mints:
                    _mark_seen_tuple(seen_tuples, report_name, mint)
                    _mark_seen_mint(seen_mints, mint)
                print(f"[*] Marked {len(emitted_mints)} tokens from {report_name} as processed.")

        if total_emitted_this_cycle == 0:
            tg_send("Cycle Report: ℹ️ No new signals emitted.")
        else:
            tg_send(f"Cycle Report: ✅ Emitted {total_emitted_this_cycle} new signal(s).")

        print(f"--- Cycle End --- Sleeping for {LOOP_SLEEP_SEC} seconds...")
        time.sleep(LOOP_SLEEP_SEC)

    print("✅ Detector finished.")
    tg_send("✅ Detector complete")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Exiting detector.")