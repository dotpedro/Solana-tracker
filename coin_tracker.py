#!/usr/bin/env python3
"""
Coin Tracker  –  extra pipeline that runs *beside* main_loop_analyzer.py
  • Rug-checks every analysed mint
  • Confirms at least one “winning” wallet still holds it
  • Monitors chart + optional Twitter buzz
  • Sends Telegram alert on best-entry moment
"""

import os, sys, time, json, math, requests, statistics
from typing import Dict, Any, List, Set, Optional
from datetime import datetime, timezone   # ← change
from dotenv import load_dotenv
load_dotenv()                       # read .env for API keys

# ---------------------------------------------------------------------------
#  THIRD-PARTY KEYS  (get them *before* you print)
# ---------------------------------------------------------------------------
BIRDEYE_API_KEY  = os.getenv("BIRDEYE_API_KEY")
HELIUS_API_KEY   = os.getenv("HELIUS_API_KEY")
TWITTER_BEARER   = os.getenv("TWITTER_BEARER_TOKEN")
TG_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT          = os.getenv("TELEGRAM_CHAT_ID")

print("Birdeye key loaded?", bool(BIRDEYE_API_KEY))   # ← print AFTER the vars




# ---------------------------------------------------------------------------
#  PATHS – reuse existing files
# ---------------------------------------------------------------------------
WALLET_STORE_FILE   = "wallet_store.json"
SNAPSHOT_FILE       = "hybrid_snapshot_output.json"   # latest run only
ANALYSIS_HISTORY    = "analysis_history.json"         # ALL mints ever
LOG_DIR             = os.path.join("reports", "coin_tracker")
os.makedirs(LOG_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
#  RUN MODES
# ---------------------------------------------------------------------------
READ_ALL_HISTORY    = False        # True ⇒ use ANALYSIS_HISTORY.json
TWITTER_ENABLED     = False        # set True if you have a Twitter key
ALERT_THRESHOLD_VOL_SPIKE = 1.8    # 10-min vol ≥ 1.8× 30-min SMA
TELEGRAM_COOLDOWN   = 2 * 60 * 60  # min seconds between alerts per mint

# ---------------------------------------------------------------------------
#  THIRD-PARTY KEYS
# ---------------------------------------------------------------------------
BIRDEYE_API_KEY     = os.getenv("BIRDEYE_API_KEY")
HELIUS_API_KEY      = os.getenv("HELIUS_API_KEY")
TWITTER_BEARER      = os.getenv("TWITTER_BEARER_TOKEN")
TG_TOKEN            = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT             = os.getenv("TELEGRAM_CHAT_ID")

# ---------------------------------------------------------------------------
#  HELPERS
# ---------------------------------------------------------------------------
def tstamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def send_telegram(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        print("[!] Telegram creds missing – skipping alert")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT, "text": msg}, timeout=15)
    except requests.RequestException as e:
        print(f"[!] Telegram send failed: {e}")

send_telegram("✅ Telegram wiring works!")        # ← test ping


def load_json(path: str) -> Any:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}

wallet_store = load_json(WALLET_STORE_FILE)
winning_wallets: Set[str] = set(wallet_store.keys())

analysis_mints: List[str] = []
if READ_ALL_HISTORY:
    analysis_mints = list(load_json(ANALYSIS_HISTORY).keys())
else:
    snap = load_json(SNAPSHOT_FILE)
    analysis_mints = [d["mint"] for d in snap.values()] if snap else []

print(f"[{tstamp()}] Tracking {len(analysis_mints)} candidate mints")

# ---------------------------------------------------------------------------
#  FETCH helpers
# ---------------------------------------------------------------------------
# ------------------------------------------------------------------
#  Solana token-info fetch  (returns None on error)
# ------------------------------------------------------------------
# ------------------------------------------------------------------
#  Solana token-info fetch  (caches & retries once on 429)
# ------------------------------------------------------------------
# ------------------------------------------------------------------
#  Get Solana token overview (price, liquidity, taxes, LP-lock, …)
# ------------------------------------------------------------------
_token_cache: Dict[str, Optional[Dict[str, Any]]] = {}

def birdeye_token(mint: str) -> Optional[Dict[str, Any]]:
    """Return Birdeye token_overview dict or None on error."""
    if mint in _token_cache:
        return _token_cache[mint]

    url  = "https://public-api.birdeye.so/defi/token_overview"
    hdrs = {
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain":  "solana",          # <— mandatory for Solana
        "accept":   "application/json",
    }
    params = {"address": mint}

    for attempt in (1, 2):            # one retry on 429
        try:
            resp = requests.get(url, headers=hdrs, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data")
            _token_cache[mint] = data
            return data
        except requests.HTTPError as e:
            if resp.status_code == 429 and attempt == 1:
                print(f"[!] 429 on {mint[:4]}…  back-off 1.2 s")
                time.sleep(1.2)
                continue
            print(f"[!] Birdeye token_overview failed for {mint[:4]}… — {e}")
            _token_cache[mint] = None
            return None
        except requests.RequestException as e:
            print(f"[!] Network error {mint[:4]}… — {e}")
            _token_cache[mint] = None
            return None




def fetch_price_series(mint: str, mins: int = 30) -> List[Dict[str, Any]]:
    url = (f"https://public-api.birdeye.so/defi/chart/price?"
           f"address={mint}&range=1H&apikey={BIRDEYE_API_KEY}")
    try:
        r = requests.get(url, timeout=30); r.raise_for_status()
        data = r.json().get("data", [])
        # keep last <mins> points
        return data[-mins:]
    except requests.RequestException:
        return []

def twitter_buzz(symbol: str) -> int:
    if not TWITTER_ENABLED or not TWITTER_BEARER:
        return 0
    q = f"${symbol} OR #{symbol}"
    url = ("https://api.twitter.com/2/tweets/search/recent"
           f"?query={q}&max_results=100")
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {TWITTER_BEARER}"}, timeout=20)
        r.raise_for_status()
        return len(r.json().get("data", []))
    except requests.RequestException:
        return 0

def wallet_holds(mint: str, wallet: str) -> bool:
    url = (f"https://api.helius.xyz/v0/addresses/{wallet}/balances?"
           f"api-key={HELIUS_API_KEY}")
    try:
        r = requests.get(url, timeout=30); r.raise_for_status()
        for tkn in r.json().get("tokens", []):
            if tkn.get("mint") == mint and int(tkn.get("amount", 0)) > 0:
                return True
    except requests.RequestException:
        pass
    return False

# cache per run
_wallet_hold_cache: Dict[str, bool] = {}
def any_winning_wallet_holds(mint: str) -> bool:
    if mint in _wallet_hold_cache:
        return _wallet_hold_cache[mint]
    for w in winning_wallets:
        if wallet_holds(mint, w):
            _wallet_hold_cache[mint] = True
            return True
    _wallet_hold_cache[mint] = False
    return False

# ---------------------------------------------------------------------------
#  COIN STATUS STORAGE (so we don’t alert repeatedly)
# ---------------------------------------------------------------------------
TRACK_FILE = os.path.join(LOG_DIR, "coin_status.json")
coin_status = load_json(TRACK_FILE)           # mint → dict

def save_coin_status():
    json.dump(coin_status, open(TRACK_FILE, "w"), indent=2)

# ---------------------------------------------------------------------------
#  MAIN LOOP  (every 2 min)
# ---------------------------------------------------------------------------
while True:
    for mint in analysis_mints:

        # ── guard: skip obviously bad / malformed mints ──────────────
        if len(mint) != 44 or not mint.isascii():
            print(f"[•] {mint[:4]}… looks malformed – skip")
            continue
        # ─────────────────────────────────────────────────────────────

        stat = coin_status.get(mint, {"stage": "new"})

        now = int(time.time())

        # ----- stage: rug-check -----------------------------------------
        if stat["stage"] == "new":
            tok = birdeye_token(mint)
            if not tok:
                print(f"[•] {mint[:4]}…  Birdeye data unavailable, skip this cycle")
                continue
            if (tok.get("canBuy") and tok.get("canSell") and
                float(tok.get("buyTax", 0))  <= 10 and
                float(tok.get("sellTax", 0)) <= 10 and
                float(tok.get("ownersPercent", 0)) < 5 and
                tok.get("isLiquidityLocked", False)):
                stat.update({
                    "symbol": tok.get("symbol", "N/A"),
                    "entry_price": float(tok.get("price", 0)),
                    "stage": "rug_ok"
                })
            else:
                print(f"[•] {mint[:4]}…  Rug-check failed")
                stat["stage"] = "failed_rug"
            coin_status[mint] = stat; save_coin_status()

        # ----- stage: wallet alignment ----------------------------------
        if stat["stage"] == "rug_ok":
            if any_winning_wallet_holds(mint):
                stat["stage"] = "wallet_ok"
                stat["wallet_ok_ts"] = now
                coin_status[mint] = stat; save_coin_status()
            else:
                continue

        # ----- stage: chart monitor ------------------------------------
        if stat["stage"] == "wallet_ok":
            series = fetch_price_series(mint, 30)
            if len(series) < 30:
                print(f"[•] {mint[:4]}…  <30 price points yet")
                continue
            prices  = [p["value"] for p in series]
            volumes = [p["volume"] for p in series]
            sma30   = statistics.mean(prices)
            sma10   = statistics.mean(prices[-10:])
            vol_sma30 = statistics.mean(volumes)
            vol_last10 = statistics.mean(volumes[-10:])
            if prices[-1] > sma30 and vol_last10 > ALERT_THRESHOLD_VOL_SPIKE * vol_sma30:
                # (optional) twitter buzz check
                buzz = twitter_buzz(stat["symbol"]) if TWITTER_ENABLED else 0
                if buzz >= 5 or not TWITTER_ENABLED:   # ≥5 tweets last hour
                    last_alert = stat.get("last_alert_ts", 0)
                    if now - last_alert > TELEGRAM_COOLDOWN:
                        msg = (f"📈 *Entry signal* for {stat['symbol']}  \n"
                               f"Price: {prices[-1]:.6f}  (above 30-min SMA {sma30:.6f})\n"
                               f"10-min vol {vol_last10/1e3:.1f}k  "
                               f"({vol_last10/vol_sma30:.1f}× 30-min avg)\n"
                               f"https://birdeye.so/token/{mint}")
                        send_telegram(msg)
                        stat["last_alert_ts"] = now
                        coin_status[mint] = stat; save_coin_status()
    # ----- daily rotate log -------------------------------------------
    today_file = os.path.join(
        LOG_DIR,
        f"coin_tracker_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json")
    json.dump(coin_status, open(today_file, "w"), indent=2)
    time.sleep(120)   # 2-minute tick
