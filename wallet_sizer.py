# wallet_sizer.py
# Middleware between TM and the live executor:
# TM -> scenarios_exec_in.jsonl --[this script]--> scenarios_exec.jsonl -> TE2
# - Preserves milestone logic (withdraw X0 at doubles, then size from retained profits).
# - Adds dynamic base capital: uses a % of live USDC balance (ALLOC_PCT_OF_USDC) minus reserve.
# - Enforces SOL fee buffer and USDC cap before forwarding scenarios.
# - Pass-through for non-buy messages (e.g., exits/controls).

from __future__ import annotations
import os, json, time, requests
from typing import Dict, Optional
from dotenv import load_dotenv
load_dotenv(override=True)

# ========= Verbose toggle (safe; logs only) =========
VERBOSE = int(os.getenv("SIZER_VERBOSE", "1"))
def vprint(*args, **kwargs):
    if VERBOSE:
        print("[sizer]", *args, **kwargs)

# --------- ENV / IO ---------
SCN_IN  = os.getenv("SCENARIOS_EXEC_IN", "scenarios_exec_in.jsonl")
SCN_OUT = os.getenv("SCENARIOS_EXEC_OUT", "scenarios_exec_sized.jsonl")
BANKROLL_STATE = os.getenv("BANKROLL_STATE", "bankroll_state.json")

# Bankroll / milestones (kept from original behavior)
START_BANKROLL_USD = float(os.getenv("START_BANKROLL_USD", "150"))
MILESTONE_WITHDRAWALS = int(os.getenv("MILESTONE_WITHDRAWALS", "2"))

# Assume N concurrent trades even if fewer signals have arrived
SIZER_ASSUME_MIN_CONCURRENCY=3     # default 1 (off)

# Enforce that a single trade may not exceed its slice (recommended = 1)
SIZER_STRICT_SLICE_CAP=1           # 1 = on, 0 = off



# Wallet / RPC (read-only here)
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
TRADER_PUBLIC_KEY = os.getenv("TRADER_PUBLIC_KEY", "")  # base58 address

# Base token to spend (typically USDC)
BASE_INPUT_MINT = os.getenv("BASE_INPUT_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")  # USDC
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))

# SOL fee safety (kept)
SOL_FEE_BUFFER_SOL   = float(os.getenv("SOL_FEE_BUFFER_SOL", "0.01"))
SOL_MIN_REQUIRED_SOL = float(os.getenv("SOL_MIN_REQUIRED_SOL", "0.003"))

# Position floors/caps (kept)
BASE_POSITION_SIZE = float(os.getenv("BASE_POSITION_SIZE", "10"))
MAX_POSITION_SIZE  = float(os.getenv("MAX_POSITION_SIZE", "2000"))

# >>> Dynamic bankroll knobs (NEW) <<<
# X_live = max(0, USDC_balance - USDC_RESERVE) * ALLOC_PCT_OF_USDC
ALLOC_PCT_OF_USDC = float(os.getenv("ALLOC_PCT_OF_USDC", "0.90"))
USDC_RESERVE      = float(os.getenv("USDC_RESERVE", "0"))
MAX_OPEN_TRADES   = int(os.getenv("MAX_OPEN_TRADES", "3"))
MIN_TRADE_USD     = float(os.getenv("MIN_TRADE_USD", "5"))
MAX_TRADE_USD     = float(os.getenv("MAX_TRADE_USD", "2000"))


OPEN_POS_DIR = os.getenv("OPEN_POS_DIR", "open_positions")
SCN_DEFER = os.getenv("SCENARIOS_EXEC_DEFERRED", "scenarios_exec_deferred.jsonl")


# --------- Small IO helpers ---------
def _touch(p: str):
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8"): pass

_touch(SCN_IN); _touch(SCN_OUT)

class Tailer:
    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        _touch(path)
    def read_new(self):
        out=[]
        with open(self.path,"r",encoding="utf-8") as f:
            f.seek(self.pos)
            for line in f:
                s=line.strip()
                if not s: continue
                try: out.append(json.loads(s))
                except: pass
            self.pos=f.tell()
        if out:
            vprint(f"read {len(out)} new scenario line(s) from {self.path}")
        return out

def append_jsonl(path: str, obj: dict):
    with open(path,"a",encoding="utf-8") as f:
        f.write(json.dumps(obj)+"\n")

def slot_count() -> int:
    try:
        return len([f for f in os.listdir(OPEN_POS_DIR) if f.endswith(".json")])
    except Exception:
        return 0

def slots_full(max_open: int) -> bool:
    return slot_count() >= max_open


# --------- Minimal RPC helpers (raw JSON-RPC; no extra deps) ---------
def _rpc(method: str, params):
    try:
        r = requests.post(
            RPC_URL,
            json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
            timeout=10
        )
        r.raise_for_status()
        return r.json().get("result")
    except Exception:
        return None

def sol_balance_sol(owner_b58: str) -> float:
    res = _rpc("getBalance", [owner_b58, {"commitment":"processed"}])
    lamports = (res or {}).get("value", 0)
    return lamports / 1_000_000_000

def usdc_balance(owner_b58: str) -> float:
    # getTokenAccountsByOwner (jsonParsed) then sum amounts
    res = _rpc("getTokenAccountsByOwner", [owner_b58,
           {"mint": USDC_MINT},
           {"encoding":"jsonParsed", "commitment":"processed"}])
    total = 0
    for acc in (res or {}).get("value", []):
        try:
            amt = int(acc["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
            total += amt
        except Exception:
            pass
    return total / (10 ** USDC_DECIMALS)

# --------- Bankroll state (kept) ---------
def load_bankroll():
    try:
        return json.load(open(BANKROLL_STATE,"r",encoding="utf-8"))
    except:
        return {
            "start_bankroll": START_BANKROLL_USD,          # initial snapshot (X0); we may override at first run
            "current_equity": START_BANKROLL_USD,          # executor can update; fallback to X_live
            "withdrawals_done": 0,
            "next_milestone": START_BANKROLL_USD*2.0,
            "last_update_ts": int(time.time())
        }

def save_bankroll(bk: dict):
    json.dump(bk, open(BANKROLL_STATE,"w",encoding="utf-8"), indent=2)

# --------- Dynamic base capital from wallet (NEW) ---------
def dynamic_base_capital_usd() -> float:
    """
    X_live = max(0, USDC_balance - USDC_RESERVE) * ALLOC_PCT_OF_USDC
    Falls back to START_BANKROLL_USD if TRADER_PUBLIC_KEY missing.
    """
    if not TRADER_PUBLIC_KEY:
        return START_BANKROLL_USD
    usdc = usdc_balance(TRADER_PUBLIC_KEY)
    spendable = max(0.0, usdc - USDC_RESERVE)
    X = spendable * ALLOC_PCT_OF_USDC
    return X

# --------- Sizing policy (kept + enhanced) ---------
def bankroll_phase_size() -> float:
    """
    Milestone behavior preserved:
      - Before completing MILESTONE_WITHDRAWALS: allocate X_live / MAX_OPEN_TRADES per trade.
      - After milestones: allocate from retained profits (equity - wd*X0) / MAX_OPEN_TRADES.
    Where:
      - X_live is from the *current* wallet (dynamic).
      - X0 is the *first snapshot* stored in bankroll_state (used for milestone math).
    Applies floors/caps and remains compatible with original behavior.
    """
    bk = load_bankroll()

    # live dynamic capital
    X_live = dynamic_base_capital_usd()

    # set initial X0 snapshot if missing; keep it stable for milestones
    X0 = float(bk.get("start_bankroll", 0)) or X_live
    if bk.get("start_bankroll") != X0:
        bk["start_bankroll"] = X0
    # current equity: if executor writes it, use that; else use X_live as conservative fallback
    eq = float(bk.get("current_equity", 0)) or X_live
    wd = int(bk.get("withdrawals_done", 0))

    # Pre-milestone: equal split of X_live
    if wd < MILESTONE_WITHDRAWALS:
        per_trade = X_live / max(1, MAX_OPEN_TRADES)
        size = per_trade
    else:
        # Post-milestone: allocate from retained profits relative to X0
        retained = max(0.0, eq - wd * X0)
        size = retained / max(1, MAX_OPEN_TRADES)

    # Apply floors/caps (respect original BASE/MAX)
    size = max(size, max(MIN_TRADE_USD, BASE_POSITION_SIZE))
    size = min(size, min(MAX_TRADE_USD, MAX_POSITION_SIZE))

    # Note: we don't save here; state is updated by executor on exits (equity/milestones).
    return float(size)

# --------- Hard wallet cap (kept) ---------
def wallet_cap_usd() -> float:
    """
    Cap spend by wallet balances + SOL fee safety.
    If BASE_INPUT_MINT=USDC → cap by USDC (but still require SOL buffer).
    If BASE_INPUT_MINT=WSOL → rough USD cap by spendable SOL * 100 (placeholder),
                              recommended to keep USDC as base for strict USD caps.
    """
    if not TRADER_PUBLIC_KEY:
        return float("inf")  # sim/dev mode
    sol = sol_balance_sol(TRADER_PUBLIC_KEY)
    if sol < SOL_MIN_REQUIRED_SOL:
        return 0.0
    # always reserve SOL buffer for fees
    if sol - SOL_FEE_BUFFER_SOL < 0:
        return 0.0

    if BASE_INPUT_MINT == USDC_MINT:
        # cap by available USDC
        return max(0.0, usdc_balance(TRADER_PUBLIC_KEY))

    if BASE_INPUT_MINT == WSOL_MINT:
        # if you ever spend SOL directly, convert here; crude placeholder:
        return max(0.0, (sol - SOL_FEE_BUFFER_SOL) * 100.0)

    return float("inf")

def final_size_usd() -> float:
    """
    Final per-trade USD size.
    - Start from live allocatable capital X_live
    - Divide by an assumed concurrency divisor (so first trade doesn't take all)
    - Apply floors/caps, then hard-cap at slice size if STRICT_SLICE_CAP=1
    - Finally clamp to wallet cap (balance & SOL fee safety)
    """
    # 1) How much we can use in total right now
    X_live = dynamic_base_capital_usd()  # (USDC - reserve) * alloc_pct

    # 2) Divisor (how many trades we assume will be open concurrently)
    assume_min = int(os.getenv("SIZER_ASSUME_MIN_CONCURRENCY", "1"))
    divisor = max(1, max(assume_min, int(os.getenv("MAX_OPEN_TRADES", "3"))))

    # 3) Raw per-trade slice
    slice_usd = X_live / float(divisor)

    # 4) Floors & usual caps
    base_pos = float(os.getenv("BASE_POSITION_SIZE", "0"))
    min_usd  = float(os.getenv("MIN_TRADE_USD", "0"))
    max_usd  = float(os.getenv("MAX_TRADE_USD", "1e12"))  # effectively off if not set
    max_pos  = float(os.getenv("MAX_POSITION_SIZE", "1e12"))

    size = max(slice_usd, max(min_usd, base_pos))
    size = min(size, min(max_usd, max_pos))

    # 5) Optional: enforce strict "slice ceiling" so one trade can't exceed its 1/N share
    strict_slice_cap = int(os.getenv("SIZER_STRICT_SLICE_CAP", "1")) == 1
    if strict_slice_cap:
        size = min(size, slice_usd)

    # 6) Final safety clamp to wallet capacity (includes SOL fee buffer logic)
    cap = wallet_cap_usd()
    final = max(0.0, min(size, cap))
    return final


# ---- Startup info (after functions so Pylance is happy) ----
vprint(f"ready. reading: {SCN_IN}  →  writing: {SCN_OUT}")
if TRADER_PUBLIC_KEY:
    try:
        vprint(
            f"wallet: {TRADER_PUBLIC_KEY[:6]}… | "
            f"usdc≈{usdc_balance(TRADER_PUBLIC_KEY):.4f} | "
            f"sol≈{sol_balance_sol(TRADER_PUBLIC_KEY):.6f}"
        )
    except Exception:
        vprint("wallet preflight: could not fetch balances (will retry during sizing)")

# --------- Main loop (kept; with helpful logs) ---------
def main():
    tail = Tailer(SCN_IN)
    while True:
        try:
            new_items = tail.read_new()
            for scn in new_items:
                side = (scn.get("side") or "buy").lower()
                mint = scn.get("mint") or "UNK"
                symbol = scn.get("symbol") or mint[:6]
                vprint(f"parse scenario: {symbol} ({mint[:6]}), side={side}")

                if side != "buy":
                    # pass-through exits/anything else untouched
                    append_jsonl(SCN_OUT, scn)
                    vprint(f"pass-through (non-buy): wrote {symbol} → {SCN_OUT}")
                    continue

                try:
                    desired = float(final_size_usd())
                    max_open = int(os.getenv("MAX_OPEN_TRADES", "3"))
                    if slots_full(max_open):
                        scn["meta"] = {**(scn.get("meta") or {}), "sizer_deferred": "slots_full"}
                        append_jsonl(SCN_DEFER, scn)
                        vprint(f"defer {symbol}: slots full ({slot_count()}/{max_open}) → {SCN_DEFER}")
                        continue


                    cap     = float(wallet_cap_usd())
                    final   = 0.0 if cap <= 0 else min(desired, cap)

                    if final <= 0:
                        scn["meta"] = {**(scn.get("meta") or {}), "sizer_skip":"insufficient_wallet_or_sol"}
                        # visibility with live balances at decision time
                        try:
                            u = usdc_balance(TRADER_PUBLIC_KEY) if TRADER_PUBLIC_KEY else -1.0
                            s = sol_balance_sol(TRADER_PUBLIC_KEY) if TRADER_PUBLIC_KEY else -1.0
                            vprint(
                                f"skip {symbol}: desired={desired:.2f}, cap={cap:.2f}, final=0.00 | "
                                f"usdc={u:.4f}, sol={s:.6f} "
                                f"(need ≥ {SOL_MIN_REQUIRED_SOL}+buf {SOL_FEE_BUFFER_SOL})"
                            )
                        except Exception:
                            vprint(f"skip {symbol}: desired={desired:.2f}, cap={cap:.2f}, final=0.00 | (could not fetch balances)")
                        # drop instead of forwarding
                        continue

                    scn["size_usd"] = float(final)
                    scn["meta"] = {
                        **(scn.get("meta") or {}),
                        "sized_by": "wallet_sizer.py",
                        "desired": round(desired, 2),
                        "cap":     round(cap, 2),
                    }
                    append_jsonl(SCN_OUT, scn)
                    vprint(f"forward {symbol}: size_usd=${final:.2f} → {SCN_OUT}")

                except Exception as e:
                    # never crash loop on a bad scenario; log marker into outgoing queue for visibility
                    scn["meta"] = {**(scn.get("meta") or {}), "sizer_error": f"{type(e).__name__}: {e}"}
                    append_jsonl(SCN_OUT, scn)
                    vprint(f"error on {symbol}: {type(e).__name__}: {e} (wrote error-tagged scenario)")
        except KeyboardInterrupt:
            vprint("stopped by user")
            break
        except Exception as e:
            vprint(f"fatal: {type(e).__name__}: {e}")
            time.sleep(0.5)

        time.sleep(0.25)

if __name__ == "__main__":
    main()
