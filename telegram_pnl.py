# telegram_monitor.py
# Comprehensive Telegram monitoring for live trading with PnL, portfolio tracking, and trade history.
# Monitors trades.jsonl and sends rich notifications with analytics.

from __future__ import annotations
import os, json, time, requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(override=True)

# =====================
# ENV / CONFIG
# =====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TRADES_FILE = os.getenv("TRADES_OUT", "trades.jsonl")
TRADER_PUBLIC_KEY = os.getenv("TRADER_PUBLIC_KEY", "")
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
USDC_MINT = os.getenv("USDC_MINT_SOL", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")

MONITOR_INTERVAL_SEC = float(os.getenv("TG_MONITOR_INTERVAL_SEC", "0.5"))
PORTFOLIO_UPDATE_INTERVAL_SEC = int(os.getenv("TG_PORTFOLIO_UPDATE_SEC", "300"))  # 5min

# =====================
# DATA STRUCTURES
# =====================
@dataclass
class Position:
    mint: str
    symbol: str
    entry_price: float
    entry_size_usd: float
    entry_ts: int
    entry_sig: str
    amount_raw: int = 0  # token amount held
    decimals: int = 6

@dataclass
class TradeHistory:
    entries: List[Dict] = field(default_factory=list)
    exits: List[Dict] = field(default_factory=list)
    errors: List[Dict] = field(default_factory=list)

# =====================
# GLOBAL STATE
# =====================
POSITIONS: Dict[str, Position] = {}  # mint -> Position
HISTORY = TradeHistory()
LAST_PORTFOLIO_UPDATE = 0
INITIAL_BALANCE = 0.0  # Set on first run

# =====================
# UTILITIES
# =====================
def _now() -> int:
    return int(time.time())

def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def format_usd(amount: float) -> str:
    return f"${amount:,.2f}"

def format_pct(pct: float) -> str:
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"

# =====================
# TELEGRAM
# =====================
def send_telegram(msg: str, parse_mode: str = "HTML"):
    """Send Telegram message with HTML formatting."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TG] Warning: Telegram credentials not configured")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": parse_mode},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print(f"[TG] Send error: {e}")
        return False

# =====================
# RPC / PRICE FETCHING
# =====================
def rpc_call(method: str, params: list) -> Optional[dict]:
    """Raw JSON-RPC call."""
    try:
        r = requests.post(
            RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=10
        )
        r.raise_for_status()
        return r.json().get("result")
    except Exception as e:
        print(f"[TG] RPC error: {e}")
        return None

def get_usdc_balance() -> float:
    """Get current USDC balance."""
    if not TRADER_PUBLIC_KEY:
        return 0.0
    result = rpc_call("getTokenAccountsByOwner", [
        TRADER_PUBLIC_KEY,
        {"mint": USDC_MINT},
        {"encoding": "jsonParsed"}
    ])
    total = 0
    for acc in (result or {}).get("value", []):
        try:
            amt = int(acc["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
            total += amt
        except:
            pass
    return total / (10 ** USDC_DECIMALS)

def get_sol_balance() -> float:
    """Get current SOL balance."""
    if not TRADER_PUBLIC_KEY:
        return 0.0
    result = rpc_call("getBalance", [TRADER_PUBLIC_KEY])
    lamports = (result or {}).get("value", 0)
    return lamports / 1_000_000_000

def get_token_balance(mint: str) -> Tuple[int, int]:
    """Get token balance (raw amount, decimals)."""
    if not TRADER_PUBLIC_KEY:
        return 0, 6
    result = rpc_call("getTokenAccountsByOwner", [
        TRADER_PUBLIC_KEY,
        {"mint": mint},
        {"encoding": "jsonParsed"}
    ])
    total_raw, decimals = 0, 6
    for acc in (result or {}).get("value", []):
        try:
            info = acc["account"]["data"]["parsed"]["info"]
            ta = info["tokenAmount"]
            total_raw += int(ta["amount"])
            decimals = int(ta.get("decimals", decimals))
        except:
            pass
    return total_raw, decimals

def get_birdeye_price(mint: str) -> Optional[float]:
    """Get current token price from Birdeye."""
    if not BIRDEYE_API_KEY:
        return None
    try:
        url = "https://public-api.birdeye.so/defi/price"
        headers = {
            "accept": "application/json",
            "x-chain": "solana",
            "X-API-KEY": BIRDEYE_API_KEY
        }
        r = requests.get(url, params={"address": mint}, headers=headers, timeout=5)
        if r.status_code == 200:
            data = (r.json() or {}).get("data") or {}
            px = data.get("value")
            return float(px) if px else None
    except:
        pass
    return None

# =====================
# POSITION TRACKING
# =====================
def update_position_balances():
    """Update all position balances from chain."""
    for mint, pos in POSITIONS.items():
        raw, dec = get_token_balance(mint)
        pos.amount_raw = raw
        pos.decimals = dec

def calculate_position_pnl(pos: Position) -> Tuple[float, float, float]:
    """
    Calculate position PnL.
    Returns: (current_value_usd, pnl_usd, pnl_pct)
    """
    if pos.amount_raw == 0:
        return 0.0, 0.0, 0.0
    
    current_price = get_birdeye_price(pos.mint)
    if not current_price:
        return 0.0, 0.0, 0.0
    
    amount = pos.amount_raw / (10 ** pos.decimals)
    current_value = amount * current_price
    pnl_usd = current_value - pos.entry_size_usd
    pnl_pct = (pnl_usd / pos.entry_size_usd * 100) if pos.entry_size_usd > 0 else 0.0
    
    return current_value, pnl_usd, pnl_pct

def calculate_total_portfolio() -> Dict[str, float]:
    """
    Calculate total portfolio value and PnL.
    Returns: {usdc, sol_usd, positions_usd, total_usd, pnl_usd, pnl_pct}
    """
    usdc = get_usdc_balance()
    sol = get_sol_balance()
    
    # Rough SOL price estimate (you could fetch this from Birdeye too)
    sol_usd = sol * 150.0  # Placeholder, update with real price if needed
    
    positions_usd = 0.0
    for pos in POSITIONS.values():
        value, _, _ = calculate_position_pnl(pos)
        positions_usd += value
    
    total_usd = usdc + sol_usd + positions_usd
    
    global INITIAL_BALANCE
    if INITIAL_BALANCE == 0:
        INITIAL_BALANCE = total_usd
    
    pnl_usd = total_usd - INITIAL_BALANCE
    pnl_pct = (pnl_usd / INITIAL_BALANCE * 100) if INITIAL_BALANCE > 0 else 0.0
    
    return {
        "usdc": usdc,
        "sol": sol,
        "sol_usd": sol_usd,
        "positions_usd": positions_usd,
        "total_usd": total_usd,
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct
    }

# =====================
# NOTIFICATION BUILDERS
# =====================
def notify_entry(trade: Dict):
    """Send entry notification."""
    mint = trade.get("mint", "")
    symbol = trade.get("symbol", "UNK")
    size = float(trade.get("size_usd", 0))
    sig = trade.get("sig", "")[:10]
    ts = trade.get("ts", _now())
    
    # Get current price
    price = get_birdeye_price(mint)
    price_str = f"${price:.6f}" if price else "N/A"
    
    msg = f"""
🟢 <b>BUY EXECUTED</b>

<b>Token:</b> {symbol}
<b>Size:</b> {format_usd(size)}
<b>Entry Price:</b> {price_str}
<b>Time:</b> {format_ts(ts)}
<b>Tx:</b> <code>{sig}...</code>

<a href="https://solscan.io/tx/{sig}">View on Solscan</a>
""".strip()
    
    send_telegram(msg)
    
    # Track position
    if mint and price:
        POSITIONS[mint] = Position(
            mint=mint,
            symbol=symbol,
            entry_price=price,
            entry_size_usd=size,
            entry_ts=ts,
            entry_sig=sig
        )
        # Update balance
        raw, dec = get_token_balance(mint)
        POSITIONS[mint].amount_raw = raw
        POSITIONS[mint].decimals = dec

def notify_exit(trade: Dict):
    """Send exit notification with PnL."""
    mint = trade.get("mint", "")
    symbol = trade.get("symbol", "UNK")
    percent = float(trade.get("percent", 100))
    sig = trade.get("sig", "")[:10]
    ts = trade.get("ts", _now())
    
    # Calculate PnL if we have the position
    pos = POSITIONS.get(mint)
    pnl_msg = ""
    
    if pos:
        exit_price = get_birdeye_price(mint)
        if exit_price:
            # Estimate exit value based on percent
            exit_value = pos.entry_size_usd * (percent / 100.0)
            pnl_usd = (exit_price - pos.entry_price) / pos.entry_price * exit_value
            pnl_pct = (exit_price - pos.entry_price) / pos.entry_price * 100
            
            pnl_emoji = "🟢" if pnl_usd >= 0 else "🔴"
            pnl_msg = f"""
<b>Entry:</b> ${pos.entry_price:.6f}
<b>Exit:</b> ${exit_price:.6f}
{pnl_emoji} <b>PnL:</b> {format_usd(pnl_usd)} ({format_pct(pnl_pct)})
"""
        
        # Remove or reduce position
        if percent >= 100:
            del POSITIONS[mint]
        else:
            pos.entry_size_usd *= (1 - percent / 100.0)
    
    msg = f"""
🔴 <b>SELL EXECUTED</b>

<b>Token:</b> {symbol}
<b>Amount:</b> {percent:.1f}%
<b>Time:</b> {format_ts(ts)}
{pnl_msg}
<b>Tx:</b> <code>{sig}...</code>

<a href="https://solscan.io/tx/{sig}">View on Solscan</a>
""".strip()
    
    send_telegram(msg)

def notify_error(trade: Dict):
    """Send error notification."""
    mint = trade.get("mint", "")
    symbol = trade.get("symbol", "UNK")
    err = trade.get("err", "Unknown error")
    trade_type = trade.get("type", "ERROR")
    ts = trade.get("ts", _now())
    
    size = trade.get("size_usd") or trade.get("percent", "N/A")
    
    msg = f"""
❌ <b>TRADE ERROR</b>

<b>Type:</b> {trade_type}
<b>Token:</b> {symbol}
<b>Size:</b> {size}
<b>Error:</b> {err}
<b>Time:</b> {format_ts(ts)}
""".strip()
    
    send_telegram(msg)

def notify_portfolio_update():
    """Send comprehensive portfolio update."""
    update_position_balances()
    portfolio = calculate_total_portfolio()
    
    # Build positions list
    positions_text = ""
    if POSITIONS:
        positions_text = "\n<b>📊 Open Positions:</b>\n"
        for pos in POSITIONS.values():
            value, pnl_usd, pnl_pct = calculate_position_pnl(pos)
            if value > 0:
                emoji = "🟢" if pnl_usd >= 0 else "🔴"
                amount = pos.amount_raw / (10 ** pos.decimals)
                positions_text += f"""
{emoji} <b>{pos.symbol}</b>
  Amt: {amount:.2f} | Value: {format_usd(value)}
  PnL: {format_usd(pnl_usd)} ({format_pct(pnl_pct)})
"""
    
    # Overall PnL emoji
    pnl_emoji = "🟢" if portfolio["pnl_usd"] >= 0 else "🔴"
    
    # Trade stats
    total_entries = len(HISTORY.entries)
    total_exits = len(HISTORY.exits)
    total_errors = len(HISTORY.errors)
    
    msg = f"""
📈 <b>PORTFOLIO UPDATE</b>

<b>💰 Total Value:</b> {format_usd(portfolio["total_usd"])}
{pnl_emoji} <b>Total PnL:</b> {format_usd(portfolio["pnl_usd"])} ({format_pct(portfolio["pnl_pct"])})

<b>💵 Cash:</b> {format_usd(portfolio["usdc"])} USDC
<b>⚡ SOL:</b> {portfolio["sol"]:.4f} (~{format_usd(portfolio["sol_usd"])})
<b>📦 Positions:</b> {format_usd(portfolio["positions_usd"])}
{positions_text}
<b>📊 Trade Stats:</b>
  Entries: {total_entries} | Exits: {total_exits} | Errors: {total_errors}

<b>🕐 Time:</b> {format_ts(_now())}
""".strip()
    
    send_telegram(msg)

# =====================
# FILE MONITORING
# =====================
class TradesMonitor:
    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        self._ensure_file()
    
    def _ensure_file(self):
        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "w", encoding="utf-8"): pass
    
    def read_new(self) -> List[Dict]:
        """Read new trades from file."""
        out = []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                f.seek(self.pos)
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        out.append(json.loads(s))
                    except json.JSONDecodeError:
                        print(f"[TG] Warning: malformed JSON in {self.path}")
                self.pos = f.tell()
        except FileNotFoundError:
            self._ensure_file()
        return out

def process_trade(trade: Dict):
    """Process a trade and send appropriate notification."""
    trade_type = trade.get("type", "")
    
    if trade_type == "ENTRY":
        HISTORY.entries.append(trade)
        notify_entry(trade)
    elif trade_type == "EXIT":
        HISTORY.exits.append(trade)
        notify_exit(trade)
    elif trade_type in ["ERROR_ENTRY", "ERROR_EXIT"]:
        HISTORY.errors.append(trade)
        notify_error(trade)
    elif trade_type == "GUARD":
        # Optional: notify about guards
        pass
    elif trade_type == "WARN":
        # Optional: notify about warnings
        pass
    elif trade_type == "FATAL":
        # Always notify about fatal errors
        err = trade.get("err", "Unknown")
        msg = f"🚨 <b>FATAL ERROR</b>\n\n{err}"
        send_telegram(msg)

# =====================
# MAIN LOOP
# =====================
def main():
    global LAST_PORTFOLIO_UPDATE
    
    print("[TG] Telegram Monitor started")
    print(f"[TG] Monitoring: {TRADES_FILE}")
    print(f"[TG] Update interval: {MONITOR_INTERVAL_SEC}s")
    print(f"[TG] Portfolio updates: every {PORTFOLIO_UPDATE_INTERVAL_SEC}s")
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TG] WARNING: Telegram credentials not configured!")
        print("[TG] Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
    
    # Send startup notification
    send_telegram(f"🤖 <b>Trading Bot Monitor Started</b>\n\n{format_ts(_now())}")
    
    # Initial portfolio snapshot
    portfolio = calculate_total_portfolio()
    global INITIAL_BALANCE
    INITIAL_BALANCE = portfolio["total_usd"]
    print(f"[TG] Initial balance: {format_usd(INITIAL_BALANCE)}")
    
    monitor = TradesMonitor(TRADES_FILE)
    
    try:
        while True:
            # Process new trades
            new_trades = monitor.read_new()
            for trade in new_trades:
                process_trade(trade)
            
            # Periodic portfolio update
            if _now() - LAST_PORTFOLIO_UPDATE >= PORTFOLIO_UPDATE_INTERVAL_SEC:
                notify_portfolio_update()
                LAST_PORTFOLIO_UPDATE = _now()
            
            time.sleep(MONITOR_INTERVAL_SEC)
    
    except KeyboardInterrupt:
        print("\n[TG] Stopped by user")
        send_telegram("🛑 <b>Trading Bot Monitor Stopped</b>")
    except Exception as e:
        print(f"[TG] Fatal error: {e}")
        send_telegram(f"🚨 <b>Monitor Crashed</b>\n\n{e}")

if __name__ == "__main__":
    main()