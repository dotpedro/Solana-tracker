# 🚀 **Solana Tracker — Automated Token Intelligence, Bottom Detection & (Optional) Auto-Execution**

This repository contains the full evolution of the **Solana Tracker** project:
a multi-stage intelligence system designed to discover early tokens, analyze wallet behavior, detect bottoms, confirm reversals, and optionally execute trades (paper or live) with a full risk engine.

Over time, the project grew to include many experimental detectors, old prototypes, early versions, and scrapers.
**These deprecated modules remain intentionally** to preserve transparency, research value, and historical development context.

This README documents:

* Which components are **active** and maintained
* Which ones are **deprecated**
* Why deprecated modules remain
* How to install + run the stable and experimental parts
* How the entire pipeline fits together

---

# 📌 **Why This Repo Contains Deprecated Code**

Several modules are no longer part of the main active pipeline — scrapers, old processors, early prototype analyzers, and outdated detectors.

They remain for **three important reasons**:

### **1. Research Value**

Many early algorithms, heuristics, and detectors contain insights that may be reused later.

### **2. Transparency**

Solana Tracker is a complex ecosystem; keeping the full history helps others understand how the system evolved.

### **3. Backward Compatibility**

Some forks and older setups still rely on earlier versions of the pipeline.

> 💡 **Deprecated modules are not recommended for active use**, but are kept for documentation and future inspiration.

---

# ✔️ **Active Pipeline (Fully Functional Today)**

These are the modules that form the **current, stable core** of the system.

---

## 🟩 **1. `main_loop_analyzer2.py` — Token Discovery Engine**

* Pulls new tokens and candiate data from Helius / Birdeye
* Identifies early wallets
* Produces raw structured reports
* Writes output to:

```
reports/holdings/pending/
```

This script is the **entry point** of the intelligence pipeline.

---

## 🟩 **2. `report_processor.py` — Enrichment & Validation**

Consumes raw reports and enriches them with:

* Liquidity, MC, supply data
* Holder statistics
* Wallet concentration
* Volume & price checks
* Optional GMGN metadata
* Cooldowns & processed caching

Writes validated reports to:

```
reports/holdings/processed/
```

This is where “raw signals” become clean, actionable intelligence.

---

## 🟩 **3. `monitor_alerts.py` — Telegram Alert Engine**

* Reads processed reports
* Detects changes (new entries, updates, disqualifications)
* Prevents duplicate alerts
* Pushes messages through the unified Telegram alert layer

Minimal, stable, and important for daily use.

---

## 🟩 **4. `telegram_command_router.py` — Interactive Bot**

A fully interactive Telegram bot offering:

* `/check <CA>` – safety audit
* `/eco_on` / `/eco_off`
* `/my_tracked`
* `/untrack <CA>`
* `/recheck_mint <MINT>`
* `/api_usage`
* Many more…

This script allows you to manually query the system from Telegram.

---

## 🟩 **5. `telegram_alert_router.py` — Core Messaging Layer**

All Telegram alerts funnel through this module.

Handles:

* Markdown safety
* Retry logic
* HTML escaping
* Multi-bot routing
* Format presets for multiple types of alerts

---

# 🧪 **Bottoming & Execution Stack (Experimental but Live)**

The following components are **the newest additions**.
They are still evolving but already **minimally functional** end-to-end.

---

## 🔷 **1. `bottom_detector.py` — Bottom Detector (BD)**

Purpose: **Find probable bottoms** based on report data + recovery patterns.

Uses:

* Processed reports
* Volume + MC windows
* Exhaustion / rebid conditions
* Wallet-set behavior
* Optional GPT gating (`OPENAI_API_KEY`)

Outputs:

```
watchlist.jsonl   ← potential bottoms to monitor
scenarios.jsonl   ← structured bottom scenarios
```

Logs to Telegram with status hints.

**Status:**
Experimental but stable enough for real usage.

---

## 🔷 **2. `exhaustion_monitor.py` — Exhaustion Monitor (EM)**

Purpose: **Confirm bottoms** using real-time price action.

Uses:

* Birdeye WebSocket stream
* Watchlist from Bottom Detector
* Recovery vs continuation logic
* Live microstructure checks

Outputs promotions to:

```
SCENARIOS_IN/
```

Maintains:

```
relief_registry.json
```

**Status:**
Middle layer of the stack. Fully functional.

---

## 🔷 **3. `trade_manager.py` — Trade Manager (TM)**

Purpose: **Brains of the execution system**.

Responsibilities:

* Enforces `MAX_CONCURRENT_TRADES`
* Accepts scenarios from EM
* Computes:

  * Bottom quality (BQ)
  * Risk/reward
  * Liquidity gates
* Writes scenarios to:

```
SCENARIOS_EXEC/
```

* Manages:

  * `open_positions/`
  * `deadlist.json`
  * `entry_registry.json`
  * Backlog candidates
* Optional GPT arbitration (`GPT_SWAP_ON`, `OPENAI_API_KEY`)

**Status:**
Actively developed. Safe for supervised use.

---

## 🔷 **4. `trade_executioner.py` (v1) — Paper Trading Simulator**

* No real swaps
* Uses performance logic identical to live executor
* Logs positions to:

  * `open_positions/`
  * `trades.jsonl`
  * `equity_curve.csv`

**Status:**
Perfect for testing BD → EM → TM → Execution flow **without real risk**.

---

## 🔷 **5. `trade_executioner2.py` (v2) — Live Executor**

* Executes real Solana trades through Jupiter
* Streams price via Birdeye WS
* Implements:

  * SL, TP, break-even
  * Trailing exits
  * Drift checks
  * Retry loops
  * Disaster SL
  * Position time limits
  * PnL tracking

Reads configuration from:

* RPC URL
* Owner pubkey
* Private key
* Size & risk settings
* LIVE_MODE flag

**Status:**
Early but functional.
Use only with **small size** and full supervision.

---

# 🗂 **Deprecated / Legacy Modules**

Kept for transparency, reference, and backward compatibility.

Includes:

* `pf_narrative_analyzer.py`
* `kol_wallet_alert_scraper.py`
* `pf_daily_summary.py`
* Early holding analyzers
* Legacy scrapers
* Old processors
* Experimental detectors

These remain **in the repo** but are not maintained.

---

# 📦 **Installation**

## 1. Clone the repo

```bash
git clone https://github.com/dotpedro/Solana-tracker.git
cd Solana-tracker
```

## 2. Create a virtual environment

```bash
python -m venv venv
venv\Scripts\activate     # Windows
# or
source venv/bin/activate  # macOS/Linux
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

# 📁 **Project Requirements**

The `requirements.txt` contains everything needed for:

* Core intelligence pipeline
* Bottom detector
* Exhaustion monitor
* Trade manager
* Paper/live execution
* Telegram bot
* Solana WS listener

The stack includes:

* requests
* python-dotenv
* schedule
* tqdm
* colorama
* numpy
* pandas
* pytz / dateutil / tzdata
* httpx
* websocket-client
* websockets
* python-telegram-bot
* solana / solders / construct / base58
* openai
* beautifulsoup4 + selenium (for deprecated scraper)

---

# 🔐 **Environment Variables**

Copy example:

```bash
copy .env.example .env
```

Fill the following:

### Required

* `HELIUS_API_KEY`
* `BIRDEYE_API_KEY`
* `TELEGRAM_BOT_TOKEN`
* `TELEGRAM_CHAT_ID`

### Bottom Stack

* `ENABLE_GPT_GATE`
* `OPENAI_API_KEY`

### Execution

* `LIVE_MODE`
* `RPC_URL`
* `OWNER_PUBKEY`
* `PRIVATE_KEY`

### Optional

* Processor-specific Telegram tokens
* GMGN API keys
* Eco Mode configuration

---

# ▶️ **Running the Core Pipeline**

Start each script in its own terminal or tmux pane:

### **1. Token discovery**

```bash
python main_loop_analyzer2.py
```

### **2. Report processing**

```bash
python report_processor.py
```

### **3. Telegram alerts**

```bash
python monitor_alerts.py
```

### **4. Telegram bot**

```bash
python telegram_command_router.py
```

---

# ▶️ **Running the Bottom + Execution Stack (Experimental)**

### **1. Bottom Detector**

```bash
python bottom_detector.py
```

### **2. Exhaustion Monitor**

```bash
python exhaustion_monitor.py
```

### **3. Trade Manager**

```bash
python trade_manager.py
```

### **4. Executor (paper mode or live)**

```bash
python trade_executioner.py      # Paper
python trade_executioner2.py     # Live (EXTREME CAUTION)
```

---

# 🧭 **Architecture Overview**

```
main_loop_analyzer2.py
      ↓
reports/holdings/pending/
      ↓
report_processor.py
      ↓
reports/holdings/processed/
      ↓
monitor_alerts.py
      ↓
Telegram Alerts

Experimental Bottoming + Execution Path:
bottom_detector.py
      ↓
watchlist.jsonl
      ↓
exhaustion_monitor.py
      ↓
SCENARIOS_IN/
      ↓
trade_manager.py
      ↓
SCENARIOS_EXEC/
      ↓
trade_executioner (paper/live)
```

---

# 🧨 **Warnings**

* **LIVE_MODE must always default to OFF**
* Test everything first in **paper trading (v1)**
* Use small size when enabling v2
* Ensure your RPC provider is reliable
* Private keys must stay in `.env` and never be committed

---

# 🎯 **Conclusion**

This repository contains:

* The **stable intelligence pipeline**
* The **experimental bottoming + execution stack**
* The **entire historical evolution** for transparency

Whether you want alerts only, bottom detection, or full automated execution — the system is built so you can choose your level of involvement.

💬 **How this relates to NOF1** (Compared to NOF1 https://nof1.ai/)

This project is essentially my own version of NOF1, but built specifically for Solana on-chain markets, with a heavier focus on:

real-time blockchain data

wallet-set behaviour

bottom detection

execution logic tied directly to on-chain liquidity

transparent and reproducible pipelines


🤖 **How This Project Uses AI** 

NOF1 builds end-to-end machine-learning agents that make full trading decisions inside a competitive arena.
Their models attempt to learn everything: detection, confirmation, risk management, sizing, entries, exits, the whole pipeline.

This project takes a completely different approach.

Instead of using AI to replace the decision pipeline, AI is used in Trade Manager only, and only for specific arbitration tasks, such as:

validating borderline bottom scenarios

scoring risk–reward vs. market conditions

providing a sanity-check when multiple signals conflict

improving prioritization inside queues

adding semantic understanding to certain token contexts

optional execution arbitration (GPT_SWAP_ON) when enabled

This means:

🔹 AI does not “trade for you”

It does not decide entries, exits, or risk.
Those are controlled by your deterministic logic.

🔹 AI acts as a second-opinion, not a decision-maker

It’s more like a safety filter or tie-breaker, making the system:

more robust

less impulsive

less dependent on brittle heuristics

🔹 The core logic is transparent and reproducible

Bottom detection, exhaustion monitoring, trade sizing, SL/TP behaviour — all of that remains fully rules-based.

🔹 No model training, no black-box intelligence

Unlike NOF1’s ML agents, you aren’t training anything.
You use GPT as an advisory layer, not a predictive engine.

🔹 You control exactly where AI is used

AI is optional and only activated when:

ENABLE_GPT_GATE=1

GPT_SWAP_ON=1

You can turn GPT off at any time, and the system still works end-to-end.

## 📸 Showcase

### 🏔️ Mount Rushmore of Main Loop Analyzer Catches (top 3) (Q2–Q4)

Some of the best calls the main loop analyzer has made over the last quarters.

<div align="center">

<img width="660" alt="Main loop analyzer catch 1" src="https://github.com/user-attachments/assets/274fbad7-5603-4b69-a34e-fd9dfc7ae761" />
<br/>

<img width="634" alt="Main loop analyzer catch 2" src="https://github.com/user-attachments/assets/76012654-9018-4044-8b2b-28694e91957b" />
<br/>

<img width="558" alt="Main loop analyzer catch 3" src="https://github.com/user-attachments/assets/a77f6617-343b-4212-8e97-366c42f0ed2e" />

</div>

These are examples of tokens that were flagged early by the discovery + processing pipeline and later went on to perform extremely well.


### 🤖 Automated Trading (top 3 - still in early testing) (Paper + Live)

The bottoming + execution stack in action – from scenario promotion to fully automated trade management.

<div align="center">

<img width="646" alt="Automated trading 1" src="https://github.com/user-attachments/assets/78a1ec37-7128-43ac-a9fa-e5b39603e99e" />
<br/>

<img width="674" alt="Automated trading 2" src="https://github.com/user-attachments/assets/b2ce60c6-4318-4131-9818-5ffed38030ca" />
<br/>

<img width="659" alt="Automated trading 3" src="https://github.com/user-attachments/assets/2a086a63-f15c-4634-8ebd-2e7f6c2b90ab" />

</div>

From these screenshots you can see:

- Bottom Detector + Exhaustion Monitor promoting scenarios into the queue  
- Trade Manager accepting, sizing and scheduling entries  
- The execution layer handling entries, exits, PnL and equity curve updates automatically

> ⚠️ **Note:** All components of this system — including detection, validation, bottoming logic, and automated execution — are heavily dependent on market conditions. Performance varies with volatility, liquidity, narrative cycles, and overall market structure. Nothing in this repository guarantees profits or consistent results.

