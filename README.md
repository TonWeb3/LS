# Polymarket BTC 15m Assistant [GTD] (Python FastAPI)

A real-time trading assistant for Polymarket **"Bitcoin Up or Down" 15-minute** markets, ported to Python and FastAPI.

> **📖 Full reference: [`documentation.md`](documentation.md)** — the strategy, the
> strike mechanism, settlement, config, API and known limits, in detail. This README is
> the quick start.

It runs a **latency-arbitrage** strategy: a fast closed-form fair probability from
Binance spot vs Polymarket's (possibly stale) implied price, traded on the gap, with
position size set by a simple percent-of-balance or fixed-dollar risk. See
[`strategy.md`](strategy.md) for the full rationale.

## Features

- Real-time Web Dashboard (FastAPI + Jinja2 + Alpine.js)
- Fast fair-probability model (closed-form GBM) + EV entry engine
- Veto filters: RSI extremes, Heiken-Ashi exhaustion
- Trade Execution: Paper Trading simulation vs Live Mode toggle
- Data Sources: Binance, Polymarket (Gamma/CLOB), Chainlink (WebSocket + RPC)
- Proxy Support: Global HTTP/HTTPS/SOCKS proxy configuration

## Requirements

- Python **3.12+** — required, not preferred. `polymarket-apis` declares
  `Requires-Python >=3.12`; on 3.11 pip ignores every published version and fails with
  the confusing `Could not find a version ... (from versions: none)`.
- pip (comes with Python)

## Local Run

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure `config.json`

Set your trading mode, risk preferences, and optional private key in `config.json`.
Position size is set by `trading.risk_type` (`"percent"` = `risk_value`% of balance,
or `"fixed"` = `risk_value` dollars) and `trading.risk_value`. The entry engine is
tuned in the `ev` block:

```jsonc
"ev": {
  "ev_threshold": 0.04,          // enter only when fair prob − share price ≥ this (the edge gate)
  "min_prob": 0.55,              // never bet near-coinflips even if EV looks positive
  "min_book_liquidity_usd": 20.0 // skip if the ask side can't absorb the stake
}
```

All of these are also editable live on the **Settings** page.

### 3) Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8017
# or simply:
python main.py
```

Access the dashboard at `http://localhost:8017`.

## Docker

```bash
docker build -t polymarket-assistant-gtd .
docker run -p 8017:8017 polymarket-assistant-gtd
```

## Deployment on Render

If you are seeing errors related to Node.js or `npm run start`, it is because Render is auto-detecting the old environment. **You must manually set the runtime to Python.**

### Recommended: Use `render.yaml`
The repository includes a `render.yaml`. When creating a new blueprint on Render, it will automatically set the correct environment.

### Manual Setup
1. Create a **Web Service** on Render.
2. Under **Runtime**, explicitly select **Python 3**.
3. Set the following commands:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port 8017`
4. Add any necessary environment variables (optional).

## Live Trading

Switching **Mode** to `live` makes the bot place real **Good 'Til Date (GTD) limit BUY**
orders on **Polymarket CLOB V2** via `polymarket-apis`, using the gasless
**deposit-wallet** flow.

Your trading wallet is **derived from your key** — there is no signature type or funder
address to choose. The bot checks the deposit, proxy and safe wallets and trades from
whichever actually holds **pUSD**.

1. Set a **private key or 12/24-word seed phrase** (Settings → Credentials, or
   `config.json`). It signs orders but holds no funds and needs no gas.
2. Set the **Relayer API key** (`config.json` → `relayer.api_key`). It sponsors the
   one-time on-chain setup, so you never pay gas. Optionally set an **Alchemy key** for
   a private Polygon RPC.
3. Click **Test Connection** — read-only, no relayer key needed. It lists every derived
   wallet with its pUSD balance and ticks the one that will be traded.
4. **Deposit pUSD** to that wallet through Polymarket.
5. Click **Setup Wallet (gasless)** to deploy + approve it. Once per fresh wallet.
6. *(Optional)* **Enable Auto-Redeem** so wins convert back to pUSD by themselves.

Orders are **slippage-capped**: the limit is the quote plus `CLOB_MAX_SLIPPAGE`
(default 2¢), so if the book moves away the order is killed rather than filled badly. A
fill is only recorded when it is positively confirmed, and the trade is stamped with the
**actual** fill price and size. In live mode the dashboard balance is the real on-chain
pUSD balance (refreshed every 30s). Order failures appear in the Console Log.

**Press Start on the dashboard** — the bot does not trade until you do.

## Safety

This is not financial advice. Use at your own risk; live mode trades real funds.

**The edge is unproven.** The model has no predictive edge over "is spot already above
the open" — that signal is fully priced by the market. The only remaining edge is
latency (beating the book's repricing). Run in **paper mode** and confirm EV-positive
trades actually exist against the real book *before* risking capital.
