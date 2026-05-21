# Hermes Trading Dashboard

A live portfolio command center for Alpaca trading accounts — built as a single-stock chart view of your entire portfolio.

![Python](https://img.shields.io/badge/python-3.12-blue) ![Flask](https://img.shields.io/badge/Flask-3.1-green) ![License](https://img.shields.io/badge/license-MIT-orange)

---

## What it does

Treats your entire Alpaca portfolio **as if it were one stock** — with OHLC candles, ATH tracking, phase detection, stop loss tiers, and an AI trading partner (Hermes/Claude) that can actually execute trades.

### Features

- **Live equity curve** — intraday chart with MA5/MA20 overlays, phase badge (PULLBACK/AT_PEAK/RECOVERY/GRINDING_UP)
- **Hermes AI chat** — Claude with full portfolio context, tool-calling to execute real Alpaca orders on confirmation
- **ATH pullback protocol** — auto-detects session high, fires 4-way split into SGOL/DIA/QQQ/VOO on drawdown
- **Stop loss tiers** — 7 levels (SOFT/WARN/HARD STOP, BREAK-EVEN, PREV ATH, DANGER, PRINCIPAL)
- **Buy list** — EDGAR-scored candidates ranked by Fidelity conviction × fundamentals, persists bought symbols across page refreshes
- **Sell list** — Hermes-generated trim candidates when ATH triggers or funds needed
- **Session notes** — auto-saves to Obsidian vault with detailed chart pattern analysis
- **🗺 Journey tab** — 100-envelope challenge tracking true profit from $0 → $100k
- **True profit tracker** — verified against actual Alpaca deposit history, not estimates
- **Checkpoint/variation** — zero a profit baseline anytime, track drift in real-time

---

## Quick Install

```bash
git clone https://github.com/sumip90x-ui/hermes-trading-dashboard.git
cd hermes-trading-dashboard
bash install.sh
```

---

## Requirements

- Python 3.10+
- Alpaca live trading account (paper trading works too)
- Anthropic API key (Claude) — optional, falls back to hermes CLI

---

## Configuration

Copy the example env file and fill in your keys:

```bash
cp .env.example .env
```

```env
# Required
ALPACA_LIVE_KEY=your_alpaca_api_key
ALPACA_LIVE_SECRET=your_alpaca_secret_key

# Optional — enables Hermes AI chat with trade execution
ANTHROPIC_API_KEY=your_anthropic_key
```

---

## Start

```bash
bash start.sh
# Opens http://localhost:6060
```

Or directly:
```bash
python3 app.py
```

---

## Architecture

```
Flask + SocketIO backend (port 6060)
  ├── /api/account        — live equity, P&L, intraday OHLC, true profit
  ├── /api/positions       — all positions ranked by gain %
  ├── /api/candle_trigger  — ATH zone detection
  ├── /api/buy_candidates  — EDGAR + Fidelity conviction ranked buys
  ├── /api/stop_tiers      — 7-tier stop loss levels
  ├── /api/journey         — 100-envelope challenge progress
  ├── /api/chat            — Hermes AI with tool-calling (place_order)
  ├── /api/save_session_note — auto-write to Trading Vault
  └── /api/ath_decision    — Claude analyzes what to trim
```

Frontend: vanilla JS + Chart.js + socket.io (no build step, no npm)

---

## The Journey

This dashboard was built to grow a $1,154 account to $100,000 over time using:
- Magic Formula screener (Joel Greenblatt)
- Fidelity 37-account conviction signals
- EDGAR fundamental scores
- ATH pullback + profit protection rules
- Long-term DCA into quality dips

**Current milestone:** Envelope #1 ($0 → $40 true profit) — 84% complete

---

## License

MIT — use it, modify it, build on it.
