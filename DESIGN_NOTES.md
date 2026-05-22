# Hermes Trading Dashboard — Design Notes
Last updated: 2026-05-22
GitHub: https://github.com/sumip90x-ui/hermes-trading-dashboard
Local: ~/trading_dashboard/
Port: 6060

---

## CRITICAL: BACKUP RULES (learned the hard way — cost $200+ to rebuild)

1. **NEVER strip line numbers from app.py** — the `read_file` tool shows `N|content` prefixes — these are DISPLAY ONLY, never write them back to the file
2. **ALWAYS commit before any fix session** — `git add -A && git commit -m "checkpoint before changes"`
3. **ALWAYS git push after working session** — `git push origin main`
4. **Backup location**: `~/Documents/Trading Vault/Dashboard_Backup/` — dated copies kept here
5. The `.bak` file is NOT a reliable backup — it gets overwritten
6. If the file ever gets corrupted with line-number prefixes again: `sed -i 's/^ *[0-9]*|//' app.py`

## SECRET line fix (always needed after restore from git)
The git version has `SECRET=*** or...` on line 37. Fix with:
```python
SECRET = os.environ.get('ALPACA_LIVE_SECRET') or os.environ.get('APCA_API_SECRET_KEY','')
```

---

## Architecture

### Stack
- Flask + SocketIO — port 6060
- Vanilla JS + Chart.js
- Alpaca Markets API (live trading)
- Anthropic Claude API (Hermes AI chat)
- MiroShark (port 5001) — simulation platform
- EDGAR API (port 5002) — SEC fundamentals

### Grid Layout
```
[HEADER — row 1, full width]
[CHART PANEL — row 2, col 1] [RIGHT PANEL — rows 2-3, col 2 (380px)]
[BUY PANEL — row 3, col 1]
```
CSS: `grid-template-columns: 1fr 380px; grid-template-rows: 95px 1fr 220px`

---

## Features

### Header (row 1)
- EQUITY, DAY P/L, ATH WATCH button, Save Note button
- INTRADAY HIGH, PULLBACK GOAL, TRUE PROFIT, VS $1,154 PRINCIPAL
- CHECKPOINT (click ⊙ to reset baseline), VARIATION %
- Principal = $1,154.00 (verified from Alpaca transfer history, Apr-May 2026)

### Chart Panel (left, row 2)
**Tabs**: Intraday | Candles | Positions | Research

**Intraday**: Live equity curve, MA5 (blue) + MA20 (gold), phase detection
- Phases: GRINDING_UP, AT_PEAK, PULLBACK, RECOVERY, CAPITULATION, CONSOLIDATING
- HERMES SEES panel (collapsible) — shows phase/MA/support/resistance
- Phase badge strip at top

**Candles**: Daily OHLC chart from Alpaca portfolio history

**Positions**: Table of all Alpaca positions with P/L

**Research**: Full-width 3-column panel (see Research Tab section below)

### Buy Panel (bottom left, row 3)
- BUY LIST tab: EDGAR-scored candidates from Fidelity CSV upload
- SELL LIST tab: ATH-triggered sell recommendations from Hermes
- BUY ALL button — sequential execution with 400ms gaps
- Columns: SYM | BUY $ | ACCTS | TYPE | REASON | EDGAR | BUY | ✕

### Right Panel (right, rows 2-3, 380px)
**Tabs**: 💬 Chat | ⚡ Trade | 🕯 Candle | 📋 Buys | 🗺 Journey

**Chat**: Hermes AI chat — injects equity curve context + previous session notes automatically

**Trade**: Manual buy/sell execution — symbol, side, notional amount

**Candle**: ATH trigger monitor, stop loss tiers (7 levels), house money tracker

**Buys**: Fidelity CSV upload + EDGAR-scored buy list (duplicate of bottom panel in tab form)

**Journey**: 100-envelope challenge $0→$100k tracker
- 4 phases: SEED ($0-$500), SPROUT ($500-$2k), GROW ($2k-$10k), HARVEST ($10k-$100k)
- Journey data: ~/Documents/Trading Vault/journey.json

---

## Research Tab

### Layout (3 columns, full width)
```
[EDGAR DATA 160px] | [MIROSHARK iframe — flex:1] | [SEED FILES + SIM REPORTS 200px]
```

### Left column — EDGAR DATA
- Lists all tickers with `{TICKER}_fundamentals.xlsx` in `~/Documents/EDGAR/companies/`
- Click → loads fundamentals table into center panel (hides MiroShark iframe)
- Currently 33 tickers: AEM, ASML, BRK-B, BTG, CEG, CELH, CMCSA, COHR, COST, CRDO, CVX, GOOGL, HAS, HD, IBM, INSM, INTU, JPM, KTOS, LITE, LMT, LXRX, MSFT, MU, NVDA, PEP, PLTR, POET, REGN, SCCO, SNOW, TSLA, VNT

### Center panel — MiroShark iframe / Fundamentals table
- Default: MiroShark iframe (`about:blank`)
- Click EDGAR ticker → iframe hides, fundamentals table appears
- Click +SIM → table hides, iframe loads MiroShark with sim ready
- Server-side HTML renderer: `/api/research/fundamentals-html/<ticker>`

### Right column — Seed Files + Sim Reports
**SEED FILES** (top half):
- Lists tickers with `{TICKER}_seed.md` — currently 13 tickers
- Each has +SIM button
- Click +SIM → calls `/api/research/launch-sim/<ticker>` → writes preset template → loads MiroShark with `?template=hermes_{ticker}` → auto-launches Steps 1-4

**SIM REPORTS** (bottom half):
- Lists completed MiroShark simulations from `~/Documents/MiroShark/backend/uploads/reports/`
- Click → opens simulation in new tab at localhost:5001

### Research Flask routes (app.py)
```
GET  /api/research/edgar-tickers        — tickers with fundamentals.xlsx
GET  /api/research/seed-tickers         — tickers with _seed.md
GET  /api/research/fundamentals-html/<ticker>  — server-side HTML table (USE THIS)
GET  /api/research/fundamentals/<ticker>       — JSON fallback
GET  /api/research/seed_url/<ticker>    — serves seed .md as text/plain
GET  /api/research/launch-sim/<ticker>  — writes template, returns ?template= URL
GET  /api/research/simulations          — MiroShark sim list
```

### MiroShark sim launch flow (CRITICAL — do not change)
The ONLY working flow is the **preset template approach**:
1. `GET /api/research/launch-sim/{ticker}` calls MiroShark `ask-stock`
2. Gets `simulation_requirement` + `seed_document`
3. Writes `~/Documents/MiroShark/backend/app/preset_templates/hermes_{ticker}.json`
4. Returns `http://localhost:5001/?template=hermes_{ticker}`
5. MiroShark's `autoLaunchTemplate()` fires → `setPendingTemplate()` → routes to Process (Step 1-4)
6. Full simulation runs automatically — NO button click needed

**Why NOT `?scenario=` or `?url=`:**
- `?scenario=` fills textarea but `canSubmit=false` because no file/urlDoc attached
- `?url=` is blocked by MiroShark SSRF protection (blocks 127.0.0.1)
- `?template=` is the only approach that bypasses canSubmit check

**Why NOT sandbox on iframe:**
- `sandbox` attribute breaks Vue's router and event handling — remove entirely

---

## Fundamentals Table Renderer

### Excel structure (all tickers, 9 columns)
```
Col 0 (named after company): metric name, section header, or NaN
Col 1 (Unnamed: 1): primary value
Col 2 (Unnamed: 2): note/context text
Cols 3-8 (Unnamed: 3-8): additional period values

Row 0: metadata (CIK, dates, source)
Row 1: blank
Row ~18: date header row (col0=NaN, cols1-8 have dates like 2025-12-31)
Rows 19+: data
```

### Section detection
- `PHASE X —` prefix → amber divider row
- `---` prefix → amber divider row
- col0=NaN + col1 has date → date header row (skip, already captured)
- col0=NaN + no values → blank row (skip)

### Value formatting
- Floats between 0 and 1.0 → displayed as percentage (e.g. 0.7194 → 71.9%)
- Long strings truncated at 30 chars with `…` — full value in `title` tooltip
- Numbers right-aligned in `#00ff88` (bright green)
- Text values in `#88cc88` (muted green)

---

## Key Constants
```python
EDGAR_BASE = os.path.expanduser('~/Documents/EDGAR/companies')
MIROSHARK_BASE = 'http://localhost:5001'
EDGAR_API_BASE = 'http://localhost:5002'
VERIFIED_PRINCIPAL = 1154.00  # verified from Alpaca transfer history
```

---

## Background Processes (started at launch)
1. `_live_updater()` — pushes live equity + trigger status every 30s via SocketIO
2. `_proactive_brain()` — Hermes pushes unsolicited observations every 5min

---

## Session Notes
Auto-saved to: `~/Documents/Trading Vault/02_Session_Notes/YYYY-MM-DD.md`
Trading Brain: `~/Documents/Trading Vault/TRADING_BRAIN.md`

---

## Checkpoint / Variation Tracker
- Stored in localStorage: `hermes_checkpoint_profit`, `hermes_checkpoint_time`
- Auto-initializes on first load to current true profit
- Click ⊙ to reset baseline
- VARIATION % = (current - checkpoint) / |checkpoint| × 100

---

## Stop Loss Tiers (7 levels)
Anchored to session ATH:
- SOFT_STOP: ATH - 1%
- WARN_STOP: ATH - 1.5%
- HARD_STOP: ATH - 2.5%
- BREAK_EVEN: today's open
- PREV_ATH: previous candle ATH
- DANGER: prev ATH - 1.5%
- PRINCIPAL: $1,154.00 floor (never go below)

---

## How to Start
```bash
# Start MiroShark suite first (if using Research tab)
bash ~/Documents/MiroShark/start-oracle-suite.sh

# Start dashboard
cd ~/trading_dashboard && python3 app.py

# Or double-click desktop icon
~/Desktop/ORACLE/Hermes-Dashboard.desktop
```

---

## GitHub
Repo: https://github.com/sumip90x-ui/hermes-trading-dashboard
Push: `cd ~/trading_dashboard && git push origin main`
