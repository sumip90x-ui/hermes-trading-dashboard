"""
fidelity_db.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fidelity portfolio history + deviation signal engine.

Importable standalone — no Flask dependency.
All paths via pathlib. No external deps beyond pandas + stdlib.

Two tables:
  snapshots   — one row per symbol per CSV upload
  deviations  — one row per symbol per snapshot pair (curr vs prev)

Usage:
  from fidelity_db import ingest_snapshot, get_snapshots, get_deviations
"""

import sqlite3
import uuid
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from collections import defaultdict

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────

VAULT_DIR = Path.home() / "Documents" / "Trading Vault" / "Fidelity_History"
DB_PATH   = VAULT_DIR / "portfolio_history.db"

# Broker-specific history directories
BROKER_DIRS = {
    "fidelity":   VAULT_DIR,
    "vanguard":   Path.home() / "Documents" / "Trading Vault" / "Vanguard_History",
    "wellsfargo": Path.home() / "Documents" / "Trading Vault" / "WellsFargo_History",
}

def _ensure_broker_dirs():
    for d in BROKER_DIRS.values():
        d.mkdir(parents=True, exist_ok=True)

_ensure_broker_dirs()

# ── DB init ───────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    """Return a connection with row_factory set so rows act like dicts."""
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    with get_conn() as conn:
        # ── Migrate: add broker column if old DB lacks it ─────────────
        cols = [r[1] for r in conn.execute("PRAGMA table_info(snapshots)").fetchall()]
        if cols and "broker" not in cols:
            conn.execute("ALTER TABLE snapshots ADD COLUMN broker TEXT NOT NULL DEFAULT 'fidelity'")
            conn.commit()
        if cols and "today_gl" not in cols:
            conn.execute("ALTER TABLE snapshots ADD COLUMN today_gl REAL")
            conn.commit()

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id     TEXT    NOT NULL,
                snapshot_date   TEXT    NOT NULL,
                filename        TEXT    NOT NULL,
                broker          TEXT    NOT NULL DEFAULT 'fidelity',
                symbol          TEXT    NOT NULL,
                description     TEXT,
                accounts        INTEGER,
                total_qty       REAL,
                last_price      REAL,
                total_value     REAL,
                total_cost      REAL,
                total_gl        REAL,
                today_gl        REAL,
                gl_pct          REAL,
                portfolio_pct   REAL
            );

            CREATE INDEX IF NOT EXISTS idx_snap_id
                ON snapshots(snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_snap_symbol
                ON snapshots(symbol);
            CREATE INDEX IF NOT EXISTS idx_snap_date
                ON snapshots(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_snap_broker
                ON snapshots(broker);

            CREATE TABLE IF NOT EXISTS deviations (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                calculated_at        TEXT    NOT NULL,
                symbol               TEXT    NOT NULL,
                prev_snapshot_id     TEXT    NOT NULL,
                curr_snapshot_id     TEXT    NOT NULL,
                prev_gl              REAL,
                curr_gl              REAL,
                gl_delta             REAL,
                budget_ceiling       REAL,
                accounts             INTEGER,
                conviction_multiplier REAL,
                portfolio_pct        REAL,
                concentration_block  INTEGER DEFAULT 0,
                deploy_amount        REAL    DEFAULT 0,
                direction            TEXT,
                signal               TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_dev_curr_snap
                ON deviations(curr_snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_dev_symbol
                ON deviations(symbol);
        """)


# ── CSV parsing ───────────────────────────────────────────────────────────────

def _parse_money(val) -> Optional[float]:
    """
    Strip $, commas, +/- prefixes from Fidelity money strings.
    Returns float or None if unparseable.
    Examples: '$1,234.56' → 1234.56 | '-$0.88' → -0.88 | '+$74.18' → 74.18
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().replace(",", "")
    # Detect negative: leading minus or '-$'
    negative = s.startswith("-") or s.startswith("−")
    # Strip everything except digits and decimal point
    digits = re.sub(r"[^\d.]", "", s)
    if not digits:
        return None
    try:
        n = float(digits)
        return -n if negative else n
    except ValueError:
        return None


def _is_valid_account(val: str) -> bool:
    """
    Valid Fidelity account numbers:
      - Letter-prefixed:   X91923586, Z23806303
      - Purely numeric:    227867696, 13066 (401k plans can be short)
    Invalid (footer/header junk):
      - Starts with quote, blank, disclaimer text, 'Account Number' header repeat
    """
    if not val or not isinstance(val, str):
        return False
    val = val.strip()
    # Must start with a letter followed by digits, OR be all digits (4-12 chars)
    # 401k plans like "13066" are only 5 digits — allow 4+ digits
    return bool(re.match(r'^[A-Z]\d+$', val) or re.match(r'^\d{4,12}$', val))


def parse_fidelity_csv(filepath: str | Path) -> list[dict]:
    """
    Parse a Fidelity multi-account CSV and return aggregated rows.

    Fidelity CSV quirks handled here:
      - UTF-8 BOM (encoding='utf-8-sig')
      - Windows line endings (\r\n)
      - Account number is the ROW INDEX (first column, before 'Account Number' header)
      - Footer rows: disclaimer text, date stamp — filtered by _is_valid_account()
      - Money values: '+$1,234.56', '-$0.88', '$42.94' — all handled by _parse_money()
      - Purely numeric account numbers (227867696) are valid — not filtered out
      - Some positions (money market) have null price/GL — handled gracefully

    Returns list of dicts matching the snapshots schema (minus snapshot metadata).
    """
    filepath = Path(filepath)

    # Read with index_col=0: Fidelity puts account# as the row index
    df = pd.read_csv(
        filepath,
        encoding="utf-8-sig",
        index_col=0,
        dtype=str,          # everything as string — we parse numerics ourselves
        on_bad_lines="skip",
    )

    # After index_col=0, the index holds account numbers.
    # Reset so account number becomes a regular column.
    df = df.reset_index()
    df.columns = ["Account_Number"] + list(df.columns[1:])

    # Drop footer/header junk rows
    df = df[df["Account_Number"].apply(_is_valid_account)].copy()

    if df.empty:
        return []

    # ── Verified Fidelity column mapping (index_col=0 shifts headers right by 1) ──
    # Confirmed from raw SGOL row: account# is index, real cols shift right.
    #   "Account Name"       → ticker symbol      ('SGOL')
    #   "Symbol"             → full company name  ('ETFS GOLD TR PHYSCL GOLD SHS')
    #   "Description"        → shares/qty         ('2.918')
    #   "Quantity"           → last price         ('$42.94')
    #   "Last Price Change"  → current value      ('$125.29')
    #   "Percent Of Account" → cost basis total   ('$132.01')
    # Total G/L dollar is not directly available after the shift.
    # Derived as: total_gl = total_value - total_cost  (after aggregation).

    # Detect Sleeve Name format — some Fidelity exports include an extra
    # "Sleeve Name" column that shifts the ticker to a different column.
    if "Sleeve Name" in df.columns:
        df["_ticker"] = df["Sleeve Name"].str.strip()
    else:
        df["_ticker"] = df["Account Name"].str.strip()
    df["_name"]     = df["Symbol"].str.strip()
    df["_qty"]      = df["Description"].apply(_parse_money)
    df["_price"]    = df["Quantity"].apply(_parse_money)
    df["_val"]      = df["Last Price Change"].apply(_parse_money)
    df["_cost"]     = df["Percent Of Account"].apply(_parse_money)
    # Today's G/L Dollar: after index_col=0 shift, "Current Value" col = today's $ change
    df["_today_gl"] = df["Current Value"].apply(_parse_money)

    # Filter out rows where ticker is blank or a section header (not a real symbol)
    # Also filter money market funds (end with **) — these are cash positions not stocks
    df = df[df["_ticker"].str.match(r'^[A-Z0-9\.\-]+$', na=False)].copy()

    if df.empty:
        return []

    # Aggregate by ticker symbol
    agg = (
        df.groupby("_ticker", sort=False)
        .agg(
            description  =("_name",         "first"),
            accounts     =("Account_Number", "nunique"),
            total_qty    =("_qty",           "sum"),
            last_price   =("_price",         "first"),
            total_value  =("_val",           "sum"),
            total_cost   =("_cost",          "sum"),
            today_gl     =("_today_gl",      "sum"),
        )
        .reset_index()
        .rename(columns={"_ticker": "Symbol"})
    )

    # Derive total G/L from aggregated value - cost (not available as a direct column)
    agg["total_gl"] = agg["total_value"] - agg["total_cost"]

    # Recalculate GL% from aggregated totals — do NOT sum the per-row percentages
    # (per-row % are meaningless after aggregation across fractional share positions)
    agg["gl_pct"] = agg.apply(
        lambda r: (r["total_gl"] / r["total_cost"] * 100) if r["total_cost"] else None,
        axis=1,
    )

    # Portfolio % = this symbol's value / total portfolio value for this snapshot
    total_portfolio_value = agg["total_value"].sum()
    agg["portfolio_pct"] = agg["total_value"].apply(
        lambda v: (v / total_portfolio_value * 100) if total_portfolio_value else None
    )

    # Convert to list of dicts
    rows = []
    for _, row in agg.iterrows():
        rows.append({
            "symbol":       str(row["Symbol"]).strip(),
            "description":  str(row["description"]).strip() if pd.notna(row["description"]) else "",
            "accounts":     int(row["accounts"]),
            "total_qty":    float(row["total_qty"]) if pd.notna(row["total_qty"]) else 0.0,
            "last_price":   float(row["last_price"]) if pd.notna(row["last_price"]) else None,
            "total_value":  float(row["total_value"]) if pd.notna(row["total_value"]) else 0.0,
            "total_cost":   float(row["total_cost"]) if pd.notna(row["total_cost"]) else 0.0,
            "total_gl":     float(row["total_gl"]) if pd.notna(row["total_gl"]) else 0.0,
            "today_gl":     float(row["today_gl"]) if pd.notna(row["today_gl"]) else 0.0,
            "gl_pct":       float(row["gl_pct"]) if pd.notna(row["gl_pct"]) else None,
            "portfolio_pct":float(row["portfolio_pct"]) if pd.notna(row["portfolio_pct"]) else None,
        })

    return rows


# ── Deviation logic ───────────────────────────────────────────────────────────

def _conviction_multiplier(accounts: int) -> float:
    """
    Scale deploy budget by how many of the 37 Fidelity accounts hold the ticker.
    More accounts = higher conviction = larger multiplier.

    1 account    → 0.50x  (experimental, untested position)
    2-5 accounts → 0.75x  (early conviction)
    6-15 accounts→ 1.00x  (established position)
    16+ accounts → 1.25x  (core holding, high conviction)
    """
    if accounts <= 1:
        return 0.50
    elif accounts <= 5:
        return 0.75
    elif accounts <= 15:
        return 1.00
    else:
        return 1.25


def _direction(gl_delta: float, curr_gl: float) -> str:
    """
    Classify which way the G/L is moving relative to its sign.

    For losing positions (curr_gl < 0):
      DETERIORATING — loss getting worse (gl_delta > 0 means less negative → wait,
                       actually delta = curr - prev, so if loss grew: curr more negative,
                       delta < 0 for losses)
      RECOVERING    — loss shrinking (delta > 0, moving toward zero)

    For gaining positions (curr_gl > 0):
      ACCELERATING  — gain growing (delta > 0)
      PULLBACK      — gain pulling back (delta < 0)

    STABLE — delta is negligible (handled by threshold gate before this is called,
              but included as a safety fallback)

    Note on sign convention:
      gl_delta = curr_gl - prev_gl
      If SGOL was -$104 and is now -$106: delta = -106 - (-104) = -2  → loss worsened → DETERIORATING
      If SGOL was -$106 and is now -$104: delta = -104 - (-106) = +2  → loss shrank  → RECOVERING
    """
    if abs(gl_delta) < 0.01:
        return "STABLE"

    if curr_gl < 0:
        # Losing position
        if gl_delta < 0:
            return "DETERIORATING"   # loss got worse (more negative)
        else:
            return "RECOVERING"      # loss is shrinking (less negative)
    else:
        # Gaining position
        if gl_delta > 0:
            return "ACCELERATING"    # gain growing — don't chase
        else:
            return "PULLBACK"        # gain pulling back — buy the dip on a winner


def _deploy_amount(
    gl_delta: float,
    budget_ceiling: float,
    conviction_mult: float,
    direction: str,
    concentration_block: bool,
) -> tuple[float, str]:
    """
    Calculate final deploy amount and signal for Alpaca.

    Returns (deploy_amount, signal) where signal is BUY / SKIP / BLOCKED.

    Logic:
      base = abs(gl_delta) × conviction_multiplier
      direction modifier:
        DETERIORATING or ACCELERATING → ×0.50  (cautious — don't catch falling knife / don't chase)
        RECOVERING or PULLBACK        → ×1.25  (lean in — position recovering / buying winner dip)
        STABLE                        → ×1.00  (shouldn't reach here due to threshold gate)
      cap at budget_ceiling
      floor at $1.10 (Alpaca fractional minimum) — below this, signal=SKIP
      concentration_block → deploy=0, signal=BLOCKED
    """
    if concentration_block:
        return 0.0, "BLOCKED"

    # Base = magnitude of the deviation × conviction
    base = abs(gl_delta) * conviction_mult

    # Direction modifier — sizing reflects risk direction
    if direction in ("DETERIORATING", "ACCELERATING"):
        base *= 0.50    # cautious: falling knife / don't chase a moon
    elif direction in ("RECOVERING", "PULLBACK"):
        base *= 1.25    # lean in: recovering loser / dip on a winner
    # STABLE: ×1.00 (passthrough, shouldn't normally fire)

    # Cap at budget ceiling (abs of current G/L)
    base = min(base, budget_ceiling)

    # Alpaca minimum — below $1.10 the order won't execute
    if base < 1.10:
        return 0.0, "SKIP"

    return round(base, 2), "BUY"


def calculate_deviations(curr_snapshot_id: str) -> list[dict]:
    """
    Compare curr_snapshot against the most recent PREVIOUS snapshot.
    Inserts deviation rows into the deviations table.
    Returns the calculated deviation dicts.

    If there is no previous snapshot (first ever upload), returns [] with no DB writes.
    """
    with get_conn() as conn:
        # Get current snapshot metadata
        curr_meta = conn.execute("""
            SELECT snapshot_date, MIN(snapshot_date) as date
            FROM snapshots
            WHERE snapshot_id = ?
            LIMIT 1
        """, (curr_snapshot_id,)).fetchone()

        if not curr_meta:
            return []

        curr_date = conn.execute(
            "SELECT snapshot_date FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (curr_snapshot_id,)
        ).fetchone()["snapshot_date"]

        # Find the most recent snapshot that is NOT the current one
        prev_meta = conn.execute("""
            SELECT snapshot_id, snapshot_date
            FROM snapshots
            WHERE snapshot_id != ?
            GROUP BY snapshot_id
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (curr_snapshot_id,)).fetchone()

        if not prev_meta:
            # First snapshot — no deviations possible yet
            return []

        prev_snapshot_id = prev_meta["snapshot_id"]

        # Load current snapshot rows keyed by symbol
        curr_rows = {
            row["symbol"]: dict(row)
            for row in conn.execute(
                "SELECT * FROM snapshots WHERE snapshot_id = ?",
                (curr_snapshot_id,)
            ).fetchall()
        }

        # Load previous snapshot rows keyed by symbol
        prev_rows = {
            row["symbol"]: dict(row)
            for row in conn.execute(
                "SELECT * FROM snapshots WHERE snapshot_id = ?",
                (prev_snapshot_id,)
            ).fetchall()
        }

    # Calculate deviations for symbols present in BOTH snapshots
    deviations = []
    now = datetime.utcnow().isoformat()

    for symbol, curr in curr_rows.items():
        if symbol not in prev_rows:
            # New position — no baseline to diff against yet
            continue

        prev = prev_rows[symbol]
        curr_gl = curr["total_gl"] or 0.0
        prev_gl = prev["total_gl"] or 0.0

        gl_delta       = curr_gl - prev_gl
        budget_ceiling = abs(curr_gl)

        # Threshold gate: skip if deviation is too small to act on.
        # Must exceed BOTH 1.5% of budget ceiling AND $2.00 minimum.
        # This prevents trading on noise from minor price fluctuations.
        threshold = max(budget_ceiling * 0.015, 2.00)
        if abs(gl_delta) < threshold:
            continue

        accounts         = curr["accounts"]
        conviction_mult  = _conviction_multiplier(accounts)
        portfolio_pct    = curr["portfolio_pct"] or 0.0
        direction        = _direction(gl_delta, curr_gl)

        # Concentration block: if this ticker is >5% of total Fidelity portfolio
        # value, don't add more — already heavily weighted.
        concentration_block = portfolio_pct > 5.0

        deploy, signal = _deploy_amount(
            gl_delta=gl_delta,
            budget_ceiling=budget_ceiling,
            conviction_mult=conviction_mult,
            direction=direction,
            concentration_block=concentration_block,
        )

        dev = {
            "calculated_at":        now,
            "symbol":               symbol,
            "prev_snapshot_id":     prev_snapshot_id,
            "curr_snapshot_id":     curr_snapshot_id,
            "prev_gl":              round(prev_gl, 4),
            "curr_gl":              round(curr_gl, 4),
            "gl_delta":             round(gl_delta, 4),
            "budget_ceiling":       round(budget_ceiling, 4),
            "accounts":             accounts,
            "conviction_multiplier":conviction_mult,
            "portfolio_pct":        round(portfolio_pct, 4),
            "concentration_block":  1 if concentration_block else 0,
            "deploy_amount":        deploy,
            "direction":            direction,
            "signal":               signal,
        }
        deviations.append(dev)

    # Bulk insert into deviations table
    if deviations:
        with get_conn() as conn:
            conn.executemany("""
                INSERT INTO deviations (
                    calculated_at, symbol, prev_snapshot_id, curr_snapshot_id,
                    prev_gl, curr_gl, gl_delta, budget_ceiling,
                    accounts, conviction_multiplier, portfolio_pct,
                    concentration_block, deploy_amount, direction, signal
                ) VALUES (
                    :calculated_at, :symbol, :prev_snapshot_id, :curr_snapshot_id,
                    :prev_gl, :curr_gl, :gl_delta, :budget_ceiling,
                    :accounts, :conviction_multiplier, :portfolio_pct,
                    :concentration_block, :deploy_amount, :direction, :signal
                )
            """, deviations)

    return deviations


# ── Vanguard CSV parser ───────────────────────────────────────────────────────

def parse_vanguard_csv(filepath: str | Path) -> list[dict]:
    """
    Parse a Vanguard brokerage holdings CSV export (OfxDownload format).

    Vanguard CSV format:
      Account Number, Investment Name, Symbol, Shares, Share Price, Total Value,

    The file has TWO sections separated by a blank row:
      Section 1: Holdings (positions) — ONLY this is used for portfolio snapshot
      Section 2: Transactions — must be IGNORED

    Money market funds (VMFXX, VMMXX, VUSXX etc.) are excluded.
    Symbols with spaces (e.g. 'BRK B') are normalized to remove spaces.
    Returns same schema as parse_fidelity_csv().
    """
    filepath = Path(filepath)
    with open(filepath, encoding="utf-8-sig") as fh:
        raw = fh.read()

    lines = raw.splitlines()

    # Find the FIRST header row (holdings section)
    holdings_header_idx = -1
    for i, line in enumerate(lines):
        lower = line.lower()
        if "account number" in lower and "symbol" in lower and "shares" in lower:
            holdings_header_idx = i
            break
    if holdings_header_idx < 0:
        return []

    # Find where the holdings section ends — blank line or second header with "Trade Date"
    holdings_end_idx = len(lines)
    for i in range(holdings_header_idx + 1, len(lines)):
        stripped = lines[i].strip().strip(",")
        if not stripped:
            holdings_end_idx = i
            break
        # Transactions header starts with "Account Number,Trade Date"
        if "trade date" in lines[i].lower():
            holdings_end_idx = i
            break

    # Parse only the holdings section
    import io
    holdings_text = "\n".join(lines[holdings_header_idx:holdings_end_idx])
    try:
        df = pd.read_csv(io.StringIO(holdings_text), dtype=str, on_bad_lines="skip")
    except Exception:
        return []

    # Column mapping — Vanguard uses exact names
    def _col(patterns):
        for p in patterns:
            for c in df.columns:
                if p.lower() in c.lower().strip():
                    return c
        return None

    sym_col   = _col(["symbol"])
    desc_col  = _col(["investment name", "description", "name"])
    qty_col   = _col(["shares", "quantity"])
    price_col = _col(["share price", "price"])
    val_col   = _col(["total value", "market value", "current value"])
    cost_col  = _col(["cost basis", "total cost"])
    acct_col  = _col(["account number", "account"])

    if not sym_col:
        return []

    # Money market tickers to exclude
    MONEY_MARKET = {"VMFXX", "VMMXX", "VUSXX", "VMRXX", "VPDFX", "VMSXX"}

    acct_set_all = set()
    map_: dict = {}

    for _, row in df.iterrows():
        raw_sym = str(row.get(sym_col, "")).strip()
        # Normalize: remove spaces (e.g. 'BRK B' → 'BRKB')
        sym = raw_sym.replace(" ", "").rstrip("*")
        if not sym or sym in ("nan", "—", "-", "Symbol"):
            continue
        if sym in MONEY_MARKET:
            continue
        if not re.match(r'^[A-Z0-9][A-Z0-9.\-]{0,9}$', sym):
            continue

        acct = str(row.get(acct_col, "VG")).strip() if acct_col else "VG"
        acct_set_all.add(acct)

        desc  = str(row.get(desc_col, "")).strip() if desc_col else ""
        qty   = _parse_money(row.get(qty_col))   if qty_col   else None
        price = _parse_money(row.get(price_col)) if price_col else None
        val   = _parse_money(row.get(val_col))   if val_col   else None
        cost  = _parse_money(row.get(cost_col))  if cost_col  else None

        if sym not in map_:
            map_[sym] = {
                "symbol": sym, "description": desc,
                "_accts": set(), "total_qty": 0.0,
                "last_price": price, "total_value": 0.0,
                "total_cost": 0.0, "total_gl": 0.0,
            }
        r = map_[sym]
        r["_accts"].add(acct)
        if qty   is not None: r["total_qty"]   += qty
        if val   is not None: r["total_value"] += val
        if cost  is not None: r["total_cost"]  += cost
        if price is not None: r["last_price"]   = price

    rows = []
    total_val = sum(r["total_value"] for r in map_.values())
    for sym, r in map_.items():
        r["total_gl"]      = r["total_value"] - r["total_cost"] if r["total_cost"] else 0.0
        r["gl_pct"]        = (r["total_gl"] / r["total_cost"] * 100) if r["total_cost"] else None
        r["portfolio_pct"] = (r["total_value"] / total_val * 100) if total_val else None
        r["accounts"]      = len(r["_accts"])
        del r["_accts"]
        rows.append(r)
    return rows


# ── Wells Fargo XLS parser ────────────────────────────────────────────────────

def parse_wellsfargo_xls(filepath: str | Path) -> list[dict]:
    """
    Parse a Wells Fargo WellsTrade portfolio positions XLS export.

    WF exports a multi-section .xls file with separate header rows per asset type:
      - Cash/Cash Alternatives (ignored — no symbol)
      - Stocks (33 columns, headers on a row containing 'Symbol')
      - ETFs   (30 columns, different layout)
      - Mutual Funds (29 columns, different layout)

    Strategy:
      - Scan all rows looking for section header rows (col 1 == 'Symbol')
      - For each section, build a dynamic column index from the header
      - Aggregate per-symbol across all sections and all tax lots

    Column positions are derived dynamically from the header row, so this
    works even if WF adds/removes columns in future exports.
    """
    try:
        import xlrd
    except ImportError:
        raise ImportError("xlrd is required for Wells Fargo XLS files: pip install xlrd")

    filepath = Path(filepath)
    wb = xlrd.open_workbook(str(filepath))
    ws = wb.sheet_by_index(0)

    SKIP_SYMBOLS = {"", "Symbol", "N/A", "Description"}
    TOTAL_KEYWORDS = ("total", "grand total")
    SECTION_KEYWORDS = ("stocks", "etfs", "mutual funds", "bonds", "options")

    map_: dict = {}
    acct_name = "WF"

    # Extract account number from header area (rows 0–6)
    for r in range(7):
        cell0 = str(ws.cell_value(r, 0)).strip()
        if "account number" in cell0.lower():
            acct_name = cell0.replace("Account Number:", "").strip() or "WF"
            break
        if "nick name" in cell0.lower():
            acct_name = cell0.replace("Nick Name:", "").strip() or "WF"

    i = 0
    while i < ws.nrows:
        row = [str(ws.cell_value(i, c)).strip() for c in range(ws.ncols)]

        # Detect section header row: col 0 is keyword, col 1 is 'Symbol'
        if row[1].lower() == "symbol" and row[0].lower() not in TOTAL_KEYWORDS:
            # Build column index from this header row
            col_idx: dict[str, int] = {}
            for c, hdr in enumerate(row):
                h = hdr.lower().strip()
                col_idx[h] = c

            def gcol(patterns):
                for p in patterns:
                    if p in col_idx:
                        return col_idx[p]
                return None

            # Map key columns — names differ per section
            sym_c   = gcol(["symbol"])
            price_c = gcol(["last price ($)", "nav ($)", "price"])
            val_c   = gcol(["market value"])
            cost_c  = gcol(["total cost1", "total cost", "total client investment"])
            gl_c    = gcol(["unrealized gain/loss ($)1", "unrealized gain/loss ($)",
                            "client inv gain/(loss) $"])
            qty_c   = gcol(["shares"])

            # Parse data rows until blank or new section
            i += 1
            while i < ws.nrows:
                drow = [str(ws.cell_value(i, c)).strip() for c in range(ws.ncols)]
                sym  = drow[sym_c] if sym_c is not None else ""

                # End of section: blank row, total row, or next section header
                if not sym or sym.lower() in ("", "n/a"):
                    i += 1
                    # Allow one blank row between tax lots
                    continue
                if any(sym.lower().startswith(k) for k in TOTAL_KEYWORDS):
                    i += 1
                    break
                if any(drow[0].lower() == k for k in SECTION_KEYWORDS):
                    break  # don't increment — outer loop will handle
                if drow[1].lower() == "symbol":
                    break  # new section header — don't increment

                # Skip section sub-headers like "Common Stock", "Closed End"
                # Also skip rows where sym is not a valid ticker (numeric, long description)
                if not sym or sym == drow[0] or sym in SKIP_SYMBOLS:
                    i += 1
                    continue
                # Skip if sym looks like a dollar amount or long description
                if re.match(r'^\d+\.?\d*$', sym) or len(sym) > 10 or ' ' in sym:
                    i += 1
                    continue

                def gval(c):
                    if c is None: return None
                    v = drow[c]
                    if v.lower() in ("n/a", "detail", "not rated", ""):
                        return None
                    return _parse_money(v)

                price = gval(price_c)
                val   = gval(val_c)
                cost  = gval(cost_c)
                gl    = gval(gl_c)
                qty   = gval(qty_c)

                if val is None and price and qty:
                    val = round(price * qty, 2)

                if sym not in map_:
                    map_[sym] = {
                        "symbol": sym,
                        "description": drow[0] if drow[0] != sym else "",
                        "_accts": {acct_name},
                        "total_qty":   0.0,
                        "last_price":  price,
                        "total_value": 0.0,
                        "total_cost":  0.0,
                        "total_gl":    0.0,
                    }
                r = map_[sym]
                r["_accts"].add(acct_name)
                if qty   is not None: r["total_qty"]   += qty
                if val   is not None: r["total_value"] += val
                if cost  is not None: r["total_cost"]  += cost
                if gl    is not None: r["total_gl"]    += gl
                if price is not None: r["last_price"]   = price
                i += 1
            continue
        i += 1

    rows = []
    total_val = sum(r["total_value"] for r in map_.values())
    for sym, r in map_.items():
        if not r["total_gl"] and r["total_value"] and r["total_cost"]:
            r["total_gl"] = round(r["total_value"] - r["total_cost"], 2)
        r["gl_pct"]        = (r["total_gl"] / r["total_cost"] * 100) if r["total_cost"] else None
        r["portfolio_pct"] = (r["total_value"] / total_val * 100) if total_val else None
        r["accounts"]      = len(r["_accts"])
        del r["_accts"]
        rows.append(r)
    return rows


# ── Wells Fargo CSV parser (fallback if WF ever exports CSV) ──────────────────

def parse_wellsfargo_csv(filepath: str | Path) -> list[dict]:
    """
    Parse a Wells Fargo WellsTrade / brokerage holdings CSV export.
    WF primarily exports XLS — this handles any future CSV format.
    Column names vary; we use flexible header matching.
    """
    filepath = Path(filepath)
    with open(filepath, encoding="utf-8-sig") as fh:
        raw = fh.read()

    lines = [l for l in raw.splitlines() if l.strip()]
    if len(lines) < 2:
        return []

    header_idx = -1
    for i, line in enumerate(lines[:25]):
        lower = line.lower()
        if ("symbol" in lower or "ticker" in lower) and \
           ("quantity" in lower or "shares" in lower or "market" in lower):
            header_idx = i
            break
    if header_idx < 0:
        return []

    import io
    df = pd.read_csv(
        io.StringIO("\n".join(lines[header_idx:])),
        dtype=str, on_bad_lines="skip",
    )

    def _col(patterns):
        for p in patterns:
            for c in df.columns:
                if p.lower() in c.lower():
                    return c
        return None

    sym_col   = _col(["symbol", "ticker"])
    desc_col  = _col(["description", "security name", "name"])
    qty_col   = _col(["quantity", "shares"])
    price_col = _col(["last price", "price"])
    val_col   = _col(["market value", "mkt value", "current value"])
    cost_col  = _col(["total cost", "cost basis"])
    gl_col    = _col(["unrealized gain/loss", "gain/loss"])

    if not sym_col:
        return []

    map_: dict = {}
    for _, row in df.iterrows():
        sym = str(row.get(sym_col, "")).strip().rstrip("*")
        if not sym or not re.match(r'^[A-Z0-9][A-Z0-9.\-]{0,9}$', sym):
            continue
        if re.match(r'^(total|grand)', sym, re.I):
            continue

        desc  = str(row.get(desc_col, "")).strip() if desc_col else ""
        qty   = _parse_money(row.get(qty_col))   if qty_col   else None
        price = _parse_money(row.get(price_col)) if price_col else None
        val   = _parse_money(row.get(val_col))   if val_col   else None
        cost  = _parse_money(row.get(cost_col))  if cost_col  else None
        gl    = _parse_money(row.get(gl_col))    if gl_col    else None

        if sym not in map_:
            map_[sym] = {"symbol": sym, "description": desc, "_accts": {"WF"},
                         "total_qty": 0.0, "last_price": price,
                         "total_value": 0.0, "total_cost": 0.0, "total_gl": 0.0}
        r = map_[sym]
        if qty   is not None: r["total_qty"]   += qty
        if val   is not None: r["total_value"] += val
        if cost  is not None: r["total_cost"]  += cost
        if gl    is not None: r["total_gl"]    += gl
        if price is not None: r["last_price"]   = price

    rows = []
    total_val = sum(r["total_value"] for r in map_.values())
    for sym, r in map_.items():
        if not r["total_gl"] and r["total_value"] and r["total_cost"]:
            r["total_gl"] = r["total_value"] - r["total_cost"]
        r["gl_pct"]        = (r["total_gl"] / r["total_cost"] * 100) if r["total_cost"] else None
        r["portfolio_pct"] = (r["total_value"] / total_val * 100) if total_val else None
        r["accounts"]      = len(r["_accts"])
        del r["_accts"]
        rows.append(r)
    return rows
    """
    Parse a Vanguard brokerage holdings CSV export.

    Vanguard export columns (typical):
      Account Number, Investment Name, Symbol, Shares, Share Price,
      Total Value, [Cost Basis Total], [Change $], [Change %]

    Some exports include 'Cost Basis Total', some don't.
    Money-market / settlement funds are filtered (no valid ticker symbol).
    Returns same schema as parse_fidelity_csv().
    """
    filepath = Path(filepath)
    with open(filepath, encoding="utf-8-sig") as fh:
        raw = fh.read()

    lines = [l for l in raw.splitlines() if l.strip()]
    if len(lines) < 2:
        return []

    # Find header row — look for a line containing 'Symbol' + ('Shares' OR 'Quantity')
    header_idx = -1
    for i, line in enumerate(lines[:20]):
        lower = line.lower()
        if "symbol" in lower and ("shares" in lower or "quantity" in lower):
            header_idx = i
            break
    if header_idx < 0:
        return []

    # Parse with pandas starting at header row
    import io
    df = pd.read_csv(
        io.StringIO("\n".join(lines[header_idx:])),
        dtype=str,
        on_bad_lines="skip",
    )

    # Normalize column names for flexible matching
    def _col(patterns):
        for p in patterns:
            for c in df.columns:
                if p.lower() in c.lower():
                    return c
        return None

    sym_col   = _col(["symbol", "ticker"])
    desc_col  = _col(["investment name", "fund name", "description", "name"])
    qty_col   = _col(["shares", "quantity"])
    price_col = _col(["share price", "price"])
    val_col   = _col(["total value", "market value", "current value"])
    cost_col  = _col(["cost basis"])
    acct_col  = _col(["account number", "account"])

    if not sym_col:
        return []

    acct_set_all = set()
    map_ = {}

    for _, row in df.iterrows():
        sym = str(row.get(sym_col, "")).strip().rstrip("*")
        if not sym or sym in ("—", "-", "nan") or not re.match(r'^[A-Z0-9][A-Z0-9.\-]{0,9}$', sym):
            continue
        # Skip money market / settlement funds (Vanguard uses VMFXX, VMMXX etc)
        if re.search(r'(money market|settlement|federal|prime|treasury)', sym, re.I):
            continue

        acct = str(row.get(acct_col, "VG")).strip() if acct_col else "VG"
        acct_set_all.add(acct)

        desc  = str(row.get(desc_col, "")).strip() if desc_col else ""
        qty   = _parse_money(row.get(qty_col))   if qty_col   else None
        price = _parse_money(row.get(price_col)) if price_col else None
        val   = _parse_money(row.get(val_col))   if val_col   else None
        cost  = _parse_money(row.get(cost_col))  if cost_col  else None

        if sym not in map_:
            map_[sym] = {
                "symbol": sym, "description": desc,
                "_accts": set(), "total_qty": 0.0,
                "last_price": price, "total_value": 0.0,
                "total_cost": 0.0, "total_gl": 0.0,
            }
        r = map_[sym]
        r["_accts"].add(acct)
        if qty   is not None: r["total_qty"]   += qty
        if val   is not None: r["total_value"] += val
        if cost  is not None: r["total_cost"]  += cost
        if price is not None: r["last_price"]   = price

    rows = []
    total_val = sum(r["total_value"] for r in map_.values())
    for sym, r in map_.items():
        r["total_gl"]    = r["total_value"] - r["total_cost"] if r["total_cost"] else 0.0
        r["gl_pct"]      = (r["total_gl"] / r["total_cost"] * 100) if r["total_cost"] else None
        r["portfolio_pct"] = (r["total_value"] / total_val * 100) if total_val else None
        r["accounts"]    = len(r["_accts"])
        del r["_accts"]
        rows.append(r)
    return rows


# ── Performance PDF parser (Vanguard + WF) ───────────────────────────────────

PERF_OVERRIDE_PATH = Path.home() / "Documents" / "Trading Vault" / "broker_performance.json"

def _load_perf_overrides() -> dict:
    """Load manually-parsed performance figures per broker."""
    try:
        if PERF_OVERRIDE_PATH.exists():
            return json.loads(PERF_OVERRIDE_PATH.read_text())
    except Exception:
        pass
    return {}

def _save_perf_overrides(data: dict) -> None:
    PERF_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PERF_OVERRIDE_PATH.write_text(json.dumps(data, indent=2))

def parse_performance_pdf(filepath: str | Path) -> dict:
    """
    Extract account-level performance from a Vanguard or WF performance PDF.

    Uses pdftotext to extract text, then parses key fields:
      - current_value   (most recent ending balance)
      - total_deposited (total deposits/withdrawals = cost basis)
      - investment_returns (total gain/loss in dollars)
      - rate_of_return (percentage)
      - as_of_date

    Returns dict suitable for storing as broker GL override.
    Raises ValueError if required fields cannot be found.
    """
    import subprocess, re
    filepath = Path(filepath)

    result = subprocess.run(
        ["pdftotext", "-layout", str(filepath), "-"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise ValueError(f"pdftotext failed: {result.stderr[:200]}")

    text = result.stdout

    parsed = {
        "source_file":  filepath.name,
        "parsed_at":    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
        "current_value":       None,
        "total_deposited":     None,
        "investment_returns":  None,
        "rate_of_return":      None,
        "as_of_date":          None,
        "broker_hint":         None,
    }

    # ── Detect broker ────────────────────────────────────────────────────────
    if "vanguard" in text.lower():
        parsed["broker_hint"] = "vanguard"
    elif "wells fargo" in text.lower() or "welltrade" in text.lower():
        parsed["broker_hint"] = "wellsfargo"

    # ── Current value ────────────────────────────────────────────────────────
    # Vanguard: "• $1,763.45" near top
    m = re.search(r'•\s*\$([0-9,]+\.\d{2})', text)
    if m:
        parsed["current_value"] = float(m.group(1).replace(",", ""))

    # ── Rate of return ────────────────────────────────────────────────────────
    # Vanguard: "19.1% (As of"
    m = re.search(r'([\d.]+)%\s*\(As of', text)
    if m:
        parsed["rate_of_return"] = float(m.group(1))

    # ── As-of date ────────────────────────────────────────────────────────────
    m = re.search(r'Value as of:\s*(.+?),\s*Eastern time', text)
    if m:
        parsed["as_of_date"] = m.group(1).strip()
    if not parsed["as_of_date"]:
        m = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if m:
            parsed["as_of_date"] = m.group(1)

    # ── Performance summary table ─────────────────────────────────────────────
    # Vanguard format:
    #   $1,826.93   $0.00    +$1,018.65    +$808.28
    # Columns: Ending balance, Beginning balance, Investment returns, Deposits & Withdrawals
    # We want: investment_returns (+$1,018.65) and deposits (+$808.28)
    m = re.search(
        r'\$([\d,]+\.\d{2})\s+\$([\d,]+\.\d{2})\s+[+\-]?\$([\d,]+\.\d{2})\s+[+\-]?\$([\d,]+\.\d{2})',
        text
    )
    if m:
        parsed["investment_returns"] = float(m.group(3).replace(",", ""))
        parsed["total_deposited"]    = float(m.group(4).replace(",", ""))

    # Fallback: look for "Total" row at bottom
    # " Total    $808.28   $965.50   $53.56   $1,018.65"
    m = re.search(r'Total\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})', text)
    if m and not parsed["investment_returns"]:
        parsed["total_deposited"]   = float(m.group(1).replace(",", ""))
        parsed["investment_returns"]= float(m.group(4).replace(",", ""))

    # ── Validate ──────────────────────────────────────────────────────────────
    if parsed["current_value"] is None:
        raise ValueError("Could not find current account value in PDF")

    # Derive GL: if we have deposits and current value but not returns
    if parsed["investment_returns"] is None and parsed["total_deposited"] is not None:
        parsed["investment_returns"] = round(
            (parsed["current_value"] or 0) - parsed["total_deposited"], 2
        )

    return parsed


def ingest_performance_pdf(filepath: str | Path, broker: str) -> dict:
    """
    Parse a performance PDF and store the GL override for the given broker.
    Saves to broker_performance.json.
    Returns the parsed performance dict.
    """
    parsed = parse_performance_pdf(filepath)

    overrides = _load_perf_overrides()
    overrides[broker] = {
        **parsed,
        "broker":       broker,
        "updated_at":   parsed["parsed_at"],
        "value":        parsed["current_value"],
        "cost":         parsed["total_deposited"],
        "gl":           parsed["investment_returns"],
        "gl_pct":       parsed["rate_of_return"],
        "has_cost":     True,
    }
    _save_perf_overrides(overrides)
    return overrides[broker]


# ── Broker-aware ingest ───────────────────────────────────────────────────────

BROKER_PARSERS = {
    "fidelity":   parse_fidelity_csv,
    "vanguard":   parse_vanguard_csv,
    "wellsfargo": parse_wellsfargo_xls,   # WF exports .xls — use XLS parser
}

def ingest_broker_snapshot(filepath: str | Path, broker: str) -> dict:
    """
    Parse and store any broker CSV into the shared snapshots DB.
    broker must be one of: 'fidelity', 'vanguard', 'wellsfargo'
    Returns same summary dict as ingest_snapshot().
    """
    filepath = Path(filepath)
    broker   = broker.lower().strip()
    if broker not in BROKER_PARSERS:
        raise ValueError(f"Unknown broker '{broker}'. Must be: {list(BROKER_PARSERS)}")

    init_db()
    rows = BROKER_PARSERS[broker](filepath)
    if not rows:
        raise ValueError(f"No valid position rows parsed from {filepath.name} (broker={broker})")

    snapshot_id   = str(uuid.uuid4())
    filename      = filepath.name
    snapshot_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    total_value   = sum(r.get("total_value", 0) or 0 for r in rows)

    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO snapshots (
                snapshot_id, snapshot_date, filename, broker,
                symbol, description, accounts,
                total_qty, last_price, total_value, total_cost,
                total_gl, today_gl, gl_pct, portfolio_pct
            ) VALUES (
                :snapshot_id, :snapshot_date, :filename, :broker,
                :symbol, :description, :accounts,
                :total_qty, :last_price, :total_value, :total_cost,
                :total_gl, :today_gl, :gl_pct, :portfolio_pct
            )
        """, [
            {**r,
             "snapshot_id":   snapshot_id,
             "snapshot_date": snapshot_date,
             "filename":      filename,
             "broker":        broker,
             "today_gl":      r.get("today_gl", 0.0) or 0.0}
            for r in rows
        ])

    # Fidelity also runs deviation signals (other brokers: future work)
    deviations = []
    if broker == "fidelity":
        deviations = calculate_deviations(snapshot_id)

    buy_signals = [d for d in deviations if d.get("signal") == "BUY"]
    buy_signals.sort(key=lambda d: d.get("deploy_amount", 0), reverse=True)

    return {
        "snapshot_id":     snapshot_id,
        "snapshot_date":   snapshot_date,
        "broker":          broker,
        "filename":        filename,
        "symbol_count":    len(rows),
        "total_value":     round(total_value, 2),
        "deviation_count": len(deviations),
        "buy_signals":     len(buy_signals),
        "top_signals":     buy_signals[:5],
        "note":            f"{broker} snapshot ingested — {len(rows)} symbols",
    }


# ── Portfolio chart history ───────────────────────────────────────────────────

def get_portfolio_chart_history(range_days: int = 0) -> list[dict]:
    """
    Build a daily time-series for the all-accounts portfolio chart.

    Strategy:
      - For each calendar day in the DB, pick the Fidelity snapshot with the most
        symbols (most complete upload). Sum all brokers that have data on that day.
      - For days where only Fidelity has data, Vanguard/WF values are carried forward
        from their most recent upload.
      - Returns list of {date, value, fidelity, vanguard, wellsfargo} sorted asc.

    Args:
        range_days: 0 = all history, 7 = last 7 days, 30 = last 30, 90 = last 90

    Returns:
        [{'date': 'YYYY-MM-DD', 'value': float, 'gl': float,
          'fidelity': float, 'vanguard': float, 'wellsfargo': float}, ...]
    """
    init_db()
    with get_conn() as conn:
        # Best Fidelity snapshot per day: most symbols wins
        fid_rows = conn.execute("""
            SELECT DATE(snapshot_date) as day,
                   snapshot_id,
                   COUNT(DISTINCT symbol) as syms,
                   SUM(total_value) as val,
                   SUM(total_cost)  as cost
            FROM snapshots
            WHERE broker = 'fidelity'
            GROUP BY snapshot_id
            ORDER BY day ASC, syms DESC
        """).fetchall()

        # Best Vanguard snapshot per day
        vg_rows = conn.execute("""
            SELECT DATE(snapshot_date) as day,
                   SUM(total_value) as val
            FROM snapshots
            WHERE broker = 'vanguard'
            GROUP BY snapshot_id
            ORDER BY day ASC
        """).fetchall()

        # Best WF snapshot per day
        wf_rows = conn.execute("""
            SELECT DATE(snapshot_date) as day,
                   SUM(total_value) as val
            FROM snapshots
            WHERE broker = 'wellsfargo'
            GROUP BY snapshot_id
            ORDER BY day ASC
        """).fetchall()

    # Build day → best Fidelity value (most symbols = most complete)
    fid_by_day: dict[str, dict] = {}
    for r in fid_rows:
        day = r["day"]
        if day not in fid_by_day or r["syms"] > fid_by_day[day]["syms"]:
            fid_by_day[day] = {"val": r["val"] or 0, "cost": r["cost"] or 0, "syms": r["syms"]}

    # Latest Vanguard value on or before each day (carry-forward)
    vg_days = sorted([(r["day"], r["val"] or 0) for r in vg_rows], key=lambda x: x[0])
    wf_days = sorted([(r["day"], r["val"] or 0) for r in wf_rows], key=lambda x: x[0])

    def _carry_forward(days_vals: list[tuple], target_day: str) -> float:
        """Return the most recent value on or before target_day."""
        val = 0.0
        for d, v in days_vals:
            if d <= target_day:
                val = v
            else:
                break
        return val

    # Build combined daily series
    all_days = sorted(fid_by_day.keys())
    if not all_days:
        return []

    # Filter by range
    if range_days > 0:
        from datetime import date, timedelta
        cutoff = (date.today() - timedelta(days=range_days)).isoformat()
        all_days = [d for d in all_days if d >= cutoff]

    points = []
    for day in all_days:
        fid_val  = fid_by_day[day]["val"]
        fid_cost = fid_by_day[day]["cost"]
        vg_val   = _carry_forward(vg_days, day)
        wf_val   = _carry_forward(wf_days, day)
        total    = round(fid_val + vg_val + wf_val, 2)
        gl       = round(total - fid_cost, 2)   # GL vs Fidelity cost basis (best we have)
        points.append({
            "date":       day,
            "value":      total,
            "gl":         gl,
            "fidelity":   round(fid_val, 2),
            "vanguard":   round(vg_val, 2),
            "wellsfargo": round(wf_val, 2),
        })

    return points


# ── Gain Guard — GL health tracking ──────────────────────────────────────────

def get_gl_health_history(alpaca_true_profit: float = 0.0) -> dict:
    """
    Track total gain/loss trajectory across ALL accounts over time.

    Fidelity: full daily history from SQLite snapshots (25+ days)
    Vanguard: single GL figure from performance PDF override (carry-forward to all days)
    Wells Fargo: GL from latest WF snapshot in SQLite (carry-forward)
    Alpaca: passed in as live parameter (true_profit = equity - principal)

    For each Fidelity snapshot day, we compute:
        total_gl = fidelity_gl + vanguard_gl (PDF) + wellsfargo_gl + alpaca_gl

    This matches what the header shows as total all-accounts G/L.

    Args:
        alpaca_true_profit: live Alpaca true_profit from /api/account (equity - principal)
    """
    init_db()

    with get_conn() as conn:
        # Fidelity: best snapshot per day (most symbols)
        fid_rows = conn.execute("""
            SELECT DATE(snapshot_date) AS day,
                   snapshot_date,
                   snapshot_id,
                   COUNT(DISTINCT symbol) AS syms,
                   SUM(total_value)       AS val,
                   SUM(total_cost)        AS cost,
                   SUM(total_gl)          AS gl,
                   COALESCE(SUM(today_gl), 0) AS day_gl
            FROM snapshots
            WHERE broker = 'fidelity'
            GROUP BY snapshot_id
            HAVING syms > 400
            ORDER BY snapshot_date ASC
        """).fetchall()

        # WF: latest single snapshot GL (carry-forward for all historical days)
        wf_latest = conn.execute("""
            SELECT SUM(total_gl) AS gl, SUM(total_value) AS val, SUM(total_cost) AS cost,
                   DATE(MAX(snapshot_date)) AS day
            FROM snapshots
            WHERE broker = 'wellsfargo'
              AND snapshot_id = (
                  SELECT snapshot_id FROM snapshots WHERE broker='wellsfargo'
                  GROUP BY snapshot_id ORDER BY MAX(snapshot_date) DESC LIMIT 1
              )
        """).fetchone()

    wf_gl    = round(wf_latest["gl"]    or 0, 2) if wf_latest else 0.0
    wf_day   = wf_latest["day"]                   if wf_latest else None

    # Vanguard: GL from performance PDF override file
    vg_gl = 0.0
    vg_day = None
    try:
        if PERF_OVERRIDE_PATH.exists():
            overrides = json.loads(PERF_OVERRIDE_PATH.read_text())
            vg = overrides.get("vanguard", {})
            if vg.get("has_cost"):
                vg_gl  = round(vg.get("gl", 0) or 0, 2)
                vg_day = (vg.get("as_of_date") or "")[:10] or None
    except Exception:
        pass

    if not fid_rows:
        return {
            "points": [], "peak_gl": 0, "peak_gl_date": None,
            "peak_value": 0, "peak_value_date": None,
            "current_gl": 0, "current_gl_pct": 0, "current_value": 0,
            "drawdown_gl": 0, "drawdown_gl_pct": 0, "recovery_needed": 0,
            "velocity_5": 0, "velocity_label": "NO DATA", "total_snapshots": 0,
            "breakdown": {}
        }

    # Deduplicate: best snapshot per day
    best_by_day: dict[str, dict] = {}
    for r in fid_rows:
        day = r["day"]
        if day not in best_by_day or r["syms"] > best_by_day[day]["syms"]:
            best_by_day[day] = dict(r)

    ordered = sorted(best_by_day.values(), key=lambda x: x["snapshot_date"])

    # Build points
    points = []
    prev_total_gl = None
    peak_gl       = None
    peak_gl_date  = None
    peak_value    = None
    peak_value_date = None

    for r in ordered:
        fid_val  = round(r["val"]    or 0, 2)
        fid_cost = round(r["cost"]   or 0, 2)
        fid_gl   = round(r["gl"]     or 0, 2)
        dgl      = round(r["day_gl"] or 0, 2)

        # Add non-Fidelity GL only if their data predates or matches this day
        # Vanguard PDF GL: always carry-forward (single figure, no daily history)
        vg_contrib  = vg_gl  # full return since account opened
        # WF: carry-forward if WF data was available on or before this day
        wf_contrib  = wf_gl if (wf_day and r["day"] >= wf_day) else 0.0
        # Alpaca: only add to CURRENT (latest) point — historical Alpaca not tracked
        alp_contrib = 0.0  # applied after loop for current point

        total_gl  = round(fid_gl + vg_contrib + wf_contrib, 2)
        total_val = round(fid_val, 2)  # value chart uses Fidelity only (VG/WF not in daily DB)
        total_cost = round(fid_cost + (vg_gl > 0 and 808.28 or 0), 2)  # approximate for pct

        gl_pct = round(total_gl / fid_cost * 100, 2) if fid_cost else 0.0
        gl_delta = round(total_gl - prev_total_gl, 2) if prev_total_gl is not None else 0.0
        prev_total_gl = total_gl

        if peak_gl is None or total_gl > peak_gl:
            peak_gl = total_gl
            peak_gl_date = r["day"]
        if peak_value is None or fid_val > peak_value:
            peak_value = fid_val
            peak_value_date = r["day"]

        points.append({
            "date":        r["day"],
            "snapshot_ts": r["snapshot_date"],
            "total_value": total_val,
            "total_cost":  fid_cost,
            "fidelity_gl": fid_gl,
            "vanguard_gl": vg_contrib,
            "wellsfargo_gl": wf_contrib,
            "alpaca_gl":   0.0,
            "total_gl":    total_gl,
            "gl_pct":      gl_pct,
            "gl_delta":    gl_delta,
            "today_gl":    dgl,
        })

    # Patch the LATEST point to include live Alpaca true_profit
    if points and alpaca_true_profit:
        latest = points[-1]
        latest["alpaca_gl"]  = round(alpaca_true_profit, 2)
        latest["total_gl"]   = round(latest["total_gl"] + alpaca_true_profit, 2)
        latest["gl_delta"]   = round(latest["total_gl"] - (points[-2]["total_gl"] if len(points) > 1 else latest["total_gl"]), 2)
        latest["gl_pct"]     = round(latest["total_gl"] / latest["total_cost"] * 100, 2) if latest["total_cost"] else 0.0
        # Re-check if this is new peak
        if latest["total_gl"] > (peak_gl or 0):
            peak_gl      = latest["total_gl"]
            peak_gl_date = latest["date"]
        prev_total_gl = latest["total_gl"]

    # Current values
    latest       = points[-1]
    current_gl   = latest["total_gl"]
    current_value = latest["total_value"]
    current_gl_pct = latest["gl_pct"]

    # Drawdown
    drawdown_gl      = round(current_gl - (peak_gl or 0), 2)
    drawdown_gl_pct  = round(drawdown_gl / peak_gl * 100, 2) if peak_gl else 0.0
    recovery_needed  = round(abs(drawdown_gl), 2) if drawdown_gl < 0 else 0.0

    # Velocity over last 5 uploads
    recent_deltas = [p["gl_delta"] for p in points[-5:] if p["gl_delta"] != 0]
    velocity_5    = round(sum(recent_deltas) / len(recent_deltas), 2) if recent_deltas else 0.0

    if velocity_5 > 50:
        velocity_label = "COMPOUNDING"
    elif velocity_5 > 0:
        velocity_label = "STABLE"
    elif velocity_5 > -50:
        velocity_label = "RECOVERING" if drawdown_gl < 0 else "FLAT"
    else:
        velocity_label = "ERODING"

    return {
        "points":            points,
        "peak_gl":           round(peak_gl or 0, 2),
        "peak_gl_date":      peak_gl_date,
        "peak_value":        round(peak_value or 0, 2),
        "peak_value_date":   peak_value_date,
        "current_gl":        current_gl,
        "current_gl_pct":    current_gl_pct,
        "current_value":     current_value,
        "drawdown_gl":       drawdown_gl,
        "drawdown_gl_pct":   drawdown_gl_pct,
        "recovery_needed":   recovery_needed,
        "velocity_5":        velocity_5,
        "velocity_label":    velocity_label,
        "total_snapshots":   len(points),
        "breakdown": {
            "fidelity_gl":    latest["fidelity_gl"],
            "vanguard_gl":    latest["vanguard_gl"],
            "wellsfargo_gl":  latest["wellsfargo_gl"],
            "alpaca_gl":      latest["alpaca_gl"],
        }
    }


# ── Fidelity today's G/L ─────────────────────────────────────────────────────

def get_fidelity_today_gl() -> dict:
    """
    Return today's total G/L from the most recent Fidelity snapshot.
    Sums today_gl across all symbols in the latest snapshot.

    Returns: {today_gl: float, snapshot_date: str, symbol_count: int}
    Used by /api/account to include Fidelity daily change in the Day P/L header.
    """
    init_db()
    with get_conn() as conn:
        # Get latest Fidelity snapshot_id
        latest = conn.execute("""
            SELECT snapshot_id, MAX(snapshot_date) AS sd
            FROM snapshots
            WHERE broker = 'fidelity'
            GROUP BY snapshot_id
            ORDER BY sd DESC
            LIMIT 1
        """).fetchone()
        if not latest:
            return {"today_gl": 0.0, "snapshot_date": None, "symbol_count": 0}

        result = conn.execute("""
            SELECT SUM(today_gl) AS tgl, COUNT(*) AS syms, MAX(snapshot_date) AS sd
            FROM snapshots
            WHERE snapshot_id = ?
        """, [latest["snapshot_id"]]).fetchone()

    return {
        "today_gl":      round(result["tgl"] or 0.0, 2),
        "snapshot_date": result["sd"],
        "symbol_count":  result["syms"] or 0,
    }


# ── Combined latest positions across all brokers ──────────────────────────────

def get_combined_latest_positions() -> dict:
    """
    Return the latest snapshot per broker, merged by symbol.

    Strategy:
      - For each broker, pick the single most recent snapshot_id
      - Aggregate symbol rows across all brokers: sum value/cost/gl/qty, count accounts
      - Recalculate gl_pct and portfolio_pct from aggregated totals
      - Return broker_breakdown + merged rows

    Returns:
    {
      rows: [{symbol, description, total_value, total_cost, total_gl, gl_pct,
              last_price, total_qty, accounts, brokers}],
      broker_breakdown: {fidelity: {value, cost, gl, symbols, snapshot_date},
                         vanguard: {...}, wellsfargo: {...}},
      totals: {value, cost, gl, gl_pct, symbols, accounts},
      loaded_brokers: ['fidelity', ...]
    }
    """
    init_db()
    with get_conn() as conn:
        # Latest snapshot_id per broker
        latest_snaps = conn.execute("""
            SELECT broker,
                   snapshot_id,
                   MAX(snapshot_date) AS snapshot_date,
                   COUNT(DISTINCT symbol) AS symbol_count,
                   SUM(total_value) AS total_value,
                   SUM(total_cost)  AS total_cost
            FROM snapshots
            GROUP BY broker, snapshot_id
            ORDER BY snapshot_date DESC
        """).fetchall()

    # Pick the most recent snapshot_id per broker
    best: dict[str, dict] = {}
    for row in latest_snaps:
        b = row["broker"]
        if b not in best:
            best[b] = dict(row)

    if not best:
        return {
            "rows": [], "broker_breakdown": {}, "loaded_brokers": [],
            "totals": {"value": 0, "cost": 0, "gl": 0, "gl_pct": 0, "symbols": 0, "accounts": 0}
        }

    # Pull all position rows for the selected snapshots
    snap_ids = [v["snapshot_id"] for v in best.values()]
    placeholders = ",".join("?" * len(snap_ids))

    with get_conn() as conn:
        position_rows = conn.execute(f"""
            SELECT broker, symbol, description, accounts,
                   total_qty, last_price, total_value, total_cost, total_gl
            FROM snapshots
            WHERE snapshot_id IN ({placeholders})
        """, snap_ids).fetchall()

    # Merge by symbol across brokers
    merged: dict[str, dict] = {}
    for r in position_rows:
        sym = r["symbol"]
        if sym not in merged:
            merged[sym] = {
                "symbol":      sym,
                "description": r["description"] or "",
                "total_qty":   0.0,
                "last_price":  r["last_price"],
                "total_value": 0.0,
                "total_cost":  0.0,
                "total_gl":    0.0,
                "accounts":    0,
                "brokers":     [],
            }
        m = merged[sym]
        m["total_qty"]   += (r["total_qty"]   or 0)
        m["total_value"] += (r["total_value"] or 0)
        m["total_cost"]  += (r["total_cost"]  or 0)
        m["total_gl"]    += (r["total_gl"]    or 0)
        m["accounts"]    += (r["accounts"]    or 0)
        if r["broker"] not in m["brokers"]:
            m["brokers"].append(r["broker"])
        if r["last_price"]:
            m["last_price"] = r["last_price"]

    # Recalculate derived fields
    total_portfolio_value = sum(m["total_value"] for m in merged.values())
    rows = []
    for m in merged.values():
        tc = m["total_cost"]
        tv = m["total_value"]
        tg = m["total_gl"]
        m["gl_pct"]       = round(tg / tc * 100, 4) if tc else None
        m["portfolio_pct"] = round(tv / total_portfolio_value * 100, 4) if total_portfolio_value else None
        rows.append(m)

    # Sort by total_value desc
    rows.sort(key=lambda r: r["total_value"] or 0, reverse=True)

    # Broker breakdown — pull stored GL from DB, NOT recomputed from value-cost
    # This is critical for brokers like Vanguard that have no cost basis data.
    # value - cost = wrong GL when cost = 0. Use stored total_gl which is 0 when unknown.
    perf_overrides = _load_perf_overrides()
    broker_breakdown = {}
    for b, snap in best.items():
        tv  = snap["total_value"] or 0
        tc  = snap["total_cost"]  or 0
        # Pull the actual stored GL sum — never recompute from tv-tc
        with get_conn() as conn:
            stored = conn.execute("""
                SELECT SUM(total_gl) AS gl, SUM(accounts) AS accts
                FROM snapshots WHERE snapshot_id = ?
            """, [snap["snapshot_id"]]).fetchone()
        gl         = round(stored["gl"] or 0, 2)
        acct_count = stored["accts"] or 0

        # Apply performance PDF override if available (Vanguard, WF)
        perf = perf_overrides.get(b)
        if perf and perf.get("has_cost"):
            gl      = round(perf.get("gl") or gl, 2)
            tc_disp = perf.get("cost") or tc
            gl_pct  = round(gl / tc_disp * 100, 2) if tc_disp else perf.get("gl_pct")
        else:
            tc_disp = tc
            gl_pct  = round(gl / tc * 100, 2) if tc else None

        broker_breakdown[b] = {
            "value":         round(tv, 2),
            "cost":          round(tc_disp, 2),
            "gl":            gl,
            "gl_pct":        gl_pct,
            "symbols":       snap["symbol_count"],
            "snapshot_date": snap["snapshot_date"],
            "accounts":      acct_count,
            "has_cost":      tc > 0 or (perf is not None and perf.get("has_cost")),
            "perf_source":   perf.get("source_file") if perf else None,
            "perf_as_of":    perf.get("as_of_date")  if perf else None,
        }

    # Add PDF-only brokers (e.g. Vanguard with no CSV positions) into broker_breakdown
    for b, perf in perf_overrides.items():
        if b not in broker_breakdown and perf.get("value"):
            broker_breakdown[b] = {
                "value":         round(perf.get("value") or 0, 2),
                "cost":          round(perf.get("cost")  or 0, 2),
                "gl":            round(perf.get("gl")    or 0, 2),
                "gl_pct":        round(perf.get("gl_pct") or 0, 2),
                "symbols":       0,
                "snapshot_date": perf.get("updated_at", ""),
                "accounts":      0,
                "has_cost":      bool(perf.get("has_cost")),
                "perf_source":   perf.get("source_file"),
                "perf_as_of":    perf.get("as_of_date"),
            }

    # Grand totals — include PDF-only broker values/gl
    tv_all   = sum(v.get("value", 0) for v in broker_breakdown.values())
    tc_all   = sum(v.get("cost",  0) for v in broker_breakdown.values())
    gl_all   = sum(v.get("gl",    0) for v in broker_breakdown.values())
    acct_all = sum(v.get("accounts", 0) for v in broker_breakdown.values())

    return {
        "rows":             rows,
        "broker_breakdown": broker_breakdown,
        "loaded_brokers":   list(best.keys()),
        "totals": {
            "value":    round(tv_all, 2),
            "cost":     round(tc_all, 2),
            "gl":       round(gl_all, 2),
            "gl_pct":   round(gl_all / tc_all * 100, 2) if tc_all else 0,
            "symbols":  len(rows),
            "accounts": acct_all,
        }
    }


# ── Ingest ────────────────────────────────────────────────────────────────────

def ingest_snapshot(filepath: str | Path) -> dict:
    """
    Full pipeline: parse CSV → insert snapshots → calculate deviations.

    Returns a summary dict:
    {
      snapshot_id: str,
      snapshot_date: str,
      filename: str,
      symbol_count: int,
      total_value: float,
      deviation_count: int,
      buy_signals: int,
      top_signals: list[dict],   # top 5 BUY by deploy_amount
      note: str,                 # e.g. 'First snapshot — no deviations yet'
    }
    """
    filepath = Path(filepath)
    init_db()

    rows = parse_fidelity_csv(filepath)
    if not rows:
        raise ValueError(f"No valid position rows parsed from {filepath.name}")

    snapshot_id   = str(uuid.uuid4())
    filename      = filepath.name
    # Parse snapshot date from filename: fidelity_YYYY-MM-DD_HHMMSS_...csv
    # Fall back to UTC now if pattern not found
    _dm = re.match(r'fidelity_(\d{4}-\d{2}-\d{2})_(\d{6})', filename)
    if _dm:
        date_str = _dm.group(1)
        time_str = _dm.group(2)
        snapshot_date = f"{date_str}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
    else:
        snapshot_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    total_value   = sum(r["total_value"] for r in rows)

    # Insert all position rows for this snapshot
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO snapshots (
                snapshot_id, snapshot_date, filename,
                symbol, description, accounts,
                total_qty, last_price, total_value, total_cost,
                total_gl, today_gl, gl_pct, portfolio_pct
            ) VALUES (
                :snapshot_id, :snapshot_date, :filename,
                :symbol, :description, :accounts,
                :total_qty, :last_price, :total_value, :total_cost,
                :total_gl, :today_gl, :gl_pct, :portfolio_pct
            )
        """, [
            {**r,
             "snapshot_id":   snapshot_id,
             "snapshot_date": snapshot_date,
             "filename":      filename,
             "today_gl":      r.get("today_gl", 0.0) or 0.0}
            for r in rows
        ])

    # Calculate deviations against previous snapshot
    deviations = calculate_deviations(snapshot_id)

    buy_signals = [d for d in deviations if d["signal"] == "BUY"]
    buy_signals.sort(key=lambda d: d["deploy_amount"], reverse=True)

    note = "" if deviations else "First snapshot or no overlapping symbols — no deviations calculated."

    return {
        "snapshot_id":    snapshot_id,
        "snapshot_date":  snapshot_date,
        "filename":       filename,
        "symbol_count":   len(rows),
        "total_value":    round(total_value, 2),
        "deviation_count":len(deviations),
        "buy_signals":    len(buy_signals),
        "top_signals":    buy_signals[:5],
        "note":           note,
    }


def filename_already_ingested(filename: str) -> bool:
    """Check if a CSV filename has already been loaded into the DB."""
    init_db()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM snapshots WHERE filename = ? LIMIT 1", (filename,)
        ).fetchone()
    return row is not None


# ── Query helpers (used by Flask routes) ─────────────────────────────────────

def get_snapshots() -> list[dict]:
    """
    Return all snapshots with summary stats.
    One row per snapshot_id sorted newest first.
    """
    init_db()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                snapshot_id,
                snapshot_date,
                filename,
                COUNT(*)        AS symbol_count,
                SUM(total_value)AS total_value
            FROM snapshots
            GROUP BY snapshot_id
            ORDER BY snapshot_date DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_deviations(snapshot_id: Optional[str] = None) -> list[dict]:
    """
    Return BUY deviations for a snapshot (defaults to latest).
    Sorted by deploy_amount descending.
    """
    init_db()
    with get_conn() as conn:
        if snapshot_id is None:
            # Use the latest snapshot
            meta = conn.execute("""
                SELECT snapshot_id FROM snapshots
                ORDER BY snapshot_date DESC LIMIT 1
            """).fetchone()
            if not meta:
                return []
            snapshot_id = meta["snapshot_id"]

        rows = conn.execute("""
            SELECT * FROM deviations
            WHERE curr_snapshot_id = ?
              AND signal = 'BUY'
              AND symbol NOT LIKE '%**%'
            ORDER BY deploy_amount DESC
        """, (snapshot_id,)).fetchall()

    return [dict(r) for r in rows]


def get_symbol_history(symbol: str) -> list[dict]:
    """
    Return full G/L time series for one symbol across all snapshots.
    Used for the sell-mistake analysis / position drift view.
    """
    init_db()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                snapshot_date,
                snapshot_id,
                filename,
                accounts,
                total_qty,
                last_price,
                total_value,
                total_cost,
                total_gl,
                gl_pct,
                portfolio_pct
            FROM snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date ASC
        """, (symbol.upper(),)).fetchall()
    return [dict(r) for r in rows]


def get_summary() -> dict:
    """
    High-level summary: snapshot count, date range, symbols tracked,
    top 10 BUY signals from the most recent deviation calc.
    """
    init_db()
    with get_conn() as conn:
        counts = conn.execute("""
            SELECT
                COUNT(DISTINCT snapshot_id) AS snapshot_count,
                MIN(snapshot_date)          AS earliest,
                MAX(snapshot_date)          AS latest,
                COUNT(DISTINCT symbol)      AS symbols_tracked
            FROM snapshots
        """).fetchone()

        latest_snap = conn.execute("""
            SELECT snapshot_id FROM snapshots
            ORDER BY snapshot_date DESC LIMIT 1
        """).fetchone()

        top_signals = []
        if latest_snap:
            rows = conn.execute("""
                SELECT * FROM deviations
                WHERE curr_snapshot_id = ?
                  AND signal = 'BUY'
                  AND symbol NOT LIKE '%**%'
                ORDER BY deploy_amount DESC
                LIMIT 10
            """, (latest_snap["snapshot_id"],)).fetchall()
            top_signals = [dict(r) for r in rows]

    return {
        "snapshot_count":  counts["snapshot_count"],
        "date_range":      {"earliest": counts["earliest"], "latest": counts["latest"]},
        "symbols_tracked": counts["symbols_tracked"],
        "top_signals":     top_signals,
    }


# ── Intelligence layer — buy signals, hot streaks, backtest ──────────────────

def get_buy_list_signals(limit: int = 20) -> list[dict]:
    """
    Return top Fidelity deviation BUY signals formatted for the dashboard BUY LIST.

    Field names match the existing buy list shape (sym, buy, source, reason,
    accts, edgar_score, is_mf, combined) so _renderBuyBar works without modification.

    Score mapping (conviction_multiplier → combined):
      0.50 → 5   1 account — experimental
      0.75 → 8   2-5 accounts — early conviction
      1.00 → 12  6-15 accounts — established
      1.25 → 16  16+ accounts — core holding
    """
    init_db()
    score_map = {0.50: 5, 0.75: 8, 1.00: 12, 1.25: 16}

    with get_conn() as conn:
        # Find the most recent snapshot that actually has BUY deviations
        # (last few uploads may be identical CSVs with 0 deviations)
        latest = conn.execute("""
            SELECT DISTINCT curr_snapshot_id AS snapshot_id
            FROM deviations
            WHERE signal = 'BUY'
              AND symbol NOT LIKE '%**%'
            ORDER BY calculated_at DESC
            LIMIT 1
        """).fetchone()

        if not latest:
            return []
        latest_id = latest["snapshot_id"]

        rows = conn.execute("""
            SELECT
                d.symbol,
                d.deploy_amount,
                d.accounts,
                d.direction,
                d.budget_ceiling,
                d.gl_delta,
                d.conviction_multiplier,
                s.description
            FROM deviations d
            JOIN snapshots s
                ON s.symbol      = d.symbol
               AND s.snapshot_id = d.curr_snapshot_id
            WHERE d.curr_snapshot_id = ?
              AND d.signal = 'BUY'
              AND d.deploy_amount >= 1.10
              AND d.symbol NOT LIKE '%**%'
            ORDER BY d.deploy_amount DESC
            LIMIT ?
        """, (latest_id, limit)).fetchall()

    results = []
    for r in rows:
        cm     = r["conviction_multiplier"]
        cm_key = min(score_map.keys(), key=lambda k: abs(k - cm))
        score  = score_map[cm_key]
        results.append({
            "sym":                   r["symbol"],
            "buy":                   round(r["deploy_amount"], 2),
            "source":                "fid-dev",
            "reason":                r["direction"],
            "accts":                 r["accounts"],
            "edgar_score":           None,
            "is_mf":                 False,
            "combined":              score,
            "budget_ceiling":        round(r["budget_ceiling"], 2),
            "gl_delta":              round(r["gl_delta"], 2),
            "conviction_multiplier": cm,
            "description":           r["description"] or "",
        })
    return results


def get_hot_streaks(min_streak: int = 3) -> list[dict]:
    """
    Return tickers with an ACTIVE BUY signal streak through the most recent snapshot.

    ACTIVE = streak must include the most recent snapshot.
    If a symbol had BUY in snaps 1-5 but NOT snap 6 (latest), streak=0, excluded.
    """
    init_db()

    with get_conn() as conn:
        all_snaps = [
            row["snapshot_id"]
            for row in conn.execute("""
                SELECT snapshot_id
                FROM snapshots
                GROUP BY snapshot_id
                ORDER BY MAX(snapshot_date) ASC
            """).fetchall()
        ]
        if len(all_snaps) < 2:
            return []

        latest_snap_id = all_snaps[-1]

        # If the latest snapshot has no BUY deviations (identical CSV re-upload),
        # walk back to find the most recent one that does
        buy_snap_check = conn.execute("""
            SELECT DISTINCT curr_snapshot_id
            FROM deviations
            WHERE signal = 'BUY' AND symbol NOT LIKE '%**%'
            ORDER BY calculated_at DESC
            LIMIT 1
        """).fetchone()
        if buy_snap_check:
            latest_snap_id = buy_snap_check["curr_snapshot_id"]

        buy_rows = conn.execute("""
            SELECT symbol, curr_snapshot_id, deploy_amount, direction, accounts
            FROM deviations
            WHERE signal = 'BUY'
              AND symbol NOT LIKE '%**%'
        """).fetchall()

    # Build lookup: symbol → {snap_id: {deploy, direction, accounts}}
    buy_lookup: dict = {}
    for r in buy_rows:
        sym = r["symbol"]
        sid = r["curr_snapshot_id"]
        if sym not in buy_lookup:
            buy_lookup[sym] = {}
        buy_lookup[sym][sid] = {
            "deploy":    r["deploy_amount"],
            "direction": r["direction"],
            "accounts":  r["accounts"],
        }

    results = []
    for symbol, snap_signals in buy_lookup.items():
        # Must have a BUY in the most recent snapshot to qualify
        if latest_snap_id not in snap_signals:
            continue
        # Walk backwards counting unbroken streak
        streak = 0
        for snap_id in reversed(all_snaps):
            if snap_id in snap_signals:
                streak += 1
            else:
                break
        if streak < min_streak:
            continue
        latest = snap_signals[latest_snap_id]
        results.append({
            "symbol":                symbol,
            "streak_length":         streak,
            "latest_deploy_amount":  round(latest["deploy"], 2),
            "latest_direction":      latest["direction"],
            "accounts":              latest["accounts"],
            "total_snapshots_as_buy":len(snap_signals),
        })

    results.sort(key=lambda x: (-x["streak_length"], -x["latest_deploy_amount"]))
    return results


def get_true_profit_history() -> dict:
    """
    Return portfolio-level true profit time series from all ingested snapshots.

    True profit = SUM(total_value) - SUM(total_cost) per snapshot.
    Cost basis IS the principal — Fidelity calculates it per position.
    No separate deposit tracking needed.

    Returns:
    {
      points: [{date, value, cost, gl, gl_pct}, ...],   # one per snapshot, oldest first
      latest: {date, value, cost, gl, gl_pct},           # most recent snapshot stats
      earliest: {date, value, cost, gl, gl_pct},         # oldest snapshot stats
      total_snapshots: int,
    }
    """
    init_db()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                snapshot_date,
                filename,
                SUM(total_value) AS portfolio_value,
                SUM(total_cost)  AS portfolio_cost
            FROM snapshots
            GROUP BY snapshot_id
            ORDER BY snapshot_date ASC
        """).fetchall()

    if not rows:
        return {"points": [], "latest": None, "earliest": None, "total_snapshots": 0}

    points = []
    for r in rows:
        val  = r["portfolio_value"] or 0.0
        cost = r["portfolio_cost"]  or 0.0
        gl   = round(val - cost, 2)
        gl_pct = round(gl / cost * 100, 2) if cost else 0.0
        points.append({
            "date":    r["snapshot_date"],
            "value":   round(val, 2),
            "cost":    round(cost, 2),
            "gl":      gl,
            "gl_pct":  gl_pct,
        })

    return {
        "points":           points,
        "latest":           points[-1],
        "earliest":         points[0],
        "total_snapshots":  len(points),
    }


def backtest_signals() -> dict:
    """
    Backtest all historical BUY signals against subsequent snapshot outcomes.

    WIN = G/L moved in predicted direction:
      RECOVERING / PULLBACK      → outcome_gl_delta > 0
      DETERIORATING / ACCELERATING → outcome_gl_delta < 0
    LOSS = opposite.
    EXCLUDE = no subsequent snapshot exists for that symbol.

    Confidence: HIGH >65% + >=20 signals, MEDIUM 50-65% or <20, LOW <50%.
    """
    init_db()

    with get_conn() as conn:
        signals = conn.execute("""
            SELECT
                d.symbol,
                d.curr_snapshot_id,
                d.direction,
                d.deploy_amount,
                d.curr_gl,
                s.snapshot_date AS curr_date
            FROM deviations d
            JOIN snapshots s
                ON s.snapshot_id = d.curr_snapshot_id
               AND s.symbol      = d.symbol
            WHERE d.signal = 'BUY'
              AND d.symbol NOT LIKE '%**%'
        """).fetchall()

        all_snap_rows = conn.execute("""
            SELECT symbol, snapshot_date, total_gl
            FROM snapshots
            ORDER BY symbol, snapshot_date ASC
        """).fetchall()

    # Per-symbol timeline: symbol → [(date, gl), ...]
    timelines: dict = defaultdict(list)
    for r in all_snap_rows:
        timelines[r["symbol"]].append((r["snapshot_date"], r["total_gl"]))

    EXPECT_IMPROVEMENT   = {"RECOVERING", "PULLBACK"}
    EXPECT_DETERIORATION = {"DETERIORATING", "ACCELERATING"}

    direction_stats: dict = {}
    for direction in ("RECOVERING", "PULLBACK", "DETERIORATING", "ACCELERATING"):
        direction_stats[direction] = {
            "direction":      direction,
            "total_signals":  0,
            "wins":           0,
            "losses":         0,
            "excluded":       0,
            "deploy_amounts": [],
            "outcome_deltas": [],
        }

    for sig in signals:
        sym       = sig["symbol"]
        curr_date = sig["curr_date"]
        curr_gl   = sig["curr_gl"]
        direction = sig["direction"]
        deploy    = sig["deploy_amount"]
        if direction not in direction_stats:
            continue  # skip STABLE
        ds = direction_stats[direction]
        ds["total_signals"] += 1
        ds["deploy_amounts"].append(deploy)

        # Find next snapshot for this symbol after curr_date
        next_gl = None
        for (snap_date, snap_gl) in timelines.get(sym, []):
            if snap_date > curr_date:
                next_gl = snap_gl
                break

        if next_gl is None:
            ds["excluded"] += 1
            continue

        outcome_delta = next_gl - (curr_gl or 0.0)
        ds["outcome_deltas"].append(outcome_delta)

        if direction in EXPECT_IMPROVEMENT:
            if outcome_delta > 0:
                ds["wins"] += 1
            else:
                ds["losses"] += 1
        else:
            if outcome_delta < 0:
                ds["wins"] += 1
            else:
                ds["losses"] += 1

    by_direction = []
    total_wins = total_losses = total_excluded = total_signals = 0

    for direction, ds in direction_stats.items():
        countable   = ds["wins"] + ds["losses"]
        win_rate    = (ds["wins"] / countable * 100) if countable > 0 else 0.0
        avg_deploy  = (sum(ds["deploy_amounts"]) / len(ds["deploy_amounts"])
                       if ds["deploy_amounts"] else 0.0)
        avg_outcome = (sum(ds["outcome_deltas"]) / len(ds["outcome_deltas"])
                       if ds["outcome_deltas"] else 0.0)
        by_direction.append({
            "direction":         direction,
            "total_signals":     ds["total_signals"],
            "wins":              ds["wins"],
            "losses":            ds["losses"],
            "excluded":          ds["excluded"],
            "win_rate_pct":      round(win_rate, 1),
            "avg_deploy_amount": round(avg_deploy, 2),
            "avg_outcome_delta": round(avg_outcome, 2),
        })
        total_signals  += ds["total_signals"]
        total_wins     += ds["wins"]
        total_losses   += ds["losses"]
        total_excluded += ds["excluded"]

    countable_total = total_wins + total_losses
    overall_rate    = (total_wins / countable_total * 100) if countable_total > 0 else 0.0

    if overall_rate > 65 and countable_total >= 20:
        confidence = "HIGH"
    elif overall_rate >= 50 and countable_total >= 20:
        confidence = "MEDIUM"
    elif countable_total < 20:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "by_direction": by_direction,
        "overall": {
            "total_signals":        total_signals,
            "total_wins":           total_wins,
            "total_losses":         total_losses,
            "total_excluded":       total_excluded,
            "overall_win_rate_pct": round(overall_rate, 1),
            "confidence_rating":    confidence,
        },
    }


def get_gain_loss_split_history() -> dict:
    """
    For every ingested snapshot, split positions into:
      - Green pool  = SUM(total_gl) for all symbols where total_gl > 0
      - Red pool    = SUM(total_gl) for all symbols where total_gl < 0
      - Net G/L     = green_pool + red_pool  (same as true profit)

    Then compute:
      - avg_green / avg_red  across ALL snapshots
      - variance from first snapshot to latest (raw delta + pct change)
      - green_count / red_count = number of symbols in each bucket per snapshot

    Returns:
    {
      points: [
        { date, green_pool, red_pool, net_gl,
          green_count, red_count, total_symbols }
      ],
      latest:   { date, green_pool, red_pool, net_gl, green_count, red_count },
      earliest: { date, green_pool, red_pool, net_gl, green_count, red_count },
      avg_green:      float,  # average green pool across all snapshots
      avg_red:        float,  # average red pool across all snapshots
      green_variance: { delta, pct },  # latest.green_pool vs earliest.green_pool
      red_variance:   { delta, pct },  # latest.red_pool   vs earliest.red_pool
      total_snapshots: int,
    }
    """
    init_db()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                snapshot_id,
                snapshot_date,
                SUM(CASE WHEN total_gl > 0 THEN total_gl ELSE 0 END) AS green_pool,
                SUM(CASE WHEN total_gl < 0 THEN total_gl ELSE 0 END) AS red_pool,
                SUM(total_gl)                                          AS net_gl,
                COUNT(CASE WHEN total_gl > 0 THEN 1 END)              AS green_count,
                COUNT(CASE WHEN total_gl < 0 THEN 1 END)              AS red_count,
                COUNT(*)                                               AS total_symbols
            FROM snapshots
            GROUP BY snapshot_id
            ORDER BY snapshot_date ASC
        """).fetchall()

    if not rows:
        return {
            "points": [], "latest": None, "earliest": None,
            "avg_green": 0.0, "avg_red": 0.0,
            "green_variance": {"delta": 0.0, "pct": 0.0},
            "red_variance":   {"delta": 0.0, "pct": 0.0},
            "total_snapshots": 0,
        }

    points = []
    for r in rows:
        gp  = round(r["green_pool"] or 0.0, 2)
        rp  = round(r["red_pool"]   or 0.0, 2)
        net = round(r["net_gl"]     or 0.0, 2)
        points.append({
            "date":          r["snapshot_date"],
            "green_pool":    gp,
            "red_pool":      rp,
            "net_gl":        net,
            "green_count":   r["green_count"]   or 0,
            "red_count":     r["red_count"]     or 0,
            "total_symbols": r["total_symbols"] or 0,
        })

    avg_green = round(sum(p["green_pool"] for p in points) / len(points), 2)
    avg_red   = round(sum(p["red_pool"]   for p in points) / len(points), 2)

    first, last = points[0], points[-1]

    def _variance(early_val, late_val):
        delta = round(late_val - early_val, 2)
        pct   = round(delta / abs(early_val) * 100, 2) if early_val else 0.0
        return {"delta": delta, "pct": pct}

    return {
        "points":           points,
        "latest":           last,
        "earliest":         first,
        "avg_green":        avg_green,
        "avg_red":          avg_red,
        "green_variance":   _variance(first["green_pool"], last["green_pool"]),
        "red_variance":     _variance(first["red_pool"],   last["red_pool"]),
        "total_snapshots":  len(points),
    }


def get_compounding_scorecard() -> dict:
    """
    Compounding engine scorecard — answers: is the system working?

    Returns per-symbol trend (last 3 snapshots), recovered positions count,
    velocity of gain/loss pools, system verdict, and ETF basket health.

    System verdict:
      COMPOUNDING  — green pool growing, loss pool stable or recovering
      STABLE       — green pool holding, losses not growing meaningfully
      WATCH        — losses growing faster than gains
      DRAWDOWN     — green pool shrinking
    """
    init_db()
    with get_conn() as conn:
        # --- last 5 distinct snapshot IDs in chronological order ---
        snap_ids = [r["snapshot_id"] for r in conn.execute("""
            SELECT DISTINCT snapshot_id, MIN(snapshot_date) AS sd
            FROM snapshots GROUP BY snapshot_id ORDER BY sd ASC
        """).fetchall()]

        if len(snap_ids) < 2:
            return {"error": "Need at least 2 snapshots", "verdict": "INSUFFICIENT_DATA"}

        last5 = snap_ids[-5:]   # up to 5 most recent
        prev3 = snap_ids[-3:]   # last 3 for per-symbol trend

        # --- per-symbol gl in the last 3 snapshots ---
        placeholders = ",".join("?" * len(prev3))
        sym_rows = conn.execute(f"""
            SELECT snapshot_id, symbol, total_gl, total_value, last_price
            FROM snapshots
            WHERE snapshot_id IN ({placeholders})
            ORDER BY symbol, snapshot_id
        """, prev3).fetchall()

        # Build { symbol: [(snap_id, total_gl, total_value, last_price), ...] }
        from collections import defaultdict
        sym_history: dict = defaultdict(list)
        for r in sym_rows:
            sym_history[r["symbol"]].append({
                "snap": r["snapshot_id"],
                "gl":   r["total_gl"]   or 0.0,
                "val":  r["total_value"] or 0.0,
                "px":   r["last_price"]  or 0.0,
            })

        # Per-symbol trend: UP / DOWN / FLAT
        sym_trends = {}
        for sym, hist in sym_history.items():
            if len(hist) < 2:
                sym_trends[sym] = "FLAT"
                continue
            delta = hist[-1]["gl"] - hist[0]["gl"]
            if delta > 0.50:
                sym_trends[sym] = "UP"
            elif delta < -0.50:
                sym_trends[sym] = "DOWN"
            else:
                sym_trends[sym] = "FLAT"

        # --- green/red pool per snapshot for last 5 ---
        pool_rows = conn.execute(f"""
            SELECT snapshot_id, MIN(snapshot_date) AS sd,
                   SUM(CASE WHEN total_gl > 0 THEN total_gl ELSE 0 END) AS green_pool,
                   SUM(CASE WHEN total_gl < 0 THEN total_gl ELSE 0 END) AS red_pool,
                   COUNT(CASE WHEN total_gl > 0 THEN 1 END) AS green_count,
                   COUNT(CASE WHEN total_gl < 0 THEN 1 END) AS red_count
            FROM snapshots
            WHERE snapshot_id IN ({",".join("?" * len(last5))})
            GROUP BY snapshot_id ORDER BY sd ASC
        """, last5).fetchall()
        pools = [{"green": r["green_pool"] or 0.0, "red": r["red_pool"] or 0.0,
                  "green_count": r["green_count"] or 0, "red_count": r["red_count"] or 0,
                  "date": r["sd"]} for r in pool_rows]

        # --- recovered positions: red in earliest snap, green in latest snap ---
        earliest_id = snap_ids[0]
        latest_id   = snap_ids[-1]
        recovered_rows = conn.execute("""
            SELECT e.symbol
            FROM snapshots e
            JOIN snapshots l ON e.symbol = l.symbol
            WHERE e.snapshot_id = ? AND l.snapshot_id = ?
              AND e.total_gl < 0 AND l.total_gl > 0
        """, [earliest_id, latest_id]).fetchall()
        recovered = [r["symbol"] for r in recovered_rows]

        # --- ETF basket health (DIA, QQQ, VOO, SGOL) in latest snapshot ---
        etf_syms = ("DIA", "QQQ", "VOO", "SGOL")
        etf_rows = conn.execute(f"""
            SELECT symbol, total_gl, gl_pct, total_value
            FROM snapshots
            WHERE snapshot_id = ? AND symbol IN ({",".join("?" * len(etf_syms))})
        """, [latest_id, *etf_syms]).fetchall()
        etf_health = {r["symbol"]: {"gl": r["total_gl"] or 0.0,
                                    "gl_pct": r["gl_pct"] or 0.0,
                                    "val": r["total_value"] or 0.0}
                      for r in etf_rows}

        # --- velocity: avg change per snapshot over last 5 ---
        if len(pools) >= 2:
            green_velocity = round(
                (pools[-1]["green"] - pools[0]["green"]) / (len(pools) - 1), 2)
            red_velocity = round(
                (pools[-1]["red"] - pools[0]["red"]) / (len(pools) - 1), 2)
        else:
            green_velocity = red_velocity = 0.0

        # --- verdict logic ---
        latest_green = pools[-1]["green"] if pools else 0.0
        latest_red   = pools[-1]["red"]   if pools else 0.0
        first_green  = pools[0]["green"]  if pools else 0.0

        if green_velocity > 0 and abs(red_velocity) < green_velocity * 0.5:
            verdict = "COMPOUNDING"
        elif green_velocity > 0 and abs(red_velocity) < green_velocity:
            verdict = "STABLE"
        elif green_velocity > 0 and abs(red_velocity) >= green_velocity:
            verdict = "WATCH"
        else:
            verdict = "DRAWDOWN"

    return {
        "verdict":          verdict,
        "green_velocity":   green_velocity,   # $ per snapshot
        "red_velocity":     red_velocity,
        "recovered_count":  len(recovered),
        "recovered_syms":   recovered[:20],   # cap at 20 for UI
        "sym_trends":       sym_trends,        # { SYM: UP/DOWN/FLAT }
        "etf_health":       etf_health,
        "latest_green":     round(latest_green, 2),
        "latest_red":       round(latest_red, 2),
        "pools_history":    pools,             # last 5 for mini sparkline
        "total_snapshots":  len(snap_ids),
    }
