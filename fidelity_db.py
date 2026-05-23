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
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────

VAULT_DIR = Path.home() / "Documents" / "Trading Vault" / "Fidelity_History"
DB_PATH   = VAULT_DIR / "portfolio_history.db"

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
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id     TEXT    NOT NULL,
                snapshot_date   TEXT    NOT NULL,
                filename        TEXT    NOT NULL,
                symbol          TEXT    NOT NULL,
                description     TEXT,
                accounts        INTEGER,
                total_qty       REAL,
                last_price      REAL,
                total_value     REAL,
                total_cost      REAL,
                total_gl        REAL,
                gl_pct          REAL,
                portfolio_pct   REAL
            );

            CREATE INDEX IF NOT EXISTS idx_snap_id
                ON snapshots(snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_snap_symbol
                ON snapshots(symbol);
            CREATE INDEX IF NOT EXISTS idx_snap_date
                ON snapshots(snapshot_date);

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
      - Purely numeric:    227867696
    Invalid (footer/header junk):
      - Starts with quote, blank, disclaimer text, 'Account Number' header repeat
    """
    if not val or not isinstance(val, str):
        return False
    val = val.strip()
    # Must start with a letter followed by digits, OR be all digits (8-10 chars)
    return bool(re.match(r'^[A-Z]\d+$', val) or re.match(r'^\d{6,12}$', val))


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

    df["_ticker"] = df["Account Name"].str.strip()
    df["_name"]   = df["Symbol"].str.strip()
    df["_qty"]    = df["Description"].apply(_parse_money)
    df["_price"]  = df["Quantity"].apply(_parse_money)
    df["_val"]    = df["Last Price Change"].apply(_parse_money)
    df["_cost"]   = df["Percent Of Account"].apply(_parse_money)

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
    snapshot_date = datetime.utcnow().isoformat()
    filename      = filepath.name
    total_value   = sum(r["total_value"] for r in rows)

    # Insert all position rows for this snapshot
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO snapshots (
                snapshot_id, snapshot_date, filename,
                symbol, description, accounts,
                total_qty, last_price, total_value, total_cost,
                total_gl, gl_pct, portfolio_pct
            ) VALUES (
                :snapshot_id, :snapshot_date, :filename,
                :symbol, :description, :accounts,
                :total_qty, :last_price, :total_value, :total_cost,
                :total_gl, :gl_pct, :portfolio_pct
            )
        """, [
            {**r, "snapshot_id": snapshot_id, "snapshot_date": snapshot_date, "filename": filename}
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
