#!/usr/bin/env python3
"""
batch_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scans ~/Documents/Trading Vault/Fidelity_History/ for all fidelity_*.csv
files and ingests any that are not already in the database.

Run once to backfill your existing saved CSVs:
    python3 batch_ingest.py

Run again after downloading new CSVs — already-ingested files are skipped.
Snapshots are processed in chronological filename order so deviation diffs
are calculated against the correct prior snapshot.
"""

import sys
from pathlib import Path

# Allow running from any directory
sys.path.insert(0, str(Path(__file__).parent))

from fidelity_db import (
    VAULT_DIR,
    init_db,
    ingest_snapshot,
    filename_already_ingested,
)


def main() -> None:
    init_db()

    # Find all fidelity_*.csv files in the vault directory
    csv_files = sorted(VAULT_DIR.glob("fidelity_*.csv"))

    if not csv_files:
        print(f"No fidelity_*.csv files found in:\n  {VAULT_DIR}")
        print("Download a Fidelity portfolio CSV and save it there first.")
        return

    print(f"Found {len(csv_files)} fidelity CSV file(s) in vault.")
    print(f"Database: {VAULT_DIR / 'portfolio_history.db'}\n")

    processed   = 0
    skipped     = 0
    errors      = 0
    total_syms  = 0
    total_devs  = 0
    total_buys  = 0

    # Process in chronological order — filename includes date so sort() is correct.
    # Pattern: fidelity_2026-05-22_185652_Portfolio_Positions_May-22-2026.csv
    for csv_path in csv_files:
        filename = csv_path.name

        if filename_already_ingested(filename):
            print(f"  SKIP  {filename}  (already in DB)")
            skipped += 1
            continue

        try:
            result = ingest_snapshot(csv_path)
            processed  += 1
            total_syms  = result["symbol_count"]   # last snapshot's count
            total_devs += result["deviation_count"]
            total_buys += result["buy_signals"]

            print(
                f"  OK    {filename}\n"
                f"        snapshot_id={result['snapshot_id'][:8]}…  "
                f"symbols={result['symbol_count']}  "
                f"value=${result['total_value']:,.2f}  "
                f"deviations={result['deviation_count']}  "
                f"BUY signals={result['buy_signals']}"
            )

            if result["note"]:
                print(f"        NOTE: {result['note']}")

            if result["top_signals"]:
                print("        Top BUY signals:")
                for sig in result["top_signals"]:
                    print(
                        f"          {sig['symbol']:<8} "
                        f"deploy=${sig['deploy_amount']:.2f}  "
                        f"direction={sig['direction']:<14}  "
                        f"accounts={sig['accounts']}  "
                        f"gl_delta=${sig['gl_delta']:+.2f}"
                    )

        except Exception as exc:
            errors += 1
            print(f"  ERR   {filename}  → {exc}")

        print()

    # Summary
    print("━" * 60)
    print(f"Batch ingest complete.")
    print(f"  Files processed : {processed}")
    print(f"  Files skipped   : {skipped}  (already in DB)")
    print(f"  Errors          : {errors}")
    print(f"  Symbols tracked : {total_syms}  (from most recent snapshot)")
    print(f"  Deviations calc : {total_devs}")
    print(f"  BUY signals     : {total_buys}")
    print(f"  DB location     : {VAULT_DIR / 'portfolio_history.db'}")


if __name__ == "__main__":
    main()
