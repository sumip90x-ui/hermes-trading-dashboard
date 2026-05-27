"""
oracle_brain.py — ORACLE Research Intelligence Reader
Reads ORACLE research files from disk and assembles context for the chatbot.
No external dependencies. No Claude calls. No vector database.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta

# Confirmed paths from audit (2026-05-27)
OBSIDIAN_TICKERS  = Path.home() / "Documents" / "OracleVault" / "ORACLE" / "tickers"
ORACLE_REPORTS    = Path.home() / "ORACLE" / "reports"
MIROFISH_SEEDS    = Path.home() / "ORACLE" / "mirofish_seeds"
EDGAR_SEEDS       = Path.home() / "Documents" / "EDGAR"
ORACLE_FILINGS    = Path.home() / "ORACLE" / "filings"
NEWS_MAX_AGE_DAYS = 7


def get_obsidian_note(ticker: str) -> str | None:
    """Read the Obsidian ticker note for this ticker."""
    ticker = ticker.upper().strip()
    path = OBSIDIAN_TICKERS / f"{ticker}.md"
    try:
        if path.exists():
            return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        pass
    return None


def get_oracle_report(ticker: str) -> tuple[str | None, datetime | None]:
    """
    Find the most recent ORACLE composite report for this ticker.
    Searches: ORACLE/reports/ .md files, then MiroShark preset_templates JSON.
    Returns (text_excerpt, mod_datetime) or (None, None).
    """
    ticker = ticker.upper().strip()
    ticker_lc = ticker.lower()

    try:
        # Search 1: ORACLE/reports/ markdown files
        candidates = []
        for f in ORACLE_REPORTS.rglob("*.md"):
            if ticker in f.name.upper():
                candidates.append(f)
        if candidates:
            candidates.sort(key=lambda f: f.stat().st_mtime, reverse=True)
            best = candidates[0]
            mod_dt = datetime.fromtimestamp(best.stat().st_mtime)
            text = best.read_text(encoding="utf-8", errors="replace")
            header = (
                f"[Source: {best.name} | Last modified: {mod_dt.strftime('%Y-%m-%d')}]\n\n"
            )
            return header + text[:8000], mod_dt

        # Search 2: MiroShark preset_templates JSON (hermes_{ticker}.json)
        import json
        preset_dir = (
            Path.home() / "Documents" / "MiroShark" / "backend" / "app" / "preset_templates"
        )
        preset_path = preset_dir / f"hermes_{ticker_lc}.json"
        if preset_path.exists():
            mod_dt = datetime.fromtimestamp(preset_path.stat().st_mtime)
            data = json.loads(preset_path.read_text(encoding="utf-8"))
            seed_doc = data.get("seed_document", "")
            if seed_doc:
                header = (
                    f"[Source: preset_template hermes_{ticker_lc}.json "
                    f"| Last modified: {mod_dt.strftime('%Y-%m-%d')}]\n\n"
                )
                return header + seed_doc[:8000], mod_dt

        return None, None
    except Exception:
        return None, None


def get_seed_data(ticker: str) -> tuple[str | None, datetime | None]:
    """
    Find the most recent seed file for this ticker.
    Checks mirofish_seeds/ first, then Documents/EDGAR/ as fallback.
    Returns (text_excerpt, mod_datetime) or (None, None).
    """
    ticker = ticker.upper().strip()
    candidates = []
    try:
        for search_dir in [MIROFISH_SEEDS, EDGAR_SEEDS]:
            if search_dir.exists():
                for f in search_dir.glob(f"{ticker}*.md"):
                    candidates.append(f)
        if not candidates:
            return None, None
        candidates.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        best = candidates[0]
        mod_dt = datetime.fromtimestamp(best.stat().st_mtime)
        text = best.read_text(encoding="utf-8", errors="replace")
        header = (
            f"[Source: {best.name} | Last modified: {mod_dt.strftime('%Y-%m-%d')}]\n\n"
        )
        return header + text[:4000], mod_dt
    except Exception:
        return None, None


def get_news_context(ticker: str) -> str | None:
    """
    Read the Stage 0C news context file for this ticker.
    Returns None if file not found.
    Returns a staleness warning if file is older than NEWS_MAX_AGE_DAYS.
    """
    ticker = ticker.upper().strip()
    path = ORACLE_FILINGS / ticker / f"{ticker}_news_context.txt"
    try:
        if not path.exists():
            return None
        mod_dt = datetime.fromtimestamp(path.stat().st_mtime)
        age = datetime.now() - mod_dt
        if age > timedelta(days=NEWS_MAX_AGE_DAYS):
            return (
                f"[News context stale — last updated {mod_dt.strftime('%Y-%m-%d')} "
                f"({age.days} days ago). Re-run oracle_fetch.py for {ticker} to refresh.]"
            )
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


def build_research_context(ticker: str) -> tuple[str, dict]:
    """
    Assemble all available ORACLE research for a ticker into one text block.

    Returns:
        (context_text: str, metadata: dict)

    metadata keys:
        has_obsidian: bool
        has_report:   bool
        has_seed:     bool
        has_news:     bool
        report_date:  datetime | None
        seed_date:    datetime | None
    """
    ticker = ticker.upper().strip()

    obsidian = get_obsidian_note(ticker)
    report, report_date = get_oracle_report(ticker)
    seed, seed_date = get_seed_data(ticker)
    news = get_news_context(ticker)

    meta = {
        "has_obsidian": obsidian is not None,
        "has_report":   report is not None,
        "has_seed":     seed is not None,
        "has_news":     news is not None,
        "report_date":  report_date,
        "seed_date":    seed_date,
    }

    if not any([obsidian, report, seed, news]):
        return (
            f"No ORACLE research available for {ticker}. "
            f"Run oracle_fetch.py to generate research before asking about this ticker."
        ), meta

    sections = [f"=== ORACLE RESEARCH CONTEXT: {ticker} ===\n"]

    if obsidian:
        sections.append("## OBSIDIAN TICKER NOTE\n")
        sections.append(obsidian)
        sections.append("\n")

    if seed:
        sections.append("## SEED DATA (EDGAR FUNDAMENTALS)\n")
        sections.append(seed)
        sections.append("\n")

    if report:
        sections.append("## ORACLE SIMULATION REPORT\n")
        sections.append(report)
        sections.append("\n")

    if news:
        sections.append("## BREAKING NEWS (Stage 0C)\n")
        sections.append(news)
        sections.append("\n")

    return "".join(sections), meta


if __name__ == "__main__":
    context, meta = build_research_context("INTU")
    print(f"Has report:   {meta['has_report']}")
    print(f"Has obsidian: {meta['has_obsidian']}")
    print(f"Has seed:     {meta['has_seed']}")
    print(f"Has news:     {meta['has_news']}")
    if meta["report_date"]:
        print(f"Report date:  {meta['report_date'].strftime('%Y-%m-%d')}")
    print("\n--- Context preview (first 500 chars) ---")
    print(context[:500])
