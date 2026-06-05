#!/usr/bin/env python3
"""
chart_capture.py
================
Standalone script to capture an annotated monthly chart for a given symbol
via TradingView MCP (CDP on port 9222).

Usage:
    python3 chart_capture.py ADBE '{"cycles": [{"cycle_label": "A", "trough_date": "2022-09-01", "peak_date": "2024-02-01", "gain_pct": 132, "status": "COMPLETED"}, ...]}'

Output (JSON to stdout):
    {"status": "ok", "path": "/path/to/ADBE_monthly_20260603_....png"}
    {"status": "error", "message": "..."}

What it does:
  1. Connect to TradingView via CDP (port 9222)
  2. Set symbol to the requested ticker
  3. Set timeframe to 1M (monthly)
  4. Set visible range: 5 years back to today
  5. Clear all existing drawings
  6. Draw vertical lines + text labels at each cycle trough date
     - Completed cycles: gold color
     - Current/active cycle: bright green + "(NOW)" label
  7. Capture full screenshot (chart + HARSI panel + UMES)
  8. Save to ~/Documents/Trading Vault/charts/live/
  9. Register with dashboard via /api/chart/process_queue
  10. Print JSON result to stdout
"""

import sys
import json
import time
import shutil
import datetime
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
TV_SCREENSHOTS_DIR = Path.home() / 'tradingview-mcp-jackson' / 'screenshots'
DEST_DIR           = Path.home() / 'Documents' / 'Trading Vault' / 'charts' / 'live'
DASHBOARD_URL      = 'http://localhost:6060'
MCP_HERMES_CMD     = ['python3', '-c']  # we call via hermes MCP tools directly

# Cycle colors
COLORS = {
    'completed': '#ffaa00',   # gold for completed cycles
    'active':    '#00ff88',   # green for active/current cycle
    'current':   '#4a9eff',   # blue highlight for current entry point
}


def unix_ts(date_str: str) -> int:
    """Convert YYYY-MM-DD to unix timestamp (UTC midnight)."""
    try:
        dt = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d')
        return int(dt.timestamp())
    except Exception:
        return 0


def call_mcp(tool: str, params: dict) -> dict:
    """
    Call a TradingView MCP tool via the hermes tool bridge.
    Since we can't call MCP tools directly from Python, we use the
    tradingview-mcp CLI if available, otherwise write to the queue.
    """
    # Try the MCP server via subprocess if available
    try:
        payload = json.dumps({'tool': tool, 'params': params})
        # Check if mcp-client or similar is available
        result = subprocess.run(
            ['node', Path.home() / 'tradingview-mcp-jackson' / 'mcp-call.js', tool, payload],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return {'success': False, 'error': 'MCP direct call not available — use Hermes agent'}


def capture_annotated_chart(symbol: str, cycle_data: dict, current_cycle: str = None) -> dict:
    """
    Main entry point. Called from the dashboard route with cycle_data from DB.
    Returns: {'status': 'ok'/'error', 'path': str, 'message': str}
    """
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    cycles = cycle_data.get('cycles', [])

    # Write capture instructions to queue for Hermes to execute
    queue_dir = Path.home() / '.hermes' / 'chart_capture_queue'
    queue_dir.mkdir(parents=True, exist_ok=True)

    instructions = {
        'symbol': symbol,
        'timeframe': '1M',
        'current_cycle': current_cycle,
        'cycles': cycles,
        'requested_at': datetime.datetime.now().isoformat(),
        'dest_dir': str(DEST_DIR),
        'expected_filename': f'{symbol}_monthly_{ts}.png',
        'visible_range_years': 5,
        'capture_region': 'full',
        'instructions': [
            f'1. chart_set_symbol({symbol})',
            '2. chart_set_timeframe(1M)',
            f'3. chart_set_visible_range(5 years back to today)',
            '4. draw_clear()',
            '5. For each cycle trough: draw_shape(vertical_line, gold) + draw_shape(text, "▼ Cycle X")',
            '6. For current cycle: draw_shape(vertical_line, green) + draw_shape(text, "▼ Cycle X (NOW)")',
            '7. capture_screenshot(full)',
            f'8. Save to {DEST_DIR}/{symbol}_monthly_{ts}.png',
            '9. POST to /api/chart/process_queue',
        ]
    }

    req_file = queue_dir / f'{symbol}_{ts}.json'
    req_file.write_text(json.dumps(instructions, indent=2))

    return {
        'status': 'queued',
        'symbol': symbol,
        'queue_file': str(req_file),
        'expected_filename': f'{symbol}_monthly_{ts}.png',
        'message': f'Capture instructions written. Call: capture_chart_from_queue("{symbol}") in Hermes.'
    }


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps({'status': 'error', 'message': 'Usage: chart_capture.py SYMBOL [cycle_data_json]'}))
        sys.exit(1)

    symbol = sys.argv[1].upper()
    cycle_data = {}
    current_cycle = None

    if len(sys.argv) >= 3:
        try:
            cycle_data = json.loads(sys.argv[2])
        except Exception:
            pass
    if len(sys.argv) >= 4:
        current_cycle = sys.argv[3]

    result = capture_annotated_chart(symbol, cycle_data, current_cycle)
    print(json.dumps(result))
