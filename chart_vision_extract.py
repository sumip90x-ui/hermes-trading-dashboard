"""
chart_vision_extract.py
=======================
Reads a TradingView monthly chart screenshot via vision AI and extracts
structured technical data. Stores results in chart_vision_data table in
signal_tracker.db.

This runs automatically after every chart capture. Data is used for:
  - Pick card display (HARSI value, UMES score, cycle position)
  - Pattern matching against winner reference library
  - Longitudinal tracking (how HARSI/UMES changes over hold period)
  - Analysis context for Hermes without re-reading the chart each time

CRITICAL RULE: this module extracts data FROM real screenshots.
It never uses LLM training data to invent chart readings.
If extraction fails or is ambiguous, it writes NULL — not a guess.

Usage:
    python3 chart_vision_extract.py ADBE /path/to/ADBE_monthly_10yr.png [baseline|live|winner_ref]
"""

import sys
import json
import sqlite3
import datetime
from pathlib import Path

DB_PATH = Path.home() / 'Documents' / 'Trading Vault' / 'signal_tracker.db'


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS chart_vision_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    chart_type      TEXT NOT NULL DEFAULT 'live',  -- live | baseline | winner_ref
    chart_path      TEXT,
    captured_at     TEXT,
    extracted_at    TEXT DEFAULT CURRENT_TIMESTAMP,

    -- HARSI oscillator (Heikin Ashi RSI)
    harsi_value     REAL,      -- numeric reading (e.g. -28.75)
    harsi_color     TEXT,      -- red | pink | yellow | green | cyan
    harsi_direction TEXT,      -- falling | flat | turning_up | rising
    harsi_zone      TEXT,      -- oversold | neutral | overbought

    -- UMES score panel
    umes_score      TEXT,      -- e.g. "2/6"
    umes_score_num  INTEGER,   -- numeric (2)
    umes_daily_rsi  TEXT,      -- BULL | BEAR | NEUTRAL
    umes_setup      TEXT,      -- B LONG | B WATCH | WEAK | etc
    umes_trend_lock TEXT,      -- BULL LOCKED | TRANSITIONING | BEAR LOCKED
    umes_exhaustion TEXT,      -- CLEAR | OVERSOLD | EXTND:Run | EXTND:ATH etc

    -- Price vs moving averages
    ma_position     TEXT,      -- above_both | below_both | crossing | above_short_below_long
    price_vs_ma     TEXT,      -- raw description

    -- Volume (VRVP)
    vrvp_up_vol     TEXT,      -- e.g. "452.59M"
    vrvp_down_vol   TEXT,
    vrvp_total_vol  TEXT,
    volume_trend    TEXT,      -- drying_up | expanding | balanced

    -- Cycle annotation (from drawn labels on chart)
    cycle_label_visible TEXT,  -- what cycle labels are visible (e.g. "A,B,C,D")
    current_cycle_marker TEXT, -- the green (NOW) marker label visible

    -- Chart geometry
    chart_geometry  TEXT,      -- cup | flat_base | v_shape | declining | coiling
    base_duration_est TEXT,    -- estimated months of base from chart

    -- Raw vision summary (full text for reference)
    raw_summary     TEXT,

    -- Confidence
    extraction_confidence TEXT  -- high | medium | low
)
"""

VISION_PROMPT = """You are reading a TradingView monthly stock chart screenshot to extract SPECIFIC NUMERIC AND CATEGORICAL DATA.
Do NOT describe the chart generally. Extract EXACTLY the following fields.
If you cannot read a value clearly, write null for that field.
Never guess — only report what you can actually see in the image.

Return ONLY a JSON object with these exact keys:

{
  "harsi_value": <number or null — the numeric reading shown on the HARSI oscillator, e.g. -28.75>,
  "harsi_color": <"red" | "pink" | "yellow" | "orange" | "green" | "cyan" | null>,
  "harsi_direction": <"falling" | "flat" | "turning_up" | "rising" | null>,
  "harsi_zone": <"oversold" | "neutral" | "overbought" | null>,
  "umes_score": <string like "2/6" or "4/6" or null>,
  "umes_score_num": <integer 0-6 or null>,
  "umes_daily_rsi": <"BULL" | "BEAR" | "NEUTRAL" | null>,
  "umes_setup": <exact text shown e.g. "B LONG" or "B WATCH" or "WEAK" or null>,
  "umes_trend_lock": <exact text e.g. "BULL LOCKED" or "TRANSITIONING" or "BEAR LOCKED" or null>,
  "umes_exhaustion": <exact text e.g. "CLEAR" or "OVERSOLD" or "EXTND:Run" or "EXTND:ATH" or null>,
  "ma_position": <"above_both" | "below_both" | "crossing" | "above_short_below_long" | null>,
  "vrvp_up_vol": <string e.g. "452.59M" or null>,
  "vrvp_down_vol": <string e.g. "390M" or null>,
  "vrvp_total_vol": <string e.g. "842.59M" or null>,
  "volume_trend": <"drying_up" | "expanding" | "balanced" | null>,
  "cycle_labels_visible": <string of visible cycle labels e.g. "A,B,C,D" or null>,
  "current_cycle_marker": <the label text on the green (NOW) vertical line e.g. "D (ENTRY)" or null>,
  "chart_geometry": <"cup" | "flat_base" | "v_shape" | "declining" | "coiling" | "parabolic" | null>,
  "base_duration_est": <string e.g. "18 months" or null>,
  "extraction_confidence": <"high" | "medium" | "low">
}

Look specifically for:
- Bottom panel: HARSI histogram bars — color and numeric values shown at top
- Top-right panel: UMES SCORE X/6, and the labeled rows below it
- Main chart: price position relative to the orange and blue moving average lines
- Right side: VRVP volume bars with Up/Down/Total numbers
- Cycle labels: colored vertical lines with text like "▼ A", "▼ B (NOW)", "▼ D (ENTRY)"
"""


def extract_from_screenshot(image_path: str) -> dict:
    """
    Run vision AI on the screenshot and return structured data dict.
    Returns empty dict with nulls on failure.
    """
    try:
        import subprocess, os, sys

        # Resolve Anthropic key — load from dashboard env file same way app.py does
        def _load_env(path):
            try:
                for line in Path(path).read_text().splitlines():
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            except Exception:
                pass

        _load_env(Path.home() / '.env')
        _load_env(Path('/etc/hermes/.env'))
        _load_env(Path.home() / '.hermes' / '.env')
        _load_env(Path.home() / 'trading_dashboard' / '.env')

        key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not key:
            return {'error': 'no_anthropic_key'}

        # Read image
        img_path = Path(image_path)
        if not img_path.exists():
            return {'error': f'File not found: {image_path}'}

        import base64, urllib.request
        img_b64 = base64.b64encode(img_path.read_bytes()).decode()

        payload = {
            'model': 'claude-opus-4-5',
            'max_tokens': 1024,
            'messages': [{
                'role': 'user',
                'content': [
                    {'type': 'text', 'text': VISION_PROMPT},
                    {'type': 'image', 'source': {
                        'type': 'base64',
                        'media_type': 'image/png',
                        'data': img_b64
                    }}
                ]
            }]
        }

        req = urllib.request.Request(
            'https://api.anthropic.com/v1/messages',
            data=json.dumps(payload).encode(),
            headers={
                'x-api-key': key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json'
            },
            method='POST'
        )

        with urllib.request.urlopen(req, timeout=45) as r:
            resp = json.loads(r.read())

        text = resp['content'][0]['text'].strip()
        # Strip markdown code fences if present
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        return json.loads(text.strip())

    except Exception as e:
        return {'error': str(e)}


def store_vision_data(symbol: str, chart_path: str, chart_type: str, data: dict):
    """Store extracted vision data in chart_vision_data table."""
    conn = sqlite3.connect(str(DB_PATH))
    
    # Ensure table exists
    conn.execute(SCHEMA)
    conn.commit()
    
    captured_at = None
    p = Path(chart_path)
    if p.exists():
        import datetime as _dt
        captured_at = _dt.datetime.fromtimestamp(p.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
    
    conn.execute("""
        INSERT INTO chart_vision_data
        (symbol, chart_type, chart_path, captured_at,
         harsi_value, harsi_color, harsi_direction, harsi_zone,
         umes_score, umes_score_num, umes_daily_rsi, umes_setup,
         umes_trend_lock, umes_exhaustion,
         ma_position, price_vs_ma,
         vrvp_up_vol, vrvp_down_vol, vrvp_total_vol, volume_trend,
         cycle_label_visible, current_cycle_marker,
         chart_geometry, base_duration_est,
         raw_summary, extraction_confidence)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        symbol.upper(), chart_type, chart_path, captured_at,
        data.get('harsi_value'), data.get('harsi_color'),
        data.get('harsi_direction'), data.get('harsi_zone'),
        data.get('umes_score'), data.get('umes_score_num'),
        data.get('umes_daily_rsi'), data.get('umes_setup'),
        data.get('umes_trend_lock'), data.get('umes_exhaustion'),
        data.get('ma_position'), data.get('price_vs_ma'),
        data.get('vrvp_up_vol'), data.get('vrvp_down_vol'),
        data.get('vrvp_total_vol'), data.get('volume_trend'),
        data.get('cycle_labels_visible'), data.get('current_cycle_marker'),
        data.get('chart_geometry'), data.get('base_duration_est'),
        json.dumps(data), data.get('extraction_confidence', 'low')
    ))
    conn.commit()
    conn.close()
    print(f"Stored vision data for {symbol} ({chart_type})")


def process_chart(symbol: str, chart_path: str, chart_type: str = 'live') -> dict:
    """Full pipeline: extract + store. Returns the extracted data."""
    print(f"\nExtracting vision data: {symbol} ({chart_type})")
    print(f"  Path: {chart_path}")
    
    data = extract_from_screenshot(chart_path)
    
    if 'error' in data:
        print(f"  ERROR: {data['error']}")
        return data
    
    store_vision_data(symbol, chart_path, chart_type, data)
    
    print(f"  HARSI: {data.get('harsi_value')} ({data.get('harsi_color')}, {data.get('harsi_direction')})")
    print(f"  UMES:  {data.get('umes_score')} | {data.get('umes_setup')} | {data.get('umes_trend_lock')}")
    print(f"  MA:    {data.get('ma_position')}")
    print(f"  Cycle: {data.get('cycle_labels_visible')} → {data.get('current_cycle_marker')}")
    print(f"  Conf:  {data.get('extraction_confidence')}")
    
    return data


def init_schema():
    """Create the chart_vision_data table if not exists."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(SCHEMA)
    conn.commit()
    conn.close()
    print("Schema OK")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: chart_vision_extract.py SYMBOL /path/to/chart.png [live|baseline|winner_ref]")
        print("       chart_vision_extract.py --init-schema")
        print("       chart_vision_extract.py --batch-all")
        sys.exit(1)

    if sys.argv[1] == '--init-schema':
        init_schema()
        sys.exit(0)

    if sys.argv[1] == '--batch-all':
        # Process all captures in live and baseline folders
        live_dir     = Path.home() / 'Documents' / 'Trading Vault' / 'charts' / 'live'
        baseline_dir = Path.home() / 'Documents' / 'Trading Vault' / 'charts' / 'baseline'
        winner_dir   = Path.home() / 'Documents' / 'Trading Vault' / 'winner_charts'
        init_schema()
        for d, ctype in [(live_dir,'live'),(baseline_dir,'baseline'),(winner_dir,'winner_ref')]:
            for f in sorted(d.glob('*.png')):
                sym = f.name.split('_monthly_')[0].split('_baseline_')[0].split('_winner_')[0]
                result = process_chart(sym, str(f), ctype)
                if 'error' in result:
                    print(f"  SKIP {sym}: {result['error']}")
        sys.exit(0)

    if len(sys.argv) < 3:
        print("Usage: chart_vision_extract.py SYMBOL /path/to/chart.png [live|baseline|winner_ref]")
        sys.exit(1)
    
    symbol     = sys.argv[1].upper()
    chart_path = sys.argv[2]
    chart_type = sys.argv[3] if len(sys.argv) > 3 else 'live'
    
    init_schema()
    result = process_chart(symbol, chart_path, chart_type)
    print(json.dumps(result, indent=2))
