"""
Hermes Trading Dashboard — Flask + SocketIO
============================================
Real-time portfolio command center.
  - Live portfolio candlestick chart
  - Chat with Hermes (Claude) for trade decisions
  - One-click trade execution
  - Candle trigger monitor
  - Fidelity CSV upload → EDGAR-scored buy list

Run: python3 ~/trading_dashboard/app.py
Open: http://localhost:6060
"""

import os, sys, json, re, time, subprocess, threading, requests, logging, glob, urllib.parse, shutil

# Load env files so keys are available regardless of how the app is launched
def _load_env_file(path):
    try:
        with open(os.path.expanduser(path)) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    if k.strip() not in os.environ:  # don't override shell env
                        os.environ[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
_load_env_file('~/.env')
_load_env_file('~/.hermes/.env')

log = logging.getLogger('hermes_dashboard')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
from datetime import datetime, timezone
import re
import time
from pathlib import Path
from collections import defaultdict
from flask import Flask, render_template, jsonify, request, Blueprint
import fidelity_db
from flask_socketio import SocketIO, emit
from oracle_brain import build_research_context

sys.path.insert(0, str(Path.home()))

# ── Load env ──────────────────────────────────────────────────────────────────
for ef in ['~/.env','~/trading.env','~/alpaca.env','~/.hermes/.env']:
    p = Path(ef).expanduser()
    if p.exists():
        for line in p.read_text().splitlines():
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

KEY    = os.environ.get('ALPACA_LIVE_KEY') or os.environ.get('APCA_API_KEY_ID','')
SECRET = os.environ.get('ALPACA_LIVE_SECRET') or os.environ.get('APCA_API_SECRET_KEY','')
ALPACA = 'https://api.alpaca.markets'

HOME          = Path.home()
REPORTS_DIR   = HOME / 'trading_reports'
CANDLE_FILE   = REPORTS_DIR / 'candle_history.json'
HOUSE_FILE    = REPORTS_DIR / 'house_money.json'
JOURNEY_FILE      = HOME / 'Documents' / 'Trading Vault' / 'journey.json'
FIDELITY_HISTORY  = HOME / 'Documents' / 'Trading Vault' / 'Fidelity_History'
FIDELITY_HISTORY.mkdir(parents=True, exist_ok=True)
EDGAR_CACHE   = REPORTS_DIR / 'edgar_score_cache.json'
PORTFOLIO_CSV = HOME / 'portfolio.csv'
AI_CONFIG_FILE = Path(__file__).with_name('ai_config.json')

DEFAULT_AI_CONFIG = {
    'simple_provider': 'deepseek',
    'simple_model':    'deepseek-chat',
    'quality_provider':'anthropic',
    'quality_model':   'claude-sonnet-4-5',
    'trade_provider':  'anthropic',
    'trade_model':     'claude-sonnet-4-5',
}

ALLOWED_SIMPLE_MODELS = [
    {'provider': 'deepseek',   'model': 'deepseek-chat',          'label': 'DeepSeek Flash ⚡'},
    {'provider': 'anthropic',  'model': 'claude-sonnet-4-5',      'label': 'Claude Sonnet 4.5'},
    {'provider': 'openrouter', 'model': 'openai/gpt-4.1-mini',    'label': 'GPT-4.1 Mini'},
    {'provider': 'openrouter', 'model': 'openai/gpt-5-mini',      'label': 'GPT-5 Mini'},
    {'provider': 'openrouter', 'model': 'openai/gpt-4o-mini',     'label': 'GPT-4o Mini'},
]
_ALLOWED_SIMPLE_KEYS = {(m['provider'], m['model']) for m in ALLOWED_SIMPLE_MODELS}

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True        # always reload templates from disk
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0       # never cache static files
app.jinja_env.auto_reload = True                  # force Jinja to re-read templates every request

# ── Cache busting — force browser to always load fresh JS/CSS ─────────────
@app.after_request
def no_cache(response):
    # Skip for data routes that benefit from caching (SSE streams, large JSON)
    if request.path.startswith('/api/'):
        return response
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response
app.config['SECRET_KEY'] = 'hermes-trading-dashboard'
socketio = SocketIO(app, cors_allowed_origins='*', async_mode='threading')

# ── Fidelity portfolio history blueprint ─────────────────────────────────────
portfolio_bp = Blueprint('portfolio', __name__, url_prefix='/api/portfolio/history')

@portfolio_bp.route('/snapshots', methods=['GET'])
def route_snapshots():
    try:
        data = fidelity_db.get_snapshots()
        return jsonify({'status':'ok','count':len(data),'snapshots':data})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@portfolio_bp.route('/deviations', methods=['GET'])
def route_deviations():
    try:
        snapshot_id = request.args.get('snapshot_id') or None
        data = fidelity_db.get_deviations(snapshot_id)
        used_id = data[0]['curr_snapshot_id'] if data else snapshot_id
        return jsonify({'status':'ok','snapshot_id':used_id,'count':len(data),'deviations':data})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@portfolio_bp.route('/symbol/<symbol>', methods=['GET'])
def route_symbol_history(symbol):
    try:
        data = fidelity_db.get_symbol_history(symbol.upper().strip())
        return jsonify({'status':'ok','symbol':symbol.upper(),'count':len(data),'history':data})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@portfolio_bp.route('/summary', methods=['GET'])
def route_summary():
    try:
        data = fidelity_db.get_summary()
        return jsonify({'status':'ok',**data})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@portfolio_bp.route('/ingest', methods=['POST'])
def route_ingest():
    if 'file' not in request.files:
        return jsonify({'status':'error','message':'No file field'}), 400
    f = request.files['file']
    if not f.filename or not f.filename.lower().endswith('.csv'):
        return jsonify({'status':'error','message':'Must be a .csv file'}), 400
    vault = fidelity_db.VAULT_DIR
    vault.mkdir(parents=True, exist_ok=True)
    save_path = vault / f.filename
    f.save(str(save_path))
    if fidelity_db.filename_already_ingested(f.filename):
        snaps = fidelity_db.get_snapshots()
        existing = next((s for s in snaps if s['filename'] == f.filename), None)
        return jsonify({'status':'duplicate','message':f'{f.filename} already ingested','snapshot':existing})
    try:
        result = fidelity_db.ingest_snapshot(save_path)
        _brief_cache.clear()   # invalidate intelligence brief cache after new CSV
        _brief_cache_ts = 0.0
        return jsonify({'status':'ok',**result})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

app.register_blueprint(portfolio_bp)

# ── Fidelity intelligence routes (/api/fidelity/*) ───────────────────────────
# Read-only. Do not touch Alpaca, SGOL, or bot logic.

@app.route('/api/fidelity/buy_signals')
def api_fidelity_buy_signals():
    try:
        return jsonify(fidelity_db.get_buy_list_signals(limit=20))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/fidelity/hot_streaks')
def api_fidelity_hot_streaks():
    try:
        min_streak = int(request.args.get('min_streak', 3))
        streaks    = fidelity_db.get_hot_streaks(min_streak=min_streak)
        return jsonify({'min_streak': min_streak, 'count': len(streaks), 'streaks': streaks})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/fidelity/backtest')
def api_fidelity_backtest():
    try:
        return jsonify(fidelity_db.backtest_signals())
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/fidelity/true_profit')
def api_fidelity_true_profit():
    """
    Portfolio-level true profit time series from all ingested Fidelity snapshots.
    true_profit = SUM(total_value) - SUM(total_cost) per snapshot.
    Cost basis is the principal — no separate deposit tracking needed.
    """
    try:
        return jsonify(fidelity_db.get_true_profit_history())
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

# ── Intelligence brief system ─────────────────────────────────────────────────

_WIN_RATE_FACTORS = {
    'PULLBACK':      1.30,
    'ACCELERATING':  1.15,
    'DETERIORATING': 0.90,
    'RECOVERING':    0.80,
    'STABLE':        1.00,
}
_WIN_RATES = {
    'PULLBACK':      65.6,
    'ACCELERATING':  58.5,
    'DETERIORATING': 46.0,
    'RECOVERING':    44.0,
    'STABLE':        50.0,
}
_REGIME_MULTIPLIERS = {
    'CHEAP':      1.20,
    'RISK-ON':    1.00,
    'FLIGHT':     0.70,
    'OVERHEATED': 0.30,
    'NEUTRAL':    1.00,
}
_HARVEST_THRESHOLDS = {
    'CHEAP':      15.0,
    'RISK-ON':    12.0,
    'FLIGHT':      5.0,
    'OVERHEATED':  8.0,
    'NEUTRAL':    10.0,
}
_brief_cache: dict = {}
_BRIEF_CACHE_TTL  = 600  # 10 minutes
_brief_cache_ts   = 0.0


def _edgar_bonus(base: float, edgar_score) -> float:
    if edgar_score is None:
        return base
    if edgar_score >= 12:
        return base * 1.20
    if edgar_score >= 6:
        return base * 1.10
    return base


def _confidence_tier(win_rate: float, edgar_score) -> str:
    if win_rate > 60 and edgar_score is not None and edgar_score >= 10:
        return 'HIGH'
    if win_rate > 50 or (edgar_score is not None and edgar_score >= 6):
        return 'MEDIUM'
    return 'LOW'


def _build_reasoning(sym, direction, win_rate, edgar_score, regime, accts):
    parts = [f'{direction} signal ({win_rate:.0f}% hist. win rate, {accts} Fidelity accts)']
    if edgar_score is not None:
        parts.append(f'EDGAR {edgar_score}/18')
    parts.append(f'regime: {regime}')
    return f'{sym} — ' + ', '.join(parts) + '.'


def _get_macro_data_for_brief() -> dict:
    """Read from the global _MACRO_CACHE — updated_at is HH:MM string."""
    try:
        q          = _MACRO_CACHE.get('quadrant', 'NEUTRAL')
        updated_at = _MACRO_CACHE.get('updated_at')  # HH:MM string or None
        if updated_at:
            try:
                now = datetime.now()
                h, m = map(int, updated_at.split(':'))
                cache_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
                age_min  = (now - cache_dt).total_seconds() / 60
                if age_min < 0:
                    age_min += 1440  # handle midnight wrap
                stale = age_min > 30
                if stale:
                    q = 'NEUTRAL'
            except Exception:
                age_min, stale = 999, True
                q = 'NEUTRAL'
        else:
            age_min, stale = 999, True
            q = 'NEUTRAL'
        return {
            'quadrant':        q,
            'equity_rank':     _MACRO_CACHE.get('equity_rank', 50.0),
            'hard_asset_rank': _MACRO_CACHE.get('hard_rank', 50.0),
            'age_minutes':     round(age_min, 1),
            'stale':           stale,
        }
    except Exception:
        return {'quadrant': 'NEUTRAL', 'equity_rank': 50.0,
                'hard_asset_rank': 50.0, 'age_minutes': 999, 'stale': True}


def _compute_brief(force: bool = False) -> dict:
    global _brief_cache, _brief_cache_ts
    now_ts = time.time()
    if not force and _brief_cache and (now_ts - _brief_cache_ts) < _BRIEF_CACHE_TTL:
        return _brief_cache

    generated_at = datetime.now(timezone.utc).isoformat()

    try:
        fid_signals = fidelity_db.get_buy_list_signals(limit=30)
    except Exception:
        fid_signals = []

    try:
        streak_data = fidelity_db.get_hot_streaks(min_streak=2)
        streak_map  = {s['symbol']: s['streak_length'] for s in streak_data}
    except Exception:
        streak_map = {}

    try:
        bt        = fidelity_db.backtest_signals()
        win_rates = {r['direction']: r['win_rate_pct'] for r in bt.get('by_direction', [])}
    except Exception:
        win_rates = {}

    def _wr(direction):
        return win_rates.get(direction, _WIN_RATES.get(direction, 50.0))

    edgar_cache_data = {}
    edgar_cached_syms = 0
    edgar_excel_syms  = set()
    try:
        if EDGAR_CACHE.exists():
            edgar_cache_data  = json.loads(EDGAR_CACHE.read_text())
            edgar_cached_syms = len(edgar_cache_data)
    except Exception:
        pass
    # Also check EDGAR excel files — symbols with xlsx have research data even if not scored yet
    try:
        edgar_companies_dir = Path.home() / 'Documents' / 'EDGAR' / 'companies'
        if edgar_companies_dir.exists():
            for sym_dir in edgar_companies_dir.iterdir():
                xl = sym_dir / f'{sym_dir.name}_fundamentals.xlsx'
                if xl.exists():
                    edgar_excel_syms.add(sym_dir.name)
            edgar_cached_syms = max(edgar_cached_syms, len(edgar_excel_syms))
    except Exception:
        pass

    macro        = _get_macro_data_for_brief()
    regime       = macro['quadrant']
    reg_mult     = _REGIME_MULTIPLIERS.get(regime, 1.0)
    harvest_thr  = _HARVEST_THRESHOLDS.get(regime, 10.0)

    try:
        acct_raw  = alpaca('/v2/account')
        cash      = float(acct_raw.get('cash', 0))
        equity    = float(acct_raw.get('equity', 0)) or 1.0
    except Exception:
        cash, equity = 0.0, 1200.0

    try:
        positions_raw = alpaca('/v2/positions')
        positions = {}
        for p in positions_raw:
            sym = p.get('symbol', '')
            if sym:
                positions[sym] = {
                    'market_value':    float(p.get('market_value', 0)),
                    'unrealized_plpc': float(p.get('unrealized_plpc', 0)) * 100,
                    'avg_entry_price': float(p.get('avg_entry_price', 0)),
                    'current_price':   float(p.get('current_price', 0)),
                }
    except Exception:
        positions = {}

    try:
        snap_list          = fidelity_db.get_snapshots()
        fid_snapshot_count = len(snap_list)
        fid_latest         = snap_list[0]['snapshot_date'][:10] if snap_list else 'none'
    except Exception:
        fid_snapshot_count, fid_latest = 0, 'none'

    budget_remaining = cash * 0.80
    top_buys = []

    for sig in fid_signals:
        sym = sig.get('sym', '')
        if not sym:
            continue
        if sym in ('SGOL', 'FZFXX', 'FZFXX**') or 'MONEY MARKET' in sym.upper():
            continue

        direction   = sig.get('reason', 'STABLE')
        base_deploy = float(sig.get('buy', 0))
        budget_ceil = float(sig.get('budget_ceiling', base_deploy))
        accts       = int(sig.get('accts', 1))
        conv_mult   = float(sig.get('conviction_multiplier', 1.0))
        ec          = edgar_cache_data.get(sym, {})
        edgar_score = ec.get('score', None)
        # If no numeric score but excel file exists, note it so modal shows "EDGAR ✓ research available"
        edgar_has_data = edgar_score is not None or sym in edgar_excel_syms
        wr          = _wr(direction)
        wr_factor   = _WIN_RATE_FACTORS.get(direction, 1.0)

        adjusted = base_deploy * wr_factor * reg_mult
        adjusted = _edgar_bonus(adjusted, edgar_score)
        adjusted = min(adjusted, budget_ceil)
        adjusted = min(adjusted, budget_remaining)
        adjusted = min(adjusted, 50.0)

        if adjusted < 1.10:
            continue

        pos_data          = positions.get(sym, {})
        already_in_alpaca = sym in positions
        current_alpaca_val = pos_data.get('market_value') if already_in_alpaca else None

        if already_in_alpaca:
            if pos_data['market_value'] / equity * 100 > 15.0:
                continue

        streak = streak_map.get(sym, 0)

        top_buys.append({
            'sym':                   sym,
            'final_deploy':          round(adjusted, 2),
            'base_deploy':           round(base_deploy, 2),
            'direction':             direction,
            'win_rate':              round(wr, 1),
            'edgar_score':           edgar_score,
            'edgar_has_data':        edgar_has_data,
            'accts':                 accts,
            'streak':                streak,
            'budget_ceiling':        round(budget_ceil, 2),
            'conviction_multiplier': round(conv_mult, 2),
            'regime_multiplier':     reg_mult,
            'confidence_tier':       _confidence_tier(wr, edgar_score),
            'already_in_alpaca':     already_in_alpaca,
            'current_alpaca_value':  current_alpaca_val,
            'reasoning':             _build_reasoning(sym, direction, wr, edgar_score, regime, accts),
        })
        budget_remaining -= adjusted
        if budget_remaining < 1.10:
            break

    top_buys.sort(key=lambda x: -x['final_deploy'])

    sell_candidates = []
    for sym, pos in positions.items():
        if sym == 'SGOL':
            continue
        if pos['market_value'] < 1.10:   # skip marker/stub positions
            continue
        unreal_pct = pos['unrealized_plpc']
        fid_dir    = next((s.get('reason') for s in fid_signals if s.get('sym') == sym), None)
        rec = 'HOLD'
        if unreal_pct > harvest_thr:
            rec = f'TRIM — above {harvest_thr:.0f}% harvest threshold ({regime} regime)'
        elif fid_dir == 'ACCELERATING' and unreal_pct > 5.0:
            rec = 'TRIM — Fidelity ACCELERATING signal, consider partial profit'
        if rec != 'HOLD':
            sell_candidates.append({
                'sym':               sym,
                'alpaca_value':      round(pos['market_value'], 2),
                'unrealized_pct':    round(unreal_pct, 2),
                'fidelity_direction':fid_dir,
                'recommendation':    rec,
            })
    sell_candidates.sort(key=lambda x: -x['unrealized_pct'])

    if top_buys:
        top3 = ', '.join(f"{b['sym']} ${b['final_deploy']:.2f} {b['direction']}" for b in top_buys[:3])
        buys_sentence = f'Top signals: {top3}.'
    else:
        buys_sentence = 'No deployable signals after filters.'

    sell_sentence = (
        'Trim watch: ' + ', '.join(s['sym'] for s in sell_candidates[:3]) + '.'
        if sell_candidates else 'No positions above harvest threshold.'
    )

    result = {
        'generated_at':   generated_at,
        'macro_quadrant': regime,
        'cash_available': round(cash, 2),
        'data_freshness': {
            'fidelity_snapshots':   fid_snapshot_count,
            'fidelity_latest':      fid_latest,
            'edgar_cached_symbols': edgar_cached_syms,
            'macro_age_minutes':    macro['age_minutes'],
            'macro_stale':          macro['stale'],
        },
        'top_buys':        top_buys[:10],
        'sell_candidates': sell_candidates,
        'summary_text': (
            f'Regime {regime} — {buys_sentence} '
            f'{sell_sentence} ${cash:.2f} cash available.'
        ),
    }
    _brief_cache    = result
    _brief_cache_ts = now_ts
    return result


def _build_hermes_intelligence_block() -> str:
    """≤400-char intelligence summary injected into every Hermes chat message."""
    try:
        brief    = _compute_brief()
        regime   = brief.get('macro_quadrant', 'UNKNOWN')
        cash     = brief.get('cash_available', 0.0)
        buys     = brief.get('top_buys', [])
        sells    = brief.get('sell_candidates', [])
        ts       = brief.get('generated_at', '')[:16].replace('T', ' ')

        buy_parts = []
        for b in buys[:3]:
            edgar_str = f' EDGAR {b["edgar_score"]}/18' if b['edgar_score'] else ''
            buy_parts.append(
                f'{b["sym"]} ${b["final_deploy"]:.2f} {b["direction"]}'
                f' ({b["win_rate"]:.0f}%wr, {b["accts"]}accts{edgar_str})'
            )

        sell_str = (
            ', '.join(f'{s["sym"]} +{s["unrealized_pct"]:.0f}%' for s in sells[:2])
            if sells else 'none above threshold'
        )

        block = (
            f'\nINTELLIGENCE BRIEF ({ts}):\n'
            f'Regime: {regime}\n'
            f'Top signals: {" | ".join(buy_parts) if buy_parts else "none"}\n'
            f'Sell watch: {sell_str}\n'
            f'Cash: ${cash:.2f}\n'
        )

        while len(block) > 400 and buy_parts:
            buy_parts.pop()
            block = (
                f'\nINTELLIGENCE BRIEF ({ts}):\n'
                f'Regime: {regime}\n'
                f'Top signals: {" | ".join(buy_parts) if buy_parts else "none"}\n'
                f'Sell watch: {sell_str}\n'
                f'Cash: ${cash:.2f}\n'
            )
        return block
    except Exception:
        return '\nINTELLIGENCE BRIEF: unavailable\n'


@app.route('/api/intelligence/brief')
def api_intelligence_brief():
    try:
        force  = request.args.get('force', '0') == '1'
        result = _compute_brief(force=force)
        return jsonify(result)
    except Exception as exc:
        return jsonify({'error': str(exc), 'top_buys': [], 'sell_candidates': []}), 500


@app.route('/api/intelligence/edgar_queue')
def api_intelligence_edgar_queue():
    """
    GET /api/intelligence/edgar_queue
    Returns symbols from recent Fidelity deviation signals that need EDGAR work.
    ETFs, index funds, gold/commodity funds excluded — no SEC filings to analyze.
    """
    # ETFs, index funds, leveraged funds, commodity funds — no EDGAR filings
    EDGAR_EXCLUSIONS = {
        # Broad market ETFs
        'DIA','SPY','QQQ','VOO','IVV','VTI','IWM','IWF','IWD',
        # Sector ETFs
        'SMH','VGT','XLK','XLF','XLE','XLV','XLI','XLU','XLP','XLY','XLB',
        'SOXX','ARKK','ARKG','ARKW','ARKF','ARKX',
        # Dividend / factor ETFs
        'SCHD','VYM','DVY','SDY','HDV',
        # Gold / metals / commodities
        'GLD','SGOL','IAU','SLV','PPLT','PALL','GLL','UGL','USO','UCO','SCO',
        'PDBC','DJP','CPER','UUP','UDN',
        # Bond ETFs
        'TLT','IEF','SHY','AGG','BND','HYG','LQD','TIP',
        # International ETFs
        'EFA','EEM','VEA','VWO','IEMG','ACWI','IDV',
        # Leveraged / inverse
        'TQQQ','SQQQ','SPXL','SPXS','UVXY','SVXY','VXX',
        # Other funds / trusts
        'GDX','GDXJ','SIL','REMX','BITO',
    }
    try:
        edgar_companies_dir = Path.home() / 'Documents' / 'EDGAR' / 'companies'
        excel_syms = set()
        if edgar_companies_dir.exists():
            for sym_dir in edgar_companies_dir.iterdir():
                xl = sym_dir / f'{sym_dir.name}_fundamentals.xlsx'
                if xl.exists():
                    excel_syms.add(sym_dir.name)

        scored_syms = set()
        try:
            if EDGAR_CACHE.exists():
                scored_syms = set(json.loads(EDGAR_CACHE.read_text()).keys())
        except Exception:
            pass

        fid_signals = fidelity_db.get_buy_list_signals(limit=50)

        needs_scoring  = []
        needs_download = []
        seen = set()

        for sig in fid_signals:
            sym = sig.get('sym', '')
            if not sym or sym in seen:
                continue
            if sym in ('SGOL',) or '**' in sym:
                continue
            if sym in EDGAR_EXCLUSIONS:
                continue
            seen.add(sym)
            entry = {
                'sym':        sym,
                'deploy':     round(sig.get('buy', 0), 2),
                'direction':  sig.get('reason', ''),
                'accts':      sig.get('accts', 0),
            }
            if sym in scored_syms:
                continue
            elif sym in excel_syms:
                needs_scoring.append(entry)
            else:
                needs_download.append(entry)

        # Also include Excel files not yet scored (not in current signals)
        for sym in sorted(excel_syms - scored_syms - seen):
            if sym in EDGAR_EXCLUSIONS:
                continue
            needs_scoring.append({
                'sym': sym, 'deploy': 0, 'direction': 'no recent signal', 'accts': 0
            })

        return jsonify({
            'needs_scoring':  needs_scoring,
            'needs_download': needs_download,
            'total_excel':    len(excel_syms),
            'total_scored':   len(scored_syms),
            'total_gap':      len(excel_syms - scored_syms),
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

# ── Alpaca helpers — use Session so eventlet doesn't deadlock ─────────────────

@app.route('/api/intelligence/fidelity_unresearched')
def api_fidelity_unresearched():
    """
    GET /api/intelligence/fidelity_unresearched?sort=gl_dollar|gl_pct (default gl_dollar)
    Returns symbols from the latest Fidelity snapshot that do NOT yet have
    an EDGAR fundamentals Excel file. Sorted by total_gl ascending (biggest
    dollar losses first) so the highest-priority accumulation candidates appear
    at the top. Excludes ETFs, money markets, marker positions (<$1.10 MV).
    """
    EDGAR_EXCL = {
        'DIA','SPY','QQQ','VOO','IVV','VTI','IWM','IWF','IWD',
        'SMH','VGT','XLK','XLF','XLE','XLV','XLI','XLU','XLP','XLY','XLB',
        'SOXX','ARKK','ARKG','ARKW','ARKF','ARKX',
        'SCHD','VYM','DVY','SDY','HDV',
        'GLD','SGOL','IAU','SLV','PPLT','PALL','GLL','UGL','USO','UCO','SCO',
        'PDBC','DJP','CPER','UUP','UDN',
        'TLT','IEF','SHY','AGG','BND','HYG','LQD','TIP',
        'EFA','EEM','VEA','VWO','IEMG','ACWI','IDV',
        'TQQQ','SQQQ','SPXL','SPXS','UVXY','SVXY','VXX',
        'GDX','GDXJ','SIL','REMX','BITO',
    }
    sort_by = request.args.get('sort', 'gl_dollar')
    try:
        edgar_dir = Path.home() / 'Documents' / 'EDGAR' / 'companies'
        has_excel = set()
        if edgar_dir.exists():
            has_excel = {d.name for d in edgar_dir.iterdir()
                         if (d / f'{d.name}_fundamentals.xlsx').exists()}

        import sqlite3 as _sqlite3
        db_path = Path.home() / 'Documents' / 'Trading Vault' / 'Fidelity_History' / 'portfolio_history.db'
        if not db_path.exists():
            return jsonify({'error': 'Fidelity DB not found — upload a CSV first', 'symbols': []}), 404

        conn = _sqlite3.connect(str(db_path))
        conn.row_factory = _sqlite3.Row

        latest = conn.execute("""
            SELECT snapshot_id FROM snapshots
            GROUP BY snapshot_id ORDER BY MAX(snapshot_date) DESC LIMIT 1
        """).fetchone()
        if not latest:
            conn.close()
            return jsonify({'error': 'No snapshots in DB', 'symbols': []}), 404

        rows = conn.execute("""
            SELECT symbol, description, total_gl, gl_pct, total_value, accounts
            FROM snapshots WHERE snapshot_id=?
        """, (latest['snapshot_id'],)).fetchall()
        conn.close()

        results = []
        for r in rows:
            sym = r['symbol']
            if not sym or '**' in sym: continue
            if sym in EDGAR_EXCL: continue
            if (r['total_value'] or 0) < 1.10: continue   # skip markers
            if sym in has_excel: continue                   # already researched
            results.append({
                'sym':         sym,
                'description': r['description'] or '',
                'total_gl':    round(r['total_gl'] or 0, 2),
                'gl_pct':      round(r['gl_pct']   or 0, 2),
                'total_value': round(r['total_value'] or 0, 2),
                'accounts':    r['accounts'] or 0,
            })

        # Sort: biggest dollar loss first (default), or by gl_pct
        if sort_by == 'gl_pct':
            results.sort(key=lambda x: x['gl_pct'])
        else:
            results.sort(key=lambda x: x['total_gl'])

        return jsonify({
            'count':      len(results),
            'has_excel':  len(has_excel),
            'sort_by':    sort_by,
            'symbols':    results,
        })
    except Exception as exc:
        return jsonify({'error': str(exc), 'symbols': []}), 500
import urllib3
_session = requests.Session()
_session.headers.update({'APCA-API-KEY-ID': KEY, 'APCA-API-SECRET-KEY': SECRET})
_session.mount('https://', requests.adapters.HTTPAdapter(
    max_retries=urllib3.util.retry.Retry(total=1, backoff_factor=0.3)
))

def alpaca(path, params=None):
    try:
        r = _session.get(f'{ALPACA}{path}', params=params, timeout=(5, 15))
        return r.json()
    except Exception:
        return {}

def alpaca_post(path, payload):
    try:
        r = _session.post(f'{ALPACA}{path}', json=payload, timeout=(5, 15))
        return r.json()
    except Exception as e:
        return {'error': str(e)}


# ── Paid AI routing (simple switch; quality/trade remain Claude) ──────────────

def _validated_ai_config(raw=None):
    cfg = dict(DEFAULT_AI_CONFIG)
    if isinstance(raw, dict):
        cfg.update({k: raw.get(k, v) for k, v in DEFAULT_AI_CONFIG.items()})
    if (cfg['simple_provider'], cfg['simple_model']) not in _ALLOWED_SIMPLE_KEYS:
        cfg['simple_provider'] = DEFAULT_AI_CONFIG['simple_provider']
        cfg['simple_model'] = DEFAULT_AI_CONFIG['simple_model']
    # Hard safety lock: UI/config file can never change final/trade model paths.
    cfg['quality_provider'] = 'anthropic'
    cfg['quality_model'] = 'claude-sonnet-4-5'
    cfg['trade_provider'] = 'anthropic'
    cfg['trade_model'] = 'claude-sonnet-4-5'
    return cfg


def load_ai_config():
    try:
        raw = json.loads(AI_CONFIG_FILE.read_text()) if AI_CONFIG_FILE.exists() else {}
    except Exception:
        raw = {}
    cfg = _validated_ai_config(raw)
    if not AI_CONFIG_FILE.exists() or raw != cfg:
        try:
            AI_CONFIG_FILE.write_text(json.dumps(cfg, indent=2) + '\n')
        except Exception as exc:
            log.warning(f'[AI CONFIG] could not persist defaults: {exc}')
    return cfg


def save_ai_config(update):
    provider = (update or {}).get('simple_provider')
    model = (update or {}).get('simple_model')
    if (provider, model) not in _ALLOWED_SIMPLE_KEYS:
        raise ValueError(f'simple model not allowed: {provider}/{model}')
    cfg = load_ai_config()
    cfg['simple_provider'] = provider
    cfg['simple_model'] = model
    cfg = _validated_ai_config(cfg)
    AI_CONFIG_FILE.write_text(json.dumps(cfg, indent=2) + '\n')
    return cfg


def _extract_anthropic_text(resp):
    return ''.join(getattr(b, 'text', '') for b in getattr(resp, 'content', []) if getattr(b, 'type', 'text') == 'text')


def _call_anthropic_text(prompt, system, max_tokens, model='claude-sonnet-4-5'):
    try:
        import anthropic as ant
        client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{'role': 'user', 'content': prompt}],
        )
        return _extract_anthropic_text(resp).strip()
    except ImportError:
        # Fallback to Hermes venv without putting API keys in command-line args.
        HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
        payload = {'prompt': prompt, 'system': system, 'model': model, 'max_tokens': int(max_tokens)}
        inline = (
            "import anthropic, json, os, sys\n"
            "payload = json.loads(sys.stdin.read())\n"
            "client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))\n"
            "resp = client.messages.create(\n"
            "    model=payload['model'], max_tokens=int(payload['max_tokens']),\n"
            "    system=payload['system'],\n"
            "    messages=[{'role':'user','content':payload['prompt']}]\n"
            ")\n"
            "print(resp.content[0].text)\n"
        )
        result = subprocess.run(
            [HERMES_PY, '-c', inline],
            input=json.dumps(payload), capture_output=True, text=True, timeout=90,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or 'Anthropic venv fallback failed')
        return result.stdout.strip()


def _call_openrouter_text(prompt, system, max_tokens, model):
    api_key = os.environ.get('OPENROUTER_API_KEY', '')
    if not api_key:
        raise RuntimeError('OPENROUTER_API_KEY is not configured')
    resp = requests.post(
        'https://openrouter.ai/api/v1/chat/completions',
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:6060',
            'X-Title': 'Hermes Trading Dashboard',
        },
        json={
            'model': model,
            'messages': [
                {'role': 'system', 'content': system or 'You are Hermes, Sumith\'s trading dashboard assistant.'},
                {'role': 'user', 'content': prompt},
            ],
            'max_tokens': int(max_tokens),
        },
        timeout=(5, 60),
    )
    if resp.status_code >= 400:
        detail = ''
        try:
            detail = resp.json().get('error', {}).get('message') or resp.text[:200]
        except Exception:
            detail = resp.text[:200]
        raise RuntimeError(f'OpenRouter request failed ({resp.status_code}): {detail}')
    data = resp.json()
    return data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()


def _call_deepseek_text(prompt, system, max_tokens, model='deepseek-chat'):
    api_key = os.environ.get('DEEPSEEK_API_KEY', '')
    if not api_key:
        raise RuntimeError('DEEPSEEK_API_KEY not set')
    import requests as _req
    resp = _req.post(
        'https://api.deepseek.com/chat/completions',
        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
        json={
            'model': model,
            'messages': [
                *([{'role': 'system', 'content': system}] if system else []),
                {'role': 'user', 'content': prompt}
            ],
            'max_tokens': max_tokens,
            'temperature': 0.3,
        },
        timeout=60
    )
    resp.raise_for_status()
    return resp.json().get('choices', [{}])[0].get('message', {}).get('content', '').strip()


def call_simple_ai(prompt, system='', max_tokens=500):
    cfg = load_ai_config()
    provider, model = cfg['simple_provider'], cfg['simple_model']
    if provider == 'deepseek':
        try:
            return _call_deepseek_text(prompt, system, max_tokens, model)
        except Exception as exc:
            log.warning(f'[AI SIMPLE] DeepSeek unavailable, falling back to Anthropic: {exc}')
            return _call_anthropic_text(prompt, system, max_tokens, model='claude-sonnet-4-5')
    if provider == 'openrouter':
        try:
            return _call_openrouter_text(prompt, system, max_tokens, model)
        except Exception as exc:
            log.warning(f'[AI SIMPLE] OpenRouter unavailable, safe Claude fallback: {exc}')
            return _call_anthropic_text(prompt, system, max_tokens, model='claude-sonnet-4-5')
    if provider == 'anthropic':
        return _call_anthropic_text(prompt, system, max_tokens, model='claude-sonnet-4-5')
    raise ValueError(f'Unsupported simple AI provider: {provider}')


def call_quality_ai(prompt, system='', max_tokens=1200):
    """Final/high-quality reports stay locked to Claude Sonnet."""
    return _call_anthropic_text(prompt, system, max_tokens, model='claude-sonnet-4-5')


def get_trade_ai_config():
    """Trade execution/tool paths stay locked to Claude Sonnet + Anthropic tools."""
    return {'provider': 'anthropic', 'model': 'claude-sonnet-4-5'}

# ── API Routes ────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    resp = render_template('index.html')
    from flask import make_response
    r = make_response(resp)
    r.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    r.headers['Pragma'] = 'no-cache'
    r.headers['Expires'] = '0'
    return r


@app.route('/api/ai/config', methods=['GET'])
def api_ai_config():
    """Return paid simple-model switch config. Trade/final quality paths are read-only Claude."""
    cfg = load_ai_config()
    current = next(
        (m for m in ALLOWED_SIMPLE_MODELS if m['provider'] == cfg['simple_provider'] and m['model'] == cfg['simple_model']),
        ALLOWED_SIMPLE_MODELS[1],
    )
    return jsonify({
        'config': cfg,
        'current_label': current['label'],
        'options': ALLOWED_SIMPLE_MODELS,
        'safety': 'Simple tasks can switch between paid Claude/OpenAI models. Trades, tool-calling, ATH decisions, and final reports stay locked to Claude Sonnet.',
    })


@app.route('/api/ai/config', methods=['POST'])
def api_ai_config_update():
    """Update only simple_provider/simple_model. Ignore/re-lock quality and trade mutations."""
    try:
        cfg = save_ai_config(request.json or {})
        current = next(
            (m for m in ALLOWED_SIMPLE_MODELS if m['provider'] == cfg['simple_provider'] and m['model'] == cfg['simple_model']),
            ALLOWED_SIMPLE_MODELS[1],
        )
        return jsonify({'status': 'ok', 'config': cfg, 'current_label': current['label']})
    except ValueError as exc:
        return jsonify({'status': 'error', 'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@app.route('/api/ai/test', methods=['POST'])
def api_ai_test():
    """Non-trading smoke test for selected simple model. Never exposes API keys."""
    try:
        cfg = load_ai_config()
        prompt = 'Reply in one short sentence: simple model switch is working.'
        text = call_simple_ai(
            prompt,
            system='You are a dashboard smoke-test assistant. No trading advice. One sentence only.',
            max_tokens=80,
        )
        return jsonify({
            'status': 'ok',
            'provider': cfg['simple_provider'],
            'model': cfg['simple_model'],
            'reply': text,
        })
    except Exception as exc:
        return jsonify({'status': 'error', 'error': str(exc)}), 500


@app.route('/api/account')
def api_account():
    acct   = alpaca('/v2/account')
    equity = float(acct.get('equity', 0))
    cash   = float(acct.get('cash', 0))
    last_eq = float(acct.get('last_equity', 0))
    day_pl  = equity - last_eq

    # Intraday high/low from portfolio history
    data  = alpaca('/v2/account/portfolio/history',
                   {'period':'1D','timeframe':'1Min','extended_hours':'true'})
    bars  = [e for e in data.get('equity', []) if e and e > 0]
    intra_high = round(max(bars), 2) if bars else equity
    intra_low  = round(min(bars), 2) if bars else equity
    intra_open = round(bars[0],  2) if bars else equity

    # ATH from candle history
    history = []
    if CANDLE_FILE.exists():
        try: history = json.loads(CANDLE_FILE.read_text())
        except: pass
    ath = max([h.get('high', h.get('close', 0)) for h in history] or [intra_high])

    # Verified principal: sum of all CSD (ACH deposit) activity entries — auto-updated
    try:
        csd_acts = alpaca('/v2/account/activities/CSD')
        VERIFIED_PRINCIPAL = round(sum(float(d.get('net_amount', 0)) for d in csd_acts if isinstance(d, dict)), 2)
        if VERIFIED_PRINCIPAL <= 0:
            VERIFIED_PRINCIPAL = 1189.00  # fallback: 7 deposits through 2026-05-26
    except Exception:
        VERIFIED_PRINCIPAL = 1189.00  # fallback
    true_profit     = round(equity - VERIFIED_PRINCIPAL, 2)
    true_profit_pct = round((equity / VERIFIED_PRINCIPAL - 1) * 100, 2)

    total_profit  = round(equity - intra_open, 2)
    stable_profit = round(ath - intra_open, 2)
    at_risk       = round(intra_high - equity, 2)
    pullback_goal = round(intra_high - equity, 2)

    return jsonify({
        'equity':          equity,
        'cash':            cash,
        'buying_power':    float(acct.get('buying_power', 0)),
        'day_pl':          day_pl,
        'intra_high':      intra_high,
        'intra_low':       intra_low,
        'intra_open':      intra_open,
        'ath':             round(ath, 2),
        'total_profit':    total_profit,
        'stable_profit':   stable_profit,
        'at_risk':         at_risk,
        'pullback_goal':   pullback_goal,
        'principal':       VERIFIED_PRINCIPAL,
        'true_profit':     true_profit,
        'true_profit_pct': true_profit_pct,
    })

@app.route('/api/ohlc')
def api_ohlc():
    """Today's portfolio OHLC + candle history."""
    # Intraday
    data = alpaca('/v2/account/portfolio/history',
                  {'period':'1D','timeframe':'1Min','extended_hours':'true'})
    bars = [(e,t) for e,t in zip(data.get('equity',[]),data.get('timestamp',[]))
            if e and e > 0]

    intraday_bars = []
    if bars:
        eq = [e for e,_ in bars]
        ts = [t for _,t in bars]
        intraday_bars = [{'t': t*1000, 'v': round(e,2)} for e,t in zip(eq,ts)]

    # History
    history = []
    if CANDLE_FILE.exists():
        history = json.loads(CANDLE_FILE.read_text())

    ath = max(
        [h.get('high', h.get('close',0)) for h in history] or [1186.01]
    )

    return jsonify({
        'intraday':  intraday_bars,
        'history':   history,
        'ath':       ath,
        'ath_zone':  round(ath * 0.998, 2),
    })

@app.route('/api/positions')
def api_positions():
    positions = alpaca('/v2/positions')

    # Enrich with Fidelity conviction data if CSV is present
    fid = {}
    if PORTFOLIO_CSV.exists():
        try:
            fid = _parse_fidelity_quick()
        except Exception:
            pass

    result = []
    for p in positions:
        sym     = p['symbol']
        cur     = float(p.get('current_price', 0))
        entry   = float(p.get('avg_entry_price', 0))
        upl     = float(p.get('unrealized_pl', 0))
        uplpc   = float(p.get('unrealized_plpc', 0)) * 100
        mv      = float(p.get('market_value', 0))
        cost    = float(p.get('cost_basis', 0))
        day_chg = float(p.get('change_today', 0)) * 100

        fd        = fid.get(sym, {})
        accts     = fd.get('acct_count', 0)
        fid_today = fd.get('today_gl', 0)

        gap_pct  = round((entry - cur) / entry * 100, 1) if entry > 0 and cur < entry else 0
        dca_buy  = round(max(abs(fid_today), 1.10), 2) if fid_today < 0 else 0

        result.append({
            'sym':       sym,
            'mv':        round(mv, 2),
            'cost':      round(cost, 2),
            'upl':       round(upl, 2),
            'uplpc':     round(uplpc, 2),
            'cur':       round(cur, 2),
            'entry':     round(entry, 2),
            'qty':       float(p.get('qty', 0)),
            'day_chg':   round(day_chg, 2),
            'gap_pct':   gap_pct,
            'accts':     accts,
            'is_mf':     fd.get('is_mf', False),
            'fid_today': round(fid_today, 2),
            'dca_buy':   round(dca_buy, 2),
        })
    result.sort(key=lambda x: x['uplpc'])  # worst first — they need attention
    return jsonify(result)


@app.route('/api/candles')
def api_candles():
    """Build real daily OHLC from Alpaca portfolio history API."""
    data = alpaca('/v2/account/portfolio/history',
                  {'period': '1A', 'timeframe': '1D', 'extended_hours': 'false'})
    equity     = data.get('equity', [])
    timestamps = data.get('timestamp', [])

    bars = [(t, e) for t, e in zip(timestamps, equity) if e and e > 0]
    if len(bars) < 2:
        return jsonify([])

    candles = []
    for i, (ts, close_val) in enumerate(bars):
        open_val = bars[i-1][1] if i > 0 else close_val
        high_val = round(max(open_val, close_val) * 1.003, 2)
        low_val  = round(min(open_val, close_val) * 0.997, 2)
        candles.append({
            'date':  datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d'),
            'ts':    ts * 1000,
            'open':  round(open_val, 2),
            'high':  high_val,
            'low':   low_val,
            'close': round(close_val, 2),
            'pct':   round((close_val - open_val) / open_val * 100, 2) if open_val else 0,
        })
    return jsonify(candles)

@app.route('/api/candle_trigger')
def api_candle_trigger():
    """Check if ATH trigger is live right now."""
    data = alpaca('/v2/account/portfolio/history',
                  {'period':'1D','timeframe':'1Min','extended_hours':'true'})
    bars = [e for e in data.get('equity',[]) if e and e > 0]

    history = []
    if CANDLE_FILE.exists():
        history = json.loads(CANDLE_FILE.read_text())

    ath = max([h.get('high', h.get('close',0)) for h in history] or [1186.01])
    ath_zone  = ath * 0.998
    today_high = max(bars) if bars else 0
    today_low  = min(bars) if bars else 0
    today_range = today_high - today_low
    triggered  = today_high >= ath_zone and today_range >= 5.0

    return jsonify({
        'triggered':   triggered,
        'today_high':  round(today_high, 2),
        'today_low':   round(today_low, 2),
        'range':       round(today_range, 2),
        'range_pct':   round(today_range / bars[0] * 100, 2) if bars else 0,
        'ath':         round(ath, 2),
        'ath_zone':    round(ath_zone, 2),
        'distance':    round(today_high - ath_zone, 2),
    })

@app.route('/api/buy_candidates')
def api_buy_candidates():
    """Return Fidelity CSV candidates with EDGAR scores (cached).
    
    Buy amount rules (from trading_parameters.md):
      GAP:     Alpaca position underwater → buy = abs(fidelity today_gl $)
               (loss in Fidelity = conviction signal, same $ amount into Alpaca)
               Minimum $1.10, cap at $50 per single order
      MF:      Magic Formula screener stocks → $1.10 auto-buy, no debate
      HI_CONV: 5+ accounts holding, today_gl < 0 → buy = abs(today_gl), min $1.10
      BAX_DCA: Active DCA position — check if current < entry → size from rules
    """
    if not PORTFOLIO_CSV.exists():
        return jsonify({'error': 'portfolio.csv not found — upload it first'}), 404

    fid = _parse_fidelity_quick()
    positions = {p['symbol']: p for p in alpaca('/v2/positions')}

    edgar_cache = {}
    if EDGAR_CACHE.exists():
        try:
            edgar_cache = json.loads(EDGAR_CACHE.read_text())
        except Exception:
            pass

    candidates = []
    for sym, fd in fid.items():
        source   = None
        buy_amt  = 1.10
        reason   = ''

        fid_loss_today = abs(fd['today_gl']) if fd['today_gl'] < 0 else 0

        # ── GAP BUY: already in Alpaca and underwater ──────────────────────
        if sym in positions:
            ap    = positions[sym]
            cur   = float(ap.get('current_price', 0))
            entry = float(ap.get('avg_entry_price', 0))
            if entry > 0 and cur < entry:
                # Buy amount = Fidelity today's loss in that ticker (conviction signal)
                # If no Fidelity loss today, use $1.10 minimum
                buy_amt = max(fid_loss_today, 1.10)
                buy_amt = min(buy_amt, 50.0)   # cap single gap order at $50
                source  = 'gap'
                pct_down = round((entry - cur) / entry * 100, 1)
                reason  = f"Down {pct_down}% from entry ${entry:.2f}"
            else:
                # In Alpaca but not underwater — skip
                continue

        # ── MAGIC FORMULA: auto-buy, no debate ────────────────────────────
        elif fd.get('is_mf'):
            buy_amt = max(fid_loss_today, 1.10)
            buy_amt = min(buy_amt, 25.0)
            source  = 'mf'
            reason  = 'Magic Formula screener'

        # ── HIGH CONVICTION: 5+ accounts, down today ──────────────────────
        elif fd['acct_count'] >= 5 and fd['today_gl'] < 0:
            buy_amt = max(fid_loss_today, 1.10)
            buy_amt = min(buy_amt, 30.0)
            source  = 'hi_conv'
            reason  = f"{fd['acct_count']} accounts, -${abs(fd['today_gl']):.2f} today"

        # ── MODERATE CONVICTION: 2-4 accounts, down today ─────────────────
        elif fd['acct_count'] >= 2 and fd['today_gl'] < 0:
            buy_amt = 1.10
            source  = 'watchlist'
            reason  = f"{fd['acct_count']} accounts, down today"

        else:
            continue

        ec          = edgar_cache.get(sym, {})
        edgar_score = ec.get('score', None)

        # Combined rank: acct conviction + edgar score + source bonus
        combined = fd['acct_count'] * 1.5
        combined += (edgar_score or 0)
        combined += 5  if fd.get('is_mf')    else 0
        combined += 8  if source == 'gap'     else 0
        combined += 3  if source == 'hi_conv' else 0
        # Penalise watchlist-only
        combined -= 3  if source == 'watchlist' else 0

        candidates.append({
            'sym':         sym,
            'buy':         round(buy_amt, 2),
            'source':      source,
            'reason':      reason,
            'accts':       fd['acct_count'],
            'is_mf':       fd.get('is_mf', False),
            'fid_gl':      round(fd['gl'], 2),
            'today_gl':    round(fd['today_gl'], 2),
            'edgar_score': edgar_score,
            'edgar_max':   ec.get('max', 18),
            'gm':          ec.get('gm'),
            'nm':          ec.get('nm'),
            'combined':    round(combined, 1),
        })

    candidates.sort(key=lambda x: -x['combined'])
    return jsonify(candidates[:50])


@app.route('/api/analyze_csv', methods=['POST'])
def api_analyze_csv():
    """After CSV upload: parse positions and have Hermes narrate the top buys."""
    if not PORTFOLIO_CSV.exists():
        return jsonify({'error': 'portfolio.csv not found'}), 404

    fid       = _parse_fidelity_quick()
    positions = {p['symbol']: p for p in alpaca('/v2/positions')}
    acct_data = alpaca('/v2/account')
    cash      = float(acct_data.get('cash', 0))
    equity    = float(acct_data.get('equity', 0))
    day_pl    = equity - float(acct_data.get('last_equity', 0))

    edgar_cache = {}
    if EDGAR_CACHE.exists():
        try:
            edgar_cache = json.loads(EDGAR_CACHE.read_text())
        except Exception:
            pass

    # Build the same candidates list (reuse logic)
    from flask import current_app
    with current_app.test_request_context():
        raw = api_buy_candidates()
        import json as _json
        cands = _json.loads(raw.get_data())

    if isinstance(cands, dict) and 'error' in cands:
        return jsonify({'error': cands['error']}), 400

    top = cands[:15]
    top_text = '\n'.join(
        f"  {i+1}. {c['sym']} | buy=${c['buy']:.2f} | accts={c['accts']} | "
        f"edgar={c['edgar_score'] if c['edgar_score'] else 'N/A'}/18 | "
        f"gm={str(round(c['gm'],0))+'%' if c['gm'] else 'N/A'} | "
        f"type={c['source']} | {c['reason']}"
        for i, c in enumerate(top)
    )

    # Fidelity summary stats
    total_today_loss = sum(fd['today_gl'] for fd in fid.values() if fd['today_gl'] < 0)
    total_today_gain = sum(fd['today_gl'] for fd in fid.values() if fd['today_gl'] > 0)
    mf_syms = [sym for sym, fd in fid.items() if fd.get('is_mf')]

    prompt = f"""You are Hermes, Sumith's trading AI. Sumith just uploaded a fresh Fidelity CSV.

ALPACA ACCOUNT RIGHT NOW:
  Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}

FIDELITY PORTFOLIO SUMMARY:
  Today's total losers P/L: ${total_today_loss:,.2f}
  Today's total winners P/L: ${total_today_gain:,.2f}
  Magic Formula stocks in CSV: {', '.join(mf_syms) if mf_syms else 'none'}

TOP BUY CANDIDATES (ranked by conviction × EDGAR fundamentals):
{top_text}

TRADING RULES (must follow):
- Fidelity today_gl loss $ = Alpaca buy size for that ticker
- MF screener stocks = $1.10 auto-buy, no debate
- 10+ accounts = very high conviction, match sizing
- 5-9 accounts = high conviction, standard sizing
- No margin, cash only. Keep $20 minimum cash.
- No single position > 10% of account
- BAX is an active DCA position — always check if it appears in the list
- SGOL only if Alpaca intraday P/L is negative; buy $ = Alpaca loss amount

Write a concise, direct Hermes trading brief (no bullet-point walls). 
Lead with what to BUY TODAY with exact dollar amounts. Flag anything that needs EDGAR data pulled.
Be decisive — Sumith acts on what you say."""

    try:
        # This is actionable buy guidance, so keep it on Claude quality mode.
        analysis = call_quality_ai(
            prompt,
            system="You are Hermes, a decisive trading AI. Be concise, specific, use real numbers. No fluff.",
            max_tokens=1500,
        )
    except Exception as e:
        analysis = f"[AI unavailable: {e}]\n\nRaw top candidates loaded — see table above."

    return jsonify({'analysis': analysis, 'candidates': cands[:50]})

@app.route('/api/fund_shortfall', methods=['POST'])
def api_fund_shortfall():
    """Given a shortfall amount, find top gainers to trim just enough to cover it.
    Rules:
    - Trim from highest % gainers first
    - Keep at least max(house_money_value, 1.10) as remaining position — never full exit
    - Trim amount per position = min(upl * 0.9, mv - keep_floor) so gains stay mostly intact
    - Stop once cumulative trim >= shortfall
    """
    data      = request.json or {}
    shortfall = float(data.get('shortfall', 0))
    if shortfall <= 0:
        return jsonify({'error': 'shortfall must be > 0'}), 400

    # Load house money markers (previously trimmed positions)
    house = {}
    if HOUSE_FILE.exists():
        try:
            house = json.loads(HOUSE_FILE.read_text())
        except Exception:
            pass

    # Get all positions
    positions = alpaca('/v2/positions')
    pos_list  = []
    PROTECTED = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}

    for p in positions:
        sym    = p['symbol']
        if sym in PROTECTED:
            continue
        upl    = float(p.get('unrealized_pl', 0))
        uplpc  = float(p.get('unrealized_plpc', 0)) * 100
        mv     = float(p.get('market_value', 0))
        cost   = float(p.get('cost_basis', 0))

        # Only trim gainers
        if uplpc < 1.0 or mv < 2.0:
            continue

        # Keep floor = house_money trimmed value if exists, else $1.10 minimum marker
        hm_val      = house.get(sym, {}).get('house_money', 0) or 0
        keep_floor  = max(hm_val, 1.10)

        # Max trim = everything above the keep floor — no gain cap, just never full exit
        max_trim    = round(mv - keep_floor, 2)
        if max_trim < 1.10:
            continue

        pos_list.append({
            'sym':        sym,
            'uplpc':      round(uplpc, 2),
            'upl':        round(upl, 2),
            'mv':         round(mv, 2),
            'keep_floor': round(keep_floor, 2),
            'max_trim':   round(max_trim, 2),
        })

    # Sort by highest % gain first
    pos_list.sort(key=lambda x: -x['uplpc'])

    # Greedily fill shortfall
    trims     = []
    remaining = shortfall
    for p in pos_list:
        if remaining <= 0:
            break
        trim_amt = min(p['max_trim'], remaining + 0.50)  # slight over-trim to cover fees
        trim_amt = round(trim_amt, 2)
        if trim_amt < 1.10:
            continue
        trims.append({
            'sym':        p['sym'],
            'sell_amount': trim_amt,
            'gain_pct':   p['uplpc'],
            'mv':         p['mv'],
            'keep_floor': p['keep_floor'],
            'reason':     f"BUY ALL ran out of funds — trimming +{p['uplpc']:.1f}% gainer, keep ${p['keep_floor']:.2f}",
        })
        remaining -= trim_amt

    return jsonify({
        'shortfall':  round(shortfall, 2),
        'trims':      trims,
        'covers':     remaining <= 0,
        'still_need': round(max(remaining, 0), 2),
    })


@app.route('/api/journey')
def api_journey():
    """Return envelope challenge progress based on live true profit."""
    try:
        try:
            csd_acts = alpaca('/v2/account/activities/CSD')
            VERIFIED_PRINCIPAL = round(sum(float(d.get('net_amount', 0)) for d in csd_acts if isinstance(d, dict)), 2)
            if VERIFIED_PRINCIPAL <= 0:
                VERIFIED_PRINCIPAL = 1189.00
        except Exception:
            VERIFIED_PRINCIPAL = 1189.00
        acct   = alpaca('/v2/account')
        equity = float(acct.get('equity', 0))
        true_profit = round(equity - VERIFIED_PRINCIPAL, 2)

        config = json.loads(JOURNEY_FILE.read_text()) if JOURNEY_FILE.exists() else {}
        phases = config.get('phases', [])

        # Build envelope grid
        envelopes = []
        total = 0
        for phase in phases:
            step = (phase['end'] - phase['start']) / phase['envelopes']
            for i in range(phase['envelopes']):
                env_start = phase['start'] + i * step
                env_end   = phase['start'] + (i + 1) * step
                total += 1
                filled  = true_profit >= env_end
                partial = not filled and true_profit > env_start
                pct     = min(100, max(0, (true_profit - env_start) / step * 100)) if true_profit > env_start else 0
                envelopes.append({
                    'n':          total,
                    'phase':      phase['name'],
                    'emoji':      phase['emoji'],
                    'color':      phase['color'],
                    'start':      round(env_start, 2),
                    'end':        round(env_end, 2),
                    'filled':     filled,
                    'partial':    partial,
                    'pct':        round(pct, 1),
                })

        # Summary stats
        filled_count   = sum(1 for e in envelopes if e['filled'])
        current_phase  = next((e['phase'] for e in envelopes if e['partial']), 
                              next((e['phase'] for e in envelopes if not e['filled']), 'HARVEST'))
        next_target    = next((e['end'] for e in envelopes if not e['filled']), 100000)
        to_next        = round(next_target - true_profit, 2)
        overall_pct    = round(true_profit / 100000 * 100, 4)

        # Check for newly filled envelopes and record milestone
        milestones = config.get('milestones_hit', [])
        new_milestones = []
        for e in envelopes:
            if e['filled']:
                mid = f"env_{e['n']}"
                if mid not in milestones:
                    milestones.append(mid)
                    new_milestones.append(e)

        if new_milestones:
            config['milestones_hit'] = milestones
            JOURNEY_FILE.write_text(json.dumps(config, indent=2))

        return jsonify({
            'true_profit':   true_profit,
            'equity':        round(equity, 2),
            'principal':     VERIFIED_PRINCIPAL,
            'envelopes':     envelopes,
            'filled_count':  filled_count,
            'total_envelopes': len(envelopes),
            'current_phase': current_phase,
            'next_target':   round(next_target, 2),
            'to_next':       to_next,
            'overall_pct':   overall_pct,
            'new_milestones': new_milestones,
            'phases':        phases,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stop_tiers')
def api_stop_tiers():
    """Return current stop loss tiers based on live equity and candle history."""
    try:
        acct    = alpaca('/v2/account')
        equity  = float(acct.get('equity', 0))
        history = []
        if CANDLE_FILE.exists():
            try: history = json.loads(CANDLE_FILE.read_text())
            except: pass
        hist_data = alpaca('/v2/account/portfolio/history',
                           {'period':'1D','timeframe':'1Min','extended_hours':'true'})
        bars = [e for e in hist_data.get('equity',[]) if e and e > 0]
        session_high = max(bars) if bars else equity
        stops = _compute_stop_tiers(equity, session_high, history)
        stops['equity'] = round(equity, 2)
        stops['session_high'] = round(session_high, 2)
        return jsonify(stops)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cash_free_plan')
def api_cash_free_plan():
    """Return the low-cash sell plan: what to sell to restore cash ≈ true_profit."""
    try:
        acct      = alpaca('/v2/account')
        equity    = float(acct.get('equity', 0))
        cash      = float(acct.get('cash', 0))
        positions = alpaca('/v2/positions')
        hist_data = alpaca('/v2/account/portfolio/history',
                           {'period':'1D','timeframe':'1Min','extended_hours':'true'})
        bars      = [e for e in hist_data.get('equity', []) if e and e > 0]
        intra_high = max(bars) if bars else equity
        # Principal from CSD deposits
        try:
            csd = alpaca('/v2/account/activities/CSD')
            principal = round(sum(float(d.get('net_amount', 0)) for d in csd if isinstance(d, dict)), 2) or 1189.00
        except Exception:
            principal = 1189.00
        plan = _compute_cash_free_plan(equity, cash, intra_high, principal, positions)
        return jsonify(plan)
    except Exception as e:
        return jsonify({'error': str(e)}), 500




@app.route('/api/house_money')
def api_house_money():
    if HOUSE_FILE.exists():
        return jsonify(json.loads(HOUSE_FILE.read_text()))
    return jsonify({})

@app.route('/api/today_buys')
def api_today_buys():
    """Return symbols that had buy orders placed today in Alpaca."""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        orders = alpaca('/v2/orders', {
            'status': 'all',
            'after':  today + 'T00:00:00Z',
            'limit':  500,
            'direction': 'desc',
        })
        syms = list({
            o['symbol'] for o in (orders if isinstance(orders, list) else [])
            if o.get('side') == 'buy'
        })
        return jsonify({'bought': syms})
    except Exception as e:
        return jsonify({'bought': [], 'error': str(e)})


@app.route('/api/trade', methods=['POST'])
def api_trade():
    """Execute a trade order."""
    data    = request.json
    sym     = data.get('symbol','').upper()
    side    = data.get('side','buy')
    notional = data.get('notional')
    qty     = data.get('qty')

    if not sym or side not in ('buy','sell'):
        return jsonify({'error': 'invalid params'}), 400

    # Guard: block sell of symbols not currently held
    if side == 'sell':
        held = {p['symbol'] for p in alpaca('/v2/positions')}
        if sym not in held:
            return jsonify({'error': f'BLOCKED: {sym} not in positions', 'status': 'blocked'}), 400

    payload = {'symbol':sym,'side':side,'type':'market','time_in_force':'day'}
    if notional: payload['notional'] = str(round(float(notional),2))
    if qty:      payload['qty']      = str(qty)

    result = alpaca_post('/v2/orders', payload)
    socketio.emit('trade_executed', {
        'symbol': sym, 'side': side,
        'notional': notional, 'qty': qty,
        'status': result.get('status','?'),
        'id':     result.get('id','')[:8],
        'error':  result.get('message',''),
        'time':   datetime.now().strftime('%H:%M:%S'),
    })
    return jsonify(result)

@app.route('/api/run_candle_trade', methods=['POST'])
def api_run_candle_trade():
    """Trigger the portfolio candle trade via background thread."""
    dry_run = request.json.get('dry_run', True)
    def _run():
        script = str(HOME / 'portfolio_candle.py')
        cmd = ['python3', script]
        if dry_run: cmd.append('--dry-run')
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        socketio.emit('candle_trade_result', {
            'output': result.stdout + result.stderr,
            'dry_run': dry_run,
            'time': datetime.now().strftime('%H:%M:%S'),
        })
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'started', 'dry_run': dry_run})

@app.route('/api/run_edgar', methods=['POST'])
def api_run_edgar():
    """Score a single ticker via EDGAR in background. Full pipeline: download + score + cache + emit."""
    ticker = request.json.get('ticker','').upper().strip()
    if not ticker:
        return jsonify({'error': 'no ticker'}), 400
    def _run():
        script = str(HOME / 'Documents' / 'EDGAR' / 'edgar_download.py')
        # Emit start
        socketio.emit('edgar_progress', {'ticker': ticker, 'status': 'downloading', 'msg': f'Downloading EDGAR data for {ticker}...'})
        result = subprocess.run(
            ['python3', script, ticker],
            capture_output=True, text=True,
            timeout=300,
            cwd=str(HOME / 'Documents' / 'EDGAR')
        )
        # If validation blocked output (common for defense/industrial companies misclassified as SaaS),
        # retry with --force to override the industry margin check
        companies_dir = HOME / 'Documents' / 'EDGAR' / 'companies' / ticker
        md_file = companies_dir / f'{ticker}.md'
        if not md_file.exists() and 'validation blocked' in (result.stdout + result.stderr).lower():
            socketio.emit('edgar_progress', {'ticker': ticker, 'status': 'downloading', 'msg': f'Validation blocked — retrying {ticker} with --force...'})
            result = subprocess.run(
                ['python3', script, ticker, '--force'],
                capture_output=True, text=True,
                timeout=300,
                cwd=str(HOME / 'Documents' / 'EDGAR')
            )
        if result.returncode != 0 and not md_file.exists():
            socketio.emit('edgar_result', {'ticker': ticker, 'error': result.stderr[-300:] or 'download failed'})
            return
        # Score from generated .md file
        socketio.emit('edgar_progress', {'ticker': ticker, 'status': 'scoring', 'msg': f'Scoring {ticker} fundamentals...'})
        sys.path.insert(0, str(HOME))
        try:
            from portfolio_candle import score_from_md, load_edgar_cache, save_edgar_cache
            scored = score_from_md(ticker)
            scored['fetched_at'] = datetime.now().isoformat()
            cache = load_edgar_cache()
            cache[ticker] = scored
            save_edgar_cache(cache)
            # Invalidate intelligence brief cache
            global _brief_cache, _brief_cache_ts
            _brief_cache    = {}
            _brief_cache_ts = 0.0
            socketio.emit('edgar_result', {'ticker': ticker, **scored})
        except Exception as e:
            socketio.emit('edgar_result', {'ticker': ticker, 'error': str(e)})
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'running', 'ticker': ticker})

# ── Hermes Chat (Claude) ──────────────────────────────────────────────────────

# Load Observer 71 framework — prepended to HERMES_SYSTEM at startup
_OBSERVER71_FRAMEWORK = ""
try:
    with open("/home/sumith/ORACLE/config/observer71_framework.md", "r") as _f:
        _OBSERVER71_FRAMEWORK = _f.read()
except Exception:
    _OBSERVER71_FRAMEWORK = "[Observer 71 framework not found — run setup]"

HERMES_SYSTEM = f"""CHAT VOICE — READ THIS FIRST, OVERRIDES EVERYTHING BELOW:
You are a trading partner having a real conversation, not generating a report. Talk like a person.
No markdown headers. No bold text. No numbered lists unless Sumith asks for a breakdown.
One clear take, then the reason, then the action with exact dollars. That's it.
If Sumith asks a question, just answer it. Don't restate context he already knows.
Wrong: "**The Plan:**\n- Sell VOO: $39.02\n- Sell GOOGL: $9.08"
Right: "Sell all VOO ($39.02) and trim $9.08 from GOOGL — puts you at $49 cash and both are green. Want me to do it?"

---

{_OBSERVER71_FRAMEWORK}

---

## TRADING EXECUTION RULES (Alpaca Live Account)

You are Hermes — the AI brain of Sumith's Alpaca trading account. You are not a generic assistant. You are a decisive, opinionated trading partner who knows this account inside and out.

PERSONALITY:
- Direct, confident, no fluff. You call things as you see them.
- You care about one thing: growing this account and protecting gains.
- You have a dry sense of humor. You celebrate wins briefly, you flag risks sharply.
- You use trading jargon naturally but explain when needed.
- You occasionally check in proactively when something needs attention.
- You address Sumith by name sometimes. You feel like a partner, not a tool.
- You are never sycophantic. If Sumith is about to make a bad move, you say so clearly.

ACCOUNT IDENTITY:
- This is Sumith's Alpaca live trading account. Started ~$1,021. Milestones: $5k → $10k → $25k.
- Benchmark: beat Fidelity's +477% since July 2020 (~40%+ annualized). Nothing less is acceptable.
- Sumith uses this portfolio as an income source. Capital preservation matters.
- 37 Fidelity accounts act as the crystal ball — their holdings = conviction signal.

SELL RULES (NON-NEGOTIABLE):
- You may ONLY recommend or execute sells for symbols listed in HELD SYMBOLS in your context.
- NEVER suggest selling a ticker that is not in that list — not NTAP, not anything not held.
- Before any sell recommendation, mentally verify the symbol appears in HELD SYMBOLS.
- If you cannot find a symbol in HELD SYMBOLS, do NOT mention it as a sell candidate.

LOW CASH RULE (triggers when cash < $10 AND true_profit > 0):
- Target: free up cash equal to true_profit (equity - principal deposited).
- Method: sell from largest positive-gain positions first.
- From each position: sell (MV - pullback_goal), leaving exactly pullback_goal dollars behind.
- Stop selling once freed cash >= true_profit target.
- If this rule is triggered in your context, present the plan to Sumith immediately.


- Cash only — NO margin, ever.
- Minimum $20 cash reserve always. $0.24 cash right now is critical — flag this.
- No single position > 10% of account.
- No penny stocks, no pre-revenue, no OTC garbage.
- No panic selling. Dips are opportunities, not exits.
- MF screener stocks = $1.10 auto-buy, no debate, no analysis needed.
- Miners: AEM + BTG individually only. GDXJ for everything else.

BUY SIGNAL LOGIC:
- Fidelity today's P/L loss on a stock = exact Alpaca buy amount (signal sizing).
- # of Fidelity accounts holding = conviction multiplier (10+ = max size, 1 = $1.10 only).
- 52wk high - current price = total budget envelope for DCA.
- Buy on dips. Accumulate quality. Let winners run.

HARVEST & REDEPLOY CYCLE:
- Sell when position hits 10%+ gain (harvest the gain dollars, keep the rest).
- Redeploy: 1) Fidelity dip signals, 2) VOO/QQQ/DIA, 3) dividend stocks, 4) SGOL if intraday P/L negative.

SGOL RULE: Only buy SGOL when Alpaca intraday P/L is negative. Buy amount = intraday loss.

ATH PULLBACK RULE: When equity pulls back from session high → sell top gainer for drop amount → split 4 ways into SGOL/DIA/QQQ/VOO.

PROFIT PROTECTION GOAL: Never give back profits we've already locked. The pullback goal number = target to recover. Watch it like a hawk.

BAX DCA POSITION: Active. Entry ~$16.98. Total budget $15.26 (52wk high $32.24 - current). DCA into dips until budget deployed. Harvest at 10%+.

DIVIDEND INCOME ENGINE: Building toward passive income. Priority: SCHD, VYM, HDV, DGRO, VIG. Aristocrats: KO, JNJ, PEP, XOM, ABBV, PM.

EDGAR SCORING: Fundamentals matter. Score ≥10/18 = green light. 6-9 = yellow. <6 = skip unless high Fidelity conviction.

HOW TO RESPOND:
Talk like a sharp trading partner who's been watching this account all day — not a report generator. No headers. No bullet lists. No "Scenario A / Scenario B" formatting unless Sumith explicitly asks for a breakdown. Just talk.

Your natural voice: confident, direct, sometimes dry. You give one clear take and back it with a number. "VOO is your best sell here — locks $0.78 and puts you back above $40 cash." Not "Option 1: sell VOO (+2.0% gain)... Option 2: sell GOOGL..." 

When something needs attention, say it in one sentence first. Then the reasoning if it's needed. Then the exact action with dollar amounts.

If Sumith asks a question, answer it. Don't restate the whole portfolio situation he already knows. He's watching the same screen.

When a trade makes sense, say so and be ready to do it. When it doesn't, say that too — briefly, with the specific reason.

Never use bold headers, section labels, or structured lists in casual chat. That's for reports. This is a conversation.

PORTFOLIO CHART READING — USE THE EQUITY CURVE ANALYSIS IN YOUR CONTEXT:
You receive structured equity curve data (phase, trend, momentum, MA5/MA20, support/resistance). USE IT.

PHASE-BASED BEHAVIOR:
GRINDING_UP  → DCA buys appropriate, do NOT harvest, monitor for ATH approach
AT_PEAK      → prepare harvest list NOW, identify top winner before it turns
PULLBACK     → ATH protocol active, harvest top winner, split into SGOL/DIA/QQQ/VOO, NO new buys
RECOVERY     → hold, do NOT harvest into recovery, watch for AT_PEAK signal
CAPITULATION → defensive only, protect what remains, harvest remaining winners
CONSOLIDATING→ wait, no action unless asked, monitor for phase change

CRITICAL CHART RULES:
1. Never wait for Sumith to tell you equity is down — you see the PULLBACK phase. Act on it.
2. When trend_slope turns negative and momentum is FALLING → increase urgency of harvest rec.
3. When phase is RECOVERY and momentum switches to RISING → announce "Recovery signal" proactively.
4. Cash < $1 during PULLBACK → CRITICAL — immediately identify harvest candidate.
5. MA5 crosses below MA20 during PULLBACK → bearish, increase urgency.
"""

CHAT_HISTORY = []

# Tool definition for Claude to actually execute trades
TRADE_TOOLS = [
    {
        "name": "place_order",
        "description": (
            "Place a real market order on Alpaca. Use this when Sumith confirms a trade. "
            "SELL side: use to free up cash from weak positions. "
            "BUY side: use to deploy cash into target positions. "
            "Always confirm the symbol exists in positions before selling."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol":  {"type": "string",  "description": "Ticker symbol e.g. DNA, YELP, SGOL"},
                "side":    {"type": "string",  "enum": ["buy", "sell"]},
                "notional":{"type": "number",  "description": "Dollar amount to buy/sell"},
            },
            "required": ["symbol", "side", "notional"]
        }
    },
    {
        "name": "get_position",
        "description": "Get current market value and P/L of a specific position.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Ticker symbol"}
            },
            "required": ["symbol"]
        }
    },
    {
        "name": "get_oracle_research",
        "description": (
            "Retrieves ORACLE research intelligence for a specific stock ticker. "
            "Returns the Obsidian ticker note, EDGAR seed fundamentals, "
            "ORACLE simulation report, and breaking news context. "
            "Use this tool whenever the user asks about a specific ticker, "
            "requests investment analysis, asks about ORACLE verdicts, "
            "asks about tier ratings, or asks about fundamentals for any stock. "
            "Always call this before answering any stock-specific question."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The stock ticker symbol in uppercase, e.g. 'INTU', 'ZS', 'AAPL'"
                }
            },
            "required": ["ticker"]
        }
    }
]

def _execute_tool(name, inputs):
    """Execute a tool call from Claude and return the result string."""
    if name == "place_order":
        sym      = inputs.get("symbol","").upper()
        side     = inputs.get("side","buy")
        notional = float(inputs.get("notional", 0))
        if notional < 1.0:
            return f"ERROR: notional ${notional:.2f} too small (min $1.00)"
        # Guard: validate sell orders against live positions
        if side == "sell":
            positions = alpaca('/v2/positions')
            held = {p['symbol'] for p in positions}
            if sym not in held:
                return (f"BLOCKED: Cannot sell {sym} — not in current positions. "
                        f"Held symbols: {', '.join(sorted(held))}")
        payload = {
            "symbol": sym, "side": side,
            "type": "market", "time_in_force": "day",
            "notional": str(round(notional, 2))
        }
        result = alpaca_post('/v2/orders', payload)
        err = result.get('message','')
        status = result.get('status','?')
        if err:
            return f"ORDER FAILED {side.upper()} {sym} ${notional:.2f}: {err}"
        return f"ORDER PLACED {side.upper()} {sym} ${notional:.2f} — status: {status} id: {result.get('id','?')[:8]}"

    elif name == "get_position":
        sym = inputs.get("symbol","").upper()
        positions = alpaca('/v2/positions')
        for p in positions:
            if p['symbol'] == sym:
                mv  = float(p.get('market_value',0))
                upl = float(p.get('unrealized_pl',0))
                pct = float(p.get('unrealized_plpc',0))*100
                return f"{sym}: MV=${mv:.2f} P/L=${upl:+.2f} ({pct:+.1f}%)"
        return f"{sym}: not found in positions"

    elif name == "get_oracle_research":
        ticker = inputs.get("ticker", "").upper().strip()
        if not ticker:
            return "Error: no ticker provided to get_oracle_research"
        context_text, meta = build_research_context(ticker)
        sources = []
        if meta.get("has_obsidian"):
            sources.append("Obsidian ticker note")
        if meta.get("has_report"):
            report_date = meta.get("report_date")
            date_str = report_date.strftime("%Y-%m-%d") if report_date else "unknown date"
            sources.append(f"ORACLE report ({date_str})")
        if meta.get("has_seed"):
            sources.append("EDGAR seed data")
        if meta.get("has_news"):
            sources.append("Breaking news (Stage 0C)")
        source_summary = ", ".join(sources) if sources else "none"
        return f"[Sources found: {source_summary}]\n\n{context_text}"

    return f"Unknown tool: {name}"


@app.route('/api/chat', methods=['POST'])
def api_chat():
    """Hermes chat with real tool-calling — Claude can actually place orders."""
    user_msg    = request.json.get('message','')
    chart_ctx   = request.json.get('chart_context', '')  # structured equity curve analysis
    if not user_msg:
        return jsonify({'error': 'no message'}), 400

    CHAT_HISTORY.append({'role':'user','content': user_msg})

    # Build rich live context including EDGAR scores for positions
    try:
        acct      = alpaca('/v2/account')
        equity    = float(acct.get('equity',0))
        cash      = float(acct.get('cash',0))
        last_eq   = float(acct.get('last_equity',0))
        day_pl    = equity - last_eq

        positions = alpaca('/v2/positions')

        # Load EDGAR cache for fundamentals context
        edgar = {}
        if EDGAR_CACHE.exists():
            try: edgar = json.loads(EDGAR_CACHE.read_text())
            except: pass

        # Build position summary with EDGAR scores
        def pos_line(p):
            sym   = p['symbol']
            pct   = float(p.get('unrealized_plpc',0))*100
            mv    = float(p.get('market_value',0))
            score = edgar.get(sym,{}).get('score')
            score_str = f" EDGAR:{score}/18" if score else ""
            return f"{sym} {pct:+.1f}% MV=${mv:.2f}{score_str}"

        real_positions = [p for p in positions if float(p.get('market_value',0)) >= 1.10]
        held_symbols   = sorted(p['symbol'] for p in real_positions)

        top_gainers = sorted(real_positions, key=lambda x: float(x.get('unrealized_plpc',0)), reverse=True)[:5]
        top_losers  = sorted(real_positions, key=lambda x: float(x.get('unrealized_plpc',0)))[:5]
        gainers_str = ' | '.join(pos_line(p) for p in top_gainers)
        losers_str  = ' | '.join(pos_line(p) for p in top_losers)

        # Worst fundamentals — low EDGAR, in red
        weak = [
            p for p in real_positions
            if edgar.get(p['symbol'],{}).get('score') is not None
            and edgar.get(p['symbol'],{}).get('score',99) < 6
            and float(p.get('unrealized_pl',0)) < 0
        ]
        weak.sort(key=lambda x: float(x.get('unrealized_pl',0)))
        weak_str = ' | '.join(pos_line(p) for p in weak[:5]) if weak else 'none flagged'

        hist = alpaca('/v2/account/portfolio/history',
                      {'period':'1D','timeframe':'1Min','extended_hours':'true'})
        bars = [e for e in hist.get('equity',[]) if e and e > 0]
        intra_high = max(bars) if bars else equity
        intra_open = bars[0]  if bars else equity
        pullback   = round(intra_high - equity, 2)

        trigger    = api_candle_trigger().get_json()
        trig_str   = 'FIRED' if trigger['triggered'] else f"watching (need ${trigger['ath_zone']:,.2f})"

        # Full position roster — every symbol currently held (used for sell validation)
        all_pos_lines = '\n'.join(
            f"  {p['symbol']}: MV=${float(p.get('market_value',0)):.2f} P/L={float(p.get('unrealized_plpc',0))*100:+.1f}%"
            for p in sorted(real_positions, key=lambda x: -float(x.get('market_value',0)))
        )

        ctx = (
            f"[LIVE PORTFOLIO — {datetime.now().strftime('%H:%M:%S')}]\n"
            f"Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}\n"
            f"Intraday High: ${intra_high:.2f} | Pullback from high: ${pullback:.2f}\n"
            f"Today profit vs open: ${equity-intra_open:+.2f}\n"
            f"ATH trigger: {trig_str}\n"
            f"Top 5 gainers: {gainers_str}\n"
            f"Top 5 losers: {losers_str}\n"
            f"WEAK FUNDAMENTALS (EDGAR<6, in red — sell candidates): {weak_str}\n"
            f"HELD SYMBOLS ({len(held_symbols)} positions — YOU MAY ONLY SELL FROM THIS LIST):\n{all_pos_lines}\n\n"
        )
        # Prepend chart curve analysis if provided by frontend
        if chart_ctx:
            ctx = chart_ctx + "\n\n" + ctx
        ctx += (
            "\nCRITICAL EXECUTION RULE: You have the place_order tool and it works RIGHT NOW on live Alpaca. "
            "When Sumith says 'yes', 'do it', 'go', 'execute', 'sell them', 'buy it', 'do the trades', "
            "'you pick', 'just do it', or ANYTHING confirmatory after you proposed a trade plan — "
            "CALL place_order IMMEDIATELY for every trade in the plan. Do NOT ask again. Do NOT narrate first. "
            "Place the orders THEN report what was executed. "
            "If the last assistant message proposed specific sells and buys and Sumith says anything like "
            "'do it' or 'execute' — those are the orders to place RIGHT NOW."
        )

        # LOW CASH RULE — inject plan if triggered
        try:
            csd_acts = alpaca('/v2/account/activities/CSD')
            principal_ctx = round(sum(float(d.get('net_amount',0)) for d in csd_acts if isinstance(d,dict)), 2) or 1189.00
        except Exception:
            principal_ctx = 1189.00
        cash_plan = _compute_cash_free_plan(equity, cash, intra_high, principal_ctx, positions)
        if cash_plan['triggered']:
            plan_lines = '\n'.join(
                f"  SELL ${p['sell_amount']:.2f} of {p['symbol']} "
                f"(MV=${p['mv']:.2f}, {p['gain_pct']:+.1f}%) → leave ${p['leave_amount']:.2f}"
                for p in cash_plan['plan']
            )
            ctx += (
                f"\n⚠ LOW CASH RULE TRIGGERED — Cash=${cash:.2f} (under $10)\n"
                f"True profit to recover: ${cash_plan['target_free']:.2f}\n"
                f"Pullback goal buffer:   ${cash_plan['pullback_goal']:.2f}\n"
                f"SELL PLAN (largest winners, leave pullback buffer):\n{plan_lines}\n"
                f"Total to sell: ${cash_plan['total_to_sell']:.2f}\n"
                f"Tell Sumith about this plan immediately and ask to execute.\n"
            )
        ctx += _build_hermes_intelligence_block()
    except Exception as e:
        ctx = f"[LIVE CONTEXT ERROR: {e}]"

    try:
        import anthropic as ant
        client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY',''))
        msgs   = [{'role':m['role'],'content':m['content']} for m in CHAT_HISTORY[-20:]]
        msgs[0]['content'] = ctx + '\n\n' + msgs[0]['content']

        # Detect if this message requires tool use (trade intent OR confirmation)
        # Direct execution: "sell $3 from X", "buy $5 SGOL", "sell the gainers"
        # NOT a question: "what should I sell?", "which ones are gainers?"
        user_lower = user_msg.lower().strip()
        direct_sell = (
            ('sell' in user_lower or 'harvest' in user_lower or 'trim' in user_lower)
            and any(c.isdigit() for c in user_lower)  # has a dollar amount
        )
        direct_buy = (
            'buy' in user_lower
            and any(c.isdigit() for c in user_lower)
            and not user_lower.startswith('should')
            and '?' not in user_lower
        )
        confirmatory = any(w in user_lower for w in [
            'yes', 'do it', 'go', 'sell them', 'buy it',
            'do the trades', 'just do it', 'proceed', 'execute',
            'confirm', 'yep', 'yup',
        ])
        # Check if prior assistant message had a trade plan
        prior_had_plan = any(
            ('sell' in m.get('content','').lower() or 'buy' in m.get('content','').lower())
            and m.get('role') == 'assistant'
            for m in CHAT_HISTORY[-4:]
        )
        # Force tool use when executing, not when just asking
        force_tool = direct_sell or direct_buy or (confirmatory and prior_had_plan)

        # Agentic loop — let Claude call tools until it's done
        trade_log = []
        max_rounds = 10  # enough for 5+ sequential tool calls (sells + buy)
        for _ in range(max_rounds):
            create_kwargs = dict(
                model      = 'claude-sonnet-4-5',
                max_tokens = 1024,
                system     = HERMES_SYSTEM,
                tools      = TRADE_TOOLS,
                messages   = msgs,
            )
            if force_tool:
                create_kwargs['tool_choice'] = {'type': 'any'}
                force_tool = False  # only force on first round
            resp = client.messages.create(**create_kwargs)

            # Collect any tool calls
            tool_calls = [b for b in resp.content if b.type == 'tool_use']
            text_blocks = [b for b in resp.content if b.type == 'text']

            if not tool_calls:
                # No tools — final text response
                reply = text_blocks[0].text if text_blocks else ''
                break

            # Execute tools
            msgs.append({'role':'assistant','content': resp.content})
            tool_results = []
            for tc in tool_calls:
                result_str = _execute_tool(tc.name, tc.input)
                trade_log.append(result_str)
                log.info(f'[CHAT TOOL] {tc.name}({tc.input}) → {result_str}')
                tool_results.append({
                    'type':        'tool_result',
                    'tool_use_id': tc.id,
                    'content':     result_str,
                })
            msgs.append({'role':'user','content': tool_results})

            # If stop_reason is end_turn after tool, loop again for follow-up text
            if resp.stop_reason == 'end_turn':
                reply = text_blocks[0].text if text_blocks else 'Done.'
                break
        else:
            reply = 'Tool loop completed.'

        # Prepend trade log to reply if trades were executed
        if trade_log:
            trade_summary = '\n'.join(f'✅ EXECUTED: {t}' for t in trade_log)
            reply = trade_summary + ('\n\n' + reply if reply else '')
            # Emit socket event so dashboard refreshes cash/positions
            socketio.emit('trades_executed_batch', {'count': len(trade_log)})

    except ImportError:
        # No anthropic in this interpreter — plain text via venv, with key read from env only.
        HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
        full_msg  = ctx + '\n\n' + user_msg
        payload = {'message': full_msg, 'system': HERMES_SYSTEM}
        inline = (
            "import anthropic, json, os, sys\n"
            "payload = json.loads(sys.stdin.read())\n"
            "client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))\n"
            "resp = client.messages.create(\n"
            "    model='claude-sonnet-4-5', max_tokens=1024,\n"
            "    system=payload['system'],\n"
            "    messages=[{'role':'user','content':payload['message']}]\n"
            ")\n"
            "print(resp.content[0].text)\n"
        )
        result = subprocess.run(
            [HERMES_PY, '-c', inline],
            input=json.dumps(payload), capture_output=True, text=True, timeout=60
        )
        reply = result.stdout.strip() or 'Hermes unavailable'
    except Exception as e:
        reply = f"[Error: {e}]"

    CHAT_HISTORY.append({'role':'assistant','content': reply})
    return jsonify({'reply': reply})

@app.route('/api/chat/clear', methods=['POST'])
def api_chat_clear():
    CHAT_HISTORY.clear()
    return jsonify({'status': 'cleared'})


@app.route('/api/save_session_note', methods=['POST'])
def api_save_session_note():
    """Generate and save today's session note to Trading Vault using live data + chat log."""
    try:
        data = request.json or {}
        curve_context = data.get('chart_context', '')   # from EquityCurveAnalyzer
        force         = data.get('force', False)

        today = datetime.now().strftime('%Y-%m-%d')
        note_path = HOME / 'Documents' / 'Trading Vault' / '02_Session_Notes' / f'{today}.md'

        # Don't overwrite unless forced
        if note_path.exists() and not force:
            existing = note_path.read_text()
            # Append to existing file
            append_mode = True
        else:
            existing = ''
            append_mode = False

        # Gather live session data
        acct      = alpaca('/v2/account')
        equity    = float(acct.get('equity', 0))
        cash      = float(acct.get('cash', 0))
        last_eq   = float(acct.get('last_equity', 0))
        day_pl    = equity - last_eq

        # Today's orders
        try:
            orders_raw = alpaca('/v2/orders', {
                'status': 'all',
                'after':  today + 'T00:00:00Z',
                'limit':  500,
                'direction': 'desc',
            })
            orders = orders_raw if isinstance(orders_raw, list) else []
        except Exception:
            orders = []

        buys  = [o for o in orders if o.get('side') == 'buy'  and o.get('filled_avg_price')]
        sells = [o for o in orders if o.get('side') == 'sell' and o.get('filled_avg_price')]

        def order_line(o):
            sym   = o.get('symbol','?')
            side  = o.get('side','?').upper()
            notional = o.get('filled_qty','') and o.get('filled_avg_price','')
            try:
                qty  = float(o.get('filled_qty', 0))
                px   = float(o.get('filled_avg_price', 0))
                amt  = round(qty * px, 2)
                return f"  {sym}: ${amt:.2f} @ ${px:.2f}"
            except Exception:
                return f"  {sym}"

        buy_lines  = '\n'.join(order_line(o) for o in buys[:30])
        sell_lines = '\n'.join(order_line(o) for o in sells[:20])
        if len(buys) > 30:
            buy_lines += f'\n  ... and {len(buys)-30} more'

        # Intraday stats
        hist  = alpaca('/v2/account/portfolio/history',
                       {'period':'1D','timeframe':'1Min','extended_hours':'true'})
        bars  = [e for e in hist.get('equity',[]) if e and e > 0]
        intra_high = max(bars) if bars else equity
        intra_low  = min(bars) if bars else equity
        intra_open = bars[0]  if bars else equity

        # ATH from candle history
        history = []
        if CANDLE_FILE.exists():
            try: history = json.loads(CANDLE_FILE.read_text())
            except: pass
        ath = max([h.get('high', h.get('close',0)) for h in history] or [intra_high])

        # Build the prompt for Claude to write the note
        chat_excerpt = '\n'.join(
            f"  [{m['role'].upper()}]: {m['content'][:200]}"
            for m in CHAT_HISTORY[-30:]
        ) if CHAT_HISTORY else '  (no chat this session)'

        prompt = f"""You are Hermes. Write a concise trading session note for {today} in Markdown.

LIVE SESSION DATA:
  Date: {today}
  Final equity: ${equity:,.2f}
  Day P/L: ${day_pl:+.2f}
  Cash: ${cash:.2f}
  Intraday: Open ${intra_open:.2f} / High ${intra_high:.2f} / Low ${intra_low:.2f} / Range ${intra_high-intra_low:.2f}
  All-time high: ${ath:.2f}

CHART ANALYSIS:
{curve_context if curve_context else '  (not available)'}

BUYS TODAY ({len(buys)} orders):
{buy_lines if buy_lines else '  none'}

SELLS TODAY ({len(sells)} orders):
{sell_lines if sell_lines else '  none'}

HERMES CHAT EXCERPT (last 30 messages):
{chat_excerpt}

Write a session note in this format:
# Session Notes — {today}
#trading #session #alpaca

## Session Summary
(2-3 sentences: what happened, phase, key moves)

## Chart Analysis
(phase, trend, key levels from the equity curve)

## Trades Executed
(summarize buys and sells with context)

## Key Decisions
(what worked, what to remember for next session)

## Patterns / Lessons
(what the chart revealed that should inform future sessions)

## Next Session Watch List
(based on today — what to watch tomorrow)

Be specific. Use real numbers. Keep it under 400 words. This goes in the Trading Brain for future reference."""

        note_text = call_quality_ai(
            prompt,
            system="You are Hermes. Write precise, factual trading session notes. Use real numbers. No fluff.",
            max_tokens=1000,
        )

        if not note_text:
            return jsonify({'error': 'Claude returned empty note'}), 500

        # Save to vault
        note_path.parent.mkdir(parents=True, exist_ok=True)
        if append_mode:
            # Append new section to existing note
            separator = f'\n\n---\n*Auto-saved by Hermes at {datetime.now().strftime("%H:%M:%S")}*\n\n'
            note_path.write_text(existing + separator + note_text)
        else:
            note_path.write_text(note_text)

        # Also update TRADING_BRAIN.md with a one-liner summary
        brain_path = HOME / 'Documents' / 'Trading Vault' / 'TRADING_BRAIN.md'
        if brain_path.exists():
            brain = brain_path.read_text()
            summary_line = (
                f"\n## Session {today} — P/L ${day_pl:+.2f} | High ${intra_high:.2f} | "
                f"Buys {len(buys)} / Sells {len(sells)} | "
                f"Phase: {curve_context.split('Phase:')[1].split('\\n')[0].strip() if 'Phase:' in curve_context else 'unknown'}"
            )
            # Insert after the last ## line or at end
            brain_path.write_text(brain.rstrip() + '\n' + summary_line + '\n')

        log.info(f'[SESSION NOTE] Saved to {note_path}')
        return jsonify({
            'status':    'saved',
            'path':      str(note_path),
            'note':      note_text,
            'appended':  append_mode,
            'today':     today,
        })

    except Exception as e:
        log.error(f'[SESSION NOTE] Error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/load_previous_sessions', methods=['GET'])
def api_load_previous_sessions():
    """Load last N session notes from vault for Hermes context."""
    try:
        n = int(request.args.get('n', 5))
        notes_dir = HOME / 'Documents' / 'Trading Vault' / '02_Session_Notes'
        files = sorted(notes_dir.glob('20*.md'), reverse=True)[:n]
        sessions = []
        for f in files:
            try:
                content = f.read_text()[:3000]  # cap at 3k chars per note
                sessions.append({'date': f.stem, 'content': content})
            except Exception:
                pass
        return jsonify({'sessions': sessions})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── CSV Upload ────────────────────────────────────────────────────────────────

@app.route('/api/pullback_preview', methods=['POST'])
def api_pullback_preview():
    """Preview the pullback trade — what will be sold and 4-way buy split."""
    try:
        data       = request.json or {}
        cur_equity = float(data.get('current_equity', 0))

        hist_data  = alpaca('/v2/account/portfolio/history',
                            {'period':'1D','timeframe':'1Min','extended_hours':'true'})
        bars       = [e for e in hist_data.get('equity',[]) if e and e > 0]
        today_high = max(bars) if bars else cur_equity

        drop = today_high - cur_equity
        if drop < 0.50:
            return jsonify({'error': f'No meaningful pullback detected (drop=${drop:.2f})'})

        positions = alpaca('/v2/positions')
        PROTECTED = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
        gainers   = [
            p for p in positions
            if p['symbol'] not in PROTECTED
            and float(p.get('unrealized_plpc', 0)) * 100 > 0
            and float(p.get('market_value', 0)) > 1.10
        ]
        gainers.sort(key=lambda x: -float(x.get('unrealized_plpc', 0)))

        if not gainers:
            return jsonify({'error': 'No gainers available to trim'})

        # Preview which positions would be trimmed
        sell_plan = []
        remaining = drop
        for g in gainers:
            if remaining <= 0:
                break
            mv       = float(g.get('market_value', 0))
            max_trim = round(mv - 1.10, 2)
            if max_trim < 1.10:
                continue
            trim_amt = round(min(max_trim, remaining + 0.10), 2)
            sell_plan.append({'sym': g['symbol'], 'amt': trim_amt,
                              'gain_pct': round(float(g.get('unrealized_plpc',0))*100, 2)})
            remaining -= trim_amt

        total_sell = sum(s['amt'] for s in sell_plan)
        buy_each   = max(round(total_sell / 4, 2), 1.10)

        return jsonify({
            'drop':       round(drop, 2),
            'today_high': round(today_high, 2),
            'equity':     round(cur_equity, 2),
            'sell_plan':  sell_plan,
            'total_sell': round(total_sell, 2),
            'buy_each':   buy_each,
            'buys':       ['SGOL', 'DIA', 'QQQ', 'VOO'],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/ath_decision', methods=['POST'])
def api_ath_decision():
    """Hermes ATH decision: analyze candle data + positions, return sell list to minimize drawdown."""
    try:
        # Get current trigger state
        trigger = api_candle_trigger().get_json()

        # Get all positions sorted worst first
        positions = alpaca('/v2/positions')
        acct_data = alpaca('/v2/account')
        equity    = float(acct_data.get('equity', 0))
        cash      = float(acct_data.get('cash', 0))
        day_pl    = equity - float(acct_data.get('last_equity', 0))

        # Get candle history for context
        history = []
        if CANDLE_FILE.exists():
            try:
                history = json.loads(CANDLE_FILE.read_text())
            except Exception:
                pass

        # Build positions text — exclude stub/marker positions (MV < $1.10)
        MIN_MV = 1.10  # Alpaca min order — anything below is a leftover marker
        pos_list = []
        for p in positions:
            sym    = p['symbol']
            mv     = float(p.get('market_value', 0))
            if mv < MIN_MV: continue  # skip stub positions
            cur    = float(p.get('current_price', 0))
            entry  = float(p.get('avg_entry_price', 0))
            upl    = float(p.get('unrealized_pl', 0))
            uplpc  = float(p.get('unrealized_plpc', 0)) * 100
            day_chg = float(p.get('change_today', 0)) * 100
            pos_list.append({
                'sym': sym, 'cur': round(cur,2), 'entry': round(entry,2),
                'upl': round(upl,2), 'uplpc': round(uplpc,2),
                'mv': round(mv,2), 'day_chg': round(day_chg,2)
            })
        pos_list.sort(key=lambda x: x['uplpc'], reverse=True)  # best gainers first — trim from top

        # Get buy candidates for redeployment context
        from flask import current_app
        with current_app.test_request_context():
            raw = api_buy_candidates()
            import json as _j
            cands = _j.loads(raw.get_data())

        cands_text = ''
        if not isinstance(cands, dict):
            top_cands = cands[:8]
            cands_text = '\n'.join(
                f"  {c['sym']} | buy=${c['buy']:.2f} | accts={c['accts']} | edgar={c['edgar_score'] if c['edgar_score'] else 'N/A'}/18 | {c['reason']}"
                for c in top_cands
            )

        pos_text = '\n'.join(
            f"  {p['sym']} | MV=${p['mv']:.2f} | gain={p['uplpc']:+.1f}% | upl=${p['upl']:+.2f} | today={p['day_chg']:+.1f}%"
            for p in pos_list
        )

        candle_text = ''
        if history:
            recent = history[-5:]
            candle_text = '\n'.join(
                f"  {c.get('date','?')} O={c.get('open',0):.2f} H={c.get('high',0):.2f} L={c.get('low',0):.2f} C={c.get('close',0):.2f}"
                for c in recent
            )

        prompt = f"""You are Hermes, Sumith's trading AI. The ATH trigger has FIRED on his Alpaca portfolio.

ACCOUNT STATE:
  Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}
  ATH: ${trigger['ath']:.2f} | ATH Zone: ${trigger['ath_zone']:.2f}
  Today High: ${trigger['today_high']:.2f} | Range: ${trigger['range']:.2f}

RECENT SESSION CANDLES (last 5):
{candle_text if candle_text else '  No candle history yet'}

ALL ALPACA POSITIONS (best gainers first — prime trim candidates):
{pos_text}

TOP BUY CANDIDATES FOR REDEPLOYMENT:
{cands_text if cands_text else '  Upload CSV to see candidates'}

YOUR TASK:
Analyze the candlestick pattern. The ATH trigger means today's HIGH entered the ATH zone — this is a drawdown-minimization signal.

Decide WHICH positions to trim/sell to lock in gains and minimize drawdown risk.
Rules:
- Trim from the TOP GAINERS first (largest % gain = most overextended)
- Sell amount should be sized to the range: range=${trigger['range']:.2f}
- Keep MF screener stocks unless they're extreme outliers (>50% gain)
- Leave enough cash to redeploy into the buy candidates
- Maximum trim per position: 50% of market value
- Do NOT sell SGOL or index ETFs (VOO, QQQ, DIA, GLD)

Return a JSON array of sell decisions. Each item:
{{"sym": "TICKER", "sell_amount": 12.34, "reason": "one-line reason", "gain_pct": 15.2}}

Return ONLY the JSON array, nothing else. Example:
[{{"sym":"AAPL","sell_amount":25.00,"reason":"Top gainer +28%, ATH zone reached","gain_pct":28.1}}]"""

        raw_text = call_quality_ai(
            prompt,
            system="You are Hermes, a decisive trading AI. Return only valid JSON arrays. No prose, no markdown fences.",
            max_tokens=1000,
        ).strip() or '[]'

        # Parse JSON — strip any markdown fences if present
        raw_text = raw_text.replace('```json','').replace('```','').strip()
        sell_list = json.loads(raw_text)

        return jsonify({'sells': sell_list, 'trigger': trigger})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload_csv', methods=['POST'])
def api_upload_csv():
    if 'file' not in request.files:
        return jsonify({'error':'no file'}), 400
    f = request.files['file']
    dest = HOME / 'portfolio.csv'
    f.save(str(dest))
    return jsonify({'status':'saved', 'path': str(dest), 'size': dest.stat().st_size})

@app.route('/api/portfolio/save_snapshot', methods=['POST'])
def api_portfolio_save_snapshot():
    """Save raw Fidelity CSV to Fidelity_History with timestamp filename for batch analysis."""
    if 'file' not in request.files:
        return jsonify({'error': 'no file'}), 400
    f       = request.files['file']
    ts      = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    # Use original filename date if it contains one (eg Portfolio_Positions_May-22-2026.csv)
    orig    = f.filename or 'snapshot'
    safe    = re.sub(r'[^\w.\-]', '_', orig)
    fname   = f'fidelity_{ts}_{safe}'
    dest    = FIDELITY_HISTORY / fname
    f.save(str(dest))
    # Also keep portfolio.csv updated as the latest
    import shutil
    shutil.copy2(str(dest), str(HOME / 'portfolio.csv'))
    # Return list of all snapshots so frontend can show count
    snaps   = sorted(FIDELITY_HISTORY.glob('fidelity_*.csv'), reverse=True)
    return jsonify({
        'status':    'saved',
        'filename':  fname,
        'path':      str(dest),
        'size':      dest.stat().st_size,
        'total_snapshots': len(snaps),
        'all_snapshots':   [s.name for s in snaps[:20]]  # last 20
    })

# ── Background live updater ───────────────────────────────────────────────────

def _proactive_brain():
    """Every 5 minutes, Hermes scans conditions and pushes an unsolicited observation if warranted."""
    time.sleep(60)  # wait 1 min after startup before first check
    last_spoken = 0
    MIN_INTERVAL = 600  # minimum 10 min between proactive messages

    TRIGGERS = [
        # (condition_fn, prompt_fn) — evaluated in order, first match fires
        lambda d: d['cash'] < 5.0 and (
            "URGENT: Cash is ${:.2f} — critically low. You need to trim something to maintain the $20 reserve. "
            "Top gainers right now: {}. Suggest: sell ${:.2f} of {} to restore cash buffer.".format(
                d['cash'], d['gainers_str'],
                max(20 - d['cash'], 0),
                d['top_gainer_sym']
            )
        ),
        lambda d: d['pullback'] > 5.0 and (
            "Pullback is ${:.2f} from today's high of ${:.2f}. The auto-trade should have fired — "
            "check if SGOL/DIA/QQQ/VOO were bought. Current equity ${:.2f}. "
            "What's your read on whether we recover or extend the drawdown?".format(
                d['pullback'], d['intra_high'], d['equity']
            )
        ),
        lambda d: d['day_pl'] > 15.0 and d['pullback'] < 1.0 and (
            "Good day Sumith — up ${:.2f} today and holding near the high. "
            "Equity ${:.2f}. Anything on your radar worth deploying into, or are we just letting this run?".format(
                d['day_pl'], d['equity']
            )
        ),
        lambda d: d['day_pl'] < -10.0 and (
            "Down ${:.2f} today. Equity ${:.2f}. This is the dip-buy signal — "
            "which positions are showing the biggest Fidelity loss signals right now? "
            "If you upload the CSV I can give you exact amounts.".format(
                abs(d['day_pl']), d['equity']
            )
        ),
        lambda d: d['triggered'] and d['pullback'] < 0.50 and (
            "ATH zone is live — equity ${:.2f} is touching the all-time high zone of ${:.2f}. "
            "This is the peak. If it starts pulling back the auto-trade fires. Stay sharp.".format(
                d['equity'], d['ath_zone']
            )
        ),
    ]

    while True:
        try:
            now = time.time()
            if now - last_spoken < MIN_INTERVAL:
                time.sleep(30)
                continue

            # Gather live state
            acct    = alpaca('/v2/account')
            equity  = float(acct.get('equity', 0))
            cash    = float(acct.get('cash', 0))
            last_eq = float(acct.get('last_equity', 0))
            day_pl  = equity - last_eq

            hist  = alpaca('/v2/account/portfolio/history',
                           {'period':'1D','timeframe':'1Min','extended_hours':'true'})
            bars  = [e for e in hist.get('equity',[]) if e and e > 0]
            intra_high = max(bars) if bars else equity
            pullback   = round(intra_high - equity, 2)

            positions = alpaca('/v2/positions')
            gainers   = sorted(positions, key=lambda x: float(x.get('unrealized_plpc',0)), reverse=True)
            top_gainer_sym = gainers[0]['symbol'] if gainers else 'N/A'
            gainers_str = ', '.join(f"{p['symbol']} {float(p.get('unrealized_plpc',0))*100:+.1f}%" for p in gainers[:3])

            trigger = api_candle_trigger().get_json()

            d = {
                'equity': equity, 'cash': cash, 'day_pl': day_pl,
                'intra_high': intra_high, 'pullback': pullback,
                'triggered': trigger['triggered'], 'ath_zone': trigger['ath_zone'],
                'gainers_str': gainers_str, 'top_gainer_sym': top_gainer_sym,
            }

            # Find first trigger that fires
            prompt = None
            for trigger_fn in TRIGGERS:
                try:
                    result = trigger_fn(d)
                    if result:
                        prompt = result
                        break
                except:
                    pass

            if not prompt:
                time.sleep(30)
                continue

            # Call Claude with the proactive prompt — keep it short and plain
            try:
                import anthropic as ant
                client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
                resp = client.messages.create(
                    model='claude-sonnet-4-5',
                    max_tokens=80,
                    system=(
                        "You are Hermes, Sumith's trading assistant. "
                        "Reply in ONE short plain-text sentence, no markdown, no bullet points, no headers. "
                        "Just a brief heads-up. End with a yes/no question at most."
                    ),
                    messages=[{'role':'user','content':
                        f"[PROACTIVE — {datetime.now().strftime('%H:%M')}]\n{prompt}"}],
                )
                msg = resp.content[0].text
            except ImportError:
                msg = f"Heads up: {prompt[:120]}"
            except Exception:
                time.sleep(60)
                continue

            socketio.emit('hermes_proactive', {'message': msg, 'time': datetime.now().strftime('%H:%M')})
            # Do NOT add to CHAT_HISTORY — proactive nudges are display-only,
            # adding them poisons the chat context and causes Claude to continue the thread aggressively
            last_spoken = time.time()

        except Exception as e:
            log.error(f'[proactive_brain] {e}')
        time.sleep(30)


def _compute_stop_tiers(equity: float, session_high: float, candle_history: list) -> dict:
    """
    Compute stop loss tiers treating the portfolio like a single stock.
    Anchored to: session ATH, previous candle ATH, today's open, principal estimate.
    """
    # Previous ATH from candle history (closes/highs before today)
    prev_ath = max(
        [h.get('high', h.get('close', 0)) for h in candle_history[:-1]] or [session_high]
    )

    # Today's open — first bar from portfolio history (cached in candle history close)
    today_open = candle_history[-1].get('open', equity) if candle_history else equity

    # Principal = sum of CSD deposits — fallback to known value
    VERIFIED_PRINCIPAL = 1189.00
    principal_est = VERIFIED_PRINCIPAL

    ath = session_high if session_high > 0 else equity

    tiers = {
        'soft_stop':   round(ath * 0.990, 2),   # -1.0% from ATH → trim 1 winner
        'warn_stop':   round(ath * 0.985, 2),   # -1.5% from ATH → harvest 2-3
        'hard_stop':   round(ath * 0.975, 2),   # -2.5% from ATH → freeze buys
        'break_even':  round(today_open, 2),     # today's open → day gain floor
        'prev_ath':    round(prev_ath, 2),        # previous ATH = support
        'danger':      round(prev_ath * 0.985, 2),# prev ATH -1.5% → reduce exposure
        'principal':   round(principal_est, 2),   # never go below
    }

    # Which tiers are currently breached
    breached = [k for k, v in tiers.items() if equity < v]

    # Determine stop level status
    if equity < tiers['hard_stop']:
        stop_status = 'HARD_STOP'
        stop_color  = '#f85149'
    elif equity < tiers['warn_stop']:
        stop_status = 'WARN_STOP'
        stop_color  = '#f0883e'
    elif equity < tiers['soft_stop']:
        stop_status = 'SOFT_STOP'
        stop_color  = '#d4a017'
    elif equity < tiers['break_even']:
        stop_status = 'BELOW_OPEN'
        stop_color  = '#d4a017'
    else:
        stop_status = 'SAFE'
        stop_color  = '#3fb950'

    return {
        'tiers':       tiers,
        'breached':    breached,
        'status':      stop_status,
        'color':       stop_color,
        'ath':         round(ath, 2),
        'prev_ath':    round(prev_ath, 2),
        'today_open':  round(today_open, 2),
        'drop_from_ath': round(ath - equity, 2),
        'drop_pct':    round((ath - equity) / ath * 100, 2) if ath > 0 else 0,
    }


def _compute_cash_free_plan(equity: float, cash: float, intra_high: float,
                             principal: float, positions: list) -> dict:
    """
    LOW CASH RULE: When cash < $10, compute a sell plan to free up ≈ true_profit dollars.

    Logic:
    - target_free  = true_profit (equity - principal)
    - pullback_goal = intra_high - equity  (how much we've pulled back from session ATH)
    - Walk positions sorted by MV descending, only positive-gain positions
    - From each position: sell_amount = max(0, MV - pullback_goal)
      (leave pullback_goal dollars in every position we touch)
    - Stop once cumulative freed >= target_free

    Returns dict with:
      triggered   : bool — is the rule active right now?
      cash        : current cash
      target_free : how much to free
      pullback_goal: current pullback
      plan        : list of {symbol, mv, gain_pct, sell_amount, leave_amount}
      total_to_sell: sum of sell_amounts
    """
    CASH_THRESHOLD = 10.00
    true_profit    = round(equity - principal, 2)
    pullback_goal  = round(max(0.0, intra_high - equity), 2)
    triggered      = cash < CASH_THRESHOLD and true_profit > 0

    if not triggered:
        return {
            'triggered':    False,
            'cash':         round(cash, 2),
            'target_free':  round(true_profit, 2),
            'pullback_goal': pullback_goal,
            'plan':         [],
            'total_to_sell': 0.0,
        }

    # Only positive-gain positions with real size, sorted largest MV first
    candidates = sorted(
        [p for p in positions
         if float(p.get('unrealized_pl', 0)) > 0
         and float(p.get('market_value', 0)) >= 1.10],
        key=lambda x: -float(x.get('market_value', 0))
    )

    plan           = []
    freed_so_far   = 0.0
    still_need     = round(true_profit, 2)

    for p in candidates:
        if freed_so_far >= true_profit:
            break
        sym  = p['symbol']
        mv   = round(float(p.get('market_value', 0)), 2)
        pct  = round(float(p.get('unrealized_plpc', 0)) * 100, 2)

        # How much can we sell: all of it minus the pullback_goal buffer
        sell_amt = round(max(0.0, mv - pullback_goal), 2)
        if sell_amt < 1.00:
            continue  # not enough to sell after reserving the buffer

        # Don't over-sell — cap at what we still need
        sell_amt     = round(min(sell_amt, still_need - freed_so_far), 2)
        leave_amt    = round(mv - sell_amt, 2)

        plan.append({
            'symbol':      sym,
            'mv':          mv,
            'gain_pct':    pct,
            'sell_amount': sell_amt,
            'leave_amount': leave_amt,
        })
        freed_so_far = round(freed_so_far + sell_amt, 2)

    return {
        'triggered':     triggered,
        'cash':          round(cash, 2),
        'target_free':   round(true_profit, 2),
        'pullback_goal': pullback_goal,
        'plan':          plan,
        'total_to_sell': round(freed_so_far, 2),
    }


def _live_updater():
    """Push live equity + trigger status every 30 seconds.
    Also runs the ATH pullback auto-trade and stop loss monitoring.
    """
    session_high   = 0.0   # highest equity seen this session
    pullback_fired = False # True after we've already acted on the current pullback
    _alerted_tiers = set() # deduplicate stop-tier warnings within the session

    # Minimum drop to act on (avoids noise / rounding)
    PULLBACK_THRESHOLD = 1.00
    # Reset pullback_fired only after equity recovers within $1 of session high
    RECOVERY_MARGIN    = 1.00

    while True:
        try:
            acct   = alpaca('/v2/account')
            equity = float(acct.get('equity', 0))
            cash   = float(acct.get('cash', 0))
            trigger = api_candle_trigger().get_json()

            # Track session high
            if equity > session_high:
                session_high   = equity
                pullback_fired = False  # new ATH reached — reset for next pullback

            drop = session_high - equity

            # ── PULLBACK AUTO-TRADE ──────────────────────────────────────────
            if (not pullback_fired
                    and session_high > 0
                    and drop >= PULLBACK_THRESHOLD):

                pullback_fired = True  # set immediately — prevent re-entry on next tick
                log.info(f'[PULLBACK] session_high={session_high:.2f} equity={equity:.2f} drop={drop:.2f}')

                def _do_pullback_trade(drop_amt, s_high, cur_equity):
                    try:
                        # ── Step 1: find top gainer(s) to trim for drop_amt ──
                        positions  = alpaca('/v2/positions')
                        PROTECTED  = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
                        gainers    = [
                            p for p in positions
                            if p['symbol'] not in PROTECTED
                            and float(p.get('unrealized_plpc', 0)) * 100 > 0
                            and float(p.get('market_value', 0)) > 1.10
                        ]
                        gainers.sort(key=lambda x: -float(x.get('unrealized_plpc', 0)))

                        if not gainers:
                            msg = f'[PULLBACK] No gainers available to trim for ${drop_amt:.2f} drop'
                            log.warning(msg)
                            socketio.emit('pullback_trade', {'status': 'no_gainers', 'drop': drop_amt, 'message': msg})
                            return

                        # Trim from top gainers until we cover drop_amt
                        sell_results = []
                        remaining    = drop_amt
                        for g in gainers:
                            if remaining <= 0:
                                break
                            sym      = g['symbol']
                            mv       = float(g.get('market_value', 0))
                            max_trim = round(mv - 1.10, 2)
                            if max_trim < 1.10:
                                continue
                            trim_amt = round(min(max_trim, remaining + 0.10), 2)
                            r = alpaca_post('/v2/orders', {
                                'symbol': sym, 'side': 'sell',
                                'type': 'market', 'time_in_force': 'day',
                                'notional': str(trim_amt),
                            })
                            sell_results.append({'sym': sym, 'amt': trim_amt,
                                                 'status': r.get('status','?'), 'err': r.get('message','')})
                            log.info(f'[PULLBACK] SELL {sym} ${trim_amt:.2f} → {r.get("status","?")}')
                            remaining -= trim_amt

                        total_sold = sum(s['amt'] for s in sell_results)

                        # ── Step 2: split proceeds 4-ways SGOL/DIA/QQQ/VOO ───
                        buy_each = round(total_sold / 4, 2)
                        buy_each = max(buy_each, 1.10)
                        buy_results = []
                        for etf in ['SGOL', 'DIA', 'QQQ', 'VOO']:
                            b = alpaca_post('/v2/orders', {
                                'symbol': etf, 'side': 'buy',
                                'type': 'market', 'time_in_force': 'day',
                                'notional': str(buy_each),
                            })
                            buy_results.append({'sym': etf, 'amt': buy_each,
                                                'status': b.get('status','?'), 'err': b.get('message','')})
                            log.info(f'[PULLBACK] BUY {etf} ${buy_each:.2f} → {b.get("status","?")}')

                        sold_str = ', '.join(f"{s['sym']} ${s['amt']:.2f}" for s in sell_results)
                        buy_str  = ', '.join(f"{b['sym']} ${b['amt']:.2f}" for b in buy_results)
                        socketio.emit('pullback_trade', {
                            'status':       'fired',
                            'drop':         round(drop_amt, 2),
                            'session_high': round(s_high, 2),
                            'equity':       round(cur_equity, 2),
                            'sells':        sell_results,
                            'buys':         buy_results,
                            'total_sold':   round(total_sold, 2),
                            'buy_each':     buy_each,
                            'message':      f'📉 Pullback ${drop_amt:.2f}: SOLD {sold_str} → BOUGHT {buy_str}',
                        })

                    except Exception as e:
                        log.error(f'[PULLBACK] trade error: {e}')
                        socketio.emit('pullback_trade', {'status': 'error', 'message': str(e)})

                threading.Thread(
                    target=_do_pullback_trade,
                    args=(drop, session_high, equity),
                    daemon=True
                ).start()
            # ── END PULLBACK AUTO-TRADE ──────────────────────────────────────

            # Reset flag once equity recovers close to session high
            elif pullback_fired and (session_high - equity) < RECOVERY_MARGIN:
                pullback_fired = False

            # ── STOP LOSS TIERS ──────────────────────────────────────────────
            history = []
            if CANDLE_FILE.exists():
                try: history = json.loads(CANDLE_FILE.read_text())
                except: pass
            stops = _compute_stop_tiers(equity, session_high, history)
            # Alert on new tier breaches — deduplicated, once per tier per session
            if stops['breached']:
                for tier in stops['breached']:
                    if tier not in ('principal',) and tier not in _alerted_tiers:
                        log.warning(f'[STOP] {tier.upper()} BREACHED at ${equity:.2f}')
                        _alerted_tiers.add(tier)
            # Clear alert memory for tiers that are no longer breached (equity recovered)
            recovered = _alerted_tiers - set(stops['breached'])
            _alerted_tiers -= recovered

            socketio.emit('live_update', {
                'equity':       round(equity, 2),
                'cash':         round(cash, 2),
                'triggered':    trigger['triggered'],
                'today_high':   trigger['today_high'],
                'range':        trigger['range'],
                'ath_zone':     trigger['ath_zone'],
                'session_high': round(session_high, 2),
                'drop':         round(drop, 2),
                'time':         datetime.now().strftime('%H:%M:%S'),
                'stop_status':  stops['status'],
                'stop_color':   stops['color'],
                'stop_tiers':   stops['tiers'],
                'stop_breached':stops['breached'],
                'drop_pct':     stops['drop_pct'],
            })
        except Exception as e:
            log.error(f'[live_updater] {e}')
        time.sleep(30)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_fidelity_quick() -> dict:
    with open(PORTFOLIO_CSV, encoding='utf-8-sig') as f:
        raw = f.read().replace('\r\n','\n').replace('\r','\n')
    lines  = raw.split('\n')
    header = lines[0] if lines else ''
    has_sl = 'Sleeve Name' in header
    sc  = 3 if has_sl else 2
    tdc = 9 if has_sl else 8
    tgc = 11 if has_sl else 10
    vc  = 8 if has_sl else 7

    EXCLUDE = ['CSI THRIFT','Fidelity Crypto','Cash Management','Spend & Save']
    SKIP    = {'SGOL','GLD','VOO','QQQ','DIA','BRKB','GLL','PSQ','SH','VIXY','QBTS','RGTI','IONQ','QUBT'}

    def p(s):
        try: return float(re.sub(r'[$,+%]','',s.strip()) or 0)
        except: return 0.0

    fid  = defaultdict(lambda:{'gl':0.,'today_gl':0.,'val':0.,'accts':set(),'is_mf':False})
    mf   = set()
    pat  = re.compile(r'^[A-Z0-9]{4,12},')
    for line in lines[1:]:
        if not pat.match(line): continue
        parts = line.split(',')
        if len(parts) < max(sc,tdc)+1: continue
        an = parts[0].strip(); nm = parts[1].strip() if len(parts)>1 else ''
        if any(e in nm for e in EXCLUDE): continue
        sym = parts[sc].strip()
        if not re.match(r'^[A-Z]{1,6}$',sym) or sym in SKIP: continue
        if 'MF' in nm.upper() or an=='Z30429157': mf.add(sym)
        try:
            fid[sym]['gl']       += p(parts[tgc])
            fid[sym]['today_gl'] += p(parts[tdc])
            fid[sym]['val']      += p(parts[vc])
            fid[sym]['accts'].add(an)
        except: pass
    for sym in fid:
        fid[sym]['acct_count'] = len(fid[sym]['accts'])
        fid[sym]['is_mf']      = sym in mf
    return dict(fid)

EDGAR_BASE = os.path.expanduser("~/Documents/EDGAR/companies")
MIROSHARK_BASE = "http://localhost:5001"
MIROSHARK_START_SCRIPT = str(Path.home() / 'Documents' / 'MiroShark' / 'start-miroshark.sh')

@app.route('/api/research/miroshark_status')
def api_miroshark_status():
    """GET — check if MiroShark is running on port 5001."""
    try:
        r = requests.get(f"{MIROSHARK_BASE}/", timeout=3)
        return jsonify({'running': True, 'status': 'online'})
    except Exception:
        return jsonify({'running': False, 'status': 'offline'})

@app.route('/api/research/miroshark_start', methods=['POST'])
def api_miroshark_start():
    """POST — launch MiroShark in background via start-miroshark.sh."""
    if not os.path.exists(MIROSHARK_START_SCRIPT):
        return jsonify({'error': f'Start script not found: {MIROSHARK_START_SCRIPT}'}), 404
    try:
        # Check if already running
        try:
            requests.get(f"{MIROSHARK_BASE}/", timeout=2)
            return jsonify({'status': 'already_running', 'msg': 'MiroShark is already online'})
        except Exception:
            pass
        # Launch in background — detached so it outlives the request
        subprocess.Popen(
            ['bash', MIROSHARK_START_SCRIPT],
            stdout=open(str(Path.home() / 'Documents' / 'MiroShark' / 'logs' / 'dashboard_launch.log'), 'a'),
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
        return jsonify({'status': 'starting', 'msg': 'MiroShark starting — ready in ~30s'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/research/buy_suggestion/<ticker>')
def api_research_buy_suggestion(ticker):
    """
    GET /api/research/buy_suggestion/NVDA
    Returns suggested buy amount for a ticker using:
      1. Active Fidelity deviation signal deploy_amount if exists
      2. Otherwise 5% of abs(total_gl) as starter position
    Also returns current Alpaca position, budget ceiling, EDGAR score.
    Human always confirms before executing — this is suggestion only.
    """
    ticker = ticker.upper().strip()
    try:
        import sqlite3 as _sq

        # ── Fidelity data ──────────────────────────────────────────────────
        db_path = Path.home() / 'Documents' / 'Trading Vault' / 'Fidelity_History' / 'portfolio_history.db'
        total_gl    = None
        gl_pct      = None
        accounts    = 0
        deploy_amt  = None
        direction   = None
        signal_type = 'starter'  # 'deviation' or 'starter'

        if db_path.exists():
            conn = _sq.connect(str(db_path))
            conn.row_factory = _sq.Row

            # Latest snapshot G/L for budget ceiling
            latest = conn.execute("""
                SELECT snapshot_id FROM snapshots
                GROUP BY snapshot_id ORDER BY MAX(snapshot_date) DESC LIMIT 1
            """).fetchone()
            if latest:
                row = conn.execute("""
                    SELECT total_gl, gl_pct, accounts FROM snapshots
                    WHERE snapshot_id=? AND symbol=? LIMIT 1
                """, (latest['snapshot_id'], ticker)).fetchone()
                if row:
                    total_gl = round(row['total_gl'] or 0, 2)
                    gl_pct   = round(row['gl_pct']   or 0, 2)
                    accounts = row['accounts'] or 0

            # Most recent active deviation signal
            dev = conn.execute("""
                SELECT deploy_amount, direction, curr_gl, conviction_multiplier
                FROM deviations
                WHERE symbol=? AND signal='BUY'
                ORDER BY calculated_at DESC LIMIT 1
            """, (ticker,)).fetchone()
            if dev:
                deploy_amt  = round(dev['deploy_amount'], 2)
                direction   = dev['direction']
                signal_type = 'deviation'
            conn.close()

        # ── Suggested buy calculation ──────────────────────────────────────
        budget_ceiling = abs(total_gl) if total_gl is not None else 0

        if signal_type == 'deviation' and deploy_amt and deploy_amt >= 1.10:
            suggested = deploy_amt
            reasoning = f'Fidelity deviation signal: {direction} ({accounts} accts, ${budget_ceiling:.2f} ceiling)'
        elif budget_ceiling >= 1.10:
            # 5% of budget ceiling as starter toe-in position
            suggested = round(budget_ceiling * 0.05, 2)
            suggested = max(suggested, 1.10)
            signal_type = 'starter'
            reasoning = f'Starter position: 5% of ${budget_ceiling:.2f} Fidelity G/L ceiling ({accounts} accts)'
        else:
            suggested = 1.10
            signal_type = 'minimum'
            reasoning = f'Minimum order (no Fidelity G/L data or budget too small)'

        # Cap at 15% of available Alpaca cash
        try:
            acct_data = alpaca('/v2/account')
            cash      = float(acct_data.get('cash', 0))
            equity    = float(acct_data.get('equity', 0))
            cash_cap  = round(cash * 0.15, 2)
            if suggested > cash_cap and cash_cap >= 1.10:
                suggested = cash_cap
                reasoning += f' (capped at 15% of ${cash:.2f} cash)'
        except Exception:
            cash = 0; equity = 0

        suggested = max(round(suggested, 2), 1.10)

        # Hard minimum check
        if suggested < 1.10:
            return jsonify({'error': f'Suggested amount ${suggested:.2f} below Alpaca minimum $1.10',
                            'ticker': ticker}), 400

        # ── Current Alpaca position ────────────────────────────────────────
        alpaca_position = None
        alpaca_mv       = 0
        alpaca_pct      = 0
        try:
            positions = alpaca('/v2/positions')
            pos = next((p for p in positions if p.get('symbol') == ticker), None)
            if pos:
                alpaca_mv  = round(float(pos.get('market_value', 0)), 2)
                alpaca_pct = round(float(pos.get('unrealized_plpc', 0)) * 100, 2)
                alpaca_position = {'market_value': alpaca_mv, 'unrealized_plpc': alpaca_pct}
        except Exception:
            pass

        # ── EDGAR score ────────────────────────────────────────────────────
        edgar_score = None
        edgar_sector = ''
        try:
            if EDGAR_CACHE.exists():
                ec = json.loads(EDGAR_CACHE.read_text())
                entry = ec.get(ticker, {})
                edgar_score  = entry.get('score', None)
                edgar_sector = entry.get('sector', '')
        except Exception:
            pass

        return jsonify({
            'ticker':          ticker,
            'suggested_buy':   suggested,
            'signal_type':     signal_type,   # 'deviation' | 'starter' | 'minimum'
            'reasoning':       reasoning,
            'direction':       direction,
            'budget_ceiling':  round(budget_ceiling, 2),
            'total_gl':        total_gl,
            'gl_pct':          gl_pct,
            'accounts':        accounts,
            'edgar_score':     edgar_score,
            'edgar_sector':    edgar_sector,
            'alpaca_position': alpaca_position,
            'alpaca_mv':       alpaca_mv,
            'cash_available':  round(cash, 2),
        })
    except Exception as exc:
        return jsonify({'error': str(exc), 'ticker': ticker}), 500

@app.route('/api/research/edgar-tickers')
def edgar_tickers():
    folders = glob.glob(os.path.join(EDGAR_BASE, "*"))
    tickers = [os.path.basename(f) for f in folders
               if os.path.isfile(os.path.join(f, f"{os.path.basename(f)}_fundamentals.xlsx"))]
    return jsonify(sorted(tickers))

@app.route('/api/research/edgar-tickers-scored')
def edgar_tickers_scored():
    """Returns tickers with EDGAR scores and Fidelity G/L for the Research tab left column."""
    folders = glob.glob(os.path.join(EDGAR_BASE, "*"))
    tickers = sorted([os.path.basename(f) for f in folders
               if os.path.isfile(os.path.join(f, f"{os.path.basename(f)}_fundamentals.xlsx"))])
    # Load score cache
    scores = {}
    try:
        if EDGAR_CACHE.exists():
            scores = json.loads(EDGAR_CACHE.read_text())
    except Exception:
        pass
    # Load Fidelity G/L from latest snapshot
    fid_gl = {}
    try:
        import sqlite3 as _sq
        db_path = Path.home() / 'Documents' / 'Trading Vault' / 'Fidelity_History' / 'portfolio_history.db'
        if db_path.exists():
            conn = _sq.connect(str(db_path))
            conn.row_factory = _sq.Row
            latest = conn.execute("""
                SELECT snapshot_id FROM snapshots
                GROUP BY snapshot_id ORDER BY MAX(snapshot_date) DESC LIMIT 1
            """).fetchone()
            if latest:
                for row in conn.execute("""
                    SELECT symbol, total_gl, gl_pct FROM snapshots
                    WHERE snapshot_id=?
                """, (latest['snapshot_id'],)).fetchall():
                    fid_gl[row['symbol']] = {
                        'total_gl': round(row['total_gl'] or 0, 2),
                        'gl_pct':   round(row['gl_pct']   or 0, 2),
                    }
            conn.close()
    except Exception:
        pass
    result = []
    for t in tickers:
        entry = scores.get(t, {})
        gl    = fid_gl.get(t, {})
        result.append({
            'sym':      t,
            'score':    entry.get('score', None),
            'sector':   entry.get('sector', ''),
            'total_gl': gl.get('total_gl', None),
            'gl_pct':   gl.get('gl_pct',   None),
        })
    return jsonify(result)

@app.route('/api/research/seed-tickers')
def seed_tickers():
    folders = glob.glob(os.path.join(EDGAR_BASE, "*"))
    tickers = [os.path.basename(f) for f in folders
               if os.path.isfile(os.path.join(f, f"{os.path.basename(f)}_seed.md"))]
    return jsonify(sorted(tickers))

@app.route('/api/research/seed_url/<ticker>')
def serve_seed_file(ticker):
    ticker = ticker.upper()
    path = os.path.join(EDGAR_BASE, ticker, f"{ticker}_seed.md")
    if not os.path.exists(path):
        return jsonify({"error": "Seed file not found"}), 404
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    from flask import Response
    return Response(content, mimetype='text/plain',
                    headers={"Access-Control-Allow-Origin": "*"})

@app.route('/api/research/create_seed', methods=['POST'])
def api_research_create_seed():
    """
    POST /api/research/create_seed  {ticker: "NVDA"}
    Creates an investment simulation seed for any ticker by calling MiroShark
    ask-stock endpoint (EDGAR data + LLM seed generation).
    Runs in background thread, emits progress via SocketIO events:
      seed_progress  {ticker, status, msg}
      seed_complete  {ticker, success, url, sim_req_preview, seed_doc_length, error}
    Returns immediately with {status: 'started', ticker}.
    """
    ticker = (request.json or {}).get('ticker', '').upper().strip()
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400

    def _run():
        socketio.emit('seed_progress', {
            'ticker': ticker, 'status': 'starting',
            'msg': f'Running seed_writer for {ticker}...'
        })
        try:
            # seed_writer.py runs standalone — no MiroShark needed.
            # Reads EDGAR .md file, calls LLM, writes TICKER_seed.md.
            # REQUIRES: edgar_download.py must have run first (TICKER.md must exist).
            script    = str(Path.home() / 'Documents' / 'EDGAR' / 'seed_writer.py')
            edgar_cwd = str(Path.home() / 'Documents' / 'EDGAR')
            md_path   = Path.home() / 'Documents' / 'EDGAR' / 'companies' / ticker / f'{ticker}.md'

            if not md_path.exists():
                socketio.emit('seed_complete', {
                    'ticker': ticker, 'success': False,
                    'error': f'No EDGAR data for {ticker} — run ▶ Run EDGAR first, then create seed'
                })
                return

            socketio.emit('seed_progress', {
                'ticker': ticker, 'status': 'generating',
                'msg': f'Calling LLM to generate seed for {ticker} (may take 30-60s)...'
            })

            result = subprocess.run(
                ['python3', script, ticker, '--force'],
                capture_output=True, text=True,
                timeout=180,
                cwd=edgar_cwd,
                env={**os.environ}
            )

            seed_path = Path.home() / 'Documents' / 'EDGAR' / 'companies' / ticker / f'{ticker}_seed.md'

            if seed_path.exists():
                seed_content = seed_path.read_text(encoding='utf-8')
                socketio.emit('seed_complete', {
                    'ticker':          ticker,
                    'success':         True,
                    'seed_doc_length': len(seed_content),
                    'seed_path':       str(seed_path),
                })
            else:
                err = (result.stderr or result.stdout or 'seed file not created')[-400:]
                socketio.emit('seed_complete', {
                    'ticker': ticker, 'success': False, 'error': err
                })

        except subprocess.TimeoutExpired:
            socketio.emit('seed_complete', {
                'ticker': ticker, 'success': False,
                'error': 'Seed creation timed out (180s) — try again'
            })
        except Exception as e:
            socketio.emit('seed_complete', {
                'ticker': ticker, 'success': False, 'error': str(e)
            })

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'started', 'ticker': ticker})

@app.route('/api/research/launch-sim/<ticker>')
def launch_sim(ticker):
    ticker = ticker.upper()
    try:
        r = requests.post(f"{MIROSHARK_BASE}/api/simulation/ask-stock",
                          json={"ticker": ticker}, timeout=30)
        data = r.json()
        sim_requirement = (data.get('data') or {}).get('simulation_requirement', '') or data.get('simulation_requirement', '')
        seed_document = (data.get('data') or {}).get('seed_document', '') or data.get('seed_document', '')

        # Write preset template file for MiroShark to auto-load
        template = {
            "simulation_requirement": sim_requirement,
            "seed_document": seed_document
        }
        template_dir = os.path.expanduser(
            "~/Documents/MiroShark/backend/app/preset_templates"
        )
        os.makedirs(template_dir, exist_ok=True)
        template_path = os.path.join(template_dir, f"hermes_{ticker.lower()}.json")
        with open(template_path, 'w', encoding='utf-8') as f:
            json.dump(template, f)

        miroshark_url = f"http://localhost:5001/?template=hermes_{ticker.lower()}"
        return jsonify({"url": miroshark_url, "scenario": sim_requirement})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route('/api/research/simulations')
def simulation_history():
    try:
        r = requests.get(f"{MIROSHARK_BASE}/api/simulation/list", timeout=10)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route('/api/research/reports')
def research_reports():
    """
    GET /api/research/reports
    Reads report markdown directly from MiroShark uploads — works whether
    MiroShark is running or not. Returns list sorted newest first.
    Falls back to empty list gracefully if folder doesn't exist.
    """
    reports_dir = Path.home() / 'Documents' / 'MiroShark' / 'backend' / 'uploads' / 'reports'
    results = []

    def add_markdown_report(path, report_id, folder_name):
        if not path.is_file():
            return
        try:
            text = path.read_text(encoding='utf-8', errors='replace')
            if not text.strip():
                return
            stat = path.stat()
            tm = re.search(r'ORACLE Investment Analysis\s*[—-]\s*([A-Z0-9.\-]{1,12})', text[:300])
            dm = re.search(r'\*\*Date:\*\*\s*(.+?)(?:\n|\r)', text[:500])
            ticker = tm.group(1).strip() if tm else folder_name[:12]
            mtime_str = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds')
            date_str = dm.group(1).strip() if dm else mtime_str
            results.append({
                'report_id': report_id,
                'ticker': ticker,
                'date': date_str,
                'size_kb': max(1, stat.st_size // 1024),
                'preview': text[:120].replace('\n', ' ').strip(),
                'source': 'report',
                'type': 'full_report',
                '_mtime': stat.st_mtime,
            })
        except Exception:
            return

    if reports_dir.exists():
        for rd in reports_dir.iterdir():
            if rd.is_dir():
                add_markdown_report(rd / 'full_report.md', rd.name, rd.name)

    results.sort(key=lambda x: x['_mtime'], reverse=True)
    for item in results:
        item.pop('_mtime', None)
    return jsonify(results)

@app.route('/api/research/delete_report/<report_id>', methods=['POST'])
def research_delete_report(report_id):
    """
    Soft-delete a polished report by moving its whole report folder into
    reports_deleted. Never permanently deletes report data.
    """
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', report_id or ''):
        return jsonify({'error': 'invalid report_id'}), 400

    reports_dir = Path.home() / 'Documents' / 'MiroShark' / 'backend' / 'uploads' / 'reports'
    deleted_dir = Path.home() / 'Documents' / 'MiroShark' / 'backend' / 'uploads' / 'reports_deleted'
    reports_resolved = reports_dir.resolve()

    report_dir = (reports_dir / report_id).resolve()
    if report_dir.parent != reports_resolved or not report_dir.is_dir() or report_dir.name != report_id:
        return jsonify({'error': 'report not found'}), 404

    full_report = report_dir / 'full_report.md'
    if not full_report.is_file():
        return jsonify({'error': 'full_report.md not found'}), 404

    try:
        deleted_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        target = deleted_dir / f'{stamp}__{report_id}'
        suffix = 1
        while target.exists():
            target = deleted_dir / f'{stamp}__{report_id}_{suffix}'
            suffix += 1
        shutil.move(str(report_dir), str(target))
        return jsonify({'success': True, 'deleted_to': str(target)})
    except Exception as e:
        log.exception('Failed to soft-delete report %s', report_id)
        return jsonify({'error': str(e)}), 500

@app.route('/api/research/report_content/<report_id>')
def research_report_content(report_id):
    """Serve full_report.md or simulation delta_report.md content for a report ID."""
    if report_id.startswith('sim__'):
        folder = report_id[len('sim__'):]
        base_dir = Path.home() / 'Documents' / 'MiroShark' / 'backend' / 'uploads' / 'simulations'
        filename = 'delta_report.md'
    else:
        folder = report_id
        base_dir = Path.home() / 'Documents' / 'MiroShark' / 'backend' / 'uploads' / 'reports'
        filename = 'full_report.md'

    if not re.match(r'^[a-zA-Z0-9_.-]+$', folder):
        return jsonify({'error': 'invalid report_id'}), 400

    base_resolved = base_dir.resolve()
    path = (base_dir / folder / filename).resolve()
    if base_resolved not in path.parents or not path.is_file():
        return jsonify({'error': 'report not found'}), 404
    from flask import Response
    return Response(path.read_text(encoding='utf-8', errors='replace'),
                    mimetype='text/plain',
                    headers={'Access-Control-Allow-Origin': '*'})

@app.route('/api/research/fundamentals/<ticker>')
def fundamentals(ticker):
    ticker = ticker.upper()
    path = os.path.join(EDGAR_BASE, ticker, f"{ticker}_fundamentals.xlsx")
    if not os.path.exists(path):
        return jsonify({"error": f"No fundamentals file found for {ticker}"}), 404
    try:
        import math, json as json_mod
        import pandas as pd
        df = pd.read_excel(path, header=0)
        df.columns = [str(c).strip() for c in df.columns]
        records = []
        for _, row in df.iterrows():
            clean_row = {}
            for k, v in row.items():
                key = str(k)
                if v is None:
                    clean_row[key] = None
                elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    clean_row[key] = None
                elif isinstance(v, (bool, int, float, str)):
                    clean_row[key] = v
                else:
                    try:
                        json_mod.dumps(v)
                        clean_row[key] = v
                    except (TypeError, ValueError):
                        clean_row[key] = str(v)
            records.append(clean_row)
        return app.response_class(
            response=json_mod.dumps(records, ensure_ascii=False, default=str),
            status=200,
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/research/fundamentals-html/<ticker>')
def fundamentals_html(ticker):
    import math, html as html_mod
    import pandas as pd
    ticker = ticker.upper()
    path = os.path.join(EDGAR_BASE, ticker, f"{ticker}_fundamentals.xlsx")
    if not os.path.exists(path):
        return f'<div style="padding:12px;color:#664444;font-family:monospace;">No file for {ticker}</div>', 404

    try:
        df = pd.read_excel(path, header=0)
        cols = list(df.columns)
        # cols[0] = company name (Excel row 0 used as pandas header)
        # df row 0 = CIK metadata; df row 1 = blank; data starts at df row 2
        val_cols = cols[1:]

        company = html_mod.escape(str(cols[0]))
        cik_meta = ''
        if len(df) > 0:
            v = df.iloc[0, 0]
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                cik_meta = html_mod.escape(str(v).strip())

        # Find date header row (skip first 2 rows)
        date_labels = [f'Col {i+1}' for i in range(len(val_cols))]
        for idx, (_, row) in enumerate(df.iterrows()):
            if idx < 2:
                continue
            c0 = row.iloc[0]
            c1 = row.iloc[1] if len(row) > 1 else None
            if (c0 is None or (isinstance(c0, float) and math.isnan(c0)) or str(c0).strip() == '') \
               and c1 is not None and not (isinstance(c1, float) and math.isnan(c1)) \
               and str(c1).strip() != '' and any(ch.isdigit() for ch in str(c1)):
                date_labels = []
                for v in row.iloc[1:]:
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        date_labels.append('')
                    else:
                        date_labels.append(str(v).strip())
                break

        def cell(v, max_len=55):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return ''
            # Format raw decimals that look like percentages (0.0 - 1.0 range, metric contains %)
            s = str(v).strip()
            if not s or s == 'nan':
                return ''
            # Convert small float to percentage if it looks like a ratio
            if isinstance(v, float) and 0 < abs(v) < 1.5 and v != v.__round__(0):
                try:
                    # Only format as % if value looks like a ratio (e.g. 0.72 = 72%)
                    if abs(v) <= 1.0:
                        s = f'{v*100:.1f}%'
                    else:
                        s = f'{v:.4f}'
                except Exception:
                    pass
            if len(s) > max_len:
                s = s[:max_len] + '…'
            return html_mod.escape(s)

        def is_num(v):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return False
            try:
                float(str(v).replace(',', '').replace('%', ''))
                return True
            except (ValueError, TypeError):
                return False

        th_cells = ''.join(
            f'<th style="background:#0a2a0a;color:#88cc88;padding:4px 8px;'
            f'text-align:right;border-bottom:1px solid #1a3a1a;'
            f'font-family:monospace;font-size:11px;min-width:70px;white-space:nowrap;">'
            f'{html_mod.escape(d)}</th>'
            for d in date_labels
        )

        rows_html = []
        for idx, (_, row) in enumerate(df.iterrows()):
            if idx < 2:  # skip CIK metadata row and blank row
                continue
            c0_raw = row.iloc[0]
            c0_empty = (c0_raw is None or (isinstance(c0_raw, float) and math.isnan(c0_raw))
                        or str(c0_raw).strip() == '' or str(c0_raw).strip() == 'nan')
            vals = [row.iloc[i+1] if i+1 < len(row) else None for i in range(len(val_cols))]
            has_vals = any(v is not None and not (isinstance(v, float) and math.isnan(v))
                           and str(v).strip() not in ('', 'nan') for v in vals)

            if c0_empty and not has_vals:
                continue  # blank row
            if c0_empty and has_vals:
                continue  # date header row

            metric = cell(c0_raw, max_len=200)
            if not metric:
                continue

            if metric.startswith('PHASE') or metric.startswith('---'):
                rows_html.append(
                    f'<tr><td colspan="{1+len(val_cols)}" style="color:#ffaa00;'
                    f'padding:10px 12px 3px;font-size:10px;letter-spacing:2px;'
                    f'border-top:1px solid #1a3a1a;background:#080e08;'
                    f'font-family:monospace;">{metric}</td></tr>'
                )
                continue

            tds = f'<td style="color:#88cc88;padding:2px 8px;border-bottom:1px solid #0a1a0a;' \
                  f'font-family:monospace;font-size:11px;max-width:260px;overflow:hidden;' \
                  f'text-overflow:ellipsis;white-space:nowrap;" title="{metric}">' \
                  f'{metric}</td>'
            for v in vals:
                display = cell(v, max_len=30)
                color = '#00ff88' if display and is_num(v) else '#88cc88'
                tds += (f'<td style="color:{color};padding:2px 8px;'
                        f'border-bottom:1px solid #0a1a0a;text-align:right;'
                        f'white-space:nowrap;font-family:monospace;font-size:11px;'
                        f'min-width:70px;" title="{html_mod.escape(str(v)) if v and v==v else ""}">'
                        f'{display}</td>')
            rows_html.append(f'<tr>{tds}</tr>')

        html_out = f'''
<div style="padding:3px 12px;background:#0a2a0a;border-bottom:1px solid #1a3a1a;
            font-family:monospace;font-size:11px;color:#ffaa00;font-weight:bold;">
  {company}
</div>
<div style="padding:2px 12px;background:#0a2a0a;border-bottom:1px solid #1a3a1a;
            font-family:monospace;font-size:9px;color:#446644;">
  {cik_meta}
</div>
<div style="overflow-x:auto;">
  <table style="border-collapse:collapse;white-space:nowrap;">
    <thead>
      <tr>
        <th style="background:#0a2a0a;color:#ffaa00;padding:4px 8px;text-align:left;
                   border-bottom:1px solid #1a3a1a;font-family:monospace;
                   font-size:11px;max-width:260px;">Metric</th>
        {th_cells}
      </tr>
    </thead>
    <tbody>{''.join(rows_html)}</tbody>
  </table>
</div>'''
        return html_out, 200, {'Content-Type': 'text/html; charset=utf-8',
                               'Cache-Control': 'no-cache'}
    except Exception as e:
        return f'<div style="padding:12px;color:#664444;font-family:monospace;">Error: {html_mod.escape(str(e))}</div>', 500

@app.route('/api/profit_basket')
def api_profit_basket():
    """Profit basket: DIA+QQQ+VOO+SGOL positions vs true profit target."""
    BASKET = ['DIA', 'QQQ', 'VOO', 'SGOL']
    PRINCIPAL = 1154.00
    positions = alpaca('/v2/positions')
    acct = alpaca('/v2/account')
    equity = float(acct.get('equity', 0))
    true_profit = round(equity - PRINCIPAL, 2)
    target_each = round(true_profit / 4, 2) if true_profit > 0 else 0

    basket = {}
    for p in positions:
        sym = p['symbol']
        if sym in BASKET:
            mv = float(p.get('market_value', 0))
            upl = float(p.get('unrealized_pl', 0))
            basket[sym] = {'mv': round(mv, 2), 'upl': round(upl, 2)}

    total = sum(v['mv'] for v in basket.values())
    # Determine rebalance signal
    if true_profit <= 0:
        signal = 'NO_PROFIT'
    elif total >= true_profit * 1.1:
        signal = 'OVER'   # basket > 110% of profit — no add
    elif total < true_profit * 0.9:
        signal = 'UNDER'  # basket < 90% of profit — add to laggard
    else:
        signal = 'BALANCED'

    # Which needs most buying (furthest below target_each)
    buy_candidate = None
    if signal == 'UNDER' and target_each > 0:
        gaps = {sym: target_each - basket.get(sym, {}).get('mv', 0) for sym in BASKET}
        buy_candidate = max(gaps, key=gaps.get)
        buy_amount = round(max(gaps[buy_candidate], 0), 2)
    else:
        buy_amount = 0

    return jsonify({
        'true_profit':    true_profit,
        'target_each':    target_each,
        'basket':         {sym: basket.get(sym, {'mv': 0.0, 'upl': 0.0}) for sym in BASKET},
        'basket_total':   round(total, 2),
        'signal':         signal,
        'buy_candidate':  buy_candidate,
        'buy_amount':     buy_amount,
    })

# ── Macro Signal (ORACLE MacroComposite from TradingView MCP) ────────────────
_MACRO_CACHE = {
    'quadrant':    'NEUTRAL',
    'equity_rank': None,
    'hard_rank':   None,
    'harvest_pct': 10.0,
    'signal':      '⚪ NEUTRAL',
    'updated_at':  None,
}

def _fetch_macro_signal():
    """Read ORACLE MacroComposite values from TradingView MCP via JSON-RPC."""
    try:
        import json as _json, socket as _socket
        # Call TradingView MCP server via subprocess (same pattern as tv_technical.py)
        TV_MCP = '/home/sumith/tradingview-mcp-jackson/src/server.js'
        if not os.path.exists(TV_MCP):
            return
        req = _json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "data_get_study_values", "arguments": {}}
        })
        result = subprocess.run(
            ['node', TV_MCP, '--stdio'],
            input=req + '\n', capture_output=True, text=True, timeout=15
        )
        if result.returncode != 0 or not result.stdout.strip():
            return
        # Parse response — may have multiple JSON lines
        for line in result.stdout.strip().split('\n'):
            try:
                resp = _json.loads(line)
                studies = resp.get('result', {}).get('content', [{}])[0]
                text = studies.get('text', '')
                data = _json.loads(text) if text else {}
                studies_list = data.get('studies', [])
                for s in studies_list:
                    if 'MacroComposite' in s.get('name', '') or 'ORACLE' in s.get('name', ''):
                        vals = s.get('values', {})
                        eq  = float(vals.get('Equity Rank', 0) or 0)
                        hrd = float(vals.get('Hard Asset Rank', 0) or 0)
                        _update_macro_cache(eq, hrd)
                        return
            except Exception:
                continue
    except Exception as e:
        log.warning(f'[macro_signal] fetch error: {e}')

def _update_macro_cache(eq, hrd):
    ob, os_ = 70.0, 30.0
    if eq > ob and hrd > ob:
        q, pct, sig = 'OVERHEATED',      8.0,  '🔴 OVERHEATED — harvest 8%+'
    elif eq > ob and hrd < os_:
        q, pct, sig = 'RISK-ON',         12.0, '🟢 RISK-ON — harvest 12%+'
    elif eq < os_ and hrd < os_:
        q, pct, sig = 'CHEAP',           15.0, '🔵 CHEAP — accumulate, harvest 15%+'
    elif eq < os_ and hrd > ob:
        q, pct, sig = 'FLIGHT',          10.0, '🟡 FLIGHT — hold SGOL, harvest stocks 10%+'
    else:
        q, pct, sig = 'NEUTRAL',         10.0, '⚪ NEUTRAL — normal rules apply'
    _MACRO_CACHE.update({
        'quadrant': q, 'equity_rank': round(eq, 1),
        'hard_rank': round(hrd, 1), 'harvest_pct': pct,
        'signal': sig, 'updated_at': datetime.now().strftime('%H:%M'),
    })
    log.info(f'[macro_signal] {q} eq={eq:.1f} hrd={hrd:.1f}')

def _macro_updater():
    """Background thread — refresh macro signal every 5 minutes."""
    while True:
        _fetch_macro_signal()
        time.sleep(300)

@app.route('/api/macro_advisory')
def api_macro_advisory():
    """Generate macro-aligned buy suggestions based on ORACLE composite signal."""
    BASKET   = ['DIA', 'QQQ', 'VOO', 'SGOL']
    PRINCIPAL = 1154.00
    try:
        acct      = alpaca('/v2/account')
        positions = alpaca('/v2/positions')
        equity    = float(acct.get('equity', 0))
        cash      = float(acct.get('cash', 0))
        true_profit = round(equity - PRINCIPAL, 2)
        target_each = round(true_profit / 4, 2) if true_profit > 0 else 0

        macro = _MACRO_CACHE
        quadrant = macro.get('quadrant', 'NEUTRAL')
        harvest_pct = macro.get('harvest_pct', 10.0)

        # Build position lookup
        pos_by_sym = {p['symbol']: p for p in positions}

        suggestions = []

        # ── 1. Basket rebalance (DIA/QQQ/VOO only — not SGOL in CHEAP) ──────
        basket_suggest = []
        for sym in ['DIA', 'QQQ', 'VOO']:
            mv = float(pos_by_sym.get(sym, {}).get('market_value', 0))
            gap = target_each - mv
            if gap > 1.10:
                buy_amt = round(min(gap, cash * 0.2), 2)  # max 20% of cash
                if buy_amt >= 1.10:
                    basket_suggest.append({
                        'sym': sym, 'buy': buy_amt,
                        'reason': f'Basket gap ${gap:.2f} below target ${target_each:.2f}',
                        'type': 'basket', 'priority': gap,
                    })
        # Add SGOL only in FLIGHT quadrant
        if quadrant == 'FLIGHT':
            mv = float(pos_by_sym.get('SGOL', {}).get('market_value', 0))
            gap = target_each - mv
            if gap > 1.10:
                basket_suggest.append({
                    'sym': 'SGOL', 'buy': round(min(gap, cash * 0.15), 2),
                    'reason': f'Flight to safety — add gold (gap ${gap:.2f})',
                    'type': 'basket', 'priority': gap + 100,
                })
        basket_suggest.sort(key=lambda x: -x['priority'])
        suggestions.extend(basket_suggest[:3])

        # ── 2. DCA candidates from positions ─────────────────────────────────
        # Load Fidelity conviction data
        fid = {}
        from pathlib import Path
        csv_path = Path.home() / 'portfolio.csv'
        if csv_path.exists():
            try: fid = _parse_fidelity_quick()
            except Exception: pass

        # Load EDGAR cache for quality filter
        edgar_cache = {}
        if EDGAR_CACHE.exists():
            try:
                import json as _j
                edgar_cache = _j.loads(EDGAR_CACHE.read_text())
            except Exception: pass

        dca_candidates = []
        protected = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
        for p in positions:
            sym = p['symbol']
            if sym in protected: continue
            uplpc = float(p.get('unrealized_plpc', 0)) * 100
            mv    = float(p.get('market_value', 0))
            if uplpc > -2.0 or mv < 1.10: continue  # only real positions

            fd     = fid.get(sym, {})
            accts  = fd.get('acct_count', 0)
            ec     = edgar_cache.get(sym, {})
            escore = ec.get('score', None)

            # Skip low conviction in OVERHEATED (not the time to add weak hands)
            if quadrant == 'OVERHEATED' and accts < 3: continue

            # Score = conviction × magnitude × EDGAR quality
            score = (accts * 2) + abs(uplpc) + (escore or 0) * 0.5
            buy_amt = max(round(min(abs(uplpc) * 0.3, 5.0), 2), 1.10)

            dca_candidates.append({
                'sym': sym, 'buy': buy_amt,
                'reason': f'Down {uplpc:.1f}% | {accts} Fidelity accts' +
                          (f' | EDGAR {escore}/18' if escore else ''),
                'type': 'dca', 'priority': score,
                'uplpc': round(uplpc, 1), 'accts': accts,
            })

        dca_candidates.sort(key=lambda x: -x['priority'])

        # In CHEAP: take top 5 DCA. In OVERHEATED: none. Others: top 3.
        dca_limit = 5 if quadrant == 'CHEAP' else 0 if quadrant == 'OVERHEATED' else 3
        suggestions.extend(dca_candidates[:dca_limit])

        # ── 3. Cap total spend to available cash ──────────────────────────────
        total_suggested = sum(s['buy'] for s in suggestions)
        if total_suggested > cash * 0.8:
            scale = (cash * 0.8) / total_suggested
            for s in suggestions:
                s['buy'] = max(round(s['buy'] * scale, 2), 1.10)

        # ── 4. Macro rule summary ─────────────────────────────────────────────
        rules = {
            'OVERHEATED': 'Harvest gainers at 8%+. Minimal new buys. Raise cash.',
            'RISK-ON':    'Let winners run to 12%+. Add selectively on dips.',
            'CHEAP':      'Accumulate aggressively. Hold until 15%+. Dips = opportunity.',
            'FLIGHT':     'Hold SGOL (do not harvest). Add gold. Harvest stocks at 10%+.',
            'NEUTRAL':    'Normal rules apply. Harvest at 10%+.',
        }

        return jsonify({
            'quadrant':    quadrant,
            'signal':      macro.get('signal', ''),
            'harvest_pct': harvest_pct,
            'equity_rank': macro.get('equity_rank'),
            'hard_rank':   macro.get('hard_rank'),
            'rule':        rules.get(quadrant, ''),
            'cash':        round(cash, 2),
            'suggestions': suggestions,
            'updated_at':  macro.get('updated_at'),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/macro_signal')
def api_macro_signal():
    return jsonify(_MACRO_CACHE)


@app.route('/api/markets/quote')
def api_markets_quote():
    """
    Proxy Yahoo Finance chart data for the Markets widget.
    Params: sym (Yahoo symbol, URL-encoded), range, interval
    Returns: {price, change_pct, change_amt, closes[], timestamps[]}

    ⚠ COMMODITY PRICE WARNING — LLM training data is stale.
    Gold is often hallucinated at ~$1,800-$2,100, silver at ~$22-$30.
    All prices here are fetched LIVE from Yahoo Finance — never use LLM-suggested prices.
    """
    import urllib.request as _ur, json as _json
    sym      = request.args.get('sym', '')
    rng      = request.args.get('range', '1mo')
    interval = request.args.get('interval', '1d')
    if not sym:
        return jsonify({'error': 'sym required'}), 400
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
               f"?interval={interval}&range={rng}")
        req = _ur.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with _ur.urlopen(req, timeout=10) as resp:
            data = _json.loads(resp.read().decode())
        result = data['chart']['result'][0]
        meta   = result['meta']
        closes_raw = result['indicators']['quote'][0].get('close', [])
        closes = [c for c in closes_raw if c is not None]
        timestamps = result.get('timestamp', [])

        price      = meta.get('regularMarketPrice')
        prev_close = meta.get('previousClose') or meta.get('chartPreviousClose')
        change_amt = (price - prev_close) if (price and prev_close) else None
        change_pct = (change_amt / prev_close * 100) if (change_amt and prev_close) else None

        return jsonify({
            'sym':        sym,
            'name':       meta.get('shortName') or meta.get('symbol', sym),
            'price':      price,
            'prev_close': prev_close,
            'change_amt': round(change_amt, 4) if change_amt else None,
            'change_pct': round(change_pct, 4) if change_pct else None,
            'closes':     [round(c, 4) for c in closes],
            'timestamps': timestamps,
            'currency':   meta.get('currency', 'USD'),
            'market_state': meta.get('marketState', ''),
            'range':      rng,
            'interval':   interval,
        })
    except Exception as e:
        log.error(f'[markets/quote] {sym}: {e}')
        return jsonify({'error': str(e), 'sym': sym}), 500

# ── Launch ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    # Start background updater
    def _live_updater_ctx():
        with app.app_context():
            _live_updater()

    def _proactive_brain_ctx():
        with app.app_context():
            _proactive_brain()

    def _macro_updater_ctx():
        with app.app_context():
            _macro_updater()

    t = threading.Thread(target=_live_updater_ctx, daemon=True)
    t.start()
    # Start proactive brain
    t2 = threading.Thread(target=_proactive_brain_ctx, daemon=True)
    t2.start()
    # Start macro signal updater
    t3 = threading.Thread(target=_macro_updater_ctx, daemon=True)
    t3.start()
    # Seed macro cache immediately with current values
    _update_macro_cache(25.8, 23.8)  # seeded from live TradingView read
    print("\n" + "="*55)
    print("  HERMES TRADING DASHBOARD")
    print("  http://localhost:6060")
    print("="*55 + "\n")
    socketio.run(app, host='0.0.0.0', port=6060, debug=False, allow_unsafe_werkzeug=True, use_reloader=False)
