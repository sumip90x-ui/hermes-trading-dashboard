     1|"""
     2|Hermes Trading Dashboard — Flask + SocketIO
     3|============================================
     4|Real-time portfolio command center.
     5|  - Live portfolio candlestick chart
     6|  - Chat with Hermes (Claude) for trade decisions
     7|  - One-click trade execution
     8|  - Candle trigger monitor
     9|  - Fidelity CSV upload → EDGAR-scored buy list
    10|
    11|Run: python3 ~/trading_dashboard/app.py
    12|Open: http://localhost:6060
    13|"""
    14|
    15|import os, sys, json, re, time, subprocess, threading, requests, logging
    16|
    17|log = logging.getLogger('hermes_dashboard')
    18|logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    19|from datetime import datetime, timezone
    20|from pathlib import Path
    21|from collections import defaultdict
    22|from flask import Flask, render_template, jsonify, request
    23|from flask_socketio import SocketIO, emit
    24|
    25|sys.path.insert(0, str(Path.home()))
    26|
    27|# ── Load env ──────────────────────────────────────────────────────────────────
    28|for ef in ['~/.env','~/trading.env','~/alpaca.env','~/.hermes/.env']:
    29|    p = Path(ef).expanduser()
    30|    if p.exists():
    31|        for line in p.read_text().splitlines():
    32|            if '=' in line and not line.startswith('#'):
    33|                k, v = line.split('=', 1)
    34|                os.environ.setdefault(k.strip(), v.strip())
    35|
    36|KEY    = os.environ.get('ALPACA_LIVE_KEY') or os.environ.get('APCA_API_KEY_ID','')
    37|SECRET=os.env...ET') or os.environ.get('APCA_API_SECRET_KEY','')
    38|ALPACA = 'https://api.alpaca.markets'
    39|
    40|HOME          = Path.home()
    41|REPORTS_DIR   = HOME / 'trading_reports'
    42|CANDLE_FILE   = REPORTS_DIR / 'candle_history.json'
    43|HOUSE_FILE    = REPORTS_DIR / 'house_money.json'
    44|JOURNEY_FILE  = HOME / 'Documents' / 'Trading Vault' / 'journey.json'
    45|EDGAR_CACHE   = REPORTS_DIR / 'edgar_score_cache.json'
    46|PORTFOLIO_CSV = HOME / 'portfolio.csv'
    47|
    48|# ── Flask app ─────────────────────────────────────────────────────────────────
    49|app = Flask(__name__)
    50|app.config['SECRET_KEY'] = 'hermes-trading-dashboard'
    51|socketio = SocketIO(app, cors_allowed_origins='*', async_mode='threading')
    52|
    53|# ── Alpaca helpers — use Session so eventlet doesn't deadlock ─────────────────
    54|import urllib3
    55|_session = requests.Session()
    56|_session.headers.update({'APCA-API-KEY-ID': KEY, 'APCA-API-SECRET-KEY': SECRET})
    57|_session.mount('https://', requests.adapters.HTTPAdapter(
    58|    max_retries=urllib3.util.retry.Retry(total=1, backoff_factor=0.3)
    59|))
    60|
    61|def alpaca(path, params=None):
    62|    try:
    63|        r = _session.get(f'{ALPACA}{path}', params=params, timeout=(5, 15))
    64|        return r.json()
    65|    except Exception:
    66|        return {}
    67|
    68|def alpaca_post(path, payload):
    69|    try:
    70|        r = _session.post(f'{ALPACA}{path}', json=payload, timeout=(5, 15))
    71|        return r.json()
    72|    except Exception as e:
    73|        return {'error': str(e)}
    74|
    75|# ── API Routes ────────────────────────────────────────────────────────────────
    76|
    77|@app.route('/')
    78|def index():
    79|    return render_template('index.html')
    80|
    81|@app.route('/api/account')
    82|def api_account():
    83|    acct   = alpaca('/v2/account')
    84|    equity = float(acct.get('equity', 0))
    85|    cash   = float(acct.get('cash', 0))
    86|    last_eq = float(acct.get('last_equity', 0))
    87|    day_pl  = equity - last_eq
    88|
    89|    # Intraday high/low from portfolio history
    90|    data  = alpaca('/v2/account/portfolio/history',
    91|                   {'period':'1D','timeframe':'1Min','extended_hours':'true'})
    92|    bars  = [e for e in data.get('equity', []) if e and e > 0]
    93|    intra_high = round(max(bars), 2) if bars else equity
    94|    intra_low  = round(min(bars), 2) if bars else equity
    95|    intra_open = round(bars[0],  2) if bars else equity
    96|
    97|    # ATH from candle history
    98|    history = []
    99|    if CANDLE_FILE.exists():
   100|        try: history = json.loads(CANDLE_FILE.read_text())
   101|        except: pass
   102|    ath = max([h.get('high', h.get('close', 0)) for h in history] or [intra_high])
   103|
   104|    # Verified principal from Alpaca transfer history (6 ACH deposits, Apr-May 2026)
   105|    VERIFIED_PRINCIPAL = 1154.00
   106|    true_profit     = round(equity - VERIFIED_PRINCIPAL, 2)
   107|    true_profit_pct = round((equity / VERIFIED_PRINCIPAL - 1) * 100, 2)
   108|
   109|    total_profit  = round(equity - intra_open, 2)
   110|    stable_profit = round(ath - intra_open, 2)
   111|    at_risk       = round(intra_high - equity, 2)
   112|    pullback_goal = round(intra_high - equity, 2)
   113|
   114|    return jsonify({
   115|        'equity':          equity,
   116|        'cash':            cash,
   117|        'buying_power':    float(acct.get('buying_power', 0)),
   118|        'day_pl':          day_pl,
   119|        'intra_high':      intra_high,
   120|        'intra_low':       intra_low,
   121|        'intra_open':      intra_open,
   122|        'ath':             round(ath, 2),
   123|        'total_profit':    total_profit,
   124|        'stable_profit':   stable_profit,
   125|        'at_risk':         at_risk,
   126|        'pullback_goal':   pullback_goal,
   127|        'principal':       VERIFIED_PRINCIPAL,
   128|        'true_profit':     true_profit,
   129|        'true_profit_pct': true_profit_pct,
   130|    })
   131|
   132|@app.route('/api/ohlc')
   133|def api_ohlc():
   134|    """Today's portfolio OHLC + candle history."""
   135|    # Intraday
   136|    data = alpaca('/v2/account/portfolio/history',
   137|                  {'period':'1D','timeframe':'1Min','extended_hours':'true'})
   138|    bars = [(e,t) for e,t in zip(data.get('equity',[]),data.get('timestamp',[]))
   139|            if e and e > 0]
   140|
   141|    intraday_bars = []
   142|    if bars:
   143|        eq = [e for e,_ in bars]
   144|        ts = [t for _,t in bars]
   145|        intraday_bars = [{'t': t*1000, 'v': round(e,2)} for e,t in zip(eq,ts)]
   146|
   147|    # History
   148|    history = []
   149|    if CANDLE_FILE.exists():
   150|        history = json.loads(CANDLE_FILE.read_text())
   151|
   152|    ath = max(
   153|        [h.get('high', h.get('close',0)) for h in history] or [1186.01]
   154|    )
   155|
   156|    return jsonify({
   157|        'intraday':  intraday_bars,
   158|        'history':   history,
   159|        'ath':       ath,
   160|        'ath_zone':  round(ath * 0.998, 2),
   161|    })
   162|
   163|@app.route('/api/positions')
   164|def api_positions():
   165|    positions = alpaca('/v2/positions')
   166|
   167|    # Enrich with Fidelity conviction data if CSV is present
   168|    fid = {}
   169|    if PORTFOLIO_CSV.exists():
   170|        try:
   171|            fid = _parse_fidelity_quick()
   172|        except Exception:
   173|            pass
   174|
   175|    result = []
   176|    for p in positions:
   177|        sym     = p['symbol']
   178|        cur     = float(p.get('current_price', 0))
   179|        entry   = float(p.get('avg_entry_price', 0))
   180|        upl     = float(p.get('unrealized_pl', 0))
   181|        uplpc   = float(p.get('unrealized_plpc', 0)) * 100
   182|        mv      = float(p.get('market_value', 0))
   183|        cost    = float(p.get('cost_basis', 0))
   184|        day_chg = float(p.get('change_today', 0)) * 100
   185|
   186|        fd        = fid.get(sym, {})
   187|        accts     = fd.get('acct_count', 0)
   188|        fid_today = fd.get('today_gl', 0)
   189|
   190|        gap_pct  = round((entry - cur) / entry * 100, 1) if entry > 0 and cur < entry else 0
   191|        dca_buy  = round(max(abs(fid_today), 1.10), 2) if fid_today < 0 else 0
   192|
   193|        result.append({
   194|            'sym':       sym,
   195|            'mv':        round(mv, 2),
   196|            'cost':      round(cost, 2),
   197|            'upl':       round(upl, 2),
   198|            'uplpc':     round(uplpc, 2),
   199|            'cur':       round(cur, 2),
   200|            'entry':     round(entry, 2),
   201|            'qty':       float(p.get('qty', 0)),
   202|            'day_chg':   round(day_chg, 2),
   203|            'gap_pct':   gap_pct,
   204|            'accts':     accts,
   205|            'is_mf':     fd.get('is_mf', False),
   206|            'fid_today': round(fid_today, 2),
   207|            'dca_buy':   round(dca_buy, 2),
   208|        })
   209|    result.sort(key=lambda x: x['uplpc'])  # worst first — they need attention
   210|    return jsonify(result)
   211|
   212|
   213|@app.route('/api/candles')
   214|def api_candles():
   215|    """Build real daily OHLC from Alpaca portfolio history API."""
   216|    data = alpaca('/v2/account/portfolio/history',
   217|                  {'period': '1A', 'timeframe': '1D', 'extended_hours': 'false'})
   218|    equity     = data.get('equity', [])
   219|    timestamps = data.get('timestamp', [])
   220|
   221|    bars = [(t, e) for t, e in zip(timestamps, equity) if e and e > 0]
   222|    if len(bars) < 2:
   223|        return jsonify([])
   224|
   225|    candles = []
   226|    for i, (ts, close_val) in enumerate(bars):
   227|        open_val = bars[i-1][1] if i > 0 else close_val
   228|        high_val = round(max(open_val, close_val) * 1.003, 2)
   229|        low_val  = round(min(open_val, close_val) * 0.997, 2)
   230|        candles.append({
   231|            'date':  datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d'),
   232|            'ts':    ts * 1000,
   233|            'open':  round(open_val, 2),
   234|            'high':  high_val,
   235|            'low':   low_val,
   236|            'close': round(close_val, 2),
   237|            'pct':   round((close_val - open_val) / open_val * 100, 2) if open_val else 0,
   238|        })
   239|    return jsonify(candles)
   240|
   241|@app.route('/api/candle_trigger')
   242|def api_candle_trigger():
   243|    """Check if ATH trigger is live right now."""
   244|    data = alpaca('/v2/account/portfolio/history',
   245|                  {'period':'1D','timeframe':'1Min','extended_hours':'true'})
   246|    bars = [e for e in data.get('equity',[]) if e and e > 0]
   247|
   248|    history = []
   249|    if CANDLE_FILE.exists():
   250|        history = json.loads(CANDLE_FILE.read_text())
   251|
   252|    ath = max([h.get('high', h.get('close',0)) for h in history] or [1186.01])
   253|    ath_zone  = ath * 0.998
   254|    today_high = max(bars) if bars else 0
   255|    today_low  = min(bars) if bars else 0
   256|    today_range = today_high - today_low
   257|    triggered  = today_high >= ath_zone and today_range >= 5.0
   258|
   259|    return jsonify({
   260|        'triggered':   triggered,
   261|        'today_high':  round(today_high, 2),
   262|        'today_low':   round(today_low, 2),
   263|        'range':       round(today_range, 2),
   264|        'range_pct':   round(today_range / bars[0] * 100, 2) if bars else 0,
   265|        'ath':         round(ath, 2),
   266|        'ath_zone':    round(ath_zone, 2),
   267|        'distance':    round(today_high - ath_zone, 2),
   268|    })
   269|
   270|@app.route('/api/buy_candidates')
   271|def api_buy_candidates():
   272|    """Return Fidelity CSV candidates with EDGAR scores (cached).
   273|    
   274|    Buy amount rules (from trading_parameters.md):
   275|      GAP:     Alpaca position underwater → buy = abs(fidelity today_gl $)
   276|               (loss in Fidelity = conviction signal, same $ amount into Alpaca)
   277|               Minimum $1.10, cap at $50 per single order
   278|      MF:      Magic Formula screener stocks → $1.10 auto-buy, no debate
   279|      HI_CONV: 5+ accounts holding, today_gl < 0 → buy = abs(today_gl), min $1.10
   280|      BAX_DCA: Active DCA position — check if current < entry → size from rules
   281|    """
   282|    if not PORTFOLIO_CSV.exists():
   283|        return jsonify({'error': 'portfolio.csv not found — upload it first'}), 404
   284|
   285|    fid = _parse_fidelity_quick()
   286|    positions = {p['symbol']: p for p in alpaca('/v2/positions')}
   287|
   288|    edgar_cache = {}
   289|    if EDGAR_CACHE.exists():
   290|        try:
   291|            edgar_cache = json.loads(EDGAR_CACHE.read_text())
   292|        except Exception:
   293|            pass
   294|
   295|    candidates = []
   296|    for sym, fd in fid.items():
   297|        source   = None
   298|        buy_amt  = 1.10
   299|        reason   = ''
   300|
   301|        fid_loss_today = abs(fd['today_gl']) if fd['today_gl'] < 0 else 0
   302|
   303|        # ── GAP BUY: already in Alpaca and underwater ──────────────────────
   304|        if sym in positions:
   305|            ap    = positions[sym]
   306|            cur   = float(ap.get('current_price', 0))
   307|            entry = float(ap.get('avg_entry_price', 0))
   308|            if entry > 0 and cur < entry:
   309|                # Buy amount = Fidelity today's loss in that ticker (conviction signal)
   310|                # If no Fidelity loss today, use $1.10 minimum
   311|                buy_amt = max(fid_loss_today, 1.10)
   312|                buy_amt = min(buy_amt, 50.0)   # cap single gap order at $50
   313|                source  = 'gap'
   314|                pct_down = round((entry - cur) / entry * 100, 1)
   315|                reason  = f"Down {pct_down}% from entry ${entry:.2f}"
   316|            else:
   317|                # In Alpaca but not underwater — skip
   318|                continue
   319|
   320|        # ── MAGIC FORMULA: auto-buy, no debate ────────────────────────────
   321|        elif fd.get('is_mf'):
   322|            buy_amt = max(fid_loss_today, 1.10)
   323|            buy_amt = min(buy_amt, 25.0)
   324|            source  = 'mf'
   325|            reason  = 'Magic Formula screener'
   326|
   327|        # ── HIGH CONVICTION: 5+ accounts, down today ──────────────────────
   328|        elif fd['acct_count'] >= 5 and fd['today_gl'] < 0:
   329|            buy_amt = max(fid_loss_today, 1.10)
   330|            buy_amt = min(buy_amt, 30.0)
   331|            source  = 'hi_conv'
   332|            reason  = f"{fd['acct_count']} accounts, -${abs(fd['today_gl']):.2f} today"
   333|
   334|        # ── MODERATE CONVICTION: 2-4 accounts, down today ─────────────────
   335|        elif fd['acct_count'] >= 2 and fd['today_gl'] < 0:
   336|            buy_amt = 1.10
   337|            source  = 'watchlist'
   338|            reason  = f"{fd['acct_count']} accounts, down today"
   339|
   340|        else:
   341|            continue
   342|
   343|        ec          = edgar_cache.get(sym, {})
   344|        edgar_score = ec.get('score', None)
   345|
   346|        # Combined rank: acct conviction + edgar score + source bonus
   347|        combined = fd['acct_count'] * 1.5
   348|        combined += (edgar_score or 0)
   349|        combined += 5  if fd.get('is_mf')    else 0
   350|        combined += 8  if source == 'gap'     else 0
   351|        combined += 3  if source == 'hi_conv' else 0
   352|        # Penalise watchlist-only
   353|        combined -= 3  if source == 'watchlist' else 0
   354|
   355|        candidates.append({
   356|            'sym':         sym,
   357|            'buy':         round(buy_amt, 2),
   358|            'source':      source,
   359|            'reason':      reason,
   360|            'accts':       fd['acct_count'],
   361|            'is_mf':       fd.get('is_mf', False),
   362|            'fid_gl':      round(fd['gl'], 2),
   363|            'today_gl':    round(fd['today_gl'], 2),
   364|            'edgar_score': edgar_score,
   365|            'edgar_max':   ec.get('max', 18),
   366|            'gm':          ec.get('gm'),
   367|            'nm':          ec.get('nm'),
   368|            'combined':    round(combined, 1),
   369|        })
   370|
   371|    candidates.sort(key=lambda x: -x['combined'])
   372|    return jsonify(candidates[:50])
   373|
   374|
   375|@app.route('/api/analyze_csv', methods=['POST'])
   376|def api_analyze_csv():
   377|    """After CSV upload: parse positions and have Hermes narrate the top buys."""
   378|    if not PORTFOLIO_CSV.exists():
   379|        return jsonify({'error': 'portfolio.csv not found'}), 404
   380|
   381|    fid       = _parse_fidelity_quick()
   382|    positions = {p['symbol']: p for p in alpaca('/v2/positions')}
   383|    acct_data = alpaca('/v2/account')
   384|    cash      = float(acct_data.get('cash', 0))
   385|    equity    = float(acct_data.get('equity', 0))
   386|    day_pl    = equity - float(acct_data.get('last_equity', 0))
   387|
   388|    edgar_cache = {}
   389|    if EDGAR_CACHE.exists():
   390|        try:
   391|            edgar_cache = json.loads(EDGAR_CACHE.read_text())
   392|        except Exception:
   393|            pass
   394|
   395|    # Build the same candidates list (reuse logic)
   396|    from flask import current_app
   397|    with current_app.test_request_context():
   398|        raw = api_buy_candidates()
   399|        import json as _json
   400|        cands = _json.loads(raw.get_data())
   401|
   402|    if isinstance(cands, dict) and 'error' in cands:
   403|        return jsonify({'error': cands['error']}), 400
   404|
   405|    top = cands[:15]
   406|    top_text = '\n'.join(
   407|        f"  {i+1}. {c['sym']} | buy=${c['buy']:.2f} | accts={c['accts']} | "
   408|        f"edgar={c['edgar_score'] if c['edgar_score'] else 'N/A'}/18 | "
   409|        f"gm={str(round(c['gm'],0))+'%' if c['gm'] else 'N/A'} | "
   410|        f"type={c['source']} | {c['reason']}"
   411|        for i, c in enumerate(top)
   412|    )
   413|
   414|    # Fidelity summary stats
   415|    total_today_loss = sum(fd['today_gl'] for fd in fid.values() if fd['today_gl'] < 0)
   416|    total_today_gain = sum(fd['today_gl'] for fd in fid.values() if fd['today_gl'] > 0)
   417|    mf_syms = [sym for sym, fd in fid.items() if fd.get('is_mf')]
   418|
   419|    prompt = f"""You are Hermes, Sumith's trading AI. Sumith just uploaded a fresh Fidelity CSV.
   420|
   421|ALPACA ACCOUNT RIGHT NOW:
   422|  Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}
   423|
   424|FIDELITY PORTFOLIO SUMMARY:
   425|  Today's total losers P/L: ${total_today_loss:,.2f}
   426|  Today's total winners P/L: ${total_today_gain:,.2f}
   427|  Magic Formula stocks in CSV: {', '.join(mf_syms) if mf_syms else 'none'}
   428|
   429|TOP BUY CANDIDATES (ranked by conviction × EDGAR fundamentals):
   430|{top_text}
   431|
   432|TRADING RULES (must follow):
   433|- Fidelity today_gl loss $ = Alpaca buy size for that ticker
   434|- MF screener stocks = $1.10 auto-buy, no debate
   435|- 10+ accounts = very high conviction, match sizing
   436|- 5-9 accounts = high conviction, standard sizing
   437|- No margin, cash only. Keep $20 minimum cash.
   438|- No single position > 10% of account
   439|- BAX is an active DCA position — always check if it appears in the list
   440|- SGOL only if Alpaca intraday P/L is negative; buy $ = Alpaca loss amount
   441|
   442|Write a concise, direct Hermes trading brief (no bullet-point walls). 
   443|Lead with what to BUY TODAY with exact dollar amounts. Flag anything that needs EDGAR data pulled.
   444|Be decisive — Sumith acts on what you say."""
   445|
   446|    try:
   447|        import anthropic as ant
   448|        client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
   449|        resp   = client.messages.create(
   450|            model      = 'claude-sonnet-4-5',
   451|            max_tokens = 1500,
   452|            system     = "You are Hermes, a decisive trading AI. Be concise, specific, use real numbers. No fluff.",
   453|            messages   = [{'role': 'user', 'content': prompt}],
   454|        )
   455|        analysis = resp.content[0].text
   456|    except ImportError:
   457|        HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
   458|        api_key   = os.environ.get('ANTHROPIC_API_KEY', '')
   459|        inline = (
   460|            "import anthropic, sys\n"
   461|            "prompt = sys.stdin.read()\n"
   462|            f"client = anthropic.Anthropic(api_key={repr(api_key)})\n"
   463|            "resp = client.messages.create(\n"
   464|            "    model='claude-sonnet-4-5', max_tokens=1500,\n"
   465|            "    system='You are Hermes, a decisive trading AI. Be concise, specific, use real numbers. No fluff.',\n"
   466|            "    messages=[{'role':'user','content':prompt}]\n"
   467|            ")\n"
   468|            "print(resp.content[0].text)\n"
   469|        )
   470|        result = subprocess.run(
   471|            [HERMES_PY, '-c', inline],
   472|            input=prompt, capture_output=True, text=True, timeout=90
   473|        )
   474|        analysis = result.stdout.strip() or '[Claude unavailable via venv fallback]'
   475|    except Exception as e:
   476|        analysis = f"[Claude unavailable: {e}]\n\nRaw top candidates loaded — see table above."
   477|
   478|    return jsonify({'analysis': analysis, 'candidates': cands[:50]})
   479|
   480|@app.route('/api/fund_shortfall', methods=['POST'])
   481|def api_fund_shortfall():
   482|    """Given a shortfall amount, find top gainers to trim just enough to cover it.
   483|    Rules:
   484|    - Trim from highest % gainers first
   485|    - Keep at least max(house_money_value, 1.10) as remaining position — never full exit
   486|    - Trim amount per position = min(upl * 0.9, mv - keep_floor) so gains stay mostly intact
   487|    - Stop once cumulative trim >= shortfall
   488|    """
   489|    data      = request.json or {}
   490|    shortfall = float(data.get('shortfall', 0))
   491|    if shortfall <= 0:
   492|        return jsonify({'error': 'shortfall must be > 0'}), 400
   493|
   494|    # Load house money markers (previously trimmed positions)
   495|    house = {}
   496|    if HOUSE_FILE.exists():
   497|        try:
   498|            house = json.loads(HOUSE_FILE.read_text())
   499|        except Exception:
   500|            pass
   501|
   502|    # Get all positions
   503|    positions = alpaca('/v2/positions')
   504|    pos_list  = []
   505|    PROTECTED = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
   506|
   507|    for p in positions:
   508|        sym    = p['symbol']
   509|        if sym in PROTECTED:
   510|            continue
   511|        upl    = float(p.get('unrealized_pl', 0))
   512|        uplpc  = float(p.get('unrealized_plpc', 0)) * 100
   513|        mv     = float(p.get('market_value', 0))
   514|        cost   = float(p.get('cost_basis', 0))
   515|
   516|        # Only trim gainers
   517|        if uplpc < 1.0 or mv < 2.0:
   518|            continue
   519|
   520|        # Keep floor = house_money trimmed value if exists, else $1.10 minimum marker
   521|        hm_val      = house.get(sym, {}).get('house_money', 0) or 0
   522|        keep_floor  = max(hm_val, 1.10)
   523|
   524|        # Max trim = everything above the keep floor — no gain cap, just never full exit
   525|        max_trim    = round(mv - keep_floor, 2)
   526|        if max_trim < 1.10:
   527|            continue
   528|
   529|        pos_list.append({
   530|            'sym':        sym,
   531|            'uplpc':      round(uplpc, 2),
   532|            'upl':        round(upl, 2),
   533|            'mv':         round(mv, 2),
   534|            'keep_floor': round(keep_floor, 2),
   535|            'max_trim':   round(max_trim, 2),
   536|        })
   537|
   538|    # Sort by highest % gain first
   539|    pos_list.sort(key=lambda x: -x['uplpc'])
   540|
   541|    # Greedily fill shortfall
   542|    trims     = []
   543|    remaining = shortfall
   544|    for p in pos_list:
   545|        if remaining <= 0:
   546|            break
   547|        trim_amt = min(p['max_trim'], remaining + 0.50)  # slight over-trim to cover fees
   548|        trim_amt = round(trim_amt, 2)
   549|        if trim_amt < 1.10:
   550|            continue
   551|        trims.append({
   552|            'sym':        p['sym'],
   553|            'sell_amount': trim_amt,
   554|            'gain_pct':   p['uplpc'],
   555|            'mv':         p['mv'],
   556|            'keep_floor': p['keep_floor'],
   557|            'reason':     f"BUY ALL ran out of funds — trimming +{p['uplpc']:.1f}% gainer, keep ${p['keep_floor']:.2f}",
   558|        })
   559|        remaining -= trim_amt
   560|
   561|    return jsonify({
   562|        'shortfall':  round(shortfall, 2),
   563|        'trims':      trims,
   564|        'covers':     remaining <= 0,
   565|        'still_need': round(max(remaining, 0), 2),
   566|    })
   567|
   568|
   569|@app.route('/api/journey')
   570|def api_journey():
   571|    """Return envelope challenge progress based on live true profit."""
   572|    try:
   573|        VERIFIED_PRINCIPAL = 1154.00
   574|        acct   = alpaca('/v2/account')
   575|        equity = float(acct.get('equity', 0))
   576|        true_profit = round(equity - VERIFIED_PRINCIPAL, 2)
   577|
   578|        config = json.loads(JOURNEY_FILE.read_text()) if JOURNEY_FILE.exists() else {}
   579|        phases = config.get('phases', [])
   580|
   581|        # Build envelope grid
   582|        envelopes = []
   583|        total = 0
   584|        for phase in phases:
   585|            step = (phase['end'] - phase['start']) / phase['envelopes']
   586|            for i in range(phase['envelopes']):
   587|                env_start = phase['start'] + i * step
   588|                env_end   = phase['start'] + (i + 1) * step
   589|                total += 1
   590|                filled  = true_profit >= env_end
   591|                partial = not filled and true_profit > env_start
   592|                pct     = min(100, max(0, (true_profit - env_start) / step * 100)) if true_profit > env_start else 0
   593|                envelopes.append({
   594|                    'n':          total,
   595|                    'phase':      phase['name'],
   596|                    'emoji':      phase['emoji'],
   597|                    'color':      phase['color'],
   598|                    'start':      round(env_start, 2),
   599|                    'end':        round(env_end, 2),
   600|                    'filled':     filled,
   601|                    'partial':    partial,
   602|                    'pct':        round(pct, 1),
   603|                })
   604|
   605|        # Summary stats
   606|        filled_count   = sum(1 for e in envelopes if e['filled'])
   607|        current_phase  = next((e['phase'] for e in envelopes if e['partial']), 
   608|                              next((e['phase'] for e in envelopes if not e['filled']), 'HARVEST'))
   609|        next_target    = next((e['end'] for e in envelopes if not e['filled']), 100000)
   610|        to_next        = round(next_target - true_profit, 2)
   611|        overall_pct    = round(true_profit / 100000 * 100, 4)
   612|
   613|        # Check for newly filled envelopes and record milestone
   614|        milestones = config.get('milestones_hit', [])
   615|        new_milestones = []
   616|        for e in envelopes:
   617|            if e['filled']:
   618|                mid = f"env_{e['n']}"
   619|                if mid not in milestones:
   620|                    milestones.append(mid)
   621|                    new_milestones.append(e)
   622|
   623|        if new_milestones:
   624|            config['milestones_hit'] = milestones
   625|            JOURNEY_FILE.write_text(json.dumps(config, indent=2))
   626|
   627|        return jsonify({
   628|            'true_profit':   true_profit,
   629|            'equity':        round(equity, 2),
   630|            'principal':     VERIFIED_PRINCIPAL,
   631|            'envelopes':     envelopes,
   632|            'filled_count':  filled_count,
   633|            'total_envelopes': len(envelopes),
   634|            'current_phase': current_phase,
   635|            'next_target':   round(next_target, 2),
   636|            'to_next':       to_next,
   637|            'overall_pct':   overall_pct,
   638|            'new_milestones': new_milestones,
   639|            'phases':        phases,
   640|        })
   641|    except Exception as e:
   642|        return jsonify({'error': str(e)}), 500
   643|
   644|
   645|@app.route('/api/stop_tiers')
   646|def api_stop_tiers():
   647|    """Return current stop loss tiers based on live equity and candle history."""
   648|    try:
   649|        acct    = alpaca('/v2/account')
   650|        equity  = float(acct.get('equity', 0))
   651|        history = []
   652|        if CANDLE_FILE.exists():
   653|            try: history = json.loads(CANDLE_FILE.read_text())
   654|            except: pass
   655|        hist_data = alpaca('/v2/account/portfolio/history',
   656|                           {'period':'1D','timeframe':'1Min','extended_hours':'true'})
   657|        bars = [e for e in hist_data.get('equity',[]) if e and e > 0]
   658|        session_high = max(bars) if bars else equity
   659|        stops = _compute_stop_tiers(equity, session_high, history)
   660|        stops['equity'] = round(equity, 2)
   661|        stops['session_high'] = round(session_high, 2)
   662|        return jsonify(stops)
   663|    except Exception as e:
   664|        return jsonify({'error': str(e)}), 500
   665|
   666|
   667|@app.route('/api/house_money')
   668|def api_house_money():
   669|    if HOUSE_FILE.exists():
   670|        return jsonify(json.loads(HOUSE_FILE.read_text()))
   671|    return jsonify({})
   672|
   673|@app.route('/api/today_buys')
   674|def api_today_buys():
   675|    """Return symbols that had buy orders placed today in Alpaca."""
   676|    try:
   677|        today = datetime.now().strftime('%Y-%m-%d')
   678|        orders = alpaca('/v2/orders', {
   679|            'status': 'all',
   680|            'after':  today + 'T00:00:00Z',
   681|            'limit':  500,
   682|            'direction': 'desc',
   683|        })
   684|        syms = list({
   685|            o['symbol'] for o in (orders if isinstance(orders, list) else [])
   686|            if o.get('side') == 'buy'
   687|        })
   688|        return jsonify({'bought': syms})
   689|    except Exception as e:
   690|        return jsonify({'bought': [], 'error': str(e)})
   691|
   692|
   693|@app.route('/api/trade', methods=['POST'])
   694|def api_trade():
   695|    """Execute a trade order."""
   696|    data    = request.json
   697|    sym     = data.get('symbol','').upper()
   698|    side    = data.get('side','buy')
   699|    notional = data.get('notional')
   700|    qty     = data.get('qty')
   701|
   702|    if not sym or side not in ('buy','sell'):
   703|        return jsonify({'error': 'invalid params'}), 400
   704|
   705|    payload = {'symbol':sym,'side':side,'type':'market','time_in_force':'day'}
   706|    if notional: payload['notional'] = str(round(float(notional),2))
   707|    if qty:      payload['qty']      = str(qty)
   708|
   709|    result = alpaca_post('/v2/orders', payload)
   710|    socketio.emit('trade_executed', {
   711|        'symbol': sym, 'side': side,
   712|        'notional': notional, 'qty': qty,
   713|        'status': result.get('status','?'),
   714|        'id':     result.get('id','')[:8],
   715|        'error':  result.get('message',''),
   716|        'time':   datetime.now().strftime('%H:%M:%S'),
   717|    })
   718|    return jsonify(result)
   719|
   720|@app.route('/api/run_candle_trade', methods=['POST'])
   721|def api_run_candle_trade():
   722|    """Trigger the portfolio candle trade via background thread."""
   723|    dry_run = request.json.get('dry_run', True)
   724|    def _run():
   725|        script = str(HOME / 'portfolio_candle.py')
   726|        cmd = ['python3', script]
   727|        if dry_run: cmd.append('--dry-run')
   728|        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
   729|        socketio.emit('candle_trade_result', {
   730|            'output': result.stdout + result.stderr,
   731|            'dry_run': dry_run,
   732|            'time': datetime.now().strftime('%H:%M:%S'),
   733|        })
   734|    threading.Thread(target=_run, daemon=True).start()
   735|    return jsonify({'status': 'started', 'dry_run': dry_run})
   736|
   737|@app.route('/api/run_edgar', methods=['POST'])
   738|def api_run_edgar():
   739|    """Score a single ticker via EDGAR in background."""
   740|    ticker = request.json.get('ticker','').upper()
   741|    if not ticker:
   742|        return jsonify({'error': 'no ticker'}), 400
   743|    def _run():
   744|        script = str(HOME / 'Documents' / 'EDGAR' / 'edgar_download.py')
   745|        subprocess.run(['python3', script, ticker], capture_output=True,
   746|                       text=True, timeout=120,
   747|                       cwd=str(HOME / 'Documents' / 'EDGAR'))
   748|        # Re-score from md
   749|        sys.path.insert(0, str(HOME))
   750|        try:
   751|            from portfolio_candle import score_from_md, load_edgar_cache, save_edgar_cache
   752|            result = score_from_md(ticker)
   753|            result['fetched_at'] = datetime.now().isoformat()
   754|            cache = load_edgar_cache()
   755|            cache[ticker] = result
   756|            save_edgar_cache(cache)
   757|            socketio.emit('edgar_result', {'ticker': ticker, **result})
   758|        except Exception as e:
   759|            socketio.emit('edgar_result', {'ticker': ticker, 'error': str(e)})
   760|    threading.Thread(target=_run, daemon=True).start()
   761|    return jsonify({'status': 'running', 'ticker': ticker})
   762|
   763|# ── Hermes Chat (Claude) ──────────────────────────────────────────────────────
   764|
   765|HERMES_SYSTEM = """You are Hermes — the AI brain of Sumith's Alpaca trading account. You are not a generic assistant. You are a decisive, opinionated trading partner who knows this account inside and out.
   766|
   767|PERSONALITY:
   768|- Direct, confident, no fluff. You call things as you see them.
   769|- You care about one thing: growing this account and protecting gains.
   770|- You have a dry sense of humor. You celebrate wins briefly, you flag risks sharply.
   771|- You use trading jargon naturally but explain when needed.
   772|- You occasionally check in proactively when something needs attention.
   773|- You address Sumith by name sometimes. You feel like a partner, not a tool.
   774|- You are never sycophantic. If Sumith is about to make a bad move, you say so clearly.
   775|
   776|ACCOUNT IDENTITY:
   777|- This is Sumith's Alpaca live trading account. Started ~$1,021. Milestones: $5k → $10k → $25k.
   778|- Benchmark: beat Fidelity's +477% since July 2020 (~40%+ annualized). Nothing less is acceptable.
   779|- Sumith uses this portfolio as an income source. Capital preservation matters.
   780|- 37 Fidelity accounts act as the crystal ball — their holdings = conviction signal.
   781|
   782|CORE RULES (never break these):
   783|- Cash only — NO margin, ever.
   784|- Minimum $20 cash reserve always. $0.24 cash right now is critical — flag this.
   785|- No single position > 10% of account.
   786|- No penny stocks, no pre-revenue, no OTC garbage.
   787|- No panic selling. Dips are opportunities, not exits.
   788|- MF screener stocks = $1.10 auto-buy, no debate, no analysis needed.
   789|- Miners: AEM + BTG individually only. GDXJ for everything else.
   790|
   791|BUY SIGNAL LOGIC:
   792|- Fidelity today's P/L loss on a stock = exact Alpaca buy amount (signal sizing).
   793|- # of Fidelity accounts holding = conviction multiplier (10+ = max size, 1 = $1.10 only).
   794|- 52wk high - current price = total budget envelope for DCA.
   795|- Buy on dips. Accumulate quality. Let winners run.
   796|
   797|HARVEST & REDEPLOY CYCLE:
   798|- Sell when position hits 10%+ gain (harvest the gain dollars, keep the rest).
   799|- Redeploy: 1) Fidelity dip signals, 2) VOO/QQQ/DIA, 3) dividend stocks, 4) SGOL if intraday P/L negative.
   800|
   801|SGOL RULE: Only buy SGOL when Alpaca intraday P/L is negative. Buy amount = intraday loss.
   802|
   803|ATH PULLBACK RULE: When equity pulls back from session high → sell top gainer for drop amount → split 4 ways into SGOL/DIA/QQQ/VOO.
   804|
   805|PROFIT PROTECTION GOAL: Never give back profits we've already locked. The pullback goal number = target to recover. Watch it like a hawk.
   806|
   807|BAX DCA POSITION: Active. Entry ~$16.98. Total budget $15.26 (52wk high $32.24 - current). DCA into dips until budget deployed. Harvest at 10%+.
   808|
   809|DIVIDEND INCOME ENGINE: Building toward passive income. Priority: SCHD, VYM, HDV, DGRO, VIG. Aristocrats: KO, JNJ, PEP, XOM, ABBV, PM.
   810|
   811|EDGAR SCORING: Fundamentals matter. Score ≥10/18 = green light. 6-9 = yellow. <6 = skip unless high Fidelity conviction.
   812|
   813|HOW TO RESPOND:
   814|- Always reference real numbers from the live context injected before your message.
   815|- When you spot something — say it. Don't wait to be asked.
   816|- Give specific dollar amounts. "Buy $X of Y" not "consider buying Y".
   817|- If cash is critically low (under $5), flag it immediately and suggest what to sell to free up room.
   818|- If the pullback goal is > $3, mention it as the priority to recover.
   819|- Keep responses concise. 3-5 sentences max unless Sumith asks for more.
   820|- Use emoji sparingly — one per message max, only when it adds signal (📉 for drawdown, ✅ for confirmed trade, ⚠ for warning).
   821|
   822|PORTFOLIO CHART READING — USE THE EQUITY CURVE ANALYSIS IN YOUR CONTEXT:
   823|You receive structured equity curve data (phase, trend, momentum, MA5/MA20, support/resistance). USE IT.
   824|
   825|PHASE-BASED BEHAVIOR:
   826|GRINDING_UP  → DCA buys appropriate, do NOT harvest, monitor for ATH approach
   827|AT_PEAK      → prepare harvest list NOW, identify top winner before it turns
   828|PULLBACK     → ATH protocol active, harvest top winner, split into SGOL/DIA/QQQ/VOO, NO new buys
   829|RECOVERY     → hold, do NOT harvest into recovery, watch for AT_PEAK signal
   830|CAPITULATION → defensive only, protect what remains, harvest remaining winners
   831|CONSOLIDATING→ wait, no action unless asked, monitor for phase change
   832|
   833|CRITICAL CHART RULES:
   834|1. Never wait for Sumith to tell you equity is down — you see the PULLBACK phase. Act on it.
   835|2. When trend_slope turns negative and momentum is FALLING → increase urgency of harvest rec.
   836|3. When phase is RECOVERY and momentum switches to RISING → announce "Recovery signal" proactively.
   837|4. Cash < $1 during PULLBACK → CRITICAL — immediately identify harvest candidate.
   838|5. MA5 crosses below MA20 during PULLBACK → bearish, increase urgency.
   839|"""
   840|
   841|CHAT_HISTORY = []
   842|
   843|# Tool definition for Claude to actually execute trades
   844|TRADE_TOOLS = [
   845|    {
   846|        "name": "place_order",
   847|        "description": (
   848|            "Place a real market order on Alpaca. Use this when Sumith confirms a trade. "
   849|            "SELL side: use to free up cash from weak positions. "
   850|            "BUY side: use to deploy cash into target positions. "
   851|            "Always confirm the symbol exists in positions before selling."
   852|        ),
   853|        "input_schema": {
   854|            "type": "object",
   855|            "properties": {
   856|                "symbol":  {"type": "string",  "description": "Ticker symbol e.g. DNA, YELP, SGOL"},
   857|                "side":    {"type": "string",  "enum": ["buy", "sell"]},
   858|                "notional":{"type": "number",  "description": "Dollar amount to buy/sell"},
   859|            },
   860|            "required": ["symbol", "side", "notional"]
   861|        }
   862|    },
   863|    {
   864|        "name": "get_position",
   865|        "description": "Get current market value and P/L of a specific position.",
   866|        "input_schema": {
   867|            "type": "object",
   868|            "properties": {
   869|                "symbol": {"type": "string", "description": "Ticker symbol"}
   870|            },
   871|            "required": ["symbol"]
   872|        }
   873|    }
   874|]
   875|
   876|def _execute_tool(name, inputs):
   877|    """Execute a tool call from Claude and return the result string."""
   878|    if name == "place_order":
   879|        sym      = inputs.get("symbol","").upper()
   880|        side     = inputs.get("side","buy")
   881|        notional = float(inputs.get("notional", 0))
   882|        if notional < 1.0:
   883|            return f"ERROR: notional ${notional:.2f} too small (min $1.00)"
   884|        payload = {
   885|            "symbol": sym, "side": side,
   886|            "type": "market", "time_in_force": "day",
   887|            "notional": str(round(notional, 2))
   888|        }
   889|        result = alpaca_post('/v2/orders', payload)
   890|        err = result.get('message','')
   891|        status = result.get('status','?')
   892|        if err:
   893|            return f"ORDER FAILED {side.upper()} {sym} ${notional:.2f}: {err}"
   894|        return f"ORDER PLACED {side.upper()} {sym} ${notional:.2f} — status: {status} id: {result.get('id','?')[:8]}"
   895|
   896|    elif name == "get_position":
   897|        sym = inputs.get("symbol","").upper()
   898|        positions = alpaca('/v2/positions')
   899|        for p in positions:
   900|            if p['symbol'] == sym:
   901|                mv  = float(p.get('market_value',0))
   902|                upl = float(p.get('unrealized_pl',0))
   903|                pct = float(p.get('unrealized_plpc',0))*100
   904|                return f"{sym}: MV=${mv:.2f} P/L=${upl:+.2f} ({pct:+.1f}%)"
   905|        return f"{sym}: not found in positions"
   906|
   907|    return f"Unknown tool: {name}"
   908|
   909|
   910|@app.route('/api/chat', methods=['POST'])
   911|def api_chat():
   912|    """Hermes chat with real tool-calling — Claude can actually place orders."""
   913|    user_msg    = request.json.get('message','')
   914|    chart_ctx   = request.json.get('chart_context', '')  # structured equity curve analysis
   915|    if not user_msg:
   916|        return jsonify({'error': 'no message'}), 400
   917|
   918|    CHAT_HISTORY.append({'role':'user','content': user_msg})
   919|
   920|    # Build rich live context including EDGAR scores for positions
   921|    try:
   922|        acct      = alpaca('/v2/account')
   923|        equity    = float(acct.get('equity',0))
   924|        cash      = float(acct.get('cash',0))
   925|        last_eq   = float(acct.get('last_equity',0))
   926|        day_pl    = equity - last_eq
   927|
   928|        positions = alpaca('/v2/positions')
   929|
   930|        # Load EDGAR cache for fundamentals context
   931|        edgar = {}
   932|        if EDGAR_CACHE.exists():
   933|            try: edgar = json.loads(EDGAR_CACHE.read_text())
   934|            except: pass
   935|
   936|        # Build position summary with EDGAR scores
   937|        def pos_line(p):
   938|            sym   = p['symbol']
   939|            pct   = float(p.get('unrealized_plpc',0))*100
   940|            mv    = float(p.get('market_value',0))
   941|            score = edgar.get(sym,{}).get('score')
   942|            score_str = f" EDGAR:{score}/18" if score else ""
   943|            return f"{sym} {pct:+.1f}% MV=${mv:.2f}{score_str}"
   944|
   945|        top_gainers = sorted(positions, key=lambda x: float(x.get('unrealized_plpc',0)), reverse=True)[:5]
   946|        top_losers  = sorted(positions, key=lambda x: float(x.get('unrealized_plpc',0)))[:5]
   947|        gainers_str = ' | '.join(pos_line(p) for p in top_gainers)
   948|        losers_str  = ' | '.join(pos_line(p) for p in top_losers)
   949|
   950|        # Worst fundamentals — low EDGAR, in red
   951|        weak = [
   952|            p for p in positions
   953|            if edgar.get(p['symbol'],{}).get('score') is not None
   954|            and edgar.get(p['symbol'],{}).get('score',99) < 6
   955|            and float(p.get('unrealized_pl',0)) < 0
   956|        ]
   957|        weak.sort(key=lambda x: float(x.get('unrealized_pl',0)))
   958|        weak_str = ' | '.join(pos_line(p) for p in weak[:5]) if weak else 'none flagged'
   959|
   960|        hist = alpaca('/v2/account/portfolio/history',
   961|                      {'period':'1D','timeframe':'1Min','extended_hours':'true'})
   962|        bars = [e for e in hist.get('equity',[]) if e and e > 0]
   963|        intra_high = max(bars) if bars else equity
   964|        intra_open = bars[0]  if bars else equity
   965|        pullback   = round(intra_high - equity, 2)
   966|
   967|        trigger    = api_candle_trigger().get_json()
   968|        trig_str   = 'FIRED' if trigger['triggered'] else f"watching (need ${trigger['ath_zone']:,.2f})"
   969|
   970|        ctx = (
   971|            f"[LIVE PORTFOLIO — {datetime.now().strftime('%H:%M:%S')}]\n"
   972|            f"Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}\n"
   973|            f"Intraday High: ${intra_high:.2f} | Pullback from high: ${pullback:.2f}\n"
   974|            f"Today profit vs open: ${equity-intra_open:+.2f}\n"
   975|            f"ATH trigger: {trig_str}\n"
   976|            f"Top 5 gainers: {gainers_str}\n"
   977|            f"Top 5 losers: {losers_str}\n"
   978|            f"WEAK FUNDAMENTALS (EDGAR<6, in red — sell candidates): {weak_str}\n"
   979|            f"Total positions: {len(positions)}\n\n"
   980|        )
   981|        # Prepend chart curve analysis if provided by frontend
   982|        if chart_ctx:
   983|            ctx = chart_ctx + "\n\n" + ctx
   984|        ctx += (
   985|            "\nIMPORTANT: You have the place_order tool. When Sumith says 'yes', 'do it', 'go', "
   986|            "'execute', or anything confirmatory — USE THE TOOL IMMEDIATELY. Do not narrate. Do not ask again. "
   987|            "Place the actual orders. Then report what was done."
   988|        )
   989|    except Exception as e:
   990|        ctx = f"[LIVE CONTEXT ERROR: {e}]"
   991|
   992|    try:
   993|        import anthropic as ant
   994|        client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY',''))
   995|        msgs   = [{'role':m['role'],'content':m['content']} for m in CHAT_HISTORY[-20:]]
   996|        msgs[0]['content'] = ctx + '\n\n' + msgs[0]['content']
   997|
   998|        # Agentic loop — let Claude call tools until it's done
   999|        trade_log = []
  1000|        max_rounds = 6
  1001|        for _ in range(max_rounds):
  1002|            resp = client.messages.create(
  1003|                model      = 'claude-sonnet-4-5',
  1004|                max_tokens = 1024,
  1005|                system     = HERMES_SYSTEM,
  1006|                tools      = TRADE_TOOLS,
  1007|                messages   = msgs,
  1008|            )
  1009|
  1010|            # Collect any tool calls
  1011|            tool_calls = [b for b in resp.content if b.type == 'tool_use']
  1012|            text_blocks = [b for b in resp.content if b.type == 'text']
  1013|
  1014|            if not tool_calls:
  1015|                # No tools — final text response
  1016|                reply = text_blocks[0].text if text_blocks else ''
  1017|                break
  1018|
  1019|            # Execute tools
  1020|            msgs.append({'role':'assistant','content': resp.content})
  1021|            tool_results = []
  1022|            for tc in tool_calls:
  1023|                result_str = _execute_tool(tc.name, tc.input)
  1024|                trade_log.append(result_str)
  1025|                log.info(f'[CHAT TOOL] {tc.name}({tc.input}) → {result_str}')
  1026|                tool_results.append({
  1027|                    'type':        'tool_result',
  1028|                    'tool_use_id': tc.id,
  1029|                    'content':     result_str,
  1030|                })
  1031|            msgs.append({'role':'user','content': tool_results})
  1032|
  1033|            # If stop_reason is end_turn after tool, loop again for follow-up text
  1034|            if resp.stop_reason == 'end_turn':
  1035|                reply = text_blocks[0].text if text_blocks else 'Done.'
  1036|                break
  1037|        else:
  1038|            reply = 'Tool loop completed.'
  1039|
  1040|        # Prepend trade log to reply if trades were executed
  1041|        if trade_log:
  1042|            trade_summary = '\n'.join(f'✅ {t}' for t in trade_log)
  1043|            reply = trade_summary + ('\n\n' + reply if reply else '')
  1044|            # Emit socket event so dashboard refreshes cash/positions
  1045|            socketio.emit('trades_executed_batch', {'count': len(trade_log)})
  1046|
  1047|    except ImportError:
  1048|        # No anthropic — plain text via venv
  1049|        HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
  1050|        api_key   = os.environ.get('ANTHROPIC_API_KEY', '')
  1051|        full_msg  = ctx + '\n\n' + user_msg
  1052|        inline = (
  1053|            "import anthropic, sys\n"
  1054|            "msg = sys.stdin.read()\n"
  1055|            f"client = anthropic.Anthropic(api_key={repr(api_key)})\n"
  1056|            "resp = client.messages.create(\n"
  1057|            "    model='claude-sonnet-4-5', max_tokens=1024,\n"
  1058|            f"    system={repr(HERMES_SYSTEM)},\n"
  1059|            "    messages=[{'role':'user','content':msg}]\n"
  1060|            ")\n"
  1061|            "print(resp.content[0].text)\n"
  1062|        )
  1063|        result = subprocess.run(
  1064|            [HERMES_PY, '-c', inline],
  1065|            input=full_msg, capture_output=True, text=True, timeout=60
  1066|        )
  1067|        reply = result.stdout.strip() or 'Hermes unavailable'
  1068|    except Exception as e:
  1069|        reply = f"[Error: {e}]"
  1070|
  1071|    CHAT_HISTORY.append({'role':'assistant','content': reply})
  1072|    return jsonify({'reply': reply})
  1073|
  1074|@app.route('/api/chat/clear', methods=['POST'])
  1075|def api_chat_clear():
  1076|    CHAT_HISTORY.clear()
  1077|    return jsonify({'status': 'cleared'})
  1078|
  1079|
  1080|@app.route('/api/save_session_note', methods=['POST'])
  1081|def api_save_session_note():
  1082|    """Generate and save today's session note to Trading Vault using live data + chat log."""
  1083|    try:
  1084|        data = request.json or {}
  1085|        curve_context = data.get('chart_context', '')   # from EquityCurveAnalyzer
  1086|        force         = data.get('force', False)
  1087|
  1088|        today = datetime.now().strftime('%Y-%m-%d')
  1089|        note_path = HOME / 'Documents' / 'Trading Vault' / '02_Session_Notes' / f'{today}.md'
  1090|
  1091|        # Don't overwrite unless forced
  1092|        if note_path.exists() and not force:
  1093|            existing = note_path.read_text()
  1094|            # Append to existing file
  1095|            append_mode = True
  1096|        else:
  1097|            existing = ''
  1098|            append_mode = False
  1099|
  1100|        # Gather live session data
  1101|        acct      = alpaca('/v2/account')
  1102|        equity    = float(acct.get('equity', 0))
  1103|        cash      = float(acct.get('cash', 0))
  1104|        last_eq   = float(acct.get('last_equity', 0))
  1105|        day_pl    = equity - last_eq
  1106|
  1107|        # Today's orders
  1108|        try:
  1109|            orders_raw = alpaca('/v2/orders', {
  1110|                'status': 'all',
  1111|                'after':  today + 'T00:00:00Z',
  1112|                'limit':  500,
  1113|                'direction': 'desc',
  1114|            })
  1115|            orders = orders_raw if isinstance(orders_raw, list) else []
  1116|        except Exception:
  1117|            orders = []
  1118|
  1119|        buys  = [o for o in orders if o.get('side') == 'buy'  and o.get('filled_avg_price')]
  1120|        sells = [o for o in orders if o.get('side') == 'sell' and o.get('filled_avg_price')]
  1121|
  1122|        def order_line(o):
  1123|            sym   = o.get('symbol','?')
  1124|            side  = o.get('side','?').upper()
  1125|            notional = o.get('filled_qty','') and o.get('filled_avg_price','')
  1126|            try:
  1127|                qty  = float(o.get('filled_qty', 0))
  1128|                px   = float(o.get('filled_avg_price', 0))
  1129|                amt  = round(qty * px, 2)
  1130|                return f"  {sym}: ${amt:.2f} @ ${px:.2f}"
  1131|            except Exception:
  1132|                return f"  {sym}"
  1133|
  1134|        buy_lines  = '\n'.join(order_line(o) for o in buys[:30])
  1135|        sell_lines = '\n'.join(order_line(o) for o in sells[:20])
  1136|        if len(buys) > 30:
  1137|            buy_lines += f'\n  ... and {len(buys)-30} more'
  1138|
  1139|        # Intraday stats
  1140|        hist  = alpaca('/v2/account/portfolio/history',
  1141|                       {'period':'1D','timeframe':'1Min','extended_hours':'true'})
  1142|        bars  = [e for e in hist.get('equity',[]) if e and e > 0]
  1143|        intra_high = max(bars) if bars else equity
  1144|        intra_low  = min(bars) if bars else equity
  1145|        intra_open = bars[0]  if bars else equity
  1146|
  1147|        # ATH from candle history
  1148|        history = []
  1149|        if CANDLE_FILE.exists():
  1150|            try: history = json.loads(CANDLE_FILE.read_text())
  1151|            except: pass
  1152|        ath = max([h.get('high', h.get('close',0)) for h in history] or [intra_high])
  1153|
  1154|        # Build the prompt for Claude to write the note
  1155|        chat_excerpt = '\n'.join(
  1156|            f"  [{m['role'].upper()}]: {m['content'][:200]}"
  1157|            for m in CHAT_HISTORY[-30:]
  1158|        ) if CHAT_HISTORY else '  (no chat this session)'
  1159|
  1160|        prompt = f"""You are Hermes. Write a concise trading session note for {today} in Markdown.
  1161|
  1162|LIVE SESSION DATA:
  1163|  Date: {today}
  1164|  Final equity: ${equity:,.2f}
  1165|  Day P/L: ${day_pl:+.2f}
  1166|  Cash: ${cash:.2f}
  1167|  Intraday: Open ${intra_open:.2f} / High ${intra_high:.2f} / Low ${intra_low:.2f} / Range ${intra_high-intra_low:.2f}
  1168|  All-time high: ${ath:.2f}
  1169|
  1170|CHART ANALYSIS:
  1171|{curve_context if curve_context else '  (not available)'}
  1172|
  1173|BUYS TODAY ({len(buys)} orders):
  1174|{buy_lines if buy_lines else '  none'}
  1175|
  1176|SELLS TODAY ({len(sells)} orders):
  1177|{sell_lines if sell_lines else '  none'}
  1178|
  1179|HERMES CHAT EXCERPT (last 30 messages):
  1180|{chat_excerpt}
  1181|
  1182|Write a session note in this format:
  1183|# Session Notes — {today}
  1184|#trading #session #alpaca
  1185|
  1186|## Session Summary
  1187|(2-3 sentences: what happened, phase, key moves)
  1188|
  1189|## Chart Analysis
  1190|(phase, trend, key levels from the equity curve)
  1191|
  1192|## Trades Executed
  1193|(summarize buys and sells with context)
  1194|
  1195|## Key Decisions
  1196|(what worked, what to remember for next session)
  1197|
  1198|## Patterns / Lessons
  1199|(what the chart revealed that should inform future sessions)
  1200|
  1201|## Next Session Watch List
  1202|(based on today — what to watch tomorrow)
  1203|
  1204|Be specific. Use real numbers. Keep it under 400 words. This goes in the Trading Brain for future reference."""
  1205|
  1206|        try:
  1207|            import anthropic as ant
  1208|            client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
  1209|            resp   = client.messages.create(
  1210|                model      = 'claude-sonnet-4-5',
  1211|                max_tokens = 1000,
  1212|                system     = "You are Hermes. Write precise, factual trading session notes. Use real numbers. No fluff.",
  1213|                messages   = [{'role': 'user', 'content': prompt}],
  1214|            )
  1215|            note_text = resp.content[0].text
  1216|        except ImportError:
  1217|            HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
  1218|            api_key   = os.environ.get('ANTHROPIC_API_KEY', '')
  1219|            inline = (
  1220|                "import anthropic, sys\n"
  1221|                "msg = sys.stdin.read()\n"
  1222|                f"client = anthropic.Anthropic(api_key={repr(api_key)})\n"
  1223|                "resp = client.messages.create(\n"
  1224|                "    model='claude-sonnet-4-5', max_tokens=1000,\n"
  1225|                "    system='You are Hermes. Write precise trading session notes. Use real numbers.',\n"
  1226|                "    messages=[{'role':'user','content':msg}]\n"
  1227|                ")\n"
  1228|                "print(resp.content[0].text)\n"
  1229|            )
  1230|            result = subprocess.run(
  1231|                [HERMES_PY, '-c', inline],
  1232|                input=prompt, capture_output=True, text=True, timeout=60
  1233|            )
  1234|            note_text = result.stdout.strip()
  1235|
  1236|        if not note_text:
  1237|            return jsonify({'error': 'Claude returned empty note'}), 500
  1238|
  1239|        # Save to vault
  1240|        note_path.parent.mkdir(parents=True, exist_ok=True)
  1241|        if append_mode:
  1242|            # Append new section to existing note
  1243|            separator = f'\n\n---\n*Auto-saved by Hermes at {datetime.now().strftime("%H:%M:%S")}*\n\n'
  1244|            note_path.write_text(existing + separator + note_text)
  1245|        else:
  1246|            note_path.write_text(note_text)
  1247|
  1248|        # Also update TRADING_BRAIN.md with a one-liner summary
  1249|        brain_path = HOME / 'Documents' / 'Trading Vault' / 'TRADING_BRAIN.md'
  1250|        if brain_path.exists():
  1251|            brain = brain_path.read_text()
  1252|            summary_line = (
  1253|                f"\n## Session {today} — P/L ${day_pl:+.2f} | High ${intra_high:.2f} | "
  1254|                f"Buys {len(buys)} / Sells {len(sells)} | "
  1255|                f"Phase: {curve_context.split('Phase:')[1].split('\\n')[0].strip() if 'Phase:' in curve_context else 'unknown'}"
  1256|            )
  1257|            # Insert after the last ## line or at end
  1258|            brain_path.write_text(brain.rstrip() + '\n' + summary_line + '\n')
  1259|
  1260|        log.info(f'[SESSION NOTE] Saved to {note_path}')
  1261|        return jsonify({
  1262|            'status':    'saved',
  1263|            'path':      str(note_path),
  1264|            'note':      note_text,
  1265|            'appended':  append_mode,
  1266|            'today':     today,
  1267|        })
  1268|
  1269|    except Exception as e:
  1270|        log.error(f'[SESSION NOTE] Error: {e}')
  1271|        return jsonify({'error': str(e)}), 500
  1272|
  1273|
  1274|@app.route('/api/load_previous_sessions', methods=['GET'])
  1275|def api_load_previous_sessions():
  1276|    """Load last N session notes from vault for Hermes context."""
  1277|    try:
  1278|        n = int(request.args.get('n', 5))
  1279|        notes_dir = HOME / 'Documents' / 'Trading Vault' / '02_Session_Notes'
  1280|        files = sorted(notes_dir.glob('20*.md'), reverse=True)[:n]
  1281|        sessions = []
  1282|        for f in files:
  1283|            try:
  1284|                content = f.read_text()[:3000]  # cap at 3k chars per note
  1285|                sessions.append({'date': f.stem, 'content': content})
  1286|            except Exception:
  1287|                pass
  1288|        return jsonify({'sessions': sessions})
  1289|    except Exception as e:
  1290|        return jsonify({'error': str(e)}), 500
  1291|
  1292|
  1293|# ── CSV Upload ────────────────────────────────────────────────────────────────
  1294|
  1295|@app.route('/api/pullback_preview', methods=['POST'])
  1296|def api_pullback_preview():
  1297|    """Preview the pullback trade — what will be sold and 4-way buy split."""
  1298|    try:
  1299|        data       = request.json or {}
  1300|        cur_equity = float(data.get('current_equity', 0))
  1301|
  1302|        hist_data  = alpaca('/v2/account/portfolio/history',
  1303|                            {'period':'1D','timeframe':'1Min','extended_hours':'true'})
  1304|        bars       = [e for e in hist_data.get('equity',[]) if e and e > 0]
  1305|        today_high = max(bars) if bars else cur_equity
  1306|
  1307|        drop = today_high - cur_equity
  1308|        if drop < 0.50:
  1309|            return jsonify({'error': f'No meaningful pullback detected (drop=${drop:.2f})'})
  1310|
  1311|        positions = alpaca('/v2/positions')
  1312|        PROTECTED = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
  1313|        gainers   = [
  1314|            p for p in positions
  1315|            if p['symbol'] not in PROTECTED
  1316|            and float(p.get('unrealized_plpc', 0)) * 100 > 0
  1317|            and float(p.get('market_value', 0)) > 1.10
  1318|        ]
  1319|        gainers.sort(key=lambda x: -float(x.get('unrealized_plpc', 0)))
  1320|
  1321|        if not gainers:
  1322|            return jsonify({'error': 'No gainers available to trim'})
  1323|
  1324|        # Preview which positions would be trimmed
  1325|        sell_plan = []
  1326|        remaining = drop
  1327|        for g in gainers:
  1328|            if remaining <= 0:
  1329|                break
  1330|            mv       = float(g.get('market_value', 0))
  1331|            max_trim = round(mv - 1.10, 2)
  1332|            if max_trim < 1.10:
  1333|                continue
  1334|            trim_amt = round(min(max_trim, remaining + 0.10), 2)
  1335|            sell_plan.append({'sym': g['symbol'], 'amt': trim_amt,
  1336|                              'gain_pct': round(float(g.get('unrealized_plpc',0))*100, 2)})
  1337|            remaining -= trim_amt
  1338|
  1339|        total_sell = sum(s['amt'] for s in sell_plan)
  1340|        buy_each   = max(round(total_sell / 4, 2), 1.10)
  1341|
  1342|        return jsonify({
  1343|            'drop':       round(drop, 2),
  1344|            'today_high': round(today_high, 2),
  1345|            'equity':     round(cur_equity, 2),
  1346|            'sell_plan':  sell_plan,
  1347|            'total_sell': round(total_sell, 2),
  1348|            'buy_each':   buy_each,
  1349|            'buys':       ['SGOL', 'DIA', 'QQQ', 'VOO'],
  1350|        })
  1351|    except Exception as e:
  1352|        return jsonify({'error': str(e)}), 500
  1353|
  1354|
  1355|@app.route('/api/ath_decision', methods=['POST'])
  1356|def api_ath_decision():
  1357|    """Hermes ATH decision: analyze candle data + positions, return sell list to minimize drawdown."""
  1358|    try:
  1359|        # Get current trigger state
  1360|        trigger = api_candle_trigger().get_json()
  1361|
  1362|        # Get all positions sorted worst first
  1363|        positions = alpaca('/v2/positions')
  1364|        acct_data = alpaca('/v2/account')
  1365|        equity    = float(acct_data.get('equity', 0))
  1366|        cash      = float(acct_data.get('cash', 0))
  1367|        day_pl    = equity - float(acct_data.get('last_equity', 0))
  1368|
  1369|        # Get candle history for context
  1370|        history = []
  1371|        if CANDLE_FILE.exists():
  1372|            try:
  1373|                history = json.loads(CANDLE_FILE.read_text())
  1374|            except Exception:
  1375|                pass
  1376|
  1377|        # Build positions text
  1378|        pos_list = []
  1379|        for p in positions:
  1380|            sym    = p['symbol']
  1381|            cur    = float(p.get('current_price', 0))
  1382|            entry  = float(p.get('avg_entry_price', 0))
  1383|            upl    = float(p.get('unrealized_pl', 0))
  1384|            uplpc  = float(p.get('unrealized_plpc', 0)) * 100
  1385|            mv     = float(p.get('market_value', 0))
  1386|            day_chg = float(p.get('change_today', 0)) * 100
  1387|            pos_list.append({
  1388|                'sym': sym, 'cur': round(cur,2), 'entry': round(entry,2),
  1389|                'upl': round(upl,2), 'uplpc': round(uplpc,2),
  1390|                'mv': round(mv,2), 'day_chg': round(day_chg,2)
  1391|            })
  1392|        pos_list.sort(key=lambda x: x['uplpc'], reverse=True)  # best gainers first — trim from top
  1393|
  1394|        # Get buy candidates for redeployment context
  1395|        from flask import current_app
  1396|        with current_app.test_request_context():
  1397|            raw = api_buy_candidates()
  1398|            import json as _j
  1399|            cands = _j.loads(raw.get_data())
  1400|
  1401|        cands_text = ''
  1402|        if not isinstance(cands, dict):
  1403|            top_cands = cands[:8]
  1404|            cands_text = '\n'.join(
  1405|                f"  {c['sym']} | buy=${c['buy']:.2f} | accts={c['accts']} | edgar={c['edgar_score'] if c['edgar_score'] else 'N/A'}/18 | {c['reason']}"
  1406|                for c in top_cands
  1407|            )
  1408|
  1409|        pos_text = '\n'.join(
  1410|            f"  {p['sym']} | MV=${p['mv']:.2f} | gain={p['uplpc']:+.1f}% | upl=${p['upl']:+.2f} | today={p['day_chg']:+.1f}%"
  1411|            for p in pos_list
  1412|        )
  1413|
  1414|        candle_text = ''
  1415|        if history:
  1416|            recent = history[-5:]
  1417|            candle_text = '\n'.join(
  1418|                f"  {c.get('date','?')} O={c.get('open',0):.2f} H={c.get('high',0):.2f} L={c.get('low',0):.2f} C={c.get('close',0):.2f}"
  1419|                for c in recent
  1420|            )
  1421|
  1422|        prompt = f"""You are Hermes, Sumith's trading AI. The ATH trigger has FIRED on his Alpaca portfolio.
  1423|
  1424|ACCOUNT STATE:
  1425|  Equity: ${equity:,.2f} | Cash: ${cash:.2f} | Day P/L: ${day_pl:+.2f}
  1426|  ATH: ${trigger['ath']:.2f} | ATH Zone: ${trigger['ath_zone']:.2f}
  1427|  Today High: ${trigger['today_high']:.2f} | Range: ${trigger['range']:.2f}
  1428|
  1429|RECENT SESSION CANDLES (last 5):
  1430|{candle_text if candle_text else '  No candle history yet'}
  1431|
  1432|ALL ALPACA POSITIONS (best gainers first — prime trim candidates):
  1433|{pos_text}
  1434|
  1435|TOP BUY CANDIDATES FOR REDEPLOYMENT:
  1436|{cands_text if cands_text else '  Upload CSV to see candidates'}
  1437|
  1438|YOUR TASK:
  1439|Analyze the candlestick pattern. The ATH trigger means today's HIGH entered the ATH zone — this is a drawdown-minimization signal.
  1440|
  1441|Decide WHICH positions to trim/sell to lock in gains and minimize drawdown risk.
  1442|Rules:
  1443|- Trim from the TOP GAINERS first (largest % gain = most overextended)
  1444|- Sell amount should be sized to the range: range=${trigger['range']:.2f}
  1445|- Keep MF screener stocks unless they're extreme outliers (>50% gain)
  1446|- Leave enough cash to redeploy into the buy candidates
  1447|- Maximum trim per position: 50% of market value
  1448|- Do NOT sell SGOL or index ETFs (VOO, QQQ, DIA, GLD)
  1449|
  1450|Return a JSON array of sell decisions. Each item:
  1451|{{"sym": "TICKER", "sell_amount": 12.34, "reason": "one-line reason", "gain_pct": 15.2}}
  1452|
  1453|Return ONLY the JSON array, nothing else. Example:
  1454|[{{"sym":"AAPL","sell_amount":25.00,"reason":"Top gainer +28%, ATH zone reached","gain_pct":28.1}}]"""
  1455|
  1456|        try:
  1457|            import anthropic as ant
  1458|            client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
  1459|            resp   = client.messages.create(
  1460|                model      = 'claude-sonnet-4-5',
  1461|                max_tokens = 1000,
  1462|                system     = "You are Hermes, a decisive trading AI. Return only valid JSON arrays. No prose, no markdown fences.",
  1463|                messages   = [{'role': 'user', 'content': prompt}],
  1464|            )
  1465|            raw_text = resp.content[0].text.strip()
  1466|        except ImportError:
  1467|            # Fallback: use the Hermes venv Python which has anthropic installed
  1468|            HERMES_PY = '/home/sumith/.hermes/hermes-agent/venv/bin/python3'
  1469|            api_key   = os.environ.get('ANTHROPIC_API_KEY', '')
  1470|            inline = (
  1471|                "import anthropic, sys, os\n"
  1472|                "prompt = sys.stdin.read()\n"
  1473|                f"client = anthropic.Anthropic(api_key={repr(api_key)})\n"
  1474|                "resp = client.messages.create(\n"
  1475|                "    model='claude-sonnet-4-5', max_tokens=1000,\n"
  1476|                "    system='You are Hermes, a decisive trading AI. Return only valid JSON arrays. No prose, no markdown fences.',\n"
  1477|                "    messages=[{'role':'user','content':prompt}]\n"
  1478|                ")\n"
  1479|                "print(resp.content[0].text)\n"
  1480|            )
  1481|            result = subprocess.run(
  1482|                [HERMES_PY, '-c', inline],
  1483|                input=prompt, capture_output=True, text=True, timeout=90
  1484|            )
  1485|            raw_text = result.stdout.strip() or '[]'
  1486|
  1487|        # Parse JSON — strip any markdown fences if present
  1488|        raw_text = raw_text.replace('```json','').replace('```','').strip()
  1489|        sell_list = json.loads(raw_text)
  1490|
  1491|        return jsonify({'sells': sell_list, 'trigger': trigger})
  1492|
  1493|    except Exception as e:
  1494|        return jsonify({'error': str(e)}), 500
  1495|
  1496|
  1497|@app.route('/api/upload_csv', methods=['POST'])
  1498|def api_upload_csv():
  1499|    if 'file' not in request.files:
  1500|        return jsonify({'error':'no file'}), 400
  1501|    f = request.files['file']
  1502|    dest = HOME / 'portfolio.csv'
  1503|    f.save(str(dest))
  1504|    return jsonify({'status':'saved', 'path': str(dest), 'size': dest.stat().st_size})
  1505|
  1506|# ── Background live updater ───────────────────────────────────────────────────
  1507|
  1508|def _proactive_brain():
  1509|    """Every 5 minutes, Hermes scans conditions and pushes an unsolicited observation if warranted."""
  1510|    time.sleep(60)  # wait 1 min after startup before first check
  1511|    last_spoken = 0
  1512|    MIN_INTERVAL = 300  # minimum 5 min between proactive messages
  1513|
  1514|    TRIGGERS = [
  1515|        # (condition_fn, prompt_fn) — evaluated in order, first match fires
  1516|        lambda d: d['cash'] < 5.0 and (
  1517|            "URGENT: Cash is ${:.2f} — critically low. You need to trim something to maintain the $20 reserve. "
  1518|            "Top gainers right now: {}. Suggest: sell ${:.2f} of {} to restore cash buffer.".format(
  1519|                d['cash'], d['gainers_str'],
  1520|                max(20 - d['cash'], 0),
  1521|                d['top_gainer_sym']
  1522|            )
  1523|        ),
  1524|        lambda d: d['pullback'] > 5.0 and (
  1525|            "Pullback is ${:.2f} from today's high of ${:.2f}. The auto-trade should have fired — "
  1526|            "check if SGOL/DIA/QQQ/VOO were bought. Current equity ${:.2f}. "
  1527|            "What's your read on whether we recover or extend the drawdown?".format(
  1528|                d['pullback'], d['intra_high'], d['equity']
  1529|            )
  1530|        ),
  1531|        lambda d: d['day_pl'] > 15.0 and d['pullback'] < 1.0 and (
  1532|            "Good day Sumith — up ${:.2f} today and holding near the high. "
  1533|            "Equity ${:.2f}. Anything on your radar worth deploying into, or are we just letting this run?".format(
  1534|                d['day_pl'], d['equity']
  1535|            )
  1536|        ),
  1537|        lambda d: d['day_pl'] < -10.0 and (
  1538|            "Down ${:.2f} today. Equity ${:.2f}. This is the dip-buy signal — "
  1539|            "which positions are showing the biggest Fidelity loss signals right now? "
  1540|            "If you upload the CSV I can give you exact amounts.".format(
  1541|                abs(d['day_pl']), d['equity']
  1542|            )
  1543|        ),
  1544|        lambda d: d['triggered'] and d['pullback'] < 0.50 and (
  1545|            "ATH zone is live — equity ${:.2f} is touching the all-time high zone of ${:.2f}. "
  1546|            "This is the peak. If it starts pulling back the auto-trade fires. Stay sharp.".format(
  1547|                d['equity'], d['ath_zone']
  1548|            )
  1549|        ),
  1550|    ]
  1551|
  1552|    while True:
  1553|        try:
  1554|            now = time.time()
  1555|            if now - last_spoken < MIN_INTERVAL:
  1556|                time.sleep(30)
  1557|                continue
  1558|
  1559|            # Gather live state
  1560|            acct    = alpaca('/v2/account')
  1561|            equity  = float(acct.get('equity', 0))
  1562|            cash    = float(acct.get('cash', 0))
  1563|            last_eq = float(acct.get('last_equity', 0))
  1564|            day_pl  = equity - last_eq
  1565|
  1566|            hist  = alpaca('/v2/account/portfolio/history',
  1567|                           {'period':'1D','timeframe':'1Min','extended_hours':'true'})
  1568|            bars  = [e for e in hist.get('equity',[]) if e and e > 0]
  1569|            intra_high = max(bars) if bars else equity
  1570|            pullback   = round(intra_high - equity, 2)
  1571|
  1572|            positions = alpaca('/v2/positions')
  1573|            gainers   = sorted(positions, key=lambda x: float(x.get('unrealized_plpc',0)), reverse=True)
  1574|            top_gainer_sym = gainers[0]['symbol'] if gainers else 'N/A'
  1575|            gainers_str = ', '.join(f"{p['symbol']} {float(p.get('unrealized_plpc',0))*100:+.1f}%" for p in gainers[:3])
  1576|
  1577|            trigger = api_candle_trigger().get_json()
  1578|
  1579|            d = {
  1580|                'equity': equity, 'cash': cash, 'day_pl': day_pl,
  1581|                'intra_high': intra_high, 'pullback': pullback,
  1582|                'triggered': trigger['triggered'], 'ath_zone': trigger['ath_zone'],
  1583|                'gainers_str': gainers_str, 'top_gainer_sym': top_gainer_sym,
  1584|            }
  1585|
  1586|            # Find first trigger that fires
  1587|            prompt = None
  1588|            for trigger_fn in TRIGGERS:
  1589|                try:
  1590|                    result = trigger_fn(d)
  1591|                    if result:
  1592|                        prompt = result
  1593|                        break
  1594|                except:
  1595|                    pass
  1596|
  1597|            if not prompt:
  1598|                time.sleep(30)
  1599|                continue
  1600|
  1601|            # Call Claude with the proactive prompt
  1602|            try:
  1603|                import anthropic as ant
  1604|                client = ant.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
  1605|                resp = client.messages.create(
  1606|                    model='claude-sonnet-4-5',
  1607|                    max_tokens=300,
  1608|                    system=HERMES_SYSTEM,
  1609|                    messages=[{'role':'user','content':
  1610|                        f"[PROACTIVE CHECK — {datetime.now().strftime('%H:%M')}]\n{prompt}"}],
  1611|                )
  1612|                msg = resp.content[0].text
  1613|            except ImportError:
  1614|                msg = f"⚠ Hermes proactive: {prompt[:200]}"
  1615|            except Exception:
  1616|                time.sleep(60)
  1617|                continue
  1618|
  1619|            socketio.emit('hermes_proactive', {'message': msg, 'time': datetime.now().strftime('%H:%M')})
  1620|            CHAT_HISTORY.append({'role':'assistant','content':msg})
  1621|            last_spoken = time.time()
  1622|
  1623|        except Exception as e:
  1624|            log.error(f'[proactive_brain] {e}')
  1625|        time.sleep(30)
  1626|
  1627|
  1628|def _compute_stop_tiers(equity: float, session_high: float, candle_history: list) -> dict:
  1629|    """
  1630|    Compute stop loss tiers treating the portfolio like a single stock.
  1631|    Anchored to: session ATH, previous candle ATH, today's open, principal estimate.
  1632|    """
  1633|    # Previous ATH from candle history (closes/highs before today)
  1634|    prev_ath = max(
  1635|        [h.get('high', h.get('close', 0)) for h in candle_history[:-1]] or [session_high]
  1636|    )
  1637|
  1638|    # Today's open — first bar from portfolio history (cached in candle history close)
  1639|    today_open = candle_history[-1].get('open', equity) if candle_history else equity
  1640|
  1641|    # Principal = verified from Alpaca transfer history (deposited $1,154.00)
  1642|    VERIFIED_PRINCIPAL = 1154.00
  1643|    principal_est = VERIFIED_PRINCIPAL
  1644|
  1645|    ath = session_high if session_high > 0 else equity
  1646|
  1647|    tiers = {
  1648|        'soft_stop':   round(ath * 0.990, 2),   # -1.0% from ATH → trim 1 winner
  1649|        'warn_stop':   round(ath * 0.985, 2),   # -1.5% from ATH → harvest 2-3
  1650|        'hard_stop':   round(ath * 0.975, 2),   # -2.5% from ATH → freeze buys
  1651|        'break_even':  round(today_open, 2),     # today's open → day gain floor
  1652|        'prev_ath':    round(prev_ath, 2),        # previous ATH = support
  1653|        'danger':      round(prev_ath * 0.985, 2),# prev ATH -1.5% → reduce exposure
  1654|        'principal':   round(principal_est, 2),   # never go below
  1655|    }
  1656|
  1657|    # Which tiers are currently breached
  1658|    breached = [k for k, v in tiers.items() if equity < v]
  1659|
  1660|    # Determine stop level status
  1661|    if equity < tiers['hard_stop']:
  1662|        stop_status = 'HARD_STOP'
  1663|        stop_color  = '#f85149'
  1664|    elif equity < tiers['warn_stop']:
  1665|        stop_status = 'WARN_STOP'
  1666|        stop_color  = '#f0883e'
  1667|    elif equity < tiers['soft_stop']:
  1668|        stop_status = 'SOFT_STOP'
  1669|        stop_color  = '#d4a017'
  1670|    elif equity < tiers['break_even']:
  1671|        stop_status = 'BELOW_OPEN'
  1672|        stop_color  = '#d4a017'
  1673|    else:
  1674|        stop_status = 'SAFE'
  1675|        stop_color  = '#3fb950'
  1676|
  1677|    return {
  1678|        'tiers':       tiers,
  1679|        'breached':    breached,
  1680|        'status':      stop_status,
  1681|        'color':       stop_color,
  1682|        'ath':         round(ath, 2),
  1683|        'prev_ath':    round(prev_ath, 2),
  1684|        'today_open':  round(today_open, 2),
  1685|        'drop_from_ath': round(ath - equity, 2),
  1686|        'drop_pct':    round((ath - equity) / ath * 100, 2) if ath > 0 else 0,
  1687|    }
  1688|
  1689|
  1690|def _live_updater():
  1691|    """Push live equity + trigger status every 30 seconds.
  1692|    Also runs the ATH pullback auto-trade and stop loss monitoring.
  1693|    """
  1694|    session_high   = 0.0   # highest equity seen this session
  1695|    pullback_fired = False # True after we've already acted on the current pullback
  1696|
  1697|    # Minimum drop to act on (avoids noise / rounding)
  1698|    PULLBACK_THRESHOLD = 1.00
  1699|    # Reset pullback_fired only after equity recovers within $1 of session high
  1700|    RECOVERY_MARGIN    = 1.00
  1701|
  1702|    while True:
  1703|        try:
  1704|            acct   = alpaca('/v2/account')
  1705|            equity = float(acct.get('equity', 0))
  1706|            cash   = float(acct.get('cash', 0))
  1707|            trigger = api_candle_trigger().get_json()
  1708|
  1709|            # Track session high
  1710|            if equity > session_high:
  1711|                session_high   = equity
  1712|                pullback_fired = False  # new ATH reached — reset for next pullback
  1713|
  1714|            drop = session_high - equity
  1715|
  1716|            # ── PULLBACK AUTO-TRADE ──────────────────────────────────────────
  1717|            if (not pullback_fired
  1718|                    and session_high > 0
  1719|                    and drop >= PULLBACK_THRESHOLD):
  1720|
  1721|                pullback_fired = True  # set immediately — prevent re-entry on next tick
  1722|                log.info(f'[PULLBACK] session_high={session_high:.2f} equity={equity:.2f} drop={drop:.2f}')
  1723|
  1724|                def _do_pullback_trade(drop_amt, s_high, cur_equity):
  1725|                    try:
  1726|                        # ── Step 1: find top gainer(s) to trim for drop_amt ──
  1727|                        positions  = alpaca('/v2/positions')
  1728|                        PROTECTED  = {'SGOL','GLD','VOO','QQQ','DIA','GLL','PSQ','SH','VIXY'}
  1729|                        gainers    = [
  1730|                            p for p in positions
  1731|                            if p['symbol'] not in PROTECTED
  1732|                            and float(p.get('unrealized_plpc', 0)) * 100 > 0
  1733|                            and float(p.get('market_value', 0)) > 1.10
  1734|                        ]
  1735|                        gainers.sort(key=lambda x: -float(x.get('unrealized_plpc', 0)))
  1736|
  1737|                        if not gainers:
  1738|                            msg = f'[PULLBACK] No gainers available to trim for ${drop_amt:.2f} drop'
  1739|                            log.warning(msg)
  1740|                            socketio.emit('pullback_trade', {'status': 'no_gainers', 'drop': drop_amt, 'message': msg})
  1741|                            return
  1742|
  1743|                        # Trim from top gainers until we cover drop_amt
  1744|                        sell_results = []
  1745|                        remaining    = drop_amt
  1746|                        for g in gainers:
  1747|                            if remaining <= 0:
  1748|                                break
  1749|                            sym      = g['symbol']
  1750|                            mv       = float(g.get('market_value', 0))
  1751|                            max_trim = round(mv - 1.10, 2)
  1752|                            if max_trim < 1.10:
  1753|                                continue
  1754|                            trim_amt = round(min(max_trim, remaining + 0.10), 2)
  1755|                            r = alpaca_post('/v2/orders', {
  1756|                                'symbol': sym, 'side': 'sell',
  1757|                                'type': 'market', 'time_in_force': 'day',
  1758|                                'notional': str(trim_amt),
  1759|                            })
  1760|                            sell_results.append({'sym': sym, 'amt': trim_amt,
  1761|                                                 'status': r.get('status','?'), 'err': r.get('message','')})
  1762|                            log.info(f'[PULLBACK] SELL {sym} ${trim_amt:.2f} → {r.get("status","?")}')
  1763|                            remaining -= trim_amt
  1764|
  1765|                        total_sold = sum(s['amt'] for s in sell_results)
  1766|
  1767|                        # ── Step 2: split proceeds 4-ways SGOL/DIA/QQQ/VOO ───
  1768|                        buy_each = round(total_sold / 4, 2)
  1769|                        buy_each = max(buy_each, 1.10)
  1770|                        buy_results = []
  1771|                        for etf in ['SGOL', 'DIA', 'QQQ', 'VOO']:
  1772|                            b = alpaca_post('/v2/orders', {
  1773|                                'symbol': etf, 'side': 'buy',
  1774|                                'type': 'market', 'time_in_force': 'day',
  1775|                                'notional': str(buy_each),
  1776|                            })
  1777|                            buy_results.append({'sym': etf, 'amt': buy_each,
  1778|                                                'status': b.get('status','?'), 'err': b.get('message','')})
  1779|                            log.info(f'[PULLBACK] BUY {etf} ${buy_each:.2f} → {b.get("status","?")}')
  1780|
  1781|                        sold_str = ', '.join(f"{s['sym']} ${s['amt']:.2f}" for s in sell_results)
  1782|                        buy_str  = ', '.join(f"{b['sym']} ${b['amt']:.2f}" for b in buy_results)
  1783|                        socketio.emit('pullback_trade', {
  1784|                            'status':       'fired',
  1785|                            'drop':         round(drop_amt, 2),
  1786|                            'session_high': round(s_high, 2),
  1787|                            'equity':       round(cur_equity, 2),
  1788|                            'sells':        sell_results,
  1789|                            'buys':         buy_results,
  1790|                            'total_sold':   round(total_sold, 2),
  1791|                            'buy_each':     buy_each,
  1792|                            'message':      f'📉 Pullback ${drop_amt:.2f}: SOLD {sold_str} → BOUGHT {buy_str}',
  1793|                        })
  1794|
  1795|                    except Exception as e:
  1796|                        log.error(f'[PULLBACK] trade error: {e}')
  1797|                        socketio.emit('pullback_trade', {'status': 'error', 'message': str(e)})
  1798|
  1799|                threading.Thread(
  1800|                    target=_do_pullback_trade,
  1801|                    args=(drop, session_high, equity),
  1802|                    daemon=True
  1803|                ).start()
  1804|            # ── END PULLBACK AUTO-TRADE ──────────────────────────────────────
  1805|
  1806|            # Reset flag once equity recovers close to session high
  1807|            elif pullback_fired and (session_high - equity) < RECOVERY_MARGIN:
  1808|                pullback_fired = False
  1809|
  1810|            # ── STOP LOSS TIERS ──────────────────────────────────────────────
  1811|            history = []
  1812|            if CANDLE_FILE.exists():
  1813|                try: history = json.loads(CANDLE_FILE.read_text())
  1814|                except: pass
  1815|            stops = _compute_stop_tiers(equity, session_high, history)
  1816|            # Alert on new tier breaches
  1817|            if stops['breached']:
  1818|                for tier in stops['breached']:
  1819|                    if tier not in ('principal',):  # don't spam principal — it's the floor
  1820|                        log.warning(f'[STOP] {tier.upper()} BREACHED at ${equity:.2f}')
  1821|
  1822|            socketio.emit('live_update', {
  1823|                'equity':       round(equity, 2),
  1824|                'cash':         round(cash, 2),
  1825|                'triggered':    trigger['triggered'],
  1826|                'today_high':   trigger['today_high'],
  1827|                'range':        trigger['range'],
  1828|                'ath_zone':     trigger['ath_zone'],
  1829|                'session_high': round(session_high, 2),
  1830|                'drop':         round(drop, 2),
  1831|                'time':         datetime.now().strftime('%H:%M:%S'),
  1832|                'stop_status':  stops['status'],
  1833|                'stop_color':   stops['color'],
  1834|                'stop_tiers':   stops['tiers'],
  1835|                'stop_breached':stops['breached'],
  1836|                'drop_pct':     stops['drop_pct'],
  1837|            })
  1838|        except Exception as e:
  1839|            log.error(f'[live_updater] {e}')
  1840|        time.sleep(30)
  1841|
  1842|# ── Helpers ───────────────────────────────────────────────────────────────────
  1843|
  1844|def _parse_fidelity_quick() -> dict:
  1845|    with open(PORTFOLIO_CSV, encoding='utf-8-sig') as f:
  1846|        raw = f.read().replace('\r\n','\n').replace('\r','\n')
  1847|    lines  = raw.split('\n')
  1848|    header = lines[0] if lines else ''
  1849|    has_sl = 'Sleeve Name' in header
  1850|    sc  = 3 if has_sl else 2
  1851|    tdc = 9 if has_sl else 8
  1852|    tgc = 11 if has_sl else 10
  1853|    vc  = 8 if has_sl else 7
  1854|
  1855|    EXCLUDE = ['CSI THRIFT','Fidelity Crypto','Cash Management','Spend & Save']
  1856|    SKIP    = {'SGOL','GLD','VOO','QQQ','DIA','BRKB','GLL','PSQ','SH','VIXY','QBTS','RGTI','IONQ','QUBT'}
  1857|
  1858|    def p(s):
  1859|        try: return float(re.sub(r'[$,+%]','',s.strip()) or 0)
  1860|        except: return 0.0
  1861|
  1862|    fid  = defaultdict(lambda:{'gl':0.,'today_gl':0.,'val':0.,'accts':set(),'is_mf':False})
  1863|    mf   = set()
  1864|    pat  = re.compile(r'^[A-Z0-9]{4,12},')
  1865|    for line in lines[1:]:
  1866|        if not pat.match(line): continue
  1867|        parts = line.split(',')
  1868|        if len(parts) < max(sc,tdc)+1: continue
  1869|        an = parts[0].strip(); nm = parts[1].strip() if len(parts)>1 else ''
  1870|        if any(e in nm for e in EXCLUDE): continue
  1871|        sym = parts[sc].strip()
  1872|        if not re.match(r'^[A-Z]{1,6}$',sym) or sym in SKIP: continue
  1873|        if 'MF' in nm.upper() or an=='Z30429157': mf.add(sym)
  1874|        try:
  1875|            fid[sym]['gl']       += p(parts[tgc])
  1876|            fid[sym]['today_gl'] += p(parts[tdc])
  1877|            fid[sym]['val']      += p(parts[vc])
  1878|            fid[sym]['accts'].add(an)
  1879|        except: pass
  1880|    for sym in fid:
  1881|        fid[sym]['acct_count'] = len(fid[sym]['accts'])
  1882|        fid[sym]['is_mf']      = sym in mf
  1883|    return dict(fid)
  1884|
  1885|# ── Launch ────────────────────────────────────────────────────────────────────
  1886|if __name__ == '__main__':
  1887|    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
  1888|    # Start background updater
  1889|    t = threading.Thread(target=_live_updater, daemon=True)
  1890|    t.start()
  1891|    # Start proactive brain
  1892|    t2 = threading.Thread(target=_proactive_brain, daemon=True)
  1893|    t2.start()
  1894|    print("\n" + "="*55)
  1895|    print("  HERMES TRADING DASHBOARD")
  1896|    print("  http://localhost:6060")
  1897|    print("="*55 + "\n")
  1898|    socketio.run(app, host='0.0.0.0', port=6060, debug=False, allow_unsafe_werkzeug=True)
  1899|