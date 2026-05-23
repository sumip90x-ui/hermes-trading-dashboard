"""
flask_routes.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Paste this block into ~/trading_dashboard/app.py.

Requirements:
  1. fidelity_db.py must be in the same directory as app.py (or on PYTHONPATH)
  2. No additional pip installs needed — fidelity_db only uses pandas + stdlib

Paste after your existing imports and before `if __name__ == '__main__':`.
The blueprint approach keeps these routes cleanly separated from your bot routes.
If you prefer not to use blueprints, convert to @app.route() directly.

─────────────────────────────────────────────────────────────────────────────
PASTE START
─────────────────────────────────────────────────────────────────────────────
"""

# ── Add these imports at the top of app.py ────────────────────────────────────

from flask import Blueprint, request, jsonify
import fidelity_db

# ── Create blueprint (or skip and use @app.route directly) ────────────────────

portfolio_bp = Blueprint("portfolio", __name__, url_prefix="/api/portfolio/history")

# Register with your app — add this line near where you register other blueprints:
#   app.register_blueprint(portfolio_bp)


# ── Routes ────────────────────────────────────────────────────────────────────

@portfolio_bp.route("/snapshots", methods=["GET"])
def route_snapshots():
    """
    GET /api/portfolio/history/snapshots

    Returns all snapshot summaries, newest first.
    Response: [{ snapshot_id, snapshot_date, filename, symbol_count, total_value }, ...]
    """
    try:
        data = fidelity_db.get_snapshots()
        return jsonify({"status": "ok", "count": len(data), "snapshots": data})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@portfolio_bp.route("/deviations", methods=["GET"])
def route_deviations():
    """
    GET /api/portfolio/history/deviations
    GET /api/portfolio/history/deviations?snapshot_id=<uuid>

    Returns BUY-signal deviations for the specified snapshot (or latest).
    Sorted by deploy_amount descending.

    Response: { snapshot_id, count, deviations: [...] }
    """
    try:
        snapshot_id = request.args.get("snapshot_id") or None
        data = fidelity_db.get_deviations(snapshot_id)
        used_id = data[0]["curr_snapshot_id"] if data else snapshot_id
        return jsonify({
            "status":      "ok",
            "snapshot_id": used_id,
            "count":       len(data),
            "deviations":  data,
        })
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@portfolio_bp.route("/symbol/<symbol>", methods=["GET"])
def route_symbol_history(symbol: str):
    """
    GET /api/portfolio/history/symbol/SGOL

    Returns full G/L time series for one symbol across all snapshots.
    Useful for sell-mistake analysis and position drift charting.

    Response: { symbol, count, history: [{ snapshot_date, total_gl, accounts, ... }] }
    """
    try:
        symbol = symbol.upper().strip()
        data = fidelity_db.get_symbol_history(symbol)
        return jsonify({
            "status":  "ok",
            "symbol":  symbol,
            "count":   len(data),
            "history": data,
        })
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@portfolio_bp.route("/summary", methods=["GET"])
def route_summary():
    """
    GET /api/portfolio/history/summary

    Returns:
    {
      snapshot_count,
      date_range: { earliest, latest },
      symbols_tracked,
      top_signals: [ top 10 BUY deviations from latest snapshot ]
    }
    """
    try:
        data = fidelity_db.get_summary()
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@portfolio_bp.route("/ingest", methods=["POST"])
def route_ingest():
    """
    POST /api/portfolio/history/ingest
    Content-Type: multipart/form-data
    Field: file (the Fidelity CSV)

    Saves the uploaded CSV to the vault directory, then ingests it.
    Returns snapshot summary + top BUY signals.

    NOTE: This is the manual upload endpoint used by the Portfolio tab UI.
    The SGOL Alpaca logic in trading_bot.py is NOT affected — this is a
    parallel read-only signal layer.

    Example curl:
      curl -X POST http://localhost:6060/api/portfolio/history/ingest \
           -F "file=@/path/to/fidelity_export.csv"
    """
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file field in request"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"status": "error", "message": "Empty filename"}), 400

    # Validate it looks like a Fidelity CSV
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"status": "error", "message": "File must be a .csv"}), 400

    # Save to vault so batch_ingest.py can find it later too
    vault = fidelity_db.VAULT_DIR
    vault.mkdir(parents=True, exist_ok=True)
    save_path = vault / f.filename

    f.save(save_path)

    # Check for duplicate
    if fidelity_db.filename_already_ingested(f.filename):
        # Still return the existing data for this filename
        snapshots = fidelity_db.get_snapshots()
        existing = next((s for s in snapshots if s["filename"] == f.filename), None)
        return jsonify({
            "status":  "duplicate",
            "message": f"{f.filename} was already ingested — returning existing data.",
            "snapshot": existing,
        })

    try:
        result = fidelity_db.ingest_snapshot(save_path)
        return jsonify({"status": "ok", **result})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# PASTE END
#
# Then add this line in your app setup (near other blueprint registrations):
#   app.register_blueprint(portfolio_bp)
#
# Or if not using blueprints, replace @portfolio_bp.route with @app.route
# and remove the Blueprint() and register_blueprint() lines.
# ─────────────────────────────────────────────────────────────────────────────
