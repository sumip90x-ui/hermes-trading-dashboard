#!/bin/bash
# Hermes Trading Dashboard Launcher
# ALWAYS kills any existing process and starts fresh — ensures new code is always loaded

set -e
export PYTHONDONTWRITEBYTECODE=1   # never write .pyc files
export PYTHONUNBUFFERED=1          # real-time log output

# Load env vars
if [ -f /home/sumith/.env ]; then
    export $(grep -v '^#' /home/sumith/.env | xargs)
fi
if [ -f /home/sumith/.hermes/.env ]; then
    export $(grep -v '^#' /home/sumith/.hermes/.env | xargs)
fi

DASHBOARD_DIR="/home/sumith/trading_dashboard"
LOG="/tmp/hermes_dashboard.log"

# ALWAYS kill existing process — ensures new code is always loaded
if pgrep -f "trading_dashboard/app.py" > /dev/null 2>&1; then
    echo "Stopping existing dashboard process..."
    pkill -f "trading_dashboard/app.py" || true
    sleep 1
fi

# Start fresh
cd "$DASHBOARD_DIR"
echo "Starting dashboard with latest code..."
nohup /usr/bin/python3 app.py >> "$LOG" 2>&1 &
DASHBOARD_PID=$!
echo "Started dashboard PID $DASHBOARD_PID — waiting for port 6060..."

# Poll until ready (up to 30 seconds)
for i in $(seq 1 30); do
    if curl -sf http://localhost:6060/api/account > /dev/null 2>&1; then
        echo "Dashboard ready after ${i}s — opening browser."
        firefox --new-window http://localhost:6060 2>/dev/null &
        exit 0
    fi
    sleep 1
done

echo "Dashboard did not start in 30s. Check $LOG for errors."
exit 1
