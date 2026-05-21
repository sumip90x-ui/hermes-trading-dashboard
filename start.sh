#!/bin/bash
# Hermes Trading Dashboard launcher
# Run: bash ~/trading_dashboard/start.sh

echo "Starting Hermes Trading Dashboard..."
cd ~/trading_dashboard
python3 app.py &
sleep 2

# Try to open browser
if command -v xdg-open &>/dev/null; then
    xdg-open http://localhost:6060
elif command -v open &>/dev/null; then
    open http://localhost:6060
fi

echo ""
echo "========================================="
echo "  Hermes Trading Dashboard"
echo "  http://localhost:6060"
echo "========================================="
echo "Press Ctrl+C to stop"
wait
