#!/bin/bash
# Hermes Trading Dashboard — One-line installer
# Usage: bash install.sh

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}"
echo "  ██╗  ██╗███████╗██████╗ ███╗   ███╗███████╗███████╗"
echo "  ██║  ██║██╔════╝██╔══██╗████╗ ████║██╔════╝██╔════╝"
echo "  ███████║█████╗  ██████╔╝██╔████╔██║█████╗  ███████╗"
echo "  ██╔══██║██╔══╝  ██╔══██╗██║╚██╔╝██║██╔══╝  ╚════██║"
echo "  ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║███████╗███████║"
echo "  ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚══════╝"
echo -e "${NC}"
echo "  Trading Dashboard — Installer"
echo "  ================================"
echo ""

# ── Step 1: Python check ──────────────────────────────────────────────────
echo -e "${YELLOW}[1/5] Checking Python...${NC}"
if ! command -v python3 &>/dev/null; then
    echo -e "${RED}Python3 not found. Please install Python 3.10+${NC}"
    exit 1
fi
PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo -e "${GREEN}✓ Python $PYVER found${NC}"

# ── Step 2: Install dependencies ─────────────────────────────────────────
echo ""
echo -e "${YELLOW}[2/5] Installing Python dependencies...${NC}"
pip3 install flask flask-socketio eventlet requests --break-system-packages 2>/dev/null || \
pip3 install flask flask-socketio eventlet requests
echo -e "${GREEN}✓ Dependencies installed${NC}"

# ── Step 3: Create env file ───────────────────────────────────────────────
echo ""
echo -e "${YELLOW}[3/5] Setting up configuration...${NC}"
if [ ! -f .env ]; then
    cp .env.example .env
    echo -e "${GREEN}✓ Created .env from template${NC}"
    echo -e "${YELLOW}  ⚠ Edit .env with your Alpaca API keys before starting${NC}"
else
    echo -e "${GREEN}✓ .env already exists${NC}"
fi

# ── Step 4: Create data directories ──────────────────────────────────────
echo ""
echo -e "${YELLOW}[4/5] Creating data directories...${NC}"
mkdir -p ~/trading_reports
mkdir -p ~/Documents/Trading\ Vault/02_Session_Notes
echo -e "${GREEN}✓ Data directories ready${NC}"

# ── Step 5: Create journey.json if missing ────────────────────────────────
echo ""
echo -e "${YELLOW}[5/5] Initializing journey tracker...${NC}"
JOURNEY_FILE=~/Documents/Trading\ Vault/journey.json
if [ ! -f "$JOURNEY_FILE" ]; then
    cat > "$JOURNEY_FILE" << 'JOURNEY'
{
  "goal": 100000,
  "principal": 0,
  "started": "",
  "phases": [
    {"name": "SEED",    "emoji": "🌱", "color": "#3fb950", "start": 0,     "end": 1000,   "envelopes": 25},
    {"name": "SPROUT",  "emoji": "🌿", "color": "#58a6ff", "start": 1000,  "end": 5000,   "envelopes": 25},
    {"name": "GROW",    "emoji": "🌳", "color": "#d4a017", "start": 5000,  "end": 25000,  "envelopes": 25},
    {"name": "HARVEST", "emoji": "💰", "color": "#f0883e", "start": 25000, "end": 100000, "envelopes": 25}
  ],
  "milestones_hit": [],
  "notes": {}
}
JOURNEY
    echo -e "${GREEN}✓ Journey tracker initialized${NC}"
else
    echo -e "${GREEN}✓ Journey tracker already exists${NC}"
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅  Installation complete!              ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""
echo "  Next steps:"
echo "  1. Edit .env with your Alpaca API keys"
echo "  2. Run: bash start.sh"
echo "  3. Open: http://localhost:6060"
echo ""
echo "  Optional: Set ANTHROPIC_API_KEY in .env to enable"
echo "  Hermes AI chat with trade execution."
echo ""
