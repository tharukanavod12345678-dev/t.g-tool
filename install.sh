#!/bin/bash
set -e

USERNAME=$(whoami)
HOME_DIR="/home/$USERNAME"
TGPULL_DIR="$HOME_DIR/tg-pull"

echo "==========================================="
echo "  Telegram Pull — Installer"
echo "  User: $USERNAME   Dir: $TGPULL_DIR"
echo "==========================================="

echo "[1/6] System packages..."
sudo apt update -qq
sudo apt install -y -qq python3 python3-pip python3-venv screen util-linux unzip

echo "[2/6] Folders..."
mkdir -p "$TGPULL_DIR/state" "$TGPULL_DIR/_downloads"

echo "[3/6] Placing files..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp "$SCRIPT_DIR/app.py" "$TGPULL_DIR/"
cp "$SCRIPT_DIR/requirements.txt" "$TGPULL_DIR/"
[ -f "$TGPULL_DIR/config.json" ] || cp "$SCRIPT_DIR/config.json" "$TGPULL_DIR/"
[ -f "$TGPULL_DIR/state/pull_state.json" ] || cp "$SCRIPT_DIR/pull_state.json" "$TGPULL_DIR/state/"
cp "$SCRIPT_DIR/.env" "$TGPULL_DIR/.env"
chmod 600 "$TGPULL_DIR/.env"

echo "[4/6] Python venv..."
cd "$TGPULL_DIR"
python3 -m venv venv
./venv/bin/pip install --quiet --upgrade pip
./venv/bin/pip install --quiet -r requirements.txt

echo "[5/6] systemd service..."
sed "s|USERNAME|$USERNAME|g" "$SCRIPT_DIR/tgpull.service" | \
    sudo tee /etc/systemd/system/tgpull.service > /dev/null
sudo systemctl daemon-reload
sudo systemctl enable tgpull

echo "[6/6] Starting service..."
sudo systemctl restart tgpull

echo ""
echo "==========================================="
echo "  ✅ Setup complete"
echo "==========================================="
echo ""
echo "  Live logs:  tail -f $TGPULL_DIR/run.log"
echo "  Stop:       sudo systemctl stop tgpull"
echo ""
echo "  Telegram → Saved Messages → wizard auto-start"
echo ""
