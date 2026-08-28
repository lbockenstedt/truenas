#!/bin/bash

# Resolve the branch this checkout is deployed on so a dev/qa host is not
# hard-reset back onto main by an installer re-run. Detached HEAD (or any git
# failure) falls back to main.
_deployed_branch() {
  local b
  b=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
  case "$b" in ""|HEAD) echo main ;; *) echo "$b" ;; esac
}

set -e

# Default Configuration
HUB_URL="auto"   # auto-discover the unified :443 hub
SPOKE_ID="${SPOKE_ID:-truenas-$(hostname -s)}"
SPOKE_SECRET="lm-secret"

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --hub) HUB_URL="$2"; shift ;;
        --id|--name) SPOKE_ID="$2"; shift ;;
        --secret) SPOKE_SECRET="$2"; shift ;;
        --hub-secret) HUB_SECRET="$2"; shift ;;
        --all-prereqs) ;;  # no-op; accepted for LM hub compat
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Accept a bare hub IP/host for --hub (e.g. `--hub 172.16.1.31` == `--hub
# wss://172.16.1.31:443`). Mirrors install_nw.sh.
if [ -n "${HUB_URL:-}" ] && [ "$HUB_URL" != "auto" ]; then
    case "$HUB_URL" in
        ws://*|wss://*) : ;;
        *:[0-9]*)       HUB_URL="wss://${HUB_URL}" ;;
        *)              HUB_URL="wss://${HUB_URL}:443" ;;
    esac
fi

if [ -z "$SPOKE_SECRET" ] || [ "$SPOKE_SECRET" == "lm-secret" ]; then
    SPOKE_SECRET=""
    echo "ℹ️  No pre-shared secret — spoke will connect unauthenticated and await admin approval in the LM WebUI."
fi

echo "🚀 Installing TrueNAS Manager Module (Native)..."

if [ "$(id -u)" -ne 0 ]; then
    echo "⚠️  This script must be run as root."
    exit 1
fi

apt-get update
apt-get install -y python3-pip python3-venv git curl

INSTALL_DIR="/opt/lm"
OLD_INSTALL_DIR="/opt/lm-manager"

if [ -d "$OLD_INSTALL_DIR" ]; then
    echo "🗑️  Removing legacy installation at $OLD_INSTALL_DIR..."
    rm -rf "$OLD_INSTALL_DIR"
fi

mkdir -p "$INSTALL_DIR"
mkdir -p /var/log/lm   # systemd `append:` won't create the parent dir → unit 206/EXEC on a clean box

# Circular logging: cap /var/log/lm/*.log so it can't fill the disk (copytruncate
# keeps the inode → the running spoke's O_APPEND FileHandler + systemd stderr
# keep appending).
cat > /etc/logrotate.d/lm <<'LOGROTATE'
/var/log/lm/*.log /var/log/client-sim-*.log {
    su root root
    size 50M
    rotate 5
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
}
LOGROTATE

cd "$INSTALL_DIR"

# ── Retire any legacy lm-generic-agent on this box (vendored from
# install_agent.sh:retire_legacy_agent — keep in sync) ────────────────────────
SERVICE_NAME="lm-truenas"
retire_legacy_agent() {
    local names="lm-generic-agent"
    local f
    for f in /etc/systemd/system/*.service /etc/systemd/system/*/*.service \
             /run/systemd/system/*.service \
             /lib/systemd/system/*.service /usr/lib/systemd/system/*.service; do
        [ -e "$f" ] || continue
        if grep -qE "/opt/lm/generic-agent" "$f" 2>/dev/null; then
            names="$names $(basename "$f" .service)"
        fi
    done
    local u
    for u in $(systemctl list-units --type=service --state=running,failed --no-legend --plain 2>/dev/null | awk '{print $1}'); do
        if systemctl show "$u" -p ExecStart 2>/dev/null | grep -q "/opt/lm/generic-agent"; then
            names="$names ${u%.service}"
        fi
    done
    local svc purged=0
    for svc in $(printf '%s\n' $names | sort -u); do
        [ -n "$svc" ] || continue
        [ "$svc" = "$SERVICE_NAME" ] && continue   # protect the new role-capable unit
        if [ -e "/etc/systemd/system/${svc}.service" ] \
           || systemctl list-unit-files "${svc}.service" 2>/dev/null | grep -qE "^${svc}\.service"; then
            systemctl stop    "$svc" 2>/dev/null || true
            systemctl disable "$svc" 2>/dev/null || true
            rm -f "/etc/systemd/system/${svc}.service"
            systemctl mask    "$svc" 2>/dev/null || true
            echo "🧹  Purged legacy leaf unit ${svc}.service."
            purged=1
        fi
    done
    if [ -d /opt/lm/generic-agent ]; then
        pkill -f "/opt/lm/generic-agent/src/agent.py" 2>/dev/null || true
        rm -rf /opt/lm/generic-agent
        echo "🧹  Removed legacy leaf dir /opt/lm/generic-agent."
        purged=1
    fi
    if [ "$purged" = 1 ]; then
        systemctl daemon-reload 2>/dev/null || true
        echo "    The role-capable ${SERVICE_NAME} now owns this box's spoke connection."
    fi
}
retire_legacy_agent

if [ -d "truenas" ]; then
    echo "📂 TrueNAS directory exists. Preparing for update..."
    SPOKE_PATH="$INSTALL_DIR/truenas"
    cd "$SPOKE_PATH"
    BR=$(_deployed_branch) && git fetch origin -q "$BR" && git reset --hard "origin/$BR"   # hard-sync
    cd "$INSTALL_DIR"
elif [ -d ".git" ]; then
    BR=$(_deployed_branch) && git fetch origin -q "$BR" && git reset --hard "origin/$BR"
    SPOKE_PATH="$(pwd)"
else
    echo "🌐 Cloning TrueNAS Manager repository..."
    git clone --branch "${TRUENAS_BRANCH:-main}" https://github.com/lbockenstedt/truenas.git
    SPOKE_PATH="$INSTALL_DIR/truenas"
fi

chown -R svc_lm:svc_lm "$SPOKE_PATH" 2>/dev/null || true
runuser -u svc_lm -- git config --global --add safe.directory "$SPOKE_PATH" 2>/dev/null || true

echo "🛠️ Setting up TrueNAS Manager..."
cd "$SPOKE_PATH"

echo "♻️ Resetting virtual environment..."
rm -rf venv

python3 -m venv venv
if [ ! -f "venv/bin/python3" ]; then
    echo "❌ Critical Error: venv creation failed."
    exit 1
fi

echo "Installing requirements..."
./venv/bin/python3 -m pip install --upgrade pip -q
if [ -f "requirements.txt" ]; then
    ./venv/bin/python3 -m pip install -r requirements.txt -q
fi

# --- Persistence Configuration ---
echo "⚙️ Configuring Spoke Identity..."
INSTALL_UUID_LINE=""
if [ -f .env ] && grep -q "^INSTALL_UUID=" .env; then
    EXISTING_UUID=$(grep "^INSTALL_UUID=" .env | cut -d= -f2-)
    [ -n "$EXISTING_UUID" ] && INSTALL_UUID_LINE="INSTALL_UUID=$EXISTING_UUID" \
        && echo "Preserving existing install UUID (hub fingerprint)."
fi
cat <<EOF > .env
HUB_URL=$HUB_URL
SPOKE_ID=$SPOKE_ID
SPOKE_SECRET=$SPOKE_SECRET
HUB_SECRET=$HUB_SECRET
${INSTALL_UUID_LINE}
EOF

# --- Systemd Service (For Remote/Independent Deployment) ---
echo "⚙️ Creating systemd service for auto-start..."
# ExecStart uses the equals-attached arg form (see install_nw.sh rationale:
# argparse nargs='?' refuses a following token starting with '-').
cat <<EOF > /etc/systemd/system/lm-truenas.service
[Unit]
Description=Lab Manager Spoke - TrueNAS Manager
After=network.target

[Service]
Type=simple
User=svc_lm
WorkingDirectory=$INSTALL_DIR/truenas
EnvironmentFile=$INSTALL_DIR/truenas/.env
Environment="PYTHONPATH=$INSTALL_DIR:$INSTALL_DIR/core/src:$INSTALL_DIR/truenas/src"
# equals-attached args: accepts values that start with '-'
ExecStart=$INSTALL_DIR/truenas/venv/bin/python3 -m src.control_plane --id=\${SPOKE_ID} --secret=\${SPOKE_SECRET} --hub=\${HUB_URL} --hub-secret=\${HUB_SECRET}
StandardOutput=append:/var/log/lm/lm-truenas.log
StandardError=append:/var/log/lm/lm-truenas.log
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable lm-truenas
systemctl restart lm-truenas

echo "🎉 TrueNAS Manager installation complete!"
echo "🌐 Hub Target: $HUB_URL"
echo "🆔 Spoke ID: $SPOKE_ID"
echo "📦 Version: $(cat VERSION 2>/dev/null || echo unknown)"