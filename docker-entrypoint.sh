#!/bin/bash
set -e

WALLET_DIR="/home/proxybase/.proxybase/wallet"
WALLET_FILE="$WALLET_DIR/keyfile.enc"
TOKEN_FILE="/home/proxybase/.proxybase/session_token"

# Bootstrap wallet if missing
if [ ! -f "$WALLET_FILE" ]; then
    echo "==> No wallet found — creating one..."
    proxybase-cli wallet create
    echo "==> Logging in..."
    proxybase-cli login
elif [ ! -f "$TOKEN_FILE" ]; then
    echo "==> Session token missing — logging in..."
    proxybase-cli login
fi

CONFIG_FILE="/home/proxybase/.proxybase/seller_config.json"

# Create default seller config if missing (direct-only, no upstreams)
if [ ! -f "$CONFIG_FILE" ]; then
    echo "==> Creating default seller config (direct-only)..."
    cat > "$CONFIG_FILE" <<'JSON'
{"upstream_proxies":[],"no_direct":false}
JSON
fi

# Run proxybase-cli with passed args, or default to seller start --foreground
if [ $# -eq 0 ]; then
    exec proxybase-cli seller start --foreground
else
    exec proxybase-cli "$@"
fi
