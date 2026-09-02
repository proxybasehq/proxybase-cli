#!/usr/bin/env bash
set -Eeuo pipefail

DATA_DIR="${PROXYBASE_DIR:-/home/proxybase/.proxybase}"
BACKEND_URL="${PROXYBASE_BACKEND:-https://api.proxybase.xyz}"
PASSWORD="${PROXYBASE_PASSWORD:-}"

echo "========================================================"
echo "          ProxyBase HD Fleet Node Initializer           "
echo "========================================================"

# -----------------------------------------------------------
# 1. Resolve HD Derivation Index
# -----------------------------------------------------------
NODE_INDEX=""
if [ -n "${PROXYBASE_HD_INDEX:-}" ]; then
    NODE_INDEX="${PROXYBASE_HD_INDEX}"
    echo "[init] Using explicit PROXYBASE_HD_INDEX: ${NODE_INDEX}"
else
    # Attempt extraction from hostname (e.g., "proxybase-seller-3" -> 3)
    HOST_NAME="$(hostname)"
    if [[ "${HOST_NAME}" =~ -([0-9]+)$ ]] || [[ "${HOST_NAME}" =~ _([0-9]+)$ ]]; then
        NODE_INDEX="${BASH_REMATCH[1]}"
        echo "[init] Extracted HD Index [${NODE_INDEX}] from hostname: ${HOST_NAME}"
    else
        # Hash fallback: 4-digit deterministic index from hostname
        NODE_INDEX=$(echo -n "${HOST_NAME}" | cksum | awk '{print $1 % 10000}')
        echo "[init] Fallback: Derived hash-based HD Index [${NODE_INDEX}] from hostname: ${HOST_NAME}"
    fi
fi

# Keystore password is consumed by `wallet import` and `login` (both read
# PROXYBASE_PASSWORD before falling back to "").
export PROXYBASE_PASSWORD="${PASSWORD}"

# -----------------------------------------------------------
# 2. Key Derivation & In-Memory Keystore Initialization
# -----------------------------------------------------------
if [ -z "${MASTER_MNEMONIC:-}" ]; then
    if [ -f "/etc/secrets/master-mnemonic" ]; then
        MASTER_MNEMONIC="$(cat /etc/secrets/master-mnemonic | xargs)"
    elif [ -f "${DATA_DIR}/mnemonic.txt" ]; then
        MASTER_MNEMONIC="$(cat "${DATA_DIR}/mnemonic.txt" | xargs)"
    fi
fi

if [ -n "${MASTER_MNEMONIC:-}" ]; then
    mkdir -p "${DATA_DIR}/wallet"
    echo "[init] Deriving secp256k1 keypair for path: m/44'/60'/0'/0/${NODE_INDEX}"
    # Re-import on every boot: stateless containers (tmpfs) always need it,
    # and re-importing the same index is idempotent (same key every time).
    proxybase-cli --backend "${BACKEND_URL}" wallet import "${MASTER_MNEMONIC}" --hd-index "${NODE_INDEX}"

    echo "[init] Authenticating node identity..."
    if ! proxybase-cli --backend "${BACKEND_URL}" login; then
        echo "[init] ERROR: Authentication failed."
        exit 1
    fi
    echo "[init] Authenticated. Session token active."
else
    # Legacy bootstrap (no master mnemonic): keep pre-HD behavior so existing
    # deployments with a persisted wallet volume are unaffected.
    echo "[init] No MASTER_MNEMONIC — legacy bootstrap mode."
    WALLET_FILE="${DATA_DIR}/wallet/keyfile.enc"
    TOKEN_FILE="${DATA_DIR}/session_token"
    if [ ! -f "$WALLET_FILE" ]; then
        echo "[init] No wallet found — creating one..."
        proxybase-cli --backend "${BACKEND_URL}" wallet create
    fi
    if [ ! -f "$TOKEN_FILE" ]; then
        echo "[init] Session token missing — logging in..."
        proxybase-cli --backend "${BACKEND_URL}" login
    fi
fi

# Ensure default seller config exists if not present (direct-only)
CONFIG_FILE="${DATA_DIR}/seller_config.json"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "[init] Creating default seller config (direct-only)..."
    mkdir -p "${DATA_DIR}"
    cat > "$CONFIG_FILE" <<'JSON'
{"upstream_proxies":[],"no_direct":false}
JSON
fi

# -----------------------------------------------------------
# 3. Assemble Arguments and Launch Seller
# -----------------------------------------------------------
if [ "${1:-}" = "proxybase-cli" ] && [ "${2:-}" = "seller" ] && [ "${3:-}" = "start" ]; then
    ARGS=("$@")
    [[ ! " ${ARGS[*]} " =~ " --foreground " ]] && ARGS+=("--foreground")
    [[ ! " ${ARGS[*]} " =~ " --backend " ]] && ARGS+=("--backend" "${BACKEND_URL}")
    [ "${PROXYBASE_VOLUNTEER:-}" = "true" ] && ARGS+=("--volunteer")
    [ "${PROXYBASE_NO_DIRECT:-}" = "true" ] && ARGS+=("--no-direct")
    if [ -n "${PROXYBASE_UPSTREAM:-}" ]; then
        IFS=',' read -ra UPSTREAMS <<< "${PROXYBASE_UPSTREAM}"
        for ups in "${UPSTREAMS[@]}"; do ARGS+=("--upstream" "${ups}"); done
    fi
    if [ -n "${PROXYBASE_UPSTREAM_USER:-}" ]; then
        IFS=',' read -ra USERS <<< "${PROXYBASE_UPSTREAM_USER}";
        for u in "${USERS[@]}"; do ARGS+=("--upstream-user" "${u}"); done
    fi
    if [ -n "${PROXYBASE_UPSTREAM_PASS:-}" ]; then
        IFS=',' read -ra PASSES <<< "${PROXYBASE_UPSTREAM_PASS}";
        for p in "${PASSES[@]}"; do ARGS+=("--upstream-pass" "${p}"); done
    fi
    echo "[init] Launching Seller Relay Daemon..."
    exec "${ARGS[@]}"
fi

if [ $# -eq 0 ]; then
    exec proxybase-cli --backend "${BACKEND_URL}" seller start --foreground
fi
exec "$@"
