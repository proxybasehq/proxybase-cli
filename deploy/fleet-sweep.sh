#!/usr/bin/env bash
set -euo pipefail

# Fleet sweeper: consolidate seller earnings from HD child wallets
# m/44'/60'/0'/0/{i} into one central Tempo address.
#
# Delegates to the native `proxybase-cli wallet sweep`, which derives each
# child in memory, authenticates with the backend, and creates a payout for
# every child whose available earnings meet the threshold. The operator's
# on-disk wallet and session token are never touched.
#
# Usage:
#   fleet-sweep.sh [NUM_NODES] [START_INDEX]
#
# Env:
#   MASTER_MNEMONIC             (required) fleet master phrase
#   CENTRAL_PAYOUT_ADDRESS      (required) central Tempo wallet, e.g. 0x71C...
#   PROXYBASE_BACKEND           (default https://api.proxybase.xyz)
#   MIN_THRESHOLD_MICROCREDITS  (default 1000000 = $1.00)
#
# Cron (daily, 04:00):
#   0 4 * * * MASTER_MNEMONIC="..." CENTRAL_PAYOUT_ADDRESS="0x..." /opt/proxybase/fleet-sweep.sh 100

MNEMONIC="${MASTER_MNEMONIC:?Set MASTER_MNEMONIC to the fleet master phrase}"
TARGET_TEMPO="${CENTRAL_PAYOUT_ADDRESS:?Set CENTRAL_PAYOUT_ADDRESS to the central Tempo address}"
BACKEND_URL="${PROXYBASE_BACKEND:-https://api.proxybase.xyz}"
MIN_THRESHOLD="${MIN_THRESHOLD_MICROCREDITS:-1000000}"
NUM_NODES="${1:-10}"
START_INDEX="${2:-0}"

echo "[sweep] Scanning HD children ${START_INDEX}..$((START_INDEX + NUM_NODES - 1)) -> ${TARGET_TEMPO}"

exec proxybase-cli --backend "${BACKEND_URL}" wallet sweep "${MNEMONIC}" \
    --start-index "${START_INDEX}" \
    --count "${NUM_NODES}" \
    --target-tempo "${TARGET_TEMPO}" \
    --min-threshold "${MIN_THRESHOLD}"
