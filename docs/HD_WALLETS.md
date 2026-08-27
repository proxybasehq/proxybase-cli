# HD Wallets: Master Seed + BIP-44 Child Key Derivation

ProxyBase v2 requires one active WebSocket connection per wallet address
(`/v2/ws/seller`). Scaling seller nodes across a fleet (multi-IP servers,
residential gateways, Docker hosts, Kubernetes clusters) with a shared wallet
address causes connection flapping and registration collisions.

HD wallets solve this: **one master mnemonic derives an unbounded number of
distinct child wallets**, one per container.

```
                    ┌────────────────────────────────────────────────────────┐
                    │            Master Seed Phrase (12/24 words)            │
                    │   "abandon abandon abandon ... about" (Single Backup)  │
                    └──────────────────────────┬─────────────────────────────┘
                                               │
                    ┌──────────────────────────┴─────────────────────────────┐
                    │         BIP-32 / BIP-44 Key Derivation Function        │
                    │               Path: m/44'/60'/0'/0/{index}             │
                    └───────┬──────────────────┬──────────────────┬──────────┘
                            │                  │                  │
                    Index = 0          Index = 1          Index = N
                            │                  │                  │
                     ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
                     │ Child Key 0 │    │ Child Key 1 │    │ Child Key N │
                     │  Wallet A0  │    │  Wallet A1  │    │  Wallet AN  │
                     └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
                            │                  │                  │
                   ┌────────▼────────┐┌────────▼────────┐┌────────▼────────┐
                   │ Container #0    ││ Container #1    ││ Container #N    │
                   └────────┬────────┘└────────┬────────┘└────────┬────────┘
                            │                  │                  │
                            └──────────────────┼──────────────────┘
                                               │ (N distinct WebSocket tunnels)
                                               ▼
                                  ┌─────────────────────────┐
                                  │ ProxyBase v2 Backend    │
                                  │ (No Connection Conflict)│
                                  └─────────────────────────┘
```

## Why this is the optimal solution

- **Single master seed** — one 12/24-word recovery phrase backs the whole fleet.
- **Zero connection collision** — each index yields a mathematically distinct
  address (A_0 ≠ A_1 ≠ A_N).
- **Stateless & resilient** — containers need no persistent volumes. A
  replacement with the same index re-derives the identical identity and
  reclaims the session and accumulated earnings.
- **Fleet-wide payout sweeping** — one command sweeps earnings from indices
  0..N into a central Tempo wallet.

## Derivation path

Standard BIP-44 multi-account hierarchy over secp256k1:

```
m / 44' / 60' / 0' / 0 / i
```

| Component | Value | Meaning |
|---|---|---|
| `44'` | hardened | BIP-44 purpose |
| `60'` | hardened | Coin type (EVM-compatible addresses) |
| `0'` | hardened | Master account index |
| `0` | normal | External / operational key chain |
| `i` | normal | Container/node ordinal, `0 <= i < 2^31` |

Address derivation: `keccak256(public_key[1..])[-20..]`, matching
`libproxybase::wallet::keypair::public_key_to_address`.

The implementation lives in `libproxybase/src/wallet/hd.rs`
(`derive_bip44_keypair`, `derive_path`) and is pinned by unit tests against
the official BIP-32 test vectors and the canonical
`abandon ... about` mnemonic accounts:

| Index | Address |
|---|---|
| 0 | `0x9858EfFD232B4033E47d90003D41EC34EcaEda94` |
| 1 | `0x6Fac4D18c912343BF86fa7049364Dd4E424Ab9C0` |
| 2 | `0xb6716976A3ebe8D39aCEB04372f22Ff8e6802D7A` |

## CLI usage

### Import a child wallet

```bash
proxybase-cli wallet import "<master phrase>" --hd-index 3
# Wallet imported successfully (HD index 3: m/44'/60'/0'/0/3)
# Address: 0xf3f5...
```

- With `--hd-index`: the wallet is derived at `m/44'/60'/0'/0/{index}`.
- Without `--hd-index`: legacy raw-seed derivation (backward compatible with
  pre-HD wallets). **The two derivations intentionally produce different
  addresses from the same phrase** — do not mix them for one node.
- The keystore password comes from `PROXYBASE_PASSWORD` (default `""`).

### Sweep earnings fleet-wide

```bash
proxybase-cli wallet sweep "<master phrase>" \
    --start-index 0 \
    --count 100 \
    --target-tempo 0x71C... \
    --min-threshold 1000000
```

For each child `i` in `[start_index, start_index + count)`:

1. Derives the keypair in memory (no keystore writes),
2. Authenticates via the `/v2/auth` challenge/verify flow,
3. Reads top-level `seller_available` from `GET /v2/wallet/balance` (the
   ledger, valid for online **and** offline nodes — `GET /v2/seller/status`
   only reports earnings for nodes currently connected to the seller pool),
4. Creates a payout (`POST /v2/payouts`) when available earnings meet
   `--min-threshold` (default 1,000,000 microcredits = $1.00).

The operator's on-disk wallet and session token are left untouched.

## Container runtime

The image (`ghcr.io/proxybasehq/proxybase-cli`) is multi-arch
(`linux/amd64` + `linux/arm64`), so ARM nodes (Raspberry Pi, ARM servers,
cloud ARM instances) pull it natively. The entrypoint
(`docker-entrypoint.sh`) supports two modes:

- **HD mode** — when `MASTER_MNEMONIC` is provided (env, `/etc/secrets/master-mnemonic`,
  or `$PROXYBASE_DIR/mnemonic.txt`): resolves the node index, imports the child
  wallet, logs in, and launches the seller.
- **Legacy mode** — when no master mnemonic is present: pre-HD bootstrap
  (create a wallet if missing, login, default seller config). Existing
  deployments with persisted volumes are unaffected.

### Index auto-discovery precedence

1. `PROXYBASE_HD_INDEX` env var (explicit, e.g. `0`, `1`, `2`)
2. Trailing number in the hostname: `proxybase-seller-0` → 0,
   `proxybase_seller_4` → 4
3. Fallback: `cksum(hostname) % 10000` (ad-hoc container runners)

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `PROXYBASE_BACKEND` | `https://api.proxybase.xyz` | Backend API base URL |
| `MASTER_MNEMONIC` | — | Master phrase for HD mode |
| `PROXYBASE_HD_INDEX` | from hostname | Child derivation index |
| `PROXYBASE_PASSWORD` | `""` | Keystore encryption password |
| `PROXYBASE_DIR` | `/home/proxybase/.proxybase` | State directory (mount as tmpfs) |
| `PROXYBASE_VOLUNTEER` | `false` | `true` = donate bandwidth unpaid |
| `PROXYBASE_NO_DIRECT` | `false` | `true` = resell upstreams only |
| `PROXYBASE_UPSTREAM(_USER/_PASS)` | — | Comma-separated upstream proxy lists |

## Orchestration blueprints

Ready-to-use manifests live in `deploy/`:

- **`deploy/kubernetes-statefulset.yaml`** — StatefulSet whose pod ordinals
  (stable hostnames `proxybase-seller-0`, `-1`, ...) become HD indices
  automatically. `emptyDir: Memory` keeps keys in RAM. `OrderedReady`
  pod management (the default) makes rolling updates collision-free: pods
  terminate in reverse ordinal order and a replacement starts only after its
  ordinal has fully terminated, so wallet A_i never has two live connections.
  Scale: `kubectl scale statefulset proxybase-seller --replicas=100`.
- **`deploy/docker-compose.hd.yaml`** — multi-node Compose stack with
  per-service `PROXYBASE_HD_INDEX` and tmpfs state.
- **`deploy/fleet-sweep.sh`** — cron-friendly wrapper around
  `wallet sweep` for periodic fleet-wide payout consolidation.

## Security & operational best practices

- **tmpfs keystores** — mount `$PROXYBASE_DIR` as RAM (`emptyDir: Memory`,
  `tmpfs:` in compose). Decrypted keys and session tokens never touch disk.
- **Master mnemonic isolation** — inject `MASTER_MNEMONIC` via
  SealedSecrets, HashiCorp Vault, or AWS Secrets Manager; restrict read
  access to the seller service account. Prefer `/etc/secrets/master-mnemonic`
  over env vars (env leaks into debug endpoints and subprocess listings).
- **Update thrashing** — with the default `OrderedReady` pod management,
  StatefulSet rolling updates terminate pods highest ordinal first
  (N → N-1 → ... → 0) and start a replacement only after its ordinal has
  fully terminated; wallet `A_i` can never have two live connections.
  `podManagementPolicy: Parallel` trades this guarantee for faster scale-up.
- **Grace period** — keep `terminationGracePeriodSeconds: 20`. `tini` (PID 1)
  forwards SIGTERM to the CLI, which closes the WebSocket cleanly before the
  replacement pod handshakes.
- **Sweep threshold** — keep `--min-threshold` high enough that payout fees
  and chain costs don't eat small balances; run the sweeper as a periodic
  cron job on an admin machine, never on the fleet nodes themselves.
- **Container healthcheck** — the image HEALTHCHECK requires backend
  reachability (`proxybase-cli health`) AND a live local seller process
  (`seller status` daemon line). Tunnel-level liveness is intentionally not
  checked: the seller reconnect loop self-heals, and a hard tunnel failure
  would only cause restart churn in the seller pool.
