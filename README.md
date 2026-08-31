# proxybase-cli

Official command-line client for ProxyBase — a decentralized bandwidth marketplace.

`proxybase-cli` allows you to manage wallets, sell internet bandwidth (directly and via external upstream proxies), purchase residential/datacenter proxy sessions, and run local SOCKS5 bridges.

---

## Table of Contents

- [Features](#features)
- [Installation & Build](#installation--build)
- [Wallet Setup & Authentication](#wallet-setup--authentication)
- [Seller: Bandwidth & Upstream Proxy Reselling](#seller-bandwidth--upstream-proxy-reselling)
  - [Overview: Direct vs. Upstream Reselling](#overview-direct-vs-upstream-reselling)
  - [How Upstream Selling Works (Technical Architecture)](#how-upstream-selling-works-technical-architecture)
  - [Seller CLI Commands & Options](#seller-cli-commands--options)
  - [Usage Examples](#usage-examples)
  - [Configuration Persistence & OS Services](#configuration-persistence--os-services)
  - [Container & Fleet Deployment (Environment Variables)](#container--fleet-deployment-environment-variables)
- [HD Fleet Wallets](#hd-fleet-wallets)
- [Buyer & Market Operations](#buyer--market-operations)
- [Local SOCKS5 Bridge](#local-socks5-bridge)
- [Self-Update](#self-update)

---

## Features

- **Multi-Path Seller Relay**: Sell your own device's internet bandwidth and/or resell third-party upstream SOCKS5 proxies concurrently under a single seller node identity.
- **Independent Path QoS**: Each upstream proxy and direct connection is classified and monitored independently for latency, uptime, IP reputation, ASN, country, and network type.
- **BIP-44 HD Fleet Wallets**: Derive thousands of node child identities from a single master mnemonic (`m/44'/60'/0'/0/{index}`) with automated sweeping into a central settlement wallet.
- **Automatic Daemonization & System Services**: Runs seamlessly in the background with automatic OS service creation (`systemd` on Linux, `launchd` on macOS, Windows Services).
- **Resilient Auto-Reconnection**: Jittered exponential backoff (1s → 60s), shared ECDSA re-authentication on session expiry, and connection watchdog monitors.
- **Local SOCKS5 Bridge**: Spin up local unauthenticated SOCKS5 listeners for purchased proxy sessions for software lacking SOCKS5 auth support.

---

## Installation & Build

### Pre-built Binaries

Download pre-built binaries for Linux (`x86_64`, `aarch64`), macOS (Apple Silicon), or Windows from the [Releases](https://github.com/proxybase/proxybase-cli/releases) page.

### Build from Source

Requirements: Rust toolchain (1.75+).

```bash
cargo build --release
# Binary placed at target/release/proxybase-cli
```

---

## Wallet Setup & Authentication

Before running a seller node or buying proxy sessions, configure your wallet identity:

```bash
# 1. Create a new wallet
proxybase-cli wallet create

# OR import an existing 12/24-word recovery phrase
proxybase-cli wallet import "your twelve word mnemonic seed phrase goes right here"

# 2. Authenticate against the backend
proxybase-cli login
```

Authentication uses an ECDSA challenge-response handshake over secp256k1. The backend issues a timestamped nonce, the CLI signs `address:nonce:timestamp`, and on verification, saves an authenticated bearer session token to `~/.proxybase/session_token`.

---

## Seller: Bandwidth & Upstream Proxy Reselling

The `seller` subsystem allows operators to monetize network access. Beyond sharing raw node bandwidth, `proxybase-cli` includes **Upstream Reselling**, allowing node operators to route ProxyBase traffic through one or more third-party upstream SOCKS5 proxies.

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                      proxybase-cli                          │
                    │                                                             │
                    │  ┌─────────────────┐       ┌─────────────────────────────┐  │
                    │  │ Direct Path     │ ───►  │ Direct TCP Connection       │──┼──► Target Host (e.g. 93.184.216.34:443)
                    │  │ (path: direct)  │       │ (Node's local IP / ISP)     │  │
                    │  └────────┬────────┘       └─────────────────────────────┘  │
                    │           │ WS Tunnel                                       │
                    │           ▼                                                 │
ProxyBase           │  ┌─────────────────┐       ┌─────────────────────────────┐  │
Backend Relay  ◄───►│  │ Upstream Path 0 │ ───►  │ SOCKS5: user:pass@host:port │──┼──► Target Host (via Upstream Proxy 0)
(/v2/ws/seller)     │  │ (upstream_0)    │       │ (External Residential IP)   │  │
                    │  └────────┬────────┘       └─────────────────────────────┘  │
                    │           │ WS Tunnel                                       │
                    │           ▼                                                 │
                    │  ┌─────────────────┐       ┌─────────────────────────────┐  │
                    │  │ Upstream Path 1 │ ───►  │ SOCKS5: user:pass@host:port │──┼──► Target Host (via Upstream Proxy 1)
                    │  │ (upstream_1)    │       │ (External Mobile/DC IP)     │  │
                    │  └─────────────────┘       └─────────────────────────────┘  │
                    └─────────────────────────────────────────────────────────────┘
```

### Overview: Direct vs. Upstream Reselling

1. **Direct Mode (Default)**:
   - Uses the host machine's own network interface and public IP.
   - Ideal for residential nodes, home servers, and local edge devices.
2. **Upstream Reselling Mode (`--upstream`)**:
   - Forwards buyer traffic through external SOCKS5 proxy endpoints.
   - Enables reselling existing residential, mobile, or datacenter proxy pools through the ProxyBase marketplace.
3. **Hybrid Mode**:
   - Runs both direct bandwidth and multiple upstream proxies concurrently under the same seller account.
4. **Upstream-Only Mode (`--no-direct`)**:
   - Shuts off direct traffic through the node's local IP. All buyer sessions are strictly routed through the configured upstream proxies.

---

### How Upstream Selling Works (Technical Architecture)

#### 1. Multi-Path Multiplexing (`build_paths`)
When you start the seller with upstream proxies, `proxybase-cli` builds independent logical paths:
- `direct` (if `--no-direct` is false)
- `upstream_0`, `upstream_1`, ..., `upstream_N` (for each `--upstream` entry)

Each path operates as an **independent async task** with its own persistent WebSocket connection to `{backend}/v2/ws/seller?token=...`.

#### 2. Path Identification & Discovery Handshake
Upon connecting, each path identifies itself to the backend:
1. Transmits the bearer session token.
2. Sends a path metadata frame: `{"type": "path_info", "path_id": "upstream_0"}`.
3. The backend assigns a unique connection ID (e.g. `upstream_0::7a8f9b1c`) and registers that specific tunnel into its routing and Quality of Service (QoS) engine.

#### 3. Independent QoS Probing & IP Classification
The ProxyBase backend treats each path as an individual exit node:
- **Active Probing**: The backend periodically executes lightweight QoS probe requests through each active path tunnel.
- **Intelligence & Categorization**: The backend verifies exit IP geolocation (Country, Region, City), ASN/ISP details, proxy type (`residential`, `datacenter`, `mobile`), latency, throughput, and uptime.
- **Pool Tiers**: Paths independently graduate from `Trial` to verified tiers based on their individual uptime and QoS scores.
- **Independent Matching**: When a buyer requests a session (e.g., US Residential), the backend matches and routes specifically to the path that meets those criteria.

#### 4. Stream Relay Lifecycle (`run_stream_relay`)
When a buyer connects to an allocated proxy session:
1. **`stream_open` Command**: The backend sends a `stream_open` message to the matching path connection over WebSocket:
   ```json
   {
     "type": "stream_open",
     "session_id": "sess_89a1c2",
     "target_ip": "93.184.216.34",
     "target_port": 443,
     "target_host": "example.com",
     "seq": 42
   }
   ```
2. **Command Acknowledgment**: The CLI sends back a `{"type": "cmd_ack", "seq": 42}` message.
3. **Outbound Connection**:
   - **For `direct` path**: Connects directly via TCP (`TcpStream::connect("target_ip:target_port")`).
   - **For `upstream_N` path**: Performs an authenticated SOCKS5 handshake using `fast-socks5` against the upstream proxy address (`Socks5Stream::connect_with_password(&proxy.address, target_dest, target_port, user, pass)`). Handshakes are bounded by a 10-second timeout to prevent dead upstreams from hanging sockets.
4. **Bidirectional Tunneling**:
   - **Inbound Data**: Target reads are base64-encoded and sent to the backend via `{"type": "relay_response", "session_id": "...", "data": "..."}`.
   - **Outbound Data**: Incoming `relay_data` frames from the buyer are base64-decoded and written to the upstream/direct socket.
5. **Clean Teardown**:
   - Bidirectional I/O uses `tokio::select!` so that closure on either side immediately tears down the full socket pair, preventing lingering `CLOSE_WAIT` sockets.
   - A 60-second sliding idle timer closes abandoned connections without interrupting long-lived active streams.

#### 5. Resilience & Authentication Recovery
- **Shared Token Context**: All paths share an `Arc<Mutex<String>>` authentication token.
- **Automated Re-Auth**: If any path receives an `AUTH_EXPIRED` (401) notice from the backend, it signs a new ECDSA challenge with the local wallet and updates the token. All other paths immediately inherit the fresh token on their next reconnection.
- **Jittered Exponential Backoff**: Disconnections trigger reconnect attempts starting at 1s, doubling up to 60s with a ±20% randomized jitter to avoid thundering-herd reconnect spikes.
- **Liveness Watchdog**: Each path sends a heartbeat frame every 15 seconds (`{"type": "heartbeat", "active_streams": N, ...}`) and resets a 90-second connection watchdog timer.

---

### Seller CLI Commands & Options

```bash
proxybase-cli seller start [OPTIONS]
```

#### Command Options

| Flag | Description |
|---|---|
| `--upstream <HOST:PORT>` | Upstream SOCKS5 proxy address (e.g. `proxy.vendor.com:1080`). Repeatable. |
| `--upstream-user <USER>` | Username for the paired `--upstream` proxy. Repeatable. |
| `--upstream-pass <PASS>` | Password for the paired `--upstream` proxy. Repeatable. |
| `--no-direct` | Disable node's direct bandwidth. Only relay through `--upstream` proxies. |
| `--volunteer` | Run in volunteer mode (donate bandwidth unpaid, 0 seller share). |
| `--foreground` | Keep process running in the terminal instead of daemonizing to background. |
| `--backend <URL>` | Override backend API URL (default: `https://api.proxybase.xyz`). |

#### Management Commands

- `proxybase-cli seller status` — Inspect background daemon PID, node type, and live backend pool statistics.
- `proxybase-cli seller stop` — Stop the background seller daemon (sends `SIGTERM`, escalates to `SIGKILL` after 5s) and remove any autostart service.
- `proxybase-cli seller install` — Install seller daemon as a persistent OS service (`systemd` on Linux, `launchd` on macOS) that automatically boots on system startup.
- `proxybase-cli seller payout create --amount <microcredits> --tempo-address <address>` — Lock accrued earnings for withdrawal (1,000,000 microcredits = $1.00 USD).
- `proxybase-cli seller payout list` — View payout transaction history and on-chain status.

---

### Usage Examples

#### 1. Direct-Only Seller (Standard Mode)
Monetize your local device's internet connection:
```bash
proxybase-cli seller start
```

#### 2. Single Upstream Proxy
Resell access through an external SOCKS5 proxy while keeping local bandwidth active:
```bash
proxybase-cli seller start \
  --upstream residential.proxyprovider.net:8000 \
  --upstream-user resi_user_102 \
  --upstream-pass secretpassword123
```

#### 3. Multiple Upstream Proxies (Hybrid Pool)
Run multiple upstream proxies and direct bandwidth concurrently:
```bash
proxybase-cli seller start \
  --upstream us-east.provider.com:1080 --upstream-user userA --upstream-pass passA \
  --upstream eu-west.provider.com:1080 --upstream-user userB --upstream-pass passB \
  --upstream ap-south.provider.com:1080 --upstream-user userC --upstream-pass passC
```
*The CLI will spawn 4 independent paths: `direct`, `upstream_0`, `upstream_1`, and `upstream_2`.*

#### 4. Resell Upstream Proxies ONLY (Zero Direct Bandwidth)
Do not expose or route traffic through the host server's local IP:
```bash
proxybase-cli seller start \
  --no-direct \
  --upstream socks5.premiumprovider.com:9050 \
  --upstream-user customer456 \
  --upstream-pass tokenXYZ
```

#### 5. Run in Foreground (Debugging & Containers)
Keep standard output attached to inspect live connection events and traffic logs:
```bash
proxybase-cli seller start --foreground \
  --upstream 10.0.0.50:1080 \
  --upstream-user proxyuser \
  --upstream-pass proxypass
```

---

### Configuration Persistence & OS Services

When `proxybase-cli seller start` is called with upstream parameters, settings are automatically serialized to:
`~/.proxybase/seller_config.json` (or `$PROXYBASE_DIR/seller_config.json`).

```json
{
  "upstream_proxies": [
    {
      "address": "residential.proxyprovider.net:8000",
      "username": "resi_user_102",
      "password": "secretpassword123",
      "country": "US",
      "proxy_category": "residential"
    }
  ],
  "no_direct": false,
  "volunteer": false
}
```

#### Daemon Restarts & Reboot Survival
- When the daemon starts without arguments or is managed by the OS service manager (`systemd` or `launchd`), it automatically reads `seller_config.json` and restores the exact upstream topology.
- Installing as a background service:
  ```bash
  proxybase-cli seller install
  ```
- Stopping and removing service:
  ```bash
  proxybase-cli seller stop
  ```

---

### Container & Fleet Deployment (Environment Variables)

When deploying with Docker, Kubernetes, or serverless container runners, upstream configuration can be passed via environment variables. The entrypoint script (`docker-entrypoint.sh`) parses these and launches the seller daemon.

| Variable | Example | Description |
|---|---|---|
| `PROXYBASE_UPSTREAM` | `10.0.1.1:1080,10.0.1.2:1080` | Comma-separated list of upstream proxy `host:port` addresses. |
| `PROXYBASE_UPSTREAM_USER` | `user1,user2` | Comma-separated list of upstream usernames (paired by index). |
| `PROXYBASE_UPSTREAM_PASS` | `pass1,pass2` | Comma-separated list of upstream passwords (paired by index). |
| `PROXYBASE_NO_DIRECT` | `true` | Set to `true` to resell upstreams only (disables direct node bandwidth). |
| `PROXYBASE_VOLUNTEER` | `false` | Set to `true` to donate bandwidth unpaid. |
| `MASTER_MNEMONIC` | `word1 word2 ... word12` | 12/24-word master seed for HD key derivation. |
| `PROXYBASE_HD_INDEX` | `0` | Explicit HD child index (`m/44'/60'/0'/0/{index}`). |
| `PROXYBASE_DIR` | `/tmp/proxybase` | Path for keystore, logs, and config files. |

#### Docker Run Example
```bash
docker run -d \
  --name proxybase-node \
  -e MASTER_MNEMONIC="apple banana cherry dog elephant fox grape horse igloo jaguar kite lemon" \
  -e PROXYBASE_HD_INDEX=0 \
  -e PROXYBASE_UPSTREAM="proxy1.reseller.com:8000,proxy2.reseller.com:8000" \
  -e PROXYBASE_UPSTREAM_USER="clientA,clientB" \
  -e PROXYBASE_UPSTREAM_PASS="passA,passB" \
  -e PROXYBASE_NO_DIRECT="false" \
  ghcr.io/proxybase/proxybase-cli:latest
```

---

## HD Fleet Wallets

ProxyBase supports hierarchical deterministic (BIP-32 / BIP-44) wallet derivation to run large-scale node fleets from a single master mnemonic without sharing private keys across nodes.

Each fleet node derives a dedicated child keypair at path `m/44'/60'/0'/0/{index}`:

```bash
# Derive and import child identity index 3
proxybase-cli wallet import "<master phrase>" --hd-index 3
proxybase-cli login
proxybase-cli seller start
```

### Central Fleet Sweeper

To collect earnings from hundreds or thousands of worker nodes, sweep child accounts in a single automated command:

```bash
proxybase-cli wallet sweep "<master phrase>" \
  --start-index 0 \
  --count 100 \
  --min-threshold 1000000 \
  --target-tempo 0x71C2B4958189874a7D1F6bC1D2A1f6e02F179782
```

- **Derives each key in-memory**: Leaves on-disk operator credentials untouched.
- **Queries ledger balances**: Checks available earnings even if child nodes are temporarily offline.
- **Threshold filtering**: Skips nodes below `--min-threshold` (in microcredits).
- **Creates automated payouts**: Dispatches payouts directly to the destination Tempo wallet.

For complete Kubernetes StatefulSet manifests and cronjob sweeper scripts, see [`docs/HD_WALLETS.md`](docs/HD_WALLETS.md) and [`deploy/`](deploy/).

---

## Buyer & Market Operations

Purchase and use proxy sessions directly through the CLI:

```bash
# 1. Check account balance
proxybase-cli buyer balance

# 2. Deposit funds (USDC on Solana)
proxybase-cli buyer deposit create --amount 5000000 --currency usdcsol
proxybase-cli buyer deposit list

# 3. Check market pricing for country and network type
proxybase-cli market prices --country US --network-type residential

# 4. Buy a rotating proxy session
proxybase-cli market buy --country US --network-type residential --session-type rotating

# 5. Buy a sticky session with a local SOCKS5 bridge on port 10800
proxybase-cli market buy \
  --country DE \
  --network-type datacenter \
  --session-type sticky \
  --sticky-duration 3600 \
  --bridge \
  --bridge-port 10800
```

---

## Local SOCKS5 Bridge

Many applications (web scrapers, command-line utilities, browsers) do not natively support SOCKS5 username/password authentication. The `bridge` command exposes a local, unauthenticated SOCKS5 listener (`127.0.0.1:<PORT>`) that transparently relays traffic to the authenticated remote ProxyBase gateway:

```bash
# Start a local bridge daemon for session
proxybase-cli bridge start <SESSION_ID> --port 10800

# View active bridges
proxybase-cli bridge list

# Route traffic through local bridge without auth headers
curl --socks5 127.0.0.1:10800 https://api.ipify.org

# Stop the bridge
proxybase-cli bridge stop <SESSION_ID>
```

---

## Self-Update

Check for and install updates to `proxybase-cli`:

```bash
# Check if a new version is available
proxybase-cli update --check

# Download and replace binary with the latest release
proxybase-cli update
```

Every command also performs a passive daily check in the background and notifies stderr if a newer release exists.

---

## License

Apache-2.0 / MIT. See repository LICENSE for details.
