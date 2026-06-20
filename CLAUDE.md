# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`proxybase-cli` is the command-line client for the ProxyBase proxy marketplace. It supports three roles: **wallet** management, **seller** (sell/relay bandwidth to the network), and **buyer** (purchase proxy sessions). All commands talk to the ProxyBase backend REST + WebSocket API.

## Build & Run

```bash
cargo build
cargo run -- <subcommand> ...
cargo test
```

## Architecture

The entire CLI lives in a single file — `src/main.rs`. It depends on the sibling crate `libproxybase` (at `../libproxybase`) for wallet crypto: secp256k1 keypairs, BIP-39 mnemonics, and encrypted keystore storage.

**Commands**: `wallet` (create/import/info), `login`, `seller` (start/stop/status/payout/install/uninstall), `buyer` (balance/deposit/transfer), `market` (countries/currencies/prices/buy/close/sessions/session-status), `health`, `version`.

**Wallet** (`~/.proxybase/`):
- Keyfile: `~/.proxybase/wallet/keyfile.enc` (encrypted secp256k1 signing key)
- Session token: `~/.proxybase/session_token` (plaintext bearer token from auth)
- Seller config: `~/.proxybase/seller_config.json` (upstream proxies + settings for daemon)
- PID file: `~/.proxybase/proxybase-seller.pid` (running daemon PID)
- Log file: `~/.proxybase/seller.log` (daemon stdout/stderr)

**Auth flow**: challenge-response using ECDSA (secp256k1). Backend sends a `nonce` + `timestamp`, CLI signs `address:nonce:timestamp`, backend verifies the signature against the SEC1-encoded public key (not the derived address). Returns a session token saved to disk.

**Seller mode** (`seller start`): Opens a persistent WebSocket to `{backend}/v2/ws/seller?token=...`. The backend sends `stream_open` messages with `target_ip`, `target_port`, `session_id`, and an optional `route_index`. The CLI connects to the target (directly via TCP, or through an upstream SOCKS5 proxy if `--upstream` was provided), then relays bidirectionally: TCP reads → base64-encoded `relay_response` WebSocket messages, `relay_data` WebSocket messages → TCP writes. Includes auto-reconnect with exponential backoff (1s → 60s max, ±jitter on failure; immediate reconnect on clean disconnect).

**Seller daemon** (`daemon-kit` crate): `seller start` daemonizes by default (forks to background on Unix). Seller config is persisted to `~/.proxybase/seller_config.json` so the daemon can restart without CLI args. `seller stop` sends SIGTERM (SIGKILL after 5s). `seller install` creates a launchd plist (macOS) or systemd user unit (Linux) with `RunAtLoad`/`KeepAlive` so the seller survives reboots. The service manager calls `proxybase-cli seller start --foreground` which loads saved config and runs the seller loop without daemonizing. `seller uninstall` removes the service. `seller stop`/`install`/`uninstall` do not require authentication.

**Upstream routing**: The `pool` is a `Vec<Option<UpstreamProxy>>` where `None` represents direct connection. Multi-upstream + direct is supported. Route selection uses `route_index` from the backend if provided, otherwise hashes the `session_id` across the pool.

**Buyer/Market**: Standard REST calls to `/v2/` endpoints. `market buy` prints SOCKS5 credentials (`127.0.0.1:1082`, username = session_id, password = session token).

## Key Dependencies

- `libproxybase` — sibling crate at `../libproxybase`, used only for `WalletManager`
- `fast-socks5` — vendored SOCKS5 client at `../proxybase2-backend/fast-socks5`, used for upstream proxy connections
- `clap` (derive) — CLI parsing
- `tokio-tungstenite` — WebSocket client for seller relay
- `k256` — ECDSA signing (secp256k1)
- `daemon-kit` — cross-platform daemonization + OS service installation (launchd/systemd/Windows Service)

## Backend URL

Hardcoded in `src/main.rs` via `DEFAULT_BACKEND_URL`:
- Debug builds: `http://localhost:8080`
- Release builds: `https://api.proxybase.xyz`

Override with `--backend <url>` on any command.
