# proxybase-cli

Official command-line client for ProxyBase.

## HD fleet wallets

Run an entire seller fleet off one master mnemonic: each node derives a
distinct BIP-44 child wallet at `m/44'/60'/0'/0/{index}`.

```bash
# Node index 3
proxybase-cli wallet import "<master phrase>" --hd-index 3
proxybase-cli login

# Sweep all children's earnings into one central Tempo wallet
proxybase-cli wallet sweep "<master phrase>" --count 100 --target-tempo 0x71C...
```

Container deployment (Kubernetes StatefulSet, Docker Compose, entrypoint
env vars, fleet sweeper cron) is documented in
[`docs/HD_WALLETS.md`](docs/HD_WALLETS.md); manifests live in [`deploy/`](deploy/).

## Build

```bash
cargo build --release
```

## GitHub Actions

This repository includes two workflows:

- CI workflow: builds the CLI on Linux x86_64, Windows x86_64, and macOS (Apple Silicon)
- Release workflow: builds release artifacts for all targets and publishes them to a GitHub Release

## Release

Push a tag using this format to publish binaries:

```bash
git tag proxybase-cli-v0.1.0
git push origin proxybase-cli-v0.1.0
```

The release workflow uploads archives for manual installs plus raw binaries and a `SHA256SUMS` file used by the self-updater.

## Self-update

```bash
proxybase-cli update           # install the latest release
proxybase-cli update --check   # only report whether a new version exists
```

Supported platforms: Linux x86_64, Windows x86_64, macOS (Apple Silicon).

Every other command also checks for updates once per day and prints a notice to stderr when a newer version is available.

Notes:

- On macOS the replaced binary is unsigned (self-replace strips any signature) — fine for a dev CLI, but Gatekeeper users may need to allow it once.
- Inside Docker the update applies only to the current container and is lost on restart; use the published image instead.
- For testing against a fork: `PROXYBASE_UPDATE_REPO=owner/repo proxybase-cli update --check`
