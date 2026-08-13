# proxybase-cli

Official command-line client for ProxyBase.

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
