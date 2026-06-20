# proxybase-cli

Official command-line client for ProxyBase.

## Build

```bash
cargo build --release
```

## GitHub Actions

This repository includes two workflows:

- CI workflow: builds the CLI on Linux, Windows, and macOS (Intel + Apple Silicon)
- Release workflow: builds release artifacts for all targets and publishes them to a GitHub Release

## Release

Push a tag using this format to publish binaries:

```bash
git tag proxybase-cli-v0.1.0
git push origin proxybase-cli-v0.1.0
```

The release workflow will upload:

- `proxybase-cli-x86_64-unknown-linux-gnu.tar.gz`
- `proxybase-cli-x86_64-pc-windows-msvc.zip`
- `proxybase-cli-x86_64-apple-darwin.tar.gz`
- `proxybase-cli-aarch64-apple-darwin.tar.gz`
