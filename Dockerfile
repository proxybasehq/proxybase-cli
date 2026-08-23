# -- Build Stage --
FROM rust:1-slim-bookworm AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# fast-socks5 is a git dependency (proxybase-cli/Cargo.toml), so the build
# stage needs network access — no vendored copies required.
COPY libproxybase ./libproxybase
COPY proxybase-cli ./proxybase-cli

WORKDIR /usr/src/app/proxybase-cli

RUN cargo build --release --bin proxybase-cli

# -- Runtime Stage --
FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="ProxyBase CLI"
LABEL org.opencontainers.image.description="Command-line interface client for ProxyBase (HD fleet seller nodes)"
LABEL org.opencontainers.image.licenses="MIT"

# tini = proper PID 1 signal forwarding (WebSocket CloseFrame on SIGTERM);
# bash = entrypoint runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    tini \
    bash \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 proxybase && \
    useradd -u 1000 -g proxybase -s /bin/bash -m proxybase

RUN mkdir -p /home/proxybase/.proxybase/wallet && \
    chown -R proxybase:proxybase /home/proxybase/.proxybase

WORKDIR /home/proxybase

COPY --from=builder /usr/src/app/proxybase-cli/target/release/proxybase-cli /usr/local/bin/proxybase-cli
COPY proxybase-cli/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Make the entrypoint script executable on all archs (COPY preserves host perms)
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER proxybase

# Liveness = backend reachable AND local seller process alive.
# `seller status` exits 0 even when the backend is unreachable (it prints
# "Backend: unreachable"), so `health` covers reachability while the grep
# covers process liveness. The WS tunnel itself self-heals via the seller's
# reconnect loop, so it is deliberately not part of the health signal.
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD proxybase-cli --backend "${PROXYBASE_BACKEND:-https://api.proxybase.xyz}" health \
    && proxybase-cli --backend "${PROXYBASE_BACKEND:-https://api.proxybase.xyz}" seller status | grep -q "running (PID:" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
