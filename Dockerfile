# -- Build Stage --
FROM rust:1.80-slim-bookworm AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY libproxybase ./libproxybase
COPY proxybase-cli ./proxybase-cli

WORKDIR /usr/src/app/proxybase-cli

RUN cargo build --release --bin proxybase-cli

# -- Runtime Stage --
FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="ProxyBase CLI"
LABEL org.opencontainers.image.description="Command-line interface client for ProxyBase"
LABEL org.opencontainers.image.licenses="MIT"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 proxybase && \
    useradd -u 1000 -g proxybase -s /bin/bash -m proxybase

WORKDIR /home/proxybase

COPY --from=builder /usr/src/app/proxybase-cli/target/release/proxybase-cli /usr/local/bin/proxybase-cli

USER proxybase

ENTRYPOINT ["/usr/local/bin/proxybase-cli"]
