# syntax=docker/dockerfile:1

# --- Build stage ---
FROM rust:slim-bookworm as builder
WORKDIR /app

# System deps for linking (OpenSSL) and pkg-config
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates build-essential && \
    rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs
RUN cargo build --release --features mcp || true

# Build
COPY . .
RUN cargo build --release --features mcp

# --- Runtime stage ---
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=info
ENV SCYLLA_URI=127.0.0.1:9042

COPY --from=builder /app/target/release/scylla-rust-mcp /usr/local/bin/scylla-rust-mcp

# Default command runs the MCP stdio server
ENTRYPOINT ["/usr/local/bin/scylla-rust-mcp"]
