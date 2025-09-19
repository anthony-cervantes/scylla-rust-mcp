# scylla-rust-mcp

Read-only MCP (Model Context Protocol) server for ScyllaDB.

This README focuses on two things:
- How to configure and run the Docker image (recommended)
- Minimal developer setup for local builds

## Docker Usage (Recommended)

Build locally (optional):

```bash
docker build -t scylla-rust-mcp:latest .
```

Run the server (stdio transport):

```bash
# Plaintext
docker run --rm -i \
  -e SCYLLA_URI=host.docker.internal:9042 \
  scylla-rust-mcp:latest

# TLS
docker run --rm -i \
  -e SCYLLA_URI=my-scylla.example.com:9142 \
  -e SCYLLA_SSL=true \
  scylla-rust-mcp:latest

# TLS + custom CA bundle
docker run --rm -i \
  -v /absolute/path/ca.pem:/ca.pem:ro \
  -e SCYLLA_URI=my-scylla.example.com:9142 \
  -e SCYLLA_SSL=true \
  -e SCYLLA_CA_BUNDLE=/ca.pem \
  scylla-rust-mcp:latest
```

Environment variables (inside the container):
- `SCYLLA_URI` (required): `host:port` of Scylla/Cassandra endpoint
- `SCYLLA_USER`, `SCYLLA_PASS` (optional): username/password auth
- `SCYLLA_SSL` (optional): `true`/`1` to enable TLS
- `SCYLLA_CA_BUNDLE` (optional): absolute path to a CA bundle inside the container
- `SCYLLA_SSL_INSECURE` (optional): `true`/`1` to skip certificate verification (lab only)
- `RUST_LOG` (optional): log level, e.g. `info`, `debug`

Codex TOML (Docker)

When `command = "docker"`, the `env` table applies to the docker CLI, not the container. Pass container env with `-e` flags in `args`.

Plaintext (macOS/Windows):

```toml
[mcp.servers."scylla-rust-mcp"]
command = "docker"
args = [
  "run", "--rm", "-i",
  "-e", "SCYLLA_URI=host.docker.internal:9042",
  "scylla-rust-mcp:latest"
]
```

TLS + custom CA:

```toml
[mcp.servers."scylla-rust-mcp"]
command = "docker"
args = [
  "run", "--rm", "-i",
  "-v", "/absolute/path/ca.pem:/ca.pem:ro",
  "-e", "SCYLLA_URI=my-scylla.example.com:9142",
  "-e", "SCYLLA_SSL=true",
  "-e", "SCYLLA_CA_BUNDLE=/ca.pem",
  "scylla-rust-mcp:latest"
]
```

Auth example:

```toml
[mcp.servers."scylla-rust-mcp"]
command = "docker"
args = [
  "run", "--rm", "-i",
  "-e", "SCYLLA_URI=my-scylla.example.com:9142",
  "-e", "SCYLLA_USER=scylla",
  "-e", "SCYLLA_PASS=secret",
  "scylla-rust-mcp:latest"
]
```

Linux host networking notes
- On Linux, `host.docker.internal` may not resolve; use the host IP (e.g., `192.168.x.x:9042`) or mapped ports.

## Dev Setup (Local)

Prereqs
- Rust stable (edition 2021)
- OpenSSL headers for TLS (if using `SCYLLA_SSL=true`):
  - macOS: `brew install openssl@3`
  - Debian/Ubuntu: `sudo apt-get install -y libssl-dev pkg-config`

Run locally (stdio server):

```bash
export SCYLLA_URI=127.0.0.1:9042
cargo run --features mcp
```

Quality checks
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Tests: `cargo test` (integration tests are `#[ignore]` unless `SCYLLA_URI` is set)
