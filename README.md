# scylla-rust-mcp

Read-only MCP (Model Context Protocol) server for ScyllaDB that exposes safe data discovery and query tools over stdio for AI agents and MCP clients.

![CI](https://github.com/anthony-cervantes/scylla-rust-mcp/actions/workflows/ci.yml/badge.svg)
![Release](https://github.com/anthony-cervantes/scylla-rust-mcp/actions/workflows/release.yml/badge.svg)

## Features
- Read-only ScyllaDB access (no writes or schema changes)
- TLS (OpenSSL) + optional username/password auth
- Prepared statements for fast, safe queries
- Pagination with cursors (`paged_select`)
- Schema discovery (`search_schema`) and rich introspection tools
- Shared connection/session and lightweight schema cache

Supported tools (MCP):
- `list_keyspaces`, `list_tables`, `describe_table`
- `list_indexes`, `list_views`, `keyspace_replication`
- `list_udts`, `list_functions`, `list_aggregates`
- `cluster_topology`, `size_estimates`
- `sample_rows`, `select`, `paged_select`, `partition_rows`
- `search_schema`

## Quick Start (Docker)

Use the prebuilt image if available, or build locally.

Build locally:
```bash
docker build -t scylla-rust-mcp:latest .
```

Run (stdio transport):
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

# TLS + custom CA
docker run --rm -i \
  -v /absolute/path/ca.pem:/ca.pem:ro \
  -e SCYLLA_URI=my-scylla.example.com:9142 \
  -e SCYLLA_SSL=true \
  -e SCYLLA_CA_BUNDLE=/ca.pem \
  scylla-rust-mcp:latest
```

Environment:
- `SCYLLA_URI` (required): `host:port` for Scylla/Cassandra
- `SCYLLA_USER`, `SCYLLA_PASS` (optional): credentials
- `SCYLLA_SSL` (optional): `true`/`1` to enable TLS
- `SCYLLA_CA_BUNDLE` (optional): absolute path to CA bundle in the container
- `SCYLLA_SSL_INSECURE` (optional): `true`/`1` to skip verification (dev only)
- `RUST_LOG` (optional): log level, e.g. `info`, `debug`

Linux note: `host.docker.internal` may not resolve; use your host IP or mapped ports.

## Use With MCP Clients

MCP clients (e.g., Codex) can launch the server via Docker. Below are sanitized examples using the Codex configuration style you provided, plus an example using the published GHCR image.

Plaintext (localhost):
```toml
[mcp_servers.scylla-rust-mcp]
command = "docker"
args = [
  "run","--rm","-i",
  "-e","SCYLLA_URI=host.docker.internal:9042",
  "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
]
```

TLS (insecure) with auth (placeholders):
```toml
[mcp_servers.scylla-rust-mcp]
command = "docker"
args = [
  "run","--rm","-i",
  "-e","SCYLLA_URI=scylla.example.com:9142",
  "-e","SCYLLA_USER=scylla",
  "-e","SCYLLA_PASS=secret",
  "-e","SCYLLA_SSL=true",
  "-e","SCYLLA_SSL_INSECURE=true",
  "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
]
```

TLS with custom CA bundle:
```toml
[mcp_servers.scylla-rust-mcp]
command = "docker"
args = [
  "run","--rm","-i",
  "-v","/absolute/path/ca.pem:/ca.pem:ro",
  "-e","SCYLLA_URI=scylla.example.com:9142",
  "-e","SCYLLA_SSL=true",
  "-e","SCYLLA_CA_BUNDLE=/ca.pem",
  "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
]
```

Pull the image from GHCR:
```bash
docker pull ghcr.io/anthony-cervantes/scylla-rust-mcp:latest
```

### JSON Examples (Claude Desktop and other MCP clients)

Claude Desktop and many MCP clients use a JSON configuration with an `mcpServers` object. Below are sanitized JSON examples mirroring the Docker/TLS cases above.

Plaintext (localhost):
```json
{
  "mcpServers": {
    "scylla-rust-mcp": {
      "command": "docker",
      "args": [
        "run","--rm","-i",
        "-e","SCYLLA_URI=host.docker.internal:9042",
        "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
      ]
    }
  }
}
```

TLS (insecure) with auth (placeholders):
```json
{
  "mcpServers": {
    "scylla-rust-mcp": {
      "command": "docker",
      "args": [
        "run","--rm","-i",
        "-e","SCYLLA_URI=scylla.example.com:9142",
        "-e","SCYLLA_USER=scylla",
        "-e","SCYLLA_PASS=secret",
        "-e","SCYLLA_SSL=true",
        "-e","SCYLLA_SSL_INSECURE=true",
        "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
      ]
    }
  }
}
```

TLS with custom CA bundle:
```json
{
  "mcpServers": {
    "scylla-rust-mcp": {
      "command": "docker",
      "args": [
        "run","--rm","-i",
        "-v","/absolute/path/ca.pem:/ca.pem:ro",
        "-e","SCYLLA_URI=scylla.example.com:9142",
        "-e","SCYLLA_SSL=true",
        "-e","SCYLLA_CA_BUNDLE=/ca.pem",
        "ghcr.io/anthony-cervantes/scylla-rust-mcp:latest"
      ]
    }
  }
}
```

## Local Development

Prerequisites
- Rust stable (edition 2021)
- OpenSSL headers for TLS
  - macOS: `brew install openssl@3`
  - Debian/Ubuntu: `sudo apt-get install -y libssl-dev pkg-config`

Build and run (stdio server):
```bash
export SCYLLA_URI=127.0.0.1:9042
cargo run
```

Quality checks
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Tests: `cargo test` (some integration tests are `#[ignore]` unless `SCYLLA_URI` is set)

## Roadmap
- Partition reads: `partition_rows` (strict prepared PK binding)
- Paged tests: add ignored integration tests for `paged_select` and `search_schema`
- Schema validation: fail early on invalid column/filter/order_by
- Observability: add row counts/durations to spans; optional metrics
- CI: boot Scylla via Docker and run ignored integration tests

## Contributing
We welcome issues and PRs. Please:
- Follow rustfmt defaults: `cargo fmt --all`
- Lint cleanly: `cargo clippy --all-targets --all-features -- -D warnings`
- Add tests where possible (unit in-file, integration under `tests/`)
- Avoid committing secrets; use env vars

## Security
This server is read-only by design. If you discover a security issue, please open a private issue or contact the maintainer.

## License
Licensed under either of
- Apache License, Version 2.0 (see `LICENSE-APACHE`)
- MIT License (see `LICENSE-MIT`)

at your option.
