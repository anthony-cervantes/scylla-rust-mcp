# scylla-rust-mcp

Read-only MCP (Model Context Protocol) server for ScyllaDB that exposes safe data discovery and query tools over stdio for AI agents and MCP clients.

![CI](https://github.com/anthony-cervantes/scylla-rust-mcp/actions/workflows/ci.yml/badge.svg)
![Release](https://github.com/anthony-cervantes/scylla-rust-mcp/actions/workflows/release.yml/badge.svg)

## Features
- Read-only ScyllaDB access (no writes or schema changes)
- TLS (OpenSSL) plus optional username/password auth
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

## Quick Start

For Codex and other local MCP clients, the simplest path is to run the binary directly.

Install from crates.io with Cargo:
```bash
cargo install scylla-rust-mcp
```

For unreleased changes, you can still install from Git:
```bash
cargo install --git https://github.com/anthony-cervantes/scylla-rust-mcp scylla-rust-mcp
```

Run it:
```bash
export SCYLLA_URI=127.0.0.1:9042
scylla-rust-mcp
```

Environment:
- `SCYLLA_URI` (required): `host:port` for Scylla/Cassandra
- `SCYLLA_USER`, `SCYLLA_PASS` (optional): credentials
- `SCYLLA_SSL` (optional): `true` or `1` to enable TLS
- `SCYLLA_CA_BUNDLE` (optional): absolute path to a CA bundle on the host
- `SCYLLA_SSL_INSECURE` (optional): `true` or `1` to skip verification (dev only)
- `RUST_LOG` or `MCP_SERVER_LOG` (optional): enable stderr logging, for example `info` or `debug`. The default stdio server stays quiet unless one of these is set.
- `MCP_FRAMING` (optional): `content-length` (default) for Codex-compatible MCP stdio framing, or `newline` for the legacy newline-delimited JSON transport (manual testing only)
- `SCYLLA_WARMUP_ON_START` (optional): set to `1` to eagerly connect to Scylla during startup. Default is off so MCP initialization stays fast and quiet.

## Use With MCP Clients

Codex-style TOML:
```toml
[mcp_servers.scylla-rust-mcp]
command = "scylla-rust-mcp"
env = { MCP_FRAMING = "content-length", SCYLLA_URI = "127.0.0.1:9042" }
```

TLS with auth:
```toml
[mcp_servers.scylla-rust-mcp]
command = "scylla-rust-mcp"
env = { MCP_FRAMING = "content-length", SCYLLA_URI = "scylla.example.com:9142", SCYLLA_USER = "scylla", SCYLLA_PASS = "<password>", SCYLLA_SSL = "true" }
```

TLS with a custom CA bundle:
```toml
[mcp_servers.scylla-rust-mcp]
command = "scylla-rust-mcp"
env = { MCP_FRAMING = "content-length", SCYLLA_URI = "scylla.example.com:9142", SCYLLA_SSL = "true", SCYLLA_CA_BUNDLE = "/absolute/path/ca.pem" }
```

JSON example:
```json
{
  "mcpServers": {
    "scylla-rust-mcp": {
      "command": "scylla-rust-mcp",
      "env": {
        "MCP_FRAMING": "content-length",
        "SCYLLA_URI": "127.0.0.1:9042"
      }
    }
  }
}
```

## Packaging Notes

- Once the crate is published, install it with `cargo install scylla-rust-mcp`.
- Until then, Cargo can install this project directly from Git with `cargo install --git ...`.
- GitHub Packages is not a Cargo registry, so it is not the right place for `cargo install`.
- Tagged releases upload prebuilt Linux, Intel macOS, and Apple Silicon macOS archives to GitHub Releases.

## Local Development

Prerequisites:
- Rust stable (edition 2021)
- OpenSSL headers for TLS
  - macOS: `brew install openssl@3`
  - Debian/Ubuntu: `sudo apt-get install -y libssl-dev pkg-config`

Build and run:
```bash
export SCYLLA_URI=127.0.0.1:9042
cargo run

# For manual newline-delimited JSON testing
MCP_FRAMING=newline cargo run
```

Quality checks:
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Tests: `cargo test` (some integration tests are `#[ignore]` unless `SCYLLA_URI` is set)

### Justfile
- `just check` - fmt, clippy, tests
- `just run` - run the stdio server locally
- `just install` - install the binary into Cargo's local bin directory
- `just version-show` - print current package version
- `just version-set 0.1.7` - bump `Cargo.toml` version and commit
- `just tag` - create and push tag `v<current-version>`
- `just release-local [x.y.z]` - clean-tree local release: bump, commit, local tag, install from tag, verify install

## Roadmap
- Partition reads: `partition_rows` (strict prepared PK binding)
- Paged tests: add ignored integration tests for `paged_select` and `search_schema`
- Schema validation: fail early on invalid column/filter/order_by
- Observability: add row counts/durations to spans and optional metrics
- CI: boot Scylla during integration testing and run ignored integration tests

## Contributing

We welcome issues and PRs. Please:
- Follow rustfmt defaults: `cargo fmt --all`
- Lint cleanly: `cargo clippy --all-targets -- -D warnings`
- Add tests where possible (unit in-file, integration under `tests/`)
- Avoid committing secrets; use env vars

## Security

This server is read-only by design. If you discover a security issue, please report it privately via GitHub Security Advisories so maintainers can triage and fix before disclosure. See `SECURITY.md` for details.

## License

Licensed under either of:
- Apache License, Version 2.0 (see `LICENSE-APACHE`)
- MIT License (see `LICENSE-MIT`)

at your option.
