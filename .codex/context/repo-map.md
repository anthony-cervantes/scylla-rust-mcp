# scylla-rust-mcp Repo Map

## Purpose
- Rust MCP server that exposes ScyllaDB/Cassandra discovery and read-only query tools over stdio.
- Primary runtime surfaces are MCP JSON-RPC over stdio, ScyllaDB session setup, CQL generation, schema discovery/cache, and env-driven TLS/auth configuration.

## Entry Points
- `src/main.rs`: initializes tracing and runs the stdio MCP server.
- `src/lib.rs::mcp`: legacy/manual MCP request handling and tool dispatch.
- `src/lib.rs::rmcp_server`: `rmcp` bridge handler for tools/list and tools/call.
- `src/lib.rs::codex_stdio`: newline and Content-Length stdio framing, JSON-RPC message handling, responses.

## Important Modules
- `schema`: small JSON value/type helper.
- `server`: server metadata, tool list, tool input schemas.
- `logging`: tracing initialization.
- `mcp`: `ToolExecutor`, request parsing, shared Scylla session state, env config.
- `db`: ScyllaDB queries, CQL construction, identifier/filter/order validation, pagination cursors, schema search.

## Security-Relevant Boundaries
- Untrusted MCP client input reaches `ToolExecutor::execute_request`, `codex_stdio::handle_message`, and db helper arguments.
- CQL identifiers and clauses are built from client-provided keyspace/table/column/filter/order inputs; values should remain prepared/bound where possible.
- Pagination cursors are client-controlled serialized state.
- Environment variables configure Scylla URI, TLS, credentials, timeouts, and schema cache behavior.
- Stdio framing accepts client-controlled message size and JSON content.

## Tests
- Unit tests are colocated in `src/lib.rs`.
- Integration-style tests live under `tests/`, including MCP tool schema/stdio behavior, value conversion, and ignored live Scylla integration tests.
- Narrow commands: `cargo test <name>`, `cargo test --test <file>`, `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`.
