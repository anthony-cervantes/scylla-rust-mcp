# Contributing

Thanks for your interest in contributing!

Development Setup
- Rust stable (edition 2021)
- OpenSSL headers (e.g., `libssl-dev`, `pkg-config`)

Workflow
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Test: `cargo test` (integration tests are `#[ignore]` unless `SCYLLA_URI` is set)

Commit Style
- Use concise, imperative messages: e.g., `feat: add partition_rows tool`

Security
- Do not include secrets in code, examples, or tests.
- Report vulnerabilities via Security Advisories (see `SECURITY.md`).

Code of Conduct
- Be respectful and inclusive. If issues arise, contact the maintainers.

