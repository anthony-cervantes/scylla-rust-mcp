# crates.io Publish Checklist

Use this before pushing a release tag.

## Preflight
- Confirm `Cargo.toml` has the intended `package.version`
- Run `cargo fmt --all`
- Run `cargo clippy --all-targets -- -D warnings`
- Run `cargo test`
- Run `cargo publish --dry-run`
- Run `cargo package --list` and sanity-check the files being shipped

## Repo Setup
- Confirm the GitHub Actions secret `CARGO_REGISTRY_TOKEN` is set for this repository
- Confirm the crates.io package name `scylla-rust-mcp` is available, or that you already own it

## Release
- Merge the release PR to `main`
- Push annotated tag `vX.Y.Z`
- Confirm the `Release` workflow publishes the crate and uploads the release archives
- Verify `cargo install scylla-rust-mcp` works after publish
