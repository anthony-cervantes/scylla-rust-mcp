# Project task runner (https://github.com/casey/just)

set shell := ["bash", "-uc"]

default:
  @just --list

# Show current package version from Cargo.toml
version-show:
  set -euo pipefail
  rg -n '^version\s*=\s*"' Cargo.toml -n | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/' | head -n1

# Set package version in Cargo.toml (within [package]) and commit
version-set version:
  set -euo pipefail
  awk 'BEGIN{v="{{version}}"; inpkg=0; done=0} /^\[package\]/{inpkg=1} /^\[.*\]/{ if ($0 !~ /^\[package\]/) inpkg=0 } inpkg && /^version *= *"/ && !done { sub(/"[0-9]+\.[0-9]+\.[0-9]+"/,"\"" v "\""); done=1 } { print }' Cargo.toml > Cargo.toml.tmp
  mv Cargo.toml.tmp Cargo.toml
  git add Cargo.toml
  git commit -m "chore(release): bump version to {{version}}"

# Create and push annotated tag `v<version>`; if version omitted, reads Cargo.toml
tag version="":
  set -euo pipefail
  v="{{version}}"
  if [ -z "$v" ]; then v="$(just version-show)"; fi
  git tag -a "v${v}" -m "v${v}"
  git push origin "v${v}"

# Formatting, linting, tests
fmt:
  cargo fmt --all

clippy:
  cargo clippy --all-targets -- -D warnings

test:
  cargo test --workspace

check:
  just fmt
  just clippy
  just test

# Run locally (stdio server); set SCYLLA_URI env before calling if needed
run:
  set -euo pipefail
  cargo run

# Docker utilities
docker-build image='ghcr.io/anthony-cervantes/scylla-rust-mcp:dev':
  docker build -t "{{image}}" .

docker-run image='ghcr.io/anthony-cervantes/scylla-rust-mcp:dev' uri='host.docker.internal:9042':
  docker run --rm -i \
    -e SCYLLA_URI="{{uri}}" \
    "{{image}}"

docker-run-tls image='ghcr.io/anthony-cervantes/scylla-rust-mcp:dev' uri='scylla.example.com:9142':
  docker run --rm -i \
    -e SCYLLA_URI="{{uri}}" \
    -e SCYLLA_SSL=true \
    "{{image}}"

docker-run-tls-ca image='ghcr.io/anthony-cervantes/scylla-rust-mcp:dev' uri='scylla.example.com:9142' ca='/absolute/path/ca.pem':
  docker run --rm -i \
    -v "{{ca}}:/ca.pem:ro" \
    -e SCYLLA_URI="{{uri}}" \
    -e SCYLLA_SSL=true \
    -e SCYLLA_CA_BUNDLE=/ca.pem \
    "{{image}}"
