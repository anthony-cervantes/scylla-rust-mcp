#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release-local.sh [--version X.Y.Z]

Creates a local release from a clean git tree:
1) bump Cargo.toml package version
2) commit + annotate tag (vX.Y.Z)
3) install from that tag via cargo install --git
4) verify installed version

If --version is omitted, patch version is incremented.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

current_version() {
  awk '
    /^\[package\]$/ { in_pkg=1; next }
    /^\[/ { in_pkg=0 }
    in_pkg && /^version[[:space:]]*=[[:space:]]*"/ {
      match($0, /"[0-9]+\.[0-9]+\.[0-9]+"/)
      if (RSTART > 0) {
        v = substr($0, RSTART + 1, RLENGTH - 2)
        print v
        exit
      }
    }
  ' Cargo.toml
}

next_patch_version() {
  local major minor patch
  IFS='.' read -r major minor patch <<<"$1"
  echo "${major}.${minor}.$((patch + 1))"
}

set_package_version() {
  local version="$1"
  local tmp
  tmp="$(mktemp)"
  awk -v target="$version" '
    BEGIN { in_pkg=0; updated=0 }
    /^\[package\]$/ { in_pkg=1 }
    /^\[/ && $0 != "[package]" { in_pkg=0 }
    in_pkg && /^version[[:space:]]*=[[:space:]]*"/ && !updated {
      sub(/"[0-9]+\.[0-9]+\.[0-9]+"/, "\"" target "\"")
      updated=1
    }
    { print }
    END {
      if (!updated) {
        exit 2
      }
    }
  ' Cargo.toml >"$tmp" || {
    rm -f "$tmp"
    die "failed to update package version in Cargo.toml"
  }
  mv "$tmp" Cargo.toml
}

ensure_clean_tree() {
  if ! git diff --quiet || ! git diff --cached --quiet; then
    die "git working tree has tracked changes; commit or stash first"
  fi
  if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    die "git working tree has untracked files; clean or commit first"
  fi
}

main() {
  local version_arg=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version|-v)
        [[ $# -ge 2 ]] || die "missing value for $1"
        version_arg="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "unexpected argument: $1 (use --help)"
        ;;
    esac
  done

  local repo_root
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || die "not inside a git repository"
  cd "$repo_root"

  ensure_clean_tree

  local current target tag
  current="$(current_version)"
  [[ -n "$current" ]] || die "unable to read current package version from Cargo.toml"

  if [[ -n "$version_arg" ]]; then
    target="$version_arg"
  else
    target="$(next_patch_version "$current")"
  fi

  [[ "$target" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be semver X.Y.Z"
  [[ "$target" != "$current" ]] || die "target version $target equals current version"

  tag="v${target}"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    die "tag ${tag} already exists"
  fi

  set_package_version "$target"
  git add Cargo.toml
  git commit -m "chore(release): bump version to ${target}"
  git tag -a "${tag}" -m "${tag}"

  cargo install --git "file://${repo_root}" --tag "${tag}" --force

  local installed_line
  installed_line="$(cargo install --list | awk '/^scylla-rust-mcp v/ { print; exit }')"
  [[ "$installed_line" == "scylla-rust-mcp v${target}"* ]] || {
    die "install verification failed; expected scylla-rust-mcp v${target}, got: ${installed_line:-<missing>}"
  }

  echo "released and installed ${installed_line}"
  echo "tagged commit: ${tag}"
}

main "$@"
