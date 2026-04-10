# Releasing

This project uses a tag-driven release process and requires PRs for all changes.

## Prerequisites
- CI must pass on the PR (fmt, clippy, tests).
- `main` is protected; merge via PR only.
- The repo secret `CARGO_REGISTRY_TOKEN` must be configured for crates.io publishing.
- Review [CRATES_IO_CHECKLIST.md](CRATES_IO_CHECKLIST.md) before pushing a tag.

## Steps
1) Bump version in Cargo.toml via PR
- Edit `Cargo.toml` `package.version` to the next SemVer (e.g., `0.1.5`).
- Update `Cargo.lock` too before merging. Running `cargo check` after the version bump is enough.
- Optionally update `CHANGELOG.md` if present.
- Open a PR titled `chore(release): vX.Y.Z` and merge after CI is green.

2) Create a tag
- After merging, create an annotated tag locally and push:

```bash
git fetch origin
git checkout main && git pull --ff-only
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

3) GitHub Actions publishes artifacts
- The `Release` workflow runs on tag `v*` and:
  - Verifies the tag version matches `Cargo.toml`
  - Builds prebuilt Linux, Intel macOS, and Apple Silicon macOS release archives
  - Publishes the crate to crates.io
  - Creates a GitHub Release for the tag and uploads the release archives

## Notes
- Tags are the source of truth; releases are cut from tags.
- Do not push commits to `main` from Actions; use PRs only.
- Consider using Conventional Commits in PR titles to make changelog generation easier.
- Crates.io versions are immutable, so double-check `version`, package contents, and README before publishing.
