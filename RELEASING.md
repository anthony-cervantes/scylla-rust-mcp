# Releasing

This project uses a tag-driven release process and requires PRs for all changes.

## Prerequisites
- CI must pass on the PR (fmt, clippy, tests).
- `main` is protected; merge via PR only.

## Steps
1) Bump version in Cargo.toml via PR
- Edit `Cargo.toml` `package.version` to the next SemVer (e.g., `0.1.5`).
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
  - Builds multi-arch Docker images
  - Pushes `ghcr.io/<org>/<repo>:vX.Y.Z` and updates `:latest`

## Notes
- Tags are the source of truth; releases are cut from tags.
- Do not push commits to `main` from Actions; use PRs only.
- Consider using Conventional Commits in PR titles to make changelog generation easier.

