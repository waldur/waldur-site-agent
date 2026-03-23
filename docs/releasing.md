# Releasing

This document describes how to create a new release of
waldur-site-agent.

## Quick Start

```bash
# Stable release
./scripts/release.sh 0.10.0
# Review the commit, then push:
git push origin main --tags

# Release candidate
./scripts/release.sh 0.10.0-rc.1
git push origin main --tags
```

The script handles version bumping, changelog generation,
committing, and tagging. CI takes care of publishing.

## Prerequisites

- You are on the `main` branch with a clean working tree.
- The [Claude CLI][claude-cli] is installed (used for
  changelog generation).
- Python 3.9+ is available.

[claude-cli]: https://docs.anthropic.com/en/docs/claude-code

## What the Release Script Does

`scripts/release.sh <VERSION>` runs four steps:

### 1. Bump Versions

Calls `scripts/bump_versions.py <VERSION>`, which auto-discovers
all packages and updates:

- `version = "..."` in the root `pyproject.toml`
- `version = "..."` in every `plugins/*/pyproject.toml`
- Internal dependency pins like `waldur-site-agent>=X.Y.Z`
  and `waldur-site-agent-keycloak-client>=X.Y.Z`

Plugin discovery is automatic — no hardcoded list. Adding a new
plugin directory with a `pyproject.toml` is all that's needed.

### 2. Generate Changelog

Calls `scripts/changelog.sh <VERSION>`, which:

1. Determines the previous version from `CHANGELOG.md`
   (or the latest git tag as fallback).
2. Runs `scripts/generate_changelog_data.py` to collect commits
   between the two versions and output structured JSON with
   categories, stats, and changed files.
3. Feeds the JSON to Claude with a prompt template
   (`scripts/prompts/changelog-prompt.md`) to draft a
   human-readable changelog entry.
4. Shows the result and asks you to **accept**, **edit**,
   **regenerate**, or **quit**.
5. Prepends the accepted entry to `CHANGELOG.md`.

### 3. Commit

Creates a single commit with the message `Release X.Y.Z`
containing:

- All updated `pyproject.toml` files
- The updated `CHANGELOG.md`

### 4. Tag

Creates a git tag `X.Y.Z` pointing at the release commit.

## What Happens After You Push

Pushing the tag to origin triggers GitLab CI, which:

| Job | What it does |
|---|---|
| **Publish python module** | Bumps versions, builds, publishes to PyPI |
| **Publish Helm chart** | Packages chart, pushes to GitHub Pages |
| **Publish Docker image** | Builds and pushes multiarch images |
| **Generate SBOM** | Creates CycloneDX SBOM, uploads to docs |

## Running Individual Scripts

### Bump versions only

Update all `pyproject.toml` files without committing or tagging:

```bash
python3 scripts/bump_versions.py <VERSION>
```

### Generate changelog only

Generate a changelog entry without bumping versions:

```bash
scripts/changelog.sh <VERSION>
```

This is useful if you want to manually edit the changelog before
running the full release.

### Collect commit data only

Get the raw commit data as JSON (useful for debugging or custom
tooling):

```bash
python3 scripts/generate_changelog_data.py <CURRENT_REF> <PREVIOUS_REF>
```

## Version Scheme

All packages (core + plugins) share the same version number,
following `MAJOR.MINOR.PATCH` (e.g. `0.10.0`). Tags do **not**
use a `v` prefix.

Release candidates use the `-rc.N` suffix in git tags
(e.g. `0.10.0-rc.1`). The release script automatically converts
this to PEP 440 format (`0.10.0rc1`) for `pyproject.toml` files
and PyPI publishing. Helm and Docker use the git tag as-is.

## Release Candidates

RC releases follow the same workflow as stable releases:

```bash
./scripts/release.sh 0.10.0-rc.1
git push origin main --tags
```

### How RCs differ from stable releases

| Aspect | Stable | RC |
|---|---|---|
| Git tag | `0.10.0` | `0.10.0-rc.1` |
| pyproject.toml version | `0.10.0` | `0.10.0rc1` (PEP 440) |
| Helm chart version | `0.10.0` | `0.10.0-rc.1` |
| Docker `:latest` tag | Updated | **Not** updated |
| Changelog | New entry | Replaces prior RC entries for same base version |

### Typical RC workflow

1. `./scripts/release.sh 0.10.0-rc.1` — first candidate
2. Test, find issues, fix on main
3. `./scripts/release.sh 0.10.0-rc.2` — replaces rc.1 changelog entry
4. `./scripts/release.sh 0.10.0` — stable release, includes all changes since last stable

## Troubleshooting

### "Error: working tree is not clean"

Commit or stash any uncommitted changes before releasing.

### "Error: tag 'X.Y.Z' already exists"

The version has already been tagged. Choose a different version
number, or delete the tag if it was created by mistake
(`git tag -d X.Y.Z`).

### "Error: 'claude' CLI is not on PATH"

The changelog generation step requires the Claude CLI. Install it
or generate the changelog manually by editing `CHANGELOG.md`
directly, then run the version bump and commit/tag steps
separately:

```bash
python3 scripts/bump_versions.py <VERSION>
# Edit CHANGELOG.md manually
git add pyproject.toml plugins/*/pyproject.toml CHANGELOG.md
git commit -m "Release <VERSION>"
git tag <VERSION>
```

### CI publish job fails

The CI publish job calls `bump_versions.py` as a safety net
before building. If versions are already correct from the release
script, this is a no-op. If someone tagged manually without
running the release script, CI still stamps the correct versions.
