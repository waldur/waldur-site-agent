# CLAUDE.md

## Project Overview

Waldur Site Agent is a stateless Python application that synchronizes
data between Waldur Mastermind and service provider backends. It uses a
**uv workspace** with a core package and 14 plugin packages under
`plugins/`.

## Repository Structure

```text
pyproject.toml                  # Core package (waldur-site-agent)
waldur_site_agent/              # Core source code
plugins/                        # Plugin packages (own pyproject.toml)
  slurm/  moab/  mup/  okd/  harbor/  croit-s3/  cscs-dwdi/
  basic_username_management/  waldur/  keycloak-client/
  k8s-ut-namespace/  rancher/  digitalocean/  opennebula/
tests/                          # Core tests
scripts/                        # Release tooling
  release.sh                    # Full release orchestrator
  bump_versions.py              # Auto-discover and bump versions
  changelog.sh                  # Generate changelog with Claude
  generate_changelog_data.py    # Collect commit data as JSON
  prompts/changelog-prompt.md   # Prompt template for changelog
helm/                           # Helm chart
docs/                           # Documentation
```

## Development Commands

```bash
# Install all packages (core + plugins)
uv sync --all-packages

# Run core tests
.venv/bin/python -m pytest tests/

# Run a specific plugin's tests
.venv/bin/python -m pytest plugins/slurm/

# Lint and format
pre-commit run --all-files
```

## Release Process

See [docs/releasing.md](docs/releasing.md) for the full guide.

```bash
./scripts/release.sh <VERSION>
git push origin main --tags
```

The CI publish job also calls `bump_versions.py` as a safety net,
so manual tagging still works.

## Key Conventions

- **Version management**: All packages share the same version.
  `scripts/bump_versions.py` auto-discovers plugins.
- **Plugin dependencies**: Plugins depend on
  `waldur-site-agent>=X.Y.Z`. Some also depend on
  `waldur-site-agent-keycloak-client>=X.Y.Z`
  (rancher, k8s-ut-namespace).
- **Workspace sources**: Each plugin uses `[tool.uv.sources]`
  to map internal deps to the workspace during development.
- **Python compatibility**: 3.9 through 3.13. CI runs tests
  and linters across all five versions.
- **Structured logging**: JSON format via structlog to stdout.
- **Test config**: Tests expect
  `waldur-site-agent-config.yaml` in the repo root, copied
  from `examples/waldur-site-agent-config.yaml.example`.

## CI/CD

- GitLab CI (`.gitlab-ci.yml`), with shared templates from
  `waldur/waldur-pipelines`.
- Tag pushes trigger: PyPI publish (all packages), Helm chart
  publish (GitHub Pages), Docker image publish, SBOM generation.
- MR/branch pushes trigger: linters (5 Python versions),
  core tests (5 versions), plugin tests (5 versions each),
  Helm lint, Dockerfile lint.
