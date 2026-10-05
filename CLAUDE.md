# CLAUDE.md

## Project Overview

Waldur Site Agent is a stateless Python application that synchronizes
data between Waldur Mastermind and service provider backends. It uses a
**uv workspace** with a core package and 22 plugin packages under
`plugins/` (the README plugin table lists them all).

## Repository Structure

```text
pyproject.toml                  # Core package (waldur-site-agent)
waldur_site_agent/              # Core source code
plugins/                        # Plugin packages (own pyproject.toml)
  azure/  basic_username_management/  ceph-s3/  cscs-dwdi/
  digitalocean/  envoy-ai-gateway/  harbor/  k8s-ut-namespace/
  keycloak-client/  ldap/  ldap-client/  ldap-roles/  litellm/
  moab/  mup/  nextcloud/  okd/  opennebula/  rancher/
  rancher-kc-crd/  slurm/  waldur/
tests/                          # Core tests
scripts/                        # Release tooling
  release.sh                    # Full release orchestrator
  bump_versions.py              # Auto-discover and bump versions
  changelog.sh                  # Generate changelog with Claude
  generate_changelog_data.py    # Collect commit data as JSON
  generate_plugin_table.py      # Regenerate plugin table in README.md
  prompts/changelog-prompt.md   # Prompt template for changelog
helm/                           # Helm chart
docs/                           # Documentation
```

## Development Commands

```bash
# Install all packages (core + plugins)
uv sync --all-packages

# Run core tests
uv run pytest tests/

# Run a specific plugin's tests
uv run pytest plugins/slurm/

# Lint and format
uvx prek run --all-files

# Regenerate plugin table in README.md (run after adding/removing plugins)
uv run python scripts/generate_plugin_table.py
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
  `waldur-site-agent>=X.Y.Z`. Two shared client packages are
  plugins too: `waldur-site-agent-keycloak-client` (used by
  rancher, k8s-ut-namespace, opennebula) and
  `waldur-site-agent-ldap-client` (used by ldap, ldap-roles,
  slurm).
- **Workspace sources**: Each plugin uses `[tool.uv.sources]`
  to map internal deps to the workspace during development.
- **Python compatibility**: 3.9 through 3.13. Merge requests
  test on 3.9 and 3.13; `main`, tags and schedules run all five
  (plus mypy on 3.9). Do **not** use
  `X | Y` union syntax or other 3.10+ features — use
  `Optional[X]` and `Union[X, Y]` from `typing` instead.
- **Structured logging**: JSON format via structlog to stdout.
- **Test config**: CI copies
  `examples/waldur-site-agent-config.yaml.example` to
  `waldur-site-agent-config.yaml` before the test jobs; the
  unit tests themselves build their configuration in code and
  run without that file.

## CI/CD

- GitLab CI (`.gitlab-ci.yml`), with shared templates from
  `waldur/waldur-pipelines`.
- Tag pushes trigger: PyPI publish (all packages), Helm chart
  publish (GitHub Pages), Docker image publish, SBOM generation.
- Merge requests trigger (by changed paths): linters on 3.13 +
  mypy on 3.9, `Run core tests`, `Run slurm tests` and one
  `Run plugin tests` job that loops over every other
  `plugins/*/tests` (each on 3.9 and 3.13), Helm lint,
  Dockerfile lint, and the E2E jobs when slurm/ldap/core/ci
  files change. Pushing a feature branch **without** an open MR
  starts no pipeline (use `-o ci.variable=RUN_E2E=true` or the
  web UI to force one).
- The `E2E: *` jobs in CI set `WALDUR_E2E_TESTS=true`
  — so any test under `plugins/<plugin>/tests/e2e/` that isn't
  gated correctly will run there even though it's "manual-only"
  in your head. Verify gating with `pytest --collect-only` from
  inside the plugin dir before pushing.

## Test gotchas worth knowing

- **Run plugin tests from inside the plugin dir.** `pytest
  plugins/slurm/tests/` from the workspace root often fails with
  `Unsupported backend type for X_backend: slurm` — plugin entry
  points only resolve when pytest runs from `plugins/<plugin>/`.
  `cd plugins/slurm && uv run pytest tests/` is the reliable form.
- **Backend `__init__` caches `backend_settings`.** Optional
  settings like `default_partition`, `enforce_offering_partitions`,
  `default_account` are read once at construction and cached on the
  instance (`self._<name>`). Module-scoped fixtures that build a
  `SlurmBackend` (or sibling) once per test module freeze those
  values at first use. A test that mutates
  `offering.backend_settings["x"]` later **also** has to patch
  `slurm_backend._x` (and restore both on teardown) — otherwise the
  add_user / add_resource path keeps using the stale cached value.
- **Generated SDK drifts from server responses on action
  endpoints.** `marketplace_provider_offerings_<action>` endpoints
  (e.g. `add_partition`) return 201 with a partial payload, but
  the typed `<Model>.from_dict` insists on fields that aren't
  there (`created`, `modified`, …) and raises `KeyError`. For E2E
  fixtures that POST to action endpoints, prefer
  `client.get_httpx_client().post(...)` and read the JSON directly.
  Regenerate the SDK before relying on the typed wrappers.

## Local dev gotchas

- **Stale plugin directories block `uv lock` / `uv sync`.** Empty
  `plugins/<name>/` dirs left over from past experiments (no
  `pyproject.toml`) error with `Workspace member ... is missing a
  pyproject.toml (matches: plugins/*)`. Use
  `./scripts/uv-lock-safe.sh` which hides empties for the duration
  of the lock, or delete the dir if you're certain it's cruft.
  Check `git status` first — if the dir contains untracked work,
  don't nuke it.
