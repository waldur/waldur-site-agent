# Local pipeline for developers: OpenAPI -> Python SDK -> Site Agent

This document describes the process of regenerating and linking the
Waldur Python SDK (`waldur-api-client`) for local development. The SDK
is published to GitHub and consumed as a git dependency, but a local
pipeline is essential when you need to work with unreleased Mastermind
API changes before they are officially deployed.

## Background

The production pipeline runs in GitLab CI on the `waldur-mastermind`
repository:

1. `uv run waldur spectacular` exports the OpenAPI schema.
2. A custom fork of `openapi-python-client` generates the Python SDK.
3. The generated `waldur_api_client/` package is pushed to
   `github.com/waldur/py-client` (main branch).
4. `waldur-site-agent` consumes it via `pyproject.toml`:

```toml
[tool.uv.sources]
waldur-api-client = { git = "https://github.com/waldur/py-client.git", rev = "main" }
```

## Prerequisites

Before proceeding, ensure you have the following:

- **uv**: For managing Python dependencies.
- **pip**: For installing the OpenAPI code generator.
- **Waldur MasterMind**: Cloned and set up in a directory (default:
  `../waldur-mastermind`).
- **py-client**: Cloned from `github.com/waldur/py-client` (default:
  `../py-client`).

## Steps to regenerate and link the SDK

### 1. Generate the OpenAPI schema

In the `waldur-mastermind` directory, run:

```bash
uv run waldur spectacular --file waldur-openapi-schema.yaml --fail-on-warn
```

This produces `waldur-openapi-schema.yaml` with the full API definition.

### 2. Generate the Python SDK from the schema

Still in the `waldur-mastermind` directory:

```bash
pip install git+https://github.com/waldur/openapi-python-client.git
openapi-python-client generate \
    --path waldur-openapi-schema.yaml \
    --output-path py-client \
    --overwrite \
    --meta poetry
```

This creates (or overwrites) the `py-client/` directory with the
generated `waldur_api_client` package.

### 3. Copy the generated code to the local py-client checkout

```bash
cp -rf py-client/waldur_api_client ../py-client/waldur_api_client
```

### 4. Point waldur-site-agent at the local py-client

Temporarily override the source in `pyproject.toml`:

```toml
[tool.uv.sources]
waldur-api-client = { path = "../py-client", editable = true }
```

Then re-sync dependencies:

```bash
uv sync --all-packages
```

### 5. Verify

```bash
uv run python -c "import waldur_api_client; print(waldur_api_client.__file__)"
```

This should print the path to your local `py-client` checkout.

## Helper script

A convenience script is provided at `docs/update-local-sdk.sh`:

```bash
./docs/update-local-sdk.sh [mastermind_path] [py_client_path]
```

It automates steps 1-4 above. After running it, remember to revert the
`pyproject.toml` source change before committing.

## Reverting to the published SDK

To switch back to the GitHub-hosted SDK:

```toml
[tool.uv.sources]
waldur-api-client = { git = "https://github.com/waldur/py-client.git", rev = "main" }
```

Then:

```bash
uv sync --all-packages
```
