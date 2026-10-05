# Local pipeline for developers: OpenAPI -> Python SDK -> Site Agent

This document describes how to regenerate the Waldur Python SDK (`waldur-api-client`) from a
local Mastermind checkout and point the site agent at it. You need this when you work against
Mastermind API changes that have not been published yet.

## How the agent consumes the SDK

The SDK is generated in the `waldur-mastermind` GitLab CI pipeline:

1. `uv run waldur spectacular` exports the OpenAPI schema.
2. A custom fork of `openapi-python-client` generates the `waldur_api_client` package.
3. The package is pushed to `github.com/waldur/py-client` and published to PyPI as
   `waldur-api-client`, including pre-release builds.

`waldur-site-agent` depends on an exact published version, pinned in `pyproject.toml`:

```toml
[project]
dependencies = [
    # ...
    "waldur-api-client==8.1.3rc21.dev20261003183418",
]
```

To pick up a newer published SDK, change that pin and run `uv lock`. There is no
`[tool.uv.sources]` entry for the SDK in the committed `pyproject.toml`; the local link below adds
one temporarily.

## Prerequisites

- **uv** and **pip**
- **Waldur Mastermind** cloned and set up (default: `../waldur-mastermind`)
- **py-client** cloned from `github.com/waldur/py-client` (default: `../py-client`)

## Steps to regenerate and link the SDK

### 1. Generate the OpenAPI schema

In the `waldur-mastermind` directory:

```bash
uv run waldur spectacular --file waldur-openapi-schema.yaml --fail-on-warn
```

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

### 3. Copy the generated code to the local py-client checkout

```bash
cp -rf py-client/waldur_api_client ../py-client/waldur_api_client
```

### 4. Point waldur-site-agent at the local py-client

Add a source override to the site agent's `pyproject.toml`:

```toml
[tool.uv.sources]
waldur-api-client = { path = "../py-client", editable = true }
```

and re-sync:

```bash
uv sync --all-packages
```

The path source replaces the pinned PyPI version even when the local package carries a different
version number.

### 5. Verify

```bash
uv run python -c "import waldur_api_client; print(waldur_api_client.__file__)"
```

This must print a path inside your local `py-client` checkout.

## Helper script

`docs/update-local-sdk.sh` runs steps 1–5 and fails if `waldur_api_client` does not load from the
local checkout afterwards:

```bash
./docs/update-local-sdk.sh [mastermind_path] [py_client_path]

# Only step 5 (link), for a py-client checkout you have already regenerated:
./docs/update-local-sdk.sh --link-only ../py-client

# Undo the link and go back to the PyPI pin:
./docs/update-local-sdk.sh --unlink
```

Before it changes anything, the script checks that the py-client path holds a `pyproject.toml` for
`waldur-api-client` and a `waldur_api_client/` directory, because step 4 replaces that directory.
It edits `[tool.uv.sources]` with a TOML parser, so running it again replaces the previous link
rather than adding a second one.

## Reverting to the published SDK

Remove the override before committing — linking edits both `pyproject.toml` and `uv.lock`:

```bash
./docs/update-local-sdk.sh --unlink
```

It removes the source entry, re-locks and re-syncs, which brings `uv.lock` back to the pinned version.
If you edited nothing else, `git checkout pyproject.toml uv.lock && uv sync --all-packages` does the
same.
