#!/bin/bash
#
# Regenerate the Waldur Python SDK from a local Mastermind checkout and point
# this site-agent workspace at it, or undo that link.
#
# Usage:
#   ./docs/update-local-sdk.sh [mastermind_path] [py_client_path]   # regenerate + link
#   ./docs/update-local-sdk.sh --link-only [py_client_path]          # link only
#   ./docs/update-local-sdk.sh --unlink                              # back to the PyPI pin
#
# Arguments:
#   mastermind_path - Path to waldur-mastermind repo (default: ../waldur-mastermind)
#   py_client_path  - Path to py-client repo (default: ../py-client)
#
# The link is a [tool.uv.sources] entry in pyproject.toml that overrides the
# pinned waldur-api-client from PyPI. Run --unlink before committing.

set -euo pipefail

MODE=full
case "${1:-}" in
    --link-only) MODE=link; shift ;;
    --unlink) MODE=unlink; shift ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd -P)"
cd "$BASE_DIR"

# Edit [tool.uv.sources] in pyproject.toml with a TOML parser, so repeated runs
# never leave a duplicate key. $1 = set|remove, $2 = py-client path (for set).
edit_sources() {
    uv run --no-project --quiet --with tomlkit python - "$@" <<'PYEOF'
import sys

import tomlkit

action = sys.argv[1]
with open("pyproject.toml", encoding="utf-8") as f:
    doc = tomlkit.parse(f.read())
tool = doc.setdefault("tool", tomlkit.table())
uv = tool.setdefault("uv", tomlkit.table())
if action == "set":
    sources = uv.setdefault("sources", tomlkit.table())
    entry = tomlkit.inline_table()
    entry.update({"path": sys.argv[2], "editable": True})
    sources["waldur-api-client"] = entry
else:
    sources = uv.get("sources")
    if sources is not None and "waldur-api-client" in sources:
        del sources["waldur-api-client"]
        if len(sources) == 0:
            del uv["sources"]
with open("pyproject.toml", "w", encoding="utf-8") as f:
    f.write(tomlkit.dumps(doc))
PYEOF
}

if [ "$MODE" = unlink ]; then
    echo "Removing the local waldur-api-client source..."
    edit_sources remove
    uv lock
    uv sync --all-packages
    echo "waldur-api-client now comes from the version pinned in pyproject.toml."
    exit 0
fi

if [ "$MODE" = link ]; then
    PY_CLIENT_ARG="${1:-../py-client}"
else
    MASTERMIND_ARG="${1:-../waldur-mastermind}"
    PY_CLIENT_ARG="${2:-../py-client}"
fi

PY_CLIENT_PATH="$(cd "$PY_CLIENT_ARG" && pwd -P)"

# Refuse anything that is not a py-client checkout BEFORE touching it: step 4
# deletes and replaces its waldur_api_client/ directory.
PY_CLIENT_NAME="$(uv run --no-project --quiet --with tomlkit python - "$PY_CLIENT_PATH/pyproject.toml" <<'PYEOF' || true
import sys

import tomlkit

try:
    with open(sys.argv[1], encoding="utf-8") as f:
        doc = tomlkit.parse(f.read())
except OSError:
    sys.exit(0)
name = doc.get("project", {}).get("name") or doc.get("tool", {}).get("poetry", {}).get("name")
print(name or "")
PYEOF
)"
if [ "$PY_CLIENT_NAME" != "waldur-api-client" ] || [ ! -d "$PY_CLIENT_PATH/waldur_api_client" ]; then
    echo "ERROR: $PY_CLIENT_PATH is not a py-client checkout" \
        "(needs pyproject.toml naming waldur-api-client and a waldur_api_client/ directory)." >&2
    exit 1
fi

echo "=== Waldur Python SDK ==="
echo "py-client:     $PY_CLIENT_PATH"
echo "site-agent:    $BASE_DIR"

if [ "$MODE" = full ]; then
    MASTERMIND_PATH="$(cd "$MASTERMIND_ARG" && pwd -P)"
    echo "Mastermind:    $MASTERMIND_PATH"
    echo ""

    echo "[1/5] Generating OpenAPI schema..."
    cd "$MASTERMIND_PATH"
    uv run waldur spectacular --file waldur-openapi-schema.yaml --fail-on-warn

    echo "[2/5] Ensuring openapi-python-client is installed..."
    pip install -q git+https://github.com/waldur/openapi-python-client.git

    echo "[3/5] Generating Python SDK from schema..."
    openapi-python-client generate \
        --path waldur-openapi-schema.yaml \
        --output-path py-client-generated \
        --overwrite \
        --meta poetry

    echo "[4/5] Copying to py-client..."
    rm -rf "$PY_CLIENT_PATH/waldur_api_client"
    cp -rf py-client-generated/waldur_api_client "$PY_CLIENT_PATH/waldur_api_client"
    rm -rf py-client-generated
    cd "$BASE_DIR"
else
    echo ""
    echo "[1-4/5] Skipped (--link-only)"
fi

echo "[5/5] Pointing site-agent at the local py-client..."
edit_sources set "$PY_CLIENT_PATH"
uv sync --all-packages

# Verify the import really resolves to the local checkout.
LOADED="$(uv run python -c 'import os, waldur_api_client; print(os.path.realpath(waldur_api_client.__file__))')"
case "$LOADED" in
    "$PY_CLIENT_PATH"/*)
        echo "      waldur_api_client now loads from $LOADED"
        ;;
    *)
        echo "ERROR: waldur_api_client still loads from $LOADED, not from $PY_CLIENT_PATH" >&2
        exit 1
        ;;
esac

echo ""
echo "=== Done! ==="
echo "Before committing, undo the link: ./docs/update-local-sdk.sh --unlink"
