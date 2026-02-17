#!/bin/bash
#
# Script to regenerate Waldur Python SDK from OpenAPI schema
# Usage: ./docs/update-local-sdk.sh [mastermind_path] [py_client_path]
#
# Arguments:
#   mastermind_path - Path to waldur-mastermind repo (default: ../waldur-mastermind)
#   py_client_path  - Path to py-client repo (default: ../py-client)
#
# Example:
#   ./docs/update-local-sdk.sh ../waldur-mastermind ../py-client
#

set -e

# Default paths (relative to current waldur-site-agent base folder)
MASTERMIND_PATH="${1:-../waldur-mastermind}"
PY_CLIENT_PATH="${2:-../py-client}"

# Resolve to absolute paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MASTERMIND_PATH="$(cd "$BASE_DIR" && cd "$MASTERMIND_PATH" && pwd)"
PY_CLIENT_PATH="$(cd "$BASE_DIR" && cd "$PY_CLIENT_PATH" && pwd)"

echo "=== Waldur Python SDK Regeneration ==="
echo "Mastermind:    $MASTERMIND_PATH"
echo "py-client:     $PY_CLIENT_PATH"
echo "site-agent:    $BASE_DIR"
echo ""

# Step 1: Generate OpenAPI schema
echo "[1/5] Generating OpenAPI schema..."
cd "$MASTERMIND_PATH"
uv run waldur spectacular --file waldur-openapi-schema.yaml --fail-on-warn
echo "      Schema generated: waldur-openapi-schema.yaml"

# Step 2: Install the code generator (if not already installed)
echo "[2/5] Ensuring openapi-python-client is installed..."
pip install -q git+https://github.com/waldur/openapi-python-client.git

# Step 3: Generate Python SDK
echo "[3/5] Generating Python SDK from schema..."
openapi-python-client generate \
    --path waldur-openapi-schema.yaml \
    --output-path py-client-generated \
    --overwrite \
    --meta poetry
echo "      SDK generated in py-client-generated/"

# Step 4: Copy to py-client checkout
echo "[4/5] Copying to py-client..."
rm -rf "$PY_CLIENT_PATH/waldur_api_client"
cp -rf py-client-generated/waldur_api_client "$PY_CLIENT_PATH/waldur_api_client"
rm -rf py-client-generated
echo "      Copied waldur_api_client/ to $PY_CLIENT_PATH"

# Step 5: Point site-agent at local py-client
echo "[5/5] Updating site-agent to use local py-client..."
cd "$BASE_DIR"

# Check current source configuration
if grep -q 'waldur-api-client = { path' pyproject.toml; then
    echo "      pyproject.toml already points to a local path, skipping."
else
    sed -i.bak \
        's|waldur-api-client = { git = "https://github.com/waldur/py-client.git", rev = "main" }|waldur-api-client = { path = "'"$PY_CLIENT_PATH"'", editable = true }|' \
        pyproject.toml
    rm -f pyproject.toml.bak
    echo "      Updated pyproject.toml to use local path."
fi

uv sync --all-packages
echo ""
echo "=== Done! ==="
echo "SDK has been regenerated and linked to waldur-site-agent."
echo ""
echo "IMPORTANT: Revert pyproject.toml before committing:"
echo "  waldur-api-client = { git = \"https://github.com/waldur/py-client.git\", rev = \"main\" }"
echo ""
echo "Run 'uv run pytest' to verify everything works."
