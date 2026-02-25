#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Usage ──────────────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 <VERSION>"
    echo "Example: $0 0.10.0"
    exit 1
fi

VERSION=$1

# ── Validate version format ───────────────────────────────────────────────
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "Error: version '$VERSION' must be in X.Y.Z format."
    exit 1
fi

# ── Pre-flight checks ────────────────────────────────────────────────────
cd "$PROJECT_DIR"

BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "main" ]; then
    echo "Warning: you are on branch '$BRANCH', not 'main'."
    read -p "Continue anyway? [y/N] " choice
    [ "$choice" = "y" ] || [ "$choice" = "Y" ] || exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: working tree is not clean. Commit or stash changes first."
    exit 1
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
    echo "Error: tag '$VERSION' already exists."
    exit 1
fi

echo "=== Releasing waldur-site-agent $VERSION ==="
echo ""

# ── Step 1: Bump versions ────────────────────────────────────────────────
echo "[1/4] Bumping versions..."
python3 "$SCRIPT_DIR/bump_versions.py" "$VERSION"
echo ""

# ── Step 2: Generate changelog ────────────────────────────────────────────
echo "[2/4] Generating changelog..."
"$SCRIPT_DIR/changelog.sh" "$VERSION"
echo ""

# ── Step 3: Commit ────────────────────────────────────────────────────────
echo "[3/4] Committing release..."
git add pyproject.toml plugins/*/pyproject.toml
git add CHANGELOG.md
git commit -m "Release $VERSION"
echo ""

# ── Step 4: Tag ───────────────────────────────────────────────────────────
echo "[4/4] Tagging $VERSION..."
git tag "$VERSION"
echo ""

echo "=== Release $VERSION prepared ==="
echo ""
echo "Review the commit and tag, then push with:"
echo "  git push origin main --tags"
