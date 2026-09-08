#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Usage ──────────────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 <VERSION>"
    echo "Example: $0 0.10.0  or  $0 0.10.0-rc.1"
    exit 1
fi

VERSION=$1

# ── Validate version format ───────────────────────────────────────────────
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$'; then
    echo "Error: version '$VERSION' must be in X.Y.Z or X.Y.Z-rc.N format."
    exit 1
fi

# ── RC detection ─────────────────────────────────────────────────────────
IS_RC=false
BASE_VERSION="$VERSION"
if echo "$VERSION" | grep -qE '\-rc\.[0-9]+$'; then
    IS_RC=true
    BASE_VERSION=$(echo "$VERSION" | sed 's/-rc\.[0-9]*$//')
fi
export IS_RC

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

# Releasing from a stale branch silently cuts the release without whatever was
# merged upstream, so refresh and compare before going any further. Fetching
# tags first also lets the duplicate-tag check below see tags pushed elsewhere.
REMOTE="${RELEASE_REMOTE:-origin}"
if ! git fetch --quiet --tags "$REMOTE" 2>/dev/null; then
    echo "Warning: could not fetch from '$REMOTE'; cannot tell whether"
    echo "'$BRANCH' is up to date."
    read -p "Continue anyway? [y/N] " choice
    [ "$choice" = "y" ] || [ "$choice" = "Y" ] || exit 1
elif git rev-parse --verify --quiet "$REMOTE/$BRANCH" >/dev/null; then
    COUNTS=$(git rev-list --left-right --count "$REMOTE/$BRANCH...HEAD")
    BEHIND=$(echo "$COUNTS" | cut -f1)
    AHEAD=$(echo "$COUNTS" | cut -f2)
    if [ "$BEHIND" -gt 0 ] && [ "$AHEAD" -gt 0 ]; then
        echo "Error: '$BRANCH' has diverged from '$REMOTE/$BRANCH'"
        echo "  ($AHEAD local commit(s), $BEHIND remote commit(s))."
        echo "Reconcile them before releasing."
        exit 1
    elif [ "$BEHIND" -gt 0 ]; then
        echo "Error: '$BRANCH' is $BEHIND commit(s) behind '$REMOTE/$BRANCH'."
        echo "The release would be cut without them. Run:"
        echo "  git pull --ff-only $REMOTE $BRANCH"
        exit 1
    elif [ "$AHEAD" -gt 0 ]; then
        echo "Warning: '$BRANCH' is $AHEAD commit(s) ahead of"
        echo "'$REMOTE/$BRANCH'; those commits are not pushed yet."
        read -p "Continue anyway? [y/N] " choice
        [ "$choice" = "y" ] || [ "$choice" = "Y" ] || exit 1
    fi
else
    echo "Warning: '$REMOTE/$BRANCH' does not exist; skipping the up-to-date check."
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
    echo "Error: tag '$VERSION' already exists."
    exit 1
fi

echo "=== Releasing waldur-site-agent $VERSION ==="
echo ""

# ── Step 1: Bump versions ────────────────────────────────────────────────
echo "[1/5] Bumping versions..."
python3 "$SCRIPT_DIR/bump_versions.py" "$VERSION"
echo ""

# ── Step 2: Regenerate lockfile ──────────────────────────────────────────
echo "[2/5] Regenerating uv.lock..."
uv lock
echo ""

# ── Step 3: Generate changelog ────────────────────────────────────────────
echo "[3/5] Generating changelog..."
CHANGELOG_BEFORE=$(git hash-object CHANGELOG.md 2>/dev/null || echo "missing")
"$SCRIPT_DIR/changelog.sh" "$VERSION"
CHANGELOG_AFTER=$(git hash-object CHANGELOG.md 2>/dev/null || echo "missing")
if [ "$CHANGELOG_BEFORE" = "$CHANGELOG_AFTER" ]; then
    echo ""
    echo "Error: CHANGELOG.md was not updated (changelog.sh found nothing to add)."
    echo "The version bump and lockfile are left in the working tree; nothing was"
    echo "committed or tagged. Fix the changelog step and re-run."
    exit 1
fi
echo ""

# ── Step 4: Commit ────────────────────────────────────────────────────────
echo "[4/5] Committing release..."
git add pyproject.toml plugins/*/pyproject.toml uv.lock
git add CHANGELOG.md
git commit -m "Release $VERSION"
echo ""

# ── Step 5: Tag ───────────────────────────────────────────────────────────
echo "[5/5] Tagging $VERSION..."
git tag "$VERSION"
echo ""

echo "=== Release $VERSION prepared ==="
echo ""
echo "Review the commit and tag, then push with:"
echo "  git push origin main --tags"
