#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CHANGELOG="$PROJECT_DIR/CHANGELOG.md"
TMP_ENTRY="/tmp/waldur-site-agent-changelog-entry.md"
TMP_DATA="/tmp/waldur-site-agent-release-data.json"

# ── Usage ──────────────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 <VERSION>"
    echo "Example: $0 0.10.0"
    exit 1
fi

VERSION=$1
DATE=$(date +%Y-%m-%d)

# ── Pre-flight checks ─────────────────────────────────────────────────────
if ! command -v claude >/dev/null 2>&1; then
    echo "Error: 'claude' CLI is not on PATH."
    exit 1
fi

# ── Determine previous version ────────────────────────────────────────────
if [ -f "$CHANGELOG" ]; then
    PREV_TAG=$(grep -m 1 "^## " "$CHANGELOG" | sed 's/^## \([^ ]*\).*/\1/')
fi

if [ -z "${PREV_TAG:-}" ]; then
    PREV_TAG=$(git -C "$PROJECT_DIR" tag --sort=-v:refname | head -n 1)
fi

if [ -z "${PREV_TAG:-}" ]; then
    echo "Error: could not determine previous version (no CHANGELOG.md header and no git tags)."
    exit 1
fi

echo "=== waldur-site-agent changelog: $VERSION (since $PREV_TAG) ==="
echo ""

# ── Step 1: Collect commit data ───────────────────────────────────────────
echo "[1/3] Collecting commit data..."
python3 "$SCRIPT_DIR/generate_changelog_data.py" "$VERSION" "$PREV_TAG" > "$TMP_DATA"

TOTAL_COMMITS=$(python3 -c "import json,sys; print(json.load(sys.stdin)['summary_stats']['total_commits'])" < "$TMP_DATA")
echo "  Found $TOTAL_COMMITS commits."

if [ "$TOTAL_COMMITS" -eq 0 ]; then
    echo "No commits found between $PREV_TAG and $VERSION. Nothing to do."
    exit 0
fi

# ── Step 2: Generate changelog with Claude ────────────────────────────────
echo ""
echo "[2/3] Generating changelog with Claude..."

PROMPT_TEMPLATE=$(cat "$SCRIPT_DIR/prompts/changelog-prompt.md")
FULL_PROMPT="${PROMPT_TEMPLATE//\{VERSION\}/$VERSION}"
FULL_PROMPT="${FULL_PROMPT//\{PREV_VERSION\}/$PREV_TAG}"
FULL_PROMPT="${FULL_PROMPT//\{DATE\}/$DATE}"

COMMIT_DATA=$(cat "$TMP_DATA")

generate_changelog() {
    printf '%s\n\nHere is the commit data:\n\n```json\n%s\n```\n' "$FULL_PROMPT" "$COMMIT_DATA" | \
        env -u CLAUDECODE claude --print > "$TMP_ENTRY"
}

generate_changelog

# ── Show result and ask for action ────────────────────────────────────────
echo ""
echo "=== Generated Changelog Entry ==="
echo ""
cat "$TMP_ENTRY"
echo ""
echo "================================="
echo ""
read -p "Accept this changelog? [y/edit/regenerate/quit] " choice

case $choice in
    y|Y|yes)
        ;;
    edit|e)
        ${EDITOR:-vim} "$TMP_ENTRY"
        ;;
    regenerate|r)
        echo "Regenerating..."
        generate_changelog
        echo ""
        cat "$TMP_ENTRY"
        echo ""
        read -p "Accept now? [y/edit/quit] " choice2
        case $choice2 in
            edit|e) ${EDITOR:-vim} "$TMP_ENTRY" ;;
            y|Y) ;;
            *) echo "Aborted."; exit 1 ;;
        esac
        ;;
    *)
        echo "Aborted."
        exit 1
        ;;
esac

# ── Step 3: Update CHANGELOG.md ───────────────────────────────────────────
echo ""
echo "[3/3] Updating CHANGELOG.md..."

if [ -f "$CHANGELOG" ]; then
    {
        echo "# Changelog"
        echo ""
        cat "$TMP_ENTRY"
        echo ""
        tail -n +3 "$CHANGELOG"   # skip existing "# Changelog\n" header
    } > /tmp/waldur-site-agent-final-changelog.md
    mv /tmp/waldur-site-agent-final-changelog.md "$CHANGELOG"
else
    {
        echo "# Changelog"
        echo ""
        cat "$TMP_ENTRY"
        echo ""
    } > "$CHANGELOG"
fi

echo "  CHANGELOG.md updated. Review and commit when ready."
