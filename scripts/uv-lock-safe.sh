#!/bin/bash
# Run `uv lock` after stashing any empty plugin directory that would
# fail uv's workspace-member discovery.
#
# `[tool.uv.workspace] members = ["plugins/*"]` requires every dir
# matching the glob to contain a pyproject.toml. A `plugins/<name>/`
# left over from a deleted experiment (or partial checkout) breaks
# the lock with:
#
#   error: Workspace member `.../plugins/<name>` is missing a
#   pyproject.toml (matches: `plugins/*`)
#
# This script moves any such dir aside, runs `uv lock` (forwarding
# additional args), then restores the dirs. The stash directory lives
# in $TMPDIR and is removed on success.
#
# Usage:
#   ./scripts/uv-lock-safe.sh             # plain uv lock
#   ./scripts/uv-lock-safe.sh --upgrade   # forward flags to uv lock
#   ./scripts/uv-lock-safe.sh --upgrade-package slurm-emulator
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# Discover plugin dirs without a pyproject.toml.
STASH_TARGETS=()
for dir in plugins/*/; do
  if [[ ! -f "${dir}pyproject.toml" ]]; then
    STASH_TARGETS+=("${dir%/}")
  fi
done

if [[ ${#STASH_TARGETS[@]} -eq 0 ]]; then
  exec uv lock "$@"
fi

STASH_DIR="$(mktemp -d -t uv-lock-safe-XXXXXX)"
echo "Hiding ${#STASH_TARGETS[@]} stale plugin dir(s) for uv lock: ${STASH_TARGETS[*]}"
echo "  (stash: $STASH_DIR — will be removed on success)"

restore() {
  for path in "${STASH_TARGETS[@]}"; do
    name="$(basename "$path")"
    if [[ -d "$STASH_DIR/$name" ]]; then
      mv "$STASH_DIR/$name" "$path"
    fi
  done
  rmdir "$STASH_DIR" 2>/dev/null || true
}
trap restore EXIT

for path in "${STASH_TARGETS[@]}"; do
  mv "$path" "$STASH_DIR/$(basename "$path")"
done

uv lock "$@"
