#!/usr/bin/env python3
"""Bump version across root and all plugin pyproject.toml files.

Usage:
    python3 scripts/bump_versions.py <VERSION>

Automatically discovers all plugins under plugins/ and updates:
  - version = "..." in each pyproject.toml
  - waldur-site-agent>=... dependency pins
  - waldur-site-agent-keycloak-client>=... dependency pins
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"

# Internal packages whose dependency pins should be updated.
# Order matters: longer names first to avoid partial matches in reporting,
# though the regex itself is safe (requires >= or == immediately after the name).
INTERNAL_PACKAGES = [
    "waldur-site-agent-keycloak-client",
    "waldur-site-agent",
]


def bump_file(path: Path, version: str) -> list[str]:
    """Update version and internal dependency pins in a pyproject.toml.

    Returns list of human-readable changes made.
    """
    text = path.read_text()
    changes = []

    # Update version = "..."
    new_text, n = re.subn(
        r'^(version\s*=\s*)"[^"]*"',
        rf'\g<1>"{version}"',
        text,
        flags=re.MULTILINE,
    )
    if n and new_text != text:
        changes.append(f"version -> {version}")
        text = new_text

    # Update internal dependency pins, e.g. "waldur-site-agent>=0.7.0"
    for pkg in INTERNAL_PACKAGES:
        pattern = rf'("{re.escape(pkg)})(>=|==)[^"]*"'
        replacement = rf'\g<1>\g<2>{version}"'
        new_text, n = re.subn(pattern, replacement, text)
        if n and new_text != text:
            changes.append(f"{pkg} dep -> {version}")
            text = new_text

    if changes:
        path.write_text(text)

    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Bump version across all packages")
    parser.add_argument("version", help="Version string (e.g. 0.10.0)")
    args = parser.parse_args()

    version = args.version

    if not re.match(r"^\d+\.\d+\.\d+", version):
        print(
            f"Error: '{version}' does not look like a valid version (expected X.Y.Z)",
            file=sys.stderr,
        )
        return 1

    # Root pyproject.toml
    root_toml = ROOT / "pyproject.toml"
    changes = bump_file(root_toml, version)
    if changes:
        print(f"  {root_toml.relative_to(ROOT)}: {', '.join(changes)}")

    # Plugin pyproject.toml files (auto-discovered)
    plugin_dirs = sorted(
        p for p in PLUGINS_DIR.iterdir() if p.is_dir() and not p.name.startswith(".")
    )

    if not plugin_dirs:
        print("Warning: no plugin directories found", file=sys.stderr)

    for plugin_dir in plugin_dirs:
        toml = plugin_dir / "pyproject.toml"
        if not toml.exists():
            print(
                f"  Warning: {toml.relative_to(ROOT)} not found, skipping",
                file=sys.stderr,
            )
            continue
        changes = bump_file(toml, version)
        if changes:
            print(f"  {toml.relative_to(ROOT)}: {', '.join(changes)}")

    print(f"\nAll packages bumped to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
