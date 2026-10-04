#!/usr/bin/env python3
"""Generate the plugin table in README.md from plugins/ metadata.

Usage:
    python3 scripts/generate_plugin_table.py          # rewrite the table
    python3 scripts/generate_plugin_table.py --check  # exit 1 if README.md is stale

Reads each plugin's pyproject.toml for its distribution name and
description and checks for a README.md.  Replaces content between marker
comments in README.md.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"
README_PATH = ROOT / "README.md"

BEGIN_MARKER = "<!-- BEGIN PLUGIN TABLE -->"
END_MARKER = "<!-- END PLUGIN TABLE -->"


PYPI_URL = "https://pypi.org/project"


def _read_field(plugin_dir: Path, field: str) -> str:
    pyproject = plugin_dir / "pyproject.toml"
    if not pyproject.exists():
        return ""
    match = re.search(rf'^{field}\s*=\s*"(.+?)"', pyproject.read_text(), re.MULTILINE)
    return match.group(1) if match else ""


def get_description(plugin_dir: Path) -> str:
    return _read_field(plugin_dir, "description")


def get_package_name(plugin_dir: Path) -> str:
    """Return the distribution name published to PyPI."""
    return _read_field(plugin_dir, "name")


def generate_table() -> str:
    rows = []
    for plugin_dir in sorted(PLUGINS_DIR.iterdir()):
        if not plugin_dir.is_dir() or not (plugin_dir / "pyproject.toml").exists():
            continue
        name = plugin_dir.name
        description = get_description(plugin_dir)
        description = description.removesuffix(" for Waldur Site Agent")
        has_readme = (plugin_dir / "README.md").exists()
        if has_readme:
            link = f"[{name}](plugins/{name}/README.md)"
        else:
            # No README yet - link to the plugin directory so every row is
            # navigable.
            link = f"[{name}](plugins/{name}/)"
        package = get_package_name(plugin_dir)
        package_link = f"[`{package}`]({PYPI_URL}/{package}/)" if package else ""
        rows.append(f"| {link} | {package_link} | {description} |")

    header = "| Plugin | PyPI package | Description |\n| ------ | ------------ | ----------- |"
    table = header + "\n" + "\n".join(rows)
    # Full PyPI URLs push the rows past the 120-character limit enforced by
    # pymarkdown, so suppress line-length for exactly the table's own lines.
    # The count covers the header, the separator and one line per plugin.
    pragma = f"<!-- pyml disable-num-lines {len(rows) + 2} line-length -->"
    return pragma + "\n" + table


def render(readme: str) -> str:
    """Return ``readme`` with the plugin table between the markers regenerated."""
    pattern = re.compile(
        rf"({re.escape(BEGIN_MARKER)})\n(.*?\n)?({re.escape(END_MARKER)})",
        re.DOTALL,
    )
    if not pattern.search(readme):
        print(f"ERROR: markers not found in {README_PATH}")
        print(f"Add {BEGIN_MARKER} and {END_MARKER} to README.md")
        raise SystemExit(1)
    table = generate_table()
    return pattern.sub(lambda m: f"{m.group(1)}\n{table}\n{m.group(3)}", readme)


def main() -> None:
    check = "--check" in sys.argv[1:]
    readme = README_PATH.read_text()
    new_readme = render(readme)

    if new_readme == readme:
        print("README.md is already up to date.")
    elif check:
        print("README.md plugin table is stale; run scripts/generate_plugin_table.py")
        raise SystemExit(1)
    else:
        README_PATH.write_text(new_readme)
        print("README.md updated.")


if __name__ == "__main__":
    main()
