#!/usr/bin/env python3
"""Generate the plugin table in README.md from plugins/ metadata.

Usage:
    python3 scripts/generate_plugin_table.py

Reads each plugin's pyproject.toml for its description and checks for
a README.md.  Replaces content between marker comments in README.md.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"
README_PATH = ROOT / "README.md"

BEGIN_MARKER = "<!-- BEGIN PLUGIN TABLE -->"
END_MARKER = "<!-- END PLUGIN TABLE -->"


def get_description(plugin_dir: Path) -> str:
    pyproject = plugin_dir / "pyproject.toml"
    if not pyproject.exists():
        return ""
    match = re.search(r'^description\s*=\s*"(.+?)"', pyproject.read_text(), re.MULTILINE)
    return match.group(1) if match else ""


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
            link = name
        rows.append(f"| {link} | {description} |")

    header = "| Plugin | Description |\n| ------ | ----------- |"
    return header + "\n" + "\n".join(rows)


def main() -> None:
    readme = README_PATH.read_text()

    pattern = re.compile(
        rf"({re.escape(BEGIN_MARKER)})\n(.*?\n)?({re.escape(END_MARKER)})",
        re.DOTALL,
    )

    if not pattern.search(readme):
        print(f"ERROR: markers not found in {README_PATH}")
        print(f"Add {BEGIN_MARKER} and {END_MARKER} to README.md")
        raise SystemExit(1)

    table = generate_table()
    new_readme = pattern.sub(rf"\1\n{table}\n\3", readme)

    if new_readme == readme:
        print("README.md is already up to date.")
    else:
        README_PATH.write_text(new_readme)
        print("README.md updated.")


if __name__ == "__main__":
    main()
