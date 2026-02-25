"""Ensure every plugin has a valid pyproject.toml that bump_versions.py can process."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"


def test_all_plugins_have_bumpable_pyproject():
    """Each plugin directory must have a pyproject.toml with a version field."""
    plugin_dirs = sorted(
        p for p in PLUGINS_DIR.iterdir() if p.is_dir() and not p.name.startswith(".")
    )

    assert plugin_dirs, "No plugin directories found"

    missing_toml = []
    missing_version = []

    for plugin_dir in plugin_dirs:
        toml = plugin_dir / "pyproject.toml"
        if not toml.exists():
            missing_toml.append(plugin_dir.name)
            continue

        content = toml.read_text()
        if not re.search(r'^version\s*=\s*"[^"]*"', content, re.MULTILINE):
            missing_version.append(plugin_dir.name)

    assert not missing_toml, (
        f"Plugins missing pyproject.toml: {', '.join(missing_toml)}"
    )
    assert not missing_version, (
        f"Plugins missing version field in pyproject.toml: {', '.join(missing_version)}"
    )
