"""Ensure every plugin gets its version bumped in the CI publish job."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"
CI_FILE = ROOT / ".gitlab-ci.yml"


def test_all_plugins_have_version_bump_in_ci():
    """Each plugin directory must have a sed version-bump line in .gitlab-ci.yml."""
    plugin_dirs = sorted(
        p.name for p in PLUGINS_DIR.iterdir() if p.is_dir() and not p.name.startswith(".")
    )
    ci_content = CI_FILE.read_text()

    missing = []
    for plugin in plugin_dirs:
        expected = f'plugins/{plugin}/pyproject.toml'
        version_sed = f'sed -i "s/^version = \\".*\\"$/version = \\"$CI_COMMIT_TAG\\"/" {expected}'
        if version_sed not in ci_content:
            missing.append(plugin)

    assert not missing, (
        f"The following plugins are missing version-bump sed commands "
        f"in the CI publish job: {', '.join(missing)}"
    )
