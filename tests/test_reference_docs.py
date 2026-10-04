"""The generated reference sections of the docs match the code.

Runs scripts/generate_reference_docs.py in check mode, so a change to a configuration
field, a console script, an environment variable or a plugin settings schema fails CI
until the docs are regenerated (or the hand-written section is updated).
"""

import importlib.util
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate_reference_docs.py"

if importlib.util.find_spec("tomllib") is None and importlib.util.find_spec("tomli") is None:
    pytest.skip("tomllib (Python 3.11+) or tomli is needed", allow_module_level=True)


def _load():
    spec = importlib.util.spec_from_file_location("generate_reference_docs", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reference_docs_are_current(capsys):
    """Fails with the list of stale sections; fix with the command it prints."""
    assert _load().run(check=True) == 0, capsys.readouterr().out


def test_stale_generated_section_is_detected(tmp_path, monkeypatch, capsys):
    gen = _load()
    copy = tmp_path / "configuration.md"
    shutil.copy(gen.CONFIGURATION_MD, copy)
    text = copy.read_text()
    row = "| Human-readable name for the offering |"
    assert row in text
    copy.write_text(text.replace(row, "| A stale description |", 1))
    monkeypatch.setattr(gen, "CONFIGURATION_MD", copy)

    assert gen.run(check=True) == 1
    out = capsys.readouterr().out
    assert "configuration.md: generated sections is stale" in out
    assert copy.read_text() == text.replace(row, "| A stale description |", 1)  # not rewritten


def test_undocumented_env_var_is_detected(tmp_path, monkeypatch, capsys):
    gen = _load()
    copy = tmp_path / "configuration.md"
    shutil.copy(gen.CONFIGURATION_MD, copy)
    text = copy.read_text()
    lines = [
        line
        for line in text.splitlines(keepends=True)
        if not line.startswith("| `WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES`")
    ]
    copy.write_text("".join(lines))
    monkeypatch.setattr(gen, "CONFIGURATION_MD", copy)

    assert gen.run(check=True) == 1
    assert "WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` is read by the agent but missing" in (
        capsys.readouterr().out
    )
