"""The heartbeat file is configurable, and the test suite never touches the real one."""

import os
from pathlib import Path
from unittest import mock

from waldur_site_agent.common import healthz


def test_env_var_moves_the_heartbeat(tmp_path, monkeypatch):
    path = tmp_path / "unit-a.heartbeat"
    monkeypatch.setenv("WALDUR_SITE_AGENT_HEARTBEAT_PATH", str(path))

    healthz.touch_heartbeat()

    assert path.exists()
    assert healthz.check_liveness()


def test_cli_flag_overrides_the_env_var(tmp_path, monkeypatch):
    fresh = tmp_path / "fresh"
    healthz.touch_heartbeat(str(fresh))
    monkeypatch.setenv("WALDUR_SITE_AGENT_HEARTBEAT_PATH", str(tmp_path / "missing"))

    with mock.patch("sys.argv", ["waldur_site_healthz", "--liveness-only"]):
        assert healthz.main() == 1
    with mock.patch(
        "sys.argv",
        ["waldur_site_healthz", "--liveness-only", "--heartbeat-path", str(fresh)],
    ):
        assert healthz.main() == 0


def test_suite_runs_on_an_isolated_heartbeat(isolated_heartbeat):
    """The autouse fixture points every test, and every processor loop, off the default."""
    default = Path(healthz.HEARTBEAT_PATH)
    before = default.stat().st_mtime if default.exists() else None

    healthz.touch_heartbeat()

    assert os.environ["WALDUR_SITE_AGENT_HEARTBEAT_PATH"] == str(isolated_heartbeat)
    assert isolated_heartbeat.exists()
    after = default.stat().st_mtime if default.exists() else None
    assert after == before
