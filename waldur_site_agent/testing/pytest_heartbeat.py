"""Pytest fixture that keeps test runs off the real liveness heartbeat file.

Processors touch the heartbeat while they loop. Without this, a test run on a
host with a live agent refreshes that agent's heartbeat and hides a stall.
Import it into a ``conftest.py``::

    from waldur_site_agent.testing.pytest_heartbeat import isolated_heartbeat  # noqa: F401
"""

from pathlib import Path

import pytest

from waldur_site_agent.common.healthz import HEARTBEAT_PATH_ENV


@pytest.fixture(autouse=True)
def isolated_heartbeat(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the heartbeat at a per-test file under ``tmp_path``."""
    path = tmp_path / "heartbeat"
    monkeypatch.setenv(HEARTBEAT_PATH_ENV, str(path))
    return path
