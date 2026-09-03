"""QoS swap against the real slurm-emulator ``sacctmgr`` on an account with a DefaultQOS.

Drives ``SlurmBackend`` + ``SlurmClient`` (no mocks) through pause and restore
on an account whose DefaultQOS is set, the setup that makes the plain
``set qos=<paused>`` swap fail on real slurmdbd. Asserts the account
ends up with the new QoS *and* the matching default, read back through the
client's own ``list associations`` parsing.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from waldur_site_agent_slurm.backend import SlurmBackend

_SACCTMGR = shutil.which("sacctmgr")

pytestmark = pytest.mark.skipif(_SACCTMGR is None, reason="slurm-emulator sacctmgr not on PATH")


@pytest.fixture
def backend(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_EMULATOR_STATE_FILE", str(tmp_path / "emu_state.json"))
    settings = {
        "default_account": "root",
        "customer_prefix": "hpc-",
        "project_prefix": "hpc-",
        "allocation_prefix": "hpc-",
        "slurm_bin_path": str(Path(_SACCTMGR).parent),
        "qos_paused": "stop",
        "qos_downscaled": "limited",
        "qos_default": "normal",
    }
    return SlurmBackend(settings, {"cpu": {"unit": "minutes"}})


class TestSwapAgainstEmulator:
    def test_pause_and_restore_account_with_default_qos(self, backend):
        account = "hpc-a136"
        client = backend.client
        client._execute_command(["add", "account", account, "description=p", "organization=o"])
        client._execute_command(["modify", "account", account, "set", "defaultqos=normal"])
        client._execute_command(["add", "user", "alice", f"account={account}"])
        if client.get_current_account_default_qos(account) != "normal":
            # slurm-emulator < 0.9.5 stores the account default but does not report it on
            # the association row, and applies no DefaultQOS-in-list check, so the swap
            # cannot be exercised meaningfully against it.
            pytest.skip("slurm-emulator too old: account DefaultQOS not reported")

        assert backend.pause_resource(account) is True
        assert client.get_current_account_qos(account) == "stop"
        assert client.get_current_account_default_qos(account) == "stop"

        assert backend.restore_resource(account) is True
        assert client.get_current_account_qos(account) == "normal"
        assert client.get_current_account_default_qos(account) == "normal"

    def test_swap_without_default_qos_stays_plain(self, backend):
        account = "hpc-plain"
        client = backend.client
        client._execute_command(["add", "account", account, "description=p", "organization=o"])
        assert client.get_current_account_default_qos(account) == ""

        assert backend.downscale_resource(account) is True
        assert client.get_current_account_qos(account) == "limited"
        assert client.get_current_account_default_qos(account) == ""
