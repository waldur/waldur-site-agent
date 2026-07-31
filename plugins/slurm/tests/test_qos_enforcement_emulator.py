"""End-to-end QoS enforcement against the real slurm-emulator binary.

Drives a real ``SlurmBackend`` + ``SlurmClient`` against the emulator's
``sacctmgr`` (shelled out, state persisted to a JSON file) — no mocked client,
no Waldur mastermind. Proves the enforce-mode flow the unit tests only assert on
command strings:

- ``add_user`` grants the selected QoS on the association (QosLevel/DefaultQOS,
  partition-scoped),
- ``pause_resource`` sets ``GrpSubmitJobs=0`` *without* clobbering the grant,
- ``restore_resource`` clears the lever.

The slurm-emulator>=0.9.1 pin guarantees GrpSubmitJobs support, so this test is
mandatory (it only skips if the emulator ``sacctmgr`` binary is not on PATH).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from waldur_site_agent_slurm.backend import SlurmBackend

_SACCTMGR = shutil.which("sacctmgr")

pytestmark = pytest.mark.skipif(_SACCTMGR is None, reason="slurm-emulator sacctmgr not on PATH")


@pytest.fixture
def state_file(tmp_path, monkeypatch):
    path = tmp_path / "emu_state.json"
    monkeypatch.setenv("SLURM_EMULATOR_STATE_FILE", str(path))
    return path


@pytest.fixture
def backend(state_file):  # noqa: ARG001 — sets the SLURM_EMULATOR_STATE_FILE env
    # GrpSubmitJobs persistence is guaranteed by the slurm-emulator>=0.9.1 pin,
    # so this test is mandatory (no capability probe / skip gate).
    bin_dir = str(Path(_SACCTMGR).parent)
    settings = {
        "default_account": "root",
        "customer_prefix": "hpc-",
        "project_prefix": "hpc-",
        "allocation_prefix": "hpc-",
        "slurm_bin_path": bin_dir,
        "qos_enforcement_enabled": True,  # opt in to enforcement
        "enforce_offering_qos": True,  # force enforcement for the test
    }
    return SlurmBackend(settings, {"cpu": {"unit": "minutes"}})


def _load(state_file: Path) -> dict:
    return json.loads(state_file.read_text())


def _alice_assoc(state_file: Path, account: str) -> dict:
    assocs = _load(state_file)["associations"]
    rows = [a for a in assocs.values() if a["user"] == "alice" and a["account"] == account]
    assert rows, "expected an association for alice"
    return rows[0]


class TestEnforcementAgainstEmulator:
    def test_grant_pause_restore(self, backend, state_file):
        account = "hpc-proj-1"
        backend.client._execute_command(
            ["add", "account", account, "description=p", "organization=o"]
        )

        resource = MagicMock()
        resource.backend_id = account
        resource.attributes.additional_properties = {"qos": "boost", "partition": "gpu"}

        # add_user should grant the selected QoS on the association.
        assert backend.add_user(resource, "alice") is True
        assoc = _alice_assoc(state_file, account)
        assert assoc["qos_list"] == ["boost"]
        assert assoc["def_qos"] == "boost"
        assert assoc["partition"] == "gpu"

        # pause blocks submission via GrpSubmitJobs=0 …
        assert backend.pause_resource(account) is True
        assert _load(state_file)["accounts"][account]["limits"]["GrpSubmitJobs"] == 0
        # … and leaves the grant intact (the whole point).
        assert _alice_assoc(state_file, account)["qos_list"] == ["boost"]

        # restore clears the lever.
        assert backend.restore_resource(account) is True
        assert "GrpSubmitJobs" not in _load(state_file)["accounts"][account]["limits"]
        assert _alice_assoc(state_file, account)["qos_list"] == ["boost"]
