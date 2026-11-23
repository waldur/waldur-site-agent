"""Termination blocks new submissions before it tears the account down.

slurmdbd refuses to remove an association while it still counts a job as
running ("Job(s) active, cancel job(s) before remove"). When that happens the
termination order fails and the account stays; it must not stay open for new
jobs meanwhile (waldur/waldur-mastermind gh-109). The pause goes through the
same lever as a regular pause, so ``restore_resource`` undoes it if the
resource is kept after all.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.backend.exceptions import BackendError

_TRES = {
    "cpu": {
        "limit": 10,
        "measured_unit": "k-Hours",
        "unit_factor": 60000,
        "accounting_type": "usage",
        "label": "CPU",
    },
}


def _backend(extra: dict[str, Any] | None = None) -> SlurmBackend:
    settings = {
        "slurm_bin_path": "/usr/bin",
        "enable_user_homedir_account_creation": False,
        **(extra or {}),
    }
    backend = SlurmBackend(settings, _TRES)
    backend.client = MagicMock()
    backend.client.get_resource.return_value = {"name": "acct1"}
    backend.client.account_has_users.return_value = True
    backend.client.get_current_account_qos.return_value = "normal"
    backend.client.get_current_account_default_qos.return_value = ""
    backend.client.list_resources.return_value = []
    return backend


def _resource() -> WaldurResource:
    return WaldurResource(uuid=uuid.uuid4(), name="alloc", backend_id="acct1")


def _call_names(backend: SlurmBackend) -> list[str]:
    return [call[0] for call in backend.client.mock_calls]


def _index(names: list[str], name: str) -> int:
    assert name in names, f"{name} not called: {names}"
    return names.index(name)


class TestDeleteBlocksSubmissionsFirst:
    def test_grp_submit_jobs_lever_runs_before_teardown(self):
        backend = _backend({"qos_management": {"skip_qos_swap": True}})

        backend.delete_resource(_resource())

        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)
        names = _call_names(backend)
        block = _index(names, "set_account_grp_submit_jobs")
        assert block < _index(names, "cancel_active_user_jobs")
        assert block < _index(names, "delete_all_users_from_account")
        assert block < _index(names, "delete_resource")

    def test_paused_qos_lever_runs_before_teardown(self):
        backend = _backend({"qos_paused": "paused", "qos_default": "normal"})

        backend.delete_resource(_resource())

        backend.client.set_account_qos.assert_called_once_with("acct1", "paused")
        backend.client.set_account_grp_submit_jobs.assert_not_called()
        names = _call_names(backend)
        block = _index(names, "set_account_qos")
        assert block < _index(names, "cancel_active_user_jobs")
        assert block < _index(names, "delete_all_users_from_account")

    def test_block_stays_when_removal_fails(self):
        backend = _backend({"qos_management": {"skip_qos_swap": True}})
        backend.client.delete_all_users_from_account.side_effect = BackendError(
            "Error with request: Job(s) active, cancel job(s) before remove"
        )

        with pytest.raises(BackendError, match="Job\\(s\\) active"):
            backend.delete_resource(_resource())

        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)
        backend.client.delete_resource.assert_not_called()

    def test_restore_lifts_the_block(self):
        backend = _backend({"qos_management": {"skip_qos_swap": True}})
        backend._pre_delete_resource(_resource())

        backend.restore_resource("acct1")

        assert backend.client.set_account_grp_submit_jobs.call_args_list[-1].args == (
            "acct1",
            -1,
        )

    def test_failed_pause_does_not_stop_termination(self):
        backend = _backend({"qos_management": {"skip_qos_swap": True}})
        backend.client.set_account_grp_submit_jobs.side_effect = BackendError("boom")

        backend.delete_resource(_resource())

        backend.client.cancel_active_user_jobs.assert_called_once_with("acct1")
        backend.client.delete_resource.assert_called_once_with("acct1")

    def test_no_pause_lever_configured_still_terminates(self):
        backend = _backend()

        backend.delete_resource(_resource())

        backend.client.set_account_qos.assert_not_called()
        backend.client.set_account_grp_submit_jobs.assert_not_called()
        backend.client.delete_resource.assert_called_once_with("acct1")

    def test_cached_qos_is_not_reported_for_the_next_account(self):
        backend = _backend({"qos_paused": "paused", "qos_default": "normal"})
        backend.client.get_current_account_qos.return_value = "normal"

        backend._pre_delete_resource(_resource())

        assert backend.get_resource_metadata("acct2") == {"qos": "normal"}
        backend.client.get_current_account_qos.assert_called_with("acct2")
