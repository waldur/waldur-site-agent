"""A SLURM association that could not be created is reported, not swallowed."""

from unittest.mock import MagicMock

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.backend import SlurmBackend


def _backend():
    backend = SlurmBackend({"default_account": "root"}, {"cpu": {"unit": "minutes"}})
    backend.client = MagicMock()
    backend.client.get_association.return_value = None
    backend.backend_settings["enable_user_homedir_account_creation"] = False
    return backend


def _resource(backend_id="acct1"):
    resource = MagicMock()
    resource.backend_id = backend_id
    return resource


def test_add_user_raises_when_sacctmgr_fails():
    backend = _backend()
    backend.client.create_association.side_effect = BackendError("injected backend failure")

    with pytest.raises(BackendError, match="injected backend failure"):
        backend.add_user(_resource(), "alice")


def test_add_users_to_resource_reports_failed_users():
    backend = _backend()

    def create_association(username, *_args, **_kwargs):
        if username == "bob":
            raise BackendError("injected backend failure")

    backend.client.create_association.side_effect = create_association

    added = backend.add_users_to_resource(_resource(), {"alice", "bob"})

    assert added == {"alice"}
    assert list(added.failed) == ["bob"]
    assert "injected backend failure" in added.failed["bob"]
