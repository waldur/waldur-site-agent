"""add_user: a missing IdP user is a skip, a failed group change is a failure."""

from unittest.mock import MagicMock

import pytest
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent_rancher.backend import RancherBackend


def _backend():
    backend = RancherBackend.__new__(RancherBackend)
    backend.keycloak_client = MagicMock()
    backend.keycloak_use_user_id = False
    backend.rancher_role = "workloads-manage"
    backend._get_keycloak_child_group_name = MagicMock(return_value="project_p_workloads-manage")
    return backend


def _resource():
    resource = MagicMock()
    resource.backend_id = "project-123"
    return resource


def test_user_not_in_keycloak_is_not_provisioned():
    backend = _backend()
    backend.keycloak_client.find_user.return_value = None
    with pytest.raises(UserNotProvisionedError):
        backend.add_user(_resource(), "alice")


def test_failed_group_add_is_a_failure():
    backend = _backend()
    backend.keycloak_client.find_user.return_value = {"id": "kc-alice"}
    backend.keycloak_client.get_group_by_name.return_value = {"id": "g1"}
    backend.keycloak_client.add_user_to_group.side_effect = RuntimeError("keycloak 500")
    with pytest.raises(BackendError, match="keycloak 500"):
        backend.add_user(_resource(), "alice")


def test_group_creation_failure_is_a_failure():
    backend = _backend()
    backend.keycloak_client.find_user.return_value = {"id": "kc-alice"}
    backend.keycloak_client.get_group_by_name.return_value = None
    backend._create_keycloak_groups = MagicMock(return_value=(None, None))
    with pytest.raises(BackendError):
        backend.add_user(_resource(), "alice")


def test_keycloak_disabled_is_nothing_to_do():
    backend = _backend()
    backend.keycloak_client = None
    assert backend.add_user(_resource(), "alice") is True
