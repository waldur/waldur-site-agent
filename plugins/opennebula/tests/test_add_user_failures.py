"""A user not yet in Keycloak is not provisioned (a skip), not a failure."""

from unittest.mock import MagicMock

import pytest
from waldur_site_agent.backend.exceptions import UserNotProvisionedError
from waldur_site_agent_opennebula.backend import OpenNebulaBackend


def test_user_not_in_keycloak_is_not_provisioned():
    backend = OpenNebulaBackend.__new__(OpenNebulaBackend)
    backend.keycloak_client = MagicMock()
    backend.keycloak_client.find_user.return_value = None
    backend.default_user_role = "user"
    backend._get_resource_slug = MagicMock(return_value="vdc1")
    with pytest.raises(UserNotProvisionedError):
        backend.add_user(MagicMock(), "alice")
