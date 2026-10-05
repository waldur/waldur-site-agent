"""A role-grant re-sync that fails is a backend failure, not a skip."""

from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_ldap_roles.backend import LdapRolesBackend


def test_add_user_raises_when_the_resync_fails():
    backend = LdapRolesBackend.__new__(LdapRolesBackend)
    with patch.object(backend, "pull_resource", side_effect=RuntimeError("ldap down")):
        with pytest.raises(BackendError, match="ldap down"):
            backend.add_user(MagicMock(), "alice")
