"""A CR re-apply that fails after a role grant is a backend failure, not a skip."""

from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_rancher_kc_crd.backend import RancherKcCrdBackend


def test_add_user_raises_when_the_cr_resync_fails():
    backend = RancherKcCrdBackend.__new__(RancherKcCrdBackend)
    with patch.object(backend, "pull_resource", side_effect=RuntimeError("apiserver down")):
        with pytest.raises(BackendError, match="apiserver down"):
            backend.add_user(MagicMock(), "alice")
