"""add_user: a missing IdP user is a skip, a failed group change is a failure."""

from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent_k8s_ut_namespace.backend import K8sUtNamespaceBackend


def _backend(backend_settings, backend_components):
    with (
        patch("waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"),
        patch("waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient") as kc_cls,
    ):
        kc = MagicMock()
        kc_cls.return_value = kc
        backend = K8sUtNamespaceBackend(backend_settings, backend_components)
    backend.keycloak_client = kc
    return backend, kc


def test_user_not_in_keycloak_is_not_provisioned(
    backend_settings, backend_components, waldur_resource
):
    backend, kc = _backend(backend_settings, backend_components)
    waldur_resource.backend_id = "waldur-test-ns"
    with patch.object(backend, "_get_keycloak_group_ids", return_value={backend.default_role: "g1"}):
        kc.find_user.return_value = None
        with pytest.raises(UserNotProvisionedError):
            backend.add_user(waldur_resource, "alice")


def test_missing_role_group_is_a_failure(backend_settings, backend_components, waldur_resource):
    backend, _ = _backend(backend_settings, backend_components)
    waldur_resource.backend_id = "waldur-test-ns"
    with patch.object(backend, "_get_keycloak_group_ids", return_value={}):
        with pytest.raises(BackendError):
            backend.add_user(waldur_resource, "alice")


def test_failed_group_add_is_a_failure(backend_settings, backend_components, waldur_resource):
    backend, kc = _backend(backend_settings, backend_components)
    waldur_resource.backend_id = "waldur-test-ns"
    with patch.object(backend, "_get_keycloak_group_ids", return_value={backend.default_role: "g1"}):
        kc.find_user.return_value = {"id": "kc-alice"}
        kc.add_user_to_group.side_effect = RuntimeError("keycloak 500")
        with pytest.raises(BackendError, match="keycloak 500"):
            backend.add_user(waldur_resource, "alice")
