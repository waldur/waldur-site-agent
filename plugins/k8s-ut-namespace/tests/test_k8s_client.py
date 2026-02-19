"""Tests for K8s UT namespace client."""

import pytest
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException

from waldur_site_agent_k8s_ut_namespace.k8s_client import (
    K8sUtNamespaceClient,
    MNS_API_GROUP,
    MNS_API_VERSION,
    MNS_PLURAL,
)
from waldur_site_agent.backend.exceptions import BackendError


@pytest.fixture
def mock_k8s_client():
    """Create a K8sUtNamespaceClient with mocked K8s API."""
    with (
        patch("waldur_site_agent_k8s_ut_namespace.k8s_client.k8s_config") as mock_config,
        patch("waldur_site_agent_k8s_ut_namespace.k8s_client.k8s_client") as mock_client_module,
    ):
        mock_custom_api = MagicMock()
        mock_client_module.CustomObjectsApi.return_value = mock_custom_api
        mock_client_module.ApiClient.return_value = MagicMock()

        client = K8sUtNamespaceClient({"kubeconfig_path": "/tmp/fake", "cr_namespace": "test-ns"})
        client.custom_api = mock_custom_api
        yield client, mock_custom_api


class TestK8sUtNamespaceClient:
    """Tests for K8sUtNamespaceClient."""

    def test_initialization(self, mock_k8s_client):
        client, _ = mock_k8s_client
        assert client.cr_namespace == "test-ns"

    def test_ping_success(self, mock_k8s_client):
        client, _ = mock_k8s_client
        with patch(
            "waldur_site_agent_k8s_ut_namespace.k8s_client.k8s_client"
        ) as mock_mod:
            mock_version = MagicMock()
            mock_mod.VersionApi.return_value = mock_version
            assert client.ping() is True

    def test_ping_failure(self, mock_k8s_client):
        client, _ = mock_k8s_client
        with patch(
            "waldur_site_agent_k8s_ut_namespace.k8s_client.k8s_client"
        ) as mock_mod:
            mock_version = MagicMock()
            mock_version.get_code.side_effect = Exception("Connection refused")
            mock_mod.VersionApi.return_value = mock_version
            assert client.ping() is False

    def test_create_managed_namespace(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-cr"},
        }

        spec = {"quota": {"cpu": "4", "memory": "8Gi"}}
        result = client.create_managed_namespace("test-cr", spec)

        assert result["metadata"]["name"] == "test-cr"
        mock_api.create_namespaced_custom_object.assert_called_once_with(
            group=MNS_API_GROUP,
            version=MNS_API_VERSION,
            namespace="test-ns",
            plural=MNS_PLURAL,
            body={
                "apiVersion": f"{MNS_API_GROUP}/{MNS_API_VERSION}",
                "kind": "ManagedNamespace",
                "metadata": {"name": "test-cr", "namespace": "test-ns"},
                "spec": spec,
            },
        )

    def test_create_managed_namespace_failure(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.create_namespaced_custom_object.side_effect = ApiException(status=409)

        with pytest.raises(BackendError, match="Failed to create ManagedNamespace"):
            client.create_managed_namespace("test-cr", {})

    def test_get_managed_namespace_found(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        expected = {"metadata": {"name": "test-cr"}, "spec": {"quota": {}}}
        mock_api.get_namespaced_custom_object.return_value = expected

        result = client.get_managed_namespace("test-cr")
        assert result == expected

    def test_get_managed_namespace_not_found(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=404)

        result = client.get_managed_namespace("missing-cr")
        assert result is None

    def test_get_managed_namespace_error(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=500)

        with pytest.raises(BackendError, match="Failed to get ManagedNamespace"):
            client.get_managed_namespace("test-cr")

    def test_list_managed_namespaces(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.list_namespaced_custom_object.return_value = {
            "items": [
                {"metadata": {"name": "ns1"}},
                {"metadata": {"name": "ns2"}},
            ]
        }

        result = client.list_managed_namespaces()
        assert len(result) == 2
        assert result[0]["metadata"]["name"] == "ns1"

    def test_patch_managed_namespace(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.patch_namespaced_custom_object.return_value = {"metadata": {"name": "test-cr"}}

        patch = {"spec": {"quota": {"cpu": "8"}}}
        result = client.patch_managed_namespace("test-cr", patch)

        assert result["metadata"]["name"] == "test-cr"
        mock_api.patch_namespaced_custom_object.assert_called_once_with(
            group=MNS_API_GROUP,
            version=MNS_API_VERSION,
            namespace="test-ns",
            plural=MNS_PLURAL,
            name="test-cr",
            body=patch,
        )

    def test_delete_managed_namespace(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        client.delete_managed_namespace("test-cr")

        mock_api.delete_namespaced_custom_object.assert_called_once_with(
            group=MNS_API_GROUP,
            version=MNS_API_VERSION,
            namespace="test-ns",
            plural=MNS_PLURAL,
            name="test-cr",
        )

    def test_delete_managed_namespace_not_found(self, mock_k8s_client):
        """Deleting a non-existent CR should not raise."""
        client, mock_api = mock_k8s_client
        mock_api.delete_namespaced_custom_object.side_effect = ApiException(status=404)

        # Should not raise
        client.delete_managed_namespace("missing-cr")

    def test_delete_managed_namespace_error(self, mock_k8s_client):
        client, mock_api = mock_k8s_client
        mock_api.delete_namespaced_custom_object.side_effect = ApiException(status=500)

        with pytest.raises(BackendError, match="Failed to delete ManagedNamespace"):
            client.delete_managed_namespace("test-cr")
