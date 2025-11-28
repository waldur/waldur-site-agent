"""Integration tests for complete Rancher plugin workflow."""

import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.offering_user import OfferingUser
from waldur_site_agent_rancher.backend import RancherBackend


class MockResourceLimits:
    """Mock ResourceLimits for testing."""

    def __init__(self, cpu=4, memory=8, storage=100, pods=50):
        self.cpu = cpu
        self.memory = memory
        self.storage = storage
        self.pods = pods

    def to_dict(self):
        return {
            "cpu": self.cpu,
            "memory": self.memory,
            "storage": self.storage,
            "pods": self.pods,
        }


@pytest.fixture
def backend_settings():
    """Complete backend settings for integration testing."""
    return {
        "backend_url": "https://rancher.example.com",
        "username": "test-access-key",
        "password": "test-secret-key",
        "cluster_id": "c-m-test123:p-test456",
        "verify_cert": False,
        "project_prefix": "waldur-",
        "keycloak_enabled": True,
        "keycloak_role_name": "workloads-manage",
        "keycloak_url": "https://keycloak.example.com/auth/",
        "keycloak_realm": "waldur",
        "keycloak_username": "admin",
        "keycloak_password": "admin-password",
        "keycloak_ssl_verify": False,
    }


@pytest.fixture
def components():
    """Component definitions for testing."""
    return {
        "cpu": {"type": "cpu", "name": "CPU", "billing_type": "limit"},
        "memory": {"type": "ram", "name": "RAM", "billing_type": "limit"},
        "storage": {"type": "storage", "name": "Storage", "billing_type": "limit"},
        "pods": {"type": "pods", "name": "Pods", "billing_type": "limit"},
    }


@pytest.fixture
def test_resource():
    """Test Waldur resource for integration testing."""
    project_uuid = uuid4()
    return WaldurResource(
        uuid=uuid4(),
        name="Test Kubernetes Project",
        slug="test-k8s-project",
        customer_slug="test-customer",
        project_slug="ai-research",
        project_uuid=project_uuid,
        limits=MockResourceLimits(),
    )


@pytest.fixture
def test_user():
    """Test offering user."""
    return OfferingUser(username="testuser")


class TestRancherIntegration:
    """Integration tests for complete workflows."""

    @patch("waldur_site_agent_rancher.backend.KeycloakClient")
    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_complete_resource_lifecycle(
        self, mock_rancher_client, mock_keycloak_client, backend_settings, components, test_resource
    ):
        """Test complete resource lifecycle: create, update limits, delete."""
        # Mock Rancher client
        mock_rancher = MagicMock()
        mock_rancher.ping.return_value = True
        mock_rancher.list_projects.return_value = []
        mock_rancher.create_project.return_value = "c-j8276:p-test123"
        mock_rancher.create_namespace.return_value = None
        mock_rancher.get_project_namespaces.return_value = ["waldur-test-resource"]
        mock_rancher.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher.get_project_quotas.return_value = {"cpu": 4, "memory": 8}
        mock_rancher.delete_project.return_value = None
        mock_rancher_client.return_value = mock_rancher

        # Mock Keycloak client
        mock_keycloak = MagicMock()
        mock_keycloak.ping.return_value = True
        mock_keycloak.get_group_by_name.return_value = None
        mock_keycloak.create_group.side_effect = ["parent-123", "child-123"]
        mock_keycloak.delete_group.return_value = None
        mock_keycloak_client.return_value = mock_keycloak

        backend = RancherBackend(backend_settings, components)

        # Test 1: Resource creation
        result = backend.create_resource(test_resource)

        assert result.backend_id == "c-j8276:p-test123"
        assert test_resource.backend_id == "c-j8276:p-test123"

        # Verify Rancher project was created
        expected_project_name = backend._get_rancher_project_name(test_resource)
        expected_description = f"{test_resource.name} (Customer: {test_resource.customer_slug}, Project: {test_resource.project_slug})"
        mock_rancher.create_project.assert_called_once_with(
            name=expected_project_name,
            description=expected_description,
            organization=test_resource.customer_slug,
            project_slug=test_resource.project_slug,
        )

        # Verify Keycloak groups were created
        assert mock_keycloak.create_group.call_count == 2

        # Verify namespace was created
        mock_rancher.create_namespace.assert_called_once()
        namespace_call_args = mock_rancher.create_namespace.call_args
        namespace_project_id = namespace_call_args[0][0]
        namespace_name = namespace_call_args[0][1]

        assert namespace_project_id == "c-j8276:p-test123"
        assert namespace_name == expected_project_name

        # Verify quotas were set on the namespace via set_namespace_custom_resource_quotas
        # Should be called once during create_resource
        assert mock_rancher.set_namespace_custom_resource_quotas.call_count >= 1
        quota_call_args = mock_rancher.set_namespace_custom_resource_quotas.call_args_list[0]
        quota_namespace = quota_call_args[0][0]
        quota_limits = quota_call_args[0][1]

        assert quota_namespace == expected_project_name
        assert "cpu" in quota_limits
        assert "memory" in quota_limits
        assert "storage" in quota_limits  # Storage is now included in quotas
        assert "pods" not in quota_limits  # Pods are not quota components

        # Test 2: Limit updates
        new_limits = {"cpu": 8, "memory": 16, "storage": 200, "pods": 100}
        backend.set_resource_limits(result.backend_id, new_limits)

        # Verify set_namespace_custom_resource_quotas was called
        # Called once in create_resource and once in set_resource_limits
        expected_call_count = 2
        assert mock_rancher.set_namespace_custom_resource_quotas.call_count == expected_call_count

        # Test 3: Resource deletion
        backend.delete_resource(test_resource)

        # Verify cleanup
        mock_rancher.delete_project.assert_called_once_with("c-j8276:p-test123")

    @patch("waldur_site_agent_rancher.backend.KeycloakClient")
    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_user_management_workflow(
        self,
        mock_rancher_client,
        mock_keycloak_client,
        backend_settings,
        components,
        test_resource,
        test_user,
    ):
        """Test user association workflow."""
        # Mock Rancher client
        mock_rancher = MagicMock()
        mock_rancher.ping.return_value = True
        mock_rancher.get_project.return_value = MagicMock(
            name=f"waldur-{test_resource.project_slug}", organization=test_resource.customer_slug
        )
        mock_rancher.remove_project_user.return_value = None
        mock_rancher_client.return_value = mock_rancher

        # Mock Keycloak client
        mock_keycloak = MagicMock()
        mock_keycloak.ping.return_value = True
        mock_keycloak.find_user_by_username.return_value = {"id": "user-123"}
        mock_keycloak.get_group_by_name.return_value = {"id": "group-123"}
        mock_keycloak.add_user_to_group.return_value = None
        mock_keycloak.remove_user_from_group.return_value = None
        mock_keycloak_client.return_value = mock_keycloak

        backend = RancherBackend(backend_settings, components)

        # Test user association creation (using enhanced interface)
        from uuid import uuid4
        from waldur_api_client.models.resource import Resource as WaldurResource

        test_resource_for_user = WaldurResource(
            uuid=uuid4(), backend_id="c-j8276:p-test123", project_slug="test-project"
        )
        result = backend.add_user(test_resource_for_user, "testuser")
        assert result is True

        # Verify Keycloak user was added to group
        mock_keycloak.add_user_to_group.assert_called_once()

        # Test user association removal (using enhanced interface)
        result = backend.remove_user(test_resource_for_user, "testuser")
        assert result is True

        # Verify cleanup
        mock_keycloak.remove_user_from_group.assert_called_once()

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_backend_without_keycloak(
        self, mock_rancher_client, backend_settings, components, test_resource
    ):
        """Test backend functionality without Keycloak integration."""
        # Disable Keycloak
        backend_settings["keycloak_enabled"] = False

        mock_rancher = MagicMock()
        mock_rancher.ping.return_value = True
        mock_rancher.list_projects.return_value = []
        mock_rancher.create_project.return_value = "project-123"
        mock_rancher.create_namespace.return_value = None
        mock_rancher.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_rancher

        backend = RancherBackend(backend_settings, components)

        # Should initialize without Keycloak client
        assert backend.keycloak_client is None

        # Resource creation should still work
        result = backend.create_resource(test_resource)
        assert result.backend_id == "project-123"

        # Verify Rancher operations were called
        mock_rancher.create_project.assert_called_once()
        mock_rancher.create_namespace.assert_called_once()

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_quota_filtering_integration(self, mock_rancher_client, backend_settings, components):
        """Test quota filtering in integration context."""
        mock_rancher = MagicMock()
        mock_rancher_client.return_value = mock_rancher

        backend = RancherBackend(backend_settings, components)

        # Test filtering with all component types
        all_limits = {"cpu": 4, "memory": 8, "storage": 100, "pods": 50}
        quota_filtered = backend._filter_quota_components(all_limits)

        # Verify CPU, memory, and storage are included (pods is excluded)
        expected = {"cpu": 4, "memory": 8, "storage": 100}
        assert quota_filtered == expected

        # Test with empty limits
        empty_filtered = backend._filter_quota_components({})
        assert empty_filtered == {}

        # Test with only pods (non-quota component)
        non_quota_limits = {"pods": 50}
        non_quota_filtered = backend._filter_quota_components(non_quota_limits)
        assert non_quota_filtered == {}
