"""Tests for Rancher backend implementation."""

import pytest
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_site_agent_rancher.backend import RancherBackend
from waldur_site_agent.backend.exceptions import BackendError


@pytest.fixture
def rancher_settings():
    """Basic Rancher settings for testing."""
    return {
        "api_url": "https://rancher.example.com/v3",
        "access_key": "test-access-key",
        "secret_key": "test-secret-key",
        "cluster_id": "c-m-test:p-test",
        "verify_cert": False,
        "project_prefix": "waldur-",
        "keycloak_enabled": False,
    }


@pytest.fixture
def rancher_settings_with_keycloak():
    """Rancher settings with Keycloak integration for testing."""
    return {
        "api_url": "https://rancher.example.com/v3",
        "access_key": "test-access-key",
        "secret_key": "test-secret-key",
        "cluster_id": "c-m-test:p-test",
        "verify_cert": False,
        "project_prefix": "waldur-",
        "keycloak_enabled": True,
        "keycloak": {
            "keycloak_url": "https://keycloak.example.com/auth/",
            "keycloak_realm": "test",
            "client_id": "admin-cli",
            "keycloak_username": "admin",
            "keycloak_password": "test-password",
            "keycloak_ssl_verify": False,
        },
    }


@pytest.fixture
def rancher_components():
    """Component definitions for testing."""
    return {
        "cpu": {"type": "cpu", "name": "CPU", "measured_unit": "cores"},
        "memory": {"type": "ram", "name": "RAM", "measured_unit": "GB"},
        "storage": {"type": "storage", "name": "Storage", "measured_unit": "GB"},
        "pods": {"type": "pods", "name": "Pods", "measured_unit": "pods"},
    }


class MockResourceLimits:
    """Mock ResourceLimits for testing."""

    def __init__(self):
        self.cpu = 4
        self.memory = 8
        self.storage = 100
        self.pods = 50

    def to_dict(self):
        return {
            "cpu": self.cpu,
            "memory": self.memory,
            "storage": self.storage,
            "pods": self.pods,
        }


@pytest.fixture
def waldur_resource():
    """Sample Waldur resource for testing."""
    resource = WaldurResource(
        uuid=uuid4(),
        name="Test Project",
        slug="test-resource",
        customer_slug="test-customer",
        project_slug="test-project",
        backend_id="",
        limits=MockResourceLimits(),
    )
    return resource


@pytest.fixture
def offering_user():
    """Sample offering user for testing."""
    return OfferingUser(
        username="testuser", user_full_name="Test User", user_email="test@example.com"
    )


class TestRancherBackend:
    """Test cases for RancherBackend."""

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_initialization_basic(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test basic initialization without Keycloak."""
        backend = RancherBackend(rancher_settings, rancher_components)

        assert backend.backend_type == "rancher"
        assert backend.project_prefix == "waldur-"
        assert backend.cluster_id == "c-m-test:p-test"
        assert backend.keycloak_client is None

        mock_rancher_client.assert_called_once_with(rancher_settings)

    @patch("waldur_site_agent_rancher.backend.KeycloakClient")
    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_initialization_with_keycloak(
        self,
        mock_rancher_client,
        mock_keycloak_client,
        rancher_settings_with_keycloak,
        rancher_components,
    ):
        """Test initialization with Keycloak integration."""
        backend = RancherBackend(rancher_settings_with_keycloak, rancher_components)

        assert backend.backend_type == "rancher"
        assert backend.keycloak_client is not None

        mock_rancher_client.assert_called_once_with(rancher_settings_with_keycloak)
        mock_keycloak_client.assert_called_once_with(rancher_settings_with_keycloak["keycloak"])

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_ping_success(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test successful ping operation."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        assert backend.ping() is True
        mock_client.ping.assert_called_once()

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_ping_failure(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test ping failure."""
        mock_client = MagicMock()
        mock_client.ping.return_value = False
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        assert backend.ping() is False
        mock_client.ping.assert_called_once()

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_ping_with_exception_raising(
        self, mock_rancher_client, rancher_settings, rancher_components
    ):
        """Test ping with exception raising enabled."""
        mock_client = MagicMock()
        mock_client.ping.return_value = False
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        with pytest.raises(BackendError, match="Failed to ping Rancher cluster"):
            backend.ping(raise_exception=True)

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_list_components(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test listing available components."""
        backend = RancherBackend(rancher_settings, rancher_components)

        components = backend.list_components()
        assert components == ["cpu", "memory", "storage", "pods"]

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_get_rancher_project_name(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test Rancher project name generation."""
        backend = RancherBackend(rancher_settings, rancher_components)

        project_name = backend._get_rancher_project_name(waldur_resource)
        # Should be {prefix}{resource-slug}
        assert project_name == f"waldur-{waldur_resource.slug}"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_get_keycloak_child_group_name(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test Keycloak child group name generation."""
        backend = RancherBackend(rancher_settings, rancher_components)

        group_name = backend._get_keycloak_child_group_name(waldur_resource)
        # Should be project_{project-slug}_{role}
        assert group_name == f"project_{waldur_resource.project_slug}_{backend.rancher_role}"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_pre_create_resource_validation(
        self, mock_rancher_client, rancher_settings, rancher_components
    ):
        """Test resource validation before creation."""
        backend = RancherBackend(rancher_settings, rancher_components)

        # Test with valid resource
        valid_resource = WaldurResource(
            uuid=uuid4(),
            name="Test",
            customer_slug="customer",
            project_slug="project",
            backend_id="",
        )

        # Should not raise an exception
        backend._pre_create_resource(valid_resource)

        # Test with invalid resource (missing slugs)
        invalid_resource = WaldurResource(
            uuid=uuid4(), name="Test", customer_slug="", project_slug="", backend_id=""
        )

        with pytest.raises(BackendError, match="has unset or missing slug fields"):
            backend._pre_create_resource(invalid_resource)

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_create_resource_basic(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test basic resource creation without Keycloak."""
        mock_client = MagicMock()
        mock_client.list_projects.return_value = []
        mock_client.create_project.return_value = "project-123"
        mock_client.create_namespace.return_value = None
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        result = backend.create_resource(waldur_resource)

        # Check the result
        assert result.backend_id == "project-123"
        assert waldur_resource.backend_id == "project-123"

        # Check that the basic BackendResourceInfo structure is correct
        # The result contains backend_id and limits, metadata is not part of the structure
        assert isinstance(result.limits, dict)

        # Check the project was created correctly with actual backend behavior
        expected_description = "Test Project (Customer: test-customer, Project: test-project)"
        mock_client.create_project.assert_called_once_with(
            name="waldur-test-resource",
            description=expected_description,
            organization="test-customer",
            project_slug="test-project",
        )

    @patch("waldur_site_agent_rancher.backend.KeycloakClient")
    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_create_resource_with_keycloak(
        self,
        mock_rancher_client,
        mock_keycloak_client,
        rancher_settings_with_keycloak,
        rancher_components,
        waldur_resource,
    ):
        """Test resource creation with Keycloak integration."""
        mock_rancher = MagicMock()
        mock_rancher.list_projects.return_value = []
        mock_rancher.create_project.return_value = "project-123"
        mock_rancher.create_namespace.return_value = None
        mock_rancher.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_rancher

        mock_keycloak = MagicMock()
        mock_keycloak.get_group_by_name.return_value = None  # Group doesn't exist
        mock_keycloak.create_group.return_value = "group-123"
        mock_keycloak_client.return_value = mock_keycloak

        backend = RancherBackend(rancher_settings_with_keycloak, rancher_components)

        result = backend.create_resource(waldur_resource)

        assert result.backend_id == "project-123"
        # Check that the basic BackendResourceInfo structure is correct
        assert isinstance(result.limits, dict)

        # Verify that keycloak group operations were called correctly
        # Should create both parent cluster group and child project group
        assert mock_keycloak.create_group.call_count == 2
        expected_calls = [
            call("c_cmtestpt", "Cluster access group for c-m-test:p-test"),
            call(
                "project_test-project_workloads-manage",
                "Project test-project members with role workloads-manage",
                "group-123",
            ),
        ]
        mock_keycloak.create_group.assert_has_calls(expected_calls)

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_delete_resource(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test resource deletion."""
        mock_client = MagicMock()
        mock_client.delete_project.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        waldur_resource.backend_id = "project-123"
        backend.delete_resource(waldur_resource)

        mock_client.delete_project.assert_called_once_with("project-123")

    @patch("waldur_site_agent_rancher.backend.KeycloakClient")
    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_delete_resource_with_keycloak(
        self,
        mock_rancher_client,
        mock_keycloak_client,
        rancher_settings_with_keycloak,
        rancher_components,
        waldur_resource,
    ):
        """Test resource deletion with Keycloak cleanup."""
        mock_rancher = MagicMock()
        mock_rancher.delete_project.return_value = None
        mock_rancher_client.return_value = mock_rancher

        mock_keycloak = MagicMock()
        mock_keycloak.get_group_by_name.return_value = {
            "id": "group-456",
            "name": "project_test-project_workloads-manage",
        }
        mock_keycloak.delete_group.return_value = None
        mock_keycloak_client.return_value = mock_keycloak

        backend = RancherBackend(rancher_settings_with_keycloak, rancher_components)

        waldur_resource.backend_id = "project-123"
        backend.delete_resource(waldur_resource)

        mock_rancher.delete_project.assert_called_once_with("project-123")

        # Check that keycloak group deletion was attempted
        mock_keycloak.get_group_by_name.assert_called_once_with(
            "project_test-project_workloads-manage"
        )
        mock_keycloak.delete_group.assert_called_once_with("group-456")

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_create_user_association_dryrun(
        self,
        mock_rancher_client,
        rancher_settings,
        rancher_components,
        offering_user,
        waldur_resource,
    ):
        """Test user association creation in dry run mode."""
        backend = RancherBackend(rancher_settings, rancher_components)

        waldur_resource.backend_id = "project-123"
        # Note: add_user doesn't have dry_run parameter, this test may need adjustment
        result = backend.add_user(waldur_resource, offering_user.username)

        assert result is True

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_delete_user_association_basic(
        self,
        mock_rancher_client,
        rancher_settings,
        rancher_components,
        offering_user,
        waldur_resource,
    ):
        """Test user association removal without Keycloak."""
        mock_client = MagicMock()
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        waldur_resource.backend_id = "project-123"
        result = backend.remove_user(waldur_resource, offering_user.username)

        # When Keycloak is disabled, backend just logs and returns True
        # OIDC handles actual access management
        assert result is True
        # No direct rancher client calls should be made when Keycloak is disabled
        mock_client.remove_project_user.assert_not_called()

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_set_resource_limits(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test setting resource limits."""
        mock_client = MagicMock()
        mock_client.get_project_namespaces.return_value = ["test-namespace"]
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        limits = {"cpu": 4, "memory": 8, "storage": 100, "pods": 50}
        backend.set_resource_limits("project-123", limits)

        # Should call set_namespace_custom_resource_quotas with namespace, limits, and mapping
        mock_client.get_project_namespaces.assert_called_once_with("project-123")
        mock_client.set_namespace_custom_resource_quotas.assert_called_once()
        call_args = mock_client.set_namespace_custom_resource_quotas.call_args
        assert call_args[0][0] == "test-namespace"
        assert call_args[0][1] == limits
        assert "component_k8s_mapping" in call_args[1]

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_filter_quota_components(
        self, mock_rancher_client, rancher_settings, rancher_components
    ):
        """Test filtering of quota components based on component type."""
        backend = RancherBackend(rancher_settings, rancher_components)

        # Test with all component types
        all_limits = {"cpu": 4, "memory": 8, "storage": 100, "pods": 50}
        quota_only = backend._filter_quota_components(all_limits)

        # Should include CPU (type: cpu), memory (type: ram), and storage (type: storage)
        assert "cpu" in quota_only
        assert "memory" in quota_only
        assert "storage" in quota_only  # storage IS a quota component
        assert "pods" not in quota_only  # pods not quota component

        assert quota_only["cpu"] == 4
        assert quota_only["memory"] == 8
        assert quota_only["storage"] == 100

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_filter_quota_components_empty(
        self, mock_rancher_client, rancher_settings, rancher_components
    ):
        """Test quota filtering with no matching components."""
        backend = RancherBackend(rancher_settings, rancher_components)

        # Test with only pods (non-quota component)
        non_quota_limits = {"pods": 50}
        quota_only = backend._filter_quota_components(non_quota_limits)

        # Should return empty dict when only non-quota components present
        assert quota_only == {}

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_create_resource_quota_filtering(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test that create_resource sets namespace quotas with filtered quota components."""
        mock_client = MagicMock()
        mock_client.list_projects.return_value = []
        mock_client.create_project.return_value = "project-123"
        mock_client.create_namespace.return_value = None
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        result = backend.create_resource(waldur_resource)

        # Check that create_namespace was called
        mock_client.create_namespace.assert_called_once()
        call_args = mock_client.create_namespace.call_args[0]
        project_id = call_args[0]
        namespace_name = call_args[1]

        assert project_id == "project-123"

        # Check that set_namespace_custom_resource_quotas was called with filtered components
        mock_client.set_namespace_custom_resource_quotas.assert_called_once()
        quota_call_args = mock_client.set_namespace_custom_resource_quotas.call_args[0]
        quota_namespace = quota_call_args[0]
        quota_components = quota_call_args[1]

        assert quota_namespace == namespace_name
        assert "cpu" in quota_components
        assert "memory" in quota_components
        assert "storage" in quota_components  # storage IS a quota component
        assert "pods" not in quota_components  # pods is NOT a quota component

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_get_usage_report(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test usage report generation from ResourceQuota status.used."""
        mock_client = MagicMock()
        mock_client.get_project_namespaces.return_value = ["waldur-test-ns"]
        mock_client.get_namespace_quota_usage.return_value = {
            "cpu": 2.5,
            "memory": 4.0,
            "storage": 50.0,
        }
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        report = backend._get_usage_report(["project-123"])

        mock_client.get_project_namespaces.assert_called_once_with("project-123")
        mock_client.get_namespace_quota_usage.assert_called_once()
        call_args = mock_client.get_namespace_quota_usage.call_args
        assert call_args[0][0] == "waldur-test-ns"
        assert "reverse_k8s_mapping" in call_args[1]
        assert "project-123" in report
        assert report["project-123"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 2.5
        assert report["project-123"]["TOTAL_ACCOUNT_USAGE"]["memory"] == 4.0

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_get_resource_metadata(self, mock_rancher_client, rancher_settings, rancher_components):
        """Test resource metadata retrieval."""
        from waldur_site_agent.backend.structures import ClientResource

        mock_client = MagicMock()
        mock_client.get_project.return_value = ClientResource(
            name="waldur-test-project",
            organization="test-customer",
            description="Test Project",
            backend_id="project-123",
        )
        mock_client.get_project_quotas.return_value = {"cpu": 4, "memory": 8}
        mock_client.list_project_users.return_value = ["user1", "user2"]
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)

        metadata = backend.get_resource_metadata("project-123")

        assert metadata["name"] == "waldur-test-project"
        assert metadata["organization"] == "test-customer"
        assert metadata["description"] == "Test Project"
        assert metadata["quotas"]["cpu"] == 4
        assert len(metadata["users"]) == 2


class TestRancherBackendNamespaceLabels:
    """Test cases for namespace label support (e.g. gpu-pool)."""

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_namespace_labels_passed_during_create(
        self, mock_rancher_client, rancher_components, waldur_resource
    ):
        """Test that namespace_labels from backend config are passed to create_namespace."""
        settings = {
            "api_url": "https://rancher.example.com/v3",
            "cluster_id": "c-m-test:p-test",
            "verify_cert": False,
            "project_prefix": "waldur-",
            "keycloak_enabled": False,
            "namespace_labels": {"gpu-pool": "h100-2x"},
        }
        mock_client = MagicMock()
        mock_client.list_projects.return_value = []
        mock_client.create_project.return_value = "project-123"
        mock_client.create_namespace.return_value = None
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(settings, rancher_components)
        backend.create_resource(waldur_resource)

        mock_client.create_namespace.assert_called_once()
        call_kwargs = mock_client.create_namespace.call_args
        assert call_kwargs[1]["extra_labels"] == {"gpu-pool": "h100-2x"}

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_no_namespace_labels_when_not_configured(
        self, mock_rancher_client, rancher_settings, rancher_components, waldur_resource
    ):
        """Test that extra_labels is None when namespace_labels not in config."""
        mock_client = MagicMock()
        mock_client.list_projects.return_value = []
        mock_client.create_project.return_value = "project-123"
        mock_client.create_namespace.return_value = None
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, rancher_components)
        backend.create_resource(waldur_resource)

        mock_client.create_namespace.assert_called_once()
        call_kwargs = mock_client.create_namespace.call_args
        assert call_kwargs[1]["extra_labels"] is None


class TestRancherBackendGpuTypes:
    """Test cases for per-GPU-type limit support."""

    @pytest.fixture
    def gpu_components(self):
        """Component definitions with multiple GPU types."""
        return {
            "cpu": {"type": "cpu", "name": "CPU", "measured_unit": "cores"},
            "memory": {"type": "ram", "name": "RAM", "measured_unit": "GB"},
            "gpu_h100": {
                "type": "gpu",
                "name": "GPU H100",
                "measured_unit": "units",
                "k8s_resource": "nvidia.com/gpu-h100",
            },
            "gpu_h200": {
                "type": "gpu",
                "name": "GPU H200",
                "measured_unit": "units",
                "k8s_resource": "nvidia.com/gpu-h200",
            },
        }

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_build_k8s_resource_mapping(
        self, mock_rancher_client, rancher_settings, gpu_components
    ):
        """Test building K8s resource mapping with custom GPU types."""
        backend = RancherBackend(rancher_settings, gpu_components)

        mapping = backend._build_k8s_resource_mapping()

        assert mapping["cpu"] == "limits.cpu"
        assert mapping["memory"] == "limits.memory"
        assert mapping["gpu_h100"] == "requests.nvidia.com/gpu-h100"
        assert mapping["gpu_h200"] == "requests.nvidia.com/gpu-h200"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_build_k8s_resource_mapping_default_gpu(
        self, mock_rancher_client, rancher_settings
    ):
        """Test backward compat: type 'gpu' without k8s_resource uses default."""
        components = {
            "cpu": {"type": "cpu", "name": "CPU", "measured_unit": "cores"},
            "gpu": {"type": "gpu", "name": "GPU", "measured_unit": "units"},
        }
        backend = RancherBackend(rancher_settings, components)

        mapping = backend._build_k8s_resource_mapping()

        assert mapping["cpu"] == "limits.cpu"
        assert mapping["gpu"] == "requests.nvidia.com/gpu"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_build_reverse_k8s_resource_mapping(
        self, mock_rancher_client, rancher_settings, gpu_components
    ):
        """Test building reverse mapping from K8s resource names to component keys."""
        backend = RancherBackend(rancher_settings, gpu_components)

        reverse = backend._build_reverse_k8s_resource_mapping()

        assert reverse["limits.cpu"] == "cpu"
        assert reverse["limits.memory"] == "memory"
        assert reverse["requests.nvidia.com/gpu-h100"] == "gpu_h100"
        assert reverse["requests.nvidia.com/gpu-h200"] == "gpu_h200"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_filter_quota_components_multi_gpu(
        self, mock_rancher_client, rancher_settings, gpu_components
    ):
        """Test that multiple GPU types pass quota filtering."""
        backend = RancherBackend(rancher_settings, gpu_components)

        limits = {"cpu": 4, "memory": 8, "gpu_h100": 2, "gpu_h200": 4}
        filtered = backend._filter_quota_components(limits)

        assert "cpu" in filtered
        assert "memory" in filtered
        assert "gpu_h100" in filtered
        assert "gpu_h200" in filtered
        assert filtered["gpu_h100"] == 2
        assert filtered["gpu_h200"] == 4

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_set_resource_limits_multi_gpu(
        self, mock_rancher_client, rancher_settings, gpu_components
    ):
        """Test that set_resource_limits passes K8s mapping to client."""
        mock_client = MagicMock()
        mock_client.get_project_namespaces.return_value = ["test-namespace"]
        mock_client.set_namespace_custom_resource_quotas.return_value = None
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, gpu_components)

        limits = {"cpu": 4, "memory": 8, "gpu_h100": 2, "gpu_h200": 4}
        backend.set_resource_limits("project-123", limits)

        mock_client.set_namespace_custom_resource_quotas.assert_called_once()
        call_kwargs = mock_client.set_namespace_custom_resource_quotas.call_args
        # Verify the mapping was passed
        k8s_mapping = call_kwargs[1]["component_k8s_mapping"]
        assert k8s_mapping["gpu_h100"] == "requests.nvidia.com/gpu-h100"
        assert k8s_mapping["gpu_h200"] == "requests.nvidia.com/gpu-h200"

    @patch("waldur_site_agent_rancher.backend.RancherClient")
    def test_get_usage_report_multi_gpu(
        self, mock_rancher_client, rancher_settings, gpu_components
    ):
        """Test usage report with multiple GPU types."""
        mock_client = MagicMock()
        mock_client.get_project_namespaces.return_value = ["waldur-test-ns"]
        mock_client.get_namespace_quota_usage.return_value = {
            "cpu": 2.5,
            "memory": 4.0,
            "gpu_h100": 1.0,
            "gpu_h200": 3.0,
        }
        mock_rancher_client.return_value = mock_client

        backend = RancherBackend(rancher_settings, gpu_components)

        report = backend._get_usage_report(["project-123"])

        # Verify reverse mapping was passed
        mock_client.get_namespace_quota_usage.assert_called_once()
        call_kwargs = mock_client.get_namespace_quota_usage.call_args
        reverse_mapping = call_kwargs[1]["reverse_k8s_mapping"]
        assert reverse_mapping["requests.nvidia.com/gpu-h100"] == "gpu_h100"
        assert reverse_mapping["requests.nvidia.com/gpu-h200"] == "gpu_h200"

        # Verify usage report
        usage = report["project-123"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["gpu_h100"] == 1.0
        assert usage["gpu_h200"] == 3.0
        assert usage["cpu"] == 2.5
