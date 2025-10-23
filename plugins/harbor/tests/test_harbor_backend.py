"""Tests for Harbor backend."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent.backend import structures

from waldur_site_agent_harbor.backend import HarborBackend
from waldur_site_agent_harbor.client import HarborClient
from waldur_site_agent_harbor.exceptions import HarborProjectError, HarborOIDCError


@pytest.fixture
def harbor_settings():
    """Harbor backend settings for testing."""
    return {
        "harbor_url": "https://registry.example.com",
        "robot_username": "robot$test",
        "robot_password": "test-password",
        "default_storage_quota_gb": 10,
        "oidc_group_prefix": "waldur-",
        "project_role_id": 2,
        "allocation_prefix": "waldur-",
    }


@pytest.fixture
def harbor_components():
    """Harbor component configuration for testing."""
    return {
        "storage": {
            "measured_unit": "GB",
            "accounting_type": "limit",
            "label": "Container Storage",
            "unit_factor": 1,
        }
    }


@pytest.fixture
def harbor_backend(harbor_settings, harbor_components):
    """Create Harbor backend instance for testing."""
    with patch("waldur_site_agent_harbor.backend.HarborClient"):
        backend = HarborBackend(harbor_settings, harbor_components)
        backend.client = Mock(spec=HarborClient)
        return backend


@pytest.fixture
def waldur_resource():
    """Create a mock Waldur resource."""
    resource = Mock(spec=WaldurResource)
    resource.uuid = "test-uuid-123"
    resource.name = "Test Registry"
    resource.slug = "test-registry"
    resource.project_slug = "test-project"
    resource.backend_id = "waldur-test-registry"
    resource.limits = {"storage": 20}
    resource.offering_plugin_options = {}
    return resource


class TestHarborBackend:
    """Test Harbor backend functionality."""

    def test_backend_initialization(self, harbor_settings, harbor_components):
        """Test backend initialization with valid settings."""
        with patch("waldur_site_agent_harbor.backend.HarborClient"):
            backend = HarborBackend(harbor_settings, harbor_components)

            assert backend.backend_type == "harbor"
            assert backend.default_storage_quota_gb == 10
            assert backend.oidc_group_prefix == "waldur-"
            assert backend.project_role_id == 2

    def test_backend_initialization_missing_settings(self, harbor_components):
        """Test backend initialization with missing required settings."""
        incomplete_settings = {
            "harbor_url": "https://registry.example.com"
            # Missing robot_username and robot_password
        }

        with pytest.raises(ValueError, match="Missing required setting"):
            HarborBackend(incomplete_settings, harbor_components)

    def test_backend_initialization_invalid_component(self, harbor_settings):
        """Test backend initialization with invalid component configuration."""
        invalid_components = {
            "storage": {
                "measured_unit": "GB",
                "accounting_type": "usage",  # Should be "limit"
                "label": "Container Storage",
            }
        }

        with pytest.raises(ValueError, match="accounting_type='limit'"):
            HarborBackend(harbor_settings, invalid_components)

    def test_ping_success(self, harbor_backend):
        """Test successful ping operation."""
        harbor_backend.client.ping.return_value = True

        result = harbor_backend.ping()
        assert result is True
        harbor_backend.client.ping.assert_called_once()

    def test_ping_failure(self, harbor_backend):
        """Test failed ping operation."""
        harbor_backend.client.ping.return_value = False

        result = harbor_backend.ping()
        assert result is False

    def test_diagnostics(self, harbor_backend):
        """Test diagnostics operation."""
        harbor_backend.client.ping.return_value = True
        harbor_backend.client.list_resources.return_value = [
            Mock(name="project1"),
            Mock(name="project2"),
        ]

        result = harbor_backend.diagnostics()
        assert result is True

    def test_list_components(self, harbor_backend):
        """Test listing supported components."""
        components = harbor_backend.list_components()
        assert components == ["storage"]

    def test_create_resource_success(self, harbor_backend, waldur_resource):
        """Test successful resource creation."""
        harbor_backend.client.create_user_group.return_value = 1
        harbor_backend.client.create_project.return_value = True
        harbor_backend.client.assign_group_to_project.return_value = True

        result = harbor_backend.create_resource(waldur_resource)

        assert result.backend_id == "waldur-test-registry"
        assert result.limits == {"storage": 20}

        # Verify method calls
        harbor_backend.client.create_user_group.assert_called_once_with(
            "waldur-test-project"
        )
        harbor_backend.client.create_project.assert_called_once_with(
            "waldur-test-registry", 20
        )
        harbor_backend.client.assign_group_to_project.assert_called_once_with(
            "waldur-test-project", "waldur-test-registry", 2
        )

    def test_create_resource_with_default_quota(self, harbor_backend, waldur_resource):
        """Test resource creation with default quota."""
        waldur_resource.limits = {}  # No limits specified

        harbor_backend.client.create_user_group.return_value = 1
        harbor_backend.client.create_project.return_value = True
        harbor_backend.client.assign_group_to_project.return_value = True

        result = harbor_backend.create_resource(waldur_resource)

        assert result.limits == {"storage": 10}  # Default quota
        harbor_backend.client.create_project.assert_called_once_with(
            "waldur-test-registry", 10
        )

    def test_delete_resource_success(self, harbor_backend, waldur_resource):
        """Test successful resource deletion."""
        harbor_backend.client.delete_project.return_value = True

        harbor_backend.delete_resource(waldur_resource)

        harbor_backend.client.delete_project.assert_called_once_with(
            "waldur-test-registry"
        )

    def test_delete_resource_empty_backend_id(self, harbor_backend, waldur_resource):
        """Test resource deletion with empty backend ID."""
        waldur_resource.backend_id = ""

        harbor_backend.delete_resource(waldur_resource)

        harbor_backend.client.delete_project.assert_not_called()

    def test_pull_backend_resource_found(self, harbor_backend):
        """Test pulling existing resource from backend."""
        project = {"name": "waldur-test-registry", "project_id": 1}
        usage_data = {"storage_bytes": 5368709120, "repository_count": 3}  # 5 GB
        limits = {"storage": 20}

        harbor_backend.client.get_project.return_value = project
        harbor_backend.client.get_project_usage.return_value = usage_data
        harbor_backend.client.get_resource_limits.return_value = limits

        result = harbor_backend._pull_backend_resource("waldur-test-registry")

        assert result is not None
        assert result.backend_id == "waldur-test-registry"
        assert result.limits == {"storage": 20}
        assert result.usage["TOTAL_ACCOUNT_USAGE"]["storage"] == 5

    def test_pull_backend_resource_not_found(self, harbor_backend):
        """Test pulling non-existent resource."""
        harbor_backend.client.get_project.return_value = None

        result = harbor_backend._pull_backend_resource("waldur-test-registry")

        assert result is None

    def test_get_usage_report(self, harbor_backend):
        """Test getting usage report for multiple resources."""
        usage_data_1 = {"storage_bytes": 5368709120, "repository_count": 3}  # 5 GB
        usage_data_2 = {"storage_bytes": 10737418240, "repository_count": 5}  # 10 GB

        harbor_backend.client.get_project_usage.side_effect = [
            usage_data_1,
            usage_data_2,
        ]

        result = harbor_backend._get_usage_report(["project1", "project2"])

        assert result["project1"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 5
        assert result["project2"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 10

    def test_collect_resource_limits_with_waldur_limits(
        self, harbor_backend, waldur_resource
    ):
        """Test collecting resource limits from Waldur resource."""
        backend_limits, waldur_limits = harbor_backend._collect_resource_limits(
            waldur_resource
        )

        assert backend_limits == {"storage": 20}
        assert waldur_limits == {"storage": 20}

    def test_collect_resource_limits_default(self, harbor_backend, waldur_resource):
        """Test collecting resource limits with default values."""
        waldur_resource.limits = None

        backend_limits, waldur_limits = harbor_backend._collect_resource_limits(
            waldur_resource
        )

        assert backend_limits == {"storage": 10}  # Default
        assert waldur_limits == {"storage": 10}

    def test_set_resource_limits(self, harbor_backend):
        """Test setting resource limits."""
        harbor_backend.client.update_project_quota.return_value = True

        harbor_backend.set_resource_limits("test-project", {"storage": 30})

        harbor_backend.client.update_project_quota.assert_called_once_with(
            "test-project", 30
        )

    def test_get_resource_limits(self, harbor_backend):
        """Test getting resource limits."""
        harbor_backend.client.get_resource_limits.return_value = {"storage": 25}

        result = harbor_backend.get_resource_limits("test-project")

        assert result == {"storage": 25}

    def test_downscale_resource(self, harbor_backend):
        """Test downscaling resource."""
        harbor_backend.client.update_project_quota.return_value = True

        result = harbor_backend.downscale_resource("test-project")

        assert result is True
        harbor_backend.client.update_project_quota.assert_called_once_with(
            "test-project", 1
        )

    def test_restore_resource(self, harbor_backend):
        """Test restoring resource."""
        harbor_backend.client.update_project_quota.return_value = True

        result = harbor_backend.restore_resource("test-project")

        assert result is True
        harbor_backend.client.update_project_quota.assert_called_once_with(
            "test-project", 10
        )

    def test_pause_resource_not_supported(self, harbor_backend):
        """Test pause resource (not supported)."""
        result = harbor_backend.pause_resource("test-project")
        assert result is False

    def test_get_resource_metadata(self, harbor_backend):
        """Test getting resource metadata."""
        project = {
            "name": "test-project",
            "project_id": 123,
            "creation_time": "2024-01-01T00:00:00Z",
        }
        usage_data = {"storage_bytes": 5368709120, "repository_count": 3}

        harbor_backend.client.get_project.return_value = project
        harbor_backend.client.get_project_usage.return_value = usage_data

        metadata = harbor_backend.get_resource_metadata("test-project")

        assert metadata["harbor_project_id"] == 123
        assert metadata["harbor_project_name"] == "test-project"
        assert metadata["repository_count"] == 3
        assert metadata["storage_used_bytes"] == 5368709120
        assert "harbor_url" in metadata

    def test_add_users_to_resource_noop(self, harbor_backend):
        """Test adding users (no-op for Harbor)."""
        result = harbor_backend.add_users_to_resource(
            "test-project", {"user1", "user2"}
        )
        assert result == set()

    def test_remove_users_from_resource_noop(self, harbor_backend):
        """Test removing users (no-op for Harbor)."""
        result = harbor_backend.remove_users_from_resource(
            "test-project", {"user1", "user2"}
        )
        assert result == []
