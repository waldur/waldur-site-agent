"""Integration tests for Harbor backend.

These tests are designed to run against a real Harbor instance.
They can be skipped if no Harbor instance is available.
"""

import os
import pytest
from unittest.mock import Mock

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_harbor.backend import HarborBackend
from waldur_site_agent_harbor.client import HarborClient


# Skip integration tests unless explicitly enabled
INTEGRATION_TESTS = (
    os.environ.get("HARBOR_INTEGRATION_TESTS", "false").lower() == "true"
)
HARBOR_URL = os.environ.get("HARBOR_URL", "https://harbor.example.com")
ROBOT_USERNAME = os.environ.get("HARBOR_ROBOT_USERNAME", "robot$test")
ROBOT_PASSWORD = os.environ.get("HARBOR_ROBOT_PASSWORD", "test-password")


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestHarborIntegration:
    """Integration tests for Harbor backend against real Harbor instance."""

    @pytest.fixture
    def harbor_client(self):
        """Create a real Harbor client."""
        return HarborClient(
            harbor_url=HARBOR_URL,
            robot_username=ROBOT_USERNAME,
            robot_password=ROBOT_PASSWORD,
        )

    @pytest.fixture
    def harbor_backend(self):
        """Create a real Harbor backend."""
        settings = {
            "harbor_url": HARBOR_URL,
            "robot_username": ROBOT_USERNAME,
            "robot_password": ROBOT_PASSWORD,
            "default_storage_quota_gb": 5,
            "oidc_group_prefix": "waldur-test-",
            "allocation_prefix": "waldur-test-",
            "project_role_id": 2,
        }
        components = {
            "storage": {
                "measured_unit": "GB",
                "accounting_type": "limit",
                "label": "Container Storage",
                "unit_factor": 1,
            }
        }
        return HarborBackend(settings, components)

    @pytest.fixture
    def test_resource(self):
        """Create a test Waldur resource."""
        resource = Mock(spec=WaldurResource)
        resource.uuid = "test-uuid-integration"
        resource.name = "Test Integration Registry"
        resource.slug = "test-integration"
        resource.project_slug = "test-project-int"
        resource.backend_id = "waldur-test-integration"
        resource.limits = {"storage": 3}
        resource.offering_plugin_options = {}
        return resource

    def test_ping(self, harbor_backend):
        """Test connectivity to Harbor."""
        assert harbor_backend.ping() is True

    def test_diagnostics(self, harbor_backend):
        """Test diagnostics functionality."""
        assert harbor_backend.diagnostics() is True

    def test_list_projects(self, harbor_client):
        """Test listing existing projects."""
        projects = harbor_client.list_resources()
        assert isinstance(projects, list)
        print(f"Found {len(projects)} projects")

    def test_project_lifecycle(self, harbor_backend, test_resource):
        """Test creating and deleting a project."""
        # Clean up any existing test project first
        try:
            harbor_backend.delete_resource(test_resource)
        except:
            pass

        # Create the project
        result = harbor_backend.create_resource(test_resource)
        assert result.backend_id == "waldur-test-integration"
        assert result.limits == {"storage": 3}

        # Verify it exists
        pulled = harbor_backend._pull_backend_resource("waldur-test-integration")
        assert pulled is not None
        assert pulled.backend_id == "waldur-test-integration"

        # Update limits
        harbor_backend.set_resource_limits("waldur-test-integration", {"storage": 5})
        limits = harbor_backend.get_resource_limits("waldur-test-integration")
        assert limits["storage"] == 5

        # Get usage
        usage_report = harbor_backend._get_usage_report(["waldur-test-integration"])
        assert "waldur-test-integration" in usage_report

        # Delete the project
        harbor_backend.delete_resource(test_resource)

        # Verify it's gone
        pulled = harbor_backend._pull_backend_resource("waldur-test-integration")
        assert pulled is None

    def test_quota_operations(self, harbor_client):
        """Test quota management operations."""
        # This test requires an existing project
        # It's primarily for manual testing
        test_project = "waldur-test-quota"

        # Try to create test project (may fail if it exists)
        try:
            harbor_client.create_project(test_project, 2)
        except:
            pass

        # Update quota
        try:
            updated = harbor_client.update_project_quota(test_project, 4)
            if updated:
                limits = harbor_client.get_resource_limits(test_project)
                assert limits["storage"] == 4
        finally:
            # Clean up
            try:
                harbor_client.delete_project(test_project)
            except:
                pass


def run_integration_tests():
    """Run integration tests manually."""
    import sys

    # Set environment variables
    os.environ["HARBOR_INTEGRATION_TESTS"] = "true"

    # You can override these with actual values for testing
    # os.environ["HARBOR_URL"] = "https://your-harbor.com"
    # os.environ["HARBOR_ROBOT_USERNAME"] = "robot$your-robot"
    # os.environ["HARBOR_ROBOT_PASSWORD"] = "your-password"

    # Run pytest
    pytest.main([__file__, "-v", "-s"])


if __name__ == "__main__":
    run_integration_tests()
