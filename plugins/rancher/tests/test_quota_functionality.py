"""Tests for quota functionality including format conversion and project updates."""

import pytest
import requests
from unittest.mock import MagicMock, patch

from waldur_site_agent_rancher.rancher_client import RancherClient
from waldur_site_agent.backend.exceptions import BackendError


@pytest.fixture
def rancher_settings():
    """Basic Rancher settings for testing."""
    return {
        "backend_url": "https://rancher.example.com",
        "username": "test-access-key",
        "password": "test-secret-key",
        "cluster_id": "c-m-test:p-test",
        "verify_cert": False,
        "project_prefix": "waldur-",
    }


class TestQuotaFunctionality:
    """Test cases for quota management functionality."""

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_format_conversion(self, mock_session, rancher_settings):
        """Test conversion between Waldur and Rancher quota formats."""
        # Mock project response
        mock_project = {"id": "project-123", "name": "test-project", "type": "project"}

        mock_get_response = MagicMock()
        mock_get_response.json.return_value = mock_project

        mock_put_response = MagicMock()
        mock_put_response.json.return_value = mock_project

        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_login_response
        mock_session_instance.get.return_value = mock_get_response
        mock_session_instance.put.return_value = mock_put_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # Test setting quotas with various values
        quotas = {"cpu": 4.5, "memory": 8.25}
        client.set_project_quotas("project-123", quotas)

        # Check the PUT call arguments
        put_call_args = mock_session_instance.put.call_args
        updated_project = put_call_args[1]["json"]

        # Verify correct format conversion
        resource_quota = updated_project["resourceQuota"]["limit"]
        assert resource_quota["limitsCpu"] == "4500m"  # 4.5 * 1000
        assert resource_quota["limitsMemory"] == "8448Mi"  # 8.25 * 1024

        # Verify both quota fields are set
        assert "resourceQuota" in updated_project
        assert "namespaceDefaultResourceQuota" in updated_project

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_retrieval(self, mock_session, rancher_settings):
        """Test retrieving quotas from project object."""
        # Mock project with quotas
        mock_project_with_quotas = {
            "id": "project-123",
            "name": "test-project",
            "resourceQuota": {"limit": {"limitsCpu": "6000m", "limitsMemory": "12288Mi"}},
        }

        mock_response = MagicMock()
        mock_response.json.return_value = mock_project_with_quotas

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = MagicMock()  # For login
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        quotas = client.get_project_quotas("project-123")

        # Verify correct format conversion back to Waldur format
        assert quotas["cpu"] == 6.0  # 6000m / 1000
        assert quotas["memory"] == 12.0  # 12288Mi / 1024

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_retrieval_empty(self, mock_session, rancher_settings):
        """Test retrieving quotas from project without quotas set."""
        # Mock project without quotas
        mock_project_no_quotas = {"id": "project-123", "name": "test-project"}

        mock_response = MagicMock()
        mock_response.json.return_value = mock_project_no_quotas

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = MagicMock()  # For login
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        quotas = client.get_project_quotas("project-123")

        # Should return empty dict
        assert quotas == {}

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_update_workflow(self, mock_session, rancher_settings):
        """Test complete quota update workflow."""
        # Mock project object
        initial_project = {
            "id": "project-123",
            "name": "test-project",
            "type": "project",
            "clusterId": "c-test",
        }

        updated_project = initial_project.copy()
        updated_project["resourceQuota"] = {
            "limit": {"limitsCpu": "4000m", "limitsMemory": "8192Mi"}
        }

        mock_get_response = MagicMock()
        mock_get_response.json.return_value = initial_project

        mock_put_response = MagicMock()
        mock_put_response.json.return_value = updated_project

        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_login_response
        mock_session_instance.get.return_value = mock_get_response
        mock_session_instance.put.return_value = mock_put_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # Set quotas
        quotas = {"cpu": 4, "memory": 8}
        client.set_project_quotas("project-123", quotas)

        # Verify the correct PUT call was made
        put_call_args = mock_session_instance.put.call_args
        url = put_call_args[0][0]  # First positional argument (URL)
        data = put_call_args[1]["json"]

        # Should use _replace=true parameter
        assert "?_replace=true" in url

        # Should have both quota fields
        assert "resourceQuota" in data
        assert "namespaceDefaultResourceQuota" in data

        # Should have correct format
        assert data["resourceQuota"]["limit"]["limitsCpu"] == "4000m"
        assert data["resourceQuota"]["limit"]["limitsMemory"] == "8192Mi"

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_conversion_edge_cases(self, mock_session, rancher_settings):
        """Test quota conversion with edge cases."""
        mock_project = {"id": "project-123", "type": "project"}

        mock_get_response = MagicMock()
        mock_get_response.json.return_value = mock_project

        mock_put_response = MagicMock()
        mock_put_response.json.return_value = mock_project

        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_login_response
        mock_session_instance.get.return_value = mock_get_response
        mock_session_instance.put.return_value = mock_put_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # Test with fractional values
        quotas = {"cpu": 0.5, "memory": 1.5}
        client.set_project_quotas("project-123", quotas)

        put_call_args = mock_session_instance.put.call_args
        data = put_call_args[1]["json"]

        # Verify fractional conversion
        assert data["resourceQuota"]["limit"]["limitsCpu"] == "500m"  # 0.5 * 1000
        assert data["resourceQuota"]["limit"]["limitsMemory"] == "1536Mi"  # 1.5 * 1024

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_quota_parsing_different_units(self, mock_session, rancher_settings):
        """Test parsing quotas with different unit formats."""
        # Test different memory units
        test_cases = [
            {"limitsMemory": "8192Mi", "expected": 8.0},
            {"limitsMemory": "8Gi", "expected": 8.0},
            {"limitsMemory": "8192", "expected": 8192.0},  # No unit
        ]

        for test_case in test_cases:
            mock_project = {
                "id": "project-123",
                "resourceQuota": {"limit": {"limitsCpu": "4000m", **test_case}},
            }

            mock_response = MagicMock()
            mock_response.json.return_value = mock_project

            mock_session_instance = MagicMock()
            mock_session_instance.post.return_value = MagicMock()  # For login
            mock_session_instance.get.return_value = mock_response
            mock_session.return_value = mock_session_instance

            client = RancherClient(rancher_settings)
            quotas = client.get_project_quotas("project-123")

            expected_memory = test_case["expected"]
            assert quotas["memory"] == expected_memory, f"Failed for {test_case}"
            assert quotas["cpu"] == 4.0  # 4000m / 1000
