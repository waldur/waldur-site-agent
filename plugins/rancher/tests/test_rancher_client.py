"""Tests for Rancher client implementation."""

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


class TestRancherClient:
    """Test cases for RancherClient."""

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_initialization(self, mock_session, rancher_settings):
        """Test client initialization."""
        client = RancherClient(rancher_settings)

        assert client.api_url == "https://rancher.example.com/v3"
        assert client.access_key == "test-access-key"
        assert client.secret_key == "test-secret-key"
        assert client.cluster_id == "c-m-test:p-test"
        assert client.verify_cert is False
        assert client.project_prefix == "waldur-"

        mock_session.assert_called_once()

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_ping_success(self, mock_session, rancher_settings):
        """Test successful ping operation."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "c-m-test:p-test", "name": "test-cluster"}

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        assert client.ping() is True
        mock_session_instance.get.assert_called()

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_ping_failure(self, mock_session, rancher_settings):
        """Test ping failure."""
        mock_session_instance = MagicMock()
        mock_session_instance.get.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        assert client.ping() is False

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_list_projects(self, mock_session, rancher_settings):
        """Test project listing."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "project-123",
                    "name": "waldur-test-project",
                    "description": "Test project",
                    "annotations": {"waldur/organization": "test-org"},
                },
                {
                    "id": "project-456",
                    "name": "other-project",
                    "description": "Non-waldur project",
                    "annotations": {},
                },
            ]
        }

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        projects = client.list_projects()

        # Should only return projects with waldur prefix
        assert len(projects) == 1
        assert projects[0].name == "waldur-test-project"
        assert projects[0].backend_id == "project-123"
        assert projects[0].organization == "test-org"

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_get_project(self, mock_session, rancher_settings):
        """Test getting a specific project."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "project-123",
            "name": "waldur-test-project",
            "description": "Test project",
            "annotations": {"waldur/organization": "test-org"},
        }

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        project = client.get_project("project-123")

        assert project is not None
        assert project.name == "waldur-test-project"
        assert project.backend_id == "project-123"
        assert project.organization == "test-org"

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_create_project(self, mock_session, rancher_settings):
        """Test project creation."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "project-123", "name": "waldur-test-project"}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        project_id = client.create_project(
            name="waldur-test-project", description="Test project", organization="test-org"
        )

        assert project_id == "project-123"

        # Check that the correct data was posted
        call_args = mock_session_instance.post.call_args
        posted_data = call_args[1]["json"]

        assert posted_data["type"] == "project"
        assert posted_data["clusterId"] == "c-m-test:p-test"
        assert posted_data["name"] == "waldur-test-project"
        assert posted_data["description"] == "Test project"
        assert posted_data["annotations"]["waldur/organization"] == "test-org"
        assert posted_data["annotations"]["waldur/managed"] == "true"

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_delete_project(self, mock_session, rancher_settings):
        """Test project deletion."""
        mock_response = MagicMock()

        mock_session_instance = MagicMock()
        mock_session_instance.delete.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        client.delete_project("project-123")

        mock_session_instance.delete.assert_called()

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_get_project_quotas(self, mock_session, rancher_settings):
        """Test getting project quotas from project object."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "project-123",
            "name": "test-project",
            "resourceQuota": {"limit": {"limitsCpu": "4000m", "limitsMemory": "8192Mi"}},
        }

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response  # For login
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        quotas = client.get_project_quotas("project-123")

        assert quotas["cpu"] == 4.0
        assert quotas["memory"] == 8.0

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_set_project_quotas(self, mock_session, rancher_settings):
        """Test setting project quotas by updating project object."""
        # Mock project object response
        mock_project = {
            "id": "project-123",
            "name": "test-project",
            "clusterId": "c-test",
            "type": "project",
        }

        mock_get_response = MagicMock()
        mock_get_response.json.return_value = mock_project

        mock_put_response = MagicMock()
        mock_put_response.json.return_value = mock_project

        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_login_response  # For login
        mock_session_instance.get.return_value = mock_get_response
        mock_session_instance.put.return_value = mock_put_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        client.set_project_quotas("project-123", {"cpu": 4, "memory": 8})

        # Check that project was updated with correct quota data
        put_call_args = mock_session_instance.put.call_args
        updated_project = put_call_args[1]["json"]

        assert "resourceQuota" in updated_project
        assert updated_project["resourceQuota"]["limit"]["limitsCpu"] == "4000m"
        assert updated_project["resourceQuota"]["limit"]["limitsMemory"] == "8192Mi"
        assert "namespaceDefaultResourceQuota" in updated_project

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_get_project_usage(self, mock_session, rancher_settings):
        """Test getting project usage metrics."""

        # Mock different responses for different endpoints
        def mock_get_side_effect(url, **kwargs):
            mock_response = MagicMock()
            if "/projects/project-123/workloads" in url:
                # Mock workloads response
                mock_response.json.return_value = {
                    "data": [
                        {
                            "containers": [
                                {
                                    "resources": {
                                        "requests": {
                                            "cpu": "2500m",  # 2.5 CPU
                                            "memory": "4Gi",  # 4GB
                                        }
                                    }
                                }
                            ],
                            "scale": 10,  # 10 replicas = 10 pods
                        }
                    ]
                }
            elif "/projects/project-123/persistentvolumeclaims" in url:
                # Mock PVC response for storage
                mock_response.json.return_value = {
                    "data": [
                        {
                            "spec": {
                                "resources": {
                                    "requests": {
                                        "storage": "50Gi"  # 50GB storage
                                    }
                                }
                            }
                        }
                    ]
                }
            else:
                mock_response.json.return_value = {}
            return mock_response

        # Mock login response
        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.get.side_effect = mock_get_side_effect
        mock_session_instance.post.return_value = mock_login_response  # For login
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        usage = client.get_project_usage("project-123")

        assert usage["cpu"] == 2.5
        assert usage["memory"] == 4.0  # Converted to GB
        assert usage["storage"] == 50.0  # Converted to GB
        assert usage["pods"] == 10

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_list_project_users(self, mock_session, rancher_settings):
        """Test listing project users."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"userId": "user1", "roleTemplateId": "project-member"},
                {"userId": "user2", "roleTemplateId": "project-owner"},
            ]
        }

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        users = client.list_project_users("project-123")

        assert "user1" in users
        assert "user2" in users
        assert len(users) == 2

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_add_project_user(self, mock_session, rancher_settings):
        """Test adding user to project."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "binding-123"}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        client.add_project_user("project-123", "testuser", "project-member")

        # Check that binding was created with correct data
        call_args = mock_session_instance.post.call_args
        posted_data = call_args[1]["json"]

        assert posted_data["type"] == "projectRoleTemplateBinding"
        assert posted_data["projectId"] == "project-123"
        assert posted_data["userId"] == "testuser"
        assert posted_data["roleTemplateId"] == "project-member"

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_remove_project_user(self, mock_session, rancher_settings):
        """Test removing user from project."""
        # First call (get bindings) returns user binding
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {
            "data": [
                {"id": "binding-123", "userId": "testuser", "roleTemplateId": "project-member"}
            ]
        }

        # Second call (delete binding) returns success
        mock_delete_response = MagicMock()

        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_get_response
        mock_session_instance.delete.return_value = mock_delete_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)
        client.remove_project_user("project-123", "testuser")

        mock_session_instance.delete.assert_called()

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_request_failure_handling(self, mock_session, rancher_settings):
        """Test handling of request failures."""
        mock_session_instance = MagicMock()
        mock_session_instance.get.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # get_project handles errors gracefully and returns None instead of raising
        result = client.get_project("nonexistent-project")
        assert result is None

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_ssl_verification_disabled(self, mock_session, rancher_settings):
        """Test SSL verification is properly disabled."""
        rancher_settings["verify_cert"] = False

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = MagicMock()  # For login
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # Check that session.verify is set to False
        assert client.session.verify is False

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_login_method(self, mock_session, rancher_settings):
        """Test login method authentication."""
        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.post.return_value = mock_login_response
        mock_session.return_value = mock_session_instance

        client = RancherClient(rancher_settings)

        # Verify login was called during initialization
        mock_session_instance.post.assert_called()

        # Verify auth is set on session
        assert client.session.auth == (client.access_key, client.secret_key)

    @patch("waldur_site_agent_rancher.rancher_client.requests.Session")
    def test_login_failure_handling(self, mock_session, rancher_settings):
        """Test login failure handling."""
        mock_session_instance = MagicMock()
        mock_session_instance.post.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
        mock_session.return_value = mock_session_instance

        # Should not raise exception, just log warning
        client = RancherClient(rancher_settings)

        # Client should still be created
        assert client.access_key == rancher_settings["username"]
