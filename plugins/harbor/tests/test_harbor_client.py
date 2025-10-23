"""Tests for Harbor client."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from waldur_site_agent_harbor.client import HarborClient
from waldur_site_agent_harbor.exceptions import (
    HarborAPIError,
    HarborAuthenticationError,
    HarborProjectError,
    HarborQuotaError,
    HarborOIDCError,
)


@pytest.fixture
def harbor_client():
    """Create a Harbor client instance for testing."""
    return HarborClient(
        harbor_url="https://registry.example.com",
        robot_username="robot$test",
        robot_password="test-password",
    )


@pytest.fixture
def mock_response():
    """Create a mock response object."""
    response = Mock(spec=requests.Response)
    response.status_code = 200
    response.headers = {}
    response.content = b'{"test": "data"}'
    response.json.return_value = {"test": "data"}
    return response


class TestHarborClient:
    """Test Harbor client functionality."""

    def test_client_initialization(self):
        """Test client initialization with credentials."""
        client = HarborClient(
            harbor_url="https://registry.example.com/",
            robot_username="robot$test",
            robot_password="test-password",
        )

        assert client.harbor_url == "https://registry.example.com"
        assert client.api_base == "https://registry.example.com/api/v2.0"
        assert client.auth == ("robot$test", "test-password")
        assert client.headers["Content-Type"] == "application/json"

    def test_ping_success(self, harbor_client, mock_response):
        """Test successful ping operation."""
        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            result = harbor_client.ping()
            assert result is True

    def test_ping_failure(self, harbor_client):
        """Test failed ping operation."""
        with patch.object(
            harbor_client,
            "_make_request",
            side_effect=HarborAPIError("Connection failed"),
        ):
            result = harbor_client.ping()
            assert result is False

    def test_create_project_success(self, harbor_client, mock_response):
        """Test successful project creation."""
        with (
            patch.object(harbor_client, "get_project", return_value=None),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
            patch.object(harbor_client, "update_project_quota", return_value=True),
        ):
            result = harbor_client.create_project("test-project", 20)
            assert result is True

    def test_create_project_already_exists(self, harbor_client):
        """Test project creation when project already exists."""
        existing_project = {"name": "test-project", "project_id": 1}
        with patch.object(harbor_client, "get_project", return_value=existing_project):
            result = harbor_client.create_project("test-project", 20)
            assert result is False

    def test_delete_project_success(self, harbor_client, mock_response):
        """Test successful project deletion."""
        project = {"name": "test-project", "project_id": 1}
        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
        ):
            result = harbor_client.delete_project("test-project")
            assert result is True

    def test_delete_project_not_found(self, harbor_client):
        """Test project deletion when project doesn't exist."""
        with patch.object(harbor_client, "get_project", return_value=None):
            result = harbor_client.delete_project("test-project")
            assert result is False

    def test_get_project_found(self, harbor_client, mock_response):
        """Test getting existing project."""
        projects = [
            {"name": "other-project", "project_id": 1},
            {"name": "test-project", "project_id": 2},
        ]
        mock_response.json.return_value = projects

        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            result = harbor_client.get_project("test-project")
            assert result is not None
            assert result["name"] == "test-project"
            assert result["project_id"] == 2

    def test_get_project_not_found(self, harbor_client, mock_response):
        """Test getting non-existent project."""
        mock_response.json.return_value = []

        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            result = harbor_client.get_project("test-project")
            assert result is None

    def test_get_project_usage(self, harbor_client, mock_response):
        """Test getting project storage usage."""
        project = {"name": "test-project", "project_id": 1}
        usage_data = {
            "quota": {
                "used": {
                    "storage": 5368709120  # 5 GB in bytes
                }
            },
            "repo_count": 3,
        }
        mock_response.json.return_value = usage_data

        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
        ):
            result = harbor_client.get_project_usage("test-project")
            assert result["storage_bytes"] == 5368709120
            assert result["repository_count"] == 3

    def test_update_project_quota_success(self, harbor_client, mock_response):
        """Test successful quota update."""
        project = {"name": "test-project", "project_id": 1}
        quotas = [{"id": 10, "reference": "project", "reference_id": "1"}]
        mock_response.json.return_value = quotas

        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(
                harbor_client, "_make_request", return_value=mock_response
            ) as mock_request,
        ):
            result = harbor_client.update_project_quota("test-project", 50)
            assert result is True

            # Check that PUT request was made with correct quota
            calls = mock_request.call_args_list
            put_call = [c for c in calls if c[0][0] == "PUT"][0]
            assert put_call[1]["json"]["hard"]["storage"] == 50 * 1024 * 1024 * 1024

    def test_create_user_group_new(self, harbor_client, mock_response):
        """Test creating a new user group."""
        mock_response.json.return_value = []  # No existing groups
        mock_response.headers = {"Location": "/api/v2.0/usergroups/5"}

        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            result = harbor_client.create_user_group("waldur-project1")
            assert result == 5

    def test_create_user_group_existing(self, harbor_client, mock_response):
        """Test creating user group when it already exists."""
        existing_groups = [{"id": 3, "group_name": "waldur-project1"}]
        mock_response.json.return_value = existing_groups

        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            result = harbor_client.create_user_group("waldur-project1")
            assert result == 3

    def test_assign_group_to_project_success(self, harbor_client, mock_response):
        """Test assigning group to project."""
        project = {"name": "test-project", "project_id": 1}
        members = []  # No existing members
        mock_response.json.return_value = members

        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(harbor_client, "create_user_group", return_value=5),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
        ):
            result = harbor_client.assign_group_to_project(
                "waldur-project1", "test-project"
            )
            assert result is True

    def test_assign_group_to_project_already_member(self, harbor_client, mock_response):
        """Test assigning group when already a member."""
        project = {"name": "test-project", "project_id": 1}
        members = [{"id": 1, "entity_name": "waldur-project1"}]
        mock_response.json.return_value = members

        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(harbor_client, "create_user_group", return_value=5),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
        ):
            result = harbor_client.assign_group_to_project(
                "waldur-project1", "test-project"
            )
            assert result is True

    def test_authentication_error(self, harbor_client):
        """Test authentication error handling."""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()

        with patch("requests.request", return_value=mock_response):
            with pytest.raises(HarborAuthenticationError):
                harbor_client._make_request("GET", "/test")

    def test_list_resources(self, harbor_client, mock_response):
        """Test listing resources (projects)."""
        projects = [
            {"name": "project1", "project_id": 1},
            {"name": "project2", "project_id": 2},
        ]
        mock_response.json.return_value = projects

        with patch.object(harbor_client, "_make_request", return_value=mock_response):
            resources = harbor_client.list_resources()
            assert len(resources) == 2
            assert resources[0].name == "project1"
            assert resources[1].name == "project2"

    def test_get_resource_limits(self, harbor_client, mock_response):
        """Test getting resource limits (quotas)."""
        project = {"name": "test-project", "project_id": 1}
        quotas = [
            {
                "id": 1,
                "hard": {"storage": 10737418240},  # 10 GB in bytes
            }
        ]
        mock_response.json.return_value = quotas

        with (
            patch.object(harbor_client, "get_project", return_value=project),
            patch.object(harbor_client, "_make_request", return_value=mock_response),
        ):
            limits = harbor_client.get_resource_limits("test-project")
            assert limits["storage"] == 10  # GB

    def test_set_resource_limits(self, harbor_client):
        """Test setting resource limits."""
        with patch.object(
            harbor_client, "update_project_quota", return_value=True
        ) as mock_update:
            harbor_client.set_resource_limits("test-project", {"storage": 25})
            mock_update.assert_called_once_with("test-project", 25)
