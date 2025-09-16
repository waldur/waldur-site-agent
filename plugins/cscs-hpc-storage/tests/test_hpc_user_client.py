"""Tests for CSCS HPC User API client."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import httpx
import pytest

from waldur_site_agent_cscs_hpc_storage.hpc_user_client import CSCSHpcUserClient


class TestCSCSHpcUserClient:
    """Test cases for CSCS HPC User client."""

    @pytest.fixture
    def client_config(self):
        """Basic client configuration."""
        return {
            "api_url": "https://api-user.hpc-user.example.com",
            "client_id": "test_client",
            "client_secret": "test_secret",
            "oidc_token_url": "https://auth.example.com/token",
            "oidc_scope": "openid profile",
        }

    @pytest.fixture
    def hpc_user_client(self, client_config):
        """Create HPC User client instance."""
        return CSCSHpcUserClient(**client_config)

    def test_init(self, client_config):
        """Test client initialization."""
        client = CSCSHpcUserClient(**client_config)

        assert client.api_url == "https://api-user.hpc-user.example.com"
        assert client.client_id == "test_client"
        assert client.client_secret == "test_secret"
        assert client.oidc_token_url == "https://auth.example.com/token"
        assert client.oidc_scope == "openid profile"
        assert client._token is None
        assert client._token_expires_at is None

    def test_init_strips_trailing_slash(self):
        """Test that API URL trailing slashes are stripped."""
        client = CSCSHpcUserClient(
            api_url="https://api-user.hpc-user.example.com/",
            client_id="test_client",
            client_secret="test_secret",
        )
        assert client.api_url == "https://api-user.hpc-user.example.com"

    def test_init_default_scope(self):
        """Test default OIDC scope is set."""
        client = CSCSHpcUserClient(
            api_url="https://api-user.hpc-user.example.com",
            client_id="test_client",
            client_secret="test_secret",
        )
        assert client.oidc_scope == "openid"

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_acquire_oidc_token_success(self, mock_client_class, hpc_user_client):
        """Test successful OIDC token acquisition."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.json.return_value = {
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        # Test token acquisition
        token = hpc_user_client._acquire_oidc_token()

        assert token == "test_access_token"
        assert hpc_user_client._token == "test_access_token"
        assert hpc_user_client._token_expires_at is not None

        # Verify HTTP request
        mock_client_instance.post.assert_called_once_with(
            "https://auth.example.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "scope": "openid profile",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_acquire_oidc_token_no_access_token(self, mock_client_class, hpc_user_client):
        """Test OIDC token acquisition when no access_token in response."""
        mock_response = Mock()
        mock_response.json.return_value = {"token_type": "Bearer"}
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        with pytest.raises(ValueError, match="No access_token in OIDC response"):
            hpc_user_client._acquire_oidc_token()

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_acquire_oidc_token_http_error(self, mock_client_class, hpc_user_client):
        """Test OIDC token acquisition HTTP error handling."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized", request=Mock(), response=Mock()
        )

        mock_client_instance = Mock()
        mock_client_instance.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        with pytest.raises(httpx.HTTPStatusError):
            hpc_user_client._acquire_oidc_token()

    def test_get_auth_token_no_oidc_url(self):
        """Test auth token acquisition when OIDC URL not configured."""
        client = CSCSHpcUserClient(
            api_url="https://api-user.hpc-user.example.com",
            client_id="test_client",
            client_secret="test_secret",
        )

        with pytest.raises(ValueError, match="hpc_user_oidc_token_url not configured"):
            client._get_auth_token()

    def test_get_auth_token_cached_valid(self, hpc_user_client):
        """Test auth token returns cached token when still valid."""
        from datetime import timedelta

        # Set up cached token that expires in the future
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "cached_token"
        hpc_user_client._token_expires_at = future_time

        token = hpc_user_client._get_auth_token()
        assert token == "cached_token"

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_get_projects_success(self, mock_client_class, hpc_user_client):
        """Test successful project data retrieval."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {
                    "posixName": "project1",
                    "unixGid": 30001,
                    "displayName": "Test Project 1",
                },
                {
                    "posixName": "project2",
                    "unixGid": 30002,
                    "displayName": "Test Project 2",
                },
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        # Test projects retrieval
        result = hpc_user_client.get_projects(["project1", "project2"])

        assert len(result) == 2
        assert result[0]["posixName"] == "project1"
        assert result[0]["unixGid"] == 30001
        assert result[1]["posixName"] == "project2"
        assert result[1]["unixGid"] == 30002

        # Verify HTTP request
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            params={"projects": ["project1", "project2"]},
            headers={"Authorization": "Bearer test_token"},
        )

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_get_projects_empty_list(self, mock_client_class, hpc_user_client):
        """Test project retrieval with empty project list."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {"projects": []}
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.get_projects([])

        assert result == []

        # Verify no projects parameter when empty list
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            params={},
            headers={"Authorization": "Bearer test_token"},
        )

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_get_project_unix_gid_found(self, mock_client_class, hpc_user_client):
        """Test successful unixGid lookup for existing project."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "projects": [
                {
                    "posixName": "project1",
                    "unixGid": 30001,
                    "displayName": "Test Project 1",
                }
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.get_project_unix_gid("project1")

        assert result == 30001

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_get_project_unix_gid_not_found(self, mock_client_class, hpc_user_client):
        """Test unixGid lookup for non-existent project."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response with different project
        mock_response = Mock()
        mock_response.json.return_value = {
            "response": [
                {
                    "posixName": "other_project",
                    "unixGid": 30099,
                    "displayName": "Other Project",
                }
            ]
        }
        mock_response.raise_for_status.return_value = None

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.get_project_unix_gid("project1")

        assert result is None

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_get_project_unix_gid_api_error(self, mock_client_class, hpc_user_client):
        """Test unixGid lookup when API request fails."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=Mock(), response=Mock()
        )

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.get_project_unix_gid("project1")

        assert result is None

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_ping_success(self, mock_client_class, hpc_user_client):
        """Test successful ping to HPC User API."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.ping()

        assert result is True

        # Verify ping request
        mock_client_instance.get.assert_called_once_with(
            "https://api-user.hpc-user.example.com/api/v1/export/waldur/projects",
            headers={"Authorization": "Bearer test_token"},
        )

    @patch("waldur_site_agent_cscs_hpc_storage.hpc_user_client.httpx.Client")
    def test_ping_failure(self, mock_client_class, hpc_user_client):
        """Test ping failure when API is not accessible."""
        from datetime import timedelta

        # Mock token acquisition
        future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        hpc_user_client._token = "test_token"
        hpc_user_client._token_expires_at = future_time

        # Mock API response with non-200 status
        mock_response = Mock()
        mock_response.status_code = 503

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client_instance

        result = hpc_user_client.ping()

        assert result is False

    def test_ping_exception(self, hpc_user_client):
        """Test ping handles exceptions gracefully."""
        # Don't mock anything to trigger exception in _get_auth_token
        result = hpc_user_client.ping()

        assert result is False
