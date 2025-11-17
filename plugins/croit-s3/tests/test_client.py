"""Tests for CroitS3Client."""

import json
import pytest
import requests
from unittest.mock import Mock, patch

from waldur_site_agent_croit_s3.client import CroitS3Client
from waldur_site_agent_croit_s3.exceptions import (
    CroitS3APIError,
    CroitS3AuthenticationError,
    CroitS3UserExistsError,
    CroitS3UserNotFoundError,
)


class TestCroitS3Client:
    """Test CroitS3Client functionality."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return CroitS3Client(
            api_url="https://test.example.com",
            username="test_user",
            password="test_pass",
            verify_ssl=False,
        )

    @pytest.fixture
    def mock_response(self):
        """Create mock response."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {}
        response.text = ""
        response.content = b""
        response.raise_for_status.return_value = None
        return response

    def test_client_initialization(self):
        """Test client initialization."""
        client = CroitS3Client(
            api_url="https://api.croit.io/",
            username="admin",
            password="secret",
            verify_ssl=True,
            timeout=60,
        )

        assert client.api_url == "https://api.croit.io/api"
        assert client.username == "admin"
        assert client.password == "secret"
        assert client.verify_ssl is True
        assert client.timeout == 60

    @patch("requests.Session.request")
    def test_ping_success(self, mock_request, client, mock_response):
        """Test successful ping."""
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_request.return_value = mock_response

        result = client.ping()

        assert result is True
        mock_request.assert_called_once()

    @patch("requests.Session.request")
    def test_ping_failure(self, mock_request, client):
        """Test ping failure."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        result = client.ping()

        assert result is False

    @patch("requests.Session.request")
    def test_ping_with_exception(self, mock_request, client):
        """Test ping raises exception when requested."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        with pytest.raises(Exception):
            client.ping(raise_exception=True)

    @patch("requests.Session.request")
    def test_create_user_success(self, mock_request, client, mock_response):
        """Test successful user creation."""
        mock_response.status_code = 201
        mock_response.json.return_value = {"uid": "test_user", "name": "Test User"}
        mock_response.content = b'{"uid": "test_user", "name": "Test User"}'
        mock_request.return_value = mock_response

        result = client.create_user(
            uid="test_user", name="Test User", email="test@example.com"
        )

        assert result == {"uid": "test_user", "name": "Test User"}
        mock_request.assert_called_once_with(
            method="POST",
            url="https://test.example.com/api/s3/users",
            json={"uid": "test_user", "name": "Test User", "email": "test@example.com"},
            params=None,
            timeout=30,
        )

    @patch("requests.Session.request")
    def test_create_user_exists(self, mock_request, client):
        """Test user creation when user already exists."""
        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "User already exists"
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3UserExistsError):
            client.create_user(uid="existing_user", name="Existing User")

    @patch("requests.Session.request")
    def test_delete_user_success(self, mock_request, client, mock_response):
        """Test successful user deletion."""
        mock_response.status_code = 204
        mock_request.return_value = mock_response

        result = client.delete_user("test_user")

        assert result is True
        mock_request.assert_called_once_with(
            method="DELETE",
            url="https://test.example.com/api/s3/users/test_user",
            json=None,
            params=None,
            timeout=30,
        )

    @patch("requests.Session.request")
    def test_delete_user_not_found(self, mock_request, client):
        """Test user deletion when user not found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "User not found"
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3UserNotFoundError):
            client.delete_user("nonexistent_user")

    @patch("requests.Session.request")
    def test_get_user_info_success(self, mock_request, client, mock_response):
        """Test successful user info retrieval."""
        mock_response.json.return_value = [
            {
                "uid": "test_user",
                "name": "Test User",
                "email": "test@example.com",
                "suspended": False,
            }
        ]
        mock_request.return_value = mock_response

        result = client.get_user_info("test_user")

        assert result["uid"] == "test_user"
        assert result["name"] == "Test User"
        assert result["email"] == "test@example.com"
        assert result["suspended"] is False

    @patch("requests.Session.request")
    def test_get_user_info_not_found(self, mock_request, client, mock_response):
        """Test user info retrieval when user not found."""
        mock_response.json.return_value = {"data": []}
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3UserNotFoundError):
            client.get_user_info("nonexistent_user")

    @patch("requests.Session.request")
    def test_get_user_keys(self, mock_request, client, mock_response):
        """Test user keys retrieval."""
        mock_response.json.return_value = {
            "user": "test_user",
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }
        mock_request.return_value = mock_response

        result = client.get_user_keys("test_user")

        assert result["access_key"] == "AKIAIOSFODNN7EXAMPLE"
        assert result["secret_key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    @patch("requests.Session.request")
    def test_get_user_buckets(self, mock_request, client, mock_response):
        """Test user buckets retrieval."""
        mock_response.json.return_value = [
            {
                "bucket": "test-bucket-1",
                "owner": "test_user",
                "usageSum": {"size": 1024000, "numObjects": 10},
            },
            {
                "bucket": "test-bucket-2",
                "owner": "test_user",
                "usageSum": {"size": 2048000, "numObjects": 20},
            },
        ]
        mock_request.return_value = mock_response

        result = client.get_user_buckets("test_user")

        assert len(result) == 2
        assert result[0]["bucket"] == "test-bucket-1"
        assert result[0]["usageSum"]["size"] == 1024000
        assert result[1]["bucket"] == "test-bucket-2"
        assert result[1]["usageSum"]["numObjects"] == 20

    @patch("requests.Session.request")
    def test_set_user_bucket_quota(self, mock_request, client, mock_response):
        """Test setting user bucket quota."""
        mock_response.status_code = 204
        mock_request.return_value = mock_response

        quota = {"enabled": True, "maxSize": 10737418240, "maxObjects": 1000}
        client.set_user_bucket_quota("test_user", quota)

        mock_request.assert_called_once_with(
            method="PUT",
            url="https://test.example.com/api/s3/users/test_user/bucket-quota",
            json=quota,
            params=None,
            timeout=30,
        )

    @patch("requests.Session.request")
    def test_authentication_error(self, mock_request, client):
        """Test authentication error handling."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3AuthenticationError):
            client.list_users()

    @patch("requests.Session.request")
    def test_api_error(self, mock_request, client):
        """Test general API error handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3APIError):
            client.list_users()

    @patch("requests.Session.request")
    def test_timeout_error(self, mock_request, client):
        """Test timeout error handling."""
        mock_request.side_effect = requests.exceptions.Timeout()

        with pytest.raises(CroitS3APIError, match="Request timeout"):
            client.list_users()

    @patch("requests.Session.request")
    def test_connection_error(self, mock_request, client):
        """Test connection error handling."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        with pytest.raises(CroitS3APIError, match="Connection error"):
            client.list_users()
