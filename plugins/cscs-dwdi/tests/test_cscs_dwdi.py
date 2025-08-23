"""Tests for CSCS-DWDI backend."""

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent_cscs_dwdi.backend import CSCSDWDIBackend
from waldur_site_agent_cscs_dwdi.client import CSCSDWDIClient


class TestCSCSDWDIClient:
    """Tests for CSCS-DWDI API client."""

    def test_client_initialization(self) -> None:
        """Test client initializes with correct parameters."""
        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
        )

        assert client.api_url == "https://api.example.com"
        assert client.client_id == "test_client"
        assert client.client_secret == "test_secret"
        assert client.oidc_token_url is None
        assert client.oidc_scope == "openid"

    def test_client_strips_trailing_slash(self) -> None:
        """Test client strips trailing slash from API URL."""
        client = CSCSDWDIClient(
            api_url="https://api.example.com/",
            client_id="test_client",
            client_secret="test_secret",
        )

        assert client.api_url == "https://api.example.com"

    @patch("httpx.Client.post")
    @patch("httpx.Client.get")
    def test_get_usage_for_month(self, mock_get: Any, mock_post: Any) -> None:
        """Test fetching monthly usage data."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "compute": [
                {
                    "account": "test_account",
                    "accountNodeHours": 100.0,
                    "users": [],
                }
            ]
        }
        mock_get.return_value = mock_response

        # Mock OIDC token response
        mock_token_response = MagicMock()
        mock_token_response.status_code = 200
        mock_token_response.json.return_value = {
            "access_token": "test_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_token_response

        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
        )

        # Test the method
        result = client.get_usage_for_month(
            accounts=["test_account"],
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 31),
        )

        # Verify the call
        mock_get.assert_called_once()
        call_args = mock_get.call_args

        # Check URL
        assert call_args[0][0] == "https://api.example.com/api/v1/compute/usage-month-multiaccount"

        # Check params
        assert call_args.kwargs["params"]["from"] == "2025-01"
        assert call_args.kwargs["params"]["to"] == "2025-01"
        assert call_args.kwargs["params"]["account"] == ["test_account"]

        # Check result
        assert result["compute"][0]["account"] == "test_account"

    @patch("httpx.Client.post")
    @patch("httpx.Client.get")
    def test_ping(self, mock_get: Any, mock_post: Any) -> None:
        """Test ping functionality."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Mock OIDC token response
        mock_token_response = MagicMock()
        mock_token_response.status_code = 200
        mock_token_response.json.return_value = {
            "access_token": "test_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_token_response

        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
        )

        assert client.ping() is True

    @patch("httpx.Client.post")
    @patch("httpx.Client.get")
    def test_ping_failure(self, mock_get: Any, mock_post: Any) -> None:
        """Test ping returns False on failure."""
        mock_get.side_effect = Exception("Connection error")

        # Mock OIDC token response
        mock_token_response = MagicMock()
        mock_token_response.status_code = 200
        mock_token_response.json.return_value = {
            "access_token": "test_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_token_response

        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
        )

        assert client.ping() is False

    def test_client_with_oidc_config(self) -> None:
        """Test client initializes with OIDC configuration."""
        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
            oidc_scope="custom:scope",
        )

        assert client.oidc_token_url == "https://oidc.example.com/token"
        assert client.oidc_scope == "custom:scope"

    @patch("httpx.Client.post")
    def test_oidc_token_acquisition(self, mock_post: Any) -> None:
        """Test actual OIDC token acquisition."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "real_token_12345",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_response

        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
            oidc_scope="cscs-dwdi:read",
        )

        token = client._get_auth_token()

        # Verify the call was made correctly
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0] == "https://oidc.example.com/token"

        # Check data
        expected_data = {
            "grant_type": "client_credentials",
            "client_id": "test_client",
            "client_secret": "test_secret",
            "scope": "cscs-dwdi:read",
        }
        assert call_args.kwargs["data"] == expected_data

        # Check headers
        assert call_args.kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"

        # Check result
        assert token == "real_token_12345"
        assert client._token == "real_token_12345"
        assert client._token_expires_at is not None

    @patch("httpx.Client.post")
    def test_oidc_token_caching(self, mock_post: Any) -> None:
        """Test that tokens are cached and reused."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "cached_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        client = CSCSDWDIClient(
            api_url="https://api.example.com",
            client_id="test_client",
            client_secret="test_secret",
            oidc_token_url="https://oidc.example.com/token",
        )

        # First call should make HTTP request
        token1 = client._get_auth_token()
        assert mock_post.call_count == 1

        # Second call should use cached token
        token2 = client._get_auth_token()
        assert mock_post.call_count == 1  # No additional call
        assert token1 == token2


class TestCSCSDWDIBackend:
    """Tests for CSCS-DWDI backend."""

    def test_backend_initialization(self) -> None:
        """Test backend initializes with correct configuration."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }

        backend = CSCSDWDIBackend(backend_settings, backend_components)

        assert backend.api_url == "https://api.example.com"
        assert backend.client_id == "test_client"
        assert backend.client_secret == "test_secret"
        assert isinstance(backend.cscs_client, CSCSDWDIClient)

    def test_backend_initialization_missing_config(self) -> None:
        """Test backend raises error when configuration is missing."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            # Missing client_id and client_secret
        }
        backend_components: dict[str, dict] = {}

        with pytest.raises(ValueError) as exc_info:
            CSCSDWDIBackend(backend_settings, backend_components)

        assert "cscs_dwdi_oidc_token_url" in str(exc_info.value)

    def test_process_api_response(self) -> None:
        """Test processing of API response into Waldur format."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }
        backend = CSCSDWDIBackend(backend_settings, backend_components)

        # Sample API response
        api_response = {
            "compute": [
                {
                    "account": "account1",
                    "accountNodeHours": 1500.75,
                    "users": [
                        {
                            "username": "user1",
                            "nodeHours": 500.25,
                        },
                        {
                            "username": "user2",
                            "nodeHours": 1000.50,
                        },
                    ],
                },
                {
                    "account": "account2",
                    "accountNodeHours": 750.0,
                    "users": [
                        {
                            "username": "user3",
                            "nodeHours": 750.0,
                        },
                    ],
                },
            ]
        }

        result = backend._process_api_response(api_response)

        # Verify account1
        assert "account1" in result
        assert result["account1"]["TOTAL_ACCOUNT_USAGE"]["nodeHours"] == 1500.75
        assert result["account1"]["user1"]["nodeHours"] == 500.25
        assert result["account1"]["user2"]["nodeHours"] == 1000.50

        # Verify account2
        assert "account2" in result
        assert result["account2"]["TOTAL_ACCOUNT_USAGE"]["nodeHours"] == 750.0
        assert result["account2"]["user3"]["nodeHours"] == 750.0

    def test_process_api_response_aggregates_users(self) -> None:
        """Test that user usage is aggregated across multiple entries."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }
        backend = CSCSDWDIBackend(backend_settings, backend_components)

        # API response with same user appearing multiple times
        api_response = {
            "compute": [
                {
                    "account": "account1",
                    "accountNodeHours": 300.0,
                    "users": [
                        {
                            "username": "user1",
                            "nodeHours": 100.0,
                            "usageDate": "2025-01-01",
                        },
                        {
                            "username": "user1",
                            "nodeHours": 50.0,
                            "usageDate": "2025-01-02",
                        },
                        {
                            "username": "user2",
                            "nodeHours": 150.0,
                        },
                    ],
                }
            ]
        }

        result = backend._process_api_response(api_response)

        # User1's usage should be aggregated
        assert result["account1"]["user1"]["nodeHours"] == 150.0
        assert result["account1"]["user2"]["nodeHours"] == 150.0

    @patch.object(CSCSDWDIClient, "get_usage_for_month")
    def test_get_usage_report(self, mock_get_usage: Any) -> None:
        """Test getting usage report for specified accounts."""
        # Mock API response
        mock_get_usage.return_value = {
            "compute": [
                {
                    "account": "account1",
                    "accountNodeHours": 100.0,
                    "users": [
                        {"username": "user1", "nodeHours": 100.0},
                    ],
                },
                {
                    "account": "account2",
                    "accountNodeHours": 200.0,
                    "users": [],
                },
            ]
        }

        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }
        backend = CSCSDWDIBackend(backend_settings, backend_components)

        # Test the method
        result = backend._get_usage_report(["account1", "account2"])

        # Verify results
        assert "account1" in result
        assert "account2" in result
        assert result["account1"]["TOTAL_ACCOUNT_USAGE"]["nodeHours"] == 100.0
        assert result["account2"]["TOTAL_ACCOUNT_USAGE"]["nodeHours"] == 200.0

        # Verify API was called with correct parameters
        mock_get_usage.assert_called_once()
        call_args = mock_get_usage.call_args
        assert call_args.kwargs["accounts"] == ["account1", "account2"]

    def test_get_usage_report_filters_accounts(self) -> None:
        """Test that usage report only includes requested accounts."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }
        backend = CSCSDWDIBackend(backend_settings, backend_components)

        # Mock the client method
        with patch.object(backend.cscs_client, "get_usage_for_month") as mock_get:
            mock_get.return_value = {
                "compute": [
                    {
                        "account": "account1",
                        "accountNodeHours": 100.0,
                        "users": [],
                    },
                    {
                        "account": "account2",
                        "accountNodeHours": 200.0,
                        "users": [],
                    },
                    {
                        "account": "account3",
                        "accountNodeHours": 300.0,
                        "users": [],
                    },
                ]
            }

            # Request only account1 and account3
            result = backend._get_usage_report(["account1", "account3"])

            # Should only include requested accounts
            assert "account1" in result
            assert "account2" not in result
            assert "account3" in result

    def test_not_implemented_methods(self) -> None:
        """Test that non-reporting methods raise NotImplementedError."""
        backend_settings = {
            "cscs_dwdi_api_url": "https://api.example.com",
            "cscs_dwdi_client_id": "test_client",
            "cscs_dwdi_client_secret": "test_secret",
            "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
        }
        backend_components = {
            "nodeHours": {
                "measured_unit": "node-hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
            }
        }
        backend = CSCSDWDIBackend(backend_settings, backend_components)

        with pytest.raises(NotImplementedError):
            backend.create_account({})

        with pytest.raises(NotImplementedError):
            backend.delete_account("test")

        with pytest.raises(NotImplementedError):
            backend.get_account("test")

        with pytest.raises(NotImplementedError):
            backend.list_accounts()

        with pytest.raises(NotImplementedError):
            backend.add_account_users("test", ["user1"])

        with pytest.raises(NotImplementedError):
            backend.delete_account_users("test", ["user1"])

        with pytest.raises(NotImplementedError):
            backend.update_account_limit_deposit("test", "cpu", 100, {})

        with pytest.raises(NotImplementedError):
            backend.reset_account_limit_deposit("test", "cpu", {})

        with pytest.raises(NotImplementedError):
            backend.set_resource_limits("test", {"cpu": 100})
