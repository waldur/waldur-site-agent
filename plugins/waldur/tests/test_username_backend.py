"""Tests for WaldurIdentityBridgeUsernameBackend."""

from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest

from waldur_api_client.types import UNSET

from waldur_site_agent_waldur.username_backend import (
    WaldurIdentityBridgeUsernameBackend,
    _extract_attributes,
    _get_waldur_username,
)


@pytest.fixture()
def identity_bridge_settings():
    """Backend settings for identity bridge username backend."""
    return {
        "target_api_url": "https://waldur-b.example.com/api/",
        "target_api_token": "test-token-waldur-b",
        "target_offering_uuid": "offering-uuid-on-waldur-b",
        "target_customer_uuid": "customer-uuid-on-waldur-b",
        "identity_bridge_source": "isd:test",
    }


@pytest.fixture()
def mock_offering():
    """Mock Offering object."""
    offering = MagicMock()
    offering.name = "Test Offering"
    offering.waldur_api_url = "https://waldur-a.example.com/api/"
    offering.waldur_api_token = "test-token-waldur-a"
    offering.waldur_offering_uuid = "offering-uuid-on-waldur-a"
    offering.backend_settings = {
        "target_api_url": "https://waldur-b.example.com/api/",
        "target_api_token": "test-token-waldur-b",
        "identity_bridge_source": "isd:test",
    }
    return offering


def _make_offering_user(**kwargs):
    """Create a mock OfferingUser with given fields."""
    ou = MagicMock()
    ou.uuid = UUID("12345678-1234-1234-1234-123456789abc")
    ou.user_username = kwargs.get("user_username", "testuser")
    ou.user_email = kwargs.get("user_email", "test@example.com")
    ou.user_full_name = kwargs.get("user_full_name", "Test User")
    ou.user_organization = kwargs.get("user_organization", UNSET)
    ou.user_affiliations = kwargs.get("user_affiliations", UNSET)
    ou.user_phone_number = kwargs.get("user_phone_number", UNSET)
    ou.user_civil_number = kwargs.get("user_civil_number", UNSET)
    ou.user_personal_title = kwargs.get("user_personal_title", UNSET)
    ou.user_place_of_birth = kwargs.get("user_place_of_birth", UNSET)
    ou.user_country_of_residence = kwargs.get("user_country_of_residence", UNSET)
    ou.user_nationality = kwargs.get("user_nationality", UNSET)
    ou.user_nationalities = kwargs.get("user_nationalities", UNSET)
    ou.user_organization_country = kwargs.get("user_organization_country", UNSET)
    ou.user_organization_type = kwargs.get("user_organization_type", UNSET)
    ou.user_eduperson_assurance = kwargs.get("user_eduperson_assurance", UNSET)
    ou.user_identity_source = kwargs.get("user_identity_source", UNSET)
    ou.user_gender = kwargs.get("user_gender", UNSET)
    ou.user_birth_date = kwargs.get("user_birth_date", UNSET)
    # These may not exist yet (Stage 0 prerequisite)
    if "user_first_name" in kwargs:
        ou.user_first_name = kwargs["user_first_name"]
    else:
        # Simulate missing attribute (pre-Stage 0)
        del ou.user_first_name
    if "user_last_name" in kwargs:
        ou.user_last_name = kwargs["user_last_name"]
    else:
        del ou.user_last_name
    return ou


@pytest.fixture()
def mock_httpx_client():
    """Create a mock httpx client that returns success responses."""
    client = MagicMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"uuid": "new-user-uuid", "username": "testuser"}
    client.post.return_value = response
    client.get.return_value = response
    return client


@pytest.fixture()
def username_backend(identity_bridge_settings, mock_offering, mock_httpx_client):
    """Create WaldurIdentityBridgeUsernameBackend with mocked HTTP client."""
    with patch.object(
        WaldurIdentityBridgeUsernameBackend, "_log_attribute_config"
    ):
        backend = WaldurIdentityBridgeUsernameBackend(
            backend_settings=identity_bridge_settings,
            offering=mock_offering,
        )
    backend._http_client = MagicMock()
    backend._http_client.get_httpx_client.return_value = mock_httpx_client
    return backend


class TestGetWaldurUsername:
    def test_returns_username(self):
        ou = _make_offering_user(user_username="alice")
        assert _get_waldur_username(ou) == "alice"

    def test_returns_none_for_unset(self):
        ou = _make_offering_user(user_username=UNSET)
        assert _get_waldur_username(ou) is None

    def test_returns_none_for_empty_string(self):
        ou = _make_offering_user(user_username="")
        assert _get_waldur_username(ou) is None


class TestExtractAttributes:
    def test_maps_email(self):
        ou = _make_offering_user(user_email="alice@example.com")
        attrs = _extract_attributes(ou)
        assert attrs["email"] == "alice@example.com"

    def test_maps_first_last_name(self):
        ou = _make_offering_user(user_first_name="Alice", user_last_name="Smith")
        attrs = _extract_attributes(ou)
        assert attrs["first_name"] == "Alice"
        assert attrs["last_name"] == "Smith"

    def test_skips_unset_fields(self):
        ou = _make_offering_user(user_organization=UNSET)
        attrs = _extract_attributes(ou)
        assert "organization" not in attrs

    def test_maps_gender_enum(self):
        gender_mock = MagicMock()
        gender_mock.value = 1
        ou = _make_offering_user(user_gender=gender_mock)
        attrs = _extract_attributes(ou)
        assert attrs["gender"] == 1

    def test_maps_birth_date_iso(self):
        from datetime import date

        ou = _make_offering_user(user_birth_date=date(1990, 1, 15))
        attrs = _extract_attributes(ou)
        assert attrs["birth_date"] == "1990-01-15"

    def test_skips_missing_first_last_name(self):
        """Before Stage 0 (Mastermind change), fields don't exist at all."""
        ou = _make_offering_user()  # no user_first_name/user_last_name
        attrs = _extract_attributes(ou)
        assert "first_name" not in attrs
        assert "last_name" not in attrs


class TestGetUsername:
    def test_returns_waldur_username(self, username_backend):
        ou = _make_offering_user(user_username="alice")
        assert username_backend.get_username(ou) == "alice"

    def test_returns_none_for_unset(self, username_backend):
        ou = _make_offering_user(user_username=UNSET)
        assert username_backend.get_username(ou) is None


class TestGenerateUsername:
    def test_pushes_to_identity_bridge(self, username_backend, mock_httpx_client):
        ou = _make_offering_user(user_username="alice")
        result = username_backend.generate_username(ou)
        assert result == "alice"
        mock_httpx_client.post.assert_called_once()
        call_kwargs = mock_httpx_client.post.call_args
        assert call_kwargs[0][0] == "/api/identity-bridge/"
        payload = call_kwargs[1]["json"]
        assert payload["username"] == "alice"
        assert payload["source"] == "isd:test"


class TestSyncUserProfiles:
    def test_pushes_all_users(self, username_backend, mock_httpx_client):
        users = [
            _make_offering_user(user_username="alice"),
            _make_offering_user(user_username="bob"),
        ]
        username_backend.sync_user_profiles(users)
        assert mock_httpx_client.post.call_count == 2

    def test_skips_without_source(self, username_backend, mock_httpx_client):
        username_backend.identity_bridge_source = ""
        users = [_make_offering_user(user_username="alice")]
        username_backend.sync_user_profiles(users)
        mock_httpx_client.post.assert_not_called()

    def test_detects_stale_users(self, username_backend, mock_httpx_client):
        # First sync: alice and bob
        users_1 = [
            _make_offering_user(user_username="alice"),
            _make_offering_user(user_username="bob"),
        ]
        username_backend.sync_user_profiles(users_1)
        assert username_backend._previous_offering_usernames == {"alice", "bob"}

        # Second sync: only alice (bob departed)
        mock_httpx_client.post.reset_mock()
        users_2 = [_make_offering_user(user_username="alice")]
        username_backend.sync_user_profiles(users_2)

        # alice pushed + bob deactivated via /remove/
        calls = mock_httpx_client.post.call_args_list
        urls = [call[0][0] for call in calls]
        assert "/api/identity-bridge/" in urls
        assert "/api/identity-bridge/remove/" in urls

    def test_no_deactivation_on_first_sync(self, username_backend, mock_httpx_client):
        """First sync should not deactivate anyone (no baseline)."""
        users = [_make_offering_user(user_username="alice")]
        username_backend.sync_user_profiles(users)
        # Only push calls, no remove calls
        for call in mock_httpx_client.post.call_args_list:
            assert call[0][0] != "/api/identity-bridge/remove/"

    def test_push_user_handles_http_error(self, username_backend, mock_httpx_client):
        mock_httpx_client.post.side_effect = httpx.HTTPStatusError(
            "Forbidden", request=MagicMock(), response=MagicMock(status_code=403)
        )
        users = [_make_offering_user(user_username="alice")]
        # Should not raise — errors are logged per-user
        username_backend.sync_user_profiles(users)


class TestDeactivateUsers:
    def test_calls_remove_endpoint(self, username_backend, mock_httpx_client):
        username_backend.deactivate_users({"alice", "bob"})
        assert mock_httpx_client.post.call_count == 2
        for call in mock_httpx_client.post.call_args_list:
            assert call[0][0] == "/api/identity-bridge/remove/"
            payload = call[1]["json"]
            assert payload["source"] == "isd:test"
            assert payload["username"] in {"alice", "bob"}

    def test_handles_http_error(self, username_backend, mock_httpx_client):
        mock_httpx_client.post.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=MagicMock(status_code=500)
        )
        # Should not raise — errors are logged per-user
        username_backend.deactivate_users({"alice"})


class TestLogAttributeConfig:
    def test_fetches_from_waldur_a(self, identity_bridge_settings, mock_offering):
        """Verify _log_attribute_config calls Waldur A."""
        mock_httpx = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "exposed_fields": ["username", "email", "first_name", "last_name"],
            "is_default": False,
        }
        mock_response.raise_for_status = MagicMock()
        mock_httpx.get.return_value = mock_response

        with patch(
            "waldur_site_agent_waldur.username_backend.AuthenticatedClient"
        ) as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_httpx_client.return_value = mock_httpx
            MockClient.return_value = mock_instance

            backend = WaldurIdentityBridgeUsernameBackend(
                backend_settings=identity_bridge_settings,
                offering=mock_offering,
            )

        # Verify Waldur A client was created with correct URL
        MockClient.assert_any_call(
            base_url="https://waldur-a.example.com/api",
            token="test-token-waldur-a",
        )

    def test_warns_on_missing_fields(self, identity_bridge_settings, mock_offering):
        """Warn when required fields are not exposed."""
        mock_httpx = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "exposed_fields": ["email"],  # missing username, first_name, last_name
            "is_default": True,
        }
        mock_response.raise_for_status = MagicMock()
        mock_httpx.get.return_value = mock_response

        with (
            patch(
                "waldur_site_agent_waldur.username_backend.AuthenticatedClient"
            ) as MockClient,
            patch("waldur_site_agent_waldur.username_backend.logger") as mock_logger,
        ):
            mock_instance = MagicMock()
            mock_instance.get_httpx_client.return_value = mock_httpx
            MockClient.return_value = mock_instance

            WaldurIdentityBridgeUsernameBackend(
                backend_settings=identity_bridge_settings,
                offering=mock_offering,
            )

        mock_logger.warning.assert_any_call(
            "Identity bridge recommended fields NOT exposed: %s. "
            "These won't be available for user sync.",
            sorted({"username", "first_name", "last_name"}),
        )

    def test_handles_no_offering(self, identity_bridge_settings):
        """Graceful no-op when offering is None."""
        with patch("waldur_site_agent_waldur.username_backend.logger") as mock_logger:
            backend = WaldurIdentityBridgeUsernameBackend.__new__(
                WaldurIdentityBridgeUsernameBackend
            )
            backend.backend_settings = identity_bridge_settings
            backend.offering = None
            backend.identity_bridge_source = "isd:test"
            backend._previous_offering_usernames = None
            # Call the real method directly (bypass __init__)
            WaldurIdentityBridgeUsernameBackend._log_attribute_config(backend)

        mock_logger.warning.assert_any_call(
            "No offering context \u2014 cannot fetch attribute config"
        )
