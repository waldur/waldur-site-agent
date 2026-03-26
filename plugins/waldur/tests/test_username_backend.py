"""Tests for WaldurIdentityBridgeUsernameBackend."""

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from waldur_api_client import errors
from waldur_api_client.models.identity_bridge_request_request import (
    IdentityBridgeRequestRequest,
)
from waldur_api_client.types import UNSET

from waldur_site_agent_waldur.username_backend import (
    WaldurIdentityBridgeUsernameBackend,
    _extract_attributes,
    _get_waldur_username,
)

MODULE = "waldur_site_agent_waldur.username_backend"


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
def username_backend(identity_bridge_settings, mock_offering):
    """Create WaldurIdentityBridgeUsernameBackend with mocked HTTP client."""
    with patch.object(
        WaldurIdentityBridgeUsernameBackend, "_log_attribute_config"
    ):
        backend = WaldurIdentityBridgeUsernameBackend(
            backend_settings=identity_bridge_settings,
            offering=mock_offering,
        )
    return backend


class TestBaseUrlStripping:
    """AuthenticatedClient base_url must not include /api suffix."""

    def test_target_api_url_with_api_suffix(self, identity_bridge_settings, mock_offering):
        with patch(
            f"{MODULE}.AuthenticatedClient"
        ) as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_httpx_client.return_value = MagicMock()
            MockClient.return_value = mock_instance

            WaldurIdentityBridgeUsernameBackend(
                backend_settings=identity_bridge_settings,
                offering=mock_offering,
            )

        # The target client (second call) must strip /api
        target_call = [
            c for c in MockClient.call_args_list
            if c[1].get("base_url", "").endswith("waldur-b.example.com")
        ]
        assert target_call, "Expected AuthenticatedClient call for target Waldur B"
        assert target_call[0][1]["base_url"] == "https://waldur-b.example.com"

    def test_target_api_url_without_trailing_slash(self, mock_offering):
        settings = {
            "target_api_url": "https://waldur-b.example.com/api",
            "target_api_token": "test-token",
            "identity_bridge_source": "isd:test",
        }
        with patch(
            f"{MODULE}.AuthenticatedClient"
        ) as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_httpx_client.return_value = MagicMock()
            MockClient.return_value = mock_instance

            WaldurIdentityBridgeUsernameBackend(
                backend_settings=settings,
                offering=mock_offering,
            )

        target_call = [
            c for c in MockClient.call_args_list
            if "waldur-b" in c[1].get("base_url", "")
        ]
        assert target_call
        assert not target_call[0][1]["base_url"].endswith("/api")


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
    def test_always_returns_none(self, username_backend):
        """get_username always returns None — username comes from Waldur B."""
        ou = _make_offering_user(user_username="alice")
        assert username_backend.get_username(ou) is None

    def test_returns_none_for_unset(self, username_backend):
        ou = _make_offering_user(user_username=UNSET)
        assert username_backend.get_username(ou) is None


class TestGenerateUsername:
    def test_pushes_to_identity_bridge(self, username_backend):
        ou = _make_offering_user(user_username="alice")
        with patch(f"{MODULE}.identity_bridge") as mock_ib:
            result = username_backend.generate_username(ou)
        assert result == ""
        mock_ib.sync.assert_called_once()
        body = mock_ib.sync.call_args[1]["body"]
        assert isinstance(body, IdentityBridgeRequestRequest)
        assert body.username == "alice"
        assert body.source == "isd:test"


class TestSyncUserProfiles:
    def test_pushes_all_users(self, username_backend):
        users = [
            _make_offering_user(user_username="alice"),
            _make_offering_user(user_username="bob"),
        ]
        with patch(f"{MODULE}.identity_bridge") as mock_ib:
            username_backend.sync_user_profiles(users)
        assert mock_ib.sync.call_count == 2

    def test_skips_without_source(self, username_backend):
        username_backend.identity_bridge_source = ""
        users = [_make_offering_user(user_username="alice")]
        with patch(f"{MODULE}.identity_bridge") as mock_ib:
            username_backend.sync_user_profiles(users)
        mock_ib.sync.assert_not_called()

    def test_detects_stale_users(self, username_backend):
        users_1 = [
            _make_offering_user(user_username="alice"),
            _make_offering_user(user_username="bob"),
        ]
        with (
            patch(f"{MODULE}.identity_bridge"),
            patch(f"{MODULE}.identity_bridge_remove") as mock_remove,
        ):
            username_backend.sync_user_profiles(users_1)
            assert username_backend._previous_offering_usernames == {"alice", "bob"}

            # Second sync: only alice (bob departed)
            users_2 = [_make_offering_user(user_username="alice")]
            username_backend.sync_user_profiles(users_2)

            # bob should be removed
            mock_remove.sync.assert_called_once()
            remove_body = mock_remove.sync.call_args[1]["body"]
            assert remove_body.username == "bob"

    def test_no_deactivation_on_first_sync(self, username_backend):
        """First sync should not deactivate anyone (no baseline)."""
        users = [_make_offering_user(user_username="alice")]
        with (
            patch(f"{MODULE}.identity_bridge"),
            patch(f"{MODULE}.identity_bridge_remove") as mock_remove,
        ):
            username_backend.sync_user_profiles(users)
            mock_remove.sync.assert_not_called()

    def test_push_user_handles_error(self, username_backend):
        with patch(f"{MODULE}.identity_bridge") as mock_ib:
            mock_ib.sync.side_effect = errors.UnexpectedStatus(
                403, b"Forbidden", "http://example.com"
            )
            users = [_make_offering_user(user_username="alice")]
            # Should not raise — errors are logged per-user
            username_backend.sync_user_profiles(users)


class TestDeactivateUsers:
    def test_calls_remove_endpoint(self, username_backend):
        with patch(f"{MODULE}.identity_bridge_remove") as mock_remove:
            username_backend.deactivate_users({"alice", "bob"})
        assert mock_remove.sync.call_count == 2
        usernames = {
            call[1]["body"].username for call in mock_remove.sync.call_args_list
        }
        assert usernames == {"alice", "bob"}

    def test_handles_error(self, username_backend):
        with patch(f"{MODULE}.identity_bridge_remove") as mock_remove:
            mock_remove.sync.side_effect = errors.UnexpectedStatus(
                500, b"Server Error", "http://example.com"
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
            f"{MODULE}.AuthenticatedClient"
        ) as MockClient:
            mock_instance = MagicMock()
            mock_instance.get_httpx_client.return_value = mock_httpx
            MockClient.return_value = mock_instance

            WaldurIdentityBridgeUsernameBackend(
                backend_settings=identity_bridge_settings,
                offering=mock_offering,
            )

        MockClient.assert_any_call(
            base_url="https://waldur-a.example.com",
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
                f"{MODULE}.AuthenticatedClient"
            ) as MockClient,
            patch(f"{MODULE}.logger") as mock_logger,
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
        with patch(f"{MODULE}.logger") as mock_logger:
            backend = WaldurIdentityBridgeUsernameBackend.__new__(
                WaldurIdentityBridgeUsernameBackend
            )
            backend.backend_settings = identity_bridge_settings
            backend.offering = None
            backend.identity_bridge_source = "isd:test"
            backend._previous_offering_usernames = None
            WaldurIdentityBridgeUsernameBackend._log_attribute_config(backend)

        mock_logger.warning.assert_any_call(
            "No offering context \u2014 cannot fetch attribute config"
        )


class TestFetchAllowedFields:
    """Tests for _fetch_allowed_fields discovery from Waldur B."""

    def test_returns_field_set(self, username_backend):
        """Successful fetch returns a set of field names."""
        mock_result = MagicMock()
        mock_result.allowed_fields = ["email", "first_name", "last_name", "organization"]

        with patch(
            f"{MODULE}.identity_bridge_allowed_fields_retrieve"
        ) as mock_retrieve:
            mock_retrieve.sync.return_value = mock_result
            result = username_backend._fetch_allowed_fields()

        assert result == {"email", "first_name", "last_name", "organization"}
        mock_retrieve.sync.assert_called_once_with(client=username_backend._http_client)

    def test_returns_none_on_error(self, username_backend):
        """SDK error returns None (graceful degradation)."""
        with patch(
            f"{MODULE}.identity_bridge_allowed_fields_retrieve"
        ) as mock_retrieve:
            mock_retrieve.sync.side_effect = errors.UnexpectedStatus(
                403, b"Forbidden", "http://example.com"
            )
            result = username_backend._fetch_allowed_fields()

        assert result is None

    def test_returns_none_on_connection_error(self, username_backend):
        """Connection error returns None."""
        with patch(
            f"{MODULE}.identity_bridge_allowed_fields_retrieve"
        ) as mock_retrieve:
            mock_retrieve.sync.side_effect = Exception("Connection refused")
            result = username_backend._fetch_allowed_fields()

        assert result is None


class TestFilterAttributes:
    """Tests for _filter_attributes field filtering."""

    def test_filters_disallowed_fields(self, username_backend):
        username_backend._allowed_fields_cache = {"email", "first_name"}
        attributes = {
            "email": "alice@example.com",
            "first_name": "Alice",
            "gender": 1,
            "country_of_residence": "FI",
        }
        result = username_backend._filter_attributes(attributes)
        assert result == {
            "email": "alice@example.com",
            "first_name": "Alice",
        }

    def test_empty_allowed_filters_all(self, username_backend):
        username_backend._allowed_fields_cache = set()
        attributes = {"email": "x", "first_name": "Alice"}
        result = username_backend._filter_attributes(attributes)
        assert result == {}

    def test_no_filtering_when_cache_is_none(self, username_backend):
        """When fetch failed (cache is None), pass everything through."""
        username_backend._allowed_fields_cache = None
        attributes = {"email": "x", "gender": 1}
        result = username_backend._filter_attributes(attributes)
        assert result == attributes


class TestPushWithFieldFiltering:
    """Tests for _push_user_to_identity_bridge with allowed-fields filtering."""

    def test_lazy_fetches_allowed_fields(self, username_backend):
        """First push fetches allowed fields from target."""
        mock_allowed = MagicMock()
        mock_allowed.allowed_fields = ["email", "first_name", "last_name"]

        ou = _make_offering_user(
            user_username="alice",
            user_email="a@b.com",
            user_country_of_residence="FI",
        )
        with (
            patch(f"{MODULE}.identity_bridge_allowed_fields_retrieve") as mock_retrieve,
            patch(f"{MODULE}.identity_bridge") as mock_ib,
        ):
            mock_retrieve.sync.return_value = mock_allowed
            username_backend._push_user_to_identity_bridge(ou)
            mock_retrieve.sync.assert_called_once()

        # Verify body was filtered
        body = mock_ib.sync.call_args[1]["body"]
        body_dict = body.to_dict()
        assert "email" in body_dict
        assert "country_of_residence" not in body_dict

    def test_uses_cached_fields_on_second_push(self, username_backend):
        """Second push uses cached fields, no SDK fetch."""
        username_backend._allowed_fields_cache = {"email", "first_name"}

        ou = _make_offering_user(user_username="alice", user_email="a@b.com")
        with (
            patch(f"{MODULE}.identity_bridge_allowed_fields_retrieve") as mock_retrieve,
            patch(f"{MODULE}.identity_bridge"),
        ):
            username_backend._push_user_to_identity_bridge(ou)
            mock_retrieve.sync.assert_not_called()

    def test_retries_on_400_with_cache_refresh(self, username_backend):
        """On 400, invalidate cache, re-fetch, and retry once."""
        username_backend._allowed_fields_cache = {
            "email", "first_name", "country_of_residence"
        }

        mock_allowed = MagicMock()
        mock_allowed.allowed_fields = ["email", "first_name"]

        ou = _make_offering_user(
            user_username="alice",
            user_email="a@b.com",
            user_country_of_residence="FI",
        )
        with (
            patch(f"{MODULE}.identity_bridge_allowed_fields_retrieve") as mock_retrieve,
            patch(f"{MODULE}.identity_bridge") as mock_ib,
        ):
            mock_retrieve.sync.return_value = mock_allowed
            # First call raises 400, second succeeds
            mock_ib.sync.side_effect = [
                errors.UnexpectedStatus(400, b"Fields not allowed", "http://example.com"),
                MagicMock(),
            ]
            username_backend._push_user_to_identity_bridge(ou)
            mock_retrieve.sync.assert_called_once()

        # Should have called sync twice (first 400, then retry)
        assert mock_ib.sync.call_count == 2
        # Retry body should be filtered
        retry_body = mock_ib.sync.call_args_list[1][1]["body"]
        retry_dict = retry_body.to_dict()
        assert "country_of_residence" not in retry_dict
        assert "email" in retry_dict
