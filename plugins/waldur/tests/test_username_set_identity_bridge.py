"""E2E test for username_set identity bridge resolution.

Bug (pre-fix): when a username_set offering-user event fires,
_add_user_to_resources called backend.add_user(resource, username)
where ``username`` is the offering_user_username (the target-side local
name).  Because no ``user_cuid`` kwarg was passed, WaldurBackend fell
back to using that local name as the identity-bridge username instead of
the real CUID.

This created duplicate users on the target Waldur with empty attributes.
"""

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from waldur_api_client.types import UNSET

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing.handlers import (
    _add_user_to_resources,
    _process_offering_user_message,
)
from waldur_site_agent_waldur.backend import WaldurBackend

RESOURCE_BACKEND_ID = "abcdef01-1234-1234-1234-123456789abc"
PROJECT_UUID = UUID("aabbccdd-1234-1234-1234-123456789abc")
CREATED_USER_UUID = UUID("11223344-1234-1234-1234-123456789abc")

OFFERING_USER_USERNAME = "localuser"  # target-side local username
USER_CUID = "aaaabbbb-cccc-dddd-eeee-ffffffffffff@idp.example.org"  # real CUID
USER_UUID = "dddddddd-1234-1234-1234-123456789abc"
OFFERING_UUID = "eeee0000-1111-2222-3333-444455556666"


def _make_offering(**overrides):
    defaults = {
        "name": "Test Offering A",
        "waldur_offering_uuid": OFFERING_UUID,
        "waldur_api_url": "https://waldur-a.example.com/api/",
        "waldur_api_token": "token",
        "backend_type": "waldur",
        "membership_sync_backend": "waldur",
        "backend_settings": {
            "target_api_url": "https://waldur-b.example.com/api/",
            "target_api_token": "test-token",
            "target_offering_uuid": "ffff0000-1111-2222-3333-444455556666",
            "target_customer_uuid": "cccc0000-1111-2222-3333-444455556666",
            "user_match_field": "cuid",
            "identity_bridge_source": "isd:test",
        },
    }
    defaults.update(overrides)
    return structures.Offering(**defaults)


def _make_resource(backend_id=RESOURCE_BACKEND_ID, restrict=False):
    resource = MagicMock()
    resource.backend_id = backend_id
    resource.restrict_member_access = restrict
    return resource


def _make_offering_user(username: str, user_username: str):
    """Create a mock OfferingUser with the given username and CUID."""
    ou = MagicMock()
    ou.username = username
    ou.user_username = user_username
    return ou


class TestUsernameSetIdentityBridgeResolution:
    """Verify that _add_user_to_resources passes the CUID — not the
    offering_user_username — to the identity bridge via backend.add_user.
    """

    @pytest.fixture()
    def mock_client(self):
        client = MagicMock()
        client.api_url = "https://waldur-b.example.com/api/"
        client.resolve_user_via_identity_bridge.return_value = CREATED_USER_UUID
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        client.get_marketplace_resource.return_value = mock_resource
        return client

    @pytest.fixture()
    def backend(self, mock_client):
        settings = {
            "target_api_url": "https://waldur-b.example.com/api/",
            "target_api_token": "test-token",
            "target_offering_uuid": "ffff0000-1111-2222-3333-444455556666",
            "target_customer_uuid": "cccc0000-1111-2222-3333-444455556666",
            "user_match_field": "cuid",
            "identity_bridge_source": "isd:test",
        }
        backend = WaldurBackend(settings, {})
        backend.client = mock_client
        return backend

    # ------------------------------------------------------------------
    # Direct _add_user_to_resources tests
    # ------------------------------------------------------------------

    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering"
    )
    def test_identity_bridge_receives_cuid_when_provided(
        self, mock_get_backend, mock_resources_list, backend, mock_client
    ):
        """When user_cuid is passed, the identity bridge must receive the CUID."""
        offering = _make_offering()
        mock_get_backend.return_value = (backend, "1.0.0")
        mock_resources_list.sync_all.return_value = [
            _make_resource(RESOURCE_BACKEND_ID),
        ]

        _add_user_to_resources(
            offering,
            OFFERING_USER_USERNAME,
            [RESOURCE_BACKEND_ID],
            MagicMock(),
            user_cuid=USER_CUID,
        )

        mock_client.resolve_user_via_identity_bridge.assert_called_once()
        actual_username = (
            mock_client.resolve_user_via_identity_bridge.call_args[0][0]
        )
        assert actual_username == USER_CUID

    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering"
    )
    def test_add_user_receives_user_cuid_kwarg(
        self, mock_get_backend, mock_resources_list, backend
    ):
        """backend.add_user must receive user_cuid so the backend can resolve
        the correct identity."""
        offering = _make_offering()

        calls: list[dict] = []
        original_add_user = backend.add_user

        def spy_add_user(waldur_resource, username, **kwargs):
            calls.append({"username": username, "kwargs": kwargs})
            return original_add_user(waldur_resource, username, **kwargs)

        backend.add_user = spy_add_user
        mock_get_backend.return_value = (backend, "1.0.0")
        mock_resources_list.sync_all.return_value = [
            _make_resource(RESOURCE_BACKEND_ID),
        ]

        _add_user_to_resources(
            offering,
            OFFERING_USER_USERNAME,
            [RESOURCE_BACKEND_ID],
            MagicMock(),
            user_cuid=USER_CUID,
        )

        assert len(calls) == 1
        assert calls[0]["kwargs"].get("user_cuid") == USER_CUID

    # ------------------------------------------------------------------
    # Full message flow: _process_offering_user_message → _add_user_to_resources
    # ------------------------------------------------------------------

    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_offering_users_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.register_event_process_service"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_full_flow_resolves_cuid_from_offering_user(
        self,
        mock_get_client,
        mock_register,
        mock_get_backend,
        mock_ou_list,
        mock_resources_list,
        backend,
        mock_client,
    ):
        """End-to-end: a username_set STOMP message must result in the identity
        bridge being called with the CUID, not the offering_user_username."""
        offering = _make_offering()
        mock_get_backend.return_value = (backend, "1.0.0")
        mock_resources_list.sync_all.return_value = [
            _make_resource(RESOURCE_BACKEND_ID),
        ]
        # The offering user lookup returns an OfferingUser with the real CUID
        mock_ou_list.sync_all.return_value = [
            _make_offering_user(OFFERING_USER_USERNAME, USER_CUID),
        ]

        message = {
            "offering_user_uuid": "ou-uuid-1",
            "user_uuid": USER_UUID,
            "username": OFFERING_USER_USERNAME,
            "action": "username_set",
            "offering_uuid": OFFERING_UUID,
            "attributes": {},
            "changed_attributes": [],
            "resource_backend_ids": [RESOURCE_BACKEND_ID],
        }

        _process_offering_user_message(message, offering, "test-agent")

        # The identity bridge must have been called with the CUID
        mock_client.resolve_user_via_identity_bridge.assert_called_once()
        actual_username = (
            mock_client.resolve_user_via_identity_bridge.call_args[0][0]
        )
        assert actual_username == USER_CUID, (
            f"Expected identity bridge to receive CUID '{USER_CUID}', "
            f"got '{actual_username}'"
        )

    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.marketplace_offering_users_list"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering"
    )
    @patch(
        "waldur_site_agent.event_processing.handlers.register_event_process_service"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_falls_back_to_username_when_cuid_unavailable(
        self,
        mock_get_client,
        mock_register,
        mock_get_backend,
        mock_ou_list,
        mock_resources_list,
        backend,
        mock_client,
    ):
        """If the offering user lookup returns no CUID, fall back to the
        offering_user_username — same as before the fix."""
        offering = _make_offering()
        mock_get_backend.return_value = (backend, "1.0.0")
        mock_resources_list.sync_all.return_value = [
            _make_resource(RESOURCE_BACKEND_ID),
        ]
        # Offering user has UNSET user_username
        ou = _make_offering_user(OFFERING_USER_USERNAME, UNSET)
        mock_ou_list.sync_all.return_value = [ou]

        message = {
            "offering_user_uuid": "ou-uuid-1",
            "user_uuid": USER_UUID,
            "username": OFFERING_USER_USERNAME,
            "action": "username_set",
            "offering_uuid": OFFERING_UUID,
            "attributes": {},
            "changed_attributes": [],
            "resource_backend_ids": [RESOURCE_BACKEND_ID],
        }

        _process_offering_user_message(message, offering, "test-agent")

        mock_client.resolve_user_via_identity_bridge.assert_called_once()
        actual_username = (
            mock_client.resolve_user_via_identity_bridge.call_args[0][0]
        )
        # Fallback: local username used when CUID is unavailable
        assert actual_username == OFFERING_USER_USERNAME
