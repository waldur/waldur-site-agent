"""Tests for OFFERING_USER event handlers."""

import json
import unittest
from unittest import mock

import stomp.utils
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing.handlers import (
    _add_user_to_resources,
    _forward_user_attributes_to_backend,
    _process_offering_user_message,
    on_offering_user_message_stomp,
)


def _make_offering(**overrides):
    defaults = {
        "name": "test-offering",
        "waldur_offering_uuid": "test-uuid",
        "waldur_api_url": "https://example.com/api/",
        "waldur_api_token": "token",
        "backend_type": "slurm",
        "membership_sync_backend": "slurm",
    }
    defaults.update(overrides)
    return structures.Offering(**defaults)


def _make_message(**overrides):
    defaults = {
        "offering_user_uuid": "ou-uuid-1",
        "user_uuid": "user-uuid-1",
        "username": "testuser",
        "action": "attribute_update",
        "offering_uuid": "test-uuid",
        "attributes": {"email": "test@example.com", "full_name": "Test User"},
        "changed_attributes": ["email"],
        "resource_backend_ids": [],
    }
    defaults.update(overrides)
    return defaults


def _make_resource(backend_id, restrict=False):
    resource = mock.Mock()
    resource.backend_id = backend_id
    resource.restrict_member_access = restrict
    return resource


class TestOfferingUserAttributeUpdateCallsBackend(unittest.TestCase):
    """Test that attribute_update action calls backend.update_user_attributes."""

    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_attribute_update_calls_backend(
        self, mock_get_backend, mock_get_client, mock_register
    ):
        offering = _make_offering()
        message = _make_message()

        mock_backend = mock.Mock()
        mock_backend.update_user_attributes = mock.Mock()
        mock_get_backend.return_value = (mock_backend, "1.0")

        _process_offering_user_message(message, offering, "test-agent")

        mock_backend.update_user_attributes.assert_called_once_with(
            "testuser", {"email": "test@example.com", "full_name": "Test User"}
        )


class TestOfferingUserNoBackendSkips(unittest.TestCase):
    """Test that offering without membership_sync_backend does not error."""

    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_no_backend_skips(self, mock_get_client, mock_register):
        offering = _make_offering(membership_sync_backend="")
        message = _make_message()

        # Should not raise
        _process_offering_user_message(message, offering, "test-agent")


class TestOfferingUserCreateForwardsAttributes(unittest.TestCase):
    """Test that action=create also forwards attributes."""

    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_create_forwards_attributes(self, mock_get_backend, mock_get_client, mock_register):
        offering = _make_offering()
        message = _make_message(action="create")

        mock_backend = mock.Mock()
        mock_backend.update_user_attributes = mock.Mock()
        mock_get_backend.return_value = (mock_backend, "1.0")

        _process_offering_user_message(message, offering, "test-agent")

        mock_backend.update_user_attributes.assert_called_once_with(
            "testuser", {"email": "test@example.com", "full_name": "Test User"}
        )


class TestOfferingUserDeleteDoesNotForward(unittest.TestCase):
    """Test that action=delete does not call backend."""

    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_delete_does_not_forward(self, mock_get_backend, mock_get_client, mock_register):
        offering = _make_offering()
        message = _make_message(action="delete")

        _process_offering_user_message(message, offering, "test-agent")

        mock_get_backend.assert_not_called()


class TestOfferingUserUnknownActionLogsWarning(unittest.TestCase):
    """Test that unknown action logs a warning but does not crash."""

    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_unknown_action_logs_warning(self, mock_get_backend, mock_get_client, mock_register):
        offering = _make_offering()
        message = _make_message(action="unknown_action")

        # Should not raise
        _process_offering_user_message(message, offering, "test-agent")

        mock_get_backend.assert_not_called()


class TestStompHandlerDelegatesToProcess(unittest.TestCase):
    """Test that the STOMP handler parses the frame and delegates."""

    @mock.patch("waldur_site_agent.event_processing.handlers._process_offering_user_message")
    def test_stomp_handler_delegates(self, mock_process):
        offering = _make_offering()
        message = _make_message()
        frame = mock.Mock(spec=stomp.utils.Frame)
        frame.body = json.dumps(message)

        on_offering_user_message_stomp(frame, offering, "test-agent")

        mock_process.assert_called_once_with(message, offering, "test-agent")


class TestForwardUserAttributesToBackend(unittest.TestCase):
    """Tests for _forward_user_attributes_to_backend helper."""

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_empty_attributes_skips(self, mock_get_backend):
        offering = _make_offering()
        _forward_user_attributes_to_backend(offering, "testuser", {}, "test-agent")
        mock_get_backend.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_no_membership_backend_skips(self, mock_get_backend):
        offering = _make_offering(membership_sync_backend="")
        _forward_user_attributes_to_backend(
            offering, "testuser", {"email": "x@y.com"}, "test-agent"
        )
        mock_get_backend.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_backend_without_method_skips(self, mock_get_backend):
        mock_backend = mock.Mock(spec=[])  # No update_user_attributes
        mock_get_backend.return_value = (mock_backend, "1.0")
        offering = _make_offering()

        _forward_user_attributes_to_backend(
            offering, "testuser", {"email": "x@y.com"}, "test-agent"
        )

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_backend_exception_is_caught(self, mock_get_backend):
        mock_backend = mock.Mock()
        mock_backend.update_user_attributes.side_effect = RuntimeError("fail")
        mock_get_backend.return_value = (mock_backend, "1.0")
        offering = _make_offering()

        # Should not raise
        _forward_user_attributes_to_backend(
            offering, "testuser", {"email": "x@y.com"}, "test-agent"
        )


class TestDetermineObjectTypesIncludesOfferingUser(unittest.TestCase):
    """Test that _determine_observable_object_types includes OFFERING_USER."""

    def test_membership_sync_includes_offering_user(self):
        from waldur_site_agent.event_processing.utils import _determine_observable_object_types

        offering = _make_offering()
        result = _determine_observable_object_types(offering)
        self.assertIn(ObservableObjectTypeEnum.OFFERING_USER, result)

    def test_no_membership_sync_excludes_offering_user(self):
        from waldur_site_agent.event_processing.utils import _determine_observable_object_types

        offering = _make_offering(membership_sync_backend="")
        result = _determine_observable_object_types(offering)
        self.assertNotIn(ObservableObjectTypeEnum.OFFERING_USER, result)


class TestAddUserToResources(unittest.TestCase):
    """Tests for _add_user_to_resources helper (username_set action)."""

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_calls_add_user_for_matching_resources(self, mock_get_backend, mock_resources_list):
        offering = _make_offering()
        mock_backend = mock.Mock()
        mock_get_backend.return_value = (mock_backend, "1.0")

        r1 = _make_resource("account-1")
        r2 = _make_resource("account-2")
        mock_resources_list.sync_all.return_value = [r1, r2]

        _add_user_to_resources(offering, "testuser", ["account-1", "account-2"], mock.Mock())

        self.assertEqual(mock_backend.add_user.call_count, 2)
        mock_backend.add_user.assert_any_call(r1, "testuser")
        mock_backend.add_user.assert_any_call(r2, "testuser")

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_skips_restricted_resources(self, mock_get_backend, mock_resources_list):
        offering = _make_offering()
        mock_backend = mock.Mock()
        mock_get_backend.return_value = (mock_backend, "1.0")

        r1 = _make_resource("account-1", restrict=True)
        r2 = _make_resource("account-2", restrict=False)
        mock_resources_list.sync_all.return_value = [r1, r2]

        _add_user_to_resources(offering, "testuser", ["account-1", "account-2"], mock.Mock())

        mock_backend.add_user.assert_called_once_with(r2, "testuser")

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_skips_resources_not_in_backend_ids(self, mock_get_backend, mock_resources_list):
        offering = _make_offering()
        mock_backend = mock.Mock()
        mock_get_backend.return_value = (mock_backend, "1.0")

        r1 = _make_resource("account-1")
        r2 = _make_resource("account-other")
        mock_resources_list.sync_all.return_value = [r1, r2]

        _add_user_to_resources(offering, "testuser", ["account-1"], mock.Mock())

        mock_backend.add_user.assert_called_once_with(r1, "testuser")

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_empty_resource_backend_ids_skips_backend(self, mock_get_backend):
        offering = _make_offering()

        _add_user_to_resources(offering, "testuser", [], mock.Mock())

        mock_get_backend.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_no_membership_sync_backend_skips(self, mock_get_backend):
        offering = _make_offering(membership_sync_backend="")

        _add_user_to_resources(offering, "testuser", ["account-1"], mock.Mock())

        mock_get_backend.assert_not_called()

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
    )
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
    def test_per_resource_error_does_not_abort_others(self, mock_get_backend, mock_resources_list):
        offering = _make_offering()
        mock_backend = mock.Mock()
        mock_backend.add_user.side_effect = [RuntimeError("fail"), None]
        mock_get_backend.return_value = (mock_backend, "1.0")

        r1 = _make_resource("account-1")
        r2 = _make_resource("account-2")
        mock_resources_list.sync_all.return_value = [r1, r2]

        # Should not raise
        _add_user_to_resources(offering, "testuser", ["account-1", "account-2"], mock.Mock())

        self.assertEqual(mock_backend.add_user.call_count, 2)


class TestUsernameSetDispatchesToHelper(unittest.TestCase):
    """Test that username_set action in _process_offering_user_message calls _add_user_to_resources."""

    @mock.patch("waldur_site_agent.event_processing.handlers._add_user_to_resources")
    @mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
    @mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_username_set_calls_helper(self, mock_get_client, mock_register, mock_add_user):
        offering = _make_offering()
        message = _make_message(
            action="username_set",
            resource_backend_ids=["account-1", "account-2"],
        )

        _process_offering_user_message(message, offering, "test-agent")

        mock_add_user.assert_called_once_with(
            offering,
            "testuser",
            ["account-1", "account-2"],
            mock_get_client.return_value,
        )
