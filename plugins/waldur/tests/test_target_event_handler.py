"""Tests for the target event handler and setup_target_event_subscriptions."""

import json
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
import stomp.utils
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from waldur_site_agent_waldur.backend import WaldurBackend
from waldur_site_agent_waldur.target_event_handler import make_target_order_handler

SOURCE_ORDER_UUID = UUID("aaaabbbb-1111-2222-3333-444455556666")
TARGET_ORDER_UUID = "ccccdddd-1111-2222-3333-444455556666"
OFFERING_UUID = "eeeeeeee-1111-2222-3333-444455556666"


@pytest.fixture()
def source_offering():
    """Create a mock source offering (Waldur A)."""
    offering = MagicMock()
    offering.api_url = "https://waldur-a.example.com/api/"
    offering.api_token = "source-token"
    offering.uuid = OFFERING_UUID
    offering.name = "Source Offering"
    offering.verify_ssl = True
    return offering


@pytest.fixture()
def target_offering():
    """Create a mock target offering (Waldur B)."""
    offering = MagicMock()
    offering.api_url = "https://waldur-b.example.com/api/"
    offering.api_token = "target-token"
    offering.uuid = "ffffffff-1111-2222-3333-444455556666"
    offering.name = "Target Offering"
    return offering


def _make_frame(order_uuid: str, order_state: str) -> stomp.utils.Frame:
    """Create a STOMP frame with an ORDER event payload."""
    body = json.dumps({
        "order_uuid": order_uuid,
        "order_state": order_state,
    })
    return stomp.utils.Frame(cmd="MESSAGE", headers={}, body=body)


class TestTargetOrderHandler:
    """Tests for make_target_order_handler closure."""

    def test_handler_ignores_non_terminal_states(self, source_offering, target_offering):
        """EXECUTING order_state -> handler returns without action."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, "executing")

        with patch(
            "waldur_site_agent_waldur.target_event_handler.get_client"
        ) as mock_get_client:
            handler(frame, target_offering, "test-agent")
            mock_get_client.assert_not_called()

    def test_handler_ignores_pending_provider_state(self, source_offering, target_offering):
        """PENDING_PROVIDER order_state -> handler returns without action."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, "pending-provider")

        with patch(
            "waldur_site_agent_waldur.target_event_handler.get_client"
        ) as mock_get_client:
            handler(frame, target_offering, "test-agent")
            mock_get_client.assert_not_called()

    def test_handler_processes_done_state(self, source_offering, target_offering):
        """DONE -> finds source order, calls set_state_done."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, OrderState.DONE.value)

        mock_source_order = MagicMock()
        mock_source_order.uuid = SOURCE_ORDER_UUID
        mock_source_order.backend_id = TARGET_ORDER_UUID

        with (
            patch(
                "waldur_site_agent_waldur.target_event_handler.get_client"
            ) as mock_get_client,
            patch(
                "waldur_site_agent_waldur.target_event_handler.marketplace_orders_list"
            ) as mock_orders_list,
            patch(
                "waldur_site_agent_waldur.target_event_handler."
                "marketplace_orders_set_state_done"
            ) as mock_set_done,
        ):
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_orders_list.sync_all.return_value = [mock_source_order]

            handler(frame, target_offering, "test-agent")

            mock_set_done.sync_detailed.assert_called_once_with(
                uuid=SOURCE_ORDER_UUID,
                client=mock_client,
            )

    def test_handler_processes_erred_state(self, source_offering, target_offering):
        """ERRED -> finds source order, calls set_state_erred."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, OrderState.ERRED.value)

        mock_source_order = MagicMock()
        mock_source_order.uuid = SOURCE_ORDER_UUID
        mock_source_order.backend_id = TARGET_ORDER_UUID

        with (
            patch(
                "waldur_site_agent_waldur.target_event_handler.get_client"
            ) as mock_get_client,
            patch(
                "waldur_site_agent_waldur.target_event_handler.marketplace_orders_list"
            ) as mock_orders_list,
            patch(
                "waldur_site_agent_waldur.target_event_handler."
                "marketplace_orders_set_state_erred"
            ) as mock_set_erred,
        ):
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_orders_list.sync_all.return_value = [mock_source_order]

            handler(frame, target_offering, "test-agent")

            mock_set_erred.sync_detailed.assert_called_once()
            call_kwargs = mock_set_erred.sync_detailed.call_args
            assert call_kwargs.kwargs["uuid"] == SOURCE_ORDER_UUID

    def test_handler_processes_canceled_state(self, source_offering, target_offering):
        """CANCELED -> finds source order, calls set_state_erred."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, OrderState.CANCELED.value)

        mock_source_order = MagicMock()
        mock_source_order.uuid = SOURCE_ORDER_UUID
        mock_source_order.backend_id = TARGET_ORDER_UUID

        with (
            patch(
                "waldur_site_agent_waldur.target_event_handler.get_client"
            ) as mock_get_client,
            patch(
                "waldur_site_agent_waldur.target_event_handler.marketplace_orders_list"
            ) as mock_orders_list,
            patch(
                "waldur_site_agent_waldur.target_event_handler."
                "marketplace_orders_set_state_erred"
            ) as mock_set_erred,
        ):
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_orders_list.sync_all.return_value = [mock_source_order]

            handler(frame, target_offering, "test-agent")

            mock_set_erred.sync_detailed.assert_called_once()

    def test_handler_no_matching_source_order(self, source_offering, target_offering):
        """No EXECUTING order with matching backend_id -> no-op."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, OrderState.DONE.value)

        with (
            patch(
                "waldur_site_agent_waldur.target_event_handler.get_client"
            ) as mock_get_client,
            patch(
                "waldur_site_agent_waldur.target_event_handler.marketplace_orders_list"
            ) as mock_orders_list,
            patch(
                "waldur_site_agent_waldur.target_event_handler."
                "marketplace_orders_set_state_done"
            ) as mock_set_done,
        ):
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            # No orders match
            mock_orders_list.sync_all.return_value = []

            handler(frame, target_offering, "test-agent")

            mock_set_done.sync_detailed.assert_not_called()

    def test_handler_skips_order_with_unset_backend_id(
        self, source_offering, target_offering
    ):
        """Source order with UNSET backend_id should not match."""
        handler = make_target_order_handler(source_offering)
        frame = _make_frame(TARGET_ORDER_UUID, OrderState.DONE.value)

        mock_source_order = MagicMock()
        mock_source_order.uuid = SOURCE_ORDER_UUID
        mock_source_order.backend_id = UNSET

        with (
            patch(
                "waldur_site_agent_waldur.target_event_handler.get_client"
            ) as mock_get_client,
            patch(
                "waldur_site_agent_waldur.target_event_handler.marketplace_orders_list"
            ) as mock_orders_list,
            patch(
                "waldur_site_agent_waldur.target_event_handler."
                "marketplace_orders_set_state_done"
            ) as mock_set_done,
        ):
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_orders_list.sync_all.return_value = [mock_source_order]

            handler(frame, target_offering, "test-agent")

            mock_set_done.sync_detailed.assert_not_called()

    def test_handler_ignores_empty_order_uuid(self, source_offering, target_offering):
        """Frame with empty order_uuid -> handler returns without action."""
        handler = make_target_order_handler(source_offering)
        body = json.dumps({"order_state": OrderState.DONE.value})
        frame = stomp.utils.Frame(cmd="MESSAGE", headers={}, body=body)

        with patch(
            "waldur_site_agent_waldur.target_event_handler.get_client"
        ) as mock_get_client:
            handler(frame, target_offering, "test-agent")
            mock_get_client.assert_not_called()


class TestSetupTargetEventSubscriptions:
    """Tests for WaldurBackend.setup_target_event_subscriptions."""

    def test_disabled_returns_empty(self, backend_settings, backend_components_passthrough):
        """target_stomp_enabled=False -> returns []."""
        backend_settings["target_stomp_enabled"] = False
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = MagicMock()

        source_offering = MagicMock()
        result = backend.setup_target_event_subscriptions(source_offering)
        assert result == []

    def test_not_set_returns_empty(self, backend_settings, backend_components_passthrough):
        """target_stomp_enabled not in settings -> returns []."""
        assert "target_stomp_enabled" not in backend_settings
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = MagicMock()

        source_offering = MagicMock()
        result = backend.setup_target_event_subscriptions(source_offering)
        assert result == []

    def test_enabled_calls_stomp_setup(
        self, backend_settings, backend_components_passthrough
    ):
        """target_stomp_enabled=True -> calls STOMP setup infrastructure."""
        backend_settings["target_stomp_enabled"] = True
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = MagicMock()

        source_offering = MagicMock()
        source_offering.name = "Source"
        source_offering.api_url = "https://waldur-a.example.com/api/"
        source_offering.api_token = "source-token"
        source_offering.uuid = OFFERING_UUID
        source_offering.verify_ssl = True
        source_offering.stomp_ws_host = None
        source_offering.stomp_ws_port = None
        source_offering.stomp_ws_path = None

        mock_consumer = (MagicMock(), MagicMock(), MagicMock())
        mock_listener = MagicMock()

        with (
            patch(
                "waldur_site_agent.event_processing.utils._register_agent_identity"
            ) as mock_register,
            patch(
                "waldur_site_agent.event_processing.utils._setup_single_stomp_subscription"
            ) as mock_setup,
            patch(
                "waldur_site_agent.common.utils.get_client"
            ),
            patch(
                "waldur_site_agent.common.agent_identity_management.AgentIdentityManager"
            ),
        ):
            mock_register.return_value = MagicMock()
            mock_setup.return_value = mock_consumer
            # Mock the connection.get_listener
            mock_consumer[0].get_listener.return_value = mock_listener

            result = backend.setup_target_event_subscriptions(source_offering)

            assert len(result) == 1
            mock_setup.assert_called_once()
            # Verify custom handler was set
            assert mock_listener.on_message_callback is not None

    def test_enabled_but_registration_fails(
        self, backend_settings, backend_components_passthrough
    ):
        """Agent identity registration failure -> returns []."""
        backend_settings["target_stomp_enabled"] = True
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = MagicMock()

        source_offering = MagicMock()
        source_offering.name = "Source"
        source_offering.stomp_ws_host = None
        source_offering.stomp_ws_port = None
        source_offering.stomp_ws_path = None

        with (
            patch(
                "waldur_site_agent.event_processing.utils._register_agent_identity"
            ) as mock_register,
            patch(
                "waldur_site_agent.common.utils.get_client"
            ),
            patch(
                "waldur_site_agent.common.agent_identity_management.AgentIdentityManager"
            ),
        ):
            mock_register.return_value = None  # Registration failed

            result = backend.setup_target_event_subscriptions(source_offering)
            assert result == []
