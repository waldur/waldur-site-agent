import json
import unittest
from unittest import mock
from uuid import UUID

from waldur_site_agent.event_processing import handlers


@mock.patch(
    "waldur_site_agent.common.agent_identity_management.marketplace_site_agent_identities_register_service"
)
@mock.patch(
    "waldur_site_agent.common.agent_identity_management.marketplace_site_agent_identities_list"
)
@mock.patch(
    "waldur_site_agent.common.agent_identity_management.marketplace_site_agent_identities_create"
)
@mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
@mock.patch("waldur_site_agent.event_processing.handlers.common_processors.OfferingOrderProcessor")
class TestOrderStateFiltering(unittest.TestCase):
    """Test order state filtering in message handlers."""

    def setUp(self):
        """Set up test fixtures."""
        self.offering = mock.Mock()
        self.offering.api_url = "https://example.com/api"
        self.offering.api_token = "test-token"
        self.offering.verify_ssl = True
        self.offering.uuid = "test-offering-uuid"
        self.offering.name = "test-offering"
        self.user_agent = "test-agent"
        self.order_uuid = "test-order-uuid"

        # Create mock identity and service for AgentIdentityManager
        self.mock_identity = mock.Mock()
        self.mock_identity.uuid = UUID("12345678-1234-5678-1234-567812345678")
        self.mock_identity.name = f"agent-{self.offering.uuid}"

        self.mock_service = mock.Mock()
        self.mock_service.uuid = UUID("87654321-4321-8765-4321-876543218765")
        self.mock_service.name = "event_process"

    def test_mqtt_handler_skips_done_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that MQTT handler skips orders in 'done' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "done"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        # Verify no processing occurred
        mock_processor_class.assert_not_called()
        mock_get_client.assert_not_called()
        mock_identity_create.sync.assert_not_called()
        mock_identity_list.sync.assert_not_called()
        mock_service_register.sync.assert_not_called()

    def test_mqtt_handler_skips_erred_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that MQTT handler skips orders in 'erred' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "erred"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        # Verify no processing occurred
        mock_processor_class.assert_not_called()
        mock_get_client.assert_not_called()
        mock_identity_create.sync.assert_not_called()
        mock_identity_list.sync.assert_not_called()
        mock_service_register.sync.assert_not_called()

    def test_mqtt_handler_processes_pending_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that MQTT handler processes orders in 'pending-provider' state."""
        # Configure mocks for agent identity management
        mock_identity_list.sync.return_value = []  # No existing identity
        mock_identity_create.sync.return_value = self.mock_identity
        mock_service_register.sync.return_value = self.mock_service

        message = {"order_uuid": self.order_uuid, "order_state": "pending-provider"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        # Verify Waldur client was created
        mock_get_client.assert_called_once_with(
            self.offering.api_url,
            self.offering.api_token,
            self.user_agent,
            self.offering.verify_ssl,
        )

        # Verify agent identity registration flow
        mock_identity_list.sync.assert_called_once()
        mock_identity_create.sync.assert_called_once()
        mock_service_register.sync.assert_called_once()

        # Verify order processing
        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)

    def test_stomp_handler_skips_done_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that STOMP handler skips orders in 'done' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "done"}

        mock_frame = mock.Mock()
        mock_frame.body = json.dumps(message)

        handlers.on_order_message_stomp(mock_frame, self.offering, self.user_agent)

        # Verify no processing occurred
        mock_processor_class.assert_not_called()
        mock_get_client.assert_not_called()
        mock_identity_create.sync.assert_not_called()
        mock_identity_list.sync.assert_not_called()
        mock_service_register.sync.assert_not_called()

    def test_stomp_handler_processes_executing_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that STOMP handler processes orders in 'executing' state."""
        # Configure mocks for agent identity management
        mock_identity_list.sync.return_value = []  # No existing identity
        mock_identity_create.sync.return_value = self.mock_identity
        mock_service_register.sync.return_value = self.mock_service

        message = {"order_uuid": self.order_uuid, "order_state": "executing"}

        mock_frame = mock.Mock()
        mock_frame.body = json.dumps(message)

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_stomp(mock_frame, self.offering, self.user_agent)

        # Verify Waldur client was created
        mock_get_client.assert_called_once_with(
            self.offering.api_url,
            self.offering.api_token,
            self.user_agent,
            self.offering.verify_ssl,
        )

        # Verify agent identity registration flow
        mock_identity_list.sync.assert_called_once()
        mock_identity_create.sync.assert_called_once()
        mock_service_register.sync.assert_called_once()

        # Verify order processing
        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)

    def test_handler_processes_valid_orders(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that handlers process orders with valid states."""
        # Configure mocks for agent identity management
        mock_identity_list.sync.return_value = []  # No existing identity
        mock_identity_create.sync.return_value = self.mock_identity
        mock_service_register.sync.return_value = self.mock_service

        message = {"order_uuid": self.order_uuid, "order_state": "pending-provider"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        # Verify Waldur client was created
        mock_get_client.assert_called_once_with(
            self.offering.api_url,
            self.offering.api_token,
            self.user_agent,
            self.offering.verify_ssl,
        )

        # Verify agent identity registration flow
        mock_identity_list.sync.assert_called_once()
        mock_identity_create.sync.assert_called_once()
        mock_service_register.sync.assert_called_once()

        # Verify order processing
        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)

    def test_mqtt_handler_reuses_existing_identity(
        self,
        mock_processor_class,
        mock_get_client,
        mock_identity_create,
        mock_identity_list,
        mock_service_register,
    ):
        """Test that MQTT handler reuses existing identity without creating a new one."""
        # Configure mocks to return existing identity
        mock_identity_list.sync.return_value = [self.mock_identity]  # Existing identity found
        mock_service_register.sync.return_value = self.mock_service

        message = {"order_uuid": self.order_uuid, "order_state": "pending-provider"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        # Verify Waldur client was created
        mock_get_client.assert_called_once_with(
            self.offering.api_url,
            self.offering.api_token,
            self.user_agent,
            self.offering.verify_ssl,
        )

        # Verify agent identity was searched
        mock_identity_list.sync.assert_called_once()

        # Verify that identity creation was SKIPPED (existing identity reused)
        mock_identity_create.sync.assert_not_called()

        # Verify service was still registered
        mock_service_register.sync.assert_called_once()

        # Verify order processing
        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)
