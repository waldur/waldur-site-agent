import json
import unittest
from unittest import mock

from waldur_site_agent.event_processing import handlers


@mock.patch("waldur_site_agent.event_processing.handlers.common_processors.OfferingOrderProcessor")
class TestOrderStateFiltering(unittest.TestCase):
    """Test order state filtering in message handlers."""

    def setUp(self):
        """Set up test fixtures."""
        self.offering = mock.Mock()
        self.user_agent = "test-agent"
        self.order_uuid = "test-order-uuid"

    def test_mqtt_handler_skips_done_orders(self, mock_processor_class):
        """Test that MQTT handler skips orders in 'done' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "done"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        mock_processor_class.assert_not_called()

    def test_mqtt_handler_skips_erred_orders(self, mock_processor_class):
        """Test that MQTT handler skips orders in 'erred' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "erred"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        mock_processor_class.assert_not_called()

    def test_mqtt_handler_processes_pending_orders(self, mock_processor_class):
        """Test that MQTT handler processes orders in 'pending-provider' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "pending-provider"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)

    def test_stomp_handler_skips_done_orders(self, mock_processor_class):
        """Test that STOMP handler skips orders in 'done' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "done"}

        mock_frame = mock.Mock()
        mock_frame.body = json.dumps(message)

        handlers.on_order_message_stomp(mock_frame, self.offering, self.user_agent)

        mock_processor_class.assert_not_called()

    def test_stomp_handler_processes_executing_orders(self, mock_processor_class):
        """Test that STOMP handler processes orders in 'executing' state."""
        message = {"order_uuid": self.order_uuid, "order_state": "executing"}

        mock_frame = mock.Mock()
        mock_frame.body = json.dumps(message)

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_stomp(mock_frame, self.offering, self.user_agent)

        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)

    def test_handler_processes_valid_orders(self, mock_processor_class):
        """Test that handlers process orders with valid states."""
        message = {"order_uuid": self.order_uuid, "order_state": "pending-provider"}

        mock_msg = mock.Mock()
        mock_msg.payload = json.dumps(message).encode("utf-8")
        mock_msg.topic = "test/topic"

        userdata = {"offering": self.offering, "user_agent": self.user_agent}

        mock_processor = mock_processor_class.return_value
        mock_order = mock.Mock()
        mock_processor.get_order_info.return_value = mock_order

        handlers.on_order_message_mqtt(mock.Mock(), userdata, mock_msg)

        mock_processor.get_order_info.assert_called_once_with(self.order_uuid)
        mock_processor.process_order_with_retries.assert_called_once_with(mock_order)
