"""Tests for payload-based routing of unified-queue messages."""

import json
import unittest
from unittest import mock

from waldur_api_client.models import ObservableObjectTypeEnum

from waldur_site_agent.event_processing import event_subscription_manager as esm


def _frame(body):
    frame = mock.Mock()
    frame.body = body if isinstance(body, str) else json.dumps(body)
    frame.headers = {"destination": "/amq/queue/consumer_abc"}
    return frame


class TestRouteMessage(unittest.TestCase):
    def setUp(self):
        self.offering = mock.Mock()
        self.handlers = {
            ObservableObjectTypeEnum.ORDER: mock.Mock(),
            ObservableObjectTypeEnum.RESOURCE: mock.Mock(),
        }
        patcher = mock.patch.dict(
            esm.OBJECT_TYPE_TO_HANDLER_STOMP, self.handlers, clear=True
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_dispatches_to_handler_for_payload_object_type(self):
        frame = _frame({"object_type": "order", "order_uuid": "x"})

        esm.route_message(frame, self.offering, "agent/1", expose_backend_error_details=False)

        self.handlers[ObservableObjectTypeEnum.ORDER].assert_called_once_with(
            frame, self.offering, "agent/1", False
        )
        self.handlers[ObservableObjectTypeEnum.RESOURCE].assert_not_called()

    def test_drops_non_json_body(self):
        esm.route_message(_frame("not json"), self.offering, "agent/1")

        for handler in self.handlers.values():
            handler.assert_not_called()

    def test_drops_message_without_object_type(self):
        esm.route_message(_frame({"order_uuid": "x"}), self.offering, "agent/1")

        for handler in self.handlers.values():
            handler.assert_not_called()

    def test_drops_unknown_object_type(self):
        esm.route_message(
            _frame({"object_type": "not_a_real_type"}), self.offering, "agent/1"
        )

        for handler in self.handlers.values():
            handler.assert_not_called()

    def test_drops_known_type_without_registered_handler(self):
        # Valid enum member, but no handler in the (patched) routing table.
        esm.route_message(
            _frame({"object_type": ObservableObjectTypeEnum.USER_ROLE.value}),
            self.offering,
            "agent/1",
        )

        for handler in self.handlers.values():
            handler.assert_not_called()

    def test_default_manager_callback_is_route_message(self):
        manager = esm.EventSubscriptionManager(self.offering)
        self.assertIs(manager.on_message_callback, esm.route_message)

    def test_custom_router_overrides_default(self):
        custom = mock.Mock()
        manager = esm.EventSubscriptionManager(self.offering, on_message_callback=custom)
        self.assertIs(manager.on_message_callback, custom)
