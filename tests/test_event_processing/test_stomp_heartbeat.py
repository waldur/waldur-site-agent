"""Tests verifying STOMP heartbeat configuration.

Regression tests for a bug where the WSStompConnection was created with
heartbeats=(0, 0) (the default), but the CONNECT frame manually declared
"heart-beat: 10000,10000".  This mismatch caused stomp.py's
HeartbeatListener to never start its heartbeat thread (because the
constructor value was (0, 0)), while RabbitMQ — having seen the CONNECT
header — expected heartbeats every 10 s.  After ~30 s of silence
(3 missed intervals) RabbitMQ closed the connection, triggering a
reconnect cycle that repeated indefinitely.

The fix: pass heartbeats=(10000, 10000) to the WSStompConnection
constructor so that stomp.py's heartbeat negotiation and sending loop
actually engage.
"""

import unittest
import uuid
from unittest import mock

from stomp.utils import calculate_heartbeats

from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing.event_subscription_manager import (
    EventSubscriptionManager,
)


class TestHeartbeatNegotiation(unittest.TestCase):
    """Show how stomp.py's calculate_heartbeats works with (0,0) vs (10000,10000)."""

    def test_zero_client_heartbeats_disables_heartbeats(self):
        """With client heartbeats=(0,0), negotiation always yields (0,0).

        This was the buggy behaviour: stomp.py never started its
        heartbeat thread regardless of what the server offered.
        """
        server_heartbeat = ("10000", "10000")
        client_heartbeat = (0, 0)

        result = calculate_heartbeats(server_heartbeat, client_heartbeat)

        self.assertEqual(result, (0, 0))

    def test_nonzero_client_heartbeats_enables_heartbeats(self):
        """With client heartbeats=(10000,10000), negotiation yields proper values.

        This is the correct behaviour: stomp.py will start the heartbeat
        thread using the negotiated intervals.
        """
        server_heartbeat = ("10000", "10000")
        client_heartbeat = (10000, 10000)

        result = calculate_heartbeats(server_heartbeat, client_heartbeat)

        self.assertEqual(result, (10000, 10000))

    def test_server_with_larger_interval(self):
        """Negotiation picks the maximum of client and server values."""
        server_heartbeat = ("30000", "30000")
        client_heartbeat = (10000, 10000)

        result = calculate_heartbeats(server_heartbeat, client_heartbeat)

        self.assertEqual(result, (30000, 30000))


class TestSetupStompConnectionHeartbeats(unittest.TestCase):
    """Verify that setup_stomp_connection passes heartbeats to WSStompConnection."""

    def setUp(self):
        self.offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-offering-uuid",
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
            order_processing_backend="slurm",
            stomp_ws_host="stomp.example.com",
            stomp_ws_port=15674,
            stomp_ws_path="/ws",
        )

        self.event_subscription = mock.Mock()
        self.event_subscription.uuid = uuid.uuid4()
        self.event_subscription.user_uuid = uuid.uuid4()

    @mock.patch("waldur_site_agent.event_processing.event_subscription_manager.stomp.WSStompConnection")
    def test_heartbeats_passed_to_connection_constructor(self, mock_ws_conn_class):
        """WSStompConnection must be created with heartbeats=(10000,10000).

        If heartbeats is (0,0) or missing, the HeartbeatListener never
        starts sending heartbeats and RabbitMQ disconnects every ~30s.
        """
        mock_connection = mock_ws_conn_class.return_value
        mock_connection.transport = mock.Mock()

        manager = EventSubscriptionManager(
            offering=self.offering,
            observable_object_type="order",
        )

        manager.setup_stomp_connection(
            self.event_subscription,
            self.offering.stomp_ws_host,
            self.offering.stomp_ws_port,
            self.offering.stomp_ws_path,
        )

        mock_ws_conn_class.assert_called_once()
        call_kwargs = mock_ws_conn_class.call_args
        # Check keyword argument
        if call_kwargs.kwargs.get("heartbeats"):
            heartbeats = call_kwargs.kwargs["heartbeats"]
        else:
            # Might be passed positionally; find it in the call
            heartbeats = call_kwargs.kwargs.get("heartbeats", (0, 0))

        self.assertNotEqual(
            heartbeats,
            (0, 0),
            "WSStompConnection must be created with non-zero heartbeats. "
            "heartbeats=(0,0) prevents stomp.py from sending heartbeats, "
            "causing RabbitMQ to disconnect every ~30s.",
        )
        self.assertEqual(heartbeats, (10000, 10000))
