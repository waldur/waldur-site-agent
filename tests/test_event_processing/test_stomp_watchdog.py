"""Event mode must recover dropped STOMP consumers and stop looking healthy while it cannot.

A consumer whose own reconnect gave up (``RECONNECT_MAX_RETRIES``), or an offering
whose STOMP setup failed at startup, used to stay dead for the life of the process
while the main loop kept touching the liveness heartbeat. These tests drive the
real ``event_processing.main.start`` loop with mocked periodic work.
"""

import contextlib
import unittest
from unittest import mock

import stomp
from stomp.exception import ConnectFailedException

from waldur_site_agent.event_processing import main as event_main
from waldur_site_agent.event_processing import utils as event_utils
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.listener import WaldurListener

TICK = 60.0


class _Stop(BaseException):
    """Ends the infinite tick loop without being caught by ``except Exception``."""


def _offering(name="offering-1", uuid="offering-uuid-1"):
    offering = mock.Mock()
    offering.name = name
    offering.uuid = uuid
    offering.stomp_enabled = True
    offering.username_reconciliation_enabled = False
    return offering


def _config(offerings):
    config = mock.Mock()
    config.waldur_offerings = offerings
    config.waldur_user_agent = "test-agent"
    config.global_proxy = ""
    config.expose_backend_error_details = True
    return config


def _consumer(offering, connected):
    """A consumer whose connection carries a real WaldurListener."""
    conn = mock.Mock(spec=stomp.WSStompConnection)
    conn.is_connected.return_value = connected
    listener = WaldurListener(
        conn, "consumer_q", "rmq-user", "rmq-pass", mock.Mock(), offering, "test-agent"
    )
    conn.get_listener.side_effect = lambda name: listener if name == WALDUR_LISTENER_NAME else None
    unified_queue = mock.Mock()
    unified_queue.queue_name = "consumer_q"
    return conn, unified_queue, offering


class _LoopTestCase(unittest.TestCase):
    """Runs ``start()`` for a fixed number of ticks."""

    def setUp(self):
        self._patches = [
            mock.patch.object(event_utils, "run_initial_offering_processing"),
            mock.patch.object(event_utils, "send_agent_health_checks"),
            mock.patch.object(event_utils, "run_periodic_order_reconciliation"),
            mock.patch.object(event_utils, "run_periodic_api_key_reconciliation"),
            mock.patch.object(event_utils, "run_periodic_offering_user_reconciliation"),
            mock.patch.object(event_utils, "run_periodic_project_hierarchy_sync"),
            mock.patch.object(event_utils, "run_periodic_username_reconciliation"),
            mock.patch.object(event_main.common_utils, "setup_log_shippers"),
            mock.patch.object(event_main.common_utils, "teardown_log_shippers"),
            mock.patch.object(
                event_utils, "offering_expects_target_consumers", return_value=False
            ),
        ]
        for patcher in self._patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_ticks(self, config, consumers_map, ticks):
        """Run the loop for ``ticks`` iterations; return the 1-based ticks that touched."""
        times = [1000.0 + TICK * i for i in range(ticks)]
        touched = []
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    event_utils, "start_stomp_consumers", return_value=consumers_map
                )
            )
            touch = stack.enter_context(mock.patch.object(event_main, "touch_heartbeat"))
            fake_time = stack.enter_context(mock.patch.object(event_main, "time"))
            fake_time.time.side_effect = times
            fake_time.sleep.side_effect = [None] * (ticks - 1) + [_Stop()]
            touch.side_effect = lambda: touched.append(fake_time.sleep.call_count + 1)
            with self.assertRaises(_Stop):
                event_main.start(config)
        return touched


class TestReconnectAfterListenerGaveUp(_LoopTestCase):
    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_tick_reconnects_a_consumer_left_disconnected(self, mock_connect):
        offering = _offering()
        conn, queue, _ = _consumer(offering, connected=False)
        consumers_map = {(offering.name, offering.uuid): [(conn, queue, offering)]}

        self.run_ticks(_config([offering]), consumers_map, ticks=2)

        mock_connect.assert_called()
        args = mock_connect.call_args[0]
        self.assertIs(args[0], conn)
        self.assertEqual(args[1:3], ("rmq-user", "rmq-pass"))

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_connected_consumer_is_left_alone(self, mock_connect):
        offering = _offering()
        conn, queue, _ = _consumer(offering, connected=True)
        consumers_map = {(offering.name, offering.uuid): [(conn, queue, offering)]}

        touched = self.run_ticks(_config([offering]), consumers_map, ticks=3)

        mock_connect.assert_not_called()
        self.assertEqual(touched, [1, 2, 3])

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_tick_does_not_race_a_reconnect_in_progress(self, mock_connect):
        offering = _offering()
        conn, queue, _ = _consumer(offering, connected=False)
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        consumers_map = {(offering.name, offering.uuid): [(conn, queue, offering)]}

        listener._reconnect_lock.acquire()
        try:
            self.run_ticks(_config([offering]), consumers_map, ticks=2)
        finally:
            listener._reconnect_lock.release()

        mock_connect.assert_not_called()


class TestRetryOfferingWhoseSetupFailed(_LoopTestCase):
    @mock.patch.object(event_utils, "_determine_observable_object_types", return_value=["order"])
    def test_offering_missing_from_map_is_set_up_again(self, _types):
        offering = _offering()
        recovered = _consumer(offering, connected=True)

        with mock.patch.object(
            event_utils, "open_offering_consumer", return_value=recovered
        ) as mock_setup:
            consumers_map = {}
            self.run_ticks(_config([offering]), consumers_map, ticks=2)

        mock_setup.assert_called()
        self.assertIs(mock_setup.call_args[0][0], offering)
        self.assertEqual(consumers_map[(offering.name, offering.uuid)], [recovered])

    @mock.patch.object(event_utils, "_determine_observable_object_types", return_value=[])
    def test_offering_without_observable_types_is_not_retried(self, _types):
        offering = _offering()

        with mock.patch.object(event_utils, "open_offering_consumer") as mock_setup:
            touched = self.run_ticks(_config([offering]), {}, ticks=20)

        mock_setup.assert_not_called()
        self.assertEqual(touched, list(range(1, 21)))


class TestLivenessWhileStompIsDown(_LoopTestCase):
    @mock.patch(
        "waldur_site_agent.event_processing.listener.connect_to_stomp_server",
        side_effect=ConnectFailedException("broker down"),
    )
    def test_heartbeat_stops_after_consumer_stays_down(self, _connect):
        offering = _offering()
        conn, queue, _ = _consumer(offering, connected=False)
        consumers_map = {(offering.name, offering.uuid): [(conn, queue, offering)]}

        touched = self.run_ticks(_config([offering]), consumers_map, ticks=30)

        # Touched while the outage is young, withheld once it outlasts the threshold.
        self.assertIn(1, touched)
        self.assertNotIn(30, touched)

    @mock.patch.object(event_utils, "_determine_observable_object_types", return_value=["order"])
    def test_heartbeat_stops_while_offering_setup_keeps_failing(self, _types):
        offering = _offering()

        with mock.patch.object(event_utils, "open_offering_consumer", return_value=None):
            touched = self.run_ticks(_config([offering]), {}, ticks=30)

        self.assertIn(1, touched)
        self.assertNotIn(30, touched)

    def test_heartbeat_resumes_once_consumer_reconnects(self):
        offering = _offering()
        conn, queue, _ = _consumer(offering, connected=False)
        consumers_map = {(offering.name, offering.uuid): [(conn, queue, offering)]}
        attempts = {"n": 0}

        def connect(connection, *_args, **_kwargs):
            attempts["n"] += 1
            if attempts["n"] < 25:
                raise ConnectFailedException("broker down")
            connection.is_connected.return_value = True

        with mock.patch(
            "waldur_site_agent.event_processing.listener.connect_to_stomp_server",
            side_effect=connect,
        ):
            touched = self.run_ticks(_config([offering]), consumers_map, ticks=30)

        self.assertTrue(conn.is_connected())
        missing = sorted(set(range(1, 31)) - set(touched))
        # Withheld during the long outage, touched again from the tick it recovered.
        self.assertTrue(missing)
        self.assertIn(30, touched)
        self.assertLess(max(missing), 30)
