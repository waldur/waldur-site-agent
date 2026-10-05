"""Bounded STOMP setup and what the watchdog counts as unhealthy.

Every connect the agent makes outside a listener's own reconnect must be bounded:
an unbounded connect at startup or from the watchdog blocks the main loop (and
with it reconciliation and the heartbeat) for as long as the broker is down.
Only what a restart can fix may withhold the liveness heartbeat.
"""

import contextlib
import threading
import unittest
import uuid
from unittest import mock

import httpx
from stomp.exception import ConnectFailedException
from waldur_api_client.errors import UnexpectedStatus

from waldur_site_agent.common import WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing import event_subscription_manager as esm
from waldur_site_agent.event_processing import listener as listener_module
from waldur_site_agent.event_processing import utils as event_utils
from waldur_site_agent.event_processing import watchdog as watchdog_module
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.listener import (
    BACKOFF_INITIAL,
    BACKOFF_JITTER,
    BACKOFF_MAX,
    RECONNECT_MAX_RETRIES,
    WaldurListener,
    connect_to_stomp_server,
)
from waldur_site_agent.event_processing.watchdog import StompWatchdog

TICK = 60.0
THRESHOLD = 300.0


def _offering(name="offering-1", offering_uuid="11111111111111111111111111111111"):
    return common_structures.Offering(
        name=name,
        waldur_offering_uuid=offering_uuid,
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="token",
        backend_type="slurm",
        order_processing_backend="slurm",
        membership_sync_backend="slurm",
        stomp_enabled=True,
        stomp_ws_host="localhost",
        stomp_ws_port=15674,
        stomp_ws_path="/ws",
    )


class _FakeConnection:
    """Stands in for WSStompConnection; ``connected`` is flipped by the fake connect."""

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.connected = False
        self.listeners = {}
        self.transport = mock.Mock()

    def is_connected(self):
        return self.connected

    def set_listener(self, name, listener):
        self.listeners[name] = listener

    def get_listener(self, name):
        return self.listeners.get(name)

    def set_ssl(self, *args, **kwargs):
        pass


class _Stack:
    """Patches REST registration and the STOMP layer below the agent's own code."""

    def __init__(self, broker_up=False):
        self.broker_up = broker_up
        self.connect_calls = []
        self.connections = []
        self.manager = mock.Mock()
        self.manager.register_identity.return_value = mock.Mock(uuid=uuid.uuid4())
        self.manager.register_queue.side_effect = self._register_queue

    def _register_queue(self, identity, object_types):
        return common_structures.UnifiedQueue(
            queue_name="consumer_q", rmq_username="rmq-user", vhost="vhost"
        )

    def connect(self, connection, username, password, max_retries=0, **kwargs):
        self.connect_calls.append({"max_retries": max_retries, **kwargs})
        if not self.broker_up:
            raise ConnectFailedException("broker down")
        connection.connected = True

    def make_connection(self, *args, **kwargs):
        connection = _FakeConnection(*args, **kwargs)
        self.connections.append(connection)
        return connection

    @contextlib.contextmanager
    def active(self):
        rest = mock.Mock(_verify_ssl=False)
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(event_utils, "get_client_for_offering", return_value=rest)
            )
            stack.enter_context(
                mock.patch.object(esm.utils, "get_client_for_offering", return_value=rest)
            )
            stack.enter_context(
                mock.patch.object(
                    event_utils.agent_identity_management,
                    "AgentIdentityManager",
                    return_value=self.manager,
                )
            )
            stack.enter_context(
                mock.patch.object(esm.stomp, "WSStompConnection", side_effect=self.make_connection)
            )
            stack.enter_context(
                mock.patch.object(esm, "connect_to_stomp_server", side_effect=self.connect)
            )
            stack.enter_context(
                mock.patch.object(
                    listener_module, "connect_to_stomp_server", side_effect=self.connect
                )
            )
            stack.enter_context(
                mock.patch.object(
                    event_utils,
                    "offering_expects_target_consumers",
                    return_value=False,
                    create=True,
                )
            )
            yield self


def _watchdog(consumers_map, offerings):
    return StompWatchdog(consumers_map, offerings, "test-agent", unhealthy_after=THRESHOLD)


# ---------------------------------------------------------------------------
# 1. The watchdog's setup retry must not block on a down broker
# ---------------------------------------------------------------------------


class TestWatchdogSetupIsBounded(unittest.TestCase):
    def test_setup_retry_connects_once_and_keeps_the_consumer(self):
        offering = _offering()
        consumers_map = {}
        with _Stack(broker_up=False).active() as stack:
            watchdog = _watchdog(consumers_map, [offering])
            watchdog.check(1000.0)

            self.assertTrue(stack.connect_calls)
            self.assertTrue(all(c["max_retries"] == 1 for c in stack.connect_calls))
            # Registered even though the broker refused: later ticks only reconnect.
            self.assertIn((offering.name, offering.uuid), consumers_map)

            watchdog.check(1000.0 + TICK)
            watchdog.check(1000.0 + 2 * TICK)

        stack.manager.register_queue.assert_called_once()
        self.assertGreaterEqual(len(stack.connect_calls), 3)

    def test_recovered_broker_is_picked_up_by_reconnect(self):
        offering = _offering()
        consumers_map = {}
        stack = _Stack(broker_up=False)
        with stack.active():
            watchdog = _watchdog(consumers_map, [offering])
            watchdog.check(1000.0)
            stack.broker_up = True
            watchdog.check(1000.0 + TICK)

        connection = consumers_map[(offering.name, offering.uuid)][0][0]
        self.assertTrue(connection.is_connected())


# ---------------------------------------------------------------------------
# 2. Startup is bounded; only restart-fixable failures withhold liveness
# ---------------------------------------------------------------------------


class TestStartupIsBounded(unittest.TestCase):
    def test_startup_connect_is_bounded_and_consumer_kept(self):
        offering = _offering()
        with _Stack(broker_up=False).active() as stack:
            consumers_map = event_utils.start_stomp_consumers([offering], "test-agent")

        self.assertTrue(stack.connect_calls)
        for call in stack.connect_calls:
            self.assertGreater(call["max_retries"], 0)
        self.assertIn((offering.name, offering.uuid), consumers_map)


class TestOnlyRestartFixableFailuresWithholdLiveness(unittest.TestCase):
    def _failing_registration(self, status_code):
        return UnexpectedStatus(status_code, b"refused", httpx.URL("https://waldur/api/"))

    def test_persistent_registration_refusal_keeps_liveness(self):
        offering = _offering()
        stack = _Stack(broker_up=True)
        stack.manager.register_queue.side_effect = self._failing_registration(409)
        with stack.active(), mock.patch.object(watchdog_module, "logger") as log:
            watchdog = _watchdog({}, [offering])
            results = [watchdog.check(1000.0 + TICK * i) for i in range(30)]

        self.assertTrue(all(results))
        self.assertTrue(log.error.called)

    def test_transient_registration_failure_withholds_liveness(self):
        offering = _offering()
        stack = _Stack(broker_up=True)
        stack.manager.register_queue.side_effect = self._failing_registration(503)
        with stack.active():
            watchdog = _watchdog({}, [offering])
            results = [watchdog.check(1000.0 + TICK * i) for i in range(30)]

        self.assertTrue(results[0])
        self.assertFalse(results[-1])

    def test_down_federation_target_keeps_liveness(self):
        offering = _offering()
        target_offering = _offering("Target: offering-1", "22222222222222222222222222222222")
        main = _FakeConnection()
        main.connected = True
        target = _FakeConnection()
        target.set_listener(
            WALDUR_LISTENER_NAME,
            WaldurListener(target, "q", "u", "p", mock.Mock(), target_offering, "ua"),
        )
        queue = mock.Mock(queue_name="q")
        consumers_map = {
            (offering.name, offering.uuid): [
                (main, queue, offering),
                (target, queue, target_offering),
            ]
        }
        with _Stack(broker_up=False).active(), mock.patch.object(
            watchdog_module, "logger"
        ) as log:
            watchdog = _watchdog(consumers_map, [offering])
            results = [watchdog.check(1000.0 + TICK * i) for i in range(30)]

        self.assertTrue(all(results))
        self.assertTrue(log.error.called)


# ---------------------------------------------------------------------------
# 3. A flapping connection is not "recovered"; a lost queue is re-registered
# ---------------------------------------------------------------------------


class TestFlappingConnection(unittest.TestCase):
    def test_connection_that_drops_between_ticks_stays_down(self):
        offering = _offering()
        connection = _FakeConnection()
        connection.set_listener(
            WALDUR_LISTENER_NAME,
            WaldurListener(connection, "q", "u", "p", mock.Mock(), offering, "ua"),
        )
        consumers_map = {
            (offering.name, offering.uuid): [(connection, mock.Mock(queue_name="q"), offering)]
        }
        with _Stack(broker_up=True).active():
            watchdog = _watchdog(consumers_map, [offering])
            results = []
            for i in range(30):
                # The server closes right after CONNECTED (SUBSCRIBE NOT_FOUND).
                connection.connected = False
                results.append(watchdog.check(1000.0 + TICK * i))

        self.assertFalse(results[-1])


class TestQueueNotFound(unittest.TestCase):
    def test_not_found_error_reregisters_the_queue(self):
        reregistered = threading.Event()
        listener = WaldurListener(
            mock.Mock(),
            "consumer_q",
            "u",
            "p",
            mock.Mock(),
            mock.Mock(),
            "ua",
            on_queue_missing=reregistered.set,
        )
        frame = mock.Mock(
            headers={"message": "NOT_FOUND - no queue 'consumer_q' in vhost 'v'"}, body=""
        )

        listener.on_error(frame)

        self.assertTrue(reregistered.wait(timeout=2))

    def test_other_errors_do_not_reregister(self):
        callback = mock.Mock()
        listener = WaldurListener(
            mock.Mock(), "q", "u", "p", mock.Mock(), mock.Mock(), "ua", on_queue_missing=callback
        )

        listener.on_error(mock.Mock(headers={"message": "ACCESS_REFUSED"}, body=""))

        callback.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Each expected consumer is retried on its own
# ---------------------------------------------------------------------------


class TestPartialSetup(unittest.TestCase):
    def test_missing_target_is_retried_while_main_is_present(self):
        offering = _offering()
        main = _FakeConnection()
        main.connected = True
        target = (_FakeConnection(), mock.Mock(queue_name="t"), _offering("T", "2" * 32))
        consumers_map = {(offering.name, offering.uuid): [(main, mock.Mock(), offering)]}
        with _Stack().active(), mock.patch.object(
            event_utils, "offering_expects_target_consumers", return_value=True
        ), mock.patch.object(
            event_utils, "setup_offering_target_consumers", return_value=[target]
        ) as setup_targets:
            _watchdog(consumers_map, [offering]).check(1000.0)

        setup_targets.assert_called_once()
        self.assertIn(target, consumers_map[(offering.name, offering.uuid)])

    def test_missing_main_is_retried_while_target_is_present(self):
        offering = _offering()
        target_offering = _offering("Target", "2" * 32)
        target = _FakeConnection()
        target.connected = True
        consumers_map = {
            (offering.name, offering.uuid): [(target, mock.Mock(), target_offering)]
        }
        with _Stack(broker_up=True).active() as stack:
            _watchdog(consumers_map, [offering]).check(1000.0)

        stack.manager.register_queue.assert_called_once()
        offerings_in_map = [c[2] for c in consumers_map[(offering.name, offering.uuid)]]
        self.assertIn(offering, offerings_in_map)


# ---------------------------------------------------------------------------
# 5. Connects are bounded in time, not only in attempts
# ---------------------------------------------------------------------------


class TestConnectTimeout(unittest.TestCase):
    def test_connect_without_connected_frame_times_out(self):
        connection = mock.Mock()
        connection.is_connected.return_value = False
        connection.transport.connection_error = False
        clock = {"now": 0.0}

        def fake_sleep(seconds):
            clock["now"] += seconds

        with mock.patch.object(listener_module.time, "monotonic", lambda: clock["now"]), \
                mock.patch.object(listener_module.time, "sleep", side_effect=fake_sleep):
            with self.assertRaises(ConnectFailedException):
                connect_to_stomp_server(
                    connection, "u", "p", max_retries=1, connect_timeout=5.0
                )

        self.assertLess(clock["now"], 60.0)

    @mock.patch.object(esm.stomp, "WSStompConnection")
    def test_websocket_connect_has_a_socket_timeout(self, ws_connection):
        ws_connection.return_value.transport = mock.Mock()
        manager = esm.EventSubscriptionManager(_offering())
        queue = common_structures.UnifiedQueue(queue_name="q", rmq_username="u", vhost="v")

        manager.setup_stomp_connection(queue, "localhost", 15674, "/ws")

        self.assertTrue(ws_connection.call_args.kwargs.get("timeout"))

    @mock.patch.object(listener_module, "connect_to_stomp_server")
    def test_watchdog_reconnect_passes_a_connect_timeout(self, connect):
        listener = WaldurListener(mock.Mock(), "q", "u", "p", mock.Mock(), mock.Mock(), "ua")

        listener.reconnect(max_retries=1)

        self.assertTrue(connect.call_args.kwargs.get("connect_timeout"))


# ---------------------------------------------------------------------------
# 6. Quieter logs and a threshold beyond the listener's own retry window
# ---------------------------------------------------------------------------


class TestNoiseAndDefaults(unittest.TestCase):
    def test_no_warning_while_listener_is_already_reconnecting(self):
        offering = _offering()
        connection = _FakeConnection()
        listener = WaldurListener(connection, "q", "u", "p", mock.Mock(), offering, "ua")
        connection.set_listener(WALDUR_LISTENER_NAME, listener)
        consumers_map = {(offering.name, offering.uuid): [(connection, mock.Mock(), offering)]}

        listener._reconnect_lock.acquire()
        try:
            with _Stack().active(), mock.patch.object(watchdog_module, "logger") as log:
                _watchdog(consumers_map, [offering]).check(1000.0)
        finally:
            listener._reconnect_lock.release()

        log.warning.assert_not_called()

    def test_default_threshold_exceeds_listener_retry_window(self):
        window = sum(
            min(BACKOFF_INITIAL * 2**attempt, BACKOFF_MAX) * (1 + BACKOFF_JITTER)
            for attempt in range(RECONNECT_MAX_RETRIES)
        )
        self.assertGreater(WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES * 60, window)
