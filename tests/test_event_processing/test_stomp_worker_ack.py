"""STOMP messages are handled off the receiver thread and acked after handling.

A handler that runs on stomp.py's receiver thread stops it reading frames and
heartbeats; once it outlives the heartbeat window the connection is dropped, and
with ``ack="auto"`` every message already written to the socket is lost with it.
"""

import threading
import time
import unittest
from typing import Callable, Optional
from unittest import mock

from waldur_site_agent.event_processing import listener as listener_module
from waldur_site_agent.event_processing.listener import WaldurListener

_WAIT = 5.0


def _wait_for(condition: Callable[[], bool], timeout: float = _WAIT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def _frame(body: str, ack_id: str) -> mock.Mock:
    frame = mock.Mock()
    frame.body = body
    frame.headers = {"ack": ack_id, "message-id": f"msg-{ack_id}", "subscription": "q"}
    return frame


class _ListenerCase(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = mock.Mock()
        self.conn.is_connected.return_value = True
        self.handled: list[str] = []
        self.events: list[str] = []
        self.conn.ack.side_effect = lambda ack_id, *a, **kw: self.events.append(f"ack:{ack_id}")

    def _listener(self, callback: Callable) -> WaldurListener:
        listener = WaldurListener(
            self.conn, "q", "user", "pass", callback, mock.Mock(), "ua"
        )
        self.addCleanup(getattr(listener, "close", lambda: None))
        return listener

    def _recording_callback(self, block: Optional[dict[str, threading.Event]] = None) -> Callable:
        block = block or {}

        def callback(frame, *_args, **_kwargs) -> None:
            self.events.append(f"started:{frame.body}")
            gate = block.get(frame.body)
            if gate is not None:
                gate.wait(_WAIT)
            self.handled.append(frame.body)
            self.events.append(f"handled:{frame.body}")

        return callback


class TestHandlerRunsOffReceiverThread(_ListenerCase):
    def test_blocking_handler_does_not_block_on_message(self) -> None:
        """on_message returns while the handler is still running, so frames keep flowing."""
        gate = threading.Event()
        listener = self._listener(self._recording_callback({'"a"': gate}))

        def deliver() -> None:
            listener.on_message(_frame('"a"', "1"))
            listener.on_message(_frame('"b"', "2"))

        receiver = threading.Thread(target=deliver, daemon=True)
        receiver.start()
        receiver.join(timeout=1.0)
        try:
            self.assertFalse(
                receiver.is_alive(), "on_message blocked the receiver thread on a slow handler"
            )
        finally:
            gate.set()

    def test_messages_are_handled_in_arrival_order(self) -> None:
        """One worker per queue keeps the queue's order, even behind a slow handler."""
        gate = threading.Event()
        listener = self._listener(self._recording_callback({'"a"': gate}))

        def deliver() -> None:
            for i, body in enumerate(['"a"', '"b"', '"c"']):
                listener.on_message(_frame(body, str(i)))

        threading.Thread(
            target=deliver,
            daemon=True,
        ).start()
        time.sleep(0.2)
        gate.set()

        self.assertTrue(_wait_for(lambda: len(self.handled) == 3))
        self.assertEqual(self.handled, ['"a"', '"b"', '"c"'])


class TestAckAfterHandling(_ListenerCase):
    def test_subscribes_with_client_individual_ack_and_prefetch(self) -> None:
        listener = self._listener(mock.Mock())

        listener.on_connected(mock.Mock())

        kwargs = self.conn.subscribe.call_args.kwargs
        self.assertEqual(kwargs["ack"], "client-individual")
        self.assertEqual(
            kwargs["headers"]["prefetch-count"], str(listener_module.STOMP_PREFETCH_COUNT)
        )

    def test_message_is_acked_after_the_handler_returns(self) -> None:
        listener = self._listener(self._recording_callback())
        listener.on_connected(mock.Mock())

        listener.on_message(_frame('"a"', "7"))

        self.assertTrue(_wait_for(lambda: "ack:7" in self.events))
        self.assertEqual(self.events, ['started:"a"', 'handled:"a"', "ack:7"])

    def test_raising_handler_still_acks(self) -> None:
        """A failing handler is logged and acked, not redelivered forever."""

        def callback(*_args, **_kwargs) -> None:
            msg = "boom"
            raise RuntimeError(msg)

        listener = self._listener(callback)
        listener.on_connected(mock.Mock())

        listener.on_message(_frame('"a"', "9"))

        self.assertTrue(_wait_for(lambda: "ack:9" in self.events))


class TestRedeliveryAfterReconnect(_ListenerCase):
    def test_unstarted_message_from_a_dropped_connection_is_handled_once(self) -> None:
        """Queued-but-unhandled frames are left to the broker's redelivery, not handled twice."""
        gate = threading.Event()
        listener = self._listener(self._recording_callback({'"a"': gate}))
        listener.on_connected(mock.Mock())

        listener.on_message(_frame('"a"', "1"))  # in flight, blocked
        listener.on_message(_frame('"b"', "2"))  # queued behind it
        self.assertTrue(_wait_for(lambda: 'started:"a"' in self.events))

        # The connection drops and comes back; the broker requeues both unacked frames.
        with mock.patch.object(listener, "reconnect", return_value=True):
            listener.on_disconnected()
        listener.on_connected(mock.Mock())
        gate.set()
        listener.on_message(_frame('"a"', "11"))
        listener.on_message(_frame('"b"', "12"))

        self.assertTrue(_wait_for(lambda: "ack:12" in self.events))
        self.assertEqual(self.handled.count('"b"'), 1)
        # Nothing is acked with an id from the dropped connection.
        self.assertNotIn("ack:1", self.events)
        self.assertNotIn("ack:2", self.events)


class TestStopClosesWorker(unittest.TestCase):
    def test_stop_stomp_connection_closes_the_listener_worker(self) -> None:
        from waldur_site_agent.event_processing import event_subscription_manager as esm

        manager = esm.EventSubscriptionManager.__new__(esm.EventSubscriptionManager)
        connection = mock.Mock()
        listener = mock.Mock()
        connection.get_listener.return_value = listener

        manager.stop_stomp_connection(connection)

        listener.close.assert_called_once()
        connection.disconnect.assert_called_once()


class TestResourceStatusReconciliation(unittest.TestCase):
    """Event mode re-applies paused/downscaled even when the message was lost."""

    def _processor(self, resources):
        from waldur_site_agent.common.processors import OfferingMembershipProcessor

        processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
        processor.resource_backend = mock.Mock()
        processor.resource_backend.handled_resource_states = ["OK"]
        processor.waldur_rest_client = mock.Mock()
        processor.offering = mock.Mock()
        processor._get_waldur_resources = mock.Mock(return_value=resources)
        processor._sync_resource_status = mock.Mock()
        return processor

    @staticmethod
    def _resource(uuid_hex, state="OK"):
        resource = mock.Mock(backend_id=f"acc-{uuid_hex}", state=state)
        resource.name = f"res-{uuid_hex}"
        resource.uuid.hex = uuid_hex
        return resource

    @mock.patch("waldur_site_agent.common.processors.touch_heartbeat")
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_retrieve")
    def test_reconcile_applies_each_fresh_resource_and_survives_a_failure(
        self, mock_retrieve, mock_touch
    ) -> None:
        """Flags come from a fresh read, not the snapshot; one failure does not stop the pass."""
        snapshot = [self._resource(u) for u in ("a1", "b2", "c3", "d4")]
        fresh = {u: self._resource(u) for u in ("a1", "b2", "c3")}
        fresh["d4"] = self._resource("d4", state="Terminating")  # changed since the snapshot
        mock_retrieve.sync.side_effect = lambda uuid, client: fresh[uuid]
        processor = self._processor(snapshot)
        processor._sync_resource_status.side_effect = [None, RuntimeError("x"), None]

        processor.reconcile_resource_statuses()

        applied = [c.args[0] for c in processor._sync_resource_status.call_args_list]
        self.assertEqual(applied, [fresh["a1"], fresh["b2"], fresh["c3"]])
        # A long pass keeps the liveness heartbeat fresh, once per resource.
        self.assertGreaterEqual(mock_touch.call_count, len(snapshot))

    @mock.patch("waldur_site_agent.event_processing.main.touch_heartbeat")
    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_main_loop_runs_status_reconciliation_on_its_interval(
        self, mock_utils, mock_time, _touch
    ) -> None:
        from waldur_site_agent.event_processing import main

        config = mock.Mock()
        config.waldur_offerings = [mock.Mock()]
        start = 1_700_000_000.0
        mock_time.time.side_effect = [start, start + 60, start + 3600 + 1]
        mock_time.sleep.side_effect = [None, None, StopIteration("break")]

        with mock.patch.object(main, "RESOURCE_STATUS_RECONCILIATION_INTERVAL", 3600), \
                self.assertRaises(StopIteration):
            main._run_without_username_reconciliation(config)

        # Initial processing just synced status: the first tick only starts the timer.
        self.assertEqual(mock_utils.run_periodic_resource_status_reconciliation.call_count, 1)

    @mock.patch("waldur_site_agent.event_processing.main.touch_heartbeat")
    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_interval_zero_disables_status_reconciliation(
        self, mock_utils, mock_time, _touch
    ) -> None:
        from waldur_site_agent.event_processing import main

        config = mock.Mock()
        config.waldur_offerings = [mock.Mock()]
        mock_time.time.side_effect = [1_700_000_000.0, 1_700_000_000.0 + 10**6]
        mock_time.sleep.side_effect = [None, StopIteration("break")]

        with mock.patch.object(main, "RESOURCE_STATUS_RECONCILIATION_INTERVAL", 0), \
                self.assertRaises(StopIteration):
            main._run_with_reconciliation(config)

        mock_utils.run_periodic_resource_status_reconciliation.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_processors")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_periodic_reconciliation_covers_event_mode_membership_offerings(
        self, mock_client, mock_backend, mock_processors
    ) -> None:
        from waldur_site_agent.event_processing import utils

        mock_backend.return_value = (mock.Mock(), "1")
        with_membership = mock.Mock(stomp_enabled=True, membership_sync_backend="slurm")
        without_membership = mock.Mock(stomp_enabled=True, membership_sync_backend="")
        polling = mock.Mock(stomp_enabled=False, membership_sync_backend="slurm")
        # Membership (and status) owned by the polling agent.
        opted_out = mock.Mock(
            stomp_enabled=True, membership_sync_backend="slurm", stomp_membership_sync_enabled=False
        )

        utils.run_periodic_resource_status_reconciliation(
            [with_membership, without_membership, polling, opted_out], "ua"
        )

        processor = mock_processors.OfferingMembershipProcessor.return_value
        processor.reconcile_resource_statuses.assert_called_once()


class TestHandlerProgressIsVisible(_ListenerCase):
    def test_running_handler_start_is_reported_and_cleared(self) -> None:
        gate = threading.Event()
        listener = self._listener(self._recording_callback({'"a"': gate}))
        listener.on_connected(mock.Mock())
        self.assertIsNone(listener.handler_running_since())

        before = time.time()
        listener.on_message(_frame('"a"', "1"))
        self.assertTrue(_wait_for(lambda: 'started:"a"' in self.events))
        started = listener.handler_running_since()
        assert started is not None
        self.assertGreaterEqual(started, before)

        gate.set()
        self.assertTrue(_wait_for(lambda: "ack:1" in self.events))
        self.assertTrue(_wait_for(lambda: listener.handler_running_since() is None))


class _Fatal(BaseException):
    """Not an Exception: would end a worker loop that only catches Exception."""


class TestWorkerSurvives(_ListenerCase):
    def test_base_exception_in_a_handler_does_not_stop_the_worker(self) -> None:
        def callback(frame, *_args, **_kwargs) -> None:
            if frame.body == '"a"':
                raise _Fatal
            self.handled.append(frame.body)

        listener = self._listener(callback)
        listener.on_connected(mock.Mock())

        listener.on_message(_frame('"a"', "1"))
        listener.on_message(_frame('"b"', "2"))

        self.assertTrue(_wait_for(lambda: '"b"' in self.handled))
        self.assertTrue(_wait_for(lambda: "ack:1" in self.events))

    def test_message_without_ack_header_is_not_acked_with_message_id(self) -> None:
        """STOMP 1.2 acks by the ``ack`` header; ``message-id`` is not a valid substitute."""
        listener = self._listener(self._recording_callback())
        listener.on_connected(mock.Mock())
        frame = mock.Mock(body='"a"', headers={"message-id": "m-1", "subscription": "q"})

        listener.on_message(frame)

        self.assertTrue(_wait_for(lambda: '"a"' in self.handled))
        time.sleep(0.1)
        self.conn.ack.assert_not_called()


class TestWatchdogSeesStuckHandlers(unittest.TestCase):
    """A hung handler holds the (prefetch 1) queue; liveness must notice it."""

    def _watchdog(self, listener):
        from waldur_site_agent.event_processing.event_subscription_manager import (
            WALDUR_LISTENER_NAME,
        )
        from waldur_site_agent.event_processing.watchdog import StompWatchdog

        offering = mock.Mock(stomp_enabled=True, uuid="o-1")
        offering.name = "offering"
        connection = mock.Mock()
        connection.is_connected.return_value = True
        connection.get_listener.side_effect = (
            lambda name: listener if name == WALDUR_LISTENER_NAME else None
        )
        queue = mock.Mock(queue_name="consumer_q")
        consumers = {("offering", "o-1"): [(connection, queue, offering)]}
        return StompWatchdog(
            consumers, [offering], "ua", unhealthy_after=60, handler_stuck_after=300
        )

    @mock.patch("waldur_site_agent.event_processing.watchdog.utils")
    def test_handler_running_too_long_withholds_liveness(self, mock_utils) -> None:
        mock_utils._determine_observable_object_types.return_value = []
        mock_utils.offering_expects_target_consumers.return_value = False
        listener = mock.Mock()
        listener.reconnect_in_progress.return_value = False
        now = 1_700_000_000.0
        listener.handler_running_since.return_value = now - 301
        watchdog = self._watchdog(listener)

        self.assertTrue(watchdog.check(now))  # stuck, but not yet for unhealthy_after
        self.assertFalse(watchdog.check(now + 120))

        listener.handler_running_since.return_value = None  # the handler finished
        self.assertTrue(watchdog.check(now + 180))

    @mock.patch("waldur_site_agent.event_processing.watchdog.utils")
    def test_watchdog_keeps_the_worker_running(self, mock_utils) -> None:
        mock_utils._determine_observable_object_types.return_value = []
        mock_utils.offering_expects_target_consumers.return_value = False
        listener = mock.Mock()
        listener.handler_running_since.return_value = None
        watchdog = self._watchdog(listener)

        watchdog.check(1_700_000_000.0)

        listener.ensure_worker.assert_called()
