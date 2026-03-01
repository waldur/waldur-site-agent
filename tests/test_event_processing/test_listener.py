"""Tests for STOMP listener reconnection logic."""

import threading
import time
import unittest
from unittest import mock

from stomp.exception import ConnectFailedException, StompException

from waldur_site_agent.event_processing.listener import (
    BACKOFF_FACTOR,
    BACKOFF_INITIAL,
    BACKOFF_MAX,
    RECONNECT_MAX_RETRIES,
    WaldurListener,
    _calculate_backoff,
    connect_to_stomp_server,
)


class TestCalculateBackoff(unittest.TestCase):
    """Tests for _calculate_backoff helper function."""

    def test_first_attempt_delay(self):
        """First attempt should have delay close to BACKOFF_INITIAL."""
        delay = _calculate_backoff(0)
        # Base delay is BACKOFF_INITIAL, jitter adds up to 25%
        self.assertGreaterEqual(delay, BACKOFF_INITIAL)
        self.assertLessEqual(delay, BACKOFF_INITIAL * 1.25)

    def test_exponential_increase(self):
        """Delay should increase exponentially with each attempt."""
        delay_0 = BACKOFF_INITIAL * (BACKOFF_FACTOR**0)  # 1s base
        delay_1 = BACKOFF_INITIAL * (BACKOFF_FACTOR**1)  # 2s base
        delay_2 = BACKOFF_INITIAL * (BACKOFF_FACTOR**2)  # 4s base

        # Run many times to verify base trend (jitter makes exact values vary)
        for _ in range(10):
            d0 = _calculate_backoff(0)
            d1 = _calculate_backoff(1)
            d2 = _calculate_backoff(2)

            self.assertGreaterEqual(d0, delay_0)
            self.assertGreaterEqual(d1, delay_1)
            self.assertGreaterEqual(d2, delay_2)

    def test_max_cap(self):
        """Delay should never exceed BACKOFF_MAX + jitter."""
        delay = _calculate_backoff(100)  # Very high attempt number
        max_with_jitter = BACKOFF_MAX * 1.25
        self.assertLessEqual(delay, max_with_jitter)

    def test_jitter_non_negative(self):
        """Delay should always be non-negative."""
        for attempt in range(20):
            delay = _calculate_backoff(attempt)
            self.assertGreater(delay, 0)


class TestConnectToStompServer(unittest.TestCase):
    """Tests for connect_to_stomp_server function."""

    def test_success_on_first_try(self):
        """Should connect successfully without retries."""
        conn = mock.Mock()
        conn.is_connected.side_effect = [False, True]

        connect_to_stomp_server(conn, "user", "pass")

        conn.connect.assert_called_once_with(
            "user",
            "pass",
            wait=True,
            headers={
                "accept-version": "1.2",
            },
        )

    @mock.patch("waldur_site_agent.event_processing.listener.time.sleep")
    def test_retry_on_stomp_exception_with_increasing_backoff(self, mock_sleep):
        """Should retry on StompException with exponential backoff."""
        conn = mock.Mock()
        # Fail twice, then succeed
        conn.is_connected.side_effect = [False, False, False, True]
        conn.connect.side_effect = [
            StompException("fail 1"),
            StompException("fail 2"),
            None,  # success
        ]

        connect_to_stomp_server(conn, "user", "pass")

        self.assertEqual(conn.connect.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        # Second sleep should be longer than first (exponential backoff)
        first_sleep = mock_sleep.call_args_list[0][0][0]
        second_sleep = mock_sleep.call_args_list[1][0][0]
        self.assertGreater(second_sleep, first_sleep)

    @mock.patch("waldur_site_agent.event_processing.listener.time.sleep")
    def test_retry_on_os_error(self, mock_sleep):
        """Should retry on OSError (network errors)."""
        conn = mock.Mock()
        conn.is_connected.side_effect = [False, False, True]
        conn.connect.side_effect = [
            OSError("Connection refused"),
            None,  # success
        ]

        connect_to_stomp_server(conn, "user", "pass")

        self.assertEqual(conn.connect.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    @mock.patch("waldur_site_agent.event_processing.listener.time.sleep")
    def test_raise_after_max_retries(self, mock_sleep):
        """Should raise ConnectFailedException after max_retries exceeded."""
        conn = mock.Mock()
        conn.is_connected.return_value = False
        conn.connect.side_effect = StompException("persistent failure")

        with self.assertRaises(ConnectFailedException):
            connect_to_stomp_server(conn, "user", "pass", max_retries=3)

        self.assertEqual(conn.connect.call_count, 3)

    def test_skip_if_already_connected(self):
        """Should return immediately if already connected."""
        conn = mock.Mock()
        conn.is_connected.return_value = True

        connect_to_stomp_server(conn, "user", "pass")

        conn.connect.assert_not_called()

    def test_unexpected_exception_propagates(self):
        """Programming errors (TypeError, etc.) should not be caught."""
        conn = mock.Mock()
        conn.is_connected.return_value = False
        conn.connect.side_effect = TypeError("unexpected bug")

        with self.assertRaises(TypeError):
            connect_to_stomp_server(conn, "user", "pass")

    @mock.patch("waldur_site_agent.event_processing.listener.time.sleep")
    def test_infinite_retries_when_max_retries_zero(self, mock_sleep):
        """With max_retries=0, should keep retrying until connected."""
        conn = mock.Mock()
        # Fail 5 times, then succeed
        side_effects = [False] * 6 + [True]
        conn.is_connected.side_effect = side_effects
        conn.connect.side_effect = [StompException("fail")] * 5 + [None]

        connect_to_stomp_server(conn, "user", "pass", max_retries=0)

        self.assertEqual(conn.connect.call_count, 6)
        self.assertEqual(mock_sleep.call_count, 5)


class TestWaldurListenerOnDisconnected(unittest.TestCase):
    """Tests for WaldurListener.on_disconnected method."""

    def _make_listener(self):
        """Create a WaldurListener with mocked dependencies."""
        conn = mock.Mock()
        offering = mock.Mock()
        listener = WaldurListener(
            conn=conn,
            queue="test-queue",
            username="user",
            password="pass",
            on_message_callback=mock.Mock(),
            offering=offering,
            user_agent="test-agent",
        )
        return listener

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_reconnects_on_disconnect(self, mock_connect):
        """Should call connect_to_stomp_server on disconnect with finite retries."""
        listener = self._make_listener()

        listener.on_disconnected()

        mock_connect.assert_called_once_with(
            listener.conn, "user", "pass", max_retries=RECONNECT_MAX_RETRIES
        )

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_concurrent_disconnects_only_trigger_one_reconnect(self, mock_connect):
        """When multiple disconnects fire, only one reconnect should proceed."""
        listener = self._make_listener()

        # Make connect_to_stomp_server block for a bit so second call overlaps
        barrier = threading.Event()

        def slow_connect(*args, **kwargs):
            barrier.set()
            time.sleep(0.2)

        mock_connect.side_effect = slow_connect

        # Start first disconnect in a thread
        t1 = threading.Thread(target=listener.on_disconnected)
        t1.start()

        # Wait for the first thread to enter connect_to_stomp_server
        barrier.wait(timeout=2)

        # Call second disconnect - should skip since lock is held
        listener.on_disconnected()

        t1.join(timeout=3)

        # Only one call to connect_to_stomp_server
        mock_connect.assert_called_once()

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_failure_is_caught_not_raised(self, mock_connect):
        """Should catch exceptions from connect_to_stomp_server without raising."""
        listener = self._make_listener()
        mock_connect.side_effect = ConnectFailedException("all retries exhausted")

        # Should not raise
        listener.on_disconnected()

    @mock.patch("waldur_site_agent.event_processing.listener.connect_to_stomp_server")
    def test_lock_released_after_failure(self, mock_connect):
        """Lock should be released even after failure, allowing future reconnects."""
        listener = self._make_listener()

        # First call fails
        mock_connect.side_effect = ConnectFailedException("fail")
        listener.on_disconnected()

        # Second call should succeed (lock was released)
        mock_connect.side_effect = None
        mock_connect.reset_mock()
        listener.on_disconnected()

        mock_connect.assert_called_once()

    def test_has_reconnect_lock(self):
        """Listener should have a threading lock for reconnection."""
        listener = self._make_listener()
        self.assertIsInstance(listener._reconnect_lock, type(threading.Lock()))
