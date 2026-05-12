"""Tests for LogShipper — background log shipping service."""

import time
import unittest
from unittest.mock import MagicMock, patch

import httpx
import respx

from waldur_site_agent.common.log_buffer import CircularLogBuffer, LogEntry
from waldur_site_agent.common.log_shipper import LogShipper, _HTTP_NOT_FOUND

_API_URL = "https://waldur.example.com/api/"
_TOKEN = "test-token-abc"
_AGENT_IDENTITY_UUID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
_ENDPOINT = f"{_API_URL}marketplace-site-agent-logs/"


def _make_buffer(*messages: str) -> CircularLogBuffer:
    buf = CircularLogBuffer()
    for msg in messages:
        buf.add(LogEntry(
            timestamp=time.time(),
            level="INFO",
            message=msg,
            module="test.module",
            size=len(msg),
        ))
    return buf


def _make_shipper(
    buf: CircularLogBuffer,
    api_url: str = _API_URL,
    api_token: str = _TOKEN,
    agent_identity_uuid: str = _AGENT_IDENTITY_UUID,
    ship_interval: int = 3600,  # very large — won't fire automatically during tests
    batch_size: int = 100,
    max_retries: int = 2,
    retry_delay: int = 0,
) -> LogShipper:
    return LogShipper(
        buffer=buf,
        api_url=api_url,
        api_token=api_token,
        agent_identity_uuid=agent_identity_uuid,
        ship_interval=ship_interval,
        batch_size=batch_size,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )


class TestLogShipperInit(unittest.TestCase):
    """Tests for LogShipper construction."""

    def test_api_url_normalised_with_trailing_slash(self):
        """api_url is stored with a trailing slash regardless of input."""
        buf = _make_buffer()
        shipper = _make_shipper(buf, api_url="https://waldur.example.com/api")
        assert shipper.api_url.endswith("/")

    def test_initial_stats_are_zero(self):
        """All counters start at zero / None."""
        buf = _make_buffer()
        shipper = _make_shipper(buf)
        stats = shipper._stats
        assert stats["logs_shipped"] == 0
        assert stats["batches_sent"] == 0
        assert stats["failed_shipments"] == 0
        assert stats["last_shipment"] is None


class TestLogShipperThread(unittest.TestCase):
    """Tests for start / stop lifecycle."""

    def test_start_spawns_daemon_thread(self):
        """start() must create a live daemon thread."""
        buf = _make_buffer()
        shipper = _make_shipper(buf)
        shipper.start()
        try:
            assert shipper._thread is not None
            assert shipper._thread.is_alive()
            assert shipper._thread.daemon
        finally:
            shipper.stop()

    def test_start_twice_does_not_create_second_thread(self):
        """Calling start() on a running shipper is a no-op."""
        buf = _make_buffer()
        shipper = _make_shipper(buf)
        shipper.start()
        first_thread = shipper._thread
        shipper.start()  # second call
        try:
            assert shipper._thread is first_thread
        finally:
            shipper.stop()

    def test_stop_terminates_thread(self):
        """stop() must join the thread within the timeout."""
        buf = _make_buffer()
        shipper = _make_shipper(buf)
        shipper.start()
        shipper.stop(timeout=5)
        assert not shipper._thread.is_alive()

    @respx.mock
    def test_stop_flushes_remaining_entries(self):
        """stop() must ship any entries still in the buffer."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer("msg-A", "msg-B")
        shipper = _make_shipper(buf)
        shipper.start()
        shipper.stop(timeout=5)

        stats = shipper._stats
        assert stats["logs_shipped"] == 2


class TestShipBatch(unittest.TestCase):
    """Tests for the HTTP shipping logic."""

    @respx.mock
    def test_successful_post_increments_stats(self):
        """A 200 response must increment logs_shipped and batches_sent."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer("log-line-1", "log-line-2", "log-line-3")
        shipper = _make_shipper(buf)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["logs_shipped"] == 3
        assert stats["batches_sent"] == 1
        assert stats["last_shipment"] is not None

    @respx.mock
    def test_post_sends_correct_auth_header(self):
        """The Authorization header must contain the configured token."""
        route = respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer("check-auth")
        shipper = _make_shipper(buf)
        shipper._ship_logs()

        request = route.calls.last.request
        assert request.headers["Authorization"] == f"Token {_TOKEN}"

    @respx.mock
    def test_post_sends_json_payload(self):
        """The request body must be valid JSON with expected fields."""
        import json

        route = respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer("payload-check")
        shipper = _make_shipper(buf)
        shipper._ship_logs()

        body = json.loads(route.calls.last.request.content)
        assert isinstance(body, list)
        assert len(body) == 1
        entry = body[0]
        assert entry["agent_identity_uuid"] == _AGENT_IDENTITY_UUID
        assert "timestamp" in entry
        assert "level" in entry
        assert "message" in entry
        assert "module" in entry
        assert entry["message"] == "payload-check"

    @respx.mock
    def test_http_404_skips_silently_no_retry(self):
        """A 404 response must not retry and must not increment failed_shipments."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(_HTTP_NOT_FOUND))

        buf = _make_buffer("will-404")
        shipper = _make_shipper(buf, max_retries=3)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["failed_shipments"] == 0
        # Only one HTTP call must have been made (no retries)
        assert len(respx.calls) == 1

    @respx.mock
    def test_http_500_retries_and_increments_failed(self):
        """A persistent 500 must retry max_retries times then record failure."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(500))

        buf = _make_buffer("server-error")
        shipper = _make_shipper(buf, max_retries=2, retry_delay=0)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["failed_shipments"] == 1
        # 1 initial attempt + 2 retries = 3 total calls
        assert len(respx.calls) == 3

    @respx.mock
    def test_retry_succeeds_on_second_attempt(self):
        """If first attempt fails but second succeeds, no failure is recorded."""
        responses = [httpx.Response(500), httpx.Response(200)]
        respx.post(_ENDPOINT).mock(side_effect=responses)

        buf = _make_buffer("retry-ok")
        shipper = _make_shipper(buf, max_retries=2, retry_delay=0)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["logs_shipped"] == 1
        assert stats["failed_shipments"] == 0

    @respx.mock
    def test_network_error_retries_and_increments_failed(self):
        """A network-level exception triggers retry and eventually records failure."""
        respx.post(_ENDPOINT).mock(side_effect=httpx.ConnectError("unreachable"))

        buf = _make_buffer("network-fail")
        shipper = _make_shipper(buf, max_retries=1, retry_delay=0)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["failed_shipments"] == 1

    def test_empty_buffer_skips_http_call(self):
        """_ship_logs() on an empty buffer must not make any HTTP request."""
        with patch("waldur_site_agent.common.log_shipper.httpx") as mock_httpx:
            buf = CircularLogBuffer()
            shipper = _make_shipper(buf)
            shipper._ship_logs()

            mock_httpx.Client.assert_not_called()

    @respx.mock
    def test_large_batch_is_split(self):
        """Entries exceeding batch_size are shipped in multiple batches."""
        route = respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer(*[f"entry-{i}" for i in range(25)])
        shipper = _make_shipper(buf, batch_size=10)
        shipper._ship_logs()

        stats = shipper._stats
        assert stats["logs_shipped"] == 25
        assert stats["batches_sent"] == 3  # 10 + 10 + 5
        assert len(route.calls) == 3


class TestLogShipperPeriodicLoop(unittest.TestCase):
    """Tests for the background ship loop firing correctly."""

    @respx.mock
    def test_ship_loop_fires_at_interval(self):
        """The shipper thread sends entries after ship_interval seconds."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        buf = _make_buffer("periodic-msg")
        shipper = _make_shipper(buf, ship_interval=1)
        shipper.start()

        time.sleep(2.5)  # allow at least one cycle
        shipper.stop(timeout=5)

        stats = shipper._stats
        assert stats["logs_shipped"] >= 1
