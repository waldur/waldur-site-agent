"""Tests for the BufferedLogHandler and LogBufferManager."""

import logging
import unittest

from waldur_site_agent.common.log_buffer import CircularLogBuffer
from waldur_site_agent.common.log_handler import BufferedLogHandler, ExcludeHttpFilter, LogBufferManager


class TestBufferedLogHandler(unittest.TestCase):
    """Tests for BufferedLogHandler."""

    def _make_handler(self, max_size_bytes: int = 1024 * 1024) -> tuple:
        buf = CircularLogBuffer(max_size_bytes=max_size_bytes)
        handler = BufferedLogHandler(buf, level=logging.DEBUG)
        return buf, handler

    def test_emit_stores_record_in_buffer(self):
        """emit() must add a LogEntry to the buffer."""
        buf, handler = self._make_handler()

        record = logging.LogRecord(
            name="test.module",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello world",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

        entries = buf.get_and_clear()
        assert len(entries) == 1
        assert entries[0].message == "hello world"
        assert entries[0].level == "INFO"
        assert entries[0].module == "test.module"

    def test_emit_respects_handler_level(self):
        """Records below the handler level must not reach the buffer.

        The level filter is applied by Logger.callHandlers(), not by emit()
        directly.  We must route log calls through an actual Logger instance
        to exercise the correct code path.
        """
        buf = CircularLogBuffer()
        handler = BufferedLogHandler(buf, level=logging.WARNING)

        test_logger = logging.getLogger("test.handler_level_check")
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)   # logger passes everything; handler filters
        test_logger.propagate = False

        try:
            test_logger.info("info msg")      # below handler threshold → ignored
            test_logger.warning("warn msg")   # at handler threshold → captured
        finally:
            test_logger.removeHandler(handler)

        entries = buf.get_and_clear()
        assert len(entries) == 1
        assert entries[0].message == "warn msg"

    def test_emit_extracts_event_from_structlog_dict(self):
        """emit() must extract the 'event' field when message is a structlog dict string."""
        buf, handler = self._make_handler()

        structlog_msg = (
            "{'event': 'There are no pending orders', 'level': 'info', "
            "'logger': 'waldur_site_agent.backend', 'timestamp': '2026-05-11T12:00:00+00:00'}"
        )
        record = logging.LogRecord(
            name="waldur_site_agent.backend", level=logging.INFO,
            pathname="", lineno=0, msg=structlog_msg, args=(), exc_info=None,
        )
        handler.emit(record)

        entries = buf.get_and_clear()
        assert len(entries) == 1
        assert entries[0].message == "There are no pending orders"

    def test_emit_keeps_plain_message_unchanged(self):
        """emit() must not alter messages that are not structlog dicts."""
        buf, handler = self._make_handler()

        record = logging.LogRecord(
            name="some.logger", level=logging.WARNING,
            pathname="", lineno=0, msg="plain warning text", args=(), exc_info=None,
        )
        handler.emit(record)

        entries = buf.get_and_clear()
        assert len(entries) == 1
        assert entries[0].message == "plain warning text"

    def test_exclude_http_filter_drops_httpx_and_httpcore(self):
        """ExcludeHttpFilter must reject httpx/httpcore records to prevent feedback loops."""
        f = ExcludeHttpFilter()

        for logger_name in ("httpx", "httpx._client", "httpcore", "httpcore.http11"):
            record = logging.LogRecord(
                name=logger_name, level=logging.INFO, pathname="",
                lineno=0, msg="GET http://example.com", args=(), exc_info=None,
            )
            assert not f.filter(record), f"expected {logger_name!r} to be filtered out"

    def test_exclude_http_filter_passes_other_loggers(self):
        """ExcludeHttpFilter must not drop unrelated logger records."""
        f = ExcludeHttpFilter()

        for logger_name in ("waldur_site_agent.backend", "slurm", "root", ""):
            record = logging.LogRecord(
                name=logger_name, level=logging.INFO, pathname="",
                lineno=0, msg="some message", args=(), exc_info=None,
            )
            assert f.filter(record), f"expected {logger_name!r} to pass through"

    def test_setup_adds_http_filter_to_handler(self):
        """LogBufferManager.setup() must install ExcludeHttpFilter so httpx logs are not buffered."""
        mgr = LogBufferManager()
        mgr.setup()

        assert any(isinstance(f, ExcludeHttpFilter) for f in mgr.handler.filters)

        httpx_record = logging.LogRecord(
            name="httpx._client", level=logging.INFO, pathname="",
            lineno=0, msg="HTTP Request: POST ...", args=(), exc_info=None,
        )
        mgr.handler.handle(httpx_record)
        entries = mgr.buffer.get_and_clear()
        assert len(entries) == 0, "httpx log must not reach the buffer"

    def test_emit_does_not_raise_on_format_error(self):
        """emit() must not propagate exceptions (handleError convention)."""
        buf = CircularLogBuffer()
        handler = BufferedLogHandler(buf)

        bad_record = logging.LogRecord(
            name="mod", level=logging.ERROR, pathname="", lineno=0,
            msg="%s %s",
            args=("only one arg",),  # mismatched args — format will fail
            exc_info=None,
        )
        # Should not raise
        try:
            handler.emit(bad_record)
        except Exception as exc:  # noqa: BLE001
            self.fail(f"emit() raised unexpectedly: {exc}")


class TestLogBufferManager(unittest.TestCase):
    """Tests for LogBufferManager lifecycle."""

    def tearDown(self) -> None:
        # Always clean up root logger handlers added during tests
        root = logging.getLogger()
        for h in list(root.handlers):
            if isinstance(h, BufferedLogHandler):
                root.removeHandler(h)

    def test_setup_creates_buffer_and_handler(self):
        """setup() must create a non-None buffer and handler."""
        mgr = LogBufferManager()
        mgr.setup(max_size_bytes=512 * 1024, log_level="INFO")

        assert mgr.buffer is not None
        assert mgr.handler is not None
        assert mgr.get_buffer() is mgr.buffer

    def test_setup_is_idempotent(self):
        """Calling setup() twice must not create a second buffer."""
        mgr = LogBufferManager()
        mgr.setup()
        first_buffer = mgr.buffer

        mgr.setup()  # second call — should be a no-op
        assert mgr.buffer is first_buffer

    def test_add_to_logger_before_setup_raises(self):
        """add_to_logger() before setup() must raise RuntimeError."""
        mgr = LogBufferManager()
        logger = logging.getLogger("test.pre_setup")
        with self.assertRaises(RuntimeError):
            mgr.add_to_logger(logger)

    def test_add_to_logger_attaches_handler(self):
        """add_to_logger() must attach the handler to the given logger."""
        mgr = LogBufferManager()
        mgr.setup()

        logger = logging.getLogger("test.attach")
        mgr.add_to_logger(logger)

        assert mgr.handler in logger.handlers

        # Emit a real log record and verify it reaches the buffer
        logger.warning("captured message")
        entries = mgr.buffer.get_and_clear()
        assert any("captured message" in e.message for e in entries)

    def test_cleanup_clears_buffer_and_handler(self):
        """cleanup() must reset buffer and handler to None."""
        mgr = LogBufferManager()
        mgr.setup()
        mgr.cleanup()

        assert mgr.buffer is None
        assert mgr.handler is None
        assert mgr.get_buffer() is None
