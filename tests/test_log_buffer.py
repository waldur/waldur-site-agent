"""Tests for the in-memory circular log buffer."""

import sys
import threading
import unittest

from waldur_site_agent.common.log_buffer import CircularLogBuffer, LogEntry


def _make_entry(message: str = "test", level: str = "INFO", module: str = "mod") -> LogEntry:
    return LogEntry(
        timestamp=1_000_000.0,
        level=level,
        message=message,
        module=module,
        size=sys.getsizeof(message),
    )


class TestCircularLogBuffer(unittest.TestCase):
    """Tests for CircularLogBuffer."""

    def test_add_single_entry(self):
        """Adding an entry increases the entry count and current size."""
        buf = CircularLogBuffer(max_size_bytes=1024 * 1024)
        entry = _make_entry("hello")
        buf.add(entry)

        stats = buf.get_stats()
        assert stats["entry_count"] == 1
        assert stats["current_size"] == entry.size

    def test_get_and_clear_returns_all_entries(self):
        """get_and_clear returns every added entry and leaves buffer empty."""
        buf = CircularLogBuffer()
        entries = [_make_entry(f"msg-{i}") for i in range(5)]
        for e in entries:
            buf.add(e)

        result = buf.get_and_clear()
        assert len(result) == 5
        assert [r.message for r in result] == [f"msg-{i}" for i in range(5)]

        stats = buf.get_stats()
        assert stats["entry_count"] == 0
        assert stats["current_size"] == 0

    def test_get_and_clear_on_empty_buffer(self):
        """get_and_clear on an empty buffer returns an empty list."""
        buf = CircularLogBuffer()
        assert buf.get_and_clear() == []

    def test_overflow_evicts_oldest_entries(self):
        """When the buffer is full the oldest entries are removed first."""
        # Use identical-length messages so every entry has the same byte size.
        # Buffer holds exactly ONE entry → adding a second always evicts the first.
        entry = _make_entry("aaaaa")
        buf = CircularLogBuffer(max_size_bytes=entry.size)

        buf.add(_make_entry("aaaaa"))   # first — fills the buffer
        buf.add(_make_entry("bbbbb"))   # second — evicts "aaaaa"

        result = buf.get_and_clear()
        messages = [r.message for r in result]
        assert "aaaaa" not in messages
        assert "bbbbb" in messages

    def test_size_never_exceeds_max(self):
        """current_size must never exceed max_size_bytes."""
        entry = _make_entry("x" * 100)
        buf = CircularLogBuffer(max_size_bytes=entry.size * 3)

        for _ in range(20):
            buf.add(_make_entry("x" * 100))

        stats = buf.get_stats()
        assert stats["current_size"] <= stats["max_size"]

    def test_clear(self):
        """clear() empties the buffer without returning entries."""
        buf = CircularLogBuffer()
        for i in range(3):
            buf.add(_make_entry(f"m{i}"))

        buf.clear()
        stats = buf.get_stats()
        assert stats["entry_count"] == 0
        assert stats["current_size"] == 0

    def test_thread_safety(self):
        """Concurrent adds from multiple threads must not corrupt the buffer."""
        buf = CircularLogBuffer(max_size_bytes=10 * 1024 * 1024)
        errors = []

        def producer(n: int) -> None:
            try:
                for _ in range(n):
                    buf.add(_make_entry("thread-msg"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=producer, args=(50,)) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        stats = buf.get_stats()
        assert stats["current_size"] >= 0
        assert stats["entry_count"] >= 0
