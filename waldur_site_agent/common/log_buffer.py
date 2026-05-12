"""In-memory log buffer for shipping logs to Waldur Mastermind.

This module provides a circular buffer implementation that stores log entries
in memory with a configurable size limit. When the buffer exceeds the limit,
the oldest entries are automatically removed to maintain the size constraint.
The buffer is thread-safe and supports concurrent read/write operations.
"""

from __future__ import annotations

import collections
import logging
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Thread-local flag to prevent re-entrant overflow warnings.
# Without this, logger.warning() inside add() would call BufferedLogHandler.emit()
# → buffer.add() → logger.warning() → infinite recursion.
_overflow_warning_active = threading.local()


@dataclass
class LogEntry:
    """Represents a single log entry with metadata."""

    timestamp: float
    level: str
    message: str
    module: str
    size: int  # Size in bytes


class CircularLogBuffer:
    """Thread-safe circular buffer for storing log entries with size limit.

    The buffer automatically removes oldest entries when the configured
    size limit is exceeded. All operations are thread-safe using RLock.
    """

    def __init__(self, max_size_bytes: int = 1024 * 1024) -> None:
        """Initialize the circular log buffer.

        Args:
            max_size_bytes: Maximum buffer size in bytes (default: 1MB)
        """
        self.max_size_bytes = max_size_bytes
        self.current_size = 0
        self.buffer: collections.deque[LogEntry] = collections.deque()
        self.lock = threading.RLock()

    def add(self, entry: LogEntry) -> None:
        """Add a log entry to the buffer.

        If adding the entry would exceed the size limit, oldest entries
        are removed until the buffer is within the limit.

        Args:
            entry: The log entry to add
        """
        dropped_count = 0
        with self.lock:
            self.buffer.append(entry)
            self.current_size += entry.size

            while self.current_size > self.max_size_bytes and self.buffer:
                removed = self.buffer.popleft()
                self.current_size -= removed.size
                dropped_count += 1

        if dropped_count and not getattr(_overflow_warning_active, "active", False):
            _overflow_warning_active.active = True
            try:
                logger.warning(
                    "Log buffer overflow: dropped %d oldest entries (max_size=%d bytes)",
                    dropped_count,
                    self.max_size_bytes,
                )
            finally:
                _overflow_warning_active.active = False

    def get_and_clear(self) -> list[LogEntry]:
        """Get all buffered entries and clear the buffer.

        Returns:
            List of all log entries currently in the buffer
        """
        with self.lock:
            entries = list(self.buffer)
            self.buffer.clear()
            self.current_size = 0
            return entries

    def get_stats(self) -> dict[str, int]:
        """Get buffer statistics.

        Returns:
            Dictionary with current_size, max_size, and entry_count
        """
        with self.lock:
            return {
                "current_size": self.current_size,
                "max_size": self.max_size_bytes,
                "entry_count": len(self.buffer),
            }

    def clear(self) -> None:
        """Clear all entries from the buffer."""
        with self.lock:
            self.buffer.clear()
            self.current_size = 0
