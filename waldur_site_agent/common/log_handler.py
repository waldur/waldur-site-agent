"""Custom logging handler for buffering log entries.

This module provides a logging handler that integrates with the CircularLogBuffer
to capture log entries and store them in memory for later shipping to Waldur Mastermind.
"""

import ast
import logging
from typing import Optional

from .log_buffer import CircularLogBuffer, LogEntry

_EXCLUDED_LOGGER_PREFIXES = ("httpx", "httpcore")


class ExcludeHttpFilter(logging.Filter):
    """Filter that drops httpx/httpcore transport logs to prevent feedback loops.

    Without this, every POST to marketplace-site-agent-logs/ would be captured
    by the buffer and re-shipped on the next cycle indefinitely.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False for httpx/httpcore records, True for everything else."""
        return not record.name.startswith(_EXCLUDED_LOGGER_PREFIXES)


class BufferedLogHandler(logging.Handler):
    """Logging handler that buffers log entries in memory.

    This handler captures log records and stores them in a CircularLogBuffer
    for later retrieval and shipping to Waldur Mastermind.
    """

    def __init__(self, buffer: CircularLogBuffer, level: int = logging.NOTSET) -> None:
        """Initialize the buffered log handler.

        Args:
            buffer: The CircularLogBuffer to store log entries
            level: Minimum logging level to handle
        """
        super().__init__(level)
        self.buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record by adding it to the buffer.

        Args:
            record: The log record to emit
        """
        try:
            msg = record.getMessage()
            try:
                data = ast.literal_eval(msg)
                if isinstance(data, dict) and "event" in data:
                    msg = str(data["event"])
            except (ValueError, SyntaxError):
                pass
            entry = LogEntry(
                timestamp=record.created,
                level=record.levelname,
                message=msg,
                module=record.name,
                size=len(msg.encode("utf-8")),
            )
            self.buffer.add(entry)
        except Exception:
            self.handleError(record)


class LogBufferManager:
    """Manager for log buffer and handler lifecycle.

    This class manages the creation and lifecycle of log buffers and handlers,
    providing a convenient interface for setting up log buffering.
    """

    def __init__(self) -> None:
        """Initialize the log buffer manager."""
        self.buffer: Optional[CircularLogBuffer] = None
        self.handler: Optional[BufferedLogHandler] = None
        self._is_setup = False

    def setup(
        self,
        max_size_bytes: int = 1024 * 1024,
        log_level: str = "INFO",
    ) -> None:
        """Set up log buffering.

        Args:
            max_size_bytes: Maximum buffer size in bytes
            log_level: Minimum log level to capture
        """
        if self._is_setup:
            return

        self.buffer = CircularLogBuffer(max_size_bytes)
        self.handler = BufferedLogHandler(self.buffer, getattr(logging, log_level.upper()))
        self.handler.addFilter(ExcludeHttpFilter())

        self._is_setup = True

    def add_to_logger(self, logger: logging.Logger) -> None:
        """Add the buffered handler to a logger.

        Args:
            logger: The logger to add the handler to
        """
        if not self._is_setup or not self.handler:
            msg = "LogBufferManager not set up. Call setup() first."
            raise RuntimeError(msg)

        logger.addHandler(self.handler)

    def get_buffer(self) -> Optional[CircularLogBuffer]:
        """Get the current log buffer.

        Returns:
            The CircularLogBuffer instance, or None if not set up
        """
        return self.buffer

    def cleanup(self) -> None:
        """Clean up the log buffer and handler."""
        if self.handler:
            self.handler.close()
        self.buffer = None
        self.handler = None
        self._is_setup = False
