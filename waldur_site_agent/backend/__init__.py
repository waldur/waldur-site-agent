"""Base module for different supported backends."""

import logging
import sys
from datetime import datetime, timezone
from enum import Enum

import structlog

ACCOUNT_NAME_REGEX = "a-zA-Z0-9-_"


class BackendType(Enum):
    """Enum for backend types."""

    SLURM = "slurm"
    MOAB = "moab"
    MUP = "mup"
    CROIT_S3 = "croit_s3"
    DIGITALOCEAN = "digitalocean"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


def _add_logger_name(
    logger: structlog.types.WrappedLogger,
    method_name: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    _ = method_name
    event_dict["logger"] = getattr(logger, "name", str(logger))
    return event_dict


def _add_timestamp(
    logger: structlog.types.WrappedLogger,
    method_name: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    _ = logger, method_name
    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


_LOGGER_NAME_PROCESSOR = getattr(
    structlog.processors, "add_logger_name", _add_logger_name
)

_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    _LOGGER_NAME_PROCESSOR,
    structlog.stdlib.PositionalArgumentsFormatter(),
    _add_timestamp,
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


def _configure_structlog() -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


_configure_structlog()

logger = structlog.get_logger(__name__)


def configure_logger(log_level: str = "INFO") -> None:
    """Configure the logger with the specified log level.

    Args:
        log_level: Logging level as a string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    _configure_structlog()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=_SHARED_PROCESSORS,
        )
    )
    handler.setLevel(level)

    root_logger = logging.getLogger()
    # Imported here to avoid a circular import: backend/__init__.py is loaded before
    # common/log_handler.py is fully initialised at module level.
    from waldur_site_agent.common.log_handler import BufferedLogHandler  # noqa: PLC0415

    buffered_handlers = [h for h in root_logger.handlers if isinstance(h, BufferedLogHandler)]
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    for bh in buffered_handlers:
        root_logger.addHandler(bh)
    root_logger.setLevel(level)


# ---------------------------------------------------------------------------
# Log buffering — used by LogShipper to capture agent logs for shipping
# ---------------------------------------------------------------------------

from waldur_site_agent.common.log_handler import LogBufferManager  # noqa: E402

_log_buffer_manager = LogBufferManager()


def setup_log_buffering(
    max_size_bytes: int = 1024 * 1024,
    log_level: str = "INFO",
) -> None:
    """Set up in-memory log buffering for shipping to Waldur Mastermind.

    Idempotent — safe to call multiple times; only the first call has effect.

    Args:
        max_size_bytes: Maximum buffer size in bytes (default: 1 MB)
        log_level: Minimum log level to capture (default: INFO)
    """
    _log_buffer_manager.setup(max_size_bytes=max_size_bytes, log_level=log_level)

    root_logger = logging.getLogger()
    if _log_buffer_manager.handler and _log_buffer_manager.handler not in root_logger.handlers:
        root_logger.addHandler(_log_buffer_manager.handler)


def get_log_buffer_manager() -> LogBufferManager:
    """Return the global LogBufferManager instance."""
    return _log_buffer_manager


def teardown_log_buffering() -> None:
    """Remove the buffered handler from the root logger and clean up."""
    root_logger = logging.getLogger()
    if _log_buffer_manager.handler and _log_buffer_manager.handler in root_logger.handlers:
        root_logger.removeHandler(_log_buffer_manager.handler)
    _log_buffer_manager.cleanup()


# ---------------------------------------------------------------------------
# Log shipping — singleton manager for all LogShipper instances
# ---------------------------------------------------------------------------

from waldur_site_agent.common.log_shipper import LogShippingManager  # noqa: E402

_log_shipping_manager = LogShippingManager()


def get_log_shipping_manager() -> LogShippingManager:
    """Return the global LogShippingManager instance."""
    return _log_shipping_manager
