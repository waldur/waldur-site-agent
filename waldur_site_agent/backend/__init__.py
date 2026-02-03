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
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
