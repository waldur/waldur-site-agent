"""Base module for different supported backends."""

import logging
import sys
from enum import Enum

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


console_handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(__name__)

formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] [%(threadName)s] %(message)s")
console_handler.setFormatter(formatter)

blank_handler = logging.StreamHandler(sys.stdout)
blank_handler.setLevel(logging.INFO)
blank_handler.setFormatter(logging.Formatter(fmt=""))

logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def configure_logger(log_level: str = "INFO") -> None:
    """Configure the logger with the specified log level.

    Args:
        log_level: Logging level as a string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    console_handler.setLevel(level)
