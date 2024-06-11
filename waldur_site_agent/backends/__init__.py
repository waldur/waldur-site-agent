"""Base module for different supported backends."""

import logging
import sys
from enum import Enum

ACCOUNT_NAME_REGEX = "a-zA-Z0-9-_"


class BackendType(Enum):
    """Enum for backend types."""

    SLURM = "slurm"
    MOAB = "moab"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


console_handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(__name__)
formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] %(message)s")
console_handler.setFormatter(formatter)

blank_handler = logging.StreamHandler(sys.stdout)
blank_handler.setLevel(logging.INFO)
blank_handler.setFormatter(logging.Formatter(fmt=""))

logger.addHandler(console_handler)
logger.setLevel(logging.INFO)
