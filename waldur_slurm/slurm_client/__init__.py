import logging
import os
import sys
from enum import Enum

handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(__name__)
formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

SLURM_DEPLOYMENT_TYPE = os.environ.get("SLURM_DEPLOYMENT_TYPE", "docker")


class SlurmDeploymentType(Enum):
    NATIVE = "native"
    DOCKER = "docker"


if SLURM_DEPLOYMENT_TYPE not in (
    SlurmDeploymentType.DOCKER.value,
    SlurmDeploymentType.NATIVE.value,
):
    logger.error(
        "SLURM_DEPLOYMENT_TYPE has invalid value: %s. Possible values are %s and %s",
        SLURM_DEPLOYMENT_TYPE,
        SlurmDeploymentType.DOCKER.value,
        SlurmDeploymentType.NATIVE.value,
    )
    exit(1)

SLURM_ALLOCATION_REGEX = "a-zA-Z0-9-_"
SLURM_ALLOCATION_NAME_MAX_LEN = os.environ.get("SLURM_ALLOCATION_NAME_MAX_LEN", 34)

SLURM_CUSTOMER_PREFIX = os.environ.get("SLURM_CUSTOMER_PREFIX", "hpc_")
SLURM_PROJECT_PREFIX = os.environ.get("SLURM_PROJECT_PREFIX", "hpc_")
SLURM_ALLOCATION_PREFIX = os.environ.get("SLURM_ALLOCATION_PREFIX", "hpc_")

SLURM_DEFAULT_LIMITS = {
    "CPU": os.environ.get(
        "SLURM_DEFAULT_CPU_LIMIT", 16000
    ),  # Measured unit is CPU-minutes
    "GPU": os.environ.get(
        "SLURM_DEFAULT_GPU_LIMIT", 400
    ),  # Measured unit is GPU-minutes
    "RAM": os.environ.get(
        "SLURM_DEFAULT_RAM_LIMIT", 100000 * 2 ** 10
    ),  # Measured unit is MB-h
}

SLURM_DEFAULT_ACCOUNT = os.environ.get("SLURM_DEFAULT_ACCOUNT", "waldur")
