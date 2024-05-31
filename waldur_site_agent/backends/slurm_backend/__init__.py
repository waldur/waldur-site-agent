"""SLURM backend module."""

import os
import sys
from enum import Enum
from pathlib import Path

import yaml

from waldur_site_agent.backends import logger

SLURM_DEPLOYMENT_TYPE = os.environ.get("SLURM_DEPLOYMENT_TYPE", "docker")


class SlurmDeploymentType(Enum):
    """Enum for deployment types of SLURM cluster.

    native - for staging and production;
    docker - for development and test env.
    """

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
    sys.exit(1)

SLURM_ALLOCATION_REGEX = "a-zA-Z0-9-_"
SLURM_ALLOCATION_NAME_MAX_LEN = int(os.environ.get("SLURM_ALLOCATION_NAME_MAX_LEN", 34))

SLURM_CUSTOMER_PREFIX = os.environ.get("SLURM_CUSTOMER_PREFIX", "hpc_")
SLURM_PROJECT_PREFIX = os.environ.get("SLURM_PROJECT_PREFIX", "hpc_")
SLURM_ALLOCATION_PREFIX = os.environ.get("SLURM_ALLOCATION_PREFIX", "hpc_")

SLURM_TRES_CONFIG_PATH = os.environ.get("SLURM_TRES_CONFIG_PATH", "config-components.yaml")

with Path(SLURM_TRES_CONFIG_PATH).open(encoding="UTF-8") as stream:
    tres_config = yaml.safe_load(stream)
    SLURM_TRES = tres_config


SLURM_DEFAULT_ACCOUNT = os.environ.get("SLURM_DEFAULT_ACCOUNT", "waldur")

SLURM_CONTAINER_NAME = os.environ.get("SLURM_CONTAINER_NAME", "slurmctld")
