import logging
import os
import sys
import types
from enum import Enum

import yaml


def log_newline(self, how_many_lines=1):
    self.removeHandler(self.console_handler)
    self.addHandler(self.blank_handler)
    for i in range(how_many_lines):
        self.info("")

    self.removeHandler(self.blank_handler)
    self.addHandler(self.console_handler)


console_handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(__name__)
formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] %(message)s")
console_handler.setFormatter(formatter)

blank_handler = logging.StreamHandler(sys.stdout)
blank_handler.setLevel(logging.INFO)
blank_handler.setFormatter(logging.Formatter(fmt=""))

logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

logger.console_handler = console_handler
logger.blank_handler = blank_handler
logger.newline = types.MethodType(log_newline, logger)

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

SLURM_TRES_CONFIG_PATH = os.environ.get(
    "SLURM_TRES_CONFIG_PATH", "config-components.yaml"
)

with open(SLURM_TRES_CONFIG_PATH, "r") as stream:
    tres_config = yaml.safe_load(stream)
    SLURM_TRES = tres_config


SLURM_DEFAULT_ACCOUNT = os.environ.get("SLURM_DEFAULT_ACCOUNT", "waldur")

SLURM_CONTAINER_NAME = os.environ.get("SLURM_CONTAINER_NAME", "slurmctld")
