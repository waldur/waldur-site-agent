import logging
import os
from enum import Enum

from waldur_client import WaldurClient
from waldur_slurm.slurm_client.client import SlurmClient

logger = logging.getLogger(__name__)


class SlurmDeploymentType(Enum):
    NATIVE = "native"
    DOCKER = "docker"


WALDUR_API_URL = os.environ["WALDUR_API_URL"]
WALDUR_API_TOKEN = os.environ["WALDUR_API_TOKEN"]
SLURM_DEPLOYMENT_TYPE = os.environ.get("SLURM_DEPLOYMENT_TYPE", "docker")

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

waldur_rest_client = WaldurClient(WALDUR_API_URL, WALDUR_API_TOKEN)

slurm_cluster_client = SlurmClient(SLURM_DEPLOYMENT_TYPE)
