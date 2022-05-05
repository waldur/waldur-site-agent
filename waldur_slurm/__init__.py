import os
from asyncio.log import logger
from enum import Enum

from waldur_client import WaldurClient

from waldur_slurm.slurm_client.backend import SlurmBackend


# PUSH stands for sync from SLURM cluster to Waldur
# PULL stands for sync from Waldur to SLURM cluster
class WaldurSyncDirection(Enum):
    PULL = "PULL"
    PUSH = "PUSH"


WALDUR_API_URL = os.environ["WALDUR_API_URL"]
WALDUR_API_TOKEN = os.environ["WALDUR_API_TOKEN"]

# TODO: add support for this option to Dockerfile

WALDUR_SYNC_DIRECTION = os.environ["WALDUR_SYNC_DIRECTION"]

if WALDUR_SYNC_DIRECTION not in [
    WaldurSyncDirection.PULL.value,
    WaldurSyncDirection.PUSH.value,
]:
    logger.error(
        "SLURM_DEPLOYMENT_TYPE has invalid value: %s. Possible values are %s and %s",
        WALDUR_SYNC_DIRECTION,
        WaldurSyncDirection.PULL.value,
        WaldurSyncDirection.PUSH.value,
    )
    exit(1)

waldur_rest_client = WaldurClient(WALDUR_API_URL, WALDUR_API_TOKEN)

slurm_backend = SlurmBackend()
