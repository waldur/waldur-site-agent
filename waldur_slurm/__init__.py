import os
from enum import Enum

from waldur_client import WaldurClient

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.backend import SlurmBackend


# "pull" stands for sync from Waldur to SLURM cluster
# "push" stands for sync from SLURM cluster to Waldur
class WaldurSyncDirection(Enum):
    PULL = "pull"
    PUSH = "push"


# "local" stands for getting a username from local Waldur user account
# "freeipa" stands for getting a username from FreeIPA user profile
class WaldurSlurmUsernameSource:
    LOCAL = "local"
    FREEIPA = "freeipa"


WALDUR_API_URL = os.environ["WALDUR_API_URL"]
WALDUR_API_TOKEN = os.environ["WALDUR_API_TOKEN"]

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

WALDUR_OFFERING_UUID = os.environ.get("WALDUR_OFFERING_UUID")

if not WALDUR_OFFERING_UUID:
    logger.error("WALDUR_OFFERING_UUID is empty")
    exit(1)

WALDUR_SLURM_USERNAME_SOURCE = os.environ.get("WALDUR_SLURM_USERNAME_SOURCE", "local")

if WALDUR_SLURM_USERNAME_SOURCE not in [
    WaldurSlurmUsernameSource.LOCAL,
    WaldurSlurmUsernameSource.FREEIPA,
]:
    logger.error(
        "WALDUR_SLURM_USERNAME_SOURCE has invalid value: %s",
        WALDUR_SLURM_USERNAME_SOURCE,
    )
    exit(1)

waldur_rest_client = WaldurClient(WALDUR_API_URL, WALDUR_API_TOKEN)

slurm_backend = SlurmBackend()
