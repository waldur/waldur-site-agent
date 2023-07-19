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

ENABLE_USER_HOMEDIR_ACCOUNT_CREATION = os.environ.get(
    "ENABLE_USER_HOMEDIR_ACCOUNT_CREATION", "false"
)

ENABLE_USER_HOMEDIR_ACCOUNT_CREATION = ENABLE_USER_HOMEDIR_ACCOUNT_CREATION.lower() in [
    "yes",
    "true",
]

waldur_rest_client = WaldurClient(WALDUR_API_URL, WALDUR_API_TOKEN)

slurm_backend = SlurmBackend()

sentry_dsn = os.environ.get("SENTRY_DSN")

if sentry_dsn:
    import sentry_sdk

    sentry_sdk.init(
        dsn=sentry_dsn,
    )
