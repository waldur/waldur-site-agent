import os
from enum import Enum
from importlib.metadata import version

import yaml

from waldur_site_agent.slurm_client import logger
from waldur_site_agent.slurm_client.backend import SlurmBackend


# "pull" stands for sync from Waldur to SLURM cluster
# "push" stands for sync from SLURM cluster to Waldur
class WaldurSyncDirection(Enum):
    PULL = "pull"
    PUSH = "push"


waldur_api_url = os.environ.get("WALDUR_API_URL")
waldur_api_token = os.environ.get("WALDUR_API_TOKEN")
waldur_offering_uuid = os.environ.get("WALDUR_OFFERING_UUID")
waldur_offering_name = os.environ.get("WALDUR_OFFERING_NAME", "Single offering")
WALDUR_OFFERINGS = []

if all([waldur_api_url, waldur_api_token, waldur_offering_uuid]):
    logger.info("Using environment variables as a config source")
    WALDUR_OFFERINGS = [
        {
            "name": waldur_offering_name,
            "api_url": waldur_api_url,
            "api_token": waldur_api_token,
            "uuid": waldur_offering_uuid,
        }
    ]
else:
    config_file_path = os.environ.get("WALDUR_CONFIG_FILE_PATH")
    if config_file_path is None:
        logger.error("WALDUR_CONFIG_FILE_PATH variable is missing.")
        exit(1)

    logger.info("Using %s as a config source", config_file_path)

    with open(config_file_path, "r") as stream:
        config = yaml.safe_load(stream)
        offering_list = config["offerings"]
        for offering_info in offering_list:
            WALDUR_OFFERINGS.append(
                {
                    "name": offering_info["name"],
                    "api_url": offering_info["waldur_api_url"],
                    "api_token": offering_info["waldur_api_token"],
                    "uuid": offering_info["waldur_offering_uuid"],
                }
            )


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


ENABLE_USER_HOMEDIR_ACCOUNT_CREATION = os.environ.get(
    "ENABLE_USER_HOMEDIR_ACCOUNT_CREATION", "false"
)

ENABLE_USER_HOMEDIR_ACCOUNT_CREATION = ENABLE_USER_HOMEDIR_ACCOUNT_CREATION.lower() in [
    "yes",
    "true",
]
waldur_site_agent_version = version("waldur-site-agent")

user_agent_dict = {
    "pull": f"waldur-site-agent-pull/{waldur_site_agent_version}",
    "push": f"waldur-site-agent-push/{waldur_site_agent_version}",
}

USER_AGENT = user_agent_dict.get(WALDUR_SYNC_DIRECTION)


slurm_backend = SlurmBackend()

sentry_dsn = os.environ.get("SENTRY_DSN")

if sentry_dsn:
    import sentry_sdk

    sentry_sdk.init(
        dsn=sentry_dsn,
    )
