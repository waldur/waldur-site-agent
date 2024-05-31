"""Init file for the main module."""

import os
import sys
from dataclasses import dataclass
from enum import Enum
from importlib.metadata import version
from pathlib import Path

import yaml

from waldur_site_agent.backends import logger


@dataclass
class Offering:
    """Offering structure for config file parsing."""

    name: str = ""
    api_url: str = ""
    api_token: str = ""
    uuid: str = ""
    backend_type: str = ""


class AgentMode(Enum):
    """Enum for agent modes."""

    ORDER_PROCESS = "order_process"
    REPORT = "report"
    MEMBERSHIP_SYNC = "membership_sync"


waldur_api_url = os.environ.get("WALDUR_API_URL", "")
waldur_api_token = os.environ.get("WALDUR_API_TOKEN", "")
waldur_offering_uuid = os.environ.get("WALDUR_OFFERING_UUID", "")
waldur_offering_name = os.environ.get("WALDUR_OFFERING_NAME", "Single offering")
waldur_backend_type = os.environ.get("WALDUR_BACKEND_TYPE", "slurm").lower()
WALDUR_OFFERINGS = []

if all([waldur_api_url, waldur_api_token, waldur_offering_uuid]):
    logger.info("Using environment variables as a config source")
    WALDUR_OFFERINGS = [
        Offering(
            waldur_offering_name,
            waldur_api_url,
            waldur_api_token,
            waldur_offering_uuid,
            waldur_backend_type,
        )
    ]
else:
    config_file_path = os.environ.get("WALDUR_CONFIG_FILE_PATH")
    if config_file_path is None:
        logger.error("WALDUR_CONFIG_FILE_PATH variable is missing.")
        sys.exit(1)

    logger.info("Using %s as a config source", config_file_path)

    with Path(config_file_path).open(encoding="UTF-8") as stream:
        config = yaml.safe_load(stream)
        offering_list = config["offerings"]
        for offering_info in offering_list:
            WALDUR_OFFERINGS.append(
                Offering(
                    offering_info["name"],
                    offering_info["waldur_api_url"],
                    offering_info["waldur_api_token"],
                    offering_info["waldur_offering_uuid"],
                    offering_info["backend_type"].lower(),
                )
            )


WALDUR_SITE_AGENT_MODE = os.environ["WALDUR_SITE_AGENT_MODE"]

if WALDUR_SITE_AGENT_MODE not in [
    AgentMode.ORDER_PROCESS.value,
    AgentMode.REPORT.value,
    AgentMode.MEMBERSHIP_SYNC.value,
]:
    logger.error(
        "WALDUR_SITE_AGENT_MODE has invalid value: %s. " "Possible values are %s, %s and %s",
        WALDUR_SITE_AGENT_MODE,
        AgentMode.ORDER_PROCESS.value,
        AgentMode.REPORT.value,
        AgentMode.MEMBERSHIP_SYNC.value,
    )
    sys.exit(1)

waldur_site_agent_version = version("waldur-site-agent")

user_agent_dict = {
    AgentMode.ORDER_PROCESS.value: "waldur-site-agent-order-process/" + waldur_site_agent_version,
    AgentMode.REPORT.value: "waldur-site-agent-report/" + waldur_site_agent_version,
    AgentMode.MEMBERSHIP_SYNC.value: "waldur-site-agent-membership-sync/"
    + waldur_site_agent_version,
}

USER_AGENT = user_agent_dict.get(WALDUR_SITE_AGENT_MODE)


sentry_dsn = os.environ.get("SENTRY_DSN")

if sentry_dsn:
    import sentry_sdk

    sentry_sdk.init(
        dsn=sentry_dsn,
    )
