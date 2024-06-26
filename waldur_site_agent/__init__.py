"""Init file for the main module."""

from dataclasses import dataclass, field
from enum import Enum

MARKETPLACE_SLURM_OFFERING_TYPE = "Marketplace.Slurm"


@dataclass
class Offering:
    """Offering structure for config file parsing."""

    name: str = ""
    api_url: str = ""
    api_token: str = ""
    uuid: str = ""
    backend_type: str = ""
    backend_settings: dict = field(default_factory=dict)
    backend_components: dict = field(default_factory=dict)


class AgentMode(Enum):
    """Enum for agent modes."""

    ORDER_PROCESS = "order_process"
    REPORT = "report"
    MEMBERSHIP_SYNC = "membership_sync"


@dataclass
class WaldurAgentConfiguration:
    """Dataclass for the agent configuration."""

    waldur_offerings: list = field(default_factory=list)
    waldur_site_agent_mode: str = ""
    waldur_user_agent: str = ""
    waldur_site_agent_version: str = ""
    sentry_dsn: str = ""
