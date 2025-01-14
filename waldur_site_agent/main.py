"""Main application module."""

from waldur_site_agent.backends import logger
from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import AgentMode
from waldur_site_agent.event_processing import main as event_processing_main
from waldur_site_agent.polling_processing import (
    agent_membership_sync,
    agent_order_process,
    agent_report,
)


def main() -> None:
    """Entrypoint for the application."""
    configuration = utils.init_configuration()
    logger.info(
        "Waldur site Agent version: %s, site: SLURM", configuration.waldur_site_agent_version
    )

    logger.info("Running agent in %s mode", configuration.waldur_site_agent_mode)
    if AgentMode.ORDER_PROCESS.value == configuration.waldur_site_agent_mode:
        agent_order_process.start(configuration)
    if AgentMode.REPORT.value == configuration.waldur_site_agent_mode:
        agent_report.start(configuration)
    if AgentMode.MEMBERSHIP_SYNC.value == configuration.waldur_site_agent_mode:
        agent_membership_sync.start(configuration)
    if AgentMode.EVENT_PROCESS.value == configuration.waldur_site_agent_mode:
        event_processing_main.start(configuration)


if __name__ == "__main__":
    main()
