"""Main application module."""

from waldur_site_agent.backends import logger

from . import AgentMode, agent_membership_sync, agent_order_process, agent_report, common_utils


def main() -> None:
    """Entrypoint for the application."""
    configuration = common_utils.init_configuration()
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


if __name__ == "__main__":
    main()
