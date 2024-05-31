"""Main application module."""

from waldur_site_agent import (
    agent_membership_sync,
    agent_order_process,
    agent_report,
    logger,
)

from . import WALDUR_SITE_AGENT_MODE, AgentMode, waldur_site_agent_version


def main() -> None:
    """Entrypoint for the application."""
    logger.info("Waldur site Agent version: %s, site: SLURM", waldur_site_agent_version)

    logger.info("Running agent in %s mode", WALDUR_SITE_AGENT_MODE)
    if AgentMode.ORDER_PROCESS.value == WALDUR_SITE_AGENT_MODE:
        agent_order_process.start()
    if AgentMode.REPORT.value == WALDUR_SITE_AGENT_MODE:
        agent_report.start()
    if AgentMode.MEMBERSHIP_SYNC.value == WALDUR_SITE_AGENT_MODE:
        agent_membership_sync.start()


if __name__ == "__main__":
    main()
