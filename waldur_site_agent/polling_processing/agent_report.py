"""Agent responsible for usage and limits reporting."""

from time import sleep

from waldur_site_agent.backends import logger
from waldur_site_agent.common import WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent
    logger.info("Synching data to Waldur")
    while True:
        logger.info("Number of offerings to process: %s", len(waldur_offerings))
        for offering in waldur_offerings:
            try:
                processor = common_processors.OfferingReportProcessor(
                    offering, user_agent, configuration.timezone
                )
                processor.process_offering()
            except Exception as e:
                logger.exception("The application crashed due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60)
