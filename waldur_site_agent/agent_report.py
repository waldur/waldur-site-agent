"""Agent responsible for usage and limits reporting."""

from time import sleep
from typing import List

from waldur_site_agent.backends import logger

from . import (
    WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES,
    Offering,
    WaldurAgentConfiguration,
    processors,
)


def process_offerings(waldur_offerings: List[Offering], user_agent: str = "") -> None:
    """Processes list of offerings."""
    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        try:
            processor = processors.OfferingReportProcessor(offering, user_agent)
            processor.process_offering()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)


def start(configuration: WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    logger.info("Synching data to Waldur")
    while True:
        try:
            process_offerings(configuration.waldur_offerings, configuration.waldur_user_agent)
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60)
