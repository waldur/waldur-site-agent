"""Agent responsible for membership control."""

from time import sleep

from waldur_site_agent.backends import logger
from waldur_site_agent.common import WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent
    while True:
        logger.info("Number of offerings to process: %s", len(waldur_offerings))
        for offering in waldur_offerings:
            try:
                if offering.mqtt_enabled or offering.stomp_enabled:
                    logger.info(
                        "Skipping HTTP polling for the offering %s, "
                        "because it uses event-based processing",
                        offering.name,
                    )
                    continue

                processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
                processor.process_offering()
            except Exception as e:
                logger.exception("Unable to process the offering due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60)
