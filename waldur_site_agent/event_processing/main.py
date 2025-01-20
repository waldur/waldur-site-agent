"""Entrypoint for event processing loop."""

import sys
import time

from waldur_site_agent.backends import logger
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing import utils


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for event-based offering processing."""
    try:
        utils.run_initial_offering_processing(
            configuration.waldur_offerings, configuration.waldur_user_agent
        )

        mqtt_consumers_map = utils.start_mqtt_consumers(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
        )

        with utils.signal_handling(mqtt_consumers_map):
            while True:
                time.sleep(60 * 60)
    except Exception as e:
        logger.error("Error in main process: %s", e)
        if "mqtt_consumers_map" in locals():
            utils.stop_mqtt_consumers(mqtt_consumers_map)
        sys.exit(1)
