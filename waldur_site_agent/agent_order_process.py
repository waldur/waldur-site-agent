"""Module for order processing."""

from __future__ import annotations

import json
import sys
from time import sleep
from typing import Dict, List

import paho.mqtt.client as mqtt

from waldur_site_agent.backends import logger

from . import (
    WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES,
    Offering,
    WaldurAgentConfiguration,
    processors,
    signal_utils,
)


def on_message(client: mqtt.Client, userdata: Dict, msg: mqtt.MQTTMessage) -> None:
    """Order-processing handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]

    order_uuid = message["order_uuid"]
    processor = processors.OfferingOrderProcessor(offering, user_agent)
    order = processor.get_order_info(order_uuid)
    if order is None:
        logger.error("Failed to process order %s", order_uuid)
        return
    processor.process_order(order)


def process_offering(offering: Offering, user_agent: str = "") -> None:
    """Processes the specified offering."""
    processor = processors.OfferingOrderProcessor(offering, user_agent)
    processor.process_offering()


def run_initial_offering_processing(waldur_offerings: List[Offering], user_agent: str = "") -> None:
    """Runs processing of offerings with MQTT feature enabled."""
    logger.info("Processing offerings with MQTT feature enabled")
    for offering in waldur_offerings:
        try:
            if not offering.mqtt_enabled:
                continue

            process_offering(offering, user_agent)
        except Exception as e:
            logger.exception("Error occurred during initial offering process: %s", e)


def start_periodic_offering_processing(
    waldur_offerings: List[Offering], user_agent: str = ""
) -> None:
    """Processes offerings one-by-one periodically."""
    while True:
        logger.info("Number of offerings to process: %s", len(waldur_offerings))
        for offering in waldur_offerings:
            try:
                if offering.mqtt_enabled:
                    logger.info(
                        "Skipping HTTP polling for the offering %s, because it uses mqtt feature",
                        offering.name,
                    )
                    continue

                process_offering(offering, user_agent)
            except Exception as e:
                logger.exception("Unable to process the offering due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES * 60)


def start(configuration: WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    try:
        run_initial_offering_processing(
            configuration.waldur_offerings, configuration.waldur_user_agent
        )

        mqtt_consumers_map = signal_utils.start_mqtt_consumers(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
            configuration.waldur_site_agent_mode,
            on_message,
        )

        if mqtt_consumers_map:
            with signal_utils.signal_handling(mqtt_consumers_map):
                start_periodic_offering_processing(
                    configuration.waldur_offerings, configuration.waldur_user_agent
                )
        else:
            start_periodic_offering_processing(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )

    except Exception as e:
        logger.error("Error in main process: %s", e)
        if "mqtt_consumers_map" in locals():
            signal_utils.stop_mqtt_consumers(mqtt_consumers_map)
        sys.exit(1)
