"""Functions for management of MQTT signals."""

from __future__ import annotations

import signal
import sys
import types
from contextlib import contextmanager
from typing import Callable, Dict, Generator, List, Tuple, Union

import paho.mqtt.client as mqtt

from waldur_site_agent.backends import logger
from waldur_site_agent.event_subscription_processor import EventSubscriptionManager

from . import AgentMode, Offering

MODE_TO_OBJECT_TYPE_MAP = {
    AgentMode.ORDER_PROCESS.value: "order",
    AgentMode.MEMBERSHIP_SYNC.value: "user_role",
}


def on_connect(
    client: mqtt.Client,
    userdata: Dict,
    flags: mqtt.ConnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: Union[mqtt.Properties, None],
) -> None:
    """Order-processing handler for MQTT connection event."""
    del flags, properties
    logger.info("Consumer connected with result code %s", reason_code)
    offering_uuid = userdata["offering"].uuid
    if not reason_code.is_failure:
        event_subscription_uuid = userdata["event_subscription"]["uuid"]
        topic_postfix = userdata["topic_postfix"]
        topic_name = (
            f"subscription/{event_subscription_uuid}/offering/{offering_uuid}/{topic_postfix}"
        )
        logger.info("Subscribing to the topic %s", topic_name)
        client.subscribe(topic_name)
    else:
        logger.error("Consumer connection error (%s): %s", offering_uuid, reason_code.getName())


def start_mqtt_consumers(
    waldur_offerings: List[Offering],
    waldur_user_agent: str,
    waldur_site_agent_mode: str,
    on_message: Callable,
) -> Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]]:
    """Start multiple MQTT consumers."""
    mqtt_consumers_map = {}
    for waldur_offering in waldur_offerings:
        if not waldur_offering.mqtt_enabled:
            logger.info("MQTT feature is disabled for the offering")
            continue

        event_subscription_manager = EventSubscriptionManager(
            waldur_offering,
            on_connect,
            on_message,
            waldur_user_agent,
            waldur_site_agent_mode,
        )
        observable_object_type = MODE_TO_OBJECT_TYPE_MAP[waldur_site_agent_mode]
        event_subscription = event_subscription_manager.create_event_subscription(
            observable_object_type
        )
        if event_subscription is None:
            logger.error(
                "Failed to create event subscription for the offering %s (%s)",
                waldur_offering.name,
                waldur_offering.uuid,
            )
            continue

        consumer = event_subscription_manager.start_mqtt_consumer(event_subscription)
        if consumer is None:
            logger.error(
                "Failed to start mqtt consumer for the offering %s (%s)",
                waldur_offering.name,
                waldur_offering.uuid,
            )
            event_subscription_manager.delete_event_subscription(event_subscription)
            continue
        mqtt_consumers_map[(waldur_offering.name, waldur_offering.uuid)] = (
            consumer,
            event_subscription,
            waldur_offering,
        )

    return mqtt_consumers_map


def stop_mqtt_consumers(
    mqtt_consumers_map: Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]],
) -> None:
    """Stop mqtt consumers and delete event subscriptions."""
    for (offering_name, offering_uuid), (
        mqttc,
        event_subscription,
        offering,
    ) in mqtt_consumers_map.items():
        try:
            event_subscription_manager = EventSubscriptionManager(
                offering,
            )
            logger.info("Stopping MQTT consumer for %s (%s)", offering_name, offering_uuid)
            event_subscription_manager.stop_mqtt_consumer(mqttc)
            logger.info("Deleting event subscription for %s (%s)", offering_name, offering_uuid)
            event_subscription_manager.delete_event_subscription(event_subscription)
        except Exception as exc:
            logger.exception("Unable to stop the consumer, reason: %s", exc)


@contextmanager
def signal_handling(
    mqtt_consumers_map: Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]],
) -> Generator[None, None, None]:
    """Context manager for handling signals gracefully."""

    def signal_handler(signum: int, _: types.FrameType | None) -> None:
        signal_name = signal.Signals(signum).name
        logger.info("Received %s signal. Shutting down gracefully...", signal_name)
        stop_mqtt_consumers(mqtt_consumers_map)
        sys.exit(0)

    # Register signal handlers
    signals = (
        signal.SIGTERM,
        signal.SIGINT,
        signal.SIGTSTP,
        signal.SIGQUIT,
    )
    original_handlers = {}

    try:
        logger.info("Setting up signal handlers")
        # Save original handlers and set new ones
        for sig in signals:
            logger.info("Setting up signal handler for %s", sig)
            original_handlers[sig] = signal.signal(sig, signal_handler)
            logger.info("Signal handler for %s set", sig)
        logger.info("Signal handlers set")
        yield
    finally:
        # Restore original handlers
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)
