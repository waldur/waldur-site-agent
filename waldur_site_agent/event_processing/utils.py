"""Functions for management of MQTT signals."""

from __future__ import annotations

import signal
import sys
import types
from collections.abc import Generator
from contextlib import contextmanager
from typing import Union

import paho.mqtt.client as mqtt
import stomp
from waldur_api_client.api.marketplace_orders import marketplace_orders_list

from waldur_site_agent.backends import logger
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.event_subscription_manager import EventSubscriptionManager
from waldur_site_agent.event_processing.structures import (
    EventSubscription,
    MqttConsumer,
    MqttConsumersMap,
    StompConsumersMap,
    UserData,
)


def on_connect(
    client: mqtt.Client,
    userdata: UserData,
    flags: mqtt.ConnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: Union[mqtt.Properties, None],
) -> None:
    """Order-processing handler for MQTT connection event."""
    del flags, properties
    logger.debug("Consumer connected with result code %s", reason_code)
    offering_uuid = userdata["offering"].uuid
    if reason_code.is_failure:
        logger.error("Consumer connection error (%s): %s", offering_uuid, reason_code.getName())
    else:
        event_subscription_uuid = userdata["event_subscription"]["uuid"]
        topic_postfix = userdata["topic_postfix"]
        topic_name = (
            f"subscription/{event_subscription_uuid}/offering/{offering_uuid}/{topic_postfix}"
        )
        logger.debug("Subscribing to the topic %s", topic_name)
        client.subscribe(topic_name)


def setup_stomp_offering_subscriptions(
    waldur_offering: common_structures.Offering, waldur_user_agent: str
) -> list[stomp.WSStompConnection]:
    """Set up STOMP subscriptions for the specified offering."""
    stomp_connections: list[stomp.WSStompConnection] = []
    for object_type in ["order", "user_role", "resource"]:
        event_subscription_manager = EventSubscriptionManager(
            waldur_offering, None, None, waldur_user_agent, object_type
        )
        event_subscription: EventSubscription | None = (
            event_subscription_manager.get_or_create_event_subscription()
        )
        if event_subscription is None:
            logger.error(
                "Failed to create event subscription for the offering %s (%s), object type %s",
                waldur_offering.name,
                waldur_offering.uuid,
                object_type,
            )
            continue
        connection = event_subscription_manager.start_stomp_connection(event_subscription)
        if connection is None:
            logger.error(
                "Failed to start STOMP connection for the offering %s (%s), object type %s",
                waldur_offering.name,
                waldur_offering.uuid,
                object_type,
            )
            event_subscription_manager.delete_event_subscription(event_subscription)
            continue

        stomp_connections.append((connection, event_subscription, waldur_offering))

    return stomp_connections


def setup_offering_subscriptions(
    waldur_offering: common_structures.Offering, waldur_user_agent: str
) -> list[MqttConsumer]:
    """Set up MQTT subscriptions for the specified offering."""
    object_type_to_handler = {
        "order": handlers.on_order_message_mqtt,
        "user_role": handlers.on_user_role_message_mqtt,
        "resource": handlers.on_resource_message_mqtt,
    }

    event_subscriptions: list[MqttConsumer] = []
    for object_type in ["order", "user_role", "resource"]:
        on_message_handler = object_type_to_handler[object_type]
        event_subscription_manager = EventSubscriptionManager(
            waldur_offering, on_connect, on_message_handler, waldur_user_agent, object_type
        )
        event_subscription = event_subscription_manager.create_event_subscription()
        if event_subscription is None:
            logger.error(
                "Failed to create event subscription for the offering %s (%s), object type %s",
                waldur_offering.name,
                waldur_offering.uuid,
                object_type,
            )
            continue
        consumer = event_subscription_manager.start_mqtt_consumer(event_subscription)
        if consumer is None:
            logger.error(
                "Failed to start mqtt consumer for the offering %s (%s), object type %s",
                waldur_offering.name,
                waldur_offering.uuid,
                object_type,
            )
            event_subscription_manager.delete_event_subscription(event_subscription)
            continue

        event_subscriptions.append((consumer, event_subscription, waldur_offering))

    return event_subscriptions


def start_stomp_consumers(
    waldur_offerings: list[common_structures.Offering],
    waldur_user_agent: str,
) -> StompConsumersMap:
    """Start multiple STOMP consumers."""
    stomp_consumers_map: StompConsumersMap = {}
    for waldur_offering in waldur_offerings:
        if not waldur_offering.stomp_enabled:
            logger.info("STOMP feature is disabled for the offering")
            continue

        stomp_connections = setup_stomp_offering_subscriptions(waldur_offering, waldur_user_agent)
        if stomp_connections:
            stomp_consumers_map[(waldur_offering.name, waldur_offering.uuid)] = stomp_connections

    return stomp_consumers_map


def stop_stomp_consumers(
    stomp_consumers_map: StompConsumersMap,
) -> None:
    """Stop STOMP consumers."""
    for (offering_name, offering_uuid), consumers in stomp_consumers_map.items():
        logger.info("Stopping STOMP connections for %s (%s)", offering_name, offering_uuid)
        for (
            connection,
            event_subscription,
            offering,
        ) in consumers:
            try:
                event_subscription_manager = EventSubscriptionManager(
                    offering,
                )
                logger.info(
                    "Stopping STOMP connection for %s (%s), observable object type: %s",
                    offering_name,
                    offering_uuid,
                    event_subscription["observable_objects"][0]["object_type"],
                )
                event_subscription_manager.stop_stomp_connection(connection)
            except Exception as exc:
                logger.exception("Unable to stop the connection, reason: %s", exc)


def start_mqtt_consumers(
    waldur_offerings: list[common_structures.Offering],
    waldur_user_agent: str,
) -> MqttConsumersMap:
    """Start multiple MQTT consumers."""
    mqtt_consumers_map: MqttConsumersMap = {}
    for waldur_offering in waldur_offerings:
        if not waldur_offering.mqtt_enabled:
            logger.info("MQTT feature is disabled for the offering")
            continue

        event_subscriptions = setup_offering_subscriptions(waldur_offering, waldur_user_agent)
        if event_subscriptions:
            mqtt_consumers_map[(waldur_offering.name, waldur_offering.uuid)] = event_subscriptions

    return mqtt_consumers_map


def stop_mqtt_consumers(
    mqtt_consumers_map: MqttConsumersMap,
) -> None:
    """Stop mqtt consumers and delete event subscriptions."""
    for (offering_name, offering_uuid), subscriptions in mqtt_consumers_map.items():
        logger.info("Stopping subscriptions for %s (%s)", offering_name, offering_uuid)
        for (
            mqttc,
            event_subscription,
            offering,
        ) in subscriptions:
            try:
                event_subscription_manager = EventSubscriptionManager(
                    offering,
                )
                logger.info(
                    "Stopping MQTT consumer for %s (%s), observable object type: %s",
                    offering_name,
                    offering_uuid,
                    event_subscription["observable_objects"][0]["object_type"],
                )
                event_subscription_manager.stop_mqtt_consumer(mqttc)
                logger.info("Deleting event subscription for %s (%s)", offering_name, offering_uuid)
                event_subscription_manager.delete_event_subscription(event_subscription)
            except Exception as exc:
                logger.exception("Unable to stop the consumer, reason: %s", exc)


@contextmanager
def signal_handling(
    mqtt_consumers_map: MqttConsumersMap,
    stomp_consumers_map: StompConsumersMap,
) -> Generator[None, None, None]:
    """Context manager for handling signals gracefully."""

    def signal_handler(signum: int, _: types.FrameType | None) -> None:
        signal_name = signal.Signals(signum).name
        logger.info("Received %s signal. Shutting down gracefully...", signal_name)
        stop_mqtt_consumers(mqtt_consumers_map)
        stop_stomp_consumers(stomp_consumers_map)
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
            original_handlers[sig] = signal.signal(sig, signal_handler)
        logger.info("Signal handlers set")
        yield
    finally:
        # Restore original handlers
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)


def run_initial_offering_processing(
    waldur_offerings: list[common_structures.Offering], user_agent: str = ""
) -> None:
    """Runs processing of offerings with MQTT feature enabled."""
    logger.info("Processing offerings with MQTT/STOMP feature enabled")
    for offering in waldur_offerings:
        try:
            if offering.mqtt_enabled or offering.stomp_enabled:
                process_offering(offering, user_agent)
        except Exception as e:
            logger.exception("Error occurred during initial offering process: %s", e)


def process_offering(offering: common_structures.Offering, user_agent: str = "") -> None:
    """Processes the specified offering."""
    logger.info("Processing offering %s (%s)", offering.name, offering.uuid)

    order_processor = common_processors.OfferingOrderProcessor(offering, user_agent)
    logger.info("Running offering order process")
    order_processor.process_offering()

    membership_processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
    logger.info("Running offering membership process")
    membership_processor.process_offering()


def send_agent_health_checks(offerings: list[common_structures.Offering], user_agent: str) -> None:
    """Sends agent health checks for the specified offerings."""
    for offering in offerings:
        try:
            processor = common_processors.OfferingOrderProcessor(offering, user_agent)
            marketplace_orders_list.sync(
                client=processor.waldur_rest_client, offering_uuid=offering.uuid, page_size=1
            )
        except Exception as e:
            logger.error(
                "Failed to send agent health check for the offering %s: %s", offering.name, e
            )
