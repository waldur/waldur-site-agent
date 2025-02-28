"""Functions for management of MQTT signals."""

from __future__ import annotations

import json
import signal
import sys
import types
from contextlib import contextmanager
from typing import Generator, List, Union

import paho.mqtt.client as mqtt

from waldur_site_agent.backends import logger
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing.event_subscription_manager import EventSubscriptionManager
from waldur_site_agent.event_processing.structures import (
    MqttConsumer,
    MqttConsumersMap,
    OrderMessage,
    ResourceMessage,
    UserData,
    UserRoleMessage,
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


def setup_offering_subscriptions(
    waldur_offering: common_structures.Offering, waldur_user_agent: str
) -> List[MqttConsumer]:
    """Set up MQTT subscriptions for the specified offering."""
    object_type_to_handler = {
        "order": on_order_message,
        "user_role": on_user_role_message,
        "resource": on_resource_message,
    }

    event_subscriptions: List[MqttConsumer] = []
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


def start_mqtt_consumers(
    waldur_offerings: List[common_structures.Offering],
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
            original_handlers[sig] = signal.signal(sig, signal_handler)
        logger.info("Signal handlers set")
        yield
    finally:
        # Restore original handlers
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)


def on_order_message(client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage) -> None:
    """Order-processing handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: OrderMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]

    order_uuid = message["order_uuid"]
    try:
        processor = common_processors.OfferingOrderProcessor(offering, user_agent)
        order = processor.get_order_info(order_uuid)
        if order is None:
            logger.error("Failed to process order %s", order_uuid)
            return
        processor.process_order_with_retries(order)
    except Exception as e:
        logger.error("Failed to process order %s: %s", order_uuid, e)


def on_user_role_message(client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage) -> None:
    """Membership sync handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: UserRoleMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]
    user_uuid = message["user_uuid"]
    user_username = message["user_username"]
    project_name = message["project_name"]
    project_uuid = message["project_uuid"]
    role_granted = message["granted"]

    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        logger.info(
            "Processing %s (%s) user role changed event in project %s, granted: %s",
            user_username,
            user_uuid,
            project_name,
            role_granted,
        )

        processor.process_user_role_changed(user_uuid, project_uuid, role_granted)
    except Exception as e:
        logger.error(
            "Failed to process user %s (%s) role change in project %s (%s) (granted: %s): %s",
            user_username,
            user_uuid,
            project_name,
            project_uuid,
            role_granted,
            e,
        )


def on_resource_message(client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage) -> None:
    """Resource update handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: ResourceMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]
    resource_uuid = message["resource_uuid"]

    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        processor.process_resource_by_uuid(resource_uuid)
    except Exception as e:
        logger.error("Failed to process resource %s: %s", resource_uuid, e)


def run_initial_offering_processing(
    waldur_offerings: List[common_structures.Offering], user_agent: str = ""
) -> None:
    """Runs processing of offerings with MQTT feature enabled."""
    logger.info("Processing offerings with MQTT feature enabled")
    for offering in waldur_offerings:
        try:
            if not offering.mqtt_enabled:
                continue

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
