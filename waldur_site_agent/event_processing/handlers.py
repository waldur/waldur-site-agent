"""Handlers for different events and protocols."""

import json

import paho.mqtt.client as mqtt
import stomp
import stomp.utils
from waldur_api_client.models import OrderState

from waldur_site_agent.backend import logger
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures
from waldur_site_agent.event_processing.structures import (
    BackendResourceRequestMessage,
    OrderMessage,
    ResourceMessage,
    ServiceAccountMessage,
    UserData,
    UserRoleMessage,
)


def on_order_message_mqtt(client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage) -> None:
    """Order-processing handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: OrderMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]

    order_uuid = message["order_uuid"]
    order_state = message.get("order_state", "")

    # Skip done and erred orders to avoid duplicate processing
    if order_state in [OrderState.DONE, OrderState.ERRED]:
        logger.info("Skipping order %s with finished state %s", order_uuid, order_state)
        return

    try:
        processor = common_processors.OfferingOrderProcessor(offering, user_agent)
        order = processor.get_order_info(order_uuid)
        if order is None:
            logger.error("Failed to process order %s", order_uuid)
            return
        processor.process_order_with_retries(order)
    except Exception as e:
        logger.error("Failed to process order %s: %s", order_uuid, e)


def on_user_role_message_mqtt(
    client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage
) -> None:
    """Membership sync handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: UserRoleMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]
    user_uuid = message.get("user_uuid")
    project_name = message["project_name"]
    project_uuid = message["project_uuid"]

    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        if user_uuid:
            user_username = message["user_username"]
            role_granted = message["granted"]
            if role_granted is not None:
                logger.info(
                    "Processing %s (%s) user role changed event in project %s, granted: %s",
                    user_username,
                    user_uuid,
                    project_name,
                    role_granted,
                )
                processor.process_user_role_changed(user_uuid, project_uuid, role_granted)
        else:
            logger.info(
                "Processing full project all users sync event for project %s",
                project_name,
            )
            processor.process_project_user_sync(project_uuid)

    except Exception as e:
        if user_uuid:
            logger.error(
                "Failed to process user %s (%s) role change in project %s (%s) (granted: %s): %s",
                user_username,
                user_uuid,
                project_name,
                project_uuid,
                role_granted,
                e,
            )
        else:
            logger.error(
                "Failed to process full project all users sync event for project %s: %s",
                project_uuid,
                e,
            )


def on_resource_message_mqtt(
    client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage
) -> None:
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


def on_service_account_message_mqtt(
    client: mqtt.Client, userdata: UserData, msg: mqtt.MQTTMessage
) -> None:
    """Resource update handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message: ServiceAccountMessage = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]
    service_account_username = message["service_account_username"]
    service_account_uuid = message["service_account_uuid"]
    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        processor.process_service_account_creation(service_account_username)
    except Exception as e:
        logger.error(
            "Failed to process creation of service account %s (%s): %s",
            service_account_username,
            service_account_uuid,
            e,
        )


def on_order_message_stomp(
    frame: stomp.utils.Frame, offering: structures.Offering, user_agent: str
) -> None:
    """Order-processing handler for STOMP message event."""
    message: OrderMessage = json.loads(frame.body)
    logger.info("Processing message: %s", message)
    order_uuid = message["order_uuid"]
    order_state = message.get("order_state", "")

    # Skip done and erred orders to avoid duplicate processing
    if order_state in [OrderState.DONE, OrderState.ERRED]:
        logger.info("Skipping order %s with finished state %s", order_uuid, order_state)
        return

    try:
        processor = common_processors.OfferingOrderProcessor(offering, user_agent)
        order = processor.get_order_info(order_uuid)
        if order is None:
            logger.error("Failed to process order %s", order_uuid)
            return
        processor.process_order_with_retries(order)
    except Exception as e:
        logger.error("Failed to process order %s: %s", order_uuid, e)


def on_user_role_message_stomp(
    frame: stomp.utils.Frame, offering: structures.Offering, user_agent: str
) -> None:
    """Membership sync handler for STOMP message event."""
    message: UserRoleMessage = json.loads(frame.body)
    logger.info("Received message: %s on topic %s", message, frame.headers.get("destination"))
    user_uuid = message.get("user_uuid")
    project_name = message["project_name"]
    project_uuid = message["project_uuid"]

    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        if user_uuid:
            user_username = message["user_username"]
            role_granted = message["granted"]
            if role_granted is None:
                logger.error("Missing required field 'granted' for user role change")
                return
            logger.info(
                "Processing %s (%s) user role changed event in project %s, granted: %s",
                user_username,
                user_uuid,
                project_name,
                role_granted,
            )
            processor.process_user_role_changed(user_uuid, project_uuid, role_granted)
        else:
            logger.info("Processing full project all users sync event for project %s", project_name)
            processor.process_project_user_sync(project_uuid)
    except Exception as e:
        if user_uuid:
            logger.error(
                "Failed to process user %s (%s) role change in project %s (%s) (granted: %s): %s",
                user_username,
                user_uuid,
                project_name,
                project_uuid,
                role_granted,
                e,
            )
        else:
            logger.error(
                "Failed to process full project all users sync event for project %s: %s",
                project_uuid,
                e,
            )


def on_resource_message_stomp(
    frame: stomp.utils.Frame, offering: structures.Offering, user_agent: str
) -> None:
    """Resource update handler for STOMP message event."""
    message: ResourceMessage = json.loads(frame.body)
    resource_uuid = message["resource_uuid"]

    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        processor.process_resource_by_uuid(resource_uuid)
    except Exception as e:
        logger.error("Failed to process resource %s: %s", resource_uuid, e)


def on_importable_resources_message_stomp(
    frame: stomp.utils.Frame, offering: structures.Offering, user_agent: str
) -> None:
    """Handler for importable resource list request for STOMP message event."""
    message: BackendResourceRequestMessage = json.loads(frame.body)
    request_uuid = message["backend_resource_request_uuid"]
    try:
        processor = common_processors.OfferingImportableResourcesProcessor(offering, user_agent)
        processor.process_request(request_uuid)
    except Exception as e:
        logger.error("Failed to process importable resource list request %s: %s", request_uuid, e)


def on_service_account_message_stomp(
    frame: stomp.utils.Frame, offering: structures.Offering, user_agent: str
) -> None:
    """Service account create handler for STOMP."""
    message: ServiceAccountMessage = json.loads(frame.body)
    service_account_uuid = message["service_account_uuid"]
    service_account_username = message["service_account_username"]
    try:
        processor = common_processors.OfferingMembershipProcessor(offering, user_agent)
        processor.process_service_account_creation(service_account_username)
    except Exception as e:
        logger.error(
            "Failed to process creation of service account %s (%s): %s",
            service_account_username,
            service_account_uuid,
            e,
        )
