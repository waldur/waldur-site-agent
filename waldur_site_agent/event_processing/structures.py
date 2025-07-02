"""This module defines data structures used in event processing for the Waldur Site Agent."""

from __future__ import annotations

from typing import TypedDict

import paho.mqtt.client as mqtt
import stomp

from waldur_site_agent.common import structures as common_structures


class ObservableObject(TypedDict):
    """Represents an object that can be observed.

    Attributes:
        object_type (str): The model name of the observable object.
        object_uuid (str): The UUID of the object. If None, indicates that the subscriber
                          observes all existing objects of object_type.
    """

    object_type: str
    object_uuid: str


class EventSubscription(TypedDict):
    """Represents an event subscription."""

    uuid: str
    user_uuid: str
    observable_objects: list[ObservableObject]


# A tuple containing MQTT client, subscription, and offering information.
MqttConsumer = tuple[mqtt.Client, EventSubscription, common_structures.Offering]

# A tuple of offering name and UUID used as a key for consumer mapping.
MqttConsumerKey = tuple[str, str]

# A tuple of offering name and UUID used as a key for connection mapping.
StompConsumerKey = tuple[str, str]

# A tuple containing STOMP connection, subscription, and offering information.
StompConsumer = tuple[stomp.WSStompConnection, EventSubscription, common_structures.Offering]

# A dictionary mapping consumer keys to lists of MQTT consumers.
MqttConsumersMap = dict[MqttConsumerKey, list[MqttConsumer]]
StompConsumersMap = dict[StompConsumerKey, list[StompConsumer]]


class UserData(TypedDict):
    """Represents user data for event handling and processing."""

    event_subscription: EventSubscription
    offering: common_structures.Offering
    user_agent: str
    topic_postfix: str


class UserRoleMessage(TypedDict):
    """Represents a message about user role changes in a project.

    Attributes:
        user_uuid (str, optional): The UUID of the user whose role is being modified.
        user_username (str, optional): The username of the user whose role is being modified.
        project_uuid (str): The UUID of the project where the role change occurred.
        project_name (str): The name of the project where the role change occurred.
        role_name (str): The name of the role that was granted or revoked.
        granted (bool, optional): True if the role was granted, False if it was revoked.
    """

    user_uuid: str | None
    user_username: str | None
    project_uuid: str
    project_name: str
    role_name: str
    granted: bool | None


class ResourceMessage(TypedDict):
    """Represents a message for a resource processing."""

    resource_uuid: str


class OrderMessage(TypedDict):
    """Represents a message for an order processing."""

    order_uuid: str
