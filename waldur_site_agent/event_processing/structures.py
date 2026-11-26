"""This module defines data structures used in event processing for the Waldur Site Agent."""

from __future__ import annotations

from typing import TypedDict

import stomp

from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.structures import UnifiedQueue

__all__ = ["UnifiedQueue"]  # re-exported for event-processing callers


class ObservableObject(TypedDict):
    """Represents an object that can be observed.

    Attributes:
        object_type (str): The model name of the observable object.
        object_uuid (str): The UUID of the object. If None, indicates that the subscriber
                          observes all existing objects of object_type.
    """

    object_type: str
    object_uuid: str


# A tuple of offering name and UUID used as a key for connection mapping.
StompConsumerKey = tuple[str, str]

# A tuple containing STOMP connection, unified-queue descriptor, and offering.
# Unified path: exactly one StompConsumer per offering (one queue, all types).
StompConsumer = tuple[stomp.WSStompConnection, UnifiedQueue, common_structures.Offering]

StompConsumersMap = dict[StompConsumerKey, list[StompConsumer]]


# The STOMP message types come with waldur-api-client, generated from
# mastermind's AsyncAPI document: see waldur_api_client.stomp_messages.
