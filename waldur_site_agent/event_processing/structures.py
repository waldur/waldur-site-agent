"""This module defines data structures used in event processing for the Waldur Site Agent."""

from __future__ import annotations

from typing import Optional, TypedDict

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


class UserRoleMessage(TypedDict):
    """Represents a message about user role changes in a project.

    Attributes:
        user_uuid (str, optional): The UUID of the user whose role is being modified.
        user_username (str, optional): The username of the user whose role is being modified.
        project_uuid (str): The UUID of the project where the role change occurred.
        project_name (str): The name of the project where the role change occurred.
        role_name (str): The name of the role that was granted or revoked.
        granted (bool, optional): True if the role was granted, False if it was revoked.
        resource_uuid (str, optional): When set (resource-scoped resync trigger),
            limits the sync to this resource instead of the whole project.
    """

    user_uuid: str | None
    user_username: str | None
    project_uuid: str
    project_name: str
    role_name: str
    granted: bool | None
    resource_uuid: str | None


class ResourceMessage(TypedDict):
    """Represents a message for a resource processing."""

    resource_uuid: str


class OrderMessage(TypedDict):
    """Represents a message for an order processing."""

    order_uuid: str
    order_state: str


class BackendResourceRequestMessage(TypedDict):
    """Represents a message for a backend resource request."""

    backend_resource_request_uuid: str


class OfferingResourcesSyncMessage(TypedDict):
    """Represents a request for forced synchronization of all offering resources."""

    offering_uuid: str
    requested_by_user_uuid: str


class AccountMessage(TypedDict):
    """Represents a message for service account processing."""

    account_uuid: str
    account_username: str
    scope_type: str
    project_uuid: str
    project_name: str
    action: str


class PeriodicLimitsMessage(TypedDict):
    """Represents a message for SLURM periodic limits updates.

    Attributes:
        resource_uuid (str): UUID of the resource in Waldur
        backend_id (str): Backend resource ID (SLURM account name)
        offering_uuid (str): UUID of the offering
        policy_uuid (str): UUID of the SLURM periodic usage policy
        action (str): Action to perform ('apply_periodic_settings')
        settings (dict): SLURM settings to apply (fairshare, limits, thresholds)
        timestamp (str): Current period timestamp
    """

    resource_uuid: str
    backend_id: str
    offering_uuid: str
    policy_uuid: str
    action: str
    settings: dict
    timestamp: str


class ApiKeyCommandMessage(TypedDict, total=False):
    """A command about one of a resource's API keys.

    Carried on the ``resource_api_key_rotation`` observable type, which is named for
    the first command and kept so subscriptions do not change.

    Attributes:
        action (str): ``create``, ``rotate``, ``pause``, ``resume``, ``update`` or
            ``delete`` (mastermind's ``ResourceApiKeyActions``)
        resource_uuid (str): UUID of the resource in Waldur
        resource_backend_id (str): backend id the key client-ids derive from
        api_key_uuid (str): the ResourceApiKey to act on
        client_id (str): the key's backend client-id; blank on a key being created
        limits (Optional[dict]): the key's limits per component, on create, resume
            and update
        allowed_models (Optional[list]): the models the key may call, on create,
            resume and update; None allows every model

    Every field is optional because this is parsed straight from an untrusted frame
    body; the handler rejects a command missing what its action needs.
    """

    action: str
    resource_uuid: str
    resource_backend_id: str
    api_key_uuid: str
    client_id: str
    limits: Optional[dict]
    allowed_models: Optional[list]


class ProjectGroupMessage(TypedDict, total=False):
    """Represents a message for provider project group events.

    Attributes:
        action (str): "create" (the group got its first GID), "update" (its name
            or GID changed), "delete", or "switch" (project groups switched on or
            off for the provider)
        service_provider_uuid (str): UUID of the service provider
        customer_uuid (str): UUID of the provider's organization
        project_group_uuid (str): UUID of the group (not on "switch")
        project_uuid (Optional[str]): UUID of the group's project, if it still exists
        name (str): Group name
        gid (Optional[int]): Group GID
        changed_fields (list[str]): Fields that changed ("update" only)
        project_groups_enabled (bool): The new setting ("switch" only)
    """

    action: str
    service_provider_uuid: str
    customer_uuid: str
    project_group_uuid: str
    project_uuid: Optional[str]
    name: str
    gid: Optional[int]
    changed_fields: list[str]
    project_groups_enabled: bool


class OfferingUserMessage(TypedDict):
    """Represents a message for offering user events.

    Attributes:
        offering_user_uuid (str): UUID of the offering user
        user_uuid (str): UUID of the user
        username (str): Username of the user
        action (str): Action type ("create", "update", "delete", "attribute_update", "username_set")
        offering_uuid (str): UUID of the offering
        attributes (dict): Filtered user profile attributes
        changed_attributes (list[str]): Fields that changed (attribute_update only)
        resource_backend_ids (list[str]): Backend IDs of resources to associate (username_set only)
    """

    offering_user_uuid: str
    user_uuid: str
    username: str
    action: str
    offering_uuid: str
    attributes: dict
    changed_attributes: list[str]
    resource_backend_ids: list[str]
