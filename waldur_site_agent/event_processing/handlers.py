"""Handlers for different events and protocols."""

import json
from typing import Optional
from uuid import UUID

import stomp
import stomp.utils
from stomp.constants import HDR_DESTINATION
from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_list,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
)
from waldur_api_client.api.marketplace_slurm_periodic_usage_policies import (
    marketplace_slurm_periodic_usage_policies_report_command_result,
)
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import (
    ObservableObjectTypeEnum,
    OrderState,
    ResourceFieldEnum,
    ResourceState,
)
from waldur_api_client.models.agent_service import AgentService
from waldur_api_client.models.slurm_command_result_request import (
    SlurmCommandResultRequest,
)
from waldur_api_client.types import UNSET

from waldur_site_agent.backend import logger
from waldur_site_agent.common import agent_identity_management, structures
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.event_processing.structures import (
    AccountMessage,
    ApiKeyRotationMessage,
    BackendResourceRequestMessage,
    OfferingResourcesSyncMessage,
    OfferingUserMessage,
    OrderMessage,
    PeriodicLimitsMessage,
    ResourceMessage,
    UserRoleMessage,
)


def register_event_process_service(
    offering: structures.Offering,
    waldur_rest_client: AuthenticatedClient,
    observable_object: ObservableObjectTypeEnum,
) -> AgentService:
    """A shortcut for initialization of the event_process service.

    Args:
        offering (structures.Offering): Waldur offering
        waldur_rest_client (AuthenticatedClient): Waldur API client
        observable_object (ObservableObjectTypeEnum): Type of observable object

    Returns:
        AgentService: Registered agent service
    """
    agent_identity_manager = agent_identity_management.AgentIdentityManager(
        offering, waldur_rest_client
    )
    agent_identity_name = f"agent-{offering.uuid}"
    agent_identity = agent_identity_manager.get_identity(agent_identity_name)
    service_name = f"{structures.AgentMode.EVENT_PROCESS.value}-{observable_object}"
    return agent_identity_manager.register_service(
        agent_identity,
        service_name,
        structures.AgentMode.EVENT_PROCESS.value,
    )


def process_account_message(
    message: AccountMessage,
    offering: structures.Offering,
    account_type: structures.AccountType,
    observable_object: ObservableObjectTypeEnum,
    user_agent: str = "",
    expose_backend_error_details: bool = True,
) -> None:
    """Process generic account message."""
    account_username = message["account_username"]
    service_account_uuid = message["account_uuid"]
    project_uuid = message["project_uuid"]
    action = message.get("action", "create")
    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)

        agent_service = register_event_process_service(
            offering, waldur_rest_client, observable_object
        )

        processor = common_processors.OfferingMembershipProcessor(
            offering,
            waldur_rest_client,
            expose_backend_error_details=expose_backend_error_details,
        )
        processor.register(agent_service)
        if action == "create":
            processor.process_account_creation(account_username, account_type)
        elif action == "delete":
            processor.process_account_removal(account_username, project_uuid)
        else:
            logger.error("Unknown action %s for course account %s", action, account_username)
    except Exception as e:
        logger.error(
            "Failed to process %s of course account %s (%s): %s",
            action,
            account_username,
            service_account_uuid,
            e,
        )


def on_order_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Order-processing handler for STOMP message event."""
    message: OrderMessage = json.loads(frame.body)
    logger.info("Processing order message: %s (offering: %s)", message, offering.name)
    order_uuid = message["order_uuid"]
    order_state = message.get("order_state", "")

    # Skip done and erred orders to avoid duplicate processing
    if order_state in [OrderState.DONE, OrderState.ERRED]:
        logger.info("Skipping order %s with finished state %s", order_uuid, order_state)
        return

    if order_state == OrderState.PENDING_CONSUMER:
        logger.info("Skipping order %s with state %s", order_uuid, order_state)
        return

    # Mastermind emits an event for every order state transition; these states
    # are never actionable for an agent (the processor would fetch the order
    # and skip it with a warning anyway), so drop them before any REST calls.
    if order_state in [
        OrderState.CANCELED,
        OrderState.REJECTED,
        OrderState.PENDING_PROJECT,
        OrderState.PENDING_START_DATE,
    ]:
        logger.info(
            "Skipping order %s with non-actionable state %s", order_uuid, order_state
        )
        return

    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)
        agent_service = register_event_process_service(
            offering, waldur_rest_client, ObservableObjectTypeEnum.ORDER
        )

        # Create backend instance for dependency injection
        resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
            offering, "order_processing_backend"
        )

        processor = common_processors.OfferingOrderProcessor(
            offering,
            waldur_rest_client,
            resource_backend=resource_backend,
            resource_backend_version=resource_backend_version,
            expose_backend_error_details=expose_backend_error_details,
        )
        processor.register(agent_service)

        order = processor.get_order_info(order_uuid)
        if order is None:
            logger.error("Failed to get order info for %s", order_uuid)
            return
        logger.info(
            "Fetched order %s: type=%s, state=%s, resource=%s",
            order_uuid,
            order.type_,
            order.state,
            order.resource_name,
        )
        processor.process_order_with_retries(order)
        logger.info("Finished processing order %s", order_uuid)
    except Exception as e:
        logger.exception("Failed to process order %s: %s", order_uuid, e)


def on_user_role_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Membership sync handler for STOMP message event."""
    message: UserRoleMessage = json.loads(frame.body)
    logger.info("Received message: %s on topic %s", message, frame.headers.get("destination"))
    user_uuid = message.get("user_uuid")
    project_name = message["project_name"]
    project_uuid = message["project_uuid"]

    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)
        agent_service = register_event_process_service(
            offering, waldur_rest_client, ObservableObjectTypeEnum.USER_ROLE
        )

        # Create backend instance for dependency injection
        resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
            offering, "membership_sync_backend"
        )

        processor = common_processors.OfferingMembershipProcessor(
            offering,
            waldur_rest_client,
            resource_backend=resource_backend,
            resource_backend_version=resource_backend_version,
            expose_backend_error_details=expose_backend_error_details,
        )
        processor.register(agent_service)
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
            role_name = message.get("role_name", "")
            processor.process_user_role_changed(
                user_uuid, project_uuid, role_granted, role_name=role_name
            )
        else:
            resource_uuid = message.get("resource_uuid")
            if resource_uuid:
                # Resource-scoped resync trigger: same USER_ROLE channel,
                # narrowed to one resource by the added payload field.
                logger.info(
                    "Processing user sync event for resource %s in project %s",
                    resource_uuid,
                    project_name,
                )
                processor.process_resource_user_sync(resource_uuid)
            else:
                logger.info(
                    "Processing full project all users sync event for project %s", project_name
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


def on_resource_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Resource update handler for STOMP message event."""
    message: ResourceMessage = json.loads(frame.body)
    resource_uuid = message["resource_uuid"]

    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)

        agent_service = register_event_process_service(
            offering, waldur_rest_client, ObservableObjectTypeEnum.RESOURCE
        )
        processor = common_processors.OfferingMembershipProcessor(
            offering,
            waldur_rest_client,
            expose_backend_error_details=expose_backend_error_details,
        )
        processor.register(agent_service)

        processor.process_resource_by_uuid(resource_uuid)
    except Exception as e:
        logger.error("Failed to process resource %s: %s", resource_uuid, e)


def on_offering_resources_sync_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Forced reconciliation of all offering resources.

    Triggered by a service provider via the Waldur API (offering action
    ``sync_resources``), e.g. to restore backend accounts after the backend
    has lost state (wiped SLURM database). Recreates missing backend
    resources, runs a full membership sync (users, limits, QoS, statuses)
    and re-processes unfinished orders without the stuck-order age threshold.
    """
    message: OfferingResourcesSyncMessage = json.loads(frame.body)
    logger.info(
        "Processing offering resources sync request for offering %s, requested by user %s",
        offering.name,
        message.get("requested_by_user_uuid"),
    )
    try:
        waldur_rest_client = common_utils.get_client(
            offering.api_url, offering.api_token, user_agent, offering.verify_ssl
        )
    except Exception as e:
        logger.exception(
            "Failed to create Waldur client for offering resources sync of %s: %s",
            offering.name,
            e,
        )
        return

    # Membership sync (account recreation + user/limit/QoS reconciliation) and
    # order re-processing are independent goals of a forced sync, so each runs in
    # its own try block: a failure in one must not skip the other.
    # The offering context resolved by the first processor is reused by the
    # second one to avoid duplicate API calls within one forced sync.
    waldur_offering = None
    service_provider = None
    current_user = None
    if offering.membership_sync_backend:
        try:
            agent_service = register_event_process_service(
                offering, waldur_rest_client, ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC
            )
            resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
                offering, "membership_sync_backend"
            )
            membership_processor = common_processors.OfferingMembershipProcessor(
                offering,
                waldur_rest_client,
                resource_backend=resource_backend,
                resource_backend_version=resource_backend_version,
                expose_backend_error_details=expose_backend_error_details,
            )
            waldur_offering = membership_processor.waldur_offering
            service_provider = membership_processor.service_provider
            current_user = membership_processor.current_user
            membership_processor.register(agent_service)
            membership_processor.process_offering(recreate_missing_resources=True)
        except Exception as e:
            logger.exception(
                "Failed to run membership sync for offering resources sync of %s: %s",
                offering.name,
                e,
            )
    else:
        logger.info(
            "Membership sync is disabled for offering %s, "
            "skipping resource recreation and membership sync",
            offering.name,
        )

    if offering.order_processing_backend:
        try:
            # Delegates to OfferingOrderProcessor.process_offering, which fetches
            # PENDING_PROVIDER/EXECUTING orders, runs the backend preflight check,
            # and processes each with retries.
            order_processor = common_processors.OfferingOrderProcessor(
                offering,
                waldur_rest_client,
                waldur_offering=waldur_offering,
                service_provider=service_provider,
                current_user=current_user,
                expose_backend_error_details=expose_backend_error_details,
            )
            order_processor.process_offering()
        except Exception as e:
            logger.exception(
                "Failed to re-process orders for offering resources sync of %s: %s",
                offering.name,
                e,
            )

    logger.info("Finished processing offering resources sync request for %s", offering.name)


def on_importable_resources_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Handler for importable resource list request for STOMP message event."""
    message: BackendResourceRequestMessage = json.loads(frame.body)
    request_uuid = message["backend_resource_request_uuid"]
    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)

        agent_service = register_event_process_service(
            offering, waldur_rest_client, ObservableObjectTypeEnum.IMPORTABLE_RESOURCES
        )

        # Create backend instance for dependency injection
        resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
            offering, "order_processing_backend"
        )

        processor = common_processors.OfferingImportableResourcesProcessor(
            offering,
            waldur_rest_client,
            resource_backend=resource_backend,
            resource_backend_version=resource_backend_version,
            expose_backend_error_details=expose_backend_error_details,
        )
        processor.register(agent_service)

        processor.process_request(request_uuid)
    except Exception as e:
        logger.error("Failed to process importable resource list request %s: %s", request_uuid, e)


def on_account_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Service/course account handler for STOMP.

    Under the unified queue the destination is ``consumer_{uuid}`` and no longer
    encodes the account type, so the type is read from the message payload
    ``object_type`` (stamped by Mastermind) rather than the queue name. Falls
    back to the legacy queue-name suffix for any message without object_type.
    """
    message: AccountMessage = json.loads(frame.body)
    account_type_raw = message.get("object_type")
    if account_type_raw is None:
        # Legacy per-object-type queue: derive from the queue name suffix.
        # TODO: drop once no Mastermind still serves per-type subscription queues.
        queue: str = frame.headers[HDR_DESTINATION]
        queue_parts = queue.split("_")
        account_type_raw = f"{queue_parts[-2]}_{queue_parts[-1]}"
    account_type = structures.AccountType.SERVICE_ACCOUNT
    observable_object = ObservableObjectTypeEnum.SERVICE_ACCOUNT
    if account_type_raw == structures.AccountType.COURSE_ACCOUNT.value:
        account_type = structures.AccountType.COURSE_ACCOUNT
        observable_object = ObservableObjectTypeEnum.COURSE_ACCOUNT
    process_account_message(
        message,
        offering,
        account_type,
        observable_object,
        user_agent,
        expose_backend_error_details=expose_backend_error_details,
    )


def _report_command_result_to_waldur(
    offering: structures.Offering,
    message: PeriodicLimitsMessage,
    result: dict,
) -> None:
    """Report command execution result back to Waldur's report-command-result endpoint."""
    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, "site-agent")

        resource_uuid_str = message.get("resource_uuid", "")
        policy_uuid_str = message.get("policy_uuid", "")

        if not policy_uuid_str:
            logger.warning("No policy_uuid in periodic limits message, cannot report result")
            return

        body = SlurmCommandResultRequest(
            resource_uuid=UUID(resource_uuid_str),
            success=result.get("success", False),
            error_message=result.get("error", ""),
        )
        body["commands_executed"] = result.get("commands_executed", [])

        marketplace_slurm_periodic_usage_policies_report_command_result.sync_detailed(
            uuid=UUID(policy_uuid_str),
            client=waldur_rest_client,
            body=body,
        )

        logger.info(
            "Reported command result for resource %s to Waldur",
            message.get("backend_id", "unknown"),
        )
    except UnexpectedStatus as e:
        logger.warning(
            "Failed to report command result to Waldur: %s",
            e,
        )
    except Exception as e:
        logger.error("Error reporting command result to Waldur: %s", e)


def on_resource_periodic_limits_update_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,  # noqa: ARG001
    expose_backend_error_details: bool = True,  # noqa: ARG001
) -> None:
    """Periodic limits update handler for STOMP message event."""
    try:
        message: PeriodicLimitsMessage = json.loads(frame.body)
        logger.info(
            "Processing periodic limits update message for resource %s",
            message.get("backend_id", "unknown"),
        )
        logger.debug("Periodic limits message: %s", message)

        # Extract message data
        backend_id = message.get("backend_id")
        action = message.get("action")
        settings = message.get("settings", {})

        if not backend_id or action != "apply_periodic_settings":
            logger.error("Invalid periodic limits message: missing backend_id or invalid action")
            _report_command_result_to_waldur(
                offering,
                message,
                {
                    "success": False,
                    "error": "Invalid periodic limits message: "
                    "missing backend_id or invalid action",
                },
            )
            return

        # Every path below reports an outcome to Waldur: an unsupported backend
        # yields an explicit failure result from the base implementation, and a
        # raising backend is converted into one here. Waldur must never be left
        # without a verdict for a command it dispatched.
        try:
            backend, _ = common_utils.get_backend_for_offering(offering, "order_processing_backend")

            if not backend.supports_periodic_settings:
                backend_name = type(backend).__name__
                logger.warning("Backend %s does not support periodic limits", backend_name)

            backend_result = backend.apply_periodic_settings(backend_id, settings)
        except Exception as e:
            logger.exception("Error applying periodic settings for resource %s", backend_id)
            backend_result = {"success": False, "error": str(e), "commands_executed": []}

        if isinstance(backend_result, dict):
            result = backend_result
        else:
            # A backend returning None or a non-dict would otherwise raise on
            # result.get() below, escape to the outer handler, and leave the
            # policy without a verdict.
            error = (
                f"apply_periodic_settings returned {type(backend_result).__name__}, expected dict"
            )
            logger.error("Invalid periodic settings result for %s: %s", backend_id, error)
            result = {"success": False, "error": error, "commands_executed": []}

        if result.get("success"):
            logger.info("Successfully applied periodic settings for resource %s", backend_id)
        else:
            logger.error(
                "Failed to apply periodic settings for resource %s: %s",
                backend_id,
                result.get("error", "unknown error"),
            )

        # Report command execution result back to Waldur
        _report_command_result_to_waldur(offering, message, result)

    except json.JSONDecodeError as e:
        logger.error("Failed to parse periodic limits STOMP message: %s", e)
    except Exception as e:
        logger.error("Error processing periodic limits update: %s", e)


def on_resource_api_key_rotation_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Handle a resource API key rotation command.

    The agent generates the key and applies it to the backend, then reports the
    outcome to Waldur via the provider endpoints. Rotation is the only command:
    the key count is fixed at provisioning.
    """
    try:
        message: ApiKeyRotationMessage = json.loads(frame.body)
        action = message.get("action")
        resource_uuid = message.get("resource_uuid")
        backend_id = message.get("resource_backend_id")
        api_key_uuid = message.get("api_key_uuid")
        client_id = message.get("client_id")
        logger.info("Processing API key %s for resource %s", action, resource_uuid)

        if not resource_uuid or not backend_id:
            logger.error("Invalid API key message: missing resource_uuid/backend_id")
            return

        backend, _ = common_utils.get_backend_for_offering(offering, "order_processing_backend")
        if not getattr(backend, "supports_resource_api_keys", False):
            logger.warning("Backend %s does not support API keys", type(backend).__name__)
            return

        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)

        if action != "rotate":
            logger.error("Unknown API key action: %s", action)
            return
        if not api_key_uuid or not client_id:
            logger.error("rotate command missing api_key_uuid/client_id")
            return
        common_utils.rotate_resource_api_key(
            waldur_rest_client,
            api_key_uuid,
            client_id,
            backend,
            backend_id,
            resource_uuid,
            expose_backend_error_details=expose_backend_error_details,
        )

    except json.JSONDecodeError as e:
        logger.error("Failed to parse API key STOMP message: %s", e)
    except Exception as e:
        logger.error("Error handling API key event: %s", e)


def on_offering_user_message_stomp(
    frame: stomp.utils.Frame,
    offering: structures.Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,  # noqa: ARG001
) -> None:
    """Offering user event handler for STOMP."""
    message: OfferingUserMessage = json.loads(frame.body)
    logger.info("Received offering user message: %s", message)
    _process_offering_user_message(message, offering, user_agent)


def _process_offering_user_message(
    message: OfferingUserMessage,
    offering: structures.Offering,
    user_agent: str,
) -> None:
    """Process an OFFERING_USER event message."""
    action = message.get("action", "")
    offering_user_uuid = message.get("offering_user_uuid", "")
    username = message.get("username", "")

    try:
        waldur_rest_client = common_utils.get_client_for_offering(offering, user_agent)
        register_event_process_service(
            offering, waldur_rest_client, ObservableObjectTypeEnum.OFFERING_USER
        )

        if action == "attribute_update":
            attributes = message.get("attributes", {})
            changed = message.get("changed_attributes", [])
            logger.info(
                "User %s attribute update: changed=%s",
                username,
                changed,
            )
            _forward_user_attributes_to_backend(offering, username, attributes, user_agent)
        elif action == "create":
            attributes = message.get("attributes", {})
            logger.info("Offering user %s created with attributes: %s", username, list(attributes))
            _forward_user_attributes_to_backend(offering, username, attributes, user_agent)
        elif action in ("update", "delete"):
            logger.info("Offering user %s action: %s (no attribute forwarding)", username, action)
        elif action == "username_set":
            resource_backend_ids = message.get("resource_backend_ids", [])
            logger.info(
                "Username set for user %s, creating associations for %d resources",
                username,
                len(resource_backend_ids),
            )
            user_cuid = _resolve_user_cuid(
                waldur_rest_client,
                message.get("user_uuid", ""),
                offering.uuid,
            )
            _add_user_to_resources(
                offering,
                username,
                resource_backend_ids,
                waldur_rest_client,
                user_cuid=user_cuid,
            )
        else:
            logger.warning("Unknown offering user action: %s", action)
    except Exception:
        logger.exception(
            "Failed to process offering user event %s for %s (%s)",
            action,
            username,
            offering_user_uuid,
        )


def _forward_user_attributes_to_backend(
    offering: structures.Offering,
    username: str,
    attributes: dict,
    user_agent: str,  # noqa: ARG001
) -> None:
    """Forward user attributes to the membership sync backend if available."""
    if not offering.membership_sync_backend:
        logger.debug(
            "No membership_sync_backend for offering %s, skipping attribute forward",
            offering.name,
        )
        return

    if not attributes:
        logger.debug("No attributes to forward for user %s", username)
        return

    try:
        backend, _ = common_utils.get_backend_for_offering(offering, "membership_sync_backend")
        if hasattr(backend, "update_user_attributes"):
            backend.update_user_attributes(username, attributes)
            logger.info("Forwarded %d attributes for user %s to backend", len(attributes), username)
        else:
            logger.debug(
                "Backend %s does not support update_user_attributes",
                type(backend).__name__,
            )
    except Exception:
        logger.exception("Failed to forward attributes for user %s", username)


def _resolve_user_cuid(
    waldur_rest_client: AuthenticatedClient,
    user_uuid: str,
    offering_uuid: str,
) -> Optional[str]:
    """Look up the CUID (user_username) for a user from their offering user record.

    The STOMP ``username_set`` message carries the offering_user_username
    (the target-side local name) but backends like the identity bridge need
    the real CUID (``user_username``).  This helper fetches the offering
    user from the source Waldur and extracts it.
    """
    if not user_uuid:
        return None
    try:
        offering_users = marketplace_offering_users_list.sync_all(
            client=waldur_rest_client,
            user_uuid=user_uuid,
            offering_uuid=[offering_uuid],
            is_restricted=False,
        )
        if offering_users:
            val = offering_users[0].user_username
            if val and not isinstance(val, type(UNSET)):
                return val
    except Exception:
        logger.exception("Failed to resolve CUID for user %s", user_uuid)
    return None


def _add_user_to_resources(
    offering: structures.Offering,
    username: str,
    resource_backend_ids: list,
    waldur_rest_client: AuthenticatedClient,
    user_cuid: Optional[str] = None,
) -> None:
    """Add a user to backend resources identified by their backend IDs.

    Fetches the resources matching the given backend IDs for this offering and
    calls add_user on the membership sync backend for each non-restricted resource.
    Mirrors the access-restriction logic of process_user_role_changed.
    """
    if not offering.membership_sync_backend:
        logger.debug(
            "No membership_sync_backend for offering %s, skipping association creation",
            offering.name,
        )
        return

    if not resource_backend_ids:
        logger.debug("No resource_backend_ids provided for user %s, skipping", username)
        return

    try:
        backend, _ = common_utils.get_backend_for_offering(offering, "membership_sync_backend")

        resources = marketplace_provider_resources_list.sync_all(
            client=waldur_rest_client,
            offering_uuid=[offering.uuid],
            state=[ResourceState.OK, ResourceState.ERRED],
            field=[
                ResourceFieldEnum.UUID,
                ResourceFieldEnum.BACKEND_ID,
                ResourceFieldEnum.RESTRICT_MEMBER_ACCESS,
            ],
        )

        backend_id_set = set(resource_backend_ids)
        for resource in resources:
            if resource.backend_id not in backend_id_set:
                continue
            if resource.restrict_member_access:
                logger.info(
                    "Resource %s is restricted, skipping association for user %s",
                    resource.backend_id,
                    username,
                )
                continue
            try:
                add_kwargs = {"user_cuid": user_cuid} if user_cuid else {}
                backend.add_user(resource, username, **add_kwargs)
            except Exception:
                logger.exception(
                    "Failed to add user %s to resource %s",
                    username,
                    resource.backend_id,
                )
    except Exception:
        logger.exception("Failed to create associations for user %s", username)
