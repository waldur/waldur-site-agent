"""Functions for management of event processing signals."""

from __future__ import annotations

import signal
import sys
import types
from collections.abc import Generator
from contextlib import contextmanager

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_orders import marketplace_orders_list
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.backend import logger
from waldur_site_agent.common import agent_identity_management
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.utils import get_backend_for_offering, get_client
from waldur_site_agent.event_processing.event_subscription_manager import EventSubscriptionManager
from waldur_site_agent.event_processing.structures import (
    StompConsumer,
    StompConsumersMap,
)


def _determine_observable_object_types(
    offering: common_structures.Offering,
) -> list[ObservableObjectTypeEnum]:
    """Determine which observable object types to subscribe to based on offering configuration.

    Args:
        offering: The Waldur offering configuration

    Returns:
        List of object types to create subscriptions for
    """
    object_types: list[ObservableObjectTypeEnum] = []

    if offering.order_processing_backend:
        object_types.append(ObservableObjectTypeEnum.ORDER)
    else:
        logger.info(
            "Order processing is disabled for offering %s, skipping start of STOMP connections",
            offering.name,
        )

    if offering.membership_sync_backend:
        object_types.extend(
            [
                ObservableObjectTypeEnum.USER_ROLE,
                ObservableObjectTypeEnum.RESOURCE,
                ObservableObjectTypeEnum.SERVICE_ACCOUNT,
                ObservableObjectTypeEnum.COURSE_ACCOUNT,
                ObservableObjectTypeEnum.OFFERING_USER,
            ]
        )
    else:
        logger.info(
            "Membership sync is disabled for offering %s, skipping start of STOMP connections",
            offering.name,
        )

    if offering.resource_import_enabled:
        object_types.append(ObservableObjectTypeEnum.IMPORTABLE_RESOURCES)
    else:
        logger.info(
            "Resource import is disabled for offering %s, skipping start of STOMP connections",
            offering.name,
        )

    # Check if periodic limits are enabled for this offering
    backend_settings = getattr(offering, "backend_settings", {})
    periodic_limits_config = backend_settings.get("periodic_limits", {})
    if periodic_limits_config.get("enabled", False):
        object_types.append(ObservableObjectTypeEnum.RESOURCE_PERIODIC_LIMITS)
        logger.info(
            "Periodic limits enabled for offering %s, subscribing to periodic limits updates",
            offering.name,
        )
    else:
        logger.debug(
            "Periodic limits disabled for offering %s, skipping periodic limits subscriptions",
            offering.name,
        )

    return object_types


def _register_agent_identity(
    offering: common_structures.Offering,
    waldur_rest_client: AuthenticatedClient,
) -> (
    tuple[agent_identity_management.AgentIdentity, agent_identity_management.AgentIdentityManager]
    | None
):
    """Register or retrieve agent identity for the offering.

    Args:
        offering: The Waldur offering configuration
        waldur_rest_client: Authenticated REST client for Waldur API

    Returns:
        (AgentIdentity, AgentIdentityManager) if successful, None if registration failed
    """
    agent_identity_manager = agent_identity_management.AgentIdentityManager(
        offering, waldur_rest_client
    )
    identity_name = f"agent-{offering.uuid}"
    try:
        identity = agent_identity_manager.register_identity(identity_name)
        return (identity, agent_identity_manager)
    except Exception as e:
        logger.exception("Failed to register identity for the offering %s: %s", offering.name, e)
        return None


def _setup_single_stomp_subscription(
    offering: common_structures.Offering,
    agent_identity: agent_identity_management.AgentIdentity,
    agent_identity_manager: agent_identity_management.AgentIdentityManager,
    waldur_user_agent: str,
    object_type: ObservableObjectTypeEnum,
    global_proxy: str = "",
) -> StompConsumer | None:
    """Setup a single STOMP subscription for the given object type.

    Args:
        offering: The Waldur offering configuration
        agent_identity: The registered agent identity
        agent_identity_manager: Manager for agent identity operations
        waldur_user_agent: User agent string
        object_type: Type of observable object to subscribe to
        global_proxy: Optional proxy configuration

    Returns:
        Tuple of (connection, event_subscription, offering) if successful, None if failed
    """
    try:
        event_subscription = agent_identity_manager.register_event_subscription(
            agent_identity, object_type
        )

        event_subscription_queue = agent_identity_manager.create_event_subscription_queue(
            event_subscription, object_type
        )
        if event_subscription_queue is None:
            logger.error(
                "Failed to create event subscription queue for the offering %s, object type %s",
                offering.name,
                object_type,
            )
            return None

        event_subscription_manager = EventSubscriptionManager(
            offering, None, None, waldur_user_agent, object_type, global_proxy
        )
        connection = event_subscription_manager.setup_stomp_connection(
            event_subscription,
            offering.stomp_ws_host,
            offering.stomp_ws_port,
            offering.stomp_ws_path,
        )
        connected = event_subscription_manager.start_stomp_connection(
            event_subscription, connection
        )
        if not connected:
            logger.error(
                "Failed to start STOMP connection for the offering %s (%s), object type %s",
                offering.name,
                offering.uuid,
                object_type,
            )
            return None

        return (connection, event_subscription, offering)
    except Exception as e:
        logger.exception(
            "Unable to register event subscription for offering %s object type %s: %s",
            offering.name,
            object_type,
            e,
        )
        return None


def setup_stomp_offering_subscriptions(
    waldur_offering: common_structures.Offering, waldur_user_agent: str, global_proxy: str = ""
) -> list[StompConsumer]:
    """Set up STOMP subscriptions for the specified offering."""
    stomp_connections: list[StompConsumer] = []

    # Determine which object types to subscribe to
    object_types = _determine_observable_object_types(waldur_offering)

    waldur_rest_client = get_client(
        waldur_offering.api_url,
        waldur_offering.api_token,
        waldur_user_agent,
        verify_ssl=waldur_offering.verify_ssl,
        proxy=global_proxy,
    )

    # Register agent identity
    result = _register_agent_identity(waldur_offering, waldur_rest_client)
    if result is None:
        return stomp_connections

    agent_identity, agent_identity_manager = result

    # Setup subscription for each object type
    for object_type in object_types:
        consumer = _setup_single_stomp_subscription(
            waldur_offering,
            agent_identity,
            agent_identity_manager,
            waldur_user_agent,
            object_type,
            global_proxy,
        )
        if consumer is not None:
            stomp_connections.append(consumer)

    # Set up target event subscriptions for backends that support them
    # (e.g., Waldur federation backend subscribes to ORDER events on Waldur B).
    # BaseBackend.setup_target_event_subscriptions returns [] by default.
    if waldur_offering.order_processing_backend:
        try:
            backend, _ = get_backend_for_offering(
                waldur_offering, "order_processing_backend"
            )
            target_consumers = backend.setup_target_event_subscriptions(
                waldur_offering, waldur_user_agent, global_proxy
            )
            stomp_connections.extend(target_consumers)
        except Exception:
            logger.exception(
                "Failed to set up target event subscriptions for %s",
                waldur_offering.name,
            )

    return stomp_connections



def start_stomp_consumers(
    waldur_offerings: list[common_structures.Offering],
    waldur_user_agent: str,
    global_proxy: str = "",
) -> StompConsumersMap:
    """Start multiple STOMP consumers."""
    stomp_consumers_map: StompConsumersMap = {}
    for waldur_offering in waldur_offerings:
        if not waldur_offering.stomp_enabled:
            logger.info("STOMP feature is disabled for the offering")
            continue

        logger.info("Starting STOMP consumers for offering %s", waldur_offering.name)
        stomp_connections = setup_stomp_offering_subscriptions(
            waldur_offering, waldur_user_agent, global_proxy
        )
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
                    event_subscription.observable_objects[0]["object_type"]
                    if event_subscription.observable_objects
                    else "N/A",
                )
                event_subscription_manager.stop_stomp_connection(connection)
            except Exception as exc:
                logger.exception("Unable to stop the connection, reason: %s", exc)


@contextmanager
def signal_handling(
    stomp_consumers_map: StompConsumersMap,
) -> Generator[None, None, None]:
    """Context manager for handling signals gracefully."""

    def signal_handler(signum: int, _: types.FrameType | None) -> None:
        signal_name = signal.Signals(signum).name
        logger.info("Received %s signal. Shutting down gracefully...", signal_name)
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
    """Runs processing of offerings with event-based processing enabled."""
    logger.info("Processing offerings with STOMP feature enabled")
    for offering in waldur_offerings:
        try:
            if offering.stomp_enabled:
                process_offering(offering, user_agent)
        except Exception as e:
            logger.exception("Error occurred during initial offering process: %s", e)


def process_offering(offering: common_structures.Offering, user_agent: str = "") -> None:
    """Processes the specified offering."""
    logger.info("Processing offering %s (%s)", offering.name, offering.uuid)

    waldur_rest_client = get_client(
        offering.api_url, offering.api_token, user_agent, verify_ssl=offering.verify_ssl
    )
    agent_identity_manager = agent_identity_management.AgentIdentityManager(
        offering, waldur_rest_client
    )
    agent_identity = agent_identity_manager.register_identity(f"agent-{offering.uuid}")
    agent_service = agent_identity_manager.register_service(
        agent_identity,
        "initial-offering-process",
        common_structures.AgentMode.EVENT_PROCESS.value,
    )

    if offering.order_processing_backend:
        order_processor = common_processors.OfferingOrderProcessor(offering, waldur_rest_client)
        order_processor.register(agent_service)
        logger.info("Running offering order process")
        order_processor.process_offering()
    else:
        logger.info("Order processing is disabled for this offering, skipping it")

    if offering.membership_sync_backend:
        membership_processor = common_processors.OfferingMembershipProcessor(
            offering, waldur_rest_client
        )
        membership_processor.register(agent_service)
        logger.info("Running offering membership process")
        membership_processor.process_offering()
    else:
        logger.info("Membership sync is disabled for this offering, skipping it")


def run_periodic_username_reconciliation(
    waldur_offerings: list[common_structures.Offering], user_agent: str = ""
) -> None:
    """Reconcile offering user usernames for STOMP-enabled offerings.

    Catches any username updates from Waldur B that may have been missed
    due to transient STOMP disconnections or message loss.
    Only runs sync_offering_user_usernames — not a full membership sync.
    """
    for offering in waldur_offerings:
        if not offering.username_reconciliation_enabled:
            continue
        try:
            waldur_rest_client = get_client(
                offering.api_url,
                offering.api_token,
                user_agent,
                verify_ssl=offering.verify_ssl,
            )
            resource_backend, _ = get_backend_for_offering(
                offering, "membership_sync_backend"
            )
            updated = resource_backend.sync_offering_user_usernames(
                offering.uuid, waldur_rest_client
            )
            if updated:
                logger.info(
                    "Reconciliation: usernames updated for offering %s",
                    offering.name,
                )
        except Exception:
            logger.exception(
                "Reconciliation failed for offering %s", offering.name
            )


def send_agent_health_checks(offerings: list[common_structures.Offering], user_agent: str) -> None:
    """Sends agent health checks for the specified offerings."""
    for offering in offerings:
        try:
            waldur_rest_client = get_client(
                offering.api_url, offering.api_token, user_agent, verify_ssl=offering.verify_ssl
            )
            processor = common_processors.OfferingOrderProcessor(offering, waldur_rest_client)
            marketplace_orders_list.sync(
                client=processor.waldur_rest_client, offering_uuid=offering.uuid
            )
        except Exception as e:
            logger.error(
                "Failed to send agent health check for the offering %s: %s", offering.name, e
            )
