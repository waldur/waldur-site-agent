"""Shared utility functions for Waldur Site Agent modules.

This module provides common functionality used across different agent components:
- Configuration loading and parsing from CLI arguments and YAML files
- Waldur API client creation and authentication
- Backend discovery and initialization via entry points
- Component loading and synchronization with Waldur offerings
- Diagnostic and utility functions for system health checks
- Error handling utilities for resource management

The module handles the initialization of the agent's core configuration
and provides the plugin discovery mechanism that allows backends to be
automatically detected and loaded via Python entry points.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional
from uuid import UUID

import yaml
from httpx import TimeoutException
from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_begin_creating,
    marketplace_offering_users_list,
    marketplace_offering_users_partial_update,
    marketplace_offering_users_set_ok,
    marketplace_offering_users_set_pending_account_linking,
    marketplace_offering_users_set_pending_additional_validation,
)
from waldur_api_client.api.marketplace_orders import marketplace_orders_list
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_create_offering_component,
    marketplace_provider_offerings_retrieve,
    marketplace_provider_offerings_update_offering_component,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_set_as_erred,
    marketplace_provider_resources_set_limits,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.api.users import users_me_retrieve
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import (
    MarketplaceResourcesListStateItem,
    ResourceSetLimitsRequest,
    ResourceSetStateErredRequest,
)
from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.marketplace_orders_list_state_item import (
    MarketplaceOrdersListStateItem,
)
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.offering_component_request import OfferingComponentRequest
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state_enum import OfferingUserStateEnum
from waldur_api_client.models.offering_user_state_transition_request import (
    OfferingUserStateTransitionRequest,
)
from waldur_api_client.models.patched_offering_user_request import PatchedOfferingUserRequest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.user import User
from waldur_api_client.models.username_generation_policy_enum import UsernameGenerationPolicyEnum

from waldur_site_agent.backend import (
    BackendType,
    logger,
)
from waldur_site_agent.backend import exceptions as backend_exceptions
from waldur_site_agent.backend.backends import (
    AbstractUsernameManagementBackend,
    BaseBackend,
    UnknownBackend,
    UnknownUsernameManagementBackend,
)
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common import structures

# Handle different Python versions
if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points, version
else:
    from importlib_metadata import entry_points, version


BACKENDS: dict[str, type[BaseBackend]] = {
    entry_point.name: entry_point.load()
    for entry_point in entry_points(group="waldur_site_agent.backends")
}

USERNAME_BACKENDS: dict[str, type[AbstractUsernameManagementBackend]] = {
    entry_point.name: entry_point.load()
    for entry_point in entry_points(group="waldur_site_agent.username_management_backends")
}


def get_client(
    api_url: str, access_token: str, agent_header: Optional[str] = None, verify_ssl: bool = True
) -> AuthenticatedClient:
    """Create an authenticated Waldur API client.

    Args:
        api_url: Base URL for the Waldur API (e.g., 'https://waldur.example.com/api/')
        access_token: Authentication token for API access
        agent_header: Optional User-Agent string for HTTP requests
        verify_ssl: Whether or not to verify SSL certificates

    Returns:
        Configured AuthenticatedClient instance ready for API calls
    """
    headers = {"User-Agent": agent_header} if agent_header else {}
    url = api_url.rstrip("/api")
    return AuthenticatedClient(
        base_url=url,
        token=access_token,
        timeout=600,
        headers=headers,
        verify_ssl=verify_ssl,
    )


def is_uuid(value: str) -> bool:
    """Validate if a string represents a valid UUID.

    Args:
        value: String to validate as UUID

    Returns:
        True if the string is a valid UUID, False otherwise
    """
    try:
        UUID(value)
        return True
    except ValueError:
        return False


def load_configuration(
    config_file_path: str, user_agent_suffix: str = "generic"
) -> structures.WaldurAgentConfiguration:
    """Load configuration from YAML file.

    Args:
        config_file_path: Path to the YAML configuration file
        user_agent_suffix: Suffix to add to the user agent string (e.g., "sync", "order-process")

    Returns:
        Configuration object with offerings loaded from file

    Raises:
        FileNotFoundError: If the configuration file cannot be found
        yaml.YAMLError: If the configuration file is malformed
    """
    configuration = structures.WaldurAgentConfiguration()

    with Path(config_file_path).open(encoding="UTF-8") as stream:
        config = yaml.safe_load(stream)
        offering_list = config["offerings"]
        waldur_offerings = [
            structures.Offering(
                name=offering_info["name"],
                api_url=offering_info["waldur_api_url"],
                api_token=offering_info["waldur_api_token"],
                uuid=offering_info["waldur_offering_uuid"],
                backend_type=offering_info["backend_type"].lower(),
                backend_settings=offering_info["backend_settings"],
                backend_components=offering_info["backend_components"],
                mqtt_enabled=offering_info.get("mqtt_enabled", False),
                stomp_enabled=offering_info.get("stomp_enabled", False),
                websocket_use_tls=offering_info.get("websocket_use_tls", True),
                username_management_backend=offering_info.get(
                    "username_management_backend", "base"
                ),
                order_processing_backend=offering_info.get("order_processing_backend", ""),
                membership_sync_backend=offering_info.get("membership_sync_backend", ""),
                reporting_backend=offering_info.get("reporting_backend", ""),
                resource_import_enabled=offering_info.get("resource_import_enabled", False),
                verify_ssl=offering_info.get("verify_ssl", True),
            )
            for offering_info in offering_list
        ]
        configuration.waldur_offerings = waldur_offerings

        # Handle Sentry configuration - initialize if DSN is provided
        sentry_dsn = config.get("sentry_dsn")
        if sentry_dsn:
            configuration.sentry_dsn = sentry_dsn
            import sentry_sdk  # noqa: PLC0415

            sentry_sdk.init(dsn=sentry_dsn)

        timezone = config.get("timezone", "UTC")
        configuration.timezone = timezone

    # Set version and user agent for all configurations
    waldur_site_agent_version = version("waldur-site-agent")
    configuration.waldur_site_agent_version = waldur_site_agent_version
    configuration.waldur_user_agent = (
        f"waldur-site-agent-{user_agent_suffix}/{waldur_site_agent_version}"
    )

    return configuration


def init_configuration() -> structures.WaldurAgentConfiguration:
    """Initialize agent configuration from CLI arguments and config file.

    Parses command-line arguments, loads the YAML configuration file,
    and creates offering configurations. Also initializes Sentry if
    configured and sets up user agent strings for different modes.

    Returns:
        Complete agent configuration with all offerings and settings

    Raises:
        FileNotFoundError: If the configuration file cannot be found
        yaml.YAMLError: If the configuration file is malformed
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        "-m",
        help="Agent mode, choices: order_process, report "
        "membership_sync and event_process; default is order_process",
        choices=["order_process", "report", "membership_sync", "event_process"],
        default="order_process",
    )

    parser.add_argument(
        "--config-file",
        "-c",
        help="Path to the config file with provider settings;"
        "default is waldur-site-agent-config.yaml",
        dest="config_file_path",
        default="waldur-site-agent-config.yaml",
        required=False,
    )

    cli_args = parser.parse_args()
    config_file_path = cli_args.config_file_path
    agent_mode = cli_args.mode

    logger.info("Using %s as a config source", config_file_path)

    # Load base configuration with mode-specific user agent
    configuration = load_configuration(config_file_path, user_agent_suffix=agent_mode)

    # Add CLI-specific configuration
    configuration.waldur_site_agent_mode = agent_mode

    return configuration


def get_backend_for_offering(offering: structures.Offering, backend_type_key: str) -> BaseBackend:
    """Create and initialize a backend instance for the specified offering.

    Uses the plugin discovery system to find and instantiate the appropriate
    backend class based on the offering's configuration.

    Args:
        offering: The offering configuration
        backend_type_key: Key to determine which backend type to use
                         (e.g., 'order_processing_backend', 'reporting_backend')

    Returns:
        Initialized backend instance, or UnknownBackend if type not supported
    """
    backend_type = getattr(offering, backend_type_key, "")
    backend_class = BACKENDS.get(backend_type)
    if not backend_class:
        logger.error("Unsupported backend type for %s: %s", backend_type_key, backend_type)
        return UnknownBackend()

    return backend_class(offering.backend_settings, offering.backend_components)


def mark_waldur_resources_as_erred(
    waldur_rest_client: AuthenticatedClient,
    resources: list[WaldurResource],
    error_details: dict[str, str],
) -> None:
    """Mark multiple resources as ERRED in Waldur with error details.

    This utility function handles batch error reporting for resources
    that have encountered processing failures.

    Args:
        waldur_rest_client: Authenticated Waldur API client
        resources: List of resources to mark as erred
        error_details: Dictionary containing 'error_message' and 'error_traceback'
    """
    logger.info("Marking Waldur resources as ERRED")
    for resource in resources:
        logger.info("Marking %s resource as ERRED", resource)
        try:
            request_body = ResourceSetStateErredRequest(
                error_message=error_details.get("error_message", ""),
                error_traceback=error_details.get("error_traceback", ""),
            )
            marketplace_provider_resources_set_as_erred.sync_detailed(
                uuid=resource.uuid.hex, client=waldur_rest_client, body=request_body
            )
        except UnexpectedStatus as e:
            logger.exception(
                "Waldur REST client error while setting resource state to Erred %s: %s",
                resource.backend_id,
                e,
            )


def load_offering_components() -> None:
    """Load and create offering components in Waldur from configuration.

    This function reads the agent configuration and creates or updates
    offering components in Waldur to match the backend component definitions.
    Used during initial setup or when component definitions change.
    """
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        logger.info("Processing %s offering", offering.name)
        waldur_rest_client = get_client(
            offering.api_url,
            offering.api_token,
            configuration.waldur_user_agent,
            offering.verify_ssl,
        )

        load_components_to_waldur(
            waldur_rest_client,
            offering.uuid,
            offering.name,
            offering.backend_components,
        )


def extend_backend_components(
    offering: structures.Offering, waldur_offering_components: list[OfferingComponent]
) -> None:
    """Synchronize local configuration with Waldur offering components.

    Fetches component definitions from Waldur and adds any missing components
    to the local offering configuration. This ensures consistency between
    the agent's configuration and Waldur's offering definition.

    Args:
        offering: Local offering configuration to extend
        waldur_offering_components: Component definitions from Waldur
    """
    logger.info("Loading Waldur components to the local config")
    remote_components: dict[str, OfferingComponent] = {
        item.type_: item for item in waldur_offering_components
    }
    missing_component_types = set(remote_components.keys()) - set(
        offering.backend_components.keys()
    )

    if missing_component_types:
        logger.info("Component types to add: %s", ", ".join(missing_component_types))
    else:
        logger.info("All remote components already exist in backend configuration, nothing to add")
    for missing_component_type in missing_component_types:
        logger.info("Loading %s", missing_component_type)
        remote_component_info = remote_components[missing_component_type]
        component_info = {
            "limit": remote_component_info.limit_amount,
            "measured_unit": remote_component_info.measured_unit,
            "unit_factor": remote_component_info.unit_factor or 1,
            "accounting_type": remote_component_info.billing_type,
            "label": remote_component_info.name,
        }
        offering.backend_components[missing_component_type] = component_info


def load_components_to_waldur(
    waldur_rest_client: AuthenticatedClient,
    offering_uuid: str,
    offering_name: str,
    components: dict,
) -> None:
    """Create or update offering components in Waldur.

    Processes the component definitions from the configuration and creates
    or updates the corresponding components in Waldur. Handles both new
    component creation and limit updates for existing components.

    Args:
        waldur_rest_client: Authenticated Waldur API client
        offering_uuid: UUID of the target offering in Waldur
        offering_name: Name of the offering (for logging)
        components: Dictionary of component definitions from configuration
    """
    logger.info(
        "Creating offering components data for the following resources: %s",
        ", ".join(components.keys()),
    )
    waldur_offering = marketplace_provider_offerings_retrieve.sync(
        client=waldur_rest_client, uuid=offering_uuid
    )
    waldur_offering_components = {
        component.type_: component for component in waldur_offering.components
    }
    for component_type, component_info in components.items():
        try:
            limit_amount = component_info.get("limit")
            accounting_type = component_info["accounting_type"]
            label = component_info["label"]

            component = OfferingComponentRequest(
                billing_type=BillingTypeEnum(accounting_type),
                type_=component_type,
                name=label,
                measured_unit=component_info["measured_unit"],
                limit_amount=limit_amount,
            )
            if component_type in waldur_offering_components:
                if component_info["accounting_type"] == "usage":
                    existing_component = waldur_offering_components[component_type]
                    logger.info(
                        "Offering component %s already exists, updating limit from %s to %s %s.",
                        component_type,
                        existing_component.limit_amount,
                        component_info.get("limit"),
                        component_info["measured_unit"],
                    )
                    marketplace_provider_offerings_update_offering_component.sync_detailed(
                        client=waldur_rest_client, uuid=existing_component.uuid, body=component
                    )
                else:
                    logger.info(
                        "Offering component %s already exists, skipping creation.",
                        component_type,
                    )
            else:
                logger.info(
                    "Creating offering component %s with type %s and limit %s %s.",
                    component_type,
                    component_info["accounting_type"],
                    component_info.get("limit"),
                    component_info["measured_unit"],
                )
                marketplace_provider_offerings_create_offering_component.sync_detailed(
                    client=waldur_rest_client, uuid=offering_uuid, body=component
                )
        except Exception as e:
            logger.info(
                "Unable to create or update a component %s for offering %s (%s):",
                component_info["label"],
                offering_name,
                offering_uuid,
            )
            logger.exception(e)


def get_current_user_from_client(waldur_rest_client: AuthenticatedClient) -> User:
    """Retrieve current authenticated user information from Waldur.

    Args:
        waldur_rest_client: Authenticated Waldur API client

    Returns:
        User object containing current user details and permissions
    """
    return users_me_retrieve.sync(client=waldur_rest_client)


def diagnostics() -> bool:
    """Perform comprehensive system diagnostics for all offerings.

    Checks connectivity to Waldur, validates offering configurations,
    tests backend availability, and reports system status. This function
    is used by the diagnostic command to verify agent setup.

    Returns:
        True if all diagnostics pass, False if any issues are detected
    """
    configuration = init_configuration()
    logger.info("-" * 10 + "DIAGNOSTICS START" + "-" * 10)
    logger.info("Provided settings:")
    format_string = "{:<30} = {:<10}"

    if structures.AgentMode.ORDER_PROCESS.value == configuration.waldur_site_agent_mode:
        logger.info(
            "Agent is running in %s mode - "
            "pulling orders from Waldur and creating resources in backend",
            structures.AgentMode.ORDER_PROCESS.name,
        )
    if structures.AgentMode.REPORT.value == configuration.waldur_site_agent_mode:
        logger.info(
            "Agent is running in %s mode - pushing usage data to Waldur",
            structures.AgentMode.REPORT.name,
        )
    if structures.AgentMode.MEMBERSHIP_SYNC.value == configuration.waldur_site_agent_mode:
        logger.info(
            "Agent is running in %s mode - pushing membership data to Waldur",
            structures.AgentMode.MEMBERSHIP_SYNC.name,
        )

    if structures.AgentMode.EVENT_PROCESS.value == configuration.waldur_site_agent_mode:
        logger.info(
            "Agent is running in %s mode - processing data from Waldur in event-based approach",
            structures.AgentMode.EVENT_PROCESS.name,
        )

    for offering in configuration.waldur_offerings:
        format_string = "{:<30} = {:<10}"
        offering_uuid = offering.uuid
        offering_name = offering.name
        offering_api_url = offering.api_url
        offering_api_token = offering.api_token

        logger.info(format_string.format("Offering name", offering_name))
        logger.info(format_string.format("Offering UUID", offering_uuid))
        logger.info(format_string.format("Waldur API URL", offering_api_url))
        logger.info(format_string.format("SENTRY_DSN", str(configuration.sentry_dsn)))

        waldur_rest_client = get_client(
            offering_api_url,
            offering_api_token,
            configuration.waldur_user_agent,
            offering.verify_ssl,
        )

        try:
            current_user = get_current_user_from_client(waldur_rest_client)
            print_current_user(current_user)
            offering_data = marketplace_provider_offerings_retrieve.sync(
                client=waldur_rest_client, uuid=offering_uuid
            )
            logger.info("Offering uuid: %s", offering_data.uuid)
            logger.info("Offering name: %s", offering_data.name)
            logger.info("Offering org: %s", offering_data.customer_name)
            logger.info("Offering state: %s", offering_data.state)

            logger.info("Offering components:")
            format_string = "{:<10} {:<10} {:<10} {:<10}"
            headers = ["Type", "Name", "Unit", "Limit"]
            logger.info(format_string.format(*headers))
            components = [
                [
                    component.type_,
                    component.name,
                    component.measured_unit,
                    component.limit_amount or "",
                ]
                for component in offering_data.components
            ]
            for component in components:
                logger.info(format_string.format(*component))

            logger.info("")
        except UnexpectedStatus as err:
            logger.error("Unable to fetch offering data, reason: %s", err)

        logger.info("")
        try:
            orders = marketplace_orders_list.sync(
                client=waldur_rest_client,
                offering_uuid=offering_uuid,
                state=[
                    MarketplaceOrdersListStateItem.PENDING_PROVIDER,
                    MarketplaceOrdersListStateItem.EXECUTING,
                ],
            )
            logger.info("Active orders:")
            format_string = "{:<10} {:<10} {:<10}"
            headers = ["Project", "Type", "State"]
            logger.info(format_string.format(*headers))
            for order in orders:
                logger.info(format_string.format(order.project_name, order.type_, order.state))
        except UnexpectedStatus as err:
            logger.error("Unable to fetch orders, reason: %s", err)

        backend = get_backend_for_offering(offering, "order_processing_backend")

        if not backend.diagnostics():
            return False

    logger.info("-" * 10 + "DIAGNOSTICS END" + "-" * 10)
    return True


def create_homedirs_for_offering_users() -> None:
    """Create home directories for all offering users.

    This utility function creates home directories for users associated
    with offerings that have home directory creation enabled. Currently
    supports SLURM backends with configurable umask settings.
    """
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        # Feature is exclusive for SLURM temporarily
        if offering.backend_type != BackendType.SLURM.value or not offering.backend_settings.get(
            "enable_user_homedir_account_creation", True
        ):
            continue

        logger.info("Creating homedirs for %s offering users", offering.name)

        waldur_rest_client = get_client(
            offering.api_url,
            offering.api_token,
            configuration.waldur_user_agent,
            offering.verify_ssl,
        )
        offering_users = marketplace_offering_users_list.sync(
            client=waldur_rest_client, offering_uuid=offering.uuid, is_restricted=False
        )

        offering_user_usernames: set[str] = {
            offering_user.username for offering_user in offering_users
        }
        umask = offering.backend_settings.get("homedir_umask", "0700")
        offering_backend = get_backend_for_offering(offering, "order_processing_backend")
        offering_backend.create_user_homedirs(offering_user_usernames, umask)


def print_current_user(current_user: User) -> None:
    """Log detailed information about a Waldur user.

    Displays user details including username, full name, staff status,
    and all associated permissions with their scopes and expiration times.

    Args:
        current_user: User object to display information for
    """
    logger.info("Current user username: %s", current_user.username)
    logger.info("Current user full name: %s", current_user.full_name)
    logger.info("Current user is staff: %s", current_user.is_staff or False)
    if current_user.is_staff:
        return
    if current_user.permissions:
        logger.info("List of permissions:")
        for permission in current_user.permissions:
            logger.info("Role name: %s", permission.role_name)
            logger.info("Role description: %s", permission.role_description)
            logger.info("Scope type: %s", permission.scope_type)
            logger.info("Scope name: %s", permission.scope_name)
            logger.info("Scope UUID: %s", permission.scope_uuid)
            logger.info("Expiration time: %s", permission.expiration_time)
    else:
        logger.info("User has no role permissions.")


def get_username_management_backend(
    offering: structures.Offering,
) -> AbstractUsernameManagementBackend:
    """Create username management backend instance for the offering.

    Uses the plugin discovery system to instantiate the appropriate
    username management backend based on the offering configuration.

    Args:
        offering: Offering configuration specifying the backend to use

    Returns:
        Username management backend instance, or UnknownUsernameManagementBackend
        if the specified backend is not available
    """
    username_management_setting = offering.username_management_backend

    if username_management_setting is None:
        logger.error(
            "No username_management_backend is set for offering %s, using the default one",
            offering.name,
        )
        return UnknownUsernameManagementBackend()

    return USERNAME_BACKENDS[username_management_setting]()


def update_offering_users(
    offering: structures.Offering,
    waldur_rest_client: AuthenticatedClient,
    offering_users: list[OfferingUser],
) -> None:
    """Generate usernames for offering users and update their state accordingly.

    This method checks if the service provider is allowed to generate usernames
    and attempts to create usernames for users who don't have them assigned.

    Args:
        offering: The offering to process,
        waldur_rest_client: Authenticated Waldur API client
        offering_users: List of offering users to process
    """

    def update_offering_user_username(offering_user: OfferingUser) -> None:
        username = username_management_backend.get_or_create_username(offering_user)
        if username:
            logger.info(
                "Updating username for offering user %s (%s) to %s",
                offering_user.user_email,
                offering_user.uuid,
                username,
            )
            offering_user.username = username
            payload = PatchedOfferingUserRequest(username=username)
            marketplace_offering_users_partial_update.sync(
                uuid=offering_user.uuid, client=waldur_rest_client, body=payload
            )
            logger.info("Setting offering user state to OK")
            marketplace_offering_users_set_ok.sync_detailed(
                uuid=offering_user.uuid, client=waldur_rest_client
            )

    if not offering_users:
        return

    # Check if usernames can be generated by a service provider
    offering_uuid = offering.uuid
    offering_details = marketplace_provider_offerings_retrieve.sync(
        client=waldur_rest_client, uuid=offering_uuid
    )
    if (
        offering_details.plugin_options.username_generation_policy
        != UsernameGenerationPolicyEnum.SERVICE_PROVIDER
    ):
        return

    username_management_backend = get_username_management_backend(offering)

    requested_offering_users = [
        offering_user
        for offering_user in offering_users
        if offering_user.state == OfferingUserStateEnum.REQUESTED
    ]

    logger.info(
        "Generating usernames for %d requested offering users in offering %s",
        len(requested_offering_users),
        offering_uuid,
    )
    for offering_user in requested_offering_users:
        try:
            logger.info(
                "Setting offering user %s (%s) state to CREATING",
                offering_user.user_email,
                offering_user.uuid,
            )
            marketplace_offering_users_begin_creating.sync_detailed(
                uuid=offering_user.uuid, client=waldur_rest_client
            )

            update_offering_user_username(offering_user)
        except backend_exceptions.OfferingUserAccountLinkingRequiredError as e:
            logger.warning(
                "Offering user %s (%s) requires user linking: %s",
                offering_user.user_email,
                offering_user.uuid,
                e,
            )
            # Build payload with separate comment and comment_url fields
            comment_url = e.comment_url if hasattr(e, "comment_url") and e.comment_url else None
            payload = OfferingUserStateTransitionRequest(comment=str(e), comment_url=comment_url)
            marketplace_offering_users_set_pending_account_linking.sync_detailed(
                uuid=offering_user.uuid, client=waldur_rest_client, body=payload
            )
        except backend_exceptions.OfferingUserAdditionalValidationRequiredError as e:
            logger.warning(
                "Offering user %s (%s) requires additional validation: %s",
                offering_user.user_email,
                offering_user.uuid,
                e,
            )
            # Build payload with separate comment and comment_url fields
            comment_url = e.comment_url if hasattr(e, "comment_url") and e.comment_url else None
            payload = OfferingUserStateTransitionRequest(comment=str(e), comment_url=comment_url)
            marketplace_offering_users_set_pending_additional_validation.sync_detailed(
                uuid=offering_user.uuid, client=waldur_rest_client, body=payload
            )
        except Exception as e:
            logger.error(
                "Failed to generate username for offering user %s (%s): %s",
                offering_user.user_email,
                offering_user.username,
                e,
            )

    pending_offering_users = [
        offering_user
        for offering_user in offering_users
        if offering_user.state
        in [
            OfferingUserStateEnum.CREATING,
            OfferingUserStateEnum.PENDING_ACCOUNT_LINKING,
            OfferingUserStateEnum.PENDING_ADDITIONAL_VALIDATION,
        ]
    ]

    logger.info("Processing %d pending offering users", len(pending_offering_users))
    for offering_user in pending_offering_users:
        try:
            logger.info(
                "Checking username for offering user %s (%s)",
                offering_user.user_email,
                offering_user.uuid,
            )

            update_offering_user_username(offering_user)
        except (
            backend_exceptions.OfferingUserAccountLinkingRequiredError,
            backend_exceptions.OfferingUserAdditionalValidationRequiredError,
        ):
            logger.info("Backend account is still in the pending state")
        except Exception as e:
            logger.error(
                "Failed to generate username for offering user %s (%s): %s",
                offering_user.user_email,
                offering_user.username,
                e,
            )


def sync_offering_users() -> None:
    """Process offering users for all configured offerings.

    This function retrieves offering users from Waldur and processes them
    according to the configured username management backend. It handles
    username generation and state transitions for offering users
    based on the backend's capabilities.
    """
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        logger.info("Processing offering users for %s", offering.name)

        waldur_rest_client = get_client(
            offering.api_url,
            offering.api_token,
            configuration.waldur_user_agent,
            offering.verify_ssl,
        )
        offering_users = marketplace_offering_users_list.sync(
            client=waldur_rest_client, offering_uuid=offering.uuid, is_restricted=False
        )
        update_offering_users(offering, waldur_rest_client, offering_users)


def sync_waldur_resource_limits(
    resource_backend: BaseBackend,
    waldur_rest_client: AuthenticatedClient,
    waldur_resource: WaldurResource,
) -> None:
    """Syncs resource limits between Waldur and the backend.

    The method is shared between utils and processors.
    """
    logger.info(
        "Syncing resource limits for resource %s (%s)",
        waldur_resource.name,
        waldur_resource.backend_id,
    )

    backend_limits = resource_backend.get_resource_limits(waldur_resource.backend_id)

    if not backend_limits:
        logger.warning("No limits found in the backend")
        return

    if waldur_resource.limits.additional_properties == backend_limits:
        logger.info("The limits are already in sync (%s), skipping", backend_limits)
        return

    # For now, we report all the limits
    logger.info("Changing resource limits from %s to %s", waldur_resource.limits, backend_limits)

    marketplace_provider_resources_set_limits.sync(
        uuid=waldur_resource.uuid.hex,
        client=waldur_rest_client,
        body=ResourceSetLimitsRequest(limits=backend_limits),
    )


def sync_resource_limits() -> None:
    """Report resource limits for the existing resources to Waldur."""
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        logger.info(
            "Processing resource limits for offering %s, backend plugin is %s",
            offering.name,
            offering.membership_sync_backend,
        )
        backend = get_backend_for_offering(offering, "membership_sync_backend")
        logger.info("Using class %s as a backend", backend.__class__.__name__)
        waldur_rest_client = get_client(
            offering.api_url,
            offering.api_token,
            configuration.waldur_user_agent,
            offering.verify_ssl,
        )
        resources = marketplace_resources_list.sync(
            client=waldur_rest_client,
            offering_uuid=offering.uuid,
            state=[MarketplaceResourcesListStateItem.OK, MarketplaceResourcesListStateItem.ERRED],
        )

        waldur_resources = [resource for resource in resources if resource.backend_id]
        logger.info("Processing limits for %s resource(s)", len(waldur_resources))
        for waldur_resource in waldur_resources:
            try:
                sync_waldur_resource_limits(backend, waldur_rest_client, waldur_resource)
            except (BackendError, UnexpectedStatus, TimeoutException) as e:
                logger.error(
                    "Failed to sync resource limits for %s, reason: %s", waldur_resource.name, e
                )
