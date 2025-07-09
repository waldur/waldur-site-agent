"""Functions shared between agent modules."""

import argparse
import sys
from pathlib import Path
from typing import Optional
from uuid import UUID

import yaml
from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_offering_users import marketplace_offering_users_list
from waldur_api_client.api.marketplace_orders import marketplace_orders_list
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_create_offering_component,
    marketplace_provider_offerings_retrieve,
    marketplace_provider_offerings_update_offering_component,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_set_as_erred,
)
from waldur_api_client.api.users import users_me_retrieve
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import ResourceSetStateErredRequest
from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.marketplace_orders_list_state_item import (
    MarketplaceOrdersListStateItem,
)
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.offering_component_request import OfferingComponentRequest
from waldur_api_client.models.user import User

from waldur_site_agent.backends import (
    BackendType,
    logger,
)
from waldur_site_agent.backends.backend import BaseBackend, UnknownBackend
from waldur_site_agent.backends.moab_backend.backend import MoabBackend
from waldur_site_agent.backends.mup_backend.backend import MUPBackend
from waldur_site_agent.backends.slurm_backend import public_utils as slurm_utils
from waldur_site_agent.backends.slurm_backend.backend import SlurmBackend
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.backends.username_backend import backend as username_backend
from waldur_site_agent.common import structures

# Handle different Python versions
if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points, version
else:
    from importlib_metadata import entry_points, version

USERNAME_BACKENDS: dict[str, type[username_backend.AbstractUsernameManagementBackend]] = {
    entry_point.name: entry_point.load()
    for entry_point in entry_points(group="waldur_site_agent.username_management")
}

RESOURCE_ERRED_STATE = "Erred"


def get_client(
    api_url: str, access_token: str, agent_header: Optional[str] = None
) -> AuthenticatedClient:
    """Get a client for the Waldur API."""
    headers = {"User-Agent": agent_header} if agent_header else {}
    url = api_url.rstrip("/api")
    return AuthenticatedClient(
        base_url=url,
        token=access_token,
        timeout=600,
        headers=headers,
    )


def is_uuid(value: str) -> bool:
    """Check if a string is a valid UUID."""
    try:
        UUID(value)
        return True
    except ValueError:
        return False


def init_configuration() -> structures.WaldurAgentConfiguration:
    """Loads configuration from CLI and config file to the dataclass."""
    configuration = structures.WaldurAgentConfiguration()
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
            )
            for offering_info in offering_list
        ]
        configuration.waldur_offerings = waldur_offerings

        sentry_dsn = config.get("sentry_dsn")
        if sentry_dsn:
            import sentry_sdk  # noqa: PLC0415

            sentry_sdk.init(
                dsn=sentry_dsn,
            )
            configuration.sentry_dsn = sentry_dsn

        timezone = config.get("timezone", "UTC")
        configuration.timezone = timezone

    waldur_site_agent_version = version("waldur-site-agent")

    user_agent_dict = {
        structures.AgentMode.ORDER_PROCESS.value: "waldur-site-agent-order-process/"
        + waldur_site_agent_version,
        structures.AgentMode.REPORT.value: "waldur-site-agent-report/" + waldur_site_agent_version,
        structures.AgentMode.MEMBERSHIP_SYNC.value: "waldur-site-agent-membership-sync/"
        + waldur_site_agent_version,
        structures.AgentMode.EVENT_PROCESS.value: "waldur-site-agent-event-process/"
        + waldur_site_agent_version,
    }

    configuration.waldur_user_agent = user_agent_dict.get(agent_mode, "")
    configuration.waldur_site_agent_mode = agent_mode
    configuration.waldur_site_agent_version = waldur_site_agent_version

    return configuration


def get_backend_for_offering(offering: structures.Offering) -> BaseBackend:
    """Creates a corresponding backend for an offering."""
    resource_backend: BaseBackend = UnknownBackend()
    if offering.backend_type == BackendType.SLURM.value:
        resource_backend = SlurmBackend(offering.backend_settings, offering.backend_components)
    elif offering.backend_type in {
        BackendType.MOAB.value,
    }:
        resource_backend = MoabBackend(offering.backend_settings, offering.backend_components)
    elif offering.backend_type == BackendType.MUP.value:
        resource_backend = MUPBackend(offering.backend_settings, offering.backend_components)
    elif offering.backend_type == BackendType.CUSTOM.value:
        resource_backend = UnknownBackend()
    else:
        logger.error("Unknown backend type: %s", offering.backend_type)

    return resource_backend


def mark_waldur_resources_as_erred(
    waldur_rest_client: AuthenticatedClient,
    resources: list[Resource],
    error_details: dict[str, str],
) -> None:
    """Marks resources in Waldur as ERRED."""
    logger.info("Marking Waldur resources as ERRED")
    for resource in resources:
        logger.info("Marking %s resource as ERRED", resource)
        try:
            request_body = ResourceSetStateErredRequest(
                error_message=error_details.get("error_message", ""),
                error_traceback=error_details.get("error_traceback", ""),
            )
            marketplace_provider_resources_set_as_erred.sync_detailed(
                uuid=resource.marketplace_uuid, client=waldur_rest_client, body=request_body
            )
        except UnexpectedStatus as e:
            logger.exception(
                "Waldur REST client error while setting resource state to Erred %s: %s",
                resource.backend_id,
                e,
            )


def load_offering_components() -> None:
    """Creates offering components in Waldur based on data from the config file."""
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        logger.info("Processing %s offering", offering.name)
        waldur_rest_client = get_client(
            offering.api_url, offering.api_token, configuration.waldur_user_agent
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
    """Pulls offering component data from Waldur and populates it to the local configuration."""
    logger.info("Loading Waldur components to the local config")
    remote_components: dict[str, OfferingComponent] = {
        item.type_: item for item in waldur_offering_components
    }
    missing_component_types = set(remote_components.keys()) - set(
        offering.backend_components.keys()
    )

    logger.info("Component types to add: %s", ", ".join(missing_component_types))
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
    """Creates offering components in Waldur."""
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
                    component.uuid = existing_component.uuid
                    marketplace_provider_offerings_update_offering_component.sync_detailed(
                        client=waldur_rest_client, uuid=offering_uuid, body=component
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
    """Get the current user from the Waldur API."""
    return users_me_retrieve.sync(client=waldur_rest_client)


def diagnostics() -> bool:
    """Performs system check for offerings."""
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
            offering_api_url, offering_api_token, configuration.waldur_user_agent
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

        backend_diagnostics_result = False
        if offering.backend_type == BackendType.SLURM.value:
            backend = SlurmBackend(offering.backend_settings, offering.backend_components)
            backend_diagnostics_result = slurm_utils.diagnostics(backend)

        if not backend_diagnostics_result:
            return False

    logger.info("-" * 10 + "DIAGNOSTICS END" + "-" * 10)
    return True


def create_homedirs_for_offering_users() -> None:
    """Creates homedirs for offering users in SLURM cluster."""
    configuration = init_configuration()
    for offering in configuration.waldur_offerings:
        # Feature is exclusive for SLURM temporarily
        if offering.backend_type != BackendType.SLURM.value or not offering.backend_settings.get(
            "enable_user_homedir_account_creation", True
        ):
            continue

        logger.info("Creating homedirs for %s offering users", offering.name)

        waldur_rest_client = get_client(
            offering.api_url, offering.api_token, configuration.waldur_user_agent
        )
        offering_users = marketplace_offering_users_list.sync(
            client=waldur_rest_client, offering_uuid=offering.uuid, is_restricted=False
        )

        offering_user_usernames: set[str] = {
            offering_user.username for offering_user in offering_users
        }
        umask = offering.backend_settings.get("homedir_umask", "0700")
        slurm_backend = SlurmBackend(offering.backend_settings, offering.backend_components)
        slurm_backend._create_user_homedirs(offering_user_usernames, umask)


def print_current_user(current_user: User) -> None:
    """Print provided user's info."""
    logger.info("Current user username: %s", current_user.username)
    logger.info("Current user full name: %s", current_user.full_name)
    logger.info("Current user is staff: %s", current_user.is_staff)
    logger.info("List of permissions:")
    for permission in current_user.permissions:
        logger.info("Role name: %s", permission.role_name)
        logger.info("Role description: %s", permission.role_description)
        logger.info("Scope type: %s", permission.scope_type)
        logger.info("Scope name: %s", permission.scope_name)
        logger.info("Scope UUID: %s", permission.scope_uuid)
        logger.info("Expiration time: %s", permission.expiration_time)


def get_username_management_backend(
    offering: structures.Offering,
) -> username_backend.AbstractUsernameManagementBackend:
    """Get username management backend based on the offering."""
    username_management_setting = offering.username_management_backend

    if username_management_setting is None:
        logger.info(
            "No username_management_backend is set for offering %s, using the default one",
            offering.name,
        )
        return username_backend.BaseUsernameManagementBackend()

    return USERNAME_BACKENDS[username_management_setting]()
