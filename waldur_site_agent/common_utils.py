"""Functions shared between agent modules."""

from typing import Set

from waldur_client import WaldurClient, WaldurClientException

from waldur_site_agent.backends import (
    ENABLE_USER_HOMEDIR_ACCOUNT_CREATION,
    BackendType,
    logger,
)
from waldur_site_agent.backends.backend import BaseBackend, UnknownBackend
from waldur_site_agent.backends.slurm_backend import utils as slurm_utils
from waldur_site_agent.backends.slurm_backend.backend import SlurmBackend
from waldur_site_agent.backends.structures import Resource

from . import (
    USER_AGENT,
    WALDUR_OFFERINGS,
    WALDUR_SITE_AGENT_MODE,
    Offering,
    sentry_dsn,
)


def get_backend_for_offering(offering: Offering) -> BaseBackend:
    """Creates a corresponding backend for an offering."""
    resource_backend: BaseBackend = UnknownBackend()
    if offering.backend_type == BackendType.SLURM.value:
        resource_backend = SlurmBackend()
    elif offering.backend_type in {
        BackendType.MOAB.value,
        BackendType.CUSTOM.value,
    }:
        return resource_backend
    else:
        logger.error("Unknown backend type: %s", offering.backend_type)
        return UnknownBackend()

    return resource_backend


def delete_associations_from_waldur_allocation(
    waldur_rest_client: WaldurClient,
    backend_resource: Resource,
    usernames: Set[str],
) -> None:
    """Deletes a SLURM association for the specified resource and username in Waldur."""
    logger.info("Stale usernames: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.delete_slurm_association(backend_resource.marketplace_uuid, username)
            logger.info(
                "The user %s has been dropped from %s (backend_id: %s)",
                username,
                backend_resource.name,
                backend_resource.backend_id,
            )
        except WaldurClientException as e:
            logger.error("User %s can not be dropped due to: %s", username, e)


def create_associations_for_waldur_allocation(
    waldur_rest_client: WaldurClient,
    backend_resource: Resource,
    usernames: Set[str],
) -> None:
    """Creates a SLURM association for the specified resource and username in Waldur."""
    logger.info("New usernames to add to Waldur allocation: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.create_slurm_association(backend_resource.marketplace_uuid, username)
            logger.info(
                "The user %s has been added to %s (backend_id: %s)",
                username,
                backend_resource.name,
                backend_resource.backend_id,
            )
        except WaldurClientException as e:
            logger.error("User %s can not be added due to: %s", username, e)


def create_offering_components() -> None:
    """Creates offering components in Waldur based on data from the config file."""
    for offering in WALDUR_OFFERINGS:
        logger.info("Processing %s offering", offering.name)
        waldur_rest_client = WaldurClient(offering.api_url, offering.api_token, USER_AGENT)

        if offering.backend_type == BackendType.SLURM.value:
            slurm_utils.create_offering_components(waldur_rest_client, offering.uuid, offering.name)


def diagnostics() -> bool:
    """Performs system check for offerings."""
    logger.info("-" * 10 + "DIAGNOSTICS START" + "-" * 10)
    logger.info("Provided settings:")
    format_string = "{:<30} = {:<10}"
    logger.info(format_string.format("WALDUR_SYNC_DIRECTION", WALDUR_SITE_AGENT_MODE))

    for offering in WALDUR_OFFERINGS:
        format_string = "{:<30} = {:<10}"
        offering_uuid = offering.uuid
        offering_name = offering.name
        offering_api_url = offering.api_url
        offering_api_token = offering.api_token

        logger.info(format_string.format("Offering name", offering_name))
        logger.info(format_string.format("Offering UUID", offering_uuid))
        logger.info(format_string.format("Waldur API URL", offering_api_url))
        logger.info(format_string.format("SENTRY_DSN", str(sentry_dsn)))

        waldur_rest_client = WaldurClient(offering_api_url, offering_api_token, USER_AGENT)

        try:
            offering_data = waldur_rest_client.get_marketplace_provider_offering(offering_uuid)
            logger.info("Offering uuid: %s", offering_data["uuid"])
            logger.info("Offering name: %s", offering_data["name"])
            logger.info("Offering org: %s", offering_data["customer_name"])
            logger.info("Offering state: %s", offering_data["state"])

            logger.info("Offering components:")
            format_string = "{:<10} {:<10} {:<10} {:<10}"
            headers = ["Type", "Name", "Unit", "Limit"]
            logger.info(format_string.format(*headers))
            components = [
                [
                    component["type"],
                    component["name"],
                    component["measured_unit"],
                    component["limit_amount"],
                ]
                for component in offering_data["components"]
            ]
            for component in components:
                logger.info(format_string.format(*component))

            logger.info("")
        except WaldurClientException as err:
            logger.error("Unable to fetch offering data, reason: %s", err)

        logger.info("")
        try:
            orders = waldur_rest_client.list_orders(
                {
                    "offering_uuid": offering_uuid,
                    "state": ["pending-provider", "executing"],
                }
            )
            logger.info("Active orders:")
            format_string = "{:<10} {:<10} {:<10}"
            headers = ["Project", "Type", "State"]
            logger.info(format_string.format(*headers))
            for order in orders:
                logger.info(
                    format_string.format(order["project_name"], order["type"], order["state"])
                )
        except WaldurClientException as err:
            logger.error("Unable to fetch orders, reason: %s", err)

        backend_diagnostics_result = False
        if offering.backend_type == BackendType.SLURM.value:
            backend_diagnostics_result = slurm_utils.diagnostics()

        if not backend_diagnostics_result:
            return False

    logger.info("-" * 10 + "DIAGNOSTICS END" + "-" * 10)
    return True


def create_homedirs_for_offering_users() -> None:
    """Creates homedirs for offering users in SLURM cluster."""
    if not ENABLE_USER_HOMEDIR_ACCOUNT_CREATION:
        logger.warning("ENABLE_USER_HOMEDIR_ACCOUNT_CREATION disabled, skipping processing")
        return

    for offering in WALDUR_OFFERINGS:
        # Feature is exclusive for SLURM temporarily
        if offering.backend_type != BackendType.SLURM.value:
            continue

        logger.info("Creating homedirs for %s offering users", offering.name)

        waldur_rest_client = WaldurClient(offering.api_url, offering.api_token, USER_AGENT)

        offering_users = waldur_rest_client.list_remote_offering_users(
            {
                "offering_uuid": offering.uuid,
            }
        )

        offering_user_usernames: Set[str] = {
            offering_user["username"] for offering_user in offering_users
        }
        slurm_backend = SlurmBackend()
        slurm_backend._create_user_homedirs(offering_user_usernames)
