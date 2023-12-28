import pprint

from waldur_client import OfferingComponent, WaldurClientException

from waldur_slurm.slurm_client import (
    SLURM_ALLOCATION_NAME_MAX_LEN,
    SLURM_ALLOCATION_PREFIX,
    SLURM_CONTAINER_NAME,
    SLURM_CUSTOMER_PREFIX,
    SLURM_DEFAULT_ACCOUNT,
    SLURM_DEPLOYMENT_TYPE,
    SLURM_PROJECT_PREFIX,
    SLURM_TRES,
)
from waldur_slurm.slurm_client.exceptions import SlurmError
from waldur_slurm.slurm_client.structures import Allocation

from . import (
    WALDUR_API_URL,
    WALDUR_OFFERING_UUID,
    WALDUR_SYNC_DIRECTION,
    WaldurSyncDirection,
    logger,
    sentry_dsn,
    slurm_backend,
    waldur_rest_client,
)


def drop_users_from_allocation(allocation: Allocation, usernames: str):
    logger.info("Stale usernames: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.delete_slurm_association(
                allocation.marketplace_uuid, username
            )
            logger.info(
                "The user %s has been dropped from %s (backend_id: %s)",
                username,
                allocation.name,
                allocation.backend_id,
            )
        except WaldurClientException as e:
            logger.error("User %s can not be dropped due to: %s", username, e)


def add_users_to_allocation(allocation: Allocation, usernames: set):
    logger.info("New usernames to add to Waldur allocation: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.create_slurm_association(
                allocation.marketplace_uuid, username
            )
            logger.info(
                "The user %s has been added to %s (backend_id: %s)",
                username,
                allocation.name,
                allocation.backend_id,
            )
        except WaldurClientException as e:
            logger.error("User %s can not be added due to: %s", username, e)


def create_offering_components():
    logger.info(
        "Creating offering components data for the following TRES: %s",
        ", ".join(SLURM_TRES.keys()),
    )
    for tres_type, tres_info in SLURM_TRES.items():
        component = OfferingComponent(
            billing_type=tres_info["accounting_type"],
            type=tres_type,
            name=tres_info["label"],
            measured_unit=tres_info["measured_unit"],
            limit_amount=tres_info["limit"],
        )
        waldur_rest_client.create_offering_component(WALDUR_OFFERING_UUID, component)


def diagnostics():
    logger.info("-" * 10 + "DIAGNOSTICS START" + "-" * 10)
    logger.info("Provided settings:")
    format_string = "{:<30} = {:<10}"

    logger.info(format_string.format("WALDUR_API_URL", WALDUR_API_URL))
    logger.info(format_string.format("WALDUR_SYNC_DIRECTION", WALDUR_SYNC_DIRECTION))
    logger.info(format_string.format("WALDUR_OFFERING_UUID", WALDUR_OFFERING_UUID))
    logger.info(format_string.format("SENTRY_DSN", str(sentry_dsn)))
    logger.newline()

    logger.info(format_string.format("SLURM_DEPLOYMENT_TYPE", SLURM_DEPLOYMENT_TYPE))
    logger.info(
        format_string.format(
            "SLURM_ALLOCATION_NAME_MAX_LEN", SLURM_ALLOCATION_NAME_MAX_LEN
        )
    )
    logger.info(format_string.format("SLURM_CUSTOMER_PREFIX", SLURM_CUSTOMER_PREFIX))
    logger.info(format_string.format("SLURM_PROJECT_PREFIX", SLURM_PROJECT_PREFIX))
    logger.info(
        format_string.format("SLURM_ALLOCATION_PREFIX", SLURM_ALLOCATION_PREFIX)
    )
    logger.info(format_string.format("SLURM_DEFAULT_ACCOUNT", SLURM_DEFAULT_ACCOUNT))
    logger.info(format_string.format("SLURM_CONTAINER_NAME", SLURM_CONTAINER_NAME))
    logger.newline()

    logger.info("SLURM tres config file content:\n%s\n", pprint.pformat(SLURM_TRES))

    if WALDUR_SYNC_DIRECTION == WaldurSyncDirection.PULL.value:
        logger.info(
            "Agent is running in pull mode - "
            "pulling orders from Waldur and creating allocations"
        )
    else:
        logger.info("Agent is running in push mode - pushing usage stats to Waldur")

    try:
        slurm_version_info = slurm_backend.client._execute_command(
            ["-V"], "sinfo", immediate=False, parsable=False
        )
        logger.info("Slurm version: %s", slurm_version_info.strip())
    except SlurmError as err:
        logger.error("Unable to fetch SLURM info, reason: %s", err)
        return False

    try:
        slurm_backend.ping(raise_exception=True)
        logger.info("SLURM cluster ping is successful")
    except SlurmError as err:
        logger.error("Unable to ping SLURM cluster, reason: %s", err)

    tres = slurm_backend.list_tres()
    logger.info("Available tres in the cluster: %s", ",".join(tres))

    default_account = slurm_backend.client.get_account(SLURM_DEFAULT_ACCOUNT)
    if default_account is None:
        logger.error("There is no account %s in the cluster", default_account)
        return False
    logger.info('Default parent account "%s" is in place', SLURM_DEFAULT_ACCOUNT)
    logger.newline()

    try:
        offering_data = waldur_rest_client._get_offering(WALDUR_OFFERING_UUID)
        logger.info("Offering uuid: %s", offering_data["uuid"])
        logger.info("Offering name: %s", offering_data["name"])
        logger.info("Offering org: %s", offering_data["customer_name"])
        logger.info("Offering state: %s", offering_data["state"])
        logger.newline()

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
    except WaldurClientException as err:
        logger.error("Unable to fetch offering data, reason: %s", err)

    logger.newline()
    try:
        orders = waldur_rest_client.list_orders(
            {
                "offering_uuid": WALDUR_OFFERING_UUID,
                "state": ["pending-provider", "executing"],
            }
        )
        logger.info("Active orders:")
        format_string = "{:<10} {:<10} {:<10}"
        headers = ["Project", "Type", "State"]
        logger.info(format_string.format(*headers))
        for order in orders:
            logger.info(
                format_string.format(
                    order["project_name"], order["type"], order["state"]
                )
            )
    except WaldurClientException as err:
        logger.error("Unable to fetch orders, reason: %s", err)

    logger.newline()
    logger.info("-" * 10 + "DIAGNOSTICS END" + "-" * 10)
    return True
