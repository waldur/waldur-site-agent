import traceback
from time import sleep

from waldur_client import SlurmAllocationState, WaldurClientException, is_uuid

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client import utils as slurm_utils
from waldur_slurm.slurm_client.exceptions import BackendError
from waldur_slurm.slurm_client.structures import Allocation

from . import (
    ENABLE_USER_HOMEDIR_ACCOUNT_CREATION,
    WALDUR_OFFERING_UUID,
    common_utils,
    slurm_backend,
    waldur_rest_client,
)


def create_allocation(order_item):
    resource_uuid = order_item["marketplace_resource_uuid"]
    resource_name = order_item["resource_name"]
    waldur_allocation_uuid = order_item["resource_uuid"]
    allocation_limits = slurm_utils.get_tres_limits()

    logger.info("Creating allocation %s", resource_name)
    resource = waldur_rest_client.get_marketplace_resource(resource_uuid)

    if not is_uuid(resource_uuid):
        logger.error("Unexpected resource UUID format, skipping the order")
        return

    if not is_uuid(waldur_allocation_uuid):
        logger.error("Unexpected allocation UUID format, skipping the order")
        return

    allocation = Allocation(
        name=order_item["resource_name"],
        uuid=waldur_allocation_uuid,
        marketplace_uuid=resource_uuid,
        project_uuid=order_item["project_uuid"],
        customer_uuid=order_item["customer_uuid"],
    )

    if resource["state"] != "Creating":
        logger.info(
            "Setting resource state (%s) to CREATING (current state is %s)",
            resource["uuid"],
            resource["state"],
        )
        waldur_rest_client.set_slurm_allocation_state(
            allocation.marketplace_uuid, SlurmAllocationState.CREATING
        )

    logger.info("Creating account in SLURM cluster")
    slurm_backend.create_allocation(
        allocation,
        project_name=order_item["project_name"],
        customer_name=order_item["customer_name"],
        limits=allocation_limits,
    )

    logger.info("Updating allocation metadata in Waldur")
    waldur_rest_client.marketplace_resource_set_backend_id(
        allocation.marketplace_uuid, allocation.backend_id
    )
    waldur_rest_client.set_slurm_allocation_backend_id(
        allocation.marketplace_uuid, allocation.backend_id
    )

    logger.info("Updating allocation limits in Waldur")
    waldur_rest_client.set_slurm_allocation_limits(
        allocation.marketplace_uuid, allocation_limits
    )

    logger.info("Updating order item state")
    waldur_rest_client.marketplace_order_item_set_state_done(order_item["uuid"])

    logger.info("Updating Waldur allocation state")
    waldur_rest_client.set_slurm_allocation_state(
        allocation.marketplace_uuid, SlurmAllocationState.OK
    )

    return allocation


def add_users_to_allocation(resource_uuid, allocation: Allocation):
    logger.info("Adding users to account in SLURM cluster")

    logger.info("Fetching Waldur resource team")
    team = waldur_rest_client.marketplace_resource_get_team(resource_uuid)
    user_uuids = {user["uuid"] for user in team}

    logger.info("Fetching Waldur offering users")
    offering_users_all = waldur_rest_client.list_remote_offering_users(
        {"offering_uuid": WALDUR_OFFERING_UUID}
    )
    offering_usernames = [
        offering_user["username"]
        for offering_user in offering_users_all
        if offering_user["user_uuid"] in user_uuids
    ]

    logger.info("Adding usernames to account in SLURM cluster")
    added_users = slurm_backend.add_users_to_account(allocation, offering_usernames)
    if ENABLE_USER_HOMEDIR_ACCOUNT_CREATION:
        slurm_backend.create_user_homedirs(added_users)

    common_utils.add_users_to_allocation(allocation, added_users)


def process_order_for_creation(order_item: dict):
    # Wait until resource is created
    attempts = 0
    while "marketplace_resource_uuid" not in order_item:
        if attempts > 4:
            logger.error("Order item processing timed out")
            return

        if order_item["state"] != "executing":
            logger.error("Order item has unexpected state %s", order_item["state"])
            return

        logger.info("Waiting for resource creation...")
        sleep(5)

        order_item = waldur_rest_client.get_order_item(order_item["uuid"])
        attempts += 1

    resource_uuid = order_item["marketplace_resource_uuid"]

    allocation: Allocation = create_allocation(order_item)

    if allocation is None:
        return

    add_users_to_allocation(resource_uuid, allocation)


def process_order_for_limits_update(order_item: dict):
    logger.info("Updating limits for %s", order_item["resource_name"])
    resource_uuid = order_item["marketplace_resource_uuid"]
    allocation_uuid = order_item["resource_uuid"]

    allocation_waldur = waldur_rest_client.get_slurm_allocation(allocation_uuid)
    allocation = Allocation(
        backend_id=allocation_waldur["backend_id"],
        project_uuid=order_item["project_uuid"],
        customer_uuid=order_item["customer_uuid"],
    )

    waldur_rest_client.set_slurm_allocation_state(
        resource_uuid, SlurmAllocationState.UPDATING
    )

    limits = order_item["limits"]
    if not limits:
        logger.error(
            "Order item %s (allocation %s) with type"
            + "Update does not include new limits",
            order_item["uuid"],
            allocation_waldur["name"],
        )

    slurm_backend.set_allocation_limits(allocation, limits)

    logger.info(
        "The limits for %s were updated successfully from %s to %s",
        allocation_waldur["name"],
        order_item["attributes"]["old_limits"],
        limits,
    )


def process_order_for_termination(order_item: dict):
    logger.info("Terminating allocation %s", order_item["resource_name"])
    allocation_uuid = order_item["resource_uuid"]

    allocation_waldur = waldur_rest_client.get_slurm_allocation(allocation_uuid)
    allocation = Allocation(
        backend_id=allocation_waldur["backend_id"],
        project_uuid=order_item["project_uuid"],
        customer_uuid=order_item["customer_uuid"],
    )
    slurm_backend.delete_allocation(allocation)

    waldur_rest_client.marketplace_order_item_set_state_done(order_item["uuid"])
    logger.info("Allocation has been terminated successfully")


def sync_data_from_waldur_to_slurm():
    # Pull data form Mastermind using REST client
    order_items = waldur_rest_client.list_order_items(
        {
            "offering_uuid": WALDUR_OFFERING_UUID,
            "state": ["pending", "executing"],
        }
    )

    if len(order_items) == 0:
        logger.info("There are no pending or executing order items")
        return

    for order_item in order_items:
        try:
            logger.info(
                "Processing order item %s (%s) with state %s",
                order_item["attributes"].get("name", "N/A"),
                order_item["uuid"],
                order_item["state"],
            )

            if order_item["state"] == "executing":
                logger.info("Order item is executing already, no need for approval")
            else:
                logger.info("Approving order item")
                waldur_rest_client.marketplace_order_item_approve(order_item["uuid"])
                logger.info("Refreshing the order item")
                order_item = waldur_rest_client.get_order_item(order_item["uuid"])

            if order_item["type"] == "Create":
                process_order_for_creation(order_item)

            if order_item["type"] == "Update":
                process_order_for_limits_update(order_item)

            if order_item["type"] == "Terminate":
                process_order_for_termination(order_item)

        except WaldurClientException as e:
            logger.exception(
                "Waldur REST client error while processing order %s: %s",
                order_item["uuid"],
                e,
            )
        except BackendError as e:
            logger.exception(
                "Waldur SLURM client error while processing order %s: %s",
                order_item["uuid"],
                e,
            )
            waldur_rest_client.marketplace_order_item_set_state_erred(
                order_item["uuid"],
                error_message=str(e),
                error_traceback=traceback.format_exc(),
            )

        logger.info("-" * 30)


def waldur_slurm_sync():
    logger.info("Synching data from Waldur to SLURM cluster")
    common_utils.create_offering_components()
    while True:
        logger.info("Pulling data from Waldur to SLURM cluster")
        try:
            sync_data_from_waldur_to_slurm()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        logger.info("/" * 30)
        sleep(2 * 60)  # Once per 2 minutes
