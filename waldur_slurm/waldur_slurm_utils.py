import traceback
from time import sleep

from waldur_client import SlurmAllocationState, WaldurClientException, is_uuid

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.exceptions import BackendError
from waldur_slurm.slurm_client.structures import Allocation

from . import (
    WALDUR_OFFERING_UUID,
    WALDUR_SLURM_USERNAME_SOURCE,
    WaldurSlurmUsernameSource,
    common_utils,
    slurm_backend,
    waldur_rest_client,
)


def fetch_usernames_registered_in_freeipa(team):
    usernames = set()
    all_freeipa_profiles = waldur_rest_client.list_freeipa_profiles()
    user_profile_mapping = {
        profile["user_uuid"]: profile["username"] for profile in all_freeipa_profiles
    }
    for user in team:
        freeipa_username = user_profile_mapping.get(user["uuid"])
        if freeipa_username:
            usernames.add(freeipa_username)
        else:
            logger.warning(
                "The user %s (%s) doesn't have any FreeIPA profiles," "skipping.",
                user["username"],
                user["full_name"],
            )
    return usernames


def process_order_for_creation(order_item: dict):
    if "marketplace_resource_uuid" not in order_item:
        logger.error(
            "The order item %s (%s) does not have a connected resource, skipping it.",
            order_item["uuid"],
            order_item["attributes"]["name"],
        )
        return
    resource_uuid = order_item["marketplace_resource_uuid"]
    resource_name = order_item["resource_name"]
    waldur_allocation_uuid = order_item["resource_uuid"]

    if not is_uuid(resource_uuid):
        logger.error("Unexpected resource UUID format, skipping the order")
        return

    if not is_uuid(waldur_allocation_uuid):
        logger.error("Unexpected allocation UUID format, skipping the order")
        return

    allocation = Allocation(
        name=order_item["resource_name"],
        uuid=waldur_allocation_uuid,
        project_uuid=order_item["project_uuid"],
        customer_uuid=order_item["customer_uuid"],
    )

    waldur_rest_client.set_slurm_allocation_state(
        resource_uuid, SlurmAllocationState.CREATING
    )

    team = waldur_rest_client.marketplace_resource_get_team(resource_uuid)
    username_fetching_function = {
        WaldurSlurmUsernameSource.LOCAL: lambda team: [
            user["username"] for user in team
        ],
        WaldurSlurmUsernameSource.FREEIPA: fetch_usernames_registered_in_freeipa,
    }
    usernames = username_fetching_function[WALDUR_SLURM_USERNAME_SOURCE](team)

    added_users, limits, backend_id = slurm_backend.create_allocation(
        allocation,
        project_name=order_item["project_name"],
        customer_name=order_item["customer_name"],
        usernames=usernames,
    )

    waldur_rest_client.marketplace_resource_set_backend_id(resource_uuid, backend_id)
    waldur_rest_client.set_slurm_allocation_backend_id(resource_uuid, backend_id)

    allocation_waldur = {
        "marketplace_resource_uuid": resource_uuid,
        "name": resource_name,
        "backend_id": allocation.backend_id,
    }
    common_utils.add_users_to_allocation(allocation_waldur, added_users)
    waldur_rest_client.set_slurm_allocation_limits(resource_uuid, limits)

    waldur_rest_client.marketplace_order_item_set_state_done(order_item["uuid"])
    waldur_rest_client.set_slurm_allocation_state(
        resource_uuid, SlurmAllocationState.OK
    )


def process_order_for_limits_update(order_item: dict):
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
    allocation_uuid = order_item["resource_uuid"]

    allocation_waldur = waldur_rest_client.get_slurm_allocation(allocation_uuid)
    allocation = Allocation(
        backend_id=allocation_waldur["backend_id"],
        project_uuid=order_item["project_uuid"],
        customer_uuid=order_item["customer_uuid"],
    )
    slurm_backend.delete_allocation(allocation)

    waldur_rest_client.marketplace_order_item_set_state_done(order_item["uuid"])


def sync_data_from_waldur_to_slurm():
    # Pull data form Mastermind using REST client
    order_items = waldur_rest_client.list_order_items(
        {
            "offering_uuid": WALDUR_OFFERING_UUID,
            "state": "executing",
        }
    )

    if len(order_items) == 0:
        logger.info("There are no approved order items")
        return

    for order_item in order_items:
        try:
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


def waldur_slurm_sync():
    while True:
        logger.info("Pulling data from Waldur to SLURM cluster")
        try:
            sync_data_from_waldur_to_slurm()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        print("/" * 30)
        sleep(2 * 60)  # Once per 2 minutes
