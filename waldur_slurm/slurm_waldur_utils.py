from time import sleep
from typing import Dict, List

from waldur_client import ComponentUsage, WaldurClientException

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.exceptions import BackendError

from . import WALDUR_OFFERING_UUID, slurm_backend, waldur_rest_client


def drop_users_from_allocation(allocation_waldur: dict, usernames: set):
    logger.info("Stale usernames: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.delete_slurm_association(
                allocation_waldur["marketplace_resource_uuid"], username
            )
            logger.info(
                "The user %s has been dropped from %s (backend_id: %s)",
                username,
                allocation_waldur["name"],
                allocation_waldur["backend_id"],
            )
        except WaldurClientException as e:
            logger.error("User %s can not be dropped due to: %s", username, e)


def add_users_to_allocation(allocation_waldur: dict, usernames: set):
    logger.info("New usernames: %s", " ,".join(usernames))
    for username in usernames:
        try:
            waldur_rest_client.create_slurm_association(
                allocation_waldur["marketplace_resource_uuid"], username
            )
            logger.info(
                "The user %s has been added to %s (backend_id: %s)",
                username,
                allocation_waldur["name"],
                allocation_waldur["backend_id"],
            )
        except WaldurClientException as e:
            logger.error("User %s can not be added due to: %s", username, e)


def submit_total_usage_for_allocation(
    allocation_waldur: dict, total_usage: Dict[str, float], waldur_components
):
    logger.info("Setting usages: %s", total_usage)
    resource_uuid = allocation_waldur["marketplace_resource_uuid"]
    plan_periods = waldur_rest_client.marketplace_resource_get_plan_periods(
        resource_uuid
    )

    if len(plan_periods) == 0:
        logger.warning(
            "A corresponding ResourcePlanPeriod for allocation %s was not found",
            allocation_waldur["name"],
        )
        return

    plan_period = plan_periods[0]
    component_types = [component["type"] for component in waldur_components]
    missing_components = set(total_usage) - set(component_types)

    if missing_components:
        logger.warning(
            "The following components are not found in Waldur: %s",
            ", ".join(missing_components),
        )

    usage_objects = [
        ComponentUsage(type=component, amount=amount)
        for component, amount in total_usage.items()
        if component in component_types
    ]
    waldur_rest_client.create_component_usages(plan_period["uuid"], usage_objects)


def sync_data_from_slurm_to_waldur(allocation_report):
    # Push SLURM data to mastermind using REST client
    waldur_offering = waldur_rest_client._get_offering(WALDUR_OFFERING_UUID)
    for allocation_backend_id, allocation_data in allocation_report.items():
        print("-" * 30)
        try:
            logger.info("Processing %s", allocation_backend_id)
            usernames: List[str] = allocation_data["users"]
            usages: Dict[str, Dict[str, float]] = allocation_data["usage"]
            limits: Dict[str, float] = allocation_data["limits"]

            waldur_allocations = waldur_rest_client.list_slurm_allocations(
                {
                    "backend_id": allocation_backend_id,
                    "offering_uuid": WALDUR_OFFERING_UUID,
                }
            )
            if len(waldur_allocations) == 0:
                logger.warning(
                    "There are no allocations in Waldur with backend_id '%s',"
                    "skipping sync",
                    allocation_backend_id,
                )
                continue

            allocation_waldur = waldur_allocations[0]

            if allocation_waldur["state"] != "OK":
                logger.error(
                    "Allocation is not in OK state, current state: %s, skipping sync",
                    allocation_backend_id,
                )
                continue

            # Sync users
            marketplace_resource_uuid = allocation_waldur["marketplace_resource_uuid"]
            associations = waldur_rest_client.list_slurm_associations(
                {"allocation_uuid": allocation_waldur["uuid"]}
            )
            remote_usernames = {association["username"] for association in associations}
            local_usernames = set(usernames)

            stale_usernames = remote_usernames - local_usernames
            drop_users_from_allocation(allocation_waldur, stale_usernames)

            new_usernames = local_usernames - remote_usernames
            add_users_to_allocation(allocation_waldur, new_usernames)

            # Submit limits
            if limits is not None:
                logger.info("Setting limits to %s", limits)
                waldur_rest_client.set_slurm_allocation_limits(
                    marketplace_resource_uuid, limits
                )

            # Submit usage
            total_usage = usages["TOTAL_ACCOUNT_USAGE"]
            submit_total_usage_for_allocation(
                allocation_waldur, total_usage, waldur_offering["components"]
            )
        except WaldurClientException as e:
            logger.exception(
                "Waldur REST client error while processing allocation %s: %s",
                allocation_backend_id,
                e,
            )
        except BackendError as e:
            logger.exception(
                "Waldur SLURM client error while processing allocation %s: %s",
                allocation_backend_id,
                e,
            )


def slurm_waldur_sync():
    while True:
        try:
            logger.info("Fetching data from SLURM cluster")
            allocation_report = slurm_backend.pull_allocations()
            sync_data_from_slurm_to_waldur(allocation_report)
            sleep(60 * 60)  # Once per hour
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
