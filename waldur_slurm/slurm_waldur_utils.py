from time import sleep
from typing import Dict, List

from waldur_client import ComponentUsage, WaldurClientException

from waldur_slurm import common_utils
from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.exceptions import BackendError
from waldur_slurm.slurm_client.structures import Allocation

from . import WALDUR_OFFERING_UUID, slurm_backend, waldur_rest_client


def submit_total_usage_for_allocation(
    allocation: Allocation, total_usage: Dict[str, float], waldur_components
):
    logger.info("Setting usages: %s", total_usage)
    resource_uuid = allocation.marketplace_uuid
    plan_periods = waldur_rest_client.marketplace_resource_get_plan_periods(
        resource_uuid
    )

    if len(plan_periods) == 0:
        logger.warning(
            "A corresponding ResourcePlanPeriod for allocation %s was not found",
            allocation.name,
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


def sync_allocation_users(allocation: Allocation, usernames: List[str]):
    logger.info("Syncing associations")
    associations = waldur_rest_client.list_slurm_associations(
        {"allocation_uuid": allocation.uuid}
    )
    remote_usernames = {association["username"] for association in associations}
    local_usernames = set(usernames)

    stale_usernames = remote_usernames - local_usernames
    common_utils.drop_users_from_allocation(allocation, stale_usernames)

    new_usernames = local_usernames - remote_usernames
    common_utils.add_users_to_allocation(allocation, new_usernames)

    # Offering users sync
    logger.info("Creating associations for offering users")
    offering_users = waldur_rest_client.list_remote_offering_users(
        {
            "offering_uuid": WALDUR_OFFERING_UUID,
        }
    )

    offering_user_usernames = [
        offering_user["username"]
        for offering_user in offering_users
        if offering_user["username"] not in local_usernames
    ]

    common_utils.add_users_to_allocation(allocation, offering_user_usernames)
    slurm_backend.add_users_to_account(allocation, offering_user_usernames)


def sync_data_from_slurm_to_waldur(allocation_report):
    waldur_offering = waldur_rest_client._get_offering(WALDUR_OFFERING_UUID)
    # Push SLURM data to Mastermind using REST client
    for allocation_backend_id, allocation_data in allocation_report.items():
        logger.info("-" * 30)
        try:
            logger.info("Processing %s", allocation_backend_id)
            usernames: List[str] = allocation_data["users"]
            usages: Dict[str, Dict[str, float]] = allocation_data["usage"]
            limits: Dict[str, float] = allocation_data["limits"]

            waldur_resources = waldur_rest_client.filter_marketplace_resources(
                {
                    "backend_id": allocation_backend_id,
                    "offering_uuid": WALDUR_OFFERING_UUID,
                    "state": "OK",
                }
            )
            if len(waldur_resources) == 0:
                logger.warning(
                    "There are no resources in Waldur with backend_id '%s',"
                    "skipping sync",
                    allocation_backend_id,
                )
                continue

            marketplace_resource = waldur_resources[0]
            allocation = Allocation(
                name=marketplace_resource["name"],
                uuid=marketplace_resource["resource_uuid"],
                marketplace_uuid=marketplace_resource["uuid"],
                backend_id=allocation_backend_id,
                project_uuid=marketplace_resource["project_uuid"],
                customer_uuid=marketplace_resource["customer_uuid"],
            )

            # Sync users
            sync_allocation_users(allocation, usernames)

            # Submit limits
            if limits is not None:
                logger.info("Setting limits to %s", limits)
                waldur_rest_client.set_slurm_allocation_limits(
                    allocation.marketplace_uuid, limits
                )

            # Submit usage
            total_usage = usages["TOTAL_ACCOUNT_USAGE"]
            submit_total_usage_for_allocation(
                allocation, total_usage, waldur_offering["components"]
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
    logger.info("Synching data from SLURM cluster to Waldur")
    while True:
        try:
            logger.info("Fetching data from SLURM cluster")
            allocation_report = slurm_backend.pull_allocations()
            sync_data_from_slurm_to_waldur(allocation_report)
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        sleep(60 * 60)  # Once per hour
