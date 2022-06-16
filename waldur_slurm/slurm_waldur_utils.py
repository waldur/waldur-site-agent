import datetime
from time import sleep
from typing import Dict, List

from waldur_client import WaldurClientException

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.exceptions import BackendError
from waldur_slurm.slurm_client.structures import Quotas

from . import slurm_backend, waldur_rest_client


def is_usage_increased(user_usage_slurm: dict, user_usage_waldur: dict):
    """
    Checks if usage has been monotonously increased in one of the usage unit values
    """

    for usage_unit in user_usage_slurm.keys():
        usage_amount_slurm = user_usage_slurm[usage_unit]
        usage_amount_waldur = user_usage_waldur[usage_unit]
        if usage_amount_slurm > usage_amount_waldur:
            return True
    return False


def report_allocation_usage(
    allocation_waldur, user_usages, year, month, username, user_uuid
):
    """
    Wrap for allocation usage setting via WaldurClient
    """
    waldur_rest_client.set_slurm_allocation_usage(
        allocation_waldur["marketplace_resource_uuid"],
        username,
        month,
        year,
        user_usages["cpu_usage"],
        user_usages["gpu_usage"],
        user_usages["ram_usage"],
        user_uuid,
    )

    logger.info(
        "Usage %s (user %s) for allocation %s (backend_id: %s) has been reported",
        user_usages,
        username,
        allocation_waldur["name"],
        allocation_waldur["backend_id"],
    )


def drop_users_from_allocation(allocation_waldur: dict, usernames: str):
    logger.info("Stale usernames: %s", usernames)
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


def add_users_to_allocation(allocation_waldur: dict, usernames: str):
    logger.info("New usernames: %s", usernames)
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


def submit_usages_for_allocation(allocation_waldur: dict, usages: Dict[str, Quotas]):
    logger.info("Setting usages: %s", usages)
    now = datetime.datetime.now()
    month = now.month
    year = now.year
    for username, usage_metrics in usages.items():
        # TODO: consider change of types in 'Quotas' model (float to int)
        user_usage_slurm = {
            "cpu_usage": round(usage_metrics.cpu),
            "gpu_usage": round(usage_metrics.gpu),
            "ram_usage": round(usage_metrics.ram),
        }
        usage_units = user_usage_slurm.keys()
        try:
            if username == "TOTAL_ACCOUNT_USAGE":
                # Check if total usage has been increased for account in this month
                if is_usage_increased(
                    user_usage_slurm,
                    {
                        usage_unit: allocation_waldur[usage_unit]
                        for usage_unit in usage_units
                    },
                ):
                    report_allocation_usage(
                        allocation_waldur, user_usage_slurm, year, month, username, None
                    )
            else:
                users = waldur_rest_client.list_users({"username": username})
                if len(users) == 0:
                    logger.error(
                        "There are no users with username %s in Waldur", username
                    )
                    continue

                user = users[0]
                user_uuid = user["uuid"]
                user_usages_waldur = (
                    waldur_rest_client.list_slurm_allocation_user_usage(
                        {
                            "allocation_uuid": allocation_waldur["uuid"],
                            "user_uuid": user_uuid,
                            "month": month,
                            "year": year,
                        }
                    )
                )
                if len(user_usages_waldur) == 0:
                    # Report usage, which is not recorded in Waldur
                    report_allocation_usage(
                        allocation_waldur,
                        user_usage_slurm,
                        year,
                        month,
                        username,
                        user_uuid,
                    )
                else:
                    # Check if user usage has been increased for account in this month
                    user_usage_waldur = user_usages_waldur[0]
                    if is_usage_increased(user_usage_slurm, user_usage_waldur):
                        report_allocation_usage(
                            allocation_waldur,
                            user_usage_slurm,
                            year,
                            month,
                            username,
                            user_uuid,
                        )
        except WaldurClientException as e:
            logger.error(e)


def sync_data_from_slurm_to_waldur(allocation_report):
    # Push SLURM data to mastermind using REST client
    for allocation_backend_id, allocation_data in allocation_report.items():
        print("-" * 30)
        try:
            logger.info("Processing %s", allocation_backend_id)
            usernames: List[str] = allocation_data["users"]
            usages: Dict[str, Quotas] = allocation_data["usage"]
            limits: Quotas = allocation_data["limits"]

            waldur_allocations = waldur_rest_client.list_slurm_allocations(
                {"backend_id": allocation_backend_id}
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

            if limits is not None:
                logger.info("Setting limits to %s", limits)
                waldur_rest_client.set_slurm_allocation_limits(
                    marketplace_resource_uuid,
                    round(limits.cpu),
                    round(limits.gpu),
                    round(limits.ram),
                )
            submit_usages_for_allocation(allocation_waldur, usages)
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
