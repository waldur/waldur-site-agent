from waldur_client import OfferingComponent, WaldurClientException

from waldur_slurm.slurm_client import SLURM_TRES
from waldur_slurm.slurm_client.structures import Allocation

from . import WALDUR_OFFERING_UUID, logger, waldur_rest_client


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
    components = [
        OfferingComponent(
            billing_type=tres_info["accounting_type"],
            type=tres_type,
            name=tres_info["label"],
            measured_unit=tres_info["measured_unit"],
        )
        for tres_type, tres_info in SLURM_TRES.items()
    ]
    logger.info(
        "Updating offering components data for the following tres: %s",
        ", ".join(SLURM_TRES.keys()),
    )
    waldur_rest_client.update_offering_components(WALDUR_OFFERING_UUID, components)
