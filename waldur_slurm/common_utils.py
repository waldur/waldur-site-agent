from waldur_client import WaldurClientException

from . import logger, waldur_rest_client


def drop_users_from_allocation(allocation_waldur: dict, usernames: str):
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
