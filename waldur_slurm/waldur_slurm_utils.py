import json
from time import sleep

from waldur_client import WaldurClientException

from waldur_slurm.slurm_client import logger
from waldur_slurm.slurm_client.exceptions import BackendError
from waldur_slurm.slurm_client.structures import Allocation

from . import slurm_backend, waldur_rest_client


def sync_data_from_waldur_to_slurm():
    # Pull data form Mastermind using REST client
    # TODO: filter by 'updated' field
    order_items = waldur_rest_client.list_order_items(
        {"offering_type": "SlurmInvoices.SlurmPackage", "state": "executing"}
    )
    for order_item in order_items:
        try:
            if order_item["type"] == "Create":
                allocation = Allocation(
                    name=order_item["resource_name"],
                    project_uuid=order_item["project_uuid"],
                    customer_uuid=order_item["customer_uuid"],
                )
                # TODO: fetch all users from the project and customer
                # usernames = ...
                slurm_backend.create_allocation(
                    allocation,
                    project_name=order_item["project_name"],
                    customer_name=order_item["customer_name"],
                )

            if order_item["type"] == "Update":
                pass

            if order_item["type"] == "Terminate":
                pass

            # Get info about users in the project related to order_item

            # Push data to SLURM cluster using slurm_backend
            print(json.dumps(slurm_backend.pull_allocations()))
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


def waldur_slurm_sync():
    while True:
        logger.info("Pushing data from Waldur to SLURM cluster")
        try:
            sync_data_from_waldur_to_slurm()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        print("/" * 30)
        sleep(2 * 60)  # Once per 2 minutes
