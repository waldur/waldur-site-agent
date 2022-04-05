import json
from time import sleep

from . import slurm_backend, waldur_rest_client
from .slurm_client.structures import Allocation


def app():
    test_allocation = Allocation(
        "creation_test", "uuid", "", "project_uuid", "customer_uuid"
    )
    created_associations, removed_associations = slurm_backend.create_allocation(
        test_allocation, "customer_name", "project_name", ["root"], None
    )
    print(created_associations)
    print(removed_associations)
    # TODO: fetch FreeIPA profiles before allocation creation and users sync
    created_associations, removed_associations = slurm_backend.sync_users(
        test_allocation, []
    )
    print(created_associations)
    print(removed_associations)
    print(waldur_rest_client)
    while True:
        # TODO: main algorithm:
        # 1) Pull data from SLURM cluster (slurm_backend.pull_allocations())
        # 2) Push SLURM data to mastermind using REST client
        # 3) Pull data form Mastermind using REST client
        # 4) Push data to SLURM cluster using slurm_backend
        print(json.dumps(slurm_backend.pull_allocations()))
        sleep(10)


if __name__ == "__main__":
    app()
