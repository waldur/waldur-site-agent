import unittest
import uuid
from unittest import mock

from waldur_slurm.slurm_client.structures import Account
from waldur_slurm.waldur_slurm_utils import sync_data_from_waldur_to_slurm


@mock.patch("waldur_slurm.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_slurm.waldur_slurm_utils.waldur_rest_client")
class TestAllocationCreation(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.order_item_uuid = uuid.uuid4().hex
        self.waldur_order_item = {
            "uuid": self.order_item_uuid,
            "type": "Create",
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "state": "pending",
            "attributes": {"name": "sample_resource"},
        }

    def test_association_and_usage_creation(
        self, waldur_client: mock.Mock, slurm_client: mock.Mock
    ):
        user_uuid = uuid.uuid4()
        allocation_account = f"hpc_{self.allocation_uuid[:5]}_test-allocation-01"[:34]
        project_account = f"hpc_{self.project_uuid}"
        waldur_client.list_order_items.return_value = [self.waldur_order_item]
        updated_order_item = self.waldur_order_item.copy()

        updated_order_item.update(
            {
                "marketplace_resource_uuid": uuid.uuid4().hex,
                "resource_name": "test-allocation-01",
                "resource_uuid": self.allocation_uuid,
            }
        )
        waldur_client.get_order_item.return_value = updated_order_item

        waldur_client.marketplace_resource_get_team.return_value = [
            {
                "uuid": user_uuid,
                "username": "test-user-01",
                "full_name": "Test User",
            },
            {
                "uuid": uuid.uuid4(),
                "username": "test-user-02",
                "full_name": "Test User",
            },
        ]

        slurm_client.get_account.return_value = None
        slurm_client._execute_command.return_value = ""

        sync_data_from_waldur_to_slurm()

        waldur_client.marketplace_order_item_approve.assert_called_once_with(
            self.order_item_uuid
        )

        self.assertEqual(3, slurm_client.create_account.call_count)

        slurm_client.create_account.assert_called_with(
            name=allocation_account,
            description="test-allocation-01",
            organization=project_account,
        )


@mock.patch("waldur_slurm.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_slurm.waldur_slurm_utils.waldur_rest_client")
class TestAllocationTermination(unittest.TestCase):
    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.waldur_order_item = {
            "uuid": uuid.uuid4().hex,
            "type": "Terminate",
            "resource_name": "test-allocation-01",
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "marketplace_resource_uuid": self.resource_uuid,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "resource_uuid": self.resource_uuid,
            "attributes": {"name": "test-allocation-01"},
        }
        self.waldur_allocation = {
            "backend_id": f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34]
        }

    def test_account_deletion(self, waldur_client: mock.Mock, slurm_client: mock.Mock):
        waldur_client.list_order_items.return_value = [self.waldur_order_item]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order_item.return_value = self.waldur_order_item

        slurm_client.get_account.return_value = Account(
            name="test-allocation-01",
            description="test-allocation-01",
            organization="hpc_" + self.project_uuid,
        )
        slurm_client.list_accounts.return_value = []
        slurm_client._execute_command.return_value = ""

        sync_data_from_waldur_to_slurm()

        # The method was called twice: for project account and for allocation account
        self.assertEqual(2, slurm_client.delete_account.call_count)


@mock.patch("waldur_slurm.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_slurm.waldur_slurm_utils.waldur_rest_client")
class TestAllocationUpdateLimits(unittest.TestCase):
    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.waldur_order_item = {
            "uuid": uuid.uuid4().hex,
            "type": "Update",
            "resource_name": "test-allocation-01",
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "marketplace_resource_uuid": self.resource_uuid,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "resource_uuid": self.resource_uuid,
            "limits": {
                "cpu": 101,
                "gpu": 201,
                "mem": 301,
            },
            "attributes": {
                "old_limits": {
                    "cpu": 100,
                    "gpu": 200,
                    "mem": 300,
                },
                "name": "test-allocation-01",
            },
        }
        self.waldur_allocation = {
            "name": "alloc",
            "backend_id": f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34],
        }

    def test_allocation_limits_update(
        self, waldur_client: mock.Mock, slurm_client: mock.Mock
    ):
        waldur_client.list_order_items.return_value = [self.waldur_order_item]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order_item.return_value = self.waldur_order_item

        sync_data_from_waldur_to_slurm()

        slurm_client.set_resource_limits.assert_called_once_with(
            self.waldur_allocation["backend_id"],
            {
                "cpu": 101,
                "gpu": 201,
                "mem": 301,
            },
        )
