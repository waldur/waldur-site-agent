import unittest
import uuid
from unittest import mock

from waldur_site_agent.slurm_client.structures import Account
from waldur_site_agent.waldur_slurm_utils import process_offerings


@mock.patch("waldur_site_agent.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_site_agent.waldur_slurm_utils.WaldurClient", autospec=True)
class TestAllocationCreation(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.uuid4().hex
        self.waldur_order = {
            "uuid": self.order_uuid,
            "type": "Create",
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "state": "pending-provider",
            "attributes": {"name": "sample_resource"},
        }

    def test_association_and_usage_creation(
        self, waldur_client_class: mock.Mock, slurm_client: mock.Mock
    ):
        user_uuid = uuid.uuid4()
        allocation_account = f"hpc_{self.allocation_uuid[:5]}_test-allocation-01"[:34]
        project_account = f"hpc_{self.project_uuid}"
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        updated_order = self.waldur_order.copy()

        updated_order.update(
            {
                "marketplace_resource_uuid": uuid.uuid4().hex,
                "resource_name": "test-allocation-01",
                "resource_uuid": self.allocation_uuid,
            }
        )
        waldur_client.get_order.return_value = updated_order

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

        process_offerings()

        waldur_client.marketplace_order_approve_by_provider.assert_called_once_with(
            self.order_uuid
        )

        self.assertEqual(3, slurm_client.create_account.call_count)

        slurm_client.create_account.assert_called_with(
            name=allocation_account,
            description="test-allocation-01",
            organization=project_account,
        )


@mock.patch("waldur_site_agent.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_site_agent.waldur_slurm_utils.WaldurClient", autospec=True)
class TestAllocationTermination(unittest.TestCase):
    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.waldur_order = {
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
            "state": "executing",
        }
        self.waldur_allocation = {
            "backend_id": f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34]
        }

    def test_account_deletion(
        self, waldur_client_class: mock.Mock, slurm_client: mock.Mock
    ):
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order

        slurm_client.get_account.return_value = Account(
            name="test-allocation-01",
            description="test-allocation-01",
            organization="hpc_" + self.project_uuid,
        )
        slurm_client.list_accounts.return_value = []
        slurm_client._execute_command.return_value = ""

        process_offerings()

        # The method was called twice: for project account and for allocation account
        self.assertEqual(2, slurm_client.delete_account.call_count)


@mock.patch("waldur_site_agent.waldur_slurm_utils.slurm_backend.client")
@mock.patch("waldur_site_agent.waldur_slurm_utils.WaldurClient", autospec=True)
class TestAllocationUpdateLimits(unittest.TestCase):
    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.waldur_order = {
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
            "state": "executing",
        }
        self.waldur_allocation = {
            "name": "alloc",
            "backend_id": f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34],
        }

    def test_allocation_limits_update(
        self, waldur_client_class: mock.Mock, slurm_client: mock.Mock
    ):
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order

        process_offerings()

        slurm_client.set_resource_limits.assert_called_once_with(
            self.waldur_allocation["backend_id"],
            {
                "cpu": 101,
                "gpu": 201,
                "mem": 301,
            },
        )
