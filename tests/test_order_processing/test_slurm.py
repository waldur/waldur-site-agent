import unittest
import uuid
from unittest import mock

from waldur_site_agent.agent_order_process import process_offerings
from waldur_site_agent.backends.structures import Account
from waldur_site_agent import MARKETPLACE_SLURM_OFFERING_TYPE
from tests.fixtures import OFFERING


@mock.patch(
    "waldur_site_agent.backends.slurm_backend.backend.SlurmClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.processors.WaldurClient", autospec=True)
class CreationOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.customer_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.waldur_order = {
            "uuid": self.order_uuid,
            "type": "Create",
            "state": "pending-provider",
            "attributes": {"name": "sample_resource"},
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "project_slug": "project-1",
            "customer_slug": "customer-1",
        }
        self.waldur_resource = {
            "uuid": self.resource_uuid,
            "name": "sample_resource",
            "resource_uuid": self.allocation_uuid,
            "project_uuid": self.project_uuid,
            "customer_uuid": self.customer_uuid,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "limits": {"cpu": 10},
            "state": "Creating",
            "slug": "sample-resource-1",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "offering_plugin_options": {},
        }

    def setup_waldur_client_mock(self, waldur_client_class):
        user_uuid = uuid.uuid4().hex
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

        updated_resource = self.waldur_resource.copy()
        updated_resource.update(
            {
                "marketplace_resource_uuid": uuid.uuid4().hex,
                "name": "test-allocation-01",
            }
        )
        waldur_client.get_order.return_value = updated_order

        waldur_client.marketplace_provider_resource_get_team.return_value = [
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
        waldur_client.list_remote_offering_users.return_value = [
            {"username": "test-offering-user-01", "user_uuid": user_uuid}
        ]
        waldur_client.get_marketplace_resource.return_value = updated_resource

        return waldur_client

    def test_allocation_creation(
        self, waldur_client_class: mock.Mock, slurm_client_class: mock.Mock
    ):
        offering_user_username = "test-offering-user-01"
        allocation_account = "hpc_sample-resource-1"
        project_account = f"hpc_project-1"

        waldur_client = self.setup_waldur_client_mock(waldur_client_class)

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = None
        slurm_client.get_account.return_value = None
        slurm_client._execute_command.return_value = ""

        process_offerings([OFFERING])

        waldur_client.marketplace_order_approve_by_provider.assert_called_once_with(self.order_uuid)

        self.assertEqual(3, slurm_client.create_account.call_count)

        slurm_client.create_account.assert_called_with(
            name=allocation_account,
            description="test-allocation-01",
            organization=project_account,
        )
        slurm_client.create_association.assert_called_with(
            offering_user_username, allocation_account, "root"
        )

    def test_allocation_creation_with_project_slug(
        self, waldur_client_class: mock.Mock, slurm_client_class: mock.Mock
    ):
        self.waldur_resource["offering_plugin_options"] = {
            "account_name_generation_policy": "project_slug"
        }
        offering_user_username = "test-offering-user-01"
        project_account = f"hpc_project-1"
        allocation_account = f"{project_account}-1"

        waldur_client = self.setup_waldur_client_mock(waldur_client_class)

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = None
        slurm_client.get_account.side_effect = [None, None, "account", None]
        slurm_client._execute_command.return_value = ""

        process_offerings([OFFERING])

        waldur_client.marketplace_order_approve_by_provider.assert_called_once_with(self.order_uuid)

        self.assertEqual(3, slurm_client.create_account.call_count)

        slurm_client.create_account.assert_called_with(
            name=allocation_account,
            description="test-allocation-01",
            organization=project_account,
        )
        slurm_client.create_association.assert_called_with(
            offering_user_username, allocation_account, "root"
        )


@mock.patch(
    "waldur_site_agent.backends.slurm_backend.backend.SlurmClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.processors.WaldurClient", autospec=True)
class TerminationOrderTest(unittest.TestCase):
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
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "project_slug": "project-1",
            "customer_slug": "customer-1",
        }
        self.waldur_allocation = {
            "backend_id": f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34]
        }

    def test_allocation_deletion(
        self, waldur_client_class: mock.Mock, slurm_client_class: mock.Mock
    ):
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order

        slurm_client = slurm_client_class.return_value
        slurm_client.get_account.return_value = Account(
            name="test-allocation-01",
            description="test-allocation-01",
            organization="hpc_" + self.project_uuid,
        )
        slurm_client.list_accounts.return_value = []
        slurm_client._execute_command.return_value = ""

        process_offerings([OFFERING])

        # The method was called twice: for project account and for allocation account
        self.assertEqual(2, slurm_client.delete_account.call_count)


@mock.patch(
    "waldur_site_agent.backends.slurm_backend.backend.SlurmClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.processors.WaldurClient", autospec=True)
class UpdateOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.backend_id = f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34]
        self.waldur_order = {
            "uuid": uuid.uuid4().hex,
            "marketplace_resource_uuid": self.marketplace_resource_uuid,
            "resource_name": "test-allocation-01",
            "type": "Update",
            "limits": {
                "cpu": 101,
                "mem": 301,
            },
            "attributes": {
                "old_limits": {
                    "cpu": 100,
                    "mem": 300,
                },
                "name": "test-allocation-01",
            },
            "state": "executing",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
        }
        self.waldur_resource = {
            "uuid": self.marketplace_resource_uuid,
            "name": "test-allocation-01",
            "resource_uuid": self.resource_uuid,
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "project_name": "Test project",
            "customer_name": "Test customer",
            "limits": {
                "cpu": 100,
                "mem": 300,
            },
            "state": "Updating",
            "backend_id": self.backend_id,
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
        }
        self.waldur_allocation = {
            "name": "test-allocation-01",
            "backend_id": self.backend_id,
        }

    def test_allocation_limits_update(
        self, waldur_client_class: mock.Mock, slurm_client_class: mock.Mock
    ):
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order
        waldur_client.get_marketplace_resource.return_value = self.waldur_resource

        process_offerings([OFFERING])

        slurm_client = slurm_client_class.return_value
        slurm_client.set_resource_limits.assert_called_once_with(
            self.waldur_allocation["backend_id"],
            {
                "cpu": 101 * 60000,
                "mem": 301 * 61440,
            },
        )
