import unittest
import uuid
from unittest import mock
from unittest.mock import Mock, patch

from tests.fixtures import OFFERING
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.backends.mup_backend.client import MUPClient, MUPError
from waldur_site_agent.backends.structures import Account


# Create MUP offering fixture
from dataclasses import replace

MUP_OFFERING = replace(
    OFFERING,
    backend_type="mup",
    backend_settings={
        "api_url": "https://mup-api.example.com/api",
        "username": "test_user",
        "password": "test_password",
        "default_research_field": 1,
        "default_agency": "FCT",
        "project_prefix": "waldur_",
        "allocation_prefix": "alloc_",
        "default_allocation_type": "compute",
        "default_storage_limit": 1000,
    },
    backend_components={
        "cpu": {
            "measured_unit": "core-hours",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "CPU Cores",
            "mup_allocation_type": "Deucalion x86_64",
        }
    },
)


@mock.patch(
    "waldur_site_agent.backends.mup_backend.backend.MUPClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
class MUPCreationOrderTest(unittest.TestCase):
    """Test MUP backend with creation orders"""

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
            "backend_id": "",
            "offering": {"uuid": str(uuid.uuid4()), "name": "MUP Offering"},
        }

    def setup_waldur_client_mock(self, waldur_client_class):
        """Setup Waldur client mock"""
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
                "email": "user1@example.com",
            }
        ]
        waldur_client.list_remote_offering_users.return_value = [
            {"username": "test-offering-user-01", "user_uuid": user_uuid}
        ]
        waldur_client.get_marketplace_provider_resource.return_value = updated_resource

        return waldur_client

    def setup_mup_client_mock(self, mup_client_class):
        """Setup MUP client mock"""
        mup_client = mup_client_class.return_value

        # Mock research fields
        mup_client.get_research_fields.return_value = [{"id": 1, "name": "Computer Science"}]

        # Mock existing users (empty - will create new)
        mup_client.get_users.return_value = []

        # Mock user creation
        mup_client.create_user_request.return_value = {"id": 1}

        # Mock project creation first (empty projects list initially)
        created_project = {
            "id": 1,
            "title": "Test project",
            "pi": f"admin@{self.project_uuid}.example.com",  # Default PI that will be updated
            "grant_number": f"waldur_{self.project_uuid}",
            "active": True,
        }

        created_allocation = {
            "id": 1,
            "type": "compute",
            "identifier": f"alloc_{self.resource_uuid}",
            "size": 10,
            "used": 0,
            "active": True,
            "project": 1,
        }

        # Mock get_projects to return empty initially, then return the created project
        # This simulates the project being created during the process
        mup_client.get_projects.side_effect = [
            [],  # First call - no projects (during create_resource)
            [created_project],  # Second call - project exists (during add_users_to_resource)
            [created_project],  # Additional calls as needed
        ]

        # Mock project and allocation creation
        mup_client.create_project.return_value = created_project
        mup_client.create_allocation.return_value = created_allocation

        # Mock allocation lookup for user addition
        mup_client.get_project_allocations.return_value = [created_allocation]

        # Mock member operations
        mup_client.add_project_member.return_value = {"status": "success"}

        return mup_client

    def test_mup_resource_creation(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test successful MUP resource creation"""
        waldur_client = self.setup_waldur_client_mock(waldur_client_class)
        mup_client = self.setup_mup_client_mock(mup_client_class)

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify Waldur order was approved
        waldur_client.marketplace_order_approve_by_provider.assert_called_once_with(self.order_uuid)

        # Verify MUP operations were called
        # Note: get_users is no longer called during resource creation (only when adding real users)
        mup_client.create_user_request.assert_called()  # For the default PI and real users
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()
        mup_client.add_project_member.assert_called()  # Users are added after resource creation

    def test_mup_resource_creation_with_existing_user(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP resource creation with existing user"""
        waldur_client = self.setup_waldur_client_mock(waldur_client_class)
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Override the side_effect to simulate existing user for offering user
        def get_users_side_effect():
            return [
                {"id": 2, "email": "test-offering-user-01", "username": "test-offering-user-01"}
            ]

        mup_client.get_users.side_effect = [
            [],  # First call during create_resource (no users)
            get_users_side_effect(),  # Second call during add_users_to_resource (user exists)
        ]

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify user creation is called for default PI but not for existing user
        mup_client.create_user_request.assert_called()

        # Project and allocation were created
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()

        # Existing user was added to project
        mup_client.add_project_member.assert_called()

    def test_mup_resource_creation_with_existing_project(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP resource creation with existing project"""
        waldur_client = self.setup_waldur_client_mock(waldur_client_class)
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Mock existing project (inactive)
        existing_project = {
            "id": 1,
            "title": "Test project",
            "pi": "pi@example.com",
            "grant_number": f"waldur_{self.resource_uuid}",
            "active": False,
        }

        created_allocation = {
            "id": 1,
            "type": "Deucalion x86_64",
            "identifier": f"alloc_{self.resource_uuid}_cpu",
            "size": 10,
            "used": 0,
            "active": True,
            "project": 1,
        }

        # Mock get_projects to return existing project
        mup_client.get_projects.side_effect = [
            [existing_project],  # First call - project exists (during create_resource)
            [existing_project],  # Second call - same project (during add_users_to_resource)
        ]

        mup_client.activate_project.return_value = {"status": "activated"}
        mup_client.create_allocation.return_value = created_allocation
        mup_client.get_project_allocations.return_value = [created_allocation]

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify project was not created (already exists) but was activated
        mup_client.create_project.assert_not_called()
        mup_client.activate_project.assert_called_once_with(1)

        # Allocation was still created
        mup_client.create_allocation.assert_called_once()

    def test_mup_resource_creation_user_creation_failure(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP resource creation when user addition fails"""
        waldur_client = self.setup_waldur_client_mock(waldur_client_class)
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Mock user creation failure for real users (not the default PI)
        def create_user_side_effect(user_data):
            # Allow default PI creation to succeed, fail real users
            if user_data.get("email", "").endswith(".example.com"):
                return {"id": 1}  # Default PI succeeds
            else:
                raise MUPError("User creation failed")  # Real user fails

        mup_client.create_user_request.side_effect = create_user_side_effect

        processor = OfferingOrderProcessor(MUP_OFFERING)

        # The processor should handle the error gracefully and not crash
        processor.process_offering()

        # Verify the order succeeded (resource was created) even though user addition failed
        waldur_client.marketplace_order_approve_by_provider.assert_called_once_with(self.order_uuid)

        # Resource creation should have succeeded
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()

        # User addition should have been attempted but failed gracefully
        mup_client.create_user_request.assert_called()  # Called for both default PI and real user

    def test_mup_resource_creation_project_creation_failure(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP resource creation when project creation fails"""
        waldur_client = self.setup_waldur_client_mock(waldur_client_class)
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Mock project creation failure
        mup_client.create_project.side_effect = MUPError("Project creation failed")

        processor = OfferingOrderProcessor(MUP_OFFERING)

        # The processor should handle the error gracefully and not crash
        processor.process_offering()

        # Verify the order was attempted but failed
        waldur_client.marketplace_order_set_state_erred.assert_called()


@mock.patch(
    "waldur_site_agent.backends.mup_backend.backend.MUPClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
class MUPTerminationOrderTest(unittest.TestCase):
    """Test MUP backend with termination orders"""

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

        self.waldur_allocation = {"backend_id": "1"}

    def test_mup_resource_termination(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP resource termination"""
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order

        mup_client = mup_client_class.return_value

        # Mock existing project with allocation
        existing_project = {
            "id": 1,
            "title": "Test project",
            "grant_number": f"waldur_{self.resource_uuid}",  # Original grant_number format
            "active": True,
        }
        mup_client.get_projects.return_value = [existing_project]
        mup_client.deactivate_project.return_value = {"status": "deactivated"}

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify delete_account was called (which should deactivate the project)
        mup_client.delete_account.assert_called()


@mock.patch(
    "waldur_site_agent.backends.mup_backend.backend.MUPClient",
    autospec=True,
)
@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
class MUPUpdateOrderTest(unittest.TestCase):
    """Test MUP backend with update orders"""

    def setUp(self) -> None:
        self.marketplace_resource_uuid = uuid.uuid4().hex
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.backend_id = "1"  # Project ID as backend ID

        self.waldur_order = {
            "uuid": uuid.uuid4().hex,
            "marketplace_resource_uuid": self.marketplace_resource_uuid,
            "resource_name": "test-allocation-01",
            "type": "Update",
            "limits": {"cpu": 20},
            "attributes": {
                "old_limits": {"cpu": 10},
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
            "limits": {"cpu": 10},
            "state": "Updating",
            "backend_id": self.backend_id,
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
        }

        self.waldur_allocation = {
            "name": "test-allocation-01",
            "backend_id": self.backend_id,
        }

    def test_mup_allocation_limits_update(
        self, waldur_client_class: mock.Mock, mup_client_class: mock.Mock
    ):
        """Test MUP allocation limits update"""
        waldur_client = waldur_client_class.return_value
        waldur_client.list_orders.return_value = [self.waldur_order]
        waldur_client.get_slurm_allocation.return_value = self.waldur_allocation
        waldur_client.get_order.return_value = self.waldur_order
        waldur_client.get_marketplace_provider_resource.return_value = self.waldur_resource

        mup_client = mup_client_class.return_value

        # Mock existing project and allocation (using backend_id format)
        existing_project = {
            "id": 1,
            "title": "Test project",
            "grant_number": f"waldur_{self.resource_uuid}",  # Original grant_number format
            "active": True,
        }
        existing_allocation = {
            "id": 1,
            "type": "Deucalion x86_64",  # Updated to match new allocation type mapping
            "identifier": f"alloc_{self.resource_uuid}_cpu",  # Allocation identifier format
            "size": 10,
            "used": 0,
            "active": True,
            "project": 1,
        }

        mup_client.get_projects.return_value = [existing_project]
        mup_client.get_project_allocations.return_value = [existing_allocation]
        mup_client.update_allocation.return_value = existing_allocation

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify allocation was updated with new limits
        mup_client.update_allocation.assert_called_once()
        call_args = mup_client.update_allocation.call_args
        project_id, allocation_id, allocation_data = call_args[0]

        self.assertEqual(project_id, 1)
        self.assertEqual(allocation_id, 1)
        self.assertEqual(allocation_data["size"], 20)  # Updated CPU limit


if __name__ == "__main__":
    unittest.main()
