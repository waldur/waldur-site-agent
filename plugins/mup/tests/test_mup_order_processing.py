import unittest
import uuid

# Create MUP offering fixture
from dataclasses import replace
from unittest import mock

import httpx
import respx
from waldur_api_client.client import AuthenticatedClient

from tests.fixtures import OFFERING
from waldur_site_agent_mup.client import MUPError
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingOrderProcessor

MUP_OFFERING = replace(
    OFFERING,
    backend_type="mup",
    order_processing_backend="mup",
    membership_sync_backend="mup",
    reporting_backend="mup",
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

BASE_URL = "https://waldur.example.com"


class BaseMUPOrderTest(unittest.TestCase):
    """Base class for MUP order processing tests with common setup."""

    def setUp(self) -> None:
        respx.start()
        self.resource_uuid = uuid.uuid4().hex
        self.marketplace_resource_uuid = self.resource_uuid
        self.project_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.uuid4().hex

        self.mock_client = AuthenticatedClient(
            base_url=BASE_URL,
            token=OFFERING.api_token,
            timeout=600,
            headers={},
        )
        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()
        self.mock_get_client.return_value = self.mock_client

        self.waldur_user = {
            "username": "test",
            "email": "test@example.com",
            "full_name": "Test User",
            "is_staff": False,
        }
        self.waldur_offering_response = {
            "uuid": MUP_OFFERING.uuid,
            "name": MUP_OFFERING.name,
            "description": "test description",
        }

    def tearDown(self) -> None:
        respx.stop()
        self.client_patcher.stop()

    def _setup_common_mocks(self) -> None:
        """Setup common respx mocks used across all tests."""
        respx.get(f"{BASE_URL}/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(f"{BASE_URL}/api/marketplace-provider-offerings/{MUP_OFFERING.uuid}/").respond(
            200, json=self.waldur_offering_response
        )

    def _setup_order_mocks(
        self, order_uuid, marketplace_resource_uuid, order_data, order_states=None
    ) -> None:
        """Setup order-specific mocks with flexible state handling."""
        respx.get(
            f"{BASE_URL}/api/marketplace-orders/",
            params={"offering_uuid": MUP_OFFERING.uuid, "state": ["pending-provider", "executing"]},
        ).respond(200, json=[order_data])

        if order_states:
            respx.get(f"{BASE_URL}/api/marketplace-orders/{order_uuid}/").mock(
                side_effect=[httpx.Response(200, json=state) for state in order_states]
            )
        else:
            respx.get(f"{BASE_URL}/api/marketplace-orders/{order_uuid}/").respond(
                200, json=order_data
            )

        respx.post(f"{BASE_URL}/api/marketplace-orders/{order_uuid}/set_state_done/").respond(
            200, json={}
        )
        respx.post(f"{BASE_URL}/api/marketplace-orders/{order_uuid}/set_state_erred/").respond(
            200, json={}
        )

        if order_data.get("state") == "pending-provider":
            respx.post(
                f"{BASE_URL}/api/marketplace-orders/{order_uuid}/approve_by_provider/"
            ).respond(200, json={})

    def _setup_resource_mocks(self, marketplace_resource_uuid, resource_data) -> None:
        """Setup resource-specific mocks."""
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-resources/{marketplace_resource_uuid}/"
        ).respond(200, json=resource_data)
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-resources/{uuid.UUID(marketplace_resource_uuid)}/"
        ).respond(200, json=resource_data)

    def _setup_offering_users_mock(self, offering_users_data) -> None:
        """Setup offering users mock."""
        respx.get(
            f"{BASE_URL}/api/marketplace-offering-users/",
            params={"offering_uuid": MUP_OFFERING.uuid, "is_restricted": False},
        ).respond(200, json=offering_users_data)

        if offering_users_data:
            respx.get(
                f"{BASE_URL}/api/marketplace-provider-offerings/{offering_users_data[0]['offering_uuid']}/"
            ).respond(200, json=self.waldur_offering_response)

    def _setup_resource_team_mock(self, marketplace_resource_uuid, team_data) -> None:
        """Setup resource team mock."""
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-resources/{marketplace_resource_uuid}/team/"
        ).respond(200, json=team_data)

    def _setup_set_backend_id_mock(self, marketplace_resource_uuid):
        """Setup set_backend_id mocks with multiple patterns for flexibility."""
        return respx.post(
            f"{BASE_URL}/api/marketplace-provider-resources/{marketplace_resource_uuid}/set_backend_id/"
        ).respond(200, json={"status": "OK"})


@mock.patch(
    "waldur_site_agent_mup.backend.MUPClient",
    autospec=True,
)
class MUPCreationOrderTest(BaseMUPOrderTest):
    """Test MUP backend with creation orders."""

    def setUp(self) -> None:
        super().setUp()
        self.allocation_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.customer_uuid = uuid.uuid4().hex

        self.user_uuid = uuid.uuid4().hex
        self.waldur_offering_user = {
            "username": "test-offering-user-01",
            "user_uuid": self.user_uuid,
            "offering_uuid": MUP_OFFERING.uuid,
        }
        self.waldur_resource_team = [
            {
                "uuid": self.user_uuid,
                "username": "test-user-01",
                "full_name": "Test User",
                "email": "user1@example.com",
                "role": "test",
                "url": f"{BASE_URL}/api/users/{uuid.uuid4().hex}/",
                "expiration_time": None,
                "offering_user_username": self.waldur_offering_user["username"],
            }
        ]
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
            "uuid": str(self.resource_uuid),
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
            "offering": {"uuid": uuid.uuid4().hex, "name": "MUP Offering"},
        }

    def _setup_order_mocks(
        self, order_uuid, marketplace_resource_uuid, order_data=None, order_states=None
    ) -> None:
        """Setup order-specific mocks for creation orders."""
        pending_order = self.waldur_order.copy()
        executing_order = self.waldur_order.copy()
        executing_order["state"] = "executing"
        executing_order["marketplace_resource_uuid"] = marketplace_resource_uuid

        super()._setup_order_mocks(
            order_uuid,
            marketplace_resource_uuid,
            self.waldur_order,
            [pending_order, executing_order],
        )

    def _setup_resource_mocks(self, marketplace_resource_uuid):
        """Setup resource-specific mocks for creation orders."""
        resource_data = self.waldur_resource.copy()
        resource_data["uuid"] = marketplace_resource_uuid

        super()._setup_resource_mocks(marketplace_resource_uuid, resource_data)
        super()._setup_resource_team_mock(marketplace_resource_uuid, self.waldur_resource_team)

        return super()._setup_set_backend_id_mock(marketplace_resource_uuid)

    def setup_mup_client_mock(self, mup_client_class):
        """Setup MUP client mock."""
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

    def test_mup_resource_creation(self, mup_client_class: mock.Mock) -> None:
        """Test successful MUP resource creation."""
        mup_client = self.setup_mup_client_mock(mup_client_class)

        self._setup_common_mocks()
        marketplace_resource_uuid = uuid.uuid4().hex
        self._setup_order_mocks(self.order_uuid, marketplace_resource_uuid)
        self._setup_resource_mocks(marketplace_resource_uuid)
        self._setup_offering_users_mock([self.waldur_offering_user])
        self._setup_resource_team_mock(marketplace_resource_uuid, self.waldur_resource_team)

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify MUP operations were called
        mup_client.create_user_request.assert_called()  # For the default PI and real users
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()
        mup_client.add_project_member.assert_called()  # Users are added after resource creation

    def test_mup_resource_creation_with_existing_user(self, mup_client_class: mock.Mock) -> None:
        """Test MUP resource creation with existing user."""
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

        self._setup_common_mocks()
        marketplace_resource_uuid = uuid.uuid4().hex
        self._setup_order_mocks(self.order_uuid, marketplace_resource_uuid)
        self._setup_resource_mocks(marketplace_resource_uuid)
        self._setup_offering_users_mock([self.waldur_offering_user])
        self._setup_resource_team_mock(marketplace_resource_uuid, self.waldur_resource_team)

        processor = OfferingOrderProcessor(MUP_OFFERING)
        processor.process_offering()

        # Verify user creation is called for default PI but not for existing user
        mup_client.create_user_request.assert_called()

        # Project and allocation were created
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()

        # Existing user was added to project
        mup_client.add_project_member.assert_called()

    def test_mup_resource_creation_with_existing_project(self, mup_client_class: mock.Mock) -> None:
        """Test MUP resource creation with existing project."""
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Setup all mocks first to get the marketplace_resource_uuid
        self._setup_common_mocks()
        marketplace_resource_uuid = uuid.uuid4().hex
        self._setup_order_mocks(self.order_uuid, str(marketplace_resource_uuid))
        self._setup_resource_mocks(str(marketplace_resource_uuid))
        self._setup_offering_users_mock([self.waldur_offering_user])
        self._setup_resource_team_mock(str(marketplace_resource_uuid), self.waldur_resource_team)

        # Mock existing project (inactive) - use the correct resource UUID
        existing_project = {
            "id": 1,
            "title": "Test project",
            "pi": "pi@example.com",
            "grant_number": f"waldur_{marketplace_resource_uuid}",  # Use the actual resource UUID
            "active": False,
        }

        created_allocation = {
            "id": 1,
            "type": "Deucalion x86_64",
            "identifier": f"alloc_{marketplace_resource_uuid}_cpu",
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

    def test_mup_resource_creation_user_creation_failure(self, mup_client_class: mock.Mock) -> None:
        """Test MUP resource creation when user addition fails."""
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Mock user creation failure for real users (not the default PI)
        def create_user_side_effect(user_data):
            # Allow default PI creation to succeed, fail real users
            if user_data.get("email", "").endswith(".example.com"):
                return {"id": 1}  # Default PI succeeds
            msg = "User creation failed"
            raise MUPError(msg)  # Real user fails

        mup_client.create_user_request.side_effect = create_user_side_effect

        self._setup_common_mocks()
        marketplace_resource_uuid = uuid.uuid4().hex
        self._setup_order_mocks(self.order_uuid, marketplace_resource_uuid)
        self._setup_resource_mocks(marketplace_resource_uuid)
        self._setup_offering_users_mock([self.waldur_offering_user])
        self._setup_resource_team_mock(marketplace_resource_uuid, self.waldur_resource_team)

        processor = OfferingOrderProcessor(MUP_OFFERING)

        # The processor should handle the error gracefully and not crash
        processor.process_offering()

        # Resource creation should have succeeded
        mup_client.create_project.assert_called_once()
        mup_client.create_allocation.assert_called_once()

        # User addition should have been attempted but failed gracefully
        mup_client.create_user_request.assert_called()  # Called for both default PI and real user

    def test_mup_resource_creation_project_creation_failure(
        self, mup_client_class: mock.Mock
    ) -> None:
        """Test MUP resource creation when project creation fails."""
        mup_client = self.setup_mup_client_mock(mup_client_class)

        # Mock project creation failure
        mup_client.create_project.side_effect = MUPError("Project creation failed")

        # Setup all mocks
        self._setup_common_mocks()
        marketplace_resource_uuid = uuid.uuid4().hex
        self._setup_order_mocks(self.order_uuid, marketplace_resource_uuid)
        self._setup_resource_mocks(marketplace_resource_uuid)
        self._setup_offering_users_mock([self.waldur_offering_user])
        self._setup_resource_team_mock(marketplace_resource_uuid, self.waldur_resource_team)

        processor = OfferingOrderProcessor(MUP_OFFERING)

        # The processor should handle the error gracefully and not crash
        processor.process_offering()


@mock.patch(
    "waldur_site_agent_mup.backend.MUPClient",
    autospec=True,
)
class MUPTerminationOrderTest(BaseMUPOrderTest):
    """Test MUP backend with termination orders."""

    def setUp(self) -> None:
        super().setUp()
        self.backend_id = "1"  # Project ID as backend ID

        self.waldur_order = {
            "uuid": self.order_uuid,
            "type": "Terminate",
            "resource_name": "test-allocation-01",
            "project_uuid": self.project_uuid,
            "customer_uuid": uuid.uuid4().hex,
            "offering_uuid": uuid.uuid4().hex,
            "marketplace_resource_uuid": str(self.resource_uuid),
            "project_name": "Test project",
            "customer_name": "Test customer",
            "resource_uuid": str(self.resource_uuid),
            "attributes": {"name": "test-allocation-01"},
            "state": "executing",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "project_slug": "project-1",
            "customer_slug": "customer-1",
        }
        self.waldur_allocation = {"backend_id": "1"}

    def _setup_termination_mocks(self) -> None:
        """Setup mocks for termination order processing."""
        self._setup_common_mocks()

        # Setup order mocks (termination orders are already in executing state)
        self._setup_order_mocks(
            self.order_uuid, str(self.marketplace_resource_uuid), self.waldur_order
        )

        # Setup resource mocks
        resource_data = {
            "uuid": str(self.marketplace_resource_uuid),
            "name": "test-allocation-01",
            "backend_id": "1",
            "state": "OK",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
        }
        self._setup_resource_mocks(str(self.marketplace_resource_uuid), resource_data)

    def test_mup_resource_termination(self, mup_client_class: mock.Mock) -> None:
        """Test MUP resource termination."""
        # Setup all mocks
        self._setup_termination_mocks()

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

        # Verify delete_resource was called (which should deactivate the project)
        mup_client.delete_resource.assert_called()


@mock.patch(
    "waldur_site_agent_mup.backend.MUPClient",
    autospec=True,
)
class MUPUpdateOrderTest(BaseMUPOrderTest):
    """Test MUP backend with update orders."""

    def setUp(self) -> None:
        super().setUp()
        self.backend_id = "1"  # Project ID as backend ID

        self.waldur_order = {
            "uuid": self.order_uuid,
            "marketplace_resource_uuid": str(self.marketplace_resource_uuid),
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
            "uuid": str(self.marketplace_resource_uuid),
            "name": "test-allocation-01",
            "resource_uuid": str(self.resource_uuid),
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

    def test_mup_allocation_limits_update(self, mup_client_class: mock.Mock) -> None:
        """Test MUP allocation limits update."""
        # Setup all mocks
        self._setup_common_mocks()
        self._setup_order_mocks(
            self.order_uuid, str(self.marketplace_resource_uuid), self.waldur_order
        )
        self._setup_resource_mocks(str(self.marketplace_resource_uuid), self.waldur_resource)

        mup_client = mup_client_class.return_value

        # Mock existing project and allocation (using backend_id format)
        existing_project = {
            "id": 1,
            "title": "Test project",
            "grant_number": f"waldur_{self.resource_uuid!s}",  # Use str() for UUID
            "active": True,
        }
        existing_allocation = {
            "id": 1,
            "type": "Deucalion x86_64",  # Updated to match new allocation type mapping
            "identifier": f"alloc_{self.resource_uuid!s}_cpu",  # Use str() for UUID
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

        assert project_id == 1
        assert allocation_id == 1
        assert allocation_data["size"] == 20  # Updated CPU limit


if __name__ == "__main__":
    unittest.main()
