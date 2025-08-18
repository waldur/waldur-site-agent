import unittest
import uuid
from datetime import datetime, timezone
from typing import Optional
from unittest import mock

import respx
from waldur_api_client import AuthenticatedClient, models
from waldur_api_client.models import ResourceState
from waldur_api_client.models.merged_plugin_options import MergedPluginOptions
from waldur_api_client.models.offering_state import OfferingState
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_api_client.models.username_generation_policy_enum import UsernameGenerationPolicyEnum
from waldur_api_client.models.offering_user_state_enum import OfferingUserStateEnum

from waldur_site_agent.backend.structures import ClientResource
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingOrderProcessor
from tests.fixtures import OFFERING


def setup_common_respx_mocks(
    base_url: str,
    waldur_user: dict,
    waldur_offering: dict,
    waldur_resource: dict,
    waldur_resource_team: Optional[list] = None,
    waldur_offering_users: Optional[dict] = None,
) -> None:
    """Setup common respx mocks for order processing tests."""
    respx.get(f"{base_url}/api/users/me/").respond(200, json=waldur_user)
    respx.get(f"{base_url}/api/marketplace-provider-offerings/{OFFERING.uuid}/").respond(
        200, json=waldur_offering
    )
    respx.get(f"{base_url}/api/marketplace-provider-resources/{waldur_resource['uuid']}/").respond(
        200, json=waldur_resource
    )

    if waldur_resource_team is not None:
        respx.get(
            f"{base_url}/api/marketplace-provider-resources/{waldur_resource['uuid']}/team/"
        ).respond(200, json=waldur_resource_team)

    if waldur_offering_users is not None:
        respx.get(
            f"{base_url}/api/marketplace-offering-users/", params={"offering_uuid": OFFERING.uuid}
        ).respond(200, json=[waldur_offering_users])

    respx.post(
        f"{base_url}/api/marketplace-provider-resources/{waldur_resource['uuid']}/set_backend_id/"
    ).respond(200, json={"status": "OK"})
    if waldur_offering_users is not None:
        respx.get(
            f"{base_url}/api/marketplace-provider-offerings/{waldur_offering_users['offering_uuid']}/"
        ).respond(200, json=waldur_offering)


def setup_order_respx_mocks(base_url: str, order_uuid: str, waldur_order: dict):
    """Setup respx mocks for order-specific endpoints."""
    order_copy = waldur_order.copy()
    respx.get(
        f"{base_url}/api/marketplace-orders/",
        params={"offering_uuid": OFFERING.uuid, "state": ["pending-provider", "executing"]},
    ).respond(200, json=[order_copy])
    respx.get(f"{base_url}/api/marketplace-orders/{order_uuid}/").respond(200, json=order_copy)
    respx.post(f"{base_url}/api/marketplace-orders/{order_uuid}/approve_by_provider/").respond(
        200, json={}
    )
    respx.post(f"{base_url}/api/marketplace-orders/{order_uuid}/set_state_done/").respond(
        200, json={}
    )
    respx.post(
        f"{base_url}/api/marketplace-orders/{uuid.UUID(order_uuid)}/set_state_erred/"
    ).respond(200, json={})
    respx.get(f"{base_url}/api/marketplace-orders/{uuid.UUID(order_uuid)}/").respond(
        200, json=order_copy
    )
    respx.post(
        f"{base_url}/api/marketplace-orders/{uuid.UUID(order_uuid)}/approve_by_provider/"
    ).respond(200, json={})
    respx.post(
        f"{base_url}/api/marketplace-orders/{uuid.UUID(order_uuid)}/set_state_done/"
    ).respond(200, json={})
    return respx.post(
        f"{base_url}/api/marketplace-orders/{uuid.UUID(order_uuid)}/set_state_erred/"
    ).respond(200, json={})


def setup_slurm_client_mocks(slurm_client_class: mock.Mock, get_resource_side_effect=None):
    """Setup common SLURM client mocks."""
    slurm_client = slurm_client_class.return_value
    slurm_client.get_association.return_value = None
    slurm_client._execute_command.return_value = ""

    if get_resource_side_effect:
        slurm_client.get_resource.side_effect = get_resource_side_effect
    else:
        slurm_client.get_resource.return_value = None

    return slurm_client


@mock.patch(
    "waldur_site_agent_slurm.backend.SlurmClient",
    autospec=True,
)
class CreationOrderTest(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.allocation_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.customer_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.UUID("2c76f6ea-3482-4cb9-a975-ae0235ba4ac7").hex
        self.resource_uuid = uuid.uuid4().hex
        self.waldur_order = models.OrderDetails(
            uuid=self.order_uuid,
            type_=RequestTypes.CREATE,
            resource_name="test-allocation-01",
            project_slug="project-1",
            customer_slug="customer-1",
        ).to_dict()
        self.waldur_user = models.User(
            uuid=uuid.uuid4(),
            username="test-user",
            date_joined=datetime(2024, 1, 1, tzinfo=timezone.utc),
            email="test@example.com",
            full_name="Test User",
        ).to_dict()

        self.waldur_offering = models.Offering(
            uuid=OFFERING.uuid,
            name=OFFERING.name,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingState.ACTIVE,
            type_=MARKETPLACE_SLURM_OFFERING_TYPE,
            plugin_options=MergedPluginOptions(
                username_generation_policy=UsernameGenerationPolicyEnum.SERVICE_PROVIDER,
            ),
        ).to_dict()

        self.waldur_resource = models.Resource(
            uuid=self.resource_uuid,
            name="sample-resource-1",
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            project_slug="project-1",
            customer_slug="customer-1",
            customer_name="test-allocation-01",
            resource_type="Slurm",
            resource_uuid=self.allocation_uuid,
            project_uuid=self.project_uuid,
            slug="sample-resource-1",
            state=ResourceState.CREATING,
            offering_plugin_options={
                "account_name_generation_policy": "resource_name",
                "account_name_prefix": "hpc_",
            },
            limits=ResourceLimits.from_dict(
                {
                    "cpu": 10,
                    "mem": 20,
                }
            ),
        ).to_dict()
        self.team_member = models.ProjectUser(
            uuid=uuid.uuid4(),
            username="test-offering-user-01",
            url="https://waldur.example.com/api/users/test-offering-user-01/",
            full_name="Test Offering User 01",
            role="admin",
            expiration_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            offering_user_username="test-offering-user-01",
        ).to_dict()
        self.waldur_resource_team = [self.team_member]
        self.waldur_offering_user = models.OfferingUser(
            username="test-offering-user-01",
            user_uuid=self.team_member["uuid"],
            offering_uuid=OFFERING.uuid,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingUserStateEnum.OK,
        ).to_dict()
        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()

        self.mock_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token=OFFERING.api_token,
            headers={},
        )
        self.mock_get_client.return_value = self.mock_client

    def tearDown(self) -> None:
        respx.stop()

    def test_allocation_creation(self, slurm_client_class: mock.Mock) -> None:
        allocation_account = "hpc_sample-resource-1"
        project_account = "hpc_project-1"
        self.waldur_order.update(
            {
                "uuid": self.order_uuid,
                "state": "pending-provider",
                "type": "Create",
                "resource_name": "test-allocation-01",
                "project_slug": "project-1",
                "customer_slug": "customer-1",
                "marketplace_resource_uuid": str(self.resource_uuid),
            }
        )
        # Setup common mocks
        setup_common_respx_mocks(
            self.BASE_URL,
            self.waldur_user,
            self.waldur_offering,
            self.waldur_resource,
            self.waldur_resource_team,
            self.waldur_offering_user,
        )
        request_order_set_as_error = setup_order_respx_mocks(
            self.BASE_URL, self.order_uuid, self.waldur_order
        )
        slurm_client = setup_slurm_client_mocks(slurm_client_class)

        processor = OfferingOrderProcessor(OFFERING)
        processor.process_offering()

        assert request_order_set_as_error.call_count == 0
        assert slurm_client.create_resource.call_count == 3

        slurm_client.create_resource.assert_called_with(
            name=allocation_account,
            description=self.waldur_resource["name"],
            organization=project_account,
            parent_name=project_account,
        )
        slurm_client.create_association.assert_called_with(
            self.waldur_offering_user["username"], allocation_account, "root"
        )

    def test_allocation_creation_with_project_slug(self, slurm_client_class: mock.Mock) -> None:
        self.waldur_order.update(
            {
                "uuid": self.order_uuid,
                "state": "pending-provider",
                "type": "Create",
                "resource_name": "test-allocation-01",
                "project_slug": "project-1",
                "customer_slug": "customer-1",
                "marketplace_resource_uuid": str(self.resource_uuid),
            }
        )
        self.waldur_resource["offering_plugin_options"] = {
            "account_name_generation_policy": "project_slug"
        }
        offering_user_username = "test-offering-user-01"

        project_account = "hpc_project-1"
        allocation_account = f"{project_account}-0"

        # Setup common mocks
        setup_common_respx_mocks(
            self.BASE_URL,
            self.waldur_user,
            self.waldur_offering,
            self.waldur_resource,
            self.waldur_resource_team,
            self.waldur_offering_user,
        )
        setup_order_respx_mocks(self.BASE_URL, self.order_uuid, self.waldur_order)
        slurm_client = setup_slurm_client_mocks(
            slurm_client_class, [None, None, None, "account", None]
        )

        processor = OfferingOrderProcessor(OFFERING)
        processor.process_offering()

        assert slurm_client.create_resource.call_count == 3

        slurm_client.create_resource.assert_called_with(
            name=allocation_account,
            description="sample-resource-1",
            organization=project_account,
            parent_name=project_account,
        )
        slurm_client.create_association.assert_called_with(
            offering_user_username, allocation_account, "root"
        )

    def test_allocation_creation_with_empty_customer_slug(
        self, slurm_client_class: mock.Mock
    ) -> None:
        """Test allocation creation failure with empty customer_slug."""

        self.waldur_order.update(
            {
                "uuid": self.order_uuid,
                "state": "pending-provider",
                "type": "Create",
                "resource_name": "test-allocation-01",
                "customer_slug": "customer-1",
                "project_slug": "project-1",
                "marketplace_resource_uuid": str(self.resource_uuid),
            }
        )

        self.waldur_resource["customer_slug"] = None

        setup_common_respx_mocks(
            self.BASE_URL,
            self.waldur_user,
            self.waldur_offering,
            self.waldur_resource,
            self.waldur_resource_team,
            self.waldur_offering_user,
        )
        request_order_set_as_error = setup_order_respx_mocks(
            self.BASE_URL, self.order_uuid, self.waldur_order
        )
        slurm_client = setup_slurm_client_mocks(slurm_client_class)

        processor = OfferingOrderProcessor(OFFERING)

        processor.process_offering()

        assert request_order_set_as_error.call_count == 1
        error_request = request_order_set_as_error.calls[0].request
        assert "unset or missing slug fields" in error_request.content.decode()

    def test_allocation_creation_with_empty_project_slug(
        self, slurm_client_class: mock.Mock
    ) -> None:
        """Test allocation creation failure with empty project_slug."""

        self.waldur_order.update(
            {
                "uuid": self.order_uuid,
                "state": "pending-provider",
                "type": "Create",
                "resource_name": "test-allocation-01",
                "customer_slug": "customer-1",
                "project_slug": "project-1",
                "marketplace_resource_uuid": str(self.resource_uuid),
            }
        )

        self.waldur_resource["project_slug"] = ""

        setup_common_respx_mocks(
            self.BASE_URL,
            self.waldur_user,
            self.waldur_offering,
            self.waldur_resource,
            self.waldur_resource_team,
            self.waldur_offering_user,
        )
        request_order_set_as_error = setup_order_respx_mocks(
            self.BASE_URL, self.order_uuid, self.waldur_order
        )
        slurm_client = setup_slurm_client_mocks(slurm_client_class)

        processor = OfferingOrderProcessor(OFFERING)

        # Process offering - should catch BackendError and mark order as erred
        processor.process_offering()

        # Check that the order was marked as erred due to empty project_slug
        assert request_order_set_as_error.call_count == 1
        error_request = request_order_set_as_error.calls[0].request
        assert "unset or missing slug fields" in error_request.content.decode()


@mock.patch(
    "waldur_site_agent_slurm.backend.SlurmClient",
    autospec=True,
)
class TerminationOrderTest(unittest.TestCase):
    base_url = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.marketplace_resource_uuid = str(uuid.uuid4())
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.uuid4()

        # Create order data
        self.waldur_order = models.OrderDetails(
            uuid=self.order_uuid,
            type_=RequestTypes.TERMINATE,
            resource_name="test-allocation-01",
            project_uuid=uuid.UUID(self.project_uuid),
            customer_uuid=uuid.uuid4(),
            offering_uuid=OFFERING.uuid,
            marketplace_resource_uuid=self.marketplace_resource_uuid,
            project_name="Test project",
            customer_name="Test customer",
            resource_uuid=self.resource_uuid,
            state=OrderState.EXECUTING,
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            project_slug="project-1",
            customer_slug="customer-1",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).to_dict()

        # Create resource data
        self.waldur_resource = models.Resource(
            uuid=self.marketplace_resource_uuid,
            name="test-allocation-01",
            backend_id=f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34],
            resource_uuid=uuid.UUID(self.resource_uuid),
            project_uuid=uuid.UUID(self.project_uuid),
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            state=ResourceState.OK,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).to_dict()

        # Create user data
        self.waldur_user = models.User(
            uuid=uuid.uuid4(),
            username="test-user",
            date_joined=datetime(2024, 1, 1, tzinfo=timezone.utc),
            email="test@example.com",
            full_name="Test User",
            is_staff=False,
        ).to_dict()

        # Create offering data
        self.waldur_offering = models.Offering(
            uuid=OFFERING.uuid,
            name=OFFERING.name,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingState.ACTIVE,
            type_=MARKETPLACE_SLURM_OFFERING_TYPE,
            plugin_options=MergedPluginOptions(
                username_generation_policy=UsernameGenerationPolicyEnum.SERVICE_PROVIDER,
            ),
        ).to_dict()

        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()

        self.mock_client = AuthenticatedClient(
            base_url=self.base_url,
            token=OFFERING.api_token,
            timeout=600,
            headers={},
        )
        self.mock_get_client.return_value = self.mock_client

    def tearDown(self) -> None:
        respx.stop()
        self.client_patcher.stop()

    def test_allocation_deletion(self, slurm_client_class: mock.Mock) -> None:
        setup_common_respx_mocks(
            self.base_url,
            self.waldur_user,
            self.waldur_offering,
            self.waldur_resource,
        )
        respx.post(
            f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/set_state_done/"
        ).respond(200, json={})
        erred_response = respx.post(
            f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/set_state_erred/"
        ).respond(200, json={})

        # Setup order-specific mocks
        respx.get(
            f"{self.base_url}/api/marketplace-orders/",
            params={"offering_uuid": OFFERING.uuid, "state": ["pending-provider", "executing"]},
        ).respond(200, json=[self.waldur_order])
        respx.get(f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/").respond(
            200, json=self.waldur_order
        )
        respx.get(f"{self.base_url}/api/marketplace-orders/{self.order_uuid.hex}/").respond(
            200, json=self.waldur_order
        )

        slurm_client = slurm_client_class.return_value
        slurm_client.get_resource.return_value = ClientResource(
            name="test-allocation-01",
            description="test-allocation-01",
            organization="hpc_project-1",
        )
        slurm_client.list_resources.return_value = []
        slurm_client._execute_command.return_value = ""

        processor = OfferingOrderProcessor(OFFERING)
        processor.process_offering()
        assert erred_response.call_count == 0
        # The method was called twice: for project account and for allocation account
        assert slurm_client.delete_resource.call_count == 2


@mock.patch(
    "waldur_site_agent_slurm.backend.SlurmClient",
    autospec=True,
)
class UpdateOrderTest(unittest.TestCase):
    base_url = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.marketplace_resource_uuid = str(uuid.uuid4())
        self.resource_uuid = uuid.uuid4().hex
        self.project_uuid = uuid.uuid4().hex
        self.order_uuid = uuid.uuid4()
        self.backend_id = f"hpc_{self.resource_uuid[:5]}_test-allocation-01"[:34]

        # Create order data
        self.waldur_order = models.OrderDetails(
            uuid=self.order_uuid,
            marketplace_resource_uuid=self.marketplace_resource_uuid,
            resource_name="test-allocation-01",
            type_=RequestTypes.UPDATE,
            limits=ResourceLimits.from_dict(
                {
                    "cpu": 101,
                    "mem": 301,
                }
            ),
            attributes={
                "old_limits": {
                    "cpu": 100,
                    "mem": 300,
                },
                "name": "test-allocation-01",
            },
            state=OrderState.EXECUTING,
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).to_dict()

        # Create resource data
        self.waldur_resource = models.Resource(
            uuid=self.marketplace_resource_uuid,
            name="test-allocation-01",
            backend_id=self.backend_id,
            resource_uuid=uuid.UUID(self.resource_uuid),
            project_uuid=uuid.UUID(self.project_uuid),
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            state=ResourceState.UPDATING,
            limits=ResourceLimits.from_dict(
                {
                    "cpu": 100,
                    "mem": 300,
                }
            ),
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).to_dict()

        # Create user data
        self.waldur_user = models.User(
            uuid=uuid.uuid4(),
            username="test-user",
            date_joined=datetime(2024, 1, 1, tzinfo=timezone.utc),
            email="test@example.com",
            full_name="Test User",
            is_staff=False,
        ).to_dict()

        # Create offering data
        self.waldur_offering = models.Offering(
            uuid=OFFERING.uuid,
            name=OFFERING.name,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingState.ACTIVE,
            type_=MARKETPLACE_SLURM_OFFERING_TYPE,
            plugin_options=MergedPluginOptions(
                username_generation_policy=UsernameGenerationPolicyEnum.SERVICE_PROVIDER,
            ),
        ).to_dict()

        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()

        self.mock_client = AuthenticatedClient(
            base_url=self.base_url,
            token=OFFERING.api_token,
            timeout=600,
            headers={},
        )
        self.mock_get_client.return_value = self.mock_client

    def tearDown(self) -> None:
        respx.stop()
        self.client_patcher.stop()

    def test_allocation_limits_update(self, slurm_client_class: mock.Mock) -> None:
        respx.get(f"{self.base_url}/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(f"{self.base_url}/api/marketplace-provider-offerings/{OFFERING.uuid}/").respond(
            200, json=self.waldur_offering
        )
        respx.get(
            f"{self.base_url}/api/marketplace-orders/",
            params={"offering_uuid": OFFERING.uuid, "state": ["pending-provider", "executing"]},
        ).respond(200, json=[self.waldur_order])
        respx.get(f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/").respond(
            200, json=self.waldur_order
        )
        respx.get(f"{self.base_url}/api/marketplace-orders/{self.order_uuid.hex}/").respond(
            200, json=self.waldur_order
        )
        respx.get(
            f"{self.base_url}/api/marketplace-provider-resources/{self.waldur_order['marketplace_resource_uuid']}/"
        ).respond(200, json=self.waldur_resource)
        respx.post(
            f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/set_state_done/"
        ).respond(200, json={})
        erred_response = respx.post(
            f"{self.base_url}/api/marketplace-orders/{self.order_uuid}/set_state_erred/"
        ).respond(200, json={})

        slurm_client = slurm_client_class.return_value
        slurm_client.set_resource_limits = mock.Mock()

        processor = OfferingOrderProcessor(OFFERING)
        processor.process_offering()
        assert slurm_client.set_resource_limits.call_count == 1
        assert erred_response.call_count == 0
        slurm_client.set_resource_limits.assert_called_once_with(
            self.backend_id,
            {
                "cpu": 101 * 60000,
                "mem": 301 * 61440,
            },
        )
