from datetime import datetime
from unittest import mock, TestCase

from waldur_api_client.models import ProjectServiceAccount, Resource, ServiceProvider

from waldur_site_agent.common.processors import OfferingMembershipProcessor
import respx
from waldur_site_agent.common import structures
import uuid
from waldur_site_agent.backend import backends


class ServiceAccountMessageTest(TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.offering = structures.Offering(
            name="test-offering",
            uuid=uuid.uuid4().hex,
            api_url=f"{self.BASE_URL}/api",
            api_token="test_token",
        )
        self.waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="test-alloc-01",
            backend_id="test-allocation-01",
            project_uuid=uuid.uuid4(),
            project_name="Test project",
        )
        self.service_provider = ServiceProvider(uuid=uuid.uuid4())
        self.service_account = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="svc-test-account",
            customer_uuid=uuid.uuid4(),
            customer_name="",
            customer_abbreviation="",
        )

    def _setup_common_mocks(self):
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200,
            json={
                "username": "test",
                "email": "test@example.com",
                "full_name": "Test User",
                "is_staff": False,
            },
        )
        customer_uuid = uuid.uuid4().hex
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, json={"customer_uuid": customer_uuid})
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/?customer_uuid={customer_uuid}"
        ).respond(200, json=[self.service_provider.to_dict()])
        respx.get(
            f"https://waldur.example.com/api/marketplace-provider-resources/?offering_uuid={self.offering.uuid}&project_uuid={self.waldur_resource.project_uuid.hex}&state=OK&state=Erred"
        ).respond(200, json=[self.waldur_resource.to_dict()])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/project_service_accounts/?username={self.service_account.username}"
        ).respond(200, json=[self.service_account.to_dict()])

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_service_account_message_processing(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_service_account_creation(self.service_account.username)

        mock_backend.add_users_to_resource.assert_called_once()
