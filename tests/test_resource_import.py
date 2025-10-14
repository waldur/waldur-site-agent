from unittest import mock, TestCase
from waldur_site_agent.common import structures
from waldur_site_agent.backend import structures as backend_structures
from waldur_site_agent.common.processors import OfferingImportableResourcesProcessor
from waldur_site_agent.backend import backends
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.backend_resource_req import BackendResourceReq
from waldur_api_client.models.resource import Resource
from waldur_api_client.models.project import Project
from waldur_api_client.models.backend_resource import BackendResource
from waldur_api_client.models.backend_resource_req_state_enum import (
    BackendResourceReqStateEnum,
)
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.service_provider import ServiceProvider
import datetime

import uuid

import respx
import httpx


class BackendResourceRequestTest(TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self):
        respx.start()
        self.project_prefix = "w-"
        self.project_slug = "test-project"
        self.offering = structures.Offering(
            name="test-offering",
            uuid=uuid.uuid4().hex,
            backend_settings={"project_prefix": self.project_prefix},
            api_url=f"{self.BASE_URL}/api",
            api_token="test_token",
        )
        self.waldur_rest_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token="test_token",
        )
        self.project_account = f"{self.project_prefix}{self.project_slug}"
        self.request_uuid = uuid.uuid4().hex
        self.backend_resource_request = BackendResourceReq(
            url="",
            uuid=self.request_uuid,
            created=datetime.datetime.now(),
            modified=datetime.datetime.now(),
            started=None,
            finished=None,
            state=BackendResourceReqStateEnum.SENT,
            offering=self.offering.uuid,
            offering_name=self.offering.name,
            offering_url="",
            error_message="",
            error_traceback="",
        )

    def tearDown(self) -> None:
        respx.stop()

    def mock_waldur_client(self):
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200,
            json={
                "username": "test",
                "email": "test@example.com",
                "full_name": "Test User",
                "is_staff": False,
            },
        )
        customer_uuid = uuid.uuid4()
        offering_details = ProviderOfferingDetails(
            uuid=uuid.UUID(self.offering.uuid),
            customer_uuid=customer_uuid,
            components=[],
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, json=offering_details.to_dict())
        service_provider = ServiceProvider(
            uuid=uuid.uuid4(),
            customer_uuid=customer_uuid,
        )
        respx.get(f"{self.BASE_URL}/api/marketplace-service-providers/").respond(
            200, json=[service_provider.to_dict()]
        )
        resources = [Resource(backend_id="test-backend-id-00").to_dict()]
        respx.get(f"{self.BASE_URL}/api/backend-resource-requests/{self.request_uuid}/").respond(
            200, json=self.backend_resource_request.to_dict()
        )
        respx.post(
            f"{self.BASE_URL}/api/backend-resource-requests/{self.request_uuid}/start_processing/"
        ).respond(200, json={})
        respx.post(
            f"{self.BASE_URL}/api/backend-resource-requests/{self.request_uuid}/set_done/"
        ).respond(200, json={})
        respx.get(f"{self.BASE_URL}/api/marketplace-provider-resources/").respond(
            200, json=resources
        )
        projects = [Project(slug=self.project_slug, uuid=uuid.uuid4()).to_dict()]
        respx.get(f"{self.BASE_URL}/api/projects/").respond(200, json=projects)
        backend_resources = [
            httpx.Response(
                200,
                json=[
                    BackendResource(
                        url="",
                        uuid=uuid.uuid4().hex,
                        backend_id="test-backend-id-00",
                        name="existing-backend-resource",
                        created=datetime.datetime.now(),
                        modified=datetime.datetime.now(),
                        project=uuid.uuid4().hex,
                        project_name="test-project",
                        project_url="",
                        offering=self.offering.uuid,
                        offering_name=self.offering.name,
                        offering_url="",
                    ).to_dict()
                ],
            ),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
        ]
        respx.get(f"{self.BASE_URL}/api/backend-resources/").mock(side_effect=backend_resources)
        respx.post(f"{self.BASE_URL}/api/backend-resources/").respond(
            201,
            json=BackendResource(
                url="",
                uuid=uuid.uuid4().hex,
                backend_id="test-backend-id-01",
                name="existing-backend-resource",
                created=datetime.datetime.now(),
                modified=datetime.datetime.now(),
                project=uuid.uuid4().hex,
                project_name="test-project",
                project_url="",
                offering=self.offering.uuid,
                offering_name=self.offering.name,
                offering_url="",
            ).to_dict(),
        )

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_process_backend_resource_requests_creates_request(
        self,
        mock_get_backend,
    ):
        local_resources = [
            backend_structures.BackendResourceInfo(
                backend_id="test-resource-id-00",
                parent_id=self.project_account,
            ),
            backend_structures.BackendResourceInfo(
                backend_id="test-resource-id-01",
                parent_id=self.project_account,
            ),
            backend_structures.BackendResourceInfo(
                backend_id="test-resource-id-02",
                parent_id=self.project_slug,
            ),
        ]

        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.list_resources = mock.Mock(return_value=local_resources)
        mock_backend.get_resource_limits = mock.Mock(return_value={})
        mock_get_backend.return_value = (mock_backend, "1.0.0")

        self.mock_waldur_client()

        processor = OfferingImportableResourcesProcessor(self.offering, self.waldur_rest_client)
        processor.process_request(self.request_uuid)

        mock_backend.list_resources.assert_called_once()
        mock_backend.get_resource_limits.assert_called_once()
