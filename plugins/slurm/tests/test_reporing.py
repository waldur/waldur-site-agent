import unittest
import uuid
from unittest import mock

import respx
from freezegun import freeze_time
from waldur_api_client.client import AuthenticatedClient
from waldur_site_agent_slurm import backend

from tests.fixtures import OFFERING
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingReportProcessor

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

allocation_slurm = BackendResourceInfo(
    backend_id="test-allocation-01",
    users=["user-01"],
    usage={
        "user-01": {
            "cpu": 10,
            "mem": 30,
        },
        "TOTAL_ACCOUNT_USAGE": {
            "cpu": 10,
            "mem": 30,
        },
    },
    limits={
        "cpu": 100,
        "mem": 300,
    },
)


@freeze_time("2022-01-01")
@mock.patch.object(backend.SlurmBackend, "_pull_backend_resource")
class ReportingTest(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.waldur_resource = {
            "uuid": "10a0f810be1c43bbb651e8cbdbb90198",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "state": "OK",
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        self.offering = OFFERING
        self.plan_period_uuid = uuid.uuid4().hex
        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()

        self.mock_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token=OFFERING.api_token,
            headers={},
        )
        self.mock_get_client.return_value = self.mock_client
        self.allocation_slurm = BackendResourceInfo(
            backend_id="test-allocation-01",
            users=["user-01"],
            usage={
                "user-01": {
                    "cpu": 10,
                    "mem": 30,
                },
                "TOTAL_ACCOUNT_USAGE": {
                    "cpu": 10,
                    "mem": 30,
                },
            },
            limits={
                "cpu": 100,
                "mem": 300,
            },
        )

    def tearDown(self) -> None:
        """Clean up after each test."""
        respx.stop()

    def test_usage_reporting(self, mock_pull_backend_resource) -> None:
        mock_pull_backend_resource.return_value = self.allocation_slurm

        # Set up respx mocks
        respx.get("https://waldur.example.com/api/users/me/").respond(
            200, json={"username": "test-user"}
        )
        respx.get(f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/").respond(
            200, json=self.waldur_offering
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/",
            params={
                "offering_uuid": self.offering.uuid,
                "state": ["OK", "Erred"],
            },
        ).respond(200, json=[self.waldur_resource])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/"
        ).respond(200, json=self.waldur_resource)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-offering-users/",
            params={"user_username": "user-01", "query": OFFERING.uuid},
        ).respond(200, json=[])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/",
            params={"resource_uuid": self.waldur_resource["uuid"], "billing_period": "2022-01-01"},
        ).respond(200, json=[])
        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})
        componet_usage_uuid_cpu = uuid.uuid4()
        componet_usage_uuid_mem = uuid.uuid4()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/",
            params={"resource_uuid": self.waldur_resource["uuid"], "date_after": "2022-01-01"},
        ).respond(
            200,
            json=[
                {
                    "uuid": str(componet_usage_uuid_cpu),
                    "type": "cpu",
                    "usage": "5.0",
                },
                {
                    "uuid": str(componet_usage_uuid_mem),
                    "type": "mem",
                    "usage": "20.0",
                },
            ],
        )
        user_usage_cpu_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/{componet_usage_uuid_cpu}/set_user_usage/"
        ).respond(201, json={})
        user_usage_mem_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/{componet_usage_uuid_mem}/set_user_usage/"
        ).respond(201, json={})
        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/set_as_erred/"
        ).respond(200, json={})

        processor = OfferingReportProcessor(self.offering)
        processor.process_offering()

        assert set_usage_response.call_count == 1
        assert user_usage_mem_response.call_count == 1
        assert user_usage_cpu_response.call_count == 1

    def test_usage_reporting_with_anomaly(self, mock_pull_backend_resource) -> None:
        """Test that the usage reporting is not called when the waldur usage is higher than the incoming usage."""
        mock_pull_backend_resource.return_value = self.allocation_slurm

        respx.get("https://waldur.example.com/api/users/me/").respond(
            200, json={"username": "test-user"}
        )
        respx.get(f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/").respond(
            200, json=self.waldur_offering
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/",
            params={
                "offering_uuid": self.offering.uuid,
                "state": ["OK", "Erred"],
            },
        ).respond(200, json=[self.waldur_resource])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/"
        ).respond(200, json=self.waldur_resource)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-offering-users/",
            params={"user_username": "user-01", "query": OFFERING.uuid},
        ).respond(200, json=[])

        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/",
            params={"resource_uuid": self.waldur_resource["uuid"], "billing_period": "2022-01-01"},
        ).respond(
            200,
            json=[
                {
                    "uuid": "23565BD44E5D433F88C1028A2E7AB5F6",
                    "type": "cpu",
                    "usage": "15.0",
                },
                {
                    "uuid": "ABFCD77BDE254F7485F839397968A12D",
                    "type": "mem",
                    "usage": "20.0",
                },
            ],
        )

        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/",
            params={"resource_uuid": self.waldur_resource["uuid"], "date_after": "2022-01-01"},
        ).respond(200, json=[])

        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/set_as_erred/"
        ).respond(200, json={})

        processor = OfferingReportProcessor(self.offering)
        processor.process_offering()

        assert set_usage_response.call_count == 0
