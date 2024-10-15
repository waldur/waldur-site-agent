import unittest
import uuid
from unittest import mock

from freezegun import freeze_time
from waldur_client import ComponentUsage

from waldur_site_agent import common_utils, MARKETPLACE_SLURM_OFFERING_TYPE
from tests.fixtures import OFFERING
from waldur_site_agent.agent_report import OfferingReportProcessor
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.structures import Resource

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

allocation_slurm = Resource(
    backend_id="test-allocation-01",
    backend_type=BackendType.SLURM.value,
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
@mock.patch("waldur_site_agent.processors.WaldurClient", autospec=True)
@mock.patch.object(common_utils.SlurmBackend, "_pull_allocation", return_value=allocation_slurm)
class ReportingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
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

    def test_usage_reporting(self, _, waldur_client_class: mock.Mock):
        processor = OfferingReportProcessor(self.offering)
        waldur_client = waldur_client_class.return_value

        waldur_client.marketplace_resource_get_plan_periods.return_value = [
            {"uuid": self.plan_period_uuid}
        ]
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
        waldur_client._get_offering.return_value = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        waldur_client.list_remote_offering_users.return_value = [
            {"uuid": "5B0DB04C6FED40A5AB6D511C0E2282C9"}
        ]
        waldur_client.list_component_usages.return_value = [
            {
                "uuid": "23565BD44E5D433F88C1028A2E7AB5F6",
                "type": "cpu",
            },
            {
                "uuid": "ABFCD77BDE254F7485F839397968A12D",
                "type": "mem",
            },
        ]
        processor.process_offering()

        waldur_client.filter_marketplace_provider_resources.assert_called_once_with(
            {
                "offering_uuid": self.offering.uuid,
                "state": "OK",
                "field": ["backend_id", "uuid", "name", "offering_type"],
            }
        )
        waldur_client.create_component_usages.assert_called_once_with(
            self.plan_period_uuid,
            [
                ComponentUsage(type="cpu", amount=10),
                ComponentUsage(type="mem", amount=30),
            ],
        )
        self.assertEqual(2, waldur_client.create_component_user_usage.call_count)
