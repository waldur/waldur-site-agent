import unittest
import uuid
from unittest import mock

from freezegun import freeze_time
from waldur_client import ComponentUsage

from waldur_site_agent import Offering, common_utils
from waldur_site_agent.agent_report import OfferingReportProcessor
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.structures import Resource

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

OFFERING_UUID = "1a6ae60417e04088b90a5aa395209ecc"
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
class TestSlurmReporting(unittest.TestCase):
    def setUp(self) -> None:
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }

        self.plan_period_uuid = uuid.uuid4().hex

    def test_usage_reporting(self, _, waldur_client_class: mock.Mock):
        offering = Offering(
            name="Test offering",
            api_url="https://api.example.com/api/",
            api_token="token",
            uuid=OFFERING_UUID,
            backend_type=BackendType.SLURM.value,
        )
        processor = OfferingReportProcessor(offering)
        waldur_client = waldur_client_class.return_value

        waldur_client.marketplace_resource_get_plan_periods.return_value = [
            {"uuid": self.plan_period_uuid}
        ]
        waldur_client.filter_marketplace_resources.return_value = [self.waldur_resource]
        waldur_client._get_offering.return_value = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        processor.process_offering()

        waldur_client.filter_marketplace_resources.assert_called_once_with(
            {
                "offering_uuid": OFFERING_UUID,
                "state": "OK",
                "field": ["backend_id", "uuid", "name"],
            }
        )
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.waldur_resource["uuid"], allocation_slurm.limits
        )
        waldur_client.create_component_usages.assert_called_once_with(
            self.plan_period_uuid,
            [
                ComponentUsage("cpu", 10),
                ComponentUsage("mem", 30),
            ],
        )
