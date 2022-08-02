import unittest
import uuid
from unittest import mock

from freezegun import freeze_time
from waldur_client import ComponentUsage

from waldur_slurm.slurm_waldur_utils import sync_data_from_slurm_to_waldur


@freeze_time("2022-01-01")
@mock.patch("waldur_slurm.slurm_waldur_utils.waldur_rest_client")
class TestSlurmToWaldurSync(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_report_slurm = {
            "test-allocation-01": {
                "users": ["user-01"],
                "usage": {
                    "user-01": {
                        "cpu": 10,
                        "gpu": 20,
                        "mem": 30,
                    },
                    "TOTAL_ACCOUNT_USAGE": {
                        "cpu": 10,
                        "gpu": 20,
                        "mem": 30,
                    },
                },
                "limits": {
                    "cpu": 100,
                    "gpu": 200,
                    "mem": 300,
                },
            }
        }

        self.allocation_waldur = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "resource_type": "SLURM.Allocation",
            "resource_uuid": "waldur-allocation-uuid",
            "state": "OK",
            "backend_id": "test-allocation-01",
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "gpu"},
                {"type": "mem"},
            ]
        }

    def test_association_and_usage_creation(self, waldur_client: mock.Mock):
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.filter_marketplace_resources.return_value = [
            self.allocation_waldur
        ]
        waldur_client.list_slurm_associations.return_value = []
        plan_period_uuid = uuid.uuid4().hex
        waldur_client.marketplace_resource_get_plan_periods.return_value = [
            {"uuid": plan_period_uuid}
        ]
        waldur_client._get_offering.return_value = {
            "components": [
                {"type": "cpu"},
                {"type": "gpu"},
                {"type": "mem"},
            ]
        }

        sync_data_from_slurm_to_waldur(allocations, self.waldur_offering)

        waldur_client.filter_marketplace_resources.assert_called_once_with(
            {
                "backend_id": "test-allocation-01",
                "offering_uuid": "1a6ae60417e04088b90a5aa395209ecc",
                "state": "OK",
            }
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.allocation_waldur["resource_uuid"]}
        )
        waldur_client.create_slurm_association.assert_called_once_with(
            self.allocation_waldur["uuid"],
            allocation_data["users"][0],
        )
        waldur_client.delete_slurm_association.assert_not_called()
        limits = allocation_data["limits"]
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.allocation_waldur["uuid"], limits
        )
        waldur_client.create_component_usages.assert_called_once_with(
            plan_period_uuid,
            [
                ComponentUsage("cpu", 10),
                ComponentUsage("gpu", 20),
                ComponentUsage("mem", 30),
            ],
        )

    def test_association_deletion(self, waldur_client: mock.Mock):
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.filter_marketplace_resources.return_value = [
            self.allocation_waldur
        ]
        waldur_client.list_slurm_associations.return_value = [
            {"username": "user-01"},
            {"username": "user-02"},
        ]

        sync_data_from_slurm_to_waldur(allocations, self.waldur_offering)

        waldur_client.filter_marketplace_resources.assert_called_once_with(
            {
                "backend_id": "test-allocation-01",
                "offering_uuid": "1a6ae60417e04088b90a5aa395209ecc",
                "state": "OK",
            }
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.allocation_waldur["resource_uuid"]}
        )
        waldur_client.create_slurm_association.assert_not_called()
        waldur_client.delete_slurm_association.assert_called_once_with(
            self.allocation_waldur["uuid"], "user-02"
        )
        limits = allocation_data["limits"]
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.allocation_waldur["uuid"], limits
        )

        waldur_client.set_slurm_allocation_usage.assert_not_called()
