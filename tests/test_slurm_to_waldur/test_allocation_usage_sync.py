import unittest
import uuid
from unittest import mock

from freezegun import freeze_time

from waldur_slurm.slurm_client.structures import Quotas
from waldur_slurm.slurm_waldur_utils import sync_data_from_slurm_to_waldur


@freeze_time("2022-01-01")
@mock.patch("waldur_slurm.slurm_waldur_utils.waldur_rest_client")
class TestSlurmToWaldurSync(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_report_slurm = {
            "test-allocation-01": {
                "users": ["user-01"],
                "usage": {
                    "user-01": Quotas(
                        cpu=10,
                        gpu=20,
                        ram=30,
                    ),
                    "TOTAL_ACCOUNT_USAGE": Quotas(
                        cpu=10,
                        gpu=20,
                        ram=30,
                    ),
                },
                "limits": Quotas(
                    cpu=100,
                    gpu=200,
                    ram=300,
                ),
            }
        }

        self.allocation_waldur = {
            "uuid": "waldur-alloc-uuid",
            "name": "test-alloc-01",
            "resource_type": "SLURM.Allocation",
            "state": "OK",
            "backend_id": "test-allocation-01",
            "cpu_limit": 1,
            "cpu_usage": 0,
            "gpu_limit": 2,
            "gpu_usage": 0,
            "ram_limit": 3,
            "ram_usage": 0,
            "marketplace_resource_uuid": "waldur-resource-uuid",
            "marketplace_resource_state": "OK",
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.waldur_user_usage = {
            "cpu_usage": 10,
            "gpu_usage": 20,
            "ram_usage": 30,
            "month": 1,
            "year": 2022,
            "username": "user-01",
        }

    def test_association_and_usage_creation(self, waldur_client: mock.Mock):
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.list_slurm_allocations.return_value = [self.allocation_waldur]
        waldur_client.list_slurm_associations.return_value = []
        waldur_client.list_slurm_allocation_user_usage.return_value = []
        waldur_client.list_users.return_value = [
            {
                "uuid": self.waldur_user_uuid,
            }
        ]

        sync_data_from_slurm_to_waldur(allocations)

        waldur_client.list_slurm_allocations.assert_called_once_with(
            {"backend_id": "test-allocation-01"}
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.allocation_waldur["uuid"]}
        )
        waldur_client.create_slurm_association.assert_called_once_with(
            self.allocation_waldur["marketplace_resource_uuid"],
            allocation_data["users"][0],
        )
        waldur_client.delete_slurm_association.assert_not_called()
        limits = allocation_data["limits"]
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.allocation_waldur["marketplace_resource_uuid"],
            limits.cpu,
            limits.gpu,
            limits.ram,
        )
        self.assertEqual(2, waldur_client.set_slurm_allocation_usage.call_count)
        waldur_client.set_slurm_allocation_usage.assert_called_with(
            self.allocation_waldur["marketplace_resource_uuid"],
            "TOTAL_ACCOUNT_USAGE",
            1,
            2022,
            allocation_data["usage"]["TOTAL_ACCOUNT_USAGE"].cpu,
            allocation_data["usage"]["TOTAL_ACCOUNT_USAGE"].gpu,
            allocation_data["usage"]["TOTAL_ACCOUNT_USAGE"].ram,
            None,
        )
        waldur_client.list_users.assert_called_once_with(
            {"username": allocation_data["users"][0]}
        )
        waldur_client.list_slurm_allocation_user_usage.assert_called_once_with(
            {
                "allocation_uuid": self.allocation_waldur["uuid"],
                "user_uuid": self.waldur_user_uuid,
                "month": 1,
                "year": 2022,
            }
        )

    def test_association_deletion(self, waldur_client: mock.Mock):
        self.allocation_waldur.update(
            {
                "cpu_limit": 100,
                "cpu_usage": 10,
                "gpu_limit": 200,
                "gpu_usage": 20,
                "ram_limit": 300,
                "ram_usage": 30,
            }
        )
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.list_slurm_allocations.return_value = [self.allocation_waldur]
        waldur_client.list_slurm_allocation_user_usage.return_value = [
            self.waldur_user_usage
        ]
        waldur_client.list_slurm_associations.return_value = [
            {"username": "user-01"},
            {"username": "user-02"},
        ]
        waldur_client.list_users.return_value = [
            {
                "uuid": self.waldur_user_uuid,
            }
        ]

        sync_data_from_slurm_to_waldur(allocations)

        waldur_client.list_slurm_allocations.assert_called_once_with(
            {"backend_id": "test-allocation-01"}
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.allocation_waldur["uuid"]}
        )
        waldur_client.create_slurm_association.assert_not_called()
        waldur_client.delete_slurm_association.assert_called_once_with(
            self.allocation_waldur["marketplace_resource_uuid"], "user-02"
        )
        limits = allocation_data["limits"]
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.allocation_waldur["marketplace_resource_uuid"],
            limits.cpu,
            limits.gpu,
            limits.ram,
        )

        waldur_client.set_slurm_allocation_usage.assert_not_called()
        waldur_client.list_users.assert_called_once_with(
            {"username": allocation_data["users"][0]}
        )
        waldur_client.list_slurm_allocation_user_usage.assert_called_once_with(
            {
                "allocation_uuid": self.allocation_waldur["uuid"],
                "user_uuid": self.waldur_user_uuid,
                "month": 1,
                "year": 2022,
            }
        )
