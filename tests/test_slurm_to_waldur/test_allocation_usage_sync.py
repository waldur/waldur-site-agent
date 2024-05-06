import unittest
import uuid
from unittest import mock

from freezegun import freeze_time
from waldur_client import ComponentUsage

import waldur_site_agent
from waldur_site_agent.slurm_waldur_utils import sync_data_from_slurm_to_waldur

waldur_client = mock.Mock()
slurm_backend = mock.Mock()

OFFERING_UUID = "1a6ae60417e04088b90a5aa395209ecc"


@freeze_time("2022-01-01")
@mock.patch.object(waldur_site_agent.slurm_waldur_utils, "slurm_backend", slurm_backend)
class TestSlurmToWaldurSync(unittest.TestCase):
    def setUp(self) -> None:
        self.allocation_report_slurm = {
            "test-allocation-01": {
                "users": ["user-01"],
                "usage": {
                    "user-01": {
                        "cpu": 10,
                        "mem": 30,
                    },
                    "TOTAL_ACCOUNT_USAGE": {
                        "cpu": 10,
                        "mem": 30,
                    },
                },
                "limits": {
                    "cpu": 100,
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
            "project_uuid": uuid.uuid4(),
            "customer_uuid": uuid.uuid4(),
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        waldur_client._get_offering.return_value = self.waldur_offering

        self.plan_period_uuid = uuid.uuid4().hex
        waldur_client.marketplace_resource_get_plan_periods.return_value = [
            {"uuid": self.plan_period_uuid}
        ]

    def tearDown(self) -> None:
        waldur_client.reset_mock()
        slurm_backend.reset_mock()
        return super().tearDown()

    def test_association_and_usage_creation(self):
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.filter_marketplace_resources.return_value = [
            self.allocation_waldur
        ]
        waldur_client.list_slurm_associations.return_value = []
        waldur_client._get_offering.return_value = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        offering_users = [{"username": "user-02"}]
        waldur_client.list_remote_offering_users.return_value = offering_users

        sync_data_from_slurm_to_waldur(waldur_client, OFFERING_UUID, allocations)

        waldur_client.filter_marketplace_resources.assert_called_once_with(
            {
                "backend_id": "test-allocation-01",
                "offering_uuid": OFFERING_UUID,
                "state": "OK",
            }
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.allocation_waldur["resource_uuid"]}
        )
        waldur_client.delete_slurm_association.assert_not_called()
        self.assertEqual(2, waldur_client.create_slurm_association.call_count)
        calls = [
            mock.call(self.allocation_waldur["uuid"], allocation_data["users"][0]),
            mock.call(self.allocation_waldur["uuid"], offering_users[0]["username"]),
        ]
        waldur_client.create_slurm_association.assert_has_calls(calls, any_order=False)
        slurm_backend.add_users_to_account.assert_called_once()
        limits = allocation_data["limits"]
        waldur_client.set_slurm_allocation_limits.assert_called_once_with(
            self.allocation_waldur["uuid"], limits
        )
        waldur_client.create_component_usages.assert_called_once_with(
            self.plan_period_uuid,
            [
                ComponentUsage("cpu", 10),
                ComponentUsage("mem", 30),
            ],
        )

    def test_association_deletion(self):
        allocations = self.allocation_report_slurm
        allocation_data = allocations["test-allocation-01"]
        waldur_client.filter_marketplace_resources.return_value = [
            self.allocation_waldur
        ]
        waldur_client.list_slurm_associations.return_value = [
            {"username": "user-01"},
            {"username": "user-02"},
        ]
        waldur_client.list_remote_offering_users.return_value = []

        sync_data_from_slurm_to_waldur(waldur_client, OFFERING_UUID, allocations)

        waldur_client.filter_marketplace_resources.assert_called_once_with(
            {
                "backend_id": "test-allocation-01",
                "offering_uuid": OFFERING_UUID,
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
