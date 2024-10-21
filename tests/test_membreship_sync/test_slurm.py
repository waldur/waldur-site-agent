import unittest
import uuid
from unittest import mock

from freezegun import freeze_time

from waldur_site_agent import common_utils, MARKETPLACE_SLURM_OFFERING_TYPE
from tests.fixtures import OFFERING
from waldur_site_agent.agent_membership_sync import OfferingMembershipProcessor
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.structures import Resource

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

OFFERING_UUID = "d629d5e45567425da9cdbdc1af67b32c"
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
@mock.patch.object(common_utils.SlurmBackend, "restore_resource", return_value=None)
class MembershipSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "resource_uuid": uuid.uuid4().hex,
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "downscaled": False,
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.plan_period_uuid = uuid.uuid4().hex
        self.offering = OFFERING

    @mock.patch.object(common_utils.SlurmBackend, "add_users_to_resource")
    def test_association_create(
        self,
        add_users_to_resource_mock,
        restore_resource_mock,
        pull_allocation_mock,
        waldur_client_class: mock.Mock,
    ):
        del restore_resource_mock, pull_allocation_mock
        user_uuid = uuid.uuid4().hex
        processor = OfferingMembershipProcessor(self.offering)

        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
        waldur_client.list_slurm_associations.return_value = []
        waldur_client.marketplace_provider_resource_get_team.return_value = [
            {
                "uuid": user_uuid,
                "username": "test-user-02",
                "full_name": "Test User02",
            },
        ]

        offering_users = [{"username": "user-02", "user_uuid": user_uuid}]
        waldur_client.list_remote_offering_users.return_value = offering_users

        processor.process_offering()

        waldur_client.filter_marketplace_provider_resources.assert_called_once_with(
            {
                "offering_uuid": self.offering.uuid,
                "state": "OK",
                "field": [
                    "backend_id",
                    "uuid",
                    "name",
                    "resource_uuid",
                    "offering_type",
                    "restrict_member_access",
                    "downscaled",
                    "paused",
                ],
            }
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.waldur_resource["resource_uuid"]}
        )
        waldur_client.delete_slurm_association.assert_not_called()
        self.assertEqual(2, waldur_client.create_slurm_association.call_count)
        calls = [
            mock.call(self.waldur_resource["uuid"], allocation_slurm.users[0]),
            mock.call(self.waldur_resource["uuid"], offering_users[0]["username"]),
        ]
        waldur_client.create_slurm_association.assert_has_calls(calls, any_order=False)
        add_users_to_resource_mock.assert_called_once()

    def test_association_delete(
        self, restore_resource_mock, pull_allocation_mock: mock.Mock, waldur_client_class: mock.Mock
    ):
        del restore_resource_mock, pull_allocation_mock
        processor = OfferingMembershipProcessor(self.offering)
        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
        waldur_client.list_slurm_associations.return_value = [
            {"username": "user-01"},
            {"username": "user-02"},
        ]
        waldur_client.list_remote_offering_users.return_value = []

        processor.process_offering()

        waldur_client.filter_marketplace_provider_resources.assert_called_once_with(
            {
                "offering_uuid": self.offering.uuid,
                "state": "OK",
                "field": [
                    "backend_id",
                    "uuid",
                    "name",
                    "resource_uuid",
                    "offering_type",
                    "restrict_member_access",
                    "downscaled",
                    "paused",
                ],
            }
        )
        waldur_client.list_slurm_associations.assert_called_once_with(
            {"allocation_uuid": self.waldur_resource["resource_uuid"]}
        )
        waldur_client.create_slurm_association.assert_not_called()
        waldur_client.delete_slurm_association.assert_called_once_with(
            self.waldur_resource["uuid"], "user-02"
        )

    @mock.patch.object(common_utils.SlurmBackend, "downscale_resource")
    def test_qos_downscaling(
        self,
        downscale_resource_mock,
        restore_resource_mock,
        pull_allocation_mock,
        waldur_client_class: mock.Mock,
    ):
        del restore_resource_mock, pull_allocation_mock
        self.waldur_resource["downscaled"] = True
        processor = OfferingMembershipProcessor(self.offering)

        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]

        processor.process_offering()

        downscale_resource_mock.assert_called_once()
