import unittest
import uuid
from unittest import mock

from freezegun import freeze_time

from tests.fixtures import OFFERING
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common import utils
from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.structures import Resource

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

OFFERING_UUID = "d629d5e45567425da9cdbdc1af67b32c"
allocation_slurm = Resource(
    backend_id="test-allocation-01",
    backend_type=BackendType.SLURM.value,
    users=["user-01", "user-03"],
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
current_qos = {"qos": "abc"}


@freeze_time("2022-01-01")
@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
@mock.patch.object(utils.SlurmBackend, "_pull_backend_resource", return_value=allocation_slurm)
@mock.patch.object(utils.SlurmBackend, "restore_resource", return_value=None)
class MembershipSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "resource_uuid": uuid.uuid4().hex,
            "offering_type": MARKETPLACE_SLURM_OFFERING_TYPE,
            "downscaled": False,
            "state": "OK",
        }
        self.waldur_user_uuid = uuid.uuid4()
        self.plan_period_uuid = uuid.uuid4().hex
        self.offering = OFFERING

    @mock.patch.object(utils.SlurmBackend, "add_users_to_resource")
    @mock.patch.object(utils.SlurmBackend, "get_resource_metadata", return_value=current_qos)
    def test_association_create(
        self,
        get_resource_metadata_mock,
        add_users_to_resource_mock,
        restore_resource_mock,
        pull_backend_resource_mock,
        waldur_client_class: mock.Mock,
    ):
        del restore_resource_mock, pull_backend_resource_mock
        user_uuid = uuid.uuid4().hex
        processor = OfferingMembershipProcessor(self.offering)

        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
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
                "state": ["OK", utils.RESOURCE_ERRED_STATE],
                "field": [
                    "backend_id",
                    "uuid",
                    "name",
                    "resource_uuid",
                    "offering_type",
                    "restrict_member_access",
                    "downscaled",
                    "paused",
                    "state",
                    "limits",
                ],
            }
        )
        add_users_to_resource_mock.assert_called_once()

        get_resource_metadata_mock.assert_called_once()
        waldur_client.marketplace_provider_resource_set_backend_metadata.assert_called_once_with(
            self.waldur_resource["uuid"], current_qos
        )

    @mock.patch.object(utils.SlurmBackend, "cancel_active_jobs_for_account_user")
    @mock.patch.object(utils.SlurmBackend, "list_active_user_jobs", return_value=["123"])
    @mock.patch.object(utils.SlurmBackend, "get_resource_metadata", return_value=current_qos)
    @mock.patch("waldur_site_agent.backends.slurm_backend.backend.SlurmClient", autospec=True)
    def test_association_delete(
        self,
        slurm_client_class,
        get_resource_metadata_mock,
        list_active_user_jobs_mock,
        cancel_active_jobs_for_account_user_mock,
        restore_resource_mock,
        pull_backend_resource_mock: mock.Mock,
        waldur_client_class: mock.Mock,
    ):
        del restore_resource_mock, pull_backend_resource_mock
        processor = OfferingMembershipProcessor(self.offering)
        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
        waldur_client.marketplace_provider_resource_get_team.return_value = []
        waldur_client.list_remote_offering_users.return_value = [
            {"username": "user-03", "user_uuid": "3D83D488E39D47CB8F620D055888950E"}
        ]

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = "exists"
        slurm_client.delete_association.return_value = "done"

        processor.process_offering()

        waldur_client.filter_marketplace_provider_resources.assert_called_once_with(
            {
                "offering_uuid": self.offering.uuid,
                "state": ["OK", utils.RESOURCE_ERRED_STATE],
                "field": [
                    "backend_id",
                    "uuid",
                    "name",
                    "resource_uuid",
                    "offering_type",
                    "restrict_member_access",
                    "downscaled",
                    "paused",
                    "state",
                    "limits",
                ],
            }
        )

        list_active_user_jobs_mock.assert_called_once()
        cancel_active_jobs_for_account_user_mock.assert_called_once_with(
            allocation_slurm.backend_id, "user-03"
        )

        get_resource_metadata_mock.assert_called_once()
        waldur_client.marketplace_provider_resource_set_backend_metadata.assert_called_once_with(
            self.waldur_resource["uuid"], current_qos
        )

    @mock.patch.object(utils.SlurmBackend, "downscale_resource")
    @mock.patch.object(utils.SlurmBackend, "get_resource_metadata", return_value=current_qos)
    def test_qos_downscaling(
        self,
        get_resource_metadata_mock,
        downscale_resource_mock,
        restore_resource_mock,
        pull_backend_resource_mock,
        waldur_client_class: mock.Mock,
    ):
        del restore_resource_mock, pull_backend_resource_mock
        self.waldur_resource["downscaled"] = True
        processor = OfferingMembershipProcessor(self.offering)

        waldur_client = waldur_client_class.return_value
        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]

        processor.process_offering()

        downscale_resource_mock.assert_called_once()

        get_resource_metadata_mock.assert_called_once()
        waldur_client.marketplace_provider_resource_set_backend_metadata.assert_called_once_with(
            self.waldur_resource["uuid"], current_qos
        )
