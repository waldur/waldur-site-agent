import unittest
import uuid
from unittest import mock

import respx
from freezegun import freeze_time
from waldur_api_client import models
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import ResourceState
from datetime import datetime, timezone
from waldur_api_client.models.storage_mode_enum import StorageModeEnum
from waldur_api_client.models.offering_state import OfferingState
from tests.fixtures import OFFERING
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE, utils
from waldur_site_agent.common.processors import OfferingMembershipProcessor

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
@mock.patch.object(utils.SlurmBackend, "_pull_backend_resource", return_value=allocation_slurm)
@mock.patch.object(utils.SlurmBackend, "restore_resource", return_value=None)
class MembershipSyncTest(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.waldur_resource = models.Resource(
            uuid="10a0f810be1c43bbb651e8cbdbb90198",
            name="test-alloc-01",
            backend_id="test-allocation-01",
            resource_uuid="10a0f810be1c43bbb651e8cbdbb90198",
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            downscaled=False,
            state=ResourceState.OK,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_sync=datetime(2024, 1, 1, tzinfo=timezone.utc),
            restrict_member_access=False,
        ).to_dict()

        self.waldur_user_uuid = uuid.uuid4()
        self.plan_period_uuid = uuid.uuid4().hex
        self.waldur_offering = models.ProviderOfferingDetails(
            uuid=OFFERING.uuid,
            name=OFFERING.name,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingState.ACTIVE,
            type_=MARKETPLACE_SLURM_OFFERING_TYPE,
            plugin_options=models.MergedPluginOptions(
                latest_date_for_resource_termination=datetime(2024, 12, 31, tzinfo=timezone.utc),
                storage_mode=StorageModeEnum.FIXED,
            ),
        ).to_dict()
        self.offering = OFFERING
        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()
        self.waldur_user = models.User(
            uuid=uuid.uuid4(),
            username="test-user",
            date_joined=datetime(2024, 1, 1, tzinfo=timezone.utc),
            email="test@example.com",
            full_name="Test User",
            is_staff=False,
        ).to_dict()
        self.team_member = models.ProjectUser(
            uuid=uuid.uuid4(),
            url="https://waldur.example.com/api/users/test-user-02/",
            username="test-user-02",
            full_name="Test User02",
            role="Member",
            expiration_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            offering_user_username="test-offering-user-01",
            email="test-user-02@example.com",
        ).to_dict()
        self.waldur_offering_user = models.OfferingUser(
            username="test-offering-user-01",
            user_uuid=self.team_member["uuid"],
            offering_uuid=OFFERING.uuid,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).to_dict()
        self.waldur_resource_team = [self.team_member]
        self.mock_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token=OFFERING.api_token,
            timeout=600,
            headers={},
        )
        self.mock_get_client.return_value = self.mock_client

    def tearDown(self) -> None:
        respx.stop()

    def _setup_common_mocks(self) -> None:
        """Setup common respx mocks used across all tests."""
        respx.post(
            f"https://waldur.example.com/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/set_as_erred/"
        ).respond(200, json={})
        respx.get("https://waldur.example.com/api/users/me/").respond(200, json=self.waldur_user)
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
        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})

    def _setup_team_mock(self, team_data=None) -> None:
        """Setup team mock with optional team data."""
        if team_data is None:
            team_data = self.waldur_resource_team
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/team/"
        ).respond(200, json=team_data)

    def _setup_offering_users_mock(self, offering_users_data=None) -> None:
        """Setup offering users mock with optional data."""
        if offering_users_data is None:
            offering_users_data = [self.waldur_offering_user]
        respx.get(
            f"{self.BASE_URL}/api/marketplace-offering-users/",
            params={"offering_uuid": OFFERING.uuid, "is_restricted": False},
        ).respond(200, json=offering_users_data)

    def _setup_offering_details_mock(self, offering_user_data=None) -> None:
        """Setup offering details mock for username generation policy check."""
        if offering_user_data is None:
            offering_user_data = self.waldur_offering_user
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{offering_user_data['offering_uuid']}/"
        ).respond(200, json=self.waldur_offering)

    @mock.patch.object(utils.SlurmBackend, "add_users_to_resource")
    @mock.patch.object(utils.SlurmBackend, "get_resource_metadata", return_value=current_qos)
    def test_association_create(
        self,
        get_resource_metadata_mock,
        add_users_to_resource_mock,
        restore_resource_mock,
        pull_backend_resource_mock,
    ) -> None:
        del restore_resource_mock, pull_backend_resource_mock

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()

        set_backend_metadata_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource['uuid']}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        add_users_to_resource_mock.assert_called_once()
        assert set_backend_metadata_response.call_count == 1
        get_resource_metadata_mock.assert_called_once()

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
    ) -> None:
        del restore_resource_mock, pull_backend_resource_mock

        stale_offering_user_data = self.waldur_offering_user.copy()
        stale_offering_user_data["username"] = "user-03"

        self._setup_common_mocks()
        self._setup_team_mock(team_data=[])
        self._setup_offering_users_mock(offering_users_data=[stale_offering_user_data])
        self._setup_offering_details_mock(offering_user_data=stale_offering_user_data)

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = "exists"
        slurm_client.delete_association.return_value = "done"

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        list_active_user_jobs_mock.assert_called_once()
        cancel_active_jobs_for_account_user_mock.assert_called_once_with(
            allocation_slurm.backend_id, "user-03"
        )
        get_resource_metadata_mock.assert_called_once()

    @mock.patch.object(utils.SlurmBackend, "downscale_resource")
    @mock.patch.object(utils.SlurmBackend, "get_resource_metadata", return_value=current_qos)
    @mock.patch.object(utils.SlurmBackend, "add_users_to_resource")
    def test_qos_downscaling(
        self,
        add_users_to_resource_mock,
        get_resource_metadata_mock,
        downscale_resource_mock,
        restore_resource_mock,
        pull_backend_resource_mock,
    ) -> None:
        del restore_resource_mock, pull_backend_resource_mock
        self.waldur_resource["downscaled"] = True
        self.waldur_resource["paused"] = False

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        downscale_resource_mock.assert_called_once()
        get_resource_metadata_mock.assert_called_once()
