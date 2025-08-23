import unittest
import uuid
from datetime import datetime, timezone
from unittest import mock

import respx
from freezegun import freeze_time
from respx import Route
from waldur_api_client import models
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import ResourceState, ServiceProvider, ProjectServiceAccount
from waldur_api_client.models.offering_state import OfferingState
from waldur_api_client.models.storage_mode_enum import StorageModeEnum
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_site_agent_slurm import backend

from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingMembershipProcessor

waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

from tests.fixtures import OFFERING

OFFERING_UUID = "d629d5e45567425da9cdbdc1af67b32c"
allocation_slurm = BackendResourceInfo(
    backend_id="test-allocation-01",
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
class MembershipSyncTest(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.waldur_resource = models.Resource(
            uuid=uuid.uuid4(),
            name="test-alloc-01",
            backend_id="test-allocation-01",
            resource_uuid=uuid.uuid4(),
            offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
            downscaled=False,
            state=ResourceState.OK,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_sync=datetime(2024, 1, 1, tzinfo=timezone.utc),
            restrict_member_access=False,
            limits=ResourceLimits.from_dict(
                {
                    "cpu": 50,
                    "mem": 200,
                }
            ),
            project_uuid=uuid.uuid4(),
            project_name="Test project",
            customer_uuid=uuid.uuid4(),
            customer_name="Test customer",
        )

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
            customer_uuid=uuid.uuid4(),
        )
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
        mock.patch.stopall()

    def _setup_common_mocks(self) -> Route:
        """Setup common respx mocks used across all tests."""
        respx.post(
            f"https://waldur.example.com/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_as_erred/"
        ).respond(200, json={})
        respx.post(
            f"https://waldur.example.com/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/refresh_last_sync/"
        ).respond(200, json={})
        respx.get("https://waldur.example.com/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/").respond(
            200, json=self.waldur_offering.to_dict()
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/",
            params={
                "offering_uuid": self.offering.uuid,
                "state": ["OK", "Erred"],
            },
        ).respond(200, json=[self.waldur_resource.to_dict()])
        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})
        service_provider = ServiceProvider(uuid=uuid.uuid4())
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/?customer_uuid={self.waldur_offering.customer_uuid.hex}"
        ).respond(200, json=[service_provider.to_dict()])
        service_account = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="svc-test-account",
            customer_uuid=self.waldur_resource.customer_uuid,
            customer_name=self.waldur_resource.customer_name,
            customer_abbreviation="",
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{service_provider.uuid.hex}/project_service_accounts/?project_uuid={self.waldur_resource.project_uuid.hex}"
        ).respond(200, json=[service_account.to_dict()])
        return respx.post(
            f"https://waldur.example.com/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_limits/"
        ).respond(200, json={"status": "ok"})

    def _setup_team_mock(self, team_data=None) -> None:
        """Setup team mock with optional team data."""
        if team_data is None:
            team_data = self.waldur_resource_team
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/team/"
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
        ).respond(200, json=self.waldur_offering.to_dict())

    def _setup_slurm_mock(self) -> None:
        self.mock_pull_backend_resource = mock.patch.object(
            backend.SlurmBackend, "_pull_backend_resource", return_value=allocation_slurm
        ).start()
        self.mock_restore_resource = mock.patch.object(
            backend.SlurmBackend, "restore_resource", return_value=None
        ).start()
        self.mock_get_resource_limits = mock.patch.object(
            backend.SlurmBackend,
            "get_resource_limits",
            return_value=allocation_slurm.limits,
        ).start()
        self.mock_get_resource_user_limits = mock.patch.object(
            backend.SlurmBackend,
            "get_resource_user_limits",
            return_value={},
        ).start()
        self.mock_add_users_to_resource = mock.patch.object(
            backend.SlurmBackend, "add_users_to_resource"
        ).start()
        self.mock_get_resource_metadata = mock.patch.object(
            backend.SlurmBackend, "get_resource_metadata", return_value=current_qos
        ).start()
        self.mock_cancel_active_jobs_for_account_user = mock.patch.object(
            backend.SlurmBackend, "cancel_active_jobs_for_account_user"
        ).start()
        self.mock_list_active_user_jobs = mock.patch.object(
            backend.SlurmBackend, "list_active_user_jobs", return_value=["123"]
        ).start()
        self.mock_downscale_resource = mock.patch.object(
            backend.SlurmBackend, "downscale_resource"
        ).start()

    def test_association_create(
        self,
    ) -> None:
        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()
        self._setup_slurm_mock()

        set_backend_metadata_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        assert self.mock_add_users_to_resource.call_count == 2
        assert set_backend_metadata_response.call_count == 1
        self.mock_get_resource_metadata.assert_called_once()

    @mock.patch("waldur_site_agent_slurm.backend.SlurmClient", autospec=True)
    def test_association_delete(
        self,
        slurm_client_class,
    ) -> None:
        stale_offering_user_data = self.waldur_offering_user.copy()
        stale_offering_user_data["username"] = "user-03"

        self._setup_common_mocks()
        self._setup_team_mock(team_data=[])
        self._setup_offering_users_mock(offering_users_data=[stale_offering_user_data])
        self._setup_offering_details_mock(offering_user_data=stale_offering_user_data)
        self._setup_slurm_mock()

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = "exists"
        slurm_client.delete_association.return_value = "done"

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        self.mock_list_active_user_jobs.assert_called_once()
        self.mock_cancel_active_jobs_for_account_user.assert_called_once_with(
            allocation_slurm.backend_id, "user-03"
        )
        self.mock_get_resource_metadata.assert_called_once()

    def test_qos_downscaling(
        self,
    ) -> None:
        self.waldur_resource.downscaled = True
        self.waldur_resource.paused = False

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()
        self._setup_slurm_mock()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        self.mock_downscale_resource.assert_called_once()
        self.mock_get_resource_metadata.assert_called_once()

    def test_limits_update(
        self,
    ) -> None:
        mock_set_limits = self._setup_common_mocks()
        self._setup_team_mock(team_data=[])
        self._setup_offering_users_mock(offering_users_data=[self.waldur_offering_user])
        self._setup_slurm_mock()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_offering()

        self.mock_get_resource_metadata.assert_called_once()
        self.mock_get_resource_limits.assert_called_once()
        assert mock_set_limits.call_count == 1
