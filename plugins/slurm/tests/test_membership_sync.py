import json
import unittest
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional
from unittest import mock

import respx
from freezegun import freeze_time
from respx import Route
from waldur_api_client import models
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import (
    CourseAccount,
    OfferingUserState,
    ProjectServiceAccount,
    ResourceState,
    ServiceAccountState,
    ServiceProvider,
)
from waldur_api_client.models.offering_state import OfferingState
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_api_client.models.storage_mode_enum import StorageModeEnum
from waldur_site_agent_slurm import backend

from tests.fixtures import OFFERING, user_me_api_response
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE
from waldur_site_agent.common.processors import OfferingMembershipProcessor


def _serialize_datetime_aware(obj: dict[str, Any]) -> bytes:
    """Serialize a dict to JSON bytes, converting datetime objects to ISO strings."""
    return json.dumps(
        obj, default=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
    ).encode()


def _must_not_fetch_known_usernames() -> set[str]:
    """Sentinel: the unfiltered offering-user fetch must not run on this path."""
    msg = (
        "_get_known_offering_usernames must not be called when "
        "preserve_unmanaged_backend_users is off or identity-bridge is on"
    )
    raise AssertionError(msg)


def _grouping_processor(
    team: list,
    backend_users: Optional[list[str]] = None,
    *,
    resource_backend: Optional[SimpleNamespace] = None,
    preserve_unmanaged: bool = False,
    known_usernames: Optional[set[str]] = None,
) -> tuple[OfferingMembershipProcessor, SimpleNamespace, SimpleNamespace]:
    """Minimal membership processor for _group_resource_usernames unit tests."""
    processor = object.__new__(OfferingMembershipProcessor)
    processor._team_cache = {}
    processor._get_exposed_fields = lambda: []  # type: ignore[assignment]
    processor.resource_backend = resource_backend or SimpleNamespace(user_resolve_method=None)
    processor.offering = SimpleNamespace(preserve_unmanaged_backend_users=preserve_unmanaged)
    processor.service_provider = None
    processor._get_waldur_resource_team = lambda _resource, **_kw: team  # type: ignore[assignment]
    if known_usernames is not None:
        processor._get_known_offering_usernames = (  # type: ignore[method-assign]
            lambda: set(known_usernames)
        )
    else:
        processor._get_known_offering_usernames = _must_not_fetch_known_usernames  # type: ignore[method-assign]
    waldur_resource = SimpleNamespace(
        uuid=SimpleNamespace(hex="r"),
        project_uuid=SimpleNamespace(hex="p"),
        backend_id="r",
    )
    backend_resource_info = SimpleNamespace(users=list(backend_users or []))
    return processor, waldur_resource, backend_resource_info


waldur_client_mock = mock.Mock()
slurm_backend_mock = mock.Mock()

OFFERING_UUID = "d629d5e45567425da9cdbdc1af67b32c"
allocation_slurm = BackendResourceInfo(
    backend_id="test-allocation-01",
    users=[],
    usage={
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
# Backend that still reports a user who has since left all of their projects.
allocation_slurm_with_stale_user = BackendResourceInfo(
    backend_id="test-allocation-01",
    users=["user-03"],
    usage={
        "user-03": {
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
            project_slug="test-project",
            customer_uuid=uuid.uuid4(),
            customer_name="Test customer",
            customer_slug="test-customer",
        )

        self.waldur_user_uuid = uuid.uuid4()
        self.plan_period_uuid = uuid.uuid4().hex
        self.offering = OFFERING
        self.waldur_offering = models.ProviderOfferingDetails(
            uuid=self.offering.uuid,
            name=self.offering.name,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingState.ACTIVE,
            type_=MARKETPLACE_SLURM_OFFERING_TYPE,
            plugin_options=models.MergedPluginOptions(
                latest_date_for_resource_termination=datetime(2024, 12, 31, tzinfo=timezone.utc),
                storage_mode=StorageModeEnum.FIXED,
                service_provider_can_create_offering_user=True,
            ),
            customer_uuid=uuid.uuid4(),
        )
        self.client_patcher = mock.patch("waldur_site_agent.common.utils.get_client")
        self.mock_get_client = self.client_patcher.start()
        self.waldur_user = user_me_api_response(
            base_url=self.BASE_URL.rstrip("/"),
            username="test-user",
        )
        self.team_member = models.ProjectUser(
            uuid=uuid.uuid4(),
            url="https://waldur.example.com/api/users/test-user-02/",
            username="test-user-02",
            full_name="Test User02",
            role="Member",
            expiration_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            offering_user_username="test-offering-user-01",
            email="test-user-02@example.com",
            offering_user_state=OfferingUserState.OK,
        ).to_dict()
        self.waldur_offering_user = models.OfferingUser(
            username="test-offering-user-01",
            user_uuid=self.team_member["uuid"],
            offering_uuid=self.offering.uuid,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
            state=OfferingUserState.OK,
        ).to_dict()
        self.waldur_resource_team = [self.team_member]
        self.mock_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token=self.offering.api_token,
            timeout=600,
            headers={},
        )

    def tearDown(self) -> None:
        respx.stop()
        mock.patch.stopall()

    def _setup_common_mocks(self) -> Route:
        """Setup common respx mocks used across all tests."""
        respx.post(
            f"{self.BASE_URL}api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_as_erred/"
        ).respond(200, json={})
        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/refresh_last_sync/"
        ).respond(200, json={})
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, content=_serialize_datetime_aware(self.waldur_offering.to_dict()))
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/",
        ).respond(200, json=[self.waldur_resource.to_dict()])
        respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})
        service_provider = ServiceProvider(uuid=uuid.uuid4())
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/",
            params={"customer_uuid": self.waldur_offering.customer_uuid.hex},
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
            state=ServiceAccountState.OK,
        )
        respx.get(
            url__regex=rf".*/api/marketplace-provider-offerings/{self.offering.uuid}/list_project_service_accounts/.*",
        ).respond(200, json=[service_account.to_dict()])
        course_account = CourseAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            user_uuid=uuid.uuid4(),
            username="course-test-00",
            customer_uuid=self.waldur_resource.customer_uuid,
            customer_name=self.waldur_resource.customer_name,
            state=ServiceAccountState.OK,
            error_message="",
            error_traceback="",
            project_slug=self.waldur_resource.project_slug,
            project_start_date=datetime.now(),
            project_end_date=datetime.now(),
        )
        respx.get(
            url__regex=rf".*/api/marketplace-provider-offerings/{self.offering.uuid}/list_course_accounts/.*",
        ).respond(200, json=[course_account.to_dict()])
        respx.get(
            f"{self.BASE_URL}/api/component-user-usage-limits/",
        ).respond(200, json=[])
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
        ).respond(200, json=offering_users_data)

    def _setup_offering_details_mock(self, offering_user_data=None) -> None:
        """Setup offering details mock for username generation policy check."""
        if offering_user_data is None:
            offering_user_data = self.waldur_offering_user
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{offering_user_data['offering_uuid']}/"
        ).respond(200, content=_serialize_datetime_aware(self.waldur_offering.to_dict()))

    def _setup_slurm_mock(self, backend_resource=None) -> None:
        if backend_resource is None:
            backend_resource = allocation_slurm
        self.mock_pull_backend_resource = mock.patch.object(
            backend.SlurmBackend, "_pull_backend_resource", return_value=backend_resource
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
        self.mock_pause_resource = mock.patch.object(
            backend.SlurmBackend, "pause_resource", return_value=True
        ).start()
        mock.patch.object(backend.SlurmBackend, "sync_resource_project").start()

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

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        assert self.mock_add_users_to_resource.call_count == 3
        assert set_backend_metadata_response.call_count == 1
        self.mock_get_resource_metadata.assert_called_once()

    @mock.patch("waldur_site_agent_slurm.backend.SlurmClient", autospec=True)
    def test_association_delete(
        self,
        slurm_client_class,
    ) -> None:
        # The backend still reports user-03, but that user left all of their projects, so the
        # team is empty and no actionable offering user is returned for them. The user must
        # still be flagged stale and removed; intersecting with the offering users list would
        # have leaked them.
        self._setup_common_mocks()
        self._setup_team_mock(team_data=[])
        self._setup_offering_users_mock(offering_users_data=[])
        self._setup_slurm_mock(backend_resource=allocation_slurm_with_stale_user)

        slurm_client = slurm_client_class.return_value
        slurm_client.get_association.return_value = "exists"
        slurm_client.delete_association.return_value = "done"

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_list_active_user_jobs.assert_called_once()
        self.mock_cancel_active_jobs_for_account_user.assert_called_once_with(
            allocation_slurm_with_stale_user.backend_id, "user-03"
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

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_downscale_resource.assert_called_once()
        self.mock_get_resource_metadata.assert_called_once()

    def test_qos_pausing(
        self,
    ) -> None:
        self.waldur_resource.paused = True
        self.waldur_resource.downscaled = False

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()
        self._setup_slurm_mock()

        set_backend_metadata_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_pause_resource.assert_called_once()
        self.mock_downscale_resource.assert_not_called()
        self.mock_restore_resource.assert_not_called()
        self.mock_get_resource_metadata.assert_called_once()
        assert set_backend_metadata_response.call_count == 1

    def test_qos_pausing_takes_precedence_over_downscaling(
        self,
    ) -> None:
        self.waldur_resource.paused = True
        self.waldur_resource.downscaled = True

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()
        self._setup_slurm_mock()

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_pause_resource.assert_called_once()
        self.mock_downscale_resource.assert_not_called()
        self.mock_restore_resource.assert_not_called()

    def test_qos_restore_when_not_paused_or_downscaled(
        self,
    ) -> None:
        self.waldur_resource.paused = False
        self.waldur_resource.downscaled = False

        self._setup_common_mocks()
        self._setup_team_mock()
        self._setup_offering_users_mock()
        self._setup_offering_details_mock()
        self._setup_slurm_mock()

        set_backend_metadata_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource.uuid.hex}/set_backend_metadata/"
        ).respond(200, json={"status": "OK"})

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_pause_resource.assert_not_called()
        self.mock_downscale_resource.assert_not_called()
        self.mock_restore_resource.assert_called_once()
        self.mock_get_resource_metadata.assert_called_once()
        assert set_backend_metadata_response.call_count == 1

    def test_limits_update(
        self,
    ) -> None:
        mock_set_limits = self._setup_common_mocks()
        self._setup_team_mock(team_data=[])
        self._setup_offering_users_mock(offering_users_data=[self.waldur_offering_user])
        self._setup_slurm_mock()

        processor = OfferingMembershipProcessor(self.offering, self.mock_client)
        processor.process_offering()

        self.mock_get_resource_metadata.assert_called_once()
        self.mock_get_resource_limits.assert_called_once()
        assert mock_set_limits.call_count == 1

    def test_group_resource_usernames_includes_cuid_when_offering_username_missing(self) -> None:
        """
        Federation flow: users may be present in Waldur A team without offering_user_username,
        because backend username is assigned on Waldur B and reconciled back to A later.

        When identity bridge resolution is enabled on the backend, membership sync falls back
        to using CUID (ProjectUser.username). Consent filtering is enforced upstream by the
        team API call (has_consent=True), so all returned team members are included.
        """
        team = [
            SimpleNamespace(
                offering_user_username="",
                username="cuid:alice",
                role="PROJECT.MANAGER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
        )

        (
            _existing_usernames,
            _stale_usernames,
            new_usernames,
            user_roles,
            _user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[]
        )

        assert "cuid:alice" in new_usernames
        assert user_roles["cuid:alice"] == "PROJECT.MANAGER"
        assert user_cuids["cuid:alice"] == "cuid:alice"

    def test_group_resource_usernames_skips_cuid_without_identity_bridge(self) -> None:
        """Without identity bridge, CUID-only users must NOT be included."""
        team = [
            SimpleNamespace(
                offering_user_username="",
                username="cuid:alice",
                role="PROJECT.MANAGER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            resource_backend=SimpleNamespace(user_resolve_method="local"),
        )

        (
            _existing_usernames,
            _stale_usernames,
            new_usernames,
            user_roles,
            _user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[]
        )

        assert "cuid:alice" not in new_usernames
        assert "cuid:alice" not in user_roles
        assert "cuid:alice" not in user_cuids

    def test_group_resource_usernames_skips_cuid_when_attr_missing(self) -> None:
        """Backend with no user_resolve_method attr should not include CUID-only users."""
        team = [
            SimpleNamespace(
                offering_user_username="",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            resource_backend=SimpleNamespace(),
        )

        (
            _existing_usernames,
            _stale_usernames,
            new_usernames,
            user_roles,
            _user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[]
        )

        assert "cuid:bob" not in new_usernames
        assert "cuid:bob" not in user_roles
        assert "cuid:bob" not in user_cuids

    def test_group_resource_usernames_mixed_users_with_identity_bridge(self) -> None:
        """With identity bridge, membership diff keys are always CUIDs."""
        team = [
            SimpleNamespace(
                offering_user_username="alice-on-b",
                username="cuid:alice",
                role="PROJECT.MANAGER",
            ),
            SimpleNamespace(
                offering_user_username="",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            ),
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
        )
        alice_ou = SimpleNamespace(
            username="alice-on-b", user_username="cuid:alice", user_email=None, state=None
        )
        bob_ou = SimpleNamespace(username=None, user_username="cuid:bob", user_email=None)

        (
            _existing_usernames,
            _stale_usernames,
            new_usernames,
            user_roles,
            _user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[alice_ou, bob_ou]
        )

        assert new_usernames == {"cuid:alice", "cuid:bob"}

        assert user_roles["cuid:alice"] == "PROJECT.MANAGER"
        assert user_roles["cuid:bob"] == "PROJECT.MEMBER"

        assert user_cuids["cuid:alice"] == "cuid:alice"
        assert user_cuids["cuid:bob"] == "cuid:bob"

    def test_group_resource_usernames_federation_cuid_matches_backend(self) -> None:
        """Regression: offering username on A must not break compare when B lists CUIDs.

        Reproduces Waldur federation churn where A team has offering_user_username
        (domeneca) but Waldur B pull_resources reports myaccessid CUIDs.
        """
        cuid = "bc7eb766-edited-e638a46f163c@myaccessid.org"
        team = [
            SimpleNamespace(
                offering_user_username="domeneca",
                username=cuid,
                role="PROJECT.MANAGER",
            ),
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            [cuid],
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
        )
        offering_user = SimpleNamespace(
            username="domeneca",
            user_username=cuid,
            user_email="domeneca@example.com",
            state=None,
        )

        (
            existing_usernames,
            stale_usernames,
            new_usernames,
            user_roles,
            user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[offering_user]
        )

        assert existing_usernames == {cuid}
        assert new_usernames == set()
        assert stale_usernames == set()
        assert user_roles[cuid] == "PROJECT.MANAGER"
        assert user_cuids[cuid] == cuid
        assert user_emails[cuid] == "domeneca@example.com"

    def test_group_resource_usernames_cuid_user_existing_on_backend(self) -> None:
        """CUID-only user (ToS accepted) already on backend should appear in existing, not new."""
        team = [
            SimpleNamespace(
                offering_user_username="",
                username="cuid:alice",
                role="PROJECT.MANAGER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["cuid:alice"],
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
        )
        alice_ou = SimpleNamespace(username=None, user_username="cuid:alice", user_email=None)

        (
            existing_usernames,
            _stale_usernames,
            new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[alice_ou]
        )

        assert "cuid:alice" in existing_usernames
        assert "cuid:alice" not in new_usernames

    def test_group_resource_usernames_includes_all_cuid_only_team_members(self) -> None:
        """All CUID-only users returned by the team API are synced.

        Consent filtering is enforced upstream via has_consent=True on the API call,
        so site-agent includes every team member returned regardless of local offering_users.
        """
        team = [
            SimpleNamespace(
                offering_user_username="",
                username="cuid:alice",
                role="PROJECT.MANAGER",
            ),
            SimpleNamespace(
                offering_user_username="",
                username="cuid:ville",
                role="PROJECT.MEMBER",
            ),
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
        )

        (
            _existing_usernames,
            _stale_usernames,
            new_usernames,
            user_roles,
            _user_emails,
            user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[]
        )

        assert "cuid:alice" in new_usernames
        assert user_roles["cuid:alice"] == "PROJECT.MANAGER"
        assert user_cuids["cuid:alice"] == "cuid:alice"

        assert "cuid:ville" in new_usernames
        assert user_roles["cuid:ville"] == "PROJECT.MEMBER"
        assert user_cuids["cuid:ville"] == "cuid:ville"

    def test_group_resource_usernames_stale_when_offering_user_absent(self) -> None:
        """User who left all projects must be flagged stale even if absent from offering_users.

        Regression for a leak where a user removed from their last project kept their backend
        association: their offering user drops out of the (state-filtered, offering-wide)
        offering_users list, so intersecting stale candidates with it never flagged them.
        Stale must be derived from the backend user list minus the current team.
        """
        remaining_ou = SimpleNamespace(
            username="remaining-user-01", user_username="cuid:bob", user_email=None, state=None
        )
        team = [
            SimpleNamespace(
                offering_user_username="remaining-user-01",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["departed-user-01", "remaining-user-01"],
        )

        (
            existing_usernames,
            stale_usernames,
            new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[remaining_ou]
        )

        assert stale_usernames == {"departed-user-01"}
        assert existing_usernames == {"remaining-user-01"}
        assert new_usernames == set()

    def test_preserve_unmanaged_soft_deleted_offering_user_is_stale(self) -> None:
        """Departed user still counts as Waldur-managed after soft-delete.

        Production: they drop out of the state-filtered offering_users list
        (REQUESTED_DELETION/DELETED) but remain in the unfiltered known set
        with the same username. They must be removed -- this is gh-13.
        """
        remaining_ou = SimpleNamespace(
            username="remaining-user-01", user_username="cuid:bob", user_email=None, state=None
        )
        team = [
            SimpleNamespace(
                offering_user_username="remaining-user-01",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["departed-user-01", "remaining-user-01"],
            preserve_unmanaged=True,
            known_usernames={"remaining-user-01", "departed-user-01"},
        )

        (
            existing_usernames,
            stale_usernames,
            new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[remaining_ou]
        )

        assert stale_usernames == {"departed-user-01"}
        assert existing_usernames == {"remaining-user-01"}
        assert new_usernames == set()

    def test_preserve_unmanaged_restricted_offering_user_is_stale(self) -> None:
        """Restricted offering users are filtered from offering_users but still known."""
        remaining_ou = SimpleNamespace(
            username="remaining-user-01", user_username="cuid:bob", user_email=None, state=None
        )
        team = [
            SimpleNamespace(
                offering_user_username="remaining-user-01",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["restricted-user-01", "remaining-user-01"],
            preserve_unmanaged=True,
            known_usernames={"remaining-user-01", "restricted-user-01"},
        )

        (
            _existing_usernames,
            stale_usernames,
            _new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[remaining_ou]
        )

        assert stale_usernames == {"restricted-user-01"}

    def test_preserve_unmanaged_keeps_hand_added_user(self) -> None:
        """Username Waldur has never seen is kept on the backend."""
        remaining_ou = SimpleNamespace(
            username="remaining-user-01", user_username="cuid:bob", user_email=None, state=None
        )
        team = [
            SimpleNamespace(
                offering_user_username="remaining-user-01",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["sp-manual-user", "remaining-user-01"],
            preserve_unmanaged=True,
            known_usernames={"remaining-user-01"},
        )

        (
            existing_usernames,
            stale_usernames,
            new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[remaining_ou]
        )

        assert stale_usernames == set()
        assert "sp-manual-user" not in stale_usernames
        assert existing_usernames == {"remaining-user-01"}
        assert new_usernames == set()

    def test_preserve_unmanaged_flag_off_does_not_fetch_known(self) -> None:
        """Flag off: extras are still stale and the unfiltered list is not fetched."""
        remaining_ou = SimpleNamespace(
            username="remaining-user-01", user_username="cuid:bob", user_email=None, state=None
        )
        team = [
            SimpleNamespace(
                offering_user_username="remaining-user-01",
                username="cuid:bob",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["sp-manual-user", "departed-user-01", "remaining-user-01"],
            preserve_unmanaged=False,
        )

        (
            _existing_usernames,
            stale_usernames,
            _new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[remaining_ou]
        )

        assert stale_usernames == {"sp-manual-user", "departed-user-01"}

    def test_preserve_unmanaged_ignored_for_identity_bridge(self) -> None:
        """Federation still removes extras even if preserve_unmanaged_backend_users is set."""
        team = [
            SimpleNamespace(
                offering_user_username="alice-on-b",
                username="cuid:alice",
                role="PROJECT.MEMBER",
            )
        ]
        processor, waldur_resource, backend_resource_info = _grouping_processor(
            team,
            ["cuid:alice", "cuid:manual"],
            resource_backend=SimpleNamespace(user_resolve_method="identity_bridge"),
            preserve_unmanaged=True,
        )
        alice_ou = SimpleNamespace(
            username="alice-on-b", user_username="cuid:alice", user_email=None, state=None
        )

        (
            existing_usernames,
            stale_usernames,
            new_usernames,
            _user_roles,
            _user_emails,
            _user_cuids,
            _user_attributes,
            _offering_user_states,
        ) = processor._group_resource_usernames(
            waldur_resource, backend_resource_info, offering_users=[alice_ou]
        )

        assert stale_usernames == {"cuid:manual"}
        assert existing_usernames == {"cuid:alice"}
        assert new_usernames == set()
