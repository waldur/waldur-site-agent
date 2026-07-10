"""Tests for the shared-project consent union guard.

When many resources collapse onto one shared backend project (Waldur-to-Waldur
federation), membership sync runs per offering. Reconciling removals against a
single resource's consented team wrongly revokes a user who consented to a
sibling offering (with a resource in the same project) but not to this one.

These tests pin the fix: the reconciled team becomes the union of consented
teams across all resources in the source project.
"""

from __future__ import annotations

import uuid
from unittest import mock

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.project_user import ProjectUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.processors import OfferingMembershipProcessor


def _make_waldur_resource(project_uuid: uuid.UUID) -> WaldurResource:
    return WaldurResource(
        uuid=uuid.uuid4(),
        name="test-resource",
        backend_id="test-backend-id",
        state=ResourceState.OK,
        project_uuid=project_uuid,
    )


def _make_project_user(username: str) -> ProjectUser:
    return ProjectUser(
        uuid=uuid.uuid4(),
        url=f"https://waldur.example.com/api/users/{username}/",
        username=username,
        full_name=f"Test {username}",
        role="Member",
        expiration_time=None,
        offering_user_username=username,
        offering_user_state=OfferingUserState.OK,
    )


def _make_offering_user(username: str) -> OfferingUser:
    return OfferingUser(
        uuid=uuid.uuid4(),
        user_uuid=uuid.uuid4(),
        username=username,
        user_username=username,
        state=OfferingUserState.OK,
        url=f"https://waldur.example.com/api/marketplace-offering-users/{uuid.uuid4().hex}/",
    )


def _make_federation_processor():
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = uuid.uuid4().hex
    processor.offering.backend_settings = {}
    processor.resource_backend = mock.Mock()
    processor.resource_backend.shared_project_membership = True
    processor.resource_backend.fetch_consented_users_only = True
    processor.resource_backend.user_resolve_method = "identity_bridge"
    processor.resource_backend.handled_resource_states = [ResourceState.OK]
    processor._team_cache = {}
    return processor


class TestProjectWideConsentUnion:
    """The reconciled team is the union of consented teams across the project."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_team_includes_sibling_consenter(self, mock_team, mock_list):
        """A user consented to a sibling resource is included in the reconciled team."""
        project_uuid = uuid.uuid4()
        resource_o1 = _make_waldur_resource(project_uuid)  # user consented here
        resource_o2 = _make_waldur_resource(project_uuid)  # current, no consent here

        mock_list.sync_all.return_value = [resource_o1, resource_o2]

        consenting_user = _make_project_user("cuid-alice")

        def team_side_effect(*_args, **kwargs):
            if kwargs["uuid"] == resource_o1.uuid.hex:
                return [consenting_user]
            return []

        mock_team.sync.side_effect = team_side_effect

        processor = _make_federation_processor()

        team = processor._get_waldur_resource_team(resource_o2, has_consent=True)

        usernames = {member.username for member in team}
        assert "cuid-alice" in usernames

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_sibling_consenter_not_marked_stale(self, mock_team, mock_list):
        """The user is not flagged for removal from the shared project."""
        project_uuid = uuid.uuid4()
        resource_o1 = _make_waldur_resource(project_uuid)
        resource_o2 = _make_waldur_resource(project_uuid)

        mock_list.sync_all.return_value = [resource_o1, resource_o2]

        consenting_user = _make_project_user("cuid-alice")

        def team_side_effect(*_args, **kwargs):
            if kwargs["uuid"] == resource_o1.uuid.hex:
                return [consenting_user]
            return []

        mock_team.sync.side_effect = team_side_effect

        processor = _make_federation_processor()

        # The shared Waldur B project already has the user (added while syncing O1).
        backend_info = BackendResourceInfo(
            backend_id="test-backend-id",
            users=["cuid-alice"],
            usage={},
        )
        offering_users = [_make_offering_user("cuid-alice")]

        (
            _existing,
            stale_usernames,
            _new,
            *_rest,
        ) = processor._group_resource_usernames(resource_o2, backend_info, offering_users)

        assert "cuid-alice" not in stale_usernames

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_user_without_any_consent_is_revoked(self, mock_team, mock_list):
        """A user who consented to no offering in the project is flagged for removal.

        The union guard must not become a blanket keep-everyone: when a backend member
        has withdrawn (or never gave) consent across every resource in the shared
        project, they are still stale and must be revoked.
        """
        project_uuid = uuid.uuid4()
        resource_o1 = _make_waldur_resource(project_uuid)
        resource_o2 = _make_waldur_resource(project_uuid)

        mock_list.sync_all.return_value = [resource_o1, resource_o2]

        # A different user still consents to O1; the user under test consents to neither.
        consenting_user = _make_project_user("cuid-alice")

        def team_side_effect(*_args, **kwargs):
            if kwargs["uuid"] == resource_o1.uuid.hex:
                return [consenting_user]
            return []

        mock_team.sync.side_effect = team_side_effect

        processor = _make_federation_processor()

        # The shared Waldur B project still lists the non-consenting user.
        backend_info = BackendResourceInfo(
            backend_id="test-backend-id",
            users=["cuid-alice", "cuid-charlie"],
            usage={},
        )
        offering_users = [_make_offering_user("cuid-alice"), _make_offering_user("cuid-charlie")]

        (
            _existing,
            stale_usernames,
            _new,
            *_rest,
        ) = processor._group_resource_usernames(resource_o2, backend_info, offering_users)

        assert "cuid-charlie" in stale_usernames
        assert "cuid-alice" not in stale_usernames

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_falls_back_to_single_resource_on_list_error(self, mock_team, mock_list):
        """If sibling resources cannot be listed, degrade to the single resource team."""
        project_uuid = uuid.uuid4()
        resource_o2 = _make_waldur_resource(project_uuid)

        mock_list.sync_all.side_effect = RuntimeError("provider scope error")
        mock_team.sync.return_value = [_make_project_user("cuid-bob")]

        processor = _make_federation_processor()

        team = processor._get_waldur_resource_team(resource_o2, has_consent=True)

        assert {member.username for member in team} == {"cuid-bob"}
        mock_team.sync.assert_called_once()
