"""Tests for per-cycle cache behavior on offering processors.

Verifies that caches avoid redundant API calls within a single processing cycle,
and that cache invalidation triggers fresh API fetches.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest import mock

import pytest
from waldur_api_client.models import OfferingUserFieldEnum
from waldur_api_client.models.course_account import CourseAccount
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.project_service_account import ProjectServiceAccount
from waldur_api_client.models.project_user import ProjectUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.models.service_account_state import ServiceAccountState
from waldur_api_client.models.service_provider import ServiceProvider

from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingReportProcessor,
)

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _make_offering_user(
    username: str, state: OfferingUserState = OfferingUserState.OK
) -> OfferingUser:
    return OfferingUser(
        uuid=uuid.uuid4(),
        user_uuid=uuid.uuid4(),
        username=username,
        state=state,
        url=f"https://waldur.example.com/api/marketplace-offering-users/{uuid.uuid4().hex}/",
    )


def _make_waldur_resource(project_uuid: uuid.UUID | None = None) -> WaldurResource:
    return WaldurResource(
        uuid=uuid.uuid4(),
        name="test-resource",
        backend_id="test-backend-id",
        state=ResourceState.OK,
        project_uuid=project_uuid or uuid.uuid4(),
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


def _make_service_account(
    username: str,
    state: ServiceAccountState = ServiceAccountState.OK,
    project_uuid: uuid.UUID | None = None,
):
    proj = project_uuid or uuid.uuid4()
    return ProjectServiceAccount(
        url="",
        uuid=uuid.uuid4(),
        created=_NOW,
        modified=_NOW,
        error_message="",
        state=state,
        token=None,
        expires_at=None,
        project=proj,
        project_uuid=proj,
        project_name="Test project",
        customer_uuid=uuid.uuid4(),
        customer_name="Test customer",
        customer_abbreviation="",
        username=username,
    )


def _make_course_account(
    username: str,
    state: ServiceAccountState = ServiceAccountState.OK,
    project_uuid: uuid.UUID | None = None,
):
    proj = project_uuid or uuid.uuid4()
    return CourseAccount(
        url="",
        uuid=uuid.uuid4(),
        created=_NOW,
        modified=_NOW,
        project=proj,
        project_uuid=proj,
        project_name="Test project",
        project_slug="test-project",
        project_start_date=_NOW.date(),
        project_end_date=_NOW.date(),
        user_uuid=uuid.uuid4(),
        username=username,
        customer_uuid=uuid.uuid4(),
        customer_name="Test customer",
        state=state,
        error_message="",
        error_traceback="",
    )


def _make_processor(cls):
    """Create a processor instance bypassing __init__ and setting minimal attributes."""
    processor = cls.__new__(cls)
    processor._offering_users_cache = None
    processor._known_offering_usernames_cache = None
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = uuid.uuid4().hex
    processor.offering.preserve_unmanaged_backend_users = False
    processor.resource_backend = mock.Mock()
    processor.service_provider = ServiceProvider(uuid=uuid.uuid4())
    processor.timezone = ""
    return processor


def _make_membership_processor():
    processor = _make_processor(OfferingMembershipProcessor)
    processor._team_cache = {}
    processor._service_accounts_cache = {}
    processor._course_accounts_cache = {}
    return processor


def _make_report_processor():
    return _make_processor(OfferingReportProcessor)


# ---------------------------------------------------------------------------
# Offering users cache (shared by all processor types via OfferingBaseProcessor)
# ---------------------------------------------------------------------------


class TestOfferingUsersCache:
    """Test _get_cached_offering_users / _invalidate_offering_users_cache."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_second_call_uses_cache(self, mock_api):
        """Second call returns cached result without an additional API call."""
        offering_users = [_make_offering_user("user-01")]
        mock_api.sync_all.return_value = offering_users

        processor = _make_membership_processor()

        result1 = processor._get_cached_offering_users()
        result2 = processor._get_cached_offering_users()

        assert result1 is result2
        assert mock_api.sync_all.call_count == 1

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_invalidation_causes_refetch(self, mock_api):
        """After invalidation the next call makes a fresh API request."""
        first_batch = [_make_offering_user("user-01")]
        second_batch = [_make_offering_user("user-01"), _make_offering_user("user-02")]
        mock_api.sync_all.side_effect = [first_batch, second_batch]

        processor = _make_membership_processor()

        result1 = processor._get_cached_offering_users()
        assert len(result1) == 1

        processor._invalidate_offering_users_cache()

        result2 = processor._get_cached_offering_users()
        assert len(result2) == 2
        assert mock_api.sync_all.call_count == 2

    @mock.patch("waldur_site_agent.common.processors.utils")
    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_update_offering_users_invalidates_cache(self, mock_api, mock_utils):
        """_update_offering_users invalidates cache when modifications occur."""
        offering_users = [_make_offering_user("user-01")]
        mock_api.sync_all.return_value = offering_users
        mock_utils.update_offering_users.return_value = True  # indicates modification

        processor = _make_membership_processor()

        # Populate cache
        processor._get_cached_offering_users()
        assert mock_api.sync_all.call_count == 1

        # Trigger update that modifies users
        processor._update_offering_users(offering_users)

        # Cache should be invalidated — next call re-fetches
        processor._get_cached_offering_users()
        assert mock_api.sync_all.call_count == 2

    @mock.patch("waldur_site_agent.common.processors.utils")
    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_update_offering_users_no_invalidation_when_unchanged(self, mock_api, mock_utils):
        """_update_offering_users does NOT invalidate cache when nothing changed."""
        offering_users = [_make_offering_user("user-01")]
        mock_api.sync_all.return_value = offering_users
        mock_utils.update_offering_users.return_value = False  # no modification

        processor = _make_membership_processor()

        # Populate cache
        processor._get_cached_offering_users()
        assert mock_api.sync_all.call_count == 1

        # Trigger update that does NOT modify users
        processor._update_offering_users(offering_users)

        # Cache should still be valid — no re-fetch
        processor._get_cached_offering_users()
        assert mock_api.sync_all.call_count == 1


# ---------------------------------------------------------------------------
# Offering users cache – state filtering in membership processor
# ---------------------------------------------------------------------------


class TestOfferingUsersCacheFiltering:
    """Test that _get_waldur_offering_users filters cached results by state."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_filters_to_actionable_states(self, mock_api):
        """Actionable states (OK, REQUESTED, and intermediate/error) are returned.

        Deletion-flow states (DELETED, DELETING, DELETION_REQUESTED) are excluded.
        """
        users = [
            _make_offering_user("user-ok", OfferingUserState.OK),
            _make_offering_user("user-requested", OfferingUserState.REQUESTED),
            _make_offering_user("user-creating", OfferingUserState.CREATING),
            _make_offering_user("user-deleted", OfferingUserState.DELETED),
        ]
        mock_api.sync_all.return_value = users

        processor = _make_membership_processor()

        result = processor._get_waldur_offering_users()

        assert len(result) == 3
        usernames = {u.username for u in result}
        assert usernames == {"user-ok", "user-requested", "user-creating"}
        # Only one API call despite filtering
        assert mock_api.sync_all.call_count == 1

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_repeated_calls_use_same_cache(self, mock_api):
        """Multiple calls to _get_waldur_offering_users share the same underlying cache."""
        users = [_make_offering_user("user-ok", OfferingUserState.OK)]
        mock_api.sync_all.return_value = users

        processor = _make_membership_processor()

        processor._get_waldur_offering_users()
        processor._get_waldur_offering_users()

        assert mock_api.sync_all.call_count == 1


class TestKnownOfferingUsernames:
    """Unfiltered username set used by preserve_unmanaged_backend_users."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_fetch_has_no_state_or_restricted_filter(self, mock_api):
        """The list call must not filter by state or is_restricted."""
        mock_api.sync_all.return_value = []
        processor = _make_membership_processor()

        processor._get_known_offering_usernames()

        mock_api.sync_all.assert_called_once()
        kwargs = mock_api.sync_all.call_args.kwargs
        assert "is_restricted" not in kwargs
        assert "state" not in kwargs
        assert kwargs["offering_uuid"] == [processor.offering.uuid]
        assert kwargs["field"] == [OfferingUserFieldEnum.USERNAME]
        assert kwargs["client"] is processor.waldur_rest_client

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_includes_deleted_and_drops_empty_usernames(self, mock_api):
        """DELETED rows stay in the known set; blank usernames are dropped."""
        mock_api.sync_all.return_value = [
            _make_offering_user("user-ok", OfferingUserState.OK),
            _make_offering_user("user-deleted", OfferingUserState.DELETED),
            _make_offering_user("user-deleting", OfferingUserState.DELETING),
            _make_offering_user("user-deletion-requested", OfferingUserState.REQUESTED_DELETION),
            _make_offering_user("", OfferingUserState.OK),
        ]
        processor = _make_membership_processor()

        known = processor._get_known_offering_usernames()

        assert known == {
            "user-ok",
            "user-deleted",
            "user-deleting",
            "user-deletion-requested",
        }

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_caches_per_cycle(self, mock_api):
        """Second call returns the cached set without another API request."""
        mock_api.sync_all.return_value = [_make_offering_user("user-ok")]
        processor = _make_membership_processor()

        first = processor._get_known_offering_usernames()
        second = processor._get_known_offering_usernames()

        assert first is second
        assert first == {"user-ok"}
        assert mock_api.sync_all.call_count == 1

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_invalidation_clears_known_cache(self, mock_api):
        """_invalidate_offering_users_cache also drops the known-username cache."""
        mock_api.sync_all.side_effect = [
            [_make_offering_user("user-01")],
            [_make_offering_user("user-01"), _make_offering_user("user-02")],
        ]
        processor = _make_membership_processor()

        assert processor._get_known_offering_usernames() == {"user-01"}
        processor._invalidate_offering_users_cache()
        assert processor._get_known_offering_usernames() == {"user-01", "user-02"}
        assert mock_api.sync_all.call_count == 2

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_does_not_reuse_filtered_offering_users_cache(self, mock_api):
        """Known-username fetch is a separate API call from _get_cached_offering_users."""
        mock_api.sync_all.side_effect = [
            [_make_offering_user("filtered-ok")],
            [_make_offering_user("filtered-ok"), _make_offering_user("deleted")],
        ]
        processor = _make_membership_processor()

        processor._get_cached_offering_users()
        known = processor._get_known_offering_usernames()

        assert mock_api.sync_all.call_count == 2
        assert known == {"filtered-ok", "deleted"}
        first_kwargs = mock_api.sync_all.call_args_list[0].kwargs
        second_kwargs = mock_api.sync_all.call_args_list[1].kwargs
        assert first_kwargs.get("is_restricted") is False
        assert "is_restricted" not in second_kwargs

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_fetch_error_propagates(self, mock_api):
        """A listing failure must not be swallowed (fail closed: no removals)."""
        mock_api.sync_all.side_effect = RuntimeError("waldur down")
        processor = _make_membership_processor()

        with pytest.raises(RuntimeError, match="waldur down"):
            processor._get_known_offering_usernames()
        assert processor._known_offering_usernames_cache is None


# ---------------------------------------------------------------------------
# Team cache (OfferingMembershipProcessor)
# ---------------------------------------------------------------------------


class TestTeamCache:
    """Test _get_waldur_resource_team per-project caching."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_same_project_uses_cache(self, mock_api):
        """Two resources in the same project share a single team API call."""
        project_uuid = uuid.uuid4()
        resource_a = _make_waldur_resource(project_uuid)
        resource_b = _make_waldur_resource(project_uuid)
        resource_b.uuid = uuid.uuid4()  # different resource, same project

        team = [_make_project_user("member-01")]
        mock_api.sync.return_value = team

        processor = _make_membership_processor()

        result_a = processor._get_waldur_resource_team(resource_a)
        result_b = processor._get_waldur_resource_team(resource_b)

        assert result_a is result_b
        assert mock_api.sync.call_count == 1

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_team_list")
    def test_different_projects_get_separate_cache(self, mock_api):
        """Resources in different projects each trigger their own API call."""
        resource_a = _make_waldur_resource()
        resource_b = _make_waldur_resource()

        team_a = [_make_project_user("member-a")]
        team_b = [_make_project_user("member-b")]
        mock_api.sync.side_effect = [team_a, team_b]

        processor = _make_membership_processor()

        result_a = processor._get_waldur_resource_team(resource_a)
        result_b = processor._get_waldur_resource_team(resource_b)

        assert result_a is not result_b
        assert mock_api.sync.call_count == 2


# ---------------------------------------------------------------------------
# Service accounts cache (OfferingMembershipProcessor)
# ---------------------------------------------------------------------------


class TestServiceAccountsCache:
    """Test _sync_resource_service_accounts per-project caching."""

    @mock.patch(
        "waldur_site_agent.common.processors"
        ".marketplace_provider_offerings_list_project_service_accounts_list"
    )
    def test_same_project_uses_cache(self, mock_api):
        """Two resources in the same project share a single service accounts API call."""
        project_uuid = uuid.uuid4()
        resource_a = _make_waldur_resource(project_uuid)
        resource_b = _make_waldur_resource(project_uuid)
        resource_b.uuid = uuid.uuid4()

        accounts = [_make_service_account("svc-01", project_uuid=project_uuid)]
        mock_api.sync_all.return_value = accounts

        processor = _make_membership_processor()

        processor._sync_resource_service_accounts(resource_a)
        processor._sync_resource_service_accounts(resource_b)

        assert mock_api.sync_all.call_count == 1
        # Backend should have been called for both resources with the account.
        assert processor.resource_backend.add_users_to_resource.call_count == 2
        for call in processor.resource_backend.add_users_to_resource.call_args_list:
            assert call.args[1] == {"svc-01"}

    @mock.patch(
        "waldur_site_agent.common.processors"
        ".marketplace_provider_offerings_list_project_service_accounts_list"
    )
    def test_listing_failure_is_best_effort(self, mock_api):
        """A failed listing is swallowed (never propagated), so account sync
        cannot ERR the resource, and no users are touched on the backend."""
        mock_api.sync_all.side_effect = RuntimeError("boom")
        processor = _make_membership_processor()
        resource = _make_waldur_resource()

        # Must not raise — the caller (e.g. _process_resources) would otherwise
        # mark the whole resource ERRED.
        processor._sync_resource_service_accounts(resource)

        processor.resource_backend.add_users_to_resource.assert_not_called()
        processor.resource_backend.remove_users_from_resource.assert_not_called()

    @mock.patch(
        "waldur_site_agent.common.processors"
        ".marketplace_provider_offerings_list_project_service_accounts_list"
    )
    def test_offering_fetched_once_and_filtered_per_project(self, mock_api):
        """One offering-scoped fetch is shared across projects; accounts are
        filtered to each resource's project client-side."""
        resource_a = _make_waldur_resource()
        resource_b = _make_waldur_resource()

        account_a = _make_service_account("svc-a", project_uuid=resource_a.project_uuid)
        account_b = _make_service_account("svc-b", project_uuid=resource_b.project_uuid)
        mock_api.sync_all.return_value = [account_a, account_b]

        processor = _make_membership_processor()

        processor._sync_resource_service_accounts(resource_a)
        processor._sync_resource_service_accounts(resource_b)

        # Single offering-level fetch, cached and reused for the second project.
        assert mock_api.sync_all.call_count == 1
        # Each resource only received its own project's account.
        calls = processor.resource_backend.add_users_to_resource.call_args_list
        assert calls[0].args[0] is resource_a
        assert calls[0].args[1] == {"svc-a"}
        assert calls[1].args[0] is resource_b
        assert calls[1].args[1] == {"svc-b"}


# ---------------------------------------------------------------------------
# Course accounts cache (OfferingMembershipProcessor)
# ---------------------------------------------------------------------------


class TestCourseAccountsCache:
    """Test _sync_resource_course_accounts per-project caching."""

    @mock.patch(
        "waldur_site_agent.common.processors"
        ".marketplace_provider_offerings_list_course_accounts_list"
    )
    def test_same_project_uses_cache(self, mock_api):
        """Two resources in the same project share a single course accounts API call."""
        project_uuid = uuid.uuid4()
        resource_a = _make_waldur_resource(project_uuid)
        resource_b = _make_waldur_resource(project_uuid)
        resource_b.uuid = uuid.uuid4()

        accounts = [_make_course_account("course-01", project_uuid=project_uuid)]
        mock_api.sync_all.return_value = accounts

        processor = _make_membership_processor()

        processor._sync_resource_course_accounts(resource_a)
        processor._sync_resource_course_accounts(resource_b)

        assert mock_api.sync_all.call_count == 1
        assert processor.resource_backend.add_users_to_resource.call_count == 2

    @mock.patch(
        "waldur_site_agent.common.processors"
        ".marketplace_provider_offerings_list_course_accounts_list"
    )
    def test_offering_fetched_once_and_filtered_per_project(self, mock_api):
        """One offering-scoped fetch is shared across projects; accounts are
        filtered to each resource's project client-side."""
        resource_a = _make_waldur_resource()
        resource_b = _make_waldur_resource()

        account_a = _make_course_account("course-a", project_uuid=resource_a.project_uuid)
        account_b = _make_course_account("course-b", project_uuid=resource_b.project_uuid)
        mock_api.sync_all.return_value = [account_a, account_b]

        processor = _make_membership_processor()

        processor._sync_resource_course_accounts(resource_a)
        processor._sync_resource_course_accounts(resource_b)

        # Single offering-level fetch, cached and reused for the second project.
        assert mock_api.sync_all.call_count == 1
        calls = processor.resource_backend.add_users_to_resource.call_args_list
        assert calls[0].args[0] is resource_a
        assert calls[0].args[1] == {"course-a"}
        assert calls[1].args[0] is resource_b
        assert calls[1].args[1] == {"course-b"}
