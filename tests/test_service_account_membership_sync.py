"""Membership sync must not treat service / course accounts as stale project members.

Service and course accounts are not project-team members, so the team-vs-backend
diff in ``_group_resource_usernames`` sees them only on the backend side. They
must still be left in place: they are owned by ``_sync_resource_service_accounts``
/ ``_sync_resource_course_accounts``, which remove them only when CLOSED.
Removing an association also cancels the account's running jobs.
"""

from __future__ import annotations

from unittest import mock

from waldur_api_client.models.service_account_state import ServiceAccountState

from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import processors

from tests.test_processor_caching import (
    _make_course_account,
    _make_membership_processor,
    _make_project_user,
    _make_service_account,
    _make_waldur_resource,
)

MEMBER = "member-01"
DEPARTED = "departed-01"
SERVICE_ACCOUNT = "svc-01"
COURSE_ACCOUNT = "course-01"


def _recording_backend() -> mock.Mock:
    """Resource backend mock that records add/remove calls in order in ``.events``."""
    backend = mock.Mock()
    backend.events = []
    backend.skip_resource_team_diff = False
    backend.user_resolve_method = None
    backend.fetch_consented_users_only = False

    def _add(_resource, usernames, **_kwargs):
        backend.events.extend(("add", username) for username in sorted(usernames))
        return set(usernames)

    def _remove(_resource, usernames, **_kwargs):
        backend.events.extend(("remove", username) for username in sorted(usernames))

    backend.add_users_to_resource.side_effect = _add
    backend.remove_users_from_resource.side_effect = _remove
    return backend


def _removed(backend: mock.Mock) -> set[str]:
    return {username for action, username in backend.events if action == "remove"}


def _setup(account_state: ServiceAccountState = ServiceAccountState.OK):
    processor = _make_membership_processor()
    processor.resource_backend = _recording_backend()
    processor.offering.backend_settings = {}
    processor.offering.username_reconciliation_enabled = False

    resource = _make_waldur_resource()
    resource.restrict_member_access = False
    processor._team_cache = {resource.project_uuid.hex: [_make_project_user(MEMBER)]}
    processor._service_accounts_cache = {
        processor.offering.uuid: [
            _make_service_account(
                SERVICE_ACCOUNT, state=account_state, project_uuid=resource.project_uuid
            ),
            # Same offering, another project: must not shield a same-named user here.
            _make_service_account(DEPARTED),
        ]
    }
    processor._course_accounts_cache = {
        processor.offering.uuid: [
            _make_course_account(
                COURSE_ACCOUNT, state=account_state, project_uuid=resource.project_uuid
            )
        ]
    }
    backend_info = BackendResourceInfo(
        backend_id=resource.backend_id,
        users=[MEMBER, DEPARTED, SERVICE_ACCOUNT, COURSE_ACCOUNT],
    )
    return processor, resource, backend_info


def _run_full_sync(processor, resource, backend_info) -> None:
    stubs = mock.patch.multiple(
        processor,
        _refresh_local_offering_users=mock.Mock(return_value=[]),
        _sync_user_profiles_to_backend=mock.DEFAULT,
        _fetch_source_project=mock.Mock(return_value=None),
        _sync_resource_status=mock.DEFAULT,
        _sync_resource_limits=mock.DEFAULT,
        _sync_resource_user_limits=mock.DEFAULT,
    )
    refresh_last_sync = mock.patch.object(
        processors, "marketplace_provider_resources_refresh_last_sync"
    )
    with stubs, refresh_last_sync:
        processor._process_resources({resource.backend_id: (resource, backend_info)})


def test_full_sync_does_not_remove_active_service_and_course_accounts():
    """_process_resources: OK service/course accounts stay associated.

    Previously the stale diff removed them (cancelling their jobs) and the
    service/course account sync that runs right after re-added them.
    """
    processor, resource, backend_info = _setup()

    _run_full_sync(processor, resource, backend_info)

    backend = processor.resource_backend
    assert _removed(backend) == {DEPARTED}, backend.events


def test_event_sync_does_not_remove_active_service_and_course_accounts():
    """process_resource_user_sync: OK service/course accounts stay associated.

    The event-driven path never runs the service/course account sync, so an
    account removed here was not re-added until the next full sync.
    """
    processor, resource, backend_info = _setup()
    processor.resource_backend.pull_resources.return_value = {
        resource.backend_id: (resource, backend_info)
    }

    with mock.patch.multiple(
        processor,
        _get_waldur_resources=mock.Mock(return_value=[resource]),
        _refresh_local_offering_users=mock.Mock(return_value=[]),
        _sync_user_profiles_to_backend=mock.DEFAULT,
    ):
        processor.process_resource_user_sync(resource.uuid.hex)

    backend = processor.resource_backend
    assert _removed(backend) == {DEPARTED}, backend.events


def test_closed_service_and_course_accounts_are_still_removed():
    processor, resource, backend_info = _setup(account_state=ServiceAccountState.CLOSED)

    _run_full_sync(processor, resource, backend_info)

    backend = processor.resource_backend
    assert _removed(backend) == {DEPARTED, SERVICE_ACCOUNT, COURSE_ACCOUNT}, backend.events


def test_erred_accounts_are_left_alone():
    """Only CLOSED accounts are removed; the account sync leaves ERRED ones alone too."""
    processor, resource, backend_info = _setup(account_state=ServiceAccountState.ERRED)

    _run_full_sync(processor, resource, backend_info)

    backend = processor.resource_backend
    assert _removed(backend) == {DEPARTED}, backend.events


def test_account_listing_failure_skips_stale_removal():
    """If the accounts can't be listed, no stale user is removed that pass."""
    processor, resource, backend_info = _setup()
    processor._service_accounts_cache = {}

    with mock.patch.object(
        processors,
        "marketplace_provider_offerings_list_project_service_accounts_list",
    ) as list_accounts:
        list_accounts.sync_all.side_effect = RuntimeError("Waldur unavailable")
        _existing, stale, *_rest = processor._group_resource_usernames(
            resource, backend_info, offering_users=[]
        )

    assert stale == set()


def test_no_service_provider_keeps_plain_team_diff():
    """Without a service provider the agent syncs no accounts, so nothing is shielded."""
    processor, resource, backend_info = _setup()
    processor.service_provider = None

    _existing, stale, *_rest = processor._group_resource_usernames(
        resource, backend_info, offering_users=[]
    )

    assert stale == {DEPARTED, SERVICE_ACCOUNT, COURSE_ACCOUNT}
