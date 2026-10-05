"""A user the backend could not add or remove is reported, not swallowed.

Every user is tried -- one bad account must not block the rest. The users that
failed are logged at ERROR with the cause and retried on the next pass; the
resource state and last_sync are left alone, since one member's problem is not
a failure of an allocation that works for everyone else. Before, a failed add
was folded into a ``False`` return and never reported at all.
"""

from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource_state import ResourceState

from tests.fixtures import ConcreteBackend
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import utils
from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
)

WALDUR_BASE_URL = "https://waldur.example.com"


def _resource(index: int = 1):
    resource = mock.Mock()
    resource.uuid = UUID(int=index)
    resource.backend_id = f"allocation-{index}"
    resource.name = f"resource-{index}"
    resource.state = ResourceState.OK
    resource.restrict_member_access = False
    return resource


def _backend(failing_adds=(), failing_removes=(), associated=False):
    backend = ConcreteBackend({}, {})
    backend.client = mock.Mock()
    backend.client.get_association.return_value = (
        mock.Mock() if associated else None
    )

    def create_association(username, *_args, **_kwargs):
        if username in failing_adds:
            raise BackendError(f"sacctmgr: error: cannot add {username}")
        return username

    def delete_association(username, *_args, **_kwargs):
        if username in failing_removes:
            raise BackendError(f"sacctmgr: error: cannot remove {username}")
        return username

    backend.client.create_association.side_effect = create_association
    backend.client.delete_association.side_effect = delete_association
    return backend


class TestBackendReportsFailedUsers:
    def test_add_user_raises_when_the_association_cannot_be_created(self):
        backend = _backend(failing_adds={"bob"})

        with pytest.raises(BackendError, match="bob"):
            backend.add_user(_resource(), "bob")

    def test_add_users_to_resource_tries_everyone_and_reports_failures(self):
        backend = _backend(failing_adds={"bob"})

        added = backend.add_users_to_resource(_resource(), {"alice", "bob", "carol"})

        assert added == {"alice", "carol"}
        assert list(added.failed) == ["bob"]
        assert "cannot add bob" in added.failed["bob"]

    def test_blank_username_is_skipped_not_failed(self):
        backend = _backend()

        added = backend.add_users_to_resource(_resource(), {"alice", ""})

        assert added == {"alice"}
        assert added.failed == {}

    def test_remove_users_from_resource_reports_failures(self):
        backend = _backend(failing_removes={"dave"}, associated=True)

        removed = backend.remove_users_from_resource(_resource(), {"dave", "erin"})

        assert removed == ["erin"]
        assert list(removed.failed) == ["dave"]
        assert "cannot remove dave" in removed.failed["dave"]

    def test_user_not_yet_in_the_idp_is_skipped_not_failed(self):
        backend = _backend()
        real_add_user = backend.add_user

        def add_user(resource, username, **kwargs):
            if username == "newcomer":
                raise UserNotProvisionedError("newcomer has not signed in yet")
            return real_add_user(resource, username, **kwargs)

        backend.add_user = add_user

        added = backend.add_users_to_resource(_resource(), {"alice", "newcomer"})

        assert added == {"alice"}
        assert added.failed == {}


@pytest.fixture()
def membership_processor():
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = utils.get_client(WALDUR_BASE_URL + "/api/", "token")
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.offering.backend_settings = {}
    processor.offering.username_reconciliation_enabled = False
    processor.expose_backend_error_details = True
    processor._refresh_local_offering_users = mock.Mock(return_value=[])
    processor._sync_user_profiles_to_backend = mock.Mock()
    processor._fetch_source_project = mock.Mock(return_value=None)
    processor._release_departed_users = mock.Mock()
    processor._report_membership_sync_statuses = mock.Mock()
    processor._sync_resource_service_accounts = mock.Mock()
    processor._sync_resource_course_accounts = mock.Mock()
    processor._sync_resource_status = mock.Mock()
    processor._sync_resource_limits = mock.Mock()
    processor._sync_resource_user_limits = mock.Mock()
    return processor


def _group(new=(), stale=()):
    """Return value of _group_resource_usernames: (existing, stale, new, roles, ...)."""
    return set(), set(stale), set(new), {}, {}, {}, {}, {}


def _run(processor, backend, new=(), stale=()):
    processor.resource_backend = backend
    processor._group_resource_usernames = mock.Mock(return_value=_group(new, stale))
    resource = _resource()
    processor._process_resources(
        {resource.backend_id: (resource, BackendResourceInfo(backend_id=resource.backend_id))}
    )
    return resource


def _error_lines(mock_error) -> list[str]:
    """The ERROR lines logged through the patched processors logger, formatted."""
    lines = []
    for call in mock_error.call_args_list:
        fmt, *args = call.args
        lines.append(fmt % tuple(args) if args else str(fmt))
    return lines


def _user_sync_errors(mock_error) -> list[str]:
    return [line for line in _error_lines(mock_error) if "User sync for resource" in line]


@mock.patch("waldur_site_agent.common.processors.logger.error")
@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_set_as_ok")
@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
class TestMembershipSyncLogsFailures:
    """A user the backend cannot add or remove is logged; the resource is not ERRED."""

    def test_failed_addition_is_logged_and_resource_stays_ok(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend(failing_adds={"bob"})

        _run(membership_processor, backend, new={"alice", "bob"})

        mock_mark_erred.assert_not_called()
        mock_refresh.sync_detailed.assert_called_once()
        errors = _user_sync_errors(mock_error)
        assert len(errors) == 1
        assert "could not add 1 user(s): bob" in errors[0]
        assert "cannot add bob" in errors[0]  # the cause
        assert "alice" not in errors[0]

    def test_other_users_are_still_added_and_later_steps_still_run(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend(failing_adds={"bob"})

        _run(membership_processor, backend, new={"alice", "bob", "carol"})

        added = {c.args[0] for c in backend.client.create_association.call_args_list}
        assert added == {"alice", "bob", "carol"}
        membership_processor._sync_resource_limits.assert_called_once()
        membership_processor._sync_resource_status.assert_called_once()
        mock_mark_erred.assert_not_called()

    def test_failed_removal_is_logged_and_resource_stays_ok(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend(failing_removes={"dave"}, associated=True)

        _run(membership_processor, backend, stale={"dave"})

        mock_mark_erred.assert_not_called()
        mock_refresh.sync_detailed.assert_called_once()
        errors = _user_sync_errors(mock_error)
        assert len(errors) == 1
        assert "could not remove 1 user(s): dave" in errors[0]
        assert "cannot remove dave" in errors[0]

    def test_erred_resource_is_set_ok_despite_user_failures(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend(failing_adds={"bob"})
        membership_processor.resource_backend = backend
        membership_processor._group_resource_usernames = mock.Mock(
            return_value=_group(new={"alice", "bob"})
        )
        resource = _resource()
        resource.state = ResourceState.ERRED

        membership_processor._process_resources(
            {resource.backend_id: (resource, BackendResourceInfo(backend_id=resource.backend_id))}
        )

        mock_mark_erred.assert_not_called()
        mock_set_ok.sync_detailed.assert_called_once()

    def test_message_shows_first_cause_and_caps_the_list(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        users = {f"user{i:02d}" for i in range(12)}
        backend = _backend(failing_adds=users)

        _run(membership_processor, backend, new=users)

        mock_mark_erred.assert_not_called()
        (message,) = _user_sync_errors(mock_error)
        assert "could not add 12 user(s)" in message
        assert "user00" in message and "user09" in message
        assert "user10" not in message and "user11" not in message
        assert "and 2 more" in message
        assert "cannot add user00" in message  # the first cause

    def test_a_later_backend_error_still_errs_and_the_users_were_logged(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        """A genuine backend failure in another step still marks ERRED, with its own
        message; the user failures were already logged before it happened."""
        backend = _backend(failing_adds={"bob"})
        membership_processor._sync_resource_status.side_effect = BackendError("qos boom")

        _run(membership_processor, backend, new={"alice", "bob"})

        mock_mark_erred.assert_called_once()
        message = mock_mark_erred.call_args.kwargs["error_details"]["error_message"]
        assert "qos boom" in message
        assert "bob" not in message
        assert len(_user_sync_errors(mock_error)) == 1

    def test_restricted_resource_removal_failure_is_logged_only(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend(failing_removes={"dave"}, associated=True)
        membership_processor.resource_backend = backend
        membership_processor._group_resource_usernames = mock.Mock(
            return_value=({"dave", "erin"}, set(), set(), {}, {}, {}, {}, {})
        )
        resource = _resource()
        resource.restrict_member_access = True

        membership_processor._process_resources(
            {resource.backend_id: (resource, BackendResourceInfo(backend_id=resource.backend_id))}
        )

        mock_mark_erred.assert_not_called()
        mock_refresh.sync_detailed.assert_called_once()
        (message,) = _user_sync_errors(mock_error)
        assert "could not remove" in message and "dave" in message

    def test_clean_pass_is_unchanged(
        self, mock_mark_erred, mock_refresh, mock_set_ok, mock_error, membership_processor
    ):
        backend = _backend()

        _run(membership_processor, backend, new={"alice"}, stale=set())

        mock_mark_erred.assert_not_called()
        mock_refresh.sync_detailed.assert_called_once()
        assert _user_sync_errors(mock_error) == []


class TestFailuresLoggedBeforeLaterSteps:
    @mock.patch("waldur_site_agent.common.processors.logger.error")
    def test_failure_is_logged_even_if_a_later_user_step_raises(
        self, mock_error, membership_processor
    ):
        backend = _backend(failing_adds={"bob"})
        backend.process_existing_users = mock.Mock(side_effect=RuntimeError("homedir boom"))
        membership_processor.resource_backend = backend
        membership_processor._group_resource_usernames = mock.Mock(
            return_value=_group(new={"alice", "bob"})
        )
        resource = _resource()

        with pytest.raises(RuntimeError, match="homedir boom"):
            membership_processor._sync_resource_users(
                resource, BackendResourceInfo(backend_id=resource.backend_id), []
            )

        (message,) = _user_sync_errors(mock_error)
        assert "bob" in message


class TestOrderPostProcessing:
    @mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_done")
    @mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
    @mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
    def test_order_completes_when_a_user_add_fails(self, mock_erred, mock_retrieve, mock_done):
        """A user the backend cannot add is membership sync's problem, not the order's."""
        processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
        processor.waldur_rest_client = utils.get_client(WALDUR_BASE_URL + "/api/", "token")
        processor.offering = mock.Mock()
        processor.offering.uuid = "offering-uuid"
        processor.offering.backend_settings = {}
        processor.expose_backend_error_details = True
        backend = _backend(failing_adds={"bob"})
        processor.resource_backend = backend
        processor._process_create_order = mock.Mock(return_value=True)
        processor._update_offering_users = mock.Mock(return_value=False)
        processor._build_user_attributes_mapping = mock.Mock(return_value={})
        resource = _resource()
        offering_users = [
            mock.Mock(username=name, user_username=f"cuid:{name}", state=OfferingUserState.OK)
            for name in ("alice", "bob")
        ]
        processor._post_process_order = lambda _order: processor._add_users_to_resource(
            resource, {"offering_users": offering_users, "team": []}
        )
        order = mock.Mock()
        order.uuid = UUID(int=7)
        order.state = OrderState.EXECUTING
        order.type_ = RequestTypes.CREATE
        refreshed = mock.Mock()
        refreshed.state = OrderState.EXECUTING
        mock_retrieve.sync.return_value = refreshed

        processor.process_order(order)

        mock_done.sync_detailed.assert_called_once()
        mock_erred.sync_detailed.assert_not_called()
        added = {c.args[0] for c in backend.client.create_association.call_args_list}
        assert added == {"alice", "bob"}


class TestEventPathLogsFailures:
    @mock.patch("waldur_site_agent.common.processors.logger.error")
    def test_resource_user_sync_logs_the_failed_users_without_raising(
        self, mock_error, membership_processor
    ):
        backend = _backend(failing_adds={"bob"})
        membership_processor.resource_backend = backend
        membership_processor._group_resource_usernames = mock.Mock(
            return_value=_group(new={"alice", "bob"})
        )
        resource = _resource()

        usernames = membership_processor._sync_resource_users(
            resource, BackendResourceInfo(backend_id=resource.backend_id), []
        )

        assert usernames == {"alice"}
        (message,) = _user_sync_errors(mock_error)
        assert "bob" in message and "cannot add bob" in message
