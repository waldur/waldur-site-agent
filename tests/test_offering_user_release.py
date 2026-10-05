"""Core hands departed offering users to the username backend's release hook.

Two triggers: right after a user's resource associations are removed (role
revoked, or stale on a full sync), and a per-cycle sweep over the offering users
Waldur has moved into a deletion state. Both go through
``AbstractUsernameManagementBackend.release_users`` and cost nothing for a
backend that leaves the hook as the inherited no-op.
"""

from __future__ import annotations

import uuid
from unittest import mock

import pytest
from waldur_api_client.models.offering_user_state import OfferingUserState

from waldur_site_agent.backend.backends import (
    DEPARTED_OFFERING_USER_STATES,
    AbstractUsernameManagementBackend,
)
from waldur_site_agent.backend import backends as backends_module
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import processors, utils

from tests.test_processor_caching import (
    _make_membership_processor,
    _make_offering_user,
    _make_project_user,
    _make_waldur_resource,
)

MEMBER = "member-01"
DEPARTED = "departed-01"
UNMANAGED = "legacy-01"


class _ReleasingBackend(AbstractUsernameManagementBackend):
    """A username backend that implements the release hook."""

    def __init__(self):
        super().__init__()
        self.released: list[list] = []

    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None

    def release_users(self, offering_users, waldur_rest_client):
        self.released.append(list(offering_users))


class _PlainBackend(AbstractUsernameManagementBackend):
    """A username backend that leaves the hook alone."""

    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None


def _patch_backend(backend):
    return mock.patch.object(
        utils, "get_username_management_backend", return_value=(backend, "1.0")
    )


class TestReleaseCapableBackend:
    def test_backend_without_override_is_not_release_capable(self):
        with _patch_backend(_PlainBackend()):
            assert utils.get_release_capable_username_backend(mock.Mock()) is None

    def test_backend_with_override_is_returned(self):
        backend = _ReleasingBackend()
        with _patch_backend(backend):
            assert utils.get_release_capable_username_backend(mock.Mock()) is backend

    def test_resolution_failure_is_swallowed(self):
        with mock.patch.object(
            utils, "get_username_management_backend", side_effect=RuntimeError("boom")
        ):
            assert utils.get_release_capable_username_backend(mock.Mock()) is None

    def test_release_errors_never_propagate(self):
        backend = mock.Mock()
        backend.release_users.side_effect = RuntimeError("directory down")
        utils.release_offering_users(
            backend, mock.Mock(name="off"), [_make_offering_user(DEPARTED)], mock.Mock()
        )

    def test_empty_list_makes_no_call(self):
        backend = mock.Mock()
        utils.release_offering_users(backend, mock.Mock(), [], mock.Mock())
        backend.release_users.assert_not_called()


class TestReleaseDepartedUsers:
    """_release_departed_users: usernames -> this offering's offering users -> hook."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_only_offering_users_are_released(self, mock_api):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        mock_api.sync_all.return_value = [_make_offering_user(MEMBER), departed]
        backend = _ReleasingBackend()
        processor = _make_membership_processor()

        with _patch_backend(backend):
            processor._release_departed_users({DEPARTED, UNMANAGED})

        # The unmanaged name never resolved, so the backend was not asked about it.
        assert backend.released == [[departed]]
        mock_api.sync_all.assert_called_once()
        assert mock_api.sync_all.call_args.kwargs["offering_uuid"] == [processor.offering.uuid]
        assert mock_api.sync_all.call_args.kwargs["field"] == utils.RELEASE_OFFERING_USER_FIELDS

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_restricted_and_live_offering_users_are_still_handed_over(self, mock_api):
        """Whether the account may go is the backend's decision, not core's."""
        live = _make_offering_user(DEPARTED, state=OfferingUserState.OK)
        live.is_restricted = True
        mock_api.sync_all.return_value = [live]
        backend = _ReleasingBackend()
        processor = _make_membership_processor()

        with _patch_backend(backend):
            processor._release_departed_users({DEPARTED})

        assert backend.released == [[live]]

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_backend_without_hook_costs_no_request(self, mock_api):
        processor = _make_membership_processor()
        with _patch_backend(_PlainBackend()):
            processor._release_departed_users({DEPARTED})
        mock_api.sync_all.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_empty_set_costs_no_request(self, mock_api):
        processor = _make_membership_processor()
        with _patch_backend(_ReleasingBackend()):
            processor._release_departed_users(set())
        mock_api.sync_all.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_lookup_failure_keeps_accounts(self, mock_api):
        mock_api.sync_all.side_effect = RuntimeError("api down")
        backend = _ReleasingBackend()
        processor = _make_membership_processor()
        with _patch_backend(backend):
            processor._release_departed_users({DEPARTED})
        assert backend.released == []

    def test_backend_is_resolved_once_per_processor(self):
        processor = _make_membership_processor()
        backend = _ReleasingBackend()
        with _patch_backend(backend) as resolve:
            assert processor._release_capable_username_backend() is backend
            assert processor._release_capable_username_backend() is backend
        resolve.assert_called_once()


class TestRequestedDeletionSweep:
    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_sweep_pulls_once_and_tears_each_down(self, mock_api):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        other = _make_offering_user("other-01", state=OfferingUserState.DELETING)
        mock_api.sync_all.return_value = [departed, other]
        processor = _make_membership_processor()
        report = {"acc-0": (_make_waldur_resource(), BackendResourceInfo(users=[DEPARTED]))}
        processor.resource_backend.pull_resources.return_value = report

        with (
            mock.patch.object(processor, "_get_waldur_resources", return_value=["r"]),
            mock.patch.object(processor, "process_offering_user_deletion") as teardown,
        ):
            processor._process_requested_deletions()

        # One users-only pull for the whole batch, handed to every teardown.
        processor.resource_backend.pull_resources.assert_called_once_with(
            ["r"], include_usage=False, strict=True
        )
        assert [c.args for c in teardown.call_args_list] == [(departed, report), (other, report)]
        assert mock_api.sync_all.call_args.kwargs["state"] == list(DEPARTED_OFFERING_USER_STATES)

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_nothing_departed_means_no_pull(self, mock_api):
        mock_api.sync_all.return_value = []
        processor = _make_membership_processor()
        processor._process_requested_deletions()
        processor.resource_backend.pull_resources.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_a_failed_pull_defers_the_whole_sweep(self, mock_api):
        """A resource that could not be pulled must not read as "no association here".

        The teardown decides by absence, so an incomplete report would
        acknowledge the deletion and strand the association.
        """
        from waldur_site_agent.backend.exceptions import BackendError

        mock_api.sync_all.return_value = [
            _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        ]
        processor = _make_membership_processor()
        processor.resource_backend.pull_resources.side_effect = BackendError("slurmdbd is down")

        with (
            mock.patch.object(processor, "_get_waldur_resources", return_value=["r"]),
            mock.patch.object(processor, "process_offering_user_deletion") as teardown,
        ):
            processor._process_requested_deletions()

        teardown.assert_not_called()
        assert processor.resource_backend.pull_resources.call_args.kwargs["strict"] is True

    def test_a_failed_pull_leaves_a_single_teardown_for_the_next_cycle(self):
        from waldur_site_agent.backend.exceptions import BackendError

        processor = _make_membership_processor()
        processor.resource_backend.pull_resources.side_effect = BackendError("slurmdbd is down")
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        with (
            mock.patch.object(processor, "_get_waldur_resources", return_value=["r"]),
            mock.patch.object(utils, "claim_offering_user_deletion") as claim,
            mock.patch.object(utils, "complete_offering_user_deletion") as complete,
        ):
            assert processor.process_offering_user_deletion(departed) is False

        # Nothing was claimed, so Waldur still lists it for the next sweep.
        claim.assert_not_called()
        complete.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.marketplace_offering_users_list")
    def test_sweep_runs_after_profile_sync_even_without_a_username_backend(self, mock_api):
        """Associations and the acknowledgement need no username backend at all."""
        mock_api.sync_all.return_value = []
        processor = _make_membership_processor()
        with mock.patch.object(
            utils,
            "get_username_management_backend",
            return_value=(processors.UnknownUsernameManagementBackend(), "unknown"),
        ):
            processor._sync_user_profiles_to_backend([])
        mock_api.sync_all.assert_called_once()
        assert mock_api.sync_all.call_args.kwargs["state"] == list(DEPARTED_OFFERING_USER_STATES)


class TestTeardownOfferingUser:
    """teardown_offering_user: associations -> release -> acknowledge, in order."""

    def _setup(self, users=(DEPARTED,), resources=1):
        resource_backend = mock.Mock()
        resource_backend.remove_user.return_value = True
        report = {}
        for i in range(resources):
            resource = _make_waldur_resource()
            resource.backend_id = f"acc-{i}"
            report[resource.backend_id] = (resource, BackendResourceInfo(users=list(users)))
        offering = mock.Mock()
        offering.name = "HPC"
        return offering, resource_backend, report

    def _run(
        self, offering, resource_backend, report, offering_user, username_backend=None, claim=True
    ):
        with (
            mock.patch.object(utils, "claim_offering_user_deletion", return_value=claim),
            mock.patch.object(utils, "complete_offering_user_deletion") as ack,
            mock.patch.object(utils, "mark_offering_user_error_deleting") as err,
        ):
            done = processors.teardown_offering_user(
                offering, offering_user, mock.Mock(), resource_backend, username_backend, report
            )
        return done, ack, err

    def test_happy_path_runs_in_order(self):
        order: list = []
        offering, resource_backend, report = self._setup(resources=2)
        resource_backend.remove_user.side_effect = lambda resource, username, **_k: (
            order.append(("assoc", resource.backend_id)) or True
        )
        backend = _ReleasingBackend()
        original = backend.release_users
        backend.release_users = lambda users, client: (
            order.append(("release", users[0].username)),
            original(users, client),
        )
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        with (
            mock.patch.object(
                utils,
                "claim_offering_user_deletion",
                side_effect=lambda ou, client: order.append(("claim", ou.username)) or True,
            ),
            mock.patch.object(
                utils,
                "complete_offering_user_deletion",
                side_effect=lambda ou, client: order.append(("deleted", ou.username)),
            ),
        ):
            assert (
                processors.teardown_offering_user(
                    offering, departed, mock.Mock(), resource_backend, backend, report
                )
                is True
            )

        # The claim (set_deleting) comes first: nothing on the provider side is
        # touched until Waldur has confirmed the row is still going away.
        assert order == [
            ("claim", DEPARTED),
            ("assoc", "acc-0"),
            ("assoc", "acc-1"),
            ("release", DEPARTED),
            ("deleted", DEPARTED),
        ]

    def test_refused_claim_leaves_a_restored_user_alone(self):
        """Re-granted between list and teardown: Waldur refuses set_deleting, we stop."""
        offering, resource_backend, report = self._setup()
        backend = _ReleasingBackend()
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        done, ack, err = self._run(offering, resource_backend, report, departed, backend, claim=False)

        assert done is False
        resource_backend.remove_user.assert_not_called()
        assert backend.released == []
        ack.assert_not_called()
        err.assert_not_called()

    def test_claim_exception_is_swallowed_and_retried_later(self):
        offering, resource_backend, report = self._setup()
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        with (
            mock.patch.object(
                utils, "claim_offering_user_deletion", side_effect=RuntimeError("api down")
            ),
            mock.patch.object(utils, "complete_offering_user_deletion") as ack,
        ):
            assert (
                processors.teardown_offering_user(
                    offering, departed, mock.Mock(), resource_backend, None, report
                )
                is False
            )
        resource_backend.remove_user.assert_not_called()
        ack.assert_not_called()

    def test_only_resources_listing_the_user_are_touched(self):
        offering, resource_backend, report = self._setup(users=(MEMBER,), resources=3)
        report["acc-1"][1].users.append(DEPARTED)
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        done, _, _ = self._run(offering, resource_backend, report, departed)

        assert done is True
        resource_backend.remove_user.assert_called_once()
        assert resource_backend.remove_user.call_args.args[0].backend_id == "acc-1"

    def test_association_exception_blocks_acknowledgement(self):
        offering, resource_backend, report = self._setup()
        resource_backend.remove_user.side_effect = RuntimeError("sacctmgr down")
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        backend = _ReleasingBackend()

        done, ack, err = self._run(offering, resource_backend, report, departed, backend)

        assert done is False
        ack.assert_not_called()
        err.assert_called_once()
        assert backend.released == []

    def test_remove_user_returning_false_means_nothing_to_remove(self):
        """False is benign by contract; failures raise (see BaseBackend.remove_user)."""
        offering, resource_backend, report = self._setup()
        resource_backend.remove_user.return_value = False
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        done, ack, err = self._run(offering, resource_backend, report, departed)

        assert done is True
        ack.assert_called_once()
        err.assert_not_called()

    def test_sweep_and_revoke_share_one_removal_call(self):
        """Both paths hand the backend the same identity: username, CUID and role."""
        resource_backend = mock.Mock()
        resource = _make_waldur_resource()
        departed = _make_offering_user(DEPARTED)
        departed.user_username = "cuid-1"
        processors.remove_user_from_resource(resource_backend, resource, departed, "PROJECT.MEMBER")
        resource_backend.remove_user.assert_called_once_with(
            resource, DEPARTED, role_name="PROJECT.MEMBER", user_cuid="cuid-1"
        )
        resource_backend.remove_user.reset_mock()
        departed.user_username = ""
        processors.remove_user_from_resource(resource_backend, resource, departed)
        resource_backend.remove_user.assert_called_once_with(
            resource, DEPARTED, role_name="", user_cuid=None
        )

    def test_release_failure_blocks_acknowledgement(self):
        offering, resource_backend, report = self._setup()
        backend = _ReleasingBackend()
        backend.release_users = mock.Mock(side_effect=RuntimeError("ldap down"))
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.DELETING)

        done, ack, err = self._run(offering, resource_backend, report, departed, backend)

        assert done is False
        ack.assert_not_called()
        assert err.call_args.args[0] is departed

    def test_no_username_backend_still_removes_and_acknowledges(self):
        offering, resource_backend, report = self._setup()
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)

        done, ack, err = self._run(offering, resource_backend, report, departed)

        assert done is True
        resource_backend.remove_user.assert_called_once()
        assert ack.call_args.args[0] is departed
        err.assert_not_called()

    def test_no_resource_backend_skips_straight_to_release(self):
        """A username-backend-only offering has no agent-managed associations."""
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        backend = _ReleasingBackend()
        offering = mock.Mock()
        offering.name = "Directory only"

        done, ack, _ = self._run(offering, None, None, departed, backend)

        assert done is True
        assert backend.released == [[departed]]
        ack.assert_called_once()

    def test_acknowledgement_failure_is_reported_not_raised(self):
        offering, resource_backend, report = self._setup()
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        with (
            mock.patch.object(utils, "claim_offering_user_deletion", return_value=True),
            mock.patch.object(
                utils, "complete_offering_user_deletion", side_effect=RuntimeError("409")
            ),
        ):
            assert (
                processors.teardown_offering_user(
                    offering, departed, mock.Mock(), resource_backend, None, report
                )
                is False
            )

    def test_unnamed_offering_user_is_acknowledged_without_teardown(self):
        offering, resource_backend, report = self._setup()
        unnamed = _make_offering_user("", state=OfferingUserState.REQUESTED_DELETION)
        unnamed.username = None

        done, ack, _ = self._run(offering, resource_backend, report, unnamed)

        assert done is True
        resource_backend.remove_user.assert_not_called()
        ack.assert_called_once()

    def test_processor_method_pulls_when_no_report_is_given(self):
        processor = _make_membership_processor()
        processor.resource_backend.remove_user.return_value = True
        processor.resource_backend.pull_resources.return_value = {}
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        with (
            mock.patch.object(processor, "_get_waldur_resources", return_value=["r"]),
            _patch_backend(_PlainBackend()),
            mock.patch.object(utils, "claim_offering_user_deletion", return_value=True),
            mock.patch.object(utils, "complete_offering_user_deletion"),
        ):
            assert processor.process_offering_user_deletion(departed) is True
        processor.resource_backend.pull_resources.assert_called_once_with(
            ["r"], include_usage=False, strict=True
        )


class TestDeletionStateHelpers:
    def _patched(self):
        return (
            mock.patch.object(utils, "marketplace_offering_users_set_deleting"),
            mock.patch.object(utils, "marketplace_offering_users_set_deleted"),
            mock.patch.object(utils, "marketplace_offering_users_set_error_deleting"),
        )

    def test_claim_moves_requested_deletion_to_deleting(self):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        deleting, _, _ = self._patched()
        with deleting as d1:
            d1.sync_detailed.return_value = mock.Mock(status_code=200)
            assert utils.claim_offering_user_deletion(departed, mock.Mock()) is True
            d1.sync_detailed.assert_called_once()

    def test_claim_of_a_row_already_deleting_needs_no_request(self):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.DELETING)
        deleting, _, _ = self._patched()
        with deleting as d1:
            assert utils.claim_offering_user_deletion(departed, mock.Mock()) is True
            d1.sync_detailed.assert_not_called()

    def test_refused_claim_is_a_no_not_an_error(self):
        """The transition validator says the row is live again: back off quietly."""
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        deleting, _, _ = self._patched()
        with deleting as d1:
            d1.sync_detailed.return_value = mock.Mock(status_code=400)
            assert utils.claim_offering_user_deletion(departed, mock.Mock()) is False

    def test_live_row_is_never_claimed(self):
        live = _make_offering_user(DEPARTED, state=OfferingUserState.OK)
        deleting, _, _ = self._patched()
        with deleting as d1:
            assert utils.claim_offering_user_deletion(live, mock.Mock()) is False
            d1.sync_detailed.assert_not_called()

    def test_complete_marks_deleted(self):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.DELETING)
        _, deleted, _ = self._patched()
        with deleted as d2:
            d2.sync_detailed.return_value = mock.Mock(status_code=200)
            utils.complete_offering_user_deletion(departed, mock.Mock())
            d2.sync_detailed.assert_called_once()

    def test_rejected_completion_raises(self):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.DELETING)
        _, deleted, _ = self._patched()
        with deleted as d2:
            d2.sync_detailed.return_value = mock.Mock(status_code=409)
            with pytest.raises(Exception, match="DELETED"):
                utils.complete_offering_user_deletion(departed, mock.Mock())

    def test_error_deleting_is_best_effort(self):
        departed = _make_offering_user(DEPARTED, state=OfferingUserState.REQUESTED_DELETION)
        _, _, error = self._patched()
        with error as e:
            e.sync_detailed.side_effect = RuntimeError("api down")
            utils.mark_offering_user_error_deleting(departed, mock.Mock())
            e.sync_detailed.assert_called_once()


class TestRemoveUserContract:
    """BaseBackend.remove_user: True removed, False nothing to remove, raise on failure."""

    def _backend(self):
        from waldur_site_agent.backend.backends import BaseBackend

        class _B(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()

        _B.__abstractmethods__ = frozenset()
        return _B()

    def test_no_association_is_false(self):
        backend = self._backend()
        backend.client.get_association.return_value = None
        assert backend.remove_user(_make_waldur_resource(), "u") is False
        backend.client.delete_association.assert_not_called()

    def test_removed_association_is_true(self):
        backend = self._backend()
        backend.client.get_association.return_value = object()
        assert backend.remove_user(_make_waldur_resource(), "u") is True
        backend.client.delete_association.assert_called_once()

    def test_failed_delete_raises_instead_of_returning_false(self):
        from waldur_site_agent.backend.exceptions import BackendError

        backend = self._backend()
        backend.client.get_association.return_value = object()
        backend.client.delete_association.side_effect = BackendError("sacctmgr down")
        with pytest.raises(BackendError, match="sacctmgr down"):
            backend.remove_user(_make_waldur_resource(), "u")

    def test_remove_users_from_resource_still_swallows_per_user_failures(self):
        from waldur_site_agent.backend.exceptions import BackendError

        backend = self._backend()
        backend.client.get_association.return_value = object()
        backend.client.delete_association.side_effect = [BackendError("boom"), None]
        removed = backend.remove_users_from_resource(_make_waldur_resource(), ["a", "b"])
        assert removed == ["b"]


class TestHookCallSites:
    def test_role_revocation_releases_after_association_removal(self):
        processor = _make_membership_processor()
        departed = _make_offering_user(DEPARTED)
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        order: list[str] = []
        processor.resource_backend.remove_user.side_effect = lambda *_a, **_k: order.append(
            "remove"
        )
        processor.resource_backend.pull_resources.return_value = {
            resource.backend_id: (resource, BackendResourceInfo(backend_id=resource.backend_id))
        }

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[departed]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
            mock.patch.object(
                processor,
                "_release_departed_users",
                side_effect=lambda names: order.append(("release", names)),
            ),
        ):
            processor.process_user_role_changed(
                departed.user_uuid.hex, resource.project_uuid.hex, granted=False
            )

        assert order == ["remove", ("release", {DEPARTED})]
        # strict: on a revocation the pull decides absence, so a resource it
        # could not read must not pass for one with no association.
        processor.resource_backend.pull_resources.assert_called_once_with(
            [resource], include_usage=False, strict=True
        )

    def test_a_failed_removal_keeps_the_account(self):
        """A still-live association must not be released on the role-change path.

        Releasing here would disable or delete the directory entry while the
        cluster association survives; the periodic sweep retries instead.
        """
        processor = _make_membership_processor()
        departed = _make_offering_user(DEPARTED)
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        processor.resource_backend.pull_resources.return_value = {
            resource.backend_id: (resource, BackendResourceInfo(backend_id=resource.backend_id))
        }

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[departed]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
            mock.patch.object(
                processors,
                "remove_user_from_resource",
                side_effect=Exception("slurmdbd refused the association delete"),
            ),
            mock.patch.object(processor, "_release_departed_users") as release,
        ):
            processor.process_user_role_changed(
                departed.user_uuid.hex, resource.project_uuid.hex, granted=False
            )

        release.assert_not_called()

    def test_role_revocation_survives_a_failing_usage_report(self):
        """REST mode cannot run sacct; the membership change must still land."""
        from waldur_site_agent.backend.backends import BaseBackend

        class _Backend(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()
                self.client.get_resource.return_value = object()
                self.client.list_resource_users.return_value = [DEPARTED]
                self.client.get_association.return_value = object()
                self.timezone = ""
                self.backend_components = {}

            def _get_usage_report(self, _ids):
                raise RuntimeError("Command not found: /usr/bin/sacct")

        # Only the pull path is exercised; the rest of the ABC is irrelevant here.
        _Backend.__abstractmethods__ = frozenset()

        backend = _Backend()
        processor = _make_membership_processor()
        processor.resource_backend = backend
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        departed = _make_offering_user(DEPARTED)

        # Sanity: the usage-bearing pull is what the old path used, and it drops
        # the resource; the users-only pull keeps it.
        assert backend.pull_resources([resource]) == {}
        assert resource.backend_id in backend.pull_resources([resource], include_usage=False)

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[departed]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
            mock.patch.object(processor, "_release_departed_users"),
        ):
            processor.process_user_role_changed(
                departed.user_uuid.hex, resource.project_uuid.hex, granted=False
            )

        backend.client.delete_association.assert_called_once()

    def test_role_grant_does_not_release(self):
        processor = _make_membership_processor()
        processor.resource_backend.pull_resources.return_value = {}
        member = _make_offering_user(MEMBER)
        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[member]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[]),
            mock.patch.object(processor, "_release_departed_users") as release,
        ):
            processor.process_user_role_changed(member.user_uuid.hex, uuid.uuid4().hex, True)
        release.assert_not_called()

    def test_stale_users_are_released_after_removal(self):
        processor = _make_membership_processor()
        processor.offering.backend_settings = {}
        processor.resource_backend.skip_resource_team_diff = False
        processor.resource_backend.user_resolve_method = None
        processor.resource_backend.fetch_consented_users_only = False
        processor.resource_backend.add_users_to_resource.return_value = set()
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        processor._team_cache = {resource.project_uuid.hex: [_make_project_user(MEMBER)]}
        processor._service_accounts_cache = {processor.offering.uuid: []}
        processor._course_accounts_cache = {processor.offering.uuid: []}
        backend_info = BackendResourceInfo(
            backend_id=resource.backend_id, users=[MEMBER, DEPARTED]
        )
        order: list = []

        def _remove(_resource, names, **_kwargs):
            order.append(("remove", set(names)))
            return sorted(names)

        processor.resource_backend.remove_users_from_resource.side_effect = _remove

        with (
            mock.patch.object(
                processor,
                "_release_departed_users",
                side_effect=lambda names: order.append(("release", set(names))),
            ),
            mock.patch.object(processor, "_report_membership_sync_statuses"),
        ):
            processor._sync_resource_users(resource, backend_info, [_make_offering_user(MEMBER)])

        assert order == [("remove", {DEPARTED}), ("release", {DEPARTED})]

    @staticmethod
    def _ready_for_sync(backend_users):
        """A processor whose team is MEMBER, against a backend listing ``backend_users``."""
        processor = _make_membership_processor()
        processor.offering.backend_settings = {}
        processor.resource_backend.skip_resource_team_diff = False
        processor.resource_backend.user_resolve_method = None
        processor.resource_backend.fetch_consented_users_only = False
        processor.resource_backend.add_users_to_resource.return_value = set()
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        processor._team_cache = {resource.project_uuid.hex: [_make_project_user(MEMBER)]}
        processor._service_accounts_cache = {processor.offering.uuid: []}
        processor._course_accounts_cache = {processor.offering.uuid: []}
        backend_info = BackendResourceInfo(backend_id=resource.backend_id, users=backend_users)
        return processor, resource, backend_info

    def test_only_confirmed_removals_are_released(self):
        """A name the backend could not remove keeps its account.

        remove_users_from_resource logs a per-user failure and leaves that name
        out of what it returns. Releasing against the requested set instead
        would disable or delete the directory entry of someone the cluster still
        lists, and nothing would put it back.
        """
        processor, resource, backend_info = self._ready_for_sync([MEMBER, DEPARTED, UNMANAGED])
        # DEPARTED went; the removal of UNMANAGED failed and was swallowed.
        processor.resource_backend.remove_users_from_resource.return_value = [DEPARTED]

        with (
            mock.patch.object(processor, "_release_departed_users") as release,
            mock.patch.object(processor, "_report_membership_sync_statuses"),
        ):
            processor._sync_resource_users(resource, backend_info, [_make_offering_user(MEMBER)])

        release.assert_called_once_with({DEPARTED})

    def test_a_backend_that_returns_nothing_releases_as_before(self):
        """An override predating the return value is trusted, not read as "none removed"."""
        processor, resource, backend_info = self._ready_for_sync([MEMBER, DEPARTED])
        processor.resource_backend.remove_users_from_resource.return_value = None

        with (
            mock.patch.object(processor, "_release_departed_users") as release,
            mock.patch.object(processor, "_report_membership_sync_statuses"),
        ):
            processor._sync_resource_users(resource, backend_info, [_make_offering_user(MEMBER)])

        release.assert_called_once_with({DEPARTED})

    def test_a_backend_that_returns_a_bare_flag_releases_as_before(self):
        """True is not a collection of names, and must not abort the sync."""
        processor, resource, backend_info = self._ready_for_sync([MEMBER, DEPARTED])
        processor.resource_backend.remove_users_from_resource.return_value = True

        with (
            mock.patch.object(processor, "_release_departed_users") as release,
            mock.patch.object(processor, "_report_membership_sync_statuses"),
        ):
            processor._sync_resource_users(resource, backend_info, [_make_offering_user(MEMBER)])

        release.assert_called_once_with({DEPARTED})

    def test_restricted_resource_does_not_release(self):
        """Restriction suspends access; the offering user is still live in Waldur."""
        processor = _make_membership_processor()
        processor.resource_backend.skip_resource_team_diff = True
        resource = _make_waldur_resource()
        resource.restrict_member_access = True
        backend_info = BackendResourceInfo(backend_id=resource.backend_id, users=[MEMBER])

        with mock.patch.object(processor, "_release_departed_users") as release:
            processor._sync_resource_users(resource, backend_info, [])
        release.assert_not_called()


class TestRoleChangePullStrictness:
    """The revocation half of the role-change path decides absence; the grant half does not."""

    def test_a_failed_pull_on_revocation_keeps_the_account(self):
        from waldur_site_agent.backend.exceptions import BackendError

        processor = _make_membership_processor()
        departed = _make_offering_user(DEPARTED)
        resource = _make_waldur_resource()
        processor.resource_backend.pull_resources.side_effect = BackendError("sacct is down")

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[departed]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
            mock.patch.object(processor, "_release_departed_users") as release,
        ):
            processor.process_user_role_changed(
                departed.user_uuid.hex, resource.project_uuid.hex, granted=False
            )

        release.assert_not_called()

    def test_a_failed_strict_pull_still_removes_from_what_answered(self):
        """Skipping the removals would keep the access the revocation just took away.

        The release is the only thing withheld: the pull was incomplete, so the
        account may still be referred to by a resource that did not answer.
        """
        from waldur_site_agent.backend.exceptions import BackendError

        processor = _make_membership_processor()
        departed = _make_offering_user(DEPARTED)
        resource = _make_waldur_resource()
        resource.restrict_member_access = False
        processor.resource_backend.pull_resources.side_effect = BackendError(
            "one resource could not be read"
        )
        removed: list = []

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[departed]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
            mock.patch.object(
                processors,
                "remove_user_from_resource",
                side_effect=lambda *_a, **_k: removed.append(DEPARTED),
            ),
            mock.patch.object(processor, "_release_departed_users") as release,
        ):
            processor.process_user_role_changed(
                departed.user_uuid.hex, resource.project_uuid.hex, granted=False
            )

        assert removed == [DEPARTED]
        release.assert_not_called()
        # The removals run against the Waldur resources already in hand: a
        # second pull would repeat whatever writes the first one made.
        assert processor.resource_backend.pull_resources.call_count == 1

    def test_a_grant_does_not_ask_for_strict(self):
        """Nothing is released on a grant, and the next cycle adds what this pull missed."""
        processor = _make_membership_processor()
        processor.resource_backend.pull_resources.return_value = {}
        member = _make_offering_user(MEMBER)
        resource = _make_waldur_resource()

        with (
            mock.patch.object(processor, "_get_user_offering_users", return_value=[member]),
            mock.patch.object(processor, "_update_offering_users", return_value=False),
            mock.patch.object(processor, "_get_waldur_resources", return_value=[resource]),
        ):
            processor.process_user_role_changed(
                member.user_uuid.hex, resource.project_uuid.hex, granted=True
            )

        processor.resource_backend.pull_resources.assert_called_once_with(
            [resource], include_usage=False, strict=False
        )


class TestStrictReachesTheDefaultPull:
    """The default pull_resource swallows everything; strict has to survive that.

    Written against a real BaseBackend rather than a mocked ``pull_resources``:
    mocking the very method under test is how the first, inert version of
    strict passed its tests while protecting nothing.
    """

    @staticmethod
    def _failing_backend():
        from waldur_site_agent.backend.backends import BaseBackend
        from waldur_site_agent.backend.exceptions import BackendError

        class _Backend(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()
                self.client.get_resource.side_effect = BackendError("slurmdbd is down")
                self.timezone = ""
                self.backend_components = {}

        _Backend.__abstractmethods__ = frozenset()
        return _Backend()

    def test_a_failed_pull_raises_for_a_strict_caller(self):
        from waldur_site_agent.backend.exceptions import BackendError

        with pytest.raises(BackendError, match="Unable to pull resource"):
            self._failing_backend().pull_resources(
                [_make_waldur_resource()], include_usage=False, strict=True
            )

    def test_the_same_failure_is_still_swallowed_for_a_lenient_caller(self):
        assert (
            self._failing_backend().pull_resources(
                [_make_waldur_resource()], include_usage=False
            )
            == {}
        )

    def test_an_override_that_swallows_must_consult_the_flag(self):
        """The contract for a plugin that catches its own pull errors.

        The re-raise lives in the base ``pull_resource``, so an override that
        replaces it wholesale bypasses strict entirely unless it asks.
        """
        from waldur_site_agent.backend.backends import BaseBackend
        from waldur_site_agent.backend.exceptions import BackendError

        class _Plugin(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()
                self.timezone = ""
                self.backend_components = {}

            def pull_resource(self, waldur_resource):
                del waldur_resource
                try:
                    msg = "the plugin's own API is down"
                    raise BackendError(msg)
                except BackendError:
                    if self.strict_pull_requested():
                        raise
                    return None

        _Plugin.__abstractmethods__ = frozenset()
        plugin = _Plugin()

        assert plugin.pull_resources([_make_waldur_resource()], include_usage=False) == {}
        with pytest.raises(BackendError, match="Unable to pull resource"):
            plugin.pull_resources([_make_waldur_resource()], include_usage=False, strict=True)

    def test_a_resource_the_backend_does_not_have_is_not_an_error(self):
        """Real absence still drops out quietly -- it is what strict asks about.

        The backend answered: there is no such resource, so there is no
        association on it either. Only a backend that could not answer is an
        error worth stopping a teardown for.
        """
        from waldur_site_agent.backend.backends import BaseBackend

        class _Backend(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()
                self.client.get_resource.return_value = None
                self.timezone = ""
                self.backend_components = {}

        _Backend.__abstractmethods__ = frozenset()
        assert (
            _Backend().pull_resources(
                [_make_waldur_resource()], include_usage=False, strict=True
            )
            == {}
        )

    def test_the_flag_is_restored_after_a_raising_pull(self):
        """The raise leaves the thread-local as it found it.

        Asserting the next pull returns {} would prove nothing: every pull
        assigns the flag on entry, so a leak could never be observed that way.
        """
        from waldur_site_agent.backend.exceptions import BackendError

        backend = self._failing_backend()
        assert backends_module._pull_strict() is False
        with pytest.raises(BackendError):
            backend.pull_resources([_make_waldur_resource()], include_usage=False, strict=True)
        assert backends_module._pull_strict() is False


class TestUnknownBackendStrictness:
    """A plugin that should have loaded must not answer "no associations here"."""

    @staticmethod
    def _backend(requested_backend_type="slurm"):
        from waldur_site_agent.backend.backends import UnknownBackend

        return UnknownBackend(requested_backend_type)

    def test_a_strict_pull_of_resources_raises(self):
        from waldur_site_agent.backend.exceptions import BackendError

        with pytest.raises(BackendError, match="did not load"):
            self._backend().pull_resources(
                [_make_waldur_resource()], include_usage=False, strict=True
            )

    def test_an_offering_that_asked_for_no_backend_still_gets_a_report(self):
        """No membership backend is a supported configuration, not a broken one.

        Refusing here would strand every teardown on such an offering: the
        polling sweep pulls strictly each cycle and would never get past it.
        """
        assert (
            self._backend("").pull_resources(
                [_make_waldur_resource()], include_usage=False, strict=True
            )
            == {}
        )

    def test_a_strict_pull_of_nothing_is_still_empty(self):
        assert self._backend().pull_resources([], include_usage=False, strict=True) == {}

    def test_a_lenient_caller_still_gets_an_empty_report(self):
        assert self._backend().pull_resources([_make_waldur_resource()]) == {}


class TestIncludeUsageThreading:
    """Plugins written before include_usage existed are called exactly as before."""

    def test_legacy_override_without_the_parameter_is_called_without_it(self):
        from waldur_site_agent.backend.backends import BaseBackend

        seen = []

        class _Legacy(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()

            def _pull_backend_resource(self, resource_backend_id):
                seen.append(resource_backend_id)
                return BackendResourceInfo(users=["u"])

        _Legacy.__abstractmethods__ = frozenset()
        resource = _make_waldur_resource()
        report = _Legacy().pull_resources([resource], include_usage=False)
        assert seen == [resource.backend_id]
        assert report[resource.backend_id][1].users == ["u"]

    def test_override_of_pull_resource_without_the_parameter_still_works(self):
        from waldur_site_agent.backend.backends import BaseBackend

        class _CustomPull(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()

            def pull_resource(self, waldur_resource):
                return BackendResourceInfo(users=["custom"])

        _CustomPull.__abstractmethods__ = frozenset()
        resource = _make_waldur_resource()
        report = _CustomPull().pull_resources([resource], include_usage=False)
        assert report[resource.backend_id][1].users == ["custom"]

    def test_flag_is_scoped_to_the_call(self):
        from waldur_site_agent.backend.backends import BaseBackend

        seen = []

        class _Aware(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()

            def _pull_backend_resource(self, resource_backend_id):
                seen.append(backends_module._pull_include_usage())
                return BackendResourceInfo()

        _Aware.__abstractmethods__ = frozenset()
        backend = _Aware()
        backend.pull_resources([_make_waldur_resource()], include_usage=False)
        backend.pull_resources([_make_waldur_resource()])
        assert seen == [False, True]
        assert backends_module._pull_include_usage() is True

    def test_flag_is_per_thread(self):
        """Two threads pulling the same instance never see each other's flag."""
        import threading

        from waldur_site_agent.backend.backends import BaseBackend

        seen: dict[str, bool] = {}
        gate = threading.Barrier(2)

        class _Shared(BaseBackend):
            def __init__(self):
                self.client = mock.Mock()

            def _pull_backend_resource(self, resource_backend_id):
                gate.wait(timeout=5)  # both threads are mid-pull at the same time
                seen[threading.current_thread().name] = backends_module._pull_include_usage()
                return BackendResourceInfo()

        _Shared.__abstractmethods__ = frozenset()
        backend = _Shared()
        threads = [
            threading.Thread(
                name="no-usage",
                target=backend.pull_resources,
                args=([_make_waldur_resource()],),
                kwargs={"include_usage": False},
            ),
            threading.Thread(
                name="usage", target=backend.pull_resources, args=([_make_waldur_resource()],)
            ),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert seen == {"no-usage": False, "usage": True}


def test_membership_processor_class_defaults_survive_new_without_init():
    """The unit-test scaffolding builds processors via __new__; the cache must not need __init__."""
    processor = processors.OfferingMembershipProcessor.__new__(
        processors.OfferingMembershipProcessor
    )
    assert processor._release_backend is None
    assert processor._release_backend_resolved is False
