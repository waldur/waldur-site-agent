"""Event-processing paths that route departed offering users to release_users.

The periodic username-backend reconciliation sweeps the whole offering; the
STOMP offering-user handler reconciles one account on an ``update`` event.
Both must send accounts in a deletion state to the release hook and everything
else to the profile sync, and both must stay free for a backend without hooks.
"""

from __future__ import annotations

import uuid
from unittest import mock

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state import OfferingUserState

from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend
from waldur_site_agent.common import structures
from waldur_site_agent.event_processing import handlers, utils


def _make_offering(**overrides) -> structures.Offering:
    defaults = dict(
        name="test-offering",
        waldur_offering_uuid="test-uuid",
        waldur_api_url="https://example.com/api/",
        waldur_api_token="token",
        backend_type="slurm",
        membership_sync_backend="slurm",
        username_management_backend="ldap",
        stomp_enabled=True,
    )
    defaults.update(overrides)
    return structures.Offering(**defaults)


def _offering_user(username: str, state: OfferingUserState) -> OfferingUser:
    return OfferingUser(uuid=uuid.uuid4(), user_uuid=uuid.uuid4(), username=username, state=state)


class _BothHooks(AbstractUsernameManagementBackend):
    def __init__(self):
        super().__init__()
        self.synced: list[list] = []
        self.released: list[list] = []

    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None

    def sync_user_profiles(self, offering_users):
        self.synced.append(list(offering_users))

    def release_users(self, offering_users, waldur_rest_client):
        self.released.append(list(offering_users))


class _ReleaseOnly(AbstractUsernameManagementBackend):
    def __init__(self):
        super().__init__()
        self.released: list[list] = []

    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None

    def release_users(self, offering_users, waldur_rest_client):
        self.released.append(list(offering_users))


class _NoHooks(AbstractUsernameManagementBackend):
    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None


def _patch_backend(backend):
    return mock.patch(
        "waldur_site_agent.common.utils.get_username_management_backend",
        return_value=(backend, "1.0"),
    )


def _patch_deletions():
    return mock.patch.object(handlers, "process_offering_user_deletions")


class TestPeriodicSweep:
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_offering_users_list")
    def test_departed_go_to_teardown_and_full_list_to_sync(self, mock_list, _client):
        live = _offering_user("alive", OfferingUserState.OK)
        departed = _offering_user("gone", OfferingUserState.REQUESTED_DELETION)
        deleted = _offering_user("old", OfferingUserState.DELETED)
        # First listing feeds the profile sync; the second, by state, the teardown.
        mock_list.sync_all.side_effect = [[live, departed, deleted], [departed]]
        backend = _BothHooks()
        offering = _make_offering()

        with _patch_backend(backend), _patch_deletions() as teardown:
            utils._run_username_backend_reconciliation(offering)

        # The profile sync keeps receiving the unfiltered list, as before; the
        # backend is expected to ignore what is not live.
        assert backend.synced == [[live, departed, deleted]]
        teardown.assert_called_once()
        assert teardown.call_args.args[2] == [departed]
        departed_query = mock_list.sync_all.call_args_list[1].kwargs
        assert departed_query["state"] == list(utils.DEPARTED_OFFERING_USER_STATES)
        # No restricted filter: a restricted user whose deletion was requested
        # while the agent was disconnected must still be torn down.
        assert "is_restricted" not in departed_query

    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_offering_users_list")
    def test_release_only_backend_still_gets_the_sweep(self, mock_list, _client):
        departed = _offering_user("gone", OfferingUserState.DELETING)
        mock_list.sync_all.side_effect = [[_offering_user("alive", OfferingUserState.OK)], [departed]]

        with _patch_backend(_ReleaseOnly()), _patch_deletions() as teardown:
            utils._run_username_backend_reconciliation(_make_offering())

        assert teardown.call_args.args[2] == [departed]

    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_offering_users_list")
    def test_backend_without_hooks_still_runs_the_deletion_sweep(self, mock_list, _client):
        """A plain SLURM offering must process Requested deletion too."""
        departed = _offering_user("gone", OfferingUserState.REQUESTED_DELETION)
        mock_list.sync_all.side_effect = [[_offering_user("alive", OfferingUserState.OK)], [departed]]
        with _patch_backend(_NoHooks()), _patch_deletions() as teardown:
            utils._run_username_backend_reconciliation(_make_offering())
        assert teardown.call_args.args[2] == [departed]

    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_offering_users_list")
    def test_periodic_loop_sweeps_offerings_without_membership_backend(self, mock_list, _c):
        """Username-backend-only offerings were skipped by the loop before."""
        departed = _offering_user("gone", OfferingUserState.REQUESTED_DELETION)
        mock_list.sync_all.side_effect = [[], [departed]]
        offering = _make_offering(membership_sync_backend=None)
        with _patch_backend(_NoHooks()), _patch_deletions() as teardown:
            utils.run_periodic_offering_user_reconciliation([offering], "agent")
        assert teardown.call_args.args[2] == [departed]


class TestStompReconcile:
    def _run(self, backend, offering_user, action="update"):
        offering = _make_offering()
        client = mock.Mock()
        with (
            _patch_backend(backend),
            mock.patch.object(
                handlers.marketplace_offering_users_retrieve, "sync", return_value=offering_user
            ),
        ):
            handlers._reconcile_offering_user(offering, offering_user.uuid.hex, client)

    def test_departed_offering_user_goes_to_teardown_not_sync(self):
        backend = _BothHooks()
        departed = _offering_user("gone", OfferingUserState.REQUESTED_DELETION)
        with _patch_deletions() as teardown:
            self._run(backend, departed)
        teardown.assert_called_once()
        assert teardown.call_args.args[2] == [departed]
        assert backend.synced == []

    def test_live_offering_user_is_synced_not_torn_down(self):
        backend = _BothHooks()
        live = _offering_user("alive", OfferingUserState.OK)
        with _patch_deletions() as teardown:
            self._run(backend, live)
        assert backend.synced == [[live]]
        teardown.assert_not_called()

    def test_departed_user_with_sync_only_backend_is_still_torn_down(self):
        """Associations and the acknowledgement do not need a release hook."""

        class _SyncOnly(_NoHooks):
            def __init__(self):
                super().__init__()
                self.synced = []

            def sync_user_profiles(self, offering_users):
                self.synced.append(list(offering_users))

        backend = _SyncOnly()
        with _patch_deletions() as teardown:
            self._run(backend, _offering_user("gone", OfferingUserState.DELETING))
        assert backend.synced == []
        teardown.assert_called_once()

    def _deletion_message(self, offering, departed):
        return {
            "offering_user_uuid": departed.uuid.hex,
            "user_uuid": departed.user_uuid.hex,
            "username": departed.username,
            "state": "Requested deletion",
            "changed_fields": ["state"],
            "action": "update",
            "offering_uuid": offering.uuid,
        }

    def test_requested_deletion_payload_end_to_end(self):
        """The STOMP update carrying state=Requested deletion drives the whole flow.

        No username-backend hooks at all: the state in the payload alone routes
        the event to the teardown, which removes associations and acknowledges.
        """
        offering = _make_offering(username_management_backend="")
        departed = _offering_user("hpc_9002", OfferingUserState.REQUESTED_DELETION)
        resource_backend = mock.Mock()
        resource_backend.pull_resources.return_value = {}
        with (
            mock.patch.object(handlers.common_utils, "get_client_for_offering"),
            mock.patch.object(handlers, "register_event_process_service"),
            _patch_backend(_NoHooks()),
            mock.patch.object(
                handlers.marketplace_offering_users_retrieve, "sync", return_value=departed
            ),
            mock.patch.object(
                handlers.common_utils,
                "get_backend_for_offering",
                return_value=(resource_backend, "1.0"),
            ),
            mock.patch.object(
                handlers.common_processors, "fetch_offering_resources", return_value=[]
            ),
            mock.patch.object(handlers.common_processors, "teardown_offering_user") as teardown,
        ):
            handlers._process_offering_user_message(
                self._deletion_message(offering, departed), offering, "agent"
            )
        teardown.assert_called_once()
        args = teardown.call_args.args
        assert args[1] is departed
        assert args[3] is resource_backend
        assert args[4] is None  # no release hook
        resource_backend.pull_resources.assert_called_once_with(
            [], include_usage=False, strict=True
        )

    def test_deletion_event_without_membership_backend_still_releases(self):
        """A username-backend-only STOMP offering: no associations, release and acknowledge."""
        offering = _make_offering(membership_sync_backend=None)
        departed = _offering_user("hpc_9002", OfferingUserState.REQUESTED_DELETION)
        backend = _ReleaseOnly()
        with (
            mock.patch.object(handlers.common_utils, "get_client_for_offering"),
            mock.patch.object(handlers, "register_event_process_service"),
            _patch_backend(backend),
            mock.patch.object(
                handlers.marketplace_offering_users_retrieve, "sync", return_value=departed
            ),
            mock.patch.object(handlers.common_utils, "get_backend_for_offering") as resolve,
            mock.patch.object(handlers.common_processors, "teardown_offering_user") as teardown,
        ):
            handlers._process_offering_user_message(
                self._deletion_message(offering, departed), offering, "agent"
            )
        resolve.assert_not_called()
        args = teardown.call_args.args
        assert args[3] is None
        assert args[4] is backend
        assert args[5] is None

    def test_deletion_event_for_a_restored_user_does_nothing(self):
        """Restored between the event and the fetch: Waldur says OK again."""
        offering = _make_offering()
        restored = _offering_user("hpc_9002", OfferingUserState.OK)
        with (
            mock.patch.object(handlers.common_utils, "get_client_for_offering"),
            mock.patch.object(handlers, "register_event_process_service"),
            mock.patch.object(
                handlers.marketplace_offering_users_retrieve, "sync", return_value=restored
            ),
            _patch_deletions() as teardown,
        ):
            handlers._process_offering_user_message(
                self._deletion_message(offering, restored), offering, "agent"
            )
        teardown.assert_not_called()

    def test_update_action_triggers_reconcile(self):
        offering = _make_offering()
        message = {
            "offering_user_uuid": "ou-1",
            "user_uuid": "u-1",
            "username": "gone",
            "action": "update",
            "offering_uuid": offering.uuid,
        }
        with (
            mock.patch.object(handlers.common_utils, "get_client_for_offering"),
            mock.patch.object(handlers, "register_event_process_service"),
            mock.patch.object(handlers, "_reconcile_offering_user") as reconcile,
        ):
            handlers._process_offering_user_message(message, offering, "agent")
        reconcile.assert_called_once()
        assert reconcile.call_args.args[1] == "ou-1"

    def test_delete_action_does_not_fetch(self):
        """A hard-deleted offering user cannot be retrieved; nothing to reconcile."""
        offering = _make_offering()
        message = {
            "offering_user_uuid": "ou-1",
            "user_uuid": "u-1",
            "username": "gone",
            "action": "delete",
            "offering_uuid": offering.uuid,
        }
        with (
            mock.patch.object(handlers.common_utils, "get_client_for_offering"),
            mock.patch.object(handlers, "register_event_process_service"),
            mock.patch.object(handlers, "_reconcile_offering_user") as reconcile,
        ):
            handlers._process_offering_user_message(message, offering, "agent")
        reconcile.assert_not_called()
