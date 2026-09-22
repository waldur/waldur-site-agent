"""release_users against a mocked LdapClient and a mocked Waldur.

The entry goes only when Waldur says nobody reads it any more: the same person
may hold the same account through a sibling offering of the provider, and a
restricted account is a suspended one, not a departed one. Every doubt keeps
the entry, and a failure to check or to delete is raised so core does not
acknowledge the deletion to Waldur.
"""

from types import SimpleNamespace
from unittest import mock

import pytest
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.types import UNSET

from waldur_site_agent_ldap_client.client import DISABLED_MARKER

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_ldap import backend as backend_module
from waldur_site_agent_ldap import reconcile

from tests.test_backend_reconcile import BASE_SETTINGS, ldap_entry, make_backend
from tests.test_backend_reconcile import offering_user as live_offering_user

USER_UUID = "user-1"
PROVIDER_UUID = "provider-1"


def departed(username="jsmith", state=OfferingUserState.REQUESTED_DELETION, **overrides):
    attrs = {
        "uuid": "ou-a",
        "username": username,
        "user_uuid": USER_UUID,
        "customer_uuid": PROVIDER_UUID,
        "offering_uuid": "off-a",
        "offering_name": "Cluster A",
        "state": state,
        "is_restricted": False,
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


def sibling(username="jsmith", state=OfferingUserState.OK, **overrides):
    attrs = {
        "uuid": "ou-b",
        "username": username,
        "offering_uuid": "off-b",
        "offering_name": "Cluster B",
        "state": state,
        "is_restricted": False,
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


@pytest.fixture
def listing():
    """Mocked provider-wide offering-user list on the Waldur side."""
    with mock.patch.object(backend_module, "marketplace_offering_users_list") as listing:
        listing.sync_all.return_value = []
        yield listing


ENTRY = {"uid": ["jsmith"], "loginShell": ["/bin/bash"], "objectClass": ["posixAccount"]}
DISABLED_ENTRY = {
    "uid": ["jsmith"],
    "loginShell": ["/usr/sbin/nologin"],
    "shadowExpire": ["1"],
    "description": [DISABLED_MARKER],
    "objectClass": ["posixAccount", "shadowAccount"],
}


def waldur_backend(**overrides):
    """A waldur-mode backend whose directory holds a live jsmith. Deletes by default here."""
    overrides.setdefault("on_departure", "delete")
    backend, client = make_backend(account_source="waldur", **overrides)
    client.user_exists.return_value = True
    client.search_user.return_value = ENTRY
    client.find_group_memberships.return_value = []
    return backend, client


class TestDefaultOfRemoveUserOnDeactivate:
    def test_waldur_mode_removes_by_default(self):
        backend, _ = make_backend(account_source="waldur")
        assert backend.remove_user_on_deactivate is True

    def test_legacy_mode_keeps_by_default(self):
        backend, _ = make_backend()
        assert backend.remove_user_on_deactivate is False

    def test_explicit_false_wins_in_waldur_mode(self):
        backend, _ = make_backend(account_source="waldur", remove_user_on_deactivate=False)
        assert backend.remove_user_on_deactivate is False

    def test_explicit_true_wins_in_legacy_mode(self):
        backend, _ = make_backend(remove_user_on_deactivate=True)
        assert backend.remove_user_on_deactivate is True


class TestDefaultOfOnDeparture:
    def test_waldur_mode_disables_by_default(self):
        backend, _ = make_backend(account_source="waldur")
        assert backend.on_departure == "disable"

    def test_legacy_mode_deletes_by_default(self):
        backend, _ = make_backend(remove_user_on_deactivate=True)
        assert backend.on_departure == "delete"

    def test_explicit_value_wins(self):
        backend, _ = make_backend(account_source="waldur", on_departure="delete")
        assert backend.on_departure == "delete"

    def test_unknown_value_is_rejected(self):
        with pytest.raises(BackendError, match="on_departure"):
            make_backend(account_source="waldur", on_departure="park")


class TestDisable:
    def test_default_waldur_mode_parks_the_entry(self, listing):
        backend, client = waldur_backend(on_departure="disable", access_groups=[{"name": "vpn"}])
        client.find_group_memberships.return_value = [
            ("vpn", "memberUid"),
            ("hpc_proj1", "memberUid"),
            ("jsmith", "memberUid"),
        ]

        backend.release_users([departed()], mock.Mock())

        client.delete_user.assert_not_called()
        client.disable_user.assert_called_once_with("jsmith")
        removed = {c.args[:3] for c in client.remove_user_from_group.call_args_list}
        # Access groups, then every project group still listing the user; the
        # personal group stays with the entry. The membership type travels with
        # each call -- dropping it would silently sweep DN-style groups as uid.
        assert ("vpn", "jsmith", "memberUid") in removed
        assert ("hpc_proj1", "jsmith", "memberUid") in removed
        assert not any(call[0] == "jsmith" for call in removed)

    def test_a_group_that_cannot_be_dropped_fails_the_release(self, listing):
        """Parking an entry a group still lists would acknowledge a half-done teardown.

        The membership is what grants the access; leaving it in place while
        Waldur is told the account is gone is the one outcome the sweep can
        never correct, because it stops looking.
        """
        backend, client = waldur_backend(on_departure="disable")
        client.find_group_memberships.return_value = [("hpc_proj1", "memberUid")]
        client.remove_user_from_group.side_effect = BackendError("insufficientAccessRights")

        with pytest.raises(BackendError, match="Could not release LDAP accounts"):
            backend.release_users([departed()], mock.Mock())

        client.disable_user.assert_not_called()

    def test_an_access_group_that_cannot_be_dropped_fails_the_release(self, listing):
        """A membership the user never had returns normally, so a raise is a real failure."""
        backend, client = waldur_backend(on_departure="delete", access_groups=[{"name": "vpn"}])
        client.remove_user_from_group.side_effect = BackendError("insufficientAccessRights")

        with pytest.raises(BackendError, match="Could not release LDAP accounts"):
            backend.release_users([departed()], mock.Mock())

        client.delete_user.assert_not_called()

    def test_already_disabled_entry_is_left_alone(self, listing):
        backend, client = waldur_backend(on_departure="disable")
        client.search_user.return_value = DISABLED_ENTRY
        backend.release_users([departed()], mock.Mock())
        client.disable_user.assert_not_called()
        client.delete_user.assert_not_called()

    def test_disable_failure_raises(self, listing):
        backend, client = waldur_backend(on_departure="disable")
        client.disable_user.side_effect = BackendError("ldap down")
        with pytest.raises(BackendError, match="jsmith"):
            backend.release_users([departed()], mock.Mock())

    def test_live_sibling_keeps_the_entry_enabled(self, listing):
        backend, client = waldur_backend(on_departure="disable")
        listing.sync_all.return_value = [departed(), sibling(state=OfferingUserState.OK)]
        backend.release_users([departed()], mock.Mock())
        client.disable_user.assert_not_called()


class TestRelease:
    def test_last_account_is_deleted(self, listing):
        backend, client = waldur_backend(access_groups=[{"name": "vpn"}])
        # Its own row, already in a deletion state, plus a sibling that is also gone.
        listing.sync_all.return_value = [departed(), sibling(state=OfferingUserState.DELETED)]

        backend.release_users([departed()], mock.Mock())

        client.remove_user_from_group.assert_called_once_with("vpn", "jsmith", "memberUid")
        client.delete_user.assert_called_once_with("jsmith")

    def test_provider_wide_query_is_scoped_to_the_person_and_the_provider(self, listing):
        backend, _ = waldur_backend()
        backend.release_users([departed()], mock.Mock())
        kwargs = listing.sync_all.call_args.kwargs
        assert kwargs["user_uuid"] == USER_UUID
        assert kwargs["provider_uuid"] == PROVIDER_UUID

    def test_live_sibling_with_same_username_keeps_the_entry(self, listing):
        backend, client = waldur_backend()
        listing.sync_all.return_value = [departed(), sibling(state=OfferingUserState.OK)]
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()

    def test_pending_sibling_counts_as_live(self, listing):
        backend, client = waldur_backend()
        listing.sync_all.return_value = [
            departed(),
            sibling(state=OfferingUserState.PENDING_ACCOUNT_LINKING),
        ]
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()

    def test_restricted_sibling_keeps_the_entry(self, listing):
        """Restriction is a suspension of a live account, not a departure."""
        backend, client = waldur_backend()
        listing.sync_all.return_value = [
            departed(),
            sibling(state=OfferingUserState.OK, is_restricted=True),
        ]
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()

    def test_still_live_on_this_offering_keeps_the_entry(self, listing):
        """Role removed in one project of several: Waldur still lists the account as OK."""
        backend, client = waldur_backend()
        still_live = departed(state=OfferingUserState.OK)
        listing.sync_all.return_value = [still_live]
        backend.release_users([still_live], mock.Mock())
        client.delete_user.assert_not_called()

    def test_differently_named_sibling_does_not_block(self, listing):
        """A sibling offering with its own account is a separate directory entry."""
        backend, client = waldur_backend()
        listing.sync_all.return_value = [
            departed(),
            sibling(username="john.smith", state=OfferingUserState.OK),
        ]
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_called_once_with("jsmith")

    def test_lookup_failure_keeps_the_entry_and_raises(self, listing):
        backend, client = waldur_backend()
        listing.sync_all.side_effect = RuntimeError("api down")
        with pytest.raises(BackendError, match="jsmith"):
            backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()

    def test_missing_identity_fields_keep_the_entry_and_raise(self, listing):
        backend, client = waldur_backend()
        with pytest.raises(BackendError):
            backend.release_users([departed(user_uuid=UNSET)], mock.Mock())
        client.delete_user.assert_not_called()
        listing.sync_all.assert_not_called()

    def test_no_username_is_skipped(self, listing):
        backend, client = waldur_backend()
        backend.release_users([departed(username=None)], mock.Mock())
        client.delete_user.assert_not_called()
        listing.sync_all.assert_not_called()

    def test_already_absent_entry_is_a_success(self, listing):
        """Another offering's agent got there first; nothing to do, nothing to raise."""
        backend, client = waldur_backend()
        client.search_user.return_value = None
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()

    def test_directory_failure_raises(self, listing):
        backend, client = waldur_backend()
        client.delete_user.side_effect = BackendError("ldap down")
        with pytest.raises(BackendError, match="jsmith"):
            backend.release_users([departed()], mock.Mock())

    def test_batch_finishes_before_raising(self, listing):
        """One failure must not stop the other accounts in the batch from going."""
        backend, client = waldur_backend()
        client.delete_user.side_effect = [BackendError("ldap hiccup"), None]
        with pytest.raises(BackendError, match="jsmith"):
            backend.release_users(
                [departed(), departed(uuid="ou-c", username="jdoe")], mock.Mock()
            )
        assert client.delete_user.call_count == 2

    def test_retained_when_removal_is_off(self, listing):
        backend, client = waldur_backend(remove_user_on_deactivate=False)
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_not_called()
        listing.sync_all.assert_not_called()

    def test_legacy_mode_releases_only_when_opted_in(self, listing):
        backend, client = make_backend(remove_user_on_deactivate=True)
        client.search_user.return_value = ENTRY
        backend.release_users([departed()], mock.Mock())
        client.delete_user.assert_called_once_with("jsmith")

    def test_every_access_group_is_dropped_before_deletion(self, listing):
        """A group the user was never in is not a failure: the client returns normally.

        Only a directory that still grants access raises, and that stops the
        deletion (see the release-failure cases above).
        """
        backend, client = waldur_backend(access_groups=[{"name": "vpn"}, {"name": "gpu"}])
        backend.release_users([departed()], mock.Mock())
        assert client.remove_user_from_group.call_count == 2
        client.delete_user.assert_called_once_with("jsmith")


class TestReconcileIgnoresDepartedAccounts:
    """The all-states sweep must never converge a doomed account back into existence."""

    @pytest.mark.parametrize(
        "state",
        [
            OfferingUserState.REQUESTED_DELETION,
            OfferingUserState.DELETING,
            OfferingUserState.ERROR_DELETING,
            OfferingUserState.DELETED,
        ],
    )
    def test_departed_or_deleted_account_is_not_created(self, state):
        backend, client = make_backend(account_source="waldur")
        offering_user = SimpleNamespace(
            uuid="ou-1",
            username="jsmith",
            uidnumber=10001,
            primarygroup=20001,
            home_directory="/home/jsmith",
            login_shell="/bin/bash",
            user_first_name="John",
            user_last_name="Smith",
            user_email="john@example.com",
            user_username="cuid-123",
            state=state,
        )
        backend.sync_user_profiles([offering_user])
        client.create_user_with_ids.assert_not_called()
        client.update_user_attributes.assert_not_called()

    def test_account_with_an_unset_state_is_still_reconciled(self):
        """A field that was never requested comes back UNSET; that is not a departure."""
        backend, client = make_backend(account_source="waldur")
        offering_user = SimpleNamespace(
            uuid="ou-1",
            username="jsmith",
            uidnumber=10001,
            primarygroup=20001,
            home_directory="/home/jsmith",
            login_shell="/bin/bash",
            user_first_name="John",
            user_last_name="Smith",
            user_email="john@example.com",
            user_username="cuid-123",
            state=UNSET,
        )
        backend.sync_user_profiles([offering_user])
        client.create_user_with_ids.assert_called_once()


class TestReenableOnReturn:
    """Depart -> disable -> regain: same DN, same uid, enabled again."""

    def _parked(self):
        return ldap_entry(
            loginShell=["/usr/sbin/nologin"],
            shadowExpire=["1"],
            description=[DISABLED_MARKER],
            objectClass=["posixAccount", "shadowAccount"],
        )

    def test_classify_reenables_a_parked_entry(self):
        desired, _ = reconcile.build_desired(
            live_offering_user(), default_home_base="/home", default_login_shell="/bin/bash"
        )
        decision = reconcile.classify(desired, self._parked())
        assert decision.outcome == reconcile.Outcome.REENABLE
        # The shell is restored by the enable step, not written twice.
        assert "loginShell" not in decision.updates

    def test_classify_treats_an_operator_disabled_entry_as_an_ordinary_update(self):
        desired, _ = reconcile.build_desired(
            live_offering_user(), default_home_base="/home", default_login_shell="/bin/bash"
        )
        by_hand = ldap_entry(loginShell=["/usr/sbin/nologin"], shadowExpire=["1"])
        decision = reconcile.classify(desired, by_hand)
        assert decision.outcome == reconcile.Outcome.UPDATE
        assert decision.updates == {"loginShell": "/bin/bash"}

    def test_reconcile_wakes_the_entry_with_the_same_uid(self):
        backend, client = make_backend(account_source="waldur", access_groups=[{"name": "vpn"}])
        client.list_users.return_value = {"jsmith": self._parked()}

        backend.sync_user_profiles([live_offering_user(state=OfferingUserState.OK)])

        client.create_user_with_ids.assert_not_called()
        client.enable_user.assert_called_once_with("jsmith", "/bin/bash")
        client.add_user_to_group.assert_called_once_with("vpn", "jsmith", "memberUid")

    def test_full_cycle_depart_disable_return(self, listing):
        """The same backend parks on departure and wakes on return; the uid never moves."""
        backend, client = waldur_backend(on_departure="disable")
        backend.release_users([departed()], mock.Mock())
        client.disable_user.assert_called_once_with("jsmith")

        client.list_users.return_value = {"jsmith": self._parked()}
        backend.sync_user_profiles([live_offering_user(state=OfferingUserState.OK)])
        client.enable_user.assert_called_once_with("jsmith", "/bin/bash")
        client.create_user_with_ids.assert_not_called()
        client.set_user_posix_attributes.assert_not_called()


def test_base_settings_are_unchanged_by_the_fixture():
    assert "remove_user_on_deactivate" not in BASE_SETTINGS
    assert "on_departure" not in BASE_SETTINGS
