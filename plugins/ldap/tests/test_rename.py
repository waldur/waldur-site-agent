"""A Waldur-side rename of an account, end to end on an in-memory directory.

The key is the person's Waldur username, written to ``waldur_username_attribute``
(employeeNumber here) on every entry the agent creates or matches. When Waldur's
POSIX username changes, the entry carrying the key under the old name, with the
account's uidNumber, is moved to the new name. Every other UID held by another
entry stays a refused collision, on the periodic pass and on a single event.
"""

from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
from waldur_api_client.models.offering_user_state import OfferingUserState

from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing import utils as event_utils
from waldur_site_agent_ldap import backend as backend_module

from tests.test_project_groups import (
    BASE_DN,
    CLUSTER_DN,
    ENABLED,
    MockLdapClient,
    SETTINGS,
    api_item,
    backend_on,
    read,
    rest_client,
)


KEY = "employeeNumber"


def keyed(client, **ldap):
    return backend_on(client, waldur_username_attribute=KEY, **ldap)


def key_of(client, username):
    entry = client.read_entry(client.user_dn(username), [KEY])
    values = entry.get(KEY) if entry else None
    return values[0] if values else None


def account(username, uid=10001, gid=20001, email="alice@example.org", **overrides):
    attrs = {
        "state": OfferingUserState.OK,
        "uuid": f"ou-{username}",
        "username": username,
        "uidnumber": uid,
        "primarygroup": gid,
        "home_directory": f"/home/{username}",
        "login_shell": "/bin/bash",
        "user_first_name": "Alice",
        "user_last_name": "A",
        "user_email": email,
        "user_username": "alice-cuid",
        "user_uuid": "u-1",
        "customer_uuid": "c-1",
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


def cycle(backend, accounts, groups):
    backend.sync_user_profiles(accounts)
    backend.reconcile_offering(rest_client(lambda r: httpx.Response(200, json=groups)))


def with_groups_ou(client):
    client.add_entry(f"ou=Groups,{BASE_DN}", {"objectClass": "top"})
    client.add_entry(
        f"cn=vpn,ou=Groups,{BASE_DN}",
        {"objectClass": ["top", "posixGroup"], "cn": "vpn", "gidNumber": 900},
    )


def event(backend, offering_user):
    """The STOMP offering-user event path: one offering user, retrieved on its own."""
    offering = SimpleNamespace(name="HPC", uuid="off-1")
    with mock.patch(
        "waldur_site_agent.common.utils.get_username_management_backend",
        return_value=(backend, "1.0"),
    ), mock.patch.object(
        handlers.marketplace_offering_users_retrieve, "sync", return_value=offering_user
    ):
        handlers._reconcile_offering_user(offering, offering_user.uuid, mock.Mock())


def periodic(backend, offering_users):
    """The periodic reconcile of event_process mode: the offering's full listing."""
    offering = SimpleNamespace(name="HPC", uuid="off-1")
    with mock.patch(
        "waldur_site_agent.common.utils.get_username_management_backend",
        return_value=(backend, "1.0"),
    ), mock.patch.object(event_utils, "get_client_for_offering"), mock.patch.object(
        event_utils, "marketplace_offering_users_list"
    ) as listing, mock.patch.object(handlers, "process_offering_user_deletions"):
        listing.sync_all.side_effect = [list(offering_users), []]
        event_utils._run_username_backend_reconciliation(offering)


@pytest.fixture
def client():
    return MockLdapClient(SETTINGS)


class TestRename:
    @pytest.mark.parametrize("path", ["periodic", "event"])
    def test_the_entry_follows_the_new_name(self, client, path):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        assert key_of(client, "alice") == "alice-cuid"
        renamed = account("alice2")
        if path == "event":
            event(backend, renamed)
        else:
            periodic(backend, [renamed])
        assert client.search_user("alice") is None
        entry = client.search_user("alice2")
        assert int(entry["uidNumber"][0]) == 10001
        assert entry["homeDirectory"] == ["/home/alice2"]
        assert key_of(client, "alice2") == "alice-cuid"

    def test_the_project_group_follows_in_the_same_cycle(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(backend, [account("alice")], [api_item("proj", 20003, members=("alice",))])
        cycle(backend, [account("alice2")], [api_item("proj", 20003, members=("alice2",))])
        assert read(client, "proj")["memberUid"] == ["alice2"]
        assert client.dn_under("proj", "ou=projects") in client.read_entry(
            CLUSTER_DN, ["member"]
        )["member"]

    def test_a_rename_back_to_an_earlier_name(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles([account("bob")])
        backend.sync_user_profiles([account("alice")])
        assert client.search_user("bob") is None
        assert int(client.search_user("alice")["uidNumber"][0]) == 10001

    def test_a_second_cycle_after_the_rename_writes_nothing(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(backend, [account("alice")], [api_item("proj", 20003, members=("alice",))])
        renamed = ([account("alice2")], [api_item("proj", 20003, ("alice2",))])
        cycle(backend, *renamed)
        with mock.patch.object(client, "modify_entry") as modify, mock.patch.object(
            client, "update_user_attributes"
        ) as update, mock.patch.object(client, "rename_user") as rename:
            cycle(backend, *renamed)
        modify.assert_not_called()
        update.assert_not_called()
        rename.assert_not_called()

    def test_personal_group_and_access_groups_move_too(self):
        client = MockLdapClient({**SETTINGS, "user_group_object_classes": ["top", "posixGroup"]})
        with_groups_ou(client)
        backend = keyed(
            client,
            personal_groups=True,
            user_group_object_classes=["top", "posixGroup"],
            access_groups=[{"name": "vpn"}],
        )
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles([account("alice2")])
        assert client.get_group_gid("alice") is None
        assert client.get_group_gid("alice2") == 20001
        assert client.list_group_members("alice2") == ["alice2"]
        assert client.list_group_members("vpn") == ["alice2"]


class TestStamping:
    def test_existing_matching_entries_are_stamped_on_the_first_cycle(self, client):
        backend_on(client).sync_user_profiles([account("alice")])  # written without a key
        assert key_of(client, "alice") is None
        keyed(client).sync_user_profiles([account("alice")])
        assert key_of(client, "alice") == "alice-cuid"

    def test_a_changed_waldur_username_is_restamped(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        with mock.patch.object(backend_module.logger, "info") as info:
            backend.sync_user_profiles([account("alice", user_username="alice-new-cuid")])
        assert key_of(client, "alice") == "alice-new-cuid"
        assert any(
            "Re-stamped" in c.args[0] and "alice-cuid" in c.args for c in info.call_args_list
        )

    def test_a_key_belonging_to_another_current_account_is_not_overwritten(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        # Waldur now lists alice-cuid on another account, and alice under a new key.
        other = account("other", uid=10005, gid=20005, user_uuid="u-5", email="o@x.org")
        backend.sync_user_profiles([account("alice", user_username="alice-new-cuid"), other])
        assert key_of(client, "alice") == "alice-cuid"

    def test_no_restamp_on_drift(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles([account("alice", uid=10009, user_username="alice-new-cuid")])
        assert key_of(client, "alice") == "alice-cuid"
        assert int(client.search_user("alice")["uidNumber"][0]) == 10001

    def test_no_stamp_on_uid_taken(self, client):
        backend = keyed(client)
        backend_on(client).sync_user_profiles(
            [account("bob", uid=10001, user_uuid="u-2", email="b@x.org")]
        )
        backend.sync_user_profiles([account("alice")])
        assert key_of(client, "bob") is None
        assert client.search_user("alice") is None

    def test_hidden_waldur_usernames_are_warned_about_once(self, client):
        backend = keyed(client)
        backend_module._WARNED_HIDDEN_USERNAMES.clear()
        with mock.patch.object(backend_module.logger, "warning") as warning:
            backend.sync_user_profiles([account("alice", user_username="")])
            backend.sync_user_profiles([account("alice", user_username="")])
        notices = [c for c in warning.call_args_list if "does not expose" in c.args[0]]
        assert len(notices) == 1
        assert key_of(client, "alice") is None

    def test_stamping_then_a_second_cycle_writes_nothing(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        with mock.patch.object(client, "update_user_attributes") as update:
            backend.sync_user_profiles([account("alice")])
        update.assert_not_called()


class TestRefused:
    """Every held UID the key does not tie to this account stays a collision."""

    @pytest.mark.parametrize("path", ["periodic", "event"])
    def test_another_person_with_the_same_mail_holding_the_uid(self, client, path):
        """A's entry holds B's UID and shares B's mail; its key is A's Waldur username."""
        backend = keyed(client, project_groups=ENABLED)
        a = account("nameofa", uid=100000, user_uuid="user-a", user_username="a-cuid")
        backend.sync_user_profiles([a])
        b = account("nameofb", uid=100000, user_uuid="user-b", user_username="b-cuid")
        if path == "event":
            event(backend, b)
        else:
            periodic(backend, [a, b])
        assert client.search_user("nameofa") is not None
        assert client.search_user("nameofb") is None

    @pytest.mark.parametrize("path", ["periodic", "event"])
    def test_a_stranger_without_a_key_holding_the_uid(self, client, path):
        backend_on(client).sync_user_profiles(
            [account("nameofa", uid=100000, user_uuid="user-a", user_username="a-cuid")]
        )
        backend = keyed(client)
        b = account("nameofb", uid=100000, user_uuid="user-b", user_username="b-cuid")
        if path == "event":
            event(backend, b)
        else:
            periodic(backend, [b])
        assert client.search_user("nameofa") is not None
        assert client.search_user("nameofb") is None

    def test_two_entries_carrying_the_key(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles(
            [account("copy", uid=10005, user_uuid="u-5", user_username="x", email="c@x.org")]
        )
        client.update_user_attributes("copy", {KEY: "alice-cuid"})
        backend.sync_user_profiles([account("alice2")])
        assert client.search_user("alice") is not None
        assert client.search_user("alice2") is None

    def test_the_key_on_an_entry_with_another_uid(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice", uid=10001)])
        backend.sync_user_profiles([account("alice2", uid=10002)])
        assert client.search_user("alice") is not None
        assert client.search_user("alice2") is None

    def test_posix_and_waldur_username_changed_at_once(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles([account("alice2", user_username="alice-new-cuid")])
        assert client.search_user("alice") is not None
        assert client.search_user("alice2") is None

    def test_without_the_attribute_it_never_renames_and_says_so_once(self, client):
        backend = backend_on(client)
        backend.sync_user_profiles([account("alice")])
        backend_module._WARNED_NO_RENAME_KEY.clear()
        with mock.patch.object(backend_module.logger, "warning") as warning:
            backend.sync_user_profiles([account("alice2")])
            backend.sync_user_profiles([account("alice2")])
        assert client.search_user("alice") is not None
        assert client.search_user("alice2") is None
        notices = [c for c in warning.call_args_list if "waldur_username_attribute" in c.args[0]]
        assert len(notices) == 1

    def test_the_group_does_not_name_an_account_that_was_refused(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(
            backend,
            [account("bob", email="bob@example.org", user_username="bob-cuid")],
            [api_item("proj", 20003, ())],
        )
        cycle(backend, [account("alice")], [api_item("proj", 20003, members=("alice",))])
        assert client.search_user("alice") is None
        assert read(client, "proj")["memberUid"] == []


class TestCreateRaces:
    """A STOMP event and the periodic pass both creating the same account."""

    def test_losing_the_account_create_race_reconciles_the_winners_entry(self, client):
        backend = backend_on(client)
        winner = backend_on(client)
        real_list = client.list_users

        def read_then_lose(*args, **kwargs):
            snapshot = real_list(*args, **kwargs)
            # The other writer creates the account right after our bulk read.
            winner.sync_user_profiles([account("alice", home_directory="/home/other")])
            return snapshot

        with mock.patch.object(backend.client, "list_users", side_effect=read_then_lose):
            with mock.patch.object(backend_module.logger, "exception") as exception:
                backend.sync_user_profiles([account("alice")])

        exception.assert_not_called()
        # Reconciled as an existing entry in the same call: Waldur's home wins.
        assert client.search_user("alice")["homeDirectory"] == ["/home/alice"]

    def test_losing_the_personal_group_race_keeps_the_winners_group(self):
        client = MockLdapClient({**SETTINGS, "user_group_object_classes": ["top", "posixGroup"]})
        client.add_entry(f"ou=Groups,{BASE_DN}", {"objectClass": "top"})
        real_exists = client.group_exists

        def absent_then_created(name):
            if name == "alice" and not getattr(absent_then_created, "raced", False):
                absent_then_created.raced = True
                client.ensure_group("alice", 20001, ["top", "posixGroup"])
                return False
            return real_exists(name)

        with mock.patch.object(client, "group_exists", side_effect=absent_then_created):
            state = client.ensure_group("alice", 20001, ["top", "posixGroup"])
        assert state == "exists"

    def test_a_lost_user_add_does_not_roll_back_the_group(self):
        client = MockLdapClient({**SETTINGS, "user_group_object_classes": ["top", "posixGroup"]})
        client.add_entry(f"ou=Groups,{BASE_DN}", {"objectClass": "top"})
        with mock.patch.object(
            client, "_add_user_entry", side_effect=backend_module.EntryExistsError("exists")
        ), pytest.raises(backend_module.EntryExistsError):
            client.create_user_with_ids(
                username="alice",
                first_name="A",
                last_name="A",
                email="a@x.org",
                uid_number=10001,
                gid_number=20001,
                home_directory="/home/alice",
                login_shell="/bin/bash",
            )
        assert client.get_group_gid("alice") == 20001


class TestRenameRaces:
    """A STOMP event and the periodic pass both following the same rename."""

    def test_losing_the_rename_race_finishes_the_reconcile(self, client):
        backend = keyed(client)
        winner = keyed(client)
        backend.sync_user_profiles([account("alice")])
        renamed = account("alice2")
        real_list = backend.client.list_users

        def read_then_lose(*args, **kwargs):
            snapshot = real_list(*args, **kwargs)
            winner.sync_user_profiles([renamed])  # renames between our read and our modrdn
            return snapshot

        with mock.patch.object(backend.client, "list_users", side_effect=read_then_lose):
            with mock.patch.object(backend_module.logger, "exception") as exception:
                backend.sync_user_profiles([renamed])

        exception.assert_not_called()
        assert client.search_user("alice") is None
        entry = client.search_user("alice2")
        assert int(entry["uidNumber"][0]) == 10001
        assert entry["homeDirectory"] == ["/home/alice2"]

    def test_a_vanished_old_entry_without_the_new_one_is_reported(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        real_list = backend.client.list_users

        def read_then_delete(*args, **kwargs):
            snapshot = real_list(*args, **kwargs)
            client.delete_user("alice")  # gone, and nobody renamed it
            return snapshot

        with mock.patch.object(backend.client, "list_users", side_effect=read_then_delete):
            with mock.patch.object(backend_module.logger, "exception") as exception:
                backend.sync_user_profiles([account("alice2")])
        assert exception.called
        assert client.search_user("alice2") is None

    def test_a_new_name_held_by_another_uid_is_reported(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        real_list = backend.client.list_users

        def read_then_squat(*args, **kwargs):
            snapshot = real_list(*args, **kwargs)
            keyed(client).sync_user_profiles(
                [
                    account(
                        "alice2", uid=10009, user_uuid="u-9", email="x@y.org", user_username="sq"
                    )
                ]
            )
            return snapshot

        # A real server refuses a modrdn onto an existing DN; the in-memory one
        # overwrites, so its answer is given here.
        refused = backend_module.EntryExistsError("entryAlreadyExists")
        with mock.patch.object(backend.client, "list_users", side_effect=read_then_squat):
            with mock.patch.object(backend.client, "rename_user", side_effect=refused):
                with mock.patch.object(backend_module.logger, "exception") as exception:
                    backend.sync_user_profiles([account("alice2")])
        assert exception.called
        assert client.search_user("alice") is not None
        assert int(client.search_user("alice2")["uidNumber"][0]) == 10009

    def test_losing_the_personal_group_rename_race(self):
        client = MockLdapClient({**SETTINGS, "user_group_object_classes": ["top", "posixGroup"]})
        with_groups_ou(client)
        backend = keyed(
            client,
            personal_groups=True,
            user_group_object_classes=["top", "posixGroup"],
            access_groups=[{"name": "vpn"}],
        )
        backend.sync_user_profiles([account("alice")])
        real_rename_group = client.rename_group

        def other_writer_first(old, new):
            real_rename_group(old, new)  # the other writer's modrdn lands first
            real_rename_group(old, new)  # ours then finds the group gone

        with mock.patch.object(backend.client, "rename_group", side_effect=other_writer_first):
            with mock.patch.object(backend_module.logger, "exception") as exception:
                backend.sync_user_profiles([account("alice2")])
        exception.assert_not_called()
        assert client.get_group_gid("alice2") == 20001
        assert client.list_group_members("vpn") == ["alice2"]


def groups_client():
    client = MockLdapClient({**SETTINGS, "user_group_object_classes": ["top", "posixGroup"]})
    with_groups_ou(client)
    return client


def keyed_with_groups(client):
    return keyed(
        client,
        personal_groups=True,
        user_group_object_classes=["top", "posixGroup"],
        access_groups=[{"name": "vpn"}],
    )


def assert_fully_renamed(client):
    assert client.search_user("alice") is None
    entry = client.search_user("alice2")
    assert int(entry["uidNumber"][0]) == 10001
    assert entry["homeDirectory"] == ["/home/alice2"]
    assert client.list_group_members("vpn") == ["alice2"]
    assert client.get_group_gid("alice2") == 20001
    assert client.get_group_gid("alice") is None
    assert client.list_group_members("alice2") == ["alice2"]
    assert not [
        d for d in entry.get("description") or [] if str(d).startswith("waldur-site-agent:renamed")
    ]


class TestCrashSafeRename:
    """A failure at any step of a rename is finished by the next cycle, nothing left behind."""

    def test_a_failure_after_step_one(self):
        client = groups_client()
        backend = keyed_with_groups(client)
        backend.sync_user_profiles([account("alice")])
        with mock.patch.object(
            backend.client, "rename_user", side_effect=backend_module.BackendError("down")
        ):
            backend.sync_user_profiles([account("alice2")])
        # New memberships added, entry still keyed under the old name.
        assert client.search_user("alice") is not None
        assert sorted(client.list_group_members("vpn")) == ["alice", "alice2"]
        backend.sync_user_profiles([account("alice2")])
        assert_fully_renamed(client)

    def test_a_failure_after_step_two(self):
        client = groups_client()
        backend = keyed_with_groups(client)
        backend.sync_user_profiles([account("alice")])
        with mock.patch.object(
            backend, "_finish_rename", side_effect=backend_module.BackendError("down")
        ):
            backend.sync_user_profiles([account("alice2")])
        assert client.search_user("alice") is None
        assert "alice" in client.list_group_members("vpn")  # left over, recorded as pending
        backend.sync_user_profiles([account("alice2")])
        assert_fully_renamed(client)

    def test_a_failure_during_step_three(self):
        client = groups_client()
        backend = keyed_with_groups(client)
        backend.sync_user_profiles([account("alice")])
        real_remove = backend.client.remove_user_from_group
        calls = []

        def fail_once(group, username, membership_type="memberUid"):
            calls.append(group)
            if len(calls) == 1:
                raise backend_module.BackendError("down")
            return real_remove(group, username, membership_type)

        with mock.patch.object(backend.client, "remove_user_from_group", side_effect=fail_once):
            backend.sync_user_profiles([account("alice2")])
        backend.sync_user_profiles([account("alice2")])
        assert_fully_renamed(client)

    def test_a_clean_rename_leaves_nothing_behind(self):
        client = groups_client()
        backend = keyed_with_groups(client)
        backend.sync_user_profiles([account("alice")])
        backend.sync_user_profiles([account("alice2")])
        assert_fully_renamed(client)

    def test_a_pending_old_name_taken_again_is_left_to_its_new_holder(self):
        client = groups_client()
        backend = keyed_with_groups(client)
        backend.sync_user_profiles([account("alice")])
        with mock.patch.object(
            backend, "_finish_rename", side_effect=backend_module.BackendError("down")
        ):
            backend.sync_user_profiles([account("alice2")])
        newcomer = account("alice", uid=10050, gid=20050, user_uuid="u-50", user_username="n")
        backend.sync_user_profiles([account("alice2"), newcomer])
        assert "alice" in client.list_group_members("vpn")  # the newcomer's own membership
        entry = client.search_user("alice2")
        assert not [d for d in entry.get("description") or [] if "renamed-from" in str(d)]


class TestRenameOrdering:
    def test_renames_run_before_creates(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        # In one batch: alice is renamed to alice2, and a new account takes the
        # freed name. Listed first, the newcomer must still not see "alice" held.
        newcomer = account("alice", uid=10050, gid=20050, user_uuid="u-50", user_username="n")
        backend.sync_user_profiles([newcomer, account("alice2")])
        assert int(client.search_user("alice2")["uidNumber"][0]) == 10001
        assert int(client.search_user("alice")["uidNumber"][0]) == 10050

    def test_an_entry_changed_since_the_read_is_not_renamed(self, client):
        backend = keyed(client)
        backend.sync_user_profiles([account("alice")])
        real_list = backend.client.list_users

        def read_then_change(*args, **kwargs):
            snapshot = real_list(*args, **kwargs)
            client.update_user_attributes("alice", {KEY: "someone-else"})
            return snapshot

        with mock.patch.object(backend.client, "list_users", side_effect=read_then_change):
            backend.sync_user_profiles([account("alice2")])
        assert client.search_user("alice") is not None
        assert client.search_user("alice2") is None

    def test_an_adopted_drift_updates_the_uid_index(self, client):
        backend = keyed(client, on_posix_mismatch="adopt")
        backend.sync_user_profiles([account("alice", uid=10001)])
        # Waldur moves alice to 10002; a new account then wants 10001 in the same batch.
        backend.sync_user_profiles(
            [
                account("alice", uid=10002),
                account("carol", uid=10001, gid=20009, user_uuid="u-9", user_username="c"),
            ]
        )
        assert int(client.search_user("alice")["uidNumber"][0]) == 10002
        assert int(client.search_user("carol")["uidNumber"][0]) == 10001


class TestGroupsNameOnlyMatchedAccounts:
    def test_a_drifted_account_is_not_listed(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(backend, [account("alice")], [api_item("proj", 20003, ())])
        # Waldur now says 10002; the directory still holds 10001 (report policy).
        cycle(backend, [account("alice", uid=10002)], [api_item("proj", 20003, ("alice",))])
        assert read(client, "proj")["memberUid"] == []

    def test_an_entry_with_another_persons_key_is_not_listed(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(backend, [account("alice")], [api_item("proj", 20003, ())])
        client.update_user_attributes("alice", {KEY: "someone-else"})
        # someone-else is another current account's Waldur username.
        other = account(
            "other", uid=10005, gid=20005, user_uuid="u-5", email="o@x.org",
            user_username="someone-else",
        )
        cycle(backend, [account("alice"), other], [api_item("proj", 20003, ("alice",))])
        assert read(client, "proj")["memberUid"] == []

    def test_an_entry_without_a_key_is_not_listed(self, client):
        backend_on(client).sync_user_profiles([account("bob", email="b@x.org")])  # no key
        backend = keyed(client, project_groups=ENABLED)
        backend.reconcile_offering(
            rest_client(lambda r: httpx.Response(200, json=[api_item("proj", 20003, ("bob",))]))
        )
        assert read(client, "proj")["memberUid"] == []

    def test_a_matched_account_is_listed(self, client):
        backend = keyed(client, project_groups=ENABLED)
        cycle(backend, [account("alice")], [api_item("proj", 20003, ("alice",))])
        assert read(client, "proj")["memberUid"] == ["alice"]
