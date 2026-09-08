"""The reconcile loop driven against a mocked LdapClient.

These assert what the backend *does* to the directory: which calls, in which
order, and — for the report policy — that it makes no mutating call at all.
"""

from types import SimpleNamespace
from unittest import mock

import pytest

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_ldap.backend import LdapUsernameBackend

BASE_SETTINGS = {
    "uri": "ldap://ldap.example.com",
    "bind_dn": "cn=admin,dc=example,dc=com",
    "bind_password": "secret",
    "base_dn": "dc=example,dc=com",
}


def make_backend(**ldap_overrides):
    settings = dict(BASE_SETTINGS)
    settings.update(ldap_overrides)
    with mock.patch("waldur_site_agent_ldap.backend.LdapClient") as client_cls:
        client = client_cls.return_value
        client.default_home_base = "/home"
        client.default_login_shell = "/bin/bash"
        client.gid_range_start = 10000
        client.gid_range_end = 65000
        client.list_users.return_value = {}
        client.list_groups.return_value = {}
        backend = LdapUsernameBackend(
            backend_settings={"ldap": settings},
            offering=SimpleNamespace(name="HPC", uuid="off-1"),
        )
    return backend, backend.client


def offering_user(**overrides):
    attrs = {
        "uuid": "ou-1",
        "username": "jsmith",
        "uidnumber": 10001,
        "primarygroup": 20001,
        "home_directory": "/home/jsmith",
        "login_shell": "/bin/bash",
        "user_first_name": "John",
        "user_last_name": "Smith",
        "user_email": "john@example.com",
        "user_username": "cuid-123",
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


def ldap_entry(**overrides):
    entry = {
        "uid": ["jsmith"],
        "uidNumber": [10001],
        "gidNumber": [20001],
        "homeDirectory": ["/home/jsmith"],
        "loginShell": ["/bin/bash"],
        "cn": ["John Smith"],
        "mail": ["john@example.com"],
        "givenName": ["John"],
        "sn": ["Smith"],
    }
    entry.update(overrides)
    return entry


class TestAuthority:
    def test_waldur_mode_is_not_username_authoritative(self):
        backend, _ = make_backend(account_source="waldur")
        assert backend.is_username_authoritative is False

    def test_legacy_mode_still_owns_usernames(self):
        backend, _ = make_backend()
        assert backend.is_username_authoritative is True

    def test_get_username_returns_waldurs_without_searching(self):
        backend, client = make_backend(account_source="waldur")
        assert backend.get_username(offering_user()) == "jsmith"
        client.search_user_by_email.assert_not_called()
        client.user_exists.assert_not_called()

    def test_generate_username_refuses_to_mint(self):
        backend, _ = make_backend(account_source="waldur")
        with pytest.raises(BackendError, match="does not generate usernames"):
            backend.generate_username(offering_user())


class TestCreate:
    def test_creates_with_waldurs_ids(self):
        backend, client = make_backend(account_source="waldur")
        backend.sync_user_profiles([offering_user()])

        client.create_user_with_ids.assert_called_once()
        kwargs = client.create_user_with_ids.call_args.kwargs
        assert kwargs["username"] == "jsmith"
        assert kwargs["uid_number"] == 10001
        assert kwargs["gid_number"] == 20001
        assert kwargs["home_directory"] == "/home/jsmith"
        assert kwargs["login_shell"] == "/bin/bash"

    def test_never_allocates(self):
        backend, client = make_backend(account_source="waldur")
        backend.sync_user_profiles([offering_user()])
        client.get_next_uid.assert_not_called()
        client.get_next_gid.assert_not_called()
        client.create_user.assert_not_called()

    def test_joins_access_groups(self):
        backend, client = make_backend(
            account_source="waldur", access_groups=[{"name": "vpn", "attribute": "memberUid"}]
        )
        backend.sync_user_profiles([offering_user()])
        client.add_user_to_group.assert_called_once_with("vpn", "jsmith", "memberUid")

    def test_a_second_account_cannot_reuse_a_uid_taken_in_the_same_batch(self):
        # Both users carry 10001; the first takes it, the second must not create.
        backend, client = make_backend(account_source="waldur")
        backend.sync_user_profiles(
            [
                offering_user(),
                offering_user(username="jdoe", user_email="jane@example.com"),
            ]
        )
        assert client.create_user_with_ids.call_count == 1


class TestNoopAndUpdate:
    def test_matching_entry_is_left_alone(self):
        backend, client = make_backend(account_source="waldur")
        client.list_users.return_value = {"jsmith": ldap_entry()}
        backend.sync_user_profiles([offering_user()])
        client.create_user_with_ids.assert_not_called()
        client.update_user_attributes.assert_not_called()

    def test_profile_drift_is_rewritten(self):
        backend, client = make_backend(account_source="waldur")
        client.list_users.return_value = {"jsmith": ldap_entry(mail=["stale@example.com"])}
        backend.sync_user_profiles([offering_user()])
        client.update_user_attributes.assert_called_once()
        username, updates = client.update_user_attributes.call_args.args
        assert username == "jsmith"
        assert updates["mail"] == "john@example.com"


class TestPosixMismatch:
    def test_report_makes_no_mutating_call(self):
        backend, client = make_backend(account_source="waldur")
        client.list_users.return_value = {"jsmith": ldap_entry(uidNumber=[999])}
        backend.sync_user_profiles([offering_user()])

        client.create_user_with_ids.assert_not_called()
        client.update_user_attributes.assert_not_called()
        client.set_user_posix_attributes.assert_not_called()
        client.set_group_gid.assert_not_called()

    def test_adopt_rewrites_the_entry_and_its_personal_group(self):
        backend, client = make_backend(account_source="waldur", on_posix_mismatch="adopt")
        client.list_users.return_value = {"jsmith": ldap_entry(uidNumber=[999])}
        backend.sync_user_profiles([offering_user()])

        client.set_user_posix_attributes.assert_called_once_with(
            "jsmith",
            uid_number=10001,
            gid_number=20001,
            home_directory="/home/jsmith",
            login_shell="/bin/bash",
        )
        client.set_group_gid.assert_called_once_with("jsmith", 20001)

    def test_adopt_refuses_to_renumber_onto_another_entrys_uid(self):
        # jsmith drifted to 999, and the UID Waldur wants (10001) is already on
        # somebody else. adopt must not take it: LDAP allows duplicate uidNumbers,
        # so the write would succeed and leave two accounts owning the same files.
        backend, client = make_backend(account_source="waldur", on_posix_mismatch="adopt")
        client.list_users.return_value = {
            "jsmith": ldap_entry(uidNumber=[999]),
            "someone_else": ldap_entry(uid=["someone_else"], mail=["other@example.com"]),
        }
        backend.sync_user_profiles([offering_user()])

        client.set_user_posix_attributes.assert_not_called()
        client.set_group_gid.assert_not_called()
        client.update_user_attributes.assert_not_called()
        client.create_user_with_ids.assert_not_called()

    def test_fail_aborts_the_cycle(self):
        backend, client = make_backend(account_source="waldur", on_posix_mismatch="fail")
        client.list_users.return_value = {"jsmith": ldap_entry(gidNumber=[999])}
        # The loop catches BackendError per user and carries on, so the cycle
        # completes but nothing is written.
        backend.sync_user_profiles([offering_user()])
        client.set_user_posix_attributes.assert_not_called()


class TestSkips:
    def test_account_without_posix_ids_is_not_provisioned(self):
        backend, client = make_backend(account_source="waldur")
        backend.sync_user_profiles([offering_user(uidnumber=None, primarygroup=None)])
        client.create_user_with_ids.assert_not_called()

    def test_account_without_a_username_is_not_provisioned(self):
        backend, client = make_backend(account_source="waldur")
        backend.sync_user_profiles([offering_user(username="")])
        client.create_user_with_ids.assert_not_called()

    def test_uid_held_by_another_entry_blocks_creation(self):
        backend, client = make_backend(account_source="waldur")
        client.list_users.return_value = {"someone": ldap_entry(uid=["someone"])}
        backend.sync_user_profiles([offering_user()])
        client.create_user_with_ids.assert_not_called()


class TestReadCost:
    def test_an_n_user_reconcile_costs_one_bulk_read(self):
        # Every LdapClient method opens its own connection, so a per-user lookup
        # here would mean thousands of binds a cycle on a real offering. This is
        # the guard against that regression.
        backend, client = make_backend(account_source="waldur")
        users = [
            offering_user(
                username=f"user{i}",
                uidnumber=10000 + i,
                primarygroup=20000 + i,
                home_directory=f"/home/user{i}",
                user_email=f"user{i}@example.com",
            )
            for i in range(50)
        ]
        backend.sync_user_profiles(users)

        assert client.list_users.call_count == 1
        client.search_user.assert_not_called()
        client.user_exists.assert_not_called()
        client.search_user_by_uid_number.assert_not_called()
        assert client.create_user_with_ids.call_count == 50


class TestLegacyModeUnchanged:
    def test_legacy_still_syncs_profiles_only(self):
        backend, client = make_backend()
        client.user_exists.return_value = True
        backend.sync_user_profiles([offering_user()])

        client.list_users.assert_not_called()
        client.create_user_with_ids.assert_not_called()
        client.update_user_attributes.assert_called_once()

    def test_legacy_skips_users_absent_from_the_directory(self):
        backend, client = make_backend()
        client.user_exists.return_value = False
        backend.sync_user_profiles([offering_user()])
        client.update_user_attributes.assert_not_called()
