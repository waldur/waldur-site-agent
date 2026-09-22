"""LdapClient against ldap3's in-memory server.

MOCK_SYNC gives a real DIT with real add/modify/search semantics, so these cover
the behaviours that only show up against an actual directory — idempotency, the
already-exists paths, and the orphan-group rollback — without a container.
"""

from unittest.mock import MagicMock, patch

import pytest
from ldap3 import MOCK_SYNC, MODIFY_ADD, MODIFY_DELETE, OFFLINE_SLAPD_2_4, Connection, Server
from waldur_site_agent_ldap_client import LdapClient
from waldur_site_agent_ldap_client.client import _extract_uid_from_dn

from waldur_site_agent.backend.exceptions import BackendError

BASE_DN = "dc=example,dc=com"
BIND_DN = "cn=admin,dc=example,dc=com"
BIND_PASSWORD = "secret"


class MockLdapClient(LdapClient):
    """LdapClient wired to an in-memory DIT shared across connections."""

    def __init__(self, settings):
        self._server = Server("mock", get_info=OFFLINE_SLAPD_2_4)
        super().__init__(settings)
        # One bootstrap connection seeds the DIT; each _connect() gets its own
        # Connection over the same Server, which is how ldap3 shares mock state.
        conn = self._raw_connection()
        conn.strategy.add_entry(BIND_DN, {"userPassword": BIND_PASSWORD, "objectClass": "person"})
        for dn in (BASE_DN, f"ou=People,{BASE_DN}", f"ou=Groups,{BASE_DN}"):
            conn.strategy.add_entry(dn, {"objectClass": "top"})
        self._seeded = conn.strategy.entries

    def _raw_connection(self):
        return Connection(
            self._server,
            user=BIND_DN,
            password=BIND_PASSWORD,
            client_strategy=MOCK_SYNC,
        )

    def _connect(self):
        conn = self._raw_connection()
        if hasattr(self, "_seeded"):
            conn.strategy.entries = self._seeded
        conn.bind()
        return conn


@pytest.fixture
def client():
    return MockLdapClient(
        {
            "uri": "ldap://mock",
            "bind_dn": BIND_DN,
            "bind_password": BIND_PASSWORD,
            "base_dn": BASE_DN,
            # groupOfNames needs a member; posixGroup alone keeps the fixture simple.
            "user_group_object_classes": ["posixGroup", "top"],
        }
    )


def create(client, username="jsmith", uid=10001, gid=20001):
    client.create_user_with_ids(
        username=username,
        first_name="John",
        last_name="Smith",
        email=f"{username}@example.com",
        uid_number=uid,
        gid_number=gid,
        home_directory=f"/home/{username}",
        login_shell="/bin/bash",
    )


class TestCreateUserWithIds:
    def test_writes_the_supplied_ids(self, client):
        create(client)
        entry = client.search_user("jsmith")
        assert int(entry["uidNumber"][0]) == 10001
        assert int(entry["gidNumber"][0]) == 20001
        assert entry["homeDirectory"][0] == "/home/jsmith"
        assert entry["loginShell"][0] == "/bin/bash"

    def test_creates_the_personal_group_with_the_same_gid(self, client):
        create(client)
        assert client.get_group_gid("jsmith") == 20001

    def test_search_returns_the_attributes_the_reconciler_diffs(self, client):
        create(client)
        entry = client.search_user("jsmith")
        for name in ("uidNumber", "gidNumber", "homeDirectory", "loginShell", "mail"):
            assert name in entry, name


class TestEnsureGroup:
    def test_creates_when_absent(self, client):
        assert client.ensure_group("proj", 30001, ["posixGroup", "top"]) == "created"
        assert client.get_group_gid("proj") == 30001

    def test_is_idempotent(self, client):
        client.ensure_group("proj", 30001, ["posixGroup", "top"])
        assert client.ensure_group("proj", 30001, ["posixGroup", "top"]) == "exists"

    def test_reports_a_gid_conflict_instead_of_raising(self, client):
        client.ensure_group("proj", 30001, ["posixGroup", "top"])
        assert client.ensure_group("proj", 39999, ["posixGroup", "top"]) == "conflict"


class TestIdempotency:
    def test_a_group_left_behind_by_a_previous_run_does_not_break_creation(self, client):
        # The failure mode the original create path could leave behind.
        client.ensure_group("jsmith", 20001, ["posixGroup", "top"])
        create(client)
        assert client.search_user("jsmith") is not None

    def test_a_stale_group_with_the_wrong_gid_is_refused(self, client):
        client.ensure_group("jsmith", 39999, ["posixGroup", "top"])
        with pytest.raises(BackendError, match="different gidNumber"):
            create(client)

    def test_a_failed_user_add_does_not_orphan_the_group(self, client):
        create(client)
        # Second attempt: the user entry already exists, so the add fails. The
        # group was already there, so nothing should be rolled back either.
        with pytest.raises(BackendError):
            create(client)
        assert client.get_group_gid("jsmith") == 20001


class TestBulkReads:
    def test_list_users_keys_by_uid(self, client):
        create(client, "jsmith", 10001, 20001)
        create(client, "jdoe", 10002, 20002)
        users = client.list_users()
        assert set(users) == {"jsmith", "jdoe"}
        assert int(users["jdoe"]["uidNumber"][0]) == 10002

    def test_list_groups_maps_name_to_gid(self, client):
        create(client, "jsmith", 10001, 20001)
        client.ensure_group("proj", 30001, ["posixGroup", "top"])
        groups = client.list_groups()
        assert groups["jsmith"] == 20001
        assert groups["proj"] == 30001

    def test_search_user_by_uid_number_finds_the_holder(self, client):
        create(client, "jsmith", 10001, 20001)
        found = client.search_user_by_uid_number(10001)
        assert found["uid"][0] == "jsmith"

    def test_search_user_by_uid_number_returns_none_when_free(self, client):
        assert client.search_user_by_uid_number(65000) is None


class TestPosixSetters:
    def test_set_user_posix_attributes_replaces_only_what_is_given(self, client):
        create(client)
        client.set_user_posix_attributes("jsmith", uid_number=11111)
        entry = client.search_user("jsmith")
        assert int(entry["uidNumber"][0]) == 11111
        assert int(entry["gidNumber"][0]) == 20001

    def test_set_group_gid(self, client):
        create(client)
        client.set_group_gid("jsmith", 29999)
        assert client.get_group_gid("jsmith") == 29999


class TestGroupDescriptions:
    MARKER = "managed_by=waldur-site-agent;resource=deadbeef"

    def test_missing_group_has_no_descriptions(self, client):
        assert client.get_group_descriptions("absent") is None

    def test_group_without_description_reads_empty(self, client):
        client.ensure_group("proj", 30001, ["posixGroup", "top"])
        assert client.get_group_descriptions("proj") == []

    def test_add_keeps_existing_values_and_is_idempotent(self, client):
        client.ensure_group(
            "proj", 30001, ["posixGroup", "top"], extra_attributes={"description": "by hand"}
        )
        client.add_group_description("proj", self.MARKER)
        client.add_group_description("proj", self.MARKER)
        assert sorted(client.get_group_descriptions("proj")) == sorted(["by hand", self.MARKER])

    def test_find_by_description_matches_only_marked_groups(self, client):
        client.ensure_group("mine", 30001, ["posixGroup", "top"])
        client.ensure_group("theirs", 30002, ["posixGroup", "top"])
        client.add_group_description("mine", self.MARKER)
        client.add_group_description("theirs", "managed_by=waldur-site-agent;resource=other")
        assert client.find_groups_by_description(self.MARKER) == ["mine"]


SETTINGS = {"uri": "ldap://mock", "bind_dn": BIND_DN, "bind_password": BIND_PASSWORD, "base_dn": BASE_DN}


@pytest.fixture
def gon_client():
    return MockLdapClient({**SETTINGS, "project_group_object_classes": ["groupOfNames", "top"]})


class TestGroupOfNames:
    """Schema rules themselves are covered against a real directory in test_client_live."""

    def test_is_created_without_gid_and_with_the_stand_in(self, gon_client):
        assert gon_client.create_project_group("g") is None
        assert gon_client.get_group_gid("g") is None
        conn = gon_client._connect()
        conn.search(gon_client._groups_dn, "(cn=g)", attributes=["member"])
        assert conn.entries[0].entry_attributes_as_dict["member"] == [
            f"cn=nobody,{BASE_DN}"
        ]

    def test_members_that_are_not_users_are_not_listed(self, gon_client):
        gon_client.create_project_group("g")
        gon_client.add_user_to_group("g", "alice", "member")
        assert gon_client.list_group_members("g", "member") == ["alice"]

    def test_stand_in_must_not_be_a_uid_dn(self):
        with pytest.raises(BackendError, match="must not be a uid= DN"):
            LdapClient({**SETTINGS, "empty_group_member_dn": f"uid=nobody,{BASE_DN}"})

    def test_last_member_is_swapped_for_the_stand_in_in_one_modify(self):
        client = LdapClient(SETTINGS)
        conn = MagicMock()
        conn.modify.side_effect = [False, True]
        conn.result = {"description": "objectClassViolation"}

        with patch.object(client, "_connect", return_value=conn):
            client.remove_user_from_group("g", "alice", "member")

        assert conn.modify.call_args.args[1] == {
            "member": [
                (MODIFY_ADD, [f"cn=nobody,{BASE_DN}"]),
                (MODIFY_DELETE, [client._user_dn("alice")]),
            ]
        }

    def test_other_removal_failures_still_raise(self):
        client = LdapClient(SETTINGS)
        conn = MagicMock()
        conn.modify.return_value = False
        conn.result = {"description": "insufficientAccessRights"}

        with patch.object(client, "_connect", return_value=conn), pytest.raises(BackendError):
            client.remove_user_from_group("g", "alice", "member")
        assert conn.modify.call_count == 1


class TestDnValues:
    @pytest.mark.parametrize(
        "username",
        ["alice", "o'brien", "a,b", "a+b", "x=y", "back\\slash", " lead", "trail ", "#hash"],
    )
    def test_member_dn_reads_back_as_the_username(self, username):
        client = LdapClient(SETTINGS)
        assert _extract_uid_from_dn(client._user_dn(username)) == username

    def test_hex_escapes_are_decoded(self):
        assert _extract_uid_from_dn(f"uid=a\\2Cb,ou=People,{BASE_DN}") == "a,b"
        assert _extract_uid_from_dn(f"uid=j\\C3\\BCrgen,ou=People,{BASE_DN}") == "jürgen"

    def test_non_user_and_malformed_dns_are_not_users(self):
        assert _extract_uid_from_dn(f"cn=nobody,{BASE_DN}") == ""
        assert _extract_uid_from_dn("not a dn") == ""

    def test_escaped_member_is_listed_by_username(self, gon_client):
        gon_client.create_project_group("g")
        gon_client.add_user_to_group("g", "a,b", "member")
        assert gon_client.list_group_members("g", "member") == ["a,b"]

    def test_create_project_group_writes_extra_attributes_in_the_add(self, client):
        client.create_project_group("proj", extra_attributes={"description": "marker"})
        assert client.get_group_descriptions("proj") == ["marker"]


class TestDisableEnable:
    """disable_user parks an entry; enable_user wakes it. Same DN, same ids throughout."""

    def test_disable_sets_the_markers_and_keeps_the_ids(self, client):
        create(client)
        client.disable_user("jsmith")
        entry = client.search_user("jsmith")
        assert entry["loginShell"][0] == "/usr/sbin/nologin"
        assert str(entry["shadowExpire"][0]) == "1"
        assert "shadowAccount" in entry["objectClass"]
        assert client.is_disabled_by_agent(entry)
        assert int(entry["uidNumber"][0]) == 10001
        assert client.get_group_gid("jsmith") == 20001

    def test_disable_is_idempotent(self, client):
        create(client)
        client.disable_user("jsmith")
        client.disable_user("jsmith")
        entry = client.search_user("jsmith")
        assert entry["objectClass"].count("shadowAccount") == 1
        assert entry["description"].count("waldur-site-agent:disabled") == 1

    def test_enable_restores_shell_and_drops_the_markers(self, client):
        create(client)
        client.disable_user("jsmith")
        client.enable_user("jsmith", "/bin/zsh")
        entry = client.search_user("jsmith")
        assert entry["loginShell"][0] == "/bin/zsh"
        assert not entry.get("shadowExpire")
        assert not client.is_disabled_by_agent(entry)
        assert int(entry["uidNumber"][0]) == 10001

    def test_enable_on_a_never_disabled_entry_only_sets_the_shell(self, client):
        create(client)
        client.enable_user("jsmith", "/bin/sh")
        assert client.search_user("jsmith")["loginShell"][0] == "/bin/sh"

    def test_disable_missing_entry_raises(self, client):
        with pytest.raises(BackendError):
            client.disable_user("nobody")

    def test_is_disabled_by_agent_ignores_hand_disabled_entries(self, client):
        create(client)
        client.update_user_attributes("jsmith", {"loginShell": "/usr/sbin/nologin"})
        assert not client.is_disabled_by_agent(client.search_user("jsmith"))

    def test_find_groups_with_member(self, client):
        create(client)
        client.create_project_group("hpc_proj1")
        client.add_user_to_group("hpc_proj1", "jsmith")
        # The personal group lists its owner too; callers decide whether to keep it.
        assert set(client.find_groups_with_member("jsmith")) == {"hpc_proj1", "jsmith"}
        assert client.find_groups_with_member("nobody") == []
