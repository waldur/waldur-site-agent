"""LdapClient against ldap3's in-memory server.

MOCK_SYNC gives a real DIT with real add/modify/search semantics, so these cover
the behaviours that only show up against an actual directory — idempotency, the
already-exists paths, and the orphan-group rollback — without a container.
"""

import pytest
from ldap3 import MOCK_SYNC, OFFLINE_SLAPD_2_4, Connection, Server

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_ldap.client import LdapClient

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
