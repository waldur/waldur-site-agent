"""LdapClient against a real OpenLDAP.

ldap3's in-memory server does not enforce schema, and what these cover are
schema rules: a groupOfNames must keep a member, and a plain groupOfNames has
no gidNumber. Opt in by pointing LDAP_LIVE_URI at a directory, e.g.:

    docker run -d -p 127.0.0.1:38389:389 -e LDAP_DOMAIN=example.com \\
        -e LDAP_ADMIN_PASSWORD=secret osixia/openldap:1.5.0
    LDAP_LIVE_URI=ldap://127.0.0.1:38389 pytest tests/test_client_live.py

osixia ships the nis schema, where posixGroup is structural and cannot be
combined with groupOfNames -- the plain-groupOfNames case.

Each test works in its own OU, removed afterwards.
"""

import os
import uuid

import pytest
from ldap3 import BASE, SUBTREE, Connection, Server
from waldur_site_agent_ldap_client import LdapClient

URI = os.environ.get("LDAP_LIVE_URI")
BASE_DN = os.environ.get("LDAP_LIVE_BASE_DN", "dc=example,dc=com")
BIND_DN = os.environ.get("LDAP_LIVE_BIND_DN", f"cn=admin,{BASE_DN}")
BIND_PASSWORD = os.environ.get("LDAP_LIVE_BIND_PASSWORD", "secret")

pytestmark = pytest.mark.skipif(not URI, reason="LDAP_LIVE_URI not set")

GROUP_OF_NAMES = ["groupOfNames", "top"]
POSIX_GROUP = ["posixGroup", "top"]


@pytest.fixture
def admin():
    conn = Connection(Server(URI), BIND_DN, BIND_PASSWORD, auto_bind=True)
    yield conn
    conn.unbind()


@pytest.fixture
def groups_ou(admin):
    ou = f"ou=test-{uuid.uuid4().hex[:8]}"
    assert admin.add(f"{ou},{BASE_DN}", ["organizationalUnit"]), admin.result
    yield ou
    admin.search(f"{ou},{BASE_DN}", "(objectClass=*)", search_scope=SUBTREE)
    for dn in sorted((e.entry_dn for e in admin.entries), key=len, reverse=True):
        admin.delete(dn)


def make_client(groups_ou, object_classes):
    return LdapClient(
        {
            "uri": URI,
            "bind_dn": BIND_DN,
            "bind_password": BIND_PASSWORD,
            "base_dn": BASE_DN,
            "groups_ou": groups_ou,
            "project_group_object_classes": object_classes,
        }
    )


def raw_members(admin, client, group_name):
    admin.search(client._group_dn(group_name), "(objectClass=*)", BASE, attributes=["member"])
    return sorted(admin.entries[0].entry_attributes_as_dict.get("member", []))


class TestGroupOfNames:
    def test_is_created_without_gid_and_with_the_stand_in(self, admin, groups_ou):
        client = make_client(groups_ou, GROUP_OF_NAMES)

        assert client.create_project_group("g") is None

        assert client.get_group_gid("g") is None
        assert raw_members(admin, client, "g") == [client.empty_group_member_dn]
        assert client.list_group_members("g", "member") == []

    def test_revoking_every_member_keeps_the_group_valid(self, admin, groups_ou):
        client = make_client(groups_ou, GROUP_OF_NAMES)
        client.create_project_group("g")
        client.add_user_to_group("g", "alice", "member")
        client.add_user_to_group("g", "bob", "member")
        assert client.list_group_members("g", "member") == ["alice", "bob"]

        client.remove_user_from_group("g", "alice", "member")
        client.remove_user_from_group("g", "bob", "member")

        assert client.list_group_members("g", "member") == []
        assert raw_members(admin, client, "g") == [client.empty_group_member_dn]

    def test_the_stand_in_replaces_the_last_member_of_a_hand_made_group(
        self, admin, groups_ou
    ):
        client = make_client(groups_ou, GROUP_OF_NAMES)
        alice_dn = client._user_dn("alice")
        assert admin.add(
            client._group_dn("g"), GROUP_OF_NAMES, {"cn": "g", "member": [alice_dn]}
        ), admin.result

        client.remove_user_from_group("g", "alice", "member")

        assert raw_members(admin, client, "g") == [client.empty_group_member_dn]

    def test_member_with_rdn_special_characters_round_trips(self, groups_ou):
        client = make_client(groups_ou, GROUP_OF_NAMES)
        client.create_project_group("g")
        client.add_user_to_group("g", "a,b+c", "member")

        assert client.list_group_members("g", "member") == ["a,b+c"]
        client.remove_user_from_group("g", "a,b+c", "member")
        assert client.list_group_members("g", "member") == []


class TestPosixGroup:
    def test_member_uid_round_trip(self, groups_ou):
        client = make_client(groups_ou, POSIX_GROUP)

        gid = client.create_project_group("g")
        assert gid == client.get_group_gid("g")

        client.add_user_to_group("g", "alice")
        assert client.list_group_members("g") == ["alice"]
        client.remove_user_from_group("g", "alice")
        assert client.list_group_members("g") == []


class TestDescriptions:
    MARKER = "managed_by=waldur-site-agent;resource=deadbeef"

    def test_marker_is_added_once_and_found(self, groups_ou):
        client = make_client(groups_ou, POSIX_GROUP)
        client.create_project_group("mine")
        client.create_project_group("theirs")

        client.add_group_description("mine", self.MARKER)
        client.add_group_description("mine", self.MARKER)

        assert client.get_group_descriptions("mine") == [self.MARKER]
        assert client.find_groups_by_description(self.MARKER) == ["mine"]
