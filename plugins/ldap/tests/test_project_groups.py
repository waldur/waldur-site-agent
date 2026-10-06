"""Provider project groups reconciled into ldap3's in-memory directory.

The layout is the one the plugin README documents: accounts in ou=users,
project groups in ou=projects, and a cluster groupOfNames in ou=clusters that
lists the DN of every project group with a resource on the offering.
"""

import threading
from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
from pydantic import ValidationError as PydanticValidationError
from waldur_api_client.models.offering_user_state import OfferingUserState
from ldap3 import MOCK_SYNC, MODIFY_ADD, OFFLINE_SLAPD_2_4, Connection, Server
from waldur_site_agent_ldap_client import LdapClient

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing import utils as event_utils
from waldur_site_agent_ldap import backend as backend_module
from waldur_site_agent_ldap import project_groups
from waldur_site_agent_ldap.backend import LdapUsernameBackend
from waldur_site_agent_ldap.project_groups import (
    ProjectGroup,
    ProjectGroupReconciler,
    fetch_provider_project_groups,
)
from waldur_site_agent_ldap.schemas import LdapSettingsSchema

BASE_DN = "dc=example,dc=org"
BIND_DN = f"cn=admin,{BASE_DN}"
BIND_PASSWORD = "secret"
CLUSTER_DN = f"cn=alps,ou=clusters,{BASE_DN}"
STAND_IN = f"cn=nobody,{BASE_DN}"
OFFERING = "11111111111111111111111111111111"
SIBLING = "22222222222222222222222222222222"

SETTINGS = {
    "uri": "ldap://mock",
    "bind_dn": BIND_DN,
    "bind_password": BIND_PASSWORD,
    "base_dn": BASE_DN,
    "people_ou": "ou=users",
}


class MockLdapClient(LdapClient):
    """LdapClient over an in-memory DIT shared across connections."""

    def __init__(self, settings, sibling_of=None):
        if sibling_of is not None:
            # A second client on the same directory, as a sibling offering's agent.
            self._server = sibling_of._server
            super().__init__(settings)
            self._seeded = sibling_of._seeded
            return
        self._server = Server("mock", get_info=OFFLINE_SLAPD_2_4)
        super().__init__(settings)
        conn = self._raw_connection()
        conn.strategy.add_entry(BIND_DN, {"userPassword": BIND_PASSWORD, "objectClass": "person"})
        for dn in (
            BASE_DN,
            f"ou=users,{BASE_DN}",
            f"ou=projects,{BASE_DN}",
            f"ou=clusters,{BASE_DN}",
        ):
            conn.strategy.add_entry(dn, {"objectClass": "top"})
        conn.strategy.add_entry(
            CLUSTER_DN, {"objectClass": ["top", "groupOfNames"], "cn": "alps", "member": STAND_IN}
        )
        self._seeded = conn.strategy.entries

    def _raw_connection(self):
        return Connection(
            self._server, user=BIND_DN, password=BIND_PASSWORD, client_strategy=MOCK_SYNC
        )

    def _connect(self):
        conn = self._raw_connection()
        if hasattr(self, "_seeded"):
            conn.strategy.entries = self._seeded
        conn.bind()
        return conn


@pytest.fixture
def client():
    return MockLdapClient(SETTINGS)


def group(name="proj", gid=20003, members=("alice", "bob"), offerings=(OFFERING,)):
    return ProjectGroup(
        name=name, gid=gid, members=sorted(members), offering_uuids=set(offerings)
    )


def reconcile(client, groups, offering=OFFERING, **settings):
    config = {"enabled": True, "ou": "ou=projects", **settings}
    config.setdefault("parents", [{"dn": CLUSTER_DN}])
    return ProjectGroupReconciler(client, config, offering).run(groups)


def read(client, name, attributes=("gidNumber", "memberUid", "member")):
    return client.read_entry(client.dn_under(name, "ou=projects"), list(attributes))


def gid_of(client, name):
    return int(read(client, name)["gidNumber"][0])


def cluster_members(client):
    return sorted(client.read_entry(CLUSTER_DN, ["member"])["member"])


_next_uid = iter(range(30000, 40000))


def add_user(client, name, gid=None):
    uid = next(_next_uid)
    client.add_entry(
        client.user_dn(name),
        {
            "objectClass": ["top", "posixAccount", "inetOrgPerson"],
            "uid": name,
            "cn": name,
            "sn": name,
            "uidNumber": uid,
            "gidNumber": gid if gid is not None else uid,
            "homeDirectory": f"/home/{name}",
        },
    )


def add_group(client, name, gid, members=()):
    attrs = {"objectClass": ["top", "posixGroup"], "cn": name, "gidNumber": gid}
    if members:
        attrs["memberUid"] = list(members)
    client.add_entry(client.dn_under(name, "ou=projects"), attrs)


class TestCreate:
    def test_creates_with_waldurs_name_gid_and_members(self, client):
        report = reconcile(client, [group()])
        assert report.created == 1
        entry = read(client, "proj")
        assert int(entry["gidNumber"][0]) == 20003
        assert sorted(entry["memberUid"]) == ["alice", "bob"]

    def test_a_group_without_members_is_created_empty(self, client):
        reconcile(client, [group(members=())])
        assert read(client, "proj")["memberUid"] == []

    def test_member_attribute_member_writes_user_dns(self, client):
        add_user(client, "alice")
        add_user(client, "bob")
        reconcile(client, [group()], member_attribute="member")
        assert sorted(read(client, "proj")["member"]) == [
            f"uid=alice,ou=users,{BASE_DN}",
            f"uid=bob,ou=users,{BASE_DN}",
        ]

    def test_member_dns_name_only_users_with_an_entry(self, client):
        """An account Waldur lists before the agent wrote it gives no dangling DN."""
        add_user(client, "alice")
        reconcile(client, [group()], member_attribute="member")
        assert read(client, "proj")["member"] == [client.user_dn("alice")]
        add_user(client, "bob")
        reconcile(client, [group()], member_attribute="member")
        assert sorted(read(client, "proj")["member"]) == sorted(
            [client.user_dn("alice"), client.user_dn("bob")]
        )

    def test_group_of_names_without_members_gets_the_stand_in(self, client):
        reconcile(
            client,
            [group(members=())],
            object_classes=["top", "groupOfNames", "posixGroup"],
            member_attribute="member",
        )
        assert read(client, "proj")["member"] == [STAND_IN]

    def test_a_group_without_gid_is_skipped(self, client):
        report = reconcile(client, [group(gid=None)])
        assert report.skipped == 1
        assert read(client, "proj") is None

    def test_a_gid_held_by_a_user_is_not_given_out(self, client):
        client.add_entry(
            client.user_dn("carol"),
            {
                "objectClass": ["top", "posixAccount", "inetOrgPerson"],
                "uid": "carol",
                "cn": "carol",
                "sn": "carol",
                "uidNumber": 10001,
                "gidNumber": 20003,
                "homeDirectory": "/home/carol",
            },
        )
        report = reconcile(client, [group()])
        assert report.conflicts == 1
        assert read(client, "proj") is None

    def test_a_gid_held_by_another_group_is_not_given_out(self, client):
        add_group(client, "other", 20003)
        report = reconcile(client, [group()])
        assert report.conflicts == 1
        assert read(client, "proj") is None

    def test_two_waldur_groups_cannot_take_one_gid_in_one_pass(self, client):
        report = reconcile(client, [group("a"), group("b")])
        assert (report.created, report.conflicts) == (1, 1)
        assert read(client, "b") is None

    def test_a_missing_ou_fails_the_pass_once(self, client):
        config = {"enabled": True, "ou": "ou=nowhere"}
        with pytest.raises(Exception, match="does not exist"):
            ProjectGroupReconciler(client, config, OFFERING).run([group()])


class TestExisting:
    def test_same_name_and_gid_is_adopted_with_only_the_marker_written(self, client):
        add_group(client, "proj", 20003, ["alice", "bob"])
        with mock.patch.object(client, "modify_entry", wraps=client.modify_entry) as modify:
            report = reconcile(client, [group()], parents=[])
        assert (report.kept, report.created, report.marked) == (1, 0, 1)
        modify.assert_called_once_with(
            client.dn_under("proj", "ou=projects"),
            {"description": [(MODIFY_ADD, ["waldur-managed"])]},
        )
        with mock.patch.object(client, "modify_entry") as modify:
            reconcile(client, [group()], parents=[])
        modify.assert_not_called()

    def test_another_gid_keeps_its_gid_but_follows_members_and_parents(self, client):
        add_group(client, "proj", 29999, ["zed"])
        report = reconcile(client, [group()])
        assert report.conflicts == 1
        assert gid_of(client, "proj") == 29999
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob"]
        assert client.dn_under("proj", "ou=projects") in cluster_members(client)

    def test_a_gid_mismatch_is_reported_every_cycle(self, client):
        add_group(client, "proj", 29999)
        with mock.patch.object(project_groups.logger, "error") as error:
            reconcile(client, [group()])
            reconcile(client, [group()])
        mismatch = [c for c in error.call_args_list if "was left unchanged" in c.args[0]]
        assert len(mismatch) == 2

    def test_another_gid_is_renumbered_under_adopt(self, client):
        add_group(client, "proj", 29999, ["zed"])
        report = reconcile(client, [group()], on_gid_mismatch="adopt")
        assert report.renumbered == 1
        assert gid_of(client, "proj") == 20003
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob"]

    def test_adopt_does_not_take_a_gid_another_entry_holds(self, client):
        add_group(client, "proj", 29999)
        add_group(client, "other", 20003)
        report = reconcile(client, [group()], on_gid_mismatch="adopt")
        assert report.conflicts == 1
        assert gid_of(client, "proj") == 29999

    def test_name_match_ignores_case(self, client):
        add_group(client, "Proj", 20003, ["alice", "bob"])
        report = reconcile(client, [group()])
        assert (report.kept, report.created) == (1, 0)


class TestMembership:
    def test_sync_adds_and_removes(self, client):
        add_group(client, "proj", 20003, ["bob", "zed"])
        reconcile(client, [group()])
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob"]

    def test_add_only_never_removes(self, client):
        add_group(client, "proj", 20003, ["bob", "zed"])
        reconcile(client, [group()], membership="add_only")
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob", "zed"]

    def test_sync_to_empty(self, client):
        add_group(client, "proj", 20003, ["zed"])
        reconcile(client, [group(members=())])
        assert read(client, "proj")["memberUid"] == []

    def test_dn_members_keep_non_user_values(self, client):
        add_user(client, "alice")
        dn = client.dn_under("proj", "ou=projects")
        client.add_entry(
            dn,
            {
                "objectClass": ["top", "groupOfNames", "posixGroup"],
                "cn": "proj",
                "gidNumber": 20003,
                "member": [client.user_dn("zed"), f"cn=admins,ou=projects,{BASE_DN}"],
            },
        )
        reconcile(client, [group(members=("alice",))], member_attribute="member")
        assert sorted(read(client, "proj")["member"]) == sorted(
            [client.user_dn("alice"), f"cn=admins,ou=projects,{BASE_DN}"]
        )

    def test_dn_member_last_removal_leaves_the_stand_in(self, client):
        dn = client.dn_under("proj", "ou=projects")
        client.add_entry(
            dn,
            {
                "objectClass": ["top", "groupOfNames", "posixGroup"],
                "cn": "proj",
                "gidNumber": 20003,
                "member": [client.user_dn("zed")],
            },
        )
        reconcile(client, [group(members=())], member_attribute="member")
        assert read(client, "proj")["member"] == [STAND_IN]


class TestParents:
    def test_lists_groups_with_a_resource_on_the_offering(self, client):
        reconcile(client, [group("a"), group("b", gid=20004, offerings=())])
        assert cluster_members(client) == sorted(
            [STAND_IN, client.dn_under("a", "ou=projects")]
        )

    def test_drops_a_group_whose_project_left_the_offering(self, client):
        reconcile(client, [group("a")])
        report = reconcile(client, [group("a", offerings=())])
        assert report.parent_updates == 1
        assert cluster_members(client) == [STAND_IN]
        # The entry and its GID stay reserved.
        assert gid_of(client, "a") == 20003

    def test_never_drops_a_dn_outside_the_project_ou(self, client):
        hand_made = f"cn=admins,ou=clusters,{BASE_DN}"
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [hand_made])]})
        reconcile(client, [group("a", offerings=())])
        assert hand_made in cluster_members(client)

    def test_keeps_an_unlisted_dn_without_a_marked_entry(self, client):
        stale = f"cn=legacy,ou=projects,{BASE_DN}"
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [stale])]})
        reconcile(client, [group("a")])
        assert stale in cluster_members(client)

    def test_keeps_the_dn_of_a_group_waldur_lists_without_gid(self, client):
        dn = client.dn_under("a", "ou=projects")
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [dn])]})
        reconcile(client, [group("a", gid=None), group("b")])
        assert dn in cluster_members(client)

    def test_drops_the_dn_of_a_group_that_could_not_be_written(self, client):
        add_group(client, "holder", 20003)
        dn = client.dn_under("a", "ou=projects")
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [dn])]})
        reconcile(client, [group("a")])
        assert read(client, "a") is None
        assert dn not in cluster_members(client)

    def test_a_gid_mismatch_still_follows_the_offerings(self, client):
        add_group(client, "a", 29999)
        dn = client.dn_under("a", "ou=projects")
        reconcile(client, [group("a")])
        assert dn in cluster_members(client)
        reconcile(client, [group("a", offerings=())])
        assert dn not in cluster_members(client)
        assert gid_of(client, "a") == 29999

    def test_other_offerings_are_not_this_parents_business(self, client):
        reconcile(client, [group("a", offerings=(SIBLING,))])
        assert cluster_members(client) == [STAND_IN]

    def test_a_shared_parent_lists_every_configured_offering(self, client):
        parents = [{"dn": CLUSTER_DN, "offering_uuids": [OFFERING, SIBLING]}]
        reconcile(client, [group("a", offerings=(SIBLING,))], parents=parents)
        assert client.dn_under("a", "ou=projects") in cluster_members(client)

    def test_offering_uuid_matches_with_dashes(self, client):
        dashed = "11111111-1111-1111-1111-111111111111"
        reconcile(client, [group("a")], offering=dashed)
        assert client.dn_under("a", "ou=projects") in cluster_members(client)

    def test_a_missing_parent_is_reported_not_raised(self, client):
        report = reconcile(client, [group()], parents=[{"dn": f"cn=gone,{BASE_DN}"}])
        assert (report.created, report.failed) == (1, 1)


class TestTwoOfferingsOneDirectory:
    def test_each_group_is_written_once_with_the_same_gid(self, client):
        sibling = MockLdapClient(SETTINGS, sibling_of=client)
        groups = [group("a", offerings=(OFFERING, SIBLING))]
        first = reconcile(client, groups)
        second = reconcile(sibling, groups, offering=SIBLING)
        assert (first.created, second.created, second.kept) == (1, 0, 1)
        assert list(client.list_entries("ou=projects", "(cn=*)", ["cn"])) == [
            client.dn_under("a", "ou=projects")
        ]
        assert gid_of(client, "a") == 20003

    def test_a_second_pass_writes_nothing(self, client):
        reconcile(client, [group()])
        with mock.patch.object(client, "modify_entry") as modify, mock.patch.object(
            client, "add_entry"
        ) as add:
            reconcile(client, [group()])
        modify.assert_not_called()
        add.assert_not_called()


class TestFetch:
    def make_client(self, handler):
        rest = mock.Mock()
        rest.get_httpx_client.return_value = httpx.Client(
            base_url="https://waldur.example.com", transport=httpx.MockTransport(handler)
        )
        return rest

    def test_follows_the_next_link(self):
        seen = []

        def handler(request):
            seen.append(request.url)
            if request.url.params.get("page") == "2":
                return httpx.Response(200, json=[{"name": "b"}])
            return httpx.Response(
                200,
                json=[{"name": "a"}],
                headers={
                    "Link": '<https://waldur.example.com/api/marketplace-service-provider-'
                    'project-groups/?page=2&page_size=100&provider_offering_uuid=x>; rel="next"'
                },
            )

        groups = fetch_provider_project_groups(self.make_client(handler), "x")
        assert [g["name"] for g in groups] == ["a", "b"]
        assert seen[0].path == "/api/marketplace-service-provider-project-groups/"
        assert seen[0].params["provider_offering_uuid"] == "x"

    def test_an_error_raises(self):
        rest = self.make_client(lambda request: httpx.Response(403))
        with pytest.raises(httpx.HTTPStatusError):
            fetch_provider_project_groups(rest, "x")

    def test_parses_an_item(self):
        parsed = ProjectGroup.from_api(
            {
                "name": "proj",
                "gid": 20003,
                "members": ["bob", "alice"],
                "offerings": [{"uuid": "AAAA-bbbb", "name": "o"}],
            }
        )
        assert parsed == ProjectGroup("proj", 20003, ["alice", "bob"], {"aaaabbbb"})
        assert ProjectGroup.from_api({"name": "p", "gid": None}).gid is None


class TestBackendWiring:
    def make_backend(self, offering_uuid=OFFERING, **ldap):
        settings = {**SETTINGS, "account_source": "waldur", **ldap}
        with mock.patch("waldur_site_agent_ldap.backend.LdapClient"):
            return LdapUsernameBackend(
                backend_settings={"ldap": settings},
                offering=SimpleNamespace(name="HPC", uuid=offering_uuid),
            )

    def test_disabled_by_default(self):
        backend = self.make_backend()
        with mock.patch.object(project_groups, "fetch_provider_project_groups") as fetch:
            backend.reconcile_offering(mock.Mock())
        fetch.assert_not_called()

    def test_profile_sync_alone_does_not_run_the_group_pass(self):
        """Single-user events call sync_user_profiles; the group pass is periodic."""
        backend = self.make_backend(project_groups={"enabled": True})
        backend.client.list_users.return_value = {}
        with mock.patch.object(project_groups, "fetch_provider_project_groups") as fetch:
            backend.sync_user_profiles([])
        fetch.assert_not_called()

    def test_reads_waldur_with_the_client_core_hands_over(self):
        backend = self.make_backend(project_groups={"enabled": True})
        rest = mock.Mock()
        with mock.patch.object(
            project_groups, "fetch_provider_project_groups", return_value=[{"name": "p", "gid": 1}]
        ) as fetch, mock.patch.object(project_groups.ProjectGroupReconciler, "run") as run:
            run.return_value = project_groups.ReconcileReport()
            backend.reconcile_offering(rest)
        assert fetch.call_args.args == (rest, OFFERING)
        assert run.call_args.args[0][0].name == "p"

    def test_a_failed_read_leaves_the_directory_alone(self):
        backend = self.make_backend(project_groups={"enabled": True})
        with mock.patch.object(
            project_groups, "fetch_provider_project_groups", side_effect=RuntimeError("down")
        ), mock.patch.object(project_groups.ProjectGroupReconciler, "run") as run:
            backend.reconcile_offering(mock.Mock())
        run.assert_not_called()

    def test_writes_project_groups_only_when_enabled(self):
        assert self.make_backend().reconciles_project_groups() is False
        assert self.make_backend(project_groups={"enabled": True}).reconciles_project_groups()

    def test_event_pass_reads_waldur_like_the_periodic_one(self):
        backend = self.make_backend(project_groups={"enabled": True})
        backend_module._FULL_ACCOUNT_PASS_DONE.add(OFFERING)
        rest = mock.Mock()
        with mock.patch.object(
            project_groups, "fetch_provider_project_groups", return_value=[{"name": "p", "gid": 1}]
        ) as fetch, mock.patch.object(project_groups.ProjectGroupReconciler, "run") as run:
            run.return_value = project_groups.ReconcileReport()
            backend.reconcile_project_groups(rest)
        assert fetch.call_args.args == (rest, OFFERING)

    def test_passes_on_one_offering_never_overlap(self):
        """The periodic and the event-triggered pass take turns on an offering."""
        backend = self.make_backend(project_groups={"enabled": True})
        other = self.make_backend(project_groups={"enabled": True})
        backend_module._FULL_ACCOUNT_PASS_DONE.add(OFFERING)
        inside = threading.Event()
        release = threading.Event()
        calls = []

        def slow_fetch(rest, offering_uuid):
            calls.append(rest)
            if len(calls) == 1:
                inside.set()
                release.wait(5)
            return []

        with mock.patch.object(
            project_groups, "fetch_provider_project_groups", side_effect=slow_fetch
        ):
            first = threading.Thread(target=backend.reconcile_project_groups, args=("first",))
            first.start()
            assert inside.wait(5)
            second = threading.Thread(target=other.reconcile_project_groups, args=("second",))
            second.start()
            second.join(0.2)
            assert second.is_alive(), "the second pass must wait for the first"
            release.set()
            first.join(5)
            second.join(5)
        assert calls == ["first", "second"]

    def test_event_pass_waits_for_the_first_full_account_pass(self):
        """Until a full account pass has run, the conflict record is incomplete."""
        backend = self.make_backend(offering_uuid="33333333333333333333333333333333",
                                    project_groups={"enabled": True})
        with mock.patch.object(
            project_groups, "fetch_provider_project_groups", return_value=[]
        ) as fetch:
            backend.reconcile_project_groups(mock.Mock())
            fetch.assert_not_called()
            backend.reconcile_offering(mock.Mock())  # the periodic pass
            backend.reconcile_project_groups(mock.Mock())
        assert fetch.call_count == 2

    def test_a_one_account_pass_keeps_the_other_accounts_conflicts(self):
        backend = self.make_backend(offering_uuid="44444444444444444444444444444444")
        key = "44444444444444444444444444444444"
        backend_module._CONFLICTED_ACCOUNTS[key] = {"alice", "bob"}
        bob = SimpleNamespace(username="bob")
        with mock.patch.object(backend, "_reconcile_accounts"):
            backend._reconcile_from_waldur([bob])
        assert backend_module._CONFLICTED_ACCOUNTS[key] == {"alice"}

    def test_the_conflict_record_changes_only_when_the_pass_is_over(self):
        backend = self.make_backend(offering_uuid="55555555555555555555555555555555")
        key = "55555555555555555555555555555555"
        backend_module._CONFLICTED_ACCOUNTS[key] = {"alice"}
        seen_during_pass = []

        def reconcile(offering_users, conflicted):
            conflicted.add("carol")
            seen_during_pass.append(set(backend_module._CONFLICTED_ACCOUNTS[key]))

        with mock.patch.object(backend, "_reconcile_accounts", side_effect=reconcile):
            backend._reconcile_from_waldur([SimpleNamespace(username="alice"),
                                            SimpleNamespace(username="carol")])
        assert seen_during_pass == [{"alice"}]
        assert backend_module._CONFLICTED_ACCOUNTS[key] == {"carol"}

    def test_a_failed_account_pass_keeps_every_recorded_conflict(self):
        backend = self.make_backend(offering_uuid="66666666666666666666666666666666")
        key = "66666666666666666666666666666666"
        backend_module._CONFLICTED_ACCOUNTS[key] = {"alice", "bob"}
        backend_module._FULL_ACCOUNT_PASS_DONE.add(key)

        def fail_part_way(offering_users, conflicted):
            conflicted.add("carol")
            raise RuntimeError("directory went away")

        with mock.patch.object(backend, "_reconcile_accounts", side_effect=fail_part_way):
            with pytest.raises(RuntimeError):
                backend._reconcile_from_waldur(
                    [SimpleNamespace(username=n) for n in ("alice", "bob", "carol")]
                )
        assert backend_module._CONFLICTED_ACCOUNTS[key] == {"alice", "bob", "carol"}
        assert key not in backend_module._FULL_ACCOUNT_PASS_DONE

    def test_a_cycle_whose_account_pass_failed_keeps_event_passes_waiting(self):
        backend = self.make_backend(offering_uuid="77777777777777777777777777777777",
                                    project_groups={"enabled": True})
        key = "77777777777777777777777777777777"
        with mock.patch.object(backend, "_reconcile_accounts", side_effect=RuntimeError("down")):
            with pytest.raises(RuntimeError):
                backend._reconcile_from_waldur([SimpleNamespace(username="alice")])
        with mock.patch.object(project_groups, "fetch_provider_project_groups", return_value=[]):
            backend.reconcile_offering(mock.Mock())
        assert key not in backend_module._FULL_ACCOUNT_PASS_DONE
        with mock.patch.object(backend, "_reconcile_accounts"):
            backend._reconcile_from_waldur([SimpleNamespace(username="alice")])
        with mock.patch.object(project_groups, "fetch_provider_project_groups", return_value=[]):
            backend.reconcile_offering(mock.Mock())
        assert key in backend_module._FULL_ACCOUNT_PASS_DONE

    def test_two_offerings_sharing_a_parent_without_offering_uuids_warn(self):
        from waldur_site_agent_ldap import backend as backend_module

        shared = f"cn=shared-{id(self)},ou=clusters,{BASE_DN}"
        config = {"enabled": True, "parents": [{"dn": shared}]}
        with mock.patch.object(backend_module.logger, "warning") as warning:
            self.make_backend(OFFERING, project_groups=config)
            self.make_backend(OFFERING, project_groups=config)
            assert not any("offering_uuids" in c.args[0] for c in warning.call_args_list)
            self.make_backend(SIBLING, project_groups=config)
            self.make_backend(SIBLING, project_groups=config)
        shared_warnings = [c for c in warning.call_args_list if "offering_uuids" in c.args[0]]
        assert len(shared_warnings) == 1

    def test_only_one_offering_listing_offering_uuids_warns(self):
        from waldur_site_agent_ldap import backend as backend_module

        shared = f"cn=half-{id(self)},ou=clusters,{BASE_DN}"
        listed = [{"dn": shared, "offering_uuids": [OFFERING, SIBLING]}]
        with mock.patch.object(backend_module.logger, "warning") as warning:
            self.make_backend(OFFERING, project_groups={"enabled": True, "parents": listed})
            self.make_backend(
                SIBLING, project_groups={"enabled": True, "parents": [{"dn": shared}]}
            )
        assert any("offering_uuids" in c.args[0] for c in warning.call_args_list)

    def test_a_shared_parent_with_offering_uuids_does_not_warn(self):
        from waldur_site_agent_ldap import backend as backend_module

        shared = f"cn=listed-{id(self)},ou=clusters,{BASE_DN}"
        parents = [{"dn": shared, "offering_uuids": [OFFERING, SIBLING]}]
        with mock.patch.object(backend_module.logger, "warning") as warning:
            self.make_backend(OFFERING, project_groups={"enabled": True, "parents": parents})
            self.make_backend(SIBLING, project_groups={"enabled": True, "parents": parents})
        assert not any("offering_uuids" in c.args[0] for c in warning.call_args_list)


class TestSettings:
    def test_defaults(self):
        parsed = LdapSettingsSchema(**SETTINGS, project_groups={"enabled": True})
        assert parsed.personal_groups is True
        config = parsed.project_groups
        assert config.ou == "ou=projects"
        assert config.object_classes == ["top", "posixGroup"]
        assert config.member_attribute == "memberUid"
        assert config.membership == "sync"
        assert config.on_gid_mismatch == "report"
        assert config.parents == []
        assert config.managed_marker == "waldur-managed"

    def test_parent_attribute_defaults_to_member(self):
        parsed = LdapSettingsSchema(
            **SETTINGS, project_groups={"enabled": True, "parents": [{"dn": CLUSTER_DN}]}
        )
        assert parsed.project_groups.parents[0].attribute == "member"

    @pytest.mark.parametrize(
        "bad",
        [
            {"member_attribute": "uniqueMember"},
            {"membership": "replace"},
            {"on_gid_mismatch": "fail"},
            {"unknown": 1},
        ],
    )
    def test_rejects_unknown_values(self, bad):
        with pytest.raises(ValueError):
            LdapSettingsSchema(**SETTINGS, project_groups={"enabled": True, **bad})

    def test_member_dns_need_a_class_that_allows_them(self):
        with pytest.raises(ValueError, match="groupOfNames"):
            LdapSettingsSchema(
                **SETTINGS, project_groups={"enabled": True, "member_attribute": "member"}
            )
        LdapSettingsSchema(
            **SETTINGS,
            project_groups={
                "enabled": True,
                "member_attribute": "member",
                "object_classes": ["top", "groupOfNames", "posixGroup"],
            },
        )

    def test_offering_uuids_must_be_uuids(self):
        with pytest.raises(ValueError, match="not a UUID"):
            LdapSettingsSchema(
                **SETTINGS,
                project_groups={
                    "enabled": True,
                    "parents": [{"dn": CLUSTER_DN, "offering_uuids": ["cluster-x"]}],
                },
            )
        LdapSettingsSchema(
            **SETTINGS,
            project_groups={
                "enabled": True,
                "parents": [{"dn": CLUSTER_DN, "offering_uuids": [OFFERING, SIBLING]}],
            },
        )

    def test_a_parent_needs_a_dn(self):
        with pytest.raises(ValueError, match="dn"):
            LdapSettingsSchema(
                **SETTINGS, project_groups={"enabled": True, "parents": [{"attribute": "member"}]}
            )

    def test_personal_groups_off_requires_waldur_accounts(self):
        with pytest.raises(ValueError, match="personal_groups"):
            LdapSettingsSchema(**SETTINGS, personal_groups=False)
        LdapSettingsSchema(**SETTINGS, personal_groups=False, account_source="waldur")


SECOND_CLUSTER_DN = f"cn=daint,ou=clusters,{BASE_DN}"


def add_second_cluster(client):
    client.add_entry(
        SECOND_CLUSTER_DN,
        {"objectClass": ["top", "groupOfNames"], "cn": "daint", "member": STAND_IN},
    )


def members_of(client, dn):
    return sorted(client.read_entry(dn, ["member"])["member"])


class TestLifecycle:
    """How the directory follows a project's resources, members and lifetime."""

    def test_two_offerings_write_the_group_once_and_only_the_parent_owner_lists_it(
        self, client
    ):
        sibling = MockLdapClient(SETTINGS, sibling_of=client)
        listed = [group("a", offerings=(OFFERING, SIBLING))]
        with mock.patch.object(sibling, "modify_entry", wraps=sibling.modify_entry) as modify:
            reconcile(sibling, listed, offering=SIBLING, parents=[])
        modify.assert_not_called()
        assert members_of(client, CLUSTER_DN) == [STAND_IN]

        report = reconcile(client, listed)
        assert (report.created, report.kept) == (0, 1)
        assert client.dn_under("a", "ou=projects") in members_of(client, CLUSTER_DN)
        assert len(client.list_entries("ou=projects", "(cn=*)", ["cn"])) == 1

    def test_one_of_two_resources_on_the_offering_ending_changes_nothing(self, client):
        reconcile(client, [group("a")])
        with mock.patch.object(client, "modify_entry") as modify:
            # Waldur still lists the offering: another resource there remains.
            reconcile(client, [group("a")])
        modify.assert_not_called()

    def test_leaving_one_offering_drops_only_that_offerings_cluster(self, client):
        add_second_cluster(client)
        sibling = MockLdapClient(SETTINGS, sibling_of=client)
        dn = client.dn_under("a", "ou=projects")
        both = [group("a", offerings=(OFFERING, SIBLING))]
        reconcile(client, both)
        reconcile(sibling, both, offering=SIBLING, parents=[{"dn": SECOND_CLUSTER_DN}])
        assert dn in members_of(client, CLUSTER_DN)
        assert dn in members_of(client, SECOND_CLUSTER_DN)

        only_first = [group("a", offerings=(OFFERING,))]
        reconcile(client, only_first)
        reconcile(sibling, only_first, offering=SIBLING, parents=[{"dn": SECOND_CLUSTER_DN}])
        assert dn in members_of(client, CLUSTER_DN)
        assert members_of(client, SECOND_CLUSTER_DN) == [STAND_IN]

    def test_two_projects_get_two_groups_with_their_own_gids(self, client):
        report = reconcile(client, [group("a", gid=20004), group("b", gid=20005)])
        assert report.created == 2
        assert (gid_of(client, "a"), gid_of(client, "b")) == (20004, 20005)
        assert members_of(client, CLUSTER_DN) == sorted(
            [STAND_IN, client.dn_under("a", "ou=projects"), client.dn_under("b", "ou=projects")]
        )

    def test_names_differing_only_in_case_never_make_two_entries(self, client):
        report = reconcile(client, [group("proj", gid=20004), group("PROJ", gid=20005)])
        assert (report.created, report.conflicts) == (1, 1)
        assert len(client.list_entries("ou=projects", "(cn=*)", ["cn"])) == 1
        assert gid_of(client, "proj") == 20004

    def test_last_resource_gone_drops_every_cluster_and_keeps_the_entry(self, client):
        add_second_cluster(client)
        parents = [
            {"dn": CLUSTER_DN},
            {"dn": SECOND_CLUSTER_DN, "offering_uuids": [SIBLING]},
        ]
        reconcile(client, [group("a", offerings=(OFFERING, SIBLING))], parents=parents)
        reconcile(client, [group("a", offerings=())], parents=parents)
        assert members_of(client, CLUSTER_DN) == [STAND_IN]
        assert members_of(client, SECOND_CLUSTER_DN) == [STAND_IN]
        assert gid_of(client, "a") == 20003
        assert sorted(read(client, "a")["memberUid"]) == ["alice", "bob"]

    def test_a_returning_project_is_listed_again_under_the_same_gid(self, client):
        reconcile(client, [group("a")])
        reconcile(client, [group("a", offerings=())])
        report = reconcile(client, [group("a")])
        assert (report.created, report.kept) == (0, 1)
        assert client.dn_under("a", "ou=projects") in members_of(client, CLUSTER_DN)
        assert gid_of(client, "a") == 20003

    def test_a_deleted_project_keeps_its_entry_and_leaves_the_cluster(self, client):
        reconcile(client, [group("a")])
        # Waldur keeps listing the group, unused, with its GID reserved.
        reconcile(client, [group("a", members=(), offerings=())])
        assert members_of(client, CLUSTER_DN) == [STAND_IN]
        assert gid_of(client, "a") == 20003

    def test_a_group_without_gid_is_skipped_while_others_are_written(self, client):
        report = reconcile(client, [group("a", gid=None), group("b")])
        assert (report.skipped, report.created) == (1, 1)
        assert read(client, "a") is None
        assert members_of(client, CLUSTER_DN) == sorted(
            [STAND_IN, client.dn_under("b", "ou=projects")]
        )

    def test_a_member_joining_and_leaving(self, client):
        reconcile(client, [group(members=("alice",))])
        reconcile(client, [group(members=("alice", "bob"))])
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob"]
        reconcile(client, [group(members=("bob",))])
        assert read(client, "proj")["memberUid"] == ["bob"]

    def test_a_member_leaving_under_add_only_stays(self, client):
        reconcile(client, [group(members=("alice", "bob"))], membership="add_only")
        reconcile(client, [group(members=("bob",))], membership="add_only")
        assert sorted(read(client, "proj")["memberUid"]) == ["alice", "bob"]

    @pytest.mark.parametrize(
        ("mode", "expected"), [("sync", ["alice"]), ("add_only", ["alice", "ops"])]
    )
    def test_a_hand_made_member_of_an_adopted_group(self, client, mode, expected):
        add_group(client, "proj", 20003, ["ops"])
        report = reconcile(client, [group(members=("alice",))], membership=mode)
        assert report.kept == 1
        assert sorted(read(client, "proj")["memberUid"]) == expected

    def test_a_second_cycle_over_a_full_layout_writes_nothing(self, client):
        add_second_cluster(client)
        add_group(client, "adopted", 20010, ["ops"])
        parents = [{"dn": CLUSTER_DN}, {"dn": SECOND_CLUSTER_DN, "offering_uuids": [SIBLING]}]
        listed = [
            group("a", offerings=(OFFERING,)),
            group("b", gid=20004, offerings=(SIBLING,)),
            group("c", gid=20005, offerings=()),
            group("adopted", gid=20010, members=("alice",)),
            group("nogid", gid=None),
        ]
        reconcile(client, listed, parents=parents)
        with mock.patch.object(client, "modify_entry") as modify, mock.patch.object(
            client, "add_entry"
        ) as add:
            report = reconcile(client, listed, parents=parents)
        modify.assert_not_called()
        add.assert_not_called()
        assert (report.created, report.member_updates, report.parent_updates) == (0, 0, 0)


class TestSettingsAsCoreHandsThemOver:
    def test_enum_values_from_a_validated_block_are_understood(self, client):
        """Core validates backend_settings and dumps the model, enums and all."""
        block = LdapSettingsSchema(
            **SETTINGS,
            account_source="waldur",
            project_groups={
                "enabled": True,
                "member_attribute": "member",
                "object_classes": ["top", "groupOfNames", "posixGroup"],
                "membership": "add_only",
                "parents": [{"dn": CLUSTER_DN}],
            },
        ).model_dump(exclude_unset=True)["project_groups"]
        add_group(client, "proj", 20003)
        add_user(client, "alice")
        ProjectGroupReconciler(client, block, OFFERING).run([group(members=("alice",))])
        assert read(client, "proj")["member"] == [client.user_dn("alice")]
        assert client.dn_under("proj", "ou=projects") in members_of(client, CLUSTER_DN)


class TestEventProcessPeriodicPath:
    """The periodic reconcile of event_process mode, with STOMP off, runs the group pass."""

    def offering(self, membership_sync_backend):
        return structures.Offering(
            name="ldap-only",
            waldur_offering_uuid=OFFERING,
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="token",
            backend_type="ldap",
            membership_sync_backend=membership_sync_backend,
            username_management_backend="ldap",
            stomp_enabled=False,
        )

    def backend(self, offering):
        with mock.patch("waldur_site_agent_ldap.backend.LdapClient"):
            backend = LdapUsernameBackend(
                backend_settings={
                    "ldap": {
                        **SETTINGS,
                        "account_source": "waldur",
                        "project_groups": {"enabled": True},
                    }
                },
                offering=offering,
            )
        backend.client.list_users.return_value = {}
        return backend

    @pytest.mark.parametrize(
        ("membership_sync_backend", "live_users"),
        [("", True), ("slurm", True), ("", False), ("slurm", False)],
    )
    def test_group_pass_runs_from_the_periodic_offering_user_reconcile(
        self, membership_sync_backend, live_users
    ):
        offering = self.offering(membership_sync_backend)
        backend = self.backend(offering)
        live = SimpleNamespace(state=OfferingUserState.OK, username=None, uuid="ou-1")
        listed = [live] if live_users else []
        with mock.patch(
            "waldur_site_agent.common.utils.get_username_management_backend",
            return_value=(backend, "1.0"),
        ), mock.patch.object(event_utils, "get_client_for_offering") as get_client, mock.patch.object(
            event_utils, "marketplace_offering_users_list"
        ) as listing, mock.patch.object(
            handlers, "process_offering_user_deletions"
        ), mock.patch.object(
            project_groups, "fetch_provider_project_groups", return_value=[]
        ) as fetch:
            # Stuck-user retry (only with a membership backend), live list, departed list.
            listing.sync_all.side_effect = (
                [[], listed, []] if membership_sync_backend else [listed, []]
            )
            event_utils.run_periodic_offering_user_reconciliation([offering], "agent")
        assert fetch.call_args.args == (get_client.return_value, OFFERING)


def api_item(name, gid, members=("alice",), offerings=(OFFERING,), uuid=None):
    return {
        "uuid": uuid or f"{name:0<32}"[:32],
        "name": name,
        "gid": gid,
        "members": list(members),
        "offerings": [{"uuid": o, "name": "o"} for o in offerings],
    }


def rest_client(handler):
    rest = mock.Mock()
    rest.get_httpx_client.return_value = httpx.Client(
        base_url="https://waldur.example.com", transport=httpx.MockTransport(handler)
    )
    return rest


def backend_on(directory, offering_uuid=OFFERING, **ldap):
    """An LdapUsernameBackend whose client is a mock client on ``directory``."""
    config = {
        **SETTINGS,
        "account_source": "waldur",
        "personal_groups": False,
        **ldap,
    }
    with mock.patch("waldur_site_agent_ldap.backend.LdapClient") as client_cls:
        client_cls.side_effect = lambda settings: MockLdapClient(settings, sibling_of=directory)
        return LdapUsernameBackend(
            backend_settings={"ldap": config},
            offering=SimpleNamespace(name="HPC", uuid=offering_uuid),
        )


def snapshot(client):
    """Every entry in the directory, for "nothing was written" assertions."""
    return {
        dn: dict(attrs)
        for dn, attrs in client.list_entries("", "(objectClass=*)", ["*"]).items()
    }


ENABLED = {"enabled": True, "parents": [{"dn": CLUSTER_DN}]}


class TestRangeWarning:
    def build(self, uuid, **offering):
        with mock.patch("waldur_site_agent_ldap.backend.LdapClient"):
            LdapUsernameBackend(
                backend_settings={"ldap": {**SETTINGS, "account_source": "waldur"}},
                offering=SimpleNamespace(name="HPC", uuid=uuid, **offering),
            )

    def range_warnings(self, warning):
        return [c for c in warning.call_args_list if "still allocated" in c.args[0]]

    def test_silent_without_a_resource_backend(self):
        from waldur_site_agent_ldap import backend as backend_module

        with mock.patch.object(backend_module.logger, "warning") as warning:
            self.build(f"no-rb-{id(self)}", membership_sync_backend="", order_processing_backend="")
        assert self.range_warnings(warning) == []

    def test_once_per_offering_with_a_resource_backend(self):
        from waldur_site_agent_ldap import backend as backend_module

        uuid = f"slurm-{id(self)}"
        with mock.patch.object(backend_module.logger, "warning") as warning:
            self.build(uuid, membership_sync_backend="slurm")
            self.build(uuid, membership_sync_backend="slurm")
        assert len(self.range_warnings(warning)) == 1


def description_of(client, name):
    return sorted(read(client, name, ("description",))["description"])


class TestManagedMarker:
    def test_a_created_group_carries_the_marker(self, client):
        reconcile(client, [group("a")])
        assert description_of(client, "a") == ["waldur-managed"]

    def test_adoption_adds_the_marker_and_keeps_the_operators_description(self, client):
        client.add_entry(
            client.dn_under("a", "ou=projects"),
            {
                "objectClass": ["top", "posixGroup"],
                "cn": "a",
                "gidNumber": 20003,
                "description": ["Project A, contact: hpc support"],
            },
        )
        reconcile(client, [group("a")])
        assert description_of(client, "a") == ["Project A, contact: hpc support", "waldur-managed"]

    def test_the_marker_is_a_setting(self, client):
        reconcile(client, [group("a")], managed_marker="managed-by=portal")
        assert description_of(client, "a") == ["managed-by=portal"]

    def test_a_hand_made_unmarked_group_in_the_cluster_survives(self, client):
        add_group(client, "benchmarking", 29500, ["ops"])
        dn = client.dn_under("benchmarking", "ou=projects")
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [dn])]})
        for _ in range(3):
            reconcile(client, [group("a")])
        assert dn in cluster_members(client)
        assert description_of(client, "benchmarking") == []

    def test_a_marked_group_that_drops_out_of_the_listing_leaves_the_cluster(self, client):
        # The offering moved to another provider: this provider no longer lists it.
        reconcile(client, [group("a"), group("b", gid=20004)])
        dn = client.dn_under("b", "ou=projects")
        assert dn in cluster_members(client)
        reconcile(client, [group("a")])
        assert dn not in cluster_members(client)
        assert gid_of(client, "b") == 20004  # the entry itself stays

    def test_an_unlisted_group_marked_with_another_marker_stays(self, client):
        reconcile(client, [group("a"), group("b", gid=20004)], managed_marker="other-portal")
        dn = client.dn_under("b", "ou=projects")
        reconcile(client, [group("a")])
        assert dn in cluster_members(client)

    def test_a_second_cycle_writes_nothing(self, client):
        add_group(client, "a", 20003, ["alice", "bob"])
        reconcile(client, [group("a"), group("b", gid=20004)])
        before = snapshot(client)
        with mock.patch.object(client, "modify_entry") as modify:
            reconcile(client, [group("a"), group("b", gid=20004)])
        modify.assert_not_called()
        assert snapshot(client) == before


class TestApiFailures:
    """A broken Waldur answer leaves the directory exactly as it was."""

    def seeded(self, client):
        backend = backend_on(client, project_groups=ENABLED)
        backend.reconcile_offering(
            rest_client(lambda r: httpx.Response(200, json=[api_item("a", 20003)]))
        )
        assert client.dn_under("a", "ou=projects") in cluster_members(client)
        return backend

    @pytest.mark.parametrize(
        "handler",
        [
            lambda r: httpx.Response(401),
            lambda r: httpx.Response(403),
            lambda r: httpx.Response(502),
            lambda r: httpx.Response(200, json=[]),
            lambda r: httpx.Response(200, json={"detail": "oops"}),
        ],
        ids=["401", "403", "502", "empty", "not-a-list"],
    )
    def test_no_write(self, client, handler):
        backend = self.seeded(client)
        before = snapshot(client)
        backend.reconcile_offering(rest_client(handler))
        assert snapshot(client) == before

    def test_failure_on_a_later_page(self, client):
        backend = self.seeded(client)
        before = snapshot(client)

        def handler(request):
            if request.url.params.get("page") == "2":
                return httpx.Response(502)
            return httpx.Response(
                200,
                json=[api_item("b", 20004, offerings=())],
                headers={"Link": '<https://waldur.example.com/x/?page=2>; rel="next"'},
            )

        backend.reconcile_offering(rest_client(handler))
        assert snapshot(client) == before

    def test_backend_unreachable(self, client):
        backend = self.seeded(client)
        before = snapshot(client)

        def handler(request):
            raise httpx.ConnectError("connection refused", request=request)

        backend.reconcile_offering(rest_client(handler))
        assert snapshot(client) == before

    def test_a_listing_that_changed_while_paging_writes_nothing(self, client):
        backend = self.seeded(client)
        before = snapshot(client)
        # Page 1 says 2 groups; by page 2 a third exists and "a" slid off the
        # end of page 1 -- the same group is seen twice and "a" not at all.
        pages = {
            None: ([api_item("b", 20004)], "2"),
            "2": ([api_item("b", 20004)], "3"),
        }

        def handler(request):
            items, count = pages[request.url.params.get("page")]
            headers = {"X-Result-Count": count}
            if request.url.params.get("page") is None:
                headers["Link"] = '<https://waldur.example.com/x/?page=2>; rel="next"'
            return httpx.Response(200, json=items, headers=headers)

        backend.reconcile_offering(rest_client(handler))
        assert snapshot(client) == before


class TestTwoAgentsAtOnce:
    def test_racing_first_cycles_converge(self, client):
        add_second_cluster(client)
        shared_dn = f"cn=shared,ou=clusters,{BASE_DN}"
        client.add_entry(
            shared_dn, {"objectClass": ["top", "groupOfNames"], "cn": "shared", "member": STAND_IN}
        )
        shared = {"dn": shared_dn, "offering_uuids": [OFFERING, SIBLING]}
        x_parents = [{"dn": CLUSTER_DN}, shared]
        y_parents = [{"dn": SECOND_CLUSTER_DN}, shared]
        listed = [
            group(f"p{i}", gid=20100 + i, offerings=(OFFERING,) if i % 2 else (SIBLING,))
            for i in range(10)
        ]
        y = MockLdapClient(SETTINGS, sibling_of=client)

        # Y reads the empty project OU, then X runs a whole cycle before Y writes.
        real_list = y.list_entries
        raced = []

        def list_then_let_x_run(ou, *args):
            result = real_list(ou, *args)
            if ou == "ou=projects" and not raced:
                raced.append(True)
                reconcile(client, listed, parents=x_parents)
            return result

        with mock.patch.object(y, "list_entries", side_effect=list_then_let_x_run):
            first = reconcile(y, listed, offering=SIBLING, parents=y_parents)
        # Every add lost the race; each was read back and reconciled as existing.
        assert (first.failed, first.created, first.kept) == (0, 0, 10)

        reconcile(client, listed, parents=x_parents)
        reconcile(y, listed, offering=SIBLING, parents=y_parents)
        assert len(client.list_entries("ou=projects", "(cn=*)", ["cn"])) == 10
        assert len(members_of(client, shared_dn)) == 11  # all ten plus the stand-in
        odd = {client.dn_under(f"p{i}", "ou=projects") for i in range(1, 10, 2)}
        even = {client.dn_under(f"p{i}", "ou=projects") for i in range(0, 10, 2)}
        assert set(members_of(client, CLUSTER_DN)) - {STAND_IN} == odd
        assert set(members_of(client, SECOND_CLUSTER_DN)) - {STAND_IN} == even

        before = snapshot(client)
        third_x = reconcile(client, listed, parents=x_parents)
        third_y = reconcile(y, listed, offering=SIBLING, parents=y_parents)
        assert snapshot(client) == before
        assert third_x.parent_updates == third_y.parent_updates == 0

    def test_a_shared_parent_without_offering_uuids_flaps(self, client):
        """Documented hazard: each agent takes out what the other added."""
        y = MockLdapClient(SETTINGS, sibling_of=client)
        listed = [group("x", gid=20100), group("y", gid=20101, offerings=(SIBLING,))]
        reconcile(client, listed)
        reconcile(y, listed, offering=SIBLING)
        assert client.dn_under("x", "ou=projects") not in cluster_members(client)
        assert client.dn_under("y", "ou=projects") in cluster_members(client)


class TestHandEdits:
    def test_a_deleted_group_entry_is_recreated(self, client):
        reconcile(client, [group("a")])
        conn = client._connect()
        conn.delete(client.dn_under("a", "ou=projects"))
        reconcile(client, [group("a")])
        assert gid_of(client, "a") == 20003
        assert sorted(read(client, "a")["memberUid"]) == ["alice", "bob"]

    def test_a_dn_removed_from_the_cluster_is_added_back(self, client):
        from ldap3 import MODIFY_DELETE

        reconcile(client, [group("a")])
        dn = client.dn_under("a", "ou=projects")
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_DELETE, [dn])]})
        reconcile(client, [group("a")])
        assert dn in cluster_members(client)

    def test_a_deleted_cluster_entry_fails_only_that_parent(self, client):
        add_second_cluster(client)
        parents = [{"dn": CLUSTER_DN}, {"dn": SECOND_CLUSTER_DN}]
        conn = client._connect()
        conn.delete(CLUSTER_DN)
        report = reconcile(client, [group("a")], parents=parents)
        assert report.failed == 1
        dn = client.dn_under("a", "ou=projects")
        assert dn in members_of(client, SECOND_CLUSTER_DN)

        client.add_entry(
            CLUSTER_DN, {"objectClass": ["top", "groupOfNames"], "cn": "alps", "member": STAND_IN}
        )
        reconcile(client, [group("a")], parents=parents)
        assert dn in cluster_members(client)

    def test_removing_the_last_dn_of_a_cluster_adds_the_placeholder_in_the_same_modify(
        self, client
    ):
        from ldap3 import MODIFY_ADD as ADD
        from ldap3 import MODIFY_DELETE

        reconcile(client, [group("a")])
        dn = client.dn_under("a", "ou=projects")
        # The operator dropped the placeholder: the group is the only member.
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_DELETE, [STAND_IN])]})
        assert cluster_members(client) == [dn]
        with mock.patch.object(client, "modify_entry", wraps=client.modify_entry) as modify:
            reconcile(client, [group("a", offerings=())])
        assert modify.call_count == 1
        changes = modify.call_args.args[1]["member"]
        assert (ADD, [STAND_IN]) in changes and (MODIFY_DELETE, [dn]) in changes
        assert cluster_members(client) == [STAND_IN]


class TestHeldGids:
    def test_a_same_named_group_outside_the_project_ou_holds_the_name(self, client):
        client.add_entry(f"ou=Groups,{BASE_DN}", {"objectClass": "top"})
        client.add_entry(
            f"cn=a,ou=Groups,{BASE_DN}",
            {"objectClass": ["top", "posixGroup"], "cn": "a", "gidNumber": 29000},
        )
        with mock.patch.object(project_groups.logger, "error") as error:
            first = reconcile(client, [group("a")])
            second = reconcile(client, [group("a")])
        assert first.conflicts == second.conflicts == 1
        assert error.call_count == 2
        assert read(client, "a") is None
        assert cluster_members(client) == [STAND_IN]

    def test_a_users_primary_gid_holds_it_until_waldur_moves_the_group(self, client):
        add_user(client, "carol", gid=20003)
        assert reconcile(client, [group("a")]).conflicts == 1
        assert read(client, "a") is None
        # set_gid in Waldur to a free value: written within one cycle.
        assert reconcile(client, [group("a", gid=20004)]).created == 1
        assert client.dn_under("a", "ou=projects") in cluster_members(client)


class TestOverride:
    def test_report_keeps_the_entry_and_follows_members_and_parents(self, client):
        reconcile(client, [group("a", gid=20004)])
        report = reconcile(client, [group("a", gid=20010, members=("carol",))])
        assert report.conflicts == 1
        assert gid_of(client, "a") == 20004
        assert read(client, "a")["memberUid"] == ["carol"]
        assert client.dn_under("a", "ou=projects") in cluster_members(client)

    def test_adopt_renumbers(self, client):
        reconcile(client, [group("a", gid=20004)])
        report = reconcile(client, [group("a", gid=20010)], on_gid_mismatch="adopt")
        assert report.renumbered == 1
        assert gid_of(client, "a") == 20010

    def test_adopt_refuses_a_held_target(self, client):
        reconcile(client, [group("a", gid=20004)])
        add_user(client, "carol", gid=20010)
        report = reconcile(client, [group("a", gid=20010)], on_gid_mismatch="adopt")
        assert report.conflicts == 1
        assert gid_of(client, "a") == 20004


class TestWithoutTheNewSettings:
    def test_no_project_groups_block_means_no_directory_call(self):
        with mock.patch("waldur_site_agent_ldap.backend.LdapClient"):
            backend = LdapUsernameBackend(
                backend_settings={"ldap": dict(SETTINGS)},
                offering=SimpleNamespace(name="HPC", uuid=OFFERING),
            )
        rest = mock.Mock()
        backend.reconcile_offering(rest)
        assert backend.client.mock_calls == []
        assert rest.mock_calls == []


class TestWithoutPersonalGroups:
    """personal_groups: false in a directory that has no groups OU at all."""

    def test_accounts_come_and_go_without_a_groups_ou(self, client):
        backend = backend_on(client)
        user = SimpleNamespace(
            state=OfferingUserState.OK,
            uuid="ou-1",
            username="alice",
            uidnumber=10001,
            primarygroup=20001,
            home_directory="/home/alice",
            login_shell="/bin/bash",
            user_first_name="Alice",
            user_last_name="A",
            user_email="alice@example.org",
            user_username="alice-cuid",
            user_uuid="u-1",
            customer_uuid="c-1",
        )
        backend.sync_user_profiles([user])
        entry = client.search_user("alice")
        assert int(entry["gidNumber"][0]) == 20001
        assert client.read_entry(f"ou=Groups,{BASE_DN}", ["objectClass"]) is None

        with mock.patch(
            "waldur_site_agent_ldap.backend.marketplace_offering_users_list"
        ) as listing:
            listing.sync_all.return_value = []
            backend.release_users([user], mock.Mock())
        # Waldur-authoritative default: the entry is parked, not deleted.
        assert client.is_disabled_by_agent(client.search_user("alice"))


class TestRenamesAndSchemaErrors:
    def test_a_rename_swaps_under_sync_and_keeps_the_old_name_under_add_only(self, client):
        reconcile(client, [group("a", members=("old",))])
        reconcile(client, [group("a", members=("new",))])
        assert read(client, "a")["memberUid"] == ["new"]

        reconcile(client, [group("b", gid=20004, members=("old",))], membership="add_only")
        reconcile(client, [group("b", gid=20004, members=("new",))], membership="add_only")
        assert sorted(read(client, "b")["memberUid"]) == ["new", "old"]

    def test_one_rejected_group_does_not_stop_the_others(self, client):
        real_add = client.add_entry

        def add(dn, attributes):
            if attributes["cn"] == "bad":
                raise project_groups.BackendError("objectClassViolation")
            return real_add(dn, attributes)

        with mock.patch.object(client, "add_entry", side_effect=add):
            report = reconcile(client, [group("bad"), group("good", gid=20004)])
        assert (report.failed, report.created) == (1, 1)
        assert cluster_members(client) == sorted(
            [STAND_IN, client.dn_under("good", "ou=projects")]
        )


class TestLastProjectLeaves:
    """The last project leaves and every account is released: the cluster is still cleaned."""

    def test_dn_leaves_the_cluster_with_no_offering_user_left(self, client):
        backend = backend_on(client, project_groups=ENABLED)
        offering = structures.Offering(
            name="ldap-only",
            waldur_offering_uuid=OFFERING,
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="token",
            backend_type="ldap",
            username_management_backend="ldap",
            stomp_enabled=False,
        )
        listing = {"groups": [api_item("a", 20003, offerings=(OFFERING,))]}

        def handler(request):
            return httpx.Response(200, json=listing["groups"])

        def cycle():
            with mock.patch(
                "waldur_site_agent.common.utils.get_username_management_backend",
                return_value=(backend, "1.0"),
            ), mock.patch.object(
                event_utils, "get_client_for_offering", return_value=rest_client(handler)
            ), mock.patch.object(event_utils, "marketplace_offering_users_list") as users, (
                mock.patch.object(handlers, "process_offering_user_deletions")
            ):
                # No live and no departed offering users left on the offering.
                users.sync_all.side_effect = [[], []]
                event_utils.run_periodic_offering_user_reconciliation([offering], "agent")

        cycle()
        dn = client.dn_under("a", "ou=projects")
        assert dn in cluster_members(client)

        listing["groups"] = [api_item("a", 20003, members=(), offerings=())]
        cycle()
        assert cluster_members(client) == [STAND_IN]
        assert gid_of(client, "a") == 20003


class TestValueRaces:
    def test_a_parent_value_added_by_someone_else_meanwhile_is_not_an_error(self, client):
        dn = client.dn_under("a", "ou=projects")
        real_modify = client.modify_entry
        raced = []

        def race_then_modify(entry_dn, changes):
            if entry_dn == CLUSTER_DN and not raced:
                raced.append(True)
                real_modify(CLUSTER_DN, {"member": [(MODIFY_ADD, [dn])]})
                raise project_groups.ValueConflictError("attributeOrValueExists")
            return real_modify(entry_dn, changes)

        with mock.patch.object(client, "modify_entry", side_effect=race_then_modify):
            report = reconcile(client, [group("a")])
        assert report.failed == 0
        assert cluster_members(client).count(dn) == 1

    def test_a_member_added_by_someone_else_meanwhile_is_not_an_error(self, client):
        add_group(client, "a", 20003, ["alice"])
        real_modify = client.modify_entry
        raced = []

        def race_then_modify(entry_dn, changes):
            if "memberUid" in changes and not raced:
                raced.append(True)
                real_modify(entry_dn, {"memberUid": [(MODIFY_ADD, ["bob"])]})
                # What a real server answers to adding a value now present (the
                # in-memory server does not check).
                raise project_groups.ValueConflictError("attributeOrValueExists")
            return real_modify(entry_dn, changes)

        with mock.patch.object(client, "modify_entry", side_effect=race_then_modify):
            report = reconcile(client, [group("a")])
        assert report.failed == 0
        assert sorted(read(client, "a")["memberUid"]) == ["alice", "bob"]


class TestFetchSafety:
    def make_client(self, handler):
        rest = mock.Mock()
        rest.get_httpx_client.return_value = httpx.Client(
            base_url="https://waldur.example.com", transport=httpx.MockTransport(handler)
        )
        return rest

    def test_a_next_link_to_another_server_is_refused(self):
        seen = []

        def handler(request):
            seen.append(request.url.host)
            return httpx.Response(
                200,
                json=[{"name": "a"}],
                headers={"Link": '<https://evil.example.net/x/?page=2>; rel="next"'},
            )

        with pytest.raises(project_groups.BackendError, match="another server"):
            fetch_provider_project_groups(self.make_client(handler), "x")
        assert seen == ["waldur.example.com"]

    @pytest.mark.parametrize(
        "link",
        ["http://waldur.example.com/x/?page=2", "https://waldur.example.com:8443/x/?page=2"],
        ids=["scheme", "port"],
    )
    def test_another_scheme_or_port_is_refused(self, link):
        def handler(request):
            return httpx.Response(
                200, json=[{"name": "a"}], headers={"Link": f'<{link}>; rel="next"'}
            )

        with pytest.raises(project_groups.BackendError):
            fetch_provider_project_groups(self.make_client(handler), "x")

    def test_the_listing_asks_for_a_stable_order(self):
        seen = []

        def handler(request):
            seen.append(request.url.params.get("o"))
            return httpx.Response(200, json=[{"name": "a"}])

        fetch_provider_project_groups(self.make_client(handler), "x")
        assert seen == ["created"]


class TestTransientErrorsKeepParents:
    def test_a_failed_adopt_write_keeps_the_groups_dn_in_the_parent(self, client):
        reconcile(client, [group("a")])
        dn = client.dn_under("a", "ou=projects")
        assert dn in cluster_members(client)
        real_modify = client.modify_entry

        def fail_on_the_group(entry_dn, changes):
            if entry_dn == dn:
                raise project_groups.BackendError("busy")
            return real_modify(entry_dn, changes)

        # The member sync fails this cycle, and the project also left the offering.
        with mock.patch.object(client, "modify_entry", side_effect=fail_on_the_group):
            report = reconcile(client, [group("a", gid=29999, offerings=())], on_gid_mismatch="adopt")
        assert report.failed == 1
        assert dn in cluster_members(client)

    def test_a_failed_post_race_read_keeps_the_dn(self, client):
        dn = client.dn_under("a", "ou=projects")
        client.modify_entry(CLUSTER_DN, {"member": [(MODIFY_ADD, [dn])]})
        real_read = client.read_entry

        def read(entry_dn, attributes):
            if entry_dn == dn:
                raise project_groups.BackendError("down")
            return real_read(entry_dn, attributes)

        with mock.patch.object(
            client, "add_entry", side_effect=project_groups.EntryExistsError("exists")
        ), mock.patch.object(client, "read_entry", side_effect=read):
            report = reconcile(client, [group("a")])
        assert report.failed == 1
        assert dn in cluster_members(client)


class TestUniqueMemberParents:
    def test_the_last_unique_member_is_replaced_by_the_stand_in(self, client):
        parent = f"cn=uniq,ou=clusters,{BASE_DN}"
        dn = client.dn_under("a", "ou=projects")
        client.add_entry(
            parent,
            {"objectClass": ["top", "groupOfUniqueNames"], "cn": "uniq", "uniqueMember": STAND_IN},
        )
        parents = [{"dn": parent, "attribute": "uniqueMember"}]
        reconcile(client, [group("a")], parents=parents)
        client.modify_entry(parent, {"uniqueMember": [(project_groups.MODIFY_DELETE, [STAND_IN])]})
        with mock.patch.object(client, "modify_entry", wraps=client.modify_entry) as modify:
            reconcile(client, [group("a", offerings=())], parents=parents)
        changes = modify.call_args.args[1]["uniqueMember"]
        assert (project_groups.MODIFY_ADD, [STAND_IN]) in changes
        assert client.read_entry(parent, ["uniqueMember"])["uniqueMember"] == [STAND_IN]
        assert dn not in client.read_entry(parent, ["uniqueMember"])["uniqueMember"]


def org_group(slug, name="proj", gid=20003, members=("alice", "bob")):
    g = group(name=name, gid=gid, members=members)
    g.customer_slug = slug
    return g


def descriptions(client, name="proj"):
    return sorted(read(client, name, ("description",)).get("description", []))


ORG = {"organization_description": "organization={slug}"}


class TestOrganizationDescription:
    def test_nothing_is_written_without_the_setting(self, client):
        reconcile(client, [org_group("cscs")])
        assert descriptions(client) == ["waldur-managed"]

    def test_a_new_group_names_its_organization(self, client):
        reconcile(client, [org_group("cscs")], **ORG)
        assert descriptions(client) == ["organization=cscs", "waldur-managed"]

    def test_an_adopted_group_gets_it_next_to_the_operators_values(self, client):
        add_group(client, "proj", 20003, ["alice", "bob"])
        client.modify_entry(
            client.dn_under("proj", "ou=projects"),
            {"description": [(MODIFY_ADD, ["Firecrest project group"])]},
        )
        report = reconcile(client, [org_group("cscs")], **ORG)
        assert report.description_updates == 1
        assert descriptions(client) == [
            "Firecrest project group",
            "organization=cscs",
            "waldur-managed",
        ]

    def test_a_changed_slug_replaces_only_the_agents_value(self, client):
        reconcile(client, [org_group("cscs")], **ORG)
        client.modify_entry(
            client.dn_under("proj", "ou=projects"),
            {"description": [(MODIFY_ADD, ["kept by the operator"])]},
        )
        report = reconcile(client, [org_group("eth")], **ORG)
        assert report.description_updates == 1
        assert descriptions(client) == [
            "kept by the operator",
            "organization=eth",
            "waldur-managed",
        ]

    def test_a_second_pass_writes_nothing(self, client):
        reconcile(client, [org_group("cscs")], **ORG)
        before = snapshot(client)
        with mock.patch.object(client, "modify_entry") as modify:
            report = reconcile(client, [org_group("cscs")], **ORG)
        assert report.description_updates == 0
        modify.assert_not_called()
        assert snapshot(client) == before

    def test_a_group_whose_project_is_gone_keeps_its_value(self, client):
        reconcile(client, [org_group("cscs")], **ORG)
        reconcile(client, [org_group("")], **ORG)
        assert "organization=cscs" in descriptions(client)

    def test_a_bare_slug_is_added_but_never_removes_anything(self, client):
        with mock.patch("waldur_site_agent_ldap.project_groups.logger") as log:
            reconcile(client, [org_group("cscs")], organization_description="{slug}")
            reconcile(client, [org_group("eth")], organization_description="{slug}")
        assert descriptions(client) == ["cscs", "eth", "waldur-managed"]
        assert any("cannot tell its value" in c.args[0] for c in log.warning.call_args_list)

    def test_the_slug_is_read_from_the_api(self):
        item = {**api_item("proj", 20003), "customer_slug": "cscs"}
        assert ProjectGroup.from_api(item).customer_slug == "cscs"
        assert ProjectGroup.from_api(api_item("proj", 20003)).customer_slug == ""


class TestOrganizationDescriptionSetting:
    @pytest.mark.parametrize("template", ["organization", "{slug}-{slug}"])
    def test_a_template_needs_slug_exactly_once(self, template):
        with pytest.raises(PydanticValidationError):
            LdapSettingsSchema(
                **{
                    **SETTINGS,
                    "account_source": "waldur",
                    "project_groups": {"enabled": True, "organization_description": template},
                }
            )

    def test_a_valid_template_is_accepted(self):
        LdapSettingsSchema(
            **{
                **SETTINGS,
                "account_source": "waldur",
                "project_groups": {"enabled": True, **ORG},
            }
        )
