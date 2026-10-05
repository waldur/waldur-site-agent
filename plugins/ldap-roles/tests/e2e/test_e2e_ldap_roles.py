r"""End-to-end tests for the ldap-roles membership-sync backend.

Runs the real OfferingMembershipProcessor against a real Waldur and a real
OpenLDAP. The suite creates the offering roles, a resource, its
ResourceProjects and the role grants through the Waldur API, then reads the
resulting groups back from the directory.

The resource is created, and finally terminated, through orders that the
agent's real OfferingOrderProcessor runs, with ldap-roles as the
order-processing backend: create sets the backend ID membership sync needs,
and terminate empties the resource's groups.

Requires:
    - Waldur API stack (ci/docker-compose.e2e.yml) with the site_agent_e2e
      preset loaded, which provides the "E2E LDAP Roles" offering
    - OpenLDAP (waldur-ldap service, compose profile "ldap")

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_LDAP_ROLES_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \
    WALDUR_E2E_LDAP_ROLES_CONFIG=ci/e2e-ci-config-ldap-roles.yaml \
    WALDUR_E2E_PROJECT_A_UUID=e2eb0000000000000000000000000001 \
    .venv/bin/python -m pytest plugins/ldap-roles/tests/e2e/test_e2e_ldap_roles.py -v

The tests run in order and share state: each step changes one grant and
checks the directory after a sync.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest
from ldap3 import BASE, LEVEL, SUBTREE, Connection, Server

from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
)
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.common.utils import get_client, load_configuration

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
CONFIG_PATH = os.environ.get("WALDUR_E2E_LDAP_ROLES_CONFIG", "")
PROJECT_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(
    not (E2E_TESTS and CONFIG_PATH and PROJECT_UUID),
    reason="WALDUR_E2E_TESTS, WALDUR_E2E_LDAP_ROLES_CONFIG and WALDUR_E2E_PROJECT_A_UUID required",
)

# Preset users, all PROJECT.MEMBER of project A (site_agent_e2e.json). Their
# Waldur usernames are their LDAP uids.
USERS = {
    "e2euser1": "e2ea0000000000000000000000000002",
    "e2euser2": "e2ea0000000000000000000000000003",
    "e2euser3": "e2ea0000000000000000000000000004",
    "e2euser4": "e2ea0000000000000000000000000005",
}
# e2euser4 deliberately gets no LDAP entry: its grant must report missing_in_idp.
LDAP_USERS = ("e2euser1", "e2euser2", "e2euser3")
RESOURCE_ROLE = "RESOURCE.OPERATOR"
RESOURCE_PROJECT_ROLE = "RESOURCE_PROJECT.MEMBER"
SUITE_OU = "ou=ldap-roles-e2e"
# The DN-based pass writes to its own OU so the two membership types never
# see each other's groups.
MEMBER_GROUPS_OU = f"ou=GroupsDN,{SUITE_OU}"


# ---------------------------------------------------------------------------
# Waldur and LDAP helpers
# ---------------------------------------------------------------------------


class Api:
    """Thin JSON wrapper over the SDK's httpx client; failures carry the response body."""

    def __init__(self, client: Any) -> None:  # noqa: ANN401
        self._http = client.get_httpx_client()

    def call(self, method: str, path: str, body: Optional[dict] = None) -> Any:  # noqa: ANN401
        response = self._http.request(method, path, json=body)
        if response.status_code >= 400:  # noqa: PLR2004
            msg = f"{method} {path} -> {response.status_code}: {response.text}"
            raise AssertionError(msg)
        return response.json() if response.content else None


@dataclass
class World:
    """Everything the steps share."""

    offering: Offering
    waldur_client: Any
    api: Api
    ldap: Connection
    base_dn: str
    resource_uuid: str = ""
    resource_slug: str = ""
    roles: dict[str, str] = field(default_factory=dict)  # role name -> uuid
    resource_projects: dict[str, str] = field(default_factory=dict)  # label -> uuid

    def sync(self, offering: Optional[Offering] = None) -> None:
        OfferingMembershipProcessor(offering or self.offering, self.waldur_client).process_offering()

    def group_name(self, role_token: str, rp_label: Optional[str] = None) -> str:
        if rp_label is None:
            return f"{self.resource_slug}_{role_token}"
        return f"{self.resource_slug}_{self.resource_projects[rp_label][:8]}_{role_token}"

    def group(self, name: str, groups_ou: str = f"ou=Groups,{SUITE_OU}") -> Optional[dict]:
        self.ldap.search(
            f"cn={name},{groups_ou},{self.base_dn}",
            "(objectClass=*)",
            BASE,
            attributes=["memberUid", "member", "description", "objectClass", "gidNumber"],
        )
        if not self.ldap.entries:
            return None
        return {
            key: sorted(str(v) for v in values)
            for key, values in self.ldap.entries[0].entry_attributes_as_dict.items()
        }

    def sync_states(self) -> dict[tuple[str, Optional[str], str], tuple[str, str]]:
        """Agent-reported grant states as Waldur serves them.

        Keyed by (username, ResourceProject UUID or None, role name); the
        value is (sync_state, sync_message).
        """
        members = self.api.call(
            "GET", f"/api/marketplace-resources/{self.resource_uuid}/team_members/?page_size=100"
        )
        states = {}
        for member in members:
            for grant in member.get("roles") or []:
                states[(member["username"], None, grant["role_name"])] = (
                    grant.get("sync_state"),
                    grant.get("sync_message") or "",
                )
            for grant in member.get("resource_projects") or []:
                rp_uuid = grant["uuid"].replace("-", "")
                states[(member["username"], rp_uuid, grant["role_name"])] = (
                    grant.get("sync_state"),
                    grant.get("sync_message") or "",
                )
        return states

    def members(self, name: str) -> list[str]:
        group = self.group(name)
        assert group is not None, f"group {name} does not exist"
        return group.get("memberUid", [])

    # -- grants ------------------------------------------------------------

    def grant_resource(self, username: str) -> None:
        self.api.call(
            "POST",
            f"/api/marketplace-resources/{self.resource_uuid}/add_user/",
            {"user": USERS[username], "role": self.roles[RESOURCE_ROLE]},
        )

    def revoke_resource(self, username: str) -> None:
        self.api.call(
            "POST",
            f"/api/marketplace-resources/{self.resource_uuid}/delete_user/",
            {"user": USERS[username], "role": self.roles[RESOURCE_ROLE]},
        )

    def grant_rp(self, label: str, username: str) -> None:
        self.api.call(
            "POST",
            f"/api/marketplace-resource-projects/{self.resource_projects[label]}/add_user/",
            {"user": USERS[username], "role": self.roles[RESOURCE_PROJECT_ROLE]},
        )

    def revoke_rp(self, label: str, username: str) -> None:
        self.api.call(
            "POST",
            f"/api/marketplace-resource-projects/{self.resource_projects[label]}/delete_user/",
            {"user": USERS[username], "role": self.roles[RESOURCE_PROJECT_ROLE]},
        )


def _ensure_offering_role(api: Api, offering_uuid: str, name: str, scope: str) -> str:
    existing = api.call(
        "GET", f"/api/marketplace-offering-roles/?offering_uuid={offering_uuid}&name={name}"
    )
    if existing:
        return existing[0]["uuid"]
    created = api.call(
        "POST",
        "/api/marketplace-offering-roles/",
        {"name": name, "content_type_input": scope, "offering": offering_uuid},
    )
    return created["uuid"]


def _run_order(api: Api, offering: Offering, waldur_client: Any, order_uuid: str) -> None:  # noqa: ANN401
    """Drive an order to done through the agent's real order processor."""
    processor = OfferingOrderProcessor(offering, waldur_client)
    deadline = time.monotonic() + 120
    while True:
        order = api.call("GET", f"/api/marketplace-orders/{order_uuid}/")
        state = order["state"]
        if state == "done":
            return
        assert state not in ("erred", "rejected", "canceled"), (
            f"order {order_uuid} is {state}: {order.get('error_message')}"
        )
        if state == "pending-consumer":
            api.call("POST", f"/api/marketplace-orders/{order_uuid}/approve_by_consumer/", {})
        else:
            processor.process_offering()
        assert time.monotonic() < deadline, f"order {order_uuid} stuck in {state}"
        time.sleep(1)


def _create_resource(api: Api, offering: Offering, waldur_client: Any) -> dict:  # noqa: ANN401
    """Order a resource and let the agent's order processor create it."""
    public = api.call(
        "GET", f"/api/marketplace-public-offerings/{offering.waldur_offering_uuid}/"
    )
    project = api.call("GET", f"/api/projects/{PROJECT_UUID}/")
    order = api.call(
        "POST",
        "/api/marketplace-orders/",
        {
            "offering": public["url"],
            "project": project["url"],
            "plan": public["plans"][0]["url"],
            "limits": {"cpu": 1},
            "attributes": {"name": f"ldap-roles-e2e-{uuid.uuid4().hex[:6]}"},
        },
    )
    _run_order(api, offering, waldur_client, order["uuid"])

    resource_uuid = api.call("GET", f"/api/marketplace-orders/{order['uuid']}/")[
        "marketplace_resource_uuid"
    ]
    resource = api.call("GET", f"/api/marketplace-provider-resources/{resource_uuid}/")
    # Membership sync only visits resources with a backend ID; create sets it.
    assert resource["backend_id"], "create did not set the resource's backend ID"
    return resource


def _delete_subtree(conn: Connection, dn: str) -> None:
    if not conn.search(dn, "(objectClass=*)", SUBTREE, attributes=[]):
        return
    for entry_dn in sorted((e.entry_dn for e in conn.entries), key=len, reverse=True):
        conn.delete(entry_dn)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world():
    config = load_configuration(CONFIG_PATH, user_agent_suffix="e2e-ldap-roles-test")
    offering = config.offerings[0]
    ldap_settings = offering.backend_settings["ldap"]
    base_dn = ldap_settings["base_dn"]

    ldap = Connection(
        Server(ldap_settings["uri"]),
        ldap_settings["bind_dn"],
        ldap_settings["bind_password"],
        auto_bind=True,
    )
    suite_dn = f"{SUITE_OU},{base_dn}"
    _delete_subtree(ldap, suite_dn)  # leftovers from an interrupted run
    for dn in (suite_dn, f"ou=People,{suite_dn}", f"ou=Groups,{suite_dn}", f"{MEMBER_GROUPS_OU},{base_dn}"):
        assert ldap.add(dn, ["organizationalUnit"]), ldap.result
    for username in LDAP_USERS:
        assert ldap.add(
            f"uid={username},ou=People,{suite_dn}",
            ["inetOrgPerson"],
            {"cn": username, "sn": username},
        ), ldap.result

    waldur_client = get_client(offering.waldur_api_url, offering.waldur_api_token)
    api = Api(waldur_client)
    resource = _create_resource(api, offering, waldur_client)
    w = World(
        offering=offering,
        waldur_client=waldur_client,
        api=api,
        ldap=ldap,
        base_dn=base_dn,
        resource_uuid=resource["uuid"],
        resource_slug=resource["slug"],
    )
    w.roles = {
        RESOURCE_ROLE: _ensure_offering_role(api, offering.waldur_offering_uuid, RESOURCE_ROLE, "resource"),
        RESOURCE_PROJECT_ROLE: _ensure_offering_role(
            api, offering.waldur_offering_uuid, RESOURCE_PROJECT_ROLE, "resource_project"
        ),
    }
    for label in ("alpha", "beta", "gamma"):
        rp = api.call(
            "POST",
            "/api/marketplace-resource-projects/",
            {"resource": w.resource_uuid, "name": f"ldap-roles-e2e-{label}"},
        )
        w.resource_projects[label] = rp["uuid"]

    yield w

    _delete_subtree(ldap, suite_dn)
    ldap.unbind()


# ---------------------------------------------------------------------------
# memberUid: grants, revocation, ownership
# ---------------------------------------------------------------------------


class TestLdapRolesMemberUid:
    def test_01_grants_create_owned_groups(self, world: World):
        # A group of gamma's name that the agent did not create.
        foreign = world.group_name("member", "gamma")
        assert world.ldap.add(
            f"cn={foreign},ou=Groups,{SUITE_OU},{world.base_dn}",
            ["posixGroup"],
            {"cn": foreign, "gidNumber": 7999, "memberUid": ["outsider"]},
        ), world.ldap.result

        world.grant_resource("e2euser1")
        world.grant_rp("alpha", "e2euser2")
        world.grant_rp("alpha", "e2euser3")
        world.grant_rp("beta", "e2euser3")
        world.grant_rp("gamma", "e2euser1")
        world.grant_rp("alpha", "e2euser4")  # no LDAP entry

        world.sync()

        marker = f"managed_by=waldur-site-agent;resource={world.resource_uuid}"
        operator = world.group(world.group_name("operator"))
        assert operator is not None
        assert operator["memberUid"] == ["e2euser1"]
        assert operator["description"] == [marker]
        assert world.members(world.group_name("member", "alpha")) == ["e2euser2", "e2euser3"]
        assert world.members(world.group_name("member", "beta")) == ["e2euser3"]
        # Not ours: neither reconciled nor adopted.
        gamma = world.group(foreign)
        assert gamma is not None
        assert gamma["memberUid"] == ["outsider"]
        assert "description" not in gamma or marker not in gamma["description"]

    def test_02_grant_sync_states_reach_waldur(self, world: World):
        """The per-grant report from the sync in test_01, as Waldur serves it."""
        rps = world.resource_projects
        states = world.sync_states()

        assert states[("e2euser1", None, RESOURCE_ROLE)] == ("synced", "")
        assert states[("e2euser2", rps["alpha"], RESOURCE_PROJECT_ROLE)] == ("synced", "")
        assert states[("e2euser3", rps["alpha"], RESOURCE_PROJECT_ROLE)] == ("synced", "")
        assert states[("e2euser3", rps["beta"], RESOURCE_PROJECT_ROLE)] == ("synced", "")

        state, message = states[("e2euser4", rps["alpha"], RESOURCE_PROJECT_ROLE)]
        assert state == "missing_in_idp"
        assert message.startswith("No LDAP entry")

        # gamma's group exists but is not the agent's: the grant cannot land.
        state, message = states[("e2euser1", rps["gamma"], RESOURCE_PROJECT_ROLE)]
        assert state == "error"
        assert world.group_name("member", "gamma") in message

    def test_03_resync_changes_nothing(self, world: World):
        before = {
            label: world.group(world.group_name("member", label)) for label in ("alpha", "beta")
        }
        world.sync()
        after = {
            label: world.group(world.group_name("member", label)) for label in ("alpha", "beta")
        }
        assert after == before

    def test_04_revoking_a_member_removes_them(self, world: World):
        world.revoke_rp("alpha", "e2euser2")
        world.sync()
        assert world.members(world.group_name("member", "alpha")) == ["e2euser3"]

    def test_05_revoking_the_last_holder_empties_the_group(self, world: World):
        world.revoke_rp("beta", "e2euser3")
        world.sync()
        # Emptied, not deleted: its GID must not pass to the next group.
        assert world.members(world.group_name("member", "beta")) == []

    def test_06_deleting_a_resource_project_empties_its_group(self, world: World):
        alpha = world.group_name("member", "alpha")
        world.api.call(
            "DELETE", f"/api/marketplace-resource-projects/{world.resource_projects['alpha']}/"
        )
        world.sync()
        assert world.members(alpha) == []

    def test_07_revoking_the_resource_role_empties_its_group(self, world: World):
        world.revoke_resource("e2euser1")
        world.sync()
        assert world.members(world.group_name("operator")) == []


# ---------------------------------------------------------------------------
# member: groupOfNames with the stand-in member
# ---------------------------------------------------------------------------


class TestLdapRolesMemberDn:
    @pytest.fixture(scope="class")
    def dn_offering(self, world: World) -> Offering:
        offering = world.offering.model_copy(deep=True)
        offering.backend_settings["membership_type"] = "member"
        offering.backend_settings["ldap"]["groups_ou"] = MEMBER_GROUPS_OU
        return offering

    def _members(self, world: World, name: str) -> list[str]:
        group = world.group(name, MEMBER_GROUPS_OU)
        assert group is not None, f"group {name} does not exist"
        assert group["objectClass"] == sorted(["groupOfNames", "top"])
        assert "gidNumber" not in group or group["gidNumber"] == []
        return group["member"]

    def test_01_grant_creates_a_group_of_names(self, world: World, dn_offering: Offering):
        world.grant_rp("beta", "e2euser2")
        world.sync(dn_offering)

        user_dn = f"uid=e2euser2,ou=People,{SUITE_OU},{world.base_dn}"
        stand_in = f"cn=nobody,{world.base_dn}"
        assert self._members(world, world.group_name("member", "beta")) == sorted(
            [stand_in, user_dn]
        )

    def test_02_revoking_the_last_member_leaves_the_stand_in(
        self, world: World, dn_offering: Offering
    ):
        world.revoke_rp("beta", "e2euser2")
        world.sync(dn_offering)

        assert self._members(world, world.group_name("member", "beta")) == [
            f"cn=nobody,{world.base_dn}"
        ]


def test_terminating_the_resource_empties_its_groups(world: World):
    """Terminate runs through the agent's order processor and revokes the resource's grants.

    Membership sync stops visiting a terminated resource, so this is the
    only point at which its groups can be emptied.
    """
    beta = world.group_name("member", "beta")
    world.grant_rp("beta", "e2euser3")
    world.sync()
    assert world.members(beta) == ["e2euser3"]

    order = world.api.call(
        "POST", f"/api/marketplace-resources/{world.resource_uuid}/terminate/", {}
    )
    _run_order(world.api, world.offering, world.waldur_client, order["order_uuid"])

    assert world.members(beta) == []
    # The group of gamma's name was never this resource's, so it is left alone.
    assert world.members(world.group_name("member", "gamma")) == ["outsider"]


def test_directory_layout_is_isolated(world: World):
    """Everything the suite wrote sits under its own OU."""
    world.ldap.search(f"ou=Groups,{world.base_dn}", "(cn=*ldaproles*)", LEVEL, attributes=["cn"])
    assert world.ldap.entries == []
