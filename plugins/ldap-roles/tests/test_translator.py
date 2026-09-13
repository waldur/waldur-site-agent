"""Unit tests for the translator (pure logic, no I/O)."""

from waldur_site_agent_ldap_roles import translator as t


def _ur(role: str, username: str, uuid: str = "") -> dict:
    return {
        "role_name": role,
        "user_username": username,
        "user_uuid": uuid or f"uuid-{username}",
    }


# ---------------------------------------------------------------------
# render_group_name
# ---------------------------------------------------------------------


class TestRenderGroupName:
    def test_substitutes_resource_slug_and_role(self):
        assert (
            t.render_group_name(
                "${resource_slug}_${role_name}", role_name="admin", resource_slug="cluster1"
            )
            == "cluster1_admin"
        )

    def test_substitutes_rp_uuid_short(self):
        assert (
            t.render_group_name(
                "${resource_slug}_${rp_uuid_short}_${role_name}",
                role_name="member",
                resource_slug="cluster1",
                rp_uuid_short="abcd1234",
            )
            == "cluster1_abcd1234_member"
        )

    def test_unknown_placeholder_left_in_place(self):
        # safe_substitute, not strict.
        assert (
            t.render_group_name("${resource_slug}_${nope}", role_name="x", resource_slug="r")
            == "r_${nope}"
        )

    def test_missing_optional_substitutes_empty(self):
        assert (
            t.render_group_name(
                "${customer_slug}_${resource_slug}_${role_name}",
                role_name="admin",
                resource_slug="cluster1",
            )
            == "_cluster1_admin"
        )


# ---------------------------------------------------------------------
# build_groups
# ---------------------------------------------------------------------


class TestBuildGroups:
    def test_empty_user_roles_produces_no_groups(self):
        assert t.build_groups([], template="${role_name}", role_map={"OWNER": "owner"}) == []

    def test_roles_outside_role_map_are_dropped(self):
        groups = t.build_groups(
            [_ur("UNMAPPED", "alice"), _ur("OWNER", "bob")],
            template="g_${role_name}",
            role_map={"OWNER": "owner"},
        )
        assert len(groups) == 1
        assert groups[0].name == "g_owner"
        assert groups[0].members == frozenset({"bob"})

    def test_multiple_roles_one_group_per_role(self):
        groups = t.build_groups(
            [
                _ur("OWNER", "alice"),
                _ur("MANAGER", "bob"),
                _ur("OWNER", "carol"),
            ],
            template="g_${role_name}",
            role_map={"OWNER": "owner", "MANAGER": "manager"},
        )
        names = sorted(g.name for g in groups)
        assert names == ["g_manager", "g_owner"]
        owner = next(g for g in groups if g.name == "g_owner")
        assert owner.members == frozenset({"alice", "carol"})

    def test_uses_uuid_when_lookup_by_user_uuid_set(self):
        groups = t.build_groups(
            [_ur("OWNER", "alice", uuid="uuid-1")],
            template="g_${role_name}",
            role_map={"OWNER": "owner"},
            lookup_by_user_uuid=True,
        )
        assert groups[0].members == frozenset({"uuid-1"})

    def test_role_with_no_resolvable_members_is_dropped(self):
        # Member without username AND without uuid → no identifier.
        groups = t.build_groups(
            [{"role_name": "OWNER", "user_username": None, "user_uuid": None}],
            template="g_${role_name}",
            role_map={"OWNER": "owner"},
        )
        assert groups == []


# ---------------------------------------------------------------------
# build_desired_state — Resource × ResourceProject scope split
# ---------------------------------------------------------------------


class TestBuildDesiredState:
    def _resource(self) -> dict:
        return {
            "uuid": "res-uuid",
            "slug": "cluster1",
            "customer_slug": "acme",
            "project_slug": "alpha",
        }

    def test_resource_only_when_only_resource_role_map_set(self):
        desired = t.build_desired_state(
            resource=self._resource(),
            resource_user_roles=[_ur("RESOURCE.ADMIN", "alice")],
            resource_project_user_roles=[],
            settings={
                "resource_role_map": {"RESOURCE.ADMIN": "admin"},
                "resource_group_template": "${resource_slug}_${role_name}",
            },
        )
        assert [g.name for g in desired] == ["cluster1_admin"]
        assert desired[0].members == frozenset({"alice"})

    def test_resource_project_only_when_only_rp_role_map_set(self):
        rp = {"uuid": "abcd1234deadbeef0000000000000000", "name": "RP-1"}
        desired = t.build_desired_state(
            resource=self._resource(),
            resource_user_roles=[],
            resource_project_user_roles=[(rp, [_ur("RESOURCE_PROJECT.MEMBER", "bob")])],
            settings={
                "resource_project_role_map": {"RESOURCE_PROJECT.MEMBER": "member"},
                "resource_project_group_template": (
                    "${resource_slug}_${rp_uuid_short}_${role_name}"
                ),
            },
        )
        assert [g.name for g in desired] == ["cluster1_abcd1234_member"]
        assert desired[0].members == frozenset({"bob"})

    def test_resource_and_rp_scopes_produce_distinct_groups(self):
        rp = {"uuid": "abcd1234deadbeef0000000000000000", "name": "RP-1"}
        desired = t.build_desired_state(
            resource=self._resource(),
            resource_user_roles=[_ur("RESOURCE.ADMIN", "alice")],
            resource_project_user_roles=[(rp, [_ur("RESOURCE_PROJECT.MEMBER", "bob")])],
            settings={
                "resource_role_map": {"RESOURCE.ADMIN": "admin"},
                "resource_project_role_map": {"RESOURCE_PROJECT.MEMBER": "member"},
                "resource_group_template": "${resource_slug}_${role_name}",
                "resource_project_group_template": (
                    "${resource_slug}_${rp_uuid_short}_${role_name}"
                ),
            },
        )
        names = [g.name for g in desired]
        assert "cluster1_admin" in names
        assert "cluster1_abcd1234_member" in names
        # And they are independent — two separate groups.
        assert len(desired) == 2

    def test_groups_with_same_rendered_name_across_scopes_merge_members(self):
        # Both templates set to the same shape so the rendered names
        # collide. Different scopes contribute different members; they
        # should union into one group rather than overwrite.
        rp = {"uuid": "abcd1234deadbeef0000000000000000", "name": "RP-1"}
        desired = t.build_desired_state(
            resource=self._resource(),
            resource_user_roles=[_ur("RESOURCE.ADMIN", "alice")],
            resource_project_user_roles=[(rp, [_ur("RESOURCE_PROJECT.ADMIN", "bob")])],
            settings={
                "resource_role_map": {"RESOURCE.ADMIN": "admin"},
                "resource_project_role_map": {"RESOURCE_PROJECT.ADMIN": "admin"},
                "resource_group_template": "${resource_slug}_${role_name}",
                "resource_project_group_template": "${resource_slug}_${role_name}",
            },
        )
        assert len(desired) == 1
        assert desired[0].name == "cluster1_admin"
        assert desired[0].members == frozenset({"alice", "bob"})

    def test_no_role_maps_means_no_api_calls_no_groups(self):
        desired = t.build_desired_state(
            resource=self._resource(),
            resource_user_roles=[_ur("RESOURCE.ADMIN", "alice")],
            resource_project_user_roles=[],
            settings={},
        )
        assert desired == []


# ---------------------------------------------------------------------
# diff_members
# ---------------------------------------------------------------------


class TestDiffMembers:
    def test_add_members_not_in_current(self):
        to_add, to_remove = t.diff_members(["alice"], frozenset({"alice", "bob"}))
        assert to_add == ["bob"]
        assert to_remove == []

    def test_remove_members_not_in_desired(self):
        to_add, to_remove = t.diff_members(["alice", "bob"], frozenset({"alice"}))
        assert to_add == []
        assert to_remove == ["bob"]

    def test_no_change_when_sets_equal(self):
        to_add, to_remove = t.diff_members(
            ["alice", "bob"], frozenset({"alice", "bob"})
        )
        assert to_add == []
        assert to_remove == []

    def test_mixed_add_remove(self):
        to_add, to_remove = t.diff_members(
            ["alice", "bob", "carol"], frozenset({"alice", "dave"})
        )
        assert to_add == ["dave"]
        assert to_remove == ["bob", "carol"]


# ---------------------------------------------------------------------
# build_grants / group_grants
# ---------------------------------------------------------------------


class TestBuildGrants:
    SETTINGS = {
        "resource_role_map": {"RESOURCE.ADMIN": "admin"},
        "resource_project_role_map": {"RESOURCE_PROJECT.MEMBER": "member"},
    }
    RESOURCE = {"slug": "cluster1"}
    RP = {"uuid": "abcd1234deadbeef0000000000000000", "name": "RP-1"}

    def _grants(self, **settings):
        return t.build_grants(
            resource=self.RESOURCE,
            resource_user_roles=[_ur("RESOURCE.ADMIN", "alice"), _ur("UNMAPPED", "zed")],
            resource_project_user_roles=[(self.RP, [_ur("RESOURCE_PROJECT.MEMBER", "bob")])],
            settings={**self.SETTINGS, **settings},
        )

    def test_one_grant_per_mapped_role_with_its_group(self):
        assert self._grants() == [
            t.Grant("resource", None, "RESOURCE.ADMIN", "alice", "cluster1_admin"),
            t.Grant(
                "resource_project",
                self.RP["uuid"],
                "RESOURCE_PROJECT.MEMBER",
                "bob",
                "cluster1_abcd1234_member",
            ),
        ]

    def test_uuid_lookup_uses_the_user_uuid_as_member(self):
        assert [g.member for g in self._grants(lookup_by_user_uuid=True)] == [
            "uuid-alice",
            "uuid-bob",
        ]

    def test_grants_without_an_identifier_are_dropped(self):
        grants = t.build_grants(
            resource=self.RESOURCE,
            resource_user_roles=[{"role_name": "RESOURCE.ADMIN", "user_username": None}],
            resource_project_user_roles=[],
            settings=self.SETTINGS,
        )
        assert grants == []

    def test_group_grants_matches_build_desired_state(self):
        kwargs = {
            "resource": self.RESOURCE,
            "resource_user_roles": [_ur("RESOURCE.ADMIN", "alice"), _ur("RESOURCE.ADMIN", "carol")],
            "resource_project_user_roles": [(self.RP, [_ur("RESOURCE_PROJECT.MEMBER", "bob")])],
            "settings": self.SETTINGS,
        }
        assert t.group_grants(t.build_grants(**kwargs)) == t.build_desired_state(**kwargs)
        assert t.build_desired_state(**kwargs) == [
            t.DesiredGroup("cluster1_abcd1234_member", frozenset({"bob"})),
            t.DesiredGroup("cluster1_admin", frozenset({"alice", "carol"})),
        ]
