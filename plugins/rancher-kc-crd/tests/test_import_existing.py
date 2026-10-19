"""Unit tests for the brownfield import (inventory + apply) with fake APIs."""

import json

import httpx
import pytest
from waldur_site_agent_rancher_kc_crd import import_existing as ie
from waldur_site_agent_rancher_kc_crd.translator import (
    LEGACY_USERNAME_IDENTITY,
    LEGACY_UUID_IDENTITY,
    IdentitySettings,
)

CIVIL_ATTRIBUTE_IDENTITY = IdentitySettings(
    source="civil_number", lookup="attribute", attribute="personalCode", lowercase=False
)
CIVIL_USERNAME_IDENTITY = IdentitySettings(
    source="civil_number", lookup="username", template="EE${value}", lowercase=True
)

RP_ROLE = "11111111111111111111111111111111"
RESOURCE_ROLE = "22222222222222222222222222222222"
OWNER_ROLE = "33333333333333333333333333333333"


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


class TestReverseRoleMap:
    def test_inverts(self):
        assert ie.reverse_role_map({"project_member": "project-member"}) == {
            "project-member": "project_member"
        }

    def test_first_sorted_wins_on_shared_template(self):
        reverse = ie.reverse_role_map({"b_role": "project-member", "a_role": "project-member"})
        assert reverse == {"project-member": "a_role"}


class TestParsePrincipal:
    @pytest.mark.parametrize(
        ("principal", "expected"),
        [
            ("keycloakoidc_user://abc", ("user", "abc")),
            ("keycloakoidc_group://devs", ("group", "devs")),
            ("local://u-123", ("other", "local://u-123")),
        ],
    )
    def test_kinds(self, principal, expected):
        assert ie.parse_principal(principal) == expected


class TestReverseIdentityTemplate:
    def test_default_template(self):
        assert ie.reverse_identity_template("${value}", "alice") == "alice"

    def test_prefix_case_insensitive(self):
        assert ie.reverse_identity_template("EE${value}", "ee3800", ignore_case=True) == "3800"

    def test_prefix_case_sensitive_mismatch(self):
        assert ie.reverse_identity_template("EE${value}", "ee3800") is None

    def test_suffix(self):
        assert ie.reverse_identity_template("${value}@idp", "bob@idp") == "bob"

    def test_regex_characters_are_literal(self):
        assert ie.reverse_identity_template("a.${value}", "abx") is None
        assert ie.reverse_identity_template("a.${value}", "a.x") == "x"

    def test_repeated_value_must_agree(self):
        assert ie.reverse_identity_template("${value}-${value}", "x-x") == "x"
        assert ie.reverse_identity_template("${value}-${value}", "x-y") is None


class TestWaldurIdentityValue:
    def test_username(self):
        user = {"id": "kc-1", "username": "alice"}
        assert ie.waldur_identity_value(user, LEGACY_USERNAME_IDENTITY) == ("alice", "")

    def test_id(self):
        user = {"id": "kc-1", "username": "alice"}
        assert ie.waldur_identity_value(user, LEGACY_UUID_IDENTITY) == ("kc-1", "")

    def test_attribute(self):
        user = {"id": "kc-1", "attributes": {"personalCode": ["38001010000"]}}
        assert ie.waldur_identity_value(user, CIVIL_ATTRIBUTE_IDENTITY) == ("38001010000", "")

    def test_attribute_missing(self):
        value, reason = ie.waldur_identity_value({"id": "kc-1"}, CIVIL_ATTRIBUTE_IDENTITY)
        assert value is None
        assert "0 values" in reason

    def test_attribute_ambiguous(self):
        user = {"attributes": {"personalCode": ["1", "2"]}}
        value, reason = ie.waldur_identity_value(user, CIVIL_ATTRIBUTE_IDENTITY)
        assert value is None
        assert "2 values" in reason

    def test_template_mismatch(self):
        value, reason = ie.waldur_identity_value({"username": "alice"}, CIVIL_USERNAME_IDENTITY)
        assert value is None
        assert "template" in reason


class TestWaldurApiRoot:
    @pytest.mark.parametrize(
        "url",
        ["https://w.example.com", "https://w.example.com/", "https://w.example.com/api/"],
    )
    def test_normalizes(self, url):
        assert ie.waldur_api_root(url) == "https://w.example.com/api"


class TestSelectResources:
    def test_filters(self):
        resources = [
            {"name": "a", "backend_id": "c-1", "state": "OK"},
            {"name": "b", "backend_id": "", "state": "OK"},
            {"name": "c", "backend_id": "c-2", "state": "Terminated"},
            {"name": "d", "backend_id": "c-1", "state": "OK"},
            {"name": "e", "backend_id": "c-3", "state": "OK"},
        ]
        assert [r["name"] for r in ie.select_resources(resources, None)] == ["a", "e"]
        assert [r["name"] for r in ie.select_resources(resources, ["c-3"])] == ["e"]


class TestFilters:
    def test_select_resources_by_resource_uuid_or_name(self):
        resources = [
            {"uuid": "11111111-0000-0000-0000-000000000000", "name": "a", "backend_id": "c-1"},
            {"uuid": "2" * 32, "name": "b", "backend_id": "c-2"},
        ]
        by_uuid = ie.select_resources(resources, resource_filters=["1111111100000000" + "0" * 16])
        assert [r["name"] for r in by_uuid] == ["a"]
        assert [r["name"] for r in ie.select_resources(resources, resource_filters=["b"])] == ["b"]
        assert ie.select_resources(resources, ["c-1"], ["b"]) == []

    @pytest.mark.parametrize("wanted", ["analytics", "c-1:p-a", "p-a"])
    def test_project_matches_name_or_id(self, wanted):
        assert ie.project_matches({"id": "c-1:p-a", "name": "analytics"}, [wanted])
        assert ie.project_matches({"rancher_project_id": "c-1:p-a", "name": "analytics"}, [wanted])

    def test_project_mismatch_and_no_filter(self):
        assert not ie.project_matches({"id": "c-1:p-a", "name": "analytics"}, ["other"])
        assert ie.project_matches({"id": "c-1:p-a", "name": "analytics"}, None)

    def test_inventory_project_filter(self):
        rancher = FakeRancher()
        collector = ie.InventoryCollector(rancher, FakeKeycloak(), LEGACY_USERNAME_IDENTITY)
        cluster = collector.cluster(
            {"uuid": "a" * 32, "name": "prod", "backend_id": "c-1"}, ["p-x1"]
        )
        (project,) = cluster["projects"]
        assert project["rancher_project_id"] == "c-1:p-x1"
        # Duplicates are still detected across the unfiltered project list.
        assert project["duplicate_name"] is True
        # Cluster-scope bindings belong to no single project.
        assert cluster["cluster_bindings"] == []

    def test_apply_project_filter(self, inventory_cluster):
        waldur = FakeWaldur()
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster, ["analytics"])
        assert {a["where"].split(" / ")[-1] for a in importer.actions} == {"analytics"}
        assert all(scope == "marketplace-resource-projects" for scope, *_ in waldur.added)


# ---------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------


class FakeRancher:
    def __init__(self):
        self.projects_by_cluster = {
            "c-1": [
                {
                    "id": "c-1:p-a",
                    "name": "analytics",
                    "description": "Data team",
                    "resourceQuota": {
                        "limit": {"limitsCpu": "8", "limitsMemory": "16Gi", "pods": "50"}
                    },
                },
                {"id": "c-1:p-sys", "name": "System"},
                {"id": "c-1:p-x1", "name": "dup"},
                {"id": "c-1:p-x2", "name": "dup"},
            ]
        }
        self.prtbs = {
            "c-1:p-a": [
                {
                    "id": "b1",
                    "roleTemplateId": "project-member",
                    "userPrincipalId": "keycloakoidc_user://kc-alice",
                },
                {
                    "id": "b2",
                    "roleTemplateId": "project-owner",
                    "groupPrincipalId": "keycloakoidc_group://analytics-admins",
                },
                {
                    "id": "b3",
                    "roleTemplateId": "project-member",
                    "userId": "u-local",
                    "userPrincipalId": "local://u-local",
                },
                # What Rancher 2.15 stores for a Keycloak user bound in the UI:
                # the user's *local* principal plus its user ID.
                {
                    "id": "b4",
                    "roleTemplateId": "project-member",
                    "userId": "u-oidc",
                    "userPrincipalId": "local://u-oidc",
                },
                # Older shape: user ID only, no principal.
                {"id": "b5", "roleTemplateId": "project-member", "userId": "u-oidc"},
            ]
        }
        self.crtbs = {
            "c-1": [
                {
                    "id": "cb1",
                    "roleTemplateId": "cluster-member",
                    "userPrincipalId": "keycloakoidc_user://kc-bob",
                }
            ]
        }
        self.users = {
            "u-local": ["local://u-local"],
            "u-oidc": ["local://u-oidc", "keycloakoidc_user://kc-carol"],
        }

    def projects(self, cluster_id):
        return self.projects_by_cluster[cluster_id]

    def project_bindings(self, project_id):
        return self.prtbs.get(project_id, [])

    def cluster_bindings(self, cluster_id):
        return self.crtbs.get(cluster_id, [])

    def user_principal_ids(self, user_id):
        return self.users.get(user_id, [])


class FakeKeycloak:
    def __init__(self):
        self.users = {
            "kc-alice": {"id": "kc-alice", "username": "alice"},
            "kc-bob": {"id": "kc-bob", "username": "bob"},
            "kc-carol": {"id": "kc-carol", "username": "carol"},
            "kc-dave": {"id": "kc-dave", "username": "dave"},
        }
        self.groups = {"analytics-admins": [{"id": "g-1", "path": "/c/analytics-admins"}]}
        self.members = {"g-1": [self.users["kc-alice"], self.users["kc-dave"]]}

    def user(self, user_id):
        return self.users.get(user_id)

    def groups_named(self, name):
        return self.groups.get(name, [])

    def group_members(self, group_id):
        return self.members[group_id]


@pytest.fixture
def inventory_cluster():
    collector = ie.InventoryCollector(FakeRancher(), FakeKeycloak(), LEGACY_USERNAME_IDENTITY)
    return collector.cluster(
        {"uuid": "aaaaaaaa-0000-0000-0000-000000000001", "name": "prod", "backend_id": "c-1"}
    )


class TestInventory:
    def test_projects(self, inventory_cluster):
        projects = {p["rancher_project_id"]: p for p in inventory_cluster["projects"]}
        assert inventory_cluster["resource_uuid"] == "aaaaaaaa000000000000000000000001"
        assert projects["c-1:p-sys"]["system"] is True
        assert projects["c-1:p-x1"]["duplicate_name"] is True
        assert projects["c-1:p-a"]["duplicate_name"] is False
        assert projects["c-1:p-a"]["description"] == "Data team"

    def test_bindings(self, inventory_cluster):
        project = next(p for p in inventory_cluster["projects"] if p["name"] == "analytics")
        by_id = {b["rancher_binding_id"]: b for b in project["bindings"]}
        assert [u["identity_value"] for u in by_id["b1"]["users"]] == ["alice"]
        assert [u["identity_value"] for u in by_id["b2"]["users"]] == ["alice", "dave"]
        assert "not a Keycloak" in by_id["b3"]["error"]
        # A local Rancher user that is linked to a Keycloak principal resolves.
        assert by_id["b4"]["principal"] == "keycloakoidc_user://kc-carol"
        assert [u["identity_value"] for u in by_id["b4"]["users"]] == ["carol"]
        assert by_id["b5"]["principal"] == "keycloakoidc_user://kc-carol"

    def test_cluster_bindings(self, inventory_cluster):
        (binding,) = inventory_cluster["cluster_bindings"]
        assert binding["rancher_role"] == "cluster-member"
        assert binding["users"][0]["identity_value"] == "bob"

    def test_missing_and_ambiguous_groups(self):
        keycloak = FakeKeycloak()
        keycloak.groups = {
            "twins": [{"id": "1", "path": "/a/twins"}, {"id": "2", "path": "/b/twins"}]
        }
        collector = ie.InventoryCollector(FakeRancher(), keycloak, LEGACY_USERNAME_IDENTITY)
        missing = collector.binding({"groupPrincipalId": "keycloakoidc_group://nope"})
        twins = collector.binding({"groupPrincipalId": "keycloakoidc_group://twins"})
        assert missing["error"] == "group not found in Keycloak"
        assert "ambiguous" in twins["error"]

    def test_deleted_keycloak_user(self):
        collector = ie.InventoryCollector(FakeRancher(), FakeKeycloak(), LEGACY_USERNAME_IDENTITY)
        entry = collector.binding({"userPrincipalId": "keycloakoidc_user://gone"})
        assert entry["users"][0]["error"] == "user not found in Keycloak"


# ---------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------


class FakeWaldur:
    def __init__(self):
        self.users_by_name = {
            "alice": {"uuid": "a" * 32, "username": "alice", "is_active": True},
            "bob": {"uuid": "b" * 32, "username": "bob", "is_active": True},
            "Carol": {"uuid": "c" * 32, "username": "Carol", "is_active": True},
            "dave": {"uuid": "d" * 32, "username": "dave", "is_active": False},
        }
        self.rps = {"aaaaaaaa000000000000000000000001": []}
        self.scope_roles = {}
        self.created = []
        self.added = []
        self.backend_ids = {}
        self.created_limits = {}
        self.fail_create = False
        self.fail_backend_id = False

    def offering_roles(self, offering_uuid):
        return [
            {"uuid": RP_ROLE, "name": "project_member", "content_type": "resource_project"},
            {"uuid": OWNER_ROLE, "name": "project_admin", "content_type": "resource_project"},
            {"uuid": RESOURCE_ROLE, "name": "resource_member", "content_type": "resource"},
        ]

    def resource_projects(self, resource_uuid):
        return self.rps[resource_uuid]

    def scope_users(self, scope_path, scope_uuid):
        return self.scope_roles.get(scope_uuid, [])

    def users(self, params):
        if "username" in params:
            user = self.users_by_name.get(params["username"])
            return [user] if user else []
        return [
            u
            for u in self.users_by_name.values()
            if params["query"].lower() in u["username"].lower()
        ]

    def user_by_uuid(self, user_uuid):
        return None

    def create_resource_project(self, resource_uuid, name, description, limits=None):
        if self.fail_create:
            raise httpx.HTTPError("create rejected")
        rp = {"uuid": f"{len(self.created) + 1:032x}", "name": name}
        self.created.append((resource_uuid, name, description))
        self.created_limits[name] = limits
        self.rps[resource_uuid].append(rp)
        return rp

    def set_resource_project_backend_id(self, rp_uuid, backend_id):
        if self.fail_backend_id:
            raise httpx.HTTPError("403 Forbidden")
        self.backend_ids[rp_uuid] = backend_id

    def add_user(self, scope_path, scope_uuid, user_uuid, role_uuid):
        self.added.append((scope_path, scope_uuid, user_uuid, role_uuid))


ROLE_MAP = {"project_member": "project-member", "project_admin": "project-owner"}
CLUSTER_ROLE_MAP = {"resource_member": "cluster-member"}


def make_importer(waldur, execute=False, cluster_role_map=CLUSTER_ROLE_MAP):
    return ie.Importer(
        waldur,
        "offering",
        ROLE_MAP,
        cluster_role_map,
        LEGACY_USERNAME_IDENTITY,
        execute=execute,
    )


class TestApply:
    def test_dry_run_changes_nothing(self, inventory_cluster):
        waldur = FakeWaldur()
        importer = make_importer(waldur)
        importer.import_cluster(inventory_cluster)
        assert waldur.created == []
        assert waldur.added == []
        ops = [(a["op"], a["where"].split(" / ")[-1]) for a in importer.actions]
        assert ("create_project", "analytics") in ops
        assert ("skip", "System") in ops
        assert ("skip", "dup") in ops

    def test_execute_creates_projects_and_grants(self, inventory_cluster):
        waldur = FakeWaldur()
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster)

        resource_uuid = "aaaaaaaa000000000000000000000001"
        assert waldur.created == [(resource_uuid, "analytics", "Data team")]
        rp_uuid = f"{1:032x}"
        assert waldur.backend_ids == {rp_uuid: "c-1:p-a"}
        assert sorted(waldur.added) == sorted(
            [
                # direct binding
                ("marketplace-resource-projects", rp_uuid, "a" * 32, RP_ROLE),
                # via group; dave is deactivated in Waldur and skipped
                ("marketplace-resource-projects", rp_uuid, "a" * 32, OWNER_ROLE),
                # local Rancher user linked to Keycloak; Waldur username differs in case
                ("marketplace-resource-projects", rp_uuid, "c" * 32, RP_ROLE),
                # cluster scope
                ("marketplace-resources", resource_uuid, "b" * 32, RESOURCE_ROLE),
            ]
        )
        reasons = [a.get("reason") for a in importer.actions if a["op"] == "skip"]
        assert "Waldur user is deactivated" in reasons

    def test_existing_project_and_grants_are_kept(self, inventory_cluster):
        waldur = FakeWaldur()
        rp_uuid = "e" * 32
        waldur.rps["aaaaaaaa000000000000000000000001"] = [{"uuid": rp_uuid, "name": "analytics"}]
        waldur.scope_roles[rp_uuid] = [
            {"user_uuid": "a" * 32, "role_name": "project_member", "role_uuid": RP_ROLE}
        ]
        importer = make_importer(waldur, execute=True, cluster_role_map={})
        importer.import_cluster(inventory_cluster)

        assert waldur.created == []
        assert sorted(waldur.added) == sorted(
            [
                ("marketplace-resource-projects", rp_uuid, "a" * 32, OWNER_ROLE),
                ("marketplace-resource-projects", rp_uuid, "c" * 32, RP_ROLE),
            ]
        )
        assert any(a["op"] == "exists" and a.get("user") == "alice" for a in importer.actions)

    def test_unmapped_role_and_missing_offering_role(self, inventory_cluster):
        waldur = FakeWaldur()
        importer = ie.Importer(
            waldur,
            "offering",
            {"project_member": "project-member", "viewer": "project-owner"},
            {},
            LEGACY_USERNAME_IDENTITY,
        )
        importer.import_cluster(inventory_cluster)
        reasons = {a.get("reason") for a in importer.actions}
        assert "offering has no resource_project role 'viewer'" in reasons

        importer = ie.Importer(
            waldur, "offering", {"project_member": "project-member"}, {}, LEGACY_USERNAME_IDENTITY
        )
        importer.import_cluster(inventory_cluster)
        reasons = {a.get("reason") for a in importer.actions}
        assert "role template not in the role map" in reasons


class TestSameRoleNameAtBothScopes:
    def test_project_grant_does_not_hide_missing_cluster_grant(self, inventory_cluster):
        """A Resource's list_users includes ResourceProject roles; match on role UUID."""
        waldur = FakeWaldur()
        resource_uuid = "aaaaaaaa000000000000000000000001"
        waldur.offering_roles = lambda offering_uuid: [
            {"uuid": RP_ROLE, "name": "member", "content_type": "resource_project"},
            {"uuid": RESOURCE_ROLE, "name": "member", "content_type": "resource"},
        ]
        # bob holds the project-scope "member" role, which the Resource lists too.
        waldur.scope_roles[resource_uuid] = [
            {"user_uuid": "b" * 32, "role_name": "member", "role_uuid": RP_ROLE}
        ]
        importer = ie.Importer(
            waldur,
            "offering",
            {"member": "project-member"},
            {"member": "cluster-member"},
            LEGACY_USERNAME_IDENTITY,
            execute=True,
        )
        importer.import_cluster(inventory_cluster)
        assert ("marketplace-resources", resource_uuid, "b" * 32, RESOURCE_ROLE) in waldur.added


class TestQuotaToLimits:
    @pytest.mark.parametrize(
        ("value", "expected"), [("8000m", 8000), ("2", 2000), ("0.5", 500), ("x", None)]
    )
    def test_parse_cpu(self, value, expected):
        assert ie.parse_cpu_millicores(value) == expected

    def test_maps_and_reports_dropped(self):
        limits, dropped = ie.quota_to_limits(
            {
                "limitsCpu": "4000m",
                "limitsMemory": "8Gi",
                "requestsStorage": "100Gi",
                "pods": "50",
                "requestsCpu": "1",
            }
        )
        assert limits == {"cpu": 4000, "memory": "8Gi", "storage": "100Gi"}
        assert dropped == ["pods", "requestsCpu"]

    def test_round_trips_through_the_operator_translation(self):
        # The operator writes cpu as f"{int(cpu)}m"; reading it back is lossless.
        limits, _ = ie.quota_to_limits({"limitsCpu": f"{int(1500)}m"})
        assert limits == {"cpu": 1500}

    def test_empty(self):
        assert ie.quota_to_limits({}) == ({}, [])

    def test_ignores_the_rancher_schema_type(self):
        limits, dropped = ie.quota_to_limits(
            {"limitsCpu": "2000m", "type": "/v3/schemas/resourceQuotaLimit"}
        )
        assert limits == {"cpu": 2000}
        assert dropped == []


class TestReviewFixes:
    def test_inventory_records_quota(self, inventory_cluster):
        project = next(p for p in inventory_cluster["projects"] if p["name"] == "analytics")
        assert project["resource_quota"] == {
            "limitsCpu": "8",
            "limitsMemory": "16Gi",
            "pods": "50",
        }

    def test_created_project_keeps_quota_and_warns_about_the_rest(self, inventory_cluster):
        waldur = FakeWaldur()
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster)
        assert waldur.created_limits["analytics"] == {"cpu": 8000, "memory": "16Gi"}
        warnings = [a["reason"] for a in importer.actions if a["op"] == "warn"]
        assert any("pods" in w for w in warnings)

    def test_existing_project_without_limits_warns(self, inventory_cluster):
        waldur = FakeWaldur()
        waldur.rps["aaaaaaaa000000000000000000000001"] = [
            {"uuid": "e" * 32, "name": "analytics", "backend_id": "c-1:p-a", "limits": {}}
        ]
        importer = make_importer(waldur)
        importer.import_cluster(inventory_cluster, ["analytics"])
        assert any(
            a["op"] == "warn" and "clears this project's Rancher quota" in a["reason"]
            for a in importer.actions
        )

    def test_project_linked_under_another_name_is_not_duplicated(self, inventory_cluster):
        waldur = FakeWaldur()
        waldur.rps["aaaaaaaa000000000000000000000001"] = [
            {"uuid": "e" * 32, "name": "analytics-old", "backend_id": "c-1:p-a"}
        ]
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster, ["analytics"])
        assert waldur.created == []
        assert waldur.added == []
        assert any("already linked" in (a.get("reason") or "") for a in importer.actions)

    def test_existing_project_gets_missing_backend_id(self, inventory_cluster):
        waldur = FakeWaldur()
        waldur.rps["aaaaaaaa000000000000000000000001"] = [
            {"uuid": "e" * 32, "name": "analytics", "backend_id": "", "limits": {"cpu": 1}}
        ]
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster, ["analytics"])
        assert waldur.backend_ids == {"e" * 32: "c-1:p-a"}

    def test_backend_id_failure_is_an_error(self, inventory_cluster):
        waldur = FakeWaldur()
        waldur.fail_backend_id = True
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster, ["analytics"])
        failed = [a for a in importer.actions if a.get("error")]
        assert [a["op"] for a in failed] == ["set_backend_id"]

    def test_grants_skipped_when_project_creation_fails(self, inventory_cluster):
        waldur = FakeWaldur()
        waldur.fail_create = True
        importer = make_importer(waldur, execute=True)
        importer.import_cluster(inventory_cluster, ["analytics"])
        assert waldur.added == []
        assert not any(a["op"] == "add_user" for a in importer.actions)
        assert any(a.get("reason") == "resource project was not created" for a in importer.actions)

    def test_deactivated_duplicate_does_not_block_active_user(self):
        waldur = FakeWaldur()
        waldur.users = lambda params: [
            {"uuid": "1" * 32, "username": "x", "civil_number": "38001010000", "is_active": False},
            {"uuid": "2" * 32, "username": "y", "civil_number": "38001010000", "is_active": True},
        ]
        importer = ie.Importer(waldur, "offering", ROLE_MAP, {}, CIVIL_ATTRIBUTE_IDENTITY)
        user, reason = importer.resolve_user("38001010000")
        assert user["uuid"] == "2" * 32, reason

    def test_user_principals_are_looked_up_once(self):
        rancher = FakeRancher()
        calls = []
        original = rancher.user_principal_ids
        rancher.user_principal_ids = lambda user_id: calls.append(user_id) or original(user_id)
        collector = ie.InventoryCollector(rancher, FakeKeycloak(), LEGACY_USERNAME_IDENTITY)
        for _ in range(3):
            collector.binding({"userId": "u-oidc", "userPrincipalId": "local://u-oidc"})
        assert calls == ["u-oidc"]


class TestLoadOffering:
    def test_picks_rancher_kc_crd_offering(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text(
            json.dumps(
                {
                    "offerings": [
                        {"name": "slurm", "backend_type": "slurm", "waldur_offering_uuid": "1"},
                        {
                            "name": "rancher",
                            "backend_type": "rancher-kc-crd",
                            "waldur_offering_uuid": "2",
                        },
                    ]
                }
            )
        )
        assert ie.load_offering(str(config), None)["name"] == "rancher"
        assert ie.load_offering(str(config), "2")["name"] == "rancher"
        with pytest.raises(SystemExit):
            ie.load_offering(str(config), "slurm")


# ---------------------------------------------------------------------
# CLI end to end, over mocked HTTP
# ---------------------------------------------------------------------

WALDUR = "https://waldur.example.com/api"
RANCHER = "https://rancher.example.com/v3"
KEYCLOAK = "https://kc.example.com"
OFFERING = "f" * 32
RESOURCE = "a" * 32


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    config = tmp_path / "agent.yaml"
    config.write_text(
        json.dumps(
            {
                "offerings": [
                    {
                        "name": "rancher",
                        "waldur_api_url": "https://waldur.example.com/api/",
                        "waldur_api_token": "agent-token",
                        "waldur_offering_uuid": OFFERING,
                        "backend_type": "rancher-kc-crd",
                        "backend_settings": {"role_map": {"project_member": "project-member"}},
                    }
                ]
            }
        )
    )
    for name, value in {
        "RANCHER_URL": "https://rancher.example.com",
        "RANCHER_BEARER_TOKEN": "r-token",
        "KEYCLOAK_URL": KEYCLOAK,
        "KEYCLOAK_REALM": "rancher",
        "KEYCLOAK_USERNAME": "admin",
        "KEYCLOAK_PASSWORD": "secret",
        "WALDUR_API_TOKEN": "staff-token",
    }.items():
        monkeypatch.setenv(name, value)
    return config


def test_cli_inventory_then_apply(cli_env, tmp_path, respx_mock):
    respx_mock.get(f"{WALDUR}/marketplace-provider-resources/").respond(
        json=[{"uuid": RESOURCE, "name": "prod", "backend_id": "c-1", "state": "OK"}]
    )
    respx_mock.get(f"{RANCHER}/projects", params={"clusterId": "c-1"}).respond(
        json={"data": [{"id": "c-1:p-a", "name": "analytics"}]}
    )
    respx_mock.get(f"{RANCHER}/projectroletemplatebindings").respond(
        json={
            "data": [
                {
                    "id": "b1",
                    "roleTemplateId": "project-member",
                    "groupPrincipalId": "keycloakoidc_group://devs",
                }
            ]
        }
    )
    respx_mock.get(f"{RANCHER}/clusterroletemplatebindings").respond(json={"data": []})
    respx_mock.post(f"{KEYCLOAK}/realms/master/protocol/openid-connect/token").respond(
        json={"access_token": "kc-token"}
    )
    respx_mock.get(f"{KEYCLOAK}/admin/realms/rancher/groups").respond(
        json=[{"id": "g-1", "name": "devs", "path": "/devs", "subGroups": []}]
    )
    members = respx_mock.get(f"{KEYCLOAK}/admin/realms/rancher/groups/g-1/members").respond(
        json=[{"id": "kc-alice", "username": "alice"}]
    )

    inventory = tmp_path / "inventory.json"
    assert ie.main(["inventory", "-c", str(cli_env), "-o", str(inventory)]) == 0
    assert members.calls.last.request.headers["Authorization"] == "Bearer kc-token"
    assert inventory.stat().st_mode & 0o777 == 0o600
    data = json.loads(inventory.read_text())
    assert data["clusters"][0]["projects"][0]["bindings"][0]["users"][0]["identity_value"] == (
        "alice"
    )

    respx_mock.get(f"{WALDUR}/marketplace-offering-roles/").respond(
        json=[{"uuid": RP_ROLE, "name": "project_member", "content_type": "resource_project"}]
    )
    respx_mock.get(f"{WALDUR}/marketplace-resource-projects/").respond(json=[])
    respx_mock.get(f"{WALDUR}/users/", params={"username": "alice"}).respond(
        json=[{"uuid": "a1" * 16, "username": "alice", "is_active": True}]
    )
    created = respx_mock.post(f"{WALDUR}/marketplace-resource-projects/").respond(
        201, json={"uuid": "e" * 32, "name": "analytics"}
    )
    backend_id = respx_mock.post(
        f"{WALDUR}/marketplace-provider-resource-projects/{'e' * 32}/set_backend_id/"
    ).respond(json={"status": "ok"})
    respx_mock.get(f"{WALDUR}/marketplace-resource-projects/{'e' * 32}/list_users/").respond(
        json=[]
    )
    added = respx_mock.post(f"{WALDUR}/marketplace-resource-projects/{'e' * 32}/add_user/").respond(
        201, json={"expiration_time": None}
    )

    # Dry run first: reads only.
    assert ie.main(["apply", "-c", str(cli_env), "-i", str(inventory)]) == 0
    assert not created.called
    assert not added.called

    assert ie.main(["apply", "-c", str(cli_env), "-i", str(inventory), "--execute"]) == 0
    assert json.loads(created.calls.last.request.content) == {
        "resource": RESOURCE,
        "name": "analytics",
        "description": "",
    }
    assert created.calls.last.request.headers["Authorization"] == "Token staff-token"
    assert json.loads(backend_id.calls.last.request.content) == {"backend_id": "c-1:p-a"}
    assert json.loads(added.calls.last.request.content) == {"user": "a1" * 16, "role": RP_ROLE}
