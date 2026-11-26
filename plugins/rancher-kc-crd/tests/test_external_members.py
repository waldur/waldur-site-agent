"""Importing people a Keycloak admin added to the operator's groups.

The operator reports them in ``status.*RoleBindings[].externalMembers``;
the agent maps each group back to its Waldur role, resolves the person's
Waldur account through the Identity Bridge and grants the role.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.types import UNSET
from waldur_site_agent_rancher_kc_crd import external_members as em
from waldur_site_agent_rancher_kc_crd.external_members import (
    CLUSTER_SCOPE,
    PROJECT_SCOPE,
    ExternalGrant,
    ExternalMemberImporter,
    collect_external_grants,
    validate_external_import,
    validate_username_prefix,
    waldur_identifier,
)
from waldur_site_agent_rancher_kc_crd.translator import (
    IdentitySettings,
    build_role_bindings,
    keep_emptied_bindings,
)

CIVIL = IdentitySettings(
    source="civil_number", lookup="username", lowercase=True, identity_provider="tara"
)
BY_USERNAME = IdentitySettings()
RP = "2be7f82b782f4be981d329f1801a0202"
RESOURCE = "d33194d5df0649a88653d4e4756f5ec8"
USER = uuid.UUID("2a4137987bb24d7fb759703dbcb33710")


def _external(username="ee37004040004", links=True) -> dict:
    member = {"username": username, "firstName": "Peeter", "lastName": "Saar"}
    member["federatedIdentities"] = (
        [{"identityProvider": "tara", "userId": username.upper()}] if links else []
    )
    return member


def _status(project=(), cluster=()) -> dict:
    return {
        "keycloakRoleBindings": [
            {"groupName": "g_member", "syncedMembers": [], "externalMembers": list(project)}
        ],
        "clusterKeycloakRoleBindings": [
            {"groupName": "g_cluster", "syncedMembers": [], "externalMembers": list(cluster)}
        ],
    }


def _grant(scope_type=PROJECT_SCOPE, role="project_member") -> ExternalGrant:
    return ExternalGrant(
        scope_type=scope_type,
        scope_uuid=RP if scope_type == PROJECT_SCOPE else RESOURCE,
        role_name=role,
        username="ee37004040004",
        first_name="Peeter",
        last_name="Saar",
        identifier="EE37004040004",
    )


class TestCollect:
    def test_project_and_cluster_members_map_to_their_roles(self):
        grants = collect_external_grants(
            _status(project=[_external()], cluster=[_external()]),
            {"g_member": "project_member"},
            {"g_cluster": "cluster_member"},
            RP,
            RESOURCE,
            CIVIL,
        )
        assert [(g.scope_type, g.scope_uuid, g.role_name) for g in grants] == [
            (PROJECT_SCOPE, RP, "project_member"),
            (CLUSTER_SCOPE, RESOURCE, "cluster_member"),
        ]
        assert grants[0].identifier == "EE37004040004"

    def test_group_the_agent_did_not_build_is_skipped(self):
        grants = collect_external_grants(
            _status(project=[_external()]), {}, {}, RP, RESOURCE, CIVIL
        )
        assert grants == []


class TestIdentifier:
    def test_link_to_the_shared_provider_wins(self):
        member = _external(username="ee37004040004")
        member["federatedIdentities"] = [{"identityProvider": "tara", "userId": "EE37004040004"}]
        assert waldur_identifier(member, CIVIL) == "EE37004040004"

    def test_link_to_another_provider_does_not_count(self):
        member = _external(links=False)
        member["federatedIdentities"] = [{"identityProvider": "other", "userId": "X"}]
        assert waldur_identifier(member, CIVIL) is None

    def test_link_to_someone_else_is_not_imported(self):
        # A local account linked to another person's identity: importing it
        # would hand that person the role.
        member = _external(username="ee37004040004")
        member["federatedIdentities"] = [{"identityProvider": "tara", "userId": "EE38001010001"}]
        assert waldur_identifier(member, CIVIL) is None

    def test_member_without_a_link_is_not_imported(self):
        # A local account named like a civil code proves nothing about whose
        # code it is; importing it would hand that person's account the role.
        assert waldur_identifier(_external(links=False), CIVIL) is None

    def test_unlinked_member_is_skipped_with_a_masked_warning(self, caplog):
        grants = collect_external_grants(
            _status(project=[_external(links=False)]),
            {"g_member": "project_member"},
            {},
            RP,
            RESOURCE,
            CIVIL,
        )
        assert grants == []
        assert "no link to tara" in caplog.text
        assert "ee37004040004" not in caplog.text

    def test_username_mode_uses_the_username(self):
        assert waldur_identifier(_external(username="peeter"), BY_USERNAME) == "peeter"


class TestValidation:
    def test_civil_number_by_username_is_accepted(self):
        validate_external_import(CIVIL, "isd:rancher")

    @pytest.mark.parametrize(
        "identity",
        [
            IdentitySettings(source="uuid", lookup="id", lowercase=False),
            IdentitySettings(source="civil_number", lookup="attribute", attribute="code"),
            IdentitySettings(source="civil_number", lookup="username", template="x-${value}"),
        ],
    )
    def test_irreversible_identities_are_refused(self, identity):
        with pytest.raises(ValueError):
            validate_external_import(identity, "isd:rancher")

    def test_bridge_source_is_required(self):
        with pytest.raises(ValueError):
            validate_external_import(CIVIL, None)

    def test_civil_number_mode_needs_the_identity_provider(self):
        with pytest.raises(ValueError):
            validate_external_import(
                IdentitySettings(source="civil_number", lookup="username"), "isd:rancher"
            )


def _role(name, content_type):
    return SimpleNamespace(
        name=name, content_type=content_type, uuid=uuid.uuid4(), is_active=True
    )


def _response(status, content=b""):
    return SimpleNamespace(status_code=status, content=content, parsed=None)


class FakeWaldur:
    """Patches the SDK calls the importer makes and records them."""

    def __init__(self, allowed=("civil_number",), grant_status=201, grant_body=b""):
        self.roles = [
            _role("project_member", PROJECT_SCOPE),
            _role("cluster_member", CLUSTER_SCOPE),
        ]
        self.allowed = list(allowed)
        self.grant_status = grant_status
        self.grant_body = grant_body
        self.bridge_bodies: list = []
        self.grants: list[tuple[str, str]] = []
        # How the user read answers: "ordinary", "staff" or "forbidden".
        self.account = "ordinary"

    def __enter__(self):
        self._patches = [
            patch.object(
                em.identity_bridge_allowed_fields_retrieve,
                "sync",
                side_effect=lambda **_: SimpleNamespace(allowed_fields=self.allowed),
            ),
            patch.object(em.identity_bridge, "sync_detailed", side_effect=self._bridge),
            patch.object(em.users_retrieve, "sync_detailed", side_effect=self._user),
            patch.object(
                em.marketplace_offering_roles_list, "sync", side_effect=self._roles_page
            ),
            patch.object(
                em.marketplace_provider_resource_projects_add_user,
                "sync_detailed",
                side_effect=lambda **kw: self._grant(PROJECT_SCOPE, kw),
            ),
            patch.object(
                em.marketplace_provider_resources_add_user,
                "sync_detailed",
                side_effect=lambda **kw: self._grant(CLUSTER_SCOPE, kw),
            ),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()

    def _user(self, uuid, client, field):
        if self.account == "forbidden":
            # What the generated client does on a status its schema lacks.
            raise UnexpectedStatus(403, b'{"detail":"EE37004040004"}', "http://waldur/api/users/")
        staff = self.account == "staff"
        user = SimpleNamespace(is_staff=staff, is_support=False, is_identity_manager=False)
        return SimpleNamespace(status_code=200, parsed=user)

    def _roles_page(self, page=1, page_size=100, **_):
        return self.roles[(page - 1) * page_size : page * page_size]

    def _bridge(self, client, body):
        self.bridge_bodies.append(body)
        result = SimpleNamespace(uuid=USER, created=True, updated_fields=[])
        return SimpleNamespace(status_code=200, content=b"", parsed=result)

    def _grant(self, scope_type, kwargs):
        self.grants.append((scope_type, kwargs["uuid"].hex))
        return _response(self.grant_status, self.grant_body)


def _importer(identity=CIVIL) -> ExternalMemberImporter:
    return ExternalMemberImporter(
        client=object(), offering_uuid="o" * 32, identity=identity, bridge_source="isd:rancher"
    )


class TestImporter:
    def test_person_is_resolved_and_granted(self):
        with FakeWaldur() as waldur:
            assert _importer().import_grants([_grant()]) == 1
        body = waldur.bridge_bodies[0]
        assert (body.source, body.civil_number) == ("isd:rancher", "EE37004040004")
        # The civil code must not become the Waldur username.
        assert body.username.startswith("rancher-")
        assert "37004040004" not in body.username
        assert (body.first_name, body.last_name) == ("Peeter", "Saar")
        assert waldur.grants == [(PROJECT_SCOPE, RP)]

    def test_username_prefix_is_configurable(self):
        importer = ExternalMemberImporter(
            client=object(),
            offering_uuid="o" * 32,
            identity=CIVIL,
            bridge_source="isd:rancher",
            username_prefix="tara.",
        )
        with FakeWaldur() as waldur:
            importer.import_grants([_grant()])
        username = waldur.bridge_bodies[0].username
        assert username.startswith("tara.")
        assert len(username) == len("tara.") + 20

    @pytest.mark.parametrize("prefix", ["", "rancher-", "tara.", "x_1-"])
    def test_valid_username_prefixes(self, prefix):
        validate_username_prefix(prefix)

    @pytest.mark.parametrize("prefix", ["-rancher", "ran cher", "ee/", "a" * 65])
    def test_invalid_username_prefixes(self, prefix):
        with pytest.raises(ValueError):
            validate_username_prefix(prefix)

    @pytest.mark.parametrize("account", ["staff", "forbidden"])
    def test_privileged_or_unreadable_account_is_not_granted(self, account, caplog):
        with FakeWaldur() as waldur:
            waldur.account = account
            assert _importer().import_grants([_grant()]) == 0
        assert waldur.grants == []
        assert "not imported" in caplog.text

    def test_one_bridge_call_per_person(self):
        with FakeWaldur() as waldur:
            _importer().import_grants([_grant(), _grant(CLUSTER_SCOPE, "cluster_member")])
        assert len(waldur.bridge_bodies) == 1
        assert len(waldur.grants) == 2

    def test_empty_names_are_left_out(self):
        grant = ExternalGrant(
            scope_type=PROJECT_SCOPE,
            scope_uuid=RP,
            role_name="project_member",
            username="ee37004040004",
            first_name="",
            last_name="",
            identifier="EE37004040004",
        )
        with FakeWaldur() as waldur:
            _importer().import_grants([grant])
        assert waldur.bridge_bodies[0].first_name is UNSET
        assert waldur.bridge_bodies[0].last_name is UNSET

    def test_raised_client_errors_stay_out_of_civil_number_logs(self, caplog):
        def bridge_raises(client, body):
            raise UnexpectedStatus(400, b'{"detail":"EE37004040004"}', "http://waldur/api/")

        with FakeWaldur() as waldur, patch.object(
            em.identity_bridge, "sync_detailed", side_effect=bridge_raises
        ):
            assert _importer().import_grants([_grant()]) == 0
        assert waldur.grants == []
        assert "cannot import" in caplog.text
        assert "37004040004" not in caplog.text

    def test_waldur_answers_stay_out_of_civil_number_logs(self, caplog):
        body = b'{"detail":"User EE37004040004 is deactivated."}'
        with FakeWaldur(grant_status=400, grant_body=body):
            _importer().import_grants([_grant()])
        assert "cannot import" in caplog.text
        assert "37004040004" not in caplog.text

    def test_offering_roles_are_read_past_the_first_page(self):
        with FakeWaldur() as waldur:
            waldur.roles = [_role(f"r{i}", PROJECT_SCOPE) for i in range(100)] + [
                _role("project_member", PROJECT_SCOPE)
            ]
            assert _importer().import_grants([_grant()]) == 1

    def test_cluster_grant_reported_by_every_cr_is_applied_once(self):
        with FakeWaldur() as waldur:
            granted = _importer().import_grants([_grant(CLUSTER_SCOPE, "cluster_member")] * 3)
        assert granted == 1
        assert waldur.grants == [(CLUSTER_SCOPE, RESOURCE)]

    def test_nothing_happens_when_the_bridge_drops_civil_numbers(self, caplog):
        with FakeWaldur(allowed=("first_name", "last_name")) as waldur:
            assert _importer().import_grants([_grant()]) == 0
        assert waldur.bridge_bodies == []
        assert waldur.grants == []
        assert "FEDERATED_IDENTITY_SYNC_ALLOWED_ATTRIBUTES" in caplog.text

    def test_username_mode_sends_no_civil_number(self):
        with FakeWaldur(allowed=()) as waldur:
            _importer(BY_USERNAME).import_grants([_grant()])
        assert waldur.bridge_bodies[0].civil_number is UNSET
        assert waldur.grants == [(PROJECT_SCOPE, RP)]

    def test_existing_grant_counts_as_done(self, caplog):
        # The CR status read right after applying a cycle's grants can still
        # list the member as external, so the grant is repeated once.
        body = b'{"non_field_errors":["User has already the same role in this scope."]}'
        with FakeWaldur(grant_status=400, grant_body=body):
            assert _importer().import_grants([_grant()]) == 0
        assert "cannot import" not in caplog.text

    def test_unknown_role_is_skipped(self, caplog):
        with FakeWaldur() as waldur:
            assert _importer().import_grants([_grant(role="project_owner")]) == 0
        assert waldur.bridge_bodies == []
        assert "no resource_project role" in caplog.text

    def test_one_failure_does_not_stop_the_rest(self):
        with FakeWaldur(grant_status=403, grant_body=b"forbidden") as waldur:
            granted = _importer().import_grants(
                [_grant(), _grant(CLUSTER_SCOPE, "cluster_member")]
            )
        assert granted == 0
        assert len(waldur.grants) == 2

    def test_civil_numbers_are_masked_in_logs(self, caplog):
        caplog.set_level("INFO")
        with FakeWaldur():
            _importer().import_grants([_grant()])
        assert "ee37004040004" not in caplog.text


class TestBindingsForImport:
    def test_every_mapped_role_gets_a_binding(self):
        roles: dict[str, str] = {}
        bindings = build_role_bindings(
            [],
            cluster_id="local",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member", "project_owner": "project-owner"},
            include_empty=True,
            group_roles=roles,
        )
        assert [(b["groupName"], b["members"]) for b in bindings] == [
            ("c_local_project_member", []),
            ("c_local_project_owner", []),
        ]
        assert roles == {
            "c_local_project_member": "project_member",
            "c_local_project_owner": "project_owner",
        }

    def test_roles_without_members_are_left_out_by_default(self):
        bindings = build_role_bindings(
            [],
            cluster_id="local",
            group_name_template="c_${role_name}",
            role_map={"project_member": "project-member"},
        )
        assert bindings == []


def _body(project=(), cluster=None) -> dict:
    spec: dict = {"keycloak": {"roleBindings": list(project)}}
    if cluster is not None:
        spec["cluster"] = {"keycloak": {"roleBindings": list(cluster)}}
    return {"spec": spec}


def _cr(project=(), cluster=None, status=None) -> dict:
    cr = _body(project, cluster)
    cr["status"] = status or {}
    return cr


def _rb(name, role, members=(1,)) -> dict:
    return {"groupName": name, "rancherRole": role, "members": list(members)}


def _group_status(name, count, external=()) -> dict:
    return {
        "groupName": name,
        "keycloakGroupId": f"id-{name}",
        "memberCount": count,
        "externalMembers": [{"username": u} for u in external],
    }


class TestKeepEmptiedBindings:
    def test_binding_whose_last_member_left_is_kept_empty(self):
        member = _rb("g_member", "project-member")
        owner = _rb("g_owner", "project-owner")
        body = _body([member])
        keep_emptied_bindings(body, _cr([member, owner]))
        assert body["spec"]["keycloak"]["roleBindings"] == [
            member,
            {"groupName": "g_owner", "rancherRole": "project-owner", "members": []},
        ]

    def test_kept_binding_is_let_go_once_its_group_is_empty(self):
        emptied = _rb("g_owner", "project-owner", members=())
        status = {"keycloakRoleBindings": [_group_status("g_owner", 0)]}
        body = _body([])
        keep_emptied_bindings(body, _cr([emptied], status=status))
        assert body["spec"]["keycloak"]["roleBindings"] == []

    def test_kept_binding_stays_while_the_group_still_has_members(self):
        emptied = _rb("g_owner", "project-owner", members=())
        status = {"keycloakRoleBindings": [_group_status("g_owner", 1)]}
        body = _body([])
        keep_emptied_bindings(body, _cr([emptied], status=status))
        assert [rb["groupName"] for rb in body["spec"]["keycloak"]["roleBindings"]] == ["g_owner"]

    def test_kept_binding_stays_while_someone_is_kept_as_external(self):
        emptied = _rb("g_owner", "project-owner", members=())
        status = {"keycloakRoleBindings": [_group_status("g_owner", 0, ["ee37004040004"])]}
        body = _body([])
        keep_emptied_bindings(body, _cr([emptied], status=status))
        assert len(body["spec"]["keycloak"]["roleBindings"]) == 1

    def test_kept_binding_whose_group_was_never_created_is_let_go(self):
        # The operator could not create the group (a role Rancher rejects):
        # it holds nobody, and keeping it would keep the error.
        emptied = _rb("g_bad", "no-such-role", members=())
        status = {"keycloakRoleBindings": [{"groupName": "g_bad", "keycloakGroupId": ""}]}
        body = _body([])
        keep_emptied_bindings(body, _cr([emptied], status=status))
        assert body["spec"]["keycloak"]["roleBindings"] == []

    def test_binding_of_an_unmapped_role_is_emptied_too(self):
        # Dropping it outright left its members with the access.
        old = _rb("g_admin", "project-admin")
        body = _body([])
        keep_emptied_bindings(body, _cr([old]))
        assert body["spec"]["keycloak"]["roleBindings"] == [
            {"groupName": "g_admin", "rancherRole": "project-admin", "members": []}
        ]

    def test_cluster_binding_is_kept_empty(self):
        old = _rb("g_cluster", "cluster-member")
        body = _body([], cluster=[])
        keep_emptied_bindings(body, _cr([], cluster=[old]))
        assert body["spec"]["cluster"]["keycloak"]["roleBindings"] == [
            {"groupName": "g_cluster", "rancherRole": "cluster-member", "members": []}
        ]

    def test_no_cluster_block_is_added(self):
        old = _rb("g_cluster", "cluster-member")
        body = _body([])
        keep_emptied_bindings(body, _cr([], cluster=[old]))
        assert "cluster" not in body["spec"]

    def test_new_cr_is_unchanged(self):
        body = _body([])
        keep_emptied_bindings(body, None)
        assert body == _body([])


class TestOfferingOfTheResource:
    """Membership sync hands over resources without offering_uuid; it is read back then."""

    def _backend(self):
        from waldur_site_agent_rancher_kc_crd.backend import RancherKcCrdBackend

        backend = RancherKcCrdBackend.__new__(RancherKcCrdBackend)
        backend.waldur_client = object()
        return backend

    def test_offering_uuid_on_the_resource_is_used(self):
        offering = uuid.uuid4()
        resource = SimpleNamespace(uuid=uuid.uuid4(), offering_uuid=offering)
        assert self._backend()._offering_uuid(resource) == offering.hex

    def test_missing_offering_uuid_is_read_from_waldur(self):
        from waldur_site_agent_rancher_kc_crd import backend as backend_module

        offering = uuid.uuid4()
        resource = SimpleNamespace(uuid=uuid.uuid4(), offering_uuid=UNSET)
        with patch.object(
            backend_module.marketplace_provider_resources_retrieve,
            "sync",
            return_value=SimpleNamespace(offering_uuid=offering),
        ) as retrieve:
            assert self._backend()._offering_uuid(resource) == offering.hex
        assert retrieve.call_args.kwargs["uuid"] == resource.uuid
