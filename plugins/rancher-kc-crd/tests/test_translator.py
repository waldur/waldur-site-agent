"""Unit tests for the translator (pure logic, no I/O)."""

import pytest
from waldur_site_agent_rancher_kc_crd import translator as t

# ---------------------------------------------------------------------
# cr_name
# ---------------------------------------------------------------------


class TestCrName:
    def test_combines_slug_and_uuid_prefix(self):
        assert (
            t.cr_name("aio-rancher-staging", "7c9eba12-3f4d-4a5b-8c2e-1234abcd5678")
            == "aio-rancher-staging-7c9eba12"
        )

    def test_strips_dashes_from_uuid(self):
        # Both dashed and undashed UUID forms map to the same prefix.
        assert t.cr_name("foo", "7c9eba123f4d4a5b8c2e1234abcd5678") == "foo-7c9eba12"

    def test_lowercases_slug(self):
        assert t.cr_name("MyResource", "abcdef0123456789").startswith("myresource-")

    def test_truncates_when_over_253_chars(self):
        long_slug = "a" * 300
        name = t.cr_name(long_slug, "abcdef0123456789")
        assert len(name) <= t.MAX_K8S_NAME_LENGTH
        # Suffix is preserved, slug gets truncated.
        assert name.endswith("-abcdef01")


# ---------------------------------------------------------------------
# render_group_name
# ---------------------------------------------------------------------


class TestRenderGroupName:
    def test_substitutes_variables(self):
        assert (
            t.render_group_name("c_${cluster_id}_${role_name}", cluster_id="c-1", role_name="admin")
            == "c_c-1_admin"
        )

    def test_substitutes_rp_uuid_and_project_name(self):
        # The two new template variables that let group names be unique
        # per (cluster x project x role) instead of shared per-cluster.
        rendered = t.render_group_name(
            "c_${cluster_id}_${rp_uuid}_${role_name}_${project_name}",
            cluster_id="c-1",
            role_name="admin",
            rp_uuid="rp-abcd",
            project_name="alpha",
        )
        assert rendered == "c_c-1_rp-abcd_admin_alpha"

    def test_unknown_placeholders_are_left_in_place(self):
        # safe_substitute, not strict.
        assert (
            t.render_group_name("c_${cluster_id}_${notavar}", cluster_id="c-1", role_name="x")
            == "c_c-1_${notavar}"
        )

    def test_substitutes_slug_variables(self):
        # Customer / project / resource slugs surfaced from the Resource
        # SDK shape so opt-in templates can produce human-readable
        # group names instead of the default UUID-only form.
        rendered = t.render_group_name(
            "c_${cluster_id}_${customer_slug}_${project_slug}_${resource_slug}_${role_name}",
            cluster_id="c-1",
            role_name="admin",
            customer_slug="hpc-demo-org",
            project_slug="genomics-2026",
            resource_slug="adamas-cluster",
        )
        assert rendered == "c_c-1_hpc-demo-org_genomics-2026_adamas-cluster_admin"

    def test_substitutes_rp_uuid_short(self):
        # rp_uuid_short is the 8-char prefix; lets the recommended
        # human-readable template stay compact while keeping per-RP
        # uniqueness via an immutable discriminator.
        rendered = t.render_group_name(
            "c_${cluster_id}_${customer_slug}_${rp_uuid_short}_${role_name}",
            cluster_id="c-1",
            role_name="admin",
            customer_slug="org-x",
            rp_uuid_short="8706dd1a",
        )
        assert rendered == "c_c-1_org-x_8706dd1a_admin"

    def test_raises_when_rendered_name_exceeds_keycloak_limit(self):
        # Keycloak's GROUP.NAME varchar(255) rejects 256+ chars with HTTP 500.
        # Catch it at render time so the operator never tries the apply
        # and the user gets an actionable error instead of an opaque
        # downstream crash.
        long_slug = "x" * 250  # combined with the rest, will exceed 255
        with pytest.raises(ValueError, match="varchar.255.|255"):
            t.render_group_name(
                "c_${cluster_id}_${customer_slug}_${role_name}",
                cluster_id="c-m-glwxdksp",
                role_name="project_member",
                customer_slug=long_slug,
            )

    def test_accepts_name_at_exact_keycloak_limit(self):
        # 255 chars exactly -- empirically confirmed Keycloak accepts.
        # Template "c_${cluster_id}_${customer_slug}_${role_name}" with
        # cluster_id="a" and role_name="x" is 5 literal chars + 1 + len(pad) + 1
        # = 7 + len(pad). For 255 total: len(pad) = 248. (assertion below)
        pad = "p" * 249
        rendered = t.render_group_name(
            "c_${cluster_id}_${customer_slug}_${role_name}",
            cluster_id="a",
            role_name="x",
            customer_slug=pad,
        )
        assert len(rendered) == 255  # noqa: PLR2004

    def test_missing_slug_substitutes_empty_string(self):
        # When the source field isn't populated (Customer with no slug),
        # the renderer substitutes an empty string -- the resulting
        # group name has a double-underscore which is ugly but valid;
        # callers should ensure the source data is populated.
        rendered = t.render_group_name(
            "c_${cluster_id}_${customer_slug}_${role_name}",
            cluster_id="c-1",
            role_name="admin",
            customer_slug="",
        )
        assert rendered == "c_c-1__admin"


# ---------------------------------------------------------------------
# build_role_bindings
# ---------------------------------------------------------------------


def _ur(role, uuid="user-uuid", username=None) -> dict:
    """UserRole fixture.

    ``username`` defaults to ``f'user_{uuid}'`` so each fixture row
    carries a distinct identifier in BOTH spaces; tests that exercise
    the (default) username path can assert against ``f'user_{uuid}'``
    without colliding with other rows.
    """
    if username is None:
        username = f"user_{uuid}"
    return {"role_name": role, "user_uuid": uuid, "user_username": username}


class TestBuildRoleBindings:
    def test_groups_users_by_role(self):
        # Default identifier path is username; _ur defaults username to
        # f"user_{uuid}", so u1 → "user_u1" etc.
        out = t.build_role_bindings(
            [_ur("project_member", "u1"), _ur("project_member", "u2"), _ur("project_admin", "u3")],
            cluster_id="c-1",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member", "project_admin": "project-owner"},
        )
        assert len(out) == 2  # noqa: PLR2004
        member_binding = next(b for b in out if b["rancherRole"] == "project-member")
        assert {m["userIdentifier"] for m in member_binding["members"]} == {"user_u1", "user_u2"}

    def test_skips_roles_not_in_role_map(self):
        out = t.build_role_bindings(
            [_ur("custom_role")],
            cluster_id="c-1",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member"},
        )
        assert out == []

    def test_skips_users_with_no_identifier(self):
        out = t.build_role_bindings(
            [{"role_name": "project_member"}],  # no user_uuid / username
            cluster_id="c-1",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member"},
        )
        assert out == []

    def test_default_uses_username(self):
        # Default is username path -- works in both OIDC and self-hosted
        # Waldur, and avoids the inherited member-churn bug that
        # operators < 0.4.0 hit when current_kc_members (UUID-keyed)
        # was diffed against desired members (username-keyed).
        out = t.build_role_bindings(
            [_ur("project_member", "u1", "alice")],
            cluster_id="c-1",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member"},
            # keycloak_use_user_id intentionally omitted -- exercises default
        )
        assert out[0]["members"][0]["userIdentifier"] == "alice"
        assert out[0]["members"][0]["lookupByID"] is False

    def test_uuid_path_when_keycloak_use_user_id_true(self):
        # Explicit opt-in still works: deployments where Waldur was
        # OIDC-provisioned and user UUIDs equal the Keycloak sub claim
        # can keep using UUID-keyed group membership.
        out = t.build_role_bindings(
            [_ur("project_member", "u1", "alice")],
            cluster_id="c-1",
            group_name_template="c_${cluster_id}_${role_name}",
            role_map={"project_member": "project-member"},
            keycloak_use_user_id=True,
        )
        assert out[0]["members"][0]["userIdentifier"] == "u1"
        assert out[0]["members"][0]["lookupByID"] is True


# ---------------------------------------------------------------------
# build_cr_spec — full assembly
# ---------------------------------------------------------------------


class TestBuildCrSpec:
    def test_minimal_resource_project_with_one_user(self):
        body = t.build_cr_spec(
            resource={
                "uuid": "r-uuid-aaaaaaaa",
                "slug": "rancher-prod",
                "backend_id": "c-m-abc",
            },
            resource_project={
                "uuid": "rp-uuid-bbbbbbbb",
                "name": "Team Alpha",
                "limits": {},
                "description": None,
            },
            user_roles=[_ur("project_member", "u1")],
            backend_settings={
                "role_map": {"project_member": "project-member"},
            },
        )
        assert body["apiVersion"] == t.CRD_API_VERSION
        assert body["kind"] == t.CRD_KIND
        assert body["metadata"]["name"] == "rancher-prod-rpuuidbb"
        # Labels enable orphan-CR pruning in pull_resource (selecting by
        # waldur.io/resource-uuid) and human debugging via -L on kubectl.
        assert body["metadata"]["labels"] == {
            "waldur.io/resource-uuid": "r-uuid-aaaaaaaa",
            "waldur.io/resource-project-uuid": "rp-uuid-bbbbbbbb",
        }
        assert body["spec"]["clusterId"] == "c-m-abc"
        assert body["spec"]["projectName"] == "Team Alpha"
        # Audit fields kept aligned with operator CRD v0.3.0+.
        assert body["spec"]["resourceUuid"] == "r-uuid-aaaaaaaa"
        assert body["spec"]["resourceProjectUuid"] == "rp-uuid-bbbbbbbb"
        # spec.namespace was removed in operator 0.3.0 (operator no
        # longer creates namespaces); make sure we don't accidentally
        # re-introduce the field.
        assert "namespace" not in body["spec"]
        kc = body["spec"]["keycloak"]
        assert kc["enabled"] is True
        assert kc["parentGroupName"] == "c_c-m-abc"
        assert len(kc["roleBindings"]) == 1

    def test_quotas_emitted_when_limits_set(self):
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c"},
            resource_project={
                "uuid": "p1",
                "name": "p",
                "limits": {"cpu": 500, "memory": "256Mi", "gpu": 2},
            },
            user_roles=[],
            backend_settings={"role_map": {}},
        )
        # Pass-through Waldur-domain dict — operator does k8s translation.
        assert body["spec"]["resourceQuotas"] == {
            "cpu": 500,
            "memory": "256Mi",
            "gpu": 2,
        }

    def test_unknown_keys_dropped_from_quotas(self):
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c"},
            resource_project={
                "uuid": "p1",
                "name": "p",
                # 'limits.cpu' is k8s-shape — operator wouldn't accept it,
                # so the translator drops it before writing the CR.
                "limits": {"cpu": 500, "limits.cpu": "500m"},
            },
            user_roles=[],
            backend_settings={"role_map": {}},
        )
        assert body["spec"]["resourceQuotas"] == {"cpu": 500}

    def test_no_quotas_block_when_limits_empty(self):
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c"},
            resource_project={"uuid": "p1", "name": "p", "limits": {}},
            user_roles=[],
            backend_settings={"role_map": {}},
        )
        assert "resourceQuotas" not in body["spec"]

    def test_description_included_when_set(self):
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c"},
            resource_project={
                "uuid": "p1",
                "name": "p",
                "description": "Team Alpha workspace",
                "limits": {},
            },
            user_roles=[],
            backend_settings={"role_map": {}},
        )
        assert body["spec"]["description"] == "Team Alpha workspace"

    def test_recommended_human_readable_template_renders_with_slugs(self):
        # End-to-end: backend dict carries customer_slug / project_slug /
        # resource_slug; with the recommended opt-in template, the
        # rendered group name carries them all plus an 8-char rp_uuid
        # discriminator.
        body = t.build_cr_spec(
            resource={
                "uuid": "r-uuid",
                "slug": "adamas-cluster",
                "backend_id": "c-m-glwxdksp",
                "customer_slug": "hpc-demo-org",
                "project_slug": "genomics-2026",
            },
            resource_project={
                "uuid": "8706dd1a145848f781f115ce8d394b76",
                "name": "Cancer Biomarkers",
                "limits": {},
            },
            user_roles=[_ur("project_member", "u1")],
            backend_settings={
                "role_map": {"project_member": "project-member"},
                "group_name_template": (
                    "c_${cluster_id}_${customer_slug}_${project_slug}_${rp_uuid_short}_${role_name}"
                ),
            },
        )
        rb = body["spec"]["keycloak"]["roleBindings"][0]
        assert rb["groupName"] == (
            "c_c-m-glwxdksp_hpc-demo-org_genomics-2026_8706dd1a_project_member"
        )

    def test_default_template_unchanged_when_slugs_absent(self):
        # Backwards-compat: a Resource dict that doesn't carry the new
        # slug fields still renders the default UUID-only template
        # cleanly -- no missing-key crashes, no spurious empty
        # substitutions.
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c-m-x"},
            resource_project={"uuid": "p1uuid", "name": "p", "limits": {}},
            user_roles=[_ur("project_member", "u1")],
            backend_settings={"role_map": {"project_member": "project-member"}},
        )
        rb = body["spec"]["keycloak"]["roleBindings"][0]
        assert rb["groupName"] == "c_c-m-x_p1uuid_project_member"

    def test_cluster_template_renders_with_slugs(self):
        # spec.cluster path also picks up customer_slug + resource_slug
        # (project_slug is meaningful too, but resource_slug is the
        # natural cluster-scope identifier since 1 Resource = 1 cluster).
        body = t.build_cr_spec(
            resource={
                "uuid": "r-uuid",
                "slug": "adamas-cluster",
                "backend_id": "c-m-glwxdksp",
                "customer_slug": "hpc-demo-org",
            },
            resource_project={"uuid": "rp1", "name": "p", "limits": {}},
            user_roles=[],
            cluster_user_roles=[_ur("resource_admin", "u1")],
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
                "cluster_group_name_template": (
                    "c_${cluster_id}_${customer_slug}_${resource_slug}_cluster_${role_name}"
                ),
            },
        )
        rb = body["spec"]["cluster"]["keycloak"]["roleBindings"][0]
        assert rb["groupName"] == (
            "c_c-m-glwxdksp_hpc-demo-org_adamas-cluster_cluster_resource_admin"
        )


class TestClusterIdResolution:
    """spec.clusterId comes from `resource.backend_id` -- each Waldur
    Resource is 1:1 with a Rancher cluster, and that's the only path.
    """

    def test_uses_resource_backend_id(self):
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "r", "backend_id": "c-m-glwxdksp"},
            resource_project={"uuid": "p1", "name": "p", "limits": {}},
            user_roles=[],
            backend_settings={"role_map": {}},
        )
        assert body["spec"]["clusterId"] == "c-m-glwxdksp"

    def test_raises_when_backend_id_empty(self):
        """Empty backend_id is a configuration bug (the offering owner
        forgot to set the cluster). Fail loudly with a message that
        names the resource, not silently emit an invalid CR.
        """
        import pytest

        with pytest.raises(KeyError, match="backend_id"):
            t.build_cr_spec(
                resource={"uuid": "r1", "slug": "rancher-prod"},
                resource_project={"uuid": "p1", "name": "p", "limits": {}},
                user_roles=[],
                backend_settings={"role_map": {}},
            )

    def test_backend_settings_cluster_id_is_ignored(self):
        """No fallback path: even if an old config sets cluster_id at
        the offering level, it must not paper over an empty
        resource.backend_id.
        """
        import pytest

        with pytest.raises(KeyError, match="backend_id"):
            t.build_cr_spec(
                resource={"uuid": "r1", "slug": "r", "backend_id": ""},
                resource_project={"uuid": "p1", "name": "p", "limits": {}},
                user_roles=[],
                backend_settings={
                    "cluster_id": "c-from-settings",  # would-be fallback
                    "role_map": {},
                },
            )


class TestPerRpKeycloakGroupNaming:
    """Default group_name_template now includes ${rp_uuid} so each
    (cluster x project x role) gets its OWN Keycloak group. Sharing
    one group across multiple RPs caused (a) member-sync thrashing and
    (b) unintended cross-project access via Rancher PRTBs bound to the
    shared group.
    """

    def _spec(self, rp_uuid: str, project_name: str) -> dict:
        return t.build_cr_spec(
            resource={"uuid": "r1", "slug": "rancher-prod", "backend_id": "c-m-x"},
            resource_project={
                "uuid": rp_uuid,
                "name": project_name,
                "limits": {},
            },
            user_roles=[_ur("project_member", "u1")],
            backend_settings={"role_map": {"project_member": "project-member"}},
        )

    def test_two_rps_get_distinct_group_names(self):
        a = self._spec("rp-aaaa-uuid", "Project A")
        b = self._spec("rp-bbbb-uuid", "Project B")
        ga = a["spec"]["keycloak"]["roleBindings"][0]["groupName"]
        gb = b["spec"]["keycloak"]["roleBindings"][0]["groupName"]
        assert ga != gb, (
            f"Two RPs on the same cluster+role must get distinct KC group "
            f"names so the operator's per-CR member-sync doesn't thrash a "
            f"shared group; got '{ga}' for both"
        )
        assert "rp-aaaa-uuid" in ga
        assert "rp-bbbb-uuid" in gb

    def test_default_template_matches_documented_shape(self):
        body = self._spec("rp-1234", "Anything")
        assert (
            body["spec"]["keycloak"]["roleBindings"][0]["groupName"]
            == "c_c-m-x_rp-1234_project_member"
        )

    def test_custom_template_is_rendered_verbatim(self):
        """Operators can override `group_name_template` and the renderer
        passes the substitution through without enforcing any specific
        shape (the per-project safety check is operational, not
        structural).
        """
        body = t.build_cr_spec(
            resource={"uuid": "r1", "slug": "rancher-prod", "backend_id": "c-m-x"},
            resource_project={"uuid": "rp-1", "name": "p", "limits": {}},
            user_roles=[_ur("project_member", "u1")],
            backend_settings={
                "role_map": {"project_member": "project-member"},
                "group_name_template": "kc__${cluster_id}__${project_name}__${role_name}",
            },
        )
        assert (
            body["spec"]["keycloak"]["roleBindings"][0]["groupName"]
            == "kc__c-m-x__p__project_member"
        )

    def test_template_without_per_project_token_collides(self):
        """Documents the rule: any custom `group_name_template` that
        omits a per-project discriminator (`rp_uuid` or `project_name`)
        will produce identical group names for two RPs on the same
        cluster + role. The plugin does NOT enforce a per-project
        token in the template -- it's a contract with the operator --
        but the README points users at the safe default. This test
        keeps the rule visible in code so a regression is caught early.
        """
        bs = {
            "role_map": {"project_member": "project-member"},
            "group_name_template": "c_${cluster_id}_${role_name}",
        }
        cr_a = t.build_cr_spec(
            resource={"uuid": "rA", "slug": "r", "backend_id": "c-m-shared"},
            resource_project={"uuid": "rp-aaaa", "name": "Project A", "limits": {}},
            user_roles=[_ur("project_member", "u1")],
            backend_settings=bs,
        )
        cr_b = t.build_cr_spec(
            resource={"uuid": "rB", "slug": "r", "backend_id": "c-m-shared"},
            resource_project={"uuid": "rp-bbbb", "name": "Project B", "limits": {}},
            user_roles=[_ur("project_member", "u2")],
            backend_settings=bs,
        )
        ga = cr_a["spec"]["keycloak"]["roleBindings"][0]["groupName"]
        gb = cr_b["spec"]["keycloak"]["roleBindings"][0]["groupName"]
        assert ga == gb == "c_c-m-shared_project_member", (
            "Template without rp_uuid/project_name MUST collide. If this "
            "assertion ever fails, render_group_name was probably changed; "
            "re-evaluate whether the safe default is still safe."
        )

        # Default template MUST disambiguate (sanity check: rp_uuid in
        # the default keeps groups distinct).
        bs_safe = {"role_map": {"project_member": "project-member"}}
        cr_a2 = t.build_cr_spec(
            resource={"uuid": "rA", "slug": "r", "backend_id": "c-m-shared"},
            resource_project={"uuid": "rp-aaaa", "name": "Project A", "limits": {}},
            user_roles=[_ur("project_member", "u1")],
            backend_settings=bs_safe,
        )
        cr_b2 = t.build_cr_spec(
            resource={"uuid": "rB", "slug": "r", "backend_id": "c-m-shared"},
            resource_project={"uuid": "rp-bbbb", "name": "Project B", "limits": {}},
            user_roles=[_ur("project_member", "u2")],
            backend_settings=bs_safe,
        )
        assert (
            cr_a2["spec"]["keycloak"]["roleBindings"][0]["groupName"]
            != cr_b2["spec"]["keycloak"]["roleBindings"][0]["groupName"]
        )


class TestClusterScopeBindings:
    """Cover the spec.cluster section added in operator 0.4.0.

    Symmetric with the project-scope tests above but exercises the
    distinct configuration knobs (cluster_role_map,
    cluster_group_name_template) and the rule that the section is
    *only* emitted when cluster_role_map is configured -- old
    operators reject unknown spec fields.
    """

    @staticmethod
    def _resource() -> dict:
        return {"uuid": "r-uuid", "slug": "ranch", "backend_id": "c-m-zzz"}

    @staticmethod
    def _rp() -> dict:
        return {
            "uuid": "rp-uuid-cccccccc",
            "name": "Project C",
            "limits": {},
            "description": None,
        }

    def test_cluster_section_omitted_without_role_map(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[_ur("resource_admin", "u1")],
            backend_settings={"role_map": {}},
        )
        assert "cluster" not in body["spec"], (
            "spec.cluster must be omitted when cluster_role_map is unset, "
            "so old operators (pre-0.4.0) accept the CR."
        )

    def test_cluster_section_omitted_when_cluster_user_roles_none(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=None,
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
            },
        )
        # cluster_user_roles=None means "the caller didn't fetch them"
        # -- treat as opt-out so partial-info reconciles don't accidentally
        # withdraw existing CRTBs by emitting an empty desired list.
        assert "cluster" not in body["spec"]

    def test_cluster_section_emitted_with_role_map(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[_ur("resource_admin", "u1")],
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
            },
        )
        assert "cluster" in body["spec"]
        cluster_kc = body["spec"]["cluster"]["keycloak"]
        assert cluster_kc["enabled"] is True
        assert len(cluster_kc["roleBindings"]) == 1
        rb = cluster_kc["roleBindings"][0]
        assert rb["rancherRole"] == "cluster-owner"
        # Default template: c_${cluster_id}_cluster_${role_name}
        assert rb["groupName"] == "c_c-m-zzz_cluster_resource_admin"
        # Default is username path; _ur(uuid="u1") yields username "user_u1".
        assert rb["members"] == [{"userIdentifier": "user_u1", "lookupByID": False}]

    def test_cluster_section_emits_empty_list_when_no_users_match(self):
        # The spec.cluster block must still be emitted (with an empty
        # roleBindings list) so the operator's _gc_orphan_cluster_bindings
        # treats this as the explicit "no cluster bindings desired"
        # signal and tears down any prior CRTBs. Suppressing the block
        # would look identical to "cluster_role_map unset" -- a no-op.
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[_ur("project_member", "u1")],  # not in cluster_role_map
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
            },
        )
        assert body["spec"]["cluster"]["keycloak"]["roleBindings"] == []

    def test_cluster_template_default_includes_literal_cluster_token(self):
        # Defends against a future template regression that would cause
        # cluster groups to collide with project groups (which always
        # carry an rp_uuid hex prefix).
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[_ur("resource_member", "u1")],
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_member": "cluster-member"},
            },
        )
        gn = body["spec"]["cluster"]["keycloak"]["roleBindings"][0]["groupName"]
        assert "_cluster_" in gn

    def test_cluster_template_override(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[_ur("resource_admin", "u1")],
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
                "cluster_group_name_template": "kc_${cluster_id}_${role_name}",
            },
        )
        gn = body["spec"]["cluster"]["keycloak"]["roleBindings"][0]["groupName"]
        assert gn == "kc_c-m-zzz_resource_admin"

    def test_project_and_cluster_sections_independent(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[_ur("project_member", "u-proj")],
            cluster_user_roles=[_ur("resource_admin", "u-clus")],
            backend_settings={
                "role_map": {"project_member": "project-member"},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
            },
        )
        proj_rb = body["spec"]["keycloak"]["roleBindings"][0]
        clus_rb = body["spec"]["cluster"]["keycloak"]["roleBindings"][0]
        # Different members, different group names, different rancher
        # role templates -- and crucially, different group-name
        # families (project carries rp_uuid, cluster does not).
        assert proj_rb["members"][0]["userIdentifier"] == "user_u-proj"
        assert clus_rb["members"][0]["userIdentifier"] == "user_u-clus"
        assert proj_rb["groupName"] != clus_rb["groupName"]
        assert proj_rb["rancherRole"] == "project-member"
        assert clus_rb["rancherRole"] == "cluster-owner"
        # Project groupName carries the rp_uuid; cluster does not.
        rp_uuid = self._rp()["uuid"]
        assert rp_uuid in proj_rb["groupName"]
        assert rp_uuid not in clus_rb["groupName"]

    def test_role_not_in_cluster_role_map_is_skipped(self):
        body = t.build_cr_spec(
            resource=self._resource(),
            resource_project=self._rp(),
            user_roles=[],
            cluster_user_roles=[
                _ur("resource_admin", "u1"),
                _ur("unknown_role", "u2"),
            ],
            backend_settings={
                "role_map": {},
                "cluster_role_map": {"resource_admin": "cluster-owner"},
            },
        )
        bindings = body["spec"]["cluster"]["keycloak"]["roleBindings"]
        assert len(bindings) == 1
        assert bindings[0]["rancherRole"] == "cluster-owner"
        # Only u1 (whose role IS mapped) ends up bound. Default username
        # path: _ur(uuid="u1") emits userIdentifier="user_u1".
        assert [m["userIdentifier"] for m in bindings[0]["members"]] == ["user_u1"]
