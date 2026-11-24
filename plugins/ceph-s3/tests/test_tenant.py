"""Per-resource RGW tenants: template rendering, addressing and the croit client."""

from unittest.mock import Mock, patch
from uuid import UUID

import httpx
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent_ceph_s3.backend import CephS3Backend
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3APIError,
    CephS3Error,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)
from waldur_site_agent_ceph_s3.tenant import (
    DEFAULT_TENANT_TEMPLATE,
    qualify,
    render_tenant,
    split,
    validate_tenant_template,
)

from conftest import RADOSGW_SETTINGS

RESOURCE_UUID = UUID("12345678-1234-5678-9abc-123456789abc")
CUSTOMER_UUID = UUID("aaaabbbb-cccc-dddd-eeee-ffff00001111")
PROJECT_UUID = UUID("99998888-7777-6666-5555-444433332222")

COMPONENTS = {
    "s3_storage": {
        "accounting_type": "usage",
        "backend_name": "storage",
        "unit_factor": 1000000000,
    }
}


def make_resource(customer_abbreviation=None, **overrides):
    fields = {
        "uuid": RESOURCE_UUID,
        "slug": "my-bucket",
        "name": "My bucket",
        "customer_slug": "Acme-Corp",
        "customer_name": "Acme Corporation",
        "customer_uuid": CUSTOMER_UUID,
        "project_slug": "research-1",
        "project_name": "Research team",
        "project_uuid": PROJECT_UUID,
        "offering_slug": "s3-storage",
    }
    fields.update(overrides)
    resource = WaldurResource(**fields)
    if customer_abbreviation is not None:
        # Newer than the SDK this plugin pins: from_dict files it here.
        resource.additional_properties["customer_abbreviation"] = customer_abbreviation
    return resource


class TestTemplate:
    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            ("{resource_uuid}", RESOURCE_UUID.hex),
            ("{resource_slug}", "my_bucket"),
            ("{customer_slug}_{resource_uuid:.8}", "acme_corp_12345678"),
            ("{customer_uuid:.8}_{resource_slug}", "aaaabbbb_my_bucket"),
            ("{project_slug}_{resource_uuid:.8}", "research_1_12345678"),
            ("{offering_slug}_{resource_slug}", "s3_storage_my_bucket"),
            ("{resource_name}_{resource_uuid:.8}", "my_bucket_12345678"),
            ("{project_name:.12}_{resource_slug}", "research_tea_my_bucket"),
            ("{customer_name}_{resource_slug}", "acme_corporation_my_bucket"),
            ("waldur_{resource_slug}", "waldur_my_bucket"),
        ],
    )
    def test_renders_a_valid_tenant(self, template, expected):
        validate_tenant_template(template)
        assert render_tenant(template, make_resource()) == expected

    def test_the_default_template_reads_org_project_and_resource(self):
        resource = make_resource(customer_abbreviation="ACME")
        assert render_tenant(DEFAULT_TENANT_TEMPLATE, resource) == (
            "acme_research_tea_my_bucket_12345678"
        )

    def test_the_abbreviation_is_read_from_an_api_payload(self):
        """The SDK this agent pins does not know the field; from_dict keeps it."""
        payload = {
            "uuid": RESOURCE_UUID.hex,
            "slug": "my-bucket",
            "customer_slug": "acme-corp",
            "customer_abbreviation": "ACME",
        }
        resource = WaldurResource.from_dict(payload)
        assert render_tenant("{customer_abbreviation}_{resource_slug}", resource) == (
            "acme_my_bucket"
        )

    def test_an_empty_abbreviation_falls_back_to_the_customer_slug(self):
        """Most organisations have no abbreviation, and older Waldur sends none."""
        for resource in (make_resource(customer_abbreviation=""), make_resource()):
            assert render_tenant("{customer_abbreviation}_{resource_slug}", resource) == (
                "acme_corp_my_bucket"
            )

    def test_a_name_with_no_ascii_letters_falls_back_to_the_slug(self):
        resource = make_resource(name="Архив")
        assert render_tenant("{resource_name}_{resource_uuid:.8}", resource) == (
            "my_bucket_12345678"
        )

    def test_names_lose_their_accents_not_their_letters(self):
        resource = make_resource(customer_name="Café Société Ångström")
        assert render_tenant("{customer_name}_{resource_slug}", resource) == (
            "cafe_societe_angstrom_my_bucket"
        )

    def test_a_cut_never_leaves_an_underscore_behind(self):
        resource = make_resource(project_name="Infra baasteenused")
        # "infra_baasteenused"[:6] is "infra_"; the dangling "_" goes.
        assert render_tenant("{project_name:.6}_{resource_slug}", resource) == (
            "infra_my_bucket"
        )

    def test_uuid_given_as_a_string_renders_the_same(self):
        resource = make_resource(uuid=str(RESOURCE_UUID))
        assert render_tenant("{resource_uuid}", resource) == RESOURCE_UUID.hex

    @pytest.mark.parametrize(
        ("template", "match"),
        [
            ("{customer}_{resource_slug}", "unknown field"),
            ("{customer_slug.__class__}_{resource_slug}", "unknown field"),
            ("{customer_slug[0]}_{resource_slug}", "unknown field"),
            ("{customer_slug!r}_{resource_slug}", "conversion"),
            ("{resource_name:>20}_{resource_slug}", "unsupported format spec"),
            ("acme-{resource_slug}", "characters RGW refuses"),
            ("Acme_{resource_slug}", "characters RGW refuses"),
            ("{customer_slug", "malformed"),
            ("", "non-empty"),
        ],
    )
    def test_refuses_a_template_that_cannot_render_a_tenant(self, template, match):
        with pytest.raises(ValueError, match=match):
            validate_tenant_template(template)

    @pytest.mark.parametrize(
        "template",
        [
            "{customer_slug}",
            "{customer_slug}_{customer_uuid:.8}",
            "{project_uuid}",
            "{offering_slug}",
            "{resource_name}",
            "fixed",
        ],
    )
    def test_refuses_a_template_resources_would_share(self, template):
        """A tenant rendered from anything but the resource is a shared namespace."""
        with pytest.raises(ValueError, match="does not name the resource"):
            validate_tenant_template(template)

    @pytest.mark.parametrize(
        ("template", "match"),
        [
            ("{resource_uuid:.7}", "at least 8"),
            ("{resource_slug:.5}", "must not truncate"),
        ],
    )
    def test_refuses_a_cut_that_loses_uniqueness(self, template, match):
        with pytest.raises(ValueError, match=match):
            validate_tenant_template(template)

    def test_a_field_the_resource_lacks_fails_the_render(self):
        resource = make_resource()
        resource.project_slug = None
        with pytest.raises(ValueError, match="project_slug"):
            render_tenant("{project_slug}_{resource_slug}", resource)

    def test_an_overlong_result_fails_the_render(self):
        with pytest.raises(ValueError, match="not a valid RGW tenant"):
            render_tenant("{customer_uuid}{project_uuid}{resource_uuid}", make_resource())

    def test_qualify_and_split_round_trip(self):
        assert qualify("acme", "root") == "acme$root"
        assert qualify("", "waldur-x") == "waldur-x"
        assert split("acme$root") == ("acme", "root")
        assert split("waldur-x") == ("", "waldur-x")


class TestBackend:
    def _backend(self, **settings):
        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
            backend = CephS3Backend(dict(RADOSGW_SETTINGS, **settings), COMPONENTS)
        backend.client = Mock()
        backend.client.create_user.return_value = {}
        backend.client.list_user_keys.return_value = []
        return backend

    def test_the_resource_is_its_tenant(self):
        """The backend id is the tenant; the one user in it is root."""
        backend = self._backend(tenant_template="{customer_slug}_{resource_uuid:.8}")

        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")

        assert info.backend_id == "acme_corp_12345678"
        assert info.backend_metadata["s3_user"] == "acme_corp_12345678$root"
        assert info.backend_metadata["s3_tenant"] == "acme_corp_12345678"
        kwargs = backend.client.create_user.call_args.kwargs
        assert kwargs["uid"] == "acme_corp_12345678$root"
        # "$" stays out of the ownership stamp's email address.
        assert "$" not in kwargs["email"]
        assert kwargs["email"] == f"waldur-{RESOURCE_UUID.hex}@invalid"
        # Any follow-up call addresses the tenant's user, never a bare namesake.
        addressed = {call.args[0] for call in backend.client.mock_calls if call.args}
        assert addressed <= {"acme_corp_12345678$root"}

    def test_offerings_without_a_template_get_the_default(self):
        backend = self._backend()
        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")
        assert info.backend_id == "acme_corp_research_tea_my_bucket_12345678"

    def test_the_allocation_prefix_does_not_name_the_user(self):
        """root is root in every tenant: nothing about it moves with a setting."""
        backend = self._backend(allocation_prefix="s3-")
        info = backend.create_resource_with_id(make_resource(), "s3-my-bucket")
        assert backend.client.create_user.call_args.kwargs["uid"] == f"{info.backend_id}$root"

    def test_default_tenant_is_refused(self):
        """One fixed tenant is the shared namespace this scheme exists to end."""
        with pytest.raises(ValueError, match="default_tenant"):
            self._backend(default_tenant="acme")

    def test_a_shared_scope_template_is_refused_at_construction(self):
        with pytest.raises(ValueError, match="does not name the resource"):
            self._backend(tenant_template="{customer_slug}_{customer_uuid:.8}")

    def test_recreating_under_a_stored_tenant_keeps_it(self):
        """A restored resource is re-created under its stored backend id, even if
        its names or the template changed since."""
        backend = self._backend(tenant_template="{resource_uuid}")
        resource = make_resource(backend_id="acme_corp_12345678", slug="renamed")

        info = backend.create_resource_with_id(resource, "acme_corp_12345678")

        assert info.backend_id == "acme_corp_12345678"
        assert backend.client.create_user.call_args.kwargs["uid"] == "acme_corp_12345678$root"

    @pytest.mark.parametrize(
        "stored", ["acme_corp_aaaabbbb$waldur-my-bucket", "waldur-my-bucket"]
    )
    def test_recreating_under_a_stored_user_id_keeps_it(self, stored):
        """Ids from before one tenant per resource name the user, and stay as they are."""
        backend = self._backend()
        resource = make_resource(backend_id=stored)

        info = backend.create_resource_with_id(resource, stored)

        assert info.backend_id == stored
        assert backend.client.create_user.call_args.kwargs["uid"] == stored

    @pytest.mark.parametrize(
        ("backend_id", "uid"),
        [
            ("acme_corp_12345678", "acme_corp_12345678$root"),
            ("acme_corp_aaaabbbb$waldur-my-bucket", "acme_corp_aaaabbbb$waldur-my-bucket"),
            ("waldur-my-bucket", "waldur-my-bucket"),
        ],
    )
    def test_every_backend_id_shape_reaches_its_user(self, backend_id, uid):
        backend = self._backend()
        backend.client.get_user_info.return_value = {
            "uid": uid,
            "email": f"waldur-{RESOURCE_UUID}-{uid.replace('$', '.')}@invalid",
        }
        backend.pause_resource(backend_id)
        backend.client.get_user_info.assert_called_with(uid)
        backend.client.set_user_suspended.assert_called_with(uid, True)

    @staticmethod
    def _rgw_with_users(backend, *uids):
        def get_user_info(uid):
            if uid in uids:
                return {"uid": uid}
            raise CephS3UserNotFoundError(uid)

        backend.client.get_user_info.side_effect = get_user_info

    @pytest.mark.parametrize("stored", ["backup", "waldur_acme_proj_1a2b3c4d"])
    def test_an_old_bare_uid_shaped_like_a_tenant_reaches_its_user(self, stored):
        """Uids from before the waldur- prefix (an empty prefix gave the bare slug;
        croit-s3 made waldur_<org>_<project>_<uuid>) look exactly like tenants."""
        backend = self._backend()
        self._rgw_with_users(backend, stored)

        backend.pause_resource(stored)

        backend.client.set_user_suspended.assert_called_with(stored, True)

    def test_a_tenant_reaches_its_root_user_first(self):
        backend = self._backend()
        self._rgw_with_users(backend, "acme_12345678$root", "acme_12345678")
        assert backend._rgw_uid("acme_12345678") == "acme_12345678$root"

    def test_an_id_naming_no_user_answers_the_root_and_is_asked_again(self):
        backend = self._backend()
        self._rgw_with_users(backend)
        assert backend._rgw_uid("acme_12345678") == "acme_12345678$root"
        assert backend._rgw_uid("acme_12345678") == "acme_12345678$root"
        assert backend.client.get_user_info.call_count == 4

    def test_delete_then_recreate_does_not_reuse_the_old_answer(self):
        """An old bare uid looking like a tenant is re-created as its root user;
        the uid remembered for it before the deletion must not survive."""
        backend = self._backend()
        resource = make_resource(backend_id="backup")
        self._rgw_with_users(backend, "backup")
        backend.delete_resource(resource)
        backend.client.delete_user.assert_called_with("backup")

        self._rgw_with_users(backend)
        backend.create_resource_with_id(resource, "backup")
        assert backend.client.create_user.call_args.kwargs["uid"] == "backup$root"

        self._rgw_with_users(backend, "backup$root")
        backend.pause_resource("backup")
        backend.client.set_user_suspended.assert_called_with("backup$root", True)

    def test_a_tenant_collision_fails_without_a_retry(self):
        """The processor retries a DuplicateResourceError under a new generated id,
        which renders the same tenant; the error has to end the order instead."""
        backend = self._backend()
        backend.client.create_user.side_effect = CephS3UserExistsError("exists")
        backend.client.get_user_info.side_effect = None
        backend.client.get_user_info.return_value = {"uid": "x", "email": "someone@else"}
        with pytest.raises(CephS3Error, match="another resource holds that tenant"):
            backend.create_resource_with_id(make_resource(), "waldur-my-bucket")

    def test_a_second_user_for_one_resource_is_refused_by_name(self):
        """RGW refuses the stamp a second time (EmailExists, measured on 20.2.5):
        the resource already has a user elsewhere. Say so, not just EmailExists."""
        backend = self._backend()
        backend.client.create_user.side_effect = CephS3APIError("EmailExists")
        with pytest.raises(CephS3Error, match="already has an S3 user"):
            backend.create_resource_with_id(make_resource(), "waldur-my-bucket")

    def test_a_tenant_safe_prefix_asks_rgw_which_user_an_id_names(self):
        """With allocation_prefix "s3_", an old bare uid "s3_backup" is also a valid
        tenant name. The user that exists decides, once."""
        backend = self._backend(allocation_prefix="s3_")

        def get_user_info(uid):
            if uid == "s3_backup":
                return {"uid": uid}
            raise CephS3UserNotFoundError(uid)

        backend.client.get_user_info.side_effect = get_user_info

        assert backend._rgw_uid("s3_backup") == "s3_backup"
        assert backend._rgw_uid("s3_backup") == "s3_backup"
        assert backend.client.get_user_info.call_count == 2  # root, then bare; cached

    def test_the_core_resource_calls_resolve_the_tenant(self):
        """The core asks the client for a resource by backend id (pull, delete,
        import); the real client must reach the tenant's user and list it by
        the backend id Waldur holds."""
        backend = CephS3Backend(dict(RADOSGW_SETTINGS), COMPONENTS)
        users = {"acme_12345678$root", "old$waldur-old", "waldur-bare"}

        def get_user_info(uid):
            if uid in users:
                return {"uid": uid}
            raise CephS3UserNotFoundError(uid)

        with (
            patch.object(type(backend.client), "get_user_info", side_effect=get_user_info),
            patch.object(type(backend.client), "list_users", return_value=sorted(users)),
            patch.object(type(backend.client), "delete_user") as delete_user,
        ):
            assert backend.client.get_resource("acme_12345678").name == "acme_12345678"
            assert backend.client.get_resource("acme_99999999") is None
            backend.client.delete_resource("acme_12345678")
            delete_user.assert_called_once_with("acme_12345678$root")
            listed = {resource.name for resource in backend.client.list_resources()}
        assert listed == {"acme_12345678", "old$waldur-old", "waldur-bare"}

    def test_a_resource_missing_a_template_field_fails_the_order(self):
        backend = self._backend(tenant_template="{project_slug}_{resource_slug}")
        resource = make_resource()
        resource.project_slug = None
        with pytest.raises(ValueError, match="project_slug"):
            backend.create_resource_with_id(resource, "waldur-my-bucket")
        backend.client.create_user.assert_not_called()

    def test_radosgw_takes_a_tenant_template(self):
        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
            backend = CephS3Backend(
                dict(RADOSGW_SETTINGS, tenant_template="{resource_uuid}"), COMPONENTS
            )
        assert backend.tenant_template == "{resource_uuid}"


class TestCroitStatistics:
    """croit is read for statistics only; a tenanted user has no per-user series."""

    def test_stats_do_not_look_up_an_untenanted_user(self, client, gateway):
        gateway.route("/stats", httpx.Response(200, json={"axis1": {"graphs": []}}))

        client.get_user_storage_series("waldur-x", -3600)

        assert [r.url.path for r in gateway.requests] == ["/api/stats"]
        assert gateway.requests[0].url.params["template-s3-user-name"] == "waldur-x"


    def test_stats_for_a_tenanted_user_are_unmeasured_without_a_request(
        self, client, gateway
    ):
        """croit rejects "$" in the template (400) and its spec offers no lookup
        for the name: the user is reported as unmeasured, and nothing croit would
        refuse is sent."""
        assert client.get_user_storage_series("acme$waldur-x", -3600) == []

        assert gateway.requests == []


class TestSessionLogin:
    """croit refuses HTTP Basic on its API; a password is exchanged for a token."""

    def _client(self, gateway):
        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        croit = CroitClient(
            api_url="https://croit.example.org", username="admin", password="pw"
        )
        croit.session = httpx.Client(transport=httpx.MockTransport(gateway.handler))
        return croit

    def test_logs_in_once_and_sends_the_session_token(self, gateway):
        gateway.route(
            "/auth/login-form",
            httpx.Response(200, json={"access_token": "sess", "expires_in": 3600}),
        )
        gateway.route("/stats", httpx.Response(200, json={"axis1": {"graphs": []}}))
        croit = self._client(gateway)

        croit.get_user_storage_series("waldur-x", -3600)
        croit.get_user_storage_series("waldur-x", -3600)

        login, *calls = gateway.requests
        assert login.url.path == "/api/auth/login-form"
        assert login.headers["content-type"] == "application/x-www-form-urlencoded"
        assert b"grant_type=password" in login.content
        assert "authorization" not in login.headers
        assert [r.headers["authorization"] for r in calls] == ["Bearer sess"] * 2

    def test_renews_an_expiring_token(self, gateway):
        gateway.route(
            "/auth/login-form",
            httpx.Response(200, json={"access_token": "sess", "expires_in": 30}),
        )
        gateway.route("/stats", httpx.Response(200, json={"axis1": {"graphs": []}}))
        croit = self._client(gateway)

        croit.get_user_storage_series("waldur-x", -3600)
        croit.get_user_storage_series("waldur-x", -3600)

        logins = [r for r in gateway.requests if r.url.path.endswith("login-form")]
        assert len(logins) == 2

    def test_a_rejected_login_names_the_user(self, gateway):
        from waldur_site_agent_ceph_s3.exceptions import CephS3AuthenticationError

        gateway.route("/auth/login-form", httpx.Response(401, json={"code": 401}))
        croit = self._client(gateway)

        with pytest.raises(CephS3AuthenticationError, match="admin"):
            croit.get_user_storage_series("waldur-x", -3600)

    def test_a_session_croit_dropped_early_is_renewed_and_the_call_retried(self):
        calls = {"login": 0}

        def handler(request):
            if request.url.path.endswith("/auth/login-form"):
                calls["login"] += 1
                return httpx.Response(
                    200, json={"access_token": f"tok{calls['login']}", "expires_in": 3600}
                )
            if request.headers["authorization"] == "Bearer tok1" and calls["login"] == 1:
                return httpx.Response(401, json={"message": "session expired"})
            return httpx.Response(200, json=[])

        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        croit = CroitClient(api_url="https://croit.example.org", username="u", password="p")
        croit.session = httpx.Client(transport=httpx.MockTransport(handler))
        croit._log_in()  # a cached token, as after an earlier call

        assert croit._request("GET", "/stats").json() == []
        assert calls["login"] == 2

    def test_a_token_refused_right_after_login_is_not_retried(self):
        """That is a credentials problem; retrying would only hide it."""
        calls = {"login": 0, "api": 0}

        def handler(request):
            if request.url.path.endswith("/auth/login-form"):
                calls["login"] += 1
                return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
            calls["api"] += 1
            return httpx.Response(401, json={"message": "no"})

        from waldur_site_agent_ceph_s3.clients.croit import CroitClient
        from waldur_site_agent_ceph_s3.exceptions import CephS3AuthenticationError

        croit = CroitClient(api_url="https://croit.example.org", username="u", password="p")
        croit.session = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(CephS3AuthenticationError):
            croit._request("GET", "/stats")
        assert calls == {"login": 1, "api": 1}
