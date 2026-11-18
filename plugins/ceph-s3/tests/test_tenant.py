"""Per-resource RGW tenants: template rendering, addressing and the croit client."""

import json
from unittest.mock import Mock, patch
from uuid import UUID

import httpx
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent_ceph_s3.backend import CephS3Backend
from waldur_site_agent_ceph_s3.exceptions import CephS3APIError, CephS3UserNotFoundError
from waldur_site_agent_ceph_s3.tenant import (
    qualify,
    render_tenant,
    split,
    validate_tenant_template,
)

from conftest import CROIT_SETTINGS, RADOSGW_SETTINGS

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


def make_resource(**overrides):
    fields = {
        "uuid": RESOURCE_UUID,
        "slug": "my-bucket",
        "name": "My bucket",
        "customer_slug": "Acme-Corp",
        "customer_uuid": CUSTOMER_UUID,
        "project_slug": "research-1",
        "project_uuid": PROJECT_UUID,
        "offering_slug": "s3-storage",
    }
    fields.update(overrides)
    return WaldurResource(**fields)


class TestTemplate:
    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            ("{customer_slug}", "acme_corp"),
            ("{customer_uuid}", CUSTOMER_UUID.hex),
            ("{customer_slug}_{customer_uuid:.8}", "acme_corp_aaaabbbb"),
            ("{project_slug}", "research_1"),
            ("{project_uuid:.8}", "99998888"),
            ("{resource_slug}", "my_bucket"),
            ("{resource_uuid}", RESOURCE_UUID.hex),
            ("{offering_slug}", "s3_storage"),
            ("waldur_{customer_slug}", "waldur_acme_corp"),
            ("fixed", "fixed"),
        ],
    )
    def test_renders_a_valid_tenant(self, template, expected):
        validate_tenant_template(template)
        assert render_tenant(template, make_resource()) == expected

    def test_uuid_given_as_a_string_renders_the_same(self):
        resource = make_resource(customer_uuid=str(CUSTOMER_UUID))
        assert render_tenant("{customer_uuid}", resource) == CUSTOMER_UUID.hex

    @pytest.mark.parametrize(
        ("template", "match"),
        [
            ("{customer}", "unknown field"),
            ("{customer_slug.__class__}", "unknown field"),
            ("{customer_slug[0]}", "unknown field"),
            ("{customer_slug!r}", "conversion"),
            ("acme-{customer_slug}", "characters RGW refuses"),
            ("Acme", "characters RGW refuses"),
            ("{customer_slug", "malformed"),
            ("", "non-empty"),
        ],
    )
    def test_refuses_a_template_that_cannot_render_a_tenant(self, template, match):
        with pytest.raises(ValueError, match=match):
            validate_tenant_template(template)

    def test_a_field_the_resource_lacks_fails_the_render(self):
        resource = make_resource()
        resource.customer_slug = None
        with pytest.raises(ValueError, match="customer_slug"):
            render_tenant("{customer_slug}", resource)

    def test_an_overlong_result_fails_the_render(self):
        with pytest.raises(ValueError, match="not a valid RGW tenant"):
            render_tenant("{customer_uuid}{project_uuid}{resource_uuid}", make_resource())

    def test_qualify_and_split_round_trip(self):
        assert qualify("acme", "waldur-x") == "acme$waldur-x"
        assert qualify("", "waldur-x") == "waldur-x"
        assert split("acme$waldur-x") == ("acme", "waldur-x")
        assert split("waldur-x") == ("", "waldur-x")


class TestBackend:
    def _backend(self, **settings):
        with patch("waldur_site_agent_ceph_s3.backend.CroitClient"):
            backend = CephS3Backend(dict(CROIT_SETTINGS, **settings), COMPONENTS)
        backend.client = Mock()
        backend.client.create_user.return_value = {}
        backend.client.list_user_keys.return_value = []
        return backend

    def test_the_backend_id_carries_the_tenant(self):
        """Everything after create addresses the user by backend id, so it must be
        tenant$uid: the bare uid is the global tenant's user of that name."""
        backend = self._backend(tenant_template="{customer_slug}_{customer_uuid:.8}")

        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")

        assert info.backend_id == "acme_corp_aaaabbbb$waldur-my-bucket"
        assert info.backend_metadata["s3_user"] == "acme_corp_aaaabbbb$waldur-my-bucket"
        assert info.backend_metadata["s3_tenant"] == "acme_corp_aaaabbbb"
        kwargs = backend.client.create_user.call_args.kwargs
        assert kwargs["uid"] == "acme_corp_aaaabbbb$waldur-my-bucket"
        assert "tenant" not in kwargs
        # "$" stays out of the ownership stamp's email address.
        assert "$" not in kwargs["email"]
        assert kwargs["email"].endswith("-acme_corp_aaaabbbb.waldur-my-bucket@invalid")
        # Every follow-up call (key sweep, quotas) addresses the tenanted user.
        addressed = {
            call.args[0] for call in backend.client.mock_calls if call.args
        }
        assert addressed == {"acme_corp_aaaabbbb$waldur-my-bucket"}

    def test_default_tenant_still_works_as_a_fixed_template(self):
        backend = self._backend(default_tenant="acme")
        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")
        assert info.backend_id == "acme$waldur-my-bucket"

    def test_a_fixed_default_tenant_keeps_croits_character_rule(self):
        """Accepted as written before templates existed, upper case included."""
        backend = self._backend(default_tenant="ProdTenant")
        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")
        assert info.backend_id == "ProdTenant$waldur-my-bucket"

    def test_a_default_tenant_rgw_cannot_hold_is_refused(self):
        with pytest.raises(ValueError, match="default_tenant"):
            self._backend(default_tenant="prod-tenant")

    def test_recreating_under_the_stored_backend_id_keeps_one_tenant(self):
        """A restored resource is re-created with its tenanted backend id."""
        backend = self._backend(tenant_template="{customer_slug}_{customer_uuid:.8}")

        info = backend.create_resource_with_id(
            make_resource(), "acme_corp_aaaabbbb$waldur-my-bucket"
        )

        assert info.backend_id == "acme_corp_aaaabbbb$waldur-my-bucket"
        assert (
            backend.client.create_user.call_args.kwargs["uid"]
            == "acme_corp_aaaabbbb$waldur-my-bucket"
        )

    def test_untenanted_offerings_keep_bare_uids(self):
        backend = self._backend()
        info = backend.create_resource_with_id(make_resource(), "waldur-my-bucket")
        assert info.backend_id == "waldur-my-bucket"

    def test_a_bad_template_fails_at_construction(self):
        with pytest.raises(ValueError, match="unknown field"):
            self._backend(tenant_template="{organisation}")

    def test_a_resource_missing_a_template_field_fails_the_order(self):
        backend = self._backend(tenant_template="{project_slug}")
        resource = make_resource()
        resource.project_slug = None
        with pytest.raises(ValueError, match="project_slug"):
            backend.create_resource_with_id(resource, "waldur-my-bucket")
        backend.client.create_user.assert_not_called()

    def test_radosgw_takes_a_tenant_template(self):
        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
            backend = CephS3Backend(
                dict(RADOSGW_SETTINGS, tenant_template="{customer_slug}"), COMPONENTS
            )
        assert backend.tenant_template == "{customer_slug}"


class TestCroitClient:
    def test_create_sends_tenant_beside_the_bare_uid(self, client, gateway):
        gateway.route("/s3/users", httpx.Response(201, json={}), method="POST")

        client.create_user(uid="acme$waldur-x", name="demo")

        assert gateway.last_body() == {"uid": "waldur-x", "name": "demo", "tenant": "acme"}

    def test_create_still_takes_tenant_as_a_keyword(self, client, gateway):
        gateway.route("/s3/users", httpx.Response(201, json={}), method="POST")

        client.create_user(uid="waldur-x", name="demo", tenant="acme")

        assert gateway.last_body()["tenant"] == "acme"
        assert gateway.last_body()["uid"] == "waldur-x"

    def test_create_refuses_two_different_tenants(self, client):
        with pytest.raises(CephS3APIError, match="tenant"):
            client.create_user(uid="acme$waldur-x", name="demo", tenant="other")

    def test_tenanted_paths_escape_the_separator(self, client, gateway):
        gateway.route("/keys", httpx.Response(200, json=[]))
        gateway.route("/quota", httpx.Response(204), method="PUT")

        client.list_user_keys("acme$waldur-x")
        client.set_user_quota("acme$waldur-x", {"max_size_bytes": 10})

        paths = [request.url.raw_path for request in gateway.requests]
        assert paths == [
            b"/api/s3/users/acme%24waldur-x/keys",
            b"/api/s3/users/acme%24waldur-x/quota",
        ]

    @pytest.mark.parametrize(
        "uid",
        [
            "acme$..",
            "..$waldur-x",
            "ac/me$waldur-x",
            "ac-me$waldur-x",
            "a$b$c",
            "$waldur-x",
            "acme$",
        ],
    )
    def test_refuses_a_tenanted_uid_that_could_leave_its_segment(self, client, uid):
        with pytest.raises(CephS3APIError, match="Refusing"):
            client.delete_user(uid)

    @pytest.mark.parametrize(
        "listed",
        [
            {"uid": "acme$waldur-x", "name": "x"},
            {"uid": "waldur-x", "tenant": "acme", "name": "x"},
        ],
    )
    def test_finds_a_tenanted_user_in_either_listed_shape(self, client, gateway, listed):
        gateway.route("/s3/users", httpx.Response(200, json=[listed]), method="GET")
        assert client.get_user_info("acme$waldur-x")["name"] == "x"

    def test_the_global_users_namesake_is_not_the_tenanted_user(self, client, gateway):
        gateway.route(
            "/s3/users",
            httpx.Response(200, json=[{"uid": "waldur-x", "name": "global"}]),
            method="GET",
        )
        with pytest.raises(CephS3UserNotFoundError):
            client.get_user_info("acme$waldur-x")

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
        gateway.route("/s3/users", httpx.Response(200, json=[]), method="GET")
        croit = self._client(gateway)

        croit.list_users()
        croit.list_users()

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
        gateway.route("/s3/users", httpx.Response(200, json=[]), method="GET")
        croit = self._client(gateway)

        croit.list_users()
        croit.list_users()

        logins = [r for r in gateway.requests if r.url.path.endswith("login-form")]
        assert len(logins) == 2

    def test_a_rejected_login_names_the_user(self, gateway):
        from waldur_site_agent_ceph_s3.exceptions import CephS3AuthenticationError

        gateway.route("/auth/login-form", httpx.Response(401, json={"code": 401}))
        croit = self._client(gateway)

        with pytest.raises(CephS3AuthenticationError, match="admin"):
            croit.list_users()

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

        assert croit._request("GET", "/s3/users").json() == []
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
            croit._request("GET", "/s3/users")
        assert calls == {"login": 1, "api": 1}
