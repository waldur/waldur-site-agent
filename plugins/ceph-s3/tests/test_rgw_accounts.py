"""Account mode: one RGW account, with its own root user, per resource.

Gateway behaviour asserted here was measured on Ceph 20.2.5; the pre-20.2.2
failure mode on 19.2.3.
"""

from unittest.mock import Mock, patch
from urllib.parse import parse_qs
from uuid import UUID

import httpx
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_attributes import ResourceAttributes

from waldur_site_agent.backend.exceptions import DuplicateResourceError
from waldur_site_agent_ceph_s3.backend import CephS3Backend, _ownership_stamp
from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3AccountExistsError,
    CephS3AccountNotFoundError,
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3Error,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)

from conftest import RADOSGW_SETTINGS

GB = 1000000000
ACCOUNT = "RGW12345678901234567"
UID = "waldur-res"
RESOURCE_UUID = UUID("12345678-1234-5678-9abc-123456789abc")
STAMP = _ownership_stamp(RESOURCE_UUID, UID)
COMPONENTS = {
    "s3_storage": {"accounting_type": "limit", "backend_name": "storage", "unit_factor": GB}
}
SETTINGS = {**RADOSGW_SETTINGS, "rgw_accounts": True}


def quota(enabled=False, size=-1, objects=-1):
    return {"enabled": enabled, "max_size_bytes": size, "max_objects": objects}


def make_backend(settings=SETTINGS):
    with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
        backend = CephS3Backend(dict(settings), COMPONENTS)
    client = Mock()
    client.create_account.return_value = {"id": ACCOUNT, "name": UID, "email": STAMP}
    client.get_user_info.return_value = {
        "uid": UID, "email": STAMP, "account_id": ACCOUNT, "type": "root"
    }
    client.get_user_buckets.return_value = []
    client.list_account_users.return_value = [UID]

    def get_account(account_id=None, name=None, tenant=""):
        del account_id, name, tenant
        applied = {}
        for call in client.set_account_quota.call_args_list:
            scope = "quota" if call.args[2] == "account" else "bucket_quota"
            applied[scope] = {**quota(), **call.args[1]}
        return {"id": ACCOUNT, "name": UID, "email": STAMP,
                "quota": applied.get("quota", quota()),
                "bucket_quota": applied.get("bucket_quota", quota())}

    client.get_account.side_effect = get_account
    backend.client = client
    return backend


def make_resource(attributes=None, uuid=RESOURCE_UUID):
    resource = WaldurResource(uuid=uuid, name="Team archive (prod)", backend_id=UID)
    resource.attributes = ResourceAttributes.from_dict(
        attributes if attributes is not None else {"max_storage_limit": 2, "max_object_limit": 3}
    )
    return resource


class TestCreate:
    def test_creates_a_stamped_account_and_its_root_user(self):
        backend = make_backend()

        info = backend.create_resource_with_id(make_resource(), UID)

        backend.client.create_account.assert_called_once_with(name=UID, email=STAMP, tenant="")
        user = backend.client.create_user.call_args.kwargs
        assert user["account_id"] == ACCOUNT
        assert user["account_root"] is True
        # The display name is the IAM UserName, which a resource name with
        # spaces and parentheses would break.
        assert user["name"] == UID
        assert info.backend_id == UID
        assert info.backend_metadata["s3_account_id"] == ACCOUNT

    def test_ceilings_go_on_the_account_not_the_user(self):
        backend = make_backend()

        backend.create_resource_with_id(make_resource(), UID)

        expected = {"enabled": True, "max_size_bytes": 2 * GB, "max_objects": 3}
        backend.client.set_account_quota.assert_any_call(ACCOUNT, expected, "account")
        backend.client.set_account_quota.assert_any_call(ACCOUNT, expected, "bucket")
        backend.client.set_user_quota.assert_not_called()
        backend.client.set_user_bucket_quota.assert_not_called()

    def test_a_retry_adopts_the_account_it_stamped(self):
        backend = make_backend()
        backend.client.create_account.side_effect = CephS3AccountExistsError("exists")

        info = backend.create_resource_with_id(make_resource(), UID)

        backend.client.get_account.assert_any_call(name=UID, tenant="")
        assert info.backend_metadata["s3_account_id"] == ACCOUNT

    def test_an_account_stamped_for_another_resource_is_refused(self):
        backend = make_backend()
        backend.client.create_account.side_effect = CephS3AccountExistsError("exists")
        other = make_resource(uuid=UUID("00000000-0000-0000-0000-000000000001"))

        with pytest.raises(DuplicateResourceError, match="refusing to take it over"):
            backend.create_resource_with_id(other, UID)
        backend.client.create_user.assert_not_called()

    def test_an_unreadable_existing_account_is_refused(self):
        backend = make_backend()
        backend.client.create_account.side_effect = CephS3AccountExistsError("exists")
        backend.client.get_account.side_effect = CephS3AccountNotFoundError("gone")

        with pytest.raises(DuplicateResourceError, match="cannot be confirmed"):
            backend.create_resource_with_id(make_resource(), UID)

    @pytest.mark.parametrize("uid", ["acme$" + "w" * 65, "w" * 65])
    def test_a_uid_iam_cannot_hold_is_refused_before_any_request(self, uid):
        backend = make_backend()

        with pytest.raises(CephS3Error, match="cannot name an RGW account user"):
            backend.create_resource_with_id(make_resource(), uid)
        backend.client.create_account.assert_not_called()

    def test_an_unapplied_quota_fails_the_order(self):
        """19.2.x answers the account quota PUT with 200 and changes nothing."""
        backend = make_backend()
        backend.client.get_account.side_effect = None
        backend.client.get_account.return_value = {
            "id": ACCOUNT, "quota": quota(), "bucket_quota": quota()
        }

        with pytest.raises(CephS3Error, match="need Ceph 20.2.2"):
            backend.create_resource_with_id(make_resource(), UID)

    def test_an_unreadable_quota_names_the_version(self):
        """Before 20.2.2 the read-back is refused with AccessDenied, not a bad value."""
        backend = make_backend()
        backend.client.get_account.side_effect = CephS3AuthenticationError("AccessDenied")

        with pytest.raises(CephS3Error, match="need Ceph 20.2.2"):
            backend.create_resource_with_id(make_resource(), UID)

    def test_our_user_outside_the_account_is_not_adopted(self):
        backend = make_backend()
        backend.client.create_user.side_effect = CephS3UserExistsError("exists")
        backend.client.get_user_info.return_value = {"uid": UID, "email": STAMP, "account_id": ""}

        with pytest.raises(CephS3Error, match="before account mode was enabled"):
            backend.create_resource_with_id(make_resource(), UID)


class TestOptionsAndMetadata:
    def test_an_option_update_rewrites_the_account_quota(self):
        backend = make_backend()

        backend.update_resource_options(make_resource(), {"max_storage_limit": 5, "max_object_limit": 0})

        backend.client.set_account_quota.assert_any_call(
            ACCOUNT, {"enabled": True, "max_size_bytes": 5 * GB, "max_objects": -1}, "account"
        )

    def test_metadata_reports_the_account_quota(self):
        backend = make_backend()
        backend.client.set_account_quota(ACCOUNT, {"enabled": True, "max_size_bytes": GB}, "account")

        meta = backend.get_resource_metadata(UID)

        assert meta["s3_account_id"] == ACCOUNT
        assert meta["quotas"]["account_quota"]["max_size_bytes"] == GB
        backend.client.get_user_quota.assert_not_called()

    def test_user_mode_metadata_has_no_account_key(self):
        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
            backend = CephS3Backend(dict(RADOSGW_SETTINGS), COMPONENTS)
        assert "s3_account_id" not in backend._connection_info(UID)


class TestDelete:
    def test_a_bucket_stops_termination_before_any_user_goes(self):
        """RGW deletes an account root user with 200 even while buckets remain."""
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "b"}]

        with pytest.raises(CephS3Error, match="still owns 1 bucket"):
            backend.delete_resource(make_resource())
        backend.client.delete_user.assert_not_called()
        backend.client.delete_account.assert_not_called()

    def test_every_member_goes_root_last_then_the_account(self):
        backend = make_backend()
        backend.client.list_account_users.return_value = [UID, "3f1c-iam-user"]

        backend.delete_resource(make_resource())

        deleted = [call.args[0] for call in backend.client.delete_user.call_args_list]
        assert deleted == ["3f1c-iam-user", UID]
        backend.client.delete_account.assert_called_once_with(ACCOUNT)

    def test_a_retry_finds_the_account_by_name_once_its_root_is_gone(self):
        backend = make_backend()
        backend.client.get_user_info.side_effect = CephS3UserNotFoundError("gone")
        backend.client.list_account_users.return_value = []

        backend.delete_resource(make_resource())

        backend.client.get_account.assert_any_call(name=UID, tenant="")
        backend.client.delete_account.assert_called_once_with(ACCOUNT)

    def test_nothing_left_is_success(self):
        backend = make_backend()
        backend.client.get_user_info.side_effect = CephS3UserNotFoundError("gone")
        backend.client.get_account.side_effect = CephS3AccountNotFoundError("gone")

        backend.delete_resource(make_resource())

        backend.client.delete_account.assert_not_called()

    def test_an_unstamped_user_is_not_deleted(self):
        backend = make_backend()
        backend.client.get_user_info.return_value = {"uid": UID, "email": "", "account_id": ACCOUNT}

        with pytest.raises(CephS3Error, match="not provisioned by Waldur"):
            backend.delete_resource(make_resource())
        backend.client.list_account_users.assert_not_called()

    def test_a_user_from_before_account_mode_is_deleted_as_a_user(self):
        backend = make_backend()
        backend.client.get_user_info.return_value = {"uid": UID, "email": STAMP, "account_id": ""}

        backend.delete_resource(make_resource())

        backend.client.delete_user.assert_called_once_with(UID)
        backend.client.delete_account.assert_not_called()


def test_croit_flavour_rejects_account_mode():
    with pytest.raises(ValueError, match="rgw_accounts"):
        CephS3Backend(
            {"api_url": "https://croit", "token": "t", "s3_endpoint": "https://s3", "rgw_accounts": True},
            COMPONENTS,
        )


# -- client ------------------------------------------------------------------


def make_client(handler):
    client = RadosGWClient(
        endpoint="https://rgw.example.org",
        access_key="ADMINACCESSKEY000000",
        secret_key="adminsecret0123456789abcdefghijklmnopqrs",
    )
    client.session = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def query(request):
    return {k: v[0] for k, v in parse_qs(request.url.query.decode(), keep_blank_values=True).items()}


class TestClient:
    def test_root_user_is_created_inside_the_account(self):
        seen = []
        client = make_client(lambda r: seen.append(r) or httpx.Response(200, json={}))

        client.create_user(UID, UID, account_id=ACCOUNT, account_root=True,
                           access_key="AKIAEXAMPLE000000001", secret_key="s")

        params = query(seen[0])
        assert params["account-id"] == ACCOUNT
        assert params["account-root"] == "True"

    def test_an_absent_account_is_account_not_found(self):
        """RGW says NoSuchKey for it, which elsewhere means an absent object."""
        client = make_client(lambda r: httpx.Response(404, json={"Code": "NoSuchKey"}))

        with pytest.raises(CephS3AccountNotFoundError):
            client.get_account(account_id=ACCOUNT)
        client.delete_account(ACCOUNT)  # absent is success

    def test_a_taken_account_name_is_account_exists(self):
        client = make_client(lambda r: httpx.Response(409, json={"Code": "AccountAlreadyExists"}))

        with pytest.raises(CephS3AccountExistsError):
            client.create_account(UID, email=STAMP)

    def test_account_quota_is_written_in_bytes_to_the_account_endpoint(self):
        seen = []
        client = make_client(lambda r: seen.append(r) or httpx.Response(200, json={}))

        client.set_account_quota(ACCOUNT, {"enabled": True, "max_size_bytes": 5 * GB}, "account")

        assert seen[0].method == "PUT" and seen[0].url.path == "/admin/account"
        params = query(seen[0])
        assert params["quota-type"] == "account"
        assert params["max-size"] == str(5 * GB)
        assert "max-objects" not in params  # a PUT merges; left out is kept

    @pytest.mark.parametrize("bad", ["RGW42", "RGW1234567890123456X", "../user"])
    def test_a_malformed_account_id_never_reaches_the_gateway(self, bad):
        client = make_client(lambda r: pytest.fail("request sent"))

        with pytest.raises(CephS3APIError, match="Not an RGW account id"):
            client.set_account_quota(bad, {}, "account")

    def test_members_are_found_across_listing_pages(self):
        users = {"a": ACCOUNT, "b": "", "c": ACCOUNT}

        def handler(request):
            params = query(request)
            if "list" in params:
                if params.get("marker") == "m1":
                    # A tenanted user, as on clusters that use tenants: never
                    # read, since an untenanted account cannot contain it.
                    return httpx.Response(200, json={"keys": ["c", "acme$x"], "truncated": False})
                return httpx.Response(200, json={"keys": ["a", "b"], "truncated": True, "marker": "m1"})
            return httpx.Response(200, json={"user_id": params["uid"], "account_id": users[params["uid"]]})

        assert make_client(handler).list_account_users(ACCOUNT) == ["a", "c"]

    def test_user_info_carries_account_membership(self):
        client = make_client(lambda r: httpx.Response(
            200, json={"user_id": UID, "account_id": ACCOUNT, "type": "root"}
        ))

        info = client.get_user_info(UID)

        assert info["account_id"] == ACCOUNT and info["type"] == "root"


class TestTenantedAccounts:
    """One tenant per resource: account, users and buckets in the tenant's namespace.

    Measured on 20.2.5: RGW refuses an untenanted or other-tenant user in a
    tenanted account, and two tenants may each own a bucket of the same name.
    """

    TENANTED = "res1$waldur-res"

    def test_the_account_is_created_in_the_resources_tenant(self):
        backend = make_backend({**SETTINGS, "tenant_template": "{resource_slug}"})
        resource = make_resource()
        resource.slug = "res1"

        info = backend.create_resource_with_id(resource, UID)

        assert info.backend_id == self.TENANTED
        backend.client.create_account.assert_called_once_with(
            name=UID, email=_ownership_stamp(RESOURCE_UUID, self.TENANTED), tenant="res1"
        )
        user = backend.client.create_user.call_args.kwargs
        assert user["uid"] == self.TENANTED
        # The IAM UserName is the bare uid; "$" is not an IAM character.
        assert user["name"] == UID
        assert info.backend_metadata["s3_tenant"] == "res1"

    def test_teardown_looks_for_members_in_the_tenant(self):
        backend = make_backend({**SETTINGS, "tenant_template": "{resource_slug}"})
        backend.client.get_user_info.return_value = {
            "uid": self.TENANTED, "email": _ownership_stamp(RESOURCE_UUID, self.TENANTED),
            "account_id": ACCOUNT,
        }
        backend.client.list_account_users.return_value = [self.TENANTED]
        resource = make_resource()
        resource.backend_id = self.TENANTED

        backend.delete_resource(resource)

        backend.client.list_account_users.assert_called_once_with(ACCOUNT, tenant="res1")
        backend.client.delete_account.assert_called_once_with(ACCOUNT)

    def test_a_retry_finds_the_tenanted_account_by_name(self):
        backend = make_backend({**SETTINGS, "tenant_template": "{resource_slug}"})
        backend.client.get_user_info.side_effect = CephS3UserNotFoundError("gone")
        backend.client.list_account_users.return_value = []
        resource = make_resource()
        resource.backend_id = self.TENANTED

        backend.delete_resource(resource)

        backend.client.get_account.assert_any_call(name=UID, tenant="res1")


class TestTenantedClient:
    def test_tenanted_uids_are_addressed(self):
        seen = []
        client = make_client(lambda r: seen.append(r) or httpx.Response(
            200, json={"user_id": "root", "tenant": "res1", "account_id": ACCOUNT}
        ))

        client.get_user_info("res1$root")

        assert query(seen[0])["uid"] == "res1$root"

    @pytest.mark.parametrize("bad", ["a-b$root", "res1$../user", "$root"])
    def test_a_malformed_tenanted_uid_never_reaches_the_gateway(self, bad):
        client = make_client(lambda r: pytest.fail("request sent"))
        with pytest.raises(CephS3APIError, match="Refusing"):
            client.get_user_info(bad)

    def test_the_account_is_created_and_found_in_its_tenant(self):
        seen = []
        client = make_client(lambda r: seen.append(r) or httpx.Response(200, json={"id": ACCOUNT}))

        client.create_account(UID, email=STAMP, tenant="res1")
        client.get_account(name=UID, tenant="res1")

        assert query(seen[0])["tenant"] == "res1"
        assert query(seen[1])["tenant"] == "res1" and query(seen[1])["name"] == UID

    def test_members_are_read_from_the_accounts_tenant_only(self):
        read = []

        def handler(request):
            params = query(request)
            if "list" in params:
                return httpx.Response(200, json={"keys": [
                    "plain", "other$x", "res1$root", "res1$3f1c-iam", "alice@example.org",
                ], "truncated": False})
            read.append(params["uid"])
            return httpx.Response(200, json={"user_id": params["uid"], "account_id": ACCOUNT})

        members = make_client(handler).list_account_users(ACCOUNT, tenant="res1")

        assert members == ["res1$root", "res1$3f1c-iam"]
        assert read == ["res1$root", "res1$3f1c-iam"]

    def test_an_unaddressable_uid_does_not_stop_the_member_lookup(self):
        """One user like "alice@example.org" on the gateway must not fail every
        account termination."""

        def handler(request):
            params = query(request)
            if "list" in params:
                return httpx.Response(200, json={"keys": ["alice@example.org", "a"], "truncated": False})
            return httpx.Response(200, json={"user_id": params["uid"], "account_id": ACCOUNT})

        assert make_client(handler).list_account_users(ACCOUNT) == ["a"]
