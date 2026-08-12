"""Tests for the RadosGW Admin Ops client.

Every expectation here was measured against Ceph 19.2.0 (squid) rather than read
off the documentation, which is wrong in at least three places the plugin cares
about.
"""

import httpx
import pytest

from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)


def make_client(handler, **kwargs):
    client = RadosGWClient(
        endpoint="https://rgw.example.org",
        access_key="ADMINACCESSKEY000000",
        secret_key="adminsecret0123456789abcdefghijklmnopqrs",
        **kwargs,
    )
    client.session = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_requests_are_signed_with_the_s3_variant_of_sigv4():
    """RGW refuses a signature computed without x-amz-content-sha256.

    botocore's generic SigV4Auth omits that header and every request comes back
    SignatureDoesNotMatch; only S3SigV4Auth emits it.
    """
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return httpx.Response(200, json={})

    make_client(handler)._request("GET", "/user", params={"list": None})

    assert "x-amz-content-sha256" in seen
    assert seen["authorization"].startswith("AWS4-HMAC-SHA256 ")


def test_admin_path_is_appended_to_the_gateway_endpoint():
    """Admin Ops lives on the gateway itself, not on a management host."""
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    make_client(handler)._request("GET", "/user", params={"uid": "u"})
    assert seen["url"].startswith("https://rgw.example.org/admin/user?")


def test_rgw_admin_entry_is_configurable():
    """rgw_admin_entry can be renamed by the operator."""
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    make_client(handler, admin_path="ceph-admin")._request("GET", "/user")
    assert seen["url"].startswith("https://rgw.example.org/ceph-admin/user")


def test_spaces_are_percent_encoded_not_plus_encoded():
    """A '+' standing in for a space is read back differently by signer and RGW."""
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    make_client(handler)._request("PUT", "/user", params={"display-name": "Two words"})

    query = seen["url"].split("?", 1)[1]
    assert "Two%20words" in query
    assert "+" not in query


def test_valueless_subresources_are_sent_with_a_trailing_equals():
    """"?key" and "?quota" must survive canonicalisation identically at both ends."""
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    make_client(handler)._request("PUT", "/user", params={"key": None, "uid": "u"})
    assert "key=&uid=u" in seen["url"]


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        (404, "NoSuchUser", CephS3UserNotFoundError),
        (409, "UserAlreadyExists", CephS3UserExistsError),
        (409, "KeyExists", CephS3UserExistsError),
        # RGW reports an absent key as 403, not 404. Classifying on the status
        # alone would turn "already deleted" into a hard failure and break the
        # replayable rotation path.
        (403, "InvalidAccessKeyId", CephS3UserNotFoundError),
        (403, "AccessDenied", CephS3AuthenticationError),
        (400, "InvalidArgument", CephS3APIError),
        (409, "BucketAlreadyExists", CephS3UserExistsError),
    ],
)
def test_errors_are_classified_on_the_code_not_the_status(status, code, expected):
    client = make_client(lambda request: httpx.Response(status, json={"Code": code}))
    with pytest.raises(expected):
        client._request("GET", "/user", params={"uid": "someone"})


def test_an_unparseable_error_still_raises_something_actionable():
    client = make_client(lambda request: httpx.Response(500, content=b"<html>502</html>"))
    with pytest.raises(CephS3APIError):
        client._request("GET", "/user")


def test_a_secret_in_the_query_never_reaches_the_error_message():
    """RGW takes secret-key as a query parameter, so errors can echo it."""
    client = make_client(
        lambda request: httpx.Response(
            400, json={"Code": "InvalidArgument: secret-key=SUPERSECRETVALUE"}
        )
    )
    with pytest.raises(CephS3APIError) as excinfo:
        client._request("PUT", "/user", params={"secret-key": "SUPERSECRETVALUE"})
    assert "SUPERSECRETVALUE" not in str(excinfo.value)


class TestQuotaOperations:
    """Ceilings. The byte/kilobyte distinction here is the whole point."""

    def test_quota_is_written_in_bytes_not_kilobytes(self):
        """max-size round-trips exactly; max-size-kb rounds up to the next KiB.

        Measured: max-size=5000000000 reads back 5000000000, while
        max-size-kb=4882813 reads back 5000000512. croit takes the KB path,
        which is why its read-backs never compare equal. This flavour is exact
        and must stay that way.
        """
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, content=b"")

        make_client(handler).set_user_quota(
            "u", {"enabled": True, "max_size_bytes": 5_000_000_000, "max_objects": 1000}
        )

        assert "max-size=5000000000" in seen["url"]
        assert "max-size-kb" not in seen["url"]
        assert "quota-type=user" in seen["url"]
        assert "enabled=True" in seen["url"]

    def test_bucket_quota_uses_the_bucket_scope(self):
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, content=b"")

        make_client(handler).set_user_bucket_quota("u", {"enabled": True, "max_objects": 5})
        assert "quota-type=bucket" in seen["url"]

    def test_a_dimension_that_was_not_ordered_is_omitted(self):
        """A quota PUT merges, so an omitted dimension keeps its stored value.

        Sending 0 instead would cap the tenant at nothing.
        """
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, content=b"")

        make_client(handler).set_user_quota("u", {"enabled": True, "max_objects": 5})
        assert "max-size" not in seen["url"]

    def test_get_user_quota_returns_both_scopes_neutrally(self):
        def handler(request):
            scope = "bucket" if "quota-type=bucket" in str(request.url) else "user"
            return httpx.Response(
                200,
                json={
                    "enabled": True,
                    "max_size": 5_000_000_000 if scope == "user" else 1_000_000,
                    "max_size_kb": 4882813,
                    "max_objects": 1000,
                },
            )

        quota = make_client(handler).get_user_quota("u")
        assert quota["user_quota"]["max_size_bytes"] == 5_000_000_000
        assert quota["bucket_quota"]["max_size_bytes"] == 1_000_000


class TestBucketOperations:
    def test_buckets_are_normalised_to_neutral_keys(self):
        """croit says usageSum/numObjects, RGW says usage['rgw.main']/num_objects.

        The backend reads one shape, so each client translates its own.
        """
        client = make_client(
            lambda request: httpx.Response(
                200,
                json=[
                    {
                        "bucket": "b1",
                        "usage": {
                            "rgw.main": {
                                "size": 5242880,
                                "size_actual": 5242880,
                                "num_objects": 5,
                            }
                        },
                    }
                ],
            )
        )
        assert client.get_user_buckets("u") == [
            {"name": "b1", "size_bytes": 5242880, "num_objects": 5}
        ]

    def test_a_bucket_with_no_statistics_reads_as_zero_not_as_an_error(self):
        """A freshly created bucket has no rgw.main container at all."""
        client = make_client(
            lambda request: httpx.Response(200, json=[{"bucket": "fresh", "usage": {}}])
        )
        assert client.get_user_buckets("u") == [
            {"name": "fresh", "size_bytes": 0, "num_objects": 0}
        ]




class TestUserOperations:
    """Create, read, list and delete, as measured on squid."""

    def test_create_user_suppresses_the_auto_generated_key(self):
        """RGW mints a key unless told not to; Waldur must own every working key.

        croit has no equivalent and has to delete the auto key afterwards. Here
        it is never created, so there is no window in which a credential Waldur
        cannot rotate is live.
        """
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"user_id": "u", "keys": []})

        make_client(handler).create_user(
            "u", "Display", access_key="AKAKAKAKAKAKAKAKAKAK", secret_key="s" * 40
        )

        assert "generate-key=False" in seen["url"]
        assert "access-key=AKAKAKAKAKAKAKAKAKAK" in seen["url"]
        assert "display-name=Display" in seen["url"]

    def test_list_users_reads_the_keys_container(self):
        """?list answers a container, not a bare array."""
        client = make_client(
            lambda request: httpx.Response(
                200, json={"keys": ["alice", "bob"], "truncated": False, "count": 2}
            )
        )
        assert client.list_users() == ["alice", "bob"]

    def test_ping_avoids_the_endpoint_that_needs_an_extra_cap(self):
        """/admin/info requires info=read; ?list needs only the users cap."""
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"keys": [], "count": 0})

        assert make_client(handler).ping() is True
        assert "/admin/user?list=" in seen["url"]
        assert "/info" not in seen["url"]

    def test_delete_user_never_purges_data(self):
        """An erred terminate is recoverable; a purged bucket is not.

        RGW would happily take the tenant's data with the user if asked, so the
        parameter is deliberately absent and the 409 is allowed to fail the order.
        """
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, content=b"")

        make_client(handler).delete_user("u")
        assert "purge-data" not in seen["url"]

    def test_a_user_that_still_owns_buckets_fails_with_a_usable_message(self):
        """RGW answers "BucketAlreadyExists" to a delete, which explains nothing."""
        client = make_client(
            lambda request: httpx.Response(409, json={"Code": "BucketAlreadyExists"})
        )
        with pytest.raises(CephS3APIError) as excinfo:
            client.delete_user("u")
        assert "still owns buckets" in str(excinfo.value)

    def test_delete_user_is_idempotent(self):
        """Termination is retried; a second delete answers 404 and must not raise."""
        client = make_client(
            lambda request: httpx.Response(404, json={"Code": "NoSuchUser"})
        )
        assert client.delete_user("u") is True

    def test_identifiers_are_validated_before_they_reach_a_url(self):
        client = make_client(lambda request: httpx.Response(200, json={}))
        with pytest.raises(CephS3APIError):
            client.get_user_info("../../v2/status")


    def test_create_user_sends_the_placement_it_is_given(self):
        """The backend's default_placement used to be dropped in **kwargs."""
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json={})

        make_client(handler).create_user(
            uid="waldur_u", name="demo", default_placement="fast-pool"
        )

        assert "default-placement=fast-pool" in seen["url"]


class TestKeyOperations:
    """Key handling, including the in-place secret rotation RGW allows."""

    def test_list_user_keys_reads_them_off_the_user_object(self):
        """There is no separate key endpoint; keys live inside the user body."""
        client = make_client(
            lambda request: httpx.Response(
                200,
                json={
                    "user_id": "u",
                    "keys": [
                        {
                            "user": "u",
                            "access_key": "AK1",
                            "secret_key": "S1",
                            "active": True,
                        }
                    ],
                },
            )
        )
        assert client.list_user_keys("u") == [
            {"user": "u", "access_key": "AK1", "secret_key": "S1"}
        ]

    def test_create_user_key_sends_both_halves(self):
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json=[])

        make_client(handler).create_user_key("u", "AK2AK2AK2AK2AK2AK2AK", "s" * 40)

        assert "key=" in seen["url"]
        assert "access-key=AK2AK2AK2AK2AK2AK2AK" in seen["url"]
        assert "secret-key=" in seen["url"]

    def test_delete_user_key_treats_invalid_access_key_id_as_already_gone(self):
        client = make_client(
            lambda request: httpx.Response(403, json={"Code": "InvalidAccessKeyId"})
        )
        client.delete_user_key("u", "GONEGONEGONEGONEGONE")


def test_get_user_info_is_normalised_to_the_shared_spelling():
    """RGW says user_id/display_name; croit says uid/name.

    The backend reads one of them with .get() defaults, so an un-normalised
    body publishes empty metadata instead of failing.
    """
    client = make_client(
        lambda request: httpx.Response(
            200,
            json={
                "user_id": "u1",
                "display_name": "User One",
                "email": "u@example.org",
                "suspended": 0,
                "default_placement": "default-placement",
                "default_storage_class": "STANDARD",
                "keys": [{"user": "u1", "access_key": "AK", "secret_key": "SK"}],
            },
        )
    )
    info = client.get_user_info("u1")

    assert info["uid"] == "u1"
    assert info["name"] == "User One"
    assert info["suspended"] is False
    assert info["default_placement"] == "default-placement"
    assert info["keys"][0]["access_key"] == "AK"
