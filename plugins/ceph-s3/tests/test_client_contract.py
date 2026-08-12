"""Behaviour every S3 admin client must exhibit, whatever the flavour.

The croit flavour runs these against a scripted gateway; the radosgw flavour runs
the same promises against a live cluster under tests/e2e. Anything asserted here
is something the backend relies on and neither client may quietly change.
"""

import pytest

from waldur_site_agent_ceph_s3.exceptions import CephS3UserNotFoundError

# The management surface. get_user_storage_series is deliberately absent: it is
# croit-only, which is why GB-day metering lives in its own reporting backend.
REQUIRED_METHODS = [
    "ping",
    "create_user",
    "delete_user",
    "get_user_info",
    "list_users",
    "list_user_keys",
    "create_user_key",
    "delete_user_key",
    "get_user_quota",
    "set_user_quota",
    "set_user_bucket_quota",
    "get_user_buckets",
]


def test_client_implements_the_full_management_interface(client):
    missing = [name for name in REQUIRED_METHODS if not hasattr(client, name)]
    assert missing == [], f"flavour is missing {missing}"


def test_unknown_user_raises_not_found(client, gateway):
    gateway.no_such_user("ghost")
    with pytest.raises(CephS3UserNotFoundError):
        client.get_user_info("ghost")


def test_deleting_an_absent_key_is_not_an_error(client, gateway):
    """Rotation replays must be safe: a key already gone is a no-op.

    croit answers 404 and RadosGW answers 403/InvalidAccessKeyId for this, so the
    promise is about the outcome, not the status code.
    """
    gateway.absent_key("user-1", "GONEACCESSKEY0000000")
    client.delete_user_key("user-1", "GONEACCESSKEY0000000")


def test_quota_is_written_from_a_flavour_neutral_dict(client, gateway):
    """The backend states a ceiling once; each client spells it its own way.

    croit takes camelCase bytes in a JSON body, RadosGW takes hyphenated query
    parameters. Leaving the backend to speak one of those would make the other
    flavour parse a dialect it has no reason to know.
    """
    gateway.accepts_quota()
    client.set_user_quota(
        "user-1",
        {"enabled": True, "max_size_bytes": 5_000_000_000, "max_objects": 1000},
    )
    assert gateway.last_body() == {
        "enabled": True,
        "maxSize": 5_000_000_000,
        "maxObjects": 1000,
    }


def test_quota_omits_dimensions_that_were_not_ordered(client, gateway):
    """An unordered dimension must not be sent as zero, which would cap at nothing."""
    gateway.accepts_quota()
    client.set_user_bucket_quota("user-1", {"enabled": True, "max_objects": 10})
    body = gateway.last_body()
    assert "maxSize" not in body
    assert body["maxObjects"] == 10


def test_get_user_info_is_normalised(client, gateway):
    """The backend reads one spelling; croit says name, RGW says display_name."""
    gateway.user("user-1")
    info = client.get_user_info("user-1")
    assert set(info) >= {"uid", "name", "email", "suspended", "keys"}


def test_get_user_buckets_is_normalised(client, gateway):
    """A wrong key here reads as zero, not as an error.

    Every consumer uses .get() with a default, so a flavour whose spelling the
    backend does not know publishes an empty storage summary silently.
    """
    gateway.buckets(
        "user-1", [{"bucket": "b", "usageSum": {"size": 1024, "numObjects": 2}}]
    )
    assert client.get_user_buckets("user-1") == [
        {"name": "b", "size_bytes": 1024, "num_objects": 2}
    ]


def test_a_bucket_without_statistics_reads_as_zero(client, gateway):
    """croit answers usageSum: null for a bucket it has not measured."""
    gateway.buckets("user-1", [{"bucket": "fresh", "usageSum": None}])
    assert client.get_user_buckets("user-1") == [
        {"name": "fresh", "size_bytes": 0, "num_objects": 0}
    ]


def test_list_users_answers_ids_not_objects(client, gateway):
    """Both flavours answer ids; radosgw cannot answer anything else.

    croit used to return its user objects here, and BaseS3AdminClient
    .list_resources called .get("uid") on each — which would have raised
    AttributeError the first time a radosgw offering synced.
    """
    gateway.user("user-1")
    assert client.list_users() == ["user-1"]


def test_get_user_quota_is_normalised(client, gateway):
    """Read back in the neutral shape, both scopes present.

    croit spells these camelCase inside the user object, and reading them
    through the normalised get_user_info returned two empty dicts for every
    resource — quotas silently absent from resource metadata.
    """
    gateway.user(
        "user-1",
        user_quota={"enabled": True, "maxSize": 5_000_000_512, "maxObjects": 1000},
        bucket_quota={"enabled": False, "maxSize": -1, "maxObjects": -1},
    )
    quota = client.get_user_quota("user-1")

    assert quota["user_quota"] == {
        "enabled": True,
        "max_size_bytes": 5_000_000_512,
        "max_objects": 1000,
    }
    assert quota["bucket_quota"]["enabled"] is False
