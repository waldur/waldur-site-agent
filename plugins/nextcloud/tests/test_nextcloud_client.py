"""Unit tests for NextcloudClient."""

import pytest
import respx
import httpx

from waldur_site_agent_nextcloud.client import NextcloudClient, _gib_to_bytes, _bytes_to_gib
from waldur_site_agent_nextcloud.exceptions import (
    NextcloudAPIError,
    NextcloudAuthError,
    NextcloudGroupError,
    NextcloudGroupFolderError,
)

BASE_URL = "https://nextcloud.example.com"
OCS = f"{BASE_URL}/ocs/v2.php"
GF = f"{BASE_URL}/apps/groupfolders/folders"


def _ocs_ok(data=None):
    return {
        "ocs": {
            "meta": {"status": "ok", "statuscode": 100, "message": "OK"},
            "data": data or {},
        }
    }


def _ocs_err(code=404, message="not found"):
    return {
        "ocs": {
            "meta": {"status": "failure", "statuscode": code, "message": message},
            "data": None,
        }
    }


@pytest.fixture
def client():
    return NextcloudClient(BASE_URL, "admin", "secret")


# ------------------------------------------------------------------
# _make_request
# ------------------------------------------------------------------

@respx.mock
def test_make_request_auth_error(client):
    respx.get(f"{OCS}/cloud/users/foo").mock(
        return_value=httpx.Response(401)
    )
    with pytest.raises(NextcloudAuthError):
        client._make_request("GET", f"{OCS}/cloud/users/foo")


@respx.mock
def test_make_request_connect_error(client):
    respx.get(f"{OCS}/cloud/users/foo").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(NextcloudAPIError, match="Cannot connect"):
        client._make_request("GET", f"{OCS}/cloud/users/foo")


# ------------------------------------------------------------------
# ping
# ------------------------------------------------------------------

@respx.mock
def test_ping_success(client):
    respx.get(f"{BASE_URL}/status.php").mock(
        return_value=httpx.Response(200, json={"installed": True, "version": "29.0.0"})
    )
    assert client.ping() is True


@respx.mock
def test_ping_not_installed(client):
    respx.get(f"{BASE_URL}/status.php").mock(
        return_value=httpx.Response(200, json={"installed": False})
    )
    assert client.ping() is False


@respx.mock
def test_ping_connect_error(client):
    respx.get(f"{BASE_URL}/status.php").mock(
        side_effect=httpx.ConnectError("refused")
    )
    assert client.ping() is False


# ------------------------------------------------------------------
# Groups
# ------------------------------------------------------------------

@respx.mock
def test_group_exists_true(client):
    respx.get(f"{OCS}/cloud/groups/mygroup").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"users": []}))
    )
    assert client.group_exists("mygroup") is True


@respx.mock
def test_group_exists_false(client):
    respx.get(f"{OCS}/cloud/groups/missing").mock(
        return_value=httpx.Response(200, json=_ocs_err())
    )
    assert client.group_exists("missing") is False


@respx.mock
def test_create_group(client):
    # group_exists → False
    respx.get(f"{OCS}/cloud/groups/newgroup").mock(
        return_value=httpx.Response(200, json=_ocs_err())
    )
    respx.post(f"{OCS}/cloud/groups").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.create_group("newgroup")  # should not raise


@respx.mock
def test_create_group_already_exists(client):
    respx.get(f"{OCS}/cloud/groups/existing").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"users": []}))
    )
    # No POST should be made
    client.create_group("existing")


@respx.mock
def test_delete_group(client):
    respx.get(f"{OCS}/cloud/groups/g1").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"users": []}))
    )
    respx.delete(f"{OCS}/cloud/groups/g1").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.delete_group("g1")


@respx.mock
def test_list_group_members(client):
    respx.get(f"{OCS}/cloud/groups/g1").mock(
        return_value=httpx.Response(
            200, json=_ocs_ok({"users": ["alice", "bob"]})
        )
    )
    assert client.list_group_members("g1") == ["alice", "bob"]


@respx.mock
def test_add_user_to_group(client):
    respx.post(f"{OCS}/cloud/users/alice/groups").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.add_user_to_group("alice", "g1")


@respx.mock
def test_add_user_to_group_already_in_group(client):
    respx.post(f"{OCS}/cloud/users/alice/groups").mock(
        return_value=httpx.Response(200, json=_ocs_err(102, "User already in group"))
    )
    client.add_user_to_group("alice", "g1")  # must not raise


@respx.mock
def test_add_user_to_group_error(client):
    respx.post(f"{OCS}/cloud/users/alice/groups").mock(
        return_value=httpx.Response(200, json=_ocs_err(998, "not allowed"))
    )
    with pytest.raises(NextcloudGroupError):
        client.add_user_to_group("alice", "g1")


@respx.mock
def test_remove_user_from_group(client):
    respx.delete(f"{OCS}/cloud/users/alice/groups").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.remove_user_from_group("alice", "g1")


# ------------------------------------------------------------------
# Group Folders
# ------------------------------------------------------------------

FOLDER_DATA = {
    "id": 7,
    "mount_point": "waldur-abc123",
    "groups": {"waldur-abc123": 31},
    "quota": _gib_to_bytes(25),
    "size": _gib_to_bytes(3),
    "acl": False,
}


@respx.mock
def test_create_group_folder(client):
    respx.post(f"{GF}").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"id": 7}))
    )
    folder_id = client.create_group_folder("waldur-abc123")
    assert folder_id == 7


@respx.mock
def test_create_group_folder_error(client):
    respx.post(f"{GF}").mock(
        return_value=httpx.Response(200, json=_ocs_err(500, "server error"))
    )
    with pytest.raises(NextcloudGroupFolderError):
        client.create_group_folder("bad")


@respx.mock
def test_get_group_folder(client):
    respx.get(f"{GF}/7").mock(
        return_value=httpx.Response(200, json=_ocs_ok(FOLDER_DATA))
    )
    folder = client.get_group_folder(7)
    assert folder["id"] == 7
    assert folder["mount_point"] == "waldur-abc123"


@respx.mock
def test_delete_group_folder(client):
    respx.delete(f"{GF}/7").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"success": True}))
    )
    client.delete_group_folder(7)


@respx.mock
def test_add_group_to_folder(client):
    respx.post(f"{GF}/7/groups").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.add_group_to_folder(7, "waldur-abc123")


@respx.mock
def test_set_group_permissions(client):
    respx.post(f"{GF}/7/groups/waldur-abc123").mock(
        return_value=httpx.Response(200, json=_ocs_ok())
    )
    client.set_group_permissions(7, "waldur-abc123", 15)


@respx.mock
def test_set_group_permissions_error(client):
    respx.post(f"{GF}/7/groups/waldur-abc123").mock(
        return_value=httpx.Response(200, json=_ocs_err(500, "server error"))
    )
    with pytest.raises(NextcloudGroupFolderError):
        client.set_group_permissions(7, "waldur-abc123", 15)


@respx.mock
def test_set_folder_quota(client):
    respx.post(f"{GF}/7/quota").mock(
        return_value=httpx.Response(200, json=_ocs_ok({"success": True}))
    )
    client.set_folder_quota(7, _gib_to_bytes(25))


@respx.mock
def test_get_folder_usage(client):
    respx.get(f"{GF}/7").mock(
        return_value=httpx.Response(200, json=_ocs_ok(FOLDER_DATA))
    )
    usage = client.get_folder_usage(7)
    assert usage == _gib_to_bytes(3)


@respx.mock
def test_get_folder_usage_raises_on_error(client):
    """Errors must propagate so callers can omit the folder from the report.

    Degrading to 0 here would make a transient outage indistinguishable from
    genuinely empty storage, and the reporting path would overwrite the
    period's real usage with 0 GiB.
    """
    respx.get(f"{GF}/99").mock(
        return_value=httpx.Response(200, json=_ocs_err())
    )
    with pytest.raises(NextcloudGroupFolderError):
        client.get_folder_usage(99)


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

def test_gib_to_bytes():
    assert _gib_to_bytes(1) == 1024**3
    assert _gib_to_bytes(25) == 25 * 1024**3
    assert _gib_to_bytes(25.9) == 25 * 1024**3  # float truncated to int


def test_bytes_to_gib():
    assert _bytes_to_gib(1024**3) == 1.0
    assert _bytes_to_gib(25 * 1024**3) == 25.0
    assert _bytes_to_gib(1024**3 - 1) == 1.0   # rounds to 2 decimal places
    assert _bytes_to_gib(512 * 1024**2) == 0.5  # sub-GB values preserved
    assert _bytes_to_gib(0) == 0.0
