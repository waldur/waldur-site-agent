"""Tests for NextcloudBackend."""

from unittest.mock import Mock, patch

import httpx
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent.backend.exceptions import DuplicateResourceError

from waldur_site_agent_nextcloud.backend import NextcloudBackend
from waldur_site_agent_nextcloud.client import NextcloudClient, _gib_to_bytes
from waldur_site_agent_nextcloud.exceptions import (
    NextcloudGroupError,
    NextcloudGroupFolderError,
)

RESOURCE_UUID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
GROUP_ID = f"waldur-{RESOURCE_UUID}"


@pytest.fixture
def nextcloud_settings():
    return {
        "nextcloud_url": "https://nextcloud.example.com",
        "admin_username": "admin",
        "admin_password": "secret",
        "group_prefix": "waldur-",
        "default_storage_quota_gb": 25,
    }


@pytest.fixture
def nextcloud_components():
    return {
        "storage": {
            "measured_unit": "GB",
            "accounting_type": "limit",
            "label": "Storage",
            "unit_factor": 1,
        }
    }


@pytest.fixture
def nextcloud_backend(nextcloud_settings, nextcloud_components):
    with patch("waldur_site_agent_nextcloud.backend.NextcloudClient"):
        backend = NextcloudBackend(nextcloud_settings, nextcloud_components)
    backend.client = Mock(spec=NextcloudClient)
    return backend


@pytest.fixture
def waldur_resource():
    resource = Mock(spec=WaldurResource)
    resource.uuid = RESOURCE_UUID
    resource.name = "My Storage"
    resource.project_name = "My Project"
    resource.backend_id = "7"
    resource.limits = {"storage": 40}
    return resource


# ------------------------------------------------------------------
# Initialisation
# ------------------------------------------------------------------

class TestInit:
    def test_valid_init(self, nextcloud_settings, nextcloud_components):
        with patch("waldur_site_agent_nextcloud.backend.NextcloudClient"):
            backend = NextcloudBackend(nextcloud_settings, nextcloud_components)
        assert backend.backend_type == "nextcloud"
        assert backend.group_prefix == "waldur-"
        assert backend.default_storage_quota_gb == 25
        assert backend.supports_decreasing_usage is True
        assert not hasattr(backend, "user_resolve_method")
        assert backend.allow_resharing is False

    def test_allow_resharing_configurable(self, nextcloud_settings, nextcloud_components):
        nextcloud_settings["allow_resharing"] = True
        with patch("waldur_site_agent_nextcloud.backend.NextcloudClient"):
            backend = NextcloudBackend(nextcloud_settings, nextcloud_components)
        assert backend.allow_resharing is True

    def test_missing_required_setting(self, nextcloud_components):
        with pytest.raises(ValueError, match="Missing required"):
            NextcloudBackend({"nextcloud_url": "https://x"}, nextcloud_components)

    def test_missing_storage_component(self, nextcloud_settings):
        with pytest.raises(ValueError, match="'storage' component"):
            NextcloudBackend(nextcloud_settings, {})

    def test_wrong_accounting_type(self, nextcloud_settings):
        with pytest.raises(ValueError, match="accounting_type='limit'"):
            NextcloudBackend(
                nextcloud_settings,
                {"storage": {"accounting_type": "usage"}},
            )


# ------------------------------------------------------------------
# ping / diagnostics
# ------------------------------------------------------------------

class TestPing:
    def test_ping_success(self, nextcloud_backend):
        nextcloud_backend.client.ping.return_value = True
        assert nextcloud_backend.ping() is True
        nextcloud_backend.client.ping.assert_called_once()

    def test_ping_failure(self, nextcloud_backend):
        nextcloud_backend.client.ping.return_value = False
        assert nextcloud_backend.ping() is False

    def test_ping_exception_returns_false(self, nextcloud_backend):
        nextcloud_backend.client.ping.side_effect = Exception("timeout")
        assert nextcloud_backend.ping() is False

    def test_ping_raise_exception(self, nextcloud_backend):
        nextcloud_backend.client.ping.side_effect = Exception("timeout")
        with pytest.raises(Exception):
            nextcloud_backend.ping(raise_exception=True)

    def test_diagnostics_returns_ping_result(self, nextcloud_backend):
        nextcloud_backend.client.ping.return_value = True
        nextcloud_backend.client.list_group_folders.return_value = []
        assert nextcloud_backend.diagnostics() is True


# ------------------------------------------------------------------
# list_components
# ------------------------------------------------------------------

def test_list_components(nextcloud_backend):
    assert nextcloud_backend.list_components() == ["storage"]


# ------------------------------------------------------------------
# create_resource
# ------------------------------------------------------------------

class TestCreateResource:
    def test_creates_group_folder_with_quota(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.create_group_folder.return_value = 7

        result = nextcloud_backend.create_resource(waldur_resource)

        assert result.backend_id == "7"
        nextcloud_backend.client.create_group.assert_called_once_with(GROUP_ID)
        nextcloud_backend.client.create_group_folder.assert_called_once_with(
            f"My Project/My Storage-{RESOURCE_UUID[:8]}"
        )
        nextcloud_backend.client.add_group_to_folder.assert_called_once_with(7, GROUP_ID)
        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(40)
        )
        # Resharing is disabled by default
        nextcloud_backend.client.set_group_permissions.assert_called_once_with(7, GROUP_ID, 15)

    def test_uses_default_quota_when_no_limits(self, nextcloud_backend, waldur_resource):
        waldur_resource.limits = None
        nextcloud_backend.client.create_group_folder.return_value = 8

        nextcloud_backend.create_resource(waldur_resource)

        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            8, _gib_to_bytes(25)
        )

    def test_mount_point_without_project_name(self, nextcloud_backend, waldur_resource):
        waldur_resource.project_name = ""
        nextcloud_backend.client.create_group_folder.return_value = 9

        nextcloud_backend.create_resource(waldur_resource)

        nextcloud_backend.client.create_group_folder.assert_called_once_with(
            f"My Storage-{RESOURCE_UUID[:8]}"
        )

    def test_sanitizes_forbidden_chars_in_mount_point(self, nextcloud_backend, waldur_resource):
        waldur_resource.project_name = "My:Project*"
        waldur_resource.name = 'Storage<Drive>"x"'
        nextcloud_backend.client.create_group_folder.return_value = 13

        nextcloud_backend.create_resource(waldur_resource)

        arg = nextcloud_backend.client.create_group_folder.call_args[0][0]
        assert ":" not in arg
        assert "*" not in arg
        assert "<" not in arg
        assert ">" not in arg
        assert '"' not in arg

    def test_allows_resharing_when_configured(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.allow_resharing = True
        nextcloud_backend.client.create_group_folder.return_value = 7

        nextcloud_backend.create_resource(waldur_resource)

        nextcloud_backend.client.set_group_permissions.assert_not_called()

    def test_recovers_from_orphaned_folder(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.create_group_folder.side_effect = NextcloudGroupFolderError(
            "duplicate"
        )
        nextcloud_backend.client.list_group_folders.return_value = [
            {"id": 12, "mount_point": f"My Project/My Storage-{RESOURCE_UUID[:8]}", "groups": {}}
        ]

        result = nextcloud_backend.create_resource(waldur_resource)

        assert result.backend_id == "12"
        nextcloud_backend.client.add_group_to_folder.assert_called_once_with(12, GROUP_ID)

    def test_raises_when_no_orphan_found(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.create_group_folder.side_effect = NextcloudGroupFolderError(
            "fatal"
        )
        nextcloud_backend.client.list_group_folders.return_value = []

        with pytest.raises(Exception):
            nextcloud_backend.create_resource(waldur_resource)


# ------------------------------------------------------------------
# create_resource_with_id
# ------------------------------------------------------------------

class TestCreateResourceWithId:
    def test_raises_duplicate_when_folder_exists_with_group(
        self, nextcloud_backend, waldur_resource
    ):
        nextcloud_backend.client.get_group_folder.return_value = {
            "groups": {GROUP_ID: 31}
        }
        with pytest.raises(DuplicateResourceError):
            nextcloud_backend.create_resource_with_id(waldur_resource, "7")

    def test_raises_duplicate_when_group_linked_to_different_folder(
        self, nextcloud_backend, waldur_resource
    ):
        nextcloud_backend.client.get_group_folder.side_effect = NextcloudGroupFolderError("gone")
        nextcloud_backend.client.group_exists.return_value = True
        nextcloud_backend.client.list_group_folders.return_value = [
            {"groups": {GROUP_ID: 31}}
        ]
        with pytest.raises(DuplicateResourceError):
            nextcloud_backend.create_resource_with_id(waldur_resource, "7")

    def test_orphan_recovery_creates_new_folder(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.get_group_folder.side_effect = NextcloudGroupFolderError("gone")
        nextcloud_backend.client.group_exists.return_value = True
        nextcloud_backend.client.list_group_folders.return_value = []
        nextcloud_backend.client.create_group_folder.return_value = 10
        nextcloud_backend.post_create_resource = Mock()

        result = nextcloud_backend.create_resource_with_id(waldur_resource, "7")

        assert result.backend_id == "10"
        nextcloud_backend.post_create_resource.assert_called_once()

    def test_fresh_creation_when_nothing_exists(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.get_group_folder.side_effect = NextcloudGroupFolderError("gone")
        nextcloud_backend.client.group_exists.return_value = False
        nextcloud_backend.client.create_group_folder.return_value = 11
        nextcloud_backend.post_create_resource = Mock()

        result = nextcloud_backend.create_resource_with_id(waldur_resource, "7")

        assert result.backend_id == "11"


# ------------------------------------------------------------------
# delete_resource
# ------------------------------------------------------------------

class TestDeleteResource:
    def test_deletes_folder_and_group(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.delete_resource(waldur_resource)
        nextcloud_backend.client.delete_group_folder.assert_called_once_with(7)
        nextcloud_backend.client.delete_group.assert_called_once_with(GROUP_ID)

    def test_skips_when_backend_id_empty(self, nextcloud_backend, waldur_resource):
        waldur_resource.backend_id = ""
        nextcloud_backend.delete_resource(waldur_resource)
        nextcloud_backend.client.delete_group_folder.assert_not_called()


# ------------------------------------------------------------------
# _pull_backend_resource
# ------------------------------------------------------------------

class TestPullBackendResource:
    def test_returns_info_when_folder_found(self, nextcloud_backend):
        nextcloud_backend.client.get_group_folder.return_value = {
            "quota": _gib_to_bytes(40),
            "size": _gib_to_bytes(3),
            "groups": {},
        }
        nextcloud_backend.client.list_group_members.return_value = []
        result = nextcloud_backend._pull_backend_resource("7")
        assert result is not None
        assert result.backend_id == "7"
        assert result.limits == {"storage": 40.0}
        assert result.usage["TOTAL_ACCOUNT_USAGE"]["storage"] == 3.0
        assert result.users == []

    def test_returns_users_from_group_members(self, nextcloud_backend):
        nextcloud_backend.client.get_group_folder.return_value = {
            "quota": _gib_to_bytes(40),
            "size": 0,
            "groups": {GROUP_ID: 15, "other-group": 31},
        }
        nextcloud_backend.client.list_group_members.side_effect = [
            ["alice", "bob"],
            ["charlie"],
        ]
        result = nextcloud_backend._pull_backend_resource("7")
        assert set(result.users) == {"alice", "bob", "charlie"}
        assert nextcloud_backend.client.list_group_members.call_count == 2

    def test_returns_none_when_not_found(self, nextcloud_backend):
        nextcloud_backend.client.get_group_folder.side_effect = NextcloudGroupFolderError("not found")
        assert nextcloud_backend._pull_backend_resource("99") is None


# ------------------------------------------------------------------
# _get_usage_report
# ------------------------------------------------------------------

class TestUsageReport:
    def test_returns_usage_in_gb(self, nextcloud_backend):
        nextcloud_backend.client.get_folder_usage.return_value = _gib_to_bytes(5)
        result = nextcloud_backend._get_usage_report(["7"])
        assert result["7"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 5.0

    def test_omits_resource_on_error(self, nextcloud_backend):
        nextcloud_backend.client.get_folder_usage.side_effect = Exception("error")
        result = nextcloud_backend._get_usage_report(["7"])
        assert "7" not in result

    def test_handles_multiple_folders(self, nextcloud_backend):
        nextcloud_backend.client.get_folder_usage.side_effect = [
            _gib_to_bytes(2),
            _gib_to_bytes(10),
        ]
        result = nextcloud_backend._get_usage_report(["7", "8"])
        assert result["7"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 2.0
        assert result["8"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 10.0

    def test_omits_resource_when_nextcloud_is_unreachable(
        self, nextcloud_settings, nextcloud_components
    ):
        """A transient Nextcloud failure must not be reported as zero usage.

        The other tests in this class mock the client, so they never exercise
        the real error path.  This one wires up a real NextcloudClient with a
        failing transport: if usage lookup silently degrades to 0, we would
        overwrite the month's real usage with 0 GiB, and the anomaly guard
        cannot catch it because supports_decreasing_usage is True.
        """
        with patch("waldur_site_agent_nextcloud.backend.NextcloudClient"):
            backend = NextcloudBackend(nextcloud_settings, nextcloud_components)
        backend.client = NextcloudClient(
            nextcloud_url=nextcloud_settings["nextcloud_url"],
            admin_username=nextcloud_settings["admin_username"],
            admin_password=nextcloud_settings["admin_password"],
        )
        backend.client.session = Mock()
        backend.client.session.request.side_effect = httpx.ConnectError("connection refused")

        result = backend._get_usage_report(["7"])

        assert result == {}, "folder must be omitted, not reported as 0 usage"


# ------------------------------------------------------------------
# Limits
# ------------------------------------------------------------------

class TestResourceLimits:
    def test_collect_limits_from_waldur_resource(self, nextcloud_backend, waldur_resource):
        backend_limits, waldur_limits = nextcloud_backend._collect_resource_limits(
            waldur_resource
        )
        assert backend_limits == {"storage": 40}
        assert waldur_limits == {"storage": 40}

    def test_collect_limits_uses_default_when_none(self, nextcloud_backend, waldur_resource):
        waldur_resource.limits = None
        backend_limits, _ = nextcloud_backend._collect_resource_limits(waldur_resource)
        assert backend_limits == {"storage": 25}

    def test_set_resource_limits(self, nextcloud_backend):
        nextcloud_backend.set_resource_limits("7", {"storage": 50})
        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(50)
        )

    def test_get_resource_limits(self, nextcloud_backend):
        nextcloud_backend.client.get_resource_limits.return_value = {"storage": 40.0}
        assert nextcloud_backend.get_resource_limits("7") == {"storage": 40.0}


# ------------------------------------------------------------------
# Lifecycle: downscale / pause / restore
# ------------------------------------------------------------------

class TestLifecycle:
    def test_downscale_sets_quota_to_1gb(self, nextcloud_backend):
        assert nextcloud_backend.downscale_resource("7") is True
        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(1)
        )

    def test_pause_sets_quota_to_zero(self, nextcloud_backend):
        assert nextcloud_backend.pause_resource("7") is True
        nextcloud_backend.client.set_folder_quota.assert_called_once_with(7, 0)

    def test_restore_is_noop_returns_false(self, nextcloud_backend):
        assert nextcloud_backend.restore_resource("7") is False
        nextcloud_backend.client.set_folder_quota.assert_not_called()


# ------------------------------------------------------------------
# sync_resource_limits
# ------------------------------------------------------------------

class TestSyncResourceLimits:
    def test_skips_when_paused(self, nextcloud_backend, waldur_resource):
        waldur_resource.paused = True
        waldur_resource.downscaled = False
        nextcloud_backend.sync_resource_limits(waldur_resource, Mock())
        nextcloud_backend.client.set_folder_quota.assert_not_called()

    def test_skips_when_downscaled(self, nextcloud_backend, waldur_resource):
        waldur_resource.paused = False
        waldur_resource.downscaled = True
        nextcloud_backend.sync_resource_limits(waldur_resource, Mock())
        nextcloud_backend.client.set_folder_quota.assert_not_called()

    def test_pushes_waldur_limits_when_active(self, nextcloud_backend, waldur_resource):
        waldur_resource.paused = False
        waldur_resource.downscaled = False
        waldur_resource.limits = {"storage": 40}

        nextcloud_backend.sync_resource_limits(waldur_resource, Mock())

        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(40)
        )

    def test_restores_quota_after_downscale(self, nextcloud_backend, waldur_resource):
        """Restoring a resource: Waldur plan limits are pushed to Nextcloud regardless of current quota."""
        waldur_resource.paused = False
        waldur_resource.downscaled = False
        waldur_resource.limits = {"storage": 50}

        nextcloud_backend.sync_resource_limits(waldur_resource, Mock())

        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(50)
        )

    def test_uses_default_quota_when_limits_missing(self, nextcloud_backend, waldur_resource):
        waldur_resource.paused = False
        waldur_resource.downscaled = False
        waldur_resource.limits = None

        nextcloud_backend.sync_resource_limits(waldur_resource, Mock())

        nextcloud_backend.client.set_folder_quota.assert_called_once_with(
            7, _gib_to_bytes(nextcloud_backend.default_storage_quota_gb)
        )


# ------------------------------------------------------------------
# User management
# ------------------------------------------------------------------

class TestUserManagement:
    def test_add_users_returns_added_set(self, nextcloud_backend, waldur_resource):
        added = nextcloud_backend.add_users_to_resource(
            waldur_resource, {"alice", "bob"}
        )
        assert added == {"alice", "bob"}
        assert nextcloud_backend.client.add_user_to_group.call_count == 2

    def test_add_users_skips_failed_users(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.add_user_to_group.side_effect = [
            None,
            NextcloudGroupError("user not found"),
        ]
        added = nextcloud_backend.add_users_to_resource(
            waldur_resource, {"alice", "bob"}
        )
        assert len(added) == 1

    def test_remove_users(self, nextcloud_backend, waldur_resource):
        removed = nextcloud_backend.remove_users_from_resource(
            waldur_resource, {"alice"}
        )
        assert removed == ["alice"]
        nextcloud_backend.client.remove_user_from_group.assert_called_once_with(
            "alice", GROUP_ID
        )

    def test_remove_users_logs_error_and_continues(self, nextcloud_backend, waldur_resource):
        nextcloud_backend.client.remove_user_from_group.side_effect = NextcloudGroupError("fail")
        removed = nextcloud_backend.remove_users_from_resource(
            waldur_resource, {"alice"}
        )
        assert removed == []


# ------------------------------------------------------------------
# Metadata
# ------------------------------------------------------------------

class TestMetadata:
    def test_returns_folder_metadata(self, nextcloud_backend):
        nextcloud_backend.client.get_group_folder.return_value = {
            "id": 7,
            "mount_point": "My Project/My Storage",
            "groups": {GROUP_ID: 31},
            "size": _gib_to_bytes(3),
            "quota": _gib_to_bytes(40),
        }
        meta = nextcloud_backend.get_resource_metadata("7")
        assert meta["folder_id"] == 7
        assert meta["mount_point"] == "My Project/My Storage"
        assert GROUP_ID in meta["groups"]

    def test_returns_empty_dict_when_not_found(self, nextcloud_backend):
        nextcloud_backend.client.get_group_folder.side_effect = NextcloudGroupFolderError("gone")
        assert nextcloud_backend.get_resource_metadata("99") == {}
