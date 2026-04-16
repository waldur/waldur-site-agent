"""Tests for homedir quota application and verification."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.quota import (
    HomedirQuotaConfig,
    apply_homedir_quota,
    get_user_homedir,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ConcreteBackend(BaseBackend):
    """Minimal concrete subclass for testing BaseBackend.create_user_homedirs."""

    def ping(self, raise_exception=False):
        return True

    def list_backend_components(self):
        return []

    def list_components(self):
        return []

    def _get_usage_report(self, resource_backend_ids):
        return {}

    def _collect_resource_limits(self, waldur_resource):
        return {}, {}

    def _pre_create_resource(self, *args, **kwargs):
        pass

    def diagnostics(self):
        return {}

    def downscale_resource(self, *args, **kwargs):
        pass

    def get_resource_metadata(self, *args, **kwargs):
        return {}

    def pause_resource(self, *args, **kwargs):
        pass

    def restore_resource(self, *args, **kwargs):
        pass


def _make_backend(settings=None, components=None):
    backend = ConcreteBackend(
        backend_settings=settings or {},
        backend_components=components or {},
    )
    backend.client = MagicMock()
    backend.client.create_linux_user_homedir.return_value = ""
    backend.client.execute_command.return_value = ""
    return backend


# ---------------------------------------------------------------------------
# HomedirQuotaConfig validation
# ---------------------------------------------------------------------------


class TestHomedirQuotaConfig:
    def test_valid_ceph_config(self):
        cfg = HomedirQuotaConfig(
            provider="ceph_xattr", max_bytes="1099511627776", max_files=100000
        )
        assert cfg.provider == "ceph_xattr"
        assert cfg.max_bytes == "1099511627776"
        assert cfg.max_files == 100000

    def test_valid_xfs_config(self):
        cfg = HomedirQuotaConfig(
            provider="xfs",
            mount_point="/home",
            block_hardlimit="1t",
            inode_hardlimit=100000,
        )
        assert cfg.provider == "xfs"
        assert cfg.mount_point == "/home"

    def test_valid_lustre_config(self):
        cfg = HomedirQuotaConfig(
            provider="lustre",
            mount_point="/home",
            block_softlimit="943718400",
            block_hardlimit="1048576000",
        )
        assert cfg.provider == "lustre"

    def test_invalid_provider_rejected(self):
        with pytest.raises(ValueError, match="Unsupported quota provider"):
            HomedirQuotaConfig(provider="zfs")

    def test_extra_fields_rejected(self):
        with pytest.raises(ValueError):
            HomedirQuotaConfig(provider="ceph_xattr", unknown_field="value")


# ---------------------------------------------------------------------------
# get_user_homedir
# ---------------------------------------------------------------------------


class TestGetUserHomedir:
    def test_with_base_path(self):
        assert get_user_homedir("alice", "/cephfs/home") == "/cephfs/home/alice"

    def test_with_trailing_slash(self):
        assert get_user_homedir("alice", "/cephfs/home/") == "/cephfs/home/alice"

    def test_without_base_path_uses_passwd(self, monkeypatch):
        import pwd

        fake_entry = MagicMock()
        fake_entry.pw_dir = "/home/alice"
        monkeypatch.setattr(pwd, "getpwnam", lambda _: fake_entry)
        assert get_user_homedir("alice") == "/home/alice"


# ---------------------------------------------------------------------------
# CephFS xattr provider
# ---------------------------------------------------------------------------


class TestCephXattrQuota:
    def test_sets_max_bytes_and_verifies(self):
        client = MagicMock()
        client.execute_command.return_value = "1099511627776"
        config = HomedirQuotaConfig(
            provider="ceph_xattr", max_bytes="1099511627776"
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)

        calls = client.execute_command.call_args_list
        assert calls[0] == call(
            ["setfattr", "-n", "ceph.quota.max_bytes", "-v", "1099511627776", "/home/alice"]
        )
        assert calls[1] == call(
            ["getfattr", "--only-values", "-n", "ceph.quota.max_bytes", "/home/alice"]
        )

    def test_sets_max_files_and_verifies(self):
        client = MagicMock()
        client.execute_command.return_value = "100000"
        config = HomedirQuotaConfig(
            provider="ceph_xattr", max_files=100000
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)

        calls = client.execute_command.call_args_list
        assert calls[0] == call(
            ["setfattr", "-n", "ceph.quota.max_files", "-v", "100000", "/home/alice"]
        )
        assert calls[1] == call(
            ["getfattr", "--only-values", "-n", "ceph.quota.max_files", "/home/alice"]
        )

    def test_sets_both_bytes_and_files(self):
        client = MagicMock()
        client.execute_command.return_value = "matching"
        config = HomedirQuotaConfig(
            provider="ceph_xattr", max_bytes="1T", max_files=50000
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)
        # set + verify for bytes, set + verify for files = 4 calls
        assert client.execute_command.call_count == 4

    def test_verification_failure_logs_warning(self, caplog):
        client = MagicMock()
        # Return mismatched value on verify
        client.execute_command.side_effect = ["", "999"]
        config = HomedirQuotaConfig(
            provider="ceph_xattr", max_bytes="1099511627776"
        )
        # Should not raise — just warn
        apply_homedir_quota(client, "alice", "/home/alice", config)

    def test_verification_error_logs_warning(self):
        client = MagicMock()
        client.execute_command.side_effect = ["", BackendError("getfattr failed")]
        config = HomedirQuotaConfig(
            provider="ceph_xattr", max_bytes="1T"
        )
        # Should not raise
        apply_homedir_quota(client, "alice", "/home/alice", config)

    def test_no_limits_configured_does_nothing(self):
        client = MagicMock()
        config = HomedirQuotaConfig(provider="ceph_xattr")
        apply_homedir_quota(client, "alice", "/home/alice", config)
        client.execute_command.assert_not_called()


# ---------------------------------------------------------------------------
# XFS user quota provider
# ---------------------------------------------------------------------------


class TestXfsQuota:
    def test_sets_all_limits(self):
        client = MagicMock()
        client.execute_command.return_value = ""
        config = HomedirQuotaConfig(
            provider="xfs",
            mount_point="/home",
            block_softlimit="900g",
            block_hardlimit="1t",
            inode_softlimit=90000,
            inode_hardlimit=100000,
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)

        set_call = client.execute_command.call_args_list[0]
        assert set_call == call(
            [
                "xfs_quota",
                "-x",
                "-c",
                "limit -u bsoft=900g bhard=1t isoft=90000 ihard=100000 alice",
                "/home",
            ]
        )

    def test_partial_limits(self):
        client = MagicMock()
        client.execute_command.return_value = ""
        config = HomedirQuotaConfig(
            provider="xfs",
            mount_point="/home",
            block_hardlimit="1t",
        )
        apply_homedir_quota(client, "bob", "/home/bob", config)

        set_call = client.execute_command.call_args_list[0]
        assert set_call == call(
            ["xfs_quota", "-x", "-c", "limit -u bhard=1t bob", "/home"]
        )

    def test_missing_mount_point_skips(self):
        client = MagicMock()
        config = HomedirQuotaConfig(
            provider="xfs", block_hardlimit="1t"
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)
        client.execute_command.assert_not_called()

    def test_no_limits_skips(self):
        client = MagicMock()
        config = HomedirQuotaConfig(provider="xfs", mount_point="/home")
        apply_homedir_quota(client, "alice", "/home/alice", config)
        client.execute_command.assert_not_called()

    def test_verification_runs_after_set(self):
        client = MagicMock()
        client.execute_command.return_value = ""
        config = HomedirQuotaConfig(
            provider="xfs", mount_point="/home", block_hardlimit="1t"
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)
        # set + verify = 2 calls
        assert client.execute_command.call_count == 2
        verify_call = client.execute_command.call_args_list[1]
        assert verify_call == call(
            ["xfs_quota", "-x", "-c", "quota -u -N -b -h alice", "/home"]
        )


# ---------------------------------------------------------------------------
# Lustre user quota provider
# ---------------------------------------------------------------------------


class TestLustreQuota:
    def test_sets_all_limits(self):
        client = MagicMock()
        client.execute_command.return_value = ""
        config = HomedirQuotaConfig(
            provider="lustre",
            mount_point="/home",
            block_softlimit="943718400",
            block_hardlimit="1048576000",
            inode_softlimit=90000,
            inode_hardlimit=100000,
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)

        set_call = client.execute_command.call_args_list[0]
        assert set_call == call(
            [
                "lfs",
                "setquota",
                "-u",
                "alice",
                "-b",
                "943718400",
                "-B",
                "1048576000",
                "-i",
                "90000",
                "-I",
                "100000",
                "/home",
            ]
        )

    def test_missing_mount_point_skips(self):
        client = MagicMock()
        config = HomedirQuotaConfig(
            provider="lustre", block_hardlimit="1048576000"
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)
        client.execute_command.assert_not_called()

    def test_verification_runs_after_set(self):
        client = MagicMock()
        client.execute_command.return_value = ""
        config = HomedirQuotaConfig(
            provider="lustre",
            mount_point="/home",
            block_hardlimit="1048576000",
        )
        apply_homedir_quota(client, "alice", "/home/alice", config)
        assert client.execute_command.call_count == 2
        verify_call = client.execute_command.call_args_list[1]
        assert verify_call == call(
            ["lfs", "quota", "-u", "alice", "/home"]
        )


# ---------------------------------------------------------------------------
# BaseBackend.create_user_homedirs integration
# ---------------------------------------------------------------------------


class TestCreateUserHomedirsWithQuota:
    def test_no_quota_config_still_creates_homedirs(self):
        backend = _make_backend()
        backend.create_user_homedirs({"alice", "bob"})
        assert backend.client.create_linux_user_homedir.call_count == 2
        # No execute_command calls for quota
        backend.client.execute_command.assert_not_called()

    def test_quota_applied_after_homedir_creation(self, monkeypatch):
        import pwd

        fake_entry = MagicMock()
        fake_entry.pw_dir = "/cephfs/home/alice"
        monkeypatch.setattr(pwd, "getpwnam", lambda _: fake_entry)

        backend = _make_backend(
            settings={
                "homedir_quota": {
                    "provider": "ceph_xattr",
                    "max_bytes": "1099511627776",
                },
            }
        )
        backend.client.execute_command.return_value = "1099511627776"
        backend.create_user_homedirs({"alice"})

        backend.client.create_linux_user_homedir.assert_called_once_with("alice", "0077")
        # setfattr + getfattr
        assert backend.client.execute_command.call_count == 2

    def test_quota_uses_homedir_base_path(self):
        backend = _make_backend(
            settings={
                "homedir_base_path": "/cephfs/home",
                "homedir_quota": {
                    "provider": "ceph_xattr",
                    "max_bytes": "1T",
                },
            }
        )
        backend.client.execute_command.return_value = "1T"
        backend.create_user_homedirs({"alice"})

        set_call = backend.client.execute_command.call_args_list[0]
        assert "/cephfs/home/alice" in set_call[0][0]

    def test_quota_failure_does_not_block_other_users(self):
        backend = _make_backend(
            settings={
                "homedir_base_path": "/home",
                "homedir_quota": {
                    "provider": "ceph_xattr",
                    "max_bytes": "1T",
                },
            }
        )
        # First user's quota fails, second succeeds
        backend.client.execute_command.side_effect = [
            BackendError("setfattr failed"),  # alice set fails
            "",  # bob set ok
            "1T",  # bob verify ok
        ]
        backend.create_user_homedirs({"alice", "bob"})
        # Both homedirs were created
        assert backend.client.create_linux_user_homedir.call_count == 2

    def test_homedir_creation_failure_skips_quota(self):
        backend = _make_backend(
            settings={
                "homedir_base_path": "/home",
                "homedir_quota": {
                    "provider": "ceph_xattr",
                    "max_bytes": "1T",
                },
            }
        )
        backend.client.create_linux_user_homedir.side_effect = BackendError("mkhomedir failed")
        backend.create_user_homedirs({"alice"})
        # Quota commands should NOT be called since homedir creation failed
        backend.client.execute_command.assert_not_called()

    def test_invalid_quota_config_is_ignored(self):
        backend = _make_backend(
            settings={
                "homedir_quota": {
                    "provider": "invalid_provider",
                },
            }
        )
        backend.create_user_homedirs({"alice"})
        backend.client.create_linux_user_homedir.assert_called_once()
        backend.client.execute_command.assert_not_called()
