"""Lustre project quota IDs for project directories (gh-24).

The Lustre project ID of a project directory is the project group's LDAP GID
plus ``project_directory.lustre_quota.project_id_offset``, so a site that also
tags home directories with project IDs from the (overlapping) UID range can
keep the two apart. The same ID must reach both ``lfs setquota -p`` and
``lfs project -p``, or the directory is tagged with an ID that has no quota.
"""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.schemas import (
    LUSTRE_MAX_PROJECT_ID,
    LustreQuotaConfig,
    ProjectDirectoryConfig,
)

from waldur_site_agent.backend.exceptions import BackendError

_CPU_COMPONENTS = {
    "cpu": {
        "measured_unit": "minutes",
        "unit_factor": 1,
        "accounting_type": "limit",
        "label": "CPU",
    }
}

_GID = 10136


def _backend(lustre_quota: Optional[dict[str, Any]] = None) -> SlurmBackend:
    quota: dict[str, Any] = {"mount_point": "/nfs", "block_hardlimit": 2147483648}
    if lustre_quota:
        quota.update(lustre_quota)
    settings: dict[str, Any] = {
        "default_account": "root",
        "customer_prefix": "c-",
        "project_prefix": "p-",
        "allocation_prefix": "a-",
        "project_directory": {
            "enabled": True,
            "base_path": "/nfs/projects",
            "lustre_quota": quota,
        },
    }
    backend = SlurmBackend(settings, _CPU_COMPONENTS)
    backend.client = MagicMock()
    ldap = MagicMock()
    ldap.get_group_gid.return_value = _GID
    backend._ldap_client = ldap
    return backend


def _lfs_project_ids(backend: SlurmBackend) -> dict[str, str]:
    """Map each lfs subcommand that was run to the project ID it was given."""
    ids = {}
    for call in backend.client.execute_command.call_args_list:
        command = call.args[0]
        if command[0] == "lfs":
            ids[command[1]] = command[command.index("-p") + 1]
    return ids


class TestLustreQuotaSchema:
    def test_offset_defaults_to_zero(self):
        assert LustreQuotaConfig().project_id_offset == 0

    def test_offset_accepted(self):
        config = ProjectDirectoryConfig(lustre_quota={"project_id_offset": 1000000000})
        assert config.lustre_quota is not None
        assert config.lustre_quota.project_id_offset == 1000000000

    @pytest.mark.parametrize("offset", [-1, LUSTRE_MAX_PROJECT_ID + 1, "5", True])
    def test_offset_rejected(self, offset):
        with pytest.raises(ValidationError, match="project_id_offset"):
            LustreQuotaConfig(project_id_offset=offset)


class TestLustreProjectId:
    def test_gid_is_the_project_id_by_default(self):
        backend = _backend()

        backend._setup_project_directory("a-proj-0")

        assert _lfs_project_ids(backend) == {"setquota": str(_GID), "project": str(_GID)}

    def test_offset_is_added_to_both_commands(self):
        backend = _backend({"project_id_offset": 1000000000})

        backend._setup_project_directory("a-proj-0")

        assert _lfs_project_ids(backend) == {
            "setquota": "1000010136",
            "project": "1000010136",
        }
        backend._ldap_client.get_group_gid.assert_called_once_with("a-proj-0")

    def test_project_id_past_the_lustre_maximum_is_skipped(self):
        backend = _backend({"project_id_offset": LUSTRE_MAX_PROJECT_ID - _GID + 1})

        backend._setup_project_directory("a-proj-0")

        assert _lfs_project_ids(backend) == {}
        backend.client.execute_command.assert_any_call(["mkdir", "-p", "/nfs/projects/a-proj-0"])

    def test_largest_project_id_is_used(self):
        backend = _backend({"project_id_offset": LUSTRE_MAX_PROJECT_ID - _GID})

        backend._setup_project_directory("a-proj-0")

        expected = str(LUSTRE_MAX_PROJECT_ID)
        assert _lfs_project_ids(backend) == {"setquota": expected, "project": expected}


class TestInvalidOffsetRefusedAtStartup:
    @pytest.mark.parametrize("offset", [-1, LUSTRE_MAX_PROJECT_ID + 1, "1000000000", True, 1.5])
    def test_invalid_offset_raises(self, offset):
        with pytest.raises(BackendError, match="project_id_offset"):
            _backend({"project_id_offset": offset})

    def test_offset_ignored_without_lustre_quota(self):
        backend = SlurmBackend(
            {"default_account": "root", "project_directory": {"enabled": True}},
            _CPU_COMPONENTS,
        )
        assert backend._lustre_project_id_offset == 0
