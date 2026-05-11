"""Tests for partition-aware Slurm user associations.

Covers:
- ``SlurmClient.create_association_with_partitions`` emits the right
  ``sacctmgr add user`` command for 1/N partitions.
- ``DefaultPartition`` is emitted only when the configured value is a
  member of the offering's partition list (defensive skip).
- Partition names are validated.
- ``SlurmBackend.add_user`` picks the right branch based on offering
  partitions, the global ``default_partition`` fallback, and absence.
- No ``sacctmgr modify … partitions=`` call exists anywhere in the
  plugin source (partition reconciliation is out of scope).
"""

from __future__ import annotations

import pathlib
import re
from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient


@pytest.fixture
def client():
    """SlurmClient with mocked low-level execute_command."""
    c = SlurmClient({}, slurm_bin_path="")
    with patch.object(c, "execute_command", return_value=""):
        yield c


def _last_command(c: SlurmClient) -> str:
    return c.executed_commands[-1]


class TestCreateAssociationWithPartitions:
    def test_single_partition(self, client):
        client.create_association_with_partitions(
            "alice", "acct1", ["zen3"], default_account="root"
        )
        cmd = _last_command(client)
        assert "add user alice" in cmd
        assert "account=acct1" in cmd
        assert "DefaultAccount=root" in cmd
        assert "Partitions=zen3" in cmd
        # Real sacctmgr does not accept DefaultPartition= on add user, so
        # the client must never emit it (would result in `Unknown option`
        # and an aborted call).
        assert "DefaultPartition" not in cmd
        assert cmd.rstrip().endswith("Share=parent")

    def test_multiple_partitions_sorted(self, client):
        client.create_association_with_partitions(
            "alice", "acct1", ["zen5", "zen3"], default_account="root"
        )
        cmd = _last_command(client)
        # Alphabetical order, single comma-joined argument.
        assert "Partitions=zen3,zen5" in cmd
        assert "Partitions=zen5,zen3" not in cmd

    def test_empty_partitions_rejected(self, client):
        with pytest.raises(BackendError, match="non-empty"):
            client.create_association_with_partitions(
                "alice", "acct1", [], default_account="root"
            )

    def test_invalid_partition_name_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM partition name"):
            client.create_association_with_partitions(
                "alice", "acct1", ["zen3; rm -rf /"], default_account="root"
            )

    def test_hyphenated_name_accepted(self, client):
        client.create_association_with_partitions(
            "alice", "acct1", ["gpu-a100", "cpu_only"], default_account="root"
        )
        cmd = _last_command(client)
        assert "Partitions=cpu_only,gpu-a100" in cmd


class TestAddUserPartitionResolution:
    """Backend-level resolution: offering > global default > none."""

    def _make_backend(
        self,
        offering_partitions=None,
        default_partition=None,
        enforce_offering_partitions=True,
    ):
        settings = {"default_account": "root"}
        if default_partition is not None:
            settings["default_partition"] = default_partition
        # Production default is False (informational only); existing test cases
        # exercise the enforcement path so default this helper to True. Cases
        # that pin the opt-out default pass enforce_offering_partitions=False
        # explicitly.
        if enforce_offering_partitions:
            settings["enforce_offering_partitions"] = True
        backend = SlurmBackend(settings, {"cpu": {"unit": "minutes"}})
        backend.offering_partitions = list(offering_partitions or [])
        backend.client = MagicMock()
        backend.client.get_association.return_value = None
        return backend

    def _resource(self, backend_id="acct1"):
        r = MagicMock()
        r.backend_id = backend_id
        return r

    def test_offering_partitions_take_precedence(self):
        backend = self._make_backend(
            offering_partitions=["zen3", "zen5"],
            default_partition="legacy",  # global fallback set but unused
        )
        result = backend.add_user(self._resource(), "alice")
        assert result is True
        backend.client.create_association_with_partitions.assert_called_once_with(
            "alice", "acct1", ["zen3", "zen5"], "root",
        )
        backend.client.create_association.assert_not_called()
        backend.client.create_association_with_partition.assert_not_called()

    def test_empty_offering_falls_back_to_default_partition(self):
        backend = self._make_backend(
            offering_partitions=[],
            default_partition="zen-legacy",
        )
        backend.add_user(self._resource(), "alice")
        backend.client.create_association_with_partition.assert_called_once_with(
            "alice", "acct1", "zen-legacy", "root",
        )
        backend.client.create_association_with_partitions.assert_not_called()
        backend.client.create_association.assert_not_called()

    def test_no_partitions_no_default_falls_back_to_unrestricted(self):
        backend = self._make_backend()
        backend.add_user(self._resource(), "alice")
        backend.client.create_association.assert_called_once_with(
            "alice", "acct1", "root",
        )
        backend.client.create_association_with_partitions.assert_not_called()
        backend.client.create_association_with_partition.assert_not_called()

    def test_existing_association_skipped(self):
        backend = self._make_backend(offering_partitions=["zen3"])
        backend.client.get_association.return_value = object()  # truthy
        result = backend.add_user(self._resource(), "alice")
        assert result is True
        backend.client.create_association_with_partitions.assert_not_called()

    def test_opt_out_default_ignores_offering_partitions(self):
        """Offerings populated for informational use (e.g. Open OnDemand) must
        not silently start enforcing partitions when the agent upgrades."""
        backend = self._make_backend(
            offering_partitions=["zen3", "zen5"],
            enforce_offering_partitions=False,
        )
        backend.add_user(self._resource(), "alice")
        backend.client.create_association_with_partitions.assert_not_called()
        backend.client.create_association.assert_called_once_with(
            "alice", "acct1", "root",
        )

    def test_opt_out_with_global_default_partition_uses_legacy_path(self):
        backend = self._make_backend(
            offering_partitions=["zen3", "zen5"],
            default_partition="legacy",
            enforce_offering_partitions=False,
        )
        backend.add_user(self._resource(), "alice")
        backend.client.create_association_with_partition.assert_called_once_with(
            "alice", "acct1", "legacy", "root",
        )
        backend.client.create_association_with_partitions.assert_not_called()


class TestPartitionReconciliationRegression:
    """The plugin must never run ``sacctmgr modify … partitions=``.

    Pins the absence of any reconciliation code that mutates partition
    associations after creation. If a future change adds one, this test
    flags it for explicit review.
    """

    def test_no_modify_partitions_in_source(self):
        plugin_root = pathlib.Path(__file__).resolve().parents[1] / "waldur_site_agent_slurm"
        offenders = []
        # Match any sacctmgr modify path that touches partitions, in either
        # case (Partitions= or partitions=) and on the same line.
        pattern = re.compile(r"modify[^\n]*\bpartitions?\s*=", re.IGNORECASE)
        for path in plugin_root.rglob("*.py"):
            text = path.read_text()
            for lineno, line in enumerate(text.splitlines(), start=1):
                if pattern.search(line):
                    offenders.append(f"{path}:{lineno}: {line.strip()}")
        assert not offenders, (
            "Found sacctmgr modify … partitions=… in source — partition "
            "reconciliation is explicitly out of scope:\n"
            + "\n".join(offenders)
        )
