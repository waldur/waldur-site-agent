"""Tests for QoS-scoped Slurm user associations and account QoS operators.

Covers:
- ``SlurmClient.create_association_with_qos`` emits ``sacctmgr add user …
  [Partitions=p1,p2] QosLevel=q1,q2 [DefaultQOS=d] Share=parent`` — the
  per-association qos_list / def_qos_id grant (slurmdb_assoc_rec_t), which
  SLURM stores as one row per partition each carrying the grant.
- QoS / partition names are validated against injection.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from waldur_site_agent_slurm.client import SlurmClient

from waldur_site_agent.backend.exceptions import BackendError


@pytest.fixture
def client():
    c = SlurmClient({}, slurm_bin_path="")
    with patch.object(c, "execute_command", return_value=""):
        yield c


def _last_command(c: SlurmClient) -> str:
    return c.executed_commands[-1]


class TestCreateAssociationWithQoS:
    def test_qos_grant(self, client):
        client.create_association_with_qos(
            "alice", "acct1", ["normal", "boost"], default_qos="boost"
        )
        cmd = _last_command(client)
        assert "add user alice" in cmd
        assert "account=acct1" in cmd
        assert "QosLevel=normal,boost" in cmd
        assert "DefaultQOS=boost" in cmd
        assert cmd.rstrip().endswith("Share=parent")

    def test_partition_scoped_grant(self, client):
        client.create_association_with_qos(
            "alice", "acct1", ["gp_debug"], partitions=["gpu"], default_account="root"
        )
        cmd = _last_command(client)
        assert "DefaultAccount=root" in cmd
        assert "Partitions=gpu" in cmd
        assert "QosLevel=gp_debug" in cmd

    def test_multi_partition_grant(self, client):
        # QoS composes across partitions: emit a Partitions= CSV so SLURM
        # creates one association row per partition, each carrying the QoS.
        client.create_association_with_qos(
            "alice", "acct1", ["gp_debug"], partitions=["gpu", "cpu"], default_qos="gp_debug"
        )
        cmd = _last_command(client)
        assert "Partitions=cpu,gpu" in cmd  # sorted for determinism
        assert "QosLevel=gp_debug" in cmd
        assert "DefaultQOS=gp_debug" in cmd

    def test_no_default_qos_omits_flag(self, client):
        client.create_association_with_qos("alice", "acct1", ["normal"])
        cmd = _last_command(client)
        assert "QosLevel=normal" in cmd
        assert "DefaultQOS" not in cmd

    def test_empty_qos_rejected(self, client):
        with pytest.raises(BackendError, match="non-empty"):
            client.create_association_with_qos("alice", "acct1", [])

    def test_invalid_qos_name_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM QoS name"):
            client.create_association_with_qos("alice", "acct1", ["normal; rm -rf /"])

    def test_invalid_default_qos_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM QoS name"):
            client.create_association_with_qos("alice", "acct1", ["normal"], default_qos="a b")

    def test_invalid_partition_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM partition name"):
            client.create_association_with_qos("alice", "acct1", ["normal"], partitions=["p;x"])


class TestAccountQoSOperators:
    def test_add_account_qos(self, client):
        client.add_account_qos("acct1", "paused")
        assert "qos+=paused" in _last_command(client)

    def test_set_account_grp_submit_jobs(self, client):
        client.set_account_grp_submit_jobs("acct1", 0)
        cmd = _last_command(client)
        assert "GrpSubmitJobs=0" in cmd
        assert "modify account acct1 set" in cmd

    def test_clear_account_grp_submit_jobs(self, client):
        client.set_account_grp_submit_jobs("acct1", -1)
        assert "GrpSubmitJobs=-1" in _last_command(client)
