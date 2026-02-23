"""Tests for SLURM command construction and flag validation.

Verifies that each SlurmClient method generates the correct command prefix
(flags like --parsable2, --noheader, --immediate) and that invalid flag
combinations are rejected at construction time.
"""

from unittest.mock import patch

import pytest
from waldur_site_agent_slurm.client import SlurmClient


class TestFlagValidationGuard:
    """Test that _execute_command rejects invalid flag+command combinations."""

    @pytest.fixture
    def client(self):
        """SlurmClient with mocked base execute_command."""
        client = SlurmClient({}, slurm_bin_path="")
        with patch.object(client, "execute_command", return_value=""):
            yield client

    def test_sacctmgr_with_immediate_allowed(self, client):
        """sacctmgr supports --immediate."""
        client._execute_command(["list", "account"], command_name="sacctmgr", immediate=True)
        assert "--immediate" in client.executed_commands[0]

    def test_sacctmgr_with_parsable_allowed(self, client):
        """sacctmgr supports --parsable2 and --noheader."""
        client._execute_command(["list", "account"], command_name="sacctmgr", parsable=True)
        assert "--parsable2" in client.executed_commands[0]
        assert "--noheader" in client.executed_commands[0]

    def test_sacct_with_immediate_raises(self, client):
        """sacct does NOT support --immediate."""
        with pytest.raises(ValueError, match="--immediate is not supported by sacct"):
            client._execute_command(["--allusers"], command_name="sacct", immediate=True)

    def test_sacct_without_immediate_allowed(self, client):
        """sacct works when immediate=False."""
        client._execute_command(
            ["--allusers"], command_name="sacct", immediate=False, parsable=True
        )
        cmd = client.executed_commands[0]
        assert cmd.startswith("sacct --parsable2 --noheader")
        assert "--immediate" not in cmd

    def test_scancel_with_immediate_raises(self, client):
        """scancel does NOT support --immediate."""
        with pytest.raises(ValueError, match="--immediate is not supported by scancel"):
            client._execute_command(["-A", "acct1"], command_name="scancel", immediate=True)

    def test_scancel_with_parsable_raises(self, client):
        """scancel does NOT support --parsable2/--noheader."""
        with pytest.raises(ValueError, match="--parsable2/--noheader are not supported by scancel"):
            client._execute_command(
                ["-A", "acct1"], command_name="scancel", parsable=True, immediate=False
            )

    def test_scancel_without_flags_allowed(self, client):
        """scancel works when both immediate=False and parsable=False."""
        client._execute_command(
            ["-A", "acct1", "-f"], command_name="scancel", parsable=False, immediate=False
        )
        assert client.executed_commands[0] == "scancel -A acct1 -f"

    def test_id_with_immediate_raises(self, client):
        """id does NOT support --immediate."""
        with pytest.raises(ValueError, match="--immediate is not supported by id"):
            client._execute_command(["-u", "user1"], command_name="id", immediate=True)

    def test_id_with_parsable_raises(self, client):
        """id does NOT support --parsable2/--noheader."""
        with pytest.raises(ValueError, match="--parsable2/--noheader are not supported by id"):
            client._execute_command(
                ["-u", "user1"], command_name="id", parsable=True, immediate=False
            )

    def test_id_without_flags_allowed(self, client):
        """id works when both immediate=False and parsable=False."""
        client._execute_command(
            ["-u", "user1"], command_name="id", parsable=False, immediate=False
        )
        assert client.executed_commands[0] == "id -u user1"


class TestCommandPrefixByMethod:
    """Verify each public SlurmClient method generates the correct command prefix.

    This documents and enforces the exact flags for every method, preventing
    regressions like the --immediate on sacct bug (fixed in fb75b284).
    """

    @pytest.fixture
    def client(self):
        """SlurmClient with mocked base execute_command returning typical output."""
        client = SlurmClient(
            {"cpu": "TRES_CPU", "gpu": "TRES_GPU", "ram": "TRES_RAM"},
            slurm_bin_path="",
        )
        with patch.object(client, "execute_command", return_value="") as mock_exec:
            yield client, mock_exec

    def _get_command(self, client, index=0):
        """Get the constructed command string at given index."""
        return client.executed_commands[index]

    # --- sacctmgr commands (with --parsable2 --noheader --immediate) ---

    def test_list_resources(self, client):
        """list_resources uses sacctmgr with all standard flags."""
        c, _ = client
        c.list_resources()
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_list_tres(self, client):
        """list_tres uses sacctmgr with all standard flags."""
        c, _ = client
        c.list_tres()
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_get_resource(self, client):
        """get_resource uses sacctmgr with all standard flags."""
        c, _ = client
        c.get_resource("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_create_resource(self, client):
        """create_resource uses sacctmgr with all standard flags."""
        c, _ = client
        c.create_resource("acct1", "desc", "org")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_delete_all_users_from_account(self, client):
        """delete_all_users_from_account uses sacctmgr with all standard flags."""
        c, _ = client
        c.delete_all_users_from_account("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_account_has_users(self, client):
        """account_has_users uses sacctmgr with all standard flags."""
        c, _ = client
        c.account_has_users("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_delete_resource(self, client):
        """delete_resource uses sacctmgr with all standard flags."""
        c, _ = client
        c.delete_resource("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_set_resource_limits(self, client):
        """set_resource_limits uses sacctmgr with all standard flags."""
        c, _ = client
        c.set_resource_limits("acct1", {"cpu": 100})
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_set_resource_user_limits(self, client):
        """set_resource_user_limits uses sacctmgr with all standard flags."""
        c, _ = client
        c.set_resource_user_limits("acct1", "user1", {"cpu": 100})
        # First call is list_tres (sacctmgr), second is modify user (sacctmgr)
        for cmd in c.executed_commands:
            assert cmd.startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_set_account_qos(self, client):
        """set_account_qos uses sacctmgr with all standard flags."""
        c, _ = client
        c.set_account_qos("acct1", "normal")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_get_association(self, client):
        """get_association uses sacctmgr with all standard flags."""
        c, _ = client
        c.get_association("user1", "acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_create_association(self, client):
        """create_association uses sacctmgr with all standard flags."""
        c, _ = client
        c.create_association("user1", "acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_delete_association(self, client):
        """delete_association uses sacctmgr with all standard flags."""
        c, _ = client
        c.delete_association("user1", "acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_list_resource_users(self, client):
        """list_resource_users uses sacctmgr with all standard flags."""
        c, _ = client
        c.list_resource_users("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_get_current_account_qos(self, client):
        """get_current_account_qos uses sacctmgr with all standard flags."""
        c, _ = client
        c.get_current_account_qos("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_set_account_fairshare(self, client):
        """set_account_fairshare uses sacctmgr with all standard flags."""
        c, _ = client
        c.set_account_fairshare("acct1", 500)
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_set_account_limits(self, client):
        """set_account_limits uses sacctmgr with all standard flags."""
        c, _ = client
        c.set_account_limits("acct1", "GrpTRESMins", {"billing": 72000})
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_reset_raw_usage(self, client):
        """reset_raw_usage uses sacctmgr with all standard flags."""
        c, _ = client
        c.reset_raw_usage("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_get_account_fairshare_method(self, client):
        """get_account_fairshare uses sacctmgr with all standard flags."""
        c, mock_exec = client
        mock_exec.return_value = "acct1|500|\n"
        c.get_account_fairshare("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    def test_get_account_limits_method(self, client):
        """get_account_limits uses sacctmgr with all standard flags."""
        c, mock_exec = client
        mock_exec.return_value = "acct1|cpu=100|billing=72000|cpu=50|billing=36000|\n"
        c.get_account_limits("acct1")
        assert self._get_command(c).startswith("sacctmgr --parsable2 --noheader --immediate")

    # --- sacctmgr commands without --immediate ---

    def test_get_resource_limits(self, client):
        """get_resource_limits uses sacctmgr with --parsable2 --noheader but NOT --immediate."""
        c, _ = client
        c.get_resource_limits("acct1")
        cmd = self._get_command(c)
        assert cmd.startswith("sacctmgr --parsable2 --noheader ")
        assert "--immediate" not in cmd

    def test_get_resource_user_limits(self, client):
        """get_resource_user_limits uses sacctmgr with --parsable2 --noheader but NOT --immediate."""
        c, _ = client
        c.get_resource_user_limits("acct1")
        cmd = self._get_command(c)
        assert cmd.startswith("sacctmgr --parsable2 --noheader ")
        assert "--immediate" not in cmd

    # --- sacct commands (--parsable2 --noheader, NO --immediate) ---

    def test_get_usage_report(self, client):
        """get_usage_report uses sacct with --parsable2 --noheader, NO --immediate."""
        c, _ = client
        c.get_usage_report(["acct1"])
        cmd = self._get_command(c)
        assert cmd.startswith("sacct --parsable2 --noheader")
        assert "--immediate" not in cmd

    def test_get_historical_usage_report(self, client):
        """get_historical_usage_report uses sacct with --parsable2 --noheader, NO --immediate."""
        c, _ = client
        c.get_historical_usage_report(["acct1"], 2026, 1)
        cmd = self._get_command(c)
        assert cmd.startswith("sacct --parsable2 --noheader")
        assert "--immediate" not in cmd

    def test_list_active_user_jobs(self, client):
        """list_active_user_jobs uses sacct with --parsable2 --noheader, NO --immediate."""
        c, _ = client
        c.list_active_user_jobs("acct1", "user1")
        cmd = self._get_command(c)
        assert cmd.startswith("sacct --parsable2 --noheader")
        assert "--immediate" not in cmd

    def test_get_current_usage(self, client):
        """get_current_usage uses sacct with NO --immediate."""
        c, _ = client
        c.get_current_usage("acct1")
        cmd = self._get_command(c)
        assert cmd.startswith("sacct")
        assert "--immediate" not in cmd

    # --- scancel commands (NO --parsable2, NO --noheader, NO --immediate) ---

    def test_cancel_active_user_jobs(self, client):
        """cancel_active_user_jobs uses scancel with NO extra flags."""
        c, _ = client
        c.cancel_active_user_jobs("acct1")
        cmd = self._get_command(c)
        assert cmd.startswith("scancel ")
        assert "--parsable2" not in cmd
        assert "--noheader" not in cmd
        assert "--immediate" not in cmd

    def test_cancel_active_user_jobs_with_user(self, client):
        """cancel_active_user_jobs with user still uses scancel with NO extra flags."""
        c, _ = client
        c.cancel_active_user_jobs("acct1", user="user1")
        cmd = self._get_command(c)
        assert cmd.startswith("scancel ")
        assert "--parsable2" not in cmd
        assert "--noheader" not in cmd
        assert "--immediate" not in cmd

    # --- id commands (NO --parsable2, NO --noheader, NO --immediate) ---

    def test_check_user_exists(self, client):
        """check_user_exists uses id with NO extra flags."""
        c, mock_exec = client
        mock_exec.return_value = "1000"
        c.check_user_exists("user1")
        cmd = self._get_command(c)
        assert cmd.startswith("id ")
        assert "--parsable2" not in cmd
        assert "--noheader" not in cmd
        assert "--immediate" not in cmd
