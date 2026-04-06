"""Tests for SLURM command construction and flag validation.

Verifies that each SlurmClient method generates the correct command prefix
(flags like --parsable2, --noheader, --immediate) and that invalid flag
combinations are rejected at construction time.
"""

from unittest.mock import patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError
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
        client._execute_command(["-u", "user1"], command_name="id", parsable=False, immediate=False)
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


class TestModifyNothingChanged:
    """sacctmgr returns exit-code 1 with 'Nothing modified' when a modify
    command sets values that already match.  This must be treated as a no-op."""

    @pytest.fixture
    def client(self):
        client = SlurmClient({}, slurm_bin_path="")
        return client

    def test_modify_nothing_modified_is_suppressed(self, client):
        """'Nothing modified' on a modify command should not raise."""
        with patch.object(
            client,
            "execute_command",
            side_effect=BackendError("sacctmgr: Request didn't affect anything\n Nothing modified"),
        ):
            result = client.set_resource_limits("acct1", {"node": 0})
        assert result == ""

    def test_modify_real_error_still_raises(self, client):
        """Other errors on a modify command must still propagate."""
        with patch.object(
            client,
            "execute_command",
            side_effect=BackendError("sacctmgr: error: some real error"),
        ):
            with pytest.raises(BackendError, match="some real error"):
                client.set_resource_limits("acct1", {"node": 0})

    def test_non_modify_error_still_raises(self, client):
        """Errors on non-modify commands must propagate regardless of message."""
        with patch.object(
            client,
            "execute_command",
            side_effect=BackendError("Nothing modified"),
        ):
            with pytest.raises(BackendError):
                client.list_resources()


class TestClusterFiltering:
    """Verify cluster=<name> is injected into commands when cluster_name is set."""

    @pytest.fixture
    def client_with_cluster(self):
        """SlurmClient with cluster_name set and mocked execute_command."""
        client = SlurmClient({"cpu": "TRES_CPU"}, slurm_bin_path="", cluster_name="mycluster")
        with patch.object(client, "execute_command", return_value=""):
            yield client

    @pytest.fixture
    def client_without_cluster(self):
        """SlurmClient without cluster_name and mocked execute_command."""
        client = SlurmClient({"cpu": "TRES_CPU"}, slurm_bin_path="")
        with patch.object(client, "execute_command", return_value=""):
            yield client

    # --- sacctmgr account/association commands get cluster= ---

    def test_list_resources_includes_cluster(self, client_with_cluster):
        client_with_cluster.list_resources()
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_get_resource_includes_cluster(self, client_with_cluster):
        client_with_cluster.get_resource("acct1")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_create_resource_includes_cluster(self, client_with_cluster):
        client_with_cluster.create_resource("acct1", "desc", "org")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_delete_resource_includes_cluster(self, client_with_cluster):
        client_with_cluster.delete_resource("acct1")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_create_association_includes_cluster(self, client_with_cluster):
        client_with_cluster.create_association("user1", "acct1")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_delete_association_includes_cluster(self, client_with_cluster):
        client_with_cluster.delete_association("user1", "acct1")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    def test_show_association_includes_cluster(self, client_with_cluster):
        client_with_cluster.account_has_users("acct1")
        assert "cluster=mycluster" in client_with_cluster.executed_commands[0]

    # --- modify commands: cluster= goes before "set" ---

    def test_set_resource_limits_cluster_before_set(self, client_with_cluster):
        client_with_cluster.set_resource_limits("acct1", {"cpu": 100})
        cmd = client_with_cluster.executed_commands[0]
        assert "cluster=mycluster" in cmd
        cluster_pos = cmd.index("cluster=mycluster")
        set_pos = cmd.index(" set ")
        assert cluster_pos < set_pos

    def test_set_account_fairshare_cluster_before_set(self, client_with_cluster):
        client_with_cluster.set_account_fairshare("acct1", 500)
        cmd = client_with_cluster.executed_commands[0]
        assert "cluster=mycluster" in cmd
        cluster_pos = cmd.index("cluster=mycluster")
        set_pos = cmd.index(" set ")
        assert cluster_pos < set_pos

    def test_modify_account_qos_cluster_before_set(self, client_with_cluster):
        client_with_cluster.set_account_qos("acct1", "normal")
        cmd = client_with_cluster.executed_commands[0]
        assert "cluster=mycluster" in cmd
        cluster_pos = cmd.index("cluster=mycluster")
        set_pos = cmd.index(" set ")
        assert cluster_pos < set_pos

    # --- QoS and TRES commands do NOT get cluster= ---

    def test_list_tres_no_cluster(self, client_with_cluster):
        client_with_cluster.list_tres()
        assert "cluster=mycluster" not in client_with_cluster.executed_commands[0]

    def test_qos_exists_no_cluster(self, client_with_cluster):
        client_with_cluster.qos_exists("normal")
        assert "cluster=mycluster" not in client_with_cluster.executed_commands[0]

    def test_create_qos_no_cluster(self, client_with_cluster):
        client_with_cluster.create_qos("normal")
        for cmd in client_with_cluster.executed_commands:
            assert "cluster=mycluster" not in cmd

    def test_delete_qos_no_cluster(self, client_with_cluster):
        client_with_cluster.delete_qos("normal")
        assert "cluster=mycluster" not in client_with_cluster.executed_commands[0]

    # --- sacct/scancel get --cluster= flag ---

    def test_get_usage_report_includes_cluster(self, client_with_cluster):
        client_with_cluster.get_usage_report(["acct1"])
        cmd = client_with_cluster.executed_commands[0]
        assert "--cluster=mycluster" in cmd

    def test_cancel_active_user_jobs_includes_cluster(self, client_with_cluster):
        client_with_cluster.cancel_active_user_jobs("acct1")
        cmd = client_with_cluster.executed_commands[0]
        assert "--cluster=mycluster" in cmd

    def test_list_active_user_jobs_includes_cluster(self, client_with_cluster):
        client_with_cluster.list_active_user_jobs("acct1", "user1")
        cmd = client_with_cluster.executed_commands[0]
        assert "--cluster=mycluster" in cmd

    # --- list_clusters itself does NOT get cluster= ---

    def test_list_clusters_no_cluster_filter(self, client_with_cluster):
        client_with_cluster.list_clusters()
        assert "cluster=mycluster" not in client_with_cluster.executed_commands[0]

    # --- No cluster_name → no injection ---

    def test_no_cluster_name_no_injection(self, client_without_cluster):
        client_without_cluster.list_resources()
        assert "cluster=" not in client_without_cluster.executed_commands[0]

    def test_no_cluster_name_sacct_no_injection(self, client_without_cluster):
        client_without_cluster.get_usage_report(["acct1"])
        assert "--cluster=" not in client_without_cluster.executed_commands[0]


# ---- Emulator integration tests (require slurm-emulator >= 0.3.0) ----

try:
    from emulator.commands.sacctmgr import SacctmgrEmulator
    from emulator.commands.sacct import SacctEmulator
    from emulator.core.database import SlurmDatabase
    from emulator.core.time_engine import TimeEngine

    EMULATOR_AVAILABLE = True
except ImportError:
    EMULATOR_AVAILABLE = False


@pytest.mark.skipif(not EMULATOR_AVAILABLE, reason="slurm-emulator not installed")
class TestClusterFilteringWithEmulator:
    """Integration tests: SlurmClient cluster filtering against the emulator."""

    @pytest.fixture
    def emulator_env(self):
        """Set up emulator with two clusters and a test account."""
        db = SlurmDatabase()
        db.add_cluster("prod")
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)
        sacct = SacctEmulator(db, te)

        # Create account on prod cluster
        sacctmgr.handle_command(["add", "account", "testacct", "cluster=prod"])

        def _route_to_emulator(command_parts, silent=False):
            """Route a full command list (with binary prefix) to the emulator."""
            # Determine which emulator to use from the binary name
            binary = command_parts[0].rsplit("/", 1)[-1]
            # Strip --parsable2, --noheader, --immediate flags
            args = [
                a
                for a in command_parts[1:]
                if a not in ("--parsable2", "--noheader", "--immediate")
            ]
            if binary == "sacctmgr":
                return sacctmgr.handle_command(args)
            if binary == "sacct":
                return sacct.handle_command(args)
            if binary == "sinfo":
                return "slurm-emulator 0.2.0"
            return ""

        return db, _route_to_emulator

    def test_list_clusters(self, emulator_env):
        """list_clusters returns cluster names from emulator."""
        db, route = emulator_env
        client = SlurmClient({"cpu": "CPU"}, slurm_bin_path="", cluster_name="prod")
        with patch.object(client, "execute_command", side_effect=route):
            clusters = client.list_clusters()
        assert "default" in clusters
        assert "prod" in clusters

    def test_create_account_on_cluster(self, emulator_env):
        """Creating an account with cluster_name routes to the correct cluster."""
        db, route = emulator_env
        client = SlurmClient({"cpu": "CPU"}, slurm_bin_path="", cluster_name="prod")
        with patch.object(client, "execute_command", side_effect=route):
            client.create_resource("newacct", "desc", "org")
        # Account exists globally
        assert db.get_account("newacct") is not None
        # Association exists on prod cluster
        assoc_key = db._association_key("", "newacct", "prod")
        assert assoc_key in db.associations

    def test_add_user_association_on_cluster(self, emulator_env):
        """Creating a user association routes to the correct cluster."""
        db, route = emulator_env
        client = SlurmClient({"cpu": "CPU"}, slurm_bin_path="", cluster_name="prod")
        with patch.object(client, "execute_command", side_effect=route):
            client.create_association("testuser", "testacct")
        assoc_key = db._association_key("testuser", "testacct", "prod")
        assert assoc_key in db.associations

    def test_cluster_name_validated_in_diagnostics(self, emulator_env):
        """SlurmBackend.diagnostics validates cluster existence."""
        from waldur_site_agent_slurm.backend import SlurmBackend

        _, route = emulator_env
        settings = {
            "default_account": "root",
            "customer_prefix": "c_",
            "project_prefix": "p_",
            "allocation_prefix": "a_",
            "cluster_name": "prod",
        }
        components = {
            "cpu": {
                "limit": 10,
                "measured_unit": "k-Hours",
                "unit_factor": 60000,
                "accounting_type": "usage",
                "label": "CPU",
            }
        }
        backend = SlurmBackend(settings, components)
        with patch.object(backend.client, "execute_command", side_effect=route):
            assert backend.diagnostics() is True

    def test_invalid_cluster_name_fails_diagnostics(self, emulator_env):
        """SlurmBackend.diagnostics fails for non-existent cluster."""
        from waldur_site_agent_slurm.backend import SlurmBackend

        _, route = emulator_env
        settings = {
            "default_account": "root",
            "customer_prefix": "c_",
            "project_prefix": "p_",
            "allocation_prefix": "a_",
            "cluster_name": "nonexistent",
        }
        components = {
            "cpu": {
                "limit": 10,
                "measured_unit": "k-Hours",
                "unit_factor": 60000,
                "accounting_type": "usage",
                "label": "CPU",
            }
        }
        backend = SlurmBackend(settings, components)
        with patch.object(backend.client, "execute_command", side_effect=route):
            assert backend.diagnostics() is False

    def test_create_qos_with_flags(self, emulator_env):
        """create_qos with flags must pass 'set' and 'flags=...' as separate args.

        Regression test for WAL-9816: the flags argument was passed as a single
        string "set flags=DenyOnLimit,NoDecay" instead of two separate elements
        ["set", "flags=DenyOnLimit,NoDecay"], causing sacctmgr to reject it with
        "Unknown option".
        """
        db, route = emulator_env
        client = SlurmClient({"cpu": "CPU"}, slurm_bin_path="")
        with patch.object(client, "execute_command", side_effect=route):
            client.create_qos("test_qos", flags="DenyOnLimit,NoDecay")

        # Verify QOS was actually created in the emulator database
        assert "test_qos" in db.qos_list
        assert db.qos_list["test_qos"].flags == "DenyOnLimit,NoDecay"

    def test_create_qos_with_flags_and_modify(self, emulator_env):
        """create_qos with flags and additional settings works end-to-end."""
        db, route = emulator_env
        client = SlurmClient({"cpu": "CPU"}, slurm_bin_path="")
        with patch.object(client, "execute_command", side_effect=route):
            client.create_qos(
                "full_qos",
                flags="DenyOnLimit,NoDecay",
                grp_tres="cpu=100",
                max_jobs=50,
            )

        assert "full_qos" in db.qos_list
        qos = db.qos_list["full_qos"]
        assert qos.flags == "DenyOnLimit,NoDecay"
        assert qos.grp_tres == "cpu=100"
        assert qos.max_jobs == 50
