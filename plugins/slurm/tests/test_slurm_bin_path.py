"""Tests for SLURM binary path resolution and emulator detection.

Verifies that SlurmClient resolves SLURM commands to the configured
slurm_bin_path and can detect emulator binaries via version output.
"""

from unittest.mock import patch

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.client import SlurmClient


class TestSlurmBinPathResolution:
    """Test that _execute_command resolves SLURM command paths correctly."""

    @pytest.fixture
    def default_client(self):
        """SlurmClient with default slurm_bin_path (/usr/bin)."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", return_value=""):
            yield client

    @pytest.fixture
    def custom_client(self):
        """SlurmClient with custom slurm_bin_path."""
        client = SlurmClient({}, slurm_bin_path="/opt/slurm/bin")
        with patch.object(client, "execute_command", return_value=""):
            yield client

    @pytest.fixture
    def empty_path_client(self):
        """SlurmClient with empty slurm_bin_path (PATH resolution for dev/emulator)."""
        client = SlurmClient({}, slurm_bin_path="")
        with patch.object(client, "execute_command", return_value=""):
            yield client

    def test_default_bin_path(self, default_client):
        """Default slurm_bin_path resolves sacctmgr to /usr/bin/sacctmgr."""
        default_client._execute_command(["list", "account"])
        cmd = default_client.executed_commands[0]
        assert cmd.startswith("/usr/bin/sacctmgr ")

    def test_bin_path_resolves_sacctmgr(self, custom_client):
        """Custom slurm_bin_path resolves sacctmgr correctly."""
        custom_client._execute_command(["list", "account"])
        cmd = custom_client.executed_commands[0]
        assert cmd.startswith("/opt/slurm/bin/sacctmgr ")

    def test_bin_path_resolves_sacct(self, custom_client):
        """Custom slurm_bin_path resolves sacct correctly."""
        custom_client._execute_command(
            ["--allusers"], command_name="sacct", immediate=False, parsable=True
        )
        cmd = custom_client.executed_commands[0]
        assert cmd.startswith("/opt/slurm/bin/sacct ")

    def test_bin_path_resolves_scancel(self, custom_client):
        """Custom slurm_bin_path resolves scancel correctly."""
        custom_client._execute_command(
            ["-A", "acct1", "-f"], command_name="scancel", parsable=False, immediate=False
        )
        cmd = custom_client.executed_commands[0]
        assert cmd.startswith("/opt/slurm/bin/scancel ")

    def test_bin_path_resolves_sinfo(self, custom_client):
        """Custom slurm_bin_path resolves sinfo correctly."""
        custom_client._execute_command(
            ["-V"], command_name="sinfo", parsable=False, immediate=False
        )
        cmd = custom_client.executed_commands[0]
        assert cmd.startswith("/opt/slurm/bin/sinfo ")

    def test_bin_path_does_not_affect_non_slurm(self, default_client):
        """Non-SLURM commands (id) stay as bare command names."""
        default_client._execute_command(
            ["-u", "user1"], command_name="id", parsable=False, immediate=False
        )
        cmd = default_client.executed_commands[0]
        assert cmd.startswith("id ")
        assert "/usr/bin/id" not in cmd

    def test_empty_bin_path_uses_bare_command(self, empty_path_client):
        """Empty slurm_bin_path uses bare command name (PATH resolution)."""
        empty_path_client._execute_command(["list", "account"])
        cmd = empty_path_client.executed_commands[0]
        assert cmd.startswith("sacctmgr ")
        assert "/" not in cmd.split()[0]

    def test_default_slurm_bin_path_value(self):
        """SlurmClient defaults slurm_bin_path to /usr/bin."""
        client = SlurmClient({})
        assert client.slurm_bin_path == "/usr/bin"

    def test_explicit_slurm_bin_path(self):
        """SlurmClient stores the provided slurm_bin_path."""
        client = SlurmClient({}, slurm_bin_path="/opt/slurm/bin")
        assert client.slurm_bin_path == "/opt/slurm/bin"


class TestValidateSlurmBinary:
    """Test emulator detection via sacctmgr --version output."""

    def test_validate_slurm_binary_real(self):
        """Returns True when sacctmgr --version outputs real SLURM version."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", return_value="slurm 24.05.4\n"):
            assert client.validate_slurm_binary() is True

    def test_validate_slurm_binary_real_with_prefix(self):
        """Returns True for version strings like 'slurm-wlm 21.08.5'."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", return_value="slurm-wlm 21.08.5\n"):
            assert client.validate_slurm_binary() is True

    def test_validate_slurm_binary_emulator(self):
        """Returns False when sacctmgr --version outputs unexpected content."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", return_value="emulator v1.0\n"):
            assert client.validate_slurm_binary() is False

    def test_validate_slurm_binary_empty_output(self):
        """Returns False when sacctmgr --version produces empty output."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", return_value=""):
            assert client.validate_slurm_binary() is False

    def test_validate_slurm_binary_error(self):
        """Returns False when sacctmgr --version raises BackendError."""
        client = SlurmClient({})
        with patch.object(client, "execute_command", side_effect=BackendError("not found")):
            assert client.validate_slurm_binary() is False

    def test_validate_uses_resolved_path(self):
        """validate_slurm_binary uses the resolved sacctmgr path."""
        client = SlurmClient({}, slurm_bin_path="/opt/slurm/bin")
        with patch.object(client, "execute_command", return_value="slurm 24.05.4\n") as mock_exec:
            client.validate_slurm_binary()
            called_command = mock_exec.call_args[0][0]
            assert called_command[0] == "/opt/slurm/bin/sacctmgr"
