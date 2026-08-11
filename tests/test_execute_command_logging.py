"""Tests for BaseClient.execute_command() error surfacing."""

import subprocess
from unittest.mock import patch

import pytest

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError


class TestExecuteCommandErrorLogging:
    """execute_command must surface the backend's own error text, not just the exit code."""

    def test_backend_error_message_contains_command_output(self):
        client = BaseClient()
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["sacctmgr", "modify", "account"],
            output="sacctmgr: error: Parent account 'p-test990' doesn't exist\n",
        )
        with patch("subprocess.check_output", side_effect=error):
            with pytest.raises(BackendError) as exc_info:
                client.execute_command(["sacctmgr", "modify", "account"])

        assert "Parent account 'p-test990' doesn't exist" in str(exc_info.value)

    def test_logs_actual_command_output_not_just_exit_status(self):
        client = BaseClient()
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["sacctmgr", "modify", "account"],
            output="sacctmgr: error: Parent account 'p-test990' doesn't exist\n",
        )
        with patch("subprocess.check_output", side_effect=error):
            with patch("waldur_site_agent.backend.clients.logger") as mock_logger:
                with pytest.raises(BackendError):
                    client.execute_command(["sacctmgr", "modify", "account"])

        # The log call must carry the real sacctmgr output, not just "%s" the
        # command — otherwise the operator only ever sees "exit status 1".
        logged_args = mock_logger.exception.call_args.args
        assert any("Parent account 'p-test990' doesn't exist" in str(arg) for arg in logged_args)

    def test_silent_suppresses_logging_but_still_raises_with_output(self):
        client = BaseClient()
        error = subprocess.CalledProcessError(
            returncode=1, cmd=["sacctmgr"], output="some backend error\n"
        )
        with patch("subprocess.check_output", side_effect=error):
            with patch("waldur_site_agent.backend.clients.logger") as mock_logger:
                with pytest.raises(BackendError) as exc_info:
                    client.execute_command(["sacctmgr"], silent=True)

        mock_logger.exception.assert_not_called()
        assert "some backend error" in str(exc_info.value)
