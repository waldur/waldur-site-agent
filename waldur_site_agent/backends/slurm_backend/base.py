"""Generic client class."""

import subprocess
from typing import List, Optional

from waldur_site_agent.backends.exceptions import (
    BackendError,
    ConfigurationError,
)

from . import logger


# TODO: move to the upper level
class BaseClient:
    """Generic cli-client for a backend communication."""

    def __init__(
        self,
        slurm_deployment_type: str,
        slurm_container_name: Optional[str] = None,
    ) -> None:
        """Inits SLURM-related data for cli."""
        if slurm_deployment_type == "docker":
            if slurm_container_name is None:
                message = "Missing name of headnode container for docker-based SLURM"
                raise ConfigurationError(message)
            self.command_prefix = ["docker", "exec", slurm_container_name]
        else:
            self.command_prefix = []

    def execute_command(self, command: List[str]) -> str:
        """Executes command on backend."""
        final_command = self.command_prefix + command
        try:
            logger.debug("Executing command: %s", " ".join(final_command))
            return subprocess.check_output(
                final_command, stderr=subprocess.STDOUT, encoding="utf-8"
            )
        except subprocess.CalledProcessError as e:
            logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            if len(lines) > 0 and lines[0].startswith("Warning: Permanently added"):
                lines = lines[1:]
            stdout = "\n".join(lines)
            raise BackendError(stdout) from e
