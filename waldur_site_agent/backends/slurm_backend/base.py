"""Generic client class."""

import subprocess
from typing import List

from waldur_site_agent.backends import logger
from waldur_site_agent.backends.exceptions import (
    BackendError,
)


# TODO: move to the upper level
class BaseClient:
    """Generic cli-client for a backend communication."""

    def execute_command(self, command: List[str]) -> str:
        """Executes command on backend."""
        try:
            logger.debug("Executing command: %s", " ".join(command))
            return subprocess.check_output(command, stderr=subprocess.STDOUT, encoding="utf-8")
        except subprocess.CalledProcessError as e:
            logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            if len(lines) > 0 and lines[0].startswith("Warning: Permanently added"):
                lines = lines[1:]
            stdout = "\n".join(lines)
            raise BackendError(stdout) from e
