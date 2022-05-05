import abc
import subprocess  # noqa: S404

from . import logger
from .exceptions import SlurmError


class BaseClient(metaclass=abc.ABCMeta):
    def __init__(self, slurm_deployment_type):
        if slurm_deployment_type == "docker":
            self.command_prefix = ["docker", "exec", "slurmctld"]
        else:
            self.command_prefix = []

    def execute_command(self, command):
        final_command = self.command_prefix + command
        try:
            logger.debug("Executing command: %s", " ".join(final_command))
            return subprocess.check_output(  # noqa: S603
                final_command, stderr=subprocess.STDOUT, encoding="utf-8"
            )
        except subprocess.CalledProcessError as e:
            logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            if len(lines) > 0 and lines[0].startswith("Warning: Permanently added"):
                lines = lines[1:]
            stdout = "\n".join(lines)
            raise SlurmError(stdout)
