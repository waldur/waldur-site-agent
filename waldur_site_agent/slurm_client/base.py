import abc
import subprocess  # noqa: S404

from . import logger
from .exceptions import ConfigurationError, SlurmError


class BaseClient(metaclass=abc.ABCMeta):
    def __init__(self, slurm_deployment_type, slurm_container_name=None):
        if slurm_deployment_type == "docker":
            if slurm_container_name is None:
                raise ConfigurationError(
                    "Missing name of headnode container for docker-based SLURM"
                )
            self.command_prefix = ["docker", "exec", slurm_container_name]
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
