import abc
import logging
import subprocess  # noqa: S404

from .exceptions import SlurmError

logger = logging.getLogger(__name__)


class BaseClient(metaclass=abc.ABCMeta):
    def __init__(self, slurm_deployment_type):
        if slurm_deployment_type == "docker":
            self.command_prefix = ["docker", "exec", "slurmctld"]
        else:
            # TODO come up with a solution for native deployment of SLURM
            self.command_prefix = []

    def execute_command(self, command):
        final_command = self.command_prefix.append(" ".join(command))
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
