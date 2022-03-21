import abc
import logging
import subprocess  # noqa: S404

from .exceptions import SlurmError
from .structures import Quotas

logger = logging.getLogger(__name__)


class BaseClient(metaclass=abc.ABCMeta):
    def __init__(self, hostname, username="root", use_sudo=False):
        self.hostname = hostname
        self.username = username
        self.use_sudo = use_sudo

    def execute_command(self, command):
        if self.use_sudo:
            account_command = ["sudo"]
        else:
            account_command = []

        account_command.extend(command)
        command_prefix = [
            "docker",
            "exec",
            "-it",
        ]
        command = command_prefix.append(" ".join(account_command))
        try:
            logger.debug("Executing command: %s", " ".join(command))
            return subprocess.check_output(  # noqa: S603
                command, stderr=subprocess.STDOUT, encoding="utf-8"
            )
        except subprocess.CalledProcessError as e:
            logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            if len(lines) > 0 and lines[0].startswith("Warning: Permanently added"):
                lines = lines[1:]
            stdout = "\n".join(lines)
            raise SlurmError(stdout)


class BaseReportLine(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def account(self):
        pass

    @abc.abstractproperty
    def user(self):
        pass

    @property
    def cpu(self):
        return 0

    @property
    def gpu(self):
        return 0

    @property
    def ram(self):
        return 0

    @property
    def duration(self):
        return 0

    @property
    def charge(self):
        return 0

    @property
    def node(self):
        return 0

    def quotas(self):
        return Quotas(
            self.cpu * self.duration,
            self.gpu * self.duration,
            self.ram * self.duration,
        )

    def __str__(self):
        return (
            "ReportLine: User=%s, Account=%s, CPU=%s,"
            " GPU=%s, RAM=%s, Duration=%s, Charge=%s, Node=%s"
        ) % (
            self.user,
            self.account,
            self.cpu,
            self.gpu,
            self.ram,
            self.duration,
            self.charge,
            self.node,
        )
