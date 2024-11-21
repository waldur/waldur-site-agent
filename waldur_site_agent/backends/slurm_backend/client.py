"""CLI-client for SLURM cluster."""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from waldur_site_agent.backends import base
from waldur_site_agent.backends import utils as backend_utils
from waldur_site_agent.backends.structures import Account, Association

from .parser import SlurmAssociationLine, SlurmReportLine


class SlurmClient(base.BaseClient):
    """This class implements Python client for SLURM.

    See also: https://slurm.schedmd.com/sacctmgr.html
    """

    def __init__(self, slurm_tres: Dict) -> None:
        """Inits SLURM-related data."""
        self.slurm_tres = slurm_tres

    def list_accounts(self) -> List[Account]:
        """Returns a list of accounts in the SLURM cluster."""
        command = ["list", "account"]
        output = self._execute_command(command)
        return [self._parse_account(line) for line in output.splitlines() if "|" in line]

    def list_tres(self) -> List[str]:
        """Returns a list of TRES available in cluster."""
        output = self._execute_command(["list", "tres"])
        tres_list = []
        for line in output.splitlines():
            if "|" not in line:
                continue
            fields = line.split("|")
            component_type = fields[0]
            component_name = fields[1]
            if component_name:
                tres_list.append(f"{component_type}/{component_name}")
            else:
                tres_list.append(component_type)
        return tres_list

    def get_account(self, name: str) -> Account | None:
        """Returns Account object from cluster based on the account name."""
        output = self._execute_command(["show", "account", name])
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_account(lines[0])

    def create_account(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Creates account in the SLURM cluster."""
        parts = [
            "add",
            "account",
            name,
            f'description="{description}"',
            f'organization="{organization}"',
        ]
        if parent_name:
            parts.append(f"parent={parent_name}")
        return self._execute_command(parts)

    def delete_all_users_from_account(self, name: str) -> str:
        """Drop all the users from the account based on the account name."""
        return self._execute_command(["remove", "user", "where", f"account={name}"])

    def account_has_users(self, account: str) -> bool:
        """Checks if the account with the specified name have related users."""
        output = self._execute_command(["show", "association", "where", f"account={account}"])
        items = [self._parse_association(line) for line in output.splitlines() if "|" in line]
        return any(item.user != "" for item in items)

    def delete_account(self, name: str) -> str:
        """Deletes account with the specified name from the SLURM cluster."""
        if self.account_has_users(name):
            self.delete_all_users_from_account(name)

        return self._execute_command(["remove", "account", "where", f"name={name}"])

    def set_resource_limits(self, account: str, limits_dict: Dict[str, int]) -> str | None:
        """Sets the limits for the account with the specified name."""
        limits_str = ",".join([f"{key}={value}" for key, value in limits_dict.items()])
        quota = f"GrpTRESMins={limits_str}"
        return self._execute_command(["modify", "account", account, "set", quota])

    def set_account_qos(self, account: str, qos: str) -> None:
        """Set the specified QoS for the account."""
        self._execute_command(["modify", "account", account, "set", f"qos={qos}"])

    def get_association(self, user: str, account: str) -> Association | None:
        """Returns associations between the user and the account if exists."""
        output = self._execute_command(
            [
                "show",
                "association",
                "where",
                f"user={user}",
                f"account={account}",
            ]
        )
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_association(lines[0])

    def create_association(
        self, username: str, account: str, default_account: Optional[str] = ""
    ) -> str:
        """Creates association between the account and the user in SLURM cluster."""
        return self._execute_command(
            [
                "add",
                "user",
                username,
                f"account={account}",
                f"DefaultAccount={default_account}",
            ]
        )

    def delete_association(self, username: str, account: str) -> str:
        """Deletes association between the account and the user in SLURM cluster."""
        return self._execute_command(
            [
                "remove",
                "user",
                "where",
                f"name={username}",
                "and",
                f"account={account}",
            ]
        )

    def get_usage_report(self, accounts: List[str]) -> List:
        """Generates per-user usage report for the accounts."""
        month_start, month_end = backend_utils.format_current_month()

        args = [
            "--noconvert",
            "--truncate",
            "--allocations",
            "--allusers",
            f"--starttime={month_start}",
            f"--endtime={month_end}",
            f"--accounts={','.join(accounts)}",
            "--format=Account,ReqTRES,Elapsed,User",
        ]
        output = self._execute_command(args, "sacct", immediate=False)
        return [
            SlurmReportLine(line, self.slurm_tres) for line in output.splitlines() if "|" in line
        ]

    def get_resource_limits(self, account: str) -> List[SlurmAssociationLine]:
        """Returns limits for the account."""
        args = [
            "show",
            "association",
            "format=account,GrpTRESMins",
            "where",
            f"accounts={account}",
        ]
        output = self._execute_command(args, immediate=False)
        return [
            SlurmAssociationLine(line, self.slurm_tres)
            for line in output.splitlines()
            if "|" in line
        ]

    def list_account_users(self, account: str) -> List[str]:
        """Returns list of users linked to the account."""
        args = [
            "list",
            "associations",
            "format=account,user",
            "where",
            f"account={account}",
        ]
        output = self._execute_command(args)
        return [
            line.split("|")[1] for line in output.splitlines() if "|" in line and line[-1] != "|"
        ]

    def create_linux_user_homedir(self, username: str, umask: str = "") -> str:
        """Creates homedir for the user in Linux system."""
        return self._execute_command(
            command_name="/sbin/mkhomedir_helper",
            command=[username, umask],
            immediate=False,
            parsable=False,
        )

    def get_current_account_qos(self, account: str) -> str:
        """Returns a name of the current QoS of the account."""
        args = [
            "list",
            "associations",
            "format=account,qos",
            "where",
            f"account={account}",
        ]
        output = self._execute_command(args)
        qos_options = [
            line.split("|")[1] for line in output.splitlines() if "|" in line and line[-1] != "|"
        ]

        return qos_options[0] if len(qos_options) > 0 else ""

    def cancel_active_user_jobs(self, account: str, user: str) -> None:
        """Cancel jobs for the account and user."""
        args = [f"-u={user}", f"-A={account}", "-f"]
        self._execute_command(args, command_name="scancel")

    def list_active_user_jobs(self, account: str, user: str) -> List[str]:
        """List active jobs for the account and user."""
        args = [
            "-a",
            f"--account={account}",
            f"--user={user}",
            "--format=JobID,JobName,Partition,Account,User,State,Elapsed,Timelimit,NodeList",
        ]
        output = self._execute_command(args, command_name="sacct")
        return [line.split("|")[0] for line in output.splitlines() if "|" in line]

    def _parse_account(self, line: str) -> Account:
        parts = line.split("|")
        return Account(
            name=parts[0],
            description=parts[1],
            organization=parts[2],
        )

    def _parse_association(self, line: str) -> Association:
        parts = line.split("|")
        value = parts[9]
        match = re.match(r"cpu=(\d+)", value)
        value_ = int(match.group(1)) if match else 0
        return Association(
            account=parts[1],
            user=parts[2],
            value=value_,
        )

    def _execute_command(
        self,
        command: List[str],
        command_name: str = "sacctmgr",
        immediate: bool = True,
        parsable: bool = True,
    ) -> str:
        """Constructs and executes a command with the given parameters."""
        account_command = [command_name]
        if parsable:
            account_command.extend(["--parsable2", "--noheader"])
        if immediate:
            account_command.append("--immediate")
        account_command.extend(command)
        return self.execute_command(account_command)
