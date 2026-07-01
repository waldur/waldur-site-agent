"""CLI-client for SLURM cluster."""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.exceptions import (
    BackendError,
)
from waldur_site_agent.backend.structures import Association, ClientResource
from waldur_site_agent_slurm.interface import SlurmClientInterface
from waldur_site_agent_slurm.parser import SlurmAssociationLine, SlurmReportLine

_PARTITION_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class SlurmClient(SlurmClientInterface):
    """This class implements Python client for SLURM.

    See also: https://slurm.schedmd.com/sacctmgr.html
    """

    # Commands that support --immediate flag (only sacctmgr)
    _COMMAND_SUPPORTS_IMMEDIATE = frozenset({"sacctmgr"})
    # Commands that support --parsable2 and --noheader flags
    _COMMAND_SUPPORTS_PARSABLE = frozenset({"sacctmgr", "sacct"})
    # SLURM commands whose path should be resolved via slurm_bin_path
    SLURM_COMMANDS = frozenset({"sacctmgr", "sacct", "scancel", "sinfo"})

    # sacctmgr entity types that are cluster-independent (global).
    _CLUSTER_INDEPENDENT_ENTITIES = frozenset({"qos", "tres", "cluster"})

    def __init__(
        self,
        slurm_tres: dict,
        slurm_bin_path: str = "/usr/bin",
        cluster_name: Optional[str] = None,
    ) -> None:
        """Inits SLURM-related data."""
        self.slurm_tres = slurm_tres
        self.slurm_bin_path = slurm_bin_path
        self.cluster_name = cluster_name
        self.executed_commands: list[str] = []

    def get_version(self) -> str:
        """Return the SLURM version string as reported by ``sinfo -V``."""
        return self._execute_command(
            ["-V"], command_name="sinfo", immediate=False, parsable=False
        ).strip()

    def validate_slurm_binary(self) -> bool:
        """Validate that sacctmgr is a real SLURM binary, not an emulator.

        Runs ``sacctmgr --version`` and checks that the output contains
        "slurm", which real SLURM binaries always include (e.g. "slurm 24.05.4").
        Returns False for emulator scripts that produce different output.
        """
        try:
            output = self._execute_command(
                ["--version"], command_name="sacctmgr", immediate=False, parsable=False
            )
            return "slurm" in output.lower()
        except BackendError:
            return False

    def list_resources(self) -> list[ClientResource]:
        """Returns a list of accounts in the SLURM cluster."""
        command = ["list", "account"]
        output = self._execute_command(command)
        return [self._parse_account(line) for line in output.splitlines() if "|" in line]

    def list_tres(self) -> list[str]:
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

    def list_clusters(self) -> list[str]:
        """Returns a list of cluster names known to SLURM."""
        output = self._execute_command(["list", "cluster", "format=cluster"])
        # Real ``sacctmgr --parsable2 --noheader format=cluster`` emits one
        # bare cluster name per line — the single requested field is also the
        # last, so SLURM's PARSABLE_NO_ENDING path prints no trailing ``|``.
        return [line.split("|")[0].strip() for line in output.splitlines() if line.strip()]

    def get_resource(self, resource_id: str) -> ClientResource | None:
        """Returns Account object from cluster based on the account name."""
        output = self._execute_command(["show", "account", resource_id])
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_account(lines[0])

    @staticmethod
    def _sanitize_sacctmgr_value(value: str) -> str:
        """Sanitize a value for use in sacctmgr key="value" arguments.

        Strips double quotes to prevent breaking out of the quoted context
        and injecting additional sacctmgr parameters.
        """
        return value.replace('"', "")

    def create_resource(
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
            f'description="{self._sanitize_sacctmgr_value(description)}"',
            f'organization="{self._sanitize_sacctmgr_value(organization)}"',
        ]
        if parent_name:
            parts.append(f'parent="{parent_name}"')
        return self._execute_command(parts)

    def get_account_parent(self, account: str) -> Optional[str]:
        """Return the current parent account name for the given account, or None if not found.

        ParentName is an association-level field — ``sacctmgr show account`` always
        leaves it blank — so the parent must be read from the association.  Each
        account has one account-level association (empty User) plus one per member
        user; only the account-level row carries the parent we want.
        """
        output = self._execute_command(
            ["show", "assoc", f"account={account}", "format=Account,ParentName,User", "-n", "-P"]
        )
        account_col, parent_col, user_col = 0, 1, 2
        for line in output.splitlines():
            parts = line.strip().split("|")
            if (
                len(parts) > user_col
                and parts[account_col].strip().lower() == account.lower()
                and parts[user_col].strip() == ""
            ):
                return parts[parent_col].strip() or None
        return None

    def set_account_parent(self, account: str, new_parent: str) -> str:
        """Reparent a SLURM account under new_parent."""
        return self._execute_command(
            ["modify", "account", "where", f"name={account}", "set", f"parent={new_parent}"]
        )

    def delete_all_users_from_account(self, name: str) -> str:
        """Drop all the users from the account based on the account name."""
        return self._execute_command(["remove", "user", "where", f"account={name}"])

    def account_has_users(self, account: str) -> bool:
        """Checks if the account with the specified name have related users."""
        output = self._execute_command(["show", "association", "where", f"account={account}"])
        items = [self._parse_association(line) for line in output.splitlines() if "|" in line]
        return any(item.user != "" for item in items)

    def delete_resource(self, name: str) -> str:
        """Deletes account with the specified name from the SLURM cluster."""
        return self._execute_command(["remove", "account", "where", f"name={name}"])

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> str | None:
        """Sets the limits for the account with the specified name."""
        limits_str = ",".join([f"{key}={value}" for key, value in sorted(limits_dict.items())])
        quota = f"GrpTRESMins={limits_str}"
        return self._execute_command(["modify", "account", resource_id, "set", quota])

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set account limits for a specific user."""
        limits_str = ",".join([f"{tres}={limits_dict.get(tres, -1)}" for tres in self.list_tres()])
        quota = f"MaxTRESMins={limits_str}"
        return self._execute_command(
            ["modify", "user", username, "where", f"account={resource_id}", "set", quota]
        )

    def set_account_qos(self, account: str, qos: str) -> None:
        """Set the specified QoS for the account."""
        self._execute_command(["modify", "account", account, "set", f"qos={qos}"])

    def get_association(self, user: str, resource_id: str) -> Association | None:
        """Returns associations between the user and the account if exists."""
        output = self._execute_command(
            [
                "show",
                "association",
                "where",
                f"user={user}",
                f"account={resource_id}",
            ]
        )
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_association(lines[0])

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Creates association between the account and the user in SLURM cluster."""
        args = ["add", "user", username, f"account={resource_id}"]
        if default_account is not None:
            args.append(f"DefaultAccount={default_account}")
        args.append("Share=parent")  # Inherits fairshare value from the parent account
        return self._execute_command(args)

    def delete_association(self, username: str, resource_id: str) -> str:
        """Deletes association between the account and the user in SLURM cluster."""
        return self._execute_command(
            [
                "remove",
                "user",
                "where",
                f"name={username}",
                "and",
                f"account={resource_id}",
            ]
        )

    def get_usage_report(
        self, resource_ids: list[str], timezone: Optional[str] = None
    ) -> list[SlurmReportLine]:
        """Generates per-user usage report for the accounts."""
        month_start, month_end = backend_utils.format_current_month(timezone or "")

        args = [
            "--noconvert",
            "--truncate",
            "--allocations",
            "--allusers",
            f"--starttime={month_start}",
            f"--endtime={month_end}",
            f"--accounts={','.join(resource_ids)}",
            "--format=Account,ReqTRES,Elapsed,User",
        ]
        output = self._execute_command(args, "sacct", immediate=False)
        return [
            SlurmReportLine(line, self.slurm_tres) for line in output.splitlines() if "|" in line
        ]

    def get_historical_usage_report(
        self, resource_ids: list[str], year: int, month: int
    ) -> list[SlurmReportLine]:
        """Generates per-user usage report for the accounts for a specific month.

        Args:
            resource_ids: List of SLURM account names to query
            year: Year to query (e.g., 2024)
            month: Month to query (1-12)

        Returns:
            List of SlurmReportLine objects containing usage data for the specified month
        """
        month_start, month_end = backend_utils.format_month_period(year, month)

        args = [
            "--noconvert",
            "--truncate",
            "--allocations",
            "--allusers",
            f"--starttime={month_start}",
            f"--endtime={month_end}",
            f"--accounts={','.join(resource_ids)}",
            "--format=Account,ReqTRES,Elapsed,User",
        ]
        output = self._execute_command(args, "sacct", immediate=False)
        return [
            SlurmReportLine(line, self.slurm_tres) for line in output.splitlines() if "|" in line
        ]

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Returns limits for the account."""
        args = [
            "show",
            "association",
            "format=account,GrpTRESMins",
            "where",
            f"accounts={resource_id}",
        ]
        output = self._execute_command(args, immediate=False)
        lines = [
            SlurmAssociationLine(line, self.slurm_tres)
            for line in output.splitlines()
            if "|" in line
        ]
        correct_lines = [
            association.tres_limits for association in lines if association.tres_limits
        ]
        if len(correct_lines) == 0:
            return {}
        return correct_lines[0]

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""
        args = [
            "show",
            "association",
            "where",
            f"accounts={resource_id}",
            "format=Account,MaxTRESMins,User",
        ]
        output = self._execute_command(args, immediate=False)
        lines = [
            SlurmAssociationLine(line, self.slurm_tres)
            for line in output.splitlines()
            if "|" in line
        ]
        return {
            association.user: association.tres_limits
            for association in lines
            if association.user != ""
        }

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Returns list of users linked to the account."""
        args = [
            "list",
            "associations",
            "format=account,user",
            "where",
            f"account={resource_id}",
        ]
        output = self._execute_command(args)
        # sacctmgr returns one row per association: the account-level
        # association (empty user column, rendered "account|") plus one row
        # per user ("account|user"). --parsable2 emits no trailing separator.
        # Keep only rows whose account column matches and whose user column is
        # non-empty, which skips the account-level rows.
        users = []
        for line in output.splitlines():
            if "|" not in line:
                continue
            account, _, user_field = line.partition("|")
            username = user_field.split("|")[0].strip()
            if account.strip() != resource_id or not username:
                continue
            users.append(username)
        return users

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
        # sacctmgr returns one row per association ("account|qos"); --parsable2
        # emits no trailing separator. Match on the account column and take the
        # qos column.
        min_columns_for_qos = 2
        for line in output.splitlines():
            if "|" not in line:
                continue
            parts = line.split("|")
            if len(parts) < min_columns_for_qos or parts[0].strip() != account:
                continue
            return parts[1].strip()
        return ""

    def cancel_active_user_jobs(self, account: str, user: Optional[str] = None) -> None:
        """Cancel jobs for the account and user.

        If user is None, cancel all the jobs for the account.
        """
        args = ["-A", account, "-f"]
        if user is not None:
            args = ["-u", user, *args]
        self._execute_command(args, command_name="scancel", parsable=False, immediate=False)

    def list_active_user_jobs(self, account: str, user: str) -> list[str]:
        """List active jobs for the account and user."""
        args = [
            "-a",
            f"--account={account}",
            f"--user={user}",
            "--format=JobID,JobName,Partition,Account,User,State,Elapsed,Timelimit,NodeList",
        ]
        output = self._execute_command(args, command_name="sacct", immediate=False)
        return [line.split("|")[0] for line in output.splitlines() if "|" in line]

    def check_user_exists(self, username: str) -> bool:
        """Check if the user exists in the system."""
        args = ["-u", username]
        try:
            output = self._execute_command(
                args, command_name="id", immediate=False, parsable=False, silent=True
            )
        except BackendError as e:
            if "no such user" in str(e):
                return False
        return output.strip().isdigit()

    def _parse_account(self, line: str) -> ClientResource:
        parts = line.split("|")
        return ClientResource(
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

    def _inject_cluster_filter(self, command: list[str], command_name: str) -> list[str]:
        """Inject cluster filtering into the command if cluster_name is set.

        For sacctmgr: adds ``cluster=<name>`` to account/association/user commands.
        Skips cluster-independent entities (QoS, TRES, cluster).
        For sacct/scancel: adds ``--cluster=<name>`` flag.
        """
        if not self.cluster_name:
            return command

        if command_name == "sacctmgr":
            # Skip cluster-independent operations and --version
            if (
                any(token in self._CLUSTER_INDEPENDENT_ENTITIES for token in command[:3])
                or "--version" in command
            ):
                return command

            cluster_arg = f"cluster={self.cluster_name}"
            command = list(command)  # avoid mutating caller's list
            if "set" in command:
                # For modify commands, cluster filter goes before "set"
                set_idx = command.index("set")
                command.insert(set_idx, cluster_arg)
            else:
                command.append(cluster_arg)

        elif command_name in ("sacct", "scancel"):
            command = [f"--cluster={self.cluster_name}", *command]

        return command

    def _execute_command(
        self,
        command: list[str],
        command_name: str = "sacctmgr",
        immediate: bool = True,
        parsable: bool = True,
        silent: bool = False,
    ) -> str:
        """Constructs and executes a command with the given parameters."""
        if immediate and command_name not in self._COMMAND_SUPPORTS_IMMEDIATE:
            raise ValueError(
                f"--immediate is not supported by {command_name}. "
                f"Use immediate=False for non-sacctmgr commands."
            )
        if parsable and command_name not in self._COMMAND_SUPPORTS_PARSABLE:
            raise ValueError(
                f"--parsable2/--noheader are not supported by {command_name}. "
                f"Use parsable=False for {command_name} commands."
            )

        command = self._inject_cluster_filter(command, command_name)

        if self.slurm_bin_path and command_name in self.SLURM_COMMANDS:
            resolved_command = str(Path(self.slurm_bin_path) / command_name)
        else:
            resolved_command = command_name
        account_command = [resolved_command]
        if parsable:
            account_command.extend(["--parsable2", "--noheader"])
        if immediate:
            account_command.append("--immediate")
        account_command.extend(command)
        self.executed_commands.append(" ".join(account_command))
        try:
            return self.execute_command(account_command, silent=silent)
        except BackendError as e:
            if command and command[0] == "modify" and "Nothing modified" in str(e):
                # Real sacctmgr prints "Nothing modified" on stdout but exits 1
                # for a no-op modify (account_functions.c:726-729 returns
                # SLURM_ERROR; sacctmgr.c:982-984 maps that to exit_code=1).
                # The desired state is already reached, so treat it as success.
                return ""
            raise

    # ===== QOS MANAGEMENT EXTENSION =====

    def qos_exists(self, qos_name: str) -> bool:
        """Check if a QoS exists in the SLURM cluster."""
        output = self._execute_command(["show", "qos", qos_name])
        return any("|" in line for line in output.splitlines())

    def create_qos(
        self,
        name: str,
        flags: Optional[str] = None,
        grp_tres: Optional[str] = None,
        max_jobs: Optional[int] = None,
        max_submit: Optional[int] = None,
        max_wall: Optional[str] = None,
        min_tres_per_job: Optional[str] = None,
    ) -> None:
        """Create a QoS with the specified parameters.

        Args:
            name: QoS name (typically matches the SLURM account name).
            flags: Comma-separated flags (e.g., "DenyOnLimit,NoDecay").
            grp_tres: Group TRES limits (e.g., "cpu=25600,node=100").
            max_jobs: Maximum concurrent jobs.
            max_submit: Maximum submitted jobs.
            max_wall: Maximum wall time (minutes or D-HH:MM:SS).
            min_tres_per_job: Minimum TRES per job (e.g., "gres/gpu=1").
        """
        parts = ["add", "qos", name]
        if flags:
            parts.extend(["set", f"flags={flags}"])
        self._execute_command(parts)

        # Apply settings in separate modify commands (matches EFP workflow)
        if grp_tres:
            self._execute_command(["modify", "qos", name, "set", f"GrpTRES={grp_tres}"])
        if max_jobs is not None:
            self._execute_command(["modify", "qos", name, "set", f"MaxJobs={max_jobs}"])
        if max_submit is not None:
            self._execute_command(["modify", "qos", name, "set", f"MaxSubmit={max_submit}"])
        if max_wall is not None:
            self._execute_command(["modify", "qos", name, "set", f"MaxWall={max_wall}"])
        if min_tres_per_job:
            self._execute_command(
                ["modify", "qos", name, "set", f"MinTRESPerJob={min_tres_per_job}"]
            )

    def delete_qos(self, name: str) -> None:
        """Delete a QoS from the SLURM cluster."""
        self._execute_command(["remove", "qos", "where", f"name={name}"])

    def set_account_qos_list(self, account: str, qos_list: list[str]) -> None:
        """Set the full QoS list for an account (qos=qos1,qos2)."""
        qos_str = ",".join(qos_list)
        self._execute_command(["modify", "account", account, "set", f"qos={qos_str}"])

    def add_account_qos(self, account: str, qos_name: str) -> None:
        """Add a QoS to an account's list (qos+=name)."""
        self._execute_command(
            ["modify", "account", "set", f"qos+={qos_name}", "where", f"account={account}"]
        )

    def set_account_default_qos(self, account: str, qos_name: str) -> None:
        """Set the default QoS for an account."""
        self._execute_command(
            ["modify", "account", "set", f"defaultqos={qos_name}", "where", f"account={account}"]
        )

    # ===== PARTITION-AWARE ASSOCIATION EXTENSION =====

    def create_association_with_partition(
        self,
        username: str,
        resource_id: str,
        partition: str,
        default_account: Optional[str] = None,
    ) -> str:
        """Create an association between a user and account with a specific partition."""
        if not _PARTITION_NAME_RE.match(partition):
            msg = f"Invalid SLURM partition name: {partition!r}"
            raise BackendError(msg)
        args = ["add", "user", username, f"account={resource_id}"]
        if default_account is not None:
            args.append(f"DefaultAccount={default_account}")
        args.append(f"Partition={partition}")
        return self._execute_command(args)

    def create_association_with_partitions(
        self,
        username: str,
        resource_id: str,
        partitions: Sequence[str],
        default_account: Optional[str] = None,
    ) -> str:
        """Create a user→account association restricted to the given partitions.

        Emits ``sacctmgr add user … Partitions=p1,p2 Share=parent``.
        Partition names are sorted alphabetically so the argument is
        deterministic. ``DefaultPartition=`` is not emitted: real
        sacctmgr does not accept it on ``add user`` (no parser in
        ``user_functions.c`` or ``sacctmgr_set_assoc_rec``) and would
        reject the call with ``Unknown option``.
        """
        if not partitions:
            msg = "partitions must be non-empty"
            raise BackendError(msg)
        for name in partitions:
            if not _PARTITION_NAME_RE.match(name):
                msg = f"Invalid SLURM partition name: {name!r}"
                raise BackendError(msg)
        sorted_parts = sorted(partitions)
        args = ["add", "user", username, f"account={resource_id}"]
        if default_account is not None:
            args.append(f"DefaultAccount={default_account}")
        args.extend([f"Partitions={','.join(sorted_parts)}", "Share=parent"])
        return self._execute_command(args)

    # ===== PERIODIC LIMITS EXTENSION =====

    def set_account_fairshare(self, account: str, fairshare: int) -> bool:
        """Set fairshare for account hierarchy."""
        try:
            self._execute_command(["modify", "account", account, "set", f"fairshare={fairshare}"])
            return True
        except BackendError as e:
            raise BackendError(f"Failed to set fairshare for account {account}: {e}") from e

    def set_account_limits(self, account: str, limit_type: str, limits: dict) -> bool:
        """Set GrpTRESMins, MaxTRESMins, or GrpTRES limits."""
        try:
            for tres_type, value in limits.items():
                limit_spec = f"{limit_type}={tres_type}={value}"
                self._execute_command(["modify", "account", account, "set", limit_spec])
            return True
        except BackendError as e:
            raise BackendError(
                f"Failed to set {limit_type} limits for account {account}: {e}"
            ) from e

    def reset_raw_usage(self, account: str) -> bool:
        """Reset raw usage for clean period start (manual reset mode)."""
        try:
            self._execute_command(["modify", "account", account, "set", "RawUsage=0"])
            return True
        except BackendError as e:
            raise BackendError(f"Failed to reset raw usage for account {account}: {e}") from e

    def get_account_fairshare(self, account: str) -> int:
        """Get current fairshare value for account."""
        try:
            command = [
                "list",
                "account",
                "format=account,fairshare",
                "where",
                f"account={account}",
            ]
            output = self._execute_command(command)

            for line in output.splitlines():
                if "|" in line and account in line:
                    parts = line.split("|")
                    min_parts_for_fairshare = 2
                    if len(parts) >= min_parts_for_fairshare:
                        try:
                            return int(parts[1])
                        except ValueError:
                            pass
            return 0

        except BackendError as e:
            raise BackendError(f"Failed to get fairshare for account {account}: {e}") from e

    def get_account_limits(self, account: str) -> dict:
        """Get current account limits (GrpTRESMins, MaxTRESMins, etc.)."""
        try:
            command = [
                "list",
                "account",
                "format=account,grptres,grptresmin,maxtres,maxtresmin",
                "where",
                f"account={account}",
            ]
            output = self._execute_command(command)

            limits: dict[str, dict[str, str]] = {
                "GrpTRES": {},
                "GrpTRESMins": {},
                "MaxTRES": {},
                "MaxTRESMins": {},
            }

            for line in output.splitlines():
                if "|" in line and account in line:
                    parts = line.split("|")
                    min_parts_for_limits = 5
                    if len(parts) >= min_parts_for_limits:
                        # Parse TRES format and populate limits dict
                        # This is simplified - production would need proper TRES parsing
                        if parts[1]:  # GrpTRES
                            limits["GrpTRES"] = self._parse_tres_string(parts[1])
                        if parts[2]:  # GrpTRESMins
                            limits["GrpTRESMins"] = self._parse_tres_string(parts[2])
                        if parts[3]:  # MaxTRES
                            limits["MaxTRES"] = self._parse_tres_string(parts[3])
                        if parts[4]:  # MaxTRESMins
                            limits["MaxTRESMins"] = self._parse_tres_string(parts[4])

            return limits

        except BackendError as e:
            raise BackendError(f"Failed to get limits for account {account}: {e}") from e

    def _parse_tres_string(self, tres_string: str) -> dict[str, str]:
        """Parse TRES string format like 'cpu=1000,mem=2000,gres/gpu=100'."""
        tres_dict: dict[str, str] = {}
        if not tres_string or tres_string == "":
            return tres_dict

        for tres_item in tres_string.split(","):
            if "=" in tres_item:
                tres_name, tres_value = tres_item.split("=", 1)
                try:
                    tres_dict[tres_name] = str(int(tres_value))
                except ValueError:
                    tres_dict[tres_name] = tres_value  # Keep as string if not numeric

        return tres_dict

    def calculate_billing_units(self, tres_usage: dict, billing_weights: dict) -> float:
        """Convert raw TRES usage to billing units using weights."""
        billing_units = 0.0

        for tres_type, usage in tres_usage.items():
            weight = billing_weights.get(tres_type, 0)
            if weight and isinstance(usage, (int, float)):
                billing_units += usage * weight

        return billing_units
