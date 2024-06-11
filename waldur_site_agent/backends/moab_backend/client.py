"""CLI-client for MOAB."""

from __future__ import annotations

from typing import Dict, List, Optional

from waldur_site_agent.backends import base, logger
from waldur_site_agent.backends import utils as backend_utils
from waldur_site_agent.backends.structures import Account, Association

from .parser import MoabReportLine


class MoabClient(base.BaseClient):
    """This class implements Python client for MOAB.

    See also MOAB Accounting Manager 9.1.1 Administrator Guide
    http://docs.adaptivecomputing.com/9-1-1/MAM/help.htm
    """

    def list_accounts(self) -> List[Account]:
        """Return list of accounts in MOAB."""
        output = self.execute_command(
            "mam-list-accounts --raw --quiet --show Name,Description,Organization".split()
        )
        return [self._parse_account(line) for line in output.splitlines() if "|" in line]

    def _parse_account(self, line: str) -> Account:
        parts = line.split("|")
        return Account(name=parts[0], description=parts[1], organization=parts[2])

    def get_account(self, name: str) -> Account | None:
        """Get MOAB account info."""
        command = (
            "mam-list-accounts --raw --quiet --show Name,Description,Organization -a %s" % name
        )
        output = self.execute_command(command.split())
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_account(lines[0])

    def create_account(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create account in MOAB."""
        del parent_name
        command = f'mam-create-account -a {name} -d "{description}" -o {organization}'
        return self.execute_command(command.split())

    def delete_account(self, name: str) -> str:
        """Delete account from MOAB."""
        command = "mam-delete-account -a %s" % name
        return self.execute_command(command.split())

    def set_resource_limits(self, account: str, limits_dict: Dict[str, int]) -> str | None:
        """Set the limits for the account with the specified name."""
        if limits_dict.get("deposit", 0) < 0:
            logger.warning(
                "Skipping limit update because pricing "
                "package is not created for the related service settings."
            )
            return None
        command = f"mam-deposit -a {account} -z {limits_dict['deposit']} --create-fund True"
        return self.execute_command(command.split())

    def get_association(self, user: str, account: str) -> Association | None:
        """Get association between user and account."""
        command = f"mam-list-funds --raw --quiet -u {user} -a {account} --show Constraints,Balance"
        output = self.execute_command(command.split())
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None

        return Association(account=account, user=user, value=int(lines[0].split("|")[-1]))

    def create_association(self, username: str, account: str, _: Optional[str] = None) -> str:
        """Create association between user and account in MOAB."""
        command = f"mam-modify-account --add-user +{username} -a {account}"
        return self.execute_command(command.split())

    def delete_association(self, username: str, account: str) -> str:
        """Delete association between user and account."""
        command = f"mam-modify-account --del-user {username} -a {account}"
        return self.execute_command(command.split())

    def get_usage_report(self, accounts: List[str]) -> List:
        """Get usages records from MOAB."""
        template = (
            "mam-list-usagerecords --raw --quiet --show "
            "Account,User,Charge "
            "-a %(account)s -s %(start)s -e %(end)s"
        )
        month_start, month_end = backend_utils.format_current_month()

        report_lines = []
        for account in accounts:
            command = template % {
                "account": account,
                "start": month_start,
                "end": month_end,
            }
            lines = self.execute_command(command.split()).splitlines()
            report_lines_to_add = [MoabReportLine(line) for line in lines if "|" in line]
            report_lines.extend(report_lines_to_add)

        return report_lines

    def list_account_users(self, account: str) -> List[str]:
        """Returns list of users lined to the account."""
        # TODO: make use of -A flag (fetch only active users)
        command = f"mam-list-users -a {account} --raw --show Name,DefaultAccount --quiet"
        output = self.execute_command(command.split())
        return [
            line.split("|")[0] for line in output.splitlines() if "|" in line and line[-1] != "|"
        ]
