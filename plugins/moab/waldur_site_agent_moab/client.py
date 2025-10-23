"""CLI-client for MOAB."""

from __future__ import annotations

from typing import Optional

from waldur_site_agent.backend import clients, exceptions, logger
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.structures import Association, ClientResource
from waldur_site_agent_moab.parser import MoabReportLine


class MoabClient(clients.BaseClient):
    """This class implements Python client for MOAB.

    See also MOAB Accounting Manager 9.1.1 Administrator Guide
    http://docs.adaptivecomputing.com/9-1-1/MAM/help.htm
    """

    def list_resources(self) -> list[ClientResource]:
        """Return list of accounts in MOAB."""
        output = self.execute_command(
            ["mam-list-accounts", "--raw", "--quiet", "--show", "Name,Description,Organization"]
        )
        return [self._parse_account(line) for line in output.splitlines() if "|" in line]

    def _parse_account(self, line: str) -> ClientResource:
        parts = line.split("|")
        return ClientResource(name=parts[0], description=parts[1], organization=parts[2])

    def _get_fund_id(self, account: str) -> Optional[int]:
        command_fund = f"mam-list-funds --raw --quiet -a {account} --show Id"
        fund_output = self.execute_command(command_fund.split())
        fund_data = fund_output.splitlines()

        if len(fund_data) == 0:
            logger.warning("No funds were found for account %s", account)
            return None

        # Assuming an account has only one fund
        fund_id_str = fund_data[0].strip()

        return int(fund_id_str)

    def get_resource(self, resource_id: str) -> ClientResource | None:
        """Get MOAB account info."""
        command = (
            f"mam-list-accounts --raw --quiet --show Name,Description,Organization -a {resource_id}"
        )
        output = self.execute_command(command.split())
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None
        return self._parse_account(lines[0])

    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create account in MOAB."""
        del parent_name
        command_account = f'mam-create-account -a {name} -d "{description}" -o {organization}'
        self.execute_command(command_account.split())

        logger.info("Creating fund for the account")
        command_fund = f"mam-create-fund -a {name}"
        return self.execute_command(command_fund.split())

    def delete_resource(self, name: str) -> str:
        """Delete account from MOAB."""
        command_account = f"mam-delete-account -a {name}"
        self.execute_command(command_account.split())

        fund_id = self._get_fund_id(name)

        if fund_id is None:
            logger.warning("Skipping fund deletion.")
            return ""

        logger.info("Deleting the account fund %s", fund_id)

        command_fund = f"mam-delete-fund -f {fund_id}"
        return self.execute_command(command_fund.split())

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> str | None:
        """Set the limits for the account with the specified name."""
        if limits_dict.get("deposit", 0) < 0:
            logger.warning(
                "Skipping limit update because pricing "
                "package is not created for the related service settings."
            )
            return None

        fund_id = self._get_fund_id(resource_id)

        if fund_id is None:
            raise exceptions.BackendError(
                f"The account {resource_id} does not have a linked fund, unable to set a deposit"
            )

        command_deposit = f"mam-deposit -a {resource_id} -z {limits_dict['deposit']} -f {fund_id}"
        return self.execute_command(command_deposit.split())

    def get_resource_limits(self, _: str) -> dict[str, int]:
        """Get account limits."""
        return {}

    def get_resource_user_limits(self, _: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set account limits for a specific user."""
        # The method is a placeholder and is not implemented yet
        del resource_id, username, limits_dict
        return ""

    def get_association(self, user: str, resource_id: str) -> Association | None:
        """Get association between user and account."""
        command = (
            f"mam-list-funds --raw --quiet -u {user} -a {resource_id} --show Constraints,Balance"
        )
        output = self.execute_command(command.split())
        lines = [line for line in output.splitlines() if "|" in line]
        if len(lines) == 0:
            return None

        return Association(
            account=resource_id, user=user, value=int(float(lines[0].split("|")[-1]))
        )

    def create_association(self, username: str, resource_id: str, _: Optional[str] = None) -> str:
        """Create association between user and account in MOAB."""
        command = f"mam-modify-account --add-user +{username} -a {resource_id}"
        return self.execute_command(command.split())

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between user and account."""
        command = f"mam-modify-account --del-user {username} -a {resource_id}"
        return self.execute_command(command.split())

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get usages records from MOAB."""
        template = (
            "mam-list-usagerecords --raw --quiet --show "
            "Account,User,Charge "
            "-a %(account)s -s %(start)s -e %(end)s"
        )
        month_start, month_end = backend_utils.format_current_month()

        report_lines = []
        for account in resource_ids:
            command = template % {
                "account": account,
                "start": month_start,
                "end": month_end,
            }
            lines = self.execute_command(command.split()).splitlines()
            report_lines_to_add = [MoabReportLine(line) for line in lines if "|" in line]
            report_lines.extend(report_lines_to_add)

        return report_lines

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Returns list of users linked to the account."""
        # TODO: make use of -A flag (fetch only active users)
        command = f"mam-list-users -a {resource_id} --raw --show Name,DefaultAccount --quiet"
        output = self.execute_command(command.split())
        return [
            line.split("|")[0] for line in output.splitlines() if "|" in line and line[-1] != "|"
        ]
