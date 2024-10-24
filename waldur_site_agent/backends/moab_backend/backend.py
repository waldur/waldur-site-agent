"""Moab-specific backend classes and functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Tuple

from waldur_site_agent.backends import BackendType, logger
from waldur_site_agent.backends import utils as backend_utils
from waldur_site_agent.backends.backend import BaseBackend
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.moab_backend.client import MoabClient

if TYPE_CHECKING:
    from waldur_site_agent.backends.moab_backend.parser import MoabReportLine


class MoabBackend(BaseBackend):
    """MOAB backend class."""

    def __init__(self, moab_settings: Dict, moab_components: Dict[str, Dict]) -> None:
        """Init backend data and creates a corresponding client."""
        super().__init__(moab_settings, moab_components)
        self.backend_type = BackendType.MOAB.value
        self.client = MoabClient()
        self.backend_components["deposit"]["unit_factor"] = 1

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if MOAB is online."""
        try:
            self.client.list_accounts()
        except BackendError as err:
            if raise_exception:
                raise
            logger.info("Error: %s", err)
            return False
        else:
            return True

    def list_components(self) -> List[str]:
        """Return deposit component."""
        return ["deposit"]

    def _get_usage_report(self, accounts: List[str]) -> Dict:
        """Get usage report."""
        report: Dict[str, Dict[str, Dict[str, int]]] = {}
        lines: List[MoabReportLine] = self.client.get_usage_report(accounts)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, {})
            user_usage_existing = report[line.account][line.user]
            user_usage_new = backend_utils.sum_dicts([user_usage_existing, line.usages])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = list(account_usage.values())
            total = backend_utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = total

        return report

    def downscale_resource(self, account: str) -> bool:
        """Temporary placeholder."""
        del account
        return False

    def pause_resource(self, account: str) -> bool:
        """Temporary placeholder."""
        del account
        return False

    def restore_resource(self, account: str) -> bool:
        """Temporary placeholder."""
        del account
        return False

    def get_resource_metadata(self, _: str) -> dict:
        """Temporary placeholder."""
        return {}

    def _collect_limits(
        self, waldur_resource: Dict[str, Dict]
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Collect deposit limit only with no conversion."""
        deposit_limit = {"deposit": waldur_resource["limits"]["deposit"]}
        return deposit_limit, deposit_limit
