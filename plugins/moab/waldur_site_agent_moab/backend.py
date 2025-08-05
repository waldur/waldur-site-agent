"""Moab-specific backend classes and functions."""

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import BackendType, logger
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_moab.client import MoabClient
from waldur_site_agent_moab.parser import MoabReportLine


class MoabBackend(BaseBackend):
    """MOAB backend class."""

    def __init__(self, moab_settings: dict, moab_components: dict[str, dict]) -> None:
        """Init backend data and creates a corresponding client."""
        super().__init__(moab_settings, moab_components)
        self.backend_type = BackendType.MOAB.value
        self.client = MoabClient()
        self.backend_components["deposit"]["unit_factor"] = 1

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if MOAB is online."""
        try:
            self.client.list_resources()
        except BackendError as err:
            if raise_exception:
                raise
            logger.info("Error: %s", err)
            return False
        else:
            return True

    def diagnostics(self) -> bool:
        """Logs info about the MOAB cluster."""
        return True

    def list_components(self) -> list[str]:
        """Return deposit component."""
        return ["deposit"]

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Get usage report."""
        report: dict[str, dict[str, dict[str, float]]] = {}
        lines: list[MoabReportLine] = self.client.get_usage_report(resource_backend_ids)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, {})
            user_usage_existing = report[line.account][line.user]
            user_usage_new = backend_utils.sum_dicts([user_usage_existing, line.usages])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = list(account_usage.values())
            total = backend_utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = {
                key: float(round(value, 2)) for key, value in total.items()
            }
            for username, user_usage in account_usage.items():
                account_usage[username] = {
                    key: float(round(value, 2)) for key, value in user_usage.items()
                }

        return report

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Temporary placeholder."""
        del resource_backend_id
        return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Temporary placeholder."""
        del resource_backend_id
        return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Temporary placeholder."""
        del resource_backend_id
        return False

    def get_resource_metadata(self, _: str) -> dict:
        """Temporary placeholder."""
        return {}

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect deposit limit only with no conversion."""
        deposit_limit = {"deposit": waldur_resource["limits"]["deposit"]}
        return deposit_limit, deposit_limit
