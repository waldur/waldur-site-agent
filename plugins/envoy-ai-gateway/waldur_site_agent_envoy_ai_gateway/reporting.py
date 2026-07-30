"""Usage reporting backend: reports per-key token usage from a usage warehouse.

Report-only (mirrors cscs-dwdi): implements usage reporting; does not manage resources.
Pair it with the `envoy` management backend (`order_processing_backend: envoy`,
`reporting_backend: envoy-usage`).
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import backends, structures
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import normalize_backend_components

from .usage_client import EnvoyUsageClient

logger = logging.getLogger(__name__)

# The token components this backend meters. Each name is also the usage-warehouse
# field name it reads counts from.
TOKEN_COMPONENTS = ("input_tokens", "output_tokens")

_REPORT_ONLY = "usage reporting backend is reporting-only and does not manage resources"


class EnvoyUsageReportingBackend(backends.BaseBackend):
    """Reports token usage from the usage warehouse to Waldur."""

    supports_decreasing_usage: bool = True

    def __init__(
        self, backend_settings: dict[str, Any], backend_components: dict[str, Any]
    ) -> None:
        """Initialize the backend, normalizing component objects to plain dicts."""
        super().__init__(backend_settings, normalize_backend_components(backend_components))
        self.backend_type = "envoy-usage"

        # The warehouse fields we meter, intersected with the offering's components.
        # If none match, every key would report zero — warn once so it is not silent.
        self._token_keys = [key for key in TOKEN_COMPONENTS if key in self.backend_components]
        if not self._token_keys:
            logger.warning(
                "No token components configured: none of %s present in backend_components %s; "
                "usage will be reported as zero",
                list(TOKEN_COMPONENTS),
                list(self.backend_components.keys()),
            )

        api_url = backend_settings.get("api_url")
        if not api_url:
            msg = "usage reporting backend requires 'api_url' in backend_settings"
            raise BackendError(msg)
        self.usage_client = EnvoyUsageClient(str(api_url), backend_settings.get("api_token"))

    # --- health / introspection -------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the usage warehouse is reachable."""
        if self.usage_client.ping():
            return True
        if raise_exception:
            msg = "usage reporting backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log backend configuration and report reachability."""
        logger.info("=== usage reporting backend diagnostics ===")
        logger.info("Warehouse URL: %s", self.usage_client.api_url)
        logger.info("Components: %s", list(self.backend_components.keys()))
        return self.ping(raise_exception=False)

    def list_components(self) -> list[str]:
        """Return the configured component names (token meters)."""
        return list(self.backend_components.keys())

    # --- usage reporting --------------------------------------------------------

    def _collect_usage(
        self, resource_backend_ids: list[str], from_month: str, to_month: str
    ) -> dict:
        if not resource_backend_ids or not self._token_keys:
            return {}
        report: dict = {}
        # Usage is attributed per resource: the usage-shipper records a resource's
        # keys under the resource's own client_id (the resource backend_id), so a
        # query by backend_id returns the resource's total.
        for row in self.usage_client.get_usage(resource_backend_ids, from_month, to_month):
            client_id = row.get("client_id")
            if not client_id:
                continue
            # A key may appear in several rows (e.g. per-model); accumulate rather
            # than overwrite, or earlier rows are silently dropped (under-billing).
            totals = report.setdefault(
                client_id, {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self._token_keys, 0)}
            )["TOTAL_ACCOUNT_USAGE"]
            for key in self._token_keys:
                totals[key] += int(row.get(key) or 0)
        return report

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Usage for the current month."""
        month = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m")
        return self._collect_usage(resource_backend_ids, month, month)

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list[str],
        year: int,
        month: int,
        waldur_resource: Optional[WaldurResource] = None,
    ) -> dict:
        """Usage for a specific billing month."""
        del waldur_resource
        target = f"{year:04d}-{month:02d}"
        return self._collect_usage(resource_backend_ids, target, target)

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> Optional[structures.BackendResourceInfo]:
        """Build resource info from usage alone.

        Report-only: unlike the base implementation we do NOT call ``client.get_resource``
        (the usage warehouse has no resource concept, only usage events). We synthesize the
        resource from the client_id's usage so the report processor can submit it.
        """
        if not waldur_resource.backend_id:
            logger.warning("Backend ID is missing for resource %s", waldur_resource.uuid)
            return None
        try:
            return self._pull_backend_resource(waldur_resource.backend_id)
        except Exception:
            logger.exception("Error while pulling resource [%s]", waldur_resource.backend_id)
            return None

    def _pull_backend_resource(
        self, resource_backend_id: str, waldur_resource: Optional[WaldurResource] = None
    ) -> Optional[structures.BackendResourceInfo]:
        """Return the client_id's usage as a BackendResourceInfo (zeros if none yet)."""
        del waldur_resource
        logger.info("Pulling resource %s", resource_backend_id)
        usage_report = self._get_usage_report([resource_backend_id])
        account_usage = usage_report.get(
            resource_backend_id,
            {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self.backend_components, 0)},
        )
        return structures.BackendResourceInfo(users=[], usage=account_usage)

    # --- report-only: not implemented ------------------------------------------

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        raise NotImplementedError(_REPORT_ONLY)

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        raise NotImplementedError(_REPORT_ONLY)

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for a reporting-only backend."""
        raise NotImplementedError(_REPORT_ONLY)

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for a reporting-only backend."""
        raise NotImplementedError(_REPORT_ONLY)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for a reporting-only backend."""
        raise NotImplementedError(_REPORT_ONLY)

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Not implemented for a reporting-only backend."""
        raise NotImplementedError(_REPORT_ONLY)
