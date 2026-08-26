"""Usage reporting backend: reports LiteLLM token usage and spend to Waldur.

Report-only (mirrors ``envoy-usage`` and ``cscs-dwdi``): it meters, it does not manage
resources. Pair it with the ``litellm`` management backend
(``order_processing_backend: litellm``, ``reporting_backend: litellm-usage``).
"""

from __future__ import annotations

import calendar
import datetime
import logging
import re
import time
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import backends, structures
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common import WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES
from waldur_site_agent.common.structures import normalize_backend_components

from .usage_client import LiteLLMUsageClient

logger = logging.getLogger(__name__)

# The components this backend can meter, and the usage-row field each reads.
# ``token_cost`` is LiteLLM's own USD spend, for offerings that price upstream rather
# than applying a Waldur-side rate per token.
USAGE_COMPONENTS = ("input_tokens", "output_tokens", "token_cost")

# Keys are minted as "<resource_backend_id>-<n>"; the slot number is stripped so usage
# is attributed to the resource.
_SLOT_SUFFIX = re.compile(r"-\d+$")

_REPORT_ONLY = "LiteLLM usage backend is reporting-only and does not manage resources"

# How long a fetched month of usage rows stays reusable, in seconds. It has to cover
# one reporting pass and end before the next one starts, so it is derived from the
# agent's own report period rather than fixed: an operator who shortens the period to
# a couple of minutes would otherwise be left with a cache spanning several passes.
#
# Half the period, not the whole of it. Equal is the one value that breaks: an entry
# stored at the top of a pass would still be valid at the top of the next, which is
# exactly when the fresh numbers are wanted. Halving leaves the whole of one pass
# covered (a pass runs in seconds) and the next pass certain to refetch.
_USAGE_CACHE_TTL_FRACTION = 0.5
DEFAULT_USAGE_CACHE_TTL = (
    WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60 * _USAGE_CACHE_TTL_FRACTION
)


class LiteLLMUsageReportingBackend(backends.BaseBackend):
    """Reports per-resource token usage and spend from a LiteLLM proxy to Waldur."""

    supports_decreasing_usage: bool = True

    def __init__(self, backend_settings: dict, backend_components: dict) -> None:
        """Initialize the backend, normalizing component objects to plain dicts."""
        super().__init__(backend_settings, normalize_backend_components(backend_components))
        self.backend_type = "litellm-usage"

        # What this backend can meter, intersected with what the offering declares, so
        # an offering that prices tokens and one that prices upstream cost both work
        # without a mode flag. If nothing matches, every resource would report zero —
        # warn once rather than let that be silent.
        self._usage_keys = [key for key in USAGE_COMPONENTS if key in self.backend_components]
        if not self._usage_keys:
            logger.warning(
                "No LiteLLM usage components configured: none of %s present in "
                "backend_components %s; usage will be reported as zero",
                list(USAGE_COMPONENTS),
                list(self.backend_components.keys()),
            )

        # ``/user/daily/activity`` has no server-side filter by key, so one call
        # returns the whole proxy's month and the rows for one resource are picked out
        # of it. The processor pulls one resource at a time (and once more per
        # historical period), so without this every resource would walk the entire
        # table again -- N x P full scans, each up to MAX_PAGES requests, per pass.
        # Keyed by date range: the periods repeat across resources, the rows do not
        # depend on which resource asked. The default TTL follows the agent's report
        # period; override it only to pin a value the period should not move.
        ttl = backend_settings.get("usage_cache_ttl")
        self._usage_cache_ttl = DEFAULT_USAGE_CACHE_TTL if ttl is None else float(ttl)
        self._rows_cache: dict = {}

        if not backend_settings.get("api_url"):
            msg = "LiteLLM usage backend requires 'api_url' in backend_settings"
            raise BackendError(msg)
        self.usage_client = LiteLLMUsageClient(backend_settings)

    # --- health / introspection -------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the LiteLLM proxy is reachable."""
        if self.usage_client.ping():
            return True
        if raise_exception:
            msg = "LiteLLM usage backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log backend configuration and report reachability."""
        logger.info("=== LiteLLM usage reporting backend diagnostics ===")
        logger.info("Proxy URL: %s", self.usage_client.api_url)
        logger.info("Metered components: %s", self._usage_keys)
        return self.ping(raise_exception=False)

    def list_components(self) -> list:
        """Return the configured component names."""
        return list(self.backend_components.keys())

    # --- usage reporting --------------------------------------------------------

    @staticmethod
    def _resource_id_for(key_alias: str) -> str:
        """Map a key alias back onto the resource that owns it.

        Usage is attributed per resource, not per key: a resource holds several keys
        and rotating one mints a fresh identity, so metering by key would split one
        tenant's bill across slots and lose the rotated-away half.
        """
        return _SLOT_SUFFIX.sub("", key_alias)

    def _usage_rows(self, start_date: str, end_date: str) -> list:
        """Return the proxy's usage rows for a date range, reusing a recent fetch.

        The cache is scoped to a reporting pass by its TTL rather than by the backend's
        lifetime: the polling agent builds a fresh backend per pass today, but a caller
        that reused one would otherwise keep reporting the first pass's numbers.
        """
        key = (start_date, end_date)
        now = time.monotonic()
        cached = self._rows_cache.get(key)
        if cached is not None and now - cached[0] < self._usage_cache_ttl:
            return cached[1]
        rows = self.usage_client.get_usage_rows(start_date, end_date)
        self._rows_cache[key] = (now, rows)
        return rows

    def _collect_usage(
        self, resource_backend_ids: list, start_date: str, end_date: str
    ) -> dict:
        if not resource_backend_ids or not self._usage_keys:
            return {}
        wanted = set(resource_backend_ids)
        report: dict = {}
        for row in self._usage_rows(start_date, end_date):
            resource_id = self._resource_id_for(row["key_alias"])
            if resource_id not in wanted:
                continue
            # A resource appears in many rows — one per key per day, and its keys may
            # be used on different days. Accumulate rather than overwrite, or all but
            # the last row is silently dropped and the resource is under-billed.
            totals = report.setdefault(
                resource_id, {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self._usage_keys, 0)}
            )["TOTAL_ACCOUNT_USAGE"]
            for key in self._usage_keys:
                totals[key] += row[key]
        return report

    @staticmethod
    def _month_range(year: int, month: int) -> tuple:
        """Return the inclusive ``YYYY-MM-DD`` bounds of a calendar month."""
        last_day = calendar.monthrange(year, month)[1]
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"

    def _get_usage_report(self, resource_backend_ids: list) -> dict:
        """Usage for the current month."""
        today = datetime.datetime.now(tz=datetime.timezone.utc)
        start, end = self._month_range(today.year, today.month)
        return self._collect_usage(resource_backend_ids, start, end)

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list,
        year: int,
        month: int,
        waldur_resource: Optional[WaldurResource] = None,
    ) -> dict:
        """Usage for a specific billing month (used by the historical-usage loader)."""
        del waldur_resource
        start, end = self._month_range(year, month)
        return self._collect_usage(resource_backend_ids, start, end)

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> Optional[structures.BackendResourceInfo]:
        """Build resource info from usage alone.

        Report-only: unlike the base implementation this does not call
        ``client.get_resource``. The spend API has no resource concept, only usage, so
        the resource is synthesized from its usage for the report processor to submit.
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
        """Return the resource's usage as a BackendResourceInfo (zeros if none yet)."""
        del waldur_resource
        logger.info("Pulling resource %s", resource_backend_id)
        usage_report = self._get_usage_report([resource_backend_id])
        # Zero-filled over the metered keys, the same set _collect_usage fills. Using
        # every declared component here instead would report a component this backend
        # cannot meter as a real zero for an idle resource, while a resource with usage
        # simply omits it.
        account_usage = usage_report.get(
            resource_backend_id,
            {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self._usage_keys, 0)},
        )
        return structures.BackendResourceInfo(users=[], usage=account_usage)

    # --- report-only: not implemented ------------------------------------------

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        raise NotImplementedError(_REPORT_ONLY)

    def _collect_resource_limits(self, waldur_resource: WaldurResource) -> tuple:
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
