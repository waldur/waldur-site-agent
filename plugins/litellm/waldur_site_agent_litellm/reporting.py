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

from .client import META_RESOURCE, LiteLLMClient
from .usage_client import LiteLLMUsageClient

logger = logging.getLogger(__name__)

# The metrics this backend can read off a usage row, and the only values accepted on the
# right-hand side of ``component_metrics``. ``token_cost`` is LiteLLM's own USD spend
# (its ``spend`` field) -- already differentiated per model by the proxy's cost map --
# for offerings that price upstream rather than applying a Waldur-side rate per token.
USAGE_METRICS = ("input_tokens", "output_tokens", "token_cost")

# Component name -> metric, for offerings that name their components after these
# metrics. The historical shape, and still the default.
USAGE_COMPONENTS = USAGE_METRICS

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

        # What this backend meters: component name -> the usage-row metric it is fed.
        # The metrics are the three in ``USAGE_METRICS`` and nothing else; the component
        # names are the offering's own and are never interpreted here.
        #
        # A component named after a metric is metered without configuration.
        # ``component_metrics`` adds the ones that are not, because a metric can
        # legitimately feed two components: an offering that both caps its spend and
        # bills it needs ``token_cost`` (the proxy's per-model USD figure) on a LIMIT
        # component, where it is the cap and is never invoiced, and again on a USAGE
        # component, where it is the charge. A Waldur component is either a cap or a
        # charge, never both, so the same number has to arrive twice under two names --
        # conventionally ``token_cost`` and ``inference_cost``, of which only the second
        # has to be configured.
        #
        # If nothing matches, every resource would report zero -- warn once rather than
        # let that be silent.
        self._usage_metrics = self._resolve_usage_metrics(backend_settings)
        if not self._usage_metrics:
            logger.warning(
                "No LiteLLM usage components configured: none of %s present in "
                "backend_components %s, and no 'component_metrics' mapping given; "
                "usage will be reported as zero",
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
        self._user_map_cache: Optional[tuple] = None

        if not backend_settings.get("api_url"):
            msg = "LiteLLM usage backend requires 'api_url' in backend_settings"
            raise BackendError(msg)
        self.usage_client = LiteLLMUsageClient(backend_settings)

        # Chat usage is attributed to a person, not to a key, so rolling it up onto a
        # resource needs the address-to-resource map -- which lives in the metadata this
        # plugin stamps on each LiteLLM user. That is a management-API read, not a spend
        # one, hence the second client rather than a new method on the usage client. It
        # is the same proxy and the same token: on a composed offering both backends
        # share one ``backend_settings`` block.
        # Only when the offering actually sells a chat surface. On an API-only
        # offering nothing ever arrives under a person's address, so the listing would
        # be a management call per pass that could only ever return an empty map -- and
        # on a proxy whose master key is scoped to the spend API, one that fails.
        self.chat_enabled = bool(backend_settings.get("openwebui"))
        self.litellm_client = LiteLLMClient(backend_settings) if self.chat_enabled else None

    def _resolve_usage_metrics(self, backend_settings: dict) -> dict:
        """Pair each declared component with the usage-row metric that feeds it.

        A component named after a metric is metered without any configuration, which is
        the historical shape and covers ``input_tokens``, ``output_tokens`` and
        ``token_cost``. ``component_metrics`` *adds* to that rather than replacing it,
        so an offering that bills upstream cost alongside its budget cap names only the
        component the default cannot infer:

            component_metrics:
              inference_cost: token_cost

        Restating the components already named after their metric would be redundant --
        the mapping carries only what the names do not say.

        A mapping entry naming a component the offering does not declare is dropped
        rather than reported: reporting usage for a component Waldur does not have
        fails the whole pass, and a stale entry left behind after an offering changed
        should not take the working ones down with it.
        """
        resolved = {
            name: name for name in USAGE_COMPONENTS if name in self.backend_components
        }
        for name, metric in dict(backend_settings.get("component_metrics") or {}).items():
            if metric not in USAGE_METRICS:
                msg = (
                    f"component_metrics maps component '{name}' to unknown metric "
                    f"'{metric}'; known metrics are {list(USAGE_METRICS)}"
                )
                raise BackendError(msg)
            if name not in self.backend_components:
                logger.warning(
                    "component_metrics names component '%s', which the offering does "
                    "not declare; skipping it",
                    name,
                )
                continue
            resolved[name] = metric
        return resolved

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
        logger.info("Metered components: %s", self._usage_metrics)
        logger.info("Chat usage attribution: %s", "on" if self.chat_enabled else "off")
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

    def _user_usage_rows(self, start_date: str, end_date: str) -> list:
        """Per-person usage rows for a date range, cached like the per-key ones."""
        key = ("users", start_date, end_date)
        now = time.monotonic()
        cached = self._rows_cache.get(key)
        if cached is not None and now - cached[0] < self._usage_cache_ttl:
            return cached[1]
        rows = self.usage_client.get_user_usage_rows(start_date, end_date)
        self._rows_cache[key] = (now, rows)
        return rows

    def _user_resource_map(self) -> dict:
        """Return ``{email: resource_backend_id}`` for every user the plugin manages.

        Read from the proxy rather than from Waldur: this backend never sees a team
        list, and the ownership stamp on the user record is in any case the thing that
        decides which resource an address bills to.

        Cached on the same TTL as the usage rows, and for the same reason -- one
        reporting pass asks per resource, and the listing does not depend on which
        resource asked.
        """
        if self.litellm_client is None:
            return {}
        now = time.monotonic()
        cached = self._user_map_cache
        if cached is not None and now - cached[0] < self._usage_cache_ttl:
            return cached[1]
        mapping = {}
        try:
            records = self.litellm_client.list_users()
        except BackendError:
            # Losing the map costs the chat half of one pass; raising would lose the
            # API half as well, and the processor would report nothing at all for every
            # resource on the offering.
            logger.exception("Unable to read the LiteLLM user list; chat usage not attributed")
            return {}
        for record in records:
            email = str(record.get("user_id") or "").strip().lower()
            metadata = record.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            resource_id = metadata.get(META_RESOURCE)
            if email and resource_id:
                mapping[email] = str(resource_id)
        self._user_map_cache = (now, mapping)
        return mapping

    def _collect_usage(
        self, resource_backend_ids: list, start_date: str, end_date: str
    ) -> dict:
        if not resource_backend_ids or not self._usage_metrics:
            return {}
        wanted = set(resource_backend_ids)
        report: dict = {}

        # 1. The API surface: usage on the keys this offering minted, attributed by
        #    alias. Every key row that lands on one of the resources in scope.
        for row in self._usage_rows(start_date, end_date):
            resource_id = self._resource_id_for(row["key_alias"])
            if resource_id not in wanted:
                continue
            self._accumulate(report, resource_id, row)

        # 2. The chat surface: usage that arrived on Open WebUI's shared key, which
        #    belongs to no resource, and is identifiable only by the address the proxy
        #    stamped onto it. Attributed to whichever resource owns that address.
        #
        #    The two breakdowns cover the same records, so a row served on a key this
        #    plugin minted -- already attributed by alias -- is dropped here.
        #    Without that, every request a person makes with their own API key would be
        #    billed twice, once by alias and once by address.
        user_map = self._user_resource_map()
        if not user_map:
            return report
        for row in self._user_usage_rows(start_date, end_date):
            owner = user_map.get(str(row["user_id"]).strip().lower())
            if owner is None or owner not in wanted:
                continue
            # Dropped when the key it was served on belongs to a resource this plugin
            # manages *at all*, not merely one in this pass's scope. Attribution by
            # alias always wins: the row is already counted in step 1 when that
            # resource is in scope, and on the pass that does have it in scope
            # otherwise. Testing only against ``wanted`` would take a row served on
            # resource A's key and, because the person's address is owned by B, bill it
            # to B on this pass and to A on the next one -- one resource's API traffic
            # charged to another, and charged twice overall.
            #
            # "Managed" is read off the alias shape alone, because this backend never
            # learns the offering's full resource list. Every alias this plugin mints is
            # ``<uuid.hex>-<slot>``, so stripping the slot suffix changes it; a key with
            # no suffix -- Open WebUI's own shared key is exactly that -- is left alone,
            # and its rows are the chat usage this step exists to pick up. Testing
            # instead against the resources seen in this pass missed a resource that has
            # keys but no chat users: its key rows were billed to the address owner's
            # resource here and to itself by alias on its own pass.
            alias = row["key_alias"]
            if alias and self._resource_id_for(alias) != alias:
                continue
            self._accumulate(report, owner, row)
        return report

    def _accumulate(self, report: dict, resource_id: str, row: dict) -> None:
        """Add one usage row onto a resource's running total.

        A resource appears in many rows — one per key and per person per day — so this
        accumulates rather than overwrites; assigning would silently drop all but the
        last row and under-bill the resource.
        """
        totals = report.setdefault(
            resource_id, {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self._usage_metrics, 0)}
        )["TOTAL_ACCOUNT_USAGE"]
        for name, metric in self._usage_metrics.items():
            totals[name] += row[metric]

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
            {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self._usage_metrics, 0)},
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
