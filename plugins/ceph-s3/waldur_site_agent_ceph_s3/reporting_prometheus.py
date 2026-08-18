"""GB-day storage metering from a Prometheus-compatible time series database.

Admin Ops keeps no stored-bytes series to integrate — ``/admin/usage`` is
bandwidth and operation counts, and bucket stats are a point-in-time size — so a
radosgw-flavoured offering has nowhere to read a curve from. This backend reads
one from a database that already holds it, which keeps the agent stateless: the
series is somebody else's to retain, exactly as it is on croit.

Something has to put the readings there — a cron job scraping ``radosgw-admin
bucket stats`` into ``/api/v1/import/prometheus``, or a RadosGW exporter behind
vmagent. Either way that collector, not the agent, is what has to keep running;
a gap in the series is a gap in the invoice.

Anything speaking the Prometheus HTTP API works. VictoriaMetrics is the sensible
target for billing data, having the retention this needs; Prometheus itself
documents that it is unsuitable where full accuracy is required, and a default
15-day retention cannot answer for last month.

Wire it up alongside the management backend::

    backend_type: ceph_s3
    order_processing_backend: ceph_s3
    membership_sync_backend: ceph_s3
    reporting_backend: prometheus_usage
"""

from typing import Dict, List, Optional, Tuple

from .backend import logger
from .promql import PromQLClient, PromQLError
from .reporting import _GbDayUsageBackend

DEFAULT_METRIC = "ceph_rgw_user_stored_bytes"
DEFAULT_OWNER_LABEL = "owner"
# Half-hourly is the report period's own default, so a series scraped to match it
# is read at its native resolution. A month at this step is 1488 points per
# series, comfortably inside the 11k-point cap a range query carries.
DEFAULT_STEP = "30m"


class PrometheusUsageBackend(_GbDayUsageBackend):
    """The management backend plus GB-day metering read out of a TSDB."""

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Build a management backend and the range-query client beside it.

        No flavour check: which cluster the bytes came from is a property of
        whatever fills the database, not of this backend. On croit prefer
        ``croit_usage`` — its series is native, sampled at 180 s, and already
        covers periods predating any collector deployed here.
        """
        super().__init__(backend_settings, backend_components)

        url = backend_settings.get("prometheus_url")
        if not url:
            raise ValueError(
                "prometheus_usage reporting requires 'prometheus_url' in "
                "backend_settings: it is where the storage series is read from"
            )

        self.usage_metric = backend_settings.get("prometheus_metric", DEFAULT_METRIC)
        self.owner_label = backend_settings.get(
            "prometheus_owner_label", DEFAULT_OWNER_LABEL
        )
        self.query_step = backend_settings.get("prometheus_step", DEFAULT_STEP)
        self.promql_client = PromQLClient(
            url,
            timeout=backend_settings.get("prometheus_timeout", 30),
            verify_ssl=backend_settings.get("prometheus_verify_ssl", True),
            username=backend_settings.get("prometheus_username"),
            password=backend_settings.get("prometheus_password"),
            token=backend_settings.get("prometheus_token"),
        )

        # One range query answers for every resource in a pass; see
        # _get_storage_series. Keyed by window so the live period, whose end moves
        # with every pass, can never be served from the previous pass's answer.
        self._window: Optional[Tuple[int, int]] = None
        self._series_by_owner: Dict[str, List[dict]] = {}

        logger.info(
            "Prometheus usage metering reading %s by %s from %s",
            self.usage_metric,
            self.owner_label,
            url,
        )

    @property
    def usage_query(self) -> str:
        """Bytes held per S3 user, summed across the buckets it owns.

        Aggregating in the query rather than in the agent is what makes one
        request answer for every resource, and it means a bucket appearing or
        disappearing mid-period needs no handling here — the sum simply follows.
        """
        return f"sum by ({self.owner_label}) ({self.usage_metric})"

    @staticmethod
    def _datapoint(timestamp: object, value: object) -> dict:
        """One reply pair as a datapoint, with NaN carried rather than billed.

        Prometheus renders a stale or absent reading as ``NaN``. That is the same
        thing croit's series says with a null, so it is handed over as one: the
        integrator carries the previous reading across it instead of billing the
        interval at zero.
        """
        reading = float(value)
        return {
            "t": int(timestamp),
            "v": None if reading != reading else reading,  # noqa: PLR0124
        }

    def _fetch_series_by_owner(
        self, period_start: int, period_end: int
    ) -> Dict[str, List[dict]]:
        """Run the one range query this pass needs and index it by owner."""
        result = self.promql_client.query_range(
            self.usage_query, period_start, period_end, self.query_step
        )

        series_by_owner: Dict[str, List[dict]] = {}
        for entry in result:
            owner = entry.get("metric", {}).get(self.owner_label)
            if not owner:
                # An unlabelled series cannot be attributed to a resource, and
                # billing an unattributed one to anybody is worse than dropping it.
                continue
            series_by_owner[owner] = [
                self._datapoint(timestamp, value)
                for timestamp, value in entry.get("values", [])
            ]

        logger.debug(
            "Range query returned %d owner series for [%d, %d]",
            len(series_by_owner),
            period_start,
            period_end,
        )
        return series_by_owner

    def _get_storage_series(
        self, username: str, period_start: int, period_end: int
    ) -> list:
        """The user's series, out of the answer already fetched for this window.

        A user absent from the reply has no series, which reads as "could not
        measure" and keeps the resource out of the report. That is the right
        reading even for a tenant that genuinely holds nothing: a collector that
        stopped and a tenant that deleted everything look identical from here, so
        the collector is the thing that has to publish an explicit zero.
        """
        window = (period_start, period_end)
        if self._window != window:
            try:
                self._series_by_owner = self._fetch_series_by_owner(
                    period_start, period_end
                )
            except PromQLError as e:
                # Held rather than re-raised per resource: the query answers for
                # all of them at once, so letting it fail per username would retry
                # a down database once per resource. Every resource ends up
                # unmeasured either way, which reports nothing and overwrites
                # nothing.
                logger.error("Could not read the storage series: %s", e)
                self._series_by_owner = {}
            self._window = window

        return self._series_by_owner.get(username, [])
