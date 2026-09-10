"""GB-day storage metering, and croit's route to the series it integrates.

Split from the management backend because a flavour either has a stored-bytes
series or it does not, and that is a fact about which class is wired into
``reporting_backend`` rather than a check inside a method.

Everything above the series — the integration, the billing period, the refusal to
report a zero — is the same either way and lives in ``_GbDayUsageBackend``. Only
the source of the datapoints differs: croit holds the series already, so this
module fetches it. RadosGW Admin Ops holds nothing to fetch (``/admin/usage`` is
bandwidth and operation counts, and bucket stats are a point-in-time size), so
that flavour reads a series collected into a time series database instead; see
``reporting_prometheus``.

Wire it up alongside the management backend::

    backend_type: ceph_s3
    order_processing_backend: ceph_s3
    membership_sync_backend: ceph_s3
    reporting_backend: croit_usage
"""

import datetime
import zoneinfo
from decimal import Decimal
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import structures

from .backend import (
    _SECONDS_PER_DAY,
    CephS3Backend,
    _is_measurable,
    _to_usage_units,
    logger,
)
from .clients.croit import CroitClient
from .settings import CROIT


class _GbDayUsageBackend(CephS3Backend):
    """The management backend plus GB-day metering, minus a source of readings.

    Subclasses supply ``_get_storage_series``; everything that turns a series into
    a billable figure is here, so the two flavours cannot drift on how a month is
    bounded, how the area under the curve is computed, or what an unmeasurable
    resource reports.
    """

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Build a management backend that can also meter."""
        super().__init__(backend_settings, backend_components)

        # Usernames whose usage the last report actually measured. See
        # _pull_backend_resource: an unread series must not become a zero.
        self._reported_usernames: set = set()

    def _get_storage_series(
        self, username: str, period_start: int, period_end: int
    ) -> list:
        """The user's stored-bytes readings across a window.

        Returns:
            ``[{"t": unix seconds, "v": bytes}]`` ascending. An empty or all-null
            series means "could not measure", which keeps the resource out of the
            report rather than zeroing it.
        """
        raise NotImplementedError

    def _before_current_period_report(self, resource_backend_ids: list) -> None:
        """Hook for whatever has to happen before the live period can be read.

        Nothing for either backend today: both read a series somebody else has
        already recorded. It is the seam a source that needs preparing per pass
        would use, and the reason ``_get_usage_report`` has somewhere to put it.
        """

    def _apply_usage_policy(
        self, resource_backend_id: str, info: structures.BackendResourceInfo
    ) -> Optional[structures.BackendResourceInfo]:
        """Keep measured usage, and drop the resource when there was none.

        The base implementation fills every component with 0 when
        ``_get_usage_report`` omits the resource. That is right for a backend where
        "no usage record" means "used nothing", and wrong here: the reported figure
        is a period-to-date accrual, so a croit outage would submit 0 and overwrite
        the month's GB-days with it. Skipping the resource instead leaves the last
        good value in place, and the next successful pass recomputes the whole
        period from the series anyway.
        """
        # Nothing is asked of the usage payload: the base pull always sets
        # TOTAL_ACCOUNT_USAGE, zero-filling it when the report omits the resource.
        # Whether this pass actually measured anything is what _reported_usernames
        # answers, and it is the only thing that can be asked here.
        if resource_backend_id not in self._reported_usernames:
            logger.warning(
                "Usage for %s could not be read, so it is not being reported; the "
                "period's accrued total is left as it stands",
                resource_backend_id,
            )
            return None

        return info

    def _reporting_tz(self) -> datetime.tzinfo:
        """The timezone the billing period is computed in.

        Injected by the reporting processor (``processors.py``:
        ``self.resource_backend.timezone = timezone``) from the agent's global
        configuration — the same value it uses to decide which period a report
        lands in, so the month boundary here cannot drift from the one there. The
        configuration validator rejects a timezone that does not parse, so the
        fallback below is unreachable in a configured agent; it used to be the
        thing that made the two disagree.
        """
        if self.timezone:
            try:
                return zoneinfo.ZoneInfo(self.timezone)
            except Exception:
                logger.warning(
                    "Unknown timezone %r, falling back to UTC for the billing period",
                    self.timezone,
                )
        return datetime.timezone.utc

    def _billing_period_start(self) -> int:
        """Unix seconds at the start of the current billing period.

        Usage accrues per calendar month, so the integral restarts here.
        """
        now = datetime.datetime.now(self._reporting_tz())
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return int(month_start.timestamp())

    def _period_bounds(self, year: int, month: int) -> tuple:
        """Unix seconds bounding a calendar month in the reporting timezone."""
        tz = self._reporting_tz()
        start = datetime.datetime(year, month, 1, tzinfo=tz)
        end = (
            datetime.datetime(year + 1, 1, 1, tzinfo=tz)
            if month == 12
            else datetime.datetime(year, month + 1, 1, tzinfo=tz)
        )
        return int(start.timestamp()), int(end.timestamp())

    @staticmethod
    def _integrate_gb_days(
        datapoints: list[dict],
        unit_factor: int,
        period_end: Optional[int] = None,
    ) -> Decimal:
        """Area under the storage curve, in component units × days.

        Each datapoint is one rectangle: the reading, held until the next sample.
        Step rather than trapezoid because storage really does hold its value
        until a discrete upload or delete changes it.

        ``period_end`` closes the last rectangle. Without it the final reading
        spans nothing and is dropped, which is the entire measured consolidation
        error — negligible at 180 s buckets and 2.13% at 6098 s ones. Pinning the
        end makes the figure correct at any resolution instead of relying on
        maxDataPoints being large enough for the dropped bucket to be small.

        The closing rectangle is capped at one sampling interval. Running it all the
        way to ``period_end`` would bill the last known reading across an arbitrarily
        long gap, so an outage of whatever produces the readings would silently inflate
        the invoice for the rest of the month rather than merely stop updating it.

        A null value *between* samples is missing telemetry, so the previous reading
        is carried across it. Skipping that interval would bill it at zero, turning a
        metrics outage into a silent discount.
        """
        divisor = Decimal(str(unit_factor)) if unit_factor > 0 else Decimal(1)
        total = Decimal(0)
        last: Optional[Decimal] = None

        boundaries = [point["t"] for point in datapoints]
        if period_end is not None and len(boundaries) >= 2:
            step = boundaries[-1] - boundaries[-2]
            closing = min(period_end, boundaries[-1] + step)
            if closing > boundaries[-1]:
                boundaries.append(closing)

        for point, next_t in zip(datapoints, boundaries[1:]):
            raw = point.get("v")
            value = last if raw is None else Decimal(str(raw))
            if value is None:
                continue
            last = value
            seconds = Decimal(next_t - point["t"])
            total += (value / divisor) * (seconds / _SECONDS_PER_DAY)

        return total

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Collect GB-day storage usage for the current billing period.

        The price list bills per GB per day, and Waldur multiplies a usage
        component's price by its quantity flat — the PER_DAY proration in invoicing
        applies only to fixed components. So the days live inside the quantity, as
        the integral of held bytes over the period.

        Recomputed from the period start on every pass rather than accumulated,
        which makes the figure absolute: idempotent, and self-healing after an
        outage, with no running total to keep straight.

        Args:
            resource_backend_ids: List of S3 usernames

        Returns:
            The shape the reporter expects — see BaseBackend._get_usage_report::

                {"<s3 user>": {"TOTAL_ACCOUNT_USAGE": {"s3_storage": 12.5}}}

            Values are in Waldur units (GB-days), with unit_factor applied. A
            resource whose series could not be read is absent from the report
            rather than present with a zero, which would overwrite the period's
            accrued total.
        """
        self._before_current_period_report(resource_backend_ids)
        now = int(datetime.datetime.now(self._reporting_tz()).timestamp())
        return self._collect_usage(
            resource_backend_ids, self._billing_period_start(), now
        )

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list[str],
        year: int,
        month: int,
        waldur_resource: Optional[WaldurResource] = None,
    ) -> dict:
        """Recompute a past month's GB-days with both window ends pinned.

        The current-period report is capped at the moment of the last pass before
        rollover, so the month's tail is otherwise never billed and an agent outage
        across the boundary becomes a permanent under-bill. The series outlives the
        period it covers, so that period is simply integrated again.
        """
        del waldur_resource
        period_start, period_end = self._period_bounds(year, month)
        return self._collect_usage(resource_backend_ids, period_start, period_end)

    def _collect_usage(
        self, resource_backend_ids: list[str], period_start: int, period_end: int
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Integrate each user's storage series over one window."""
        report: dict[str, dict[str, dict[str, float]]] = {}
        # _pull_backend_resource needs to distinguish "measured zero" from "could
        # not measure"; the report alone cannot say which, once the base class has
        # zero-filled it.
        self._reported_usernames = set()

        storage_components = self._storage_components()

        for username in resource_backend_ids:
            try:
                datapoints = self._get_storage_series(
                    username, period_start, period_end
                )
                if not _is_measurable(datapoints):
                    logger.warning(
                        "Storage series for user %s carries no measurable reading "
                        "(%d datapoint(s), %d non-null); skipping its usage report",
                        username,
                        len(datapoints),
                        sum(1 for point in datapoints if point.get("v") is not None),
                    )
                    continue

                # Reported regardless of accounting type. A limit component's usage
                # is display-only in Waldur -- it never reaches an invoice item --
                # so reporting it cannot affect billing either way, and reporting
                # the same figure in both modes means flipping the accounting type
                # cannot silently change what the agent measures.
                usage = {
                    name: _to_usage_units(
                        self._integrate_gb_days(
                            datapoints,
                            config.get("unit_factor", 1),
                            period_end=period_end,
                        )
                    )
                    for name, config in storage_components.items()
                }

                if usage:
                    # The reporter reads TOTAL_ACCOUNT_USAGE and skips the resource
                    # without it. There is exactly one S3 user per resource, so the
                    # total is that user's usage; per-user entries would key on
                    # Waldur offering users, which this backend does not manage.
                    report[username] = {"TOTAL_ACCOUNT_USAGE": usage}
                    self._reported_usernames.add(username)
                    logger.debug("Usage for user %s: %s", username, usage)

            except Exception as e:
                logger.error("Failed to collect usage for user %s: %s", username, e)

        logger.info("Collected usage report for %d users", len(report))
        return report


class CroitUsageBackend(_GbDayUsageBackend):
    """The management backend plus the GB-day metering only croit can do."""

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Build a management backend and refuse a flavour that cannot meter.

        Failing here rather than at the first reporting pass: a reporting backend
        that silently measures nothing looks exactly like a tenant that stored
        nothing, and the difference only shows up on an invoice.
        """
        super().__init__(backend_settings, backend_components)
        if self.flavour != CROIT:
            raise ValueError(
                "croit_usage reporting requires the croit flavour: vanilla Ceph "
                "exposes no per-user stored-bytes series to integrate, so there "
                "is nothing to compute GB-days from"
            )
        # The client is croit's by construction now, which is what makes
        # get_user_storage_series available below.
        self.croit_client: CroitClient = self.client  # type: ignore[assignment]

    def _get_storage_series(
        self, username: str, period_start: int, period_end: int
    ) -> list:
        """Read croit's per-user storage series straight from its statistics API.

        ``GET /api/stats?graph=s3-user-data`` already holds the whole period at
        180 s resolution, so there is nothing for the agent to record or retain.
        """
        return self.croit_client.get_user_storage_series(
            username, period_start, period_end
        )
