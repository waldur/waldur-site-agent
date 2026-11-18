"""GB-day storage metering from croit's statistics subsystem.

Split from the management backend because the series this integrates has no
equivalent in RadosGW Admin Ops: /admin/usage carries bandwidth and operation
counts, and bucket stats are a point-in-time size. A radosgw-flavoured offering
can create users, keys and quotas but cannot be metered, so "this flavour cannot
report" is a fact about which class is wired into ``reporting_backend`` rather
than a check inside a method.

Wire it up alongside the management backend::

    backend_type: ceph_s3
    order_processing_backend: ceph_s3
    membership_sync_backend: ceph_s3
    reporting_backend: croit_usage

On a croit cluster managed natively (``flavour: radosgw``) the series still comes
from croit, through credentials of its own in ``croit_reporting``. Resources in
RGW account mode are metered per bucket: croit attributes an account's storage to
its buckets and none of it to the account's users.
"""

import datetime
import json
import os
import zoneinfo
from decimal import Decimal
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import structures
from waldur_site_agent.backend.exceptions import BackendError

from .backend import (
    _SECONDS_PER_DAY,
    CephS3Backend,
    _is_measurable,
    _to_usage_units,
    logger,
)
from .clients.croit import CroitClient
from .settings import CROIT
from .tenant import split

# Months whose bucket sets are kept: a year and the current month, so a past
# period recomputed by the historical loader still knows the buckets deleted in it.
_LEDGER_MONTHS_KEPT = 13


def _until_last_reading(datapoints: list) -> list:
    """Cut a bucket series after its last real reading.

    A deleted bucket keeps its series, and croit fills every interval after the
    deletion with null. The integrator carries the previous reading across a null
    -- right for a gap in a live user's telemetry, wrong here, where it would
    bill a deleted bucket until the month ends. Trailing nulls are therefore
    dropped: for a bucket that still exists they are samples croit has not taken
    yet, and since every pass recomputes the period, they are billed as soon as
    they arrive.
    """
    for index in range(len(datapoints) - 1, -1, -1):
        if datapoints[index].get("v") is not None:
            return datapoints[: index + 1]
    return []


class _BucketLedger:
    """Every bucket an account resource has held during a billing period.

    croit keeps a deleted bucket's series, but nothing lists a bucket once it is
    gone. Without this, deleting a bucket on the 20th would drop its first nineteen
    days from the month's total at the next report. In memory for the life of the
    agent process, and in ``state_file`` too when one is configured, so a restart
    does not forget the buckets deleted before it.
    """

    def __init__(self, state_file: str = "") -> None:
        self.state_file = state_file
        self._seen: dict = {}
        self._dirty = False
        if state_file and os.path.exists(state_file):
            try:
                with open(state_file) as handle:
                    loaded = json.load(handle)
                if isinstance(loaded, dict):
                    self._seen = loaded
            except (OSError, ValueError) as e:
                logger.warning("Ignoring unreadable bucket ledger %s: %s", state_file, e)

    def remember(self, username: str, period_start: int, buckets: list) -> list:
        """Record the buckets listed now; return every bucket seen this period.

        The answer is taken before pruning, and the period asked about is never
        pruned: a historical recompute of a month older than those kept must still
        get that month's buckets back, not a KeyError.
        """
        periods = self._seen.setdefault(username, {})
        key = str(period_start)
        seen = sorted(set(periods.get(key, [])) | set(buckets))
        if seen != periods.get(key):
            periods[key] = seen
            self._dirty = True
        for old in sorted(periods, key=int)[:-_LEDGER_MONTHS_KEPT]:
            if old != key:
                del periods[old]
                self._dirty = True
        return seen

    def seen(self, username: str, period_start: int) -> list:
        return list(self._seen.get(username, {}).get(str(period_start), []))

    def flush(self) -> None:
        """Write the ledger out, once per reporting pass and only if it changed."""
        if self._dirty:
            self._save()
            self._dirty = False

    def _save(self) -> None:
        if not self.state_file:
            return
        tmp = self.state_file + ".tmp"
        try:
            with open(tmp, "w") as handle:
                json.dump(self._seen, handle)
            os.replace(tmp, self.state_file)
        except OSError as e:
            logger.warning("Could not save bucket ledger %s: %s", self.state_file, e)


class CroitUsageBackend(CephS3Backend):
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
        reporting = backend_settings.get("croit_reporting") or {}
        self.croit_client: CroitClient
        if self.flavour == CROIT:
            if reporting:
                raise ValueError(
                    "croit_reporting is for a radosgw-flavour offering on a croit "
                    "cluster; the croit flavour already reports through api_url"
                )
            # The management client is croit's by construction here.
            self.croit_client = self.client  # type: ignore[assignment]
        else:
            if not isinstance(reporting, dict) or not reporting.get("api_url"):
                raise ValueError(
                    "croit_usage on the radosgw flavour reads croit's statistics, so "
                    "it needs croit's address and credentials in croit_reporting "
                    "(api_url plus token, or username and password). Plain Ceph has "
                    "no stored-bytes series to integrate"
                )
            if not reporting.get("token") and not (
                reporting.get("username") and reporting.get("password")
            ):
                raise ValueError(
                    "croit_reporting needs either 'token' or both 'username' and "
                    "'password'"
                )
            self.croit_client = CroitClient(
                api_url=reporting["api_url"],
                username=reporting.get("username"),
                password=reporting.get("password"),
                token=reporting.get("token"),
                verify_ssl=reporting.get("verify_ssl", True),
                timeout=reporting.get("timeout", 30),
            )
        self._bucket_ledger = _BucketLedger(str(reporting.get("bucket_state_file") or ""))

        # Usernames whose usage the last report actually measured. See
        # _pull_backend_resource: an unread series must not become a zero.
        self._reported_usernames: set = set()

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
        long gap, so an outage of croit's statistics subsystem would silently inflate
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
        outage, with no state on the agent side.

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
        across the boundary becomes a permanent under-bill. croit keeps the series,
        so the period is simply integrated again.
        """
        del waldur_resource
        period_start, period_end = self._period_bounds(year, month)
        report = self._collect_usage(resource_backend_ids, period_start, period_end)
        if self.historical_load:
            # The loader reads a missing resource as "no usage" and, with
            # --reconcile-stale, zeroes the month; the base class asks a backend
            # that cannot measure a period to raise instead.
            unmeasured = sorted(set(resource_backend_ids) - set(report))
            if unmeasured:
                raise BackendError(
                    f"croit has no measurable storage series for {year}-{month:02d} "
                    f"for {', '.join(unmeasured)}; refusing to report that period "
                    "rather than have it read as zero"
                )
        return report

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
                if self.rgw_accounts or split(username)[0]:
                    # Per bucket: an account's storage belongs to the account, not
                    # its users, and croit's per-user graph cannot name a tenanted
                    # user at all -- but it names a tenanted bucket.
                    usage = self._bucket_usage(
                        username, period_start, period_end, storage_components
                    )
                    if usage:
                        report[username] = {"TOTAL_ACCOUNT_USAGE": usage}
                        self._reported_usernames.add(username)
                        logger.debug("Bucket-summed usage for %s: %s", username, usage)
                    continue

                datapoints = self.croit_client.get_user_storage_series(
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

        self._bucket_ledger.flush()
        logger.info("Collected usage report for %d users", len(report))
        return report

    def _bucket_usage(
        self,
        username: str,
        period_start: int,
        period_end: int,
        storage_components: dict,
    ) -> dict:
        """GB-days as the sum over every bucket the resource held this period.

        For an account resource, and for a tenanted user, whose storage croit's
        per-user graph cannot report. Interim for accounts, until croit can report
        an account as a whole. Integrals add, so
        each bucket is integrated on its own and the results summed; a bucket croit
        has not sampled yet contributes nothing until it has.

        Returns:
            Usage by component, or {} when nothing could be measured -- which the
            caller turns into "not reported", never into a zero. That includes an
            account with no bucket known this period: after an agent restart
            without a state file that is indistinguishable from an account whose
            buckets were all deleted, and a zero would wipe the period's accrual.
        """
        # Listing through the root user returns every bucket of the account,
        # including those its IAM users created.
        # croit's listing spells a tenanted bucket "tenant:bucket", RGW's gives the
        # bare name; the ledger and the statistics both want the bare one. A
        # bucket name cannot contain ":", so the split is unambiguous.
        current = [
            bucket["name"].rpartition(":")[2]
            for bucket in self.client.get_user_buckets(username)
        ]
        buckets = self._bucket_ledger.remember(username, period_start, current)
        if not buckets:
            logger.info(
                "%s holds no bucket this period; nothing to report",
                username,
            )
            return {}
        tenant = split(username)[0]

        series = {}
        for bucket in buckets:
            datapoints = _until_last_reading(
                self.croit_client.get_bucket_storage_series(
                    bucket, tenant, period_start, period_end
                )
            )
            if _is_measurable(datapoints):
                series[bucket] = datapoints
            else:
                logger.info(
                    "No measurable croit series yet for bucket %s of %s",
                    bucket,
                    username,
                )
        if not series:
            logger.warning(
                "None of the %d bucket(s) of %s has a measurable croit series; "
                "skipping its usage report",
                len(buckets),
                username,
            )
            return {}

        return {
            name: _to_usage_units(
                sum(
                    (
                        self._integrate_gb_days(
                            datapoints, config.get("unit_factor", 1), period_end=period_end
                        )
                        for datapoints in series.values()
                    ),
                    Decimal(0),
                )
            )
            for name, config in storage_components.items()
        }
