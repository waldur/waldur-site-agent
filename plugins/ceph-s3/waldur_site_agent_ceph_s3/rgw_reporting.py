"""Usage metering from RadosGW itself, for a cluster without croit.

Two sources, both on the gateway's Admin Ops API:

- **Storage** is a level: the sum of the resource's bucket sizes now
  (``GET /admin/bucket?uid=…&stats=true``). Plain Ceph keeps no history of it, so
  the agent accrues it into GB-days the way Waldur accrues OpenStack usage: each
  pass adds the level times the time since the previous pass, with the checkpoint
  kept in Waldur's usage row (see ``waldur_site_agent/common/usage_accrual.py``).
  No state is kept by the agent.
- **Traffic and requests** are totals the gateway logs per bucket owner and hour
  (``GET /admin/usage``): bytes sent, bytes received and operations. The log is
  kept by the cluster, so any month can be read again, past ones included.

Wire it up alongside the management backend::

    backend_type: ceph_s3
    order_processing_backend: ceph_s3
    membership_sync_backend: ceph_s3
    reporting_backend: rgw_usage

In account mode everything is measured for the whole account: the root user
lists every bucket of the account, including those its IAM users created, and the
usage log files all of the account's traffic under the account id.
"""

import datetime
from dataclasses import dataclass, field
from decimal import Decimal

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent.backend import structures
from waldur_site_agent.backend.exceptions import BackendError

from .backend import CephS3Backend, _to_usage_units, logger
from .periods import BillingPeriodMixin
from .settings import RADOSGW

STORAGE = "storage"
# backend_name -> the usage log's field.
TRAFFIC_FIELDS = {
    "bytes_sent": "bytes_sent",
    "bytes_received": "bytes_received",
    "ops": "ops",
}
# Bucket statistics: logical bytes (what croit's "Data" series bills, so moving an
# offering off croit_usage keeps invoices comparable) or allocated bytes.
STORAGE_SIZES = {"logical": "logical_bytes", "allocated": "size_bytes"}
ACCRUAL_UNITS = {"day": 86400, "hour": 3600}
# The usage log files traffic per hour, and a gateway flushes its entries every
# rgw_usage_log_tick_interval (30 s by default). An hour that ended this long ago
# holds all it ever will, so its totals can be kept instead of read again.
SETTLED_AFTER = datetime.timedelta(hours=1)
# How many months back a finished month's totals are kept in memory.
_CLOSED_MONTHS_KEPT = 13
# Whether the usage log is written at all is judged from this much of its recent
# past, across every owner, and asked at most this often.
_LOG_CHECK_WINDOW = datetime.timedelta(days=1)
_LOG_CHECK_EVERY = datetime.timedelta(hours=1)


def _aware(moment: object) -> datetime.datetime | None:
    if not isinstance(moment, datetime.datetime):
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.timezone.utc)


def _hour_of(moment: datetime.datetime) -> datetime.datetime:
    """The start of the UTC hour a moment falls in: the usage log's granularity."""
    return moment.astimezone(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)


def _add(left: dict, right: dict) -> dict:
    return {key: left.get(key, 0) + right.get(key, 0) for key in set(left) | set(right)}


@dataclass
class _ClusterCache:
    """What the agent remembers about one RGW cluster between reporting passes.

    The report loop builds a fresh backend for every offering on every pass
    (``polling_processing/agent_report.py``), so anything kept on the backend
    instance is gone by the next pass. This lives for the agent process instead,
    one per cluster: every key in it (an owner, an owner and a month) names
    something on that cluster alone, so offerings sharing a cluster share it
    safely. Losing it (a restart) costs one full read, never a wrong figure.
    """

    # (username, resource creation) -> its RGW account id.
    account_ids: dict = field(default_factory=dict)
    # owner -> (window start, settled moment its totals reach, totals).
    settled: dict = field(default_factory=dict)
    # (owner, window start, window end) -> totals of a finished, settled window.
    closed_months: dict = field(default_factory=dict)
    # (month start, month end) -> whether the usage log holds anything for it.
    month_log: dict = field(default_factory=dict)
    # owner -> the period-to-date totals the previous pass read, to tell new
    # traffic from what was logged earlier in the month.
    last_totals: dict = field(default_factory=dict)
    # When the usage log was last seen written, and last checked.
    log_seen_at: datetime.datetime | None = None
    log_checked_at: datetime.datetime | None = None


_CLUSTER_CACHES: dict = {}


class RgwUsageBackend(BillingPeriodMixin, CephS3Backend):
    """The management backend plus metering from RGW's own Admin Ops."""

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Build a management backend and refuse what this one cannot meter.

        Failing at construction, as croit_usage does: a reporting backend that
        quietly measures nothing looks exactly like a tenant that used nothing.
        """
        super().__init__(backend_settings, backend_components)
        if self.flavour != RADOSGW:
            raise ValueError(
                "rgw_usage meters through RGW's Admin Ops, so it needs "
                "flavour: radosgw; a croit-flavoured offering reports through "
                "croit_usage"
            )
        size = str(backend_settings.get("storage_size") or "logical")
        if size not in STORAGE_SIZES:
            raise ValueError(
                f"storage_size must be one of {', '.join(STORAGE_SIZES)}; got {size!r}"
            )
        self._size_field = STORAGE_SIZES[size]

        self._accrued: dict[str, int] = {}
        for name, config in self._storage_components().items():
            unit = str(config.get("accrual_unit") or "day")
            if unit not in ACCRUAL_UNITS:
                raise ValueError(
                    f"The storage component {name!r} has accrual_unit {unit!r}; "
                    f"use one of {', '.join(ACCRUAL_UNITS)} (GB-days or GB-hours)"
                )
            self._accrued[name] = ACCRUAL_UNITS[unit]

        self._traffic: dict[str, str] = {}
        for name, config in self.backend_components.items():
            field = TRAFFIC_FIELDS.get(str(config.get("backend_name") or ""))
            if field is None:
                continue
            unit_factor = config.get("unit_factor", 1)
            if not isinstance(unit_factor, (int, float)) or unit_factor <= 0:
                raise ValueError(
                    f"The component {name!r} needs a positive 'unit_factor' (bytes or "
                    f"operations per billed unit); got {unit_factor!r}"
                )
            self._traffic[name] = field

        if not self._accrued and not self._traffic:
            raise ValueError(
                "rgw_usage has nothing to meter: give a component backend_name "
                f"'{STORAGE}' (accrued GB-days) or one of "
                f"{', '.join(TRAFFIC_FIELDS)} (from the usage log)"
            )

        # Usernames whose usage the last report actually measured; see
        # _apply_usage_policy.
        self._reported_usernames: set = set()
        # Kept per cluster for the life of the process, not per backend, which
        # lasts one pass; see _ClusterCache.
        cluster = (
            str(backend_settings.get("s3_endpoint") or "").rstrip("/"),
            str(backend_settings.get("admin_path") or "admin"),
        )
        self._cluster = _CLUSTER_CACHES.setdefault(cluster, _ClusterCache())
        # Each resource's creation time, from the Waldur resource it was pulled
        # for; see pull_resource.
        self._created: dict[str, datetime.datetime] = {}

    def accrued_usage_components(self) -> dict[str, int]:
        """Storage is reported as a level and accrued by the reporting processor."""
        return dict(self._accrued)

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> structures.BackendResourceInfo | None:
        """Note when the resource was created, then pull it as usual.

        A uid freed by a deleted resource can come back for a new one, and the
        usage log files both under it. Reading from the new resource's creation on
        keeps the old one's traffic off its bill, and keys what is remembered about
        the owner (its account, its totals) to this resource rather than the uid.
        """
        created = _aware(getattr(waldur_resource, "created", None))
        backend_id = getattr(waldur_resource, "backend_id", None)
        if created is not None and isinstance(backend_id, str):
            self._created[backend_id] = created
        return super().pull_resource(waldur_resource)

    def _apply_usage_policy(
        self, resource_backend_id: str, info: structures.BackendResourceInfo
    ) -> structures.BackendResourceInfo | None:
        """Keep measured usage, and drop the resource when nothing was measured.

        The base pull fills every component with 0 when the report omits the
        resource. Here a 0 would bill storage at nothing and overwrite the month's
        traffic, so the resource is skipped instead and the next pass measures it.
        """
        if resource_backend_id not in self._reported_usernames:
            logger.warning(
                "Usage for %s could not be read, so it is not being reported",
                resource_backend_id,
            )
            return None
        return info

    def _usage_owner(self, username: str, created: datetime.datetime | None) -> str:
        """Whom the usage log files a resource's traffic under.

        The bucket owner: the account in account mode (whichever of its users
        made the request), the user otherwise. A resource's account never changes,
        so it is looked up once per resource -- keyed by its creation too, since a
        resource re-created under the same uid gets a new account.
        """
        if not self.rgw_accounts:
            return username
        key = (username, created)
        if key not in self._cluster.account_ids:
            self._cluster.account_ids[key] = self._account_id_of(username)
        return self._cluster.account_ids[key]

    def _storage_level(self, username: str) -> dict:
        """What the resource stores now, per storage component, in its units."""
        held = sum(
            int(bucket.get(self._size_field) or 0)
            for bucket in self.client.get_user_buckets(username)
        )
        return {
            name: float(
                Decimal(held) / Decimal(str(self.backend_components[name].get("unit_factor", 1)))
            )
            for name in self._accrued
        }

    @staticmethod
    def _settled_until(now: datetime.datetime) -> datetime.datetime:
        """The latest whole UTC hour whose usage-log entries are all written."""
        return _hour_of(now) - SETTLED_AFTER

    @staticmethod
    def _window_start(
        start: datetime.datetime, created: datetime.datetime | None
    ) -> datetime.datetime:
        """Where a resource's usage-log window starts: the period, or its creation.

        The log is hourly, so the creation hour counts whole: RGW drops an hour
        whose bound falls inside it.
        """
        if created is None:
            return start
        return max(start, _hour_of(created))

    def _period_to_date(
        self, owner: str, start: datetime.datetime, now: datetime.datetime
    ) -> dict:
        """An owner's usage-log totals from the window start to now.

        Only the settled part is kept: each pass reads the hours that settled
        since the previous one, plus the unsettled tail (an hour or two), instead
        of the whole month again.
        """
        settled = max(start, self._settled_until(now))
        kept = self._cluster.settled.get(owner)
        if kept is None or kept[0] != start or kept[1] > settled:
            upto, totals = start, {}
        else:
            upto, totals = kept[1], kept[2]
        if settled > upto:
            totals = _add(totals, self.client.get_usage(owner, upto, settled))  # type: ignore[attr-defined]
            upto = settled
        self._cluster.settled[owner] = (start, upto, totals)
        if now > upto:
            return _add(totals, self.client.get_usage(owner, upto, now))  # type: ignore[attr-defined]
        return dict(totals)

    def _finished_month(
        self, owner: str, start: datetime.datetime, end: datetime.datetime
    ) -> dict:
        """An owner's usage-log totals for a finished window, read once it has settled.

        Keyed by the window itself, so offerings with different reporting
        timezones, or a resource created inside the month, never share an entry.
        """
        closed = self._cluster.closed_months
        key = (owner, start, end)
        if key in closed:
            return dict(closed[key])
        totals = self.client.get_usage(owner, start, end)  # type: ignore[attr-defined]
        now = self._now()
        if now >= end + SETTLED_AFTER:
            closed[key] = totals
            oldest = now - datetime.timedelta(days=31 * _CLOSED_MONTHS_KEPT)
            for stale in [k for k in closed if k[2] < oldest]:
                del closed[stale]
        return dict(totals)

    def _in_units(self, totals: dict) -> dict:
        return {
            name: _to_usage_units(
                Decimal(totals.get(field, 0))
                / Decimal(str(self.backend_components[name].get("unit_factor", 1)))
            )
            for name, field in self._traffic.items()
        }

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Storage levels now and traffic since the start of the billing period.

        A part that cannot be read is left out, never reported as zero: storage
        then keeps its checkpoint, so the next pass bills the gap, and traffic
        keeps the figure Waldur holds.
        """
        # One clock reading per pass: a period start and a now taken at different
        # instants would pair one month's start with the next month's now.
        now = self._now()
        start = self._period_start(now)
        report: dict[str, dict[str, dict[str, float]]] = {}
        self._reported_usernames = set()
        # Owners kept from an earlier period (a resource since deleted, say) are
        # read afresh if they come back, so their totals need not be kept.
        settled = self._cluster.settled
        for owner in [o for o, kept in settled.items() if kept[0] < start]:
            del settled[owner]
        last = self._cluster.last_totals
        for owner in [o for o, kept in last.items() if kept[0] < start]:
            del last[owner]

        storage: dict[str, dict] = {}
        traffic: dict[str, dict] = {}
        grew = False
        for username in resource_backend_ids:
            if self._accrued:
                try:
                    storage[username] = self._storage_level(username)
                except Exception as e:
                    logger.error("Failed to read the buckets of %s: %s", username, e)
            if self._traffic:
                created = self._created.get(username)
                try:
                    owner = self._usage_owner(username, created)
                    window = self._window_start(start, created)
                    totals = self._period_to_date(owner, window, now)
                    traffic[username] = self._in_units(totals)
                    previous = self._cluster.last_totals.get(owner)
                    if previous is not None and previous[0] == window:
                        grew = grew or totals.get("ops", 0) > previous[1].get("ops", 0)
                    self._cluster.last_totals[owner] = (window, totals)
                except Exception as e:
                    logger.error("Failed to read the usage log for %s: %s", username, e)

        if traffic:
            # Judged on what was logged since the previous pass, not the month so
            # far: traffic from before the log was switched off would otherwise
            # pass for a working log all month.
            self._note_log_state(grew, now)

        for username in resource_backend_ids:
            usage = {**storage.get(username, {}), **traffic.get(username, {})}
            if usage:
                report[username] = {"TOTAL_ACCOUNT_USAGE": usage}
                self._reported_usernames.add(username)
                logger.debug("Usage for %s: %s", username, usage)

        logger.info("Collected usage report for %d users", len(report))
        return report

    def _note_log_state(self, saw_traffic: bool, now: datetime.datetime) -> None:
        """Warn when the usage log looks unwritten, since that reads as zero traffic.

        Ceph ships with ``rgw_enable_usage_log`` off, and an unwritten log reads
        exactly like tenants that sent nothing. Zeros are still reported -- an
        empty log is not a failed read, and a quiet month is real -- but when every
        resource reads zero, the last day of the log is checked across every owner,
        and an empty one is reported to the operator. Checked at most hourly, and
        not at all while the log has been seen written within the last day.
        """
        cluster = self._cluster
        if saw_traffic:
            cluster.log_seen_at = now
            return
        if cluster.log_seen_at is not None and now - cluster.log_seen_at < _LOG_CHECK_WINDOW:
            return
        if cluster.log_checked_at is not None and now - cluster.log_checked_at < _LOG_CHECK_EVERY:
            return
        cluster.log_checked_at = now
        try:
            written = self.client.usage_log_has_entries(now - _LOG_CHECK_WINDOW, now)  # type: ignore[attr-defined]
        except Exception as e:
            logger.warning("Could not check whether RGW's usage log is written: %s", e)
            return
        if written:
            cluster.log_seen_at = now
            return
        logger.warning(
            "RGW's usage log holds nothing for any owner in the last day, so every "
            "resource reports zero traffic and requests. If that is not true, check "
            "that rgw_enable_usage_log is on for every gateway"
        )

    def _month_log_written(self, start: datetime.datetime, end: datetime.datetime) -> bool:
        """Whether the usage log holds anything, for any owner, in a finished month.

        Asked only when every resource read zero for it, and once per month: the
        answer for a finished month does not change.
        """
        month_log = self._cluster.month_log
        key = (start, end)
        if key not in month_log:
            month_log[key] = bool(
                self.client.usage_log_has_entries(start, end)  # type: ignore[attr-defined]
            )
            written = month_log[key]
            oldest = self._now() - datetime.timedelta(days=31 * _CLOSED_MONTHS_KEPT)
            for stale in [k for k in month_log if k[1] < oldest and k != key]:
                del month_log[stale]
            return written
        return month_log[key]

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list[str],
        year: int,
        month: int,
        waldur_resource: WaldurResource | None = None,
    ) -> dict:
        """A past month's traffic, read again from the usage log.

        Storage is not in it: it was accrued live, and the reporting processor
        closes the month at its end from the checkpoint in Waldur. For the
        historical loader a resource that cannot be read raises rather than being
        left out, which the loader would read as no usage at all; so does a month
        the usage log holds nothing for, for anyone, since its zeros were never
        measured.
        """
        report: dict = {}
        start, end = (
            datetime.datetime.fromtimestamp(bound, datetime.timezone.utc)
            for bound in self._period_bounds(year, month)
        )
        created = _aware(getattr(waldur_resource, "created", None))
        if created is not None and created >= end:
            # The resource did not exist yet: nothing to report, not zeros.
            return report
        if not self._traffic:
            if self.historical_load:
                raise BackendError(
                    "rgw_usage accrues storage as it happens and keeps no history "
                    f"of it, so {year}-{month:02d} cannot be recomputed"
                )
            return report

        unread = []
        for username in resource_backend_ids:
            try:
                owner = self._usage_owner(username, created)
                totals = self._finished_month(owner, self._window_start(start, created), end)
                report[username] = {"TOTAL_ACCOUNT_USAGE": self._in_units(totals)}
            except Exception as e:
                logger.error(
                    "Failed to read the usage log for %s in %d-%02d: %s",
                    username,
                    year,
                    month,
                    e,
                )
                unread.append(username)
        if unread and self.historical_load:
            raise BackendError(
                f"Could not read the RGW usage log for {year}-{month:02d} for "
                f"{', '.join(unread)}; refusing to report that period rather than "
                "have it read as zero"
            )

        if report and not any(
            any(entry["TOTAL_ACCOUNT_USAGE"].values()) for entry in report.values()
        ):
            try:
                written = self._month_log_written(start, end)
            except Exception as e:
                logger.warning("Could not check whether RGW's usage log is written: %s", e)
                written = True
            if not written:
                message = (
                    f"RGW's usage log holds nothing for any owner in {year}-{month:02d}, "
                    "so its traffic and requests were not measured; check that "
                    "rgw_enable_usage_log is on for every gateway"
                )
                if self.historical_load:
                    raise BackendError(f"{message}. Refusing to report that month as zero")
                logger.warning(message)
        return report
