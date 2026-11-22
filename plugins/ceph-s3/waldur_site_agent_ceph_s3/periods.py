"""Billing periods as the reporting backends compute them."""

import datetime
import logging
import zoneinfo
from typing import Optional

logger = logging.getLogger(__name__)


class BillingPeriodMixin:
    """Month boundaries in the agent's reporting timezone.

    The reporting processor injects that timezone (``processors.py``:
    ``self.resource_backend.timezone = timezone``) and files usage by it, so the
    month a backend measures cannot drift from the month the usage lands in.
    """

    timezone: str

    def _reporting_tz(self) -> datetime.tzinfo:
        """The timezone the billing period is computed in.

        The configuration validator rejects a timezone that does not parse, so the
        fallback below is unreachable in a configured agent; it used to be the thing
        that made the two disagree.
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

    def _now(self) -> datetime.datetime:
        """Now, in the reporting timezone; a seam for tests."""
        return datetime.datetime.now(self._reporting_tz())

    def _period_start(self, now: datetime.datetime) -> datetime.datetime:
        """The start of the billing period (a calendar month) that ``now`` is in.

        Takes the instant rather than reading the clock, so a caller pairs it with
        the same now it measures to: two readings can straddle a month boundary.
        """
        local = now.astimezone(self._reporting_tz())
        return local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def _billing_period_start(self, now: Optional[datetime.datetime] = None) -> int:
        """Unix seconds at the start of the current billing period."""
        return int(self._period_start(now or self._now()).timestamp())

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
