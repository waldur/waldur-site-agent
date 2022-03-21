import datetime
import logging
from functools import cache

from . import base, utils

logger = logging.getLogger(__name__)


def parse_duration(value):
    """
    Returns duration in minutes as an integer number.
    For example 00:01:00 is equal to 1
    """
    days_sep = "-"
    us_sep = "."
    simple_fmt = "%H:%M:%S"
    days_fmt = "%d-%H:%M:%S"
    us_fmt = "%H:%M:%S.%f"
    days_us_fmt = "%d-%H:%M:%S.%f"

    if days_sep not in value:
        if us_sep in value:  # Simple time with microseconds
            dt = datetime.datetime.strptime(value, us_fmt)
            delta = datetime.timedelta(
                hours=dt.hour,
                minutes=dt.minute,
                seconds=dt.second,
                microseconds=dt.microsecond,
            )
        else:  # Simple time
            dt = datetime.datetime.strptime(value, simple_fmt)
            delta = datetime.timedelta(
                hours=dt.hour, minutes=dt.minute, seconds=dt.second
            )
    else:
        if us_sep in value:  # Simple time with microseconds and days
            dt = datetime.datetime.strptime(value, days_us_fmt)
            delta = datetime.timedelta(
                days=dt.day,
                hours=dt.hour,
                minutes=dt.minute,
                seconds=dt.second,
                microseconds=dt.microsecond,
            )
        else:  # Simple time with days
            dt = datetime.datetime.strptime(value, days_fmt)
            delta = datetime.timedelta(
                days=dt.day, hours=dt.hour, minutes=dt.minute, seconds=dt.second
            )

    return int(delta.total_seconds()) // 60


class SlurmReportLine(base.BaseReportLine):
    def __init__(self, line):
        self._parts = line.split("|")

    @cache
    def account(self):
        return self._parts[0].strip()

    @cache
    def user(self):
        return self._parts[3]

    @cache
    def cpu(self):
        return self.parse_field("cpu")

    @cache
    def gpu(self):
        return self.parse_field("gres/gpu")

    @cache
    def ram(self):
        return self.parse_field("mem") // 2 ** 20  # Convert from Bytes to MB

    @cache
    def node(self):
        return self.parse_field("node")

    @cache
    def duration(self):
        return parse_duration(self._parts[2])

    @cache
    def _resources(self):
        pairs = self._parts[1].split(",")
        return dict(pair.split("=") for pair in pairs)

    def parse_field(self, field):
        if field not in self._resources:
            return 0
        return utils.parse_int(self._resources[field])


class SlurmAssociationLine(SlurmReportLine):
    @cache
    def user(self):
        return None

    @cache
    def node(self):
        return None

    @cache
    def duration(self):
        return None

    @cache
    def _resources(self):
        if self._parts[1] != "":
            pairs = self._parts[1].split(",")
            return dict(pair.split("=") for pair in pairs)

    @cache
    def resource_limits(self):
        return self._resources
