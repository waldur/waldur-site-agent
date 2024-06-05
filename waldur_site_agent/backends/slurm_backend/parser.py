"""Parser module for SLURM backend."""

import datetime
from functools import cached_property
from typing import Dict

from . import utils


def parse_duration(value: str) -> float:
    """Returns duration in minutes as a float number.

    For example:
    00:01:00 is equal to 1.0
    00:00:03 is equal to 0.05
    00:01:03 is equal to 1.05
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
            delta = datetime.timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second)
    elif us_sep in value:  # Simple time with microseconds and days
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
        delta = datetime.timedelta(days=dt.day, hours=dt.hour, minutes=dt.minute, seconds=dt.second)

    return int(delta.total_seconds()) / 60


class SlurmReportLine:
    """Class for SLURM report line parsing."""

    def __init__(self, line: str, slurm_tres: Dict) -> None:
        """Inits parts field from the specified line."""
        self._parts = line.split("|")
        self.slurm_tres = slurm_tres

    @cached_property
    def account(self) -> str:
        """Returns account name from the report line."""
        return self._parts[0].strip()

    @cached_property
    def user(self) -> str:
        """Returns username from the report line."""
        return self._parts[3]

    @cached_property
    def duration(self) -> float:
        """Shortcut for parse_duration function."""
        return parse_duration(self._parts[2])

    @cached_property
    def _resources(self) -> Dict:
        pairs = self._parts[1].split(",")
        return dict(pair.split("=") for pair in pairs)

    def parse_field(self, field: str) -> int:
        """Parses integer field."""
        if field not in self._resources:
            return 0
        return utils.parse_int(self._resources[field])

    @cached_property
    def tres_usage(self) -> Dict:
        """TRES usage for the line."""
        usage = {}
        slurm_tres_set = set(self.slurm_tres.keys())
        for resource in self._resources:
            if resource not in slurm_tres_set:
                continue
            usage_raw = self.parse_field(resource)
            usage[resource] = usage_raw * self.duration
        if "mem" in usage:
            usage["mem"] = usage["mem"] // 2**20  # Convert from Bytes to MB
        return usage


class SlurmAssociationLine(SlurmReportLine):
    """Class for SLURM association line parsing."""

    @cached_property
    def user(self) -> str:
        """Always empty for association lines."""
        return ""

    @cached_property
    def duration(self) -> float:
        """Always empty for association lines."""
        return 0.0

    @cached_property
    def _resources(self) -> Dict:
        if self._parts[1] != "":
            pairs = self._parts[1].split(",")
            return dict(pair.split("=") for pair in pairs)
        return {}

    @cached_property
    def tres_limits(self) -> Dict:
        """TRES limits in the line."""
        resources = self._resources
        limits = {}
        slurm_tres_set = set(self.slurm_tres.keys())

        for tres, limit in resources.items():
            if tres not in slurm_tres_set:
                continue
            if tres == "mem":
                limits[tres] = utils.parse_int(limit) // 2**20  # Convert from Bytes to MB
            else:
                limits[tres] = utils.parse_int(limit)

        return limits
