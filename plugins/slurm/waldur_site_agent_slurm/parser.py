"""Parser module for SLURM backend."""

import datetime
import re
from functools import cached_property

UNIT_PATTERN = re.compile(r"(\d+)([KMGTP]?)")

UNITS: dict[str, int] = {
    "K": 2**10,
    "M": 2**20,
    "G": 2**30,
    "T": 2**40,
}

# Constants for time parsing
MIN_TIME_COMPONENTS = 3


def parse_int(value: str) -> int:
    """Converts human-readable integers to machine-readable ones.

    Example: 5K to 5000.
    """
    match = re.match(UNIT_PATTERN, value)
    if not match:
        return 0
    value_ = int(match.group(1))
    unit = match.group(2)
    factor = UNITS[unit] if unit else 1
    return factor * value_


def parse_duration(value: str) -> float:
    """Returns duration in minutes as a float number.

    For example:
    00:01:00 is equal to 1.0
    00:00:03 is equal to 0.05
    00:01:03 is equal to 1.05
    850:00:00 is equal to 51000.0 (supports large hour values)
    """
    days_sep = "-"
    us_sep = "."
    simple_fmt = "%H:%M:%S"
    days_fmt = "%d-%H:%M:%S"
    us_fmt = "%H:%M:%S.%f"
    days_us_fmt = "%d-%H:%M:%S.%f"

    if days_sep not in value:
        if us_sep in value:  # Simple time with microseconds
            try:
                dt = datetime.datetime.strptime(value, us_fmt)
                delta = datetime.timedelta(
                    hours=dt.hour,
                    minutes=dt.minute,
                    seconds=dt.second,
                    microseconds=dt.microsecond,
                )
            except ValueError:
                # Handle large hour values that exceed 24 hours
                parts = value.split(":")
                if len(parts) >= MIN_TIME_COMPONENTS:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    seconds_parts = parts[2].split(".")
                    seconds = int(seconds_parts[0])
                    microseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
                    delta = datetime.timedelta(
                        hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds
                    )
                else:
                    return 0.0
        else:  # Simple time
            try:
                dt = datetime.datetime.strptime(value, simple_fmt)
                delta = datetime.timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second)
            except ValueError:
                # Handle large hour values that exceed 24 hours (like 850:00:00)
                parts = value.split(":")
                if len(parts) >= MIN_TIME_COMPONENTS:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    seconds = int(parts[2])
                    delta = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
                else:
                    return 0.0
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

    def __init__(self, line: str, slurm_tres: dict) -> None:
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
    def _resources(self) -> dict:
        pairs = self._parts[1].split(",")
        resources = {}
        for pair in pairs:
            if "=" in pair:
                key, value = pair.split("=", 1)  # Split only on first =
                resources[key] = value
            # Skip pairs that don't contain = to avoid crashes
        return resources

    def parse_field(self, field: str) -> int:
        """Parses integer field."""
        if field not in self._resources:
            return 0
        return parse_int(self._resources[field])

    @cached_property
    def tres_usage(self) -> dict:
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
        """Returns an optional username in the association."""
        user_limit_association_len = 3
        return self._parts[2] if len(self._parts) >= user_limit_association_len else ""

    @cached_property
    def duration(self) -> float:
        """Always empty for association lines."""
        return 0.0

    @cached_property
    def _resources(self) -> dict:
        if self._parts[1] != "":
            pairs = self._parts[1].split(",")
            resources = {}
            for pair in pairs:
                if "=" in pair:
                    key, value = pair.split("=", 1)  # Split only on first =
                    resources[key] = value
                # Skip pairs that don't contain = to avoid crashes
            return resources
        return {}

    @cached_property
    def tres_limits(self) -> dict:
        """TRES limits in the line."""
        resources = self._resources
        limits = {}
        slurm_tres_set = set(self.slurm_tres.keys())

        for tres, limit in resources.items():
            if tres not in slurm_tres_set:
                continue
            if tres == "mem":
                limits[tres] = parse_int(limit) // 2**20  # Convert from Bytes to MB
            else:
                limits[tres] = parse_int(limit)

        return limits
