"""Functions shared between different backends."""

import calendar
import datetime
from zoneinfo import ZoneInfo

import yaml


def month_start(date: datetime.datetime) -> datetime.datetime:
    """Returns first day of a month for the date."""
    return datetime.datetime(day=1, month=date.month, year=date.year)


def month_end(date: datetime.datetime) -> datetime.date:
    """Returns last day of a month for the date."""
    days_in_month = calendar.monthrange(date.year, date.month)[1]
    return datetime.date(month=date.month, year=date.year, day=days_in_month)


def get_current_time_in_timezone(timezone_str: str = "") -> datetime.datetime:
    """Returns current time in the specified timezone.

    Or system timezone if no timezone is specified.
    """
    if timezone_str:
        try:
            tz = ZoneInfo(timezone_str)
            return datetime.datetime.now(tz)
        except Exception:
            return datetime.datetime.now()
    else:
        return datetime.datetime.now()


def format_current_month(timezone_str: str = "") -> tuple[str, str]:
    """Returns strings for start and end date of the current month."""
    today = get_current_time_in_timezone(timezone_str)
    start = month_start(today).strftime("%Y-%m-%dT00:00:00")
    end = month_end(today).strftime("%Y-%m-%dT23:59:59")
    return start, end


def get_usage_based_limits(resource_limits: dict) -> dict[str, int]:
    """Returns dictionary of limits for usage-based computing resources.

    The limits converted to SLURM-readable values.
    I.e. CPU-minutes, MB-minutes.
    """
    return {
        tres: data["limit"] * data.get("unit_factor", 1)
        for tres, data in resource_limits.items()
        if data["accounting_type"] == "usage" and data.get("limit") is not None
    }


def prettify_limits(limits: dict[str, int], slurm_tres: dict) -> str:
    """Makes limits human-readable."""
    limits_info = {
        slurm_tres[key]["label"]: f"{value} {slurm_tres[key]['measured_unit']}"
        for key, value in limits.items()
    }
    return yaml.dump(limits_info)


def format_month_period(year: int, month: int) -> tuple[str, str]:
    """Returns strings for start and end date of a specific month.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)

    Returns:
        Tuple of (start_date_str, end_date_str) formatted for SLURM sacct
    """
    # Create datetime for first day of month
    start_date = datetime.datetime(year=year, month=month, day=1)

    # Get last day of month
    days_in_month = calendar.monthrange(year, month)[1]
    end_date = datetime.date(year=year, month=month, day=days_in_month)

    start = start_date.strftime("%Y-%m-%dT00:00:00")
    end = end_date.strftime("%Y-%m-%dT23:59:59")
    return start, end


def generate_monthly_periods(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[tuple[int, int, str, str]]:
    """Generate list of monthly periods between start and end dates.

    Args:
        start_year: Starting year
        start_month: Starting month (1-12)
        end_year: Ending year
        end_month: Ending month (1-12)

    Returns:
        List of tuples: (year, month, start_date_str, end_date_str)
    """
    periods = []
    current_year = start_year
    current_month = start_month

    while current_year < end_year or (current_year == end_year and current_month <= end_month):
        start_str, end_str = format_month_period(current_year, current_month)
        periods.append((current_year, current_month, start_str, end_str))

        # Move to next month
        months_per_year = 12
        current_month += 1
        if current_month > months_per_year:
            current_month = 1
            current_year += 1

    return periods


def sum_dicts(dict_list: list[dict]) -> dict:
    """Sums dictionaries by keys."""
    result_dict: dict[str, int] = {}

    # Iterate through each dictionary in the list
    for curr_dict in dict_list:
        # Sum values for each key in the current dictionary
        for key, value in curr_dict.items():
            result_dict[key] = result_dict.get(key, 0) + value

    return result_dict
