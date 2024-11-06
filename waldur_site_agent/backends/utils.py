"""Functions shared between different backends."""

import calendar
import datetime
from typing import Dict, List, Tuple

import yaml


def month_start(date: datetime.datetime) -> datetime.datetime:
    """Returns first day of a month for the date."""
    return datetime.datetime(day=1, month=date.month, year=date.year)


def month_end(date: datetime.datetime) -> datetime.date:
    """Returns last day of a month for the date."""
    days_in_month = calendar.monthrange(date.year, date.month)[1]
    return datetime.date(month=date.month, year=date.year, day=days_in_month)


def format_current_month() -> Tuple[str, str]:
    """Returns strings for start and end date of the current month."""
    today = datetime.datetime.now()
    start = month_start(today).strftime("%Y-%m-%d")
    end = month_end(today).strftime("%Y-%m-%d")
    return start, end


def get_usage_based_limits(resource_limits: Dict) -> Dict[str, int]:
    """Returns dictionary of limits for usage-based computing resources.

    The limits converted to SLURM-readable values.
    I.e. CPU-minutes, MB-minutes.
    """
    return {
        tres: data["limit"] * data.get("unit_factor", 1)
        for tres, data in resource_limits.items()
        if data["accounting_type"] == "usage" and data.get("limit") is not None
    }


def prettify_limits(limits: Dict[str, int], slurm_tres: Dict) -> str:
    """Makes limits human-readable."""
    limits_info = {
        slurm_tres[key]["label"]: f"{value} {slurm_tres[key]['measured_unit']}"
        for key, value in limits.items()
    }
    return yaml.dump(limits_info)


def sum_dicts(dict_list: List[Dict]) -> Dict[str, int]:
    """Sums dictionaries by keys."""
    result_dict: Dict[str, int] = {}

    # Iterate through each dictionary in the list
    for curr_dict in dict_list:
        # Sum values for each key in the current dictionary
        for key, value in curr_dict.items():
            result_dict[key] = result_dict.get(key, 0) + value

    return result_dict
