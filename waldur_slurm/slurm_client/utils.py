import calendar
import datetime
import re
from typing import Dict

import yaml

from . import SLURM_TRES

UNIT_PATTERN = re.compile(r"(\d+)([KMGTP]?)")

UNITS = {
    "K": 2**10,
    "M": 2**20,
    "G": 2**30,
    "T": 2**40,
}

SLURM_ALLOCATION_REGEX = "a-zA-Z0-9-_"
SLURM_ALLOCATION_NAME_MAX_LEN = 34


def month_start(date):
    return datetime.datetime(day=1, month=date.month, year=date.year)


def month_end(date):
    days_in_month = calendar.monthrange(date.year, date.month)[1]
    return datetime.date(month=date.month, year=date.year, day=days_in_month)


def format_current_month():
    today = datetime.datetime.now()
    start = month_start(today).strftime("%Y-%m-%d")
    end = month_end(today).strftime("%Y-%m-%d")
    return start, end


def sanitize_allocation_name(name):
    incorrect_symbols_regex = r"[^%s]+" % SLURM_ALLOCATION_REGEX
    return re.sub(incorrect_symbols_regex, "", name)


def parse_int(value):
    """
    Convert 5K to 5000.
    """
    match = re.match(UNIT_PATTERN, value)
    if not match:
        return 0
    value = int(match.group(1))
    unit = match.group(2)
    if unit:
        factor = UNITS[unit]
    else:
        factor = 1
    return factor * value


def get_tres_list():
    return SLURM_TRES.keys()


def get_tres_limits():
    return {
        tres: data["limit"]
        for tres, data in SLURM_TRES.items()
        if data["accounting_type"] == "usage"
    }


def sum_dicts(dict_list):
    result_dict = {}

    # Iterate through each dictionary in the list
    for curr_dict in dict_list:
        # Sum values for each key in the current dictionary
        for key, value in curr_dict.items():
            result_dict[key] = result_dict.get(key, 0) + value

    return result_dict


def prettify_limits(limits: Dict[str, int]):
    limits_info = {
        SLURM_TRES[key]["label"]: " ".join(
            [str(value), SLURM_TRES[key]["measured_unit"]]
        )
        for key, value in limits.items()
    }
    return yaml.dump(limits_info)
