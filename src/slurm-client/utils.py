import calendar
import datetime
import re

UNIT_PATTERN = re.compile(r"(\d+)([KMGTP]?)")

UNITS = {
    "K": 2 ** 10,
    "M": 2 ** 20,
    "G": 2 ** 30,
    "T": 2 ** 40,
}

MAPPING = {
    "cpu_usage": "nc_cpu_usage",
    "gpu_usage": "nc_gpu_usage",
    "ram_usage": "nc_ram_usage",
}

SLURM_ALLOCATION_REGEX = "a-zA-Z0-9-_"
SLURM_ALLOCATION_NAME_MAX_LEN = 34

FIELD_NAMES = MAPPING.keys()

QUOTA_NAMES = MAPPING.values()


def month_start(date):
    return datetime.datetime(day=1, month=date.month, year=date.year)


def month_end(date):
    days_in_month = calendar.monthrange(date.year, date.month)[1]
    return datetime.date(month=date.month, year=date.year, day=days_in_month)


def format_current_month():
    today = datetime.now()
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
