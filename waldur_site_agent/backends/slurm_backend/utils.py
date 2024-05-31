"""Utils for SLURM backend."""

import calendar
import datetime
import pprint
import re
from typing import Dict, List, Tuple

import yaml
from waldur_client import OfferingComponent, WaldurClient

from waldur_site_agent import WALDUR_SITE_AGENT_MODE, AgentMode
from waldur_site_agent.backends import logger
from waldur_site_agent.backends.exceptions import BackendError

from . import (
    SLURM_ALLOCATION_PREFIX,
    SLURM_CONTAINER_NAME,
    SLURM_CUSTOMER_PREFIX,
    SLURM_DEFAULT_ACCOUNT,
    SLURM_DEPLOYMENT_TYPE,
    SLURM_PROJECT_PREFIX,
    SLURM_TRES,
)
from .backend import SlurmBackend

UNIT_PATTERN = re.compile(r"(\d+)([KMGTP]?)")

UNITS: Dict[str, int] = {
    "K": 2**10,
    "M": 2**20,
    "G": 2**30,
    "T": 2**40,
}

SLURM_ALLOCATION_REGEX = "a-zA-Z0-9-_"
SLURM_ALLOCATION_NAME_MAX_LEN = 34


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


def sanitize_allocation_name(name: str) -> str:
    """Formats allocation name respecting configured regex."""
    incorrect_symbols_regex = rf"[^{SLURM_ALLOCATION_REGEX}]+"
    return re.sub(incorrect_symbols_regex, "", name)


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


def get_tres_list() -> List[str]:
    """Returns list of TRES."""
    return SLURM_TRES.keys()


def get_slurm_tres_limits() -> Dict[str, int]:
    """Returns dictionary of limits for usage-based TRES from config file.

    The limits converted to SLURM-readable values.
    I.e. CPU-minutes, MB-minutes.
    """
    return {
        tres: data["limit"] * data["unit_factor"]
        for tres, data in SLURM_TRES.items()
        if data["accounting_type"] == "usage"
    }


def sum_dicts(dict_list: List[Dict]) -> Dict[str, int]:
    """Sums dictionaries by keys."""
    result_dict: Dict[str, int] = {}

    # Iterate through each dictionary in the list
    for curr_dict in dict_list:
        # Sum values for each key in the current dictionary
        for key, value in curr_dict.items():
            result_dict[key] = result_dict.get(key, 0) + value

    return result_dict


def prettify_limits(limits: Dict[str, int]) -> str:
    """Makes limits human-readable."""
    limits_info = {
        SLURM_TRES[key]["label"]: f"{value} {SLURM_TRES[key]['measured_unit']}"
        for key, value in limits.items()
    }
    return yaml.dump(limits_info)


def diagnostics() -> bool:
    """Runs diagnostics for SLURM cluster."""
    slurm_backend = SlurmBackend()
    format_string = "{:<30} = {:<10}"
    logger.info(format_string.format("SLURM_DEPLOYMENT_TYPE", SLURM_DEPLOYMENT_TYPE))
    logger.info(
        format_string.format("SLURM_ALLOCATION_NAME_MAX_LEN", SLURM_ALLOCATION_NAME_MAX_LEN)
    )
    logger.info(format_string.format("SLURM_CUSTOMER_PREFIX", SLURM_CUSTOMER_PREFIX))
    logger.info(format_string.format("SLURM_PROJECT_PREFIX", SLURM_PROJECT_PREFIX))
    logger.info(format_string.format("SLURM_ALLOCATION_PREFIX", SLURM_ALLOCATION_PREFIX))
    logger.info(format_string.format("SLURM_DEFAULT_ACCOUNT", SLURM_DEFAULT_ACCOUNT))
    logger.info(format_string.format("SLURM_CONTAINER_NAME", SLURM_CONTAINER_NAME))
    logger.info("")

    logger.info("SLURM tres config file content:\n%s\n", pprint.pformat(SLURM_TRES))

    if AgentMode.ORDER_PROCESS.value == WALDUR_SITE_AGENT_MODE:
        logger.info(
            "Agent is running in %s mode - "
            "pulling orders from Waldur and creating resources in backend",
            AgentMode.ORDER_PROCESS.name,
        )
    if AgentMode.REPORT.value == WALDUR_SITE_AGENT_MODE:
        logger.info(
            "Agent is running in %s mode - pushing usage data to Waldur",
            AgentMode.REPORT.name,
        )
    if AgentMode.MEMBERSHIP_SYNC.value == WALDUR_SITE_AGENT_MODE:
        logger.info(
            "Agent is running in %s mode - pushing membership data to Waldur",
            AgentMode.MEMBERSHIP_SYNC.name,
        )

    try:
        slurm_version_info = slurm_backend.client._execute_command(
            ["-V"], "sinfo", immediate=False, parsable=False
        )
        logger.info("Slurm version: %s", slurm_version_info.strip())
    except BackendError as err:
        logger.error("Unable to fetch SLURM info, reason: %s", err)
        return False

    try:
        slurm_backend.ping(raise_exception=True)
        logger.info("SLURM cluster ping is successful")
    except BackendError as err:
        logger.error("Unable to ping SLURM cluster, reason: %s", err)

    tres = slurm_backend.list_components()
    logger.info("Available tres in the cluster: %s", ",".join(tres))

    default_account = slurm_backend.client.get_account(SLURM_DEFAULT_ACCOUNT)
    if default_account is None:
        logger.error("There is no account %s in the cluster", default_account)
        return False
    logger.info('Default parent account "%s" is in place', SLURM_DEFAULT_ACCOUNT)
    logger.info("")

    return True


def create_offering_components(
    waldur_rest_client: WaldurClient, offering_uuid: str, offering_name: str
) -> None:
    """Creates offering components for SLURM in Waldur."""
    logger.info(
        "Creating offering components data for the following TRES: %s",
        ", ".join(SLURM_TRES.keys()),
    )
    for tres_type, tres_info in SLURM_TRES.items():
        component = OfferingComponent(
            billing_type=tres_info["accounting_type"],
            type=tres_type,
            name=tres_info["label"],
            measured_unit=tres_info["measured_unit"],
            limit_amount=tres_info["limit"],
        )
        try:
            waldur_rest_client.create_offering_component(offering_uuid, component)
        except Exception as e:
            logger.info(
                "Unable to create a component %s for offering %s (%s):",
                tres_info["label"],
                offering_name,
                offering_uuid,
            )
            logger.exception(e)


def convert_slurm_units_to_waldur_ones(units: Dict, to_int: bool = False) -> Dict:
    """Converts SLURM computing units to Waldur ones."""
    converted_units = {}

    for tres_name, value in units.items():
        converted_value = value
        converted_value = converted_value / SLURM_TRES[tres_name].get("unit_factor", 1)

        if to_int:
            converted_units[tres_name] = int(converted_value)
        else:
            converted_units[tres_name] = round(converted_value, 2)

    return converted_units
