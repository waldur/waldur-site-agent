"""Utils for SLURM backend."""

import pprint
import re
from typing import Dict

from waldur_site_agent.backends import logger
from waldur_site_agent.backends.exceptions import BackendError

from .backend import SlurmBackend

UNIT_PATTERN = re.compile(r"(\d+)([KMGTP]?)")

UNITS: Dict[str, int] = {
    "K": 2**10,
    "M": 2**20,
    "G": 2**30,
    "T": 2**40,
}


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


def diagnostics(slurm_backend: SlurmBackend) -> bool:
    """Runs diagnostics for SLURM cluster."""
    default_account_name = slurm_backend.backend_settings["default_account"]

    format_string = "{:<30} = {:<10}"
    logger.info(
        format_string.format(
            "SLURM customer prefix", slurm_backend.backend_settings["customer_prefix"]
        )
    )
    logger.info(
        format_string.format(
            "SLURM project prefix", slurm_backend.backend_settings["project_prefix"]
        )
    )
    logger.info(
        format_string.format(
            "SLURM allocation prefix", slurm_backend.backend_settings["allocation_prefix"]
        )
    )
    logger.info(format_string.format("SLURM default account", default_account_name))
    logger.info("")

    logger.info("SLURM tres components:\n%s\n", pprint.pformat(slurm_backend.backend_components))

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

    default_account = slurm_backend.client.get_account(default_account_name)
    if default_account is None:
        logger.error("There is no account %s in the cluster", default_account)
        return False
    logger.info('Default parent account "%s" is in place', default_account_name)
    logger.info("")

    return True


def convert_slurm_units_to_waldur_ones(slurm_tres: Dict, units: Dict, to_int: bool = False) -> Dict:
    """Converts SLURM computing units to Waldur ones."""
    converted_units = {}

    for tres_name, value in units.items():
        converted_value = value
        converted_value = converted_value / slurm_tres[tres_name].get("unit_factor", 1)

        if to_int:
            converted_units[tres_name] = int(converted_value)
        else:
            converted_units[tres_name] = round(converted_value, 2)

    return converted_units
