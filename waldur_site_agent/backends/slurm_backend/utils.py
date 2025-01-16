"""Utils for SLURM backend."""

from typing import Dict


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
