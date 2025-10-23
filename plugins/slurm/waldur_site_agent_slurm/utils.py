"""Utils for SLURM backend."""


def convert_slurm_units_to_waldur_ones(slurm_tres: dict, units: dict, to_int: bool = False) -> dict:
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
