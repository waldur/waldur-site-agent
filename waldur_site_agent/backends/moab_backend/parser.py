"""Parsing classes for MOAB."""

import decimal
from functools import cached_property
from typing import Dict


class MoabReportLine:
    """Parser for report lines from MOAB."""

    def __init__(self, line: str) -> None:
        """Constructor."""
        self._parts = line.split("|")

    @cached_property
    def account(self) -> str:
        """Parses account name."""
        return self._parts[0].strip()

    @cached_property
    def user(self) -> str:
        """Parses username."""
        return self._parts[1]

    @cached_property
    def charge(self) -> decimal.Decimal:
        """Parses charge."""
        return decimal.Decimal(self._parts[2])

    @cached_property
    def usages(self) -> Dict[str, int]:
        """Return deposit usage from the report line."""
        return {"deposit": int(self.charge)}
