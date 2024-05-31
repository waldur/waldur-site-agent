"""Structures specific for SLURM backend."""

from dataclasses import dataclass


@dataclass
class Account:
    """SLURM account."""

    name: str = ""
    description: str = ""
    organization: str = ""


@dataclass
class Association:
    """SLURM association between a user and an account."""

    account: str = ""
    user: str = ""
    value: int = 0
