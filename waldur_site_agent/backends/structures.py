"""Common structures shared between different backends."""

from dataclasses import dataclass, field


@dataclass
class Account:
    """Account model for SLURM and MOAB."""

    name: str = ""
    description: str = ""
    organization: str = ""


@dataclass
class Association:
    """Association between a user and an account in SLURM or MOAB."""

    account: str = ""
    user: str = ""
    value: int = 0


@dataclass
class Resource:
    """Common resource structure."""

    backend_type: str = ""
    name: str = ""
    marketplace_uuid: str = ""
    backend_id: str = ""
    limits: dict = field(default_factory=dict)
    restrict_member_access: bool = False
    downscaled: bool = False
    paused: bool = False
    users: list[str] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    state: str = ""
