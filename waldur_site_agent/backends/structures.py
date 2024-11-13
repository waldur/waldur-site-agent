"""Common structures shared between different backends."""

from dataclasses import dataclass, field
from typing import Dict, List


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
    marketplace_scope_uuid: str = ""
    backend_id: str = ""
    limits: Dict = field(default_factory=dict)
    restrict_member_access: bool = False
    downscaled: bool = False
    paused: bool = False
    users: List[str] = field(default_factory=list)
    usage: Dict = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)
    state: str = ""
