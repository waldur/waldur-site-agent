"""Common structures shared between different backends."""

from dataclasses import dataclass, field


@dataclass
class ClientResource:
    """Account model for SLURM and MOAB."""

    name: str = ""
    description: str = ""
    organization: str = ""


@dataclass
class Association:
    """Association between a user and a resource."""

    account: str = ""
    user: str = ""
    value: int = 0


@dataclass
class BackendResourceInfo:
    """Resource info from a backend."""

    backend_id: str = ""
    parent_id: str = ""
    users: list[str] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    limits: dict = field(default_factory=dict)
