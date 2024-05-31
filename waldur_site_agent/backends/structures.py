"""Common structures shared between different backends."""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Resource:
    """Common resource structure."""

    backend_type: str = ""
    name: str = ""
    marketplace_uuid: str = ""
    marketplace_scope_uuid: str = ""
    backend_id: str = ""
    limits: Dict = field(default_factory=dict)
    users: List[str] = field(default_factory=list)
    usage: Dict = field(default_factory=dict)
