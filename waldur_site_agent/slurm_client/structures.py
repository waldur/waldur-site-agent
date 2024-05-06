import collections
from dataclasses import dataclass

Account = collections.namedtuple("Account", ["name", "description", "organization"])
Association = collections.namedtuple("Association", ["account", "user", "value"])


@dataclass
class Allocation:
    name: str = ""
    uuid: str = ""
    marketplace_uuid: str = ""
    backend_id: str = ""
    project_uuid: str = ""
    customer_uuid: str = ""
