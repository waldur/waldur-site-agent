import collections
from dataclasses import dataclass

Account = collections.namedtuple("Account", ["name", "description", "organization"])
Association = collections.namedtuple("Association", ["account", "user", "value"])


@dataclass
class Allocation:
    name: str = ""
    uuid: str = ""
    backend_id: str = ""
    project_uuid: str = ""
    customer_uuid: str = ""


@dataclass
class Quotas:
    cpu: float = 0.0
    gpu: float = 0.0
    ram: float = 0.0

    def __add__(self, other):
        return Quotas(
            self.cpu + other.cpu,
            self.gpu + other.gpu,
            self.ram + other.ram,
        )

    def __str__(self):
        return "Quotas: CPU=%s, GPU=%s, RAM=%s" % (
            self.cpu,
            self.gpu,
            self.ram,
        )

    def __repr__(self) -> str:
        return self.__str__()
