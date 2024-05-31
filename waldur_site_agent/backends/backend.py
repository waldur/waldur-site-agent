"""Generic backend classes."""

from abc import ABC, abstractmethod
from typing import Dict, List, Set

from . import structures

UNKNOWN_BACKEND_TYPE = "unknown"


class BaseBackend(ABC):
    """Abstract backend class."""

    def __init__(self) -> None:
        """Inits backend-related info."""
        self.backend_type = "generic"

    @abstractmethod
    def ping(self, raise_exception: bool = False) -> bool:
        """Checks if backend is online."""

    @abstractmethod
    def list_components(self) -> List[str]:
        """Returns a list of computing components on the backend."""

    @abstractmethod
    def pull_resources(
        self, resources_info: List[structures.Resource]
    ) -> Dict[str, structures.Resource]:
        """Pulls data of resources available in the backend."""

    @abstractmethod
    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Deletes resource on the backend and returns list of linked users."""

    @abstractmethod
    def create_resource(self, waldur_resource: Dict) -> structures.Resource:
        """Creates resource on the backend."""

    @abstractmethod
    def add_users_to_resource(self, resource_backend_id: str, user_ids: Set[str]) -> Set[str]:
        """Adds specified users to the resource on the backend."""

    @abstractmethod
    def set_resource_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        """Sets limits for the resource on the backend."""


class UnknownBackend(BaseBackend):
    """Common class for unknown backends."""

    def __init__(self) -> None:
        """Placeholder."""
        super().__init__()
        self.backend_type = UNKNOWN_BACKEND_TYPE

    def ping(self, _: bool = False) -> bool:
        """Placeholder."""
        return False

    def list_components(self) -> List[str]:
        """Placeholder."""
        return []

    def pull_resources(self, _: List[structures.Resource]) -> Dict[str, structures.Resource]:
        """Placeholder."""
        return {}

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Placeholder."""
        del kwargs, resource_backend_id

    def create_resource(self, _: Dict) -> structures.Resource:
        """Placeholder."""
        return structures.Resource()

    def add_users_to_resource(self, _: str, user_ids: Set[str]) -> Set[str]:
        """Placeholder."""
        return user_ids

    def set_resource_limits(self, _: str, limits: Dict[str, int]) -> None:
        """Placeholder."""
        del limits
