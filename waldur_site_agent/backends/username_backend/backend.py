"""Abstract classes and functions for username management backends."""

from abc import ABC, abstractmethod
from typing import Optional

from waldur_api_client.models.offering_user import OfferingUser

from waldur_site_agent.backends import logger


class AbstractUsernameManagementBackend(ABC):
    """Base class for username management backends."""

    @abstractmethod
    def generate_username(self, offering_user: OfferingUser) -> str:
        """Generate username based on offering user details."""

    @abstractmethod
    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Get username in local IDP if exists."""

    def get_or_create_username(self, offering_user: OfferingUser) -> str:
        """Get username from local IDP if exists, otherwise request generation."""
        logger.info(
            "Retrieving username for offering user %s (email %s) from the backend",
            offering_user["uuid"],
            offering_user["email"],
        )
        username = self.get_username(offering_user)
        if username:
            return username
        logger.info(
            "Generating username for offering user %s (email %s) in the backend",
            offering_user["uuid"],
            offering_user["email"],
        )
        return self.generate_username(offering_user)


class BaseUsernameManagementBackend(AbstractUsernameManagementBackend):
    """Base class for username management backends."""

    def generate_username(self, offering_user: OfferingUser) -> str:
        """Generate username based on offering user details."""
        del offering_user
        return ""

    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Get username in local IDP if exists."""
        del offering_user
        return None
