"""Abstract classes and functions for username management backends."""

from typing import Optional

from waldur_api_client.models.offering_user import OfferingUser

from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend


class BaseUsernameManagementBackend(AbstractUsernameManagementBackend):
    """Class for basic username management backend."""

    def generate_username(self, offering_user: OfferingUser) -> str:
        """Generate username based on offering user details."""
        del offering_user
        return ""

    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Get username in local IDP if exists."""
        del offering_user
        return None
