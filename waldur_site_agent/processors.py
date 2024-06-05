"""Module for abstract offering processor."""

import abc

from waldur_client import WaldurClient

from waldur_site_agent import Offering, common_utils
from waldur_site_agent.backends import BackendType
from waldur_site_agent.backends.exceptions import BackendError


class OfferingBaseProcessor(abc.ABC):
    """Abstract class for an offering processing."""

    def __init__(self, offering: Offering, user_agent: str = "") -> None:
        """Constructor."""
        self.offering: Offering = offering
        self.waldur_rest_client: WaldurClient = WaldurClient(
            offering.api_url, offering.api_token, user_agent
        )
        self.resource_backend = common_utils.get_backend_for_offering(offering)
        if self.resource_backend.backend_type == BackendType.UNKNOWN.value:
            raise BackendError(f"Unable to create backend for {self.offering}")

    @abc.abstractmethod
    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""
