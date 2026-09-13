"""Azure Resource Manager clients, split by service.

``AzureClient`` is the single object the backend holds; the per-service clients
hang off it and build their SDK client on first use, so a client a run never
touches costs nothing.
"""

from __future__ import annotations

import functools

from .base import AzureClientError, AzureCredentials
from .compute import AzureImage, ComputeClient
from .network import NetworkClient
from .resource import ResourceClient

__all__ = [
    "AzureClient",
    "AzureClientError",
    "AzureCredentials",
    "AzureImage",
    "ComputeClient",
    "NetworkClient",
    "ResourceClient",
]


class AzureClient:
    """Facade over the per-service Azure clients."""

    def __init__(
        self, subscription_id: str, tenant_id: str, client_id: str, client_secret: str
    ) -> None:
        """Build the shared credentials the service clients authenticate with."""
        self.credentials = AzureCredentials(
            subscription_id=subscription_id,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

    @property
    def subscription_id(self) -> str:
        """Return the subscription the clients operate on."""
        return self.credentials.subscription_id

    @functools.cached_property
    def resource(self) -> ResourceClient:
        """Return the resource group and subscription client."""
        return ResourceClient(self.credentials)

    @functools.cached_property
    def compute(self) -> ComputeClient:
        """Return the virtual machine, image and disk client."""
        return ComputeClient(self.credentials)

    @functools.cached_property
    def network(self) -> NetworkClient:
        """Return the network client."""
        return NetworkClient(self.credentials)

    def ping(self) -> bool:
        """Report whether the configured subscription is reachable."""
        try:
            return self.resource.ping()
        except AzureClientError:
            return False
