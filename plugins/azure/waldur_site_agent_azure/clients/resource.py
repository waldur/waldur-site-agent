"""Subscriptions and resource groups."""

from __future__ import annotations

import functools
from typing import Any, Iterable, Optional

from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient

from .base import BaseAzureClient, wrap_azure_errors


class ResourceClient(BaseAzureClient):
    """Resource Manager calls: locations and resource groups."""

    @functools.cached_property
    def subscription_client(self) -> SubscriptionClient:
        """Return the subscription-scoped management client."""
        return SubscriptionClient(self.credentials.credential)

    @functools.cached_property
    def resource_client(self) -> ResourceManagementClient:
        """Return the resource-group-scoped management client."""
        return ResourceManagementClient(self.credentials.credential, self.subscription_id)

    @wrap_azure_errors
    def ping(self) -> bool:
        """Report whether the configured subscription can be read.

        Fetching the subscription exercises both the credential and the
        subscription id, which is what separates a usable configuration from one
        that merely parses.
        """
        self.subscription_client.subscriptions.get(self.subscription_id)
        return True

    @wrap_azure_errors
    def list_locations(self) -> Iterable[Any]:
        """List the regions available to the subscription."""
        return self.subscription_client.subscriptions.list_locations(self.subscription_id)

    @wrap_azure_errors
    def get_resource_group_locations(self) -> Optional[list[str]]:
        """Return the regions that accept resource groups.

        Resource Manager itself is available everywhere, but a subscription may
        still be barred from individual regions, so this is narrower than
        ``list_locations``.
        """
        provider = self.resource_client.providers.get("Microsoft.Resources")
        for resource in provider.resource_types:
            if resource.resource_type == "resourceGroups":
                return resource.locations
        return None

    @wrap_azure_errors
    def list_resource_groups(self) -> Iterable[Any]:
        """List the resource groups in the subscription."""
        return self.resource_client.resource_groups.list()

    @wrap_azure_errors
    def create_resource_group(self, location: str, resource_group_name: str) -> Any:
        """Create a resource group, or update its location if it exists."""
        return self.resource_client.resource_groups.create_or_update(
            resource_group_name, {"location": location}
        )

    @wrap_azure_errors
    def delete_resource_group(self, resource_group_name: str) -> Any:
        """Start deletion of a resource group and return the poller."""
        return self.resource_client.resource_groups.begin_delete(resource_group_name)
