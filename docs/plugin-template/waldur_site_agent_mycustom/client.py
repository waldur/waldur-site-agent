"""Custom client implementation.

This module implements the BaseClient interface for communicating
with the custom backend system (CLI commands or API calls).
"""

from typing import Optional

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.structures import Association, ClientResource


class MyCustomClient(BaseClient):
    """Client for communicating with the custom backend system.

    TODO: Replace with description of your backend communication method.

    For CLI-based backends, use self.execute_command() from BaseClient.
    For API-based backends, use requests/httpx or similar.
    """

    def __init__(self) -> None:
        """Initialize the client.

        TODO: Accept and store connection parameters (API URL, credentials, etc.).
        """

    def list_resources(self) -> list[ClientResource]:
        """List all resources on the backend.

        TODO: Query backend and return ClientResource objects.
        Each must have 'name' (backend_id) and 'organization' (parent_id).
        """
        # TODO: Implement
        # Example for CLI: output = self.execute_command(["my-tool", "list"])
        # Example for API: response = requests.get(f"{self.api_url}/resources")
        return []

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get a single resource by its backend ID.

        Must return None if the resource does not exist (do not raise).

        TODO: Query backend for the specific resource.
        """
        # TODO: Implement
        del resource_id
        return None

    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create a new resource on the backend.

        TODO: Create the resource and return confirmation.
        """
        # TODO: Implement resource creation
        del description, organization, parent_name
        return name

    def delete_resource(self, name: str) -> str:
        """Delete a resource from the backend.

        TODO: Delete the resource and return confirmation.
        """
        # TODO: Implement
        return name

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set resource limits on the backend.

        limits_dict values are in backend-native units (already multiplied by unit_factor).

        TODO: Apply limits to the backend resource.
        """
        # TODO: Implement
        del resource_id, limits_dict
        return None

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get current resource limits from the backend.

        TODO: Query and return limits in backend-native units.
        """
        # TODO: Implement
        del resource_id
        return {}

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for a resource.

        TODO: Return {username: {component: value}} dict.
        """
        # TODO: Implement
        del resource_id
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set limits for a specific user on a resource.

        TODO: Apply per-user limits on the backend.
        """
        # TODO: Implement
        del resource_id, username, limits_dict
        return ""

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Check if a user-resource association exists.

        Must return None if no association exists (do not raise).

        TODO: Query backend for the association.
        """
        # TODO: Implement
        del user, resource_id
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create an association between a user and a resource.

        TODO: Create the association on the backend.
        """
        # TODO: Implement
        del resource_id, default_account
        return username

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete an association between a user and a resource.

        TODO: Remove the association on the backend.
        """
        # TODO: Implement
        del resource_id
        return username

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get raw usage records from the backend.

        Returns backend-specific data that will be processed by
        BaseBackend._get_usage_report() into the standard format.

        TODO: Fetch raw usage data from the backend.
        """
        # TODO: Implement
        del resource_ids
        return []

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List all users associated with a resource.

        TODO: Query backend and return list of usernames.
        """
        # TODO: Implement
        del resource_id
        return []
