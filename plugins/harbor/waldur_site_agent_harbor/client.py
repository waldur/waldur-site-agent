"""Harbor Client for waldur site agent.

This module provides HTTP client for communicating with Harbor
container registry API. It implements the BaseClient interface for
managing projects, quotas, and OIDC groups.
"""

import logging
from typing import Any, Optional
from urllib.parse import quote

import requests

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.structures import Association, ClientResource
from waldur_site_agent_harbor.exceptions import (
    HarborAPIError,
    HarborAuthenticationError,
    HarborProjectError,
    HarborQuotaError,
    HarborOIDCError,
)

logger = logging.getLogger(__name__)


class HarborClient(BaseClient):
    """Client for communicating with Harbor API v2.0."""

    def __init__(
        self, harbor_url: str, robot_username: str, robot_password: str
    ) -> None:
        """Initialize Harbor client with robot account credentials."""
        super().__init__()
        self.harbor_url = harbor_url.rstrip("/")
        self.api_base = f"{self.harbor_url}/api/v2.0"

        # Store credentials for direct requests (avoid session CSRF issues)
        self.auth = (robot_username, robot_password)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> requests.Response:
        """Make HTTP request to Harbor API with error handling."""
        # For absolute paths, use them directly; otherwise prepend api_base
        if endpoint.startswith("/api/"):
            url = f"{self.harbor_url}{endpoint}"
        else:
            url = f"{self.api_base}/{endpoint.lstrip('/')}"

        # Use direct requests with auth tuple (avoids session CSRF issues)
        if "headers" not in kwargs:
            kwargs["headers"] = self.headers.copy()
        else:
            # Merge with default headers
            merged_headers = self.headers.copy()
            merged_headers.update(kwargs["headers"])
            kwargs["headers"] = merged_headers

        kwargs["auth"] = self.auth

        try:
            response = requests.request(method, url, **kwargs)

            # Check for authentication errors specifically
            if response.status_code == 401:
                raise HarborAuthenticationError("Authentication failed with Harbor API")

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.ConnectionError):
                raise HarborAPIError(
                    f"Cannot connect to Harbor at {self.harbor_url}"
                ) from e

            # Get response details if available
            response_details = ""
            if hasattr(e, "response") and e.response is not None:
                try:
                    response_details = f" | Status: {e.response.status_code} | Body: {e.response.text[:500]}"
                except Exception:
                    response_details = " | Unable to get response details"

            logger.exception(
                "Harbor API request failed: %s %s%s",
                method,
                url,
                response_details,
            )
            raise HarborAPIError(f"API request failed: {e}{response_details}") from e

        return response

    def _parse_json_response(self, response: requests.Response) -> Any:
        """Safely parse JSON response with proper error handling."""
        if response.status_code == 204:  # No content
            return {}

        if not response.content:
            return {}

        try:
            return response.json()
        except ValueError as e:
            raise HarborAPIError(f"Invalid JSON response: {e}") from e

    def ping(self) -> bool:
        """Check if Harbor API is accessible."""
        try:
            response = self._make_request("GET", "/api/v2.0/health")
            return response.status_code == 200
        except (HarborAPIError, HarborAuthenticationError):
            return False

    def create_project(self, project_name: str, storage_quota_gb: int) -> bool:
        """Create a new Harbor project with storage quota.

        Args:
            project_name: Name of the project to create
            storage_quota_gb: Storage quota in GB (-1 for unlimited)

        Returns:
            True if project was created successfully
        """
        # First check if project already exists
        if self.get_project(project_name):
            logger.info("Project %s already exists in Harbor", project_name)
            return False

        project_data = {"project_name": project_name, "metadata": {"public": "false"}}

        try:
            self._make_request("POST", "/projects", json=project_data)
            logger.info("Created Harbor project: %s", project_name)

            # Set quota after project creation if specified
            if storage_quota_gb > 0:
                quota_updated = self.update_project_quota(
                    project_name, storage_quota_gb
                )
                if quota_updated:
                    logger.info(
                        "Set quota for project %s to %dGB",
                        project_name,
                        storage_quota_gb,
                    )
                else:
                    logger.warning("Failed to set quota for project %s", project_name)

            return True
        except HarborAPIError as e:
            raise HarborProjectError(
                f"Failed to create project {project_name}: {e}"
            ) from e

    def delete_project(self, project_name: str) -> bool:
        """Delete a Harbor project.

        Args:
            project_name: Name of the project to delete

        Returns:
            True if project was deleted successfully
        """
        project = self.get_project(project_name)
        if not project:
            logger.warning("Project %s does not exist in Harbor", project_name)
            return False

        project_id = project.get("project_id")

        try:
            self._make_request("DELETE", f"/projects/{project_id}")
            logger.info("Deleted Harbor project: %s", project_name)
            return True
        except HarborAPIError as e:
            raise HarborProjectError(
                f"Failed to delete project {project_name}: {e}"
            ) from e

    def get_project(self, project_name: str) -> Optional[dict]:
        """Get project details by name.

        Args:
            project_name: Name of the project

        Returns:
            Project details dict or None if not found
        """
        try:
            response = self._make_request(
                "GET", f"/projects?name={quote(project_name)}"
            )
            projects = self._parse_json_response(response)

            if projects and len(projects) > 0:
                for project in projects:
                    if project.get("name") == project_name:
                        return project
            return None
        except HarborAPIError:
            return None

    def get_project_usage(self, project_name: str) -> dict:
        """Get storage usage statistics for a project.

        Args:
            project_name: Name of the project

        Returns:
            Dictionary with usage statistics
        """
        project = self.get_project(project_name)
        if not project:
            return {"storage_bytes": 0, "repository_count": 0}

        project_id = project.get("project_id")

        try:
            # Get project summary which includes storage usage
            response = self._make_request("GET", f"/projects/{project_id}/summary")
            summary = self._parse_json_response(response)

            return {
                "storage_bytes": summary.get("quota", {})
                .get("used", {})
                .get("storage", 0),
                "repository_count": summary.get("repo_count", 0),
            }
        except HarborAPIError as e:
            logger.error("Failed to get usage for project %s: %s", project_name, e)
            return {"storage_bytes": 0, "repository_count": 0}

    def update_project_quota(self, project_name: str, new_quota_gb: int) -> bool:
        """Update storage quota for a project.

        Args:
            project_name: Name of the project
            new_quota_gb: New storage quota in GB (-1 for unlimited)

        Returns:
            True if quota was updated successfully
        """
        project = self.get_project(project_name)
        if not project:
            raise HarborProjectError(f"Project {project_name} not found")

        project_id = project.get("project_id")

        # Get current quota ID
        try:
            response = self._make_request(
                "GET", f"/quotas?reference=project&reference_id={project_id}"
            )
            quotas = self._parse_json_response(response)

            if not quotas:
                raise HarborQuotaError(f"No quota found for project {project_name}")

            quota_id = quotas[0].get("id")

            # Update the quota
            quota_data = {
                "hard": {
                    "storage": new_quota_gb * 1024 * 1024 * 1024
                    if new_quota_gb > 0
                    else -1
                }
            }

            self._make_request("PUT", f"/quotas/{quota_id}", json=quota_data)
            logger.info(
                "Updated quota for project %s to %dGB", project_name, new_quota_gb
            )
            return True

        except HarborAPIError as e:
            raise HarborQuotaError(
                f"Failed to update quota for project {project_name}: {e}"
            ) from e

    def create_user_group(
        self, group_name: str, group_type: str = "OIDC"
    ) -> Optional[int]:
        """Create a user group in Harbor.

        Args:
            group_name: Name of the group
            group_type: Type of group (OIDC, LDAP, or HTTP)

        Returns:
            Group ID if created successfully, None otherwise
        """
        try:
            # Check if group already exists
            response = self._make_request(
                "GET", f"/usergroups/search?groupname={quote(group_name)}"
            )
            existing_groups = self._parse_json_response(response)

            if existing_groups:
                for group in existing_groups:
                    if group.get("group_name") == group_name:
                        logger.info("User group %s already exists", group_name)
                        return group.get("id")

            # Create new group
            group_data = {
                "group_name": group_name,
                "group_type": 3
                if group_type == "OIDC"
                else 1,  # 1=LDAP, 2=HTTP, 3=OIDC
                "ldap_group_dn": "",
            }

            response = self._make_request("POST", "/usergroups", json=group_data)

            # Get the created group ID from location header
            location = response.headers.get("Location", "")
            if location:
                group_id = int(location.split("/")[-1])
                logger.info("Created user group %s with ID %d", group_name, group_id)
                return group_id

            return None

        except HarborAPIError as e:
            raise HarborOIDCError(
                f"Failed to create user group {group_name}: {e}"
            ) from e

    def assign_group_to_project(
        self, group_name: str, project_name: str, role_id: int = 2
    ) -> bool:
        """Assign a user group to a project with specified role.

        Args:
            group_name: Name of the user group
            project_name: Name of the project
            role_id: Role ID (1=Admin, 2=Developer, 3=Guest, 4=Maintainer)

        Returns:
            True if assignment was successful
        """
        # Get project details
        project = self.get_project(project_name)
        if not project:
            raise HarborProjectError(f"Project {project_name} not found")

        project_id = project.get("project_id")

        # Get or create group
        group_id = self.create_user_group(group_name)
        if not group_id:
            raise HarborOIDCError(f"Failed to get or create group {group_name}")

        try:
            # Check if member already exists
            response = self._make_request("GET", f"/projects/{project_id}/members")
            members = self._parse_json_response(response)

            for member in members:
                entity = member.get("entity_name", "")
                if entity == group_name:
                    logger.info(
                        "Group %s is already member of project %s",
                        group_name,
                        project_name,
                    )
                    return True

            # Add group as project member
            member_data = {
                "role_id": role_id,
                "member_group": {
                    "id": group_id,
                    "group_name": group_name,
                    "group_type": 3,  # OIDC
                },
            }

            self._make_request(
                "POST", f"/projects/{project_id}/members", json=member_data
            )
            logger.info(
                "Assigned group %s to project %s with role %d",
                group_name,
                project_name,
                role_id,
            )
            return True

        except HarborAPIError as e:
            raise HarborOIDCError(
                f"Failed to assign group {group_name} to project {project_name}: {e}"
            ) from e

    def remove_group_from_project(self, group_name: str, project_name: str) -> bool:
        """Remove a user group from a project.

        Args:
            group_name: Name of the user group
            project_name: Name of the project

        Returns:
            True if removal was successful
        """
        project = self.get_project(project_name)
        if not project:
            logger.warning("Project %s not found", project_name)
            return False

        project_id = project.get("project_id")

        try:
            # Find member ID for the group
            response = self._make_request("GET", f"/projects/{project_id}/members")
            members = self._parse_json_response(response)

            member_id = None
            for member in members:
                if member.get("entity_name") == group_name:
                    member_id = member.get("id")
                    break

            if not member_id:
                logger.warning(
                    "Group %s is not a member of project %s", group_name, project_name
                )
                return False

            # Remove the member
            self._make_request("DELETE", f"/projects/{project_id}/members/{member_id}")
            logger.info("Removed group %s from project %s", group_name, project_name)
            return True

        except HarborAPIError as e:
            logger.error(
                "Failed to remove group %s from project %s: %s",
                group_name,
                project_name,
                e,
            )
            return False

    # BaseClient abstract method implementations

    def list_resources(self) -> list[ClientResource]:
        """List all projects in Harbor as resources."""
        try:
            response = self._make_request("GET", "/projects")
            projects = self._parse_json_response(response)

            return [
                ClientResource(
                    name=project.get("name", ""),
                    organization="",  # Harbor doesn't have organizations
                    description=project.get("name", ""),
                )
                for project in projects
            ]
        except HarborAPIError as e:
            logger.error("Failed to list Harbor projects: %s", e)
            return []

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get a specific project by name."""
        project = self.get_project(resource_id)
        if project:
            return ClientResource(
                name=project.get("name", ""),
                organization="",
                description=project.get("name", ""),
            )
        return None

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Create a new project (resource) in Harbor."""
        # Default quota if not specified
        default_quota_gb = 10
        self.create_project(name, default_quota_gb)
        return name

    def delete_resource(self, resource_id: str) -> str:
        """Delete a project (resource) from Harbor."""
        self.delete_project(resource_id)
        return resource_id

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List users associated with a project."""
        # Harbor doesn't directly list users, returns empty list
        # User access is managed through OIDC groups
        return []

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get storage quota for a project."""
        project = self.get_project(resource_id)
        if not project:
            return {}

        project_id = project.get("project_id")

        try:
            response = self._make_request(
                "GET", f"/quotas?reference=project&reference_id={project_id}"
            )
            quotas = self._parse_json_response(response)

            if quotas:
                storage_bytes = quotas[0].get("hard", {}).get("storage", 0)
                storage_gb = storage_bytes // (1024**3) if storage_bytes > 0 else 0
                return {"storage": storage_gb}

            return {}
        except HarborAPIError:
            return {}

    def set_resource_limits(
        self, resource_id: str, limits: dict[str, int]
    ) -> Optional[str]:
        """Set storage quota for a project."""
        storage_gb = limits.get("storage", 10)
        self.update_project_quota(resource_id, storage_gb)
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between user and project (not directly supported in Harbor)."""
        # User associations are managed through OIDC groups
        # This is a no-op for Harbor
        return ""

    def get_association(self, username: str, resource_id: str) -> Optional[Association]:
        """Get association between user and project (not directly supported in Harbor)."""
        # User associations are managed through OIDC groups
        # This always returns None for Harbor
        return None

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between user and project (not directly supported in Harbor)."""
        # User associations are managed through OIDC groups
        # This is a no-op for Harbor
        return ""

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits (not supported in Harbor)."""
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits: dict[str, int]
    ) -> str:
        """Set per-user limits (not supported in Harbor)."""
        # Harbor doesn't support per-user quotas
        return ""

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get usage records for Harbor projects."""
        usage_records = []
        for resource_id in resource_ids:
            try:
                usage_data = self.get_project_usage(resource_id)
                storage_gb = usage_data["storage_bytes"] // (1024**3)
                usage_records.append(
                    {
                        "resource_id": resource_id,
                        "storage": storage_gb,
                        "repository_count": usage_data.get("repository_count", 0),
                    }
                )
            except Exception as e:
                logger.error("Failed to get usage for project %s: %s", resource_id, e)
        return usage_records
