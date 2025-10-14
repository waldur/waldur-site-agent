"""Harbor Backend for waldur site agent.

This module provides integration between Waldur Mastermind and Harbor
container registry. It implements the backend interface for managing
projects, storage quotas, and OIDC group permissions.

Mapping:
- Waldur Resource -> Harbor Project (with storage quota)
- Waldur Project -> OIDC Group (for access control)
- Usage tracking -> Storage usage reporting
"""

import logging
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import backends, structures
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_harbor.client import HarborClient
from waldur_site_agent_harbor.exceptions import (
    HarborError,
    HarborProjectError,
    HarborOIDCError,
)

logger = logging.getLogger(__name__)


class HarborBackend(backends.BaseBackend):
    """Harbor backend implementation for Waldur Site Agent.

    This backend manages Harbor container registry projects based on Waldur
    marketplace orders. Each Waldur resource becomes a separate Harbor project
    with its own storage quota. Access control is managed through OIDC groups
    at the Waldur project level.
    """

    def __init__(
        self, harbor_settings: dict, harbor_components: dict[str, dict]
    ) -> None:
        """Initialize Harbor backend with settings and components.

        Args:
            harbor_settings: Backend configuration including:
                - harbor_url: Harbor registry URL
                - robot_username: Robot account username
                - robot_password: Robot account password
                - default_storage_quota_gb: Default storage quota in GB
                - oidc_group_prefix: Prefix for OIDC group names
                - project_role_id: Role ID for OIDC groups (1=Admin, 2=Developer, 3=Guest, 4=Maintainer)
            harbor_components: Component definitions (should include 'storage')
        """
        super().__init__(harbor_settings, harbor_components)
        self.backend_type = "harbor"

        # Validate required settings
        required_settings = ["harbor_url", "robot_username", "robot_password"]
        for setting in required_settings:
            if setting not in harbor_settings:
                raise ValueError(f"Missing required setting: {setting}")

        # Validate components - Harbor only supports storage with limit accounting
        if "storage" not in harbor_components:
            raise ValueError("Harbor backend requires 'storage' component")

        storage_component = harbor_components["storage"]
        if storage_component.get("accounting_type") != "limit":
            raise ValueError(
                "Harbor backend storage component must have accounting_type='limit'"
            )

        # Initialize Harbor client
        self.client = HarborClient(
            harbor_url=harbor_settings["harbor_url"],
            robot_username=harbor_settings["robot_username"],
            robot_password=harbor_settings["robot_password"],
        )

        # Configuration defaults
        self.default_storage_quota_gb = harbor_settings.get(
            "default_storage_quota_gb", 10
        )
        self.oidc_group_prefix = harbor_settings.get("oidc_group_prefix", "waldur-")
        self.project_role_id = harbor_settings.get(
            "project_role_id", 2
        )  # 2=Developer by default

        logger.info(
            "Initialized Harbor backend for %s with default quota %dGB",
            harbor_settings["harbor_url"],
            self.default_storage_quota_gb,
        )

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if Harbor API is accessible.

        Args:
            raise_exception: If True, raise exception on failure

        Returns:
            True if Harbor is accessible, False otherwise
        """
        try:
            if not isinstance(self.client, HarborClient):
                return False
            is_alive = self.client.ping()
            if is_alive:
                logger.info("Harbor backend is accessible")
            else:
                logger.warning("Harbor backend is not responding")
                if raise_exception:
                    raise BackendError("Harbor backend is not responding")
            return is_alive
        except Exception as e:
            logger.error("Failed to ping Harbor backend: %s", e)
            if raise_exception:
                raise
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about the Harbor backend.

        Returns:
            True if diagnostics completed successfully
        """
        logger.info("=== Harbor Backend Diagnostics ===")
        logger.info("Harbor URL: %s", self.backend_settings["harbor_url"])
        logger.info("Robot Username: %s", self.backend_settings["robot_username"])
        logger.info("Default Storage Quota: %d GB", self.default_storage_quota_gb)
        logger.info("OIDC Group Prefix: %s", self.oidc_group_prefix)
        logger.info("Project Role ID: %d", self.project_role_id)

        # Test connectivity
        is_accessible = self.ping()
        logger.info("API Accessible: %s", is_accessible)

        if is_accessible:
            # List existing projects
            try:
                if not isinstance(self.client, HarborClient):
                    logger.error("Client is not a HarborClient")
                    return False
                resources = self.client.list_resources()
                logger.info("Number of existing projects: %d", len(resources))
            except Exception as e:
                logger.error("Failed to list projects: %s", e)

        return is_accessible

    def list_components(self) -> list[str]:
        """Return list of supported components.

        Returns:
            List of component names (only 'storage' for Harbor)
        """
        return list(self.backend_components.keys())

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Prepare for resource creation by setting up OIDC group.

        Creates an OIDC group for the Waldur project if it doesn't exist.
        This group will be used to grant access to all Harbor projects
        within the Waldur project.

        Args:
            waldur_resource: Waldur resource being created
            user_context: Optional user context (not used for Harbor)
        """
        # Create OIDC group for the Waldur project
        oidc_group_name = f"{self.oidc_group_prefix}{waldur_resource.project_slug}"

        try:
            if not isinstance(self.client, HarborClient):
                raise HarborOIDCError("Client is not a HarborClient")
            group_id = self.client.create_user_group(oidc_group_name)
            if group_id:
                logger.info(
                    "OIDC group %s ready for Waldur project %s",
                    oidc_group_name,
                    waldur_resource.project_slug,
                )
        except HarborOIDCError as e:
            logger.error("Failed to create OIDC group: %s", e)
            # Continue anyway - group might already exist

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> structures.BackendResourceInfo:
        """Create Harbor project for Waldur resource.

        Creates a new Harbor project with storage quota and assigns the
        appropriate OIDC group for access control.

        Args:
            waldur_resource: Waldur resource to create
            user_context: Optional user context

        Returns:
            BackendResourceInfo with created project details
        """
        logger.info(
            "Creating Harbor project for Waldur resource %s", waldur_resource.uuid
        )

        # Prepare OIDC group
        self._pre_create_resource(waldur_resource, user_context)

        # Generate Harbor project name from Waldur resource
        harbor_project_name = self._get_resource_backend_id(waldur_resource.slug)

        # Calculate storage quota from Waldur limits
        storage_quota_gb = self._calculate_storage_quota(waldur_resource)

        try:
            # Create Harbor project
            if not isinstance(self.client, HarborClient):
                raise HarborProjectError("Client is not a HarborClient")
            created = self.client.create_project(harbor_project_name, storage_quota_gb)
            if created:
                logger.info(
                    "Created Harbor project %s with %dGB quota",
                    harbor_project_name,
                    storage_quota_gb,
                )

            # Assign OIDC group to the project
            oidc_group_name = f"{self.oidc_group_prefix}{waldur_resource.project_slug}"
            if not isinstance(self.client, HarborClient):
                raise HarborOIDCError("Client is not a HarborClient")
            self.client.assign_group_to_project(
                oidc_group_name,
                harbor_project_name,
                self.project_role_id,
            )
            logger.info(
                "Assigned OIDC group %s to Harbor project %s",
                oidc_group_name,
                harbor_project_name,
            )

        except (HarborProjectError, HarborOIDCError) as e:
            raise BackendError(f"Failed to create Harbor project: {e}") from e

        return structures.BackendResourceInfo(
            backend_id=harbor_project_name,
            limits={"storage": storage_quota_gb},
        )

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:
        """Delete Harbor project for Waldur resource.

        Args:
            waldur_resource: Waldur resource to delete
            **kwargs: Additional arguments (not used)
        """
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id.strip():
            logger.warning("Empty backend_id for resource, skipping deletion")
            return

        try:
            if not isinstance(self.client, HarborClient):
                raise HarborProjectError("Client is not a HarborClient")
            deleted = self.client.delete_project(resource_backend_id)
            if deleted:
                logger.info("Deleted Harbor project %s", resource_backend_id)
        except HarborProjectError as e:
            logger.error(
                "Failed to delete Harbor project %s: %s", resource_backend_id, e
            )

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> Optional[structures.BackendResourceInfo]:
        """Pull resource data from Harbor.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            BackendResourceInfo with project details or None if not found
        """
        logger.info("Pulling Harbor project %s", resource_backend_id)

        if not isinstance(self.client, HarborClient):
            return None
        project = self.client.get_project(resource_backend_id)
        if not project:
            logger.warning("Harbor project %s not found", resource_backend_id)
            return None

        # Get usage information
        usage_data = self.client.get_project_usage(resource_backend_id)
        storage_gb = usage_data["storage_bytes"] // (1024**3)

        # Get current limits
        limits = self.client.get_resource_limits(resource_backend_id)

        return structures.BackendResourceInfo(
            backend_id=resource_backend_id,
            limits=limits,
            usage={
                "TOTAL_ACCOUNT_USAGE": {
                    "storage": storage_gb,
                }
            },
            users=[],  # Harbor doesn't directly track users per project
        )

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect storage usage report for specified Harbor projects.

        Args:
            resource_backend_ids: List of Harbor project names

        Returns:
            Dictionary mapping project names to usage data
        """
        usage_report = {}

        for project_name in resource_backend_ids:
            try:
                if not isinstance(self.client, HarborClient):
                    continue
                usage_data = self.client.get_project_usage(project_name)
                storage_gb = usage_data["storage_bytes"] // (1024**3)

                usage_report[project_name] = {
                    "TOTAL_ACCOUNT_USAGE": {
                        "storage": storage_gb,
                    }
                }

                logger.info(
                    "Harbor project %s is using %d GB of storage",
                    project_name,
                    storage_gb,
                )

            except Exception as e:
                logger.error("Failed to get usage for project %s: %s", project_name, e)
                # Return zero usage on error
                usage_report[project_name] = {
                    "TOTAL_ACCOUNT_USAGE": {
                        "storage": 0,
                    }
                }

        return usage_report

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Calculate storage limits from Waldur resource.

        Args:
            waldur_resource: Waldur resource with limit information

        Returns:
            Tuple of (backend_limits, waldur_limits) dictionaries
        """
        # Get storage limit from Waldur resource or use default
        storage_limit = self.default_storage_quota_gb

        if waldur_resource.limits and "storage" in waldur_resource.limits:
            # Waldur limits are already in the correct unit (GB)
            storage_limit = waldur_resource.limits["storage"]

        backend_limits = {"storage": storage_limit}
        waldur_limits = {"storage": storage_limit}

        return backend_limits, waldur_limits

    def _calculate_storage_quota(self, waldur_resource: WaldurResource) -> int:
        """Calculate storage quota in GB from Waldur resource.

        Args:
            waldur_resource: Waldur resource with limit information

        Returns:
            Storage quota in GB
        """
        backend_limits, _ = self._collect_resource_limits(waldur_resource)
        return backend_limits.get("storage", self.default_storage_quota_gb)

    def set_resource_limits(
        self, resource_backend_id: str, limits: dict[str, int]
    ) -> None:
        """Update storage quota for Harbor project.

        Args:
            resource_backend_id: Harbor project name
            limits: Dictionary with 'storage' key containing quota in GB
        """
        storage_gb = limits.get("storage", self.default_storage_quota_gb)

        try:
            if not isinstance(self.client, HarborClient):
                raise HarborError("Client is not a HarborClient")
            self.client.update_project_quota(resource_backend_id, storage_gb)
            logger.info(
                "Updated quota for project %s to %d GB", resource_backend_id, storage_gb
            )
        except HarborError as e:
            raise BackendError(f"Failed to update project quota: {e}") from e

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Get current storage quota for Harbor project.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            Dictionary with 'storage' key containing quota in GB
        """
        if not isinstance(self.client, HarborClient):
            return {}
        return self.client.get_resource_limits(resource_backend_id)

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add users to Harbor project (not directly supported).

        User access in Harbor is managed through OIDC groups, not individual
        user associations. This method is a no-op for Harbor.

        Args:
            resource_backend_id: Harbor project name
            user_ids: Set of usernames to add
            **kwargs: Additional arguments

        Returns:
            Empty set (no users added individually)
        """
        logger.info(
            "User management is handled through OIDC groups for Harbor project %s",
            resource_backend_id,
        )
        return set()

    def remove_users_from_resource(
        self, resource_backend_id: str, usernames: set[str]
    ) -> list[str]:
        """Remove users from Harbor project (not directly supported).

        User access in Harbor is managed through OIDC groups, not individual
        user associations. This method is a no-op for Harbor.

        Args:
            resource_backend_id: Harbor project name
            usernames: Set of usernames to remove

        Returns:
            Empty list (no users removed individually)
        """
        logger.info(
            "User management is handled through OIDC groups for Harbor project %s",
            resource_backend_id,
        )
        return []

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale Harbor project by reducing quota to minimum.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            True if downscaled successfully
        """
        try:
            # Set quota to 1 GB (minimum)
            if not isinstance(self.client, HarborClient):
                return False
            self.client.update_project_quota(resource_backend_id, 1)
            logger.info(
                "Downscaled Harbor project %s to 1GB quota", resource_backend_id
            )
            return True
        except HarborError as e:
            logger.error("Failed to downscale project %s: %s", resource_backend_id, e)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause Harbor project (not directly supported).

        Harbor doesn't have a pause concept. This could be implemented
        by removing all group permissions temporarily.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            False (not implemented)
        """
        logger.warning("Pause operation not supported for Harbor projects")
        return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore Harbor project after downscaling.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            True if restored successfully
        """
        try:
            # Restore to default quota
            if not isinstance(self.client, HarborClient):
                return False
            self.client.update_project_quota(
                resource_backend_id, self.default_storage_quota_gb
            )
            logger.info(
                "Restored Harbor project %s to %dGB quota",
                resource_backend_id,
                self.default_storage_quota_gb,
            )
            return True
        except HarborError as e:
            logger.error("Failed to restore project %s: %s", resource_backend_id, e)
            return False

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get Harbor-specific metadata for a project.

        Args:
            resource_backend_id: Harbor project name

        Returns:
            Dictionary with Harbor project metadata
        """
        if not isinstance(self.client, HarborClient):
            return {}
        project = self.client.get_project(resource_backend_id)
        if not project:
            return {}

        usage_data = self.client.get_project_usage(resource_backend_id)

        return {
            "harbor_project_id": project.get("project_id"),
            "harbor_project_name": project.get("name"),
            "harbor_url": f"{self.backend_settings['harbor_url']}/harbor/projects/{project.get('project_id')}",
            "repository_count": usage_data.get("repository_count", 0),
            "storage_used_bytes": usage_data.get("storage_bytes", 0),
            "creation_time": project.get("creation_time"),
        }
