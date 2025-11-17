"""Croit S3 storage backend for Waldur Site Agent.

This module provides integration between Waldur Mastermind and Croit S3 storage
via RadosGW API. It implements the backend interface for managing S3 users,
bucket quotas, and usage reporting.

Key Features:
- S3 user provisioning with slug-based naming
- Usage and limit-based accounting support
- Bucket quota enforcement for limit-based components
- Comprehensive usage reporting with storage and object metrics
- S3 credentials exposure via resource metadata
"""

import logging
import re
from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent.backend import BackendType, backends, structures

from .client import CroitS3Client
from .exceptions import CroitS3Error, CroitS3UserExistsError, CroitS3UserNotFoundError

logger = logging.getLogger(__name__)


class CroitS3Backend(backends.BaseBackend):
    """Croit S3 storage backend implementation for Waldur Site Agent.

    This backend manages S3 user lifecycle, bucket quotas, and usage reporting
    for Croit storage systems. It supports both usage-based and limit-based
    accounting models.
    """

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Initialize backend with settings and component configuration.

        Args:
            backend_settings: Backend configuration including API credentials
            backend_components: Component definitions for accounting
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = BackendType.CROIT_S3.value

        # Required settings validation
        if "token" not in backend_settings and (
            "username" not in backend_settings or "password" not in backend_settings
        ):
            raise ValueError(
                "Either 'token' or both 'username' and 'password' must be provided"
            )

        # Initialize client
        self.client: CroitS3Client = CroitS3Client(
            api_url=backend_settings["api_url"],
            username=backend_settings.get("username"),
            password=backend_settings.get("password"),
            token=backend_settings.get("token"),
            verify_ssl=backend_settings.get("verify_ssl", True),
            timeout=backend_settings.get("timeout", 30),
        )

        # Backend-specific settings
        self.user_prefix = backend_settings.get("user_prefix", "waldur_")
        self.default_tenant = backend_settings.get("default_tenant", "")
        self.default_placement = backend_settings.get("default_placement", "")
        self.default_storage_class = backend_settings.get("default_storage_class", "")

        # Username generation settings
        self.slug_separator = backend_settings.get("slug_separator", "_")
        self.max_username_length = backend_settings.get("max_username_length", 64)

        logger.info(
            "Croit S3 backend initialized with API: %s", backend_settings["api_url"]
        )

    def _generate_username(self, resource: WaldurResource) -> str:
        """Generate S3 username from organization, project, and resource slugs.

        Format: {prefix}{org_slug}_{project_slug}_{resource_uuid_short}

        Args:
            resource: Waldur resource object

        Returns:
            Generated username string
        """
        # Extract slugs from resource
        org_slug = resource.organization.get("slug", "org")
        project_slug = resource.project.get("slug", "proj")

        # Use first 8 characters of resource UUID for uniqueness
        resource_short = resource.uuid[:8]

        # Clean slugs to ensure S3 compatibility
        org_slug = self._clean_slug(org_slug)
        project_slug = self._clean_slug(project_slug)

        # Generate username
        username = f"{self.user_prefix}{org_slug}{self.slug_separator}{project_slug}{self.slug_separator}{resource_short}"

        # Ensure username doesn't exceed maximum length
        if len(username) > self.max_username_length:
            # Truncate middle parts if too long, keep prefix and resource short
            remaining_length = (
                self.max_username_length
                - len(self.user_prefix)
                - len(resource_short)
                - 2
            )
            org_limit = remaining_length // 2
            proj_limit = remaining_length - org_limit

            org_slug = org_slug[:org_limit]
            project_slug = project_slug[:proj_limit]

            username = f"{self.user_prefix}{org_slug}{self.slug_separator}{project_slug}{self.slug_separator}{resource_short}"

        logger.debug("Generated username: %s for resource %s", username, resource.uuid)
        return username

    def _clean_slug(self, slug: str) -> str:
        """Clean slug for S3 username compatibility.

        S3 usernames should follow RadosGW naming conventions:
        - Alphanumeric characters and underscores
        - No special characters except underscore

        Args:
            slug: Original slug

        Returns:
            Cleaned slug
        """
        # Replace non-alphanumeric characters (except underscore) with underscore
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", slug)

        # Remove multiple consecutive underscores
        cleaned = re.sub(r"_+", "_", cleaned)

        # Remove leading/trailing underscores
        cleaned = cleaned.strip("_")

        # Ensure at least one character remains
        if not cleaned:
            cleaned = "default"

        return cleaned.lower()

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if backend is online and accessible.

        Args:
            raise_exception: Whether to raise exception on failure

        Returns:
            True if backend is accessible, False otherwise
        """
        return self.client.ping(raise_exception=raise_exception)

    def diagnostics(self) -> bool:
        """Log diagnostic information about the backend.

        Returns:
            True if diagnostics completed successfully
        """
        try:
            logger.info("=== Croit S3 Backend Diagnostics ===")
            logger.info("API URL: %s", self.client.api_url)
            logger.info("Username: %s", self.client.username)
            logger.info("SSL Verification: %s", self.client.verify_ssl)
            logger.info("User Prefix: %s", self.user_prefix)
            logger.info("Components: %s", list(self.backend_components.keys()))

            # Test connectivity
            if self.ping():
                logger.info("✓ API connectivity successful")

                # List current users
                users = self.client.list_users()
                logger.info("Current S3 users: %d", len(users))

                # Show component configuration
                for component_name, config in self.backend_components.items():
                    logger.info("Component %s: %s", component_name, config)

                logger.info("=== Diagnostics completed successfully ===")
                return True
            else:
                logger.error("✗ API connectivity failed")
                return False

        except Exception as e:
            logger.exception("Diagnostics failed: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of computing components supported by this backend.

        Returns:
            List of component names
        """
        return list(self.backend_components.keys())

    def create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict[Any, Any]] = None,
    ) -> structures.BackendResourceInfo:
        """Create S3 user resource with optional bucket quotas.

        Args:
            waldur_resource: Waldur resource object containing limits and metadata
            user_context: Optional user context including team members and offering users

        Returns:
            Backend resource info with created S3 username
        """
        try:
            # Generate username from slugs
            username = self._generate_username(waldur_resource)

            # Prepare user data
            user_data = {
                "uid": username,
                "name": waldur_resource.name or f"User for {waldur_resource.uuid}",
            }

            # Add optional properties
            if self.default_tenant:
                user_data["tenant"] = self.default_tenant
            if self.default_placement:
                user_data["defaultPlacement"] = self.default_placement
            if self.default_storage_class:
                user_data["defaultStorageClass"] = self.default_storage_class

            # Create S3 user
            logger.info(
                "Creating S3 user: %s for resource %s", username, waldur_resource.uuid
            )
            self.client.create_user(**user_data)

            # Apply bucket quotas if safety limits are specified in resource options
            resource_options = getattr(waldur_resource, "attributes", {})
            if resource_options:
                self._apply_bucket_quotas(username, resource_options)

            logger.info("S3 user %s created successfully", username)
            return structures.BackendResourceInfo(backend_id=username)

        except CroitS3UserExistsError:
            logger.warning("S3 user %s already exists", username)
            return structures.BackendResourceInfo(backend_id=username)
        except CroitS3Error as e:
            # Check if it's a user exists error (sometimes returns as 500 error)
            if "exists" in str(e):
                logger.warning("S3 user %s already exists (500 error)", username)
                return structures.BackendResourceInfo(backend_id=username)
            logger.error(
                "Failed to create S3 user for resource %s: %s", waldur_resource.uuid, e
            )
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error creating S3 user for resource %s: %s",
                waldur_resource.uuid,
                e,
            )
            raise

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:
        """Delete S3 user resource.

        Args:
            waldur_resource: Waldur resource object
            **kwargs: Additional arguments
        """
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id:
            logger.warning("No backend ID found for resource %s", waldur_resource.uuid)
            return

        try:
            logger.info("Deleting S3 user: %s", resource_backend_id)
            self.client.delete_user(resource_backend_id)
            logger.info("S3 user %s deleted successfully", resource_backend_id)

        except CroitS3UserNotFoundError:
            logger.warning(
                "S3 user %s not found, considering as deleted", resource_backend_id
            )
        except CroitS3Error as e:
            logger.error("Failed to delete S3 user %s: %s", resource_backend_id, e)
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error deleting S3 user %s: %s", resource_backend_id, e
            )
            raise

    def _apply_bucket_quotas(self, username: str, resource_options: dict) -> None:
        """Apply safety limits from resource options as bucket quotas.

        Args:
            username: S3 username
            resource_options: Resource options containing safety limits

        Raises:
            CroitS3Error: If quota setting fails
        """
        quota_request: dict[str, Any] = {"enabled": True}
        quota_applied = False

        # Get safety limits from resource options
        storage_limit = resource_options.get("storage_limit", 0)
        object_limit = resource_options.get("object_limit", 0)

        # Apply storage quota if configured
        if storage_limit > 0:
            storage_config = self.backend_components.get("s3_storage", {})
            if storage_config.get("enforce_limits", False):
                unit_factor = storage_config.get("unit_factor", 1)
                quota_request["maxSize"] = int(storage_limit * unit_factor)
                quota_applied = True
                logger.debug(
                    "Setting storage quota: %d bytes (%d GB)",
                    quota_request["maxSize"],
                    storage_limit,
                )

        # Apply object quota if configured
        if object_limit > 0:
            objects_config = self.backend_components.get("s3_objects", {})
            if objects_config.get("enforce_limits", False):
                quota_request["maxObjects"] = int(object_limit)
                quota_applied = True
                logger.debug(
                    "Setting object quota: %d objects", quota_request["maxObjects"]
                )

        # Apply quota if any limits were configured
        if quota_applied:
            logger.info(
                "Applying bucket quotas for user %s: %s", username, quota_request
            )
            self.client.set_user_bucket_quota(username, quota_request)
        else:
            logger.info(
                "No safety limits configured, skipping quota enforcement for user %s",
                username,
            )

    def update_resource_limits(
        self, resource_backend_id: str, limits: ResourceLimits
    ) -> bool:
        """Update bucket quotas - not applicable for options-based configuration.

        Args:
            resource_backend_id: S3 username
            limits: Resource limits (not used for Croit S3)

        Returns:
            True (quotas are set via resource options during creation)
        """
        logger.info(
            "Update limits not applicable for S3 user %s (quotas set via options)",
            resource_backend_id,
        )
        return True

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Collect usage report including storage and object metrics.

        Args:
            resource_backend_ids: List of S3 usernames

        Returns:
            Usage report in Waldur format: {user_id: {component: {usage: value}}}
        """
        report: dict[str, dict[str, dict[str, int]]] = {}

        for username in resource_backend_ids:
            try:
                # Get user buckets with usage data
                buckets = self.client.get_user_buckets(username)

                # Calculate totals across all buckets
                total_storage_bytes = 0
                total_objects = 0

                for bucket in buckets:
                    usage_sum = bucket.get("usageSum", {})
                    total_storage_bytes += usage_sum.get("size", 0)
                    total_objects += usage_sum.get("numObjects", 0)

                # Initialize user report
                user_report: dict[str, dict[str, int]] = {}

                # Process each component based on its configuration
                for component_name, component_config in self.backend_components.items():
                    accounting_type = component_config.get("accounting_type", "limit")

                    # Only include usage data for usage-based components
                    if accounting_type != "usage":
                        continue

                    backend_name = component_config.get("backend_name", "")
                    unit_factor = component_config.get("unit_factor", 1)

                    if backend_name == "storage":
                        # Convert bytes to component units (e.g., GB)
                        usage_value = (
                            int(total_storage_bytes // unit_factor)
                            if unit_factor > 0
                            else total_storage_bytes
                        )
                        user_report[component_name] = {"usage": usage_value}

                    elif backend_name == "objects":
                        user_report[component_name] = {"usage": int(total_objects)}

                # Add user report to main report
                if user_report:
                    report[username] = user_report
                    logger.debug("Usage for user %s: %s", username, user_report)

            except Exception as e:
                logger.error("Failed to collect usage for user %s: %s", username, e)

        logger.info("Collected usage report for %d users", len(report))
        return report

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get S3 user metadata including credentials and usage summary.

        Args:
            resource_backend_id: S3 username

        Returns:
            Metadata dictionary with credentials and usage info
        """
        try:
            # Get user information
            user_info = self.client.get_user_info(resource_backend_id)

            # Get access credentials
            credentials = self.client.get_user_keys(resource_backend_id)

            # Get bucket information
            buckets = self.client.get_user_buckets(resource_backend_id)

            # Calculate storage summary
            total_size = sum(b.get("usageSum", {}).get("size", 0) for b in buckets)
            total_objects = sum(
                b.get("usageSum", {}).get("numObjects", 0) for b in buckets
            )

            # Get quota information
            quota_info = self.client.get_user_quota(resource_backend_id)

            # Format S3 endpoint URL (remove /api suffix for S3 operations)
            s3_endpoint = self.client.api_url.replace("/api", "")

            return {
                "s3_credentials": {
                    "access_key": credentials.get("access_key"),
                    "secret_key": credentials.get("secret_key"),
                    "endpoint": s3_endpoint,
                    "region": "default",
                },
                "user_info": {
                    "uid": user_info.get("uid"),
                    "name": user_info.get("name"),
                    "email": user_info.get("email"),
                    "suspended": user_info.get("suspended", False),
                    "default_placement": user_info.get("defaultPlacement"),
                    "default_storage_class": user_info.get("defaultStorageClass"),
                },
                "storage_summary": {
                    "bucket_count": len(buckets),
                    "total_size_bytes": total_size,
                    "total_objects": int(total_objects),
                    "buckets": [
                        {
                            "name": bucket.get("bucket"),
                            "size_bytes": bucket.get("usageSum", {}).get("size", 0),
                            "objects": bucket.get("usageSum", {}).get("numObjects", 0),
                        }
                        for bucket in buckets
                    ],
                },
                "quotas": {
                    "bucket_quota": quota_info.get("bucket_quota", {}),
                    "user_quota": quota_info.get("user_quota", {}),
                },
                "backend_info": {
                    "backend_type": self.backend_type,
                    "api_url": self.client.api_url,
                    "created_via": "waldur_site_agent_croit_s3",
                },
            }

        except Exception as e:
            logger.error(
                "Failed to get metadata for user %s: %s", resource_backend_id, e
            )
            return {
                "error": f"Failed to retrieve metadata: {e}",
                "backend_type": self.backend_type,
            }

    # Abstract method implementations (minimal/no-op for S3 storage)
    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource - not applicable for S3 storage."""
        logger.info("Downscale not applicable for S3 user: %s", resource_backend_id)
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause resource - not applicable for S3 storage."""
        logger.info("Pause not applicable for S3 user: %s", resource_backend_id)
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource - not applicable for S3 storage."""
        logger.info("Restore not applicable for S3 user: %s", resource_backend_id)
        return True

    def add_user(self, resource_backend_id: str, username: str) -> bool:
        """Add user to S3 resource - not applicable for individual S3 users."""
        logger.info("Add user not applicable for S3 storage: %s", resource_backend_id)
        return True

    def remove_user(self, resource_backend_id: str, username: str) -> bool:
        """Remove user from S3 resource - not applicable for individual S3 users."""
        logger.info(
            "Remove user not applicable for S3 storage: %s", resource_backend_id
        )
        return True

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect limits for backend and Waldur separately.

        Args:
            waldur_resource: Waldur resource object

        Returns:
            Tuple of (backend_limits, waldur_limits) dictionaries
        """
        backend_limits: dict[str, int] = {}
        waldur_limits: dict[str, int] = {}

        if not waldur_resource.limits:
            return backend_limits, waldur_limits

        # Process each component and extract limits
        for component_name, component_config in self.backend_components.items():
            limit_value = getattr(waldur_resource.limits, component_name, 0)
            if limit_value > 0:
                # Only include actual configurable components (not s3_user)
                if component_name != "s3_user":
                    backend_limits[component_name] = limit_value
                    waldur_limits[component_name] = limit_value

        logger.debug(
            "Collected limits - Backend: %s, Waldur: %s", backend_limits, waldur_limits
        )
        return backend_limits, waldur_limits

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Perform actions prior to S3 user creation.

        Args:
            waldur_resource: Waldur resource object
            user_context: Optional user context (not used for S3)
        """
        # For S3 storage, we don't need pre-creation steps
        # Username generation and validation happens in create_resource
        logger.debug(
            "Pre-create actions for S3 user - resource: %s", waldur_resource.uuid
        )

        # Validate that we can generate a valid username
        username = self._generate_username(waldur_resource)
        logger.info("Will create S3 user with username: %s", username)
