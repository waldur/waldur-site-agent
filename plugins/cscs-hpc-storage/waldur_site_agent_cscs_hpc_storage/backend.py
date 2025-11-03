"""CSCS HPC Storage backend for Waldur Site Agent."""

from datetime import datetime
from typing import Optional
from uuid import NAMESPACE_OID, uuid5

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_customers_list,
    marketplace_provider_offerings_retrieve,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .hpc_user_client import CSCSHpcUserClient


class CscsHpcStorageBackend(backends.BaseBackend):
    """CSCS HPC Storage backend for JSON file generation."""

    def __init__(
        self,
        backend_settings: dict,
        backend_components: dict[str, dict],
        hpc_user_api_settings: Optional[dict] = None,
    ) -> None:
        """Initialize CSCS storage backend.

        Args:
            backend_settings: Backend-specific configuration settings
            backend_components: Component configuration
            hpc_user_api_settings: Optional HPC User API configuration
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "cscs-hpc-storage"

        # Configuration with defaults
        self.storage_file_system = backend_settings.get("storage_file_system", "lustre")
        self.inode_soft_coefficient = backend_settings.get("inode_soft_coefficient", 1.33)
        self.inode_hard_coefficient = backend_settings.get("inode_hard_coefficient", 2.0)
        self.inode_base_multiplier = backend_settings.get("inode_base_multiplier", 1_000_000)
        self.use_mock_target_items = backend_settings.get("use_mock_target_items", False)
        self.development_mode = backend_settings.get("development_mode", False)

        # HPC User service configuration
        # Support both new separate section and legacy backend_settings location
        if hpc_user_api_settings:
            # Use new separate configuration section
            self.hpc_user_api_url = hpc_user_api_settings.get("api_url")
            self.hpc_user_client_id = hpc_user_api_settings.get("client_id")
            self.hpc_user_client_secret = hpc_user_api_settings.get("client_secret")
            self.hpc_user_oidc_token_url = hpc_user_api_settings.get("oidc_token_url")
            self.hpc_user_oidc_scope = hpc_user_api_settings.get("oidc_scope")
            self.hpc_user_socks_proxy = hpc_user_api_settings.get("socks_proxy")
            if self.hpc_user_socks_proxy:
                logger.info(
                    "SOCKS proxy configured from hpc_user_api settings: %s",
                    self.hpc_user_socks_proxy,
                )
        else:
            # Fall back to legacy configuration in backend_settings
            self.hpc_user_api_url = backend_settings.get("hpc_user_api_url")
            self.hpc_user_client_id = backend_settings.get("hpc_user_client_id")
            self.hpc_user_client_secret = backend_settings.get("hpc_user_client_secret")
            self.hpc_user_oidc_token_url = backend_settings.get("hpc_user_oidc_token_url")
            self.hpc_user_oidc_scope = backend_settings.get("hpc_user_oidc_scope")
            self.hpc_user_socks_proxy = backend_settings.get("hpc_user_socks_proxy")

        # Initialize HPC User client if configured
        self.hpc_user_client: Optional[CSCSHpcUserClient] = None
        if self.hpc_user_api_url and self.hpc_user_client_id and self.hpc_user_client_secret:
            self.hpc_user_client = CSCSHpcUserClient(
                api_url=self.hpc_user_api_url,
                client_id=self.hpc_user_client_id,
                client_secret=self.hpc_user_client_secret,
                oidc_token_url=self.hpc_user_oidc_token_url,
                oidc_scope=self.hpc_user_oidc_scope,
                socks_proxy=self.hpc_user_socks_proxy,
            )
            logger.info("HPC User client initialized with URL: %s", self.hpc_user_api_url)
            if self.hpc_user_socks_proxy:
                logger.info("Using SOCKS proxy: %s", self.hpc_user_socks_proxy)
        else:
            logger.info("HPC User client not configured - using mock unixGid values")

        # Initialize GID cache (persists until server restart)
        self._gid_cache: dict[str, int] = {}
        logger.info("Project GID cache initialized (persists until server restart)")

        # Validate configuration
        self._validate_configuration()

    def _generate_deterministic_uuid(self, name: str) -> str:
        """Generate a deterministic UUID from a string name."""
        return str(uuid5(NAMESPACE_OID, name))

    def _apply_filters(
        self,
        storage_resources: list[dict],
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """Apply filtering to storage resources list.

        Args:
            storage_resources: List of storage resource dictionaries
            storage_system: Optional filter for storage system
            data_type: Optional filter for data type
            status: Optional filter for status

        Returns:
            Filtered list of storage resources
        """
        logger.debug(
            "Applying filters: storage_system=%s, data_type=%s, status=%s on %d resources",
            storage_system,
            data_type,
            status,
            len(storage_resources),
        )
        filtered_resources = []

        for resource in storage_resources:
            # Optional storage_system filter
            if storage_system:
                resource_storage_system = resource.get("storageSystem", {}).get("key", "")
                if resource_storage_system != storage_system:
                    continue

            # Optional data_type filter
            if data_type:
                resource_data_type = resource.get("storageDataType", {}).get("key", "")
                logger.debug(
                    "Comparing data_type filter '%s' with resource data_type '%s'",
                    data_type,
                    resource_data_type,
                )
                if resource_data_type != data_type:
                    continue

            # Optional status filter
            if status:
                resource_status = resource.get("status", "")
                if resource_status != status:
                    continue

            filtered_resources.append(resource)

        logger.debug(
            "Applied filters: storage_system=%s, data_type=%s, status=%s. "
            "Filtered %d resources from %d total",
            storage_system,
            data_type,
            status,
            len(filtered_resources),
            len(storage_resources),
        )

        return filtered_resources

    def _validate_configuration(self) -> None:
        """Validate backend configuration settings."""
        if (
            not isinstance(self.inode_soft_coefficient, (int, float))
            or self.inode_soft_coefficient <= 0
        ):
            msg = "inode_soft_coefficient must be a positive number"
            raise ValueError(msg)

        if (
            not isinstance(self.inode_hard_coefficient, (int, float))
            or self.inode_hard_coefficient <= 0
        ):
            msg = "inode_hard_coefficient must be a positive number"
            raise ValueError(msg)

        if self.inode_hard_coefficient < self.inode_soft_coefficient:
            msg = (
                f"inode_hard_coefficient {self.inode_hard_coefficient} must be greater than "
                f"inode_soft_coefficient {self.inode_soft_coefficient}"
            )
            raise ValueError(msg)

        if not isinstance(self.storage_file_system, str) or not self.storage_file_system.strip():
            msg = "storage_file_system must be a non-empty string"
            raise ValueError(msg)

        if (
            not isinstance(self.inode_base_multiplier, (int, float))
            or self.inode_base_multiplier <= 0
        ):
            msg = "inode_base_multiplier must be a positive number"
            raise ValueError(msg)

    def _validate_resource_data(self, waldur_resource: WaldurResource) -> None:
        """Validate that required resource data is present and not Unset."""
        missing_fields = []

        if isinstance(waldur_resource.offering_slug, Unset):
            missing_fields.append("offering_slug")

        if isinstance(waldur_resource.uuid, Unset):
            missing_fields.append("uuid")

        if isinstance(waldur_resource.slug, Unset):
            missing_fields.append("slug")

        if isinstance(waldur_resource.customer_slug, Unset):
            missing_fields.append("customer_slug")

        if isinstance(waldur_resource.project_slug, Unset):
            missing_fields.append("project_slug")

        if missing_fields:
            resource_id = (
                waldur_resource.slug
                if not isinstance(waldur_resource.slug, Unset)
                else str(waldur_resource.uuid)
                if not isinstance(waldur_resource.uuid, Unset)
                else "unknown"
            )
            raise BackendError(
                f"Resource {resource_id} is missing required fields from Waldur API: "
                f"{', '.join(missing_fields)}. This indicates incomplete data from the "
                f"marketplace API response."
            )

    def ping(self, raise_exception: bool = False) -> bool:  # noqa: ARG002
        """Check if backend is accessible (always returns True for storage proxy backend)."""
        # Note: raise_exception is part of the BaseBackend interface but not used here
        return True

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """For CSCS storage backend the step is empty."""

    def diagnostics(self) -> bool:
        """Log backend diagnostics information."""
        logger.info("CSCS HPC Storage Backend Diagnostics")
        logger.info("=====================================")
        logger.info("Storage file system: %s", self.storage_file_system)
        logger.info("Inode soft coefficient: %s", self.inode_soft_coefficient)
        logger.info("Inode hard coefficient: %s", self.inode_hard_coefficient)
        logger.info("Inode base multiplier: %s", self.inode_base_multiplier)
        logger.info("Use mock target items: %s", self.use_mock_target_items)
        logger.info("Development mode: %s", self.development_mode)
        logger.info("Backend components: %s", list(self.backend_components.keys()))

        # HPC User client diagnostics
        if self.hpc_user_client:
            logger.info("HPC User API configured: %s", self.hpc_user_api_url)
            hpc_user_available = self.hpc_user_client.ping()
            logger.info("HPC User API accessible: %s", hpc_user_available)
            if not hpc_user_available:
                logger.warning("HPC User API not accessible, falling back to mock unixGid values")
        else:
            logger.info("HPC User API: Not configured (using mock unixGid values)")

        # Test basic functionality
        can_write = self.ping()
        logger.info("Backend functionality: %s", can_write)

        # Backend is functional as long as basic functionality works
        # HPC User service failure doesn't break backend since we have fallback
        return can_write

    def list_components(self) -> list[str]:
        """Return list of storage components."""
        return list(self.backend_components.keys())

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Return empty usage report (not applicable for storage backend)."""
        del resource_backend_ids
        return {}

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Not applicable for storage backend."""
        del resource_backend_id
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Not applicable for storage backend."""
        del resource_backend_id
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Not applicable for storage backend."""
        del resource_backend_id
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return empty metadata for storage resources."""
        del resource_backend_id
        return {}

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect storage limits from Waldur resource."""
        backend_limits = {}
        waldur_limits = {}

        # Extract storage size from resource limits or attributes
        if waldur_resource.limits:
            for component_name, limit_value in waldur_resource.limits.additional_properties.items():
                if component_name in self.backend_components:
                    backend_limits[component_name] = limit_value
                    waldur_limits[component_name] = limit_value

        return backend_limits, waldur_limits

    def _generate_mount_point(
        self,
        storage_system: str,
        tenant_id: str,
        customer: str,
        project_id: str,
        data_type: str = "store",
    ) -> str:
        """Generate mount point path based on hierarchy and storage data type."""
        return f"/{storage_system}/{data_type.lower()}/{tenant_id}/{customer}/{project_id}"

    def _generate_customer_mount_point(
        self,
        storage_system: str,
        tenant_id: str,
        customer: str,
        data_type: str = "store",
    ) -> str:
        """Generate mount point path for customer-level entry."""
        return f"/{storage_system}/{data_type.lower()}/{tenant_id}/{customer}"

    def _generate_tenant_mount_point(
        self,
        storage_system: str,
        tenant_id: str,
        data_type: str = "store",
    ) -> str:
        """Generate mount point path for tenant-level entry."""
        return f"/{storage_system}/{data_type.lower()}/{tenant_id}"

    def _calculate_inode_quotas(self, storage_quota_tb: float) -> tuple[int, int]:
        """Calculate inode quotas based on storage size and coefficients."""
        # Base calculation: storage in TB * configurable base multiplier
        base_inodes = storage_quota_tb * self.inode_base_multiplier
        soft_limit = int(base_inodes * self.inode_soft_coefficient)
        hard_limit = int(base_inodes * self.inode_hard_coefficient)
        return soft_limit, hard_limit

    def _get_project_unix_gid(self, project_slug: str) -> Optional[int]:
        """Get unixGid for project from HPC User service with caching.

        Cache persists until server restart. No TTL-based expiration.

        In production mode: Returns None if service fails (resource should be skipped)
        In development mode: Falls back to mock values if service fails

        Args:
            project_slug: Project slug to look up

        Returns:
            unixGid value from service, mock value (dev mode), or None (prod mode on failure)
        """
        # Check cache first
        if project_slug in self._gid_cache:
            cached_gid = self._gid_cache[project_slug]
            logger.debug("Found cached unixGid %d for project %s", cached_gid, project_slug)
            return cached_gid

        # Try to fetch from HPC User service
        if self.hpc_user_client:
            try:
                unix_gid = self.hpc_user_client.get_project_unix_gid(project_slug)
                if unix_gid is not None:
                    # Cache the successful result
                    self._gid_cache[project_slug] = unix_gid
                    logger.debug(
                        "Found and cached unixGid %d for project %s from HPC User service",
                        unix_gid,
                        project_slug,
                    )
                    return unix_gid

                # Project not found in service
                if self.development_mode:
                    logger.warning(
                        "Project %s not found in HPC User service, using mock value (dev mode)",
                        project_slug,
                    )
                else:
                    logger.error(
                        "Project %s not found in HPC User service, "
                        "skipping resource (production mode)",
                        project_slug,
                    )
                    return None

            except Exception as e:
                logger.error(
                    "Failed to fetch unixGid for project %s from HPC User service: %s",
                    project_slug,
                    e,
                )
                if self.development_mode:
                    logger.info(
                        "Falling back to mock unixGid for project %s (dev mode)", project_slug
                    )
                else:
                    logger.error(
                        "HPC User service unavailable for project %s, "
                        "skipping resource (production mode)",
                        project_slug,
                    )
                    return None

        # No HPC User client configured - use development mode behavior
        if not self.development_mode:
            logger.error(
                "HPC User service not configured for project %s, "
                "skipping resource (production mode)",
                project_slug,
            )
            return None

        # Development mode or no HPC client: use mock value and cache it
        mock_gid = 30000 + hash(project_slug) % 10000
        self._gid_cache[project_slug] = mock_gid
        logger.debug(
            "Using and caching mock unixGid %d for project %s (dev mode)", mock_gid, project_slug
        )
        return mock_gid

    def _get_offering_customers(self, offering_uuid: str, client: AuthenticatedClient) -> dict:
        """Get customers for a specific offering.

        Args:
            offering_uuid: UUID of the offering
            client: Authenticated Waldur API client

        Returns:
            Dictionary mapping customer slugs to customer information
        """
        try:
            response = marketplace_provider_offerings_customers_list.sync_detailed(
                uuid=offering_uuid, client=client
            )

            if not response.parsed:
                logger.warning("No customers found for offering %s", offering_uuid)
                return {}

            customers = {}
            for customer in response.parsed:
                customers[customer.slug] = {
                    "itemId": customer.uuid.hex,
                    "key": customer.slug,
                    "name": customer.name,
                    "uuid": customer.uuid.hex,
                }

            logger.debug("Found %d customers for offering %s", len(customers), offering_uuid)
            return customers

        except Exception as e:
            logger.error("Failed to fetch customers for offering %s: %s", offering_uuid, e)
            return {}

    def get_gid_cache_stats(self) -> dict:
        """Get statistics about the GID cache.

        Returns:
            Dictionary with cache statistics
        """
        max_projects_to_list = 10
        return {
            "total_entries": len(self._gid_cache),
            "cache_policy": "Persists until server restart",
            "projects": list(self._gid_cache.keys())
            if len(self._gid_cache) <= max_projects_to_list
            else f"{len(self._gid_cache)} projects (too many to list)",
        }

    def _get_target_status_from_waldur_state(self, waldur_resource: WaldurResource) -> str:
        """Map Waldur resource state to target item status (pending, active, removing)."""
        # Map Waldur resource state to target item status
        target_status_mapping = {
            "Creating": "pending",
            "OK": "active",
            "Erred": "pending",  # Treat errors as pending for target items
            "Terminating": "removing",
            "Terminated": "removing",  # Treat terminated as removing for target items
        }

        # Get status from waldur resource state, default to "pending"
        waldur_state = getattr(waldur_resource, "state", None)
        if waldur_state and not isinstance(waldur_state, Unset):
            return target_status_mapping.get(str(waldur_state), "pending")
        return "pending"

    def _get_target_item_data(  # noqa: PLR0911
        self, waldur_resource: WaldurResource, target_type: str
    ) -> Optional[dict]:
        """Get target item data from backend_metadata or generate mock data."""
        if not self.use_mock_target_items and waldur_resource.backend_metadata:
            # Try to get real data from backend_metadata
            target_data = waldur_resource.backend_metadata.additional_properties.get(
                f"{target_type}_item"
            )
            if target_data:
                return target_data

        # Generate mock data for development/testing
        if target_type == "tenant":
            return {
                "itemId": self._generate_deterministic_uuid(
                    f"tenant:{waldur_resource.customer_slug}"
                ),
                "key": waldur_resource.customer_slug.lower(),
                "name": waldur_resource.customer_name,
            }
        if target_type == "customer":
            return {
                "itemId": self._generate_deterministic_uuid(
                    f"customer:{waldur_resource.project_slug}"
                ),
                "key": waldur_resource.project_slug.lower(),
                "name": waldur_resource.project_name,
            }
        if target_type == "project":
            target_status = self._get_target_status_from_waldur_state(waldur_resource)
            unix_gid = self._get_project_unix_gid(waldur_resource.project_slug)
            if unix_gid is None:
                return None  # Skip resource when unixGid lookup fails in production
            return {
                "itemId": self._generate_deterministic_uuid(f"project:{waldur_resource.slug}"),
                "status": target_status,
                "name": waldur_resource.slug,
                "unixGid": unix_gid,
                "active": target_status == "active",  # Active only when status is "active"
            }
        if target_type == "user":
            target_status = self._get_target_status_from_waldur_state(waldur_resource)
            project_slug = (
                waldur_resource.project_slug
                if not isinstance(waldur_resource.project_slug, Unset)
                else "default-project"
            )
            # TODO: Just a placeholder, for user a default gid would be needed, which could be
            # looked up from https://api-user.hpc-user.tds.cscs.ch/api/v1/export/cscs/users/{username}
            unix_gid = self._get_project_unix_gid(project_slug)
            if unix_gid is None:
                return None  # Skip resource when unixGid lookup fails in production
            return {
                "itemId": self._generate_deterministic_uuid(f"user:{waldur_resource.slug}"),
                "status": target_status,
                "email": f"user-{waldur_resource.slug}@example.com",  # Mock email
                "unixUid": 20000 + hash(waldur_resource.slug) % 10000,  # Mock UID
                "primaryProject": {
                    "name": project_slug,
                    "unixGid": unix_gid,
                    "active": target_status == "active",  # Active only when status is "active"
                },
                "active": target_status == "active",  # Active only when status is "active"
            }

        return {}

    def _get_target_data(
        self, waldur_resource: WaldurResource, storage_data_type: str
    ) -> Optional[dict]:
        """Get target data based on storage data type mapping."""
        # Validate storage_data_type is a string
        if not isinstance(storage_data_type, str):
            error_msg = (
                f"Invalid storage_data_type for resource {waldur_resource.uuid}: "
                f"expected string, got {type(storage_data_type).__name__}. "
                f"Value: {storage_data_type!r}"
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Map storage data types to target types
        data_type_to_target = {
            "store": "project",
            "archive": "project",
            "users": "user",
            "scratch": "user",
        }

        # Validate that storage_data_type is a supported type
        if storage_data_type not in data_type_to_target:
            logger.warning(
                "Unknown storage_data_type '%s' for resource %s, using default 'project' "
                "target type. Supported types: %s",
                storage_data_type,
                waldur_resource.uuid,
                list(data_type_to_target.keys()),
            )

        target_type = data_type_to_target.get(storage_data_type, "project")
        logger.debug(
            "  Mapped storage_data_type '%s' to target_type '%s'",
            storage_data_type,
            target_type,
        )

        target_item = self._get_target_item_data(waldur_resource, target_type)
        if target_item is None:
            return (
                None  # Skip resource when target item creation fails (e.g., unixGid lookup fails)
            )

        return {
            "targetType": target_type,
            "targetItem": target_item,
        }

    def _create_storage_resource_json(
        self,
        waldur_resource: WaldurResource,
        storage_system: str,
        client: Optional[AuthenticatedClient] = None,
    ) -> Optional[dict]:
        """Create JSON structure for a single storage resource."""
        logger.debug("Creating storage resource JSON for resource %s", waldur_resource.uuid)
        logger.debug("  Input storage_system: %s (type: %s)", storage_system, type(storage_system))

        # Validate storage_system is a string
        if not isinstance(storage_system, str):
            error_msg = (
                f"Invalid storage_system type for resource {waldur_resource.uuid}: "
                f"expected string, got {type(storage_system).__name__}. "
                f"Value: {storage_system!r}"
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        if not storage_system:
            error_msg = (
                f"Empty storage_system provided for resource {waldur_resource.uuid}. "
                "A valid storage system name is required."
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        logger.debug("  Final storage_system: %s", storage_system)

        # Extract storage size from resource limits (assuming in terabytes)
        storage_quota_tb = 0.0
        if waldur_resource.limits:
            logger.debug("  Processing limits: %s", waldur_resource.limits.additional_properties)
            # Only accept 'storage' limit - be strict about supported limits
            storage_limit = waldur_resource.limits.additional_properties.get("storage")
            if storage_limit is not None:
                try:
                    storage_quota_tb = float(storage_limit)  # Assume already in TB
                    logger.debug("  Found storage limit: %s TB", storage_quota_tb)
                except (ValueError, TypeError):
                    logger.warning(
                        "  Invalid storage limit value for resource %s: %s (type: %s). Using 0 TB.",
                        waldur_resource.uuid,
                        storage_limit,
                        type(storage_limit).__name__,
                    )
            else:
                logger.debug("  No 'storage' limit found in limits")
        else:
            logger.debug("  No limits present")

        inode_soft, inode_hard = self._calculate_inode_quotas(storage_quota_tb)

        # Get permissions and data type from resource attributes first (needed for mount point)
        permissions = "775"  # default
        storage_data_type = "store"  # default

        if waldur_resource.attributes:
            logger.debug(
                "  Processing attributes: %s",
                waldur_resource.attributes.additional_properties,
            )

            perm_value = waldur_resource.attributes.additional_properties.get(
                "permissions", permissions
            )
            logger.debug("  Raw permissions value: %s (type: %s)", perm_value, type(perm_value))

            # Validate permissions is a string
            if perm_value is not None and not isinstance(perm_value, str):
                error_msg = (
                    f"Invalid permissions type for resource {waldur_resource.uuid}: "
                    f"expected string or None, got {type(perm_value).__name__}. "
                    f"Value: {perm_value!r}"
                )
                logger.error(error_msg)
                raise TypeError(error_msg)

            permissions = perm_value if perm_value else permissions
            logger.debug("  Final permissions: %s", permissions)

            storage_type_value = waldur_resource.attributes.additional_properties.get(
                "storage_data_type", storage_data_type
            )
            logger.debug(
                "  Raw storage_data_type value: %s (type: %s)",
                storage_type_value,
                type(storage_type_value),
            )

            # Validate storage_data_type is a string
            if storage_type_value is not None and not isinstance(storage_type_value, str):
                error_msg = (
                    f"Invalid storage_data_type for resource {waldur_resource.uuid}: "
                    f"expected string or None, got {type(storage_type_value).__name__}. "
                    f"Value: {storage_type_value!r}"
                )
                logger.error(error_msg)
                raise TypeError(error_msg)

            storage_data_type = storage_type_value if storage_type_value else storage_data_type
            logger.debug("  Final storage_data_type: %s", storage_data_type)
        else:
            logger.debug("  No attributes present, using defaults")

        # Generate mount point now that we have the storage_data_type
        mount_point = self._generate_mount_point(
            storage_system=storage_system,
            tenant_id=waldur_resource.provider_slug,
            customer=waldur_resource.customer_slug,
            project_id=waldur_resource.project_slug,  # might not be unique
            data_type=storage_data_type,
        )

        # Initialize separate soft and hard storage quota variables
        storage_quota_soft_tb = storage_quota_tb
        storage_quota_hard_tb = storage_quota_tb

        # Check for override values in the options field
        if waldur_resource.options:
            options_dict = waldur_resource.options
            logger.debug("  Processing options for overrides: %s", options_dict)

            # Override permissions if provided in options
            options_permissions = options_dict.get("permissions")
            if options_permissions is not None:
                if not isinstance(options_permissions, str):
                    logger.warning(
                        "  Invalid permissions type in options for resource %s: "
                        "expected string, got %s. Ignoring override.",
                        waldur_resource.uuid,
                        type(options_permissions).__name__,
                    )
                else:
                    permissions = options_permissions
                    logger.debug("  Override permissions from options: %s", permissions)

            # Override storage quotas if provided in options
            options_soft_quota = options_dict.get("soft_quota_space")
            options_hard_quota = options_dict.get("hard_quota_space")

            if options_soft_quota is not None:
                try:
                    storage_quota_soft_tb = float(options_soft_quota)
                    logger.debug(
                        "  Override storage soft quota from options: %s TB", storage_quota_soft_tb
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        "  Invalid soft_quota_space type in options for resource %s: "
                        "expected numeric, got %s. Ignoring override.",
                        waldur_resource.uuid,
                        type(options_soft_quota).__name__,
                    )

            if options_hard_quota is not None:
                try:
                    storage_quota_hard_tb = float(options_hard_quota)
                    logger.debug(
                        "  Override storage hard quota from options: %s TB", storage_quota_hard_tb
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        "  Invalid hard_quota_space type in options for resource %s: "
                        "expected numeric, got %s. Ignoring override.",
                        waldur_resource.uuid,
                        type(options_hard_quota).__name__,
                    )

            # Override inode quotas if provided in options
            options_soft_inodes = options_dict.get("soft_quota_indoes")
            options_hard_inodes = options_dict.get("hard_quota_indoes")

            if options_soft_inodes is not None or options_hard_inodes is not None:
                logger.debug(
                    "  Found inode quota overrides in options - soft: %s, hard: %s",
                    options_soft_inodes,
                    options_hard_inodes,
                )

                # Override calculated inode quotas
                if options_soft_inodes is not None:
                    try:
                        inode_soft = int(float(options_soft_inodes))
                        logger.debug("  Override inode soft quota from options: %d", inode_soft)
                    except (ValueError, TypeError):
                        logger.warning(
                            "  Invalid soft_quota_indoes type in options for resource %s: "
                            "expected numeric, got %s. Ignoring override.",
                            waldur_resource.uuid,
                            type(options_soft_inodes).__name__,
                        )

                if options_hard_inodes is not None:
                    try:
                        inode_hard = int(float(options_hard_inodes))
                        logger.debug("  Override inode hard quota from options: %d", inode_hard)
                    except (ValueError, TypeError):
                        logger.warning(
                            "  Invalid hard_quota_indoes type in options for resource %s: "
                            "expected numeric, got %s. Ignoring override.",
                            waldur_resource.uuid,
                            type(options_hard_inodes).__name__,
                        )
        else:
            logger.debug(
                "  No options present or no additional_properties, using calculated values"
            )

        # Map Waldur resource state to CSCS status
        status_mapping = {
            "Creating": "pending",
            "OK": "active",
            "Erred": "error",
            "Terminating": "removing",
            "Terminated": "removed",
        }

        # Get status from waldur resource state, default to "pending"
        waldur_state = getattr(waldur_resource, "state", None)
        if waldur_state and not isinstance(waldur_state, Unset):
            cscs_status = status_mapping.get(str(waldur_state), "pending")
        else:
            cscs_status = "pending"

        logger.debug("  Mapped waldur state '%s' to CSCS status '%s'", waldur_state, cscs_status)

        # Get target data - return None if target creation fails
        # (e.g., unixGid lookup fails in production)
        target_data = self._get_target_data(waldur_resource, storage_data_type.lower())
        if target_data is None:
            logger.warning(
                "Skipping resource %s due to target data creation failure (production mode)",
                waldur_resource.uuid,
            )
            return None

        # Create JSON structure
        storage_json = {
            "itemId": waldur_resource.uuid.hex,
            "status": cscs_status,
            "mountPoint": {"default": mount_point},
            "permission": {"permissionType": "octal", "value": permissions},
            "quotas": [
                {
                    "type": "space",
                    "quota": float(storage_quota_soft_tb),
                    "unit": "tera",
                    "enforcementType": "soft",
                },
                {
                    "type": "space",
                    "quota": float(storage_quota_hard_tb),
                    "unit": "tera",
                    "enforcementType": "hard",
                },
                {
                    "type": "inodes",
                    "quota": float(inode_soft),
                    "unit": "none",
                    "enforcementType": "soft",
                },
                {
                    "type": "inodes",
                    "quota": float(inode_hard),
                    "unit": "none",
                    "enforcementType": "hard",
                },
            ]
            if storage_quota_soft_tb > 0 or storage_quota_hard_tb > 0
            else None,
            "target": target_data,
            "storageSystem": {
                "itemId": self._generate_deterministic_uuid(f"storage_system:{storage_system}"),
                "key": storage_system.lower(),
                "name": storage_system.upper(),
                "active": True,
            },
            "storageFileSystem": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_file_system:{self.storage_file_system}"
                ),
                "key": self.storage_file_system.lower(),
                "name": self.storage_file_system.upper(),
                "active": True,
            },
            "storageDataType": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_data_type:{storage_data_type}"
                ),
                "key": storage_data_type.lower(),
                "name": storage_data_type.upper(),
                "path": storage_data_type.lower(),
                "active": True,
            },
            "parentItemId": None,  # Will be set for hierarchical resources
        }

        # Add provider action URLs if order is available from order_in_progress
        if (
            hasattr(waldur_resource, "order_in_progress")
            and not isinstance(waldur_resource.order_in_progress, Unset)
            and waldur_resource.order_in_progress is not None
            and hasattr(waldur_resource.order_in_progress, "uuid")
            and not isinstance(waldur_resource.order_in_progress.uuid, Unset)
        ):
            order_uuid = waldur_resource.order_in_progress.uuid

            # Get base URL from client if available
            base_url = ""
            if client:
                try:
                    httpx_client = client.get_httpx_client()
                    base_url = str(httpx_client.base_url).rstrip("/")
                except Exception as e:
                    logger.warning("Failed to get base URL from client: %s", e)

            # Ensure /api/ is in the URL but don't duplicate it
            api_path = "/api" if not base_url.endswith("/api") else ""

            # Provider review actions
            storage_json["approve_by_provider_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/approve_by_provider/"
            )
            storage_json["reject_by_provider_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/reject_by_provider/"
            )

            storage_json["set_state_executing_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/set_state_executing/"
            )
            storage_json["set_state_done_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/set_state_done/"
            )
            storage_json["set_state_erred_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/set_state_erred/"
            )

            # Provider order management actions
            storage_json["set_backend_id_url"] = (
                f"{base_url}{api_path}/marketplace-orders/{order_uuid}/set_backend_id/"
            )
            logger.debug(
                "Added provider action URLs to storage resource JSON for resource %s with order %s "
                "(base_url: %s)",
                waldur_resource.uuid,
                order_uuid,
                base_url,
            )

        # Add allowed transitions based on current order and resource states
        storage_json["allowed_transitions"] = self._get_allowed_transitions(
            waldur_resource, storage_json.get("state")
        )

        return storage_json

    def _get_allowed_transitions(
        self, waldur_resource: WaldurResource, resource_state: Optional[str]
    ) -> list[str]:
        """Determine allowed transitions based on current order and resource states.

        Args:
            waldur_resource: Waldur resource object
            resource_state: Current resource state string

        Returns:
            List of allowed action names
        """
        allowed_actions = []

        # Get order state if available
        order_state = None
        if (
            hasattr(waldur_resource, "order_in_progress")
            and not isinstance(waldur_resource.order_in_progress, Unset)
            and waldur_resource.order_in_progress is not None
            and hasattr(waldur_resource.order_in_progress, "state")
            and not isinstance(waldur_resource.order_in_progress.state, Unset)
        ):
            order_state = waldur_resource.order_in_progress.state

        # Order state transitions (based on OrderStates enum and transition rules)
        if order_state:
            # Provider review actions - available for PENDING_PROVIDER state
            if order_state == "pending-provider":
                allowed_actions.extend(["approve_by_provider", "reject_by_provider"])

            # Provider can set executing state from pending-provider state
            if order_state == "pending-provider":
                allowed_actions.append("set_state_executing")

            # Provider can set done/erred from executing state
            if order_state == "executing":
                allowed_actions.extend(["set_state_done", "set_state_erred"])

            # Backend ID can be set for any non-terminal order
            terminal_states = {"done", "erred", "canceled", "rejected"}
            if order_state not in terminal_states:
                allowed_actions.append("set_backend_id")

        # Resource state transitions (based on ResourceStates enum)
        # End date can be set by provider for active resources
        if resource_state and resource_state in {"Creating", "OK", "Erred", "Updating"}:
            allowed_actions.append("set_end_date_by_provider")

        return list(set(allowed_actions))  # Remove duplicates

    def _create_tenant_storage_resource_json(
        self,
        tenant_id: str,
        tenant_name: str,
        storage_system: str,
        storage_data_type: str,
        offering_uuid: Optional[str] = None,
    ) -> dict:
        """Create JSON structure for a tenant-level storage resource."""
        logger.debug("Creating tenant storage resource JSON for tenant %s", tenant_id)

        # Generate tenant mount point
        mount_point = self._generate_tenant_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            data_type=storage_data_type,
        )

        # Use offering UUID if provided, otherwise generate deterministic UUID
        tenant_item_id = (
            offering_uuid
            if offering_uuid
            else self._generate_deterministic_uuid(
                f"tenant:{tenant_id}-{storage_system}-{storage_data_type}"
            )
        )

        return {
            "itemId": tenant_item_id,
            "status": "pending",  # Tenant entries are always pending
            "mountPoint": {"default": mount_point},
            "permission": {
                "permissionType": "octal",
                "value": "775",  # Default permissions for tenant level
            },
            "quotas": None,  # Tenant entries don't have quotas
            "target": {
                "targetType": "tenant",
                "targetItem": {
                    "itemId": offering_uuid
                    if offering_uuid
                    else self._generate_deterministic_uuid(f"tenant:{tenant_id}"),
                    "key": tenant_id.lower(),
                    "name": tenant_name,
                },
            },
            "storageSystem": {
                "itemId": self._generate_deterministic_uuid(f"storage_system:{storage_system}"),
                "key": storage_system.lower(),
                "name": storage_system.upper(),
                "active": True,
            },
            "storageFileSystem": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_file_system:{self.storage_file_system}"
                ),
                "key": self.storage_file_system.lower(),
                "name": self.storage_file_system.upper(),
                "active": True,
            },
            "storageDataType": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_data_type:{storage_data_type}"
                ),
                "key": storage_data_type.lower(),
                "name": storage_data_type.upper(),
                "active": True,
            },
            "parentItemId": None,  # Tenant entries are top-level
        }

    def _create_customer_storage_resource_json(
        self,
        customer_info: dict,
        storage_system: str,
        storage_data_type: str,
        tenant_id: str,
        parent_tenant_id: Optional[str] = None,
    ) -> dict:
        """Create JSON structure for a customer-level storage resource."""
        logger.debug(
            "Creating customer storage resource JSON for customer %s", customer_info["key"]
        )

        # Generate customer mount point
        mount_point = self._generate_customer_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_info["key"],
            data_type=storage_data_type,
        )

        return {
            "itemId": customer_info["itemId"],
            "status": "pending",  # Customer entries are always pending
            "mountPoint": {"default": mount_point},
            "permission": {
                "permissionType": "octal",
                "value": "775",  # Default permissions for customer level
            },
            "quotas": None,  # Customer entries typically don't have quotas
            "target": {
                "targetType": "customer",
                "targetItem": {
                    "itemId": customer_info["itemId"],
                    "key": customer_info["key"],
                    "name": customer_info["name"],
                },
            },
            "storageSystem": {
                "itemId": self._generate_deterministic_uuid(f"storage_system:{storage_system}"),
                "key": storage_system.lower(),
                "name": storage_system.upper(),
                "active": True,
            },
            "storageFileSystem": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_file_system:{self.storage_file_system}"
                ),
                "key": self.storage_file_system.lower(),
                "name": self.storage_file_system.upper(),
                "active": True,
            },
            "storageDataType": {
                "itemId": self._generate_deterministic_uuid(
                    f"storage_data_type:{storage_data_type}"
                ),
                "key": storage_data_type.lower(),
                "name": storage_data_type.upper(),
                "active": True,
            },
            "parentItemId": parent_tenant_id,  # Reference to parent tenant entry
        }

    def _get_all_storage_resources(
        self,
        offering_uuid: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        """Fetch storage resources from Waldur API with pagination and filtering support.

        Args:
            offering_uuid: UUID of the offering to fetch resources for
            client: Authenticated Waldur API client for API access
            state: Optional resource state filter
            page: Page number (1-based)
            page_size: Number of items per page
            storage_system: Optional filter for storage system (e.g., 'capstor', 'vast', 'iopsstor')
            data_type: Optional filter for data type (e.g., 'users', 'scratch', 'store', 'archive')
            status: Optional filter for status (e.g., 'pending', 'removing', 'active')

        Returns:
            Tuple of (storage resource list, pagination info dict)
        """
        try:
            # Fetch paginated resources from Waldur API using sync_detailed
            filters = {}
            if state:
                filters["state"] = state

            # Use sync_detailed to get both content and headers
            response = marketplace_resources_list.sync_detailed(
                client=client,
                offering_uuid=[offering_uuid],
                page=page,
                page_size=page_size,
                **filters,
            )

            # Extract resources from response parsed data
            # sync_detailed returns parsed objects in response.parsed
            waldur_resources = response.parsed if response.parsed else []

            # Extract pagination info from headers
            total_count = 0
            # Headers is a httpx.Headers object, access it like a dict (case-insensitive)
            header_value = response.headers.get("x-result-count")
            if header_value:
                try:
                    total_count = int(header_value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid X-Result-Count header value: {header_value}")

            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
            offset = (page - 1) * page_size

            pagination_info = {
                "current": page,
                "limit": page_size,
                "offset": offset,
                "pages": total_pages,
                "total": total_count,
            }

            # Get offering customers for hierarchical resources
            offering_customers = self._get_offering_customers(offering_uuid, client)

            # Convert Waldur resources to storage JSON format
            # We'll create tenant, customer-level and project-level entries (three-tier hierarchy)
            storage_resources = []
            tenant_entries = {}  # Track unique tenant entries by tenant_id-storage_system-data_type
            customer_entries = {}  # Track unique customer entries by slug-system-data_type

            for i, resource in enumerate(waldur_resources):
                # Log raw resource data for debugging
                logger.info("Processing resource %d/%d", i + 1, len(waldur_resources))
                logger.info(f"Resource {resource.uuid} / {resource.name}")
                logger.debug("Raw resource data from Waldur SDK:")
                logger.debug(
                    "  Slug: %s",
                    resource.slug if not isinstance(resource.slug, Unset) else "Unset",
                )
                logger.debug(
                    "  State: %s",
                    resource.state if not isinstance(resource.state, Unset) else "Unset",
                )
                logger.debug(
                    "  Customer: slug=%s, name=%s, uuid=%s",
                    resource.customer_slug
                    if not isinstance(resource.customer_slug, Unset)
                    else "Unset",
                    resource.customer_name
                    if not isinstance(resource.customer_name, Unset)
                    else "Unset",
                    resource.customer_uuid
                    if not isinstance(resource.customer_uuid, Unset)
                    else "Unset",
                )
                logger.debug(
                    "  Project: slug=%s, name=%s, uuid=%s",
                    resource.project_slug
                    if not isinstance(resource.project_slug, Unset)
                    else "Unset",
                    resource.project_name
                    if not isinstance(resource.project_name, Unset)
                    else "Unset",
                    resource.project_uuid
                    if not isinstance(resource.project_uuid, Unset)
                    else "Unset",
                )
                logger.debug(
                    "  Offering: slug=%s, uuid=%s, type=%s",
                    resource.offering_slug
                    if not isinstance(resource.offering_slug, Unset)
                    else "Unset",
                    resource.offering_uuid
                    if not isinstance(resource.offering_uuid, Unset)
                    else "Unset",
                    resource.offering_type
                    if not isinstance(resource.offering_type, Unset)
                    else "Unset",
                )

                # Log limits if present
                if resource.limits and not isinstance(resource.limits, Unset):
                    logger.debug("  Limits: %s", resource.limits.additional_properties)
                else:
                    logger.debug("  Limits: None or Unset")

                # Log attributes if present
                if resource.attributes and not isinstance(resource.attributes, Unset):
                    logger.debug("  Attributes: %s", resource.attributes.additional_properties)
                else:
                    logger.debug("  Attributes: None or Unset")

                # Validate resource data before processing
                self._validate_resource_data(resource)

                # Use offering_slug as the storage system name
                storage_system_name = resource.offering_slug
                logger.debug("  Using storage_system from offering_slug: %s", storage_system_name)

                # Get storage data type for the resource
                storage_data_type = "store"  # default
                if resource.attributes and not isinstance(resource.attributes, Unset):
                    storage_data_type = resource.attributes.additional_properties.get(
                        "storage_data_type", storage_data_type
                    )

                # Get tenant information
                tenant_id = resource.provider_slug  # tenant is the offering customer
                tenant_name = (
                    resource.provider_name
                    if hasattr(resource, "provider_name")
                    and not isinstance(resource.provider_name, Unset)
                    else tenant_id.upper()
                )

                # Create tenant-level entry if not already created for this combination
                tenant_key = f"{tenant_id}-{storage_system_name}-{storage_data_type}"
                if tenant_key not in tenant_entries:
                    # Get offering UUID from resource (use str() for proper UUID format)
                    offering_uuid_str = (
                        str(resource.offering_uuid)
                        if hasattr(resource, "offering_uuid")
                        and not isinstance(resource.offering_uuid, Unset)
                        else None
                    )

                    tenant_resource = self._create_tenant_storage_resource_json(
                        tenant_id=tenant_id,
                        tenant_name=tenant_name,
                        storage_system=storage_system_name,
                        storage_data_type=storage_data_type,
                        offering_uuid=offering_uuid_str,
                    )
                    storage_resources.append(tenant_resource)
                    tenant_entries[tenant_key] = tenant_resource["itemId"]
                    logger.debug(
                        "Created tenant entry for %s with offering UUID %s",
                        tenant_key,
                        offering_uuid_str,
                    )

                # Create customer-level entry if not already created for this combination
                customer_key = f"{resource.customer_slug}-{storage_system_name}-{storage_data_type}"
                if (
                    customer_key not in customer_entries
                    and resource.customer_slug in offering_customers
                ):
                    customer_info = offering_customers[resource.customer_slug]
                    # Get parent tenant ID for this customer
                    parent_tenant_id = tenant_entries.get(tenant_key)

                    customer_resource = self._create_customer_storage_resource_json(
                        customer_info=customer_info,
                        storage_system=storage_system_name,
                        storage_data_type=storage_data_type,
                        tenant_id=tenant_id,
                        parent_tenant_id=parent_tenant_id,  # Link to parent tenant
                    )
                    storage_resources.append(customer_resource)
                    customer_entries[customer_key] = customer_resource["itemId"]
                    logger.debug(
                        "Created customer entry for %s with parent tenant %s",
                        customer_key,
                        parent_tenant_id,
                    )

                # Check transitional state and skip if order is not pending-provider
                if (
                    hasattr(resource, "state")
                    and not isinstance(resource.state, Unset)
                    and resource.state in ["Creating", "Terminating", "Updating"]
                ):
                    # For transitional resources, only process if order is in pending-provider state
                    if (
                        hasattr(resource, "order_in_progress")
                        and not isinstance(resource.order_in_progress, Unset)
                        and resource.order_in_progress is not None
                    ):
                        # Check order state
                        if (
                            hasattr(resource.order_in_progress, "state")
                            and not isinstance(resource.order_in_progress.state, Unset)
                            and resource.order_in_progress.state
                            in ["pending-consumer", "pending-project", "pending-start-date"]
                        ):
                            logger.info(
                                "Skipping resource %s in transitional state (%s) - "
                                "order state is %s, which is in early pending states",
                                resource.uuid,
                                resource.state,
                                resource.order_in_progress.state,
                            )
                            continue

                        # Display order URL for transitional resources with pending-provider order
                        if hasattr(resource.order_in_progress, "url") and not isinstance(
                            resource.order_in_progress.url, Unset
                        ):
                            logger.info(
                                "Resource in transitional state (%s) with pending-provider order - "
                                "Order URL: %s",
                                resource.state,
                                resource.order_in_progress.url,
                            )
                        else:
                            # Log that URL field is not available
                            logger.warning(
                                "Resource in transitional state (%s) with pending-provider order "
                                "but order URL not available",
                                resource.state,
                            )
                    else:
                        # No order in progress for transitional resource - skip it
                        logger.info(
                            "Skipping resource %s in transitional state (%s) - no order",
                            resource.uuid,
                            resource.state,
                        )
                        continue

                # Create project-level resource (the original resource)
                storage_resource = self._create_storage_resource_json(
                    resource, storage_system_name, client
                )
                if storage_resource is not None:
                    # Set parent reference if customer entry exists
                    if customer_key in customer_entries:
                        storage_resource["parentItemId"] = customer_entries[customer_key]
                        logger.debug(
                            "Set parentItemId %s for resource %s",
                            customer_entries[customer_key],
                            resource.uuid,
                        )

                    storage_resources.append(storage_resource)

            # Apply filters to the converted storage resources
            storage_resources = self._apply_filters(
                storage_resources, storage_system, data_type, status
            )

            # Update pagination info based on filtered results
            filtered_count = len(storage_resources)
            filtered_pages = (
                (filtered_count + page_size - 1) // page_size if filtered_count > 0 else 1
            )

            pagination_info.update(
                {
                    "total": filtered_count,
                    "pages": filtered_pages,
                }
            )

            logger.info(
                "Retrieved %d filtered storage resources for offering %s (page %d/%d, total: %d)",
                len(storage_resources),
                offering_uuid,
                page,
                pagination_info["pages"],
                pagination_info["total"],
            )
            return storage_resources, pagination_info

        except Exception as e:
            logger.error("Failed to fetch storage resources from Waldur API: %s", e)
            # Re-raise the exception to be handled by the caller
            raise

    def get_debug_resources(
        self,
        offering_uuid: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        """Get raw Waldur resources for debug mode without translation.

        Returns raw resource data with minimal processing for debugging purposes.
        """
        try:
            # First, fetch offering details from Waldur API
            waldur_offering: dict[str, object] = {}
            try:
                offering_response = marketplace_provider_offerings_retrieve.sync_detailed(
                    client=client,
                    uuid=offering_uuid,
                )

                if offering_response.parsed:
                    offering = offering_response.parsed

                    # Helper function to safely convert values to JSON-serializable format
                    def serialize_value(value: object) -> object:  # noqa: PLR0911
                        # Handle None and Unset values
                        if isinstance(value, Unset) or value is None:
                            return None

                        # Handle primitive types that are already JSON-serializable
                        if isinstance(value, (str, int, float, bool)):
                            return value

                        # Handle collections
                        if isinstance(value, (list, tuple)):
                            return [serialize_value(item) for item in value]
                        if isinstance(value, dict):
                            return {k: serialize_value(v) for k, v in value.items()}

                        # Handle special object types
                        if hasattr(value, "hex") and callable(value.hex):
                            return value.hex  # UUID objects
                        if hasattr(value, "isoformat") and callable(value.isoformat):
                            return value.isoformat()  # datetime objects
                        if hasattr(value, "additional_properties"):
                            return serialize_value(value.additional_properties)  # API client models

                        # Handle complex objects by converting their public attributes to dict
                        if hasattr(value, "__dict__"):
                            try:
                                result = {}
                                for k, v in value.__dict__.items():
                                    # Skip private/protected attributes and methods
                                    if not k.startswith("_") and not callable(v):
                                        try:
                                            result[k] = serialize_value(v)
                                        except Exception as e:
                                            # If we can't serialize nested value, convert to string
                                            logger.debug(
                                                "Could not serialize nested value %s.%s: %s",
                                                type(value).__name__,
                                                k,
                                                e,
                                            )
                                            result[k] = str(v)
                                return result
                            except Exception as e:
                                # If we can't access __dict__, convert to string
                                logger.debug(
                                    "Could not serialize object %s: %s", type(value).__name__, e
                                )
                                return str(value)

                        # For any other type, convert to string as fallback
                        try:
                            return str(value)
                        except Exception:
                            return f"<{type(value).__name__} object>"

                    # Get all attributes from the offering object, excluding secret_options
                    waldur_offering = {}
                    for attr_name in dir(offering):
                        # Skip private/magic methods and secret_options
                        if attr_name.startswith("_") or attr_name == "secret_options":
                            continue

                        try:
                            attr_value = getattr(offering, attr_name)
                            # Skip methods/callables
                            if callable(attr_value):
                                continue

                            # Serialize the value to be JSON-safe
                            waldur_offering[attr_name] = serialize_value(attr_value)

                        except Exception as e:
                            # If we can't access an attribute, log it but continue
                            logger.debug("Could not access attribute %s: %s", attr_name, e)
                            waldur_offering[attr_name] = f"Error accessing attribute: {e}"
            except Exception as e:
                logger.warning("Failed to fetch offering details: %s", e)
                waldur_offering = {"error": f"Failed to fetch offering details: {e}"}

            # Then fetch resources from Waldur API
            filters = {}
            if state:
                filters["state"] = state

            response = marketplace_resources_list.sync_detailed(
                client=client,
                offering_uuid=[offering_uuid],
                page=page,
                page_size=page_size,
                **filters,
            )

            waldur_resources = response.parsed if response.parsed else []

            # Convert resources to dictionaries, filtering by offering_slug if needed
            raw_resources = []
            for resource in waldur_resources:
                # Apply storage_system filter based on offering_slug
                if storage_system and resource.offering_slug != storage_system:
                    continue

                # Apply data_type filter if present in attributes
                if data_type and resource.attributes and not isinstance(resource.attributes, Unset):
                    resource_data_type = resource.attributes.additional_properties.get(
                        "storage_data_type"
                    )
                    if resource_data_type != data_type:
                        continue

                # Apply status filter based on state mapping
                if status:
                    status_mapping = {
                        "Creating": "pending",
                        "OK": "active",
                        "Erred": "error",
                        "Terminating": "removing",
                        "Terminated": "removed",
                    }
                    resource_status = status_mapping.get(str(resource.state), "pending")
                    if resource_status != status:
                        continue

                # Convert resource to dictionary format
                resource_dict = {
                    "uuid": resource.uuid.hex if not isinstance(resource.uuid, Unset) else None,
                    "name": resource.name if not isinstance(resource.name, Unset) else None,
                    "slug": resource.slug if not isinstance(resource.slug, Unset) else None,
                    "state": str(resource.state) if not isinstance(resource.state, Unset) else None,
                    "customer_slug": (
                        resource.customer_slug
                        if not isinstance(resource.customer_slug, Unset)
                        else None
                    ),
                    "customer_name": (
                        resource.customer_name
                        if not isinstance(resource.customer_name, Unset)
                        else None
                    ),
                    "project_slug": (
                        resource.project_slug
                        if not isinstance(resource.project_slug, Unset)
                        else None
                    ),
                    "project_name": (
                        resource.project_name
                        if not isinstance(resource.project_name, Unset)
                        else None
                    ),
                    "offering_slug": (
                        resource.offering_slug
                        if not isinstance(resource.offering_slug, Unset)
                        else None
                    ),
                    "offering_type": (
                        resource.offering_type
                        if not isinstance(resource.offering_type, Unset)
                        else None
                    ),
                    "limits": (
                        resource.limits.additional_properties
                        if resource.limits and not isinstance(resource.limits, Unset)
                        else {}
                    ),
                    "attributes": (
                        resource.attributes.additional_properties
                        if resource.attributes and not isinstance(resource.attributes, Unset)
                        else {}
                    ),
                    "backend_metadata": (
                        resource.backend_metadata.additional_properties
                        if resource.backend_metadata
                        and not isinstance(resource.backend_metadata, Unset)
                        else {}
                    ),
                    "created": (
                        resource.created.isoformat()
                        if hasattr(resource, "created") and not isinstance(resource.created, Unset)
                        else None
                    ),
                    "modified": (
                        resource.modified.isoformat()
                        if hasattr(resource, "modified")
                        and not isinstance(resource.modified, Unset)
                        else None
                    ),
                }
                raw_resources.append(resource_dict)

            # Update count based on filtered results
            filtered_count = len(raw_resources)
            filtered_pages = (
                (filtered_count + page_size - 1) // page_size if filtered_count > 0 else 1
            )

            return {
                "waldur_offering": waldur_offering,
                "resources": raw_resources,
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": filtered_pages,
                    "total": filtered_count,
                },
                "filters_applied": {
                    "storage_system": storage_system,
                    "data_type": data_type,
                    "status": status,
                    "state": str(state) if state else None,
                },
            }

        except Exception as e:
            logger.error("Error fetching debug resources: %s", e)
            return {
                "error": str(e),
                "waldur_offering": {"error": f"Failed to fetch offering due to error: {e}"},
                "resources": [],
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": 0,
                    "total": 0,
                },
            }

    def generate_all_resources_json(
        self,
        offering_uuid: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        """Generate JSON data with all storage resources with pagination support."""
        try:
            storage_resources, pagination_info = self._get_all_storage_resources(
                offering_uuid,
                client,
                state,
                page=page,
                page_size=page_size,
                storage_system=storage_system,
                data_type=data_type,
                status=status,
            )

            return {
                "status": "success",
                "code": 200,
                "meta": {"date": datetime.now().isoformat(), "appVersion": "1.4.0"},
                "result": {
                    "storageResources": storage_resources,
                    "paginate": pagination_info,
                },
            }

        except Exception as e:
            logger.error("Error generating storage resources JSON: %s", e)
            # Return error response instead of empty results
            return {
                "status": "error",
                "code": 500,
                "meta": {"date": datetime.now().isoformat(), "appVersion": "1.4.0"},
                "message": f"Failed to fetch storage resources: {e!s}",
                "result": {
                    "storageResources": [],
                    "paginate": {
                        "current": page,
                        "limit": page_size,
                        "offset": (page - 1) * page_size,
                        "pages": 0,
                        "total": 0,
                    },
                },
            }

    def create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create storage resource."""
        del user_context
        logger.info("Creating CSCS storage resource: %s", waldur_resource.name)

        # Return resource structure
        return BackendResourceInfo(
            backend_id=waldur_resource.slug,  # Use slug as backend ID
            limits=self._collect_resource_limits(waldur_resource)[1],
        )

    def generate_all_resources_json_by_slugs(
        self,
        offering_slugs: list[str],
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        storage_system_filter: Optional[str] = None,
    ) -> dict:
        """Generate JSON with resources filtered by multiple offering slugs."""
        try:
            storage_resources, pagination_info = self._get_resources_by_offering_slugs(
                offering_slugs=offering_slugs,
                client=client,
                state=state,
                page=page,
                page_size=page_size,
                data_type=data_type,
                status=status,
                storage_system_filter=storage_system_filter,
            )

            return {
                "status": "success",
                "resources": storage_resources,
                "pagination": pagination_info,
                "filters_applied": {
                    "offering_slugs": offering_slugs,
                    "storage_system": storage_system_filter,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error("Failed to generate storage resources JSON: %s", e, exc_info=True)
            return {
                "status": "error",
                "error": f"Failed to fetch storage resources: {e}",
                "code": 500,
            }

    def generate_all_resources_json_by_slug(
        self,
        offering_slug: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        """Generate JSON with resources filtered by offering slug instead of UUID."""
        try:
            storage_resources, pagination_info = self._get_resources_by_offering_slug(
                offering_slug=offering_slug,
                client=client,
                state=state,
                page=page,
                page_size=page_size,
                data_type=data_type,
                status=status,
            )

            return {
                "status": "success",
                "resources": storage_resources,
                "pagination": pagination_info,
                "filters_applied": {
                    "offering_slug": offering_slug,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error("Failed to generate storage resources JSON: %s", e, exc_info=True)
            return {
                "status": "error",
                "error": f"Failed to fetch storage resources: {e}",
                "code": 500,
            }

    def get_debug_resources_by_slugs(
        self,
        offering_slugs: list[str],
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        storage_system_filter: Optional[str] = None,
    ) -> dict:
        """Get raw Waldur resources for debug mode without translation (multiple slugs)."""
        try:
            # Note: Offering details fetching removed - API doesn't support slug-based filtering
            # The offering information is available in the resource data itself
            waldur_offerings: dict[str, dict] = {
                slug: {"note": "Offering details available in resource data"}
                for slug in offering_slugs
            }

            # Fetch raw resources filtered by offering slugs
            filters = {
                "client": client,
                "page": page,
                "page_size": page_size,
                "offering_slug": offering_slugs,
            }
            if state:
                filters["state"] = state

            response = marketplace_resources_list.sync_detailed(**filters)

            raw_resources = []
            total_count = int(response.headers.get("x-total-count", "0"))

            if response.parsed:
                for resource in response.parsed:
                    # Apply storage_system filter if provided
                    if storage_system_filter and resource.offering_slug != storage_system_filter:
                        continue

                    # Apply additional filters
                    if not self._resource_matches_filters(resource, data_type, status):
                        continue

                    raw_resources.append(self._serialize_resource(resource))

            pagination_info = {
                "current": page,
                "limit": page_size,
                "offset": (page - 1) * page_size,
                "pages": (len(raw_resources) + page_size - 1) // page_size if raw_resources else 0,
                "total": len(raw_resources),
                "api_total": total_count,
            }

            return {
                "offering_details": waldur_offerings,
                "resources": raw_resources,
                "pagination": pagination_info,
                "filters_applied": {
                    "offering_slugs": offering_slugs,
                    "storage_system": storage_system_filter,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error("Failed to fetch debug resources by slugs: %s", e, exc_info=True)
            return {
                "error": f"Failed to fetch debug resources: {e}",
                "offering_details": {},
                "resources": [],
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": 0,
                    "total": 0,
                },
            }

    def get_debug_resources_by_slug(
        self,
        offering_slug: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        """Get raw resource data filtered by offering slug for debugging."""
        try:
            # Fetch resources directly with offering slug filter
            filters = {
                "client": client,
                "page": page,
                "page_size": page_size,
                "offering_slug": [offering_slug],  # Filter by offering slug
            }
            if state:
                filters["state"] = state

            response = marketplace_resources_list.sync_detailed(**filters)

            if not response.parsed:
                return {
                    "resources": [],
                    "pagination": {
                        "current": page,
                        "limit": page_size,
                        "offset": (page - 1) * page_size,
                        "pages": 0,
                        "total": 0,
                    },
                    "filters_applied": {
                        "offering_slug": offering_slug,
                        "data_type": data_type,
                        "status": status,
                        "state": state.value if state else None,
                    },
                }

            resources = response.parsed
            # Extract pagination info from response headers
            total_count = int(response.headers.get("X-Total-Count", len(resources)))

            # Serialize resources for JSON response first
            serialized_resources = []
            for resource in resources:
                try:
                    serialized_resource = self._serialize_resource(resource)
                    serialized_resources.append(serialized_resource)
                except Exception as e:
                    resource_id = getattr(resource, "uuid", "unknown")
                    logger.warning("Failed to serialize resource %s: %s", resource_id, e)

            # Apply additional filters (data_type, status) in memory after serialization
            logger.debug(
                "About to apply filters on %d serialized resources", len(serialized_resources)
            )
            filtered_resources = self._apply_filters(serialized_resources, None, data_type, status)

            # Update pagination info based on filtered results
            filtered_count = len(filtered_resources)
            pages = (filtered_count + page_size - 1) // page_size

            return {
                "resources": filtered_resources,
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": max(1, pages),
                    "total": filtered_count,
                    "raw_total_from_api": total_count,
                },
                "filters_applied": {
                    "offering_slug": offering_slug,
                    "data_type": data_type,
                    "status": status,
                    "state": state.value if state else None,
                },
            }

        except Exception as e:
            logger.error("Failed to fetch debug resources by slug: %s", e, exc_info=True)
            return {
                "error": f"Failed to fetch resources: {e}",
                "resources": [],
                "pagination": {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": 0,
                    "total": 0,
                },
            }

    def _get_resources_by_offering_slugs(
        self,
        offering_slugs: list[str],
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        storage_system_filter: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        """Fetch and process resources filtered by multiple offering slugs."""
        logger.debug("_get_resources_by_offering_slugs called with data_type=%s", data_type)
        try:
            # Use single API call with comma-separated offering slugs
            logger.info("Fetching resources for offering slugs: %s", ", ".join(offering_slugs))

            filters = {
                "client": client,
                "page": page,
                "page_size": page_size,
                "offering_slug": ",".join(offering_slugs),  # Comma-separated slugs for Waldur API
            }
            if state:
                filters["state"] = state

            response = marketplace_resources_list.sync_detailed(**filters)

            all_storage_resources = []
            total_api_count = 0

            if response.parsed:
                # Get count from headers
                total_api_count = int(response.headers.get("x-result-count", "0"))
                logger.debug("Response headers: %s", dict(response.headers))
                logger.debug("Total API count from headers: %d", total_api_count)

                logger.info(
                    "Found %d resources from API (total: %d)", len(response.parsed), total_api_count
                )

                # Get offering customers for hierarchical resources
                # For multiple slugs, we need to get customers for all unique offerings
                offering_uuids = set()
                for resource in response.parsed:
                    if hasattr(resource, "offering_uuid") and not isinstance(
                        resource.offering_uuid, Unset
                    ):
                        offering_uuids.add(resource.offering_uuid.hex)

                all_offering_customers = {}
                for offering_uuid in offering_uuids:
                    customers = self._get_offering_customers(offering_uuid, client)
                    all_offering_customers.update(customers)  # Merge all customers

                tenant_entries = {}  # Track unique tenant entries
                customer_entries = {}  # Track unique customer entries

                logger.debug("Starting to process %d resources", len(response.parsed))
                for resource in response.parsed:
                    try:
                        # Apply storage_system filter if provided
                        if (
                            storage_system_filter
                            and resource.offering_slug != storage_system_filter
                        ):
                            logger.debug("Skipping resource due to storage_system filter")
                            continue

                        # Note: Additional filters (data_type, status) are applied
                        # after serialization

                        # Get storage data type for the resource
                        storage_data_type = "store"  # default
                        if resource.attributes and not isinstance(resource.attributes, Unset):
                            storage_data_type = resource.attributes.additional_properties.get(
                                "storage_data_type", storage_data_type
                            )

                        # Get tenant information
                        tenant_id = resource.provider_slug
                        tenant_name = (
                            resource.provider_name
                            if hasattr(resource, "provider_name")
                            and not isinstance(resource.provider_name, Unset)
                            else tenant_id.upper()
                        )

                        # Create tenant-level entry if not already created for this combination
                        tenant_key = f"{tenant_id}-{resource.offering_slug}-{storage_data_type}"
                        if tenant_key not in tenant_entries:
                            # Get offering UUID from resource
                            offering_uuid_str = (
                                str(resource.offering_uuid)
                                if hasattr(resource, "offering_uuid")
                                and not isinstance(resource.offering_uuid, Unset)
                                else None
                            )

                            tenant_resource = self._create_tenant_storage_resource_json(
                                tenant_id=tenant_id,
                                tenant_name=tenant_name,
                                storage_system=resource.offering_slug,
                                storage_data_type=storage_data_type,
                                offering_uuid=offering_uuid_str,
                            )
                            all_storage_resources.append(tenant_resource)
                            tenant_entries[tenant_key] = tenant_resource["itemId"]
                            logger.debug(
                                "Created tenant entry for %s with offering UUID %s",
                                tenant_key,
                                offering_uuid_str,
                            )

                        # Create customer-level entry if not already created for this combination
                        customer_key = (
                            f"{resource.customer_slug}-{resource.offering_slug}-{storage_data_type}"
                        )
                        if (
                            customer_key not in customer_entries
                            and resource.customer_slug in all_offering_customers
                        ):
                            customer_info = all_offering_customers[resource.customer_slug]
                            parent_tenant_id = tenant_entries.get(tenant_key)

                            customer_resource = self._create_customer_storage_resource_json(
                                customer_info=customer_info,
                                storage_system=resource.offering_slug,
                                storage_data_type=storage_data_type,
                                tenant_id=tenant_id,
                                parent_tenant_id=parent_tenant_id,
                            )
                            all_storage_resources.append(customer_resource)
                            customer_entries[customer_key] = customer_resource["itemId"]
                            logger.debug(
                                "Created customer entry for %s with parent tenant %s",
                                customer_key,
                                parent_tenant_id,
                            )

                        # Create project-level resource (the original resource)
                        storage_resource = self._create_storage_resource_json(
                            resource, resource.offering_slug, client
                        )
                        if storage_resource is not None:
                            # Set parent reference if customer entry exists
                            if customer_key in customer_entries:
                                storage_resource["parentItemId"] = customer_entries[customer_key]
                                logger.debug(
                                    "Set parentItemId %s for resource %s",
                                    customer_entries[customer_key],
                                    resource.uuid,
                                )

                            all_storage_resources.append(storage_resource)

                    except Exception as e:
                        logger.warning(
                            "Failed to process resource %s: %s",
                            getattr(resource, "uuid", "unknown"),
                            e,
                        )
                        continue
            else:
                logger.warning(
                    "No resources found for offering slugs: %s", ", ".join(offering_slugs)
                )

            storage_resources = all_storage_resources
            total_count = total_api_count

            # Apply additional filters (data_type, status) in memory after JSON serialization
            logger.debug("About to apply filters on %d resources", len(storage_resources))
            filtered_resources = self._apply_filters(storage_resources, None, data_type, status)
            storage_resources = filtered_resources

            # Calculate pagination based on filtered results
            total_pages = (
                (len(storage_resources) + page_size - 1) // page_size if storage_resources else 0
            )
            pagination_info = {
                "current": page,
                "limit": page_size,
                "offset": (page - 1) * page_size,
                "pages": total_pages,
                "total": len(storage_resources),
                "api_total": total_count,
            }

            return storage_resources, pagination_info

        except Exception as e:
            logger.error("Failed to fetch storage resources by slugs: %s", e, exc_info=True)
            raise

    def _get_resources_by_offering_slug(
        self,
        offering_slug: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        """Fetch and process resources filtered by offering slug."""
        try:
            # Fetch resources with offering slug filter
            filters = {
                "client": client,
                "page": page,
                "page_size": page_size,
                "offering_slug": [offering_slug],  # Filter by offering slug
            }
            if state:
                filters["state"] = state

            response = marketplace_resources_list.sync_detailed(**filters)

            if not response.parsed:
                return [], {
                    "current": page,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                    "pages": 0,
                    "total": 0,
                }

            resources = response.parsed
            # Extract pagination info from response headers
            total_count = int(response.headers.get("X-Total-Count", len(resources)))

            # Get offering customers for hierarchical resources
            # Note: For slug-based lookup, we need to convert slug to UUID first
            offering_uuid_for_customers = None
            if resources:
                # Get UUID from the first resource's offering_uuid field
                first_resource = resources[0]
                if hasattr(first_resource, "offering_uuid") and not isinstance(
                    first_resource.offering_uuid, Unset
                ):
                    offering_uuid_for_customers = first_resource.offering_uuid.hex

            offering_customers = {}
            if offering_uuid_for_customers:
                offering_customers = self._get_offering_customers(
                    offering_uuid_for_customers, client
                )

            storage_resources = []
            tenant_entries = {}  # Track unique tenant entries
            customer_entries = {}  # Track unique customer entries by slug-system-data_type
            processed_count = 0

            logger.info("Processing resource %d/%d", processed_count + 1, len(resources))

            for resource in resources:
                processed_count += 1
                logger.info("Processing resource %d/%d", processed_count, len(resources))
                logger.info("Resource %s / %s", resource.uuid, resource.name)

                # Check transitional state and skip if order is not pending-provider on creation
                if (
                    hasattr(resource, "state")
                    and not isinstance(resource.state, Unset)
                    and resource.state in ["Creating"]
                ):
                    # For transitional resources, only process if order is in pending-provider state
                    if (
                        hasattr(resource, "order_in_progress")
                        and not isinstance(resource.order_in_progress, Unset)
                        and resource.order_in_progress is not None
                    ):
                        # Check order state
                        if (
                            hasattr(resource.order_in_progress, "state")
                            and not isinstance(resource.order_in_progress.state, Unset)
                            and resource.order_in_progress.state
                            in ["pending-consumer", "pending-project", "pending-start-date"]
                        ):
                            logger.info(
                                "Skipping resource %s in transitional state (%s) - "
                                "order state is %s, which is in early pending states",
                                resource.uuid,
                                resource.state,
                                resource.order_in_progress.state,
                            )
                            continue

                        # Display order URL for transitional resources with pending-provider order
                        if hasattr(resource.order_in_progress, "url") and not isinstance(
                            resource.order_in_progress.url, Unset
                        ):
                            logger.info(
                                "Resource in transitional state (%s) with pending-provider order - "
                                "Order URL: %s",
                                resource.state,
                                resource.order_in_progress.url,
                            )
                        else:
                            # Log that URL field is not available
                            logger.warning(
                                "Resource in transitional state (%s) with pending-provider order "
                                "but order URL not available",
                                resource.state,
                            )
                    else:
                        # No order in progress for transitional resource - skip it
                        logger.info(
                            "Skipping resource %s in transitional state (%s) - no order",
                            resource.uuid,
                            resource.state,
                        )
                        continue

                try:
                    storage_system_name = resource.offering_slug
                    logger.debug(
                        "  Using storage_system from offering_slug: %s", storage_system_name
                    )

                    # Get storage data type for the resource
                    storage_data_type = "store"  # default
                    if resource.attributes and not isinstance(resource.attributes, Unset):
                        storage_data_type = resource.attributes.additional_properties.get(
                            "storage_data_type", storage_data_type
                        )
                    # Get tenant information
                    tenant_id = resource.provider_slug
                    tenant_name = (
                        resource.provider_name
                        if hasattr(resource, "provider_name")
                        and not isinstance(resource.provider_name, Unset)
                        else tenant_id.upper()
                    )

                    # Create tenant-level entry if not already created for this combination
                    tenant_key = f"{tenant_id}-{storage_system_name}-{storage_data_type}"
                    if tenant_key not in tenant_entries:
                        # Get offering UUID from resource
                        offering_uuid_str = (
                            str(resource.offering_uuid)
                            if hasattr(resource, "offering_uuid")
                            and not isinstance(resource.offering_uuid, Unset)
                            else None
                        )

                        tenant_resource = self._create_tenant_storage_resource_json(
                            tenant_id=tenant_id,
                            tenant_name=tenant_name,
                            storage_system=storage_system_name,
                            storage_data_type=storage_data_type,
                            offering_uuid=offering_uuid_str,
                        )
                        storage_resources.append(tenant_resource)
                        tenant_entries[tenant_key] = tenant_resource["itemId"]
                        logger.debug(
                            "Created tenant entry for %s with offering UUID %s",
                            tenant_key,
                            offering_uuid_str,
                        )

                    # Create customer-level entry if not already created for this combination
                    customer_key = (
                        f"{resource.customer_slug}-{storage_system_name}-{storage_data_type}"
                    )
                    if (
                        customer_key not in customer_entries
                        and resource.customer_slug in offering_customers
                    ):
                        customer_info = offering_customers[resource.customer_slug]
                        parent_tenant_id = tenant_entries.get(tenant_key)

                        customer_resource = self._create_customer_storage_resource_json(
                            customer_info=customer_info,
                            storage_system=storage_system_name,
                            storage_data_type=storage_data_type,
                            tenant_id=tenant_id,
                            parent_tenant_id=parent_tenant_id,
                        )
                        storage_resources.append(customer_resource)
                        customer_entries[customer_key] = customer_resource["itemId"]
                        logger.debug(
                            "Created customer entry for %s with parent tenant %s",
                            customer_key,
                            parent_tenant_id,
                        )

                    # Create project-level resource (the original resource)
                    storage_resource = self._create_storage_resource_json(
                        resource, storage_system_name, client
                    )
                    if storage_resource is not None:
                        # Set parent reference if customer entry exists
                        if customer_key in customer_entries:
                            storage_resource["parentItemId"] = customer_entries[customer_key]
                            logger.debug(
                                "Set parentItemId %s for resource %s",
                                customer_entries[customer_key],
                                resource.uuid,
                            )

                        storage_resources.append(storage_resource)

                except Exception as e:
                    logger.error(
                        "Failed to process resource %s: %s", resource.uuid, e, exc_info=True
                    )

            # Apply additional filters (data_type, status) in memory
            filtered_resources = self._apply_filters(
                storage_resources, offering_slug, data_type, status
            )

            # Update pagination info based on filtered results
            filtered_count = len(filtered_resources)
            pages = (filtered_count + page_size - 1) // page_size

            pagination_info = {
                "current": page,
                "limit": page_size,
                "offset": (page - 1) * page_size,
                "pages": max(1, pages),
                "total": filtered_count,
                "raw_total_from_api": total_count,
            }

            return filtered_resources, pagination_info

        except Exception as e:
            logger.error("Failed to fetch storage resources by slug: %s", e, exc_info=True)
            raise

    def _serialize_resource(self, resource: object) -> dict:
        """Serialize a Waldur resource object for JSON output."""

        def serialize_value(value: object) -> object:
            """Convert various types to JSON-serializable format."""
            if hasattr(value, "__dict__"):
                return {k: serialize_value(v) for k, v in value.__dict__.items()}
            if isinstance(value, (list, tuple)):
                return [serialize_value(item) for item in value]
            if isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            return str(value) if value is not None else None

        result = serialize_value(resource)
        return result if isinstance(result, dict) else {"serialized": result}

    def _resource_matches_filters(
        self,
        resource: WaldurResource,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> bool:
        """Check if a resource matches the given filters."""
        # Check data_type filter
        if data_type:
            storage_data_type = getattr(resource, "storage_data_type", None)
            logger.debug(
                "Comparing raw resource storage_data_type '%s' with filter '%s'",
                storage_data_type,
                data_type,
            )
            if storage_data_type != data_type:
                return False

        # Check status filter
        if status:
            # Map resource state to status
            state_to_status_map = {
                ResourceState.CREATING: "pending",
                ResourceState.OK: "active",
                ResourceState.ERRED: "error",
                ResourceState.TERMINATING: "removing",
                ResourceState.TERMINATED: "removed",
            }
            resource_status = state_to_status_map.get(resource.state, "unknown")
            if resource_status != status:
                return False

        return True
