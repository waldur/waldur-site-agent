"""CSCS HPC Storage backend for Waldur Site Agent."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import NAMESPACE_OID, uuid5

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_retrieve,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo


class CscsHpcStorageBackend(backends.BaseBackend):
    """CSCS HPC Storage backend for JSON file generation."""

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize CSCS storage backend."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "cscs-hpc-storage"

        # Configuration with defaults
        self.output_directory = backend_settings.get("output_directory", "cscs-storage-orders/")
        self.storage_file_system = backend_settings.get("storage_file_system", "lustre")
        self.inode_soft_coefficient = backend_settings.get("inode_soft_coefficient", 1.33)
        self.inode_hard_coefficient = backend_settings.get("inode_hard_coefficient", 2.0)
        self.inode_base_multiplier = backend_settings.get("inode_base_multiplier", 1_000_000)
        self.use_mock_target_items = backend_settings.get("use_mock_target_items", False)

        # Validate configuration
        self._validate_configuration()

        # Ensure output directory exists
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

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
            storage_system: Required filter for storage system
            data_type: Optional filter for data type
            status: Optional filter for status

        Returns:
            Filtered list of storage resources
        """
        filtered_resources = []

        for resource in storage_resources:
            # Required storage_system filter
            if storage_system:
                resource_storage_system = resource.get("storageSystem", {}).get("key", "")
                if resource_storage_system != storage_system:
                    continue

            # Optional data_type filter
            if data_type:
                resource_data_type = resource.get("storageDataType", {}).get("key", "")
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

        if self.inode_hard_coefficient <= self.inode_soft_coefficient:
            msg = "inode_hard_coefficient must be greater than inode_soft_coefficient"
            raise ValueError(msg)

        if not isinstance(self.storage_file_system, str) or not self.storage_file_system.strip():
            msg = "storage_file_system must be a non-empty string"
            raise ValueError(msg)

        if not isinstance(self.output_directory, str) or not self.output_directory.strip():
            msg = "output_directory must be a non-empty string"
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

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if backend is accessible (always returns True for file-based backend)."""
        try:
            # Test if we can write to output directory
            test_file = Path(self.output_directory) / "test_write.tmp"
            test_file.touch()
            test_file.unlink()
            return True
        except Exception as e:
            if raise_exception:
                raise
            logger.error("Cannot write to output directory %s: %s", self.output_directory, e)
            return False

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """For CSCS storage backend the step is empty."""

    def diagnostics(self) -> bool:
        """Log backend diagnostics information."""
        logger.info("CSCS HPC Storage Backend Diagnostics")
        logger.info("=====================================")
        logger.info("Output directory: %s", self.output_directory)
        logger.info("Storage file system: %s", self.storage_file_system)
        logger.info("Inode soft coefficient: %s", self.inode_soft_coefficient)
        logger.info("Inode hard coefficient: %s", self.inode_hard_coefficient)
        logger.info("Inode base multiplier: %s", self.inode_base_multiplier)
        logger.info("Use mock target items: %s", self.use_mock_target_items)
        logger.info("Backend components: %s", list(self.backend_components.keys()))

        # Test directory accessibility
        can_write = self.ping()
        logger.info("Output directory writable: %s", can_write)

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

    def _calculate_inode_quotas(self, storage_quota_tb: float) -> tuple[int, int]:
        """Calculate inode quotas based on storage size and coefficients."""
        # Base calculation: storage in TB * configurable base multiplier
        base_inodes = storage_quota_tb * self.inode_base_multiplier
        soft_limit = int(base_inodes * self.inode_soft_coefficient)
        hard_limit = int(base_inodes * self.inode_hard_coefficient)
        return soft_limit, hard_limit

    def _get_target_item_data(self, waldur_resource: WaldurResource, target_type: str) -> dict:
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
            return {
                "itemId": self._generate_deterministic_uuid(f"project:{waldur_resource.slug}"),
                "status": "open",
                "name": waldur_resource.slug,
                "unixGid": 30000 + hash(waldur_resource.slug) % 10000,  # Mock GID
                "active": True,
            }
        if target_type == "user":
            return {
                "itemId": self._generate_deterministic_uuid(f"user:{waldur_resource.slug}"),
                "status": "active",
                "email": f"user-{waldur_resource.slug}@example.com",  # Mock email
                "unixUid": 20000 + hash(waldur_resource.slug) % 10000,  # Mock UID
                "primaryProject": {
                    "name": (
                        waldur_resource.project_slug
                        if not isinstance(waldur_resource.project_slug, Unset)
                        else "default-project"
                    ),
                    "unixGid": (
                        30000 + hash(str(waldur_resource.project_slug)) % 10000
                    ),  # Mock project GID
                    "active": True,
                },
                "active": True,
            }

        return {}

    def _get_target_data(self, waldur_resource: WaldurResource, storage_data_type: str) -> dict:
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

        return {
            "targetType": target_type,
            "targetItem": self._get_target_item_data(waldur_resource, target_type),
        }

    def _create_storage_resource_json(
        self, waldur_resource: WaldurResource, storage_system: str
    ) -> dict:
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
            tenant_id=waldur_resource.customer_slug,
            customer=waldur_resource.project_slug,
            project_id=waldur_resource.slug,
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

        # Create JSON structure
        return {
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
            "target": self._get_target_data(waldur_resource, storage_data_type.lower()),
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
            "parentItemId": None,  # Could be set based on hierarchy if needed
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
            storage_system: Required filter for storage system (e.g., 'capstor', 'vast', 'iopsstor')
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
                offering_uuid=offering_uuid,
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

            # Convert Waldur resources to storage JSON format
            storage_resources = []
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

                storage_resource = self._create_storage_resource_json(resource, storage_system_name)
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

    def _write_json_file(self, filename: str, data: dict) -> None:
        """Write JSON data to file."""
        filepath = Path(self.output_directory) / filename

        try:
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info("Generated JSON file: %s", filepath)
        except Exception as e:
            logger.error("Failed to write JSON file %s: %s", filepath, e)

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
                offering_uuid=offering_uuid,
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
        write_file: bool = True,
        page: int = 1,
        page_size: int = 100,
        storage_system: Optional[str] = None,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        """Generate JSON file with all storage resources with pagination support."""
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        filename = f"{timestamp}-all.json"

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

            json_data = {
                "status": "success",
                "code": 200,
                "meta": {"date": datetime.now().isoformat(), "appVersion": "1.4.0"},
                "result": {
                    "storageResources": storage_resources,
                    "paginate": pagination_info,
                },
            }

            if write_file:
                self._write_json_file(filename, json_data)

            return json_data

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

    def generate_order_json(self, waldur_resource: WaldurResource, order_type: str) -> None:
        """Generate JSON file for a specific order."""
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        filename = f"{timestamp}-{order_type}_{waldur_resource.uuid.hex}.json"

        # Validate resource data before processing
        self._validate_resource_data(waldur_resource)

        # Use offering_slug as the storage system name
        storage_system_name = waldur_resource.offering_slug

        storage_resource = self._create_storage_resource_json(waldur_resource, storage_system_name)

        json_data = {
            "status": "success",
            "code": 200,
            "meta": {"date": datetime.now().isoformat(), "appVersion": "1.4.0"},
            "result": {"storageResources": [storage_resource]},
        }

        self._write_json_file(filename, json_data)

    def create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create storage resource and generate JSON files."""
        del user_context
        logger.info("Creating CSCS storage resource: %s", waldur_resource.name)

        # Generate JSON for specific order only
        # Note: Bulk all.json generation is handled by separate sync script
        self.generate_order_json(waldur_resource, "create")

        # Return resource structure
        return BackendResourceInfo(
            backend_id=waldur_resource.slug,  # Use slug as backend ID
            limits=self._collect_resource_limits(waldur_resource)[1],
        )
