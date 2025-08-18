"""CSCS HPC Storage backend for Waldur Site Agent."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.pagination import get_all_paginated


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
        self, storage_system: str, tenant_id: str, customer: str, project_id: str
    ) -> str:
        """Generate mount point path based on hierarchy."""
        return f"/{storage_system}/store/{tenant_id}/{customer}/{project_id}"

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
                "itemId": waldur_resource.customer_uuid.hex,
                "key": waldur_resource.customer_slug,
                "name": waldur_resource.customer_name,
            }
        if target_type == "customer":
            return {
                "itemId": waldur_resource.project_uuid.hex,
                "key": waldur_resource.project_slug,
                "name": waldur_resource.project_name,
            }
        if target_type == "project":
            return {
                "itemId": waldur_resource.uuid.hex,
                "status": "open",
                "name": waldur_resource.slug,
                "unixGid": 30000 + hash(waldur_resource.slug) % 10000,  # Mock GID
                "active": True,
            }

        return {}

    def _create_storage_resource_json(
        self, waldur_resource: WaldurResource, storage_system: str
    ) -> dict:
        """Create JSON structure for a single storage resource."""
        # Extract storage size from resource limits (assuming in terabytes)
        storage_quota_tb = 0.0
        if waldur_resource.limits:
            # Get storage component limit (typically the first component configured)
            for component_name, limit_value in waldur_resource.limits.additional_properties.items():
                if component_name in self.backend_components:
                    storage_quota_tb = float(limit_value)  # Assume already in TB
                    break

        inode_soft, inode_hard = self._calculate_inode_quotas(storage_quota_tb)

        # Generate mount point
        mount_point = self._generate_mount_point(
            storage_system=storage_system,
            tenant_id=waldur_resource.customer_slug,
            customer=waldur_resource.project_slug,
            project_id=waldur_resource.slug,
        )

        # Get permissions and data type from resource attributes
        permissions = "775"  # default
        storage_data_type = "store"  # default

        if waldur_resource.attributes:
            permissions = waldur_resource.attributes.additional_properties.get(
                "permissions", permissions
            )
            storage_data_type = waldur_resource.attributes.additional_properties.get(
                "storage_data_type", storage_data_type
            )

        # Create JSON structure
        return {
            "itemId": waldur_resource.uuid.hex,
            "status": "pending",
            "mountPoint": {"default": mount_point},
            "permission": {"permissionType": "octal", "value": permissions},
            "quotas": [
                {
                    "type": "space",
                    "quota": int(storage_quota_tb),
                    "unit": "tera",
                    "enforcementType": "soft",
                },
                {
                    "type": "space",
                    "quota": int(storage_quota_tb),
                    "unit": "tera",
                    "enforcementType": "hard",
                },
                {"type": "inodes", "quota": inode_soft, "unit": "none", "enforcementType": "soft"},
                {"type": "inodes", "quota": inode_hard, "unit": "none", "enforcementType": "hard"},
            ]
            if storage_quota_tb > 0
            else None,
            "target": {
                "targetType": "project",
                "targetItem": self._get_target_item_data(waldur_resource, "project"),
            },
            "storageSystem": {
                "itemId": str(uuid4()),  # Generate for storage system
                "key": storage_system,
                "name": storage_system.upper(),
                "active": True,
            },
            "storageFileSystem": {
                "itemId": str(uuid4()),  # Generate for file system
                "key": self.storage_file_system,
                "name": self.storage_file_system.upper(),
                "active": True,
            },
            "storageDataType": {
                "itemId": str(uuid4()),  # Generate for data type
                "key": storage_data_type,
                "name": storage_data_type.upper(),
                "path": storage_data_type,
                "active": True,
            },
            "parentItemId": None,  # Could be set based on hierarchy if needed
        }

    def _get_all_storage_resources(
        self, offering_uuid: str, client: AuthenticatedClient, state: Optional[ResourceState] = None
    ) -> list[dict]:
        """Fetch all storage resources from Waldur API with proper pagination.

        Args:
            offering_uuid: UUID of the offering to fetch resources for
            client: Authenticated Waldur API client for API access
            state: Optional resource state

        Returns:
            List of storage resource dictionaries in JSON format
        """
        try:
            # Fetch all resources using the reusable pagination utility
            filters = {}
            if state:
                filters["state"] = state
            waldur_resources = get_all_paginated(
                marketplace_resources_list.sync,
                client,
                offering_uuid=offering_uuid,
                **filters,
            )

            # Convert Waldur resources to storage JSON format
            storage_resources = []
            for resource in waldur_resources:
                storage_system = resource.offering_slug
                storage_resource = self._create_storage_resource_json(resource, storage_system)
                storage_resources.append(storage_resource)

            logger.info(
                "Retrieved %d storage resources for offering %s",
                len(storage_resources),
                offering_uuid,
            )
            return storage_resources

        except Exception as e:
            logger.error("Failed to fetch storage resources from Waldur API: %s", e)
            return []

    def _write_json_file(self, filename: str, data: dict) -> None:
        """Write JSON data to file."""
        filepath = Path(self.output_directory) / filename

        try:
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info("Generated JSON file: %s", filepath)
        except Exception as e:
            logger.error("Failed to write JSON file %s: %s", filepath, e)

    def generate_all_resources_json(
        self,
        offering_uuid: str,
        client: AuthenticatedClient,
        state: Optional[ResourceState] = None,
        write_file: bool = True,
    ) -> dict:
        """Generate JSON file with all storage resources."""
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        filename = f"{timestamp}-all.json"

        storage_resources = self._get_all_storage_resources(offering_uuid, client, state)

        json_data = {
            "status": "success",
            "code": 200,
            "meta": {"date": datetime.now().isoformat(), "appVersion": "1.4.0"},
            "result": {"storageResources": storage_resources},
        }

        if write_file:
            self._write_json_file(filename, json_data)

        return json_data

    def generate_order_json(self, waldur_resource: WaldurResource, order_type: str) -> None:
        """Generate JSON file for a specific order."""
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        filename = f"{timestamp}-{order_type}_{waldur_resource.uuid.hex}.json"

        storage_system = waldur_resource.offering_slug
        storage_resource = self._create_storage_resource_json(waldur_resource, storage_system)

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
