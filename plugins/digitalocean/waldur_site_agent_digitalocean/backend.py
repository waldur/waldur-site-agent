"""DigitalOcean backend for Waldur Site Agent."""

from __future__ import annotations

import logging
import re
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import BackendType, backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .client import DigitalOceanBackendError, DigitalOceanClient

logger = logging.getLogger(__name__)


class DigitalOceanBackend(backends.BaseBackend):
    """DigitalOcean backend implementation for Waldur Site Agent."""

    def __init__(
        self, backend_settings: dict[str, object], backend_components: dict[str, dict]
    ) -> None:
        """Initialize DigitalOcean backend with settings and components."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = BackendType.DIGITALOCEAN.value

        token_value = backend_settings.get("token")
        token = self._coerce_str(token_value)
        if not token:
            msg = "DigitalOcean backend requires 'token' in backend_settings"
            raise BackendError(msg)

        self.client: DigitalOceanClient = DigitalOceanClient(token=token)

        # Defaults for droplet creation
        self.default_region = backend_settings.get("default_region")
        self.default_image = backend_settings.get("default_image")
        self.default_size = backend_settings.get("default_size")
        self.default_user_data = backend_settings.get("default_user_data")
        self.default_tags = backend_settings.get("default_tags", [])

        # Default SSH key configuration
        self.default_ssh_key_id = backend_settings.get("default_ssh_key_id")
        self.default_ssh_key_fingerprint = backend_settings.get("default_ssh_key_fingerprint")
        self.default_ssh_key_name = backend_settings.get("default_ssh_key_name")
        self.default_ssh_public_key = backend_settings.get("default_ssh_public_key")

        # Optional mapping for resize based on limits
        self.size_mapping = backend_settings.get("size_mapping", {})

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if DigitalOcean API is reachable."""
        if self.client.ping():
            return True
        if raise_exception:
            msg = "DigitalOcean backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about backend settings."""
        logger.info("=== DigitalOcean backend diagnostics ===")
        logger.info("Components: %s", list(self.backend_components.keys()))
        logger.info("Default region: %s", self.default_region)
        logger.info("Default image: %s", self.default_image)
        logger.info("Default size: %s", self.default_size)
        return self.ping(raise_exception=False)

    def list_components(self) -> list[str]:
        """Return list of component names."""
        return list(self.backend_components.keys())

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        del resource_backend_ids
        return {}

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        del waldur_resource, user_context

    def _sanitize_name(self, name: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9.-]", "-", name).strip("-")
        if not sanitized:
            sanitized = "waldur-droplet"
        return sanitized.lower()

    def _get_resource_attributes(self, resource: WaldurResource) -> dict[str, object]:
        attributes = getattr(resource, "attributes", None) or {}
        options = getattr(resource, "options", None) or {}
        if isinstance(options, dict):
            attributes = {**options, **attributes}
        return attributes

    def _resolve_attribute(self, attrs: dict[str, object], *keys: str) -> Optional[object]:
        for key in keys:
            value = attrs.get(key)
            if value not in (None, ""):
                return value
        return None

    def _coerce_int(self, value: object) -> Optional[int]:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return int(value)
            except ValueError:
                return None
        return None

    def _coerce_str(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return str(value)

    def _select_size_slug_from_limits(self, limits: dict[str, int]) -> Optional[str]:
        if not isinstance(self.size_mapping, dict):
            return None
        for size_slug, size_limits in self.size_mapping.items():
            if not isinstance(size_limits, dict):
                continue
            if all(limits.get(key) == value for key, value in size_limits.items()):
                return size_slug
        return None

    def _resolve_size_slug(
        self, attrs: dict[str, object], waldur_resource: WaldurResource
    ) -> Optional[str]:
        size_slug = self._resolve_attribute(attrs, "backend_size_id", "size", "size_slug")
        if size_slug:
            return str(size_slug)
        if self.default_size:
            return str(self.default_size)
        if waldur_resource.limits:
            limits = waldur_resource.limits.to_dict()
            return self._select_size_slug_from_limits(limits)
        return None

    def _resolve_ssh_key_ids(self, attrs: dict[str, object]) -> list[int]:
        ssh_key_id_value = self._resolve_attribute(attrs, "ssh_key_id")
        resolved_ssh_key_id = self._coerce_int(ssh_key_id_value)
        if resolved_ssh_key_id is None:
            resolved_ssh_key_id = self._coerce_int(self.default_ssh_key_id)

        if ssh_key_id_value is not None and resolved_ssh_key_id is None:
            logger.warning("Invalid ssh_key_id value %s, ignoring", ssh_key_id_value)

        ssh_key_fingerprint = self._coerce_str(
            self._resolve_attribute(attrs, "ssh_key_fingerprint")
            or self.default_ssh_key_fingerprint
        )
        ssh_key_name = self._coerce_str(
            self._resolve_attribute(attrs, "ssh_key_name") or self.default_ssh_key_name
        )
        ssh_public_key = self._coerce_str(
            self._resolve_attribute(attrs, "ssh_public_key") or self.default_ssh_public_key
        )

        ssh_key = self.client.resolve_ssh_key(
            ssh_key_id=resolved_ssh_key_id,
            ssh_key_fingerprint=ssh_key_fingerprint,
            ssh_key_public_key=ssh_public_key,
            ssh_key_name=ssh_key_name,
        )

        return [ssh_key.id] if ssh_key is not None else []

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Provision a droplet for the given Waldur resource."""
        del user_context
        attrs = self._get_resource_attributes(waldur_resource)

        region = (
            self._resolve_attribute(attrs, "backend_region_id", "region")
            or self.default_region
        )
        image = self._resolve_attribute(attrs, "backend_image_id", "image") or self.default_image
        size_slug = self._resolve_size_slug(attrs, waldur_resource)

        if not region:
            msg = "DigitalOcean droplet region is not specified"
            raise BackendError(msg)
        if not image:
            msg = "DigitalOcean droplet image is not specified"
            raise BackendError(msg)
        if not size_slug:
            msg = "DigitalOcean droplet size is not specified"
            raise BackendError(msg)

        name_source = (
            waldur_resource.name
            or getattr(waldur_resource, "slug", "")
            or f"waldur-{waldur_resource.uuid.hex[:8]}"
        )
        droplet_name = self._sanitize_name(name_source)

        user_data = (
            self._resolve_attribute(attrs, "user_data", "cloud_init") or self.default_user_data
        )
        tags = self._resolve_attribute(attrs, "tags")
        if not tags:
            tags = self.default_tags
        if tags is None:
            tags = []
        if not isinstance(tags, list):
            tags = [str(tags)]

        ssh_key_ids = self._resolve_ssh_key_ids(attrs)

        logger.info(
            "Creating DigitalOcean droplet %s (region=%s, image=%s, size=%s)",
            droplet_name,
            region,
            image,
            size_slug,
        )
        droplet = self.client.create_droplet(
            name=droplet_name,
            region=str(region),
            image=image,
            size_slug=str(size_slug),
            user_data=user_data,
            ssh_key_ids=ssh_key_ids,
            tags=tags,
        )

        backend_id = str(droplet.id)
        logger.info("DigitalOcean droplet created with ID %s", backend_id)

        waldur_limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        return BackendResourceInfo(backend_id=backend_id, limits=waldur_limits)

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:
        """Delete droplet corresponding to the Waldur resource."""
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id:
            logger.warning("No backend ID found for resource %s", waldur_resource.uuid)
            return
        try:
            self.client.delete_resource(resource_backend_id)
            logger.info("Deleted DigitalOcean droplet %s", resource_backend_id)
        except DigitalOceanBackendError as exc:
            logger.warning(
                "Failed to delete DigitalOcean droplet %s: %s", resource_backend_id, exc
            )

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource by shutting down the droplet."""
        try:
            self.client.shutdown_droplet(resource_backend_id)
            return True
        except DigitalOceanBackendError as exc:
            logger.warning("Unable to downscale droplet %s: %s", resource_backend_id, exc)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause resource by shutting down the droplet."""
        try:
            self.client.shutdown_droplet(resource_backend_id)
            return True
        except DigitalOceanBackendError as exc:
            logger.warning("Unable to pause droplet %s: %s", resource_backend_id, exc)
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource by powering on the droplet."""
        try:
            self.client.power_on_droplet(resource_backend_id)
            return True
        except DigitalOceanBackendError as exc:
            logger.warning("Unable to restore droplet %s: %s", resource_backend_id, exc)
            return False

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        if not waldur_resource.limits:
            return {}, {}
        limits = waldur_resource.limits.to_dict()
        return {}, limits

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Resize droplet if limits match size mapping."""
        size_slug = self._select_size_slug_from_limits(limits)
        if not size_slug:
            logger.info(
                "No size mapping found for limits %s, skipping resize for %s",
                limits,
                resource_backend_id,
            )
            return
        logger.info(
            "Resizing droplet %s to size %s based on limits %s",
            resource_backend_id,
            size_slug,
            limits,
        )
        self.client.resize_droplet(resource_backend_id, size_slug=size_slug, disk=False)

    def _gb_to_mib(self, value_gb: float) -> int:
        return int(value_gb * 1024)

    def _tb_to_mib(self, value_tb: float) -> int:
        return int(value_tb * 1024 * 1024)

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Return droplet limits in Waldur units."""
        droplet = self.client.get_droplet(resource_backend_id)
        if droplet is None:
            return {}

        limits: dict[str, int] = {}
        if "cpu" in self.backend_components and getattr(droplet, "vcpus", None) is not None:
            limits["cpu"] = int(droplet.vcpus)
        if "ram" in self.backend_components and getattr(droplet, "memory", None) is not None:
            limits["ram"] = int(droplet.memory)
        if "disk" in self.backend_components and getattr(droplet, "disk", None) is not None:
            limits["disk"] = self._gb_to_mib(float(droplet.disk))

        size_info = getattr(droplet, "size", None)
        transfer_tb = None
        if isinstance(size_info, dict):
            transfer_tb = size_info.get("transfer")
        if getattr(droplet, "transfer", None) is not None:
            transfer_tb = droplet.transfer
        if (
            "transfer" in self.backend_components
            and transfer_tb is not None
            and isinstance(transfer_tb, (int, float))
        ):
            limits["transfer"] = self._tb_to_mib(float(transfer_tb))

        return limits

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return droplet metadata for Waldur backend_metadata."""
        droplet = self.client.get_droplet(resource_backend_id)
        if droplet is None:
            return {"backend_type": self.backend_type, "error": "Droplet not found"}

        networks = getattr(droplet, "networks", {}) or {}
        v4_networks = networks.get("v4", [])
        v6_networks = networks.get("v6", [])

        public_ipv4 = next(
            (net.get("ip_address") for net in v4_networks if net.get("type") == "public"),
            None,
        )
        private_ipv4 = next(
            (net.get("ip_address") for net in v4_networks if net.get("type") == "private"),
            None,
        )
        public_ipv6 = next(
            (net.get("ip_address") for net in v6_networks if net.get("type") == "public"),
            None,
        )

        return {
            "backend_type": self.backend_type,
            "droplet": {
                "id": str(droplet.id),
                "name": droplet.name,
                "status": getattr(droplet, "status", None),
                "region": getattr(droplet, "region", None),
                "size_slug": getattr(droplet, "size_slug", None),
                "image": getattr(droplet, "image", None),
                "created_at": getattr(droplet, "created_at", None),
                "networks": {
                    "public_ipv4": public_ipv4,
                    "private_ipv4": private_ipv4,
                    "public_ipv6": public_ipv6,
                },
            },
        }
