"""OpenNebula backend for waldur site agent.

This module provides the backend implementation for managing OpenNebula
resources. It supports two resource types:

- **vdc** (default): Virtual Data Centers with groups, quotas, and networking.
- **vm**: Virtual Machines instantiated within a VDC.

The ``resource_type`` is read from ``backend_settings`` and determines
which code paths are taken during create, delete, and usage reporting.
"""

import logging
import secrets
from typing import Any, Optional
from uuid import UUID

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .client import OpenNebulaClient

logger = logging.getLogger(__name__)


class OpenNebulaBackend(BaseBackend):
    """Backend for managing OpenNebula VDCs and VMs.

    Each Waldur resource maps to either an OpenNebula VDC + group (resource_type=vdc)
    or a VM (resource_type=vm). Resource limits are enforced via group quotas for VDCs
    and via template parameters for VMs.

    When ``resource_type`` is ``"vdc"``, networking (VXLAN VNet, Virtual Router,
    Security Groups) is auto-provisioned when ``offering_plugin_options`` includes
    networking configuration keys.
    """

    # Quota usage reflects current state (VMs currently running),
    # not accumulated historical usage.
    supports_decreasing_usage: bool = True
    client: OpenNebulaClient

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Initialize OpenNebula backend.

        Required backend_settings keys:
            api_url: OpenNebula XML-RPC endpoint.
            credentials: "username:password" string.

        Optional backend_settings keys:
            zone_id: OpenNebula zone ID (default 0).
            cluster_ids: List of cluster IDs.
            resource_type: "vdc" (default) or "vm".

        backend_components may be empty — they will be populated by
        extend_backend_components() from Waldur offering before any
        resource processing.
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "opennebula"
        self.resource_type = backend_settings.get("resource_type", "vdc")

        required_keys = ["api_url", "credentials"]
        for key in required_keys:
            if key not in backend_settings:
                msg = f"Missing required backend setting: '{key}'"
                raise ValueError(msg)

        self.client = OpenNebulaClient(
            api_url=backend_settings["api_url"],
            credentials=backend_settings["credentials"],
            zone_id=backend_settings.get("zone_id", 0),
            cluster_ids=backend_settings.get("cluster_ids"),
        )

        # Stores network config between _pre_create_resource and _create_backend_resource
        self._pending_network_config: Optional[dict[str, Any]] = None
        # Stores network metadata after creation for get_resource_metadata
        self._resource_network_metadata: dict[str, dict[str, Any]] = {}
        # Stores VM creation context between _pre_create_resource and _create_backend_resource
        self._pending_vm_config: Optional[dict[str, Any]] = None

    def ping(self, raise_exception: bool = False) -> bool:
        """Check connectivity by listing VDCs."""
        try:
            self.client.list_resources()
            return True
        except BackendError:
            if raise_exception:
                raise
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information and check connectivity."""
        logger.info(
            "OpenNebula backend: api_url=%s, zone_id=%s",
            self.backend_settings["api_url"],
            self.backend_settings.get("zone_id", 0),
        )
        return self.ping()

    def list_components(self) -> list[str]:
        """Return configured component types."""
        return list(self.backend_components.keys())

    @staticmethod
    def _build_network_config(
        waldur_resource: WaldurResource,
        backend_settings: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Build network configuration from Waldur resource.

        Reads infrastructure config from offering_plugin_options (Waldur side)
        with fallback to backend_settings (agent YAML). Returns None if
        networking is not configured.

        Args:
            waldur_resource: Waldur resource with offering_plugin_options and attributes.
            backend_settings: Agent backend_settings dict (fallback source).

        Returns:
            Network configuration dict or None.
        """
        raw_plugin_options = waldur_resource.offering_plugin_options
        plugin_options: dict[str, Any] = (
            raw_plugin_options.to_dict()
            if hasattr(raw_plugin_options, "to_dict")
            else raw_plugin_options or {}
        )

        # Fall back to backend_settings for network config keys
        if backend_settings:
            for key in (
                "external_network_id",
                "virtual_router_template_id",
                "vn_mad",
                "vxlan_phydev",
                "default_dns",
                "internal_network_base",
                "internal_network_prefix",
                "subnet_prefix_length",
                "security_group_defaults",
                "sched_requirements",
                "cluster_ids",
                "zone_id",
            ):
                if key not in plugin_options and key in backend_settings:
                    plugin_options[key] = backend_settings[key]

        # Networking requires at minimum an external network and VR template
        if "external_network_id" not in plugin_options:
            return None
        if "virtual_router_template_id" not in plugin_options:
            return None

        raw_attributes = waldur_resource.attributes
        attributes: dict[str, Any] = (
            raw_attributes.to_dict()
            if hasattr(raw_attributes, "to_dict")
            else raw_attributes or {}
        )

        config: dict[str, Any] = {
            "zone_id": plugin_options.get("zone_id", 0),
            "cluster_ids": plugin_options.get("cluster_ids", []),
            "external_network_id": int(plugin_options["external_network_id"]),
            "vn_mad": plugin_options.get("vn_mad", "vxlan"),
            "vxlan_phydev": plugin_options.get("vxlan_phydev", "eth0"),
            "virtual_router_template_id": int(
                plugin_options["virtual_router_template_id"]
            ),
            "default_dns": plugin_options.get("default_dns", "8.8.8.8"),
            "internal_network_base": plugin_options.get(
                "internal_network_base", "10.0.0.0"
            ),
            "internal_network_prefix": int(
                plugin_options.get("internal_network_prefix", 8)
            ),
            "subnet_prefix_length": int(plugin_options.get("subnet_prefix_length", 24)),
            "security_group_defaults": plugin_options.get(
                "security_group_defaults", []
            ),
            "sched_requirements": plugin_options.get("sched_requirements", ""),
        }

        # User inputs (optional)
        if attributes.get("subnet_cidr"):
            config["subnet_cidr"] = attributes["subnet_cidr"]

        return config

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Prepare creation context based on resource_type.

        For VDC: builds network config from offering_plugin_options.
        For VM: extracts template_id, parent VDC, and SSH key from attributes.
            Uses pre-resolved plan_quotas and ssh_keys from user_context
            (injected by the processor).
        """
        if self.resource_type == "vm":
            self._pending_vm_config = self._build_vm_config(
                waldur_resource, user_context
            )
            logger.info(
                "VM creation configured: template=%s, parent_vdc=%s",
                self._pending_vm_config.get("template_id"),
                self._pending_vm_config.get("parent_backend_id"),
            )
        else:
            self._pending_network_config = self._build_network_config(
                waldur_resource, self.backend_settings
            )
            if self._pending_network_config:
                logger.info(
                    "Networking configured for resource: subnet_prefix=/%d, external_net=%d",
                    self._pending_network_config["subnet_prefix_length"],
                    self._pending_network_config["external_network_id"],
                )

    @staticmethod
    def _resolve_ssh_key_from_context(
        ssh_value: str, ssh_keys: dict[str, str]
    ) -> str:
        """Resolve an SSH key value that may be a UUID reference.

        Looks up the UUID in the pre-resolved ssh_keys dict (provided by
        the processor via user_context). Falls back to returning the raw
        value if it is not a UUID.

        Args:
            ssh_value: Raw SSH key text or a Waldur SSH key UUID.
            ssh_keys: Mapping of key UUID (hex) to public key text,
                pre-fetched by the processor.

        Returns:
            The resolved SSH public key text.
        """
        if not ssh_value or not ssh_value.strip():
            return ssh_value

        try:
            key_uuid = UUID(ssh_value.strip())
        except ValueError:
            return ssh_value

        resolved = ssh_keys.get(str(key_uuid), "")
        if resolved:
            logger.info("Resolved SSH key UUID '%s' to public key", key_uuid)
            return resolved

        # Try matching without dashes (hex format)
        resolved = ssh_keys.get(key_uuid.hex, "")
        if resolved:
            logger.info("Resolved SSH key UUID '%s' to public key", key_uuid)
            return resolved

        logger.warning("SSH key UUID '%s' not found in pre-resolved keys", key_uuid)
        return ""

    def _build_vm_config(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Extract VM creation parameters from Waldur resource attributes.

        Uses pre-resolved plan_quotas and ssh_keys from user_context
        (injected by the processor) instead of calling the Waldur API directly.

        Args:
            waldur_resource: Waldur resource with attributes containing
                template_id, parent_backend_id, and optionally ssh_public_key.
            user_context: Dict with pre-resolved data including
                plan_quotas and ssh_keys.

        Returns:
            Configuration dict for VM creation.
        """
        user_context = user_context or {}
        raw_attrs = waldur_resource.attributes
        attributes: dict[str, Any] = (
            raw_attrs.to_dict() if hasattr(raw_attrs, "to_dict") else raw_attrs or {}
        )
        # VM specs come from the plan's fixed-component quantities (quotas),
        # pre-resolved by the processor.
        plan_quotas = user_context.get("plan_quotas", {})

        raw_plugin_options = waldur_resource.offering_plugin_options
        plugin_options: dict[str, Any] = (
            raw_plugin_options.to_dict()
            if hasattr(raw_plugin_options, "to_dict")
            else raw_plugin_options or {}
        )

        # Fall back to backend_settings for VM config keys
        bs = self.backend_settings or {}
        for key in (
            "parent_vdc_backend_id",
            "template_id",
            "cluster_ids",
            "sched_requirements",
        ):
            if key not in plugin_options and key in bs:
                plugin_options[key] = bs[key]

        ssh_value = attributes.get("ssh_public_key", "")
        ssh_keys = user_context.get("ssh_keys", {})
        resolved_ssh_key = self._resolve_ssh_key_from_context(ssh_value, ssh_keys)

        # parent_backend_id: resource attributes > plugin_options > backend_settings
        parent_backend_id = attributes.get(
            "parent_backend_id",
            plugin_options.get("parent_vdc_backend_id", ""),
        )

        # template_id: resource attributes > plugin_options > backend_settings
        template_id_raw = attributes.get(
            "template_id",
            plugin_options.get(
                "template_id", self.backend_settings.get("template_id")
            ),
        )
        template_id = int(template_id_raw) if template_id_raw is not None else None

        if not plan_quotas:
            raise BackendError(
                "VM creation requires a plan with fixed-component quantities "
                "(vcpu, vm_ram, vm_disk). No plan quotas found on resource."
            )

        config: dict[str, Any] = {
            "template_id": template_id,
            "parent_backend_id": parent_backend_id,
            "ssh_public_key": resolved_ssh_key,
            "vcpu": int(plan_quotas.get("vcpu", 1)),
            "vm_ram": int(plan_quotas.get("vm_ram", 512)),
            "vm_disk": int(plan_quotas.get("vm_disk", 10240)),
            "cluster_ids": plugin_options.get("cluster_ids", []),
            "sched_requirements": plugin_options.get("sched_requirements", ""),
        }

        if not config["template_id"]:
            raise BackendError(
                "VM creation requires 'template_id' in resource attributes"
            )
        if not config["parent_backend_id"]:
            raise BackendError(
                "VM creation requires 'parent_backend_id' in resource attributes "
                "or 'parent_vdc_backend_id' in offering plugin_options"
            )

        return config

    def _create_backend_resource(
        self,
        resource_backend_id: str,
        resource_name: str,
        resource_organization: str,
        resource_parent_name: Optional[str] = None,
    ) -> bool:
        """Create a VDC resource.

        VM creation is handled by the overridden ``create_resource_with_id``.
        """
        logger.info(
            "Creating %s resource %s in %s backend (backend ID = %s)",
            self.resource_type,
            resource_name,
            self.backend_type,
            resource_backend_id,
        )

        return self._create_vdc_resource(
            resource_backend_id,
            resource_name,
            resource_organization,
            resource_parent_name,
        )

    def _create_vdc_resource(
        self,
        resource_backend_id: str,
        resource_name: str,
        resource_organization: str,
        resource_parent_name: Optional[str] = None,
    ) -> bool:
        """Create a VDC with optional networking infrastructure."""
        if self.client.get_resource(resource_backend_id) is None:
            self.client.create_resource(
                name=resource_backend_id,
                description=resource_name,
                organization=resource_organization,
                parent_name=resource_parent_name,
                network_config=self._pending_network_config,
            )
            # Store network metadata if available
            if (
                hasattr(self.client, "_network_metadata")
                and self.client._network_metadata
            ):
                self._resource_network_metadata[resource_backend_id] = (
                    self.client._network_metadata.copy()
                )
            return True
        logger.info(
            "The resource with ID %s already exists in the cluster", resource_backend_id
        )
        return False

    def _create_vm_resource(self, resource_backend_id: str) -> int:
        """Instantiate a VM from template in the parent VDC.

        Returns the numeric OpenNebula VM ID.
        """
        if self._pending_vm_config is None:
            raise BackendError(
                "VM config not set — _pre_create_resource must be called first"
            )

        vm_id = self.client.create_vm(
            template_id=self._pending_vm_config["template_id"],
            vm_name=resource_backend_id,
            parent_vdc_name=self._pending_vm_config["parent_backend_id"],
            ssh_key=self._pending_vm_config.get("ssh_public_key", ""),
            vcpu=self._pending_vm_config.get("vcpu", 1),
            ram_mb=self._pending_vm_config.get("vm_ram", 512),
            disk_mb=self._pending_vm_config.get("vm_disk", 10240),
            cluster_ids=self._pending_vm_config.get("cluster_ids"),
            sched_requirements=self._pending_vm_config.get("sched_requirements", ""),
        )

        # Store VM metadata keyed by numeric ID
        vm_backend_id = str(vm_id)
        ip_address = self.client.get_vm_ip_address(vm_id)
        self._resource_network_metadata[vm_backend_id] = {
            "vm_id": vm_id,
            "ip_address": ip_address or "",
        }
        return vm_id

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Override for VM mode: use the numeric VM ID as backend_id."""
        if self.resource_type != "vm":
            return super().create_resource_with_id(
                waldur_resource, resource_backend_id, user_context
            )

        logger.info("Creating VM resource (name hint: %s)", resource_backend_id)
        self._pre_create_resource(waldur_resource, user_context)

        vm_id = self._create_vm_resource(resource_backend_id)
        vm_backend_id = str(vm_id)

        backend_resource_info = BackendResourceInfo(
            backend_id=vm_backend_id,
            limits={},
        )

        self.post_create_resource(
            backend_resource_info, waldur_resource, user_context
        )
        return backend_resource_info

    @staticmethod
    def _generate_opennebula_username(vdc_name: str) -> str:
        """Derive a deterministic OpenNebula username from a VDC name."""
        return f"{vdc_name}_admin"

    def post_create_resource(
        self,
        resource: BackendResourceInfo,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Post-creation actions.

        For VDC with ``create_opennebula_user`` enabled: creates an OpenNebula
        user in the VDC's group and sets backend_metadata on the resource info
        so the processor can push it to Waldur.

        For VM: no additional actions needed.
        """
        del user_context, waldur_resource

        if self.resource_type != "vdc":
            return

        if not self.backend_settings.get("create_opennebula_user", False):
            return

        vdc_name = resource.backend_id
        username = self._generate_opennebula_username(vdc_name)
        password = secrets.token_urlsafe(16)

        try:
            self.client.create_user(username, password, vdc_name)
        except BackendError as e:
            logger.warning(
                "Failed to create OpenNebula user '%s' for VDC '%s': %s",
                username,
                vdc_name,
                e,
            )
            return

        # Merge credentials into cached metadata
        cred_metadata = {
            "opennebula_username": username,
            "opennebula_password": password,
        }
        if vdc_name in self._resource_network_metadata:
            self._resource_network_metadata[vdc_name].update(cred_metadata)
        else:
            self._resource_network_metadata[vdc_name] = cred_metadata

        # Set backend_metadata on the resource info for the processor to push
        resource.backend_metadata = self._resource_network_metadata[vdc_name].copy()

    def _pre_delete_resource(self, waldur_resource: WaldurResource) -> None:
        """Clean up OpenNebula user before VDC deletion."""
        if self.resource_type != "vdc":
            return

        if not self.backend_settings.get("create_opennebula_user", False):
            return

        backend_id = waldur_resource.backend_id
        if not backend_id or not backend_id.strip():
            return

        username = self._generate_opennebula_username(backend_id)
        try:
            self.client.delete_user(username)
        except BackendError as e:
            logger.warning(
                "Failed to delete OpenNebula user '%s' during VDC deletion: %s",
                username,
                e,
            )

    def reset_vdc_user_password(
        self, resource_backend_id: str
    ) -> dict[str, str]:
        """Reset the password for the VDC's OpenNebula admin user.

        Generates a new random password, updates it in OpenNebula,
        and refreshes the in-memory metadata cache.

        Args:
            resource_backend_id: Backend ID of the VDC.

        Returns:
            Dict with ``opennebula_username`` and ``opennebula_password``.

        Raises:
            BackendError: If the user is not found or the reset fails.
        """
        username = self._generate_opennebula_username(resource_backend_id)
        new_password = secrets.token_urlsafe(16)

        self.client.reset_user_password(username, new_password)

        cred_metadata = {
            "opennebula_username": username,
            "opennebula_password": new_password,
        }

        if resource_backend_id in self._resource_network_metadata:
            self._resource_network_metadata[resource_backend_id].update(cred_metadata)
        else:
            self._resource_network_metadata[resource_backend_id] = cred_metadata

        return cred_metadata

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> None:
        """Delete resource from the backend.

        For VDC: delegates to base class (which calls client.delete_resource).
        For VM: terminates the VM by numeric ID.
        """
        if self.resource_type == "vm":
            resource_backend_id = waldur_resource.backend_id
            if not resource_backend_id or not resource_backend_id.strip():
                logger.warning("Empty backend_id for VM resource, skipping deletion")
                return
            self.client.delete_vm(int(resource_backend_id))
            return

        super().delete_resource(waldur_resource, **kwargs)

    def set_resource_limits(
        self, resource_backend_id: str, limits: dict[str, int]
    ) -> None:
        """Set resource limits — VDC quotas or VM resize.

        For VDC mode: delegates to the client to set OpenNebula group quotas.
        For VM mode: resizes the VM's CPU, RAM, and disk via poweroff/resize/resume.
        """
        if self.resource_type == "vm":
            vcpu = limits.get("vcpu", 0)
            ram_mb = limits.get("vm_ram", 0)
            disk_mb = limits.get("vm_disk", 0)
            if not any([vcpu, ram_mb, disk_mb]):
                logger.warning(
                    "No VM specs (vcpu, vm_ram, vm_disk) found in limits "
                    "for resource %s, skipping resize",
                    resource_backend_id,
                )
                return
            logger.info(
                "Resizing VM %s: vcpu=%d, ram=%d MB, disk=%d MB",
                resource_backend_id, vcpu, ram_mb, disk_mb,
            )
            self.client.resize_vm(int(resource_backend_id), vcpu, ram_mb, disk_mb)
            return

        # VDC mode: set group quotas
        self.client.set_resource_limits(resource_backend_id, limits)

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Convert Waldur resource limits to backend limits.

        Multiplies each component value by its unit_factor.
        """
        backend_limits: dict[str, int] = {}
        waldur_limits: dict[str, int] = {}

        raw_limits = waldur_resource.limits
        resource_limits: dict[str, Any] = (
            raw_limits.to_dict() if hasattr(raw_limits, "to_dict") else raw_limits or {}
        )

        for component_key, component_config in self.backend_components.items():
            waldur_value = resource_limits.get(component_key)
            if waldur_value is not None:
                unit_factor = component_config.get("unit_factor", 1)
                backend_limits[component_key] = int(waldur_value) * unit_factor
                waldur_limits[component_key] = int(waldur_value)

        return backend_limits, waldur_limits

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage report from OpenNebula.

        For VDC: reads group quota usage.
        For VM: reads per-VM resource allocation (vcpu, ram, disk).

        Returns nested dict with TOTAL_ACCOUNT_USAGE for each resource.
        Values are in Waldur units (divided by unit_factor).
        """
        if self.resource_type == "vm":
            return self._get_vm_usage_report(resource_backend_ids)

        return self._get_vdc_usage_report(resource_backend_ids)

    def _get_vdc_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect VDC usage from OpenNebula group quotas."""
        raw_reports = self.client.get_usage_report(resource_backend_ids)

        report: dict[str, dict] = {}
        for raw in raw_reports:
            resource_id = raw["resource_id"]
            backend_usage = raw["usage"]

            # Convert to Waldur units
            waldur_usage: dict[str, int] = {}
            for component_key in self.backend_components:
                backend_value = backend_usage.get(component_key, 0)
                unit_factor = self.backend_components[component_key].get(
                    "unit_factor", 1
                )
                waldur_usage[component_key] = backend_value // max(unit_factor, 1)

            report[resource_id] = {"TOTAL_ACCOUNT_USAGE": waldur_usage}

        return report

    def _get_vm_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect per-VM usage from OpenNebula VM info."""
        report: dict[str, dict] = {}

        for vm_backend_id in resource_backend_ids:
            vm_usage = self.client.get_vm_usage(int(vm_backend_id))
            if vm_usage is None:
                logger.warning("VM %s not found for usage report", vm_backend_id)
                continue

            # Convert to Waldur units
            waldur_usage: dict[str, int] = {}
            for component_key in self.backend_components:
                backend_value = vm_usage.get(component_key, 0)
                unit_factor = self.backend_components[component_key].get(
                    "unit_factor", 1
                )
                waldur_usage[component_key] = backend_value // max(unit_factor, 1)

            report[vm_backend_id] = {"TOTAL_ACCOUNT_USAGE": waldur_usage}

        return report

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Not supported — return True."""
        del resource_backend_id
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Not supported — return True."""
        del resource_backend_id
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Not supported — return True."""
        del resource_backend_id
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return metadata for the resource.

        For VDC: returns network metadata (vnet_id, vr_id, subnet, etc.)
        and credentials if ``create_opennebula_user`` is enabled.
        For VM: returns vm_id and ip_address (from cache or live query).

        On cache miss for VDC with user creation enabled, re-reads
        credentials from the OpenNebula user TEMPLATE so they survive
        agent restarts.
        """
        cached = self._resource_network_metadata.get(resource_backend_id)
        if cached:
            return cached

        if self.resource_type == "vm":
            vm_id = int(resource_backend_id)
            vm_info = self.client.get_vm(vm_id)
            if vm_info is None:
                return {}
            ip_address = self.client.get_vm_ip_address(vm_id)
            return {
                "vm_id": vm_id,
                "ip_address": ip_address or "",
            }

        # VDC path: try to refetch credentials from ONE user TEMPLATE
        if self.backend_settings.get("create_opennebula_user", False):
            username = self._generate_opennebula_username(resource_backend_id)
            creds = self.client.get_user_credentials(username)
            if creds is not None:
                self._resource_network_metadata[resource_backend_id] = creds
                return creds

        return {}
