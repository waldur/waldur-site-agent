"""OpenNebula backend for waldur site agent.

This module provides the backend implementation for managing OpenNebula
resources. It supports two resource types:

- **vdc** (default): Virtual Data Centers with groups, quotas, and networking.
- **vm**: Virtual Machines instantiated within a VDC.

The ``resource_type`` is read from ``backend_settings`` and determines
which code paths are taken during create, delete, and usage reporting.
"""

import logging
import os
import secrets
from typing import Any, Optional
from uuid import UUID

import yaml
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .client import OpenNebulaClient

logger = logging.getLogger(__name__)

DEFAULT_VDC_ROLES: list[dict[str, Any]] = [
    {
        "name": "admin",
        "one_group_suffix": "admins",
        "default_view": "groupadmin",
        "views": "groupadmin,user,cloud",
        "group_admin": True,
    },
    {
        "name": "user",
        "one_group_suffix": "users",
        "default_view": "user",
        "views": "user",
    },
    {
        "name": "cloud",
        "one_group_suffix": "cloud",
        "default_view": "cloud",
        "views": "cloud",
    },
]


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
        # Stores current waldur_resource for use in _create_vdc_resource
        self._current_waldur_resource: Optional[WaldurResource] = None

        # Initialize Keycloak client if configured
        self.keycloak_client = None
        if backend_settings.get("keycloak_enabled", False):
            from waldur_site_agent_keycloak_client import KeycloakClient

            keycloak_settings = backend_settings.get("keycloak", {})
            try:
                self.keycloak_client = KeycloakClient(keycloak_settings)
                logger.info("Keycloak integration enabled")
            except Exception as e:
                logger.warning("Failed to initialize Keycloak client: %s", e)
                self.keycloak_client = None

        self.saml_mapping_file = backend_settings.get(
            "saml_mapping_file", "/var/lib/one/keycloak_groups.yaml"
        )
        self.default_user_role = backend_settings.get("default_user_role", "user")
        self.vdc_roles: list[dict[str, Any]] = backend_settings.get(
            "vdc_roles", DEFAULT_VDC_ROLES
        )

    def ping(self, raise_exception: bool = False) -> bool:
        """Check connectivity to OpenNebula and Keycloak (if enabled)."""
        try:
            self.client.list_resources()
        except BackendError:
            if raise_exception:
                raise
            return False

        if self.keycloak_client:
            keycloak_ok = self.keycloak_client.ping()
            if not keycloak_ok:
                if raise_exception:
                    raise BackendError("Failed to ping Keycloak")
                return False

        return True

    def diagnostics(self) -> bool:
        """Log diagnostic information and check connectivity."""
        logger.info(
            "OpenNebula backend: api_url=%s, zone_id=%s",
            self.backend_settings["api_url"],
            self.backend_settings.get("zone_id", 0),
        )
        if self.keycloak_client:
            keycloak_settings = self.backend_settings.get("keycloak", {})
            logger.info(
                "Keycloak integration: url=%s, realm=%s",
                keycloak_settings.get("keycloak_url", "N/A"),
                keycloak_settings.get("keycloak_realm", "N/A"),
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
        self._current_waldur_resource = waldur_resource

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
        """Create a VDC with optional networking and Keycloak SAML integration."""
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

            # Set up Keycloak groups and ONE SAML mappings
            if self.keycloak_client and self._current_waldur_resource:
                try:
                    slug = self._get_resource_slug(self._current_waldur_resource)
                    vdc_resource = self.client._get_vdc_by_name(resource_backend_id)
                    vdc_id: Optional[int] = (
                        getattr(vdc_resource, "ID", None) if vdc_resource else None
                    )

                    kc_groups = self._create_keycloak_groups(
                        self._current_waldur_resource
                    )
                    if kc_groups and vdc_id is not None:
                        saml_mappings = self._create_one_keycloak_groups(
                            slug, vdc_id, kc_groups
                        )
                        if saml_mappings:
                            self._update_saml_mapping_file(saml_mappings)
                except Exception as e:
                    logger.warning(
                        "Keycloak setup failed for VDC %s, "
                        "VDC created without SAML integration: %s",
                        resource_backend_id,
                        e,
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
        """Clean up OpenNebula user and Keycloak groups before VDC deletion."""
        if self.resource_type != "vdc":
            return

        backend_id = waldur_resource.backend_id
        if not backend_id or not backend_id.strip():
            return

        # Clean up Keycloak groups and ONE SAML groups
        if self.keycloak_client:
            try:
                slug = self._get_resource_slug(waldur_resource)
                self._delete_one_keycloak_groups(slug)
                self._delete_keycloak_groups(waldur_resource)
                self._remove_saml_mapping_entries(slug)
            except Exception as e:
                logger.warning(
                    "Failed to clean up Keycloak resources during VDC deletion: %s", e
                )

        if not self.backend_settings.get("create_opennebula_user", False):
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

    # ── Keycloak / SAML integration ────────────────────────────────

    @staticmethod
    def _get_resource_slug(waldur_resource: WaldurResource) -> str:
        """Extract a slug for naming Keycloak/ONE groups.

        Uses the backend_id if set, otherwise falls back to the resource name
        sanitised for use in group names.
        """
        slug = waldur_resource.backend_id
        if slug and slug.strip():
            return slug.strip()
        name = waldur_resource.name or "unnamed"
        return name.lower().replace(" ", "_")

    def _create_keycloak_groups(
        self, waldur_resource: WaldurResource
    ) -> dict[str, str]:
        """Create hierarchical Keycloak groups for a VDC.

        Creates a parent group ``vdc_{slug}`` with child groups for each
        configured role (admin, user, cloud by default).

        Returns:
            Dict mapping role name to Keycloak child group ID.
            Empty dict on failure.
        """
        if not self.keycloak_client:
            return {}

        slug = self._get_resource_slug(waldur_resource)
        parent_group_name = f"vdc_{slug}"

        try:
            # Create or reuse parent group
            parent_group = self.keycloak_client.get_group_by_name(parent_group_name)
            if parent_group:
                parent_group_id = parent_group["id"]
                logger.info("Using existing Keycloak parent group: %s", parent_group_name)
            else:
                parent_group_id = self.keycloak_client.create_group(
                    parent_group_name, f"VDC access group for {slug}"
                )
                logger.info("Created Keycloak parent group: %s", parent_group_name)

            # Fetch existing children of this parent to avoid name collisions
            existing_children: dict[str, str] = {}
            try:
                children = self.keycloak_client.keycloak_admin.get_group_children(
                    parent_group_id
                )
                existing_children = {c["name"]: c["id"] for c in children}
            except Exception:
                pass

            # Create child groups per role
            role_groups: dict[str, str] = {}
            for role in self.vdc_roles:
                role_name = role["name"]
                if role_name in existing_children:
                    role_groups[role_name] = existing_children[role_name]
                    logger.info(
                        "Using existing Keycloak child group: %s/%s",
                        parent_group_name,
                        role_name,
                    )
                else:
                    child_id = self.keycloak_client.create_group(
                        role_name,
                        f"VDC {slug} — {role_name} role",
                        parent_group_id,
                    )
                    role_groups[role_name] = child_id
                    logger.info(
                        "Created Keycloak child group: %s/%s",
                        parent_group_name,
                        role_name,
                    )

            return role_groups

        except Exception as e:
            logger.error("Failed to create Keycloak groups for VDC %s: %s", slug, e)
            return {}

    def _create_one_keycloak_groups(
        self,
        slug: str,
        vdc_id: int,
        kc_role_groups: dict[str, str],
    ) -> dict[str, int]:
        """Create OpenNebula groups with SAML template attributes for each role.

        For each VDC role:
        1. Create an ONE group ``{slug}-{suffix}``
        2. Set SAML_GROUP and FIREEDGE template attributes
        3. Add the group to the VDC

        Args:
            slug: Resource slug for naming.
            vdc_id: Numeric OpenNebula VDC ID.
            kc_role_groups: Dict of ``{role_name: keycloak_group_id}``.

        Returns:
            Dict mapping ``{kc_group_path: one_group_id}`` for the SAML mapping file.
        """
        saml_mappings: dict[str, int] = {}
        parent_kc_name = f"vdc_{slug}"

        for role in self.vdc_roles:
            role_name = role["name"]
            suffix = role["one_group_suffix"]
            one_group_name = f"{slug}-{suffix}"

            if role_name not in kc_role_groups:
                continue

            try:
                group_id = self.client._create_group(one_group_name)

                # Build SAML_GROUP + FIREEDGE template
                kc_group_path = f"/{parent_kc_name}/{role_name}"
                template = (
                    f'SAML_GROUP="{kc_group_path}"\n'
                    f'FIREEDGE=[\n'
                    f'  DEFAULT_VIEW="{role["default_view"]}",\n'
                    f'  GROUP_ADMIN_DEFAULT_VIEW="{role["default_view"]}",\n'
                    f'  VIEWS="{role["views"]}"\n'
                    f']'
                )
                self.client._update_group_template(group_id, template)

                # Add group to VDC
                self.client._add_group_to_vdc(vdc_id, group_id)

                # Mark admin groups
                if role.get("group_admin"):
                    try:
                        # Use oneadmin (user 0) as group admin
                        self.client._add_admin_to_group(group_id, 0)
                    except BackendError:
                        logger.debug(
                            "Could not add admin to group %s (non-critical)", one_group_name
                        )

                saml_mappings[kc_group_path] = group_id
                logger.info(
                    "Created ONE group '%s' (ID=%d) with SAML mapping to %s",
                    one_group_name,
                    group_id,
                    kc_group_path,
                )
            except BackendError as e:
                logger.error(
                    "Failed to create ONE group '%s' for role '%s': %s",
                    one_group_name,
                    role_name,
                    e,
                )

        return saml_mappings

    def _update_saml_mapping_file(self, new_mappings: dict[str, int]) -> None:
        """Merge new SAML group mappings into the mapping YAML file.

        The file maps Keycloak group paths to OpenNebula group IDs.
        Creates the file if it does not exist.
        """
        existing: dict[str, int] = {}
        if os.path.exists(self.saml_mapping_file):
            try:
                with open(self.saml_mapping_file) as fh:
                    loaded = yaml.safe_load(fh)
                    if isinstance(loaded, dict):
                        existing = loaded
            except Exception as e:
                logger.warning("Failed to read SAML mapping file: %s", e)

        existing.update(new_mappings)

        try:
            # Atomic write via temp file + rename
            tmp_path = self.saml_mapping_file + ".tmp"
            with open(tmp_path, "w") as fh:
                yaml.safe_dump(existing, fh, default_flow_style=False)
            os.replace(tmp_path, self.saml_mapping_file)
            logger.info(
                "Updated SAML mapping file %s with %d entries",
                self.saml_mapping_file,
                len(new_mappings),
            )
        except Exception as e:
            logger.error("Failed to write SAML mapping file: %s", e)

    def _remove_saml_mapping_entries(self, slug: str) -> None:
        """Remove SAML mapping entries for a VDC from the mapping file."""
        if not os.path.exists(self.saml_mapping_file):
            return

        prefix = f"/vdc_{slug}/"
        try:
            with open(self.saml_mapping_file) as fh:
                loaded = yaml.safe_load(fh)
                if not isinstance(loaded, dict):
                    return

            filtered = {k: v for k, v in loaded.items() if not k.startswith(prefix)}
            if len(filtered) == len(loaded):
                return  # Nothing to remove

            tmp_path = self.saml_mapping_file + ".tmp"
            with open(tmp_path, "w") as fh:
                yaml.safe_dump(filtered, fh, default_flow_style=False)
            os.replace(tmp_path, self.saml_mapping_file)
            logger.info("Removed SAML mapping entries for slug '%s'", slug)
        except Exception as e:
            logger.warning("Failed to clean SAML mapping file: %s", e)

    def _delete_keycloak_groups(self, waldur_resource: WaldurResource) -> None:
        """Delete Keycloak groups for a VDC (children first, then parent)."""
        if not self.keycloak_client:
            return

        slug = self._get_resource_slug(waldur_resource)
        parent_group_name = f"vdc_{slug}"

        try:
            parent_group = self.keycloak_client.get_group_by_name(parent_group_name)
            if not parent_group:
                logger.info("Keycloak parent group '%s' not found, skipping", parent_group_name)
                return

            # Delete child groups first
            for subgroup in parent_group.get("subGroups", []):
                try:
                    self.keycloak_client.delete_group(subgroup["id"])
                except Exception as e:
                    logger.warning(
                        "Failed to delete Keycloak child group %s: %s",
                        subgroup.get("name"),
                        e,
                    )

            # Delete parent
            self.keycloak_client.delete_group(parent_group["id"])
            logger.info("Deleted Keycloak groups for VDC '%s'", slug)

        except Exception as e:
            logger.warning("Failed to delete Keycloak groups for VDC '%s': %s", slug, e)

    def _delete_one_keycloak_groups(self, slug: str) -> None:
        """Delete OpenNebula groups created for Keycloak SAML integration."""
        for role in self.vdc_roles:
            one_group_name = f"{slug}-{role['one_group_suffix']}"
            try:
                group = self.client._get_group_by_name(one_group_name)
                if group:
                    self.client._delete_group(getattr(group, "ID"))
                    logger.info("Deleted ONE group '%s'", one_group_name)
            except BackendError as e:
                logger.warning("Failed to delete ONE group '%s': %s", one_group_name, e)

    def _find_vdc_role_group(
        self, slug: str, role_name: str
    ) -> Optional[dict]:
        """Find a Keycloak role group under the VDC parent group.

        Looks up the parent group ``vdc_{slug}`` and then searches its
        children for one named ``role_name``. This avoids name collisions
        when multiple VDCs have child groups with the same role name.
        """
        if not self.keycloak_client:
            return None

        parent_group_name = f"vdc_{slug}"
        parent = self.keycloak_client.get_group_by_name(parent_group_name)
        if not parent:
            return None

        # Search subgroups of the parent
        try:
            children = self.keycloak_client.keycloak_admin.get_group_children(
                parent["id"]
            )
            for child in children:
                if child.get("name") == role_name:
                    return child
        except Exception as e:
            logger.warning("Failed to list children of group %s: %s", parent_group_name, e)

        return None

    def add_user(
        self, waldur_resource: WaldurResource, username: str, **kwargs: str
    ) -> bool:
        """Add user to VDC by managing Keycloak group membership.

        Finds the user in Keycloak and adds them to the appropriate
        role group for this VDC.

        Args:
            waldur_resource: The Waldur VDC resource.
            username: Username or Keycloak user identifier.
            **kwargs: Optional ``role`` key to override default_user_role.

        Returns:
            True if the user was added successfully.

        Raises:
            BackendError: If Keycloak is not enabled or user not found.
        """
        if not self.keycloak_client:
            raise BackendError(
                "Cannot add user: Keycloak integration is not enabled"
            )

        role = kwargs.get("role", self.default_user_role)
        slug = self._get_resource_slug(waldur_resource)
        parent_group_name = f"vdc_{slug}"

        logger.info("Adding user %s to VDC %s with role %s", username, slug, role)

        try:
            keycloak_user = self.keycloak_client.find_user(username)
            if not keycloak_user:
                raise BackendError(f"User '{username}' not found in Keycloak")

            # Find the role group under this VDC's parent
            group = self._find_vdc_role_group(slug, role)
            if not group:
                # Try to create missing groups
                logger.info(
                    "Keycloak group '%s/%s' not found, creating groups for VDC %s",
                    parent_group_name,
                    role,
                    slug,
                )
                kc_groups = self._create_keycloak_groups(waldur_resource)
                if role not in kc_groups:
                    raise BackendError(
                        f"Failed to create Keycloak group for role '{role}'"
                    )
                group = {"id": kc_groups[role]}

            self.keycloak_client.add_user_to_group(keycloak_user["id"], group["id"])
            logger.info(
                "Added user %s to Keycloak group %s/%s",
                username,
                parent_group_name,
                role,
            )
            return True

        except BackendError:
            raise
        except Exception as e:
            logger.error("Failed to add user %s to VDC %s: %s", username, slug, e)
            raise BackendError(f"Failed to add user {username}: {e}") from e

    def remove_user(
        self, waldur_resource: WaldurResource, username: str, **kwargs: str
    ) -> bool:
        """Remove user from VDC by removing from all Keycloak role groups.

        Args:
            waldur_resource: The Waldur VDC resource.
            username: Username or Keycloak user identifier.
            **kwargs: Unused.

        Returns:
            True if removal succeeded, False if user not found.

        Raises:
            BackendError: If Keycloak is not enabled.
        """
        del kwargs

        if not self.keycloak_client:
            raise BackendError(
                "Cannot remove user: Keycloak integration is not enabled"
            )

        slug = self._get_resource_slug(waldur_resource)

        logger.info("Removing user %s from VDC %s", username, slug)

        try:
            keycloak_user = self.keycloak_client.find_user(username)
            if not keycloak_user:
                logger.warning("User '%s' not found in Keycloak, skipping", username)
                return False

            # Remove from all role groups for this VDC
            for role in self.vdc_roles:
                role_name = role["name"]
                group = self._find_vdc_role_group(slug, role_name)
                if group:
                    try:
                        self.keycloak_client.remove_user_from_group(
                            keycloak_user["id"], group["id"]
                        )
                        logger.info(
                            "Removed user %s from Keycloak group vdc_%s/%s",
                            username,
                            slug,
                            role_name,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to remove user from group %s: %s", role_name, e
                        )

            return True

        except BackendError:
            raise
        except Exception as e:
            logger.error("Failed to remove user %s from VDC %s: %s", username, slug, e)
            raise BackendError(f"Failed to remove user {username}: {e}") from e

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
