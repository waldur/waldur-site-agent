# mypy: disable-error-code="attr-defined"
"""OpenNebula client for waldur site agent.

This module provides a client for communicating with OpenNebula via the
XML-RPC API using the pyone library. It manages VDCs, groups, quotas,
VXLAN networks, virtual routers, and security groups.
"""

import ipaddress
import logging
import time
from typing import Any, Optional, Union

import pyone
from pyone import LCM_STATE, VM_STATE

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource

logger = logging.getLogger(__name__)

# Mapping from Waldur component keys to OpenNebula quota XML elements
QUOTA_COMPONENT_MAP = {
    "cpu": ("VM", "CPU"),
    "ram": ("VM", "MEMORY"),
    "storage": ("DATASTORE", "SIZE"),
    "floating_ip": ("NETWORK", "LEASES"),
}


class OpenNebulaClient(BaseClient):
    """Client for communicating with OpenNebula via XML-RPC API."""

    def __init__(
        self,
        api_url: str,
        credentials: str,
        zone_id: int = 0,
        cluster_ids: Optional[list[int]] = None,
    ) -> None:
        """Initialize OpenNebula client.

        Args:
            api_url: OpenNebula XML-RPC endpoint (e.g. http://host:2633/RPC2).
            credentials: Authentication string in "username:password" format.
            zone_id: OpenNebula zone ID (default 0).
            cluster_ids: Specific cluster IDs to add to VDCs. If None,
                all clusters from the pool are used.
        """
        super().__init__()
        self.api_url = api_url
        self.credentials = credentials
        self.zone_id = zone_id
        self.cluster_ids = cluster_ids or []
        self.one = pyone.OneServer(api_url, session=credentials)

    # ── VDC helpers ──────────────────────────────────────────────────

    def _get_vdc_by_name(self, name: str) -> Optional[object]:
        """Find a VDC by name from the pool.

        Returns the pyone VDC object or None if not found.
        """
        try:
            pool = self.one.vdcpool.info()
            for vdc in pool.VDC:
                if vdc.NAME == name:
                    return vdc
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list VDCs: {e}") from e
        return None

    def _create_vdc(self, name: str) -> int:
        """Create a new VDC, or return existing one if name is taken.

        Returns the numeric VDC ID.
        """
        try:
            return self.one.vdc.allocate(f'NAME="{name}"')
        except pyone.OneInternalException as e:
            if "already taken" in str(e):
                existing = self._get_vdc_by_name(name)
                if existing is not None:
                    logger.info(
                        "VDC '%s' already exists (ID=%d), reusing", name, existing.ID
                    )
                    return existing.ID
            raise BackendError(f"Failed to create VDC '{name}': {e}") from e
        except pyone.OneException as e:
            raise BackendError(f"Failed to create VDC '{name}': {e}") from e

    def _delete_vdc(self, vdc_id: int) -> None:
        """Delete a VDC by numeric ID."""
        try:
            self.one.vdc.delete(vdc_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to delete VDC {vdc_id}: {e}") from e

    def _add_group_to_vdc(self, vdc_id: int, group_id: int) -> None:
        """Add a group to a VDC.

        Tolerates "already assigned" errors for idempotent retries.
        """
        try:
            self.one.vdc.addgroup(vdc_id, group_id)
        except pyone.OneInternalException as e:
            if "already assigned" in str(e):
                logger.info(
                    "Group %d is already assigned to VDC %d, skipping",
                    group_id,
                    vdc_id,
                )
                return
            raise BackendError(
                f"Failed to add group {group_id} to VDC {vdc_id}: {e}"
            ) from e
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to add group {group_id} to VDC {vdc_id}: {e}"
            ) from e

    def _add_clusters_to_vdc(self, vdc_id: int, cluster_ids: list[int]) -> None:
        """Add clusters to a VDC.

        Tolerates "already assigned" errors for idempotent retries.

        Args:
            vdc_id: VDC numeric ID.
            cluster_ids: List of cluster IDs to add. If empty, enumerates
                all clusters from the cluster pool.
        """
        if not cluster_ids:
            cluster_ids = self._list_cluster_ids()

        for cluster_id in cluster_ids:
            try:
                self.one.vdc.addcluster(vdc_id, self.zone_id, cluster_id)
            except pyone.OneInternalException as e:
                if "already assigned" in str(e):
                    logger.info(
                        "Cluster %d is already assigned to VDC %d, skipping",
                        cluster_id,
                        vdc_id,
                    )
                    continue
                raise BackendError(
                    f"Failed to add cluster {cluster_id} to VDC {vdc_id}: {e}"
                ) from e
            except pyone.OneException as e:
                raise BackendError(
                    f"Failed to add cluster {cluster_id} to VDC {vdc_id}: {e}"
                ) from e

    def _list_cluster_ids(self) -> list[int]:
        """List all cluster IDs from the cluster pool."""
        try:
            pool = self.one.clusterpool.info()
            return [cluster.ID for cluster in pool.CLUSTER]
        except pyone.OneException as e:
            raise BackendError(f"Failed to list clusters: {e}") from e

    # ── Group helpers ────────────────────────────────────────────────

    def _get_group_by_name(self, name: str) -> Optional[object]:
        """Find a group by name from the pool.

        Returns the pyone group object or None if not found.
        """
        try:
            pool = self.one.grouppool.info()
            for group in pool.GROUP:
                if group.NAME == name:
                    return group
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list groups: {e}") from e
        return None

    def _create_group(self, name: str) -> int:
        """Create a new group, or return existing one if name is taken.

        Returns the numeric group ID. Handles the case where the group
        was already created (e.g. due to a retried request after a
        connection reset).
        """
        try:
            return self.one.group.allocate(name)
        except pyone.OneInternalException as e:
            if "already taken" in str(e):
                existing = self._get_group_by_name(name)
                if existing is not None:
                    logger.info(
                        "Group '%s' already exists (ID=%d), reusing", name, existing.ID
                    )
                    return existing.ID
            raise BackendError(f"Failed to create group '{name}': {e}") from e
        except pyone.OneException as e:
            raise BackendError(f"Failed to create group '{name}': {e}") from e

    def _delete_group(self, group_id: int) -> None:
        """Delete a group by numeric ID."""
        try:
            self.one.group.delete(group_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to delete group {group_id}: {e}") from e

    def _set_group_quota(self, group_id: int, quota_xml: str) -> None:
        """Set quota on a group."""
        try:
            self.one.group.quota(group_id, quota_xml)
        except pyone.OneException as e:
            raise BackendError(f"Failed to set quota on group {group_id}: {e}") from e

    def _get_group_info(self, group_id: int) -> object:
        """Get detailed group info including quotas."""
        try:
            return self.one.group.info(group_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to get group info {group_id}: {e}") from e

    # ── Quota helpers ────────────────────────────────────────────────

    @staticmethod
    def _build_quota_template(limits: dict[str, int]) -> str:
        """Build OpenNebula quota template string from component limits dict.

        Uses OpenNebula template format (not XML), which is required for
        quota persistence in some OpenNebula versions.

        Args:
            limits: Dict mapping component keys (cpu, ram, storage, floating_ip)
                to values.

        Returns:
            Quota template string for one.group.quota().
        """
        vm_parts: dict[str, int] = {}
        ds_parts: dict[str, int] = {}
        net_parts: dict[str, int] = {}

        for component_key, value in limits.items():
            if component_key not in QUOTA_COMPONENT_MAP:
                logger.warning("Unknown component key '%s', skipping", component_key)
                continue
            section, quota_key = QUOTA_COMPONENT_MAP[component_key]
            if section == "VM":
                vm_parts[quota_key] = value
            elif section == "DATASTORE":
                ds_parts[quota_key] = value
            elif section == "NETWORK":
                net_parts[quota_key] = value

        parts = []
        if vm_parts:
            attrs = ", ".join(f'{k}="{v}"' for k, v in vm_parts.items())
            parts.append(f'VM=[{attrs}, VMS="-1"]')
        if ds_parts:
            attrs = ", ".join(f'{k}="{v}"' for k, v in ds_parts.items())
            parts.append(f'DATASTORE=[ID="-1", {attrs}, IMAGES="-1"]')
        if net_parts:
            attrs = ", ".join(f'{k}="{v}"' for k, v in net_parts.items())
            parts.append(f'NETWORK=[ID="-1", {attrs}]')

        return "\n".join(parts)

    @staticmethod
    def _get_first_vm_quota(group_info: object) -> Optional[object]:
        """Extract the first VM quota entry from group info.

        pyone returns VM_QUOTA.VM as a list of quota entries.
        """
        try:
            vm_list = group_info.VM_QUOTA.VM
            if isinstance(vm_list, list) and vm_list:
                return vm_list[0]
            if not isinstance(vm_list, list) and vm_list is not None:
                return vm_list
        except (AttributeError, TypeError):
            pass
        return None

    @staticmethod
    def _get_datastore_quotas(group_info: object) -> list:
        """Extract datastore quota entries from group info.

        pyone returns DATASTORE_QUOTA.DATASTORE as a list.
        """
        try:
            ds = group_info.DATASTORE_QUOTA.DATASTORE
            if isinstance(ds, list):
                return ds
            if ds is not None:
                return [ds]
        except (AttributeError, TypeError):
            pass
        return []

    @staticmethod
    def _get_network_quotas(group_info: object) -> list:
        """Extract network quota entries from group info.

        pyone returns NETWORK_QUOTA.NETWORK as a list.
        """
        try:
            nq = group_info.NETWORK_QUOTA.NETWORK
            if isinstance(nq, list):
                return nq
            if nq is not None:
                return [nq]
        except (AttributeError, TypeError):
            pass
        return []

    @classmethod
    def _parse_group_quota_usage(cls, group_info: object) -> dict[str, int]:
        """Parse current quota usage from group info object.

        Returns dict mapping component keys to used values.
        """
        usage: dict[str, int] = {}

        vm = cls._get_first_vm_quota(group_info)
        if vm is not None:
            if hasattr(vm, "CPU_USED"):
                usage["cpu"] = int(float(vm.CPU_USED))
            if hasattr(vm, "MEMORY_USED"):
                usage["ram"] = int(vm.MEMORY_USED)

        total_size_used = 0
        for ds in cls._get_datastore_quotas(group_info):
            if hasattr(ds, "SIZE_USED"):
                total_size_used += int(ds.SIZE_USED)
        if total_size_used > 0:
            usage["storage"] = total_size_used

        total_leases_used = 0
        for nq in cls._get_network_quotas(group_info):
            if hasattr(nq, "LEASES_USED"):
                total_leases_used += int(nq.LEASES_USED)
        if total_leases_used > 0:
            usage["floating_ip"] = total_leases_used

        return usage

    @classmethod
    def _parse_group_quota_limits(cls, group_info: object) -> dict[str, int]:
        """Parse current quota limits from group info object.

        Returns dict mapping component keys to limit values.
        """
        limits: dict[str, int] = {}

        vm = cls._get_first_vm_quota(group_info)
        if vm is not None:
            if hasattr(vm, "CPU"):
                limits["cpu"] = int(float(vm.CPU))
            if hasattr(vm, "MEMORY"):
                limits["ram"] = int(vm.MEMORY)

        for ds in cls._get_datastore_quotas(group_info):
            if hasattr(ds, "SIZE"):
                size = int(ds.SIZE)
                if size >= 0:
                    limits["storage"] = size
                    break

        total_leases = 0
        for nq in cls._get_network_quotas(group_info):
            if hasattr(nq, "LEASES"):
                leases = int(nq.LEASES)
                if leases >= 0:
                    total_leases += leases
        if total_leases > 0:
            limits["floating_ip"] = total_leases

        return limits

    # ── Network / VNet helpers ────────────────────────────────────────

    def _get_vnet_by_name(self, name: str) -> Optional[object]:
        """Find a virtual network by name from the pool.

        Returns the pyone VNet object or None if not found.
        Uses filter flag -2 (all) to list all VNets.
        """
        try:
            pool = self.one.vnpool.info(-2, -1, -1, -1)
            for vnet in pool.VNET:
                if vnet.NAME == name:
                    return vnet
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list VNets: {e}") from e
        return None

    def _allocate_next_subnet(self, base: str, prefix: int, subnet_len: int) -> str:
        """Allocate the next available subnet from the pool.

        Scans existing VNets named ``waldur_*_internal`` to find used subnets,
        then returns the first available subnet from the pool.

        Args:
            base: Base network address (e.g. "10.0.0.0").
            prefix: Base network prefix length (e.g. 8 for /8).
            subnet_len: Per-tenant subnet prefix length (e.g. 24 for /24).

        Returns:
            Network address of the allocated subnet (e.g. "10.0.1.0").
        """
        pool_network = ipaddress.ip_network(f"{base}/{prefix}", strict=False)

        # Collect used subnets from existing waldur internal networks
        used_subnets: set[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]] = set()
        try:
            vnet_pool = self.one.vnpool.info(-2, -1, -1, -1)
            for vnet in vnet_pool.VNET:
                if not vnet.NAME.startswith("waldur_") or not vnet.NAME.endswith(
                    "_internal"
                ):
                    continue
                # Extract IP from the address range template
                try:
                    ar_pool = vnet.AR_POOL
                    if ar_pool is not None:
                        ar_list = (
                            ar_pool.AR if isinstance(ar_pool.AR, list) else [ar_pool.AR]
                        )
                        for ar in ar_list:
                            if hasattr(ar, "IP"):
                                subnet = ipaddress.ip_network(
                                    f"{ar.IP}/{subnet_len}", strict=False
                                )
                                used_subnets.add(subnet)
                except (AttributeError, TypeError):
                    continue
        except pyone.OneNoExistsException:
            pass
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to scan VNet pool for subnet allocation: {e}"
            ) from e

        # Iterate through possible subnets and find the first unused one
        for subnet in pool_network.subnets(new_prefix=subnet_len):
            # Skip the base network itself (e.g. 10.0.0.0/24)
            if subnet.network_address == pool_network.network_address:
                continue
            if subnet not in used_subnets:
                return str(subnet.network_address)

        raise BackendError(
            f"No available subnets in pool {base}/{prefix} with /{subnet_len}"
        )

    def _create_vxlan_network(
        self,
        name: str,
        cidr: str,
        gateway: str,
        phydev: str,
        dns: str,
        cluster_id: int,
        vn_mad: str = "vxlan",
    ) -> int:
        """Create a virtual network for tenant isolation.

        Args:
            name: VNet name (e.g. "waldur_<slug>_internal").
            cidr: Subnet CIDR (e.g. "10.0.1.0/24").
            gateway: Gateway IP address (e.g. "10.0.1.1").
            phydev: Physical device for VXLAN tunnels (e.g. "eth0").
            dns: DNS server address.
            cluster_id: Cluster ID to place the VNet in.
            vn_mad: Network driver (e.g. "vxlan", "bridge"). Default "vxlan".

        Returns:
            Numeric VNet ID.
        """
        network = ipaddress.ip_network(cidr, strict=False)
        # AR includes gateway IP (first usable) through last usable address.
        # Size excludes only network and broadcast addresses.
        size = network.num_addresses - 2  # network + broadcast excluded

        lines = [
            f'NAME="{name}"',
            f'VN_MAD="{vn_mad}"',
        ]
        if vn_mad == "vxlan":
            lines.append('AUTOMATIC_VLAN_ID="YES"')
            lines.append(f'PHYDEV="{phydev}"')
        lines.extend([
            "AR=[",
            '  TYPE="IP4",',
            f'  IP="{network.network_address + 1}",',
            f'  SIZE="{size}"',
            "]",
            f'DNS="{dns}"',
            f'GATEWAY="{gateway}"',
        ])
        template = "\n".join(lines)

        try:
            return self.one.vn.allocate(template, cluster_id)
        except pyone.OneInternalException as e:
            if "already taken" in str(e):
                existing = self._get_vnet_by_name(name)
                if existing is not None:
                    logger.info(
                        "VNet '%s' already exists (ID=%d), reusing", name, existing.ID
                    )
                    return existing.ID
            raise BackendError(f"Failed to create VXLAN VNet '{name}': {e}") from e
        except pyone.OneException as e:
            raise BackendError(f"Failed to create VXLAN VNet '{name}': {e}") from e

    def _delete_vnet(self, vnet_id: int) -> None:
        """Delete a virtual network by ID."""
        try:
            self.one.vn.delete(vnet_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to delete VNet {vnet_id}: {e}") from e

    def _add_vnet_to_vdc(self, vdc_id: int, zone_id: int, vnet_id: int) -> None:
        """Add a virtual network to a VDC."""
        try:
            self.one.vdc.addvnet(vdc_id, zone_id, vnet_id)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to add VNet {vnet_id} to VDC {vdc_id}: {e}"
            ) from e

    # ── Virtual Router helpers ────────────────────────────────────────

    def _get_vrouter_by_name(self, name: str) -> Optional[object]:
        """Find a virtual router by name from the pool.

        Returns the pyone VRouter object or None if not found.
        Uses filter flag -2 (all).
        """
        try:
            pool = self.one.vrouterpool.info(-2, -1, -1)
            for vr in pool.VROUTER:
                if vr.NAME == name:
                    return vr
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list virtual routers: {e}") from e
        return None

    def _create_virtual_router(self, name: str) -> int:
        """Create a virtual router object.

        Returns the numeric VRouter ID.
        """
        template = f'NAME="{name}"'
        try:
            return self.one.vrouter.allocate(template)
        except pyone.OneInternalException as e:
            if "already taken" in str(e):
                existing = self._get_vrouter_by_name(name)
                if existing is not None:
                    logger.info(
                        "VRouter '%s' already exists (ID=%d), reusing",
                        name,
                        existing.ID,
                    )
                    return existing.ID
            raise BackendError(f"Failed to create virtual router '{name}': {e}") from e
        except pyone.OneException as e:
            raise BackendError(f"Failed to create virtual router '{name}': {e}") from e

    def _instantiate_vr(
        self,
        vr_id: int,
        template_id: int,
        name: str,
        extra_template: str = "",
    ) -> int:
        """Instantiate a VR VM from a template.

        Args:
            vr_id: Virtual router ID.
            template_id: VM template ID for the VNF appliance.
            name: Name for the VR VM.
            extra_template: Additional template attributes (NIC definitions, etc.).

        Returns:
            VM ID of the instantiated VR.
        """
        try:
            # API: one.vrouter.instantiate(vr_id, n_vms, template_id, name, hold, extra_template)
            self.one.vrouter.instantiate(
                vr_id, 1, template_id, name, False, extra_template
            )
            # vrouter.instantiate returns the VR ID, not the VM ID.
            # Read the VR info to get the actual VM ID.
            vr_info = self.one.vrouter.info(vr_id)
            vm_ids = vr_info.VMS.ID
            if isinstance(vm_ids, list):
                vm_id = vm_ids[-1]
            else:
                vm_id = vm_ids
            logger.info(
                "VR %d instantiated, VM ID: %d", vr_id, vm_id
            )
            return int(vm_id)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to instantiate VR {vr_id} from template {template_id}: {e}"
            ) from e

    def _delete_virtual_router(self, vr_id: int) -> None:
        """Delete a virtual router and its associated VMs."""
        try:
            self.one.vrouter.delete(vr_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to delete virtual router {vr_id}: {e}") from e

    # ── Security Group helpers ────────────────────────────────────────

    def _get_secgroup_by_name(self, name: str) -> Optional[object]:
        """Find a security group by name from the pool.

        Returns the pyone SecGroup object or None if not found.
        Uses filter flag -2 (all).
        """
        try:
            pool = self.one.secgrouppool.info(-2, -1, -1)
            for sg in pool.SECURITY_GROUP:
                if sg.NAME == name:
                    return sg
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list security groups: {e}") from e
        return None

    def _create_security_group(self, name: str, rules: list[dict[str, str]]) -> int:
        """Create a security group with the given rules.

        Args:
            name: Security group name.
            rules: List of rule dicts with keys like direction, protocol, range, type.

        Returns:
            Numeric security group ID.
        """
        rule_parts = []
        for rule in rules:
            attrs = []
            if "protocol" in rule:
                attrs.append(f'PROTOCOL="{rule["protocol"]}"')
            if "direction" in rule:
                rule_type = rule["direction"].lower()
                attrs.append(f'RULE_TYPE="{rule_type}"')
            if "range" in rule:
                attrs.append(f'RANGE="{rule["range"]}"')
            if "type" in rule:
                attrs.append(f'ICMP_TYPE="{rule["type"]}"')
            rule_parts.append("RULE=[" + ", ".join(attrs) + "]")

        template = f'NAME="{name}"\n' + "\n".join(rule_parts)

        try:
            return self.one.secgroup.allocate(template)
        except pyone.OneInternalException as e:
            if "already taken" in str(e):
                existing = self._get_secgroup_by_name(name)
                if existing is not None:
                    logger.info(
                        "SG '%s' already exists (ID=%d), reusing", name, existing.ID
                    )
                    return existing.ID
            raise BackendError(f"Failed to create security group '{name}': {e}") from e
        except pyone.OneException as e:
            raise BackendError(f"Failed to create security group '{name}': {e}") from e

    def _delete_security_group(self, sg_id: int) -> None:
        """Delete a security group by ID."""
        try:
            self.one.secgroup.delete(sg_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to delete security group {sg_id}: {e}") from e

    # ── Networking orchestration ──────────────────────────────────────

    def _setup_networking(
        self, name: str, vdc_id: int, network_config: dict[str, Any]
    ) -> dict[str, Any]:
        """Set up networking infrastructure for a VDC.

        Creates internal VXLAN network, virtual router, and security group.
        Returns metadata about created resources.

        Args:
            name: Resource base name (used for naming convention).
            vdc_id: VDC numeric ID.
            network_config: Configuration dict with keys:
                - zone_id: OpenNebula zone ID
                - cluster_ids: Cluster IDs for placement
                - external_network_id: Public network for router uplink
                - vxlan_phydev: Physical interface for VXLAN
                - virtual_router_template_id: VNF appliance template ID
                - default_dns: DNS server address
                - internal_network_base: Base address for tenant subnets
                - internal_network_prefix: Base pool prefix length
                - subnet_prefix_length: Per-tenant subnet prefix
                - security_group_defaults: Default SG rules
                - subnet_cidr: User-specified subnet (optional)
                - enable_internet_access: Enable NAT (default True)

        Returns:
            Dict with created resource metadata (vnet_id, vr_id, sg_id, subnet_cidr, etc.)
        """
        vnet_name = f"{name}_internal"
        vr_name = f"{name}_router"
        sg_name = f"{name}_default"
        zone_id = network_config.get("zone_id", self.zone_id)
        cluster_ids = network_config.get("cluster_ids", self.cluster_ids)
        cluster_id = cluster_ids[0] if cluster_ids else 0

        # Step 1: Allocate or use specified subnet
        subnet_cidr = network_config.get("subnet_cidr")
        if not subnet_cidr:
            base = network_config["internal_network_base"]
            prefix = network_config["internal_network_prefix"]
            subnet_len = network_config["subnet_prefix_length"]
            subnet_addr = self._allocate_next_subnet(base, prefix, subnet_len)
            subnet_cidr = f"{subnet_addr}/{subnet_len}"

        network = ipaddress.ip_network(subnet_cidr, strict=False)
        gateway_ip = str(network.network_address + 1)
        phydev = network_config["vxlan_phydev"]
        dns = network_config.get("default_dns", "8.8.8.8")

        vnet_id = None
        vr_id = None
        sg_id = None

        vn_mad = network_config.get("vn_mad", "vxlan")

        try:
            # Step 2: Create VNet
            logger.info(
                "Creating %s VNet '%s' with subnet %s", vn_mad, vnet_name, subnet_cidr
            )
            vnet_id = self._create_vxlan_network(
                vnet_name, subnet_cidr, gateway_ip, phydev, dns, cluster_id,
                vn_mad=vn_mad,
            )

            # Step 2b: Add VNet to additional clusters
            for extra_cluster_id in cluster_ids:
                if extra_cluster_id != cluster_id:
                    logger.info(
                        "Adding VNet %d to cluster %d", vnet_id, extra_cluster_id
                    )
                    try:
                        self.one.cluster.addvnet(extra_cluster_id, vnet_id)
                    except pyone.OneException as e:
                        raise BackendError(
                            f"Failed to add VNet {vnet_id} to cluster {extra_cluster_id}: {e}"
                        ) from e

            # Step 3: Add VNet to VDC
            logger.info("Adding VNet %d to VDC %d", vnet_id, vdc_id)
            self._add_vnet_to_vdc(vdc_id, zone_id, vnet_id)

            # Step 4: Create Virtual Router
            logger.info("Creating virtual router '%s'", vr_name)
            vr_id = self._create_virtual_router(vr_name)

            # Step 5: Instantiate VR with NICs
            vr_template_id = network_config["virtual_router_template_id"]
            external_net_id = network_config["external_network_id"]

            nic_template = (
                f'NIC=[NETWORK_ID="{vnet_id}", IP="{gateway_ip}"]\n'
                f'NIC=[NETWORK_ID="{external_net_id}"]'
            )
            sched_req = self._build_sched_requirements(
                cluster_ids,
                network_config.get("sched_requirements", ""),
            )
            if sched_req:
                nic_template += f'\nSCHED_REQUIREMENTS="{sched_req}"'
            logger.info("Instantiating VR %d from template %d", vr_id, vr_template_id)
            vr_vm_id = self._instantiate_vr(
                vr_id, vr_template_id, f"{vr_name}_vm", nic_template
            )

            # Step 5b: Wait for VR VM to reach RUNNING state
            self._wait_for_vm_running(vr_vm_id)

            # Step 6: Create Security Group
            sg_rules = network_config.get("security_group_defaults", [])
            if sg_rules:
                logger.info("Creating security group '%s'", sg_name)
                sg_id = self._create_security_group(sg_name, sg_rules)

        except BackendError:
            # Rollback in reverse order
            logger.warning("Networking setup failed for '%s', rolling back", name)
            if sg_id is not None:
                try:
                    self._delete_security_group(sg_id)
                except BackendError:
                    logger.warning("Failed to rollback security group %d", sg_id)
            if vr_id is not None:
                try:
                    self._delete_virtual_router(vr_id)
                except BackendError:
                    logger.warning("Failed to rollback virtual router %d", vr_id)
            if vnet_id is not None:
                try:
                    self._delete_vnet(vnet_id)
                except BackendError:
                    logger.warning("Failed to rollback VNet %d", vnet_id)
            raise

        metadata = {
            "vnet_id": vnet_id,
            "vnet_name": vnet_name,
            "subnet_cidr": subnet_cidr,
            "gateway_ip": gateway_ip,
            "vr_id": vr_id,
            "vr_vm_id": vr_vm_id,
            "vr_name": vr_name,
        }
        if sg_id is not None:
            metadata["sg_id"] = sg_id
            metadata["sg_name"] = sg_name

        return metadata

    def _teardown_networking(self, name: str) -> None:
        """Tear down networking infrastructure for a VDC.

        Finds and deletes VR, VNet, and SG by naming convention.
        Waits for VR VM termination before deleting VNet (leases must
        be released first).

        Args:
            name: Resource base name.
        """
        vr_name = f"{name}_router"
        vnet_name = f"{name}_internal"
        sg_name = f"{name}_default"

        # Delete virtual router first (releases NICs)
        vr = self._get_vrouter_by_name(vr_name)
        if vr is not None:
            # Collect VM IDs before deletion so we can wait for them
            vr_info = self.one.vrouter.info(vr.ID)
            vr_vm_ids: list[int] = []
            if hasattr(vr_info.VMS, "ID") and vr_info.VMS.ID is not None:
                ids = vr_info.VMS.ID
                vr_vm_ids = ids if isinstance(ids, list) else [ids]
            logger.info("Deleting virtual router '%s' (ID %d)", vr_name, vr.ID)
            self._delete_virtual_router(vr.ID)
            # Wait for VR VMs to reach DONE so NIC leases are released
            for vm_id in vr_vm_ids:
                self._wait_for_vm_state(vm_id, VM_STATE.DONE)

        # Delete internal VNet
        vnet = self._get_vnet_by_name(vnet_name)
        if vnet is not None:
            logger.info("Deleting VNet '%s' (ID %d)", vnet_name, vnet.ID)
            self._delete_vnet(vnet.ID)

        # Delete security group
        sg = self._get_secgroup_by_name(sg_name)
        if sg is not None:
            logger.info("Deleting security group '%s' (ID %d)", sg_name, sg.ID)
            self._delete_security_group(sg.ID)

    # ── Scheduling helpers ─────────────────────────────────────────────

    @staticmethod
    def _build_sched_requirements(
        cluster_ids: list[int],
        custom_expression: str = "",
    ) -> str:
        """Build SCHED_REQUIREMENTS string.

        If custom_expression is provided, use it directly.
        Otherwise auto-generate from cluster_ids.
        Returns empty string if neither is available.
        """
        if custom_expression:
            return custom_expression
        if cluster_ids:
            clauses = [f"CLUSTER_ID={cid}" for cid in cluster_ids]
            return " | ".join(clauses)
        return ""

    # ── VM helpers ────────────────────────────────────────────────────

    FAILURE_VM_STATES = frozenset({VM_STATE.FAILED, VM_STATE.CLONING_FAILURE})

    FAILURE_LCM_STATES = frozenset(
        {
            LCM_STATE.FAILURE,
            LCM_STATE.BOOT_FAILURE,
            LCM_STATE.BOOT_MIGRATE_FAILURE,
            LCM_STATE.PROLOG_MIGRATE_FAILURE,
            LCM_STATE.PROLOG_FAILURE,
            LCM_STATE.EPILOG_FAILURE,
            LCM_STATE.EPILOG_STOP_FAILURE,
            LCM_STATE.EPILOG_UNDEPLOY_FAILURE,
            LCM_STATE.PROLOG_MIGRATE_POWEROFF_FAILURE,
            LCM_STATE.PROLOG_MIGRATE_SUSPEND_FAILURE,
            LCM_STATE.BOOT_UNDEPLOY_FAILURE,
            LCM_STATE.BOOT_STOPPED_FAILURE,
            LCM_STATE.PROLOG_RESUME_FAILURE,
            LCM_STATE.PROLOG_UNDEPLOY_FAILURE,
            LCM_STATE.PROLOG_MIGRATE_UNKNOWN_FAILURE,
        }
    )

    def _wait_for_vm_running(
        self, vm_id: int, timeout: int = 300, poll_interval: int = 5
    ) -> None:
        """Poll VM until it reaches ACTIVE/RUNNING or a failure state.

        Args:
            vm_id: Numeric VM ID to monitor.
            timeout: Maximum seconds to wait before raising.
            poll_interval: Seconds between polls.

        Raises:
            BackendError: If the VM enters a failure state or times out.
        """
        deadline = time.monotonic() + timeout
        prev_state: Optional[tuple[int, int]] = None

        while True:
            vm_info = self._get_vm_info(vm_id)
            state = int(vm_info.STATE)
            lcm_state = int(vm_info.LCM_STATE)

            if (state, lcm_state) != prev_state:
                logger.info(
                    "VM %d state: STATE=%d LCM_STATE=%d", vm_id, state, lcm_state
                )
                prev_state = (state, lcm_state)

            # Success
            if state == VM_STATE.ACTIVE and lcm_state == LCM_STATE.RUNNING:
                logger.info("VM %d is now ACTIVE/RUNNING", vm_id)
                return

            # Failure states
            if state in self.FAILURE_VM_STATES:
                raise BackendError(
                    f"VM {vm_id} entered failure state: STATE={state}"
                )
            if lcm_state in self.FAILURE_LCM_STATES:
                raise BackendError(
                    f"VM {vm_id} entered failure LCM state: "
                    f"STATE={state} LCM_STATE={lcm_state}"
                )

            # Timeout
            if time.monotonic() >= deadline:
                raise BackendError(
                    f"VM {vm_id} did not reach RUNNING within {timeout}s "
                    f"(last STATE={state} LCM_STATE={lcm_state})"
                )

            time.sleep(poll_interval)

    def _get_vm_by_name(self, vm_name: str) -> Optional[object]:
        """Find a VM by name from the pool.

        Returns the pyone VM object or None if not found.
        Uses filter flag -2 (all), no range limits.
        """
        try:
            pool = self.one.vmpool.info(-2, -1, -1, -1)
            for vm in pool.VM:
                if vm.NAME == vm_name:
                    return vm
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list VMs: {e}") from e
        return None

    def _get_vm_info(self, vm_id: int) -> object:
        """Get detailed VM info by numeric ID."""
        try:
            return self.one.vm.info(vm_id)
        except pyone.OneException as e:
            raise BackendError(f"Failed to get VM info {vm_id}: {e}") from e

    def create_vm(
        self,
        template_id: int,
        vm_name: str,
        parent_vdc_name: str,
        ssh_key: str = "",
        vcpu: int = 1,
        ram_mb: int = 512,
        disk_mb: int = 10240,
        cluster_ids: Optional[list[int]] = None,
        sched_requirements: str = "",
    ) -> int:
        """Instantiate a VM from a template within a VDC.

        The VM is placed on the VDC's internal network (named
        ``{parent_vdc_name}_internal``) and assigned the VDC's default
        security group (``{parent_vdc_name}_default``).

        After instantiation, the VM is chowned to the VDC's group so that
        it counts against group quotas.

        Args:
            template_id: OpenNebula VM template ID.
            vm_name: Name for the new VM.
            parent_vdc_name: Backend ID of the parent VDC.
            ssh_key: Optional SSH public key for CONTEXT.
            vcpu: Number of virtual CPUs.
            ram_mb: Memory in MB.
            disk_mb: Disk size in MB.

        Returns:
            Numeric VM ID.
        """
        # Resolve VDC network and security group
        vnet_name = f"{parent_vdc_name}_internal"
        sg_name = f"{parent_vdc_name}_default"

        vnet = self._get_vnet_by_name(vnet_name)
        if vnet is None:
            raise BackendError(
                f"Internal network '{vnet_name}' not found for VDC '{parent_vdc_name}'"
            )
        vnet_id = vnet.ID

        sg_id = ""
        sg = self._get_secgroup_by_name(sg_name)
        if sg is not None:
            sg_id = str(sg.ID)

        # Build extra template for instantiation
        extra_parts = [
            f'NIC=[NETWORK_ID="{vnet_id}"'
            + (f', SECURITY_GROUPS="{sg_id}"' if sg_id else "")
            + "]",
            f'CPU="{vcpu}"',
            f'VCPU="{vcpu}"',
            f'MEMORY="{ram_mb}"',
        ]

        context_parts = ['NETWORK="YES"']
        if ssh_key:
            # Escape double quotes in SSH key
            safe_key = ssh_key.replace('"', '\\"')
            context_parts.append(f'SSH_PUBLIC_KEY="{safe_key}"')
        extra_parts.append("CONTEXT=[" + ", ".join(context_parts) + "]")

        sched_req = self._build_sched_requirements(
            cluster_ids or [], sched_requirements
        )
        if sched_req:
            extra_parts.append(f'SCHED_REQUIREMENTS="{sched_req}"')

        extra_template = "\n".join(extra_parts)

        try:
            vm_id = self.one.template.instantiate(
                template_id, vm_name, False, extra_template
            )
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to instantiate VM '{vm_name}' from template {template_id}: {e}"
            ) from e

        # Chown VM to VDC's group so it counts against group quotas
        try:
            self._chown_vm_to_group(vm_id, parent_vdc_name)
        except BackendError:
            logger.warning(
                "Failed to chown VM %d to group '%s', attempting cleanup",
                vm_id,
                parent_vdc_name,
            )
            try:
                self.one.vm.action("terminate-hard", vm_id)
            except pyone.OneException:
                logger.warning("Failed to terminate VM %d during rollback", vm_id)
            raise

        # Wait for VM to reach RUNNING state
        try:
            self._wait_for_vm_running(vm_id)
        except BackendError:
            logger.warning(
                "VM %d failed to reach RUNNING state, attempting cleanup",
                vm_id,
            )
            try:
                self.one.vm.action("terminate-hard", vm_id)
            except pyone.OneException:
                logger.warning("Failed to terminate VM %d during rollback", vm_id)
            raise

        logger.info(
            "Created VM '%s' (ID %d) in VDC '%s'",
            vm_name,
            vm_id,
            parent_vdc_name,
        )
        return vm_id

    def delete_vm(self, vm_id: int) -> None:
        """Terminate a VM by numeric ID.

        Uses ``terminate-hard`` to immediately destroy the VM.
        """
        try:
            self.one.vm.action("terminate-hard", vm_id)
            logger.info("Terminated VM ID %d", vm_id)
        except pyone.OneNoExistsException:
            logger.warning("VM ID %d not found for deletion", vm_id)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to terminate VM ID {vm_id}: {e}"
            ) from e

    def resize_vm(
        self,
        vm_id: int,
        vcpu: int,
        ram_mb: int,
        disk_mb: int,
    ) -> None:
        """Resize a VM's CPU, RAM, and disk.

        The VM is powered off (if running), resized, then resumed.

        Args:
            vm_id: Numeric OpenNebula VM ID.
            vcpu: New number of virtual CPUs.
            ram_mb: New memory in MB.
            disk_mb: New disk size in MB.

        Raises:
            BackendError: If the VM is not found or resize fails.
        """
        vm_info = self._get_vm_info(vm_id)
        state = int(vm_info.STATE)

        # Power off if the VM is running (state 3 = ACTIVE)
        needs_resume = False
        if state == VM_STATE.ACTIVE:
            logger.info("Powering off VM %d for resize", vm_id)
            try:
                self.one.vm.action("poweroff-hard", vm_id)
            except pyone.OneException as e:
                raise BackendError(
                    f"Failed to power off VM {vm_id} for resize: {e}"
                ) from e
            self._wait_for_vm_state(vm_id, VM_STATE.POWEROFF)
            needs_resume = True
        elif state != VM_STATE.POWEROFF:
            raise BackendError(
                f"VM {vm_id} is in state {state}, cannot resize "
                "(must be ACTIVE or POWEROFF)"
            )

        # Resize CPU and RAM
        resize_template = f'CPU="{vcpu}"\nVCPU="{vcpu}"\nMEMORY="{ram_mb}"'
        try:
            # enforce=False: skip ONE-level quota checks — Waldur is the
            # authority for resource allocation and the agent runs as admin.
            self.one.vm.resize(vm_id, resize_template, False)
            logger.info(
                "Resized VM %d: vcpu=%d, ram=%d MB",
                vm_id, vcpu, ram_mb,
            )
        except pyone.OneException as e:
            if needs_resume:
                self._safe_resume(vm_id)
            raise BackendError(
                f"Failed to resize VM {vm_id}: {e}"
            ) from e

        # Resize disk if needed (use disk 0 — the primary disk)
        vm_info = self._get_vm_info(vm_id)
        current_disk_mb = self._get_primary_disk_size(vm_info)
        if current_disk_mb is not None and disk_mb > current_disk_mb:
            try:
                self.one.vm.diskresize(vm_id, 0, str(disk_mb))
                logger.info(
                    "Resized VM %d disk 0: %d MB -> %d MB",
                    vm_id, current_disk_mb, disk_mb,
                )
            except pyone.OneException as e:
                logger.warning(
                    "Failed to resize disk for VM %d: %s", vm_id, e,
                )
        elif current_disk_mb is not None and disk_mb < current_disk_mb:
            logger.warning(
                "Disk shrink not supported: VM %d disk is %d MB, "
                "requested %d MB — skipping disk resize",
                vm_id, current_disk_mb, disk_mb,
            )

        # Resume the VM
        if needs_resume:
            self._safe_resume(vm_id)
            self._wait_for_vm_running(vm_id)

        logger.info("VM %d resize complete", vm_id)

    def _wait_for_vm_state(
        self, vm_id: int, target_state: int,
        timeout: int = 300, poll_interval: int = 5,
    ) -> None:
        """Poll VM until it reaches the target state.

        Raises:
            BackendError: If the VM enters a failure state or times out.
        """
        deadline = time.monotonic() + timeout
        while True:
            vm_info = self._get_vm_info(vm_id)
            state = int(vm_info.STATE)
            if state == target_state:
                logger.info("VM %d reached state %d", vm_id, target_state)
                return
            if state in self.FAILURE_VM_STATES:
                raise BackendError(
                    f"VM {vm_id} entered failure state {state} "
                    f"while waiting for state {target_state}"
                )
            if time.monotonic() >= deadline:
                raise BackendError(
                    f"VM {vm_id} did not reach state {target_state} "
                    f"within {timeout}s (current state={state})"
                )
            time.sleep(poll_interval)

    def _safe_resume(self, vm_id: int) -> None:
        """Resume a VM, logging but not raising on failure."""
        try:
            self.one.vm.action("resume", vm_id)
            logger.info("Resumed VM %d", vm_id)
        except pyone.OneException:
            logger.warning(
                "Failed to resume VM %d after resize", vm_id,
            )

    @staticmethod
    def _get_primary_disk_size(vm_info: object) -> Optional[int]:
        """Get the size of disk 0 from VM info, in MB."""
        try:
            template = vm_info.TEMPLATE
            if hasattr(template, "DISK"):
                disks = template.DISK
                if not isinstance(disks, list):
                    disks = [disks]
                if disks and hasattr(disks[0], "SIZE"):
                    return int(disks[0].SIZE)
        except (AttributeError, TypeError, ValueError):
            pass
        return None

    def get_vm(self, vm_id: int) -> Optional[dict[str, Any]]:
        """Get VM info by numeric ID.

        Returns dict with vm_id, name, state, or None if not found.
        """
        try:
            vm_info = self._get_vm_info(vm_id)
        except BackendError:
            return None
        return {
            "vm_id": vm_id,
            "name": vm_info.NAME,
            "state": int(vm_info.STATE),
            "lcm_state": int(vm_info.LCM_STATE),
        }

    def get_vm_usage(self, vm_id: int) -> Optional[dict[str, int]]:
        """Get resource usage for a VM by numeric ID.

        Returns dict with vcpu, vm_ram, vm_disk values in native units,
        or None if VM not found.
        """
        try:
            vm_info = self._get_vm_info(vm_id)
        except BackendError:
            return None

        usage: dict[str, int] = {}

        try:
            template = vm_info.TEMPLATE
            if hasattr(template, "VCPU"):
                usage["vcpu"] = int(template.VCPU)
            elif hasattr(template, "CPU"):
                usage["vcpu"] = int(float(template.CPU))
            if hasattr(template, "MEMORY"):
                usage["vm_ram"] = int(template.MEMORY)
            # Disk: sum all DISK sizes
            if hasattr(template, "DISK"):
                disks = template.DISK
                if not isinstance(disks, list):
                    disks = [disks]
                total_disk = 0
                for disk in disks:
                    if hasattr(disk, "SIZE"):
                        total_disk += int(disk.SIZE)
                if total_disk > 0:
                    usage["vm_disk"] = total_disk
        except (AttributeError, TypeError, ValueError) as e:
            logger.warning("Failed to parse VM %d template: %s", vm_id, e)

        return usage

    def get_vm_ip_address(self, vm_id: int) -> Optional[str]:
        """Get the first IP address of a VM by numeric ID.

        Returns the IP string or None if VM not found or has no NIC.
        """
        try:
            vm_info = self._get_vm_info(vm_id)
        except BackendError:
            return None
        try:
            template = vm_info.TEMPLATE
            if hasattr(template, "NIC"):
                nics = template.NIC
                if not isinstance(nics, list):
                    nics = [nics]
                for nic in nics:
                    if hasattr(nic, "IP"):
                        return str(nic.IP)
        except (AttributeError, TypeError):
            pass
        return None

    def _chown_vm_to_group(self, vm_id: int, group_name: str) -> None:
        """Change VM ownership to a group (by group name).

        Sets the group ID on the VM. User ID is set to -1 (no change).
        """
        group = self._get_group_by_name(group_name)
        if group is None:
            raise BackendError(f"Group '{group_name}' not found for VM chown")

        try:
            # one.vm.chown(vm_id, user_id, group_id) — use -1 to keep user
            self.one.vm.chown(vm_id, -1, group.ID)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to chown VM {vm_id} to group '{group_name}': {e}"
            ) from e

    # ── User management helpers ────────────────────────────────────────

    def _get_user_by_name(self, username: str) -> Optional[object]:
        """Find a user by name from the pool.

        Returns the pyone user object or None if not found.
        """
        try:
            pool = self.one.userpool.info()
            for user in pool.USER:
                if user.NAME == username:
                    return user
        except pyone.OneNoExistsException:
            return None
        except pyone.OneException as e:
            raise BackendError(f"Failed to list users: {e}") from e
        return None

    def create_user(
        self, username: str, password: str, group_name: str
    ) -> int:
        """Create an OpenNebula user and assign to a group.

        Creates a regular user (driver="core"), sets the primary group
        to the VDC group, and stores the password in the user TEMPLATE
        as WALDUR_PASSWORD for later retrieval.

        Idempotent: if the user already exists, returns the existing ID.

        Args:
            username: OpenNebula user name.
            password: Authentication password.
            group_name: Name of the group to assign as primary group.

        Returns:
            Numeric user ID.
        """
        existing = self._get_user_by_name(username)
        if existing is not None:
            logger.info(
                "User '%s' already exists (ID=%d), reusing", username, existing.ID
            )
            return existing.ID

        try:
            user_id = self.one.user.allocate(username, password, "core")
        except pyone.OneException as e:
            raise BackendError(f"Failed to create user '{username}': {e}") from e

        # Set primary group
        group = self._get_group_by_name(group_name)
        if group is None:
            # Rollback: delete the user we just created
            logger.warning(
                "Group '%s' not found, rolling back user '%s'", group_name, username
            )
            try:
                self.one.user.delete(user_id)
            except pyone.OneException:
                logger.warning("Failed to rollback user %d during group lookup", user_id)
            raise BackendError(
                f"Group '{group_name}' not found for user '{username}'"
            )

        try:
            self.one.user.chgrp(user_id, group.ID)
        except pyone.OneException as e:
            # Rollback: delete the user
            logger.warning(
                "Failed to assign user '%s' to group '%s', rolling back",
                username,
                group_name,
            )
            try:
                self.one.user.delete(user_id)
            except pyone.OneException:
                logger.warning("Failed to rollback user %d during chgrp failure", user_id)
            raise BackendError(
                f"Failed to assign user '{username}' to group '{group_name}': {e}"
            ) from e

        # Store password in TEMPLATE for later retrieval
        try:
            self.one.user.update(
                user_id, f'WALDUR_PASSWORD="{password}"', 1  # 1 = merge
            )
        except pyone.OneException as e:
            logger.warning(
                "Failed to store password in user '%s' template: %s", username, e
            )

        logger.info(
            "Created user '%s' (ID=%d) in group '%s'", username, user_id, group_name
        )
        return user_id

    def delete_user(self, username: str) -> None:
        """Delete an OpenNebula user by name.

        Silent no-op if the user does not exist.
        """
        user = self._get_user_by_name(username)
        if user is None:
            logger.info("User '%s' not found for deletion, skipping", username)
            return

        try:
            self.one.user.delete(user.ID)
            logger.info("Deleted user '%s' (ID=%d)", username, user.ID)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to delete user '{username}' (ID {user.ID}): {e}"
            ) from e

    def get_user_credentials(self, username: str) -> Optional[dict[str, str]]:
        """Read credentials from a user's TEMPLATE.

        Returns dict with 'opennebula_username' and 'opennebula_password',
        or None if the user is not found.
        """
        user = self._get_user_by_name(username)
        if user is None:
            return None

        try:
            user_info = self.one.user.info(user.ID)
            template = user_info.TEMPLATE
            password = ""
            if isinstance(template, dict):
                password = str(template.get("WALDUR_PASSWORD", ""))
            elif hasattr(template, "WALDUR_PASSWORD"):
                password = str(template.WALDUR_PASSWORD)
            return {
                "opennebula_username": username,
                "opennebula_password": password,
            }
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to get user info for '{username}': {e}"
            ) from e

    def reset_user_password(
        self, username: str, new_password: str
    ) -> None:
        """Reset a user's password and update TEMPLATE.

        Args:
            username: OpenNebula user name.
            new_password: New authentication password.

        Raises:
            BackendError: If user not found or API call fails.
        """
        user = self._get_user_by_name(username)
        if user is None:
            raise BackendError(f"User '{username}' not found for password reset")

        try:
            self.one.user.passwd(user.ID, new_password)
        except pyone.OneException as e:
            raise BackendError(
                f"Failed to reset password for user '{username}': {e}"
            ) from e

        try:
            self.one.user.update(
                user.ID, f'WALDUR_PASSWORD="{new_password}"', 1  # 1 = merge
            )
        except pyone.OneException as e:
            logger.warning(
                "Password reset succeeded but failed to update TEMPLATE for '%s': %s",
                username,
                e,
            )

    # ── BaseClient abstract methods ──────────────────────────────────

    def list_resources(self) -> list[ClientResource]:
        """List all VDCs as resources."""
        try:
            pool = self.one.vdcpool.info()
            resources = []
            for vdc in pool.VDC:
                resources.append(
                    ClientResource(
                        name=vdc.NAME,
                        backend_id=vdc.NAME,
                    )
                )
            return resources
        except pyone.OneException as e:
            raise BackendError(f"Failed to list VDCs: {e}") from e

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get a VDC by name. Returns None if not found."""
        vdc = self._get_vdc_by_name(resource_id)
        if vdc is None:
            return None
        return ClientResource(
            name=vdc.NAME,
            backend_id=vdc.NAME,
        )

    def create_resource(  # type: ignore[override]
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
        network_config: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create a VDC with an associated group and optional networking.

        Creates a group, then a VDC, links them, adds clusters, and optionally
        sets up VXLAN network, virtual router, and security group.

        Args:
            name: Resource name.
            description: Resource description (unused).
            organization: Organization name (unused).
            parent_name: Parent resource name (unused).
            network_config: Optional networking configuration dict. When
                provided, VXLAN VNet, VR, and SG are created for the VDC.

        Returns:
            Resource name.
        """
        del description, organization, parent_name  # Not used for OpenNebula

        # Use cluster_ids and zone_id from network_config if available,
        # otherwise fall back to client-level defaults
        zone_id = self.zone_id
        cluster_ids = self.cluster_ids
        if network_config:
            zone_id = network_config.get("zone_id", zone_id)
            cluster_ids = network_config.get("cluster_ids", cluster_ids)

        logger.info("Creating OpenNebula group '%s'", name)
        group_id = self._create_group(name)

        try:
            logger.info("Creating OpenNebula VDC '%s'", name)
            vdc_id = self._create_vdc(name)
        except BackendError:
            logger.warning("VDC creation failed, rolling back group '%s'", name)
            self._delete_group(group_id)
            raise

        try:
            logger.info("Adding group %d to VDC %d", group_id, vdc_id)
            self._add_group_to_vdc(vdc_id, group_id)

            logger.info("Adding clusters (zone %d) to VDC %d", zone_id, vdc_id)
            self._add_clusters_to_vdc(vdc_id, cluster_ids)
        except BackendError:
            logger.warning(
                "VDC setup failed, rolling back VDC %d and group %d", vdc_id, group_id
            )
            self._delete_vdc(vdc_id)
            self._delete_group(group_id)
            raise

        # Set up networking if configured
        self._network_metadata: dict[str, Any] = {}
        if network_config:
            try:
                self._network_metadata = self._setup_networking(
                    name, vdc_id, network_config
                )
            except BackendError:
                logger.warning(
                    "Networking setup failed, rolling back VDC %d and group %d",
                    vdc_id,
                    group_id,
                )
                self._delete_vdc(vdc_id)
                self._delete_group(group_id)
                raise

        return name

    def delete_resource(self, name: str) -> str:
        """Delete a VDC and its associated group, including networking.

        Tears down networking (VR, VNet, SG) first, then VDC and group.
        """
        # Tear down networking by naming convention
        self._teardown_networking(name)

        vdc = self._get_vdc_by_name(name)
        if vdc is not None:
            logger.info("Deleting VDC '%s' (ID %d)", name, vdc.ID)
            self._delete_vdc(vdc.ID)

        group = self._get_group_by_name(name)
        if group is not None:
            logger.info("Deleting group '%s' (ID %d)", name, group.ID)
            self._delete_group(group.ID)

        return name

    def set_resource_limits(
        self, resource_id: str, limits_dict: dict[str, int]
    ) -> Optional[str]:
        """Set group quotas for the VDC's associated group."""
        group = self._get_group_by_name(resource_id)
        if group is None:
            raise BackendError(f"Group '{resource_id}' not found for setting limits")

        quota_template = self._build_quota_template(limits_dict)
        if quota_template:
            self._set_group_quota(group.ID, quota_template)
        return None

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get current group quota limits."""
        group = self._get_group_by_name(resource_id)
        if group is None:
            return {}

        group_info = self._get_group_info(group.ID)
        return self._parse_group_quota_limits(group_info)

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Not supported — Waldur shields users."""
        del resource_id
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Not supported — Waldur shields users."""
        del resource_id, username, limits_dict
        return ""

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Not supported — Waldur shields users."""
        del user, resource_id
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Not supported — Waldur shields users."""
        del resource_id, default_account
        return username

    def delete_association(self, username: str, resource_id: str) -> str:
        """Not supported — Waldur shields users."""
        del resource_id
        return username

    def get_usage_report(self, resource_ids: list[str], timezone: Optional[str] = None) -> list[dict]:
        """Get quota usage for VDC-associated groups.

        Returns a list of dicts with group quota usage keyed by resource name.
        """
        del timezone  # Not used for OpenNebula
        results = []
        for resource_name in resource_ids:
            group = self._get_group_by_name(resource_name)
            if group is None:
                logger.warning("Group '%s' not found for usage report", resource_name)
                continue

            group_info = self._get_group_info(group.ID)
            usage = self._parse_group_quota_usage(group_info)
            results.append({"resource_id": resource_name, "usage": usage})

        return results

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Not supported — Waldur shields users."""
        del resource_id
        return []
