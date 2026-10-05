"""Virtual networks, subnets, network interfaces, public IPs and security groups."""

from __future__ import annotations

import functools
from typing import Any, Iterable, Optional

from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import (
    NetworkInterface,
    NetworkInterfaceIPConfiguration,
    NetworkSecurityGroup,
    SecurityRule,
)

from .base import BaseAzureClient, wrap_azure_errors

# Azure requires a priority on every rule; 1000 is the middle of the allowed
# range, leaving room for operator rules on either side of it.
_SSH_RULE_PRIORITY = 1000


class NetworkClient(BaseAzureClient):
    """Network calls: networks, subnets, interfaces, public IPs, security groups."""

    @functools.cached_property
    def network_client(self) -> NetworkManagementClient:
        """Return the network management client."""
        return NetworkManagementClient(self.credentials.credential, self.subscription_id)

    @wrap_azure_errors
    def create_network(
        self, location: str, resource_group_name: str, network_name: str, cidr: str
    ) -> Any:
        """Start creation of a virtual network and return the poller."""
        return self.network_client.virtual_networks.begin_create_or_update(
            resource_group_name,
            network_name,
            {"location": location, "address_space": {"address_prefixes": [cidr]}},
        )

    @wrap_azure_errors
    def get_network(self, resource_group_name: str, network_name: str) -> Any:
        """Fetch a virtual network."""
        return self.network_client.virtual_networks.get(resource_group_name, network_name)

    @wrap_azure_errors
    def create_subnet(
        self, resource_group_name: str, network_name: str, subnet_name: str, cidr: str
    ) -> Any:
        """Start creation of a subnet and return the poller."""
        return self.network_client.subnets.begin_create_or_update(
            resource_group_name, network_name, subnet_name, {"address_prefix": cidr}
        )

    @wrap_azure_errors
    def get_subnet(
        self, resource_group_name: str, network_name: str, subnet_name: str
    ) -> Any:
        """Fetch a subnet."""
        return self.network_client.subnets.get(
            resource_group_name, network_name, subnet_name
        )

    @wrap_azure_errors
    def get_network_interface(
        self, resource_group_name: str, network_interface_name: str
    ) -> Any:
        """Fetch a network interface."""
        return self.network_client.network_interfaces.get(
            resource_group_name, network_interface_name
        )

    @wrap_azure_errors
    def create_network_interface(
        self,
        location: str,
        resource_group_name: str,
        interface_name: str,
        config_name: str,
        subnet_id: str,
        public_ip_id: Optional[str] = None,
        security_group_id: Optional[str] = None,
    ) -> Any:
        """Start creation of a network interface and return the poller."""
        ip_configuration = NetworkInterfaceIPConfiguration(
            name=config_name, subnet={"id": subnet_id}
        )
        if public_ip_id:
            ip_configuration.public_ip_address = {"id": public_ip_id}

        interface_parameters = NetworkInterface(
            location=location, ip_configurations=[ip_configuration]
        )
        if security_group_id:
            interface_parameters.network_security_group = {"id": security_group_id}

        return self.network_client.network_interfaces.begin_create_or_update(
            resource_group_name, interface_name, interface_parameters
        )

    @wrap_azure_errors
    def delete_network_interface(
        self, resource_group_name: str, network_interface_name: str
    ) -> Any:
        """Start deletion of a network interface and return the poller.

        Needed to clean up after a machine in a shared resource group, where the
        interface does not go away with the group.
        """
        return self.network_client.network_interfaces.begin_delete(
            resource_group_name, network_interface_name
        )

    @wrap_azure_errors
    def delete_subnet(
        self, resource_group_name: str, network_name: str, subnet_name: str
    ) -> Any:
        """Start deletion of a subnet and return the poller."""
        return self.network_client.subnets.begin_delete(
            resource_group_name, network_name, subnet_name
        )

    @wrap_azure_errors
    def delete_network(self, resource_group_name: str, network_name: str) -> Any:
        """Start deletion of a virtual network and return the poller."""
        return self.network_client.virtual_networks.begin_delete(
            resource_group_name, network_name
        )

    @wrap_azure_errors
    def create_ssh_security_group(
        self,
        location: str,
        resource_group_name: str,
        network_security_group_name: str,
        source_ranges: list[str],
    ) -> Any:
        """Start creation of a security group allowing inbound SSH.

        ``source_ranges`` are CIDR prefixes, single addresses, or Azure's own
        service tags. Ports are strings in ARM, not integers: the service rejects
        an integer port.
        """
        ssh_rule = SecurityRule(
            name="default-allow-ssh",
            protocol="Tcp",
            source_port_range="*",
            destination_port_range="22",
            direction="Inbound",
            source_address_prefixes=list(source_ranges),
            destination_address_prefix="*",
            access="Allow",
            priority=_SSH_RULE_PRIORITY,
        )
        security_group = NetworkSecurityGroup(location=location, security_rules=[ssh_rule])
        return self.network_client.network_security_groups.begin_create_or_update(
            resource_group_name, network_security_group_name, security_group
        )

    @wrap_azure_errors
    def delete_network_security_group(
        self, resource_group_name: str, network_security_group_name: str
    ) -> Any:
        """Start deletion of a security group and return the poller."""
        return self.network_client.network_security_groups.begin_delete(
            resource_group_name, network_security_group_name
        )

    @wrap_azure_errors
    def get_public_ip(self, resource_group_name: str, public_ip_address_name: str) -> Any:
        """Fetch a public IP address."""
        return self.network_client.public_ip_addresses.get(
            resource_group_name, public_ip_address_name
        )

    @wrap_azure_errors
    def list_all_public_ips(self) -> Iterable[Any]:
        """List every public IP address in the subscription."""
        return self.network_client.public_ip_addresses.list_all()

    @wrap_azure_errors
    def create_public_ip(
        self, location: str, resource_group_name: str, public_ip_address_name: str
    ) -> Any:
        """Start creation of a static IPv4 public address and return the poller."""
        return self.network_client.public_ip_addresses.begin_create_or_update(
            resource_group_name,
            public_ip_address_name,
            {
                "location": location,
                "sku": {"name": "Standard"},
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": "IPv4",
            },
        )

    @wrap_azure_errors
    def delete_public_ip(
        self, resource_group_name: str, public_ip_address_name: str
    ) -> Any:
        """Start deletion of a public IP address and return the poller."""
        return self.network_client.public_ip_addresses.begin_delete(
            resource_group_name, public_ip_address_name
        )
