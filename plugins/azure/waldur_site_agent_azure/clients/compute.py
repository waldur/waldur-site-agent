"""Virtual machines, sizes, images and disks."""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional

from azure.core.exceptions import AzureError
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import (
    DiskCreateOption,
    DiskCreateOptionTypes,
    DiskDeleteOptionTypes,
    LinuxConfiguration,
    OSProfile,
    SshConfiguration,
    SshPublicKey,
)

from .base import AzureClientError, BaseAzureClient, wrap_azure_errors


@dataclass
class AzureImage:
    """An image version together with the names that address it."""

    image: Any
    publisher_name: str
    offer_name: str
    sku_name: str
    version_name: str


class ComputeClient(BaseAzureClient):
    """Compute calls: machine sizes, images, virtual machines and disks."""

    @functools.cached_property
    def compute_client(self) -> ComputeManagementClient:
        """Return the compute management client."""
        return ComputeManagementClient(self.credentials.credential, self.subscription_id)

    @wrap_azure_errors
    def list_virtual_machine_sizes(self, location: str) -> Iterable[Any]:
        """List the machine sizes offered in a region."""
        return self.compute_client.virtual_machine_sizes.list(location)

    @wrap_azure_errors
    def list_virtual_machine_size_availability_zones(
        self, location: str
    ) -> dict[str, list[str]]:
        """Map each machine size in a region to the availability zones offering it."""
        all_skus = self.compute_client.resource_skus.list(filter=f"location eq '{location}'")
        zones = {}
        for sku in (sku for sku in all_skus if sku.resource_type == "virtualMachines"):
            # A SKU that lists no locations reports None rather than an empty
            # list, and iterating that is a TypeError rather than a quiet skip.
            for location_info in sku.location_info or []:
                if location_info.location == location:
                    zones[sku.name] = location_info.zones
        return zones

    def list_virtual_machine_images(
        self, location: str, selected_provider: Optional[Iterable[str]] = None
    ) -> Iterator[AzureImage]:
        """Yield every image version in a region, optionally limited to publishers.

        A generator rather than a list: the full catalogue is four nested API
        listings deep, and callers filter it down. The error translation is inside
        the body because a decorator would only cover the call that creates the
        generator, not the requests made while it is consumed.
        """
        try:
            images = self.compute_client.virtual_machine_images
            publishers = images.list_publishers(location)
            if selected_provider:
                publishers = [
                    publisher for publisher in publishers if publisher.name in selected_provider
                ]
            for publisher in publishers:
                for offer in images.list_offers(location, publisher.name):
                    for sku in images.list_skus(location, publisher.name, offer.name):
                        versions = images.list(location, publisher.name, offer.name, sku.name)
                        for version in versions:
                            yield AzureImage(
                                images.get(
                                    location,
                                    publisher.name,
                                    offer.name,
                                    sku.name,
                                    version.name,
                                ),
                                publisher.name,
                                offer.name,
                                sku.name,
                                version.name,
                            )
        except AzureError as exc:
            raise AzureClientError(str(exc)) from exc

    @wrap_azure_errors
    def list_all_virtual_machines(self) -> Iterable[Any]:
        """List every virtual machine in the subscription."""
        return self.compute_client.virtual_machines.list_all()

    @wrap_azure_errors
    def list_virtual_machines_in_group(self, resource_group_name: str) -> Iterable[Any]:
        """List the virtual machines in a resource group."""
        return self.compute_client.virtual_machines.list(resource_group_name)

    @wrap_azure_errors
    def get_virtual_machine(
        self, resource_group_name: str, vm_name: str, expand: Optional[str] = None
    ) -> Any:
        """Fetch a virtual machine, optionally expanding its instance view."""
        return self.compute_client.virtual_machines.get(
            resource_group_name, vm_name, expand=expand
        )

    @wrap_azure_errors
    def create_virtual_machine(
        self,
        location: str,
        resource_group_name: str,
        vm_name: str,
        size_name: str,
        nic_id: str,
        image_reference: dict[str, str],
        username: str,
        password: str,
        custom_data: Optional[str] = None,
        ssh_key: Optional[str] = None,
    ) -> Any:
        """Start creation of a virtual machine and return the poller."""
        os_profile = OSProfile(
            computer_name=vm_name,
            admin_username=username,
            admin_password=password,
        )
        if custom_data:
            os_profile.custom_data = custom_data
        if ssh_key:
            os_profile.linux_configuration = LinuxConfiguration(
                ssh=SshConfiguration(public_keys=[SshPublicKey(key_data=ssh_key)])
            )
        return self.compute_client.virtual_machines.begin_create_or_update(
            resource_group_name,
            vm_name,
            {
                "location": location,
                "os_profile": os_profile,
                "hardware_profile": {"vm_size": size_name},
                "storage_profile": {
                    "image_reference": {
                        "publisher": image_reference["publisher"],
                        "offer": image_reference["offer"],
                        "sku": image_reference["sku"],
                        "version": image_reference["version"],
                    },
                    # Deleting a machine does not take its managed disk unless the
                    # disk says so, and the name Azure generates for it is not one
                    # the agent could find again. Left at the default, every
                    # terminated machine leaves a disk billing forever.
                    "os_disk": {
                        "create_option": DiskCreateOptionTypes.FROM_IMAGE,
                        "delete_option": DiskDeleteOptionTypes.DELETE,
                    },
                },
                "network_profile": {"network_interfaces": [{"id": nic_id}]},
            },
        )

    @wrap_azure_errors
    def delete_virtual_machine(self, resource_group_name: str, vm_name: str) -> Any:
        """Start deletion of a virtual machine and return the poller."""
        return self.compute_client.virtual_machines.begin_delete(resource_group_name, vm_name)

    @wrap_azure_errors
    def start_virtual_machine(self, resource_group_name: str, vm_name: str) -> Any:
        """Start a stopped virtual machine."""
        return self.compute_client.virtual_machines.begin_start(resource_group_name, vm_name)

    @wrap_azure_errors
    def restart_virtual_machine(self, resource_group_name: str, vm_name: str) -> Any:
        """Restart a virtual machine."""
        return self.compute_client.virtual_machines.begin_restart(resource_group_name, vm_name)

    @wrap_azure_errors
    def deallocate_virtual_machine(self, resource_group_name: str, vm_name: str) -> Any:
        """Deallocate a virtual machine, releasing the host that bills for it.

        Not ``begin_power_off``: a powered-off machine stays allocated on its
        host, and Azure charges the full compute rate for it. Only deallocation
        stops that. The disks keep billing either way.
        """
        return self.compute_client.virtual_machines.begin_deallocate(
            resource_group_name, vm_name
        )

    @wrap_azure_errors
    def create_disk(
        self, location: str, resource_group_name: str, disk_name: str, disk_size_gb: int
    ) -> Any:
        """Start creation of an empty managed disk and return the poller."""
        return self.compute_client.disks.begin_create_or_update(
            resource_group_name,
            disk_name,
            {
                "location": location,
                "disk_size_gb": disk_size_gb,
                "creation_data": {"create_option": DiskCreateOption.empty},
            },
        )
