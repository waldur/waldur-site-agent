"""An in-memory stand-in for the Azure Resource Manager clients.

Azure has no local emulator — Azurite covers Storage alone — so the SDK boundary
is where the E2E tests stop being real. Everything above it is: the order
pipeline, the backend, the naming, the metering and the Waldur API calls all run
as they would in production.

The double keeps the objects it was asked to create, so a test can assert on the
state of the "subscription" rather than on the calls that got there.
"""

from __future__ import annotations

from typing import Any, Optional


class _Poller:
    """What every ``begin_*`` call in the Azure SDK returns."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def result(self) -> Any:
        return self._value

    def wait(self) -> None:
        return None


class _Object:
    """An ARM object: an id, a name, and whatever else the caller asked for.

    The SDK's models are generated and carry dozens of fields; a double that
    declared them would be a second implementation to keep in step. The two
    dunders below are what let the attributes stay free-form without every
    reader having to reach into ``__dict__``.
    """

    def __init__(self, **attributes: Any) -> None:
        self.__dict__.update(attributes)

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        self.__dict__[name] = value


def _arm_id(subscription: str, group: str, provider: str, collection: str, name: str) -> str:
    return (
        f"/subscriptions/{subscription}/resourceGroups/{group}"
        f"/providers/{provider}/{collection}/{name}"
    )


class FakeAzure:
    """Facade with the same shape as ``AzureClient``."""

    def __init__(
        self, subscription_id: str, tenant_id: str, client_id: str, client_secret: str
    ) -> None:
        del tenant_id, client_id, client_secret
        self.subscription_id = subscription_id
        self.resource_groups: dict[str, str] = {}
        self.machines: dict[str, _Object] = {}
        self.networks: set[str] = set()
        self.public_ips: dict[str, _Object] = {}
        self.security_groups: dict[str, list[str]] = {}
        self.interface_security_groups: dict[str, Optional[str]] = {}

        self.resource = _ResourceClient(self)
        self.compute = _ComputeClient(self)
        self.network = _NetworkClient(self)

    def ping(self) -> bool:
        return True


class _ResourceClient:
    def __init__(self, azure: FakeAzure) -> None:
        self.azure = azure

    def ping(self) -> bool:
        return True

    def create_resource_group(self, location: str, resource_group_name: str) -> _Object:
        self.azure.resource_groups[resource_group_name] = location
        return _Object(id=f"/subscriptions/{self.azure.subscription_id}", name=resource_group_name)

    def delete_resource_group(self, resource_group_name: str) -> _Poller:
        self.azure.resource_groups.pop(resource_group_name, None)
        for store in (self.azure.machines, self.azure.public_ips):
            for key in [k for k, v in store.items() if v.resource_group == resource_group_name]:
                del store[key]
        return _Poller(None)


class _ComputeClient:
    def __init__(self, azure: FakeAzure) -> None:
        self.azure = azure

    def list_virtual_machine_sizes(self, location: str) -> list[_Object]:
        del location
        size = _Object(
            number_of_cores=2,
            memory_in_mb=4096,
            os_disk_size_in_mb=30720,
            resource_disk_size_in_mb=8192,
        )
        size.name = "Standard_B2s"
        return [size]

    def create_virtual_machine(self, **kwargs: Any) -> _Poller:
        name = kwargs["vm_name"]
        group = kwargs["resource_group_name"]
        machine = _Object(
            id=_arm_id(
                self.azure.subscription_id, group, "Microsoft.Compute", "virtualMachines", name
            ),
            name=name,
            location=kwargs["location"],
            resource_group=group,
            hardware_profile=_Object(vm_size=kwargs["size_name"]),
            instance_view=_Object(statuses=[_Object(code="PowerState/running")]),
            ssh_key=kwargs.get("ssh_key"),
            power_state="running",
        )
        self.azure.machines[machine.id] = machine
        return _Poller(machine)

    def get_virtual_machine(
        self, resource_group_name: str, vm_name: str, expand: Optional[str] = None
    ) -> _Object:
        del expand
        machine = self.azure.machines.get(
            _arm_id(
                self.azure.subscription_id,
                resource_group_name,
                "Microsoft.Compute",
                "virtualMachines",
                vm_name,
            )
        )
        if machine is None:
            raise _not_found(f"virtual machine {vm_name}")
        return machine

    def list_all_virtual_machines(self) -> list[_Object]:
        return list(self.azure.machines.values())

    def delete_virtual_machine(self, resource_group_name: str, vm_name: str) -> _Poller:
        self.azure.machines.pop(
            _arm_id(
                self.azure.subscription_id,
                resource_group_name,
                "Microsoft.Compute",
                "virtualMachines",
                vm_name,
            ),
            None,
        )
        return _Poller(None)

    def deallocate_virtual_machine(self, resource_group_name: str, vm_name: str) -> _Poller:
        machine = self.get_virtual_machine(resource_group_name, vm_name)
        machine.power_state = "deallocated"
        machine.instance_view = _Object(statuses=[_Object(code="PowerState/deallocated")])
        return _Poller(None)

    def start_virtual_machine(self, resource_group_name: str, vm_name: str) -> _Poller:
        machine = self.get_virtual_machine(resource_group_name, vm_name)
        machine.power_state = "running"
        machine.instance_view = _Object(statuses=[_Object(code="PowerState/running")])
        return _Poller(None)


class _NetworkClient:
    def __init__(self, azure: FakeAzure) -> None:
        self.azure = azure

    def create_network(self, location: str, group: str, name: str, cidr: str) -> _Poller:
        del location, cidr
        self.azure.networks.add(f"{group}/{name}")
        return _Poller(_Object(id=f"net/{group}/{name}", name=name))

    def create_subnet(self, group: str, network: str, name: str, cidr: str) -> _Poller:
        del cidr
        return _Poller(_Object(id=f"subnet/{group}/{network}/{name}", name=name))

    def create_public_ip(self, location: str, group: str, name: str) -> _Poller:
        del location
        address = _Object(
            id=f"ip/{group}/{name}",
            name=name,
            resource_group=group,
            ip_address="203.0.113.10",
        )
        self.azure.public_ips[f"{group}/{name}"] = address
        return _Poller(address)

    def get_public_ip(self, group: str, name: str) -> _Object:
        address = self.azure.public_ips.get(f"{group}/{name}")
        if address is None:
            raise _not_found(f"public ip {name}")
        return address

    def create_ssh_security_group(
        self, location: str, group: str, name: str, source_ranges: list[str]
    ) -> _Poller:
        del location
        self.azure.security_groups[f"{group}/{name}"] = list(source_ranges)
        return _Poller(_Object(id=f"nsg/{group}/{name}", name=name))

    def delete_network_security_group(self, group: str, name: str) -> _Poller:
        self.azure.security_groups.pop(f"{group}/{name}", None)
        return _Poller(None)

    def create_network_interface(
        self,
        location: str,
        group: str,
        name: str,
        config_name: str,
        subnet_id: str,
        public_ip_id: Optional[str] = None,
        security_group_id: Optional[str] = None,
    ) -> _Poller:
        del location, config_name, subnet_id, public_ip_id
        self.azure.interface_security_groups[f"{group}/{name}"] = security_group_id
        return _Poller(
            _Object(
                id=f"nic/{group}/{name}",
                name=name,
                ip_configurations=[_Object(private_ip_address="10.0.0.4")],
            )
        )

    def get_network_interface(self, group: str, name: str) -> _Object:
        return _Object(
            id=f"nic/{group}/{name}",
            name=name,
            ip_configurations=[_Object(private_ip_address="10.0.0.4")],
        )

    def delete_network_interface(self, group: str, name: str) -> _Poller:
        del group, name
        return _Poller(None)

    def delete_public_ip(self, group: str, name: str) -> _Poller:
        self.azure.public_ips.pop(f"{group}/{name}", None)
        return _Poller(None)

    def delete_subnet(self, group: str, network: str, name: str) -> _Poller:
        del group, network, name
        return _Poller(None)

    def delete_network(self, group: str, name: str) -> _Poller:
        self.azure.networks.discard(f"{group}/{name}")
        return _Poller(None)


def _not_found(what: str) -> Exception:
    from waldur_site_agent_azure.clients import AzureClientError

    return AzureClientError(f"{what} not found", status_code=404)
