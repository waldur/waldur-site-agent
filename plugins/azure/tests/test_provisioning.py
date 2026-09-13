"""Creating, deleting and inspecting a virtual machine."""

from unittest import mock
from uuid import UUID

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_azure.clients import AzureClientError
from waldur_site_agent_azure.backend import AzureBackend

CREDENTIALS = {
    "subscription_id": "sub-id",
    "tenant_id": "tenant-id",
    "client_id": "client-id",
    "client_secret": "client-secret",
}
COMPONENTS = {"cpu": {"measured_unit": "Cores", "unit_factor": 1, "accounting_type": "limit"}}

VM_ID = (
    "/subscriptions/sub-id/resourceGroups/group-ab12-web"
    "/providers/Microsoft.Compute/virtualMachines/web"
)
# A machine in a group the operator named: the group is not ours to delete.
SHARED_VM_ID = (
    "/subscriptions/sub-id/resourceGroups/shared"
    "/providers/Microsoft.Compute/virtualMachines/web-1234abcd"
)
RESOURCE_UUID = UUID("1234abcd5678efab9012cdef34567890")
DEFAULT_SSH_KEY = "ssh-ed25519 AAAAdefault"


def make_backend(**settings):
    backend = AzureBackend({**CREDENTIALS, **settings}, COMPONENTS)
    backend.azure_client = mock.Mock()
    backend.client.azure_client = backend.azure_client
    return backend


def poller(**attributes):
    """Azure returns a poller from every begin_* call; `.result()` is the object."""
    result = mock.Mock(**attributes)
    return mock.Mock(**{"result.return_value": result})


def waldur_resource(name="web", backend_id="", attributes=None, offering_type="Azure.VirtualMachine"):
    resource = mock.Mock()
    resource.name = name
    resource.slug = name
    resource.offering_type = offering_type
    resource.backend_id = backend_id
    resource.uuid = RESOURCE_UUID
    resource.limits = None
    # An order without a key is refused, so the default here is an order that
    # can actually be provisioned; the tests about the key say so themselves.
    resource.attributes = {"ssh_public_key": DEFAULT_SSH_KEY} if attributes is None else attributes
    return resource


@pytest.fixture
def backend():
    backend = make_backend(
        default_location="westeurope",
        default_size="Standard_B1s",
        default_image="Canonical:jammy:22_04-lts:latest",
    )
    network = backend.azure_client.network
    network.create_network.return_value = poller(id="net-id")
    network.create_subnet.return_value = poller(id="subnet-id")
    network.create_public_ip.return_value = poller(id="ip-id", ip_address="203.0.113.10")
    network.create_network_interface.return_value = poller(
        id="nic-id", ip_configurations=[mock.Mock(private_ip_address="10.0.0.4")]
    )
    backend.azure_client.compute.create_virtual_machine.return_value = poller(
        # Creation does not expand the instance view, so the power state is
        # absent until the resource is pulled.
        id=VM_ID,
        name="web",
        location="westeurope",
        instance_view=None,
    )
    return backend


def test_the_machine_is_built_in_dependency_order(backend):
    """Each call consumes the id the previous one returned; a different order
    would ask Azure to attach an interface to a subnet that does not exist."""
    backend.create_resource(waldur_resource())

    network = backend.azure_client.network
    backend.azure_client.resource.create_resource_group.assert_called_once()
    _, group_name = backend.azure_client.resource.create_resource_group.call_args[0]
    assert group_name.endswith("-web")

    network.create_subnet.assert_called_once_with(group_name, "netweb", "subnetweb", "10.0.0.0/24")
    nic_args = network.create_network_interface.call_args
    assert nic_args[0][4] == "subnet-id"
    assert nic_args[1]["public_ip_id"] == "ip-id"
    assert backend.azure_client.compute.create_virtual_machine.call_args[1]["nic_id"] == "nic-id"


def test_creation_returns_the_arm_id_and_the_addresses(backend):
    info = backend.create_resource(waldur_resource())

    assert info.backend_id == VM_ID
    assert info.backend_metadata["virtual_machine"]["public_ip"] == "203.0.113.10"


def test_the_configured_ranges_are_used(backend):
    backend.network_cidr = "192.168.0.0/16"
    backend.subnet_cidr = "192.168.1.0/24"
    backend.create_resource(waldur_resource())

    assert backend.azure_client.network.create_network.call_args[0][3] == "192.168.0.0/16"
    assert backend.azure_client.network.create_subnet.call_args[0][3] == "192.168.1.0/24"


def test_an_order_can_override_the_offering_defaults(backend):
    backend.create_resource(
        waldur_resource(
            attributes={
                "location": "northeurope",
                "size": "Standard_D2s_v3",
                "image": "Debian:debian-12:12:latest",
                "ssh_public_key": DEFAULT_SSH_KEY,
            }
        )
    )

    call = backend.azure_client.compute.create_virtual_machine.call_args[1]
    assert call["location"] == "northeurope"
    assert call["size_name"] == "Standard_D2s_v3"
    assert call["image_reference"] == {
        "publisher": "Debian",
        "offer": "debian-12",
        "sku": "12",
        "version": "latest",
    }


def test_a_machine_without_a_location_is_refused():
    """Azure would reject it too, but only after the resource group exists."""
    backend = make_backend(default_size="Standard_B1s", default_image="a:b:c:d")

    with pytest.raises(BackendError, match="location"):
        backend.create_resource(waldur_resource())

    backend.azure_client.resource.create_resource_group.assert_not_called()


def test_a_malformed_image_is_refused(backend):
    with pytest.raises(BackendError, match="publisher:offer:sku:version"):
        backend.create_resource(waldur_resource(attributes={"image": "ubuntu"}))


def test_an_ssh_key_uuid_is_resolved_against_the_provider_keys(backend):
    backend.create_resource(
        waldur_resource(attributes={"ssh_key": "key-uuid"}),
        user_context={"ssh_keys": {"key-uuid": "ssh-ed25519 AAAA"}},
    )

    assert backend.azure_client.compute.create_virtual_machine.call_args[1]["ssh_key"] == (
        "ssh-ed25519 AAAA"
    )


def test_an_order_without_a_key_is_refused_before_anything_is_created(backend):
    """The generated password is reported to no one, so a machine provisioned
    without a key would bill and admit nobody."""
    with pytest.raises(BackendError, match="no SSH key"):
        backend.create_resource(waldur_resource(attributes={}))

    backend.azure_client.resource.create_resource_group.assert_not_called()
    backend.azure_client.compute.create_virtual_machine.assert_not_called()


def test_an_unknown_key_uuid_is_refused_like_a_missing_one(backend):
    """A key the provider has not registered leaves the machine just as
    unreachable as no key at all."""
    with pytest.raises(BackendError, match="no SSH key"):
        backend.create_resource(
            waldur_resource(attributes={"ssh_key": "missing"}), user_context={"ssh_keys": {}}
        )

    backend.azure_client.resource.create_resource_group.assert_not_called()


def test_the_generated_password_is_never_reported(backend):
    info = backend.create_resource(waldur_resource())
    password = backend.azure_client.compute.create_virtual_machine.call_args[1]["password"]

    assert password not in str(info.backend_metadata)


def test_a_dedicated_group_is_deleted_whole(backend):
    """It holds nothing but this machine, and one call takes the network with it."""
    backend.delete_resource(waldur_resource(backend_id=VM_ID))

    backend.azure_client.resource.delete_resource_group.assert_called_once_with(
        "group-ab12-web"
    )
    backend.azure_client.compute.delete_virtual_machine.assert_not_called()


def test_in_a_shared_group_each_object_goes_separately(backend):
    """Dropping an operator's group would take resources the agent never made."""
    backend.default_resource_group = "shared"
    network = backend.azure_client.network

    backend.delete_resource(waldur_resource(backend_id=SHARED_VM_ID))

    backend.azure_client.resource.delete_resource_group.assert_not_called()
    backend.azure_client.compute.delete_virtual_machine.assert_called_once_with(
        "shared", "web-1234abcd"
    )
    network.delete_network_interface.assert_called_once_with("shared", "nicweb-1234abcd")
    network.delete_public_ip.assert_called_once_with("shared", "pubipweb-1234abcd")
    network.delete_subnet.assert_called_once_with(
        "shared", "netweb-1234abcd", "subnetweb-1234abcd"
    )
    network.delete_network.assert_called_once_with("shared", "netweb-1234abcd")


def test_the_delete_route_follows_the_id_not_the_current_setting(backend):
    """An operator clearing default_resource_group after provisioning would
    otherwise have their shared group deleted whole, with everything in it."""
    backend.default_resource_group = None

    backend.delete_resource(waldur_resource(backend_id=SHARED_VM_ID))

    backend.azure_client.resource.delete_resource_group.assert_not_called()
    backend.azure_client.compute.delete_virtual_machine.assert_called_once()


def test_a_group_of_our_own_is_dropped_even_once_a_shared_one_is_configured(backend):
    """The mirror case: the group holds one machine and nothing else."""
    backend.default_resource_group = "shared"

    backend.delete_resource(waldur_resource(backend_id=VM_ID))

    backend.azure_client.resource.delete_resource_group.assert_called_once_with(
        "group-ab12-web"
    )


def test_machines_in_a_shared_group_are_told_apart_by_the_resource(backend):
    """Two names that sanitize alike would otherwise address one machine, and the
    second order would reconfigure the first instead of creating its own."""
    backend.default_resource_group = "shared"

    backend.create_resource(waldur_resource(name="Проект"))

    created = backend.azure_client.compute.create_virtual_machine.call_args[1]["vm_name"]
    assert created == "waldur-vm-1234abcd"


def test_deleting_a_resource_that_was_never_created_is_not_an_error(backend):
    backend.delete_resource(waldur_resource(backend_id=""))

    backend.azure_client.resource.delete_resource_group.assert_not_called()


def test_a_backend_id_that_is_not_a_machine_is_reported(backend):
    with pytest.raises(BackendError, match="Not an Azure virtual machine id"):
        backend.delete_resource(waldur_resource(backend_id="droplet-42"))


def test_metadata_reports_the_power_state_not_the_provisioning_state(backend):
    backend.azure_client.compute.get_virtual_machine.return_value = mock.Mock(
        id=VM_ID,
        name="web",
        location="westeurope",
        hardware_profile=mock.Mock(vm_size="Standard_B1s"),
        instance_view=mock.Mock(
            statuses=[
                mock.Mock(code="ProvisioningState/succeeded"),
                mock.Mock(code="PowerState/running"),
            ]
        ),
    )
    backend.azure_client.network.get_public_ip.return_value = mock.Mock(
        ip_address="203.0.113.10"
    )
    backend.azure_client.network.get_network_interface.return_value = mock.Mock(
        ip_configurations=[mock.Mock(private_ip_address="10.0.0.4")]
    )

    metadata = backend.get_resource_metadata(VM_ID)["virtual_machine"]

    assert metadata["power_state"] == "running"
    assert metadata["public_ip"] == "203.0.113.10"
    assert metadata["private_ip"] == "10.0.0.4"
    assert metadata["size"] == "Standard_B1s"


def test_metadata_survives_a_machine_without_a_public_address(backend):
    """A deleted or never-created address is absence, not a failure to report."""
    backend.azure_client.compute.get_virtual_machine.return_value = mock.Mock(
        id=VM_ID, name="web", location="westeurope", instance_view=None
    )
    backend.azure_client.network.get_public_ip.side_effect = AzureClientError(
        "gone", status_code=404
    )
    backend.azure_client.network.get_network_interface.side_effect = AzureClientError(
        "gone", status_code=404
    )

    metadata = backend.get_resource_metadata(VM_ID)["virtual_machine"]

    assert metadata["public_ip"] is None
    assert metadata["private_ip"] is None
    assert metadata["power_state"] is None


def test_metadata_does_not_hide_a_broken_connection(backend):
    """Reporting "no address" because Azure would not answer is a lie."""
    backend.azure_client.compute.get_virtual_machine.return_value = mock.Mock(instance_view=None)
    backend.azure_client.network.get_public_ip.side_effect = AzureClientError(
        "service unavailable", status_code=503
    )

    with pytest.raises(AzureClientError):
        backend.get_resource_metadata(VM_ID)


@pytest.mark.parametrize("action", ["pause_resource", "downscale_resource"])
def test_pausing_deallocates_the_machine(backend, action):
    """A powered-off machine still holds its host and still bills for it; only
    deallocation stops the compute charge these two actions exist for."""
    assert getattr(backend, action)(VM_ID) is True
    backend.azure_client.compute.deallocate_virtual_machine.assert_called_with(
        "group-ab12-web", "web"
    )


def test_restoring_powers_the_machine_on(backend):
    assert backend.restore_resource(VM_ID) is True
    backend.azure_client.compute.start_virtual_machine.assert_called_once_with(
        "group-ab12-web", "web"
    )


def test_the_processor_entry_point_provisions(backend):
    """The order processor calls create_resource_with_id, never create_resource.

    Testing only the latter is what let the plugin ship a provisioning path the
    agent never reached: the base implementation goes through
    ``client.create_resource``, which this plugin refuses.
    """
    info = backend.create_resource_with_id(waldur_resource(), "suggested-id")

    assert info.backend_id == VM_ID
    backend.azure_client.compute.create_virtual_machine.assert_called_once()


def test_the_suggested_backend_id_is_not_used(backend):
    """Azure names its own resources; keeping the suggestion would put an id in
    Waldur that addresses nothing."""
    info = backend.create_resource_with_id(waldur_resource(), "e2e-measur")

    assert info.backend_id != "e2e-measur"


def test_a_missing_resource_is_not_recreated(backend):
    """A new machine gets a new ARM id, so Waldur would keep pointing at the old
    one while the new machine runs up a bill."""
    resource = waldur_resource(backend_id=VM_ID)

    assert backend.recreate_missing_resource(resource) is False
    backend.azure_client.compute.create_virtual_machine.assert_not_called()


def test_ssh_is_opened_to_the_configured_ranges(backend):
    """A Standard-SKU public address admits nothing on its own, so without a
    security group the machine is unreachable by its only route."""
    backend.allowed_ssh_ranges = ["203.0.113.0/24", "198.51.100.7"]
    backend.azure_client.network.create_ssh_security_group.return_value = poller(id="nsg-id")

    backend.create_resource(waldur_resource())

    network = backend.azure_client.network
    location, group, name, ranges = network.create_ssh_security_group.call_args[0]
    assert (location, name) == ("westeurope", "nsgweb")
    assert ranges == ["203.0.113.0/24", "198.51.100.7"]
    assert network.create_network_interface.call_args[1]["security_group_id"] == "nsg-id"


def test_without_configured_ranges_no_security_group_is_made(backend):
    """A machine that names no ranges accepts no SSH connections."""
    backend.allowed_ssh_ranges = []

    backend.create_resource(waldur_resource())

    backend.azure_client.network.create_ssh_security_group.assert_not_called()
    assert (
        backend.azure_client.network.create_network_interface.call_args[1]["security_group_id"]
        is None
    )


def test_the_security_group_goes_with_the_machine_in_a_shared_group(backend):
    backend.default_resource_group = "shared"

    backend.delete_resource(waldur_resource(backend_id=SHARED_VM_ID))

    backend.azure_client.network.delete_network_security_group.assert_called_once_with(
        "shared", "nsgweb-1234abcd"
    )


def test_a_machine_made_before_ssh_was_configured_still_deletes(backend):
    """It has no security group, and a 404 for one must not strand the rest."""
    backend.default_resource_group = "shared"
    backend.azure_client.network.delete_network_security_group.side_effect = AzureClientError(
        "gone", status_code=404
    )

    backend.delete_resource(waldur_resource(backend_id=SHARED_VM_ID))

    backend.azure_client.network.delete_network.assert_called_once()


def test_a_failed_chain_takes_its_own_group_with_it(backend):
    """Waldur records a machine's id only once the machine exists, so anything a
    failed chain left behind would bill with nothing in Waldur pointing at it."""
    backend.azure_client.compute.create_virtual_machine.side_effect = AzureClientError(
        "size not available in this region", status_code=409
    )

    with pytest.raises(AzureClientError):
        backend.create_resource(waldur_resource())

    deleted = backend.azure_client.resource.delete_resource_group.call_args[0][0]
    created = backend.azure_client.resource.create_resource_group.call_args[0][1]
    assert deleted == created


def test_a_failed_chain_in_a_shared_group_removes_only_its_own_objects(backend):
    backend.default_resource_group = "shared"
    backend.azure_client.network.create_network_interface.side_effect = AzureClientError(
        "quota exceeded", status_code=409
    )

    with pytest.raises(AzureClientError):
        backend.create_resource(waldur_resource())

    backend.azure_client.resource.delete_resource_group.assert_not_called()
    backend.azure_client.network.delete_public_ip.assert_called_once()
    backend.azure_client.network.delete_network.assert_called_once()


def test_rollback_tolerates_what_was_never_created(backend):
    """Most of the chain does not exist when an early step fails."""
    backend.default_resource_group = "shared"
    backend.azure_client.network.create_network.side_effect = AzureClientError(
        "address space overlaps", status_code=400
    )
    backend.azure_client.network.delete_public_ip.side_effect = AzureClientError(
        "gone", status_code=404
    )

    with pytest.raises(AzureClientError):
        backend.create_resource(waldur_resource())

    backend.azure_client.network.delete_network.assert_called_once()
