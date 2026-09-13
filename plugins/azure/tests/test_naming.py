"""Names derived from a resource, and identifiers read back from Azure."""

import pytest

from waldur_site_agent_azure.naming import (
    VirtualMachineNames,
    is_dedicated_resource_group,
    parse_virtual_machine_id,
    sanitize_name,
)

VM_ID = (
    "/subscriptions/sub-id/resourceGroups/group-ab12-web-server"
    "/providers/Microsoft.Compute/virtualMachines/web-server"
)


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("Web Server", "web-server"),
        ("Проект", "waldur-vm"),
        ("--weird--name--", "weird-name"),
        ("", "waldur-vm"),
        ("a" * 80, "a" * 50),
    ],
)
def test_names_are_reduced_to_what_azure_accepts(given, expected):
    """Azure rejects a bad name outright, and the user sees it as a failed order."""
    assert sanitize_name(given) == expected


def test_derived_names_follow_the_machine():
    names = VirtualMachineNames.build("web server", resource_group="shared")
    assert names.virtual_machine == "web-server"
    assert names.resource_group == "shared"
    assert names.network == "netweb-server"
    assert names.subnet == "subnetweb-server"
    assert names.network_interface == "nicweb-server"
    assert names.ip_configuration == "ipconfweb-server"
    assert names.public_ip == "pubipweb-server"


def test_a_machine_without_a_shared_group_gets_its_own():
    """Resource group names are unique per subscription, and two projects may
    well ask for the same machine name."""
    first = VirtualMachineNames.build("web")
    second = VirtualMachineNames.build("web")
    assert first.resource_group != second.resource_group
    assert first.resource_group.endswith("-web")


def test_the_resource_group_is_read_back_from_the_id():
    """The group is read from the id, never derived again, so any group name
    round-trips."""
    vm_id = parse_virtual_machine_id(VM_ID)
    assert vm_id.subscription_id == "sub-id"
    assert vm_id.resource_group == "group-ab12-web-server"
    assert vm_id.name == "web-server"
    assert vm_id.names.public_ip == "pubipweb-server"
    assert vm_id.names.resource_group == "group-ab12-web-server"


@pytest.mark.parametrize(
    "backend_id",
    [
        "",
        "web-server",
        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/publicIPAddresses/ip",
        "/subscriptions/sub/providers/Microsoft.Compute/virtualMachines/vm",
    ],
)
def test_anything_that_is_not_a_machine_id_is_rejected(backend_id):
    with pytest.raises(ValueError, match="Not an Azure virtual machine id"):
        parse_virtual_machine_id(backend_id)


def test_a_long_name_survives_the_trip_through_the_arm_id():
    """The names a delete addresses are derived from the id Azure returned. A
    machine name is trimmed once, on the way in; trimming it again on the way
    back would address a machine that does not exist, and a delete that finds
    nothing reports success while the machine keeps running."""
    created = VirtualMachineNames.build(
        "a" * 55, resource_group="shared-ops", unique_suffix="1234abcd"
    )
    arm_id = (
        f"/subscriptions/sub-id/resourceGroups/shared-ops"
        f"/providers/Microsoft.Compute/virtualMachines/{created.virtual_machine}"
    )

    assert parse_virtual_machine_id(arm_id).names == created


def test_a_dedicated_group_is_named_from_the_resource_not_from_chance():
    """A retried order must derive the group the killed attempt created,
    otherwise the first machine bills under a name nothing points at."""
    first = VirtualMachineNames.build("web", unique_suffix="1234abcd")
    retry = VirtualMachineNames.build("web", unique_suffix="1234abcd")

    assert first == retry
    assert first.resource_group == "group-1234-web"
    assert is_dedicated_resource_group(first.resource_group, first.virtual_machine)


def test_a_resource_without_a_uuid_still_gets_a_group_of_its_own():
    """Uniqueness is what the group name cannot do without; findability is what
    a resource with no uuid cannot have."""
    names = VirtualMachineNames.build("web", unique_suffix=None)

    assert is_dedicated_resource_group(names.resource_group, names.virtual_machine)
