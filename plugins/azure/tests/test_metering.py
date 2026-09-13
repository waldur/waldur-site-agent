"""What the agent reports a resource holds, and when it reports nothing."""

from unittest import mock

import pytest

from waldur_site_agent_azure.clients import AzureClientError
from waldur_site_agent_azure.metering import TOTAL_USAGE, AllocationMeter

VM_ID = (
    "/subscriptions/sub-id/resourceGroups/group-ab12-web"
    "/providers/Microsoft.Compute/virtualMachines/web"
)

COMPONENTS = {
    "cpu": {"measured_unit": "Cores", "accounting_type": "limit"},
    "ram": {"measured_unit": "MiB", "accounting_type": "limit"},
    "disk": {"measured_unit": "MiB", "accounting_type": "limit"},
}


def size(cores=2, ram=4096, os_disk=30720, resource_disk=8192):
    return mock.Mock(
        number_of_cores=cores,
        memory_in_mb=ram,
        os_disk_size_in_mb=os_disk,
        resource_disk_size_in_mb=resource_disk,
    )


@pytest.fixture
def azure_client():
    client = mock.Mock()
    client.compute.get_virtual_machine.return_value = mock.Mock(
        location="westeurope", hardware_profile=mock.Mock(vm_size="Standard_B2s")
    )
    standard_b2s = size()
    standard_b2s.name = "Standard_B2s"
    client.compute.list_virtual_machine_sizes.return_value = [standard_b2s]
    return client


def test_a_machine_is_reported_as_the_shape_it_runs_at(azure_client):
    meter = AllocationMeter(azure_client, COMPONENTS)

    report = meter.report([VM_ID])

    assert report == {VM_ID: {TOTAL_USAGE: {"cpu": 2, "ram": 4096, "disk": 38912}}}


def test_both_disks_count_towards_the_machine_disk(azure_client):
    """The OS disk and the temporary resource disk are both occupied by the
    machine."""
    meter = AllocationMeter(azure_client, {"disk": {}})

    assert meter.report([VM_ID])[VM_ID][TOTAL_USAGE]["disk"] == 30720 + 8192


def test_component_names_are_converted_to_waldur_units(azure_client):
    meter = AllocationMeter(azure_client, {"ram": {"unit_factor": 1024}})

    assert meter.report([VM_ID])[VM_ID][TOTAL_USAGE]["ram"] == 4


def test_an_explicit_backend_name_beats_the_guess(azure_client):
    """An offering whose component is called something else entirely still gets
    a figure."""
    meter = AllocationMeter(azure_client, {"processors": {"backend_name": "cores"}})

    assert meter.report([VM_ID])[VM_ID][TOTAL_USAGE] == {"processors": 2}


def test_sizes_are_read_once_per_region(azure_client):
    meter = AllocationMeter(azure_client, COMPONENTS)

    meter.report([VM_ID, VM_ID, VM_ID])

    azure_client.compute.list_virtual_machine_sizes.assert_called_once_with("westeurope")


def test_a_size_the_region_no_longer_offers_is_left_out(azure_client):
    """Reported as zero, it would read as a machine that holds nothing."""
    azure_client.compute.list_virtual_machine_sizes.return_value = []
    meter = AllocationMeter(azure_client, COMPONENTS)

    assert meter.report([VM_ID]) == {}


def test_a_resource_azure_will_not_talk_about_is_left_out(azure_client):
    azure_client.compute.get_virtual_machine.side_effect = AzureClientError(
        "service unavailable", status_code=503
    )
    meter = AllocationMeter(azure_client, COMPONENTS)

    assert meter.report([VM_ID]) == {}


def test_a_deleted_resource_is_left_out(azure_client):
    azure_client.compute.get_virtual_machine.side_effect = AzureClientError(
        "gone", status_code=404
    )
    meter = AllocationMeter(azure_client, COMPONENTS)

    assert meter.report([VM_ID]) == {}


def test_an_id_from_another_backend_is_left_out(azure_client):
    meter = AllocationMeter(azure_client, COMPONENTS)

    assert meter.report(["droplet-42"]) == {}


def test_a_component_azure_says_nothing_about_is_absent(azure_client):
    """Not zero: the offering meters something this plugin cannot see."""
    meter = AllocationMeter(azure_client, {"gpu": {}, "cpu": {}})

    assert meter.report([VM_ID])[VM_ID][TOTAL_USAGE] == {"cpu": 2}
