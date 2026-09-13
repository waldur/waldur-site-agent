"""Orders placed in Waldur, carried through the agent, landing on Azure."""

from __future__ import annotations

import logging

from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.models.order_state import OrderState

from .conftest import (
    VM_OFFERING_UUID,
    create_order,
    process_orders,
    resource_of_order,
)

logger = logging.getLogger(__name__)


def test_an_order_provisions_a_machine_and_reports_it_back(
    vm_offering, waldur_client, azure, make_backend
):
    """The whole path: order in Waldur, machine on Azure, backend id on the
    resource. None of it is exercised by the unit tests, which start at the
    backend and stop at the SDK."""
    backend = make_backend(vm_offering)
    order_uuid = create_order(waldur_client, VM_OFFERING_UUID, name="e2e-machine")

    process_orders(vm_offering, waldur_client, backend)

    assert len(azure.machines) == 1
    machine = next(iter(azure.machines.values()))
    assert machine.name == "e2e-machine"

    resource = resource_of_order(waldur_client, order_uuid)
    assert resource.get("backend_id") == machine.id
    metadata = resource.get("backend_metadata") or {}
    assert metadata.get("virtual_machine", {}).get("public_ip") == "203.0.113.10"


def test_ssh_is_opened_and_attached_to_the_interface(
    vm_offering, waldur_client, azure, make_backend
):
    """A security group nobody attached leaves the machine as unreachable as no
    security group at all."""
    backend = make_backend(vm_offering)
    create_order(waldur_client, VM_OFFERING_UUID, name="e2e-reachable")

    process_orders(vm_offering, waldur_client, backend)

    assert list(azure.security_groups.values()) == [["203.0.113.0/24"]]
    attached = [value for value in azure.interface_security_groups.values() if value]
    assert attached, "the interface was created without a security group"


def test_the_machine_gets_its_own_resource_group(
    vm_offering, waldur_client, azure, make_backend
):
    backend = make_backend(vm_offering)
    create_order(waldur_client, VM_OFFERING_UUID, name="e2e-grouped")

    process_orders(vm_offering, waldur_client, backend)

    assert [group for group in azure.resource_groups if group.endswith("-e2e-grouped")]


def test_order_attributes_reach_the_backend(vm_offering, waldur_client, azure, make_backend):
    """The offering defaults are overridden per order, which only holds if the
    attributes survive the trip through Waldur."""
    backend = make_backend(vm_offering)
    create_order(
        waldur_client,
        VM_OFFERING_UUID,
        name="e2e-sized",
        attributes={"ssh_public_key": "ssh-ed25519 AAAAE2E"},
    )

    process_orders(vm_offering, waldur_client, backend)

    machine = next(m for m in azure.machines.values() if m.name == "e2e-sized")
    assert machine.ssh_key == "ssh-ed25519 AAAAE2E"


def test_an_order_without_a_key_creates_nothing(
    vm_offering, waldur_client, azure, make_backend
):
    """A machine has no credential but the key, so the order fails instead of
    handing over something nobody can reach."""
    backend = make_backend(vm_offering)
    order_uuid = create_order(
        waldur_client, VM_OFFERING_UUID, name="e2e-keyless", attributes={"ssh_public_key": ""}
    )

    process_orders(vm_offering, waldur_client, backend)

    assert azure.machines == {}
    assert azure.resource_groups == {}
    order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
    assert order.state == OrderState.ERRED


def test_terminating_an_order_removes_the_machine(
    vm_offering, waldur_client, azure, make_backend
):
    backend = make_backend(vm_offering)
    order_uuid = create_order(waldur_client, VM_OFFERING_UUID, name="e2e-doomed")
    process_orders(vm_offering, waldur_client, backend)
    resource = resource_of_order(waldur_client, order_uuid)

    response = waldur_client.get_httpx_client().post(
        f"/api/marketplace-provider-resources/{resource['uuid']}/terminate/"
    )
    response.raise_for_status()
    process_orders(vm_offering, waldur_client, backend)

    assert azure.machines == {}
    assert azure.resource_groups == {}


def test_usage_reaches_waldur(vm_offering, waldur_client, azure, make_backend):
    """Allocation figures are the plugin's whole reporting story, and the pull
    path is where they are written."""
    backend = make_backend(vm_offering)
    order_uuid = create_order(waldur_client, VM_OFFERING_UUID, name="e2e-measured")
    process_orders(vm_offering, waldur_client, backend)
    resource = resource_of_order(waldur_client, order_uuid)

    report = backend._get_usage_report([resource["backend_id"]])  # noqa: SLF001

    assert report[resource["backend_id"]]["TOTAL_ACCOUNT_USAGE"] == {
        "cpu": 2,
        "ram": 4096,
        "disk": 38912,
    }
