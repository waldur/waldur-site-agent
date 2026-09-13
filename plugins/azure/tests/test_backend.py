"""What the skeleton backend guarantees before any provisioning exists."""

from unittest import mock

import pytest

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_azure.backend import AzureBackend

VALID_SETTINGS = {
    "subscription_id": "sub-id",
    "tenant_id": "tenant-id",
    "client_id": "client-id",
    "client_secret": "client-secret",
}

COMPONENTS = {
    "cpu": {"measured_unit": "Cores", "unit_factor": 1, "accounting_type": "limit"},
    "ram": {"measured_unit": "MiB", "unit_factor": 1, "accounting_type": "limit"},
}


def test_backend_type_is_azure():
    backend = AzureBackend(VALID_SETTINGS, COMPONENTS)
    assert backend.backend_type == "azure"


def test_components_come_from_the_offering():
    backend = AzureBackend(VALID_SETTINGS, COMPONENTS)
    assert backend.list_components() == ["cpu", "ram"]


@pytest.mark.parametrize("missing", sorted(VALID_SETTINGS))
def test_every_credential_is_required(missing):
    """A partially configured offering must fail at construction.

    Deferring the failure to the first Azure call would surface it as a
    provisioning error on a user's order instead of a configuration error.
    """
    settings = {key: value for key, value in VALID_SETTINGS.items() if key != missing}
    with pytest.raises(BackendError) as exc_info:
        AzureBackend(settings, COMPONENTS)
    assert missing in str(exc_info.value)


@pytest.mark.parametrize(
    ("action", "arguments"),
    [
        ("create_resource", ("name", "description", "organization")),
        ("delete_resource", ("name",)),
        ("set_resource_limits", ("resource-id", {"cpu": 1})),
        ("get_usage_report", (["resource-id"],)),
    ],
)
def test_the_core_client_refuses_what_it_cannot_do(action, arguments):
    """The inherited client answers these with empty values, and the core reads
    that as "the resource is gone" — for Azure, a second machine on the bill."""
    backend = AzureBackend(VALID_SETTINGS, COMPONENTS)
    with pytest.raises(NotImplementedError):
        getattr(backend.client, action)(*arguments)


def test_membership_calls_do_nothing_rather_than_fail():
    """The core calls these after every create and on every membership sync
    pass. A machine has no membership to change -- access is the SSH key the
    order carried -- so failing here would log an error per resource per pass
    and change nothing."""
    backend = AzureBackend(VALID_SETTINGS, COMPONENTS)

    assert backend.client.get_association("alice", "resource-id") is None
    assert backend.client.create_association("alice", "resource-id") == "alice"
    assert backend.client.delete_association("alice", "resource-id") == "alice"
    assert backend.client.list_resource_users("resource-id") == []


def test_machines_are_listed_by_the_id_waldur_stores():
    """The import check compares this against Waldur's backend_id, which holds
    the ARM id; reporting names instead makes every managed machine look new."""
    backend = AzureBackend(VALID_SETTINGS, COMPONENTS)
    arm_id = (
        "/subscriptions/sub-id/resourceGroups/group-ab12-web"
        "/providers/Microsoft.Compute/virtualMachines/web"
    )
    backend.azure_client.compute.list_all_virtual_machines = mock.Mock(
        return_value=[mock.Mock(id=arm_id, name="web")]
    )

    assert [r.backend_id for r in backend.list_resources()] == [arm_id]
