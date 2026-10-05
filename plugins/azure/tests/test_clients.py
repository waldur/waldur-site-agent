"""How the service clients are wired and how Azure failures surface."""

from unittest import mock

import pytest
from azure.core.exceptions import HttpResponseError

from waldur_site_agent_azure.clients.base import _TranslatingPoller
from waldur_site_agent_azure.clients import (
    AzureClient,
    AzureClientError,
    ComputeClient,
    NetworkClient,
    ResourceClient,
)

CREDENTIALS = {
    "subscription_id": "sub-id",
    "tenant_id": "tenant-id",
    "client_id": "client-id",
    "client_secret": "client-secret",
}


@pytest.fixture
def client():
    return AzureClient(**CREDENTIALS)


def test_service_clients_share_one_credential_set(client):
    """One service principal, one credential object: re-authenticating per service
    would multiply token requests for no gain."""
    services = [client.resource, client.compute, client.network]
    assert {id(service.credentials) for service in services} == {id(client.credentials)}


@pytest.mark.parametrize(
    ("attribute", "expected_type"),
    [
        ("resource", ResourceClient),
        ("compute", ComputeClient),
        ("network", NetworkClient),
    ],
)
def test_each_service_is_exposed_once(client, attribute, expected_type):
    service = getattr(client, attribute)
    assert isinstance(service, expected_type)
    assert getattr(client, attribute) is service


def test_subscription_id_is_readable_without_touching_azure(client):
    """Diagnostics logs it, and must not trigger authentication to do so."""
    assert client.subscription_id == "sub-id"


def test_sdk_failures_become_client_errors(client):
    """An ``azure.core`` exception reaching the agent's core would not be
    recognised as a backend failure."""
    resource = client.resource
    resource.__dict__["subscription_client"] = mock.Mock()
    resource.subscription_client.subscriptions.get.side_effect = HttpResponseError(
        message="subscription not found"
    )

    with pytest.raises(AzureClientError, match="subscription not found"):
        resource.ping()


def test_ping_reports_unreachable_rather_than_raising(client):
    """``AzureBackend.ping`` asks a yes/no question; the poller decides what to do."""
    resource = client.resource
    resource.__dict__["subscription_client"] = mock.Mock()
    resource.subscription_client.subscriptions.get.side_effect = HttpResponseError(
        message="denied"
    )

    assert client.ping() is False


def test_ping_succeeds_when_the_subscription_is_readable(client):
    resource = client.resource
    resource.__dict__["subscription_client"] = mock.Mock()

    assert client.ping() is True
    resource.subscription_client.subscriptions.get.assert_called_once_with("sub-id")


def test_image_listing_translates_failures_raised_while_iterating(client):
    """The catalogue is paged lazily, so the failure arrives after the call that
    created the generator returned."""
    compute = client.compute
    compute.__dict__["compute_client"] = mock.Mock()
    images = compute.compute_client.virtual_machine_images
    images.list_publishers.side_effect = HttpResponseError(message="throttled")

    with pytest.raises(AzureClientError, match="throttled"):
        list(compute.list_virtual_machine_images("westeurope"))


def test_resource_group_locations_are_read_from_the_resources_provider(client):
    resource = client.resource
    resource.__dict__["resource_client"] = mock.Mock()
    provider = resource.resource_client.providers.get.return_value
    provider.resource_types = [
        mock.Mock(resource_type="deployments", locations=["northeurope"]),
        mock.Mock(resource_type="resourceGroups", locations=["westeurope"]),
    ]

    assert resource.get_resource_group_locations() == ["westeurope"]


def test_resource_group_locations_are_absent_when_the_provider_omits_them(client):
    """Returning an empty list here would read as "no regions available"."""
    resource = client.resource
    resource.__dict__["resource_client"] = mock.Mock()
    provider = resource.resource_client.providers.get.return_value
    provider.resource_types = [mock.Mock(resource_type="deployments", locations=["eu"])]

    assert resource.get_resource_group_locations() is None


def test_a_machine_disk_is_created_to_die_with_its_machine(client):
    """Azure keeps a managed disk when its machine goes unless the disk says
    otherwise, and the name it generated is not one the agent could find again."""
    compute = client.compute
    compute.__dict__["compute_client"] = mock.Mock()

    compute.create_virtual_machine(
        location="westeurope",
        resource_group_name="rg",
        vm_name="web",
        size_name="Standard_B1s",
        nic_id="nic",
        image_reference={"publisher": "p", "offer": "o", "sku": "s", "version": "v"},
        username="waldur",
        password="secret",
    )

    body = compute.compute_client.virtual_machines.begin_create_or_update.call_args[0][2]
    assert body["storage_profile"]["os_disk"]["delete_option"] == "Delete"


def test_a_failure_while_waiting_is_translated_too():
    """`begin_*` returns before Azure has done the work, so the failure usually
    arrives out of result(). Untranslated it would pass every handler that
    tolerates an object already gone."""
    poller = mock.Mock()
    poller.result.side_effect = HttpResponseError(message="gone")
    poller.result.side_effect.status_code = 404
    translating = _TranslatingPoller(poller)

    with pytest.raises(AzureClientError) as exc_info:
        translating.result()

    assert exc_info.value.not_found


def test_the_wrapped_poller_is_otherwise_the_poller():
    poller = mock.Mock(status="Succeeded")
    translating = _TranslatingPoller(poller)

    assert translating.status == "Succeeded"
    assert translating.result() is poller.result.return_value
