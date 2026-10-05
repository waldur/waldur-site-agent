"""Fixtures for the Azure E2E tests.

These drive the real ``OfferingOrderProcessor`` and ``OfferingReportProcessor``
against a real Waldur instance, with the Azure SDK replaced by ``FakeAzure``.
What they cover is the half that unit tests cannot reach: whether an order
reaches the plugin with the attributes it expects, whether the backend id and
metadata it returns land on the Waldur resource, and whether usage and
credentials make it back.

Environment variables:
    WALDUR_E2E_TESTS=true            - Gate: skip everything if not set
    WALDUR_E2E_CONFIG=<path>         - Path to the agent config YAML
    WALDUR_E2E_PROJECT_A_UUID=<uuid> - Project the orders are placed in
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import pytest
from waldur_api_client.api.marketplace_orders import marketplace_orders_create
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.order_create_request import OrderCreateRequest

from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.common.utils import get_client, load_configuration

from .fake_azure import FakeAzure

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_CONFIG", "")
E2E_PROJECT_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

DEFAULT_SSH_KEY = "ssh-ed25519 AAAAE2Edefault"
VM_OFFERING_UUID = "e2ef0000000000000000000000000201"

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS is not set")


@pytest.fixture(scope="session")
def config():
    """Load the agent configuration the tests share with the CI job."""
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-azure")


def _offering(config, uuid: str):
    for offering in config.offerings:
        if offering.waldur_offering_uuid == uuid:
            return offering
    pytest.skip(f"Offering {uuid} is not in {E2E_CONFIG_PATH}")
    return None


@pytest.fixture(scope="session")
def vm_offering(config):
    """The offering whose orders create virtual machines."""
    return _offering(config, VM_OFFERING_UUID)


@pytest.fixture(scope="session")
def waldur_client(vm_offering):
    """Authenticated client for the Waldur instance under test."""
    return get_client(vm_offering.waldur_api_url, vm_offering.waldur_api_token)


@pytest.fixture
def azure():
    """The fake subscription every backend in a test shares."""
    return FakeAzure("e2e-subscription", "tenant", "client", "secret")


@pytest.fixture
def make_backend(azure, monkeypatch):
    """Build the backend exactly as the agent does, with the SDK faked.

    Through the core's own factory rather than by calling ``AzureBackend``
    directly: constructing it by hand is how the first version of this fixture
    passed component objects where the agent passes dictionaries, and the
    difference only surfaced as an AttributeError deep in metering.
    """
    from waldur_site_agent_azure import backend as backend_module

    from waldur_site_agent.common.utils import get_backend_for_offering

    def factory(offering):
        monkeypatch.setattr(backend_module, "AzureClient", lambda **kwargs: azure)
        instance, _ = get_backend_for_offering(offering, "order_processing_backend")
        return instance

    return factory


def project_url(client) -> str:
    """Return the URL of the project the orders are placed in."""
    if not E2E_PROJECT_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    response = client.get_httpx_client().get(f"/api/projects/{E2E_PROJECT_UUID}/")
    response.raise_for_status()
    return response.json()["url"]


def offering_urls(client, offering_uuid: str) -> tuple[str, str]:
    """Return the offering and plan URLs an order has to reference."""
    response = client.get_httpx_client().get(
        f"/api/marketplace-public-offerings/{offering_uuid}/"
    )
    response.raise_for_status()
    data = response.json()
    plans = data.get("plans", [])
    if not plans:
        msg = f"No plans on offering {offering_uuid}"
        raise RuntimeError(msg)
    return data["url"], plans[0]["url"]


def create_order(
    client,
    offering_uuid: str,
    name: str,
    attributes: Optional[dict] = None,
) -> str:
    """Place a CREATE order and return its UUID.

    An order carries an SSH key unless the caller says otherwise: without one the
    backend refuses the order, which is its own test rather than the setup of
    every other one.
    """
    offering_url, plan_url = offering_urls(client, offering_uuid)
    order_attributes = GenericOrderAttributes()
    order_attributes["name"] = name
    order_attributes["ssh_public_key"] = DEFAULT_SSH_KEY
    for key, value in (attributes or {}).items():
        order_attributes[key] = value

    order = marketplace_orders_create.sync(
        client=client,
        body=OrderCreateRequest(
            offering=offering_url,
            project=project_url(client),
            plan=plan_url,
            attributes=order_attributes,
        ),
    )
    order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
    logger.info("Created order %s on offering %s", order_uuid, offering_uuid)
    return order_uuid


def process_orders(offering, client, backend) -> None:
    """Run one order-processing cycle, as the agent would."""
    OfferingOrderProcessor(
        offering=offering, waldur_rest_client=client, resource_backend=backend
    ).process_offering()


def resource_of_order(client, order_uuid: str) -> dict:
    """Return the resource an order produced, as Waldur sees it."""
    response = client.get_httpx_client().get(f"/api/marketplace-orders/{order_uuid}/")
    response.raise_for_status()
    resource_uuid = response.json().get("marketplace_resource_uuid")
    if not resource_uuid:
        return {}
    response = client.get_httpx_client().get(
        f"/api/marketplace-provider-resources/{resource_uuid}/"
    )
    response.raise_for_status()
    return response.json()
