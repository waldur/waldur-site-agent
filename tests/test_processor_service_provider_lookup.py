"""The processor names the problem when the token cannot see the service provider.

A personal access token bound to the offering alone cannot list the offering's
service provider, and the processor used to fail with a bare IndexError.
"""

import uuid
from unittest import mock

import pytest
import respx
from waldur_api_client.client import AuthenticatedClient

from tests.fixtures import user_me_api_response
from waldur_site_agent.backend import backends
from waldur_site_agent.backend.exceptions import ConfigurationError
from waldur_site_agent.common import structures
from waldur_site_agent.common.processors import OfferingOrderProcessor

BASE_URL = "https://waldur.example.com"


@respx.mock
@mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
def test_invisible_service_provider_raises_configuration_error(mock_get_backend):
    backend = backends.UnknownBackend()
    backend.backend_type = "test"
    mock_get_backend.return_value = (backend, "1.0.0")
    offering_uuid = uuid.uuid4().hex
    customer_uuid = uuid.uuid4().hex
    respx.get(f"{BASE_URL}/api/users/me/").respond(
        200, json=user_me_api_response(base_url=BASE_URL)
    )
    respx.get(f"{BASE_URL}/api/marketplace-provider-offerings/{offering_uuid}/").respond(
        200, json={"customer_uuid": customer_uuid, "components": []}
    )
    respx.get(
        f"{BASE_URL}/api/marketplace-service-providers/",
        params={"customer_uuid": customer_uuid, "field": "uuid"},
    ).respond(200, json=[])
    offering = structures.Offering(
        name="pat-offering",
        waldur_offering_uuid=offering_uuid,
        waldur_api_url=f"{BASE_URL}/api/",
        waldur_api_token="w_1789000000_abc",  # noqa: S106
        backend_type="slurm",
    )
    client = AuthenticatedClient(base_url=BASE_URL, token="w_1789000000_abc", prefix="Bearer")

    with pytest.raises(ConfigurationError, match="bind it to the service provider"):
        OfferingOrderProcessor(offering, client)
