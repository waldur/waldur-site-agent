"""Shared settings, and a scripted gateway for the croit statistics client."""

import httpx
import pytest

from waldur_site_agent_ceph_s3.clients.croit import CroitClient

# croit, as croit_usage reaches it for statistics: under croit_reporting.
CROIT_REPORTING = {"api_url": "https://croit.example.org", "token": "t"}

RADOSGW_SETTINGS = {
    "flavour": "radosgw",
    "s3_endpoint": "https://rgw.example.org",
    "admin_access_key": "ADMINACCESSKEY000000",
    "admin_secret_key": "s" * 40,
}


class ScriptedGateway:
    """A stand-in gateway whose answers each test sets up explicitly.

    Routes are matched on a path suffix so a test can describe the case it cares
    about ("this key is gone") without restating the flavour's URL layout.
    """

    def __init__(self):
        self.routes = {}
        self.requests = []

    def route(self, suffix, response, method=None):
        self.routes[(suffix, method)] = response

    def handler(self, request):
        self.requests.append(request)
        path = request.url.path
        for (suffix, method), response in self.routes.items():
            if path.endswith(suffix) and method in (None, request.method):
                return response
        return httpx.Response(200, json=[])


@pytest.fixture
def gateway():
    return ScriptedGateway()


@pytest.fixture
def client(gateway):
    """A croit statistics client wired to the scripted gateway."""
    croit = CroitClient(api_url="https://croit.example.org", token="t")
    croit.session = httpx.Client(transport=httpx.MockTransport(gateway.handler))
    return croit
