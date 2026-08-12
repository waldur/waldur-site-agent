"""Shared fixtures for the flavour-independent contract suite."""

import json

import httpx
import pytest

from waldur_site_agent_ceph_s3.clients.croit import CroitClient

CROIT_SETTINGS = {
    "api_url": "https://croit.example.org",
    "token": "t",
    "s3_endpoint": "https://rgw.example.org",
}

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

    # -- case helpers -------------------------------------------------------

    def no_such_user(self, uid):
        """croit has no get-one-user endpoint; an empty list is how it says 404."""
        del uid
        self.route("/s3/users", httpx.Response(200, json=[]))

    def user(self, uid, keys=(), user_quota=None, bucket_quota=None):
        body = {"uid": uid, "name": uid, "keys": list(keys)}
        if user_quota is not None:
            body["userQuota"] = user_quota
        if bucket_quota is not None:
            body["bucketQuota"] = bucket_quota
        self.route("/s3/users", httpx.Response(200, json=[body]), method="GET")

    def absent_key(self, uid, access_key):
        self.route(
            f"/s3/users/{uid}/keys/{access_key}",
            httpx.Response(404, json={"message": "no such key"}),
        )

    def buckets(self, uid, buckets):
        self.route(f"/s3/users/{uid}/buckets", httpx.Response(200, json=buckets))

    def accepts_quota(self):
        self.route("/quota", httpx.Response(204), method="PUT")
        self.route("/bucket-quota", httpx.Response(204), method="PUT")

    def last_body(self):
        return json.loads(self.requests[-1].content)


@pytest.fixture
def gateway():
    return ScriptedGateway()


@pytest.fixture
def client(gateway):
    """A croit client wired to the scripted gateway."""
    croit = CroitClient(api_url="https://croit.example.org", token="t")
    croit.session = httpx.Client(transport=httpx.MockTransport(gateway.handler))
    return croit
