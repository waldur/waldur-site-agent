"""A minted S3 secret must never reach a log sink.

croit's create-key endpoint takes the secret as a query parameter, so the HTTP
transport's own request log is a credential sink unless it is explicitly quieted.
The agent logs at INFO by default, which is exactly the level httpx logs request
URLs at.
"""

import io
import logging
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import httpx
import pytest
import yaml

from waldur_site_agent.backend import configure_logger
from waldur_site_agent.common.sentry import scrub_secret_query_params
from waldur_site_agent.common.utils import load_configuration

SECRET = "SUPERSECRET_s3_secret_value_0123456789ab"


@pytest.fixture
def croit_stub():
    """A server that accepts the create-key PUT, so the request is really made."""

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_a_minted_secret_does_not_reach_the_agent_log(croit_stub):
    """The transport logs whole URLs at INFO, and the agent's root logger is INFO."""
    configure_logger("INFO")
    captured = io.StringIO()
    handler = logging.StreamHandler(captured)
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    try:
        httpx.Client().put(
            f"{croit_stub}/api/s3/users/waldur_u/keys/AKIAEXAMPLE123456789",
            params={"secretKey": SECRET},
        )
    finally:
        logging.getLogger().removeHandler(handler)

    assert SECRET not in captured.getvalue()


def test_secret_query_params_are_scrubbed():
    """Belt-and-braces for anything that captures a URL before the logger filter."""
    url = f"PUT https://croit/api/s3/users/u/keys/AKIA?secretKey={SECRET} 204"
    scrubbed = scrub_secret_query_params(url)

    assert SECRET not in scrubbed
    assert "secretKey=[REDACTED]" in scrubbed
    assert "/keys/AKIA" in scrubbed


def test_sentry_is_initialised_without_frame_locals(monkeypatch):
    """A minted secret is a bare local, which no scrubber can reach.

    sentry-sdk ships every frame's locals with an exception event, and the frames
    that mint a key hold the secret unadorned: create_user_key's ``secret_key``,
    ``_request``'s ``params`` and signed ``url``. ``before_send`` never sees frame
    vars, and the query-parameter scrubber matches ``name=value`` shapes, which a
    40-character secret on its own does not have. Turning the capture off is the
    only thing that closes it.
    """
    import sentry_sdk

    captured = {}
    monkeypatch.setattr(sentry_sdk, "init", lambda **kwargs: captured.update(kwargs))

    config_data = {
        "offerings": [
            {
                "name": "Test Offering",
                "waldur_api_url": "http://localhost:8081/api/",
                "waldur_api_token": "test-token",
                "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
                "backend_type": "test-backend",
                "backend_settings": {},
                "backend_components": {},
            }
        ],
        "sentry_dsn": "https://example@sentry.io/123456",
        # The agent's timezone is validated on load, so a config without it never
        # reaches the Sentry branch this test is about.
        "timezone": "Europe/London",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as handle:
        yaml.dump(config_data, handle)
        config_file_path = handle.name

    try:
        load_configuration(config_file_path, user_agent_suffix="sync")
    finally:
        Path(config_file_path).unlink()

    assert captured["include_local_variables"] is False
