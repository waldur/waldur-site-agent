"""Every offering-scoped Waldur client must honour OIDC-only offerings.

An OIDC-only offering has an empty ``waldur_api_token``. Any code path that
builds its client from that token directly sends ``Authorization: Token `` and
fails, so each entry point is checked to build its client with the bearer token
obtained from the OIDC provider.
"""

import contextlib
import json
from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
import respx
import stomp.utils

from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.polling_processing import agent_order_process

OIDC_OFFERING = {
    "name": "oidc-offering",
    "waldur_api_url": "https://waldur.example.com/api/",
    "waldur_offering_uuid": "12345678123412341234123456789abc",
    "waldur_api_token": "",
    "oidc_token_url": "https://idp.example.com/token",
    "oidc_client_id": "agent",
    "oidc_client_secret": "secret",
    "backend_type": "slurm",
    "order_processing_backend": "slurm",
    "membership_sync_backend": "slurm",
}


class _Stop(Exception):
    """Raised by the recording client factory to end the code path under test."""


@pytest.fixture(autouse=True)
def _clear_oidc_cache():
    utils._OIDC_TOKEN_CACHE.clear()
    yield
    utils._OIDC_TOKEN_CACHE.clear()


@pytest.fixture
def offering() -> Offering:
    return Offering(**OIDC_OFFERING)


@pytest.fixture
def configuration(offering):
    return SimpleNamespace(
        waldur_offerings=[offering],
        waldur_user_agent="test-agent",
        waldur_site_agent_mode="order_process",
        global_proxy="",
        log_shipping=None,
        expose_backend_error_details=True,
        timezone="UTC",
        sentry_dsn=None,
        elastic_apm_server_url=None,
    )


@pytest.fixture
def recorded_clients():
    """Record every client built, then stop the caller."""
    with (
        mock.patch.object(utils, "fetch_oidc_token", return_value="jwt-from-idp"),
        mock.patch.object(utils, "get_client", side_effect=_Stop) as get_client,
    ):
        yield get_client


def _assert_bearer_client(get_client: mock.Mock) -> None:
    assert get_client.call_args_list, "no Waldur client was built"
    for call in get_client.call_args_list:
        args, kwargs = call
        token = kwargs.get("access_token", args[1] if len(args) > 1 else None)
        prefix = kwargs.get("token_prefix", args[5] if len(args) > 5 else "Token")
        assert token == "jwt-from-idp"
        assert prefix == "Bearer"


def _run(func, *args):
    with contextlib.suppress(_Stop, SystemExit):
        func(*args)


def test_polling_order_process_uses_oidc(configuration, recorded_clients):
    _run(agent_order_process._process_offerings, configuration)
    _assert_bearer_client(recorded_clients)


def test_offering_resources_sync_handler_uses_oidc(offering, recorded_clients):
    frame = mock.Mock(spec=stomp.utils.Frame)
    frame.body = json.dumps({"offering_uuid": offering.uuid, "requested_by_user_uuid": "u"})
    _run(handlers.on_offering_resources_sync_message_stomp, frame, offering, "test-agent")
    _assert_bearer_client(recorded_clients)


@pytest.mark.parametrize(
    "command",
    [
        "load_offering_components",
        "diagnostics",
        "create_homedirs_for_offering_users",
        "sync_offering_users",
        "sync_resource_limits",
    ],
)
def test_cli_commands_use_oidc(command, configuration, recorded_clients):
    backend_class = mock.Mock(supports_user_homedirs=True)
    with (
        mock.patch.object(utils, "init_configuration", return_value=configuration),
        mock.patch.object(utils, "get_backend_class_for_offering", return_value=backend_class),
        mock.patch.object(utils, "get_backend_for_offering", return_value=(mock.Mock(), "1")),
    ):
        _run(getattr(utils, command))
    _assert_bearer_client(recorded_clients)


class TestEmptyProxyMeansNoProxy:
    """``global_proxy`` defaults to "", which httpx rejects as a proxy URL."""

    def test_fetch_oidc_token_treats_empty_proxy_as_none(self):
        response = mock.MagicMock()
        response.json.return_value = {"access_token": "jwt", "expires_in": 300}
        client = mock.MagicMock()
        client.__enter__.return_value = client
        client.post.return_value = response
        with mock.patch.object(utils.httpx, "Client", return_value=client) as client_cls:
            utils.fetch_oidc_token("https://idp.example.com/token", "cid", "s", proxy="")

        assert client_cls.call_args.kwargs["proxy"] is None

    def test_get_client_for_offering_with_empty_proxy_does_not_raise(self, offering):
        response = mock.MagicMock()
        response.json.return_value = {"access_token": "jwt", "expires_in": 300}
        transport = mock.MagicMock()
        transport.__enter__.return_value = transport
        transport.post.return_value = response
        real_client = utils.httpx.Client

        def _client(**kwargs):
            # Validate the proxy argument the way httpx does, without network I/O.
            real_client(proxy=kwargs["proxy"]).close()
            return transport

        with mock.patch.object(utils.httpx, "Client", side_effect=_client):
            client = utils.get_client_for_offering(offering, "agent", proxy="")

        assert client.token == "jwt"


class TestStompRequiresStaticToken:
    """RabbitMQ authenticates the STOMP session with the agent's static token."""

    def test_stomp_with_oidc_only_auth_is_rejected(self):
        with pytest.raises(ValueError, match="stomp_enabled"):
            Offering(**OIDC_OFFERING, stomp_enabled=True)

    def test_stomp_with_static_token_is_accepted(self):
        Offering(**{**OIDC_OFFERING, "waldur_api_token": "static"}, stomp_enabled=True)

    def test_oidc_without_stomp_is_accepted(self):
        Offering(**OIDC_OFFERING)


class TestPartialOidcConfig:
    """Setting only some oidc_* keys is a mistake, not a silent fallback."""

    @pytest.mark.parametrize("missing", ["oidc_token_url", "oidc_client_id", "oidc_client_secret"])
    def test_partial_oidc_config_is_rejected_even_with_static_token(self, missing):
        settings = {**OIDC_OFFERING, "waldur_api_token": "static"}
        del settings[missing]
        with pytest.raises(ValueError, match="oidc_"):
            Offering(**settings)


class TestTokenRefreshPerRequest:
    """A long-lived client must not keep sending an expired JWT."""

    @respx.mock
    def test_client_sends_fresh_token_after_refresh(self, offering):
        tokens = iter(["jwt-1", "jwt-1", "jwt-2"])
        route = respx.get("https://waldur.example.com/api/users/me/").mock(
            return_value=httpx.Response(200, json={})
        )
        with mock.patch.object(utils, "fetch_oidc_token", side_effect=lambda *a, **k: next(tokens)):
            client = utils.get_client_for_offering(offering, "agent")
            http = client.get_httpx_client()
            http.get("/api/users/me/")
            http.get("/api/users/me/")

        sent = [call.request.headers["Authorization"] for call in route.calls]
        assert sent == ["Bearer jwt-1", "Bearer jwt-2"]


class TestLogShipperAuth:
    """Log shipping authenticates the way the offering does."""

    def _shipper_for(self, offering):
        from waldur_site_agent.common.log_buffer import CircularLogBuffer
        from waldur_site_agent.common.structures import LogShippingConfig

        shippers = {}
        manager = mock.Mock(shippers=shippers)
        manager.add_shipper.side_effect = shippers.__setitem__
        buffer_manager = mock.Mock()
        buffer_manager.get_buffer.return_value = CircularLogBuffer()
        with (
            mock.patch.object(utils, "get_log_shipping_manager", return_value=manager),
            mock.patch.object(utils, "get_log_buffer_manager", return_value=buffer_manager),
            mock.patch.object(utils.LogShipper, "start"),
        ):
            utils.ensure_log_shipper(offering, "agent-uuid", LogShippingConfig(enabled=True))
        return shippers["agent-uuid"]

    def _shipped_authorization(self, shipper):
        from waldur_site_agent.common.log_buffer import LogEntry

        route = respx.post("https://waldur.example.com/api/marketplace-site-agent-logs/").mock(
            return_value=httpx.Response(201, json={})
        )
        shipper._ship_batch([LogEntry(timestamp=0.0, level="INFO", message="m", module="x", size=1)])
        assert route.called, "batch was not shipped"
        return route.calls.last.request.headers["Authorization"]

    @respx.mock
    def test_oidc_offering_ships_with_bearer_token(self, offering):
        shipper = self._shipper_for(offering)
        with mock.patch.object(utils, "fetch_oidc_token", return_value="jwt-from-idp"):
            assert self._shipped_authorization(shipper) == "Bearer jwt-from-idp"

    @respx.mock
    def test_static_token_offering_ships_with_token(self):
        shipper = self._shipper_for(Offering(**{**OIDC_OFFERING, "waldur_api_token": "static",
                                                "oidc_token_url": None, "oidc_client_id": None,
                                                "oidc_client_secret": None}))
        assert self._shipped_authorization(shipper) == "Token static"
