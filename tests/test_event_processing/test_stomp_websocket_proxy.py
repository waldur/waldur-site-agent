"""The STOMP WebSocket goes through global_proxy, like the agent's REST calls."""

from __future__ import annotations

import uuid
from unittest import mock

import pytest
import websocket

import stomp

from waldur_site_agent.common.structures import Offering, UnifiedQueue
from waldur_site_agent.event_processing.event_subscription_manager import (
    EventSubscriptionManager,
)


def _opts(proxy_type: str, host: str, port: int) -> dict:
    return {"proxy_type": proxy_type, "http_proxy_host": host, "http_proxy_port": port}


def _offering(global_proxy: str = "") -> Offering:
    offering = Offering(
        name="proxy test",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="token",
        waldur_offering_uuid=uuid.uuid4().hex,
        backend_type="slurm",
        stomp_enabled=True,
        websocket_use_tls=False,
    )
    offering._global_proxy = global_proxy
    return offering


def _queue() -> UnifiedQueue:
    return UnifiedQueue(
        queue_name="consumer_abc",
        rmq_username="rmq-user",
        vhost="vhost",
        observable_object_types=[],
    )


def _ws_connect_kwargs(offering: Offering, global_proxy: str = "") -> dict:
    """Build the connection the agent builds and return what websocket gets."""
    with mock.patch(
        "waldur_site_agent.event_processing.event_subscription_manager.utils.get_client_for_offering"
    ) as get_client:
        get_client.return_value._verify_ssl = False
        manager = EventSubscriptionManager(offering, global_proxy=global_proxy)
    connection = manager.setup_stomp_connection(
        _queue(), custom_stomp_ws_host="broker.example.com", custom_stomp_ws_port=15674
    )
    transport = connection.transport
    transport.running = True
    with mock.patch.object(websocket, "create_connection") as create_connection:
        create_connection.return_value = mock.Mock()
        transport.attempt_connection()
    assert create_connection.call_count == 1
    url = create_connection.call_args.args[0]
    assert url.startswith("ws://broker.example.com:15674/")
    return dict(create_connection.call_args.kwargs)


def test_http_proxy_with_credentials_reaches_the_websocket() -> None:
    kwargs = _ws_connect_kwargs(_offering("http://agent:s3cret@proxy.example.com:3128"))

    assert kwargs["proxy_type"] == "http"
    assert kwargs["http_proxy_host"] == "proxy.example.com"
    assert kwargs["http_proxy_port"] == 3128
    assert kwargs["http_proxy_auth"] == ("agent", "s3cret")


def test_socks5_proxy_reaches_the_websocket() -> None:
    kwargs = _ws_connect_kwargs(_offering("socks5://127.0.0.1:1080"))

    # socks5h: the proxy resolves the broker name, as httpx does for REST.
    assert kwargs["proxy_type"] == "socks5h"
    assert kwargs["http_proxy_host"] == "127.0.0.1"
    assert kwargs["http_proxy_port"] == 1080
    assert "http_proxy_auth" not in kwargs


def test_proxy_passed_to_the_manager_wins_over_the_offering() -> None:
    # Federation target consumers get the proxy as an argument.
    kwargs = _ws_connect_kwargs(_offering(""), global_proxy="http://proxy.example.com:8080")

    assert kwargs["http_proxy_host"] == "proxy.example.com"
    assert kwargs["http_proxy_port"] == 8080


def test_without_global_proxy_the_websocket_gets_no_proxy_options() -> None:
    kwargs = _ws_connect_kwargs(_offering(""))

    assert not {k for k in kwargs if "proxy" in k}


def test_socks_support_is_installed() -> None:
    # websocket-client needs python-socks for socks4/socks5 proxies.
    import python_socks  # noqa: F401, PLC0415


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://p:3128", _opts("http", "p", 3128)),
        ("http://p", _opts("http", "p", 80)),
        ("socks5://p:1080", _opts("socks5h", "p", 1080)),
        ("socks5h://p:1080", _opts("socks5h", "p", 1080)),
        ("http://[2001:db8::1]:3128", _opts("http", "2001:db8::1", 3128)),
        ("http://a%40b:p%3Aw@p:3128", {**_opts("http", "p", 3128), "http_proxy_auth": ("a@b", "p:w")}),
        # https proxies: websocket-client cannot use them; the WebSocket goes direct
        # (environment proxies honoured as without global_proxy).
        ("https://p:3128", {}),
        ("", {}),
    ],
)
def test_proxy_url_parsing(url: str, expected: dict) -> None:
    from waldur_site_agent.event_processing.ws_proxy import (  # noqa: PLC0415
        websocket_proxy_options,
    )

    options = websocket_proxy_options(url)
    # http_no_proxy is covered by test_explicit_proxy_ignores_no_proxy_like_rest.
    assert bool(options.pop("http_no_proxy", None)) == bool(expected)
    assert options == expected


def test_unsupported_proxy_error_hides_credentials() -> None:
    from waldur_site_agent.event_processing.ws_proxy import (  # noqa: PLC0415
        websocket_proxy_options,
    )

    with pytest.raises(ValueError, match="socks4://proxy.example.com") as excinfo:
        websocket_proxy_options("socks4://agent:s3cret@proxy.example.com:1080")
    assert "s3cret" not in str(excinfo.value)
    assert "agent" not in str(excinfo.value)


def test_https_global_proxy_falls_back_to_a_direct_websocket(caplog) -> None:  # noqa: ANN001
    kwargs = _ws_connect_kwargs(_offering("https://agent:s3cret@proxy.example.com:3128"))

    assert not {k for k in kwargs if "proxy" in k}


def test_explicit_proxy_ignores_no_proxy_like_rest(monkeypatch) -> None:  # noqa: ANN001
    # httpx sends everything through an explicit proxy; the WebSocket must not let
    # NO_PROXY bypass it.
    monkeypatch.setenv("no_proxy", "broker.example.com")
    kwargs = _ws_connect_kwargs(_offering("http://proxy.example.com:3128"))

    no_proxy = kwargs["http_no_proxy"]
    assert no_proxy
    from websocket._url import _is_no_proxy_host  # noqa: PLC0415

    assert not _is_no_proxy_host("broker.example.com", no_proxy)
    assert not _is_no_proxy_host("10.0.0.1", no_proxy)


@pytest.mark.parametrize("url", ["socks4://p:1080", "socks4a://p:1080", "ftp://p:21"])
def test_config_rejects_proxy_schemes_rest_cannot_use(url: str) -> None:
    from pydantic import ValidationError  # noqa: PLC0415

    from waldur_site_agent.common.structures import RootConfiguration  # noqa: PLC0415

    with pytest.raises(ValidationError, match="global_proxy"):
        RootConfiguration(offerings=[], global_proxy=url)


def test_config_rejection_hides_credentials() -> None:
    from pydantic import ValidationError  # noqa: PLC0415

    from waldur_site_agent.common.structures import RootConfiguration  # noqa: PLC0415

    with pytest.raises(ValidationError) as excinfo:
        RootConfiguration(offerings=[], global_proxy="socks4://agent:s3cret@p:1080")
    assert "s3cret" not in str(excinfo.value)


def test_config_warns_once_that_https_proxy_skips_the_websocket(caplog) -> None:  # noqa: ANN001
    import logging  # noqa: PLC0415

    from waldur_site_agent.common.structures import RootConfiguration  # noqa: PLC0415

    with caplog.at_level(logging.WARNING):
        config = RootConfiguration(offerings=[], global_proxy="https://agent:s3cret@p:3128")
    assert config.global_proxy == "https://agent:s3cret@p:3128"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "WebSocket" in warnings[0]
    assert "s3cret" not in warnings[0]


@pytest.mark.parametrize("url", ["http://p:3128", "https://p:3128", "socks5://p:1080", "socks5h://p:1080", ""])
def test_config_accepts_proxy_schemes_rest_supports(url: str) -> None:
    from waldur_site_agent.common.structures import RootConfiguration  # noqa: PLC0415

    assert RootConfiguration(offerings=[], global_proxy=url).global_proxy == url


def test_proxied_connection_accepts_every_wsstompconnection_parameter() -> None:
    import inspect  # noqa: PLC0415

    from waldur_site_agent.event_processing.ws_proxy import (  # noqa: PLC0415
        ProxiedWSStompConnection,
        ws_stomp_connection,
    )

    params = [p for p in inspect.signature(stomp.WSStompConnection.__init__).parameters if p != "self"]
    defaults = {
        p: v.default
        for p, v in inspect.signature(stomp.WSStompConnection.__init__).parameters.items()
        if p != "self"
    }
    defaults["host_and_ports"] = [("broker.example.com", 443)]
    defaults["heartbeats"] = (5000, 7000)
    defaults["heart_beat_receive_scale"] = 2.0
    connection = ws_stomp_connection(proxy_url="http://p:3128", **defaults)

    assert isinstance(connection, ProxiedWSStompConnection)
    assert set(params) <= set(defaults)
    assert connection.heartbeats == (5000, 7000)


def test_concurrent_proxied_and_direct_connects_stay_separate() -> None:
    import threading  # noqa: PLC0415

    from waldur_site_agent.event_processing.ws_proxy import ws_stomp_connection  # noqa: PLC0415

    proxied = ws_stomp_connection(proxy_url="http://p:3128", host_and_ports=[("a.example", 1)])
    direct = ws_stomp_connection(proxy_url="", host_and_ports=[("b.example", 2)])
    seen: dict[str, dict] = {}
    both_inside = threading.Barrier(2, timeout=5)

    def fake_create_connection(url: str, **kwargs):  # noqa: ANN202
        both_inside.wait()  # both threads inside create_connection at once
        seen[url] = kwargs
        return mock.Mock()

    def run(connection) -> None:  # noqa: ANN001
        connection.transport.running = True
        connection.transport.attempt_connection()

    with mock.patch.object(websocket, "create_connection", side_effect=fake_create_connection):
        threads = [threading.Thread(target=run, args=(c,)) for c in (proxied, direct)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(5)

    proxied_kwargs = next(v for k, v in seen.items() if "a.example" in k)
    direct_kwargs = next(v for k, v in seen.items() if "b.example" in k)
    assert proxied_kwargs["http_proxy_host"] == "p"
    assert not {k for k in direct_kwargs if "proxy" in k}


def test_proxy_options_cleared_when_the_connect_attempt_raises() -> None:
    from waldur_site_agent.event_processing import ws_proxy  # noqa: PLC0415

    connection = ws_proxy.ws_stomp_connection(proxy_url="http://p:3128", host_and_ports=[("a.example", 1)])
    connection.transport.running = True
    with (
        mock.patch.object(websocket, "create_connection", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        connection.transport.attempt_connection()

    assert not getattr(ws_proxy._active, "options", None)


def test_proxied_connection_keeps_stomp_heartbeats() -> None:
    from waldur_site_agent.event_processing.ws_proxy import (  # noqa: PLC0415
        ProxiedWSTransport,
        ws_stomp_connection,
    )

    connection = ws_stomp_connection(
        proxy_url="http://proxy.example.com:3128",
        host_and_ports=[("broker.example.com", 443)],
        heartbeats=(10000, 10000),
        vhost="vhost",
        ws_path="/ws",
        timeout=30,
        reconnect_attempts_max=1,
    )

    assert isinstance(connection.transport, ProxiedWSTransport)
    # Protocol12 registers stomp.py's heartbeat listener on the transport it was given.
    assert connection.get_listener("protocol-listener") is not None
    assert connection.transport.vhost == "vhost"


def test_stomp_still_connects_through_websocket_create_connection() -> None:
    # The proxy options are injected where stomp.py's WSTransport calls
    # websocket.create_connection; fail loudly if a stomp.py upgrade changes that.
    import inspect  # noqa: PLC0415

    from stomp.adapter import ws as stomp_ws  # noqa: PLC0415

    assert "websocket.create_connection(" in inspect.getsource(stomp_ws.WSTransport.attempt_connection)
