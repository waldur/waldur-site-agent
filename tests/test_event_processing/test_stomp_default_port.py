"""Default STOMP WebSocket port when stomp_ws_port is not configured."""

from unittest import mock

import pytest

from waldur_site_agent.event_processing import event_subscription_manager as esm


def _connect_port(*, use_tls: bool, verify_ssl: bool) -> int:
    manager = esm.EventSubscriptionManager.__new__(esm.EventSubscriptionManager)
    manager.offering = mock.Mock(
        api_url="https://waldur.example.com/api/",
        api_token="token",
        websocket_use_tls=use_tls,
    )
    manager.waldur_rest_client = mock.Mock(_verify_ssl=verify_ssl)
    manager.global_proxy = ""
    manager.user_agent = ""
    manager.expose_backend_error_details = False
    manager.on_message_callback = mock.Mock()
    manager.on_connect_callback = None
    queue = mock.Mock(rmq_username="user", queue_name="queue", vhost="vhost")
    with mock.patch.object(esm, "ws_stomp_connection") as factory, mock.patch.object(
        esm, "WaldurListener"
    ):
        manager.setup_stomp_connection(queue)
    ((_host, port),) = factory.call_args.kwargs["host_and_ports"]
    return port


@pytest.mark.parametrize("verify_ssl", [True, False])
def test_tls_websocket_defaults_to_443_whatever_verify_ssl(verify_ssl):
    """A self-signed https Waldur (verify_ssl off) still talks TLS on 443."""
    assert _connect_port(use_tls=True, verify_ssl=verify_ssl) == 443


@pytest.mark.parametrize("verify_ssl", [True, False])
def test_plain_websocket_defaults_to_80_whatever_verify_ssl(verify_ssl):
    assert _connect_port(use_tls=False, verify_ssl=verify_ssl) == 80
