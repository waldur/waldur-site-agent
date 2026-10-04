"""global_proxy must reach every Waldur client, in event mode as well as polling mode."""

import json
from collections.abc import Iterator
from unittest import mock

import httpx
import pytest

from waldur_site_agent.common import structures, utils
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing import main as event_main
from waldur_site_agent.event_processing import utils as event_utils

PROXY = "http://proxy.example.com:3128"


def _offering_data(**overrides) -> dict:
    data = {
        "name": "Proxied offering",
        "waldur_api_url": "https://waldur.example.com/api/",
        "waldur_api_token": "token",
        "waldur_offering_uuid": "e2ef0000000000000000000000000001",
        "backend_type": "slurm",
        "order_processing_backend": "slurm",
    }
    data.update(overrides)
    return data


def _configuration(global_proxy: str = PROXY, **overrides) -> structures.WaldurAgentConfiguration:
    root = structures.RootConfiguration(
        offerings=[_offering_data(**overrides)], global_proxy=global_proxy
    )
    return root.to_agent_configuration()


@pytest.fixture(autouse=True)
def _clear_oidc_cache() -> Iterator[None]:
    utils._OIDC_TOKEN_CACHE.clear()
    yield
    utils._OIDC_TOKEN_CACHE.clear()


def _proxy_of(get_client_mock: mock.Mock) -> object:
    """Proxy argument of the last get_client call (positional or keyword)."""
    call = get_client_mock.call_args
    return call.kwargs["proxy"] if "proxy" in call.kwargs else call.args[4]


class TestGetClientForOfferingProxy:
    def test_offering_from_config_carries_global_proxy(self):
        offering = _configuration().waldur_offerings[0]
        with mock.patch.object(utils, "get_client") as get_client:
            utils.get_client_for_offering(offering, "agent")
        assert _proxy_of(get_client) == PROXY

    def test_empty_explicit_proxy_falls_back_to_global_proxy(self):
        offering = _configuration().waldur_offerings[0]
        with mock.patch.object(utils, "get_client") as get_client:
            utils.get_client_for_offering(offering, "agent", "")
        assert _proxy_of(get_client) == PROXY

    def test_explicit_proxy_wins(self):
        offering = _configuration().waldur_offerings[0]
        with mock.patch.object(utils, "get_client") as get_client:
            utils.get_client_for_offering(offering, "agent", "socks5://other:1080")
        assert _proxy_of(get_client) == "socks5://other:1080"

    def test_no_proxy_configured_passes_none(self):
        offering = _configuration(global_proxy="").waldur_offerings[0]
        with mock.patch.object(utils, "get_client") as get_client:
            utils.get_client_for_offering(offering, "agent", "")
        assert _proxy_of(get_client) is None

    def test_oidc_token_fetch_gets_none_not_empty_string(self):
        """An empty proxy must not reach httpx, which rejects ``proxy=""``."""
        offering = _configuration(
            global_proxy="",
            waldur_api_token="",
            oidc_token_url="https://idp.example.com/token",
            oidc_client_id="agent",
            oidc_client_secret="secret",
        ).waldur_offerings[0]
        with mock.patch.object(utils, "fetch_oidc_token", return_value="jwt") as fetch:
            with mock.patch.object(utils, "get_client"):
                utils.get_client_for_offering(offering, "agent", "")
        assert fetch.call_args.args[4] is None

    def test_global_proxy_is_not_part_of_offering_dump(self):
        offering = _configuration().waldur_offerings[0]
        assert "global_proxy" not in offering.model_dump()


class TestEventModeUsesProxy:
    def test_periodic_order_reconciliation_uses_global_proxy(self):
        offering = _configuration().waldur_offerings[0]
        with mock.patch.object(
            utils, "get_client", side_effect=RuntimeError("stop here")
        ) as get_client:
            event_utils.run_periodic_order_reconciliation([offering], "agent")
        assert _proxy_of(get_client) == PROXY

    def test_order_handler_uses_global_proxy(self):
        offering = _configuration().waldur_offerings[0]
        frame = mock.Mock()
        frame.body = json.dumps(
            {"order_uuid": "a" * 32, "order_state": "pending-provider"}
        )
        with mock.patch.object(
            utils, "get_client", side_effect=RuntimeError("stop here")
        ) as get_client:
            handlers.on_order_message_stomp(frame, offering, "agent")
        assert _proxy_of(get_client) == PROXY

    def test_start_passes_global_proxy_to_stomp_consumers(self):
        configuration = _configuration()
        with mock.patch.object(event_main, "utils") as mocked_utils:
            with mock.patch.object(event_main, "common_utils"):
                with mock.patch.object(event_main, "_run_without_username_reconciliation"):
                    event_main.start(configuration)
        assert mocked_utils.start_stomp_consumers.call_args.kwargs["global_proxy"] == PROXY


def test_socks_proxy_is_supported():
    """``socks5://`` needs httpx's SOCKS extra; without it httpx raises ImportError."""
    with httpx.Client(proxy="socks5://127.0.0.1:1080"):
        pass
