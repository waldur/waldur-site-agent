"""waldur_site_diagnostics: Waldur-side failures are reported, not raised."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
from waldur_api_client.errors import UnexpectedStatus

from waldur_site_agent.common import structures, utils

URL = httpx.URL("https://waldur.example.com/api/users/me/")


def _configuration() -> SimpleNamespace:
    offering = SimpleNamespace(
        uuid="0" * 32,
        name="Test offering",
        api_url="https://waldur.example.com/api/",
    )
    return SimpleNamespace(
        waldur_site_agent_mode=structures.AgentMode.ORDER_PROCESS.value,
        waldur_offerings=[offering],
        waldur_user_agent="test",
        global_proxy="",
        sentry_dsn=None,
        elastic_apm_server_url=None,
    )


def _offering_data() -> SimpleNamespace:
    return SimpleNamespace(
        uuid="0" * 32,
        name="Test offering",
        customer_name="Org",
        state="Active",
        components=[],
        backend_id="",
    )


@pytest.fixture
def backend() -> mock.Mock:
    backend = mock.Mock(cluster_name=None)
    backend.diagnostics.return_value = True
    return backend


def _run(backend, *, me=None, offering=None, orders=None) -> int:
    with (
        mock.patch.object(utils, "init_configuration", return_value=_configuration()),
        mock.patch.object(utils, "get_client_for_offering", return_value=mock.Mock()),
        mock.patch.object(utils, "get_current_user_from_client", **(me or {"return_value": mock.Mock()})),
        mock.patch.object(utils, "print_current_user"),
        mock.patch.object(
            utils.marketplace_provider_offerings_retrieve,
            "sync",
            **(offering or {"return_value": _offering_data()}),
        ),
        mock.patch.object(
            utils.marketplace_orders_list, "sync_all", **(orders or {"return_value": []})
        ),
        mock.patch.object(utils, "get_backend_for_offering", return_value=(backend, "1.0")),
    ):
        return utils.diagnostics()


def test_healthy_run_exits_zero(backend) -> None:
    assert _run(backend) == 0


def test_rejected_token_is_reported_as_an_authentication_failure(backend, caplog) -> None:
    caplog.set_level(logging.ERROR)
    rejected = UnexpectedStatus(401, b'{"detail":"Invalid token."}', URL)

    result = _run(backend, me={"side_effect": rejected})

    assert result == 1
    messages = " ".join(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)
    assert "authentication failed" in messages.lower()
    assert "401" in messages


def test_forbidden_offering_is_reported_and_exits_one(backend, caplog) -> None:
    caplog.set_level(logging.ERROR)
    forbidden = UnexpectedStatus(403, b"{}", URL)

    result = _run(backend, offering={"side_effect": forbidden})

    assert result == 1
    messages = " ".join(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)
    assert "authentication failed" in messages.lower() or "403" in messages


def test_server_error_on_orders_is_reported_and_exits_one(backend, caplog) -> None:
    caplog.set_level(logging.ERROR)
    broken = UnexpectedStatus(500, b"oops", URL)

    result = _run(backend, orders={"side_effect": broken})

    assert result == 1
    assert any("500" in r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)


def test_unreachable_waldur_is_reported_and_exits_one(backend, caplog) -> None:
    caplog.set_level(logging.ERROR)

    result = _run(backend, me={"side_effect": httpx.ConnectError("connection refused")})

    assert result == 1
    assert any(
        "connection refused" in r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR
    )


def test_backend_is_still_diagnosed_after_a_waldur_failure(backend) -> None:
    rejected = UnexpectedStatus(401, b"{}", URL)

    _run(backend, me={"side_effect": rejected})

    backend.diagnostics.assert_called_once()
