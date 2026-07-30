"""Tests for the usage warehouse HTTP client."""

from __future__ import annotations

from unittest import mock

import httpx
import pytest
from waldur_site_agent_envoy_ai_gateway.usage_client import EnvoyUsageBackendError, EnvoyUsageClient


@mock.patch("waldur_site_agent_envoy_ai_gateway.usage_client.httpx.Client")
def test_ping_ok(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value.__enter__.return_value
    instance.get.return_value = mock.Mock(status_code=200)
    assert EnvoyUsageClient("http://usage-warehouse:9000").ping() is True


@mock.patch("waldur_site_agent_envoy_ai_gateway.usage_client.httpx.Client")
def test_get_usage_builds_params_and_returns_rows(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value.__enter__.return_value
    response = mock.Mock()
    response.raise_for_status = mock.Mock()
    response.json.return_value = {
        "usage": [{"client_id": "a", "input_tokens": 10, "output_tokens": 5}]
    }
    instance.get.return_value = response

    client = EnvoyUsageClient("http://usage-warehouse:9000/")
    rows = client.get_usage(["a", "b"], "2026-06", "2026-06")

    assert rows == [{"client_id": "a", "input_tokens": 10, "output_tokens": 5}]
    assert instance.get.call_args.args[0] == "http://usage-warehouse:9000/usage-month"
    params = instance.get.call_args.kwargs["params"]
    assert ("from", "2026-06") in params
    assert ("client_id", "a") in params
    assert ("client_id", "b") in params


@mock.patch("waldur_site_agent_envoy_ai_gateway.usage_client.httpx.Client")
def test_get_usage_wraps_http_error(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value.__enter__.return_value
    instance.get.side_effect = httpx.ConnectError("refused")
    with pytest.raises(EnvoyUsageBackendError):
        EnvoyUsageClient("http://usage-warehouse:9000").get_usage(["a"], "2026-06", "2026-06")


@mock.patch("waldur_site_agent_envoy_ai_gateway.usage_client.httpx.Client")
def test_get_usage_wraps_invalid_json(mock_client_cls: mock.Mock) -> None:
    # A 200 with a non-JSON body (proxy error page, truncation) raises ValueError
    # from response.json(); it must be wrapped like the httpx errors, not escape raw.
    instance = mock_client_cls.return_value.__enter__.return_value
    response = mock.Mock()
    response.raise_for_status = mock.Mock()
    response.json.side_effect = ValueError("not json")
    instance.get.return_value = response
    with pytest.raises(EnvoyUsageBackendError):
        EnvoyUsageClient("http://usage-warehouse:9000").get_usage(["a"], "2026-06", "2026-06")
