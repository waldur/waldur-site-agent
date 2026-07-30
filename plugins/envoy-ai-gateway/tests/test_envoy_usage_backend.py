"""Tests for the usage reporting backend (client mocked)."""

from __future__ import annotations

import logging
from unittest import mock

import pytest
from waldur_site_agent_envoy_ai_gateway.reporting import EnvoyUsageReportingBackend

from waldur_site_agent.backend.exceptions import BackendError

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {"api_url": "http://usage-warehouse:9000"}


def _make_backend() -> EnvoyUsageReportingBackend:
    backend = EnvoyUsageReportingBackend(dict(SETTINGS), dict(COMPONENTS))
    backend.usage_client = mock.MagicMock()
    return backend


def test_requires_api_url() -> None:
    with pytest.raises(BackendError):
        EnvoyUsageReportingBackend({}, dict(COMPONENTS))


def test_list_components() -> None:
    assert set(_make_backend().list_components()) == {"input_tokens", "output_tokens"}


def test_usage_report_shape() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage.return_value = [
        {"client_id": "a", "input_tokens": 17, "output_tokens": 8, "total_tokens": 25},
        {"client_id": "b", "input_tokens": 1, "output_tokens": 1},
    ]

    report = backend._get_usage_report(["a", "b"])

    assert report == {
        "a": {"TOTAL_ACCOUNT_USAGE": {"input_tokens": 17, "output_tokens": 8}},
        "b": {"TOTAL_ACCOUNT_USAGE": {"input_tokens": 1, "output_tokens": 1}},
    }


def test_usage_sums_multiple_rows_per_client() -> None:
    # The warehouse may return more than one row per key (e.g. per-model);
    # they must be summed, not overwritten last-wins.
    backend = _make_backend()
    backend.usage_client.get_usage.return_value = [
        {"client_id": "a", "input_tokens": 100, "output_tokens": 0},
        {"client_id": "a", "input_tokens": 50, "output_tokens": 5},
    ]

    report = backend._get_usage_report(["a"])

    assert report == {"a": {"TOTAL_ACCOUNT_USAGE": {"input_tokens": 150, "output_tokens": 5}}}


def test_usage_report_filters_unconfigured_components() -> None:
    # Only components the offering defines are reported.
    backend = EnvoyUsageReportingBackend(
        dict(SETTINGS), {"input_tokens": {"measured_unit": "tokens"}}
    )
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_usage.return_value = [
        {"client_id": "a", "input_tokens": 7, "output_tokens": 9},
    ]

    report = backend._get_usage_report(["a"])

    assert report == {"a": {"TOTAL_ACCOUNT_USAGE": {"input_tokens": 7}}}


def test_report_for_period_queries_that_month() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage.return_value = []
    backend.get_usage_report_for_period(["a"], 2026, 6)
    backend.usage_client.get_usage.assert_called_once_with(["a"], "2026-06", "2026-06")


def test_empty_ids_returns_empty_without_calling() -> None:
    backend = _make_backend()
    assert backend._get_usage_report([]) == {}
    backend.usage_client.get_usage.assert_not_called()


def test_warns_when_no_token_components_configured(caplog: pytest.LogCaptureFixture) -> None:
    # If none of the offering's components map to a token meter, every key reports
    # zero — that must be a visible warning, not silent under-billing.
    with caplog.at_level(logging.WARNING):
        EnvoyUsageReportingBackend(dict(SETTINGS), {"gpu_hours": {"measured_unit": "hours"}})
    assert "token" in caplog.text.lower()


def test_no_warning_when_token_components_present(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        EnvoyUsageReportingBackend(dict(SETTINGS), dict(COMPONENTS))
    assert caplog.text == ""


def test_pull_resource_none_without_backend_id() -> None:
    backend = _make_backend()
    assert backend.pull_resource(mock.Mock(backend_id="")) is None
    backend.usage_client.get_usage.assert_not_called()


def test_report_only_methods_raise() -> None:
    backend = _make_backend()
    for call in (
        lambda: backend.pause_resource("x"),
        lambda: backend.restore_resource("x"),
        lambda: backend.downscale_resource("x"),
        lambda: backend.get_resource_metadata("x"),
    ):
        with pytest.raises(NotImplementedError):
            call()
