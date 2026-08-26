"""Tests for the LiteLLM spend/usage HTTP client."""

from __future__ import annotations

from unittest import mock

import httpx
import pytest
from waldur_site_agent_litellm.usage_client import (
    DEFAULT_TIMEOUT,
    LiteLLMUsageBackendError,
    LiteLLMUsageClient,
)

SETTINGS = {"api_url": "http://litellm:4000/", "api_token": "sk-master"}


def _client() -> LiteLLMUsageClient:
    return LiteLLMUsageClient(dict(SETTINGS))


def _ok(json_body: object) -> mock.Mock:
    response = mock.Mock(status_code=200)
    response.raise_for_status = mock.Mock()
    response.json.return_value = json_body
    return response


def _day(date: str, api_keys: dict, extra_breakdown: object = None) -> dict:
    breakdown = {"api_keys": api_keys}
    if extra_breakdown is not None:
        breakdown.update(extra_breakdown)
    return {"date": date, "breakdown": breakdown}


def _entry(alias: str, prompt: int, completion: int, spend: float) -> dict:
    return {
        "metadata": {"key_alias": alias, "team_id": None},
        "metrics": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "spend": spend,
        },
    }


def test_requires_api_url_and_token() -> None:
    with pytest.raises(LiteLLMUsageBackendError):
        LiteLLMUsageClient({"api_token": "sk-master"})
    with pytest.raises(LiteLLMUsageBackendError):
        LiteLLMUsageClient({"api_url": "http://litellm:4000"})


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_ping_requires_a_connected_database(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok({"status": "healthy", "db": "connected"})
    assert _client().ping() is True
    # Authenticated, so a proxy that gates /health/readiness is not read as down.
    # The auth header lives on the pooled client, so every call carries it.
    assert mock_client_cls.call_args.kwargs["headers"]["Authorization"] == "Bearer sk-master"

    instance.get.return_value = _ok({"status": "healthy", "db": "Not connected"})
    assert _client().ping() is False


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_ping_survives_a_connection_error(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.side_effect = httpx.ConnectError("refused")
    assert _client().ping() is False


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_rows_are_keyed_by_alias_not_by_hash(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok(
        {
            "results": [_day("2026-08-01", {"hash-a": _entry("res-1", 10, 20, 0.5)})],
            "metadata": {"total_pages": 1, "has_more": False},
        }
    )

    rows = _client().get_usage_rows("2026-08-01", "2026-08-31")

    # A rotation that goes through delete-and-mint changes the hash but keeps the
    # alias, so attributing by hash would split a slot's month in two.
    assert rows == [
        {
            "key_alias": "res-1",
            "input_tokens": 10,
            "output_tokens": 20,
            "token_cost": 0.5,
        }
    ]


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_only_the_api_keys_breakdown_is_read(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok(
        {
            "results": [
                _day(
                    "2026-08-01",
                    {"hash-a": _entry("res-1", 10, 20, 0.5)},
                    # The same spend reappears here, split by model and model group.
                    # Walking these too would bill the tenant two or three times over.
                    {
                        "models": {
                            "gpt-4o": {
                                "api_key_breakdown": {
                                    "hash-a": _entry("res-1", 10, 20, 0.5)
                                }
                            }
                        },
                        "model_groups": {
                            "gpt": {
                                "api_key_breakdown": {
                                    "hash-a": _entry("res-1", 10, 20, 0.5)
                                }
                            }
                        },
                    },
                )
            ],
            "metadata": {"total_pages": 1, "has_more": False},
        }
    )

    assert len(_client().get_usage_rows("2026-08-01", "2026-08-31")) == 1


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_a_key_used_on_several_days_yields_a_row_per_day(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok(
        {
            "results": [
                _day("2026-08-01", {"hash-a": _entry("res-1", 10, 20, 0.5)}),
                _day("2026-08-02", {"hash-a": _entry("res-1", 5, 6, 0.25)}),
            ],
            "metadata": {"total_pages": 1, "has_more": False},
        }
    )
    rows = _client().get_usage_rows("2026-08-01", "2026-08-31")
    assert [row["input_tokens"] for row in rows] == [10, 5]


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_a_key_with_no_alias_is_skipped(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok(
        {
            "results": [
                _day(
                    "2026-08-01",
                    {
                        "hash-a": {"metadata": {}, "metrics": {"prompt_tokens": 9}},
                        "hash-b": _entry("res-1", 10, 20, 0.5),
                    },
                )
            ],
            "metadata": {"total_pages": 1, "has_more": False},
        }
    )
    # A key deleted before the sweep, or one minted outside the agent, has no
    # resource to bill.
    rows = _client().get_usage_rows("2026-08-01", "2026-08-31")
    assert [row["key_alias"] for row in rows] == ["res-1"]


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_paging_follows_has_more(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.side_effect = [
        _ok(
            {
                "results": [_day("2026-08-01", {"a": _entry("res-1", 1, 1, 0.1)})],
                "metadata": {"total_pages": 2, "has_more": True},
            }
        ),
        _ok(
            {
                "results": [_day("2026-08-02", {"a": _entry("res-1", 2, 2, 0.2)})],
                "metadata": {"total_pages": 2, "has_more": False},
            }
        ),
    ]

    rows = _client().get_usage_rows("2026-08-01", "2026-08-31")

    assert len(rows) == 2
    assert instance.get.call_args_list[1].kwargs["params"]["page"] == 2


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_an_empty_page_stops_the_walk_despite_a_stuck_has_more(
    mock_client_cls: mock.Mock,
) -> None:
    # A proxy that never clears has_more (or reports it without a total_pages) would
    # otherwise keep the loop going for all MAX_PAGES requests inside one pass.
    instance = mock_client_cls.return_value
    instance.get.side_effect = [
        _ok(
            {
                "results": [_day("2026-08-01", {"h1": _entry("res1-1", 5, 7, 0.5)})],
                "metadata": {"has_more": True},
            }
        ),
        _ok({"results": [], "metadata": {"has_more": True}}),
    ]

    rows = _client().get_usage_rows("2026-08-01", "2026-08-31")

    assert len(rows) == 1
    assert instance.get.call_count == 2


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_the_day_in_progress_is_requested(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _ok({"results": [], "metadata": {"total_pages": 1}})

    _client().get_usage_rows("2026-08-01", "2026-08-31")

    params = instance.get.call_args.kwargs["params"]
    # Without this the proxy can omit today, so every cycle before midnight would
    # report the month as zero.
    assert params["include_current_utc_day"] == "true"
    assert params["start_date"] == "2026-08-01"
    assert params["end_date"] == "2026-08-31"


@mock.patch("waldur_site_agent_litellm.usage_client.httpx.Client")
def test_http_and_json_failures_are_wrapped(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.side_effect = httpx.ConnectError("refused")
    with pytest.raises(LiteLLMUsageBackendError):
        _client().get_usage_rows("2026-08-01", "2026-08-31")

    response = _ok(None)
    response.json.side_effect = ValueError("not json")
    instance.get.side_effect = None
    instance.get.return_value = response
    with pytest.raises(LiteLLMUsageBackendError):
        _client().get_usage_rows("2026-08-01", "2026-08-31")


def test_the_usage_client_shares_the_settings_defaults() -> None:
    client = LiteLLMUsageClient({**SETTINGS, "verify_ssl": None, "timeout": None})
    assert client.verify_ssl is True
    assert client.timeout == DEFAULT_TIMEOUT
