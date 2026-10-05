"""Tests for the LiteLLM management HTTP client."""

from __future__ import annotations

from unittest import mock

import httpx
import pytest
from waldur_site_agent_litellm.client import (
    DEFAULT_TIMEOUT,
    LiteLLMBackendError,
    LiteLLMClient,
    LiteLLMEnterpriseFeatureError,
)

SETTINGS = {"api_url": "http://litellm:4000/", "api_token": "sk-master"}


def _client() -> LiteLLMClient:
    return LiteLLMClient(dict(SETTINGS))


def _response(
    status: int = 200, json_body: object = None, text: str = ""
) -> mock.Mock:
    response = mock.Mock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.text = text
    response.json.return_value = json_body
    if response.is_success:
        response.raise_for_status = mock.Mock()
    else:
        response.raise_for_status = mock.Mock(
            side_effect=httpx.HTTPStatusError("boom", request=mock.Mock(), response=response)
        )
    return response


def test_requires_api_url() -> None:
    with pytest.raises(LiteLLMBackendError):
        LiteLLMClient({"api_token": "sk-master"})


def test_requires_api_token() -> None:
    with pytest.raises(LiteLLMBackendError):
        LiteLLMClient({"api_url": "http://litellm:4000"})


def test_strips_trailing_slash_from_url() -> None:
    assert _client().api_url == "http://litellm:4000"


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_ping_requires_connected_database(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.return_value = _response(json_body={"status": "healthy", "db": "connected"})
    assert _client().ping() is True
    # Authenticated, so a proxy that gates /health/readiness is not read as down.
    # The auth header lives on the pooled client, so every call carries it.
    assert mock_client_cls.call_args.kwargs["headers"]["Authorization"].startswith("Bearer ")

    # A proxy whose key database is down cannot serve the management API at all,
    # so a "healthy" status with no database must not read as up.
    instance.get.return_value = _response(json_body={"status": "healthy", "db": "Not connected"})
    assert _client().ping() is False


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_ping_survives_connection_error(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.get.side_effect = httpx.ConnectError("refused")
    assert _client().ping() is False


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_generate_key_omits_unset_fields(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"key": "sk-new", "token": "hash"})

    result = _client().generate_key("res-1", models=["gpt-4o"], blocked=True)

    assert result["key"] == "sk-new"
    body = instance.request.call_args.kwargs["json"]
    assert body == {"key_alias": "res-1", "models": ["gpt-4o"], "blocked": True}
    # No `key` is sent, so the proxy mints the material -- this keeps working when it
    # sets disable_custom_api_keys. No `duration` either: the keys must not expire.
    assert "key" not in body
    assert "duration" not in body


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_generate_key_rejects_a_response_without_a_key(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"key_alias": "res-1"})
    with pytest.raises(LiteLLMBackendError):
        _client().generate_key("res-1")


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_list_keys_uses_substring_narrowing_and_pages(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.side_effect = [
        _response(json_body={"keys": [{"key_alias": "res-1"}], "total_pages": 2}),
        _response(json_body={"keys": [{"key_alias": "res-2"}], "total_pages": 2}),
    ]

    keys = _client().list_keys("res-")

    assert [key["key_alias"] for key in keys] == ["res-1", "res-2"]
    params = instance.request.call_args_list[0].kwargs["params"]
    assert params["key_alias"] == "res-"
    assert params["substring_matching"] == "true"
    # Without this the records carry no `token` or `blocked`, which the backend needs.
    assert params["return_full_object"] == "true"
    assert instance.request.call_args_list[1].kwargs["params"]["page"] == 2


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_an_empty_page_stops_the_key_walk_despite_a_stale_total_pages(
    mock_client_cls: mock.Mock,
) -> None:
    # total_pages counts the whole match, so a key deleted between two requests leaves
    # the proxy advertising a page that is no longer there. Trusting the count alone
    # would spend the rest of MAX_PAGES fetching nothing.
    instance = mock_client_cls.return_value
    instance.request.side_effect = [
        _response(json_body={"keys": [{"key_alias": "res-1"}], "total_pages": 5}),
        _response(json_body={"keys": [], "total_pages": 5}),
    ]

    keys = _client().list_keys("res-")

    assert [key["key_alias"] for key in keys] == ["res-1"]
    assert instance.request.call_count == 2


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_list_keys_stops_on_a_single_page(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"keys": [], "total_pages": 0})
    assert _client().list_keys("res-") == []
    assert instance.request.call_count == 1


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_block_and_unblock_report_a_missing_key(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(status=404, text="Key not found.")

    # A key the proxy no longer holds is a state, not a failure: it cannot serve
    # traffic, so a pause that finds it gone has nothing left to do.
    assert _client().block("hash") is False
    assert _client().unblock("hash") is False


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_block_returns_true_when_applied(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"blocked": True})
    assert _client().block("hash") is True
    assert instance.request.call_args.kwargs["json"] == {"key": "hash"}


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_block_raises_on_a_real_failure(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(status=500, text="upstream exploded")
    with pytest.raises(LiteLLMBackendError):
        _client().block("hash")


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_delete_keys_batches_and_skips_an_empty_list(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"deleted_keys": ["a", "b"]})

    client = _client()
    client.delete_keys([])
    assert instance.request.call_count == 0

    client.delete_keys(["a", "b"])
    assert instance.request.call_args.kwargs["json"] == {"keys": ["a", "b"]}


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_update_key_addresses_by_hash(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={})
    _client().update_key("hash", {"max_budget": 10})
    assert instance.request.call_args.kwargs["json"] == {"max_budget": 10, "key": "hash"}


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_get_key_unwraps_info_and_maps_404_to_none(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"info": {"key_alias": "res-1"}})
    assert _client().get_key("hash") == {"key_alias": "res-1"}

    instance.request.return_value = _response(status=404, text="Key not found.")
    assert _client().get_key("hash") is None


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_regenerate_raises_the_enterprise_error(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    # The proxy answers 500 here, not 403 -- the status is not a reliable signal, so
    # the licence refusal has to be recognised by its message.
    instance.request.return_value = _response(
        status=500,
        text=(
            "Regenerating Virtual Keys is an Enterprise feature, You must be a LiteLLM "
            "Enterprise user to use this feature. If you have a license please set "
            "`LITELLM_LICENSE` in your env."
        ),
    )
    with pytest.raises(LiteLLMEnterpriseFeatureError):
        _client().regenerate_key("hash")


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_a_licence_refusal_is_recognised_without_the_word_license(
    mock_client_cls: mock.Mock,
) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(
        status=500,
        text="You must be a LiteLLM Enterprise user to use this feature.",
    )
    # Requiring "license" as well would break rotation outright on an OSS proxy whose
    # message drops that half; over-matching only picks the delete-and-mint path, which
    # is the one an OSS proxy uses anyway.
    with pytest.raises(LiteLLMEnterpriseFeatureError):
        _client().regenerate_key("hash")


def test_a_bare_verify_ssl_line_does_not_disable_verification() -> None:
    # Present-but-null: bool(None) is False, which would turn TLS verification off
    # over a stray colon in the YAML.
    assert LiteLLMClient({**SETTINGS, "verify_ssl": None}).verify_ssl is True
    assert LiteLLMClient({**SETTINGS, "verify_ssl": False}).verify_ssl is False


def test_a_bare_timeout_line_falls_back_to_the_default() -> None:
    # float(None) would raise and take the backend down at construction.
    assert LiteLLMClient({**SETTINGS, "timeout": None}).timeout == DEFAULT_TIMEOUT
    assert LiteLLMClient({**SETTINGS, "timeout": 5}).timeout == 5.0


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_regenerate_returns_the_new_key(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    instance.request.return_value = _response(json_body={"key": "sk-rotated"})
    assert _client().regenerate_key("hash") == "sk-rotated"
    assert instance.request.call_args.args[1] == "http://litellm:4000/key/hash/regenerate"


@mock.patch("waldur_site_agent_litellm.client.httpx.Client")
def test_invalid_json_is_wrapped(mock_client_cls: mock.Mock) -> None:
    instance = mock_client_cls.return_value
    response = _response()
    response.json.side_effect = ValueError("not json")
    instance.request.return_value = response
    with pytest.raises(LiteLLMBackendError):
        _client().update_key("hash", {})
