"""Tests for the Envoy AI Gateway Kubernetes Secret client."""

from __future__ import annotations

import base64
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException
from waldur_site_agent_envoy_ai_gateway.client import (
    EnvoyAIGatewayBackendError,
    EnvoyAIGatewayClient,
)

SETTINGS = {"namespace": "llm-test", "apikey_secret": "keys"}


def _make_client() -> tuple[EnvoyAIGatewayClient, mock.Mock]:
    core_api = mock.Mock()
    client = EnvoyAIGatewayClient(dict(SETTINGS), core_api=core_api)
    return client, core_api


def _secret_with(entries: dict[str, str]) -> mock.Mock:
    data = {cid: base64.b64encode(value.encode()).decode() for cid, value in entries.items()}
    return mock.Mock(data=data)


def test_requires_namespace() -> None:
    with pytest.raises(EnvoyAIGatewayBackendError):
        EnvoyAIGatewayClient({}, core_api=mock.Mock())


def test_blocked_secret_defaults_from_apikey_secret() -> None:
    client, _ = _make_client()
    assert client.blocked_secret == "keys-blocked"  # noqa: S105


def test_provision_key_patches_active_secret() -> None:
    client, core_api = _make_client()
    client.provision_key("cid-1", "sk-secret")
    core_api.patch_namespaced_secret.assert_called_once_with(
        "keys", "llm-test", {"stringData": {"cid-1": "sk-secret"}}
    )


def test_rotate_key_overwrites_active() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.side_effect = lambda name, ns: (
        _secret_with({"cid-1": "sk-old"}) if name == "keys" else _secret_with({})
    )
    assert client.rotate_key("cid-1", "sk-new") is True
    core_api.patch_namespaced_secret.assert_called_once_with(
        "keys", "llm-test", {"stringData": {"cid-1": "sk-new"}}
    )


def test_rotate_key_overwrites_blocked_when_paused() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.side_effect = lambda name, ns: (
        _secret_with({"cid-1": "sk-old"}) if name == "keys-blocked" else _secret_with({})
    )
    assert client.rotate_key("cid-1", "sk-new") is True
    core_api.patch_namespaced_secret.assert_called_once_with(
        "keys-blocked", "llm-test", {"stringData": {"cid-1": "sk-new"}}
    )


def test_rotate_key_returns_false_when_absent() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({})
    assert client.rotate_key("cid-1", "sk-new") is False
    core_api.patch_namespaced_secret.assert_not_called()


def test_deprovision_removes_from_both_secrets() -> None:
    client, core_api = _make_client()
    client.deprovision_key("cid-1")
    core_api.patch_namespaced_secret.assert_any_call("keys", "llm-test", {"data": {"cid-1": None}})
    core_api.patch_namespaced_secret.assert_any_call(
        "keys-blocked", "llm-test", {"data": {"cid-1": None}}
    )


def test_deprovision_raises_when_active_removal_fails() -> None:
    client, core_api = _make_client()

    def _patch(name: str, namespace: str, body: dict) -> None:
        del namespace, body
        if name == "keys":
            raise ApiException(status=500)

    core_api.patch_namespaced_secret.side_effect = _patch
    with pytest.raises(EnvoyAIGatewayBackendError):
        client.deprovision_key("cid-1")


def test_deprovision_tolerates_blocked_removal_failure() -> None:
    client, core_api = _make_client()

    def _patch(name: str, namespace: str, body: dict) -> None:
        del namespace, body
        if name == "keys-blocked":
            raise ApiException(status=500)

    core_api.patch_namespaced_secret.side_effect = _patch
    client.deprovision_key("cid-1")  # best-effort on blocked: must not raise


def test_block_moves_active_to_blocked() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({"cid-1": "sk-secret"})

    assert client.block("cid-1") is True

    core_api.patch_namespaced_secret.assert_any_call(
        "keys-blocked", "llm-test", {"stringData": {"cid-1": "sk-secret"}}
    )
    core_api.patch_namespaced_secret.assert_any_call("keys", "llm-test", {"data": {"cid-1": None}})


def test_block_returns_false_when_absent() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({})
    assert client.block("cid-x") is False


def test_block_rolls_back_blocked_copy_when_active_clear_fails() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({"cid-1": "sk-secret"})

    calls: list = []

    def _patch(name: str, namespace: str, body: dict) -> None:
        del namespace
        calls.append((name, body))
        if name == "keys" and body == {"data": {"cid-1": None}}:
            raise ApiException(status=500)

    core_api.patch_namespaced_secret.side_effect = _patch
    with pytest.raises(EnvoyAIGatewayBackendError):
        client.block("cid-1")
    # the blocked copy must be rolled back so the key is not left in both Secrets
    assert ("keys-blocked", {"data": {"cid-1": None}}) in calls


def test_unblock_moves_blocked_to_active() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({"cid-1": "sk-secret"})

    assert client.unblock("cid-1") is True

    core_api.patch_namespaced_secret.assert_any_call(
        "keys", "llm-test", {"stringData": {"cid-1": "sk-secret"}}
    )
    core_api.patch_namespaced_secret.assert_any_call(
        "keys-blocked", "llm-test", {"data": {"cid-1": None}}
    )


def test_unblock_returns_false_when_absent() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({})
    assert client.unblock("cid-x") is False


def test_read_value_handles_missing_secret() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.side_effect = ApiException(status=404)
    assert client.is_active("cid-1") is False


def test_ping_returns_false_on_connection_error() -> None:
    # A connection/DNS failure raises a non-ApiException (e.g. urllib3 MaxRetryError),
    # which must not escape ping() — it exists to report (un)availability as a bool.
    client, core_api = _make_client()
    core_api.read_namespaced_secret.side_effect = OSError("connection refused")
    assert client.ping() is False


def test_exists_true_when_key_in_active_secret() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({"cid-1": "sk-a"})
    assert client.exists("cid-1") is True


def test_exists_false_when_absent_from_both_secrets() -> None:
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with({})
    assert client.exists("cid-x") is False


def test_exists_finds_key_only_in_blocked_secret() -> None:
    client, core_api = _make_client()

    def _read(name: str, namespace: str) -> mock.Mock:
        del namespace
        return _secret_with({"cid-1": "sk-a"}) if name == "keys-blocked" else _secret_with({})

    core_api.read_namespaced_secret.side_effect = _read
    assert client.exists("cid-1") is True


def test_list_client_ids_matches_own_keys_across_both_secrets() -> None:
    client, core_api = _make_client()

    def _read(name: str, namespace: str) -> mock.Mock:
        del namespace
        if name == "keys":
            return _secret_with({"res-1-0": "sk-a"})
        return _secret_with({"res-1-1": "sk-b"})  # blocked

    core_api.read_namespaced_secret.side_effect = _read
    assert client.list_client_ids("res-1") == ["res-1-0", "res-1-1"]


def test_list_client_ids_excludes_sibling_resource_keys() -> None:
    # A prefix match would let "res-1" capture "res-10-0" and "res-1-extra-0";
    # the exact "<backend_id>-<digits>" pattern must exclude both.
    client, core_api = _make_client()
    core_api.read_namespaced_secret.return_value = _secret_with(
        {
            "res-1-0": "sk-own",
            "res-1-2": "sk-own",
            "res-10-0": "sk-sibling",
            "res-1-extra-0": "sk-sibling",
        }
    )
    assert client.list_client_ids("res-1") == ["res-1-0", "res-1-2"]


def test_patch_error_is_wrapped() -> None:
    client, core_api = _make_client()
    core_api.patch_namespaced_secret.side_effect = ApiException(status=500)
    with pytest.raises(EnvoyAIGatewayBackendError):
        client.provision_key("cid-1", "sk-secret")


@mock.patch("waldur_site_agent_envoy_ai_gateway.client.k8s_client")
@mock.patch("waldur_site_agent_envoy_ai_gateway.client.k8s_config")
def test_explicit_context_pins_cluster(mock_config: mock.Mock, mock_k8s_client: mock.Mock) -> None:
    del mock_k8s_client
    EnvoyAIGatewayClient({"namespace": "llm-test", "kube_context": "docker-desktop"})
    mock_config.load_kube_config.assert_called_once_with(config_file=None, context="docker-desktop")
    mock_config.load_incluster_config.assert_not_called()


@mock.patch("waldur_site_agent_envoy_ai_gateway.client.k8s_config")
def test_patches_are_sent_as_strategic_merge(mock_config: mock.Mock) -> None:
    """The `{key: None}` delete idiom is merge-patch semantics.

    The generated Kubernetes client defaults to `application/json-patch+json`, which
    requires an array of operations and rejects the dict bodies this client sends, so
    the content type has to be pinned. Asserted at the transport boundary because the
    other tests inject `core_api` and never exercise header negotiation.
    """
    del mock_config
    client = EnvoyAIGatewayClient({"namespace": "llm-test", "apikey_secret": "keys"})

    captured: dict = {}

    def _spy(method: str, url: str, **kwargs: object) -> None:
        del method, url
        captured["headers"] = kwargs.get("headers")
        captured["body"] = kwargs.get("body")
        raise ApiException(status=500)

    client.core_api.api_client.rest_client.request = _spy

    with pytest.raises(EnvoyAIGatewayBackendError):
        client.provision_key("cid-1", "sk-secret")

    assert captured["headers"]["Content-Type"] == "application/strategic-merge-patch+json"
    assert captured["body"] == {"stringData": {"cid-1": "sk-secret"}}
