"""Tests for the Envoy AI Gateway backend (Kubernetes client mocked)."""

from __future__ import annotations

import logging
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest
from waldur_site_agent_envoy_ai_gateway.backend import EnvoyAIGatewayBackend
from waldur_site_agent_envoy_ai_gateway.client import EnvoyAIGatewayBackendError

from waldur_site_agent.backend.exceptions import BackendError

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {
    "namespace": "llm-test",
    "gateway_url": "https://llm-ng.hpc.ut.ee",
    "apikey_secret": "keys",
}


class _FakeLimits:
    def __init__(self, data: dict) -> None:
        self._data = data

    def to_dict(self) -> dict:
        return dict(self._data)


def _make_backend() -> EnvoyAIGatewayBackend:
    with mock.patch("waldur_site_agent_envoy_ai_gateway.backend.EnvoyAIGatewayClient"):
        backend = EnvoyAIGatewayBackend(dict(SETTINGS), dict(COMPONENTS))
    backend.gateway_client = mock.MagicMock()
    return backend


def _make_resource(limits: Optional[dict] = None, backend_id: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid.uuid4(),
        name="Test key",
        slug="test-key",
        backend_id=backend_id,
        limits=_FakeLimits(limits) if limits is not None else None,
    )


def test_requires_gateway_url() -> None:
    with (
        mock.patch("waldur_site_agent_envoy_ai_gateway.backend.EnvoyAIGatewayClient"),
        pytest.raises(BackendError),
    ):
        EnvoyAIGatewayBackend({"namespace": "llm-test"}, dict(COMPONENTS))


def test_list_components() -> None:
    assert set(_make_backend().list_components()) == {"input_tokens", "output_tokens"}


def test_create_resource_does_not_mint_a_key() -> None:
    # Keys are generated separately (generate_resource_keys) and pushed to Waldur;
    # _provision only registers the resource.
    backend = _make_backend()
    resource = _make_resource(limits={"input_tokens": 1000})

    info = backend.create_resource(resource)

    assert info.backend_id == resource.uuid.hex
    assert info.endpoints[0]["url"] == "https://llm-ng.hpc.ut.ee/v1"
    assert info.backend_metadata == {}
    backend.gateway_client.provision_key.assert_not_called()


def test_generate_resource_keys_makes_two_by_default() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = []

    keys = backend.generate_resource_keys("res-1")

    assert [k["client_id"] for k in keys] == ["res-1-1", "res-1-2"]
    assert all(k["api_key"].startswith("sk-") for k in keys)
    assert backend.gateway_client.provision_key.call_count == 2


def test_generate_resource_keys_continues_past_existing() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["res-1-1", "res-1-2"]

    keys = backend.generate_resource_keys("res-1", count=1)

    assert [k["client_id"] for k in keys] == ["res-1-3"]


def test_rotate_resource_key_generates_and_applies() -> None:
    backend = _make_backend()
    backend.gateway_client.rotate_key.return_value = True

    new_key = backend.rotate_resource_key("res-1-1")

    assert new_key.startswith("sk-")
    backend.gateway_client.rotate_key.assert_called_once_with("res-1-1", new_key)
    backend.gateway_client.provision_key.assert_not_called()


def test_rotate_resource_key_provisions_when_absent() -> None:
    backend = _make_backend()
    backend.gateway_client.rotate_key.return_value = False
    backend.gateway_client.list_client_ids.return_value = []

    new_key = backend.rotate_resource_key("res-1-1")

    backend.gateway_client.provision_key.assert_called_once_with("res-1-1", new_key, blocked=False)


def test_rotate_fallback_stays_blocked_on_paused_resource() -> None:
    # rotate found no entry to overwrite; the resource's sibling keys are all
    # blocked (paused), so the re-applied key must land blocked, not resurrect the
    # resource on the active Secret.
    backend = _make_backend()
    backend.gateway_client.rotate_key.return_value = False
    backend.gateway_client.list_client_ids.return_value = ["res-1-1", "res-1-2"]
    backend.gateway_client.is_active.return_value = False

    new_key = backend.rotate_resource_key("res-1-1")

    backend.gateway_client.provision_key.assert_called_once_with("res-1-1", new_key, blocked=True)


def test_generate_resource_keys_blocks_new_key_on_paused_resource() -> None:
    # An add to a paused resource (all existing keys blocked) must land blocked,
    # or the add would silently un-pause it and bypass quota enforcement.
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["res-1-1"]
    backend.gateway_client.is_active.return_value = False

    backend.generate_resource_keys("res-1", count=1)

    backend.gateway_client.provision_key.assert_called_once_with("res-1-2", mock.ANY, blocked=True)


def test_generate_resource_keys_active_when_resource_live() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["res-1-1"]
    backend.gateway_client.is_active.return_value = True

    backend.generate_resource_keys("res-1", count=1)

    backend.gateway_client.provision_key.assert_called_once_with("res-1-2", mock.ANY, blocked=False)


def test_revoke_resource_key_removes_one() -> None:
    backend = _make_backend()
    backend.revoke_resource_key("res-1-1")
    backend.gateway_client.deprovision_key.assert_called_once_with("res-1-1")


def test_create_resource_with_id_uses_given_id() -> None:
    # The site-agent processor calls create_resource_with_id with its own backend id.
    backend = _make_backend()
    info = backend.create_resource_with_id(_make_resource(), "agent-backend-id")
    assert info.backend_id == "agent-backend-id"
    backend.gateway_client.provision_key.assert_not_called()


def test_backend_declares_api_key_support() -> None:
    assert _make_backend().supports_resource_api_keys is True


def test_pull_resource_detects_existing_key() -> None:
    # Idempotency: an existing resource (>=1 key) must be reported so the order
    # processor skips re-creation instead of regenerating keys.
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["cid-1-1"]
    info = backend.pull_resource(_make_resource(backend_id="cid-1"))
    assert info is not None
    assert info.backend_id == "cid-1"


def test_pull_resource_none_when_absent() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = []
    assert backend.pull_resource(_make_resource(backend_id="cid-1")) is None


def test_recreate_missing_resource_never_recreates() -> None:
    # The agent does not hold prior key values, so a forced sync never
    # regenerates keys behind the user's back — it only reports state.
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = []
    assert backend.recreate_missing_resource(_make_resource(backend_id="cid-1")) is False
    backend.gateway_client.provision_key.assert_not_called()


def test_pause_blocks_every_key() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["cid-1-1", "cid-1-2"]
    backend.gateway_client.block.return_value = True
    assert backend.pause_resource("cid-1") is True
    assert backend.gateway_client.block.call_count == 2


def test_pause_failure_logs_error(caplog: pytest.LogCaptureFixture) -> None:
    # A swallowed pause silently defeats quota enforcement, so it must be loud (ERROR).
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["cid-1-1"]
    backend.gateway_client.block.side_effect = EnvoyAIGatewayBackendError("boom")
    with caplog.at_level(logging.ERROR):
        assert backend.pause_resource("cid-1") is False
    assert any(record.levelno == logging.ERROR for record in caplog.records)


def test_restore_unblocks_every_key() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["cid-1-1", "cid-1-2"]
    backend.gateway_client.unblock.return_value = True
    assert backend.restore_resource("cid-1") is True
    assert backend.gateway_client.unblock.call_count == 2


def test_delete_resource_deprovisions_every_key() -> None:
    backend = _make_backend()
    backend.gateway_client.list_client_ids.return_value = ["cid-1-1", "cid-1-2"]
    backend.delete_resource(_make_resource(backend_id="cid-1"))
    assert backend.gateway_client.deprovision_key.call_count == 2


def test_usage_report_is_empty() -> None:
    # Usage is handled by the separate reporting backend.
    assert _make_backend()._get_usage_report(["cid-1"]) == {}


def test_create_does_not_push_quota() -> None:
    # Limits are enforced by Waldur (report -> pause), not the gateway: provisioning
    # must never touch the (now-removed) gateway quota path.
    backend = _make_backend()
    backend.create_resource(_make_resource(limits={"input_tokens": 1000, "output_tokens": 500}))
    backend.gateway_client.set_key_limits.assert_not_called()


def test_set_resource_limits_is_noop() -> None:
    # Kept for the limit-change order path, but there is nothing to push to the gateway.
    backend = _make_backend()
    backend.set_resource_limits("cid-1", {"input_tokens": 20, "output_tokens": 10})
    backend.gateway_client.set_key_limits.assert_not_called()
    backend.gateway_client.remove_key_limit.assert_not_called()


def test_collect_resource_limits_returns_empty() -> None:
    # Limits live in Waldur, not the backend.
    backend = _make_backend()
    assert backend._collect_resource_limits(_make_resource(limits={"input_tokens": 5})) == ({}, {})
