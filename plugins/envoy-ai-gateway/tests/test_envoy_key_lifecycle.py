"""Per-key lifecycle of the Envoy backend, against an in-memory Secret store.

The client and backend run for real here; only the Kubernetes API is faked, so the
assertions are about where each key's entry ends up — which is what the gateway reads.
"""

from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException
from waldur_site_agent_envoy_ai_gateway.backend import EnvoyAIGatewayBackend
from waldur_site_agent_envoy_ai_gateway.client import (
    EnvoyAIGatewayBackendError,
    EnvoyAIGatewayClient,
)

from waldur_site_agent.backend.exceptions import BackendError

ACTIVE = "keys"
BLOCKED = "keys-blocked"
SETTINGS = {
    "namespace": "llm-test",
    "gateway_url": "https://gateway.example.com",
    "apikey_secret": ACTIVE,
}
COMPONENTS = {"input_tokens": {}, "output_tokens": {}}


class FakeCoreApi:
    """Secrets as plain dicts, patched with strategic-merge semantics."""

    def __init__(self, secrets: Optional[dict[str, dict[str, str]]] = None) -> None:
        self.secrets = {ACTIVE: {}, BLOCKED: {}}
        for name, entries in (secrets or {}).items():
            self.secrets[name] = dict(entries)
        self.fail_patch_on: set[str] = set()

    def read_namespaced_secret(self, name: str, namespace: str) -> SimpleNamespace:
        del namespace
        if name not in self.secrets:
            raise ApiException(status=404)
        return SimpleNamespace(
            data={
                key: base64.b64encode(value.encode()).decode()
                for key, value in self.secrets[name].items()
            }
        )

    def patch_namespaced_secret(self, name: str, namespace: str, body: dict) -> None:
        del namespace
        if name in self.fail_patch_on:
            raise ApiException(status=500)
        data = self.secrets[name]
        for key, value in (body.get("data") or {}).items():
            if value is None:
                data.pop(key, None)
            else:
                data[key] = base64.b64decode(value).decode()
        data.update(body.get("stringData") or {})


def _backend(
    secrets: Optional[dict[str, dict[str, str]]] = None,
) -> tuple[EnvoyAIGatewayBackend, FakeCoreApi]:
    core_api = FakeCoreApi(secrets)
    with mock.patch("waldur_site_agent_envoy_ai_gateway.backend.EnvoyAIGatewayClient"):
        backend = EnvoyAIGatewayBackend(dict(SETTINGS), dict(COMPONENTS))
    backend.gateway_client = EnvoyAIGatewayClient(dict(SETTINGS), core_api=core_api)
    return backend, core_api


def _two_live_keys() -> dict[str, dict[str, str]]:
    return {ACTIVE: {"res-1-1": "sk-one", "res-1-2": "sk-two"}}


def test_backend_declares_the_lifecycle() -> None:
    backend, _ = _backend()
    assert backend.supports_resource_api_keys is True
    assert backend.supports_resource_api_key_lifecycle is True


# --- pause / resume -----------------------------------------------------------


def test_pausing_one_key_leaves_its_siblings_serving() -> None:
    backend, api = _backend(_two_live_keys())

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_resuming_a_key_returns_it_with_its_value() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")

    backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {}


def test_resource_restore_leaves_a_paused_key_paused() -> None:
    # The membership sync restores every non-paused resource on each cycle. If that
    # moved a key paused on its own back, a per-key pause would last one cycle.
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")
    backend.pause_resource("res-1")

    assert api.secrets[ACTIVE] == {}
    backend.restore_resource("res-1")

    assert api.secrets[ACTIVE] == {"res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_pausing_a_key_of_a_paused_resource_outlasts_the_restore() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource("res-1")

    backend.pause_resource_key("res-1-1", "res-1")
    backend.restore_resource("res-1")

    assert api.secrets[ACTIVE] == {"res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_resuming_a_key_of_a_paused_resource_keeps_it_blocked_until_the_restore() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")
    backend.pause_resource("res-1")

    backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {
        "res-1-1": "sk-one",
        "res-1-2": "sk-two",
        "res-1.resource-paused": "paused",
    }
    backend.restore_resource("res-1")
    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}


def test_resuming_the_only_paused_key_of_a_live_resource_makes_it_active() -> None:
    # With every key paused on its own account there is no sibling to say the
    # resource is paused, and it is not.
    backend, api = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})

    backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one"}


def test_pause_is_idempotent() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}
    assert "res-1-1" not in api.secrets[ACTIVE]


def test_replayed_pause_clears_a_live_copy_left_by_an_interrupted_one() -> None:
    backend, api = _backend(
        {ACTIVE: {"res-1-1": "sk-one"}, BLOCKED: {"res-1-1.paused": "sk-one"}}
    )

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_pausing_a_key_with_no_entry_succeeds() -> None:
    # It cannot authenticate, which is what a pause asks for.
    backend, api = _backend()
    backend.pause_resource_key("res-1-1", "res-1")
    assert api.secrets == {ACTIVE: {}, BLOCKED: {}}


def test_pause_rolls_back_when_the_live_copy_cannot_be_cleared() -> None:
    backend, api = _backend(_two_live_keys())
    api.fail_patch_on.add(ACTIVE)

    with pytest.raises(EnvoyAIGatewayBackendError):
        backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {}


def test_resume_rolls_back_when_the_paused_copy_cannot_be_cleared() -> None:
    backend, api = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})
    real_patch = api.patch_namespaced_secret

    def _patch(name: str, namespace: str, body: dict) -> None:
        if name == BLOCKED:
            raise ApiException(status=500)
        real_patch(name, namespace, body)

    api.patch_namespaced_secret = _patch  # type: ignore[method-assign]

    with pytest.raises(EnvoyAIGatewayBackendError):
        backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_resuming_a_key_that_is_not_paused_is_a_no_op() -> None:
    backend, api = _backend(_two_live_keys())
    backend.resume_resource_key("res-1-1", "res-1")
    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}


def test_resuming_a_key_with_no_entry_fails() -> None:
    backend, _ = _backend()
    with pytest.raises(BackendError, match="rotate"):
        backend.resume_resource_key("res-1-1", "res-1")


def test_rotating_a_key_left_paused_makes_it_serve() -> None:
    # Waldur rotates only an OK or Erred key and settles it OK. A key still paused
    # at the gateway — its pause applied, the acknowledgement lost — must serve once
    # Waldur shows it OK, or Waldur reveals a key the gateway rejects.
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")

    new_key = backend.rotate_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": new_key, "res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {}


def test_rotating_a_key_of_a_paused_resource_keeps_it_blocked() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource("res-1")

    new_key = backend.rotate_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED]["res-1-1"] == new_key


# --- delete -------------------------------------------------------------------


@pytest.mark.parametrize(
    "secrets",
    [
        {ACTIVE: {"res-1-1": "sk-one"}},
        {BLOCKED: {"res-1-1": "sk-one"}},
        {BLOCKED: {"res-1-1.paused": "sk-one"}},
        {},
    ],
    ids=["active", "resource-paused", "key-paused", "already-gone"],
)
def test_deleting_a_key_removes_its_entry_wherever_it_is(
    secrets: dict[str, dict[str, str]],
) -> None:
    backend, api = _backend({**secrets, ACTIVE: {**secrets.get(ACTIVE, {}), "res-1-2": "sk-two"}})

    backend.delete_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-2": "sk-two"}
    assert api.secrets[BLOCKED] == {}


def test_deleting_the_resource_removes_paused_keys_too() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource_key("res-1-1", "res-1")

    backend.delete_resource(SimpleNamespace(uuid="u", backend_id="res-1"))

    assert api.secrets == {ACTIVE: {}, BLOCKED: {}}


# --- mint ---------------------------------------------------------------------


def test_minting_adds_one_key_and_leaves_the_others_alone() -> None:
    backend, api = _backend(_two_live_keys())

    key = backend.mint_resource_key("res-1", reserved_client_ids=["res-1-1", "res-1-2"])

    assert key["client_id"] == "res-1-3"
    assert key["api_key"].startswith("sk-")
    assert api.secrets[ACTIVE] == {
        "res-1-1": "sk-one",
        "res-1-2": "sk-two",
        "res-1-3": key["api_key"],
    }


def test_minting_skips_a_deleted_keys_client_id() -> None:
    # res-1-1 was deleted: its entry is gone but Waldur keeps its row and refuses
    # the identifier, since usage is attributed by it.
    backend, _ = _backend({ACTIVE: {"res-1-2": "sk-two"}})

    key = backend.mint_resource_key("res-1", reserved_client_ids=["res-1-1", "res-1-2"])

    assert key["client_id"] == "res-1-3"


def test_minting_skips_a_paused_keys_client_id() -> None:
    backend, _ = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})
    key = backend.mint_resource_key("res-1", reserved_client_ids=[])
    assert key["client_id"] == "res-1-2"


def test_a_key_minted_onto_a_paused_resource_lands_blocked() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource("res-1")

    key = backend.mint_resource_key("res-1", reserved_client_ids=[])

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED][key["client_id"]] == key["api_key"]


def test_the_resource_pause_is_recorded_even_with_no_key_to_block() -> None:
    # Both keys paused on their own (or deleted), then the resource hits its
    # limit: nothing is left to block, but a key requested now must not serve.
    backend, api = _backend(
        {BLOCKED: {"res-1-1.paused": "sk-one", "res-1-2.paused": "sk-two"}}
    )
    backend.pause_resource("res-1")

    key = backend.mint_resource_key("res-1", reserved_client_ids=[])

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED][key["client_id"]] == key["api_key"]
    backend.restore_resource("res-1")
    assert api.secrets[ACTIVE] == {key["client_id"]: key["api_key"]}
    assert "res-1.resource-paused" not in api.secrets[BLOCKED]


def test_resuming_beside_only_paused_keys_on_a_paused_resource_stays_blocked() -> None:
    backend, api = _backend(
        {BLOCKED: {"res-1-1.paused": "sk-one", "res-1-2.paused": "sk-two"}}
    )
    backend.pause_resource("res-1")

    backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED]["res-1-1"] == "sk-one"


def test_restore_writes_nothing_when_no_pause_is_recorded() -> None:
    # restore_resource runs on every membership-sync cycle.
    backend, api = _backend(_two_live_keys())
    with mock.patch.object(api, "patch_namespaced_secret") as patch:
        backend.restore_resource("res-1")
    patch.assert_not_called()


def test_restore_does_not_revive_a_key_paused_on_its_own() -> None:
    # Residue of a key pause that raced a resource pause: both entries exist.
    backend, api = _backend({BLOCKED: {"res-1-1": "sk-one", "res-1-1.paused": "sk-one"}})

    backend.restore_resource("res-1")

    assert api.secrets[ACTIVE] == {}


def test_a_key_minted_beside_only_paused_keys_is_live() -> None:
    backend, api = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})

    key = backend.mint_resource_key("res-1", reserved_client_ids=[])

    assert api.secrets[ACTIVE] == {key["client_id"]: key["api_key"]}


def test_minting_with_a_model_allowlist_is_refused_before_anything_is_applied() -> None:
    backend, api = _backend(_two_live_keys())

    with pytest.raises(BackendError, match="models"):
        backend.mint_resource_key("res-1", reserved_client_ids=[], allowed_models=["gpt"])

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}


# --- update -------------------------------------------------------------------


def test_update_accepts_limits_without_touching_the_gateway() -> None:
    backend, api = _backend(_two_live_keys())

    backend.update_resource_key("res-1-1", "res-1", limits={"input_tokens": 1000})

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one", "res-1-2": "sk-two"}


def test_updating_a_key_left_paused_makes_it_serve() -> None:
    # An erred resume retried as an update (the user cleared the model list) is
    # settled OK by Waldur, so the key must come out of its pause.
    backend, api = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})

    backend.update_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": "sk-one"}
    assert api.secrets[BLOCKED] == {}


def test_updating_a_key_with_no_entry_fails() -> None:
    backend, _ = _backend()
    with pytest.raises(BackendError, match="rotate"):
        backend.update_resource_key("res-1-1", "res-1")


@pytest.mark.parametrize("method", ["update_resource_key", "resume_resource_key"])
def test_a_model_allowlist_is_refused(method: str) -> None:
    backend, api = _backend({BLOCKED: {"res-1-1.paused": "sk-one"}})

    with pytest.raises(BackendError, match="models"):
        getattr(backend, method)("res-1-1", "res-1", allowed_models=["gpt"])

    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


@pytest.mark.parametrize("allowed_models", [None, []])
def test_an_empty_model_allowlist_is_accepted(allowed_models: Optional[list]) -> None:
    backend, _ = _backend(_two_live_keys())
    backend.update_resource_key("res-1-1", "res-1", allowed_models=allowed_models)


# --- key_states ---------------------------------------------------------------


def test_key_states_reports_where_each_key_lives() -> None:
    backend, _ = _backend(
        {
            ACTIVE: {"res-1-1": "a", "res-10-1": "other"},
            BLOCKED: {"res-1-2": "b", "res-1-3.paused": "c", "res-1-x.paused": "junk"},
        }
    )
    assert backend.gateway_client.key_states("res-1") == {
        "res-1-1": "active",
        "res-1-2": "blocked",
        "res-1-3": "paused",
    }
    assert backend.list_resource_client_ids("res-1") == ["res-1-1", "res-1-2", "res-1-3"]


# --- residue of interrupted or racing moves -------------------------------------


def test_pausing_clears_a_live_copy_beside_a_blocked_one() -> None:
    # A restore caught between writing the active copy and clearing the blocked one.
    backend, api = _backend({ACTIVE: {"res-1-1": "sk-one"}, BLOCKED: {"res-1-1": "sk-one"}})

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-one"}


def test_rotating_a_key_with_both_blocked_entries_installs_the_new_value() -> None:
    backend, api = _backend({BLOCKED: {"res-1-1": "sk-old", "res-1-1.paused": "sk-old"}})

    new_key = backend.rotate_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": new_key}
    assert api.secrets[BLOCKED] == {}


def test_deleting_a_paused_resource_clears_its_pause_marker() -> None:
    backend, api = _backend(_two_live_keys())
    backend.pause_resource("res-1")

    backend.delete_resource(SimpleNamespace(uuid="u", backend_id="res-1"))

    assert api.secrets == {ACTIVE: {}, BLOCKED: {}}


def test_a_steady_resource_pause_writes_nothing() -> None:
    # pause_resource runs on every membership-sync cycle for a paused resource.
    backend, api = _backend(_two_live_keys())
    backend.pause_resource("res-1")
    with mock.patch.object(api, "patch_namespaced_secret") as patch:
        backend.pause_resource("res-1")
    patch.assert_not_called()


def test_deleting_a_key_fails_when_its_blocked_entry_cannot_be_removed() -> None:
    # Acknowledged as deleted, the surviving entry would come back live on the next
    # resource restore.
    backend, api = _backend({BLOCKED: {"res-1-1": "sk-one"}})
    api.fail_patch_on.add(BLOCKED)

    with pytest.raises(EnvoyAIGatewayBackendError):
        backend.delete_resource_key("res-1-1", "res-1")


def test_rotating_a_live_key_drops_a_stale_paused_copy() -> None:
    backend, api = _backend({ACTIVE: {"res-1-1": "sk-old"}, BLOCKED: {"res-1-1.paused": "sk-old"}})

    new_key = backend.rotate_resource_key("res-1-1", "res-1")
    backend.pause_resource_key("res-1-1", "res-1")
    backend.resume_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {"res-1-1": new_key}
    assert api.secrets[BLOCKED] == {}


def test_pausing_keeps_the_live_value_over_a_stale_paused_copy() -> None:
    backend, api = _backend({ACTIVE: {"res-1-1": "sk-new"}, BLOCKED: {"res-1-1.paused": "sk-old"}})

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-new"}


def test_pausing_prefers_a_live_value_over_a_plain_blocked_copy() -> None:
    backend, api = _backend({ACTIVE: {"res-1-1": "sk-new"}, BLOCKED: {"res-1-1": "sk-old"}})

    backend.pause_resource_key("res-1-1", "res-1")

    assert api.secrets[ACTIVE] == {}
    assert api.secrets[BLOCKED] == {"res-1-1.paused": "sk-new"}
