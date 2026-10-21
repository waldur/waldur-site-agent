"""Tests for the per-key API key lifecycle of the LiteLLM backend (HTTP client mocked).

A key is created, paused, resumed, reconfigured and deleted on its own, and keeps its
own pause, limits and model list through everything the resource goes through.
"""

from __future__ import annotations

from typing import Optional
from unittest import mock

import pytest
from waldur_site_agent_litellm.backend import (
    META_KEY_LIMITS,
    META_KEY_MODELS,
    META_KEY_PAUSED,
    META_RESOURCE_LIMITS,
    NO_MODELS,
    LiteLLMBackend,
)
from waldur_site_agent_litellm.client import LiteLLMEnterpriseFeatureError

from waldur_site_agent.backend import ALL_MODELS
from waldur_site_agent.backend.exceptions import BackendError

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {
    "api_url": "https://litellm.example.com/",
    "api_token": "sk-master",
    "models": ["gpt-4o", "gpt-4o-mini"],
}
RID = "abc123"


def _make_backend(settings: Optional[dict] = None) -> LiteLLMBackend:
    with mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}
    return backend


def _key(
    alias: str,
    token: str,
    *,
    blocked: bool = False,
    metadata: Optional[dict] = None,
    **fields: object,
) -> dict:
    record = {"key_alias": alias, "token": token, "blocked": blocked, **fields}
    if metadata is not None:
        record["metadata"] = metadata
    return record


def _self_paused(alias: str, token: str, **fields: object) -> dict:
    return _key(alias, token, blocked=True, metadata={META_KEY_PAUSED: True}, **fields)


def _update_for(backend: LiteLLMBackend, token: str) -> dict:
    """The body of the one ``/key/update`` sent for ``token``."""
    calls = [
        call for call in backend.litellm_client.update_key.call_args_list
        if call.args[0] == token
    ]
    assert len(calls) == 1, calls
    return calls[0].args[1]


# --- capability ---------------------------------------------------------------


def test_the_backend_declares_the_per_key_lifecycle() -> None:
    # Without it the core refuses every command but rotate and errs the key.
    assert LiteLLMBackend.supports_resource_api_key_lifecycle is True


# --- mint ---------------------------------------------------------------------


def test_mint_takes_the_next_slot_neither_the_proxy_nor_waldur_holds() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]

    # Slot 3 belonged to a key deleted last week: gone from the proxy, still held by
    # Waldur, which attributes usage by alias and refuses a reused one.
    key = backend.mint_resource_key(RID, [f"{RID}-1", f"{RID}-2", f"{RID}-3"])

    assert key == {"client_id": f"{RID}-4", "api_key": "sk-new"}
    assert backend.litellm_client.generate_key.call_args.args[0] == f"{RID}-4"


def test_mint_records_the_keys_own_limits_and_its_resources() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]

    backend.mint_resource_key(RID, [], limits={"rpm": 30, "input_tokens": 1000})

    metadata = backend.litellm_client.generate_key.call_args.kwargs["metadata"]
    # Only what the proxy has a field for: input_tokens is Waldur's to enforce.
    assert metadata[META_KEY_LIMITS] == {"rpm": 30}
    assert metadata[META_RESOURCE_LIMITS] == {"token_cost": 50}
    # The resource's budget reaches the new key now, not on the next cycle.
    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 50


def test_mint_onto_a_paused_resource_lands_blocked() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]

    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is True


def test_mint_onto_a_serving_resource_lands_serving() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]

    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


# --- pause / resume / update --------------------------------------------------


def test_pause_blocks_the_key_and_records_why_in_one_write() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", metadata={META_KEY_LIMITS: {"rpm": 30}}),
        _key(f"{RID}-2", "h2"),
    ]

    backend.pause_resource_key(f"{RID}-1", RID)

    # Blocked and recorded together: a block without the record would be undone by
    # the next resource restore.
    assert _update_for(backend, "h1") == {
        "blocked": True,
        "metadata": {META_KEY_LIMITS: {"rpm": 30}, META_KEY_PAUSED: True},
    }
    # The sibling keeps serving.
    backend.litellm_client.block.assert_not_called()
    assert backend.litellm_client.update_key.call_count == 1


def test_pausing_a_key_the_proxy_lost_succeeds_without_writing() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2", "h2")]

    backend.pause_resource_key(f"{RID}-1", RID)

    backend.litellm_client.update_key.assert_not_called()


def test_pausing_twice_writes_the_same_state() -> None:
    # The sweep replays a pause whose acknowledgement was lost.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_self_paused(f"{RID}-1", "h1")]

    backend.pause_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1") == {
        "blocked": True,
        "metadata": {META_KEY_PAUSED: True},
    }


def test_resume_lifts_the_keys_own_pause_and_applies_its_settings() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]

    backend.resume_resource_key(f"{RID}-1", RID, limits={}, allowed_models=[ALL_MODELS])

    body = _update_for(backend, "h1")
    assert body["blocked"] is False
    assert body["models"] == ["gpt-4o", "gpt-4o-mini"]
    assert body["metadata"][META_KEY_PAUSED] is False


def test_update_lifts_a_pause_whose_acknowledgement_was_lost() -> None:
    # Waldur settles an updated key OK, so it must serve afterwards.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]

    backend.update_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["blocked"] is False


def test_resume_on_a_paused_resource_keeps_the_key_blocked() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]

    backend.resume_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["blocked"] is True


def test_resuming_a_key_the_proxy_lost_fails() -> None:
    # Reporting it OK would show a key in the portal that cannot authenticate.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2", "h2")]

    with pytest.raises(BackendError):
        backend.resume_resource_key(f"{RID}-1", RID)


# --- delete -------------------------------------------------------------------


def test_delete_removes_only_that_key() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]

    backend.delete_resource_key(f"{RID}-1", RID)

    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])


def test_deleting_a_key_that_is_already_gone_succeeds() -> None:
    # The core's withdraw path and the sweep both delete keys that may be gone.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2", "h2")]

    backend.delete_resource_key(f"{RID}-1", RID)

    backend.litellm_client.delete_keys.assert_not_called()


# --- the resource around a key ------------------------------------------------


def test_restoring_the_resource_leaves_a_self_paused_key_blocked() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2", blocked=True),
    ]
    backend.litellm_client.unblock.return_value = True

    # Membership sync calls this every cycle for an unpaused resource.
    assert backend.restore_resource(RID) is True

    backend.litellm_client.unblock.assert_called_once_with("h2")


def test_resource_limits_are_stamped_without_losing_the_keys_own_pause() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_self_paused(f"{RID}-1", "h1")]

    backend.set_resource_limits(RID, {"token_cost": 50})

    assert _update_for(backend, "h1")["metadata"] == {
        META_KEY_PAUSED: True,
        META_RESOURCE_LIMITS: {"token_cost": 50},
    }


def test_rotation_by_delete_and_mint_keeps_the_keys_models_and_metadata() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(
            f"{RID}-1",
            "h1",
            models=["gpt-4o-mini"],
            tpm_limit=100,
            metadata={META_KEY_LIMITS: {"tpm": 100}},
        ),
    ]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")

    backend.rotate_resource_key(f"{RID}-1", RID)

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    # Minting with the offering's list instead would widen the key to every model.
    assert kwargs["models"] == ["gpt-4o-mini"]
    assert kwargs["tpm_limit"] == 100
    assert kwargs["metadata"] == {META_KEY_LIMITS: {"tpm": 100}, META_KEY_PAUSED: False}


def test_rotation_lifts_a_stale_own_pause_before_regenerating() -> None:
    # Waldur rotates only a key it shows as serving, and settles it OK.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-rotated"

    assert _update_for(backend, "h1") == {
        "blocked": False,
        "metadata": {META_KEY_PAUSED: False},
    }


def _rotatable(models: Optional[list] = None, **metadata: object) -> list:
    """A key with its own wider settings, beside a sibling stamped with the resource's."""
    return [
        _key(
            f"{RID}-1",
            "h1",
            models=models or ["gpt-4o", "gpt-4o-mini"],
            max_budget=40,
            metadata={META_KEY_LIMITS: {"token_cost": 40}, **metadata},
        ),
        _key(f"{RID}-2", "h2", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]


def test_rotation_in_place_applies_waldurs_settings() -> None:
    # A key that changed hands is rotated with the settings the same edit set.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = _rotatable()
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    assert (
        backend.rotate_resource_key(
            f"{RID}-1", RID, limits={"token_cost": 10}, allowed_models=["gpt-4o"]
        )
        == "sk-rotated"
    )

    body = _update_for(backend, "h1")
    assert body["models"] == ["gpt-4o"]
    assert body["max_budget"] == 10
    assert body["blocked"] is False
    assert body["metadata"] == {
        META_KEY_LIMITS: {"token_cost": 10},
        META_RESOURCE_LIMITS: {"token_cost": 50},
        META_KEY_MODELS: ["gpt-4o"],
        META_KEY_PAUSED: False,
    }
    backend.litellm_client.regenerate_key.assert_called_once_with("h1")
    backend.litellm_client.generate_key.assert_not_called()


def test_rotation_in_place_keeps_the_tighter_resource_limit() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = _rotatable()
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(f"{RID}-1", RID, limits={"token_cost": 80}, allowed_models=[])

    body = _update_for(backend, "h1")
    assert body["max_budget"] == 50
    assert body["models"] == [NO_MODELS]


def test_rotation_by_delete_and_mint_applies_waldurs_settings() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = _rotatable()
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")

    assert (
        backend.rotate_resource_key(
            f"{RID}-1", RID, limits={"token_cost": 10}, allowed_models=["gpt-4o"]
        )
        == "sk-new"
    )

    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])
    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    # Not the record's wider list and budget: Waldur's settings for the key.
    assert kwargs["models"] == ["gpt-4o"]
    assert kwargs["max_budget"] == 10
    assert kwargs["blocked"] is False
    assert kwargs["metadata"] == {
        META_KEY_LIMITS: {"token_cost": 10},
        META_RESOURCE_LIMITS: {"token_cost": 50},
        META_KEY_MODELS: ["gpt-4o"],
        META_KEY_PAUSED: False,
    }


def test_rotation_with_settings_lifts_a_stale_own_pause() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(f"{RID}-1", RID, limits={}, allowed_models=[ALL_MODELS])

    body = _update_for(backend, "h1")
    assert body["blocked"] is False
    assert body["metadata"][META_KEY_PAUSED] is False
    assert body["models"] == SETTINGS["models"]


def test_rotation_with_a_model_the_offering_does_not_serve_changes_nothing() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = _rotatable()

    with pytest.raises(BackendError):
        backend.rotate_resource_key(f"{RID}-1", RID, limits={}, allowed_models=["o3"])

    backend.litellm_client.update_key.assert_not_called()
    backend.litellm_client.regenerate_key.assert_not_called()
    backend.litellm_client.delete_keys.assert_not_called()


# --- telling a resource pause from a key pause --------------------------------


def test_a_self_paused_key_does_not_make_its_resource_look_paused() -> None:
    # Toy story: the resource's only key was paused by its owner. A new key for a
    # colleague must serve -- the resource itself was never paused.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_self_paused(f"{RID}-1", "h1")]

    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


def test_a_paused_resource_with_no_keys_mints_its_first_key_blocked() -> None:
    # The core applies Waldur's pause right before the mint (_apply_resource_pause);
    # with no keys there is nothing on the proxy to block, only the record to keep.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.pause_resource(RID)
    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is True


def test_a_restored_resource_with_no_keys_mints_its_first_key_serving() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.pause_resource(RID)
    backend.restore_resource(RID)
    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


def test_the_pause_of_one_resource_does_not_hold_another() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.pause_resource("other")
    backend.mint_resource_key(RID, [])

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


def test_resuming_a_key_while_its_resource_is_paused_keeps_it_blocked() -> None:
    # Key 1 paused on its own, key 2 blocked by the resource's pause.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _self_paused(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2", blocked=True),
    ]

    backend.resume_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["blocked"] is True


# --- a key's own limits against the resource's --------------------------------


@pytest.mark.parametrize(
    ("key_limits", "resource_limits", "expected"),
    [
        pytest.param({"tpm": 100}, {"tpm": 500}, {"tpm": 100}, id="key-tighter"),
        pytest.param({"tpm": 900}, {"tpm": 500}, {"tpm": 500}, id="resource-tighter"),
        pytest.param({"rpm": 30}, {}, {"rpm": 30}, id="key-only"),
        pytest.param({}, {"token_cost": 50}, {"token_cost": 50}, id="resource-only"),
        pytest.param({"tpm": 0}, {"tpm": 500}, {"tpm": 500}, id="key-zero-is-no-limit"),
        pytest.param({"tpm": 0}, {}, {}, id="key-zero-and-no-resource-limit"),
        pytest.param(
            {"tpm": 100, "token_cost": 80},
            {"token_cost": 50, "rpm": 60},
            {"tpm": 100, "token_cost": 50, "rpm": 60},
            id="per-field",
        ),
    ],
)
def test_tighter_limits(key_limits: dict, resource_limits: dict, expected: dict) -> None:
    assert LiteLLMBackend._tighter_limits(key_limits, resource_limits) == expected


def test_reconciliation_keeps_a_keys_own_tighter_limit() -> None:
    # Membership sync runs this every cycle; it must not raise the key back to the
    # resource's limit.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", tpm_limit=500, metadata={META_KEY_LIMITS: {"tpm": 100}}),
    ]

    backend.set_resource_limits(RID, {"tpm": 500})

    assert _update_for(backend, "h1")["tpm_limit"] == 100


def test_a_minted_key_gets_its_own_limit_when_it_is_tighter() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]

    backend.mint_resource_key(RID, [], limits={"token_cost": 10})

    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 10


# --- a key's model list -------------------------------------------------------


def test_every_model_means_the_offerings_list() -> None:
    backend = _make_backend()
    assert backend._key_models([ALL_MODELS]) == ["gpt-4o", "gpt-4o-mini"]
    # A Waldur older than ["*"] sends no list at all.
    assert backend._key_models(None) == ["gpt-4o", "gpt-4o-mini"]


def test_a_narrower_list_is_written_as_it_is() -> None:
    assert _make_backend()._key_models(["gpt-4o-mini"]) == ["gpt-4o-mini"]


def test_a_model_the_offering_does_not_serve_is_refused() -> None:
    with pytest.raises(BackendError, match="o3"):
        _make_backend()._key_models(["gpt-4o", "o3"])


def test_no_models_is_written_as_the_reserved_name() -> None:
    # The proxy reads an empty list as every model: written as it is, the portal
    # would show a key that can call nothing while it can call everything.
    assert _make_backend()._key_models([]) == [NO_MODELS]


@pytest.mark.parametrize("settings", [SETTINGS, {**SETTINGS, "models": []}])
def test_every_model_beside_other_names_is_refused(settings: dict) -> None:
    # On an unrestricted offering "*" would otherwise be written onto the key as a name.
    with pytest.raises(BackendError):
        _make_backend(settings)._key_models([ALL_MODELS, "gpt-4o"])


def test_an_unrestricted_offering_accepts_any_name() -> None:
    backend = _make_backend({**SETTINGS, "models": []})
    assert backend._key_models(["o3"]) == ["o3"]


def test_a_refused_list_mints_nothing() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    with pytest.raises(BackendError):
        backend.mint_resource_key(RID, [], allowed_models=["o3"])

    backend.litellm_client.generate_key.assert_not_called()


def test_an_update_writes_the_keys_narrower_list() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]

    backend.update_resource_key(f"{RID}-1", RID, allowed_models=["gpt-4o-mini"])

    assert _update_for(backend, "h1")["models"] == ["gpt-4o-mini"]


def test_an_update_to_no_models_leaves_the_key_none() -> None:
    # Waldur sends it when the key's only model is switched off on its resource.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]

    backend.update_resource_key(f"{RID}-1", RID, allowed_models=[])

    assert _update_for(backend, "h1")["models"] == [NO_MODELS]


# --- races with the every-cycle reconciliations --------------------------------


def test_resuming_the_only_key_of_a_paused_resource_keeps_it_blocked() -> None:
    # Every key is paused on its own, so the keys say nothing about the resource. The
    # core applies Waldur's pause on the same backend right before the resume.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_self_paused(f"{RID}-1", "h1")]
    backend.litellm_client.block.return_value = True

    backend.pause_resource(RID)
    backend.resume_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["blocked"] is True


def test_restore_leaves_a_serving_key_alone() -> None:
    # It runs every cycle; unblocking a key that is not blocked is a write for nothing.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]

    backend.restore_resource(RID)

    backend.litellm_client.unblock.assert_not_called()


def test_restore_does_not_undo_a_key_pause_that_landed_after_its_listing() -> None:
    # Listed blocked by the resource's pause; paused on its own by the time it is read
    # again. Unblocking it would undo a pause Waldur has already been told of.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1", blocked=True)]
    backend.litellm_client.get_key.return_value = _self_paused(f"{RID}-1", "h1")

    backend.restore_resource(RID)

    backend.litellm_client.unblock.assert_not_called()


def test_limits_are_written_onto_the_keys_metadata_as_it_is_now() -> None:
    # The listing predates a pause that /key/update would otherwise drop: it replaces
    # metadata wholesale.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.return_value = _self_paused(f"{RID}-1", "h1")

    backend.set_resource_limits(RID, {"token_cost": 50})

    assert _update_for(backend, "h1")["metadata"] == {
        META_KEY_PAUSED: True,
        META_RESOURCE_LIMITS: {"token_cost": 50},
    }


def test_removing_a_keys_own_limit_returns_it_to_the_resources() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(
            f"{RID}-1",
            "h1",
            tpm_limit=100,
            metadata={META_KEY_LIMITS: {"tpm": 100}, META_RESOURCE_LIMITS: {"tpm": 500}},
        ),
    ]

    backend.update_resource_key(f"{RID}-1", RID, limits={})

    assert _update_for(backend, "h1")["tpm_limit"] == 500


def test_a_key_pause_is_written_onto_the_keys_metadata_as_it_is_now() -> None:
    # A limits reconciliation stamped the resource's limits since the listing was taken;
    # /key/update replaces metadata, so writing the listing's copy would drop them.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.return_value = _key(
        f"{RID}-1", "h1", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}
    )

    backend.pause_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["metadata"] == {
        META_RESOURCE_LIMITS: {"token_cost": 50},
        META_KEY_PAUSED: True,
    }


def test_a_resume_is_written_onto_the_keys_metadata_as_it_is_now() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_self_paused(f"{RID}-1", "h1"), _key(f"{RID}-2", "h2")]
    backend.litellm_client.get_key.return_value = _key(
        f"{RID}-1", "h1", metadata={META_KEY_PAUSED: True, "operator_note": "keep me"}
    )

    backend.resume_resource_key(f"{RID}-1", RID)

    assert _update_for(backend, "h1")["metadata"]["operator_note"] == "keep me"


def test_restore_re_blocks_a_key_paused_on_its_own_that_is_serving() -> None:
    # The residue of a pause that landed between an earlier restore's re-read and its
    # unblock: nothing else would ever block it again.
    backend = _make_backend()
    serving = _key(f"{RID}-1", "h1", blocked=False, metadata={META_KEY_PAUSED: True})
    backend.litellm_client.list_keys.return_value = [serving]
    backend.litellm_client.get_key.return_value = serving

    backend.restore_resource(RID)

    backend.litellm_client.block.assert_called_once_with("h1")
    backend.litellm_client.unblock.assert_not_called()


def test_restore_does_not_re_block_a_key_resumed_since_its_listing() -> None:
    # Listed paused on its own and serving; resumed by the time it is read again.
    # Blocking it would undo a resume Waldur has already been told of.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=False, metadata={META_KEY_PAUSED: True})
    ]
    backend.litellm_client.get_key.return_value = _key(
        f"{RID}-1", "h1", metadata={META_KEY_PAUSED: False}
    )

    backend.restore_resource(RID)

    backend.litellm_client.block.assert_not_called()


# --- a key the proxy lost -----------------------------------------------------


def test_a_lost_key_is_minted_back_with_its_own_models_and_limits() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-2", "h2", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]

    backend.rotate_resource_key(
        f"{RID}-1", RID, limits={"token_cost": 10, "input_tokens": 5}, allowed_models=["gpt-4o"]
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert backend.litellm_client.generate_key.call_args.args[0] == f"{RID}-1"
    # Minting with the offering's list would widen the key, and its holder's chat.
    assert kwargs["models"] == ["gpt-4o"]
    assert kwargs["max_budget"] == 10
    assert kwargs["blocked"] is False
    assert kwargs["metadata"] == {
        META_KEY_LIMITS: {"token_cost": 10},
        META_RESOURCE_LIMITS: {"token_cost": 50},
        META_KEY_MODELS: ["gpt-4o"],
    }


def test_a_lost_key_with_every_model_gets_the_offerings_list() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.rotate_resource_key(f"{RID}-1", RID, limits={}, allowed_models=[ALL_MODELS])

    assert backend.litellm_client.generate_key.call_args.kwargs["models"] == SETTINGS["models"]


def test_a_lost_key_whose_settings_are_unknown_is_minted_closed() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2", "h2")]

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-new"

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["models"] == [NO_MODELS]
    assert kwargs["blocked"] is True
    # Paused on its own, so a resource restore leaves it blocked until Waldur's update
    # or resume writes its settings.
    assert kwargs["metadata"][META_KEY_PAUSED] is True


def test_a_lost_only_key_is_minted_back_with_its_resources_limits_from_waldur() -> None:
    # The resource's only key: no sibling carries a stamp to read its limits from.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.rotate_resource_key(
        f"{RID}-1",
        RID,
        limits={"token_cost": 5},
        allowed_models=[ALL_MODELS],
        resource_limits={"token_cost": 2, "tpm": 100000, "input_tokens": 9},
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 2
    assert kwargs["tpm_limit"] == 100000
    assert kwargs["metadata"][META_KEY_LIMITS] == {"token_cost": 5}
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 2, "tpm": 100000}


def test_a_lost_key_takes_waldurs_resource_limits_over_a_stale_stamp() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-2", "h2", metadata={META_RESOURCE_LIMITS: {"token_cost": 500}}),
    ]

    backend.rotate_resource_key(
        f"{RID}-1", RID, limits={}, allowed_models=[ALL_MODELS], resource_limits={"token_cost": 50}
    )

    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 50


def test_a_lost_key_minted_closed_still_takes_waldurs_resource_limits() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.rotate_resource_key(f"{RID}-1", RID, resource_limits={"token_cost": 2})

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["models"] == [NO_MODELS]
    assert kwargs["max_budget"] == 2
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 2}


def test_rotation_in_place_takes_waldurs_resource_limits_over_the_stamp() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = _rotatable()
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(
        f"{RID}-1", RID, limits={}, allowed_models=[ALL_MODELS], resource_limits={"token_cost": 20}
    )

    body = _update_for(backend, "h1")
    assert body["max_budget"] == 20
    assert body["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 20}


def test_a_lost_key_is_not_minted_with_a_model_the_offering_does_not_serve() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    with pytest.raises(BackendError):
        backend.rotate_resource_key(f"{RID}-1", RID, limits={}, allowed_models=["o3"])

    backend.litellm_client.generate_key.assert_not_called()


# --- the first key of a resource ----------------------------------------------


def test_a_first_key_takes_the_resources_limits_from_waldur() -> None:
    # No sibling carries a stamp to read them from: under key management the agent
    # mints no keys at creation.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []

    backend.mint_resource_key(
        RID, [], limits={"rpm": 30}, resource_limits={"token_cost": 50, "input_tokens": 9}
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 50
    assert kwargs["rpm_limit"] == 30
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 50}


def test_waldurs_resource_limits_win_over_a_stale_stamp() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", metadata={META_RESOURCE_LIMITS: {"token_cost": 500}}),
    ]

    backend.mint_resource_key(RID, [], resource_limits={"token_cost": 50})

    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 50


# --- a limits write racing a key pause ----------------------------------------


def test_a_key_pause_landing_inside_a_limits_write_is_kept() -> None:
    # Read unpaused just before the write; blocked by the time it is read after, with
    # no pause recorded: the write may have dropped a key pause, so it is put back.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.side_effect = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-1", "h1", blocked=True, metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    writes = [call.args[1] for call in backend.litellm_client.update_key.call_args_list]
    assert len(writes) == 2
    assert writes[1] == {
        "metadata": {META_RESOURCE_LIMITS: {"token_cost": 50}, META_KEY_PAUSED: True}
    }


def test_a_key_pause_landing_after_a_limits_write_keeps_the_new_stamp() -> None:
    # The pause read the key before the write and wrote back the old stamp.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.side_effect = [
        _key(f"{RID}-1", "h1"),
        _self_paused(f"{RID}-1", "h1"),
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    writes = [call.args[1] for call in backend.litellm_client.update_key.call_args_list]
    assert writes[-1] == {
        "metadata": {META_KEY_PAUSED: True, META_RESOURCE_LIMITS: {"token_cost": 50}}
    }


def test_a_limits_write_with_nothing_landing_is_one_write() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.side_effect = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-1", "h1", metadata={META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    assert backend.litellm_client.update_key.call_count == 1


def test_restore_fails_for_a_key_gone_between_its_listing_and_its_unblock() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1", blocked=True)]
    backend.litellm_client.get_key.return_value = None

    assert backend.restore_resource(RID) is False
    backend.litellm_client.unblock.assert_not_called()


# --- models dropped from the offering -----------------------------------------


def test_mint_update_and_rotate_record_the_list_waldur_sent() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.get_key.return_value = _key(f"{RID}-1", "h1")

    backend.mint_resource_key(RID, [], allowed_models=[ALL_MODELS])
    assert backend.litellm_client.generate_key.call_args.kwargs["metadata"][META_KEY_MODELS] == [
        ALL_MODELS
    ]

    backend.update_resource_key(f"{RID}-1", RID, limits={}, allowed_models=[])
    assert _update_for(backend, "h1")["metadata"][META_KEY_MODELS] == []


def _offering_shrunk(*keys: dict) -> LiteLLMBackend:
    """A backend whose offering dropped gpt-4o-mini since the keys were written."""
    backend = _make_backend({**SETTINGS, "models": ["gpt-4o"]})
    backend.litellm_client.list_keys.return_value = list(keys)
    backend.litellm_client.get_key.side_effect = lambda token: next(
        key for key in keys if key["token"] == token
    )
    return backend


def test_every_model_key_loses_a_model_dropped_from_the_offering() -> None:
    backend = _offering_shrunk(
        _key(
            f"{RID}-1",
            "h1",
            models=["gpt-4o", "gpt-4o-mini"],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: [ALL_MODELS]},
        ),
    )

    backend.set_resource_limits(RID, {})

    body = _update_for(backend, "h1")
    assert body["models"] == ["gpt-4o"]
    # Written whole, the stamp kept: the key still allows every model in Waldur.
    assert body["metadata"][META_KEY_MODELS] == [ALL_MODELS]


def test_a_named_key_loses_a_model_dropped_from_the_offering() -> None:
    backend = _offering_shrunk(
        _key(
            f"{RID}-1",
            "h1",
            models=["gpt-4o", "gpt-4o-mini"],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: ["gpt-4o", "gpt-4o-mini"]},
        ),
        _key(
            f"{RID}-2",
            "h2",
            models=["gpt-4o-mini"],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: ["gpt-4o-mini"]},
        ),
    )

    backend.set_resource_limits(RID, {})

    assert _update_for(backend, "h1")["models"] == ["gpt-4o"]
    # None of its models left: the reserved name, never an empty list (every model).
    assert _update_for(backend, "h2")["models"] == [NO_MODELS]


def test_a_key_without_the_stamp_loses_the_dropped_model_and_gains_nothing() -> None:
    backend = _offering_shrunk(
        _key(f"{RID}-1", "h1", models=["gpt-4o-mini"], metadata={META_RESOURCE_LIMITS: {}}),
        _key(f"{RID}-2", "h2", models=[NO_MODELS], metadata={META_RESOURCE_LIMITS: {}}),
        _key(f"{RID}-3", "h3", models=["gpt-4o"], metadata={META_RESOURCE_LIMITS: {}}),
    )

    backend.set_resource_limits(RID, {})

    assert _update_for(backend, "h1")["models"] == [NO_MODELS]
    written = [call.args[0] for call in backend.litellm_client.update_key.call_args_list]
    assert written == ["h1"]


def test_a_key_already_in_line_is_not_written() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(
            f"{RID}-1",
            "h1",
            models=["gpt-4o-mini", "gpt-4o"],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: [ALL_MODELS]},
        ),
        _key(
            f"{RID}-2",
            "h2",
            models=[NO_MODELS],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: []},
        ),
        _key(f"{RID}-3", "h3", models=["gpt-4o"], metadata={META_RESOURCE_LIMITS: {}}),
    ]

    backend.set_resource_limits(RID, {})

    backend.litellm_client.update_key.assert_not_called()


def test_an_every_model_key_follows_a_model_added_to_the_offering() -> None:
    backend = _make_backend({**SETTINGS, "models": ["gpt-4o", "gpt-4o-mini", "o3"]})
    record = _key(
        f"{RID}-1",
        "h1",
        models=["gpt-4o", "gpt-4o-mini"],
        metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: [ALL_MODELS]},
    )
    backend.litellm_client.list_keys.return_value = [record]
    backend.litellm_client.get_key.return_value = record

    backend.set_resource_limits(RID, {})

    assert _update_for(backend, "h1")["models"] == ["gpt-4o", "gpt-4o-mini", "o3"]


def test_an_unrestricted_offering_narrows_no_key_without_the_stamp() -> None:
    backend = _make_backend({**SETTINGS, "models": []})
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", models=["gpt-4o"], metadata={META_RESOURCE_LIMITS: {}}),
        _key(
            f"{RID}-2",
            "h2",
            models=["o3"],
            metadata={META_RESOURCE_LIMITS: {}, META_KEY_MODELS: ["o3"]},
        ),
    ]

    backend.set_resource_limits(RID, {})

    backend.litellm_client.update_key.assert_not_called()
