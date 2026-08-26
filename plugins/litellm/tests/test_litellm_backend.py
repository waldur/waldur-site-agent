"""Tests for the LiteLLM management backend (HTTP client mocked)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest
from waldur_site_agent_litellm.backend import LiteLLMBackend
from waldur_site_agent_litellm.client import (
    LiteLLMBackendError,
    LiteLLMEnterpriseFeatureError,
)

from waldur_site_agent.backend.exceptions import BackendError

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {
    "api_url": "https://litellm.example.com/",
    "api_token": "sk-master",
    "models": ["gpt-4o"],
}
RID = "abc123"


class _FakeLimits:
    def __init__(self, data: dict) -> None:
        self._data = data

    def to_dict(self) -> dict:
        return dict(self._data)


def _make_backend(settings: Optional[dict] = None) -> LiteLLMBackend:
    with mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    return backend


def _make_resource(
    limits: Optional[dict] = None, backend_id: str = ""
) -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid.uuid4(),
        name="Inference",
        slug="inference",
        backend_id=backend_id,
        limits=_FakeLimits(limits) if limits is not None else None,
    )


def _key(alias: str, token: str, *, blocked: bool = False) -> dict:
    return {"key_alias": alias, "token": token, "blocked": blocked}


# --- configuration ------------------------------------------------------------


def test_requires_api_url() -> None:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        pytest.raises(BackendError),
    ):
        LiteLLMBackend({"api_token": "sk-master"}, dict(COMPONENTS))


def test_ping_raises_when_asked() -> None:
    backend = _make_backend()
    backend.litellm_client.ping.return_value = False
    assert backend.ping() is False
    with pytest.raises(BackendError):
        backend.ping(raise_exception=True)


# --- provisioning -------------------------------------------------------------


def test_create_surfaces_the_openai_endpoint_and_mints_nothing() -> None:
    backend = _make_backend()
    resource = _make_resource(limits={"input_tokens": 100})

    info = backend.create_resource_with_id(resource, RID)

    assert info.backend_id == RID
    assert info.endpoints == [
        {"name": "OpenAI API", "url": "https://litellm.example.com/v1"}
    ]
    assert info.limits == {"input_tokens": 100}
    # Keys are minted by generate_resource_keys so the core can report each one; a key
    # created here would never reach Waldur.
    assert info.backend_metadata == {}
    backend.litellm_client.generate_key.assert_not_called()


def test_pull_reports_the_resource_as_existing_when_it_owns_keys() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    assert backend._pull_backend_resource(RID).backend_id == RID

    backend.litellm_client.list_keys.return_value = []
    assert backend._pull_backend_resource(RID) is None


def test_recreate_missing_resource_never_mints() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    assert backend.recreate_missing_resource(_make_resource(backend_id=RID)) is False
    backend.litellm_client.generate_key.assert_not_called()


# --- alias matching -----------------------------------------------------------


def test_alias_matching_is_exact_not_a_prefix() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
        # substring_matching also returns a sibling resource's keys. A prefix match
        # would capture them, so a pause or a terminate would fan out across resource
        # boundaries.
        _key(f"{RID}-extra-1", "h3"),
        _key(f"{RID}extra-1", "h4"),
        _key(f"other-{RID}-1", "h5"),
    ]
    assert backend.list_resource_client_ids(RID) == [f"{RID}-1", f"{RID}-2"]


def test_lookup_narrows_server_side_by_prefix() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.list_resource_client_ids(RID)
    backend.litellm_client.list_keys.assert_called_once_with(f"{RID}-")


# --- key generation -----------------------------------------------------------


def test_generate_numbers_slots_past_the_existing_ones() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.generate_key.side_effect = [
        {"key": "sk-2"},
        {"key": "sk-3"},
    ]

    keys = list(backend.generate_resource_keys(RID, count=2))

    # Slot 1 is live and LiteLLM rejects a duplicate alias outright, so reusing it
    # would fail the cycle rather than quietly replace a key.
    assert [key["client_id"] for key in keys] == [f"{RID}-2", f"{RID}-3"]
    assert [key["api_key"] for key in keys] == ["sk-2", "sk-3"]


def test_generate_yields_each_key_before_minting_the_next() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.side_effect = [
        {"key": "sk-1"},
        LiteLLMBackendError("proxy died"),
    ]

    produced = []
    with pytest.raises(LiteLLMBackendError):
        for key in backend.generate_resource_keys(RID, count=2):
            produced.append(key["client_id"])

    # The first key reached the caller before the second was attempted. Returning a
    # list instead would strand it: live at the proxy, with no row in Waldur.
    assert produced == [f"{RID}-1"]


def test_generate_applies_the_model_allowlist_and_backstops() -> None:
    backend = _make_backend(
        {**SETTINGS, "budget_duration": "30d", "tpm_limit": 100, "rpm_limit": 10}
    )
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-1"}

    list(backend.generate_resource_keys(RID, count=1))

    backend.litellm_client.generate_key.assert_called_once_with(
        f"{RID}-1",
        models=["gpt-4o"],
        blocked=False,
        max_budget=None,
        # No budget, so no reset period: a duration outliving the budget it belonged
        # to is meaningless.
        budget_duration=None,
        tpm_limit=100,
        rpm_limit=10,
    )


def test_a_new_key_carries_the_resource_limits_not_only_the_offering_defaults() -> None:
    backend = _make_backend(
        {**SETTINGS, "budget_duration": "30d", "tpm_limit": 100, "rpm_limit": 10}
    )
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-1"}

    backend.create_resource_with_id(
        _make_resource(limits={"token_cost": 50, "tpm": 5000}), RID
    )
    list(backend.generate_resource_keys(RID, count=1))

    # Creation mints the keys in a separate call that carries only the backend id, so
    # without the handover a resource would run on the offering-wide defaults until
    # someone edited its limits -- with no max_budget at all.
    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 50
    assert kwargs["tpm_limit"] == 5000
    assert kwargs["rpm_limit"] == 10
    assert kwargs["budget_duration"] == "30d"


def test_the_parked_limits_are_not_reused_by_another_resource() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-1"}

    backend.create_resource_with_id(_make_resource(limits={"token_cost": 50}), RID)
    list(backend.generate_resource_keys("other", count=1))

    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] is None


def test_limits_are_reconciled_from_waldur_onto_the_keys() -> None:
    backend = _make_backend({**SETTINGS, "budget_duration": "30d"})
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]

    backend.sync_resource_limits(
        _make_resource(limits={"token_cost": 50}, backend_id=RID), mock.MagicMock()
    )

    # Waldur is the authority: the base implementation reconciles the other way and
    # would only ever read an empty set back from the proxy.
    # The full target state, not a patch: the fields the resource does not limit are
    # written as None so a cap removed in Waldur is cleared on the key.
    backend.litellm_client.update_key.assert_called_once_with(
        "h1",
        {
            "max_budget": 50,
            "tpm_limit": None,
            "rpm_limit": None,
            "budget_duration": "30d",
        },
    )


def test_a_removed_limit_is_cleared_off_the_key() -> None:
    # Waldur no longer limits tpm, so the cap it left behind has to come off. Writing
    # only the fields the resource still carries would strand it on the key forever.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "max_budget": 50, "tpm_limit": 100}
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    assert backend.litellm_client.update_key.call_args.args[1]["tpm_limit"] is None


def test_a_removed_limit_falls_back_to_the_offering_default() -> None:
    # The offering-wide default is the floor, not "no cap": clearing a resource limit
    # returns the key to what a freshly minted one would carry.
    backend = _make_backend({**SETTINGS, "tpm_limit": 100})
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "tpm_limit": 500}
    ]

    backend.set_resource_limits(RID, {})

    assert backend.litellm_client.update_key.call_args.args[1]["tpm_limit"] == 100


def test_clearing_every_limit_still_reconciles() -> None:
    # An empty limit set is how a resource whose limits were removed gets its keys
    # back; skipping it would leave every stale cap in place indefinitely.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "max_budget": 50}
    ]

    backend.sync_resource_limits(_make_resource(limits={}, backend_id=RID), mock.MagicMock())

    assert backend.litellm_client.update_key.call_args.args[1]["max_budget"] is None


def test_reconciliation_skips_a_key_that_already_carries_the_limits() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "max_budget": 50}
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    # It runs every membership-sync cycle; rewriting what the key already holds would
    # be one /key/update per key per cycle.
    backend.litellm_client.update_key.assert_not_called()


def test_reconciliation_skips_a_key_that_already_carries_a_budget_duration() -> None:
    # The skip compares every backstop field, budget_duration included, against the
    # /key/list record. Verified against a live proxy: return_full_object=true does
    # carry budget_duration, and the duration round-trips verbatim rather than being
    # normalised to a canonical unit -- so a configured duration still matches and
    # does not turn reconciliation into one /key/update per key per cycle.
    backend = _make_backend({**SETTINGS, "budget_duration": "30d"})
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "max_budget": 50, "budget_duration": "30d"}
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    backend.litellm_client.update_key.assert_not_called()


def test_reconciliation_rewrites_a_key_whose_budget_duration_drifted() -> None:
    backend = _make_backend({**SETTINGS, "budget_duration": "30d"})
    backend.litellm_client.list_keys.return_value = [
        {**_key(f"{RID}-1", "h1"), "max_budget": 50, "budget_duration": "7d"}
    ]

    backend.set_resource_limits(RID, {"token_cost": 50})

    # A duration changed on the offering has to reach keys minted under the old one.
    assert backend.litellm_client.update_key.call_args.args[1]["budget_duration"] == "30d"


def test_a_key_minted_onto_a_paused_resource_lands_blocked() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]
    backend.litellm_client.generate_key.return_value = {"key": "sk-3"}

    list(backend.generate_resource_keys(RID, count=1))

    # A live key on a paused resource un-pauses it in practice and serves traffic
    # past the quota that paused it.
    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is True


def test_a_partly_blocked_resource_is_not_paused() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=False),
    ]
    backend.litellm_client.generate_key.return_value = {"key": "sk-3"}

    list(backend.generate_resource_keys(RID, count=1))

    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


def test_a_resource_with_no_keys_is_not_paused() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-1"}
    list(backend.generate_resource_keys(RID, count=1))
    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is False


# --- rotation -----------------------------------------------------------------


def test_rotate_uses_regenerate_when_available() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-rotated"

    backend.litellm_client.regenerate_key.assert_called_once_with("h1")
    backend.litellm_client.delete_keys.assert_not_called()


def test_rotate_falls_back_to_delete_and_mint_without_a_licence() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-new"

    # The delete has to come first: LiteLLM requires aliases to be globally unique, so
    # the slot cannot be re-minted while the old key still holds the name.
    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])
    assert backend.litellm_client.generate_key.call_args.args[0] == f"{RID}-1"


def test_rotating_a_paused_resource_mints_the_replacement_blocked() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}

    backend.rotate_resource_key(f"{RID}-1", RID)

    # The pause state is read before the delete. Reading it afterwards would see only
    # the siblings and could hand a paused resource a live key.
    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is True


def test_rotate_recreates_a_slot_the_proxy_lost() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2", "h2", blocked=True)]
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-new"

    backend.litellm_client.regenerate_key.assert_not_called()
    backend.litellm_client.delete_keys.assert_not_called()
    # Its only sibling is blocked, so the resource is paused and the replacement must
    # not resurrect it.
    assert backend.litellm_client.generate_key.call_args.kwargs["blocked"] is True


def test_rotate_leaves_the_siblings_alone() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}

    backend.rotate_resource_key(f"{RID}-1", RID)

    # Rotation is zero-downtime only if the other key is untouched.
    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])


# --- pruning ------------------------------------------------------------------


def test_prune_drops_only_the_slots_waldur_does_not_hold() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
        _key(f"{RID}-extra-1", "h3"),
    ]

    backend.prune_unknown_resource_keys(RID, [f"{RID}-2"])

    # Slot 1 is residue from an interrupted create -- live, with no row in Waldur to
    # rotate it by. The foreign alias is out of scope entirely.
    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])


def test_prune_is_a_no_op_when_everything_is_known() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.prune_unknown_resource_keys(RID, [f"{RID}-1"])
    backend.litellm_client.delete_keys.assert_not_called()


# --- state transitions --------------------------------------------------------


def test_pause_blocks_every_key() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]
    backend.litellm_client.block.return_value = True

    assert backend.pause_resource(RID) is True
    assert [call.args[0] for call in backend.litellm_client.block.call_args_list] == ["h1", "h2"]


def test_pause_keeps_going_after_one_key_fails_and_logs_an_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]
    backend.litellm_client.block.side_effect = [LiteLLMBackendError("nope"), True]

    with caplog.at_level("ERROR"):
        # The processor logs "Pausing is successfully completed" on True and nothing
        # retries within the cycle, so a half-blocked resource must not report success.
        assert backend.pause_resource(RID) is False

    # A swallowed block leaves an over-limit key serving, so it is an ERROR -- a
    # warning would not surface in the operator's alerting.
    assert any(record.levelname == "ERROR" for record in caplog.records)
    # The second key is still attempted: one failure must not strand the rest.
    assert backend.litellm_client.block.call_count == 2


def test_pause_counts_a_key_the_proxy_lost_as_paused() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.block.return_value = False
    # A key the proxy no longer holds cannot serve traffic, which is all a pause asks.
    assert backend.pause_resource(RID) is True


def test_pause_reports_false_when_the_resource_owns_no_keys() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    assert backend.pause_resource(RID) is False


def test_restore_unblocks_every_key() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]
    backend.litellm_client.unblock.return_value = True
    assert backend.restore_resource(RID) is True
    assert backend.litellm_client.unblock.call_count == 2


def test_restore_reports_false_when_one_key_cannot_come_back() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=True),
    ]
    backend.litellm_client.unblock.side_effect = [True, False]

    # Asymmetric with pause on purpose: a lost key satisfies "nothing is serving" but
    # not "everything is serving again".
    assert backend.restore_resource(RID) is False
    assert backend.litellm_client.unblock.call_count == 2


def test_downscale_is_a_pause() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.block.return_value = True
    assert backend.downscale_resource(RID) is True
    backend.litellm_client.block.assert_called_once_with("h1")


def test_metadata_reports_active_while_any_key_serves() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
        _key(f"{RID}-2", "h2", blocked=False),
    ]
    assert backend.get_resource_metadata(RID)["active"] is True

    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1", blocked=True),
    ]
    assert backend.get_resource_metadata(RID)["active"] is False


# --- deletion -----------------------------------------------------------------


def test_delete_removes_every_key_of_the_resource() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
        _key(f"{RID}-extra-1", "h3"),
    ]

    backend.delete_resource(_make_resource(backend_id=RID))

    backend.litellm_client.delete_keys.assert_called_once_with(["h1", "h2"])


def test_delete_raises_when_a_key_cannot_be_addressed() -> None:
    # Mirrors pause_resource: a key with no token handle keeps serving after the
    # resource is gone, so the terminate must err rather than report a clean removal.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        {"key_alias": f"{RID}-2", "blocked": False},
    ]

    with pytest.raises(LiteLLMBackendError):
        backend.delete_resource(_make_resource(backend_id=RID))

    # The addressable key is still deleted -- one bad record must not strand the rest.
    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])


def test_delete_without_a_backend_id_touches_nothing() -> None:
    backend = _make_backend()
    backend.delete_resource(_make_resource(backend_id=""))
    backend.litellm_client.delete_keys.assert_not_called()


# --- limits -------------------------------------------------------------------


def test_limits_are_mirrored_onto_every_key_in_full() -> None:
    backend = _make_backend({**SETTINGS, "budget_duration": "30d"})
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", "h1"),
        _key(f"{RID}-2", "h2"),
    ]

    backend.set_resource_limits(RID, {"token_cost": 40, "tpm": 900, "rpm": 60})

    expected = {
        "max_budget": 40,
        "tpm_limit": 900,
        "rpm_limit": 60,
        "budget_duration": "30d",
    }
    # Each key carries the resource's whole budget: the keys are alternatives for one
    # consumer, so splitting it would throttle a single-key consumer to half.
    for call in backend.litellm_client.update_key.call_args_list:
        assert call.args[1] == expected
    assert backend.litellm_client.update_key.call_count == 2


def test_unmapped_limits_are_ignored() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.set_resource_limits(RID, {"input_tokens": 1000})
    backend.litellm_client.update_key.assert_not_called()


def test_a_failed_backstop_does_not_fail_the_order() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", "h1")]
    backend.litellm_client.update_key.side_effect = LiteLLMBackendError("nope")

    # Waldur still pauses on reported usage, so the backstop is not load-bearing.
    backend.set_resource_limits(RID, {"token_cost": 10})


def test_usage_is_reported_by_the_other_backend() -> None:
    assert _make_backend()._get_usage_report([RID]) == {}


def test_a_rotated_key_keeps_the_resources_backstop() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        {"key_alias": f"{RID}-1", "token": "hash1", "blocked": False, "max_budget": 30.0},
        {"key_alias": f"{RID}-2", "token": "hash2", "blocked": False, "max_budget": 30.0},
    ]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("gated")
    backend.litellm_client.generate_key.return_value = {"key": "sk-new", "token": "hash3"}

    backend.rotate_resource_key(f"{RID}-1", RID)

    # Delete-and-mint builds a brand new key. Minting it bare would leave the rotated
    # slot uncapped next to siblings that still carry the budget, until the next
    # membership_sync pass happened to repair it.
    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 30.0


def test_a_replacement_for_a_vanished_key_is_capped_from_its_siblings() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        {"key_alias": f"{RID}-2", "token": "hash2", "blocked": False, "max_budget": 30.0},
    ]
    backend.litellm_client.generate_key.return_value = {"key": "sk-new", "token": "hash9"}

    backend.rotate_resource_key(f"{RID}-1", RID)

    # The slot itself is gone, so its own budget is unrecoverable -- but the siblings
    # of one resource all carry the same backstop, and an uncapped key is worse than
    # one capped from a sibling.
    assert backend.litellm_client.generate_key.call_args.kwargs["max_budget"] == 30.0
