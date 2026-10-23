"""Per-period spending limits on the proxy, and usage per day (waldur/waldur-site-agent#85)."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest
from waldur_api_client.models.resource import Resource

from waldur_site_agent_litellm import ownership
from waldur_site_agent_litellm.backend import (
    META_HOLDER_CHAT,
    META_HOLDER_CHAT_WINDOWS,
    META_KEY_LIMITS,
    META_RESOURCE_LIMITS,
    LiteLLMBackend,
)
from waldur_site_agent_litellm.client import META_KEY, META_RESOURCE, META_USERNAME
from waldur_site_agent_litellm.reporting import (
    LiteLLMUsageReportingBackend,
    chat_spend_by_user,
)

SETTINGS = {"api_url": "https://litellm.example.com/", "api_token": "sk-master"}
CHAT = {
    **SETTINGS,
    "openwebui": {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "url": "https://chat.example.com",
        "account_provisioning": "managed_password",
        "initial_password": "ChangeMe-123",
    },
}
COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "token_cost": {"measured_unit": "USD", "accounting_type": "usage"},
}
RID = "abc123"
ADA = "ada@example.com"
DAY = {"max_budget": 5, "budget_duration": "1d"}
WEEK = {"max_budget": 20, "budget_duration": "7d"}
FUTURE = "2999-01-01T00:00:00Z"
PAST = "2000-01-01T00:00:00Z"


def _make_backend(settings: Optional[dict] = None) -> LiteLLMBackend:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
    ):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}
    if backend.usage_client is not None:
        backend.usage_client = mock.MagicMock()
        backend.usage_client.get_user_usage_rows.return_value = []
    if backend.openwebui_client is not None:
        backend.openwebui_client = mock.MagicMock()
    backend.litellm_client.get_key.side_effect = lambda token: next(
        (k for k in backend.litellm_client.list_keys.return_value if k["token"] == token), None
    )
    return backend


def _key(alias: str, *, windows: Optional[list] = None, **meta) -> dict:
    record: dict = {"key_alias": alias, "token": f"h-{alias}", "blocked": False}
    if windows is not None:
        record["budget_limits"] = windows
    if meta:
        record["metadata"] = dict(meta)
    return record


def _window(budget: float, duration: str, reset_at: str = FUTURE) -> dict:
    return {"max_budget": budget, "budget_duration": duration, "reset_at": reset_at}


def _writes(backend: LiteLLMBackend) -> dict:
    """``{token: fields}`` of the last ``/key/update`` per key."""
    return {call.args[0]: call.args[1] for call in backend.litellm_client.update_key.call_args_list}


def _resource(**fields) -> Resource:
    return Resource.from_dict({"uuid": uuid.uuid4().hex, "backend_id": RID, **fields})


# --- the windows a key gets --------------------------------------------------------


def test_a_keys_day_and_week_limits_become_windows_beside_its_month_budget() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(
        f"{RID}-1",
        RID,
        limits={"token_cost": 50},
        spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}},
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 50
    assert fields["budget_duration"] == "1mo"
    assert fields["budget_limits"] == [DAY, WEEK]
    assert fields["metadata"][META_KEY_LIMITS] == {
        "token_cost": 50,
        "spend/day": 5,
        "spend/week": 20,
        "spend/month": 50,
    }


def test_quarter_and_year_are_left_to_waldur() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(
        f"{RID}-1", RID, spending_limits={"token_cost": {"quarter": 100, "year": 300}}
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] is None
    # No window to write and none to clear: a proxy that predates windows never sees them.
    assert "budget_limits" not in fields


def test_a_total_limit_is_left_to_waldur() -> None:
    # A total never resets (waldur/waldur-mastermind#841): no proxy budget can follow it.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(f"{RID}-1", RID, spending_limits={"token_cost": {"total": 500}})

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] is None
    assert "budget_limits" not in fields


def test_a_total_limit_leaves_the_other_periods_windows_as_they_are() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(
        f"{RID}-1",
        RID,
        limits={"token_cost": 50},
        spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50, "total": 500}},
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 50
    assert fields["budget_duration"] == "1mo"
    assert fields["budget_limits"] == [DAY, WEEK]
    assert fields["metadata"][META_KEY_LIMITS] == {
        "token_cost": 50,
        "spend/day": 5,
        "spend/week": 20,
        "spend/month": 50,
    }


def test_a_limit_on_a_component_the_proxy_does_not_spend_is_waldurs() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(
        f"{RID}-1", RID, spending_limits={"input_tokens": {"day": 1000, "month": 9000}}
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] is None
    assert "budget_limits" not in fields


def test_a_component_fed_by_the_spend_metric_is_mirrored_too() -> None:
    backend = _make_backend({**SETTINGS, "component_metrics": {"inference_cost": "token_cost"}})
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(f"{RID}-1", RID, spending_limits={"inference_cost": {"day": 5}})

    assert _writes(backend)[f"h-{RID}-1"]["budget_limits"] == [DAY]


def test_each_window_is_the_tighter_of_the_key_and_the_resource() -> None:
    backend = _make_backend()
    stamped = {"token_cost": 40, "spend/day": 3, "spend/week": 30}
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: stamped})
    ]

    backend.update_resource_key(
        f"{RID}-1", RID, spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}}
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 40
    assert fields["budget_limits"] == [
        {"max_budget": 3, "budget_duration": "1d"},
        {"max_budget": 20, "budget_duration": "7d"},
    ]


def test_a_month_spending_limit_and_a_limit_based_budget_take_the_lower() -> None:
    backend = _make_backend({**SETTINGS, "component_metrics": {"inference_cost": "token_cost"}})
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_limits(
        _resource(
            limits={"token_cost": 100}, spending_limits={"inference_cost": {"month": 60}}
        ),
        mock.Mock(),
    )

    assert _writes(backend)[f"h-{RID}-1"]["max_budget"] == 60


def test_an_older_waldurs_key_limit_of_zero_is_still_no_limit() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 40}})
    ]

    backend.update_resource_key(f"{RID}-1", RID, limits={"token_cost": 0}, spending_limits=None)

    assert _writes(backend)[f"h-{RID}-1"]["max_budget"] == 40


def test_a_new_key_is_minted_with_its_windows() -> None:
    backend = _make_backend()

    backend.mint_resource_key(
        RID, [], limits={"token_cost": 50}, spending_limits={"token_cost": {"day": 5, "month": 50}}
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 50
    assert kwargs["budget_limits"] == [DAY]


def test_a_rotation_by_delete_and_mint_keeps_the_windows() -> None:
    from waldur_site_agent_litellm.client import LiteLLMEnterpriseFeatureError

    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", windows=[_window(5, "1d"), _window(20, "7d")])
    ]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("ee")

    backend.rotate_resource_key(f"{RID}-1", RID)

    assert backend.litellm_client.generate_key.call_args.kwargs["budget_limits"] == [DAY, WEEK]


def test_a_rotation_with_settings_applies_the_keys_windows() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(
        f"{RID}-1",
        RID,
        limits={"token_cost": 50},
        spending_limits={"token_cost": {"day": 5, "month": 50}},
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 50
    assert fields["budget_limits"] == [DAY]


def test_a_key_the_proxy_lost_is_minted_back_with_its_windows() -> None:
    backend = _make_backend()

    backend.rotate_resource_key(
        f"{RID}-1",
        RID,
        limits={"token_cost": 50},
        spending_limits={"token_cost": {"week": 20, "month": 50}},
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 50
    assert kwargs["budget_limits"] == [WEEK]


def test_waldurs_resource_limits_on_a_mint_keep_the_stamped_spending_limits() -> None:
    # The core reads the limit-based components only; the spending limits stay as stamped.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(
            f"{RID}-1",
            windows=[_window(5, "1d")],
            **{META_RESOURCE_LIMITS: {"token_cost": 50, "spend/day": 5}},
        )
    ]

    backend.mint_resource_key(RID, [], resource_limits={"token_cost": 40})

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 40
    assert kwargs["budget_limits"] == [DAY]
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 40, "spend/day": 5}


def test_a_resources_first_key_gets_its_windows_at_mint() -> None:
    # No sibling carries a stamp, so the windows can only come from Waldur's read.
    backend = _make_backend()

    backend.mint_resource_key(
        RID,
        [],
        resource_limits={"token_cost": 100},
        resource_spending_limits={"token_cost": {"day": 5, "week": 20}},
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 100
    assert kwargs["budget_limits"] == [DAY, WEEK]
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {
        "token_cost": 100,
        "spend/day": 5,
        "spend/week": 20,
    }


def test_a_resources_total_limit_gives_its_first_key_no_window() -> None:
    backend = _make_backend()

    backend.mint_resource_key(
        RID,
        [],
        resource_limits={"token_cost": 100},
        resource_spending_limits={"token_cost": {"day": 5, "total": 500}},
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 100
    assert kwargs["budget_limits"] == [DAY]
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 100, "spend/day": 5}


def test_waldurs_windows_on_a_mint_win_over_the_stamp() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 50, "spend/day": 1}})
    ]

    backend.mint_resource_key(
        RID, [], resource_limits={"token_cost": 50}, resource_spending_limits={}
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    # Waldur's empty set is a resource with no windows: the stamped day limit goes.
    assert kwargs["budget_limits"] == []
    assert kwargs["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 50}


def test_a_rotation_keeps_the_resources_windows_from_waldur() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 50, "spend/day": 1}})
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(
        f"{RID}-1",
        RID,
        limits={},
        resource_limits={"token_cost": 50},
        resource_spending_limits={"token_cost": {"week": 20}},
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["budget_limits"] == [WEEK]
    assert fields["metadata"][META_RESOURCE_LIMITS] == {"token_cost": 50, "spend/week": 20}


def test_a_lost_only_key_is_minted_back_with_the_resources_windows() -> None:
    backend = _make_backend()

    backend.rotate_resource_key(
        f"{RID}-1",
        RID,
        limits={},
        allowed_models=None,
        resource_limits={"token_cost": 50},
        resource_spending_limits={"token_cost": {"day": 5}},
    )

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 50
    assert kwargs["budget_limits"] == [DAY]


def test_waldurs_windows_without_its_limits_keep_the_stamped_limits() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 40, "spend/day": 1}})
    ]

    backend.mint_resource_key(RID, [], resource_spending_limits={"token_cost": {"day": 5}})

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 40
    assert kwargs["budget_limits"] == [DAY]


# --- the resource's spending limits -------------------------------------------------


def test_the_resources_spending_limits_reach_its_keys() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]

    backend.sync_resource_limits(
        _resource(spending_limits={"token_cost": {"day": 5, "month": 50}}), mock.Mock()
    )

    writes = _writes(backend)
    for alias in (f"{RID}-1", f"{RID}-2"):
        assert writes[f"h-{alias}"]["max_budget"] == 50
        assert writes[f"h-{alias}"]["budget_limits"] == [DAY]


def test_a_resources_total_limit_reaches_no_key() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_limits(
        _resource(spending_limits={"token_cost": {"day": 5, "month": 50, "total": 500}}),
        mock.Mock(),
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 50
    assert fields["budget_limits"] == [DAY]


def test_an_order_keeps_the_spending_limits_last_reconciled() -> None:
    # An order carries the limit-based components only.
    backend = _make_backend()
    stamped = {"tpm": 100, "spend/day": 5}
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", windows=[_window(5, "1d")], **{META_RESOURCE_LIMITS: stamped})
    ]

    backend.set_resource_limits(RID, {"tpm": 200})

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["tpm_limit"] == 200
    assert fields["metadata"][META_RESOURCE_LIMITS] == {"tpm": 200, "spend/day": 5}
    # The window is unchanged, so it is not written again.
    assert "budget_limits" not in fields


def test_a_listing_without_the_field_keeps_them_and_an_empty_set_clears_them() -> None:
    stamped = {"spend/day": 5}
    records = [_key(f"{RID}-1", windows=[_window(5, "1d")], **{META_RESOURCE_LIMITS: stamped})]

    kept = _make_backend()
    kept.litellm_client.list_keys.return_value = records
    kept.sync_resource_limits(_resource(), mock.Mock())
    kept.litellm_client.update_key.assert_not_called()

    cleared = _make_backend()
    cleared.litellm_client.list_keys.return_value = records
    cleared.sync_resource_limits(_resource(spending_limits={}), mock.Mock())
    assert _writes(cleared)[f"h-{RID}-1"]["budget_limits"] == []


def test_a_key_already_carrying_its_windows_is_not_written() -> None:
    backend = _make_backend()
    record = _key(
        f"{RID}-1",
        windows=[_window(5, "1d")],
        **{META_RESOURCE_LIMITS: {"spend/day": 5, "spend/month": 50}},
    )
    record.update(max_budget=50, budget_duration="1mo", tpm_limit=None, rpm_limit=None)
    backend.litellm_client.list_keys.return_value = [record]

    backend.sync_resource_limits(
        _resource(spending_limits={"token_cost": {"day": 5, "month": 50}}), mock.Mock()
    )

    backend.litellm_client.update_key.assert_not_called()


@pytest.mark.parametrize(("reset_at", "written"), [(PAST, False), (FUTURE, True)])
def test_windows_are_not_rewritten_before_the_proxy_has_reset_them(
    reset_at: str, written: bool
) -> None:
    # A write moves every window's reset to the next one from now and keeps its spend:
    # at 00:03, before the proxy's reset job, yesterday's spend would hold all day.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", windows=[_window(5, "1d", reset_at)], **{META_RESOURCE_LIMITS: {}})
    ]

    backend.update_resource_key(f"{RID}-1", RID, spending_limits={"token_cost": {"day": 8}})

    fields = _writes(backend)[f"h-{RID}-1"]
    assert ("budget_limits" in fields) is written
    # The rest of the key's settings go out either way.
    assert fields["blocked"] is False


# --- chat taken off each window -----------------------------------------------------


def _chat(spend: float, date: str) -> dict:
    return {"date": date, "user_id": ADA, "key_alias": None, "token_cost": spend}


def _holder(alias: str) -> dict:
    metadata = {META_RESOURCE: RID, META_USERNAME: "ada", META_KEY: alias}
    return {"user_id": ADA, "user_alias": "Ada", "metadata": metadata}


def test_each_window_loses_its_holders_chat_in_its_own_period() -> None:
    backend = _make_backend(CHAT)
    backend.litellm_client.list_users.return_value = [_holder(f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    # Wednesday 21 October: today, earlier this week, earlier this month.
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat(1.0, "2026-10-21"),
        _chat(2.0, "2026-10-19"),
        _chat(4.0, "2026-10-05"),
    ]

    with mock.patch("waldur_site_agent_litellm.backend.datetime") as clock:
        clock.timezone.utc = datetime.timezone.utc
        clock.timedelta = datetime.timedelta
        clock.datetime.now.return_value = datetime.datetime(
            2026, 10, 21, 12, tzinfo=datetime.timezone.utc
        )
        clock.datetime.fromisoformat = datetime.datetime.fromisoformat
        backend.update_resource_key(
            f"{RID}-1", RID, spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}}
        )

    backend.usage_client.get_user_usage_rows.assert_called_once_with("2026-10-01", "2026-10-21")
    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 43.0
    assert fields["budget_limits"] == [
        {"max_budget": 4.0, "budget_duration": "1d"},
        {"max_budget": 17.0, "budget_duration": "7d"},
    ]
    assert fields["metadata"][META_HOLDER_CHAT] == 7.0
    assert fields["metadata"][META_HOLDER_CHAT_WINDOWS] == {"day": 1.0, "week": 3.0}


def test_every_window_loses_only_chat_from_the_days_the_holder_was_here() -> None:
    # Ada's address moved here from resource "other" on Tuesday 20 October; the report
    # bills that day and every one before it to "other".
    holder = _holder(f"{RID}-1")
    periods = ownership.release(ownership.claim([], "other", "2026-10-01"), "other", "2026-10-20")
    holder["metadata"] = ownership.stamp(
        holder["metadata"], ownership.claim(periods, RID, "2026-10-20")
    )
    backend = _make_backend(CHAT)
    backend.litellm_client.list_users.return_value = [holder]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat(1.0, "2026-10-21"),
        _chat(1.5, "2026-10-20"),
        _chat(2.0, "2026-10-19"),
        _chat(4.0, "2026-10-05"),
    ]

    with mock.patch("waldur_site_agent_litellm.backend.datetime") as clock:
        clock.timezone.utc = datetime.timezone.utc
        clock.timedelta = datetime.timedelta
        clock.datetime.now.return_value = datetime.datetime(
            2026, 10, 21, 12, tzinfo=datetime.timezone.utc
        )
        clock.datetime.fromisoformat = datetime.datetime.fromisoformat
        backend.update_resource_key(
            f"{RID}-1", RID, spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}}
        )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 49.0
    assert fields["budget_limits"] == [
        {"max_budget": 4.0, "budget_duration": "1d"},
        {"max_budget": 19.0, "budget_duration": "7d"},
    ]
    assert fields["metadata"][META_HOLDER_CHAT_WINDOWS] == {"day": 1.0, "week": 1.0}


def test_unreadable_chat_keeps_every_window_net_of_the_last_amounts() -> None:
    from waldur_site_agent.backend.exceptions import BackendError

    backend = _make_backend(CHAT)
    backend.litellm_client.list_users.return_value = [_holder(f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(
            f"{RID}-1",
            **{META_HOLDER_CHAT: 7.0, META_HOLDER_CHAT_WINDOWS: {"day": 1.0, "week": 3.0}},
        )
    ]
    backend.usage_client.get_user_usage_rows.side_effect = BackendError("down")

    backend.update_resource_key(
        f"{RID}-1", RID, spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}}
    )

    fields = _writes(backend)[f"h-{RID}-1"]
    assert fields["max_budget"] == 43.0
    assert [window["max_budget"] for window in fields["budget_limits"]] == [4.0, 17.0]


def test_chat_spend_is_kept_per_day() -> None:
    rows = [
        _chat(1.0, "2026-10-21"),
        _chat(0.5, "2026-10-21"),
        _chat(2.0, "2026-10-19"),
        {**_chat(8.0, "2026-10-21"), "key_alias": f"{RID}-1"},
    ]

    # The row served on a minted key is that key's API usage, not chat.
    assert chat_spend_by_user(rows) == {ADA: {"2026-10-21": 1.5, "2026-10-19": 2.0}}


# --- usage per day -------------------------------------------------------------------


def _usage_backend(settings: Optional[dict] = None) -> LiteLLMUsageReportingBackend:
    with (
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"),
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMClient"),
    ):
        backend = LiteLLMUsageReportingBackend(
            dict(settings or {**SETTINGS, "openwebui": {"api_url": "x", "api_token": "y"}}),
            dict(COMPONENTS),
        )
    backend.usage_client = mock.MagicMock()
    backend.litellm_client = mock.MagicMock()
    return backend


def _row(alias: Optional[str], date: str, tokens: int, cost: float, user: str = "") -> dict:
    row = {"date": date, "key_alias": alias, "input_tokens": tokens, "output_tokens": 0}
    row["token_cost"] = cost
    if user:
        row["user_id"] = user
    return row


def test_the_resource_is_reported_per_day_with_the_same_rows_as_its_month() -> None:
    backend = _usage_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", "2026-10-01", 10, 1.0),
        _row(f"{RID}-2", "2026-10-01", 5, 0.5),
        _row(f"{RID}-1", "2026-10-02", 20, 2.0),
        _row("other-1", "2026-10-02", 99, 9.9),
    ]
    backend.litellm_client.list_users.return_value = [
        {"user_id": ADA, "metadata": {META_RESOURCE: RID}}
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _row("openwebui", "2026-10-02", 7, 0.7, user=ADA),
        # API usage under Ada's address, already counted by alias.
        _row(f"{RID}-1", "2026-10-02", 20, 2.0, user=ADA),
    ]

    daily = backend.get_daily_usage_report_for_period([RID], 2026, 10)[RID]
    month = backend.get_usage_report_for_period([RID], 2026, 10)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert daily == {
        "2026-10-01": {"input_tokens": 15, "token_cost": 1.5},
        "2026-10-02": {"input_tokens": 27, "token_cost": pytest.approx(2.7)},
    }
    assert month["input_tokens"] == 42
    assert month["token_cost"] == pytest.approx(4.2)


def test_each_key_is_reported_per_day_chat_on_its_holders_key() -> None:
    backend = _usage_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", "2026-10-01", 10, 1.0),
    ]
    backend.litellm_client.list_users.return_value = [
        {"user_id": ADA, "metadata": {META_RESOURCE: RID, META_KEY: f"{RID}-2"}}
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _row("openwebui", "2026-10-02", 7, 0.7, user=ADA),
    ]

    report = backend.get_resource_key_daily_usage_report([RID])[RID]

    assert report == {
        f"{RID}-1": {"2026-10-01": {"input_tokens": 10, "token_cost": 1.0}},
        f"{RID}-2": {"2026-10-02": {"input_tokens": 7, "token_cost": 0.7}},
    }


def test_keys_are_not_reported_per_day_when_the_user_list_cannot_be_read() -> None:
    from waldur_site_agent.backend.exceptions import BackendError

    backend = _usage_backend()
    backend.usage_client.get_usage_rows.return_value = []
    backend.litellm_client.list_users.side_effect = BackendError("down")

    assert backend.get_resource_key_daily_usage_report([RID]) == {RID: None}


def test_a_failing_spend_api_is_not_taken_for_an_unreadable_user_list() -> None:
    from waldur_site_agent_litellm.usage_client import LiteLLMUsageBackendError

    backend = _usage_backend()
    backend.usage_client.get_usage_rows.side_effect = LiteLLMUsageBackendError("500")

    with pytest.raises(LiteLLMUsageBackendError):
        backend.get_resource_key_daily_usage_report([RID])
    with pytest.raises(LiteLLMUsageBackendError):
        backend.get_resource_key_usage_report([RID])


def test_the_backends_declare_what_they_take() -> None:
    assert LiteLLMUsageReportingBackend.supports_daily_usage is True
    assert LiteLLMBackend.supports_spending_limit_periods is True


def test_a_resource_command_carries_its_spending_limits_to_new_keys() -> None:
    backend = _make_backend()
    resource = SimpleNamespace(
        uuid=uuid.uuid4(),
        backend_id="",
        limits=None,
        additional_properties={"spending_limits": {"token_cost": {"week": 20}}},
    )

    backend.create_resource_with_id(resource, RID)
    list(backend.generate_resource_keys(RID, count=1))

    assert backend.litellm_client.generate_key.call_args.kwargs["budget_limits"] == [WEEK]
