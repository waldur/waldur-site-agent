"""A delete-and-mint rotation keeps the key's budget, and chat is billed only on the
offering's shared keys (waldur/waldur-site-agent#97)."""

from __future__ import annotations

import contextlib
import datetime
import logging
from unittest import mock

import pytest
from waldur_site_agent_litellm import reporting
from waldur_site_agent_litellm.backend import (
    META_CARRIED_WINDOWS,
    META_KEY_LIMITS,
    META_RESOURCE_LIMITS,
    LiteLLMBackend,
)
from waldur_site_agent_litellm.client import LiteLLMEnterpriseFeatureError
from waldur_site_agent_litellm.reporting import (
    LiteLLMUsageReportingBackend,
    chat_spend_by_user,
    served_on_shared_key,
    shared_keys_setting,
)

from waldur_site_agent.backend.exceptions import BackendError

SETTINGS = {"api_url": "https://litellm.example.com/", "api_token": "sk-master"}
COMPONENTS = {"token_cost": {"measured_unit": "USD", "accounting_type": "usage"}}
RID = "abc123"
SLOT = f"{RID}-1"
ADA = "ada@example.com"
FUTURE = "2999-01-01T00:00:00Z"
PAST = "2000-01-01T00:00:00Z"
# Wednesday 21 October 2026: the day window starts today, the week on Monday the 19th.
TODAY = "2026-10-21"
MONDAY = "2026-10-19"


@contextlib.contextmanager
def _on_wednesday():
    with mock.patch("waldur_site_agent_litellm.backend.datetime") as clock:
        clock.timezone.utc = datetime.timezone.utc
        clock.timedelta = datetime.timedelta
        clock.datetime.now.return_value = datetime.datetime(
            2026, 10, 21, 12, tzinfo=datetime.timezone.utc
        )
        clock.datetime.fromisoformat = datetime.datetime.fromisoformat
        yield


def _make_backend() -> LiteLLMBackend:
    with mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"):
        backend = LiteLLMBackend(dict(SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("ee")
    backend.litellm_client.get_key.side_effect = lambda token: next(
        (k for k in backend.litellm_client.list_keys.return_value if k["token"] == token), None
    )
    # An API-only offering has no usage client; the rotation builds its own.
    backend._window_usage_client = mock.MagicMock()
    backend._window_usage_client.get_usage_rows.return_value = []
    return backend


def _key(
    *,
    spend: float = 0.0,
    reset_at: str | None = FUTURE,
    day: float | None = None,
    week: float | None = None,
    **metadata,
) -> dict:
    windows = []
    if day is not None:
        windows.append({"max_budget": day, "budget_duration": "1d", "reset_at": FUTURE})
    if week is not None:
        windows.append({"max_budget": week, "budget_duration": "7d", "reset_at": FUTURE})
    return {
        "key_alias": SLOT,
        "token": f"h-{SLOT}",
        "blocked": False,
        "max_budget": 50,
        "budget_duration": "1mo",
        "budget_reset_at": reset_at,
        "spend": spend,
        "budget_limits": windows,
        "metadata": dict(metadata),
    }


def _api(alias: str, spend: float, date: str) -> dict:
    return {"date": date, "key_alias": alias, "token_cost": spend}


def _minted(backend: LiteLLMBackend) -> dict:
    return backend.litellm_client.generate_key.call_args.kwargs


def _windows(kwargs: dict) -> dict:
    return {w["budget_duration"]: w["max_budget"] for w in kwargs["budget_limits"]}


# --- the month: the key's own spend ------------------------------------------------


def test_a_delete_and_mint_rotation_carries_the_months_spend() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(spend=30.0)]

    backend.rotate_resource_key(SLOT, RID)

    backend.litellm_client.delete_keys.assert_called_once_with([f"h-{SLOT}"])
    kwargs = _minted(backend)
    assert kwargs["max_budget"] == 50
    assert kwargs["spend"] == 30.0


def test_last_months_spend_is_not_carried_before_the_proxy_has_reset_it() -> None:
    # The 1st, before the reset job ran: the old key's spend is last month's.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(spend=30.0, reset_at=PAST)]

    backend.rotate_resource_key(SLOT, RID)

    assert "spend" not in _minted(backend)


def test_a_key_with_no_budget_period_carries_no_spend() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(spend=30.0, reset_at=None)]

    backend.rotate_resource_key(SLOT, RID)

    assert "spend" not in _minted(backend)


def test_an_unused_key_is_rotated_with_no_window_read() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key()]

    backend.rotate_resource_key(SLOT, RID)

    assert "spend" not in _minted(backend)
    backend._window_usage_client.get_usage_rows.assert_not_called()


def test_an_enterprise_regenerate_is_left_as_it_was() -> None:
    backend = _make_backend()
    backend.litellm_client.regenerate_key.side_effect = None
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"
    backend.litellm_client.list_keys.return_value = [_key(spend=30.0, day=5)]

    assert backend.rotate_resource_key(SLOT, RID) == "sk-rotated"

    backend.litellm_client.generate_key.assert_not_called()
    backend._window_usage_client.get_usage_rows.assert_not_called()


# --- the day and week windows ------------------------------------------------------


def test_the_windows_are_minted_net_of_the_slots_spend_in_them() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(day=5, week=20)]
    backend._window_usage_client.get_usage_rows.return_value = [
        _api(SLOT, 1.0, TODAY),
        _api(SLOT, 2.0, MONDAY),
        _api(SLOT, 8.0, "2026-10-18"),  # last week
        _api(f"{RID}-2", 4.0, TODAY),  # a sibling slot
    ]

    with _on_wednesday():
        backend.rotate_resource_key(SLOT, RID)

    backend._window_usage_client.get_usage_rows.assert_called_once_with(MONDAY, TODAY)
    kwargs = _minted(backend)
    assert _windows(kwargs) == {"1d": 4.0, "7d": 17.0}
    assert kwargs["metadata"][META_CARRIED_WINDOWS] == {
        "day": {"since": TODAY, "spend": 1.0},
        "week": {"since": MONDAY, "spend": 3.0},
    }


def test_a_window_is_never_minted_below_zero() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(day=5)]
    backend._window_usage_client.get_usage_rows.return_value = [_api(SLOT, 9.0, TODAY)]

    with _on_wednesday():
        backend.rotate_resource_key(SLOT, RID)

    assert _windows(_minted(backend)) == {"1d": 0.0}


def test_a_second_rotation_takes_off_only_the_spend_since_the_first() -> None:
    # The first rotation left the day at 5 - 1 and noted the 1.0; 0.5 more since.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(day=4.0, **{META_CARRIED_WINDOWS: {"day": {"since": TODAY, "spend": 1.0}}})
    ]
    backend._window_usage_client.get_usage_rows.return_value = [
        _api(SLOT, 1.0, TODAY),
        _api(SLOT, 0.5, TODAY),
    ]

    with _on_wednesday():
        backend.rotate_resource_key(SLOT, RID)

    kwargs = _minted(backend)
    assert _windows(kwargs) == {"1d": 3.5}
    assert kwargs["metadata"][META_CARRIED_WINDOWS] == {"day": {"since": TODAY, "spend": 1.5}}


def test_a_second_rotation_with_settings_takes_off_only_the_spend_since_the_first() -> None:
    # Waldur rotates a key that changes hands with its settings: the morning's 1.0,
    # carried by the first rotation, must not come back.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(
            day=4.0,
            **{
                META_CARRIED_WINDOWS: {"day": {"since": TODAY, "spend": 1.0}},
                META_RESOURCE_LIMITS: {},
            },
        )
    ]
    backend._window_usage_client.get_usage_rows.return_value = [
        _api(SLOT, 1.0, TODAY),
        _api(SLOT, 0.5, TODAY),
    ]

    with _on_wednesday():
        backend.rotate_resource_key(
            SLOT, RID, limits={}, spending_limits={"token_cost": {"day": 5, "month": 50}}
        )

    kwargs = _minted(backend)
    assert _windows(kwargs) == {"1d": 3.5}
    assert kwargs["metadata"][META_CARRIED_WINDOWS] == {"day": {"since": TODAY, "spend": 1.5}}


def test_a_read_that_lost_earlier_spend_never_shrinks_the_carried_amount() -> None:
    # The proxy no longer resolves the deleted key's rows: only 0.5 shows today.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(day=4.0, **{META_CARRIED_WINDOWS: {"day": {"since": TODAY, "spend": 1.0}}})
    ]
    backend._window_usage_client.get_usage_rows.return_value = [_api(SLOT, 0.5, TODAY)]

    with _on_wednesday():
        backend.rotate_resource_key(SLOT, RID)

    kwargs = _minted(backend)
    assert _windows(kwargs) == {"1d": 4.0}
    assert kwargs["metadata"][META_CARRIED_WINDOWS] == {"day": {"since": TODAY, "spend": 1.0}}


def test_a_rotation_with_settings_nets_its_windows_too() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key()]
    backend._window_usage_client.get_usage_rows.return_value = [_api(SLOT, 2.0, TODAY)]

    with _on_wednesday():
        backend.rotate_resource_key(
            SLOT, RID, limits={}, spending_limits={"token_cost": {"day": 5, "month": 50}}
        )

    assert _windows(_minted(backend)) == {"1d": 3.0}


def test_unreadable_window_spend_does_not_stop_the_rotation() -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(spend=3.0, day=5)]
    backend._window_usage_client.get_usage_rows.side_effect = BackendError("down")

    with _on_wednesday():
        assert backend.rotate_resource_key(SLOT, RID) == "sk-new"

    kwargs = _minted(backend)
    assert _windows(kwargs) == {"1d": 5}
    assert kwargs["spend"] == 3.0


def test_a_chat_offering_reads_the_windows_with_its_own_usage_client() -> None:
    backend = _make_backend()
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_usage_rows.return_value = [_api(SLOT, 1.0, TODAY)]
    backend.litellm_client.list_keys.return_value = [_key(day=5)]

    with _on_wednesday():
        backend.rotate_resource_key(SLOT, RID)

    assert _windows(_minted(backend)) == {"1d": 4.0}
    backend._window_usage_client.get_usage_rows.assert_not_called()


def test_an_api_only_offering_builds_a_usage_client_for_the_windows() -> None:
    with mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"):
        backend = LiteLLMBackend(dict(SETTINGS), dict(COMPONENTS))
    assert backend.usage_client is None
    with mock.patch("waldur_site_agent_litellm.backend.LiteLLMUsageClient") as factory:
        factory.return_value.get_usage_rows.return_value = []
        assert backend._slot_window_spend(SLOT, {"day": TODAY, "week": MONDAY}) == {}
        backend._slot_window_spend(SLOT, {"day": TODAY, "week": MONDAY})
    factory.assert_called_once_with(SETTINGS)


def test_a_later_budget_write_keeps_the_carried_spend_off_until_the_window_resets() -> None:
    # Carried into today's day window, and into last week's: only today's still counts.
    carried = {
        "day": {"since": TODAY, "spend": 1.0},
        "week": {"since": "2026-10-12", "spend": 6.0},
    }
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [
        _key(day=4.0, week=14.0, **{META_CARRIED_WINDOWS: carried, META_RESOURCE_LIMITS: {}})
    ]

    with _on_wednesday():
        backend.update_resource_key(
            SLOT, RID, spending_limits={"token_cost": {"day": 5, "week": 20, "month": 50}}
        )

    (call,) = backend.litellm_client.update_key.call_args_list
    fields = call.args[1]
    assert _windows(fields) == {"1d": 4.0, "7d": 20}
    assert fields["metadata"][META_CARRIED_WINDOWS] == carried


def test_the_limits_sync_keeps_the_carried_spend_off() -> None:
    record = _key(
        day=5,
        **{
            META_CARRIED_WINDOWS: {"day": {"since": TODAY, "spend": 1.0}},
            META_KEY_LIMITS: {},
            META_RESOURCE_LIMITS: {"spend/day": 5},
        },
    )
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [record]

    with _on_wednesday():
        backend._write_key_budgets(RID, {SLOT: record}, None)

    (call,) = backend.litellm_client.update_key.call_args_list
    assert _windows(call.args[1]) == {"1d": 4.0}


# --- chat only on the shared keys --------------------------------------------------


def _chat(alias: str | None, spend: float, key_hash: str | None = None) -> dict:
    return {
        "date": TODAY,
        "user_id": ADA,
        "key_alias": alias,
        "key_hash": key_hash,
        "token_cost": spend,
    }


def test_the_shared_keys_setting_takes_a_list_or_one_key() -> None:
    assert shared_keys_setting({}) is None
    assert shared_keys_setting({"shared_keys": "openwebui-1"}) == frozenset({"openwebui-1"})
    assert shared_keys_setting({"shared_keys": [" a ", "", "b"]}) == frozenset({"a", "b"})
    assert shared_keys_setting({"shared_keys": []}) == frozenset()


def test_a_shared_key_named_like_a_slot_is_chat() -> None:
    rows = [_chat("openwebui-1", 2.0)]

    assert chat_spend_by_user(rows) == {}  # by the alias shape: a minted key
    assert chat_spend_by_user(rows, frozenset({"openwebui-1"})) == {ADA: {TODAY: 2.0}}


def test_a_key_outside_the_list_cannot_bill_an_address() -> None:
    # Someone's hand-made key, sending Ada's address in the header.
    rows = [_chat("personal", 5.0), _chat(None, 7.0, key_hash="h-unnamed")]

    assert chat_spend_by_user(rows) == {ADA: {TODAY: 12.0}}  # by the alias shape
    assert chat_spend_by_user(rows, frozenset({"openwebui"})) == {}


def test_a_shared_key_without_an_alias_is_named_by_its_hash() -> None:
    rows = [_chat(None, 2.0, key_hash="h-owui"), _chat(None, 3.0, key_hash="h-other")]

    assert chat_spend_by_user(rows, frozenset({"h-owui"})) == {ADA: {TODAY: 2.0}}


def test_unsplit_usage_is_chat_only_on_a_day_only_shared_keys_served() -> None:
    shared = frozenset({"openwebui", "h-owui"})
    unsplit = {"key_alias": None, "key_hash": None}

    only_shared = {**unsplit, "day_key_aliases": ["openwebui", None],
                   "day_key_hashes": ["h-1", "h-owui"]}
    with_other = {**unsplit, "day_key_aliases": ["openwebui", "personal"],
                  "day_key_hashes": ["h-1", "h-2"]}
    no_keys = {**unsplit, "day_key_aliases": []}

    assert served_on_shared_key(only_shared, shared)
    assert not served_on_shared_key(with_other, shared)
    assert not served_on_shared_key(no_keys, shared)
    assert not served_on_shared_key(unsplit, shared)


def _reporting(openwebui: dict) -> LiteLLMUsageReportingBackend:
    settings = {**SETTINGS, "openwebui": {"api_url": "http://chat:8080", **openwebui}}
    with (
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"),
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMClient"),
    ):
        backend = LiteLLMUsageReportingBackend(settings, dict(COMPONENTS))
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_usage_rows.return_value = []
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = [
        {"user_id": ADA, "metadata": {"waldur_resource": RID}}
    ]
    return backend


def test_the_report_bills_by_address_only_the_shared_keys_rows() -> None:
    backend = _reporting({"shared_keys": ["openwebui-1"]})
    backend.usage_client.get_user_usage_rows.return_value = [
        {**_chat("openwebui-1", 2.0), "input_tokens": 0},
        {**_chat("personal", 5.0), "input_tokens": 0},
    ]

    report = backend._collect_usage([RID], "2026-10-01", "2026-10-31")

    assert report[RID]["TOTAL_ACCOUNT_USAGE"]["token_cost"] == 2.0


@pytest.fixture(autouse=True)
def _fresh_warnings():
    reporting._SHARED_KEYS_WARNED.clear()
    yield
    reporting._SHARED_KEYS_WARNED.clear()


def test_an_offering_without_shared_keys_keeps_the_alias_rule_and_says_so(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="waldur_site_agent_litellm.reporting"):
        backend = _reporting({})
    backend.usage_client.get_user_usage_rows.return_value = [
        {**_chat("personal", 5.0), "input_tokens": 0},
    ]

    report = backend._collect_usage([RID], "2026-10-01", "2026-10-31")

    assert report[RID]["TOTAL_ACCOUNT_USAGE"]["token_cost"] == 5.0
    assert "openwebui.shared_keys is not set" in caplog.text


def test_an_empty_shared_keys_list_is_warned_of(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="waldur_site_agent_litellm.reporting"):
        _reporting({"shared_keys": []})

    assert "openwebui.shared_keys is empty" in caplog.text


def test_the_shared_keys_warning_is_logged_once_per_process(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="waldur_site_agent_litellm.reporting"):
        _reporting({})
        _reporting({})

    assert caplog.text.count("openwebui.shared_keys is not set") == 1


def test_a_key_rotated_with_no_month_budget_gets_no_spend() -> None:
    # Waldur removed the month limit in the same edit: spend there would never reset.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(spend=30.0)]

    backend.rotate_resource_key(SLOT, RID, limits={}, spending_limits={}, resource_limits={})

    kwargs = _minted(backend)
    assert kwargs["max_budget"] is None
    assert "spend" not in kwargs


def test_the_holders_chat_is_netted_only_from_the_shared_keys() -> None:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
    ):
        backend = LiteLLMBackend(
            {
                **SETTINGS,
                "openwebui": {
                    "api_url": "https://chat.example.com",
                    "api_token": "owui-admin",
                    "shared_keys": ["openwebui-1"],
                },
            },
            dict(COMPONENTS),
        )
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat("openwebui-1", 2.0),
        _chat("personal", 5.0),
    ]

    with _on_wednesday():
        spend = backend._chat_spend_by_user(backend._chat_period_starts())

    assert spend == {ADA: {TODAY: 2.0}}
