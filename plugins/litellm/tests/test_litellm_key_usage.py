"""Tests for per-key usage reporting: API usage by alias, chat usage on the holder's key."""

from __future__ import annotations

from typing import Optional
from unittest import mock

from waldur_site_agent_litellm.client import META_KEY, META_RESOURCE
from waldur_site_agent_litellm.reporting import LiteLLMUsageReportingBackend

SETTINGS = {
    "api_url": "http://litellm:4000",
    "api_token": "sk-master",
    "openwebui": {"api_url": "http://chat:8080", "api_token": "owui-admin"},
}
COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "token_cost": {"measured_unit": "USD", "accounting_type": "usage"},
}
RID = "abc123"
SHARED = "openwebui-shared"


def _make_backend(settings: Optional[dict] = None) -> LiteLLMUsageReportingBackend:
    with (
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"),
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMClient"),
    ):
        backend = LiteLLMUsageReportingBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_usage_rows.return_value = []
    backend.usage_client.get_user_usage_rows.return_value = []
    if backend.litellm_client is not None:
        backend.litellm_client = mock.MagicMock()
        backend.litellm_client.list_users.return_value = []
    return backend


def _key_row(alias: Optional[str], prompt: int, completion: int, cost: float) -> dict:
    return {"key_alias": alias, "input_tokens": prompt, "output_tokens": completion, "token_cost": cost}


def _user_row(email: str, alias: Optional[str], prompt: int, completion: int, cost: float) -> dict:
    return {"user_id": email, **_key_row(alias, prompt, completion, cost)}


def _user(email: str, *, key: Optional[str] = None, resource: str = RID) -> dict:
    metadata: dict = {META_RESOURCE: resource}
    if key is not None:
        metadata[META_KEY] = key
    return {"user_id": email, "metadata": metadata}


def _usage(prompt: int, completion: int, cost: float) -> dict:
    return {"input_tokens": prompt, "output_tokens": completion, "token_cost": cost}


def test_the_backend_declares_per_key_usage() -> None:
    assert LiteLLMUsageReportingBackend.supports_resource_api_key_usage is True


def test_api_usage_lands_on_the_key_it_was_served_on() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _key_row(f"{RID}-1", 10, 20, 0.5),
        _key_row(f"{RID}-1", 1, 2, 0.1),
        _key_row(f"{RID}-2", 5, 6, 0.25),
        _key_row("other-1", 99, 99, 9.0),
    ]

    report = backend.get_resource_key_usage_report([RID])

    assert report == {
        RID: {f"{RID}-1": _usage(11, 22, 0.6), f"{RID}-2": _usage(5, 6, 0.25)}
    }


def test_a_holders_chat_usage_lands_on_their_key() -> None:
    # Toy story: Ada holds key 2. Her chat arrives on Open WebUI's shared key under her
    # address; it counts against her key, which is how its limit holds in chat.
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-2", 10, 20, 0.5)]
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", key=f"{RID}-2")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", SHARED, 100, 200, 1.5)
    ]

    report = backend.get_resource_key_usage_report([RID])

    assert report == {RID: {f"{RID}-2": _usage(110, 220, 2.0)}}


def test_usage_on_a_minted_key_is_counted_once_whatever_address_it_carried() -> None:
    # Bob sends Ada's address with his own key 1. The alias decides: the row stays on
    # key 1, and is not added to Ada's key a second time.
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", key=f"{RID}-2")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", f"{RID}-1", 10, 20, 0.5)
    ]

    report = backend.get_resource_key_usage_report([RID])

    assert report == {RID: {f"{RID}-1": _usage(10, 20, 0.5)}}


def test_unsplit_usage_on_a_day_with_minted_keys_stays_off_the_address_holders_key() -> None:
    # As above, from a proxy that gave no per-key split: Bob's call under Ada's address
    # might have been served on key 1, so it is not put on Ada's key 2 as well.
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", key=f"{RID}-2")]
    backend.usage_client.get_user_usage_rows.return_value = [
        {
            **_user_row("ada@example.com", None, 10, 20, 0.5),
            "day_key_aliases": [f"{RID}-1", SHARED],
        }
    ]

    report = backend.get_resource_key_usage_report([RID])

    assert report == {RID: {f"{RID}-1": _usage(10, 20, 0.5)}}


def test_unsplit_usage_on_a_day_with_only_the_shared_key_lands_on_the_holders_key() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", key=f"{RID}-2")]
    backend.usage_client.get_user_usage_rows.return_value = [
        {
            **_user_row("ada@example.com", None, 100, 200, 1.5),
            "day_key_aliases": [SHARED],
        }
    ]

    report = backend.get_resource_key_usage_report([RID])

    assert report == {RID: {f"{RID}-2": _usage(100, 200, 1.5)}}


def test_the_keys_sum_to_the_resource_total() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _key_row(f"{RID}-1", 10, 20, 0.5),
        _key_row(f"{RID}-2", 5, 6, 0.25),
    ]
    backend.litellm_client.list_users.return_value = [
        _user("ada@example.com", key=f"{RID}-1"),
        _user("bob@example.com", key=f"{RID}-2"),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", SHARED, 100, 200, 1.5),
        _user_row("bob@example.com", SHARED, 7, 8, 0.75),
        _user_row("ada@example.com", f"{RID}-1", 10, 20, 0.5),
    ]

    per_key = backend.get_resource_key_usage_report([RID])[RID]
    total = backend._get_usage_report([RID])[RID]["TOTAL_ACCOUNT_USAGE"]

    for component in COMPONENTS:
        assert sum(usage[component] for usage in per_key.values()) == total[component]


def test_chat_usage_with_no_key_to_land_on_goes_on_no_key() -> None:
    # Carol chatted on key 1 until it was reassigned to Bob this month. Her usage is not
    # Bob's, and it must not stop Bob's key being reported and held to its limit.
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]
    backend.litellm_client.list_users.return_value = [
        _user("carol@example.com", key=""),
        _user("bob@example.com", key=f"{RID}-1"),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("carol@example.com", SHARED, 1, 1, 0.01),
        _user_row("bob@example.com", SHARED, 2, 2, 0.02),
    ]

    assert backend.get_resource_key_usage_report([RID]) == {
        RID: {
            f"{RID}-1": _usage(12, 22, 0.52),
            # Waldur holds no key by this name; it keeps the sum equal to the total.
            f"{RID}-unassigned": _usage(1, 1, 0.01),
        }
    }


def test_chat_usage_of_a_member_on_membership_chat_goes_on_no_key_too() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_user("dan@example.com")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("dan@example.com", SHARED, 1, 1, 0.01)
    ]

    assert backend.get_resource_key_usage_report([RID]) == {
        RID: {f"{RID}-unassigned": _usage(1, 1, 0.01)}
    }


def test_another_resources_chat_does_not_touch_this_one() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]
    backend.litellm_client.list_users.return_value = [
        _user("bob@example.com", key="", resource="other")
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("bob@example.com", SHARED, 1, 1, 0.01)
    ]

    assert backend.get_resource_key_usage_report([RID]) == {
        RID: {f"{RID}-1": _usage(10, 20, 0.5)}
    }


def test_a_resource_with_no_usage_reports_no_keys() -> None:
    # The core reports every key Waldur holds as zero when the backend has none.
    assert _make_backend().get_resource_key_usage_report([RID]) == {RID: {}}


def test_an_api_only_offering_reports_by_alias_alone() -> None:
    backend = _make_backend({"api_url": "http://litellm:4000", "api_token": "sk-master"})
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]

    assert backend.get_resource_key_usage_report([RID]) == {
        RID: {f"{RID}-1": _usage(10, 20, 0.5)}
    }
    backend.usage_client.get_user_usage_rows.assert_not_called()


def test_a_failed_user_listing_reports_no_key_this_round() -> None:
    # Reporting the keys without their holders' chat would drop a key paused at its limit
    # by chat back under it, and Waldur would resume it.
    from waldur_site_agent.backend.exceptions import BackendError  # noqa: PLC0415

    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_key_row(f"{RID}-1", 10, 20, 0.5)]
    backend.litellm_client.list_users.side_effect = BackendError("proxy down")

    assert backend.get_resource_key_usage_report([RID]) == {RID: None}
    # The resource total keeps its API half, as before.
    assert backend._get_usage_report([RID])[RID]["TOTAL_ACCOUNT_USAGE"]["input_tokens"] == 10
