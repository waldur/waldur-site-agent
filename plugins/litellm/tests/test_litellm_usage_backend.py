"""Tests for the LiteLLM usage reporting backend (usage client mocked)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest import mock

import pytest
from waldur_site_agent_litellm import reporting
from waldur_site_agent_litellm.reporting import LiteLLMUsageReportingBackend

from waldur_site_agent.backend.exceptions import BackendError

SETTINGS = {"api_url": "http://litellm:4000", "api_token": "sk-master"}
COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "token_cost": {"measured_unit": "USD", "accounting_type": "usage"},
}
RID = "abc123"


def _make_backend(
    components: object = None, settings: object = None
) -> LiteLLMUsageReportingBackend:
    with mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"):
        backend = LiteLLMUsageReportingBackend(
            dict(SETTINGS if settings is None else settings),
            dict(COMPONENTS if components is None else components),
        )
    backend.usage_client = mock.MagicMock()
    return backend


def _row(alias: str, prompt: int, completion: int, cost: float) -> dict:
    return {
        "key_alias": alias,
        "input_tokens": prompt,
        "output_tokens": completion,
        "token_cost": cost,
    }


def test_requires_api_url() -> None:
    with (
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"),
        pytest.raises(BackendError),
    ):
        LiteLLMUsageReportingBackend({"api_token": "sk-master"}, dict(COMPONENTS))


def test_ping_raises_when_asked() -> None:
    backend = _make_backend()
    backend.usage_client.ping.return_value = False
    assert backend.ping() is False
    with pytest.raises(BackendError):
        backend.ping(raise_exception=True)


def test_usage_rolls_up_from_keys_onto_the_resource() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", 10, 20, 0.5),
        _row(f"{RID}-2", 5, 6, 0.25),
    ]

    report = backend.get_usage_report_for_period([RID], 2026, 8)

    # Metering per key would split a tenant's bill across slots, and a rotated-away
    # key's half would never be attributed to anyone.
    assert report == {
        RID: {
            "TOTAL_ACCOUNT_USAGE": {
                "input_tokens": 15,
                "output_tokens": 26,
                "token_cost": 0.75,
            }
        }
    }


def test_repeated_rows_for_one_key_are_summed_not_overwritten() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", 10, 20, 0.5),
        _row(f"{RID}-1", 3, 4, 0.1),
    ]

    report = backend.get_usage_report_for_period([RID], 2026, 8)

    # One key produces a row per day it was used. Overwriting would keep only the
    # last day and under-bill the month.
    assert report[RID]["TOTAL_ACCOUNT_USAGE"]["input_tokens"] == 13
    assert report[RID]["TOTAL_ACCOUNT_USAGE"]["token_cost"] == pytest.approx(0.6)


def test_only_the_requested_resources_are_reported() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", 10, 20, 0.5),
        _row("other-1", 99, 99, 9.9),
    ]
    report = backend.get_usage_report_for_period([RID], 2026, 8)
    assert set(report) == {RID}


def test_only_the_declared_components_are_metered() -> None:
    # An offering that prices tokens Waldur-side declares no token_cost. Reporting it
    # anyway would submit a component the offering does not have.
    backend = _make_backend(
        {key: COMPONENTS[key] for key in ("input_tokens", "output_tokens")}
    )
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]

    usage = backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert set(usage) == {"input_tokens", "output_tokens"}


def test_a_cost_only_offering_works_without_a_mode_flag() -> None:
    backend = _make_backend({"token_cost": COMPONENTS["token_cost"]})
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]

    usage = backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert usage == {"token_cost": 0.5}


def test_no_matching_components_warns_and_reports_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        backend = _make_backend({"gpu_hours": {"measured_unit": "h"}})
    assert any("No LiteLLM usage components" in record.message for record in caplog.records)
    assert backend.get_usage_report_for_period([RID], 2026, 8) == {}


def test_the_period_is_the_whole_calendar_month() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = []

    backend.get_usage_report_for_period([RID], 2026, 2)
    assert backend.usage_client.get_usage_rows.call_args.args == ("2026-02-01", "2026-02-28")

    backend.get_usage_report_for_period([RID], 2024, 2)
    # A leap February must not stop on the 28th and lose a day of usage.
    assert backend.usage_client.get_usage_rows.call_args.args == ("2024-02-01", "2024-02-29")

    backend.get_usage_report_for_period([RID], 2026, 12)
    assert backend.usage_client.get_usage_rows.call_args.args == ("2026-12-01", "2026-12-31")


def test_no_resources_means_no_query() -> None:
    backend = _make_backend()
    assert backend.get_usage_report_for_period([], 2026, 8) == {}
    backend.usage_client.get_usage_rows.assert_not_called()


def test_pull_resource_synthesizes_from_usage() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]
    resource = SimpleNamespace(uuid=uuid.uuid4(), backend_id=RID)

    info = backend.pull_resource(resource)

    assert info.usage["TOTAL_ACCOUNT_USAGE"]["input_tokens"] == 10
    assert info.users == []


def test_pull_resource_reports_zeros_for_an_unused_resource() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = []
    resource = SimpleNamespace(uuid=uuid.uuid4(), backend_id=RID)

    info = backend.pull_resource(resource)

    assert info.usage["TOTAL_ACCOUNT_USAGE"] == dict.fromkeys(COMPONENTS, 0)


def test_pull_resource_without_a_backend_id_is_none() -> None:
    backend = _make_backend()
    resource = SimpleNamespace(uuid=uuid.uuid4(), backend_id="")
    assert backend.pull_resource(resource) is None


def test_pull_resource_swallows_a_backend_failure() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.side_effect = RuntimeError("proxy down")
    resource = SimpleNamespace(uuid=uuid.uuid4(), backend_id=RID)
    # A reporting sweep covers many resources; one unreachable proxy must not abort it.
    assert backend.pull_resource(resource) is None


@pytest.mark.parametrize(
    "method",
    ["pause_resource", "restore_resource", "downscale_resource", "get_resource_metadata"],
)
def test_management_operations_are_refused(method: str) -> None:
    backend = _make_backend()
    with pytest.raises(NotImplementedError):
        getattr(backend, method)(RID)


# --- fetch reuse --------------------------------------------------------------


def test_one_month_is_fetched_once_for_every_resource_of_a_pass() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [
        _row(f"{RID}-1", 10, 5, 0.5),
        _row("other-1", 70, 30, 7.0),
    ]

    first = backend._get_usage_report([RID])
    second = backend._get_usage_report(["other"])

    # The endpoint cannot be filtered by key, so each call would otherwise walk the
    # whole proxy's month again -- once per resource, once more per historical period.
    backend.usage_client.get_usage_rows.assert_called_once()
    assert first[RID]["TOTAL_ACCOUNT_USAGE"]["input_tokens"] == 10
    assert second["other"]["TOTAL_ACCOUNT_USAGE"]["input_tokens"] == 70


def test_each_period_is_fetched_on_its_own() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = []

    backend.get_usage_report_for_period([RID], 2026, 6)
    backend.get_usage_report_for_period([RID], 2026, 7)
    backend.get_usage_report_for_period([RID], 2026, 6)

    # Keyed by date range: June is reused, July is a different month.
    assert backend.usage_client.get_usage_rows.call_count == 2


def test_the_default_ttl_ends_before_the_next_reporting_pass() -> None:
    period = reporting.WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60
    # Strictly inside one period: at equal, an entry stored at the top of a pass is
    # still valid at the top of the next one.
    assert 0 < _make_backend()._usage_cache_ttl < period


def test_a_zero_ttl_disables_the_reuse() -> None:
    backend = _make_backend(settings={**SETTINGS, "usage_cache_ttl": 0})
    backend.usage_client.get_usage_rows.return_value = []

    backend._get_usage_report([RID])
    backend._get_usage_report([RID])

    assert backend.usage_client.get_usage_rows.call_count == 2


def test_the_reuse_expires_so_it_cannot_outlive_a_pass() -> None:
    backend = _make_backend(settings={**SETTINGS, "usage_cache_ttl": 60})
    backend.usage_client.get_usage_rows.return_value = []

    # One monotonic read per fetch attempt: the first stores, the second is 61s later.
    with mock.patch(
        "waldur_site_agent_litellm.reporting.time.monotonic", side_effect=[0, 61]
    ):
        backend._get_usage_report([RID])
        backend._get_usage_report([RID])

    # A pass builds a fresh backend today, but a caller that reused one must not keep
    # reporting the first pass's numbers.
    assert backend.usage_client.get_usage_rows.call_count == 2


def test_an_idle_resource_zero_fills_only_the_metered_components() -> None:
    backend = _make_backend(
        {**COMPONENTS, "requests": {"measured_unit": "calls", "accounting_type": "usage"}}
    )
    backend.usage_client.get_usage_rows.return_value = []

    info = backend._pull_backend_resource(RID)

    # "requests" is not something this backend can meter. Zero-filling it would report
    # a measured zero for an idle resource, while a resource with usage omits it.
    assert set(info.usage["TOTAL_ACCOUNT_USAGE"]) == set(backend._usage_metrics)


# --- chat usage -------------------------------------------------------------

CHAT_SETTINGS = dict(
    SETTINGS, openwebui={"api_url": "http://chat:8080", "api_token": "owui-admin"}
)


def _chat_backend(components: object = None) -> LiteLLMUsageReportingBackend:
    with (
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMUsageClient"),
        mock.patch("waldur_site_agent_litellm.reporting.LiteLLMClient"),
    ):
        backend = LiteLLMUsageReportingBackend(
            dict(CHAT_SETTINGS), dict(COMPONENTS if components is None else components)
        )
    backend.usage_client = mock.MagicMock()
    backend.usage_client.get_usage_rows.return_value = []
    backend.usage_client.get_user_usage_rows.return_value = []
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    return backend


def _user(email: str, resource: str) -> dict:
    return {"user_id": email, "metadata": {"waldur_resource": resource}}


def _user_row(email: str, alias: object, prompt: int, completion: int, cost: float) -> dict:
    return {
        "user_id": email,
        "key_alias": alias,
        "input_tokens": prompt,
        "output_tokens": completion,
        "token_cost": cost,
    }


def test_chat_usage_is_billed_to_the_resource_that_owns_the_address() -> None:
    # Open WebUI's traffic arrives on a shared key this plugin does not own, so it is
    # invisible by alias and identifiable only by the address the proxy stamped on it.
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", RID)]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", "openwebui-shared", 100, 200, 1.5)
    ]

    report = backend.get_usage_report_for_period([RID], 2026, 8)

    assert report[RID]["TOTAL_ACCOUNT_USAGE"] == {
        "input_tokens": 100,
        "output_tokens": 200,
        "token_cost": 1.5,
    }


def test_the_two_surfaces_add_up_on_one_resource() -> None:
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", RID)]
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", "openwebui-shared", 100, 200, 1.5)
    ]

    totals = backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert totals == {"input_tokens": 110, "output_tokens": 220, "token_cost": 2.0}


def test_a_request_on_the_resources_own_key_is_not_billed_twice() -> None:
    # The api_keys and entities breakdowns describe the same records from two angles.
    # A person using their own API key appears in both, and summing both would double
    # every request they make.
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", RID)]
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", f"{RID}-1", 10, 20, 0.5)
    ]

    totals = backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert totals == {"input_tokens": 10, "output_tokens": 20, "token_cost": 0.5}


def test_usage_under_an_unmanaged_address_is_billed_to_nobody() -> None:
    backend = _chat_backend()
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("stranger@example.com", "openwebui-shared", 100, 200, 1.5),
        # LiteLLM's bucket for requests on a key with no user attached — every key this
        # plugin mints. It must never match a resource.
        _user_row("Unassigned", f"{RID}-1", 10, 20, 0.5),
    ]

    assert backend.get_usage_report_for_period([RID], 2026, 8) == {}


def test_chat_usage_for_another_resource_does_not_leak_into_this_one() -> None:
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("bob@example.com", "other")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("bob@example.com", "openwebui-shared", 100, 200, 1.5)
    ]

    assert backend.get_usage_report_for_period([RID], 2026, 8) == {}


def test_an_api_only_offering_never_reads_the_user_list() -> None:
    backend = _make_backend()
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]

    assert backend.litellm_client is None
    assert backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"][
        "input_tokens"
    ] == 10
    backend.usage_client.get_user_usage_rows.assert_not_called()


def test_a_failing_user_list_does_not_take_the_api_usage_down_with_it() -> None:
    backend = _chat_backend()
    backend.litellm_client.list_users.side_effect = BackendError("no permission")
    backend.usage_client.get_usage_rows.return_value = [_row(f"{RID}-1", 10, 20, 0.5)]

    totals = backend.get_usage_report_for_period([RID], 2026, 8)[RID]["TOTAL_ACCOUNT_USAGE"]

    assert totals["input_tokens"] == 10


def test_the_user_map_is_read_once_for_every_resource_of_a_pass() -> None:
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", RID)]

    backend.get_usage_report_for_period([RID], 2026, 8)
    backend.get_usage_report_for_period(["other"], 2026, 8)

    assert backend.litellm_client.list_users.call_count == 1


# --- component_metrics: decoupling component names from metrics ------------------


def test_component_metrics_feeds_one_metric_to_two_components() -> None:
    """Spend caps on a LIMIT component and bills on a USAGE one, from one metric."""
    components = {
        "token_cost": {"measured_unit": "USD", "accounting_type": "limit"},
        "inference_cost": {"measured_unit": "USD", "accounting_type": "usage"},
    }
    backend = _make_backend(
        components,
        {**SETTINGS, "component_metrics": {"inference_cost": "token_cost"}},
    )
    report: dict = {}
    backend._accumulate(report, RID, _row(RID, 10, 20, 0.5))
    backend._accumulate(report, RID, _row(RID, 1, 2, 0.25))

    # token_cost is metered by its name alone; only inference_cost needed configuring.
    assert report[RID]["TOTAL_ACCOUNT_USAGE"] == {
        "token_cost": pytest.approx(0.75),
        "inference_cost": pytest.approx(0.75),
    }


def test_component_metrics_rejects_unknown_metric() -> None:
    with pytest.raises(BackendError, match="unknown metric"):
        _make_backend(
            {"inference_cost": {"measured_unit": "USD", "accounting_type": "usage"}},
            {**SETTINGS, "component_metrics": {"inference_cost": "spend"}},
        )


def test_component_metrics_skips_components_the_offering_lacks() -> None:
    """A stale mapping entry is dropped, not reported: Waldur rejects the whole pass."""
    backend = _make_backend(
        {"inference_cost": {"measured_unit": "USD", "accounting_type": "usage"}},
        {
            **SETTINGS,
            "component_metrics": {
                "inference_cost": "token_cost",
                "retired": "input_tokens",
            },
        },
    )
    assert backend._usage_metrics == {"inference_cost": "token_cost"}


def test_component_metrics_adds_to_the_name_based_defaults() -> None:
    """The mapping carries only what the names do not already say."""
    backend = _make_backend(
        {**COMPONENTS, "inference_cost": {"measured_unit": "USD", "accounting_type": "usage"}},
        {**SETTINGS, "component_metrics": {"inference_cost": "token_cost"}},
    )
    assert backend._usage_metrics == {
        "input_tokens": "input_tokens",
        "output_tokens": "output_tokens",
        "token_cost": "token_cost",
        "inference_cost": "token_cost",
    }


def test_without_component_metrics_names_still_drive_metering() -> None:
    backend = _make_backend()
    assert backend._usage_metrics == {
        "input_tokens": "input_tokens",
        "output_tokens": "output_tokens",
        "token_cost": "token_cost",
    }


def test_api_usage_on_another_resources_key_is_not_billed_to_the_address_owner() -> None:
    # Ada's address is owned by RID, but this request was served on a key of "other".
    # Attribution by alias wins: the row belongs to "other" and is counted on the pass
    # that has "other" in scope. Billing it to RID here would charge one resource's API
    # traffic to another, and charge it twice overall.
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [
        _user("ada@example.com", RID),
        _user("bob@example.com", "other"),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", "other-1", 10, 20, 0.5)
    ]

    assert backend.get_usage_report_for_period([RID], 2026, 8) == {}


def test_api_usage_on_a_key_only_resource_is_not_billed_to_the_address_owner() -> None:
    # "other" holds keys but has no chat users of its own, so it never appears in the
    # user map. Attribution is still by alias: billing this row to RID here would charge
    # it to the wrong resource, and "other"'s own pass would charge it again.
    backend = _chat_backend()
    backend.litellm_client.list_users.return_value = [_user("ada@example.com", RID)]
    backend.usage_client.get_user_usage_rows.return_value = [
        _user_row("ada@example.com", "other-1", 10, 20, 0.5)
    ]

    assert backend.get_usage_report_for_period([RID], 2026, 8) == {}
