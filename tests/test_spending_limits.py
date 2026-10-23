"""Per-day usage and per-period key limits against a Waldur with spending limits.

waldur/waldur-site-agent#85, on the API of waldur/waldur-mastermind#789 and #817.
"""

from __future__ import annotations

import datetime
import unittest
import uuid
from types import SimpleNamespace
from typing import Any, Optional
from unittest import mock

from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.merged_plugin_options import MergedPluginOptions
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.resource import Resource
from waldur_api_client.types import UNSET

from tests.fixtures import api_key_row
from waldur_site_agent.common import resource_api_keys, spending_limits
from waldur_site_agent.common.processors import OfferingReportProcessor
from waldur_site_agent.common.resource_api_keys import ApiKeyCommand

KEYS = "waldur_site_agent.common.resource_api_keys"
PROCESSORS = "waldur_site_agent.common.processors"
TODAY = datetime.date(2026, 10, 3)


def _component(type_: str, billing_type: BillingTypeEnum, **extra: object) -> OfferingComponent:
    """An offering component as the provider listing returns it; ``extra`` are its JSON fields."""
    return OfferingComponent.from_dict({"type": type_, "billing_type": billing_type.value, **extra})


class TestReadingFields(unittest.TestCase):
    def test_a_field_the_client_predates_is_read_from_the_extra_json(self) -> None:
        resource = Resource.from_dict({"spending_limits": {"token_cost": {"day": "5.00"}}})
        self.assertEqual(spending_limits.resource_spending_limits(resource), {
            "token_cost": {"day": 5.0}
        })

    def test_a_resource_listed_without_the_field_has_none(self) -> None:
        self.assertIsNone(spending_limits.resource_spending_limits(Resource.from_dict({})))

    def test_a_resource_with_no_limits_has_an_empty_set(self) -> None:
        resource = Resource.from_dict({"spending_limits": {}})
        self.assertEqual(spending_limits.resource_spending_limits(resource), {})

    def test_waldur_takes_days_when_its_components_carry_periods(self) -> None:
        old = [_component("token_cost", BillingTypeEnum.USAGE)]
        new = [_component("token_cost", BillingTypeEnum.USAGE, spending_limit_periods={})]
        self.assertFalse(spending_limits.accepts_daily_usage(old))
        self.assertTrue(spending_limits.accepts_daily_usage(new))

    def test_a_key_takes_days_when_its_row_carries_spending_limits(self) -> None:
        self.assertFalse(spending_limits.key_accepts_daily_usage(api_key_row()))
        self.assertTrue(spending_limits.key_accepts_daily_usage(api_key_row(spending_limits=None)))


class TestFillDays(unittest.TestCase):
    def test_every_day_up_to_today_is_reported_zeros_included(self) -> None:
        filled = spending_limits.fill_days(
            {"2026-10-02": {"token_cost": 1.5}}, 2026, 10, {"token_cost"}, TODAY
        )
        self.assertEqual(
            filled,
            {
                "2026-10-01": {"token_cost": 0.0},
                "2026-10-02": {"token_cost": 1.5},
                "2026-10-03": {"token_cost": 0.0},
            },
        )

    def test_a_past_month_is_filled_to_its_last_day(self) -> None:
        filled = spending_limits.fill_days({}, 2026, 9, {"token_cost"}, TODAY)
        self.assertEqual(len(filled), 30)
        self.assertEqual(max(filled), "2026-09-30")

    def test_items_carry_two_decimals(self) -> None:
        self.assertEqual(
            spending_limits.daily_usage_items({"2026-10-01": {"token_cost": 0.125}}),
            [{"type": "token_cost", "date": "2026-10-01", "amount": "0.12"}],
        )

    def test_the_days_sum_to_the_rounded_month(self) -> None:
        # Rounded alone, each $0.004 day would be 0.00 and the month 0.00, not 0.01.
        daily = {f"2026-10-0{day}": {"token_cost": 0.004} for day in (1, 2, 3)}
        items = spending_limits.daily_usage_items(daily)
        self.assertEqual([item["amount"] for item in items], ["0.00", "0.01", "0.00"])
        self.assertAlmostEqual(sum(float(item["amount"]) for item in items), round(0.012, 2))

    def test_the_listing_asks_for_spending_limits_whatever_the_client_knows(self) -> None:
        (field,) = spending_limits.resource_fields()
        self.assertEqual(field.value, "spending_limits")


@mock.patch(f"{PROCESSORS}.spending_limits.utc_today", return_value=TODAY)
@mock.patch(f"{PROCESSORS}.marketplace_component_usages_set_usage")
@mock.patch(f"{PROCESSORS}.marketplace_component_usages_list")
class TestResourceUsagePerDay(unittest.TestCase):
    COMPONENTS = [
        _component("token_cost", BillingTypeEnum.USAGE, spending_limit_periods={"day": {}}),
        _component("input_tokens", BillingTypeEnum.USAGE, spending_limit_periods={}),
        _component("budget", BillingTypeEnum.LIMIT, spending_limit_periods={}),
    ]

    def _processor(self) -> OfferingReportProcessor:
        processor = object.__new__(OfferingReportProcessor)
        processor.waldur_rest_client = mock.sentinel.client
        processor.timezone = ""
        processor.offering = SimpleNamespace(  # type: ignore[assignment]
            omit_anomalous_usage_components=False
        )
        processor.resource_backend = SimpleNamespace(  # type: ignore[assignment]
            supports_decreasing_usage=True
        )
        return processor

    def _submit(self, mock_set: mock.Mock, daily: Optional[dict]) -> dict:
        resource = SimpleNamespace(uuid=uuid.uuid4(), backend_id="res-1")
        self._processor()._submit_total_usage_for_resource(
            resource,
            {"token_cost": 3.0, "input_tokens": 30, "budget": 3.0},
            self.COMPONENTS,
            report_date=datetime.datetime(2026, 10, 1, tzinfo=datetime.timezone.utc),
            billing_period_start=datetime.date(2026, 10, 1),
            daily_usage=daily,
        )
        return mock_set.sync_detailed.call_args.kwargs["body"].to_dict()

    def test_usage_based_components_go_out_per_day(self, mock_list, mock_set, _today) -> None:
        mock_list.sync_all.return_value = []
        body = self._submit(
            mock_set,
            {
                "2026-10-01": {"token_cost": 1.0, "input_tokens": 10, "budget": 1.0},
                "2026-10-02": {"token_cost": 2.0, "input_tokens": 20, "budget": 2.0},
            }
        )
        # The limit-based component keeps its month total; Waldur takes no days for it.
        self.assertEqual([usage["type"] for usage in body["usages"]], ["budget"])
        self.assertEqual(
            sorted((item["type"], item["date"], item["amount"]) for item in body["daily_usages"]),
            [
                ("input_tokens", "2026-10-01", "10.00"),
                ("input_tokens", "2026-10-02", "20.00"),
                ("input_tokens", "2026-10-03", "0.00"),
                ("token_cost", "2026-10-01", "1.00"),
                ("token_cost", "2026-10-02", "2.00"),
                ("token_cost", "2026-10-03", "0.00"),
            ],
        )

    def test_without_days_the_month_totals_go_out_as_before(
        self, mock_list, mock_set, _today
    ) -> None:
        mock_list.sync_all.return_value = []
        body = self._submit(mock_set, None)
        self.assertNotIn("daily_usages", body)
        self.assertEqual(
            sorted(usage["type"] for usage in body["usages"]),
            ["budget", "input_tokens", "token_cost"],
        )

    def test_a_component_the_days_do_not_cover_keeps_its_month_total(
        self, mock_list, mock_set, _today
    ) -> None:
        mock_list.sync_all.return_value = []
        body = self._submit(mock_set, {"2026-10-01": {"token_cost": 3.0}})
        self.assertEqual(
            sorted(usage["type"] for usage in body["usages"]), ["budget", "input_tokens"]
        )
        self.assertEqual({item["type"] for item in body["daily_usages"]}, {"token_cost"})


class TestDailyUsageGate(unittest.TestCase):
    def _processor(self, backend: Any) -> OfferingReportProcessor:  # noqa: ANN401
        processor = object.__new__(OfferingReportProcessor)
        processor.resource_backend = backend
        return processor

    def test_asked_only_where_backend_and_waldur_both_take_days(self) -> None:
        new = SimpleNamespace(components=TestResourceUsagePerDay.COMPONENTS)
        old = SimpleNamespace(components=[_component("token_cost", BillingTypeEnum.USAGE)])
        daily_backend = mock.Mock(supports_daily_usage=True)
        daily_backend.get_daily_usage_report_for_period.return_value = {"res-1": {"d": {}}}

        self.assertEqual(
            self._processor(daily_backend)._daily_usage("res-1", new, 2026, 10), {"d": {}}
        )
        self.assertIsNone(self._processor(daily_backend)._daily_usage("res-1", old, 2026, 10))
        monthly_backend = SimpleNamespace(supports_daily_usage=False)
        self.assertIsNone(self._processor(monthly_backend)._daily_usage("res-1", new, 2026, 10))


@mock.patch(f"{PROCESSORS}.logger")
class TestApiKeyDailyUsageWarning(unittest.TestCase):
    """A backend reporting per-key usage per day, on an offering that does not say so."""

    def _warn(
        self,
        options: dict,
        backend: Any = None,  # noqa: ANN401
        components: Optional[list] = None,
    ) -> None:
        processor = object.__new__(OfferingReportProcessor)
        processor.offering = SimpleNamespace(uuid="offering-uuid")  # type: ignore[assignment]
        processor.resource_backend = backend or SimpleNamespace(  # type: ignore[assignment]
            supports_daily_usage=True, supports_resource_api_key_usage=True
        )
        processor.waldur_offering = SimpleNamespace(  # type: ignore[assignment]
            plugin_options=MergedPluginOptions.from_dict(options)
        )
        if components is None:
            components = TestResourceUsagePerDay.COMPONENTS
        processor._warn_without_api_key_daily_usage(
            SimpleNamespace(components=components)  # type: ignore[arg-type]
        )

    def _warnings(self, mock_logger: mock.Mock) -> list:
        return [call.args[0] % call.args[1:] for call in mock_logger.warning.call_args_list]

    def test_an_offering_without_the_option_is_told_what_it_would_offer(
        self, mock_logger: mock.Mock
    ) -> None:
        self._warn({})

        self.assertEqual(
            self._warnings(mock_logger),
            [
                "Offering offering-uuid does not declare api_key_daily_usage, so Waldur "
                "offers only monthly limits on its API keys; turn it on to offer day and "
                "week limits."
            ],
        )

    def test_an_offering_with_it_off_is_told_too(self, mock_logger: mock.Mock) -> None:
        self._warn({"api_key_daily_usage": False})

        self.assertEqual(len(self._warnings(mock_logger)), 1)

    def test_an_offering_with_it_on_is_not(self, mock_logger: mock.Mock) -> None:
        self._warn({"api_key_daily_usage": True})

        mock_logger.warning.assert_not_called()

    def test_an_offering_whose_keys_take_no_limits_is_not(self, mock_logger: mock.Mock) -> None:
        self._warn({"api_key_limits": False})

        mock_logger.warning.assert_not_called()

    def test_a_backend_without_daily_or_key_usage_is_not(self, mock_logger: mock.Mock) -> None:
        for daily, per_key in ((False, True), (True, False)):
            self._warn(
                {},
                SimpleNamespace(
                    supports_daily_usage=daily, supports_resource_api_key_usage=per_key
                ),
            )

        mock_logger.warning.assert_not_called()

    def test_a_waldur_without_usage_per_day_is_not(self, mock_logger: mock.Mock) -> None:
        self._warn({}, components=[_component("token_cost", BillingTypeEnum.USAGE)])

        mock_logger.warning.assert_not_called()

    @mock.patch(f"{PROCESSORS}.marketplace_provider_resources_list")
    @mock.patch(f"{PROCESSORS}.marketplace_provider_offerings_retrieve")
    def test_a_cycle_warns_once_and_reports_as_before(
        self, mock_retrieve: mock.Mock, mock_list: mock.Mock, mock_logger: mock.Mock
    ) -> None:
        processor = object.__new__(OfferingReportProcessor)
        processor.offering = SimpleNamespace(  # type: ignore[assignment]
            uuid="offering-uuid", name="LLM"
        )
        processor.waldur_rest_client = mock.sentinel.client
        processor.resource_backend = SimpleNamespace(  # type: ignore[assignment]
            supports_daily_usage=True, supports_resource_api_key_usage=True
        )
        processor.waldur_offering = SimpleNamespace(  # type: ignore[assignment]
            plugin_options=MergedPluginOptions()
        )
        mock_retrieve.sync.return_value = SimpleNamespace(
            components=TestResourceUsagePerDay.COMPONENTS
        )
        mock_list.sync_all.return_value = [mock.sentinel.r1, mock.sentinel.r2]

        with mock.patch.object(
            OfferingReportProcessor, "_process_resource_with_retries"
        ) as mock_process:
            processor.process_offering()

        self.assertEqual(len(self._warnings(mock_logger)), 1)
        self.assertEqual(mock_process.call_count, 2)


@mock.patch(f"{KEYS}.spending_limits.utc_today", return_value=TODAY)
@mock.patch(f"{KEYS}._current_period", return_value=datetime.date(2026, 10, 1))
@mock.patch(f"{KEYS}.marketplace_resource_api_keys_report_usage")
@mock.patch(f"{KEYS}.list_all_api_keys")
class TestKeyUsagePerDay(unittest.TestCase):
    K1 = "00000000-0000-0000-0000-000000000001"
    K2 = "00000000-0000-0000-0000-000000000002"

    @staticmethod
    def _backend(daily: object) -> mock.Mock:
        backend = mock.Mock(supports_daily_usage=True)
        backend.get_resource_key_daily_usage_report.return_value = {"res-1": daily}
        return backend

    def _bodies(self, mock_report: mock.Mock) -> dict:
        return {
            str(call.kwargs["uuid"]): call.kwargs["body"].to_dict()
            for call in mock_report.sync.call_args_list
        }

    def test_a_key_of_a_waldur_with_days_is_reported_per_day(
        self, mock_list, mock_report, _period, _today
    ) -> None:
        mock_list.return_value = [
            api_key_row(uuid=self.K1, client_id="res-1-1", spending_limits=None),
            api_key_row(uuid=self.K2, client_id="res-1-2", spending_limits=None),
        ]
        resource_api_keys.report_api_key_usages(
            mock.sentinel.client,
            self._backend({"res-1-1": {"2026-10-02": {"token_cost": 2.5}}}),
            "res-uuid",
            "res-1",
            ["token_cost"],
        )
        bodies = self._bodies(mock_report)
        self.assertEqual(bodies[self.K1]["usages"], {})
        self.assertEqual(
            [(item["date"], item["amount"]) for item in bodies[self.K1]["daily_usages"]],
            [("2026-10-01", "0.00"), ("2026-10-02", "2.50"), ("2026-10-03", "0.00")],
        )
        # A key with no usage left this month still has its days set to zero: chat that
        # moved off it with a reassignment would otherwise keep counting there.
        self.assertEqual(
            {item["amount"] for item in bodies[self.K2]["daily_usages"]}, {"0.00"}
        )

    def test_a_key_of_a_waldur_without_days_gets_the_month_from_the_days(
        self, mock_list, mock_report, _period, _today
    ) -> None:
        mock_list.return_value = [api_key_row(uuid=self.K1, client_id="res-1-1")]
        resource_api_keys.report_api_key_usages(
            mock.sentinel.client,
            self._backend(
                {"res-1-1": {"2026-10-01": {"token_cost": 1.0}, "2026-10-02": {"token_cost": 2.5}}}
            ),
            "res-uuid",
            "res-1",
            ["token_cost"],
        )
        body = self._bodies(mock_report)[self.K1]
        self.assertEqual(body["usages"], {"token_cost": 3.5})
        self.assertNotIn("daily_usages", body)

    def test_unattributable_usage_is_not_reported(
        self, mock_list, mock_report, _period, _today
    ) -> None:
        resource_api_keys.report_api_key_usages(
            mock.sentinel.client, self._backend(None), "res-uuid", "res-1", ["token_cost"]
        )
        mock_list.assert_not_called()
        mock_report.sync.assert_not_called()


class TestKeyCommandLimits(unittest.TestCase):
    PERIODS = {"token_cost": {"day": 5, "month": 50}}

    def test_a_message_carries_the_per_period_limits(self) -> None:
        command = ApiKeyCommand.from_message(
            {"action": "update", "limits": {"token_cost": 50}, "spending_limits": self.PERIODS}
        )
        self.assertEqual(command.spending_limits, {"token_cost": {"day": 5.0, "month": 50.0}})

    def test_a_message_of_an_older_waldur_carries_none(self) -> None:
        command = ApiKeyCommand.from_message({"action": "update", "limits": {"token_cost": 50}})
        self.assertIsNone(command.spending_limits)

    def test_a_listed_key_carries_them_too(self) -> None:
        command = ApiKeyCommand.from_listing(
            api_key_row(client_id="res-1-1", spending_limits=self.PERIODS), "update"
        )
        self.assertEqual(command.spending_limits, {"token_cost": {"day": 5.0, "month": 50.0}})

    def test_only_a_backend_that_takes_periods_is_handed_them(self) -> None:
        command = ApiKeyCommand(
            action="update",
            api_key_uuid="k",
            resource_uuid="r",
            resource_backend_id="res-1",
            client_id="res-1-1",
            limits={"token_cost": 50},
            spending_limits=self.PERIODS,
        )
        periodic = mock.Mock(supports_spending_limit_periods=True)
        monthly = mock.Mock(spec=["update_resource_key"])
        for backend in (periodic, monthly):
            with (
                mock.patch(f"{KEYS}._apply_resource_pause"),
                mock.patch(f"{KEYS}._read_resource"),
                mock.patch(f"{KEYS}.marketplace_resource_api_keys_set_ok"),
            ):
                resource_api_keys._run(mock.sentinel.client, backend, command)

        self.assertEqual(
            periodic.update_resource_key.call_args.kwargs["spending_limits"], self.PERIODS
        )
        self.assertNotIn("spending_limits", monthly.update_resource_key.call_args.kwargs)

    @mock.patch(f"{KEYS}.utils.rotate_resource_api_key")
    @mock.patch(f"{KEYS}._read_resource", return_value=SimpleNamespace(paused=False))
    def test_a_rotation_hands_them_over_too(self, _read, mock_rotate) -> None:
        command = ApiKeyCommand(
            action="rotate",
            api_key_uuid="k",
            resource_uuid="r",
            resource_backend_id="res-1",
            client_id="res-1-1",
            limits={"token_cost": 50},
            spending_limits=self.PERIODS,
        )
        periodic = mock.Mock(
            supports_resource_api_key_lifecycle=True, supports_spending_limit_periods=True
        )
        monthly = mock.Mock(supports_resource_api_key_lifecycle=True)
        settings = []
        for backend in (periodic, monthly):
            resource_api_keys._rotate(mock.sentinel.client, backend, command, True)
            settings.append(mock_rotate.call_args.kwargs["key_settings"])

        self.assertEqual(settings[0]["spending_limits"], self.PERIODS)
        self.assertNotIn("spending_limits", settings[1])


def test_unset_is_what_an_absent_field_reads_as() -> None:
    assert spending_limits.generated_field(Resource.from_dict({}), "spending_limits") is UNSET
