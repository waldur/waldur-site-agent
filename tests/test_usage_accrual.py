"""Accrual of levels into period totals, kept in Waldur's usage description.

The pure rules in ``common/usage_accrual.py``, and the report processor driving
them against a respx-stubbed Waldur.
"""

from __future__ import annotations

import datetime
import json
import unittest
import uuid
from decimal import Decimal
from types import SimpleNamespace
from unittest import mock

import respx
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.offering_component import OfferingComponent

from tests.fixtures import OFFERING
from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import processors, usage_accrual
from waldur_site_agent.common.processors import OfferingReportProcessor

UTC = datetime.timezone.utc
DAY = 86400


def _at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(year, month, day, hour, minute, tzinfo=UTC)


def _checkpoint(description: object) -> usage_accrual.Checkpoint:
    checkpoint = usage_accrual.parse_checkpoint(description)
    assert checkpoint is not None
    return checkpoint


class TestCheckpoint(unittest.TestCase):
    def test_round_trip(self) -> None:
        text = usage_accrual.format_checkpoint(Decimal("12.3456789"), _at(2026, 11, 21, 23, 40), 9.5)
        checkpoint = usage_accrual.parse_checkpoint(text)
        assert checkpoint == usage_accrual.Checkpoint(
            Decimal("12.345679"), _at(2026, 11, 21, 23, 40), Decimal("9.5")
        )
        assert "Z" in text and "9.5" in text

    def test_foreign_or_empty_descriptions_are_no_checkpoint(self) -> None:
        for description in ("", None, "Reported by hand", "Accrued x to yesterday", 5):
            assert usage_accrual.parse_checkpoint(description) is None

    def test_offset_without_zone_is_refused_and_offsets_are_read(self) -> None:
        assert usage_accrual.parse_checkpoint("Accrued 1 to 2026-11-21T10:00:00") is None
        checkpoint = usage_accrual.parse_checkpoint("Accrued 1 to 2026-11-21T12:00:00+02:00")
        assert checkpoint is not None and checkpoint.at == _at(2026, 11, 21, 10)

    def test_latest_of_several_rows_wins(self) -> None:
        older = usage_accrual.format_checkpoint(Decimal(1), _at(2026, 11, 10), 1)
        newer = usage_accrual.format_checkpoint(Decimal(2), _at(2026, 11, 20), 1)
        checkpoint = usage_accrual.latest_checkpoint([newer, "", older, None])
        assert checkpoint is not None and checkpoint.total == Decimal(2)


class TestAccrue(unittest.TestCase):
    def _accrue(self, **overrides: object) -> usage_accrual.Accrual:
        kwargs: dict = {
            "level": 10.0,
            "unit_seconds": DAY,
            "stored_total": None,
            "checkpoint": None,
            "start_without_checkpoint": _at(2026, 11, 1),
            "until": _at(2026, 11, 1, 12),
            "max_gap_seconds": DAY,
        }
        kwargs.update(overrides)
        return usage_accrual.accrue(**kwargs)

    def test_level_times_elapsed_since_start(self) -> None:
        accrual = self._accrue()
        assert accrual.total == Decimal(5)  # 10 GB for half a day
        assert accrual.elapsed_seconds == DAY / 2
        assert usage_accrual.parse_checkpoint(accrual.description) == usage_accrual.Checkpoint(
            Decimal(5), _at(2026, 11, 1, 12), Decimal(10)
        )

    def test_the_interval_is_billed_at_the_checkpoints_level(self) -> None:
        checkpoint = usage_accrual.Checkpoint(Decimal(5), _at(2026, 11, 1, 12), Decimal(0))
        accrual = self._accrue(level=1000.0, stored_total=5.0, checkpoint=checkpoint, until=_at(2026, 11, 4))
        # Empty until this pass: the days before it bill nothing, and 1000 is
        # recorded for the next interval.
        assert accrual.total == Decimal(5)
        assert _checkpoint(accrual.description).level == Decimal(1000)

    def test_tiny_and_huge_levels_survive_the_description(self) -> None:
        for level in (1e-05, 0.715, 123456789.123):
            text = usage_accrual.format_checkpoint(Decimal(1), _at(2026, 11, 1), level)
            assert _checkpoint(text).level == Decimal(repr(level))

    def test_continues_from_the_checkpoint_exact_total(self) -> None:
        # Waldur holds 0.01 (two decimals); the description keeps 0.0052083...
        checkpoint = usage_accrual.Checkpoint(Decimal("0.005208"), _at(2026, 11, 1, 12))
        accrual = self._accrue(
            level=0.25,
            stored_total=0.01,
            checkpoint=checkpoint,
            until=_at(2026, 11, 1, 12, 30),
        )
        assert accrual.total == Decimal("0.005208") + Decimal("0.25") * Decimal(1800) / DAY

    def test_rounding_never_drifts_over_many_passes(self) -> None:
        # A 0.715 GB resource at 30-minute passes adds 0.0149 GB-days a pass: read
        # back at two decimals, every pass would drop a third of it.
        checkpoint = None
        stored = None
        until = _at(2026, 11, 1)
        for _ in range(48 * 30):
            until += datetime.timedelta(minutes=30)
            accrual = self._accrue(
                level=0.715,
                stored_total=stored,
                checkpoint=checkpoint,
                start_without_checkpoint=_at(2026, 11, 1),
                until=until,
            )
            stored = float(accrual.total.quantize(Decimal("0.01")))
            checkpoint = usage_accrual.parse_checkpoint(accrual.description)
        assert stored is not None
        assert abs(stored - 0.715 * 30) < 0.01

    def test_a_changed_waldur_figure_is_continued_from(self) -> None:
        checkpoint = usage_accrual.Checkpoint(Decimal(5), _at(2026, 11, 1, 12))
        with self.assertLogs(usage_accrual.logger, "WARNING"):
            accrual = self._accrue(stored_total=7.0, checkpoint=checkpoint, until=_at(2026, 11, 2))
        assert accrual.total == Decimal(12)

    def test_gap_beyond_the_cap_bills_only_the_cap(self) -> None:
        checkpoint = usage_accrual.Checkpoint(Decimal(0), _at(2026, 11, 1))
        accrual = self._accrue(stored_total=0.0, checkpoint=checkpoint, until=_at(2026, 11, 4))
        assert accrual.total == Decimal(10)
        assert accrual.capped_seconds == 2 * DAY

    def test_a_checkpoint_never_moves_backwards(self) -> None:
        # Another instance, its clock ahead, wrote a checkpoint after this "now".
        checkpoint = usage_accrual.Checkpoint(Decimal(5), _at(2026, 11, 2), Decimal(10))
        accrual = self._accrue(stored_total=5.0, checkpoint=checkpoint, until=_at(2026, 11, 1, 23, 50))
        assert _checkpoint(accrual.description).at == _at(2026, 11, 2)

    def test_checkpoint_in_the_future_adds_nothing(self) -> None:
        checkpoint = usage_accrual.Checkpoint(Decimal(5), _at(2026, 11, 2))
        accrual = self._accrue(stored_total=5.0, checkpoint=checkpoint, until=_at(2026, 11, 1, 23))
        assert accrual.total == Decimal(5) and accrual.elapsed_seconds == 0


class TestProcessorAccrual(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"
    RESOURCE_UUID = "10a0f810be1c43bbb651e8cbdbb90198"

    def setUp(self) -> None:
        processors._SETTLED_PAST_PERIODS.clear()
        self.addCleanup(processors._SETTLED_PAST_PERIODS.clear)
        self.backend = mock.MagicMock(spec=BaseBackend)
        self.backend.backend_type = "level"
        self.backend.supports_decreasing_usage = False
        self.backend.max_accrual_gap_seconds = DAY
        self.backend.backend_components = {"storage": {}, "traffic": {}}
        self.backend.accrued_usage_components.return_value = {"storage": DAY}
        self.backend.get_usage_report_for_period.return_value = {
            "res-01": {"TOTAL_ACCOUNT_USAGE": {"traffic": 3.0}}
        }
        proc = OfferingReportProcessor.__new__(OfferingReportProcessor)
        proc.resource_backend = self.backend
        proc.timezone = ""  # type: ignore[attr-defined]
        proc.waldur_rest_client = AuthenticatedClient(  # type: ignore[attr-defined]
            base_url=self.BASE_URL, token=OFFERING.api_token, headers={}
        )
        proc.offering = OFFERING.model_copy(update={"omit_anomalous_usage_components": False})
        proc._offering_users_cache = []
        self.proc = proc
        self.resource = SimpleNamespace(
            uuid=uuid.UUID(self.RESOURCE_UUID),
            name="res",
            backend_id="res-01",
            created=_at(2026, 10, 15),
        )
        self.offering = SimpleNamespace(
            components=[OfferingComponent(type_="storage"), OfferingComponent(type_="traffic")]
        )

    def _rows(self, rows: list[dict]) -> None:
        respx.get(f"{self.BASE_URL}/api/marketplace-component-usages/").respond(200, json=rows)
        respx.get(f"{self.BASE_URL}/api/marketplace-component-user-usages/").respond(200, json=[])

    def _run(self, now: datetime.datetime, *, is_current: bool, year: int, month: int, level=10.0):
        set_usage = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})
        info = BackendResourceInfo(
            backend_id="res-01",
            usage={"TOTAL_ACCOUNT_USAGE": {"storage": level, "traffic": 1.0}},
        )
        with mock.patch.object(usage_accrual, "utcnow", return_value=now):
            self.proc._process_resource_period(
                waldur_resource_info=self.resource,
                waldur_offering=self.offering,
                resource_backend_id="res-01",
                backend_resource_info=info,
                year=year,
                month=month,
                is_current=is_current,
            )
        if not set_usage.call_count:
            return None
        return {
            u["type"]: (u["amount"], u.get("description"))
            for u in json.loads(set_usage.calls.last.request.content)["usages"]
        }

    @respx.mock
    def test_first_pass_accrues_from_the_month_start(self) -> None:
        self._rows([])
        sent = self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11)
        assert sent is not None
        assert sent["storage"][0] == "5.00"
        assert _checkpoint(sent["storage"][1]).at == _at(2026, 11, 1, 12)
        # Traffic is a period total: passed through, no checkpoint.
        assert sent["traffic"] == ("1.00", None)

    @respx.mock
    def test_a_resource_created_this_month_accrues_from_its_creation(self) -> None:
        self.resource.created = _at(2026, 11, 1, 6)
        self._rows([])
        sent = self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11)
        assert sent["storage"][0] == "2.50"

    @respx.mock
    def test_next_pass_adds_the_interval_since_the_checkpoint(self) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(5), _at(2026, 11, 1, 12), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "5.00", "description": checkpoint},
        ])
        sent = self._run(_at(2026, 11, 1, 18), is_current=True, year=2026, month=11, level=20.0)
        # The quarter day is billed at the 10 GB the checkpoint recorded; 20 GB
        # applies from now on.
        assert sent["storage"][0] == "7.50"
        assert _checkpoint(sent["storage"][1]).level == Decimal(20)

    @respx.mock
    def test_a_figure_without_checkpoint_is_kept_and_accrual_starts_now(self) -> None:
        self._rows([{"uuid": uuid.uuid4().hex, "type": "storage", "usage": "40.00", "description": ""}])
        sent = self._run(_at(2026, 11, 20), is_current=True, year=2026, month=11)
        assert sent["storage"][0] == "40.00"
        assert _checkpoint(sent["storage"][1]).at == _at(2026, 11, 20)

    @respx.mock
    def test_an_unmeasured_level_is_not_reported(self) -> None:
        self._rows([])
        sent = self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11, level=None)
        assert sent == {"traffic": ("1.00", None)}

    @respx.mock
    def test_previous_month_is_closed_to_its_end(self) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(100), _at(2026, 10, 31, 18), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "100.00", "description": checkpoint},
        ])
        sent = self._run(_at(2026, 11, 1, 0, 10), is_current=False, year=2026, month=10)
        assert sent["storage"][0] == "102.50"  # 6 h at 10 GB
        assert _checkpoint(sent["storage"][1]).at == _at(2026, 11, 1)
        # The past month's other components come from the backend's history.
        assert sent["traffic"] == ("3.00", None)

    @respx.mock
    def test_a_closed_previous_month_is_left_alone(self) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(102.5), _at(2026, 11, 1), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "102.50", "description": checkpoint},
            {"uuid": uuid.uuid4().hex, "type": "traffic", "usage": "3.00", "description": ""},
        ])
        # Nothing changed for the closed month: no write, not even for traffic.
        assert self._run(_at(2026, 11, 1, 0, 40), is_current=False, year=2026, month=10) is None

    @respx.mock
    def test_a_closed_past_month_is_not_read_again(self) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(102.5), _at(2026, 11, 1), 10)
        rows = respx.get(f"{self.BASE_URL}/api/marketplace-component-usages/").respond(200, json=[
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "102.50", "description": checkpoint},
            {"uuid": uuid.uuid4().hex, "type": "traffic", "usage": "3.00", "description": ""},
        ])
        respx.get(f"{self.BASE_URL}/api/marketplace-component-user-usages/").respond(200, json=[])
        self._run(_at(2026, 11, 1, 0, 40), is_current=False, year=2026, month=10)
        first = rows.call_count
        self._run(_at(2026, 11, 1, 1, 10), is_current=False, year=2026, month=10)
        # The second pass reads only for the traffic comparison, not for storage.
        assert rows.call_count - first == 1
        assert (self.RESOURCE_UUID, 2026, 10) in processors._SETTLED_PAST_PERIODS

    @respx.mock
    def test_backends_without_levels_send_no_descriptions(self) -> None:
        self.backend.accrued_usage_components.return_value = {}
        self._rows([])
        sent = self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11)
        assert sent == {"storage": ("10.00", None), "traffic": ("1.00", None)}

    def _idle_rows(self, at: datetime.datetime, total: str = "5.00", level: float = 0) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(total), at, level)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": total, "description": checkpoint},
            {"uuid": uuid.uuid4().hex, "type": "traffic", "usage": "1.00", "description": ""},
        ])

    @respx.mock
    def test_an_unchanged_idle_resource_is_not_written(self) -> None:
        self._idle_rows(_at(2026, 11, 1))
        assert self._run(_at(2026, 11, 1, 6), is_current=True, year=2026, month=11, level=0.0) is None

    @respx.mock
    def test_an_upload_after_idling_is_billed_from_now_not_back(self) -> None:
        # Checkpoint three days old: the skipped passes held an empty bucket.
        self._idle_rows(_at(2026, 11, 1), level=0)
        sent = self._run(_at(2026, 11, 1, 11), is_current=True, year=2026, month=11, level=10000.0)
        assert sent is not None
        assert sent["storage"][0] == "5.00"
        assert _checkpoint(sent["storage"][1]).level == Decimal(10000)

    @respx.mock
    def test_a_quiet_checkpoint_is_written_before_half_the_cap(self) -> None:
        self._idle_rows(_at(2026, 11, 1))
        sent = self._run(_at(2026, 11, 1, 13), is_current=True, year=2026, month=11, level=0.0)
        assert sent is not None
        assert _checkpoint(sent["storage"][1]).at == _at(2026, 11, 1, 13)

    @respx.mock
    def test_a_plan_switch_month_is_accrued_not_flagged_as_an_anomaly(self) -> None:
        # One row per plan after a switch; the month's total is their sum.
        older = usage_accrual.format_checkpoint(Decimal(5), _at(2026, 11, 10), 10)
        newer = usage_accrual.format_checkpoint(Decimal(7), _at(2026, 11, 20), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "5.00", "description": older},
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "2.00", "description": newer},
        ])
        sent = self._run(_at(2026, 11, 20, 12), is_current=True, year=2026, month=11)
        assert sent is not None
        assert sent["storage"][0] == "12.00"  # 7 + 10 GB for half a day

    @respx.mock
    def test_the_usage_rows_are_read_once_per_period(self) -> None:
        rows = respx.get(f"{self.BASE_URL}/api/marketplace-component-usages/").respond(200, json=[])
        respx.get(f"{self.BASE_URL}/api/marketplace-component-user-usages/").respond(200, json=[])
        self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11)
        periods = [call.request.url.params.get("billing_period") for call in rows.calls]
        # November once (with descriptions, reused for the submission), and
        # October once, for the level the month opened at.
        assert sorted(periods) == ["2026-10-01", "2026-11-01"]
        november = next(c for c in rows.calls if c.request.url.params.get("billing_period") == "2026-11-01")
        assert "description" in november.request.url.params.get_list("field")

    @respx.mock
    def test_a_month_opens_at_the_level_the_previous_month_last_recorded(self) -> None:
        october = usage_accrual.format_checkpoint(Decimal(100), _at(2026, 10, 31, 23, 45), 0)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/", params={"billing_period": "2026-10-01"}
        ).respond(200, json=[{"type": "storage", "usage": "100.00", "description": october}])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/", params={"billing_period": "2026-11-01"}
        ).respond(200, json=[])
        respx.get(f"{self.BASE_URL}/api/marketplace-component-user-usages/").respond(200, json=[])
        # 10 GB uploaded at 00:14: the first quarter hour of November was empty.
        sent = self._run(_at(2026, 11, 1, 0, 15), is_current=True, year=2026, month=11, level=10.0)
        assert sent is not None
        assert sent["storage"][0] == "0.00"
        assert _checkpoint(sent["storage"][1]).level == Decimal(10)

    @respx.mock
    def test_an_outage_shorter_than_the_cap_is_billed_whole_after_a_quiet_spell(self) -> None:
        # The checkpoint lagged 11 h (quiet), then the agent was down 13 h.
        self._idle_rows(_at(2026, 11, 1), total="0.00", level=10)
        sent = self._run(_at(2026, 11, 2, 1), is_current=True, year=2026, month=11, level=10.0)
        assert sent is not None
        assert sent["storage"][0] == "10.42"  # 25 h at 10 GB, not clipped to 24 h

    @respx.mock
    def test_a_plan_switch_month_does_not_block_other_components(self) -> None:
        # Traffic has one row per plan too; the report is the month's total.
        checkpoint = usage_accrual.format_checkpoint(Decimal(7), _at(2026, 11, 20), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "7.00", "description": checkpoint},
            {"uuid": uuid.uuid4().hex, "type": "traffic", "usage": "0.40", "description": ""},
            {"uuid": uuid.uuid4().hex, "type": "traffic", "usage": "0.50", "description": ""},
        ])
        sent = self._run(_at(2026, 11, 20, 12), is_current=True, year=2026, month=11)
        assert sent is not None
        assert sent["storage"][0] == "12.00" and sent["traffic"][0] == "1.00"

    @respx.mock
    def test_closing_records_the_level_it_billed_at(self) -> None:
        # Held 10 GB to the end of October; emptied before the first November pass.
        checkpoint = usage_accrual.format_checkpoint(Decimal(100), _at(2026, 10, 31, 18), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "100.00", "description": checkpoint},
        ])
        sent = self._run(_at(2026, 11, 1, 0, 10), is_current=False, year=2026, month=10, level=0.0)
        assert sent is not None
        assert sent["storage"][0] == "102.50"
        # November opens at 10 GB, not at the 0 measured after the boundary.
        assert _checkpoint(sent["storage"][1]).level == Decimal(10)

    @respx.mock
    def test_an_unreadable_previous_month_leaves_storage_for_the_next_pass(self) -> None:
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/", params={"billing_period": "2026-10-01"}
        ).respond(500)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/", params={"billing_period": "2026-11-01"}
        ).respond(200, json=[])
        respx.get(f"{self.BASE_URL}/api/marketplace-component-user-usages/").respond(200, json=[])
        sent = self._run(_at(2026, 11, 1, 12), is_current=True, year=2026, month=11)
        assert sent == {"traffic": ("1.00", None)}

    @respx.mock
    def test_a_month_is_closed_even_when_nothing_was_measured_this_pass(self) -> None:
        checkpoint = usage_accrual.format_checkpoint(Decimal(100), _at(2026, 10, 31, 18), 10)
        self._rows([
            {"uuid": uuid.uuid4().hex, "type": "storage", "usage": "100.00", "description": checkpoint},
        ])
        sent = self._run(_at(2026, 11, 1, 0, 10), is_current=False, year=2026, month=10, level=None)
        assert sent is not None and sent["storage"][0] == "102.50"


def test_a_negative_level_is_no_checkpoint() -> None:
    assert usage_accrual.parse_checkpoint("Accrued 5 to 2026-11-01T00:00:00Z, at a level of -3.0 at that moment") is None


class TestSettledPastPeriods(unittest.TestCase):
    def setUp(self) -> None:
        processors._SETTLED_PAST_PERIODS.clear()
        self.addCleanup(processors._SETTLED_PAST_PERIODS.clear)

    def test_old_entries_are_pruned_as_months_pass(self) -> None:
        processors._settle_past_period("r", 2025, 1)
        processors._settle_past_period("r", 2026, 10)
        assert ("r", 2025, 1) not in processors._SETTLED_PAST_PERIODS
        assert ("r", 2026, 10) in processors._SETTLED_PAST_PERIODS
