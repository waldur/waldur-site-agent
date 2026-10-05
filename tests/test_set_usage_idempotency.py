"""Tests for the idempotency check on ``_submit_total_usage_for_resource``.

Covers the pure helper (``_usage_matches_existing``) and a respx-driven
end-to-end check that confirms ``marketplace_component_usages_set_usage``
is NOT called when the reported usage matches every existing record.
"""

from __future__ import annotations

import datetime
import decimal
import json
import unittest
import uuid
from types import SimpleNamespace
from unittest import mock

import respx
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import ServiceProvider
from waldur_api_client.models.component_usage import ComponentUsage
from waldur_api_client.types import UNSET

from tests.fixtures import OFFERING, user_me_api_response
from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.processors import OfferingReportProcessor, UsageAnomalyError


class TestUsageMatchesExisting(unittest.TestCase):
    """Pure unit tests for the static comparison helper."""

    fn = staticmethod(OfferingReportProcessor._usage_matches_existing)

    def _eu(self, type_, usage) -> ComponentUsage:
        """Build a ComponentUsage with just the fields the helper inspects."""
        return ComponentUsage(type_=type_, usage=usage)

    def test_no_existing_no_match(self) -> None:
        assert self.fn({"cpu": 100}, [], ["cpu"]) is False

    def test_empty_input_empty_existing_matches(self) -> None:
        assert self.fn({}, [], ["cpu"]) is True

    def test_exact_match_returns_true(self) -> None:
        existing = [self._eu("cpu", 100), self._eu("mem", 50)]
        assert self.fn({"cpu": 100.0, "mem": 50.0}, existing, ["cpu", "mem"]) is True

    def test_amount_differs_returns_false(self) -> None:
        existing = [self._eu("cpu", 100)]
        assert self.fn({"cpu": 100.01}, existing, ["cpu"]) is False

    def test_amount_close_within_tolerance_returns_true(self) -> None:
        # 100 vs 100.0000001 — abs diff well inside abs_tol=1e-6
        existing = [self._eu("cpu", 100.0)]
        assert self.fn({"cpu": 100.0000001}, existing, ["cpu"]) is True

    def test_new_component_appears_returns_false(self) -> None:
        existing = [self._eu("cpu", 100)]
        assert self.fn({"cpu": 100, "mem": 50}, existing, ["cpu", "mem"]) is False

    def test_component_disappears_returns_false(self) -> None:
        existing = [self._eu("cpu", 100), self._eu("mem", 50)]
        assert self.fn({"cpu": 100}, existing, ["cpu", "mem"]) is False

    def test_decimal_string_amounts_are_compared(self) -> None:
        # Waldur returns amounts as Decimal; the helper uses float() on both sides
        existing = [self._eu("cpu", "100.00")]
        assert self.fn({"cpu": 100.0}, existing, ["cpu"]) is True

    def test_zero_usage_matches_zero(self) -> None:
        # Important for chronically-zero accounts that the DWDI plugin reports
        # every cycle. Once the zero baseline is written, repeated zero reports
        # should be skipped.
        existing = [self._eu("cpu", 0)]
        assert self.fn({"cpu": 0.0}, existing, ["cpu"]) is True

    def test_extra_component_not_in_offering_is_ignored(self) -> None:
        # component_types is the source of truth for what we'd actually submit;
        # if the backend reports a component Waldur doesn't know, it's logged
        # and dropped — and shouldn't affect idempotency.
        existing = [self._eu("cpu", 100)]
        assert self.fn(
            {"cpu": 100, "unknown_in_waldur": 999},
            existing,
            ["cpu"],
        ) is True

    def test_unparseable_existing_amount_returns_false(self) -> None:
        existing = [self._eu("cpu", "not-a-number")]
        assert self.fn({"cpu": 100}, existing, ["cpu"]) is False

    def test_duplicate_existing_type_returns_false(self) -> None:
        # Two records for the same component-type are unexpected; safer to
        # re-submit and let the server reconcile than to skip.
        existing = [self._eu("cpu", 100), self._eu("cpu", 100)]
        assert self.fn({"cpu": 100}, existing, ["cpu"]) is False

    def test_unset_fields_are_skipped(self) -> None:
        # The SDK declares type_ and usage as Union[Unset, ...]. Records
        # where the listing didn't populate them must not be treated as a
        # match — otherwise we could skip a legitimate submission.
        existing = [
            ComponentUsage(type_=UNSET, usage=100),
            self._eu("cpu", 100),
        ]
        # The UNSET record is skipped; the cpu record matches.
        assert self.fn({"cpu": 100.0}, existing, ["cpu"]) is True

        # If the only existing record has UNSET, it contributes nothing and
        # the comparison falls through to "no existing for cpu" → mismatch.
        only_unset = [ComponentUsage(type_=UNSET, usage=100)]
        assert self.fn({"cpu": 100}, only_unset, ["cpu"]) is False


class TestReportedAmountsMatchExisting(unittest.TestCase):
    """Pure unit tests for the post-anomaly amount comparison helper."""

    fn = staticmethod(OfferingReportProcessor._reported_amounts_match_existing)

    def _eu(self, type_, usage) -> ComponentUsage:
        return ComponentUsage(type_=type_, usage=usage)

    def test_empty_reported_matches(self) -> None:
        existing = [self._eu("cpu", 100), self._eu("mem", 50)]
        assert self.fn({}, existing, ["cpu", "mem"]) is True

    def test_omitted_type_is_ignored(self) -> None:
        # mem exists on Waldur but was dropped from the payload (anomaly).
        # cpu still matches, so there is nothing left to submit.
        existing = [self._eu("cpu", 100), self._eu("mem", 50)]
        assert self.fn({"cpu": 100}, existing, ["cpu", "mem"]) is True

    def test_remaining_amount_differs(self) -> None:
        existing = [self._eu("cpu", 100), self._eu("mem", 50)]
        assert self.fn({"cpu": 4000}, existing, ["cpu", "mem"]) is False

    def test_new_remaining_component_does_not_match(self) -> None:
        existing = [self._eu("mem", 50)]
        assert self.fn({"cpu": 100}, existing, ["cpu", "mem"]) is False


class TestSubmitTotalUsageIdempotencyIntegration(unittest.TestCase):
    """End-to-end: confirm set_usage is NOT POSTed when the reported usage
    matches what Waldur already has for the billing period."""

    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        self.resource_uuid = "10a0f810be1c43bbb651e8cbdbb90198"
        self.waldur_resource = {
            "uuid": self.resource_uuid,
            "name": "alloc-01",
            "backend_id": "alloc-01",
            "state": "OK",
        }
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
            ],
            "customer_uuid": uuid.uuid4().hex,
        }
        self.client = AuthenticatedClient(
            base_url=self.BASE_URL, token=OFFERING.api_token, headers={}
        )
        self.backend = mock.MagicMock(spec=BaseBackend)
        self.backend.backend_type = "slurm"
        self.backend.supports_decreasing_usage = False
        self.backend.backend_components = {
            "cpu": {"limit": 10, "measured_unit": "h", "unit_factor": 1,
                    "accounting_type": "usage", "label": "CPU"},
        }
        self.backend.timezone = ""

    def _stub_common(self) -> None:
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200, json=user_me_api_response(base_url=self.BASE_URL, username="test-user")
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/"
        ).respond(200, json=self.waldur_offering)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/"
        ).respond(200, json=[self.waldur_resource])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.resource_uuid}/"
        ).respond(200, json=self.waldur_resource)
        sp = ServiceProvider(uuid=uuid.uuid4())
        respx.get(f"{self.BASE_URL}/api/marketplace-service-providers/").respond(
            200, json=[sp.to_dict()]
        )

    def _build_processor(self) -> OfferingReportProcessor:
        # Bypass __init__ — _submit_total_usage_for_resource only uses these
        # attributes, so leaving the rest unset keeps the test focused.
        proc = OfferingReportProcessor.__new__(OfferingReportProcessor)
        proc.resource_backend = self.backend
        proc.timezone = ""  # type: ignore[attr-defined]
        proc.waldur_rest_client = self.client  # type: ignore[attr-defined]
        proc.offering = OFFERING.model_copy(
            update={"omit_anomalous_usage_components": False}
        )
        return proc

    @respx.mock
    def test_skips_set_usage_when_usage_unchanged(self) -> None:
        self._stub_common()
        # Existing usage in Waldur: cpu=100 for the current billing period
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[{
            "uuid": uuid.uuid4().hex,
            "type": "cpu",
            "usage": "100.0000",
        }])
        # Spy on set_usage — should not be called
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        # build a minimal waldur_resource Pydantic-ish object
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"

        # Components mirrors what waldur_offering exposes
        offering_components = [SimpleNamespace(type_="cpu")]

        proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 100.0},
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert set_usage_route.call_count == 0, (
            f"set_usage was called {set_usage_route.call_count} times; "
            "expected 0 because usage was unchanged"
        )

    @respx.mock
    def test_submits_set_usage_when_amount_differs(self) -> None:
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[{
            "uuid": uuid.uuid4().hex,
            "type": "cpu",
            "usage": "100.0000",
        }])
        # When supports_decreasing_usage=False, the anomaly path also lists user
        # usages. Stub it as empty.
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-user-usages/"
        ).respond(200, json=[])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"
        offering_components = [SimpleNamespace(type_="cpu")]

        proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 150.0},   # ← differs from existing 100
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert set_usage_route.call_count == 1, (
            f"set_usage was called {set_usage_route.call_count} times; expected 1"
        )

    @respx.mock
    def test_skips_set_usage_when_only_sub_cent_precision_differs(self) -> None:
        # The backend reports full-precision usage (100.004) but Waldur stores
        # amounts rounded to 2 decimals (100.0000). Because submission rounds to
        # 2 decimals, the reported value must also be rounded before the
        # idempotency comparison — otherwise 100.004 vs 100.00 would look
        # different and we'd resubmit (and fan out the marketplace signals) on
        # every cycle even though nothing effectively changed.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[{
            "uuid": uuid.uuid4().hex,
            "type": "cpu",
            "usage": "100.0000",
        }])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"
        offering_components = [SimpleNamespace(type_="cpu")]

        proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 100.004},  # rounds to 100.00 → matches existing
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert set_usage_route.call_count == 0, (
            f"set_usage was called {set_usage_route.call_count} times; expected 0 "
            "because the reported value rounds to the stored amount"
        )

    @respx.mock
    def test_submitted_amount_is_rounded_to_two_decimals(self) -> None:
        # Waldur rejects amounts with more than 2 decimal places, so the wire
        # payload must be rounded regardless of the backend's precision.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[{
            "uuid": uuid.uuid4().hex,
            "type": "cpu",
            "usage": "100.0000",
        }])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-user-usages/"
        ).respond(200, json=[])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"
        offering_components = [SimpleNamespace(type_="cpu")]

        proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 100.126},  # would be rejected as-is
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert set_usage_route.call_count == 1
        body = json.loads(set_usage_route.calls.last.request.content)
        amounts = {u["type"]: u["amount"] for u in body["usages"]}
        assert amounts["cpu"] == "100.13", (
            f"expected amount rounded to 2 decimals, got {amounts['cpu']!r}"
        )

    @respx.mock
    def test_decimal_amount_from_backend_still_submits(self) -> None:
        # A backend computing in Decimal (to keep partial units off the floor)
        # used to abort the whole report here: the diff line mixed the reported
        # Decimal with the float parsed from the API, and Decimal - float raises.
        # The submission was skipped and the failure surfaced only as a warning,
        # so usage silently stopped flowing. Nothing about a debug log should be
        # able to do that.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[{
            "uuid": uuid.uuid4().hex,
            "type": "cpu",
            "usage": "100.0000",
        }])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-user-usages/"
        ).respond(200, json=[])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"
        offering_components = [SimpleNamespace(type_="cpu")]

        proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            # Deliberately off-annotation: the signature says float, and the
            # point of this test is what happens when a backend hands over a
            # Decimal regardless. mypy checks the plugins but nothing enforces
            # the report's value type at runtime.
            total_usage={"cpu": decimal.Decimal("150.50")},  # type: ignore[dict-item]
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert set_usage_route.call_count == 1
        body = json.loads(set_usage_route.calls.last.request.content)
        amounts = {u["type"]: u["amount"] for u in body["usages"]}
        assert amounts["cpu"] == "150.50"


class TestSubmitTotalUsagePartialAnomaly(unittest.TestCase):
    """Anomaly detection omits decreasing components but still submits the rest."""

    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        self.resource_uuid = "10a0f810be1c43bbb651e8cbdbb90198"
        self.waldur_resource = {
            "uuid": self.resource_uuid,
            "name": "alloc-01",
            "backend_id": "alloc-01",
            "state": "OK",
        }
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ],
            "customer_uuid": uuid.uuid4().hex,
        }
        self.client = AuthenticatedClient(
            base_url=self.BASE_URL, token=OFFERING.api_token, headers={}
        )
        self.backend = mock.MagicMock(spec=BaseBackend)
        self.backend.backend_type = "slurm"
        self.backend.supports_decreasing_usage = False
        self.backend.backend_components = {
            "cpu": {"limit": 10, "measured_unit": "h", "unit_factor": 1,
                    "accounting_type": "usage", "label": "CPU"},
            "mem": {"limit": 10, "measured_unit": "GBh", "unit_factor": 1,
                    "accounting_type": "usage", "label": "MEM"},
        }
        self.backend.timezone = ""

    def _stub_common(self) -> None:
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200, json=user_me_api_response(base_url=self.BASE_URL, username="test-user")
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/"
        ).respond(200, json=self.waldur_offering)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/"
        ).respond(200, json=[self.waldur_resource])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.resource_uuid}/"
        ).respond(200, json=self.waldur_resource)
        sp = ServiceProvider(uuid=uuid.uuid4())
        respx.get(f"{self.BASE_URL}/api/marketplace-service-providers/").respond(
            200, json=[sp.to_dict()]
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-user-usages/"
        ).respond(200, json=[])

    def _build_processor(self, omit_anomalous: bool = False) -> OfferingReportProcessor:
        proc = OfferingReportProcessor.__new__(OfferingReportProcessor)
        proc.resource_backend = self.backend
        proc.timezone = ""  # type: ignore[attr-defined]
        proc.waldur_rest_client = self.client  # type: ignore[attr-defined]
        proc.offering = OFFERING.model_copy(
            update={"omit_anomalous_usage_components": omit_anomalous}
        )
        return proc

    def _resource_and_components(self):
        waldur_resource = mock.MagicMock()
        waldur_resource.uuid.hex = self.resource_uuid
        waldur_resource.backend_id = "alloc-01"
        offering_components = [SimpleNamespace(type_="cpu"), SimpleNamespace(type_="mem")]
        return waldur_resource, offering_components

    @respx.mock
    def test_submits_only_non_anomalous_component(self) -> None:
        # cpu decreased (anomaly), mem increased — POST mem only.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[
            {"uuid": uuid.uuid4().hex, "type": "cpu", "usage": "15.0000"},
            {"uuid": uuid.uuid4().hex, "type": "mem", "usage": "20.0000"},
        ])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor(omit_anomalous=True)
        waldur_resource, offering_components = self._resource_and_components()

        skipped = proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 10.0, "mem": 30.0},
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert skipped == ["cpu"]
        assert set_usage_route.call_count == 1
        body = json.loads(set_usage_route.calls.last.request.content)
        amounts = {u["type"]: u["amount"] for u in body["usages"]}
        assert amounts == {"mem": "30.00"}

    @respx.mock
    def test_raises_when_every_component_is_anomalous(self) -> None:
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[
            {"uuid": uuid.uuid4().hex, "type": "cpu", "usage": "15.0000"},
            {"uuid": uuid.uuid4().hex, "type": "mem", "usage": "40.0000"},
        ])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource, offering_components = self._resource_and_components()

        with self.assertRaises(UsageAnomalyError):
            proc._submit_total_usage_for_resource(
                waldur_resource=waldur_resource,
                total_usage={"cpu": 10.0, "mem": 30.0},
                waldur_components=offering_components,
                report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
            )

        assert set_usage_route.call_count == 0

    @respx.mock
    def test_skips_set_usage_when_remaining_already_matches(self) -> None:
        # mem decreased (anomaly); cpu is unchanged. After dropping mem
        # there is nothing new to send.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[
            {"uuid": uuid.uuid4().hex, "type": "cpu", "usage": "214.77"},
            {"uuid": uuid.uuid4().hex, "type": "mem", "usage": "3546.83"},
        ])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor(omit_anomalous=True)
        waldur_resource, offering_components = self._resource_and_components()

        skipped = proc._submit_total_usage_for_resource(
            waldur_resource=waldur_resource,
            total_usage={"cpu": 214.77, "mem": 3494.0},
            waldur_components=offering_components,
            report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        assert skipped == ["mem"]
        assert set_usage_route.call_count == 0

    @respx.mock
    def test_default_blocks_whole_payload_on_one_anomaly(self) -> None:
        # cpu decreased, mem increased — default drop-all skips the POST.
        self._stub_common()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[
            {"uuid": uuid.uuid4().hex, "type": "cpu", "usage": "15.0000"},
            {"uuid": uuid.uuid4().hex, "type": "mem", "usage": "20.0000"},
        ])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        proc = self._build_processor()
        waldur_resource, offering_components = self._resource_and_components()

        with self.assertRaises(UsageAnomalyError):
            proc._submit_total_usage_for_resource(
                waldur_resource=waldur_resource,
                total_usage={"cpu": 10.0, "mem": 30.0},
                waldur_components=offering_components,
                report_date=datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc),
            )

        assert set_usage_route.call_count == 0

    @respx.mock
    def test_omitted_components_are_not_submitted_as_user_usages(self) -> None:
        # cpu decreased, mem increased. set_usage posts mem only; per-user
        # must not write cpu either, or the next cycle would skip anomaly
        # detection for that type.
        self._stub_common()
        cpu_uuid = uuid.uuid4()
        mem_uuid = uuid.uuid4()
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
        ).respond(200, json=[
            {"uuid": str(cpu_uuid), "type": "cpu", "usage": "15.0000"},
            {"uuid": str(mem_uuid), "type": "mem", "usage": "20.0000"},
        ])
        set_usage_route = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})
        user_usage_cpu = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
            f"{cpu_uuid}/set_user_usages/"
        ).respond(201, json={})
        user_usage_mem = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/"
            f"{mem_uuid}/set_user_usages/"
        ).respond(201, json={})

        proc = self._build_processor(omit_anomalous=True)
        proc._offering_users_cache = []
        waldur_resource, offering_components = self._resource_and_components()
        waldur_offering = SimpleNamespace(components=offering_components)
        backend_resource_info = BackendResourceInfo(
            backend_id="alloc-01",
            users=["user-01"],
            usage={
                "user-01": {"cpu": 10.0, "mem": 30.0},
                "TOTAL_ACCOUNT_USAGE": {"cpu": 10.0, "mem": 30.0},
            },
        )

        proc._process_resource_period(
            waldur_resource_info=waldur_resource,
            waldur_offering=waldur_offering,
            resource_backend_id="alloc-01",
            backend_resource_info=backend_resource_info,
            year=2024,
            month=6,
            is_current=True,
        )

        assert set_usage_route.call_count == 1
        amounts = {
            u["type"]: u["amount"]
            for u in json.loads(set_usage_route.calls.last.request.content)["usages"]
        }
        assert amounts == {"mem": "30.00"}
        assert user_usage_mem.call_count == 1
        assert user_usage_cpu.call_count == 0
