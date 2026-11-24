"""Metering from RGW's own Admin Ops (rgw_usage), without croit.

The shapes are the ones Ceph 20.2.5 answered: bucket statistics through the root
user list every bucket of an account, and the usage log files an account's
traffic under the account id.
"""

import datetime
import json
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient
from waldur_site_agent_ceph_s3.exceptions import CephS3AuthenticationError
from waldur_site_agent_ceph_s3 import rgw_reporting
from waldur_site_agent_ceph_s3.rgw_reporting import RgwUsageBackend

from conftest import RADOSGW_SETTINGS

GB = 1000000000
UTC = datetime.timezone.utc
STORAGE = {"accounting_type": "usage", "backend_name": "storage", "unit_factor": GB}
COMPONENTS = {
    "s3_storage": STORAGE,
    "s3_egress": {"accounting_type": "usage", "backend_name": "bytes_sent", "unit_factor": GB},
    "s3_ingress": {"accounting_type": "usage", "backend_name": "bytes_received", "unit_factor": GB},
    "s3_requests": {"accounting_type": "usage", "backend_name": "ops", "unit_factor": 1000},
}
ACCOUNTS = {**RADOSGW_SETTINGS, "rgw_accounts": True}
ROOT = "tenant1$res-root"
ACCOUNT = "RGW33937345823507741"


@pytest.fixture(autouse=True)
def _fresh_cluster_caches():
    """Each test starts as a freshly started agent."""
    rgw_reporting._CLUSTER_CACHES.clear()
    yield
    rgw_reporting._CLUSTER_CACHES.clear()


class FakeLog:
    """The usage log of one owner: totals per UTC hour, summed over [start, end)."""

    def __init__(self, hours):
        self.hours = dict(hours)
        self.windows = []

    def __call__(self, owner, start, end):
        self.windows.append((start.astimezone(UTC), end.astimezone(UTC)))
        totals = {"bytes_sent": 0, "bytes_received": 0, "ops": 0, "successful_ops": 0}
        for hour, (sent, received, ops) in self.hours.items():
            if start <= hour < end:
                totals["bytes_sent"] += sent
                totals["bytes_received"] += received
                totals["ops"] += ops
                totals["successful_ops"] += ops
        return totals


def make_backend(settings=RADOSGW_SETTINGS, components=COMPONENTS):
    with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"):
        backend = RgwUsageBackend(json.loads(json.dumps(settings)), components)
    backend.client = Mock()
    backend.client.get_user_buckets.return_value = [
        {"name": "bkt-one", "size_bytes": 4005888, "logical_bytes": 4000000},
        {"name": "bkt-iam", "size_bytes": 5001216, "logical_bytes": 5000000},
    ]
    backend.client.get_usage.return_value = {
        "bytes_sent": 3001249,
        "bytes_received": 11000000,
        "ops": 13,
        "successful_ops": 13,
    }
    backend.client.get_user_info.return_value = {"account_id": ACCOUNT}
    return backend


class TestConfiguration:
    def test_an_offering_still_on_croit_management_is_refused(self):
        with pytest.raises(ValueError, match="no longer manages users through croit"):
            make_backend({**RADOSGW_SETTINGS, "flavour": "croit"})

    def test_an_unknown_size_figure_is_refused(self):
        with pytest.raises(ValueError, match="storage_size"):
            make_backend({**RADOSGW_SETTINGS, "storage_size": "raw"})

    def test_an_unknown_accrual_unit_is_refused(self):
        with pytest.raises(ValueError, match="accrual_unit"):
            make_backend(components={"s3_storage": {**STORAGE, "accrual_unit": "week"}})

    def test_nothing_to_meter_is_refused(self):
        with pytest.raises(ValueError, match="nothing to meter"):
            make_backend(components={"s3_objects": {"backend_name": "objects", "unit_factor": 1}})

    def test_a_traffic_component_needs_a_positive_factor(self):
        with pytest.raises(ValueError, match="positive 'unit_factor'"):
            make_backend(components={"s3_egress": {"backend_name": "bytes_sent", "unit_factor": 0}})

    def test_storage_is_accrued_per_day_or_hour(self):
        assert make_backend().accrued_usage_components() == {"s3_storage": 86400}
        hourly = make_backend(components={"s3_storage": {**STORAGE, "accrual_unit": "hour"}})
        assert hourly.accrued_usage_components() == {"s3_storage": 3600}


class TestCurrentReport:
    def test_storage_level_and_month_to_date_traffic(self):
        backend = make_backend()
        # A fresh agent: the whole period so far, read in a settled part and a tail
        # that together hold the one logged hour.
        log = FakeLog({datetime.datetime(2026, 11, 3, 10, tzinfo=UTC): (3001249, 11000000, 13)})
        backend.client.get_usage.side_effect = log
        backend._now = lambda: datetime.datetime(2026, 11, 21, 23, 41, tzinfo=UTC)
        report = backend._get_usage_report(["res-01"])
        assert report == {
            "res-01": {
                "TOTAL_ACCOUNT_USAGE": {
                    # A level, unrounded: the processor accrues it.
                    "s3_storage": 0.009,
                    "s3_egress": 0.0,
                    "s3_ingress": 0.01,
                    "s3_requests": 0.01,
                }
            }
        }
        assert log.windows == [
            (datetime.datetime(2026, 11, 1, tzinfo=UTC), datetime.datetime(2026, 11, 21, 22, tzinfo=UTC)),
            (datetime.datetime(2026, 11, 21, 22, tzinfo=UTC), datetime.datetime(2026, 11, 21, 23, 41, tzinfo=UTC)),
        ]

    def test_allocated_bytes_when_configured(self):
        backend = make_backend({**RADOSGW_SETTINGS, "storage_size": "allocated"})
        usage = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["s3_storage"] == pytest.approx(0.009007104)

    def test_an_account_is_metered_under_its_id(self):
        backend = make_backend(ACCOUNTS)
        backend._get_usage_report([ROOT])
        backend.client.get_user_buckets.assert_called_once_with(ROOT)
        assert backend.client.get_usage.call_args.args[0] == ACCOUNT

    def test_a_plain_user_is_metered_under_its_uid(self):
        backend = make_backend()
        backend._get_usage_report(["tenant1$alice"])
        assert backend.client.get_usage.call_args.args[0] == "tenant1$alice"

    def test_a_tenant_backend_id_is_metered_as_its_root_and_reported_as_itself(self):
        backend = make_backend()
        report = backend._get_usage_report(["acme_12345678"])
        backend.client.get_user_buckets.assert_called_once_with("acme_12345678$root")
        assert backend.client.get_usage.call_args.args[0] == "acme_12345678$root"
        assert list(report) == ["acme_12345678"]

    def test_a_failed_part_is_left_out_not_zeroed(self):
        backend = make_backend()
        backend.client.get_usage.side_effect = CephS3AuthenticationError("AccessDenied")
        usage = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert usage == {"s3_storage": 0.009}

    def test_an_unread_resource_is_not_pulled_with_zeros(self):
        backend = make_backend()
        backend.client.get_user_buckets.side_effect = RuntimeError("gateway down")
        backend.client.get_usage.side_effect = RuntimeError("gateway down")
        assert backend._get_usage_report(["res-01"]) == {}
        info = Mock(usage={"TOTAL_ACCOUNT_USAGE": {"s3_storage": 0}})
        assert backend._apply_usage_policy("res-01", info) is None

    def test_a_measured_resource_keeps_its_usage(self):
        backend = make_backend()
        report = backend._get_usage_report(["res-01"])
        info = Mock(usage=report["res-01"])
        assert backend._apply_usage_policy("res-01", info) is info


class TestPastPeriods:
    def test_a_past_month_is_its_traffic_from_the_log(self):
        backend = make_backend()
        report = backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert set(report["res-01"]["TOTAL_ACCOUNT_USAGE"]) == {"s3_egress", "s3_ingress", "s3_requests"}
        _, start, end = backend.client.get_usage.call_args.args
        assert start == datetime.datetime(2026, 10, 1, tzinfo=UTC)
        assert end == datetime.datetime(2026, 11, 1, tzinfo=UTC)

    def test_the_month_follows_the_reporting_timezone(self):
        backend = make_backend()
        backend.timezone = "Europe/Tallinn"
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        _, start, end = backend.client.get_usage.call_args.args
        assert start == datetime.datetime(2026, 9, 30, 21, tzinfo=UTC)
        assert end == datetime.datetime(2026, 10, 31, 22, tzinfo=UTC)

    def test_a_month_before_the_resource_existed_is_not_reported(self):
        backend = make_backend()
        resource = Mock(created=datetime.datetime(2026, 11, 22, 0, 7, tzinfo=UTC))
        assert backend.get_usage_report_for_period(["res-01"], 2026, 10, waldur_resource=resource) == {}
        backend.client.get_usage.assert_not_called()
        report = backend.get_usage_report_for_period(["res-01"], 2026, 11, waldur_resource=resource)
        assert "res-01" in report

    def test_storage_only_has_no_history(self):
        backend = make_backend(components={"s3_storage": STORAGE})
        assert backend.get_usage_report_for_period(["res-01"], 2026, 10) == {}
        backend.historical_load = True
        with pytest.raises(BackendError, match="cannot be recomputed"):
            backend.get_usage_report_for_period(["res-01"], 2026, 10)

    def test_an_unreadable_log_fails_the_historical_loader(self):
        backend = make_backend()
        backend.client.get_usage.side_effect = RuntimeError("down")
        assert backend.get_usage_report_for_period(["res-01"], 2026, 10) == {}
        backend.historical_load = True
        with pytest.raises(BackendError, match="refusing to report"):
            backend.get_usage_report_for_period(["res-01"], 2026, 10)


def make_client(handler):
    client = RadosGWClient(
        endpoint="https://rgw.example.org",
        access_key="ADMINACCESSKEY000000",
        secret_key="adminsecret0123456789abcdefghijklmnopqrs",
    )
    client.session = httpx.Client(transport=httpx.MockTransport(handler))
    return client


class TestUsageLogClient:
    def test_reads_the_owners_summary_for_the_window(self):
        seen = {}

        def handler(request):
            seen.update(parse_qs(urlparse(str(request.url)).query))
            return httpx.Response(200, json={
                "entries": [],
                "summary": [{
                    "user": ACCOUNT,
                    "total": {"bytes_sent": 3001249, "bytes_received": 11000000,
                              "ops": 13, "successful_ops": 12},
                }],
            })

        totals = make_client(handler).get_usage(
            ACCOUNT,
            datetime.datetime(2026, 11, 1, tzinfo=UTC),
            datetime.datetime(2026, 11, 21, 23, 41, 5, tzinfo=UTC),
        )
        assert totals == {"bytes_sent": 3001249, "bytes_received": 11000000,
                          "ops": 13, "successful_ops": 12}
        assert seen["uid"] == [ACCOUNT]
        assert seen["start"] == ["2026-11-01 00:00:00"]
        assert seen["end"] == ["2026-11-21 23:41:05"]

    def test_nothing_logged_is_zero(self):
        totals = make_client(lambda request: httpx.Response(200, json={"entries": [], "summary": []})).get_usage(
            "tenant1$alice", datetime.datetime(2026, 11, 1, tzinfo=UTC), datetime.datetime(2026, 11, 2, tzinfo=UTC)
        )
        assert totals == {"bytes_sent": 0, "bytes_received": 0, "ops": 0, "successful_ops": 0}

    def test_a_missing_cap_names_it(self):
        def handler(request):
            return httpx.Response(403, json={"Code": "AccessDenied"})

        with pytest.raises(CephS3AuthenticationError, match="usage=read"):
            make_client(handler).get_usage(
                ACCOUNT, datetime.datetime(2026, 11, 1, tzinfo=UTC), datetime.datetime(2026, 11, 2, tzinfo=UTC)
            )

    def test_bucket_listing_carries_both_sizes(self):
        def handler(request):
            return httpx.Response(200, json=[{
                "bucket": "bkt-one",
                "usage": {"rgw.main": {"size": 4000000, "size_actual": 4005888, "num_objects": 2}},
            }])

        assert make_client(handler).get_user_buckets(ROOT) == [{
            "name": "bkt-one", "size_bytes": 4005888, "logical_bytes": 4000000, "num_objects": 2,
        }]


class TestUnwrittenUsageLog:
    ZERO = {"bytes_sent": 0, "bytes_received": 0, "ops": 0, "successful_ops": 0}
    NOW = datetime.datetime(2026, 11, 21, 23, tzinfo=UTC)

    def _zero_backend(self, log_written):
        backend = make_backend()
        backend.client.get_usage.return_value = dict(self.ZERO)
        backend.client.usage_log_has_entries.return_value = log_written
        backend._now = lambda: self.NOW
        return backend

    def test_zeros_are_reported_and_an_empty_log_is_warned_about(self, caplog):
        backend = self._zero_backend(False)
        usage = backend._get_usage_report(["res-01", "res-02"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["s3_egress"] == 0.0 and usage["s3_requests"] == 0.0
        assert "rgw_enable_usage_log" in caplog.text
        start, end = backend.client.usage_log_has_entries.call_args.args
        assert end - start == datetime.timedelta(days=1)

    def test_zeros_on_a_written_log_raise_no_warning(self, caplog):
        backend = self._zero_backend(True)
        backend._get_usage_report(["res-01"])
        assert "rgw_enable_usage_log" not in caplog.text

    def test_growing_traffic_counts_as_a_written_log(self):
        backend = make_backend()
        backend.client.usage_log_has_entries.return_value = True
        log = FakeLog({self.NOW - datetime.timedelta(hours=5): (100, 100, 1)})
        backend.client.get_usage.side_effect = log
        backend._now = lambda: self.NOW
        backend._get_usage_report(["res-01"])  # first pass: nothing to compare with
        assert backend.client.usage_log_has_entries.call_count == 1
        log.hours[self.NOW + datetime.timedelta(minutes=10)] = (5, 5, 1)
        backend._now = lambda: self.NOW + datetime.timedelta(days=2)
        backend._get_usage_report(["res-01"])
        assert backend.client.usage_log_has_entries.call_count == 1  # grew: seen

    def test_traffic_from_before_the_log_stopped_does_not_hide_it(self, caplog):
        backend = make_backend()
        log = FakeLog({self.NOW - datetime.timedelta(days=5): (100, 100, 1)})
        backend.client.get_usage.side_effect = log
        backend.client.usage_log_has_entries.return_value = False
        backend._now = lambda: self.NOW
        backend._get_usage_report(["res-01"])
        caplog.clear()
        backend._now = lambda: self.NOW + datetime.timedelta(days=2)
        backend._get_usage_report(["res-01"])
        assert "rgw_enable_usage_log" in caplog.text

    def test_the_check_runs_at_most_hourly_and_not_after_the_log_is_seen(self):
        backend = self._zero_backend(False)
        backend._get_usage_report(["res-01"])
        backend._now = lambda: self.NOW + datetime.timedelta(minutes=30)
        backend._get_usage_report(["res-01"])
        assert backend.client.usage_log_has_entries.call_count == 1

        backend.client.usage_log_has_entries.return_value = True
        backend._now = lambda: self.NOW + datetime.timedelta(hours=2)
        backend._get_usage_report(["res-01"])
        backend._now = lambda: self.NOW + datetime.timedelta(hours=4)
        backend._get_usage_report(["res-01"])
        assert backend.client.usage_log_has_entries.call_count == 2

    def test_a_past_month_the_log_holds_nothing_for_is_warned_about(self, caplog):
        backend = self._zero_backend(False)
        report = backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert report["res-01"]["TOTAL_ACCOUNT_USAGE"]["s3_requests"] == 0.0
        assert "2026-10" in caplog.text and "rgw_enable_usage_log" in caplog.text
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert backend.client.usage_log_has_entries.call_count == 1  # once per month

    def test_the_loader_refuses_a_month_the_log_holds_nothing_for(self):
        backend = self._zero_backend(False)
        backend.historical_load = True
        with pytest.raises(BackendError, match="Refusing to report that month as zero"):
            backend.get_usage_report_for_period(["res-01"], 2026, 10)

    def test_the_loader_refuses_an_old_unlogged_month_too(self):
        # Older than the cache keeps: the answer must still be used, not pruned.
        backend = self._zero_backend(False)
        backend.historical_load = True
        with pytest.raises(BackendError, match="Refusing to report that month as zero"):
            backend.get_usage_report_for_period(["res-01"], 2024, 5)

    def test_a_past_month_of_real_zeros_is_reported(self):
        backend = self._zero_backend(True)
        backend.historical_load = True
        report = backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert report["res-01"]["TOTAL_ACCOUNT_USAGE"]["s3_egress"] == 0.0

    def test_the_probe_asks_without_an_owner(self):
        seen = {}

        def handler(request):
            seen.update(parse_qs(urlparse(str(request.url)).query))
            return httpx.Response(200, json={"entries": [], "summary": []})

        client = make_client(handler)
        assert client.usage_log_has_entries(
            datetime.datetime(2026, 11, 1, tzinfo=UTC), datetime.datetime(2026, 11, 2, tzinfo=UTC)
        ) is False
        assert "uid" not in seen and seen["start"] == ["2026-11-01 00:00:00"]


def test_an_accounts_id_is_looked_up_once():
    backend = make_backend(ACCOUNTS)
    backend._get_usage_report([ROOT])
    backend._get_usage_report([ROOT])
    backend.get_usage_report_for_period([ROOT], 2026, 10)
    assert backend.client.get_user_info.call_count == 1


class TestIncrementalReads:
    def _at(self, day, hour, minute=0):
        return datetime.datetime(2026, 11, day, hour, minute, tzinfo=UTC)

    def test_later_passes_read_only_what_settled_since_plus_the_tail(self):
        backend = make_backend()
        log = FakeLog({self._at(d, 10): (1000000000, 0, 10) for d in range(1, 22)})
        backend.client.get_usage.side_effect = log

        backend._now = lambda: self._at(21, 23, 10)
        first = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert first["s3_egress"] == 21.0 and first["s3_requests"] == 0.21

        # Same hour: only the tail is read again.
        log.windows.clear()
        backend._now = lambda: self._at(21, 23, 40)
        backend._get_usage_report(["res-01"])
        assert log.windows == [(self._at(21, 22), self._at(21, 23, 40))]

        # Next day: the newly settled hours, then the tail.
        log.hours[self._at(22, 10)] = (500000000, 0, 5)
        log.windows.clear()
        backend._now = lambda: self._at(22, 12, 5)
        later = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert log.windows == [
            (self._at(21, 22), self._at(22, 11)),
            (self._at(22, 11), self._at(22, 12, 5)),
        ]
        assert later["s3_egress"] == 21.5 and later["s3_requests"] == 0.22

    def test_a_failed_read_keeps_the_previous_settled_totals(self):
        backend = make_backend()
        log = FakeLog({self._at(5, 10): (1000000000, 0, 10)})
        backend.client.get_usage.side_effect = log
        backend._now = lambda: self._at(21, 23, 10)
        backend._get_usage_report(["res-01"])
        kept = backend._cluster.settled["res-01"]

        backend.client.get_usage.side_effect = RuntimeError("gateway down")
        backend._now = lambda: self._at(22, 12)
        usage = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert set(usage) == {"s3_storage"}
        assert backend._cluster.settled["res-01"] == kept

    def test_a_new_period_starts_from_nothing(self):
        backend = make_backend()
        log = FakeLog({self._at(5, 10): (1000000000, 0, 10)})
        backend.client.get_usage.side_effect = log
        backend._now = lambda: self._at(30, 23)
        backend._get_usage_report(["res-01"])
        log.windows.clear()
        backend._now = lambda: datetime.datetime(2026, 12, 1, 0, 20, tzinfo=UTC)
        usage = backend._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert log.windows == [(datetime.datetime(2026, 12, 1, tzinfo=UTC), datetime.datetime(2026, 12, 1, 0, 20, tzinfo=UTC))]
        assert usage["s3_egress"] == 0.0

    def test_a_settled_finished_month_is_read_once(self):
        backend = make_backend()
        backend._now = lambda: datetime.datetime(2026, 11, 1, 0, 30, tzinfo=UTC)
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert backend.client.get_usage.call_count == 2  # not settled yet: read again

        backend._now = lambda: datetime.datetime(2026, 11, 1, 1, 30, tzinfo=UTC)
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        backend.get_usage_report_for_period(["res-01"], 2026, 10)
        assert backend.client.get_usage.call_count == 3

    def test_a_pass_reads_the_clock_once(self):
        backend = make_backend()
        instants = iter([
            datetime.datetime(2026, 11, 30, 23, 59, 59, tzinfo=UTC),
            datetime.datetime(2026, 12, 1, 0, 0, 1, tzinfo=UTC),
        ])
        backend._now = lambda: next(instants)
        log = FakeLog({})
        backend.client.get_usage.side_effect = log
        backend._get_usage_report(["res-01"])
        assert log.windows[0][0] == datetime.datetime(2026, 11, 1, tzinfo=UTC)
        assert log.windows[-1][1] == datetime.datetime(2026, 11, 30, 23, 59, 59, tzinfo=UTC)


class TestCachesOutliveAPass:
    """The report loop builds a new backend for every pass."""

    def _at(self, day, hour, minute=0):
        return datetime.datetime(2026, 11, day, hour, minute, tzinfo=UTC)

    def test_a_new_backend_for_the_same_cluster_reads_only_the_tail(self):
        log = FakeLog({self._at(5, 10): (1000000000, 0, 10)})
        first = make_backend()
        first.client.get_usage.side_effect = log
        first._now = lambda: self._at(21, 23, 10)
        first._get_usage_report(["res-01"])

        log.windows.clear()
        second = make_backend()
        second.client.get_usage.side_effect = log
        second._now = lambda: self._at(21, 23, 40)
        usage = second._get_usage_report(["res-01"])["res-01"]["TOTAL_ACCOUNT_USAGE"]
        assert log.windows == [(self._at(21, 22), self._at(21, 23, 40))]
        assert usage["s3_egress"] == 1.0

    def test_account_ids_and_finished_months_carry_over(self):
        first = make_backend(ACCOUNTS)
        first._now = lambda: self._at(21, 23)
        first._get_usage_report([ROOT])
        first.get_usage_report_for_period([ROOT], 2026, 10)

        second = make_backend(ACCOUNTS)
        second._now = lambda: self._at(21, 23, 30)
        second._get_usage_report([ROOT])
        second.get_usage_report_for_period([ROOT], 2026, 10)
        second.client.get_user_info.assert_not_called()
        assert all(
            call.args[1] != datetime.datetime(2026, 10, 1, tzinfo=UTC)
            for call in second.client.get_usage.call_args_list
        )

    def test_another_cluster_has_its_own(self):
        make_backend()._cluster.account_ids["u"] = "RGW00000000000000001"
        other = make_backend({**RADOSGW_SETTINGS, "s3_endpoint": "https://other.example.org"})
        assert other._cluster.account_ids == {}

    def test_owners_from_an_earlier_period_are_dropped(self):
        backend = make_backend()
        backend._cluster.settled["gone"] = (datetime.datetime(2026, 10, 1, tzinfo=UTC), self._at(1, 0), {})
        backend._now = lambda: self._at(21, 23)
        backend._get_usage_report(["res-01"])
        assert "gone" not in backend._cluster.settled and "res-01" in backend._cluster.settled


class TestReusedUid:
    """A uid freed by a deleted resource can come back for a new one."""

    def _resource(self, created):
        return Mock(backend_id=ROOT, created=created, spec=["backend_id", "created", "name"])

    def test_traffic_is_read_from_the_new_resources_creation(self):
        backend = make_backend()
        log = FakeLog({})
        backend.client.get_usage.side_effect = log
        backend._created["res-01"] = datetime.datetime(2026, 11, 15, 9, 40, tzinfo=UTC)
        backend._now = lambda: datetime.datetime(2026, 11, 21, 23, 10, tzinfo=UTC)
        backend._get_usage_report(["res-01"])
        assert log.windows[0][0] == datetime.datetime(2026, 11, 15, 9, tzinfo=UTC)

    def test_a_recreated_account_resource_gets_its_new_account(self):
        backend = make_backend(ACCOUNTS)
        backend._now = lambda: datetime.datetime(2026, 11, 21, 23, tzinfo=UTC)
        backend._created[ROOT] = datetime.datetime(2026, 11, 2, tzinfo=UTC)
        backend._get_usage_report([ROOT])
        backend.client.get_user_info.return_value = {"account_id": "RGW99999999999999999"}
        recreated = make_backend(ACCOUNTS)
        recreated.client = backend.client
        recreated._now = backend._now
        recreated._created[ROOT] = datetime.datetime(2026, 11, 20, tzinfo=UTC)
        recreated._get_usage_report([ROOT])
        assert backend.client.get_usage.call_args.args[0] == "RGW99999999999999999"

    def test_pull_resource_records_the_creation(self):
        backend = make_backend()
        created = datetime.datetime(2026, 11, 3, tzinfo=UTC)
        with patch.object(BaseBackend, "pull_resource", return_value=None):
            backend.pull_resource(self._resource(created))
        assert backend._created[ROOT] == created

    def test_finished_months_are_kept_per_window(self):
        utc = make_backend()
        utc._now = lambda: datetime.datetime(2026, 11, 21, tzinfo=UTC)
        utc.get_usage_report_for_period(["res-01"], 2026, 10)
        tallinn = make_backend()
        tallinn.client = utc.client
        tallinn.timezone = "Europe/Tallinn"
        tallinn._now = utc._now
        tallinn.get_usage_report_for_period(["res-01"], 2026, 10)
        assert utc.client.get_usage.call_count == 2


def test_the_period_start_is_taken_from_the_given_instant():
    backend = make_backend()
    backend.timezone = "Europe/Tallinn"
    instant = datetime.datetime(2026, 11, 30, 22, 30, tzinfo=UTC)  # 00:30 on 1 Dec in Tallinn
    assert backend._period_start(instant) == datetime.datetime(2026, 11, 30, 22, tzinfo=UTC)
