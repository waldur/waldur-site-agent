"""Metering a radosgw offering out of a time series database.

RadosGW keeps no stored-bytes series, so somebody else collects one and this
backend reads it. That keeps the agent stateless, and it means the failure that
matters is a series with holes in it — which must report nothing rather than
report a zero over the period's accrued total.
"""

import sys
from unittest.mock import Mock, patch

import pytest

from waldur_site_agent_ceph_s3.promql import PromQLError
from waldur_site_agent_ceph_s3.reporting import _GbDayUsageBackend
from waldur_site_agent_ceph_s3.reporting_prometheus import PrometheusUsageBackend

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

GB = 1_000_000_000
DAY = 86400

SETTINGS = {
    "flavour": "radosgw",
    "s3_endpoint": "https://rgw.example.org",
    "admin_access_key": "ADMINACCESSKEY000000",
    "admin_secret_key": "s" * 40,
    "prometheus_url": "https://vm.example.org/",
}
COMPONENTS = {
    "s3_storage": {
        "accounting_type": "usage",
        "backend_name": "storage",
        "unit_factor": GB,
    }
}


def series(owner, *pairs):
    return {"metric": {"owner": owner}, "values": [list(p) for p in pairs]}


@pytest.fixture
def backend():
    return PrometheusUsageBackend(dict(SETTINGS), dict(COMPONENTS))


class TestWiring:
    def test_prometheus_usage_is_registered_as_its_own_backend(self):
        names = {ep.name for ep in entry_points(group="waldur_site_agent.backends")}
        assert "prometheus_usage" in names

    def test_it_shares_the_gb_day_machinery(self):
        """The integration and the billing period must not fork per source."""
        assert issubclass(PrometheusUsageBackend, _GbDayUsageBackend)

    def test_it_refuses_to_start_without_a_url(self):
        settings = {k: v for k, v in SETTINGS.items() if k != "prometheus_url"}
        with pytest.raises(ValueError, match="prometheus_url"):
            PrometheusUsageBackend(settings, dict(COMPONENTS))

    def test_it_sums_buckets_per_owner_in_the_query(self, backend):
        """One request answers for every resource, and bucket churn needs no code."""
        assert backend.usage_query == "sum by (owner) (ceph_rgw_user_stored_bytes)"

    def test_the_metric_and_label_are_configurable(self):
        """A community RadosGW exporter names these differently."""
        backend = PrometheusUsageBackend(
            dict(
                SETTINGS,
                prometheus_metric="radosgw_usage_bucket_bytes",
                prometheus_owner_label="user",
            ),
            dict(COMPONENTS),
        )
        assert backend.usage_query == "sum by (user) (radosgw_usage_bucket_bytes)"

    def test_a_trailing_slash_does_not_double_up(self, backend):
        assert backend.promql_client.url == "https://vm.example.org"


class TestMetering:
    def test_one_gb_held_for_one_day_is_one_gb_day(self, backend):
        reply = [series("user-1", (0, "1e9"), (DAY, "1e9"))]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            report = backend._collect_usage(["user-1"], 0, DAY)

        assert report["user-1"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == pytest.approx(
            1.0
        )

    def test_the_level_is_stepped_not_averaged(self, backend):
        """Half a day at 2 GB then half a day at 4 GB is 1 + 2 GB-days."""
        reply = [
            series("user-1", (0, "2e9"), (DAY // 2, "4e9"), (DAY, "4e9")),
        ]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            report = backend._collect_usage(["user-1"], 0, DAY)

        assert report["user-1"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == pytest.approx(
            3.0
        )

    def test_one_query_answers_for_every_resource(self, backend):
        """Per-resource queries would hammer the database once per tenant."""
        reply = [
            series("user-1", (0, "1e9"), (DAY, "1e9")),
            series("user-2", (0, "2e9"), (DAY, "2e9")),
        ]
        with patch.object(
            backend.promql_client, "query_range", return_value=reply
        ) as query:
            report = backend._collect_usage(["user-1", "user-2"], 0, DAY)

        assert query.call_count == 1
        assert report["user-2"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == pytest.approx(
            2.0
        )

    def test_a_new_window_is_not_served_from_the_last_one(self, backend):
        """The live period ends at 'now', so every pass is a different window."""
        reply = [series("user-1", (0, "1e9"), (DAY, "1e9"))]
        with patch.object(
            backend.promql_client, "query_range", return_value=reply
        ) as query:
            backend._collect_usage(["user-1"], 0, DAY)
            backend._collect_usage(["user-1"], 0, DAY + 1800)

        assert query.call_count == 2

    def test_an_owner_absent_from_the_reply_is_absent_from_the_report(self, backend):
        reply = [series("someone-else", (0, "1e9"), (DAY, "1e9"))]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            assert backend._collect_usage(["user-1"], 0, DAY) == {}

    def test_a_single_point_measures_nothing(self, backend):
        """One reading says what is stored, not for how long."""
        reply = [series("user-1", (0, "1e9"))]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            assert backend._collect_usage(["user-1"], 0, DAY) == {}

    def test_an_unlabelled_series_is_dropped(self, backend):
        """It cannot be attributed, and billing it to anybody is worse."""
        reply = [{"metric": {}, "values": [[0, "1e9"], [DAY, "1e9"]]}]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            assert backend._collect_usage(["user-1"], 0, DAY) == {}


class TestWhenTheSeriesHasHoles:
    def test_a_nan_carries_the_previous_reading(self, backend):
        """Staleness is missing telemetry, not a drop to zero.

        Billing the gap at zero would turn a collector outage into a discount.
        """
        reply = [series("user-1", (0, "1e9"), (DAY // 2, "NaN"), (DAY, "1e9"))]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            report = backend._collect_usage(["user-1"], 0, DAY)

        assert report["user-1"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == pytest.approx(
            1.0
        )

    def test_an_all_nan_series_measures_nothing(self, backend):
        reply = [series("user-1", (0, "NaN"), (DAY, "NaN"))]
        with patch.object(backend.promql_client, "query_range", return_value=reply):
            assert backend._collect_usage(["user-1"], 0, DAY) == {}

    def test_a_failed_query_reports_nothing_rather_than_zero(self, backend):
        with patch.object(
            backend.promql_client, "query_range", side_effect=PromQLError("down")
        ):
            assert backend._collect_usage(["user-1", "user-2"], 0, DAY) == {}

    def test_a_failed_query_is_not_retried_once_per_resource(self, backend):
        """The query answers for all of them, so one failure is one failure."""
        with patch.object(
            backend.promql_client, "query_range", side_effect=PromQLError("down")
        ) as query:
            backend._collect_usage([f"user-{i}" for i in range(10)], 0, DAY)

        assert query.call_count == 1

    def test_a_resource_it_could_not_measure_is_not_pulled(self, backend):
        info = Mock()
        info.usage = {"TOTAL_ACCOUNT_USAGE": {"s3_storage": 0}}
        backend._reported_usernames = set()
        with patch.object(
            _GbDayUsageBackend.__bases__[0].__bases__[0],
            "_pull_backend_resource",
            return_value=info,
        ):
            assert backend._pull_backend_resource("user-1") is None


class TestPastPeriods:
    def test_a_past_month_is_read_back_with_both_ends_pinned(self, backend):
        """The database keeps the history, so the month's tail is billable."""
        backend.timezone = "UTC"
        start, end = backend._period_bounds(2026, 1)
        reply = [series("user-1", (start, "1e9"), (end, "1e9"))]
        with patch.object(
            backend.promql_client, "query_range", return_value=reply
        ) as query:
            report = backend.get_usage_report_for_period(["user-1"], 2026, 1)

        query.assert_called_once_with(backend.usage_query, start, end, "30m")
        assert report["user-1"]["TOTAL_ACCOUNT_USAGE"][
            "s3_storage"
        ] == pytest.approx((end - start) / DAY)
