"""croit reporting for a radosgw-managed offering, and per-bucket metering of accounts.

croit attributes an RGW account's storage to its buckets, never to its users; the
series shapes here are the ones croit v2611.1 answered on Ceph 20.2.5.
"""

import json
from decimal import Decimal
from unittest.mock import Mock, patch

import httpx
import pytest

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_ceph_s3.clients.croit import CroitClient
from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend, _BucketLedger

from conftest import RADOSGW_SETTINGS

GB = 1000000000
DAY = 86400
COMPONENTS = {
    "s3_storage": {"accounting_type": "usage", "backend_name": "storage", "unit_factor": GB}
}
CROIT = {"api_url": "https://croit.example.org", "token": "t"}
ACCOUNTS = {**RADOSGW_SETTINGS, "rgw_accounts": True, "croit_reporting": CROIT}


def flat(gb, start=0, days=2):
    """A series holding ``gb`` from ``start`` for ``days`` daily samples."""
    return [{"t": start + day * DAY, "v": gb * GB} for day in range(days + 1)]


def make_backend(settings=ACCOUNTS, state_file=""):
    settings = json.loads(json.dumps(settings))
    if state_file:
        settings["croit_reporting"]["bucket_state_file"] = state_file
    with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"), patch(
        "waldur_site_agent_ceph_s3.reporting.CroitClient"
    ):
        backend = CroitUsageBackend(settings, COMPONENTS)
    backend.client = Mock()
    backend.croit_client = Mock()
    return backend


class TestConfiguration:
    def test_radosgw_management_reports_through_its_own_croit_credentials(self):
        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient"), patch(
            "waldur_site_agent_ceph_s3.reporting.CroitClient"
        ) as croit:
            backend = CroitUsageBackend({**RADOSGW_SETTINGS, "croit_reporting": CROIT}, COMPONENTS)

        assert croit.call_args.kwargs["api_url"] == CROIT["api_url"]
        assert croit.call_args.kwargs["token"] == "t"
        assert backend.croit_client is croit.return_value
        assert backend.croit_client is not backend.client

    def test_radosgw_without_croit_reporting_is_refused(self):
        with pytest.raises(ValueError, match="croit_reporting"):
            CroitUsageBackend(dict(RADOSGW_SETTINGS), COMPONENTS)

    def test_croit_reporting_needs_credentials(self):
        settings = {**RADOSGW_SETTINGS, "croit_reporting": {"api_url": "https://croit"}}
        with pytest.raises(ValueError, match="token"):
            CroitUsageBackend(settings, COMPONENTS)

    def test_the_croit_flavour_does_not_take_a_second_croit(self):
        settings = {
            "api_url": "https://croit", "token": "t", "s3_endpoint": "https://s3",
            "croit_reporting": CROIT,
        }
        with pytest.raises(ValueError, match="already reports through api_url"):
            CroitUsageBackend(settings, COMPONENTS)


class TestAccountUsage:
    def test_an_account_is_the_sum_of_its_buckets(self):
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "a"}, {"name": "b"}]
        backend.croit_client.get_bucket_storage_series.side_effect = (
            lambda bucket, tenant, start, end: flat(2 if bucket == "a" else 3)
        )

        report = backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        assert report == {"waldur-x": {"TOTAL_ACCOUNT_USAGE": {"s3_storage": 10.0}}}
        backend.croit_client.get_user_storage_series.assert_not_called()

    def test_the_per_user_graph_is_kept_for_plain_users(self):
        backend = make_backend({**RADOSGW_SETTINGS, "croit_reporting": CROIT})
        backend.croit_client.get_user_storage_series.return_value = flat(1)

        report = backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0
        backend.croit_client.get_bucket_storage_series.assert_not_called()

    def test_a_bucket_deleted_mid_period_stays_billed(self):
        """Gone from every listing, but croit still has its series for the period."""
        backend = make_backend()
        backend.croit_client.get_bucket_storage_series.side_effect = (
            lambda bucket, tenant, start, end: flat(1)
        )
        backend.client.get_user_buckets.return_value = [{"name": "a"}, {"name": "b"}]
        backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        backend.client.get_user_buckets.return_value = [{"name": "a"}]
        report = backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        queried = {c.args[0] for c in backend.croit_client.get_bucket_storage_series.call_args_list}
        assert queried == {"a", "b"}
        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 4.0

    def test_the_ledger_survives_a_restart_through_its_state_file(self, tmp_path):
        state = str(tmp_path / "ledger.json")
        first = make_backend(state_file=state)
        first.client.get_user_buckets.return_value = [{"name": "gone-later"}]
        first.croit_client.get_bucket_storage_series.return_value = flat(1)
        first._collect_usage(["waldur-x"], 0, 2 * DAY)

        restarted = make_backend(state_file=state)
        restarted.client.get_user_buckets.return_value = []
        restarted.croit_client.get_bucket_storage_series.return_value = flat(1)
        report = restarted._collect_usage(["waldur-x"], 0, 2 * DAY)

        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0

    def test_an_account_without_any_known_bucket_is_not_reported(self):
        """After a restart without a state file this may be an account whose buckets
        were all deleted; a zero would wipe the accrued total."""
        backend = make_backend()
        backend.client.get_user_buckets.return_value = []

        assert backend._collect_usage(["waldur-x"], 0, 2 * DAY) == {}
        assert "waldur-x" not in backend._reported_usernames

    def test_buckets_croit_has_not_sampled_are_not_a_zero(self):
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "new"}]
        backend.croit_client.get_bucket_storage_series.return_value = []

        assert backend._collect_usage(["waldur-x"], 0, 2 * DAY) == {}

    def test_an_unsampled_bucket_does_not_hide_the_measured_ones(self):
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "old"}, {"name": "new"}]
        backend.croit_client.get_bucket_storage_series.side_effect = (
            lambda bucket, tenant, start, end: flat(1) if bucket == "old" else []
        )

        report = backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0

    def test_a_listing_failure_skips_the_resource(self):
        backend = make_backend()
        backend.client.get_user_buckets.side_effect = RuntimeError("gateway down")

        assert backend._collect_usage(["waldur-x"], 0, 2 * DAY) == {}


    def test_a_deleted_bucket_is_billed_only_until_its_last_reading(self):
        """croit answers null for every interval after the deletion (measured)."""
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "kept"}]
        backend._bucket_ledger.remember("waldur-x", 0, ["deleted"])
        held_one_day = [
            {"t": 0, "v": GB}, {"t": DAY, "v": GB},
            {"t": 2 * DAY, "v": None}, {"t": 3 * DAY, "v": None}, {"t": 4 * DAY, "v": None},
        ]
        backend.croit_client.get_bucket_storage_series.side_effect = (
            lambda bucket, tenant, start, end: held_one_day if bucket == "deleted" else flat(0, days=4)
        )

        report = backend._collect_usage(["waldur-x"], 0, 4 * DAY)

        # Day one held, plus the closing interval of one sample -- not four days.
        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0

    def test_a_gap_between_readings_is_still_carried(self):
        backend = make_backend()
        backend.client.get_user_buckets.return_value = [{"name": "b"}]
        backend.croit_client.get_bucket_storage_series.return_value = [
            {"t": 0, "v": GB}, {"t": DAY, "v": None}, {"t": 2 * DAY, "v": GB},
        ]

        report = backend._collect_usage(["waldur-x"], 0, 2 * DAY)

        # 1 GB held across both days: the null day carries the reading before it.
        assert report["waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0


class TestLedger:
    def test_a_year_of_periods_is_kept(self):
        ledger = _BucketLedger()
        for month in range(1, 16):
            ledger.remember("u", month, [f"b{month}"])

        assert ledger.seen("u", 2) == []
        assert ledger.seen("u", 3) == ["b3"]
        assert ledger.seen("u", 15) == ["b15"]

    def test_a_period_older_than_those_kept_is_still_answered(self):
        """A historical recompute of an old month must not crash on pruning."""
        ledger = _BucketLedger()
        for month in range(100, 113):
            ledger.remember("u", month, ["x"])

        assert ledger.remember("u", 1, ["old"]) == ["old"]

    def test_the_state_file_is_written_once_per_pass_and_only_on_change(self, tmp_path):
        backend = make_backend(state_file=str(tmp_path / "ledger.json"))
        backend.client.get_user_buckets.return_value = [{"name": "b"}]
        backend.croit_client.get_bucket_storage_series.return_value = flat(1)

        with patch.object(_BucketLedger, "_save") as save:
            backend._collect_usage([f"u{i}" for i in range(5)], 0, 2 * DAY)
            backend._collect_usage([f"u{i}" for i in range(5)], 0, 2 * DAY)

        assert save.call_count == 1

    def test_an_unreadable_state_file_starts_empty(self, tmp_path):
        state = tmp_path / "ledger.json"
        state.write_text("{not json")

        assert _BucketLedger(str(state)).seen("u", 1) == []


class TestCroitClient:
    def make_client(self, handler):
        client = CroitClient(api_url="https://croit.example.org", token="t")
        client.session = httpx.Client(transport=httpx.MockTransport(handler))
        return client

    def test_reads_the_logical_plot_of_the_bucket_graph(self):
        seen = {}

        def handler(request):
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"axis1": {"graphs": [
                {"name": "Data with overhead", "datapoints": [{"t": 1, "v": 9}]},
                {"name": "Data", "datapoints": [{"t": 1, "v": 7}]},
            ]}})

        points = self.make_client(handler).get_bucket_storage_series("b1", "", 0, 10)

        assert points == [{"t": 1, "v": 7}]
        assert seen["graph"] == "s3-bucket-data"
        assert seen["template-s3-bucket-name"] == "b1"

    def test_a_tenanted_bucket_is_named_tenant_underscore_bucket(self):
        seen = {}

        def handler(request):
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"axis1": {"graphs": []}})

        assert self.make_client(handler).get_bucket_storage_series("logs", "acme", 0) == []
        assert seen["template-s3-bucket-name"] == "acme_logs"


def test_integration_adds_across_buckets():
    """Summing per-bucket integrals equals integrating the summed curve."""
    a, b = flat(1), flat(3)
    total = CroitUsageBackend._integrate_gb_days(a, GB, 2 * DAY) + CroitUsageBackend._integrate_gb_days(b, GB, 2 * DAY)
    summed = [{"t": p["t"], "v": p["v"] + q["v"]} for p, q in zip(a, b)]
    assert total == CroitUsageBackend._integrate_gb_days(summed, GB, 2 * DAY) == Decimal(8)


class TestHistoricalLoad:
    def test_an_unmeasurable_period_raises_for_the_loader(self):
        """The loader reads a missing resource as zero and, with --reconcile-stale,
        zeroes the month."""
        backend = make_backend()
        backend.historical_load = True
        backend.client.get_user_buckets.return_value = []

        with pytest.raises(BackendError, match="waldur-x"):
            backend.get_usage_report_for_period(["waldur-x"], 2026, 9)

    def test_the_reporting_cycle_still_skips_quietly(self):
        backend = make_backend()
        backend.client.get_user_buckets.return_value = []

        assert backend.get_usage_report_for_period(["waldur-x"], 2026, 9) == {}


class TestTenantedUsers:
    """croit's per-user graph cannot name a tenanted user; its buckets it can."""

    def test_a_tenanted_croit_user_is_metered_through_its_buckets(self):
        with patch("waldur_site_agent_ceph_s3.backend.CroitClient"):
            backend = CroitUsageBackend(
                {"api_url": "https://croit", "token": "t", "s3_endpoint": "https://s3",
                 "tenant_template": "{resource_slug}"},
                COMPONENTS,
            )
        backend.client = Mock()
        backend.croit_client = Mock()
        backend.client.get_user_buckets.return_value = [{"name": "data"}]
        backend.croit_client.get_bucket_storage_series.return_value = flat(1)

        report = backend._collect_usage(["acme$waldur-x"], 0, 2 * DAY)

        backend.croit_client.get_bucket_storage_series.assert_called_once_with(
            "data", "acme", 0, 2 * DAY
        )
        backend.croit_client.get_user_storage_series.assert_not_called()
        assert report["acme$waldur-x"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 2.0

    def test_croits_tenant_colon_bucket_listing_is_read_as_the_bare_name(self):
        """croit lists a tenanted bucket as "tenant:bucket" (measured); RGW does not."""
        with patch("waldur_site_agent_ceph_s3.backend.CroitClient"):
            backend = CroitUsageBackend(
                {"api_url": "https://croit", "token": "t", "s3_endpoint": "https://s3"},
                COMPONENTS,
            )
        backend.client = Mock()
        backend.croit_client = Mock()
        backend.client.get_user_buckets.return_value = [{"name": "smit:client-data"}]
        backend.croit_client.get_bucket_storage_series.return_value = flat(1)

        backend._collect_usage(["smit$owner"], 0, 2 * DAY)

        backend.croit_client.get_bucket_storage_series.assert_called_once_with(
            "client-data", "smit", 0, 2 * DAY
        )
