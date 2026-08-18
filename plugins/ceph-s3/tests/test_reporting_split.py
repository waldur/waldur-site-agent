"""Metering lives outside the management backend, and never reports a zero.

The management backend serves both flavours and meters neither: croit's series
comes from its statistics subsystem, and RadosGW's has to be recorded before it
can be integrated, so neither belongs on the path that also creates users and
applies quotas. GB-day reporting therefore lives in its own backend, wired
through ``reporting_backend``.

This file covers the split itself and croit's half of it. The radosgw half is in
``test_reporting_radosgw``.
"""

import sys
from unittest.mock import Mock, patch

import pytest

from waldur_site_agent_ceph_s3.backend import CephS3Backend
from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

CROIT_SETTINGS = {
    "api_url": "https://croit.example.org",
    "token": "t",
    "s3_endpoint": "https://rgw.example.org",
}
RADOSGW_SETTINGS = {
    "flavour": "radosgw",
    "s3_endpoint": "https://rgw.example.org",
    "admin_access_key": "ADMINACCESSKEY000000",
    "admin_secret_key": "s" * 40,
}
COMPONENTS = {
    "s3_storage": {
        "accounting_type": "usage",
        "backend_name": "storage",
        "unit_factor": 1000000000,
    }
}


@pytest.fixture
def croit_backend():
    return CephS3Backend(dict(CROIT_SETTINGS), dict(COMPONENTS))


@pytest.fixture
def radosgw_backend():
    return CephS3Backend(dict(RADOSGW_SETTINGS), dict(COMPONENTS))


@pytest.fixture
def usage_backend():
    return CroitUsageBackend(dict(CROIT_SETTINGS), dict(COMPONENTS))


class TestTheSplit:
    def test_the_reporting_backend_is_also_a_management_backend(self):
        """It inherits the whole client surface; only metering is added."""
        assert issubclass(CroitUsageBackend, CephS3Backend)

    def test_croit_usage_is_registered_as_its_own_backend(self):
        names = {ep.name for ep in entry_points(group="waldur_site_agent.backends")}
        assert "croit_usage" in names

    def test_the_reporting_backend_refuses_the_radosgw_flavour(self):
        """Nothing to integrate: Admin Ops exposes no stored-bytes series."""
        with pytest.raises(ValueError, match="croit"):
            CroitUsageBackend(dict(RADOSGW_SETTINGS), dict(COMPONENTS))


class TestManagementBackendDoesNotMeter:
    def test_it_reports_nothing(self, croit_backend):
        """membership_sync calls this too, so it returns empty rather than raising."""
        assert croit_backend._get_usage_report(["user-1"]) == {}

    def test_it_reports_nothing_on_radosgw_either(self, radosgw_backend):
        assert radosgw_backend._get_usage_report(["user-1"]) == {}

    def test_it_never_substitutes_a_zero(self, croit_backend):
        """The base fills every component with 0 when a report omits a resource.

        This backend never reports usage at all, so without an override every
        membership-sync pass would overwrite the period's accrued GB-days with
        zeros — silently, because zero is a legitimate usage value.
        """
        info = Mock()
        info.usage = {"TOTAL_ACCOUNT_USAGE": {"s3_storage": 0}}
        with patch.object(
            CephS3Backend.__bases__[0], "_pull_backend_resource", return_value=info
        ):
            pulled = croit_backend._pull_backend_resource("user-1")

        assert pulled is not None, "membership sync still needs the user list"
        assert pulled.usage == {}, "an unmeasured resource must carry no usage"


class TestReportingBackendStillMeters:
    def test_it_integrates_the_series(self, usage_backend):
        """One GB held for one day is one GB-day."""
        series = [
            {"t": 0, "v": 1_000_000_000},
            {"t": 86400, "v": 1_000_000_000},
        ]
        with patch.object(
            usage_backend.client, "get_user_storage_series", return_value=series
        ):
            report = usage_backend._collect_usage(["user-1"], 0, 86400)

        assert report["user-1"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == pytest.approx(1.0)

    def test_an_unreadable_series_is_absent_rather_than_zero(self, usage_backend):
        with patch.object(
            usage_backend.client, "get_user_storage_series", return_value=[]
        ):
            report = usage_backend._collect_usage(["user-1"], 0, 86400)

        assert report == {}

    def test_a_resource_it_could_not_measure_is_not_pulled(self, usage_backend):
        """The guard and the set it reads must stay on the same instance."""
        info = Mock()
        info.usage = {"TOTAL_ACCOUNT_USAGE": {"s3_storage": 0}}
        usage_backend._reported_usernames = set()
        with patch.object(
            CephS3Backend.__bases__[0], "_pull_backend_resource", return_value=info
        ):
            assert usage_backend._pull_backend_resource("user-1") is None
