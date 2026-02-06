"""Tests for the custom backend plugin.

TODO: Replace stub tests with real assertions for your backend.
"""

from unittest.mock import MagicMock

from waldur_site_agent.backend.structures import ClientResource


class TestPing:
    def test_ping_success(self, backend):
        backend.client.list_resources.return_value = []
        assert backend.ping() is True

    def test_ping_failure(self, backend):
        backend.client.list_resources.side_effect = Exception("Connection refused")
        assert backend.ping() is False


class TestUsageReport:
    def test_empty_usage_report(self, backend):
        report = backend._get_usage_report(["alloc_1"])
        assert "alloc_1" in report
        assert "TOTAL_ACCOUNT_USAGE" in report["alloc_1"]
        # All components should be present with zero values
        for component in backend.backend_components:
            assert component in report["alloc_1"]["TOTAL_ACCOUNT_USAGE"]
            assert report["alloc_1"]["TOTAL_ACCOUNT_USAGE"][component] == 0

    def test_usage_report_multiple_resources(self, backend):
        report = backend._get_usage_report(["alloc_1", "alloc_2"])
        assert "alloc_1" in report
        assert "alloc_2" in report


class TestCollectResourceLimits:
    def test_no_limits(self, backend):
        resource = MagicMock()
        resource.limits = None
        backend_limits, waldur_limits = backend._collect_resource_limits(resource)
        assert backend_limits == {}
        assert waldur_limits == {}


class TestResourceLifecycle:
    def test_downscale_resource(self, backend):
        assert backend.downscale_resource("alloc_1") is True

    def test_pause_resource(self, backend):
        assert backend.pause_resource("alloc_1") is True

    def test_restore_resource(self, backend):
        assert backend.restore_resource("alloc_1") is True

    def test_get_resource_metadata(self, backend):
        metadata = backend.get_resource_metadata("alloc_1")
        assert isinstance(metadata, dict)


class TestListComponents:
    def test_list_components(self, backend):
        components = backend.list_components()
        assert isinstance(components, list)
