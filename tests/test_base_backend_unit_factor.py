"""Tests for unit_factor conversion in BaseBackend.set_resource_limits."""

from unittest.mock import MagicMock

from waldur_site_agent.backend.backends import BaseBackend


class ConcreteBackend(BaseBackend):
    """Minimal concrete subclass that inherits set_resource_limits unchanged."""

    def ping(self, raise_exception=False):
        return True

    def list_backend_components(self):
        return []

    def list_components(self):
        return []

    def _get_usage_report(self, resource_backend_ids):
        return {}

    def _collect_resource_limits(self, waldur_resource):
        return {}, {}

    def _pre_create_resource(self, *args, **kwargs):
        pass

    def diagnostics(self):
        return {}

    def downscale_resource(self, *args, **kwargs):
        pass

    def get_resource_metadata(self, *args, **kwargs):
        return {}

    def pause_resource(self, *args, **kwargs):
        pass

    def restore_resource(self, *args, **kwargs):
        pass


class TestSetResourceLimitsUnitFactor:
    def _make_backend(self, components):
        backend = ConcreteBackend(
            backend_settings={}, backend_components=components
        )
        backend.client = MagicMock()
        return backend

    def test_limits_multiplied_by_unit_factor(self):
        backend = self._make_backend(
            {"cpu": {"unit_factor": 60}, "mem": {"unit_factor": 1024}}
        )
        backend.set_resource_limits("res-1", {"cpu": 10, "mem": 4})
        backend.client.set_resource_limits.assert_called_once_with(
            "res-1", {"cpu": 600, "mem": 4096}
        )

    def test_unit_factor_defaults_to_one(self):
        backend = self._make_backend({"cpu": {"measured_unit": "cores"}})
        backend.set_resource_limits("res-1", {"cpu": 8})
        backend.client.set_resource_limits.assert_called_once_with(
            "res-1", {"cpu": 8}
        )

    def test_unknown_component_uses_factor_one(self):
        backend = self._make_backend({})
        backend.set_resource_limits("res-1", {"unknown": 5})
        backend.client.set_resource_limits.assert_called_once_with(
            "res-1", {"unknown": 5}
        )

    def test_empty_limits_passed_through(self):
        backend = self._make_backend({"cpu": {"unit_factor": 60}})
        backend.set_resource_limits("res-1", {})
        backend.client.set_resource_limits.assert_called_once_with("res-1", {})
