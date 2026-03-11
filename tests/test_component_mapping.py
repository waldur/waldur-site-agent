"""Tests for core ComponentMapper forward and reverse conversion."""

import pytest

from waldur_site_agent.common.component_mapping import ComponentMapper


@pytest.fixture
def components_with_conversion():
    return {
        "node_hours": {
            "measured_unit": "Hours",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "Node Hours",
            "target_components": {
                "gpu_hours": {"factor": 5.0},
                "storage_gb_hours": {"factor": 10.0},
            },
        },
    }


@pytest.fixture
def components_passthrough():
    return {
        "cpu": {"measured_unit": "Hours"},
        "mem": {"measured_unit": "GB"},
    }


@pytest.fixture
def components_mixed():
    return {
        "node_hours": {
            "measured_unit": "Hours",
            "target_components": {"gpu_hours": {"factor": 5.0}},
        },
        "storage": {"measured_unit": "GB"},
    }


class TestForwardConversion:
    def test_single_source_to_multiple_targets(self, components_with_conversion):
        mapper = ComponentMapper(components_with_conversion)
        result = mapper.convert_limits_to_target({"node_hours": 100})
        assert result == {"gpu_hours": 500, "storage_gb_hours": 1000}

    def test_passthrough(self, components_passthrough):
        mapper = ComponentMapper(components_passthrough)
        result = mapper.convert_limits_to_target({"cpu": 100, "mem": 200})
        assert result == {"cpu": 100, "mem": 200}

    def test_mixed(self, components_mixed):
        mapper = ComponentMapper(components_mixed)
        result = mapper.convert_limits_to_target({"node_hours": 100, "storage": 50})
        assert result == {"gpu_hours": 500, "storage": 50}

    def test_empty(self, components_with_conversion):
        mapper = ComponentMapper(components_with_conversion)
        assert mapper.convert_limits_to_target({}) == {}


class TestReverseConversion:
    def test_reverse_sum(self, components_with_conversion):
        mapper = ComponentMapper(components_with_conversion)
        result = mapper.convert_usage_from_target({"gpu_hours": 500, "storage_gb_hours": 800})
        assert result == {"node_hours": 180.0}

    def test_passthrough_reverse(self, components_passthrough):
        mapper = ComponentMapper(components_passthrough)
        result = mapper.convert_usage_from_target({"cpu": 100, "mem": 200})
        assert result == {"cpu": 100.0, "mem": 200.0}

    def test_unknown_target_ignored(self, components_with_conversion):
        mapper = ComponentMapper(components_with_conversion)
        assert mapper.convert_usage_from_target({"unknown": 100}) == {}


class TestProperties:
    def test_passthrough_detection(self, components_passthrough):
        assert ComponentMapper(components_passthrough).is_passthrough is True

    def test_conversion_not_passthrough(self, components_with_conversion):
        assert ComponentMapper(components_with_conversion).is_passthrough is False

    def test_source_components(self, components_with_conversion):
        assert ComponentMapper(components_with_conversion).source_components == {"node_hours"}

    def test_target_components(self, components_with_conversion):
        assert ComponentMapper(components_with_conversion).target_components == {
            "gpu_hours",
            "storage_gb_hours",
        }
