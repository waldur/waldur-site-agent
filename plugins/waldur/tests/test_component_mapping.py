"""Tests for ComponentMapper forward and reverse conversion."""

import pytest

from waldur_site_agent_waldur.component_mapping import ComponentMapper


class TestForwardConversion:
    """Tests for convert_limits_to_target."""

    def test_conversion_single_source_to_multiple_targets(
        self, backend_components_with_conversion
    ):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_limits_to_target({"node_hours": 100})
        assert result == {"gpu_hours": 500, "storage_gb_hours": 1000}

    def test_passthrough_preserves_values(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        result = mapper.convert_limits_to_target({"cpu": 100, "mem": 200})
        assert result == {"cpu": 100, "mem": 200}

    def test_mixed_conversion_and_passthrough(self, backend_components_mixed):
        mapper = ComponentMapper(backend_components_mixed)
        result = mapper.convert_limits_to_target({"node_hours": 100, "storage": 50})
        assert result == {"gpu_hours": 500, "storage": 50}

    def test_zero_limits(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_limits_to_target({"node_hours": 0})
        assert result == {"gpu_hours": 0, "storage_gb_hours": 0}

    def test_empty_limits(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_limits_to_target({})
        assert result == {}

    def test_missing_component_passes_through(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        result = mapper.convert_limits_to_target({"unknown_comp": 42})
        assert result == {"unknown_comp": 42}

    def test_multiple_sources_to_same_target_sums(self):
        components = {
            "comp_a": {
                "measured_unit": "H",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "A",
                "target_components": {"shared_target": {"factor": 2.0}},
            },
            "comp_b": {
                "measured_unit": "H",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "B",
                "target_components": {"shared_target": {"factor": 3.0}},
            },
        }
        mapper = ComponentMapper(components)
        result = mapper.convert_limits_to_target({"comp_a": 10, "comp_b": 10})
        # 10 * 2 + 10 * 3 = 50
        assert result == {"shared_target": 50}


class TestReverseConversion:
    """Tests for convert_usage_from_target."""

    def test_reverse_sum_strategy(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_usage_from_target(
            {"gpu_hours": 500, "storage_gb_hours": 800}
        )
        # node_hours = 500/5 + 800/10 = 100 + 80 = 180
        assert result == {"node_hours": 180.0}

    def test_passthrough_reverse(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        result = mapper.convert_usage_from_target({"cpu": 100, "mem": 200})
        assert result == {"cpu": 100.0, "mem": 200.0}

    def test_mixed_reverse(self, backend_components_mixed):
        mapper = ComponentMapper(backend_components_mixed)
        result = mapper.convert_usage_from_target({"gpu_hours": 500, "storage": 50})
        assert result == {"node_hours": 100.0, "storage": 50.0}

    def test_zero_usage(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_usage_from_target(
            {"gpu_hours": 0, "storage_gb_hours": 0}
        )
        assert result == {"node_hours": 0.0}

    def test_empty_usage(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_usage_from_target({})
        assert result == {}

    def test_unknown_target_component_ignored(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        result = mapper.convert_usage_from_target({"unknown_comp": 100})
        assert result == {}

    def test_partial_target_usage(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        # Only one of the two targets is present
        result = mapper.convert_usage_from_target({"gpu_hours": 500})
        assert result == {"node_hours": 100.0}


class TestPassthroughDetection:
    """Tests for is_passthrough property."""

    def test_all_passthrough(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        assert mapper.is_passthrough is True

    def test_all_conversion(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        assert mapper.is_passthrough is False

    def test_mixed_not_passthrough(self, backend_components_mixed):
        mapper = ComponentMapper(backend_components_mixed)
        assert mapper.is_passthrough is False


class TestComponentSets:
    """Tests for source_components and target_components properties."""

    def test_source_components(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        assert mapper.source_components == {"node_hours"}

    def test_target_components_with_conversion(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        assert mapper.target_components == {"gpu_hours", "storage_gb_hours"}

    def test_passthrough_source_equals_target(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        assert mapper.source_components == {"cpu", "mem"}
        assert mapper.target_components == {"cpu", "mem"}


class TestRoundTrip:
    """Tests for forward-then-reverse consistency."""

    def test_passthrough_round_trip(self, backend_components_passthrough):
        mapper = ComponentMapper(backend_components_passthrough)
        original = {"cpu": 100, "mem": 200}
        target = mapper.convert_limits_to_target(original)
        back = mapper.convert_usage_from_target(
            {k: float(v) for k, v in target.items()}
        )
        assert back == {"cpu": 100.0, "mem": 200.0}

    def test_conversion_round_trip(self, backend_components_with_conversion):
        mapper = ComponentMapper(backend_components_with_conversion)
        original = {"node_hours": 100}
        target = mapper.convert_limits_to_target(original)
        # target = {gpu_hours: 500, storage_gb_hours: 1000}
        back = mapper.convert_usage_from_target(
            {k: float(v) for k, v in target.items()}
        )
        # node_hours = 500/5 + 1000/10 = 100 + 100 = 200
        # Note: round-trip with fan-out sums contributions, so 200 != 100
        # This is expected - forward fans out, reverse sums back
        assert back == {"node_hours": 200.0}
