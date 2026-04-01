"""Tests for SlurmReportLine parser, especially tres_usage filtering."""

from waldur_site_agent_slurm.parser import SlurmReportLine

# Passthrough config: source names match SLURM TRES names
PASSTHROUGH_TRES = {
    "cpu": {"measured_unit": "Hours", "unit_factor": 1},
    "mem": {"measured_unit": "MB", "unit_factor": 1},
    "gres/gpu": {"measured_unit": "Hours", "unit_factor": 1},
}

# Mapped config: source name (node_hours) differs from SLURM TRES names (cpu, gpu)
MAPPED_TRES = {
    "node_hours": {
        "measured_unit": "Hours",
        "unit_factor": 1,
        "target_components": {
            "cpu": {"factor": 64.0},
            "gpu": {"factor": 8.0},
        },
    },
}


def _make_line(tres_string, slurm_tres, duration="01:00:00"):
    """Build a SlurmReportLine from a TRES string."""
    raw = f"account1|{tres_string}|{duration}|user1"
    return SlurmReportLine(raw, slurm_tres)


class TestTresUsagePassthrough:
    def test_passthrough_keeps_known_tres(self):
        line = _make_line("cpu=100,mem=2048,gres/gpu=4", PASSTHROUGH_TRES)
        usage = line.tres_usage
        assert "cpu" in usage
        assert "mem" in usage
        assert "gres/gpu" in usage

    def test_passthrough_drops_unknown_tres(self):
        line = _make_line("cpu=100,billing=999", PASSTHROUGH_TRES)
        usage = line.tres_usage
        assert "cpu" in usage
        assert "billing" not in usage


class TestTresUsageMappedComponents:
    """Regression tests for WAL-9815.

    When backend_components use target_components mapping, SLURM output
    contains target names (cpu, gpu) not source names (node_hours).
    tres_usage must accept the target names so that _convert_usage_report
    can reverse-map them later.
    """

    def test_mapped_tres_are_accepted(self):
        line = _make_line("cpu=640,gpu=80", MAPPED_TRES)
        usage = line.tres_usage
        assert "cpu" in usage
        assert "gpu" in usage

    def test_mapped_tres_values_correct(self):
        # 1 hour duration → raw * 60 minutes
        line = _make_line("cpu=640,gpu=80", MAPPED_TRES, duration="01:00:00")
        usage = line.tres_usage
        assert usage["cpu"] == 640 * 60
        assert usage["gpu"] == 80 * 60

    def test_mapped_drops_unknown_tres(self):
        line = _make_line("cpu=640,billing=999", MAPPED_TRES)
        usage = line.tres_usage
        assert "cpu" in usage
        assert "billing" not in usage

    def test_mapped_empty_when_no_target_match(self):
        line = _make_line("billing=999,energy=50", MAPPED_TRES)
        usage = line.tres_usage
        assert usage == {}
