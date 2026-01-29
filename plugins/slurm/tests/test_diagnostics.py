"""Tests for SLURM account diagnostics."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from waldur_site_agent.backend.structures import ClientResource
from waldur_site_agent.common.structures import BackendComponent, Offering
from waldur_site_agent_slurm.diagnostic_service import SlurmAccountDiagnosticService
from waldur_site_agent_slurm.diagnostics import (
    AccountDiagnostic,
    ComparisonResult,
    ComponentUnitInfo,
    DiagnosticStatus,
    ExpectedSettings,
    PolicyInfo,
    SlurmAccountInfo,
    WaldurResourceInfo,
)


@pytest.fixture
def mock_offering() -> Offering:
    """Create a mock offering for testing."""
    return Offering(
        name="Test SLURM Offering",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="test-token",
        waldur_offering_uuid="12345678-1234-1234-1234-123456789012",
        backend_type="slurm",
        backend_settings={
            "allocation_prefix": "alloc_",
            "project_prefix": "proj_",
            "customer_prefix": "org_",
            "default_account": "root",
            "qos_default": "normal",
            "qos_downscaled": "slowdown",
            "qos_paused": "blocked",
        },
        backend_components={
            "cpu": BackendComponent(
                measured_unit="Hours",
                unit_factor=60.0,
                accounting_type="usage",
                label="CPU",
            ),
            "mem": BackendComponent(
                measured_unit="GB-Hours",
                unit_factor=1000.0,
                accounting_type="usage",
                label="Memory",
            ),
        },
    )


@pytest.fixture
def mock_slurm_client() -> MagicMock:
    """Create a mock SLURM client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_waldur_client() -> MagicMock:
    """Create a mock Waldur client."""
    client = MagicMock()
    return client


class TestSlurmAccountInfo:
    """Tests for SlurmAccountInfo data structure."""

    def test_slurm_account_info_exists(self) -> None:
        """Test SlurmAccountInfo when account exists."""
        info = SlurmAccountInfo(
            exists=True,
            name="test_account",
            description="Test Account",
            fairshare=1000,
            qos="normal",
            grp_tres_mins={"cpu": "6000", "mem": "10000"},
            users=["user1", "user2"],
        )
        assert info.exists is True
        assert info.name == "test_account"
        assert info.fairshare == 1000
        assert info.qos == "normal"

    def test_slurm_account_info_not_exists(self) -> None:
        """Test SlurmAccountInfo when account doesn't exist."""
        info = SlurmAccountInfo(
            exists=False,
            name="missing_account",
            error="Account not found",
        )
        assert info.exists is False
        assert info.error == "Account not found"


class TestWaldurResourceInfo:
    """Tests for WaldurResourceInfo data structure."""

    def test_waldur_resource_info_exists(self) -> None:
        """Test WaldurResourceInfo when resource exists."""
        info = WaldurResourceInfo(
            exists=True,
            uuid="abcd-1234",
            name="Test Resource",
            state="OK",
            limits={"cpu": 100, "mem": 10},
        )
        assert info.exists is True
        assert info.uuid == "abcd-1234"
        assert info.limits == {"cpu": 100, "mem": 10}

    def test_waldur_resource_info_not_exists(self) -> None:
        """Test WaldurResourceInfo when resource doesn't exist."""
        info = WaldurResourceInfo(
            exists=False,
            error="Resource not found",
        )
        assert info.exists is False
        assert info.error == "Resource not found"


class TestPolicyInfo:
    """Tests for PolicyInfo data structure."""

    def test_policy_info_exists(self) -> None:
        """Test PolicyInfo when policy exists."""
        info = PolicyInfo(
            exists=True,
            uuid="policy-uuid",
            period="quarterly",
            limit_type="GrpTRESMins",
            tres_billing_enabled=True,
            grace_ratio=0.2,
        )
        assert info.exists is True
        assert info.limit_type == "GrpTRESMins"
        assert info.grace_ratio == 0.2


class TestAccountDiagnostic:
    """Tests for AccountDiagnostic data structure."""

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        diagnostic = AccountDiagnostic(
            account_name="test_account",
            slurm_info=SlurmAccountInfo(exists=True, name="test_account"),
            waldur_info=WaldurResourceInfo(exists=True, uuid="uuid-1234"),
            policy_info=PolicyInfo(exists=False),
            comparisons=[],
            overall_status=DiagnosticStatus.OK,
        )
        result = diagnostic.to_dict()
        assert result["account_name"] == "test_account"
        assert result["overall_status"] == "ok"
        assert result["slurm_info"]["exists"] is True
        assert result["waldur_info"]["exists"] is True


class TestSlurmAccountDiagnosticService:
    """Tests for SlurmAccountDiagnosticService."""

    def test_get_slurm_account_info_exists(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test getting account info when account exists."""
        mock_slurm_client.get_resource.return_value = ClientResource(
            name="test_account",
            description="Test Account",
            organization="Test Org",
        )
        mock_slurm_client.get_account_fairshare.return_value = 1000
        mock_slurm_client.get_current_account_qos.return_value = "normal"
        mock_slurm_client.get_account_limits.return_value = {
            "GrpTRESMins": {"cpu": "6000", "mem": "10000"},
            "MaxTRESMins": {},
            "GrpTRES": {},
            "MaxTRES": {},
        }
        mock_slurm_client.list_resource_users.return_value = ["user1", "user2"]

        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        info = service.get_slurm_account_info("test_account")

        assert info.exists is True
        assert info.name == "test_account"
        assert info.fairshare == 1000
        assert info.qos == "normal"
        assert info.users == ["user1", "user2"]

    def test_get_slurm_account_info_not_exists(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test getting account info when account doesn't exist."""
        mock_slurm_client.get_resource.return_value = None

        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        info = service.get_slurm_account_info("missing_account")

        assert info.exists is False
        assert "not found" in info.error.lower()

    def test_calculate_expected_settings_with_policy(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test calculating expected settings from policy."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        waldur_info = WaldurResourceInfo(
            exists=True,
            uuid="resource-uuid",
            limits={"cpu": 100, "mem": 10},
        )
        policy_info = PolicyInfo(
            exists=True,
            limit_type="GrpTRESMins",
            component_limits=[
                {"type": "cpu", "limit": 100},
                {"type": "mem", "limit": 10},
            ],
        )

        expected = service.calculate_expected_settings(waldur_info, policy_info)

        assert expected is not None
        assert expected.limit_type == "GrpTRESMins"
        # cpu: 100 * 60 (unit_factor) = 6000
        assert expected.limits["cpu"] == 6000
        # mem: 10 * 1000 (unit_factor) = 10000
        assert expected.limits["mem"] == 10000
        assert expected.qos == "normal"

        # Check unit conversion info is populated
        assert "cpu" in expected.unit_info
        assert "mem" in expected.unit_info
        cpu_unit = expected.unit_info["cpu"]
        assert cpu_unit.waldur_unit == "Hours"
        assert cpu_unit.waldur_value == 100.0
        assert cpu_unit.slurm_unit == "TRES-minutes"
        assert cpu_unit.slurm_value == 6000
        assert cpu_unit.unit_factor == 60.0

        mem_unit = expected.unit_info["mem"]
        assert mem_unit.waldur_unit == "GB-Hours"
        assert mem_unit.waldur_value == 10.0
        assert mem_unit.slurm_unit == "TRES-minutes"
        assert mem_unit.slurm_value == 10000
        assert mem_unit.unit_factor == 1000.0

    def test_calculate_expected_settings_no_policy(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test calculating expected settings without policy."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        waldur_info = WaldurResourceInfo(exists=False)
        policy_info = PolicyInfo(exists=False)

        expected = service.calculate_expected_settings(waldur_info, policy_info)

        assert expected is None

    def test_compare_settings_all_match(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test comparison when all settings match."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        slurm_info = SlurmAccountInfo(
            exists=True,
            name="test_account",
            qos="normal",
            grp_tres_mins={"cpu": "6000", "mem": "10000"},
        )
        expected = ExpectedSettings(
            qos="normal",
            limits={"cpu": 6000, "mem": 10000},
            limit_type="GrpTRESMins",
            reasoning={},
        )

        comparisons = service.compare_settings(slurm_info, expected, "test_account")

        assert all(c.status == DiagnosticStatus.OK for c in comparisons)

    def test_compare_settings_mismatch(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test comparison when settings don't match."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        slurm_info = SlurmAccountInfo(
            exists=True,
            name="test_account",
            qos="slowdown",  # Mismatched
            grp_tres_mins={"cpu": "5000", "mem": "10000"},  # cpu mismatched
        )
        expected = ExpectedSettings(
            qos="normal",
            limits={"cpu": 6000, "mem": 10000},
            limit_type="GrpTRESMins",
            reasoning={},
            unit_info={
                "cpu": ComponentUnitInfo(
                    component_type="cpu",
                    waldur_unit="Hours",
                    waldur_value=100.0,
                    slurm_unit="TRES-minutes",
                    slurm_value=6000,
                    unit_factor=60.0,
                ),
                "mem": ComponentUnitInfo(
                    component_type="mem",
                    waldur_unit="GB-Hours",
                    waldur_value=10.0,
                    slurm_unit="TRES-minutes",
                    slurm_value=10000,
                    unit_factor=1000.0,
                ),
            },
        )

        comparisons = service.compare_settings(slurm_info, expected, "test_account")

        # Find mismatches
        mismatches = [c for c in comparisons if c.status == DiagnosticStatus.MISMATCH]
        assert len(mismatches) >= 2  # qos and cpu should be mismatched

        # Check fix commands exist
        for mismatch in mismatches:
            assert mismatch.fix_command is not None
            assert "sacctmgr" in mismatch.fix_command

        # Check unit info is included in limit comparisons
        cpu_comp = next(c for c in comparisons if "cpu" in c.field)
        assert cpu_comp.waldur_value == 100.0
        assert cpu_comp.waldur_unit == "Hours"
        assert cpu_comp.slurm_unit == "TRES-minutes"
        assert cpu_comp.unit_factor == 60.0

    def test_generate_fix_commands(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test generating fix commands from comparisons."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        comparisons = [
            ComparisonResult(
                field="qos",
                actual="slowdown",
                expected="normal",
                status=DiagnosticStatus.MISMATCH,
                fix_command="sacctmgr -i modify account test set qos=normal",
            ),
            ComparisonResult(
                field="fairshare",
                actual=500,
                expected=1000,
                status=DiagnosticStatus.MISMATCH,
                fix_command="sacctmgr -i modify account test set fairshare=1000",
            ),
            ComparisonResult(
                field="GrpTRESMins[cpu]",
                actual=5000,
                expected=6000,
                status=DiagnosticStatus.OK,  # This one is OK
            ),
        ]

        fix_commands = service.generate_fix_commands(comparisons)

        assert len(fix_commands) == 2
        assert "qos=normal" in fix_commands[0]
        assert "fairshare=1000" in fix_commands[1]

    def test_generate_fix_commands_deduplication(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test that duplicate fix commands are deduplicated."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        # Two different fields with the same fix command (e.g., TRES limits)
        same_fix = "sacctmgr -i modify account test set GrpTRESMins=cpu=6000,mem=10000"
        comparisons = [
            ComparisonResult(
                field="GrpTRESMins[cpu]",
                actual=5000,
                expected=6000,
                status=DiagnosticStatus.MISMATCH,
                fix_command=same_fix,
            ),
            ComparisonResult(
                field="GrpTRESMins[mem]",
                actual=9000,
                expected=10000,
                status=DiagnosticStatus.MISMATCH,
                fix_command=same_fix,
            ),
        ]

        fix_commands = service.generate_fix_commands(comparisons)

        # Should only have one command, not two
        assert len(fix_commands) == 1

    def test_determine_overall_status_ok(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test overall status determination when all is OK."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        slurm_info = SlurmAccountInfo(exists=True, name="test")
        waldur_info = WaldurResourceInfo(exists=True)
        comparisons = [
            ComparisonResult(
                field="qos",
                actual="normal",
                expected="normal",
                status=DiagnosticStatus.OK,
            ),
        ]

        status = service._determine_overall_status(slurm_info, waldur_info, comparisons)
        assert status == DiagnosticStatus.OK

    def test_determine_overall_status_mismatch(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test overall status determination when there's a mismatch."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        slurm_info = SlurmAccountInfo(exists=True, name="test")
        waldur_info = WaldurResourceInfo(exists=True)
        comparisons = [
            ComparisonResult(
                field="qos",
                actual="slowdown",
                expected="normal",
                status=DiagnosticStatus.MISMATCH,
            ),
        ]

        status = service._determine_overall_status(slurm_info, waldur_info, comparisons)
        assert status == DiagnosticStatus.MISMATCH

    def test_determine_overall_status_missing(
        self,
        mock_offering: Offering,
        mock_slurm_client: MagicMock,
        mock_waldur_client: MagicMock,
    ) -> None:
        """Test overall status when resource is missing."""
        service = SlurmAccountDiagnosticService(
            slurm_client=mock_slurm_client,
            waldur_client=mock_waldur_client,
            offering=mock_offering,
        )

        slurm_info = SlurmAccountInfo(exists=False, name="test")
        waldur_info = WaldurResourceInfo(exists=True)
        comparisons: list[ComparisonResult] = []

        status = service._determine_overall_status(slurm_info, waldur_info, comparisons)
        assert status == DiagnosticStatus.MISSING


class TestDiagnosticCLIHelpers:
    """Tests for CLI helper functions."""

    def test_find_slurm_offering_single(self, mock_offering: Offering) -> None:
        """Test finding SLURM offering when there's only one."""
        from waldur_site_agent_slurm.diagnostic_cli import find_slurm_offering

        offering = find_slurm_offering([mock_offering])
        assert offering == mock_offering

    def test_find_slurm_offering_by_uuid(self, mock_offering: Offering) -> None:
        """Test finding SLURM offering by UUID."""
        from waldur_site_agent_slurm.diagnostic_cli import find_slurm_offering

        offering = find_slurm_offering(
            [mock_offering],
            offering_uuid=mock_offering.waldur_offering_uuid,
        )
        assert offering == mock_offering

    def test_find_slurm_offering_by_prefix(self, mock_offering: Offering) -> None:
        """Test finding SLURM offering by account prefix."""
        from waldur_site_agent_slurm.diagnostic_cli import find_slurm_offering

        offering = find_slurm_offering(
            [mock_offering],
            account_name="alloc_test_project",
        )
        assert offering == mock_offering

    def test_find_slurm_offering_no_match(self) -> None:
        """Test finding SLURM offering when there's no match."""
        from waldur_site_agent_slurm.diagnostic_cli import find_slurm_offering

        # Create a non-SLURM offering
        non_slurm = Offering(
            name="Non-SLURM",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            waldur_offering_uuid="uuid",
            backend_type="moab",
            backend_settings={},
            backend_components={},
        )

        offering = find_slurm_offering([non_slurm])
        assert offering is None

    def test_format_tres_dict(self) -> None:
        """Test TRES dictionary formatting."""
        from waldur_site_agent_slurm.diagnostic_cli import format_tres_dict

        assert format_tres_dict(None) == "(none)"
        assert format_tres_dict({}) == "(none)"
        assert format_tres_dict({"cpu": "1000"}) == "cpu=1000"
        assert format_tres_dict({"cpu": "1000", "mem": "2000"}) == "cpu=1000,mem=2000"
