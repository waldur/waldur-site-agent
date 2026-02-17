"""Tests for _report_command_result_to_waldur handler."""

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from waldur_api_client.models.slurm_command_result_request import (
    SlurmCommandResultRequest,
)

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing.handlers import (
    _report_command_result_to_waldur,
)


@pytest.fixture
def offering():
    return structures.Offering(
        name="Test Offering",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="test-token",
        waldur_offering_uuid="11111111-1111-1111-1111-111111111111",
        backend_type="slurm",
        backend_settings={"periodic_limits": {"enabled": True}},
    )


@pytest.fixture
def message():
    return {
        "resource_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "policy_uuid": "11111111-2222-3333-4444-555555555555",
        "backend_id": "test-project",
        "offering_uuid": "11111111-1111-1111-1111-111111111111",
        "action": "apply_periodic_settings",
        "settings": {},
        "timestamp": "2026-01-30T00:00:00Z",
    }


class TestReportCommandResult:
    """Tests for _report_command_result_to_waldur."""

    @patch(
        "waldur_site_agent.event_processing.handlers"
        ".marketplace_slurm_periodic_usage_policies_report_command_result"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_sends_commands_executed(self, mock_get_client, mock_report, offering, message):
        """commands_executed from result are passed via additional_properties."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_report.sync_detailed.return_value = MagicMock(status_code=200)

        result = {
            "success": True,
            "error": "",
            "commands_executed": [
                "sacctmgr --parsable2 --noheader --immediate modify account test-project set fairshare=500",
                "sacctmgr --parsable2 --noheader --immediate modify account test-project set RawUsage=0",
            ],
        }

        _report_command_result_to_waldur(offering, message, result)

        mock_report.sync_detailed.assert_called_once()
        call_kwargs = mock_report.sync_detailed.call_args
        body = call_kwargs.kwargs["body"]

        assert body.resource_uuid == UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        assert body.success is True
        assert body["commands_executed"] == result["commands_executed"]

    @patch(
        "waldur_site_agent.event_processing.handlers"
        ".marketplace_slurm_periodic_usage_policies_report_command_result"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_sends_empty_commands_when_absent(self, mock_get_client, mock_report, offering, message):
        """When result has no commands_executed, sends empty list."""
        mock_get_client.return_value = MagicMock()
        mock_report.sync_detailed.return_value = MagicMock(status_code=200)

        result = {"success": True, "error": ""}

        _report_command_result_to_waldur(offering, message, result)

        body = mock_report.sync_detailed.call_args.kwargs["body"]
        assert body["commands_executed"] == []

    @patch(
        "waldur_site_agent.event_processing.handlers"
        ".marketplace_slurm_periodic_usage_policies_report_command_result"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_skips_when_no_policy_uuid(self, mock_get_client, mock_report, offering, message):
        """Does not call API when policy_uuid is missing."""
        message["policy_uuid"] = ""
        result = {"success": True}

        _report_command_result_to_waldur(offering, message, result)

        mock_report.sync_detailed.assert_not_called()

    @patch(
        "waldur_site_agent.event_processing.handlers"
        ".marketplace_slurm_periodic_usage_policies_report_command_result"
    )
    @patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
    def test_passes_failure_with_commands(self, mock_get_client, mock_report, offering, message):
        """On failure, sends error_message and partial commands_executed."""
        mock_get_client.return_value = MagicMock()
        mock_report.sync_detailed.return_value = MagicMock(status_code=200)

        result = {
            "success": False,
            "error": "SLURM down",
            "commands_executed": [
                "sacctmgr --parsable2 --noheader --immediate modify account test-project set fairshare=500",
            ],
        }

        _report_command_result_to_waldur(offering, message, result)

        body = mock_report.sync_detailed.call_args.kwargs["body"]
        assert body.success is False
        assert body.error_message == "SLURM down"
        assert len(body["commands_executed"]) == 1
