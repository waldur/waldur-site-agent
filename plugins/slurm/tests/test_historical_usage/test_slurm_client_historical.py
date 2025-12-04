"""Test SLURM client historical usage functionality with emulator."""

from datetime import datetime
from unittest.mock import patch

import pytest
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.parser import SlurmReportLine


class TestSlurmClientHistorical:
    """Test SlurmClient historical usage methods."""

    def test_get_historical_usage_report_basic(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test basic historical usage report functionality."""
        client = SlurmClient(mock_slurm_tres)

        # Test January 2024 usage
        usage_lines = client.get_historical_usage_report(["test_account_123"], 2024, 1)

        assert len(usage_lines) > 0
        assert all(isinstance(line, SlurmReportLine) for line in usage_lines)

        # Verify account filtering
        for line in usage_lines:
            assert line.account == "test_account_123"

    def test_get_historical_usage_report_multiple_months(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test historical usage report across multiple months."""
        client = SlurmClient(mock_slurm_tres)

        # Test each month
        months_data = [
            (2024, 1, historical_usage_data["2024-01"]),
            (2024, 2, historical_usage_data["2024-02"]),
            (2024, 3, historical_usage_data["2024-03"]),
        ]

        for year, month, expected_data in months_data:
            usage_lines = client.get_historical_usage_report(["test_account_123"], year, month)

            # Verify we get data for this month
            assert len(usage_lines) > 0

            # Verify users are present
            users_found = {line.user for line in usage_lines}
            expected_users = set(expected_data.keys()) - {"total"}
            assert users_found.issuperset(expected_users)

    def test_get_historical_usage_report_empty_month(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test historical usage report for month with no data."""
        client = SlurmClient(mock_slurm_tres)

        # Test December 2023 (before our test data)
        usage_lines = client.get_historical_usage_report(["test_account_123"], 2023, 12)

        # Should return empty list for months with no data
        assert len(usage_lines) == 0

    def test_get_historical_usage_report_nonexistent_account(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test historical usage report for non-existent account."""
        client = SlurmClient(mock_slurm_tres)

        # Test with non-existent account
        usage_lines = client.get_historical_usage_report(["nonexistent_account"], 2024, 1)

        # Should return empty list for non-existent accounts
        assert len(usage_lines) == 0

    def test_get_historical_usage_report_tres_data(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test that historical usage report includes correct TRES data."""
        client = SlurmClient(mock_slurm_tres)

        # Test January 2024 usage
        usage_lines = client.get_historical_usage_report(["test_account_123"], 2024, 1)

        # Find testuser1's record
        user1_lines = [line for line in usage_lines if line.user == "testuser1"]
        assert len(user1_lines) > 0

        user1_line = user1_lines[0]

        # Verify TRES usage data is present and correct
        assert "cpu" in user1_line.tres_usage or "CPU" in user1_line.tres_usage
        assert "mem" in user1_line.tres_usage or "Mem" in user1_line.tres_usage

        # Verify some usage values are non-zero
        total_usage = sum(user1_line.tres_usage.values())
        assert total_usage > 0

    def test_get_historical_usage_report_date_filtering(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test that date filtering works correctly."""
        client = SlurmClient(mock_slurm_tres)

        # Test that February data is different from January data
        jan_lines = client.get_historical_usage_report(["test_account_123"], 2024, 1)
        feb_lines = client.get_historical_usage_report(["test_account_123"], 2024, 2)

        # Should have data for both months
        assert len(jan_lines) > 0
        assert len(feb_lines) > 0

        # Calculate total usage for each month
        jan_total = sum(sum(line.tres_usage.values()) for line in jan_lines)
        feb_total = sum(sum(line.tres_usage.values()) for line in feb_lines)

        # February has more usage than January in our test data
        assert feb_total > jan_total

    def test_get_historical_usage_report_multiple_accounts(
        self, emulator_available, patched_slurm_client, mock_slurm_tres, historical_usage_data
    ):
        """Test historical usage report with multiple accounts."""
        client = SlurmClient(mock_slurm_tres)

        # Test with multiple accounts (one exists, one doesn't)
        usage_lines = client.get_historical_usage_report(
            ["test_account_123", "another_account"], 2024, 1
        )

        # Should only get data for the existing account
        accounts_found = {line.account for line in usage_lines}
        assert "test_account_123" in accounts_found
        assert "another_account" not in accounts_found

    def test_historical_vs_current_usage_methods_consistency(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_tres,
        historical_usage_data,
        time_engine,
    ):
        """Test that historical and current usage methods are consistent."""
        client = SlurmClient(mock_slurm_tres)

        # Set current time to March 2024
        time_engine.set_time(datetime(2024, 3, 15))

        # Get current month usage (should be March)
        with patch(
            "waldur_site_agent.backend.utils.format_current_month",
            return_value=("2024-03-01T00:00:00", "2024-03-31T23:59:59"),
        ):
            current_usage = client.get_usage_report(["test_account_123"])

        # Get historical usage for March 2024
        historical_usage = client.get_historical_usage_report(["test_account_123"], 2024, 3)

        # Should have similar structure (both should contain the same users)
        current_users = {line.user for line in current_usage}
        historical_users = {line.user for line in historical_usage}

        assert current_users == historical_users

        # Both should return SlurmReportLine objects
        assert all(isinstance(line, SlurmReportLine) for line in current_usage)
        assert all(isinstance(line, SlurmReportLine) for line in historical_usage)

    def test_get_historical_usage_report_edge_cases(
        self, emulator_available, patched_slurm_client, mock_slurm_tres
    ):
        """Test edge cases for historical usage reports."""
        client = SlurmClient(mock_slurm_tres)

        # Test invalid month - should raise ValueError for invalid month
        with pytest.raises(ValueError, match="month must be in"):
            client.get_historical_usage_report(["test_account_123"], 2024, 13)

        # Test invalid month 0 - should also raise ValueError
        with pytest.raises(ValueError, match="month must be in"):
            client.get_historical_usage_report(["test_account_123"], 2024, 0)

        # Test very old year (should return empty, not raise error)
        usage_lines = client.get_historical_usage_report(["test_account_123"], 1999, 1)
        assert len(usage_lines) == 0

        # Test empty account list
        usage_lines = client.get_historical_usage_report([], 2024, 1)
        assert len(usage_lines) == 0
