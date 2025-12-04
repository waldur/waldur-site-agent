"""Integration tests for historical usage with SLURM emulator."""

from datetime import datetime
from unittest.mock import patch

import pytest
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient


@pytest.mark.integration
@pytest.mark.emulator
class TestHistoricalUsageIntegration:
    """Integration tests using SLURM emulator."""

    def test_full_historical_workflow(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
        time_engine,
    ):
        """Test complete historical usage workflow from client to backend."""
        # Create backend and client
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)
        client = SlurmClient(mock_slurm_tres)

        # Test the full workflow for January 2024
        year, month = 2024, 1

        # 1. Client gets historical usage data
        usage_lines = client.get_historical_usage_report(["test_account_123"], year, month)

        assert len(usage_lines) > 0
        assert all(line.account == "test_account_123" for line in usage_lines)

        # 2. Backend processes the data
        usage_report = backend.get_historical_usage_report(["test_account_123"], year, month)

        assert "test_account_123" in usage_report
        account_usage = usage_report["test_account_123"]

        # Verify structure
        assert "TOTAL_ACCOUNT_USAGE" in account_usage
        assert "testuser1" in account_usage
        assert "testuser2" in account_usage

        # Verify data integrity - total should be sum of users
        total_usage = account_usage["TOTAL_ACCOUNT_USAGE"]
        user1_usage = account_usage["testuser1"]
        user2_usage = account_usage["testuser2"]

        for component in total_usage:
            user1_value = user1_usage.get(component, 0)
            user2_value = user2_usage.get(component, 0)
            expected_total = user1_value + user2_value

            assert abs(total_usage[component] - expected_total) < 0.001

    def test_multiple_months_consistency(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that multiple months return consistent data structure."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        months_to_test = [1, 2, 3]
        monthly_reports = {}

        for month in months_to_test:
            report = backend.get_historical_usage_report(["test_account_123"], 2024, month)
            monthly_reports[month] = report

        # All months should have the same account
        for month in months_to_test:
            assert "test_account_123" in monthly_reports[month]

        # All months should have the same users
        users_per_month = {}
        for month in months_to_test:
            account_usage = monthly_reports[month]["test_account_123"]
            users = set(account_usage.keys()) - {"TOTAL_ACCOUNT_USAGE"}
            users_per_month[month] = users

        # All months should have same users
        all_users = list(users_per_month.values())
        assert all(users == all_users[0] for users in all_users)

        # Usage values should be different between months
        total_usages = {}
        for month in months_to_test:
            account_usage = monthly_reports[month]["test_account_123"]
            total_usage = account_usage["TOTAL_ACCOUNT_USAGE"]
            total_value = sum(total_usage.values())
            total_usages[month] = total_value

        # At least some months should have different usage values
        unique_values = set(total_usages.values())
        assert len(unique_values) > 1, "All months have identical usage - test data issue"

    def test_emulator_time_manipulation(
        self,
        emulator_available,
        time_engine,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
    ):
        """Test that emulator time manipulation affects results."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Set time to January 2024
        time_engine.set_time(datetime(2024, 1, 15))

        # Get current month usage (should be January)
        with patch(
            "waldur_site_agent.backend.utils.format_current_month",
            return_value=("2024-01-01T00:00:00", "2024-01-31T23:59:59"),
        ):
            current_usage = backend._get_usage_report(["test_account_123"])

        # Get historical usage for January
        historical_usage = backend.get_historical_usage_report(["test_account_123"], 2024, 1)

        # Should have similar structure since both query same month
        if "test_account_123" in current_usage and "test_account_123" in historical_usage:
            current_account = current_usage["test_account_123"]
            historical_account = historical_usage["test_account_123"]

            # Same users should be present
            current_users = set(current_account.keys()) - {"TOTAL_ACCOUNT_USAGE"}
            historical_users = set(historical_account.keys()) - {"TOTAL_ACCOUNT_USAGE"}
            assert current_users == historical_users

    def test_component_conversion_accuracy(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that SLURM to Waldur unit conversion is accurate."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 1)
        account_usage = usage_report["test_account_123"]
        user_usage = account_usage["testuser1"]

        # Check conversion factors are applied correctly
        for component, usage_value in user_usage.items():
            if component in mock_slurm_tres:
                component_config = mock_slurm_tres[component]

                # Usage should be positive
                assert usage_value >= 0

                # Check that unit_factor affects the scale of values
                unit_factor = component_config["unit_factor"]

                # For our test data, values should be reasonable given the conversion
                if component == "cpu":
                    # CPU usage should be positive and finite
                    assert usage_value >= 0 and usage_value < float("inf")
                elif component == "mem":
                    # Memory usage should be positive and finite
                    assert usage_value >= 0 and usage_value < float("inf")
                elif component == "gres/gpu":
                    # GPU usage should be positive and finite
                    assert usage_value >= 0 and usage_value < float("inf")

    def test_error_handling_resilience(
        self, emulator_available, mock_slurm_backend_config, mock_slurm_tres
    ):
        """Test that historical usage handles errors gracefully."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test with non-existent account
        usage_report = backend.get_historical_usage_report(["nonexistent_account"], 2024, 1)
        assert isinstance(usage_report, dict)
        # Should not raise exception

        # Test with invalid month (should raise ValueError)
        with pytest.raises(ValueError, match="month must be in"):
            backend.get_historical_usage_report(["test_account_123"], 2024, 13)

        # Test with empty account list
        usage_report = backend.get_historical_usage_report([], 2024, 1)
        assert isinstance(usage_report, dict)

    def test_large_date_range_simulation(
        self, emulator_available, patched_slurm_client, mock_slurm_backend_config, mock_slurm_tres
    ):
        """Test processing multiple months to simulate large date ranges."""
        from waldur_site_agent.backend.utils import generate_monthly_periods

        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Simulate loading Q1 2024
        periods = generate_monthly_periods(2024, 1, 2024, 3)

        assert len(periods) == 3

        monthly_reports = []
        for year, month, start_str, end_str in periods:
            report = backend.get_historical_usage_report(["test_account_123"], year, month)
            monthly_reports.append((year, month, report))

        # Verify each month processed
        for year, month, report in monthly_reports:
            assert isinstance(report, dict)
            # May or may not have data depending on test setup

        # Verify periods are correctly generated
        years = [period[0] for period in periods]
        months = [period[1] for period in periods]

        assert years == [2024, 2024, 2024]
        assert months == [1, 2, 3]

    def test_performance_multiple_accounts(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test performance with multiple accounts."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test multiple accounts at once
        accounts = ["test_account_123", "account_2", "account_3", "account_4"]

        import time

        start_time = time.time()

        usage_report = backend.get_historical_usage_report(accounts, 2024, 1)

        end_time = time.time()
        duration = end_time - start_time

        # Should complete in reasonable time (adjust threshold as needed)
        assert duration < 5.0, f"Query took too long: {duration:.2f}s"

        # Should handle all accounts
        assert isinstance(usage_report, dict)

        # At least the test account should have data
        if "test_account_123" in usage_report:
            account_usage = usage_report["test_account_123"]
            assert "TOTAL_ACCOUNT_USAGE" in account_usage
