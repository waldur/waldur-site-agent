"""Test SLURM backend historical usage functionality with emulator."""

from datetime import datetime
from unittest.mock import patch

from waldur_site_agent_slurm.backend import SlurmBackend


class TestSlurmBackendHistorical:
    """Test SlurmBackend historical usage methods."""

    def test_get_historical_usage_report_basic(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test basic historical usage report from backend."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test January 2024 usage
        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 1)

        assert isinstance(usage_report, dict)
        assert "test_account_123" in usage_report

        account_usage = usage_report["test_account_123"]
        assert "TOTAL_ACCOUNT_USAGE" in account_usage
        assert "testuser1" in account_usage
        assert "testuser2" in account_usage

    def test_get_historical_usage_report_unit_conversion(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that SLURM units are correctly converted to Waldur units."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test January 2024 usage
        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 1)

        account_usage = usage_report["test_account_123"]
        user1_usage = account_usage["testuser1"]

        # Verify that usage values are converted
        assert isinstance(user1_usage, dict)

        # Check for expected component types (after conversion)
        expected_components = {"cpu", "mem", "gres/gpu"}
        found_components = set(user1_usage.keys())

        # At least some components should be present
        assert len(found_components.intersection(expected_components)) > 0

        # All values should be numeric
        for component, value in user1_usage.items():
            assert isinstance(value, (int, float))
            assert value >= 0

    def test_get_historical_usage_report_aggregation(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that usage is properly aggregated per user and total."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test February 2024 usage (has more usage)
        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 2)

        account_usage = usage_report["test_account_123"]

        # Get individual user usage
        user1_usage = account_usage.get("testuser1", {})
        user2_usage = account_usage.get("testuser2", {})
        total_usage = account_usage["TOTAL_ACCOUNT_USAGE"]

        # Verify total is sum of individual users
        for component in total_usage:
            user1_component = user1_usage.get(component, 0)
            user2_component = user2_usage.get(component, 0)
            expected_total = user1_component + user2_component

            # Allow for small floating point differences
            assert abs(total_usage[component] - expected_total) < 0.001

    def test_get_historical_usage_report_multiple_months(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test historical usage across multiple months."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        monthly_reports = {}
        for month in [1, 2, 3]:
            monthly_reports[month] = backend.get_historical_usage_report(
                ["test_account_123"], 2024, month
            )

        # All months should have data
        for month in [1, 2, 3]:
            assert "test_account_123" in monthly_reports[month]
            account_usage = monthly_reports[month]["test_account_123"]
            assert "TOTAL_ACCOUNT_USAGE" in account_usage

            # Should have some non-zero usage
            total_usage = account_usage["TOTAL_ACCOUNT_USAGE"]
            total_value = sum(total_usage.values())
            assert total_value > 0

    def test_get_historical_usage_report_empty_results(
        self, emulator_available, patched_slurm_client, mock_slurm_backend_config, mock_slurm_tres
    ):
        """Test historical usage report when no data is available."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test month with no data
        usage_report = backend.get_historical_usage_report(["test_account_123"], 2023, 12)

        # Should return empty dict or dict with no usage
        assert isinstance(usage_report, dict)
        if "test_account_123" in usage_report:
            account_usage = usage_report["test_account_123"]
            # If account exists, it should have empty or zero usage
            if "TOTAL_ACCOUNT_USAGE" in account_usage:
                total_value = sum(account_usage["TOTAL_ACCOUNT_USAGE"].values())
                assert total_value == 0

    def test_get_historical_usage_report_multiple_accounts(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test historical usage report with multiple accounts."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Test with multiple accounts
        usage_report = backend.get_historical_usage_report(
            ["test_account_123", "nonexistent_account"], 2024, 1
        )

        # Should have data for existing account
        assert "test_account_123" in usage_report

        # Non-existent account may or may not be in report, but if it is, should have no usage
        if "nonexistent_account" in usage_report:
            nonexistent_usage = usage_report["nonexistent_account"]
            if "TOTAL_ACCOUNT_USAGE" in nonexistent_usage:
                total_value = sum(nonexistent_usage["TOTAL_ACCOUNT_USAGE"].values())
                assert total_value == 0

    def test_historical_vs_current_usage_consistency(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
        time_engine,
    ):
        """Test that historical and current usage methods have consistent structure."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        # Set current time to March 2024
        time_engine.set_time(datetime(2024, 3, 15))

        # Get current usage (should be March)
        with patch(
            "waldur_site_agent.backend.utils.format_current_month",
            return_value=("2024-03-01T00:00:00", "2024-03-31T23:59:59"),
        ):
            current_usage = backend._get_usage_report(["test_account_123"])

        # Get historical usage for March 2024
        historical_usage = backend.get_historical_usage_report(["test_account_123"], 2024, 3)

        # Both should have the same structure
        assert isinstance(current_usage, dict)
        assert isinstance(historical_usage, dict)

        # Both should have the account
        if "test_account_123" in current_usage and "test_account_123" in historical_usage:
            current_account = current_usage["test_account_123"]
            historical_account = historical_usage["test_account_123"]

            # Both should have TOTAL_ACCOUNT_USAGE
            assert "TOTAL_ACCOUNT_USAGE" in current_account
            assert "TOTAL_ACCOUNT_USAGE" in historical_account

            # Both should have the same users
            current_users = set(current_account.keys()) - {"TOTAL_ACCOUNT_USAGE"}
            historical_users = set(historical_account.keys()) - {"TOTAL_ACCOUNT_USAGE"}
            assert current_users == historical_users

    def test_get_historical_usage_report_component_filtering(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that only configured components are included in the report."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 1)
        account_usage = usage_report["test_account_123"]
        total_usage = account_usage["TOTAL_ACCOUNT_USAGE"]

        # All components in the report should be in our backend configuration
        configured_components = set(mock_slurm_tres.keys())
        reported_components = set(total_usage.keys())

        # All reported components should be configured (subset)
        assert reported_components.issubset(configured_components)

    def test_get_historical_usage_report_data_types(
        self,
        emulator_available,
        patched_slurm_client,
        mock_slurm_backend_config,
        mock_slurm_tres,
        historical_usage_data,
    ):
        """Test that the historical usage report returns correct data types."""
        backend = SlurmBackend(mock_slurm_backend_config, mock_slurm_tres)

        usage_report = backend.get_historical_usage_report(["test_account_123"], 2024, 1)

        # Should be a dictionary
        assert isinstance(usage_report, dict)

        for account_name, account_usage in usage_report.items():
            assert isinstance(account_name, str)
            assert isinstance(account_usage, dict)

            for user_name, user_usage in account_usage.items():
                assert isinstance(user_name, str)
                assert isinstance(user_usage, dict)

                for component_name, component_value in user_usage.items():
                    assert isinstance(component_name, str)
                    assert isinstance(component_value, (int, float))
                    assert component_value >= 0  # Usage should never be negative
