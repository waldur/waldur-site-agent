"""Test historical usage loader command."""

from datetime import datetime
from unittest.mock import Mock, patch
from uuid import UUID

import pytest

from waldur_site_agent.common.historical_usage_loader import (
    _submit_resource_usage,
    _submit_user_usage,
    load_historical_usage_for_month,
    parse_date_range,
    validate_staff_user,
)

MODULE = "waldur_site_agent.common.historical_usage_loader"


class TestHistoricalUsageLoader:
    """Test the historical usage loader functionality."""

    def test_parse_date_range_valid(self):
        """Test parsing valid date ranges."""
        start_date, end_date = parse_date_range("2024-01-01", "2024-03-31")

        assert start_date.year == 2024
        assert start_date.month == 1
        assert start_date.day == 1

        assert end_date.year == 2024
        assert end_date.month == 3
        assert end_date.day == 31

    def test_parse_date_range_invalid_format(self):
        """Test parsing invalid date formats."""
        with pytest.raises(ValueError):
            parse_date_range("2024-13-01", "2024-03-31")  # Invalid month

        with pytest.raises(ValueError):
            parse_date_range("invalid-date", "2024-03-31")  # Invalid format

    def test_parse_date_range_end_before_start(self):
        """Test that end date before start date raises error."""
        with pytest.raises(ValueError, match="End date .* cannot be before start date"):
            parse_date_range("2024-03-31", "2024-01-01")

    def test_parse_date_range_too_large(self):
        """Test that very large date ranges are rejected."""
        with pytest.raises(SystemExit):  # Should exit with error
            parse_date_range("2020-01-01", "2030-12-31")  # 10+ years

    def test_validate_staff_user_success(self):
        """Test staff user validation with valid staff user."""
        mock_offering = Mock()
        mock_offering.api_url = "https://waldur.example.com/api/"
        mock_offering.verify_ssl = True

        mock_user = Mock()
        mock_user.is_staff = True
        mock_user.username = "admin@example.com"

        with (
            patch(f"{MODULE}.utils.get_client") as mock_get_client,
            patch(
                f"{MODULE}.utils.get_current_user_from_client",
                return_value=mock_user,
            ) as mock_get_user,
            patch(f"{MODULE}.utils.print_current_user") as mock_print_user,
        ):
            # Should not raise exception
            validate_staff_user("staff-token", mock_offering)

            mock_get_client.assert_called_once()
            mock_get_user.assert_called_once()
            mock_print_user.assert_called_once()

    def test_validate_staff_user_non_staff(self):
        """Test staff user validation with non-staff user."""
        mock_offering = Mock()
        mock_offering.api_url = "https://waldur.example.com/api/"
        mock_offering.verify_ssl = True

        mock_user = Mock()
        mock_user.is_staff = False
        mock_user.username = "regular@example.com"

        with (
            patch(f"{MODULE}.utils.get_client"),
            patch(
                f"{MODULE}.utils.get_current_user_from_client",
                return_value=mock_user,
            ),
            patch(f"{MODULE}.utils.print_current_user"),
        ):
            with pytest.raises(SystemExit):
                validate_staff_user("regular-token", mock_offering)

    def test_submit_resource_usage(self):
        """Test submitting resource-level usage data."""
        mock_client = Mock()
        mock_resource = Mock()
        mock_resource.name = "test_resource"
        mock_resource.uuid = UUID("12345678-1234-1234-1234-123456789abc")

        usage_data = {"cpu": 1500, "mem": 2048, "gres/gpu": 100}

        usage_date = datetime(2024, 1, 1)
        mock_offering = Mock()

        with patch(
            f"{MODULE}.marketplace_component_usages_set_usage"
        ) as mock_set_usage:
            _submit_resource_usage(
                mock_client, mock_resource, usage_data, usage_date, mock_offering
            )

            mock_set_usage.sync_detailed.assert_called_once()
            call_args = mock_set_usage.sync_detailed.call_args

            # Verify client was passed
            assert call_args[1]["client"] == mock_client

            # Verify request body structure
            request_body = call_args[1]["body"]
            assert request_body.resource == mock_resource.uuid
            assert request_body.date == usage_date
            assert len(request_body.usages) == 3  # cpu, mem, gres/gpu

    def test_submit_resource_usage_zero_usage(self):
        """Test submitting resource usage with zero values."""
        mock_client = Mock()
        mock_resource = Mock()
        mock_resource.name = "test_resource"

        usage_data = {"cpu": 0, "mem": 0, "gres/gpu": 0}

        usage_date = datetime(2024, 1, 1)
        mock_offering = Mock()

        with patch(
            f"{MODULE}.marketplace_component_usages_set_usage"
        ) as mock_set_usage:
            _submit_resource_usage(
                mock_client, mock_resource, usage_data, usage_date, mock_offering
            )

            # Should not call API for zero usage
            mock_set_usage.sync_detailed.assert_not_called()

    def test_submit_user_usage(self):
        """Test submitting user-level usage data."""
        mock_client = Mock()
        mock_resource = Mock()
        mock_resource.name = "test_resource"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "resource-uuid-hex"

        username = "testuser1"
        user_usage = {
            "cpu": 750,
            "mem": 1024,
        }

        # Mock component usages
        mock_component_usages = [
            Mock(type_="cpu", uuid=Mock()),
            Mock(type_="mem", uuid=Mock()),
        ]

        # Mock offering user
        mock_offering_user = Mock()
        mock_offering_user.url = "https://waldur.example.com/api/offering-users/1/"
        username_to_offering_user = {username: mock_offering_user}

        usage_date = datetime(2024, 1, 1)
        mock_offering = Mock()

        with (
            patch(
                f"{MODULE}.marketplace_component_usages_list.sync_all",
                return_value=mock_component_usages,
            ) as mock_pagination,
            patch(
                f"{MODULE}.marketplace_component_usages_set_user_usage"
            ) as mock_set_user_usage,
        ):
            _submit_user_usage(
                mock_client,
                mock_resource,
                username,
                user_usage,
                username_to_offering_user,
                usage_date,
                mock_offering,
            )

            # Should call pagination to get component usages
            mock_pagination.assert_called_once()

            # Should call set_user_usage for each component
            assert mock_set_user_usage.sync_detailed.call_count == 2

    def test_submit_user_usage_no_offering_user(self):
        """Test submitting user usage when offering user not found."""
        mock_client = Mock()
        mock_resource = Mock()
        mock_resource.name = "test_resource"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "resource-uuid-hex"

        username = "unknown_user"
        user_usage = {"cpu": 750}

        mock_component_usages = [Mock(type_="cpu", uuid=Mock())]
        username_to_offering_user = {}  # Empty mapping

        usage_date = datetime(2024, 1, 1)
        mock_offering = Mock()

        with (
            patch(
                f"{MODULE}.marketplace_component_usages_list.sync_all",
                return_value=mock_component_usages,
            ),
            patch(
                f"{MODULE}.marketplace_component_usages_set_user_usage"
            ) as mock_set_user_usage,
        ):
            _submit_user_usage(
                mock_client,
                mock_resource,
                username,
                user_usage,
                username_to_offering_user,
                usage_date,
                mock_offering,
            )

            # Should still call API but without user URL
            mock_set_user_usage.sync_detailed.assert_called_once()
            call_args = mock_set_user_usage.sync_detailed.call_args
            request_body = call_args[1]["body"]

            assert request_body.username == username

    def test_load_historical_usage_for_month_integration(self):
        """Test the complete monthly usage loading workflow."""
        # Mock offering
        mock_offering = Mock()
        mock_offering.api_url = "https://waldur.example.com/api/"
        mock_offering.uuid = "offering-uuid"
        mock_offering.verify_ssl = True

        # Mock Waldur resources
        mock_resource = Mock()
        mock_resource.name = "test_resource"
        mock_resource.backend_id = "test_account_123"
        mock_resource.uuid = Mock()

        # Mock backend
        mock_backend = Mock()
        mock_backend.get_usage_report_for_period.return_value = {
            "test_account_123": {
                "TOTAL_ACCOUNT_USAGE": {"cpu": 1500, "mem": 2048},
                "testuser1": {"cpu": 750, "mem": 1024},
                "testuser2": {"cpu": 750, "mem": 1024},
            }
        }

        user_token = "staff-token"
        year, month = 2024, 1

        with (
            patch(f"{MODULE}.utils.get_client") as mock_get_client,
            patch(
                f"{MODULE}.marketplace_provider_resources_list.sync_all",
                return_value=[mock_resource],
            ),
            patch(
                f"{MODULE}.marketplace_offering_users_list.sync_all",
                return_value=[],
            ),
            patch(
                f"{MODULE}.marketplace_component_usages_list.sync_all",
                return_value=[],
            ),
            patch(
                f"{MODULE}.utils.get_backend_for_offering",
                return_value=(mock_backend, "1.0.0"),
            ),
            patch(f"{MODULE}._submit_resource_usage") as mock_submit_resource,
            patch(f"{MODULE}._submit_user_usage") as mock_submit_user,
        ):
            # Should not raise exception
            load_historical_usage_for_month(mock_offering, user_token, year, month, 1, 1)

            # Verify backend was called correctly
            mock_backend.get_usage_report_for_period.assert_called_once_with(
                ["test_account_123"], year, month
            )

            # Verify resource usage submission
            mock_submit_resource.assert_called_once()

            # Verify user usage submission (called for each user)
            assert mock_submit_user.call_count == 2

    def test_load_historical_usage_for_month_no_resources(self):
        """Test monthly loading when no resources are found."""
        mock_offering = Mock()
        mock_offering.uuid = "offering-uuid"

        with (
            patch(f"{MODULE}.utils.get_client"),
            patch(
                f"{MODULE}.marketplace_provider_resources_list.sync_all",
                return_value=[],
            ),
            patch(
                f"{MODULE}.utils.get_backend_for_offering",
                return_value=(Mock(), "1.0.0"),
            ),
        ):
            # Should complete without error and log that no resources were found
            load_historical_usage_for_month(mock_offering, "staff-token", 2024, 1, 1, 1)

    def test_load_historical_usage_for_month_empty_report(self):
        """Test monthly loading when backend returns empty usage report."""
        mock_offering = Mock()
        mock_offering.uuid = "offering-uuid"

        mock_resource = Mock()
        mock_resource.backend_id = "test_account"

        mock_backend = Mock()
        mock_backend.get_usage_report_for_period.return_value = {}

        with (
            patch(f"{MODULE}.utils.get_client"),
            patch(
                f"{MODULE}.marketplace_provider_resources_list.sync_all",
                return_value=[mock_resource],
            ),
            patch(
                f"{MODULE}.marketplace_offering_users_list.sync_all",
                return_value=[],
            ),
            patch(
                f"{MODULE}.utils.get_backend_for_offering",
                return_value=(mock_backend, "1.0.0"),
            ),
        ):
            # Should complete without error
            load_historical_usage_for_month(mock_offering, "staff-token", 2024, 1, 1, 1)

    def test_load_historical_usage_skip_user_usage(self):
        """Test that --skip-user-usage prevents per-user submission."""
        mock_offering = Mock()
        mock_offering.api_url = "https://waldur.example.com/api/"
        mock_offering.uuid = "offering-uuid"
        mock_offering.verify_ssl = True

        mock_resource = Mock()
        mock_resource.name = "test_resource"
        mock_resource.backend_id = "test_account_123"
        mock_resource.uuid = Mock()

        mock_backend = Mock()
        mock_backend.get_usage_report_for_period.return_value = {
            "test_account_123": {
                "TOTAL_ACCOUNT_USAGE": {"cpu": 1500},
                "testuser1": {"cpu": 750},
                "testuser2": {"cpu": 750},
            }
        }

        with (
            patch(f"{MODULE}.utils.get_client"),
            patch(
                f"{MODULE}.marketplace_provider_resources_list.sync_all",
                return_value=[mock_resource],
            ),
            patch(
                f"{MODULE}.marketplace_offering_users_list.sync_all",
            ) as mock_offering_users_list,
            patch(
                f"{MODULE}.utils.get_backend_for_offering",
                return_value=(mock_backend, "1.0.0"),
            ),
            patch(f"{MODULE}._submit_resource_usage") as mock_submit_resource,
            patch(f"{MODULE}._submit_user_usage") as mock_submit_user,
        ):
            load_historical_usage_for_month(
                mock_offering, "staff-token", 2024, 1, 1, 1, skip_user_usage=True
            )

            # Resource usage should still be submitted
            mock_submit_resource.assert_called_once()

            # User usage should NOT be submitted
            mock_submit_user.assert_not_called()

            # Offering users list should NOT be called
            mock_offering_users_list.assert_not_called()

    def test_no_staff_check_skips_validation(self):
        """Test that --no-staff-check skips validate_staff_user call."""
        mock_offering = Mock()
        mock_offering.api_url = "https://waldur.example.com/api/"
        mock_offering.uuid = "test-uuid"
        mock_offering.name = "Test Offering"
        mock_offering.verify_ssl = True

        mock_config = Mock()
        mock_config.waldur_offerings = [mock_offering]

        with (
            patch(f"{MODULE}.utils.load_configuration", return_value=mock_config),
            patch(f"{MODULE}.find_offering_by_uuid", return_value=mock_offering),
            patch(f"{MODULE}.validate_staff_user") as mock_validate,
            patch(f"{MODULE}.parse_date_range", return_value=(Mock(), Mock())),
            patch(f"{MODULE}.backend_utils.generate_monthly_periods", return_value=[]),
        ):
            # Simulate args with no_staff_check=True
            with patch(
                "argparse.ArgumentParser.parse_args",
                return_value=Mock(
                    config="config.yaml",
                    offering_uuid="test-uuid",
                    user_token="token",
                    start_date="2024-01-01",
                    end_date="2024-03-31",
                    skip_user_usage=False,
                    no_staff_check=True,
                ),
            ):
                main()

            mock_validate.assert_not_called()


# Import main for the no_staff_check test
from waldur_site_agent.common.historical_usage_loader import main  # noqa: E402
