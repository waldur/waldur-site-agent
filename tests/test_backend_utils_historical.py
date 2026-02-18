"""Test backend utils historical functionality."""

from datetime import datetime

from waldur_site_agent.backend.utils import (
    format_month_period,
    generate_monthly_periods,
)


class TestBackendUtilsHistorical:
    """Test backend utility functions for historical usage."""

    def test_format_month_period_january(self):
        """Test formatting January month period."""
        start, end = format_month_period(2024, 1)

        assert start == "2024-01-01T00:00:00"
        assert end == "2024-01-31T23:59:59"

    def test_format_month_period_february_leap_year(self):
        """Test formatting February in leap year."""
        start, end = format_month_period(2024, 2)

        assert start == "2024-02-01T00:00:00"
        assert end == "2024-02-29T23:59:59"  # 2024 is a leap year

    def test_format_month_period_february_non_leap_year(self):
        """Test formatting February in non-leap year."""
        start, end = format_month_period(2023, 2)

        assert start == "2023-02-01T00:00:00"
        assert end == "2023-02-28T23:59:59"  # 2023 is not a leap year

    def test_format_month_period_december(self):
        """Test formatting December month period."""
        start, end = format_month_period(2024, 12)

        assert start == "2024-12-01T00:00:00"
        assert end == "2024-12-31T23:59:59"

    def test_generate_monthly_periods_single_month(self):
        """Test generating periods for single month."""
        periods = generate_monthly_periods(2024, 1, 2024, 1)

        assert len(periods) == 1
        year, month, start, end = periods[0]

        assert year == 2024
        assert month == 1
        assert start == "2024-01-01T00:00:00"
        assert end == "2024-01-31T23:59:59"

    def test_generate_monthly_periods_same_year(self):
        """Test generating periods within same year."""
        periods = generate_monthly_periods(2024, 1, 2024, 3)

        assert len(periods) == 3

        # Check January
        year, month, start, end = periods[0]
        assert year == 2024 and month == 1
        assert start == "2024-01-01T00:00:00"
        assert end == "2024-01-31T23:59:59"

        # Check February
        year, month, start, end = periods[1]
        assert year == 2024 and month == 2
        assert start == "2024-02-01T00:00:00"
        assert end == "2024-02-29T23:59:59"  # 2024 is leap year

        # Check March
        year, month, start, end = periods[2]
        assert year == 2024 and month == 3
        assert start == "2024-03-01T00:00:00"
        assert end == "2024-03-31T23:59:59"

    def test_generate_monthly_periods_cross_year(self):
        """Test generating periods across year boundary."""
        periods = generate_monthly_periods(2023, 11, 2024, 2)

        assert len(periods) == 4

        # Check November 2023
        year, month, start, end = periods[0]
        assert year == 2023 and month == 11
        assert start == "2023-11-01T00:00:00"
        assert end == "2023-11-30T23:59:59"

        # Check December 2023
        year, month, start, end = periods[1]
        assert year == 2023 and month == 12
        assert start == "2023-12-01T00:00:00"
        assert end == "2023-12-31T23:59:59"

        # Check January 2024
        year, month, start, end = periods[2]
        assert year == 2024 and month == 1
        assert start == "2024-01-01T00:00:00"
        assert end == "2024-01-31T23:59:59"

        # Check February 2024
        year, month, start, end = periods[3]
        assert year == 2024 and month == 2
        assert start == "2024-02-01T00:00:00"
        assert end == "2024-02-29T23:59:59"  # 2024 is leap year

    def test_generate_monthly_periods_full_year(self):
        """Test generating periods for full year."""
        periods = generate_monthly_periods(2024, 1, 2024, 12)

        assert len(periods) == 12

        # Check that all months are included
        months = [period[1] for period in periods]
        assert months == list(range(1, 13))

        # Check that all years are 2024
        years = [period[0] for period in periods]
        assert all(year == 2024 for year in years)

    def test_generate_monthly_periods_multiple_years(self):
        """Test generating periods across multiple years."""
        periods = generate_monthly_periods(2023, 6, 2025, 3)

        # Should have: 7 months in 2023 (Jun-Dec) + 12 months in 2024 + 3 months in 2025 (Jan-Mar)
        expected_count = 7 + 12 + 3
        assert len(periods) == expected_count

        # Check first period (June 2023)
        year, month, start, end = periods[0]
        assert year == 2023 and month == 6

        # Check last period (March 2025)
        year, month, start, end = periods[-1]
        assert year == 2025 and month == 3

        # Verify continuous sequence
        for i, (year, month, _, _) in enumerate(periods):
            if i == 0:
                continue
            prev_year, prev_month, _, _ = periods[i - 1]

            if month == 1:
                # Year boundary
                assert year == prev_year + 1
                assert prev_month == 12
            else:
                # Same year
                assert year == prev_year
                assert month == prev_month + 1

    def test_generate_monthly_periods_edge_cases(self):
        """Test edge cases for monthly period generation."""
        # Same month and year
        periods = generate_monthly_periods(2024, 6, 2024, 6)
        assert len(periods) == 1

        # December to January
        periods = generate_monthly_periods(2023, 12, 2024, 1)
        assert len(periods) == 2

        year, month, start, end = periods[0]
        assert year == 2023 and month == 12

        year, month, start, end = periods[1]
        assert year == 2024 and month == 1

    def test_monthly_periods_date_format_consistency(self):
        """Test that all generated periods have consistent date formats."""
        periods = generate_monthly_periods(2024, 1, 2024, 12)

        for year, month, start_str, end_str in periods:
            # Verify start date format
            assert start_str.endswith("T00:00:00")
            assert start_str.startswith(f"{year:04d}-{month:02d}-01")

            # Verify end date format
            assert end_str.endswith("T23:59:59")

            # Verify dates can be parsed
            start_dt = datetime.fromisoformat(start_str)
            end_dt = datetime.fromisoformat(end_str)

            assert start_dt.year == year
            assert start_dt.month == month
            assert start_dt.day == 1
            assert start_dt.hour == 0
            assert start_dt.minute == 0
            assert start_dt.second == 0

            assert end_dt.year == year
            assert end_dt.month == month
            assert end_dt.hour == 23
            assert end_dt.minute == 59
            assert end_dt.second == 59

            # End day should be last day of month
            if month == 2:
                # February - check leap year logic
                if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                    assert end_dt.day == 29  # Leap year
                else:
                    assert end_dt.day == 28  # Non-leap year
            elif month in [4, 6, 9, 11]:
                assert end_dt.day == 30  # 30-day months
            else:
                assert end_dt.day == 31  # 31-day months
