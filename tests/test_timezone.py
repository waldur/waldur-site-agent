import datetime
import unittest
from unittest import mock
from zoneinfo import ZoneInfo

from freezegun import freeze_time

from waldur_site_agent.backends.utils import get_current_time_in_timezone, format_current_month
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.common.processors import OfferingReportProcessor
from waldur_site_agent.backends import BackendType
from waldur_site_agent.common import utils
from tests.fixtures import OFFERING


class TimezoneUtilsTest(unittest.TestCase):
    """Test timezone utility functions."""

    def test_get_current_time_in_timezone_utc(self):
        """Test getting current time in UTC timezone."""
        utc_time = get_current_time_in_timezone("UTC")

        self.assertEqual(utc_time.tzinfo, ZoneInfo("UTC"))
        self.assertIsInstance(utc_time, datetime.datetime)

    def test_get_current_time_in_timezone_europe_tallinn(self):
        """Test getting current time in Europe/Tallinn timezone."""
        tallinn_time = get_current_time_in_timezone("Europe/Tallinn")

        self.assertEqual(tallinn_time.tzinfo, ZoneInfo("Europe/Tallinn"))
        self.assertIsInstance(tallinn_time, datetime.datetime)

    def test_get_current_time_in_timezone_empty_string(self):
        """Test getting current time with empty timezone (system default)."""
        system_time = get_current_time_in_timezone("")
        self.assertIsNone(system_time.tzinfo)
        self.assertIsInstance(system_time, datetime.datetime)

    def test_get_current_time_in_timezone_invalid(self):
        """Test getting current time with invalid timezone (should fallback)."""
        invalid_time = get_current_time_in_timezone("Invalid/Timezone")
        self.assertIsNone(invalid_time.tzinfo)
        native_time = datetime.datetime.now()

        time_diff = abs((invalid_time - native_time).total_seconds())
        self.assertLess(time_diff, 1.0)
        self.assertIsInstance(invalid_time, datetime.datetime)

    def test_format_current_month_europe_tallinn(self):
        """Test formatting current month with Europe/Tallinn timezone."""
        start, end = format_current_month("Europe/Tallinn")

        self.assertIsInstance(start, str)
        self.assertIsInstance(end, str)
        self.assertTrue(start.endswith("T00:00:00"))
        self.assertTrue(end.endswith("T23:59:59"))

    def test_format_current_month_empty_string(self):
        """Test formatting current month with empty timezone (system default)."""
        start, end = format_current_month("")

        self.assertIsInstance(start, str)
        self.assertIsInstance(end, str)
        self.assertTrue(start.endswith("T00:00:00"))
        self.assertTrue(end.endswith("T23:59:59"))


@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
@mock.patch("waldur_site_agent.common.processors.utils.print_current_user")
class ProcessorTimezoneTest(unittest.TestCase):
    """Test processor timezone functionality."""

    def test_processor_constructor_timezone(self, mock_print_user, waldur_client_class: mock.Mock):
        """Test that processor constructor accepts timezone parameter."""
        offering = Offering(
            name="test",
            api_url="http://test.com",
            api_token="token",
            uuid="uuid",
            backend_type="slurm",
            backend_settings={},
            backend_components={},
        )

        processor = OfferingReportProcessor(offering, "test-agent", "UTC")
        self.assertEqual(processor.timezone, "UTC")

    def test_processor_constructor_timezone_empty(
        self, mock_print_user, waldur_client_class: mock.Mock
    ):
        """Test that processor constructor accepts empty timezone."""
        offering = Offering(
            name="test",
            api_url="http://test.com",
            api_token="token",
            uuid="uuid",
            backend_type="slurm",
            backend_settings={},
            backend_components={},
        )

        # Mock the WaldurClient methods that are called during initialization
        waldur_client = waldur_client_class.return_value
        waldur_client.get_current_user.return_value = {"username": "test"}
        waldur_client.get_marketplace_provider_offering.return_value = {"components": []}

        processor = OfferingReportProcessor(offering, "test-agent", "")
        self.assertEqual(processor.timezone, "")


@freeze_time("2025-07-01 00:30:00")
class TimezoneMonthBoundaryTest(unittest.TestCase):
    def test_timezone_differences(self):
        """Test that different timezones give different times."""
        utc_time = get_current_time_in_timezone("UTC")
        tallinn_time = get_current_time_in_timezone("Europe/Tallinn")

        self.assertNotEqual(utc_time.tzinfo, tallinn_time.tzinfo)

    def test_month_boundary_edge_case(self):
        """Test edge case around month boundaries."""

        utc_start, _ = format_current_month("UTC")
        tallinn_start, _ = format_current_month("Europe/Tallinn")

        self.assertEqual(len(utc_start), 19)
        self.assertEqual(len(tallinn_start), 19)

        self.assertTrue(utc_start.endswith("T00:00:00"))
        self.assertTrue(tallinn_start.endswith("T00:00:00"))


@mock.patch("waldur_site_agent.common.processors.WaldurClient", autospec=True)
@mock.patch.object(utils.SlurmBackend, "_pull_backend_resource")
class TimezoneIntegrationTest(unittest.TestCase):
    """Integration tests for timezone functionality with mocked dependencies."""

    def setUp(self) -> None:
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "state": "OK",
        }
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ]
        }
        self.offering = OFFERING

    def test_processor_uses_timezone_for_billing_period(
        self, mock_pull_backend_resource, waldur_client_class: mock.Mock
    ):
        """Test that processor uses configured timezone for billing period calculation."""
        mock_pull_backend_resource.return_value = None

        processor = OfferingReportProcessor(self.offering, "test-agent", "UTC")
        waldur_client = waldur_client_class.return_value

        waldur_client.filter_marketplace_provider_resources.return_value = [self.waldur_resource]
        waldur_client.get_marketplace_provider_resource.return_value = self.waldur_resource
        waldur_client.get_marketplace_provider_offering.return_value = self.waldur_offering

        self.assertEqual(processor.timezone, "UTC")


if __name__ == "__main__":
    unittest.main()
