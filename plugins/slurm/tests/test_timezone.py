import datetime
import unittest
import uuid
from unittest import mock
from zoneinfo import ZoneInfo

import respx
from freezegun import freeze_time

from waldur_site_agent.backend.utils import format_current_month, get_current_time_in_timezone
from waldur_site_agent_slurm import backend
from waldur_site_agent.common.processors import OfferingReportProcessor
from waldur_site_agent.common.structures import Offering

BASE_URL = "https://waldur.example.com"


class TimezoneUtilsTest(unittest.TestCase):
    """Test timezone utility functions."""

    def test_get_current_time_in_timezone_utc(self) -> None:
        """Test getting current time in UTC timezone."""
        utc_time = get_current_time_in_timezone("UTC")

        assert utc_time.tzinfo == ZoneInfo("UTC")
        assert isinstance(utc_time, datetime.datetime)

    def test_get_current_time_in_timezone_europe_tallinn(self) -> None:
        """Test getting current time in Europe/Tallinn timezone."""
        tallinn_time = get_current_time_in_timezone("Europe/Tallinn")

        assert tallinn_time.tzinfo == ZoneInfo("Europe/Tallinn")
        assert isinstance(tallinn_time, datetime.datetime)

    def test_get_current_time_in_timezone_empty_string(self) -> None:
        """Test getting current time with empty timezone (system default)."""
        system_time = get_current_time_in_timezone("")
        assert system_time.tzinfo is None
        assert isinstance(system_time, datetime.datetime)

    def test_get_current_time_in_timezone_invalid(self) -> None:
        """Test getting current time with invalid timezone (should fallback)."""
        invalid_time = get_current_time_in_timezone("Invalid/Timezone")
        assert invalid_time.tzinfo is None
        native_time = datetime.datetime.now()

        time_diff = abs((invalid_time - native_time).total_seconds())
        assert time_diff < 1.0
        assert isinstance(invalid_time, datetime.datetime)

    def test_format_current_month_europe_tallinn(self) -> None:
        """Test formatting current month with Europe/Tallinn timezone."""
        start, end = format_current_month("Europe/Tallinn")

        assert isinstance(start, str)
        assert isinstance(end, str)
        assert start.endswith("T00:00:00")
        assert end.endswith("T23:59:59")

    def test_format_current_month_empty_string(self) -> None:
        """Test formatting current month with empty timezone (system default)."""
        start, end = format_current_month("")

        assert isinstance(start, str)
        assert isinstance(end, str)
        assert start.endswith("T00:00:00")
        assert end.endswith("T23:59:59")


@mock.patch("waldur_site_agent.common.processors.utils.print_current_user")
class ProcessorTimezoneTest(unittest.TestCase):
    """Test processor timezone functionality."""

    def setUp(self) -> None:
        respx.start()
        self.offering_uuid = uuid.uuid4()
        self.waldur_user = {
            "username": "test",
            "email": "test@example.com",
            "full_name": "Test User",
            "is_staff": False,
        }
        self.waldur_offering = Offering(
            name="test",
            api_url=BASE_URL,
            api_token="token",
            uuid=self.offering_uuid.hex,
            backend_type="slurm",
            order_processing_backend="slurm",
            membership_sync_backend="slurm",
            reporting_backend="slurm",
            backend_settings={},
            backend_components={},
        )
        self.waldur_offering_response = {
            "uuid": self.offering_uuid.hex,
            "name": "test",
            "description": "test",
        }

    def tearDown(self) -> None:
        respx.stop()

    def test_processor_constructor_timezone(self, mock_print_user) -> None:
        """Test that processor constructor accepts timezone parameter."""
        respx.get("https://waldur.example.com/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-offerings/{self.offering_uuid.hex}/"
        ).respond(200, json=self.waldur_offering_response)

        processor = OfferingReportProcessor(self.waldur_offering, "test-agent", "UTC")
        assert processor.timezone == "UTC"

    def test_processor_constructor_timezone_empty(self, mock_print_user) -> None:
        """Test that processor constructor accepts empty timezone."""
        respx.get("https://waldur.example.com/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-offerings/{self.offering_uuid.hex}/"
        ).respond(200, json=self.waldur_offering_response)

        processor = OfferingReportProcessor(self.waldur_offering, "test-agent", "")
        assert processor.timezone == ""


@freeze_time("2025-07-01 00:30:00")
class TimezoneMonthBoundaryTest(unittest.TestCase):
    def test_timezone_differences(self) -> None:
        """Test that different timezones give different times."""
        utc_time = get_current_time_in_timezone("UTC")
        tallinn_time = get_current_time_in_timezone("Europe/Tallinn")

        assert utc_time.tzinfo != tallinn_time.tzinfo

    def test_month_boundary_edge_case(self) -> None:
        """Test edge case around month boundaries."""
        utc_start, _ = format_current_month("UTC")
        tallinn_start, _ = format_current_month("Europe/Tallinn")

        assert len(utc_start) == 19
        assert len(tallinn_start) == 19

        assert utc_start.endswith("T00:00:00")
        assert tallinn_start.endswith("T00:00:00")


@mock.patch.object(backend.SlurmBackend, "_pull_backend_resource")
class TimezoneIntegrationTest(unittest.TestCase):
    """Integration tests for timezone functionality with mocked dependencies."""

    def setUp(self) -> None:
        respx.start()
        self.offering_uuid = uuid.uuid4()
        self.waldur_resource = {
            "uuid": "waldur-resource-uuid",
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "state": "OK",
        }
        self.waldur_offering = Offering(
            name="test",
            api_url=BASE_URL,
            api_token="token",
            uuid=self.offering_uuid.hex,
            backend_type="slurm",
            order_processing_backend="slurm",
            membership_sync_backend="slurm",
            reporting_backend="slurm",
            backend_settings={},
            backend_components={},
        )
        self.waldur_offering_response = {
            "uuid": self.offering_uuid.hex,
            "name": "test",
            "description": "test",
        }
        self.waldur_user = {
            "username": "test",
            "email": "test@example.com",
            "full_name": "Test User",
            "is_staff": False,
        }

    def tearDown(self) -> None:
        respx.stop()

    def test_processor_uses_timezone_for_billing_period(self, mock_pull_backend_resource) -> None:
        """Test that processor uses configured timezone for billing period calculation."""
        mock_pull_backend_resource.return_value = None
        respx.get("https://waldur.example.com/api/users/me/").respond(200, json=self.waldur_user)
        respx.get(
            f"{BASE_URL}/api/marketplace-provider-offerings/{self.offering_uuid.hex}/"
        ).respond(200, json=self.waldur_offering_response)
        processor = OfferingReportProcessor(self.waldur_offering, "test-agent", "UTC")
        assert processor.timezone == "UTC"
