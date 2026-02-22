"""Tests for multi-period usage reporting."""

import datetime
import unittest
import uuid
from unittest import mock

import respx
from freezegun import freeze_time
from pydantic import ValidationError
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import ServiceProvider

from tests.fixtures import OFFERING
from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.processors import OfferingReportProcessor
from waldur_site_agent.common.structures import RootConfiguration, WaldurAgentConfiguration


class TestReportingPeriodsConfig(unittest.TestCase):
    """Test reporting_periods configuration field."""

    def test_default_value(self) -> None:
        config = WaldurAgentConfiguration()
        assert config.reporting_periods == 2

    def test_valid_value(self) -> None:
        config = WaldurAgentConfiguration(reporting_periods=5)
        assert config.reporting_periods == 5

    def test_min_value(self) -> None:
        config = WaldurAgentConfiguration(reporting_periods=1)
        assert config.reporting_periods == 1

    def test_max_value(self) -> None:
        config = WaldurAgentConfiguration(reporting_periods=12)
        assert config.reporting_periods == 12

    def test_below_min_raises(self) -> None:
        with self.assertRaises(ValidationError):
            WaldurAgentConfiguration(reporting_periods=0)

    def test_above_max_raises(self) -> None:
        with self.assertRaises(ValidationError):
            WaldurAgentConfiguration(reporting_periods=13)

    def test_root_config_threads_through(self) -> None:
        raw_config = RootConfiguration(
            offerings=[
                {
                    "name": "test",
                    "waldur_api_url": "https://example.com/api/",
                    "waldur_api_token": "token",
                    "waldur_offering_uuid": uuid.uuid4().hex,
                    "backend_type": "slurm",
                    "backend_components": {},
                }
            ],
            reporting_periods=3,
        )
        agent_config = raw_config.to_agent_configuration()
        assert agent_config.reporting_periods == 3

    def test_root_config_default(self) -> None:
        raw_config = RootConfiguration(
            offerings=[
                {
                    "name": "test",
                    "waldur_api_url": "https://example.com/api/",
                    "waldur_api_token": "token",
                    "waldur_offering_uuid": uuid.uuid4().hex,
                    "backend_type": "slurm",
                    "backend_components": {},
                }
            ],
        )
        agent_config = raw_config.to_agent_configuration()
        assert agent_config.reporting_periods == 2


class TestComputeReportingPeriods(unittest.TestCase):
    """Test _compute_reporting_periods static method."""

    def test_single_period(self) -> None:
        current_time = datetime.datetime(2024, 6, 15)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 1)
        assert periods == [(2024, 6, True)]

    def test_two_periods(self) -> None:
        current_time = datetime.datetime(2024, 6, 15)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 2)
        assert periods == [(2024, 5, False), (2024, 6, True)]

    def test_three_periods(self) -> None:
        current_time = datetime.datetime(2024, 6, 15)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 3)
        assert periods == [(2024, 4, False), (2024, 5, False), (2024, 6, True)]

    def test_year_boundary_january(self) -> None:
        current_time = datetime.datetime(2024, 1, 10)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 2)
        assert periods == [(2023, 12, False), (2024, 1, True)]

    def test_year_boundary_three_periods(self) -> None:
        current_time = datetime.datetime(2024, 2, 5)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 3)
        assert periods == [(2023, 12, False), (2024, 1, False), (2024, 2, True)]

    def test_oldest_first_current_last(self) -> None:
        current_time = datetime.datetime(2024, 6, 15)
        periods = OfferingReportProcessor._compute_reporting_periods(current_time, 4)
        # Oldest first
        assert periods[0] == (2024, 3, False)
        # Current last
        assert periods[-1] == (2024, 6, True)
        # All past periods have is_current=False
        for _year, _month, is_current in periods[:-1]:
            assert is_current is False


class TestBaseBackendGetUsageReportForPeriod(unittest.TestCase):
    """Test that BaseBackend.get_usage_report_for_period returns {} by default."""

    def test_returns_empty_dict(self) -> None:
        # Create a minimal concrete backend to test the non-abstract method
        class MinimalBackend(BaseBackend):
            def ping(self, raise_exception=False):
                return True

            def diagnostics(self):
                return True

            def list_components(self):
                return []

            def _get_usage_report(self, resource_backend_ids):
                return {}

            def downscale_resource(self, resource_backend_id):
                return True

            def pause_resource(self, resource_backend_id):
                return True

            def restore_resource(self, resource_backend_id):
                return True

            def get_resource_metadata(self, resource_backend_id):
                return {}

            def _collect_resource_limits(self, waldur_resource):
                return {}, {}

            def _pre_create_resource(self, waldur_resource, user_context=None):
                pass

        backend = MinimalBackend({}, {})
        result = backend.get_usage_report_for_period(["acc1"], 2024, 1)
        assert result == {}


@freeze_time("2024-06-15")
class TestMultiPeriodProcessorFlow(unittest.TestCase):
    """Test that the processor loops over multiple periods."""

    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.waldur_resource_uuid = "10a0f810be1c43bbb651e8cbdbb90198"
        self.waldur_resource = {
            "uuid": self.waldur_resource_uuid,
            "name": "test-alloc-01",
            "backend_id": "test-allocation-01",
            "state": "OK",
        }
        self.waldur_offering = {
            "components": [
                {"type": "cpu"},
                {"type": "mem"},
            ],
            "customer_uuid": uuid.uuid4().hex,
        }
        self.mock_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token=OFFERING.api_token,
            headers={},
        )
        self.mock_backend = mock.MagicMock(spec=BaseBackend)
        self.mock_backend.backend_type = "slurm"
        self.mock_backend.supports_decreasing_usage = False
        self.mock_backend.backend_components = {
            "cpu": {"limit": 10, "measured_unit": "k-Hours", "unit_factor": 60000,
                    "accounting_type": "limit", "label": "CPU"},
            "mem": {"limit": 10, "measured_unit": "gb-Hours", "unit_factor": 61440,
                    "accounting_type": "usage", "label": "RAM"},
        }
        self.mock_backend.timezone = ""

    def tearDown(self) -> None:
        respx.stop()

    def _setup_common_mocks(self) -> None:
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200, json={"username": "test-user"}
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{OFFERING.uuid}/"
        ).respond(200, json=self.waldur_offering)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/",
        ).respond(200, json=[self.waldur_resource])
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-resources/{self.waldur_resource_uuid}/"
        ).respond(200, json=self.waldur_resource)

        service_provider = ServiceProvider(uuid=uuid.uuid4())
        respx.get(f"{self.BASE_URL}/api/marketplace-service-providers/").respond(
            200, json=[service_provider.to_dict()]
        )
        # Mock component usages list (for anomaly check and per-user)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-usages/",
        ).respond(200, json=[])
        # Mock component user usages list (for filtering per-user records)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-component-user-usages/",
        ).respond(200, json=[])
        # Mock offering users
        respx.get(
            f"{self.BASE_URL}/api/marketplace-offering-users/",
        ).respond(200, json=[])

    def test_two_periods_calls_set_usage_twice(self) -> None:
        """With reporting_periods=2, set_usage should be called for both periods."""
        self._setup_common_mocks()

        # Current month data from pull_resource
        current_usage = BackendResourceInfo(
            backend_id="test-allocation-01",
            usage={
                "TOTAL_ACCOUNT_USAGE": {"cpu": 10, "mem": 30},
            },
        )
        self.mock_backend.pull_resource.return_value = current_usage

        # Past month data
        self.mock_backend.get_usage_report_for_period.return_value = {
            "test-allocation-01": {
                "TOTAL_ACCOUNT_USAGE": {"cpu": 5, "mem": 15},
            }
        }

        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        processor = OfferingReportProcessor(
            OFFERING,
            self.mock_client,
            resource_backend=self.mock_backend,
            resource_backend_version="test",
            reporting_periods=2,
        )
        processor.process_offering()

        # set_usage called for past month + current month = 2
        assert set_usage_response.call_count == 2
        # get_usage_report_for_period called once for the past month
        self.mock_backend.get_usage_report_for_period.assert_called_once_with(
            ["test-allocation-01"], 2024, 5
        )

    def test_single_period_no_historical_call(self) -> None:
        """With reporting_periods=1, only current month is reported."""
        self._setup_common_mocks()

        current_usage = BackendResourceInfo(
            backend_id="test-allocation-01",
            usage={
                "TOTAL_ACCOUNT_USAGE": {"cpu": 10, "mem": 30},
            },
        )
        self.mock_backend.pull_resource.return_value = current_usage

        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        processor = OfferingReportProcessor(
            OFFERING,
            self.mock_client,
            resource_backend=self.mock_backend,
            resource_backend_version="test",
            reporting_periods=1,
        )
        processor.process_offering()

        assert set_usage_response.call_count == 1
        self.mock_backend.get_usage_report_for_period.assert_not_called()

    def test_past_period_empty_data_skipped(self) -> None:
        """When past period returns empty dict, current period is still reported."""
        self._setup_common_mocks()

        current_usage = BackendResourceInfo(
            backend_id="test-allocation-01",
            usage={
                "TOTAL_ACCOUNT_USAGE": {"cpu": 10, "mem": 30},
            },
        )
        self.mock_backend.pull_resource.return_value = current_usage
        self.mock_backend.get_usage_report_for_period.return_value = {}

        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        processor = OfferingReportProcessor(
            OFFERING,
            self.mock_client,
            resource_backend=self.mock_backend,
            resource_backend_version="test",
            reporting_periods=2,
        )
        processor.process_offering()

        # Only current month reported
        assert set_usage_response.call_count == 1

    def test_past_period_error_does_not_block_current(self) -> None:
        """If past period raises an exception, current period is still reported."""
        self._setup_common_mocks()

        current_usage = BackendResourceInfo(
            backend_id="test-allocation-01",
            usage={
                "TOTAL_ACCOUNT_USAGE": {"cpu": 10, "mem": 30},
            },
        )
        self.mock_backend.pull_resource.return_value = current_usage
        self.mock_backend.get_usage_report_for_period.side_effect = Exception(
            "Historical API error"
        )

        set_usage_response = respx.post(
            f"{self.BASE_URL}/api/marketplace-component-usages/set_usage/"
        ).respond(201, json={})

        processor = OfferingReportProcessor(
            OFFERING,
            self.mock_client,
            resource_backend=self.mock_backend,
            resource_backend_version="test",
            reporting_periods=2,
        )
        processor.process_offering()

        # Current month still reported despite past month failure
        assert set_usage_response.call_count == 1


if __name__ == "__main__":
    unittest.main()
