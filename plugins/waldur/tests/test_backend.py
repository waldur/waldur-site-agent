"""Tests for WaldurBackend with mocked WaldurClient."""

import datetime
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from httpx import URL

from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.oecd_fos_2007_code_enum import OecdFos2007CodeEnum
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.exceptions import BackendError, BackendNotReadyError
from waldur_site_agent.backend.structures import BackendResourceInfo

from pydantic import ValidationError

from waldur_site_agent_waldur.backend import WaldurBackend
from waldur_site_agent_waldur.enums import EndDateSyncDirection, LimitSyncDirection
from waldur_site_agent_waldur.schemas import WaldurBackendSettingsSchema

# Valid test UUIDs
ORDER_UUID = UUID("12345678-1234-1234-1234-123456789abc")
RESOURCE_UUID = UUID("abcdef01-1234-1234-1234-123456789abc")
PROJECT_UUID = UUID("aabbccdd-1234-1234-1234-123456789abc")
USER_UUID = UUID("11223344-1234-1234-1234-123456789abc")


@pytest.fixture()
def mock_client():
    """Create a mocked WaldurClient."""
    client = MagicMock()
    client.api_url = "https://waldur-b.example.com/api/"
    client.offering_uuid = "abcdef01-0000-0000-0000-000000000001"
    client.ping.return_value = True
    return client


@pytest.fixture()
def backend(backend_settings, backend_components_passthrough, mock_client):
    """Create a WaldurBackend with mocked client."""
    backend = WaldurBackend(backend_settings, backend_components_passthrough)
    backend.client = mock_client
    return backend


@pytest.fixture()
def backend_with_conversion(backend_settings, backend_components_with_conversion, mock_client):
    """Create a WaldurBackend with component conversion."""
    backend = WaldurBackend(backend_settings, backend_components_with_conversion)
    backend.client = mock_client
    return backend


class TestInitialization:
    def test_init_with_valid_settings(self, backend_settings, backend_components_passthrough):
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        assert backend.backend_type == "waldur"
        assert backend.target_offering_uuid == "offering-uuid-on-waldur-b"
        assert backend.target_customer_uuid == "customer-uuid-on-waldur-b"

    def test_init_missing_required_setting(self, backend_components_passthrough):
        with pytest.raises(ValueError, match="Missing required"):
            WaldurBackend({"target_api_url": "https://example.com"}, backend_components_passthrough)

    def test_passthrough_detection(self, backend):
        assert backend.component_mapper.is_passthrough is True

    def test_conversion_detection(self, backend_with_conversion):
        assert backend_with_conversion.component_mapper.is_passthrough is False


class TestSchemaValidation:
    """Regression tests for WaldurBackendSettingsSchema field coverage.

    These guard against fields that are consumed by the backend but missing
    from the Pydantic schema — which causes 'Extra inputs are not permitted'
    validation errors on startup when extra="forbid" is in effect.
    """

    BASE_SETTINGS = {
        "target_api_url": "https://waldur-b.example.com/api/",
        "target_api_token": "token",
        "target_offering_uuid": "offering-uuid",
        "target_customer_uuid": "customer-uuid",
    }

    def test_efp_fields_accepted(self):
        """end_date_sync_direction, passthrough_attributes, fetch_consented_users_only
        must not raise ValidationError (regression: all three were missing from schema)."""
        schema = WaldurBackendSettingsSchema(
            **self.BASE_SETTINGS,
            end_date_sync_direction="a_to_b",
            passthrough_attributes=["researchFields", "storageRequest"],
            fetch_consented_users_only=True,
        )
        assert schema.end_date_sync_direction == EndDateSyncDirection.A_TO_B
        assert schema.passthrough_attributes == ["researchFields", "storageRequest"]
        assert schema.fetch_consented_users_only is True

    def test_end_date_sync_direction_invalid_value_rejected(self):
        with pytest.raises(ValidationError):
            WaldurBackendSettingsSchema(
                **self.BASE_SETTINGS,
                end_date_sync_direction="invalid_value",
            )

    def test_unknown_field_rejected(self):
        with pytest.raises(ValidationError):
            WaldurBackendSettingsSchema(
                **self.BASE_SETTINGS,
                totally_unknown_field="oops",
            )

    def test_defaults(self):
        schema = WaldurBackendSettingsSchema(**self.BASE_SETTINGS)
        assert schema.end_date_sync_direction == EndDateSyncDirection.BIDIRECTIONAL
        assert schema.passthrough_attributes == []
        assert schema.fetch_consented_users_only is False

    def test_limit_sync_direction_accepted(self):
        schema = WaldurBackendSettingsSchema(
            **self.BASE_SETTINGS,
            limit_sync_direction="disabled",
        )
        assert schema.limit_sync_direction == LimitSyncDirection.DISABLED

    def test_limit_sync_direction_invalid_value_rejected(self):
        with pytest.raises(ValidationError):
            WaldurBackendSettingsSchema(
                **self.BASE_SETTINGS,
                limit_sync_direction="sideways",
            )

    def test_limit_sync_direction_default(self):
        schema = WaldurBackendSettingsSchema(**self.BASE_SETTINGS)
        assert schema.limit_sync_direction == LimitSyncDirection.B_TO_A


class TestPingAndDiagnostics:
    def test_ping_success(self, backend, mock_client):
        mock_client.ping.return_value = True
        assert backend.ping() is True

    def test_ping_failure(self, backend, mock_client):
        mock_client.ping.return_value = False
        assert backend.ping() is False

    def test_ping_exception_with_raise(self, backend, mock_client):
        mock_client.ping.side_effect = Exception("Connection error")
        with pytest.raises(BackendError):
            backend.ping(raise_exception=True)

    def test_diagnostics(self, backend, mock_client):
        mock_client.ping.return_value = True
        assert backend.diagnostics() is True

    def test_supports_cycle_preflight_enabled(self, backend):
        assert backend.supports_cycle_preflight is True

    def test_run_preflight_success(self, backend, mock_client):
        mock_client.ping.return_value = True
        backend.run_preflight()

    def test_run_preflight_raises_not_ready(self, backend, mock_client):
        mock_client.ping.return_value = False
        with pytest.raises(BackendNotReadyError, match="not reachable"):
            backend.run_preflight()

    def test_list_components(self, backend):
        components = backend.list_components()
        assert set(components) == {"cpu", "mem"}


class TestResourceCreation:
    def test_create_resource_returns_pending_order(self, backend, mock_client):
        """Non-blocking creation: returns pending_order_id for async tracking."""
        mock_client.find_or_create_project.return_value = {
            "uuid": str(PROJECT_UUID),
            "url": f"/api/projects/{PROJECT_UUID}/",
            "name": "Test Project",
        }
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": str(PROJECT_UUID),
            "url": f"/api/projects/{PROJECT_UUID}/",
            "name": "Test Project",
        }
        mock_client.get_project_url.return_value = f"/api/projects/{PROJECT_UUID}/"
        mock_client.get_offering_url.return_value = "/api/marketplace-provider-offerings/off-uuid/"
        mock_client.get_customer_url.return_value = "/api/customers/cust-uuid/"

        mock_order = MagicMock()
        mock_order.uuid = ORDER_UUID
        mock_order.marketplace_resource_uuid = RESOURCE_UUID
        mock_client.create_marketplace_order.return_value = mock_order

        waldur_resource = MagicMock()
        waldur_resource.uuid = "source-resource-uuid"
        waldur_resource.project_uuid = "proj-uuid-a"
        waldur_resource.customer_uuid = "cust-uuid-a"
        waldur_resource.project_name = "Test Project"
        waldur_resource.name = "Test Resource"
        waldur_resource.limits = MagicMock()
        waldur_resource.limits.__contains__ = lambda self, key: key in {"cpu", "mem"}
        waldur_resource.limits.__getitem__ = lambda self, key: {"cpu": 100, "mem": 200}[key]

        result = backend.create_resource_with_id(waldur_resource, "test-backend-id")

        assert isinstance(result, BackendResourceInfo)
        assert result.backend_id == str(RESOURCE_UUID)
        assert result.pending_order_id == str(ORDER_UUID)
        # poll_order_completion should NOT be called (non-blocking)
        mock_client.poll_order_completion.assert_not_called()

    def test_create_resource_no_resource_uuid_on_order(self, backend, mock_client):
        """Error when order response has no marketplace_resource_uuid."""
        mock_client.find_or_create_project.return_value = {
            "uuid": str(PROJECT_UUID),
            "url": f"/api/projects/{PROJECT_UUID}/",
            "name": "Test Project",
        }
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": str(PROJECT_UUID),
        }
        mock_client.get_project_url.return_value = f"/api/projects/{PROJECT_UUID}/"
        mock_client.get_offering_url.return_value = "/api/offerings/off-uuid/"
        mock_client.get_customer_url.return_value = "/api/customers/cust-uuid/"

        mock_order = MagicMock()
        mock_order.uuid = ORDER_UUID
        mock_order.marketplace_resource_uuid = UNSET
        mock_client.create_marketplace_order.return_value = mock_order

        waldur_resource = MagicMock()
        waldur_resource.uuid = "source-resource-uuid"
        waldur_resource.project_uuid = "proj-uuid-a"
        waldur_resource.customer_uuid = "cust-uuid-a"
        waldur_resource.project_name = "Test Project"
        waldur_resource.name = "Test Resource"
        waldur_resource.limits = MagicMock()
        waldur_resource.limits.__contains__ = lambda self, key: key in {"cpu"}
        waldur_resource.limits.__getitem__ = lambda self, key: {"cpu": 100}[key]

        with pytest.raises(BackendError, match="no marketplace_resource_uuid"):
            backend.create_resource_with_id(waldur_resource, "test-backend-id")


class TestCheckPendingOrder:
    def test_check_pending_order_done(self, backend, mock_client):
        """Returns True when target order has completed."""
        mock_order = MagicMock()
        mock_order.state = OrderState.DONE
        mock_client.get_order.return_value = mock_order

        result = backend.check_pending_order(str(ORDER_UUID))
        assert result is True
        mock_client.get_order.assert_called_once_with(ORDER_UUID)

    def test_check_pending_order_still_executing(self, backend, mock_client):
        """Returns False when target order is still executing."""
        mock_order = MagicMock()
        mock_order.state = OrderState.EXECUTING
        mock_client.get_order.return_value = mock_order

        result = backend.check_pending_order(str(ORDER_UUID))
        assert result is False

    def test_check_pending_order_erred(self, backend, mock_client):
        """Raises BackendError when target order has failed."""
        mock_order = MagicMock()
        mock_order.state = OrderState.ERRED
        mock_client.get_order.return_value = mock_order

        with pytest.raises(BackendError, match="failed"):
            backend.check_pending_order(str(ORDER_UUID))

    def test_check_pending_order_canceled(self, backend, mock_client):
        """Raises BackendError when target order was canceled."""
        mock_order = MagicMock()
        mock_order.state = OrderState.CANCELED
        mock_client.get_order.return_value = mock_order

        with pytest.raises(BackendError, match="failed"):
            backend.check_pending_order(str(ORDER_UUID))

    def test_check_pending_order_rejected(self, backend, mock_client):
        """Raises BackendError when target order was rejected."""
        mock_order = MagicMock()
        mock_order.state = OrderState.REJECTED
        mock_client.get_order.return_value = mock_order

        with pytest.raises(BackendError, match="failed"):
            backend.check_pending_order(str(ORDER_UUID))

    def test_check_pending_order_pending_provider(self, backend, mock_client):
        """Returns False when target order is pending provider approval."""
        mock_order = MagicMock()
        mock_order.state = OrderState.PENDING_PROVIDER
        mock_client.get_order.return_value = mock_order

        result = backend.check_pending_order(str(ORDER_UUID))
        assert result is False

    def test_check_pending_order_503_raises_not_ready(self, backend, mock_client):
        """A 5xx from Waldur B raises BackendNotReadyError so the order retries next cycle."""
        mock_client.get_order.side_effect = UnexpectedStatus(
            status_code=503,
            content=b"Service Unavailable",
            url=URL(f"https://waldur-b.example.com/api/marketplace-orders/{ORDER_UUID}/"),
        )

        with pytest.raises(BackendNotReadyError, match="503"):
            backend.check_pending_order(str(ORDER_UUID))

    def test_check_pending_order_4xx_still_raises_unexpected_status(self, backend, mock_client):
        """A 4xx from Waldur B is a real error and must not be swallowed."""
        mock_client.get_order.side_effect = UnexpectedStatus(
            status_code=404,
            content=b"Not Found",
            url=URL(f"https://waldur-b.example.com/api/marketplace-orders/{ORDER_UUID}/"),
        )

        with pytest.raises(UnexpectedStatus):
            backend.check_pending_order(str(ORDER_UUID))


class TestResourceDeletion:
    @staticmethod
    def _b_resource(state: ResourceState) -> MagicMock:
        resource = MagicMock()
        resource.state = state
        return resource

    def test_delete_resource(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(ResourceState.OK)
        mock_client.create_terminate_order.return_value = ORDER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        pending_order_id = backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_called_once()
        mock_client.poll_order_completion.assert_not_called()
        assert pending_order_id == str(ORDER_UUID)

    def test_delete_resource_erred_on_b(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(ResourceState.ERRED)
        mock_client.create_terminate_order.return_value = ORDER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        pending_order_id = backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_called_once()
        assert pending_order_id == str(ORDER_UUID)

    def test_delete_resource_terminating_adopts_order(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(
            ResourceState.TERMINATING
        )
        in_flight_order = MagicMock()
        in_flight_order.uuid = ORDER_UUID
        mock_client.get_in_flight_order.return_value = in_flight_order

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        pending_order_id = backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_not_called()
        mock_client.get_in_flight_order.assert_called_once_with(
            RESOURCE_UUID, RequestTypes.TERMINATE
        )
        assert pending_order_id == str(ORDER_UUID)

    def test_delete_resource_terminating_without_order_raises(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(
            ResourceState.TERMINATING
        )
        mock_client.get_in_flight_order.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        with pytest.raises(BackendError, match="no in-flight TERMINATE order"):
            backend.delete_resource(waldur_resource)

    def test_delete_resource_terminated_on_b(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(
            ResourceState.TERMINATED
        )

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        pending_order_id = backend.delete_resource(waldur_resource)
        assert pending_order_id is None
        mock_client.create_terminate_order.assert_not_called()

    def test_delete_resource_creating_raises_not_ready(self, backend, mock_client):
        mock_client.get_marketplace_resource.return_value = self._b_resource(
            ResourceState.CREATING
        )

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        with pytest.raises(BackendNotReadyError, match="cannot terminate yet"):
            backend.delete_resource(waldur_resource)

    def test_delete_resource_not_found(self, backend, mock_client):
        mock_client.get_marketplace_resource.side_effect = Exception("not found")

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        assert backend.delete_resource(waldur_resource) is None
        mock_client.create_terminate_order.assert_not_called()

    def test_delete_resource_empty_backend_id(self, backend, mock_client):
        waldur_resource = MagicMock()
        waldur_resource.backend_id = ""

        backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_not_called()


class TestSetLimits:
    def test_set_resource_limits(self, backend, mock_client):
        mock_client.create_update_order.return_value = ORDER_UUID

        pending_order_id = backend.set_resource_limits(str(RESOURCE_UUID), {"cpu": 200})
        mock_client.create_update_order.assert_called_once()
        mock_client.poll_order_completion.assert_not_called()
        assert pending_order_id == str(ORDER_UUID)

    def test_set_resource_limits_with_conversion(self, backend_with_conversion, mock_client):
        mock_client.create_update_order.return_value = ORDER_UUID

        pending_order_id = backend_with_conversion.set_resource_limits(
            str(RESOURCE_UUID), {"node_hours": 100}
        )
        mock_client.poll_order_completion.assert_not_called()
        assert pending_order_id == str(ORDER_UUID)

        # Verify the limits were converted
        call_args = mock_client.create_update_order.call_args
        target_limits = call_args.kwargs["limits"]
        assert target_limits == {"gpu_hours": 500, "storage_gb_hours": 1000}


class TestGetLimits:
    def test_get_resource_limits_passthrough(self, backend, mock_client):
        mock_client.get_resource_limits.return_value = {"cpu": 200, "mem": 100}

        limits = backend.get_resource_limits(str(RESOURCE_UUID))

        assert limits == {"cpu": 200, "mem": 100}

    def test_get_resource_limits_with_conversion(self, backend_with_conversion, mock_client):
        mock_client.get_resource_limits.return_value = {
            "gpu_hours": 500,
            "storage_gb_hours": 800,
        }

        limits = backend_with_conversion.get_resource_limits(str(RESOURCE_UUID))

        # node_hours = 500/5 + 800/10 = 100 + 80 = 180
        assert limits["node_hours"] == 180

    def test_get_resource_limits_with_conversion_missing_target_component(
        self, backend_with_conversion, mock_client
    ):
        mock_client.get_resource_limits.return_value = {"gpu_hours": 250}

        limits = backend_with_conversion.get_resource_limits(str(RESOURCE_UUID))

        # node_hours = 250/5 = 50
        assert limits["node_hours"] == 50


class TestSyncResourceLimits:
    """WaldurBackend.sync_resource_limits honours limit_sync_direction.

    The generic B->A reconciliation lives in BaseBackend; the Waldur override
    only adds the 'disabled' option.
    """

    def _waldur_resource(self, limits):
        resource = MagicMock()
        resource.name = "test-resource"
        resource.backend_id = str(RESOURCE_UUID)
        resource.uuid = RESOURCE_UUID
        resource.limits.additional_properties = limits
        return resource

    @patch("waldur_site_agent.common.utils.marketplace_provider_resources_set_limits")
    def test_b_to_a_writes_backend_limits_to_waldur_a(
        self, mock_set_limits, backend_with_conversion, mock_client
    ):
        # B reports gpu_hours=500, storage_gb_hours=800 -> node_hours=180 (differs from A's 100)
        mock_client.get_resource_limits.return_value = {
            "gpu_hours": 500,
            "storage_gb_hours": 800,
        }
        waldur_resource = self._waldur_resource({"node_hours": 100})

        backend_with_conversion.sync_resource_limits(waldur_resource, MagicMock())

        # B -> A: provider set_limits called against Waldur A
        mock_set_limits.sync.assert_called_once()

    @patch("waldur_site_agent.common.utils.marketplace_provider_resources_set_limits")
    def test_b_to_a_skips_when_in_sync(
        self, mock_set_limits, backend_with_conversion, mock_client
    ):
        mock_client.get_resource_limits.return_value = {
            "gpu_hours": 500,
            "storage_gb_hours": 800,
        }
        # A already equals reverse(B) = node_hours 180
        waldur_resource = self._waldur_resource({"node_hours": 180})

        backend_with_conversion.sync_resource_limits(waldur_resource, MagicMock())

        mock_set_limits.sync.assert_not_called()

    @patch("waldur_site_agent.common.utils.marketplace_provider_resources_set_limits")
    def test_disabled_skips_reconciliation(
        self, mock_set_limits, backend_with_conversion, mock_client
    ):
        # Offering opted out -> the override returns before the generic pull:
        # no backend reads, no writes to A.
        backend_with_conversion.limit_sync_direction = LimitSyncDirection.DISABLED
        waldur_resource = self._waldur_resource({"node_hours": 100})

        backend_with_conversion.sync_resource_limits(waldur_resource, MagicMock())

        mock_client.get_resource_limits.assert_not_called()
        mock_set_limits.sync.assert_not_called()


class TestLimitSyncDirection:
    """The backend owns the limit_sync_direction policy (mirrors end_date sync)."""

    def test_default_is_b_to_a(self, backend):
        assert backend.limit_sync_direction == LimitSyncDirection.B_TO_A

    def test_disabled_parsed_from_settings(
        self, backend_settings, backend_components_passthrough
    ):
        backend_settings["limit_sync_direction"] = "disabled"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        assert backend.limit_sync_direction == LimitSyncDirection.DISABLED


class TestUsageReporting:
    def test_get_usage_report_passthrough(self, backend, mock_client):
        mock_usage = MagicMock()
        mock_usage.type_ = "cpu"
        mock_usage.usage = 100

        mock_usage2 = MagicMock()
        mock_usage2.type_ = "mem"
        mock_usage2.usage = 200

        mock_client.get_component_usages.return_value = [mock_usage, mock_usage2]
        mock_client.get_component_user_usages.return_value = []

        report = backend._get_usage_report([str(RESOURCE_UUID)])

        assert str(RESOURCE_UUID) in report
        total = report[str(RESOURCE_UUID)]["TOTAL_ACCOUNT_USAGE"]
        assert total["cpu"] == 100.0
        assert total["mem"] == 200.0

    def test_get_usage_report_with_conversion(self, backend_with_conversion, mock_client):
        mock_usage1 = MagicMock()
        mock_usage1.type_ = "gpu_hours"
        mock_usage1.usage = 500

        mock_usage2 = MagicMock()
        mock_usage2.type_ = "storage_gb_hours"
        mock_usage2.usage = 800

        mock_client.get_component_usages.return_value = [mock_usage1, mock_usage2]
        mock_client.get_component_user_usages.return_value = []

        report = backend_with_conversion._get_usage_report([str(RESOURCE_UUID)])

        total = report[str(RESOURCE_UUID)]["TOTAL_ACCOUNT_USAGE"]
        # node_hours = 500/5 + 800/10 = 100 + 80 = 180
        assert total["node_hours"] == 180.0

    def test_get_usage_report_with_user_usages(self, backend, mock_client):
        mock_total = MagicMock()
        mock_total.type_ = "cpu"
        mock_total.usage = 100
        mock_client.get_component_usages.return_value = [mock_total]

        mock_user_usage = MagicMock()
        mock_user_usage.username = "user1"
        mock_user_usage.component_type = "cpu"
        mock_user_usage.usage = 60
        mock_client.get_component_user_usages.return_value = [mock_user_usage]

        report = backend._get_usage_report([str(RESOURCE_UUID)])

        assert "user1" in report[str(RESOURCE_UUID)]
        assert report[str(RESOURCE_UUID)]["user1"]["cpu"] == 60.0

    def test_get_usage_report_api_error(self, backend, mock_client):
        mock_client.get_component_usages.side_effect = Exception("API error")

        report = backend._get_usage_report([str(RESOURCE_UUID)])

        # Should return empty usage, not raise
        assert str(RESOURCE_UUID) in report
        total = report[str(RESOURCE_UUID)]["TOTAL_ACCOUNT_USAGE"]
        assert total["cpu"] == 0.0
        assert total["mem"] == 0.0

    def test_get_usage_report_rounds_converted_usage(
        self, backend_settings, backend_components_gpu_conversion, mock_client
    ):
        # Reverse conversion can yield more than 2 decimals: 48.58 / 4 = 12.145
        backend = WaldurBackend(backend_settings, backend_components_gpu_conversion)
        backend.client = mock_client

        mock_usage = MagicMock()
        mock_usage.type_ = "cpu_k_hours"
        mock_usage.usage = 48.58

        mock_client.get_component_usages.return_value = [mock_usage]
        mock_client.get_component_user_usages.return_value = []

        report = backend._get_usage_report([str(RESOURCE_UUID)])

        total = report[str(RESOURCE_UUID)]["TOTAL_ACCOUNT_USAGE"]
        assert total["node_hours"] == 12.14

    def test_get_usage_report_rounds_per_user_usage(
        self, backend_settings, backend_components_gpu_conversion, mock_client
    ):
        backend = WaldurBackend(backend_settings, backend_components_gpu_conversion)
        backend.client = mock_client

        mock_total = MagicMock()
        mock_total.type_ = "cpu_k_hours"
        mock_total.usage = 1.39
        mock_client.get_component_usages.return_value = [mock_total]

        mock_user_usage = MagicMock()
        mock_user_usage.username = "user1"
        mock_user_usage.component_type = "cpu_k_hours"
        mock_user_usage.usage = 1.39
        mock_client.get_component_user_usages.return_value = [mock_user_usage]

        report = backend._get_usage_report([str(RESOURCE_UUID)])

        # 1.39 / 4 = 0.3475 -> rounded to 0.35 for both total and per-user
        assert report[str(RESOURCE_UUID)]["TOTAL_ACCOUNT_USAGE"]["node_hours"] == 0.35
        assert report[str(RESOURCE_UUID)]["user1"]["node_hours"] == 0.35


class TestUserSync:
    def test_add_users_to_resource(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_users_to_resource(waldur_resource, {"user1"})
        assert "user1" in result
        mock_client.add_user_to_project.assert_called_once()

    def test_add_users_user_not_found_warn(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_users_to_resource(waldur_resource, {"user1"})
        assert "user1" not in result
        mock_client.add_user_to_project.assert_not_called()

    def test_add_users_user_not_found_fail(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_not_found_action"] = "fail"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        # Should not propagate - caught by add_users_to_resource
        result = backend.add_users_to_resource(waldur_resource, {"user1"})
        assert "user1" not in result

    def test_remove_users_from_resource(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID
        mock_client.list_project_users.return_value = []

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.remove_users_from_resource(
            waldur_resource, {"user1"}, user_roles={"user1": "PROJECT.ADMIN"},
        )
        assert "user1" in result
        mock_client.remove_user_from_project.assert_called_once()

    def test_user_resolution_caching(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        # First call
        backend.add_users_to_resource(waldur_resource, {"user1"})
        # Second call should use cache
        backend.add_users_to_resource(waldur_resource, {"user1"})

        # resolve should only be called once due to caching
        assert mock_client.resolve_user_via_identity_bridge.call_count == 1

    def test_resolve_remote_user_does_not_cache_none(self, backend, mock_client):
        """When user resolution fails, None should NOT be cached so retries work."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        # First call: user not found
        mock_client.resolve_user_via_identity_bridge.return_value = None
        result = backend._resolve_remote_user("unknown_user")
        assert result is None

        # Second call: user now exists
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID
        result = backend._resolve_remote_user("unknown_user")
        assert result == USER_UUID

        # resolve should be called twice (None was not cached)
        assert mock_client.resolve_user_via_identity_bridge.call_count == 2

    def test_resolve_via_identity_bridge(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_resolve_method"] = "identity_bridge"
        backend_settings["identity_bridge_source"] = "isd:test"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        result = backend._resolve_remote_user("user-cuid")
        assert result == USER_UUID
        mock_client.resolve_user_via_identity_bridge.assert_called_once_with(
            "user-cuid", "isd:test", attributes=None,
        )
        mock_client.resolve_user_by_cuid.assert_not_called()

    def test_resolve_via_identity_bridge_missing_source(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_resolve_method"] = "identity_bridge"
        backend_settings["identity_bridge_source"] = ""
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        result = backend._resolve_remote_user("user-cuid")
        assert result is None
        mock_client.resolve_user_via_identity_bridge.assert_not_called()

    def test_resolve_via_remote_eduteams(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_resolve_method"] = "remote_eduteams"
        backend_settings["user_match_field"] = "cuid"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_client.resolve_user_by_cuid.return_value = USER_UUID

        result = backend._resolve_remote_user("user-cuid")
        assert result == USER_UUID
        mock_client.resolve_user_by_cuid.assert_called_once_with("user-cuid")
        mock_client.resolve_user_via_identity_bridge.assert_not_called()

    def test_resolve_via_user_field(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_resolve_method"] = "user_field"
        backend_settings["user_match_field"] = "email"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_client.resolve_user_by_field.return_value = USER_UUID

        result = backend._resolve_remote_user("user@example.com")
        assert result == USER_UUID
        mock_client.resolve_user_by_field.assert_called_once_with(
            "user@example.com", "email"
        )

    def test_resolve_via_user_field_defaults_to_username(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["user_resolve_method"] = "user_field"
        backend_settings["user_match_field"] = "cuid"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_client.resolve_user_by_field.return_value = USER_UUID

        result = backend._resolve_remote_user("user-cuid")
        assert result == USER_UUID
        mock_client.resolve_user_by_field.assert_called_once_with(
            "user-cuid", "username"
        )

    def test_add_users_uses_cuid_for_identity_bridge(self, backend, mock_client):
        """add_users_to_resource should resolve via CUID, not offering username."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        user_cuids = {"testuser": "testuser-cuid@idp.example.org"}
        result = backend.add_users_to_resource(
            waldur_resource, {"testuser"}, user_cuids=user_cuids,
        )
        assert "testuser" in result
        # Identity bridge should be called with the CUID, not the offering username
        mock_client.resolve_user_via_identity_bridge.assert_called_once_with(
            "testuser-cuid@idp.example.org", "isd:test", attributes=None,
        )

    def test_add_users_passes_attributes_to_identity_bridge(self, backend, mock_client):
        """add_users_to_resource should forward user attributes to identity bridge."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        user_cuids = {"testuser": "testuser-cuid@idp.example.org"}
        user_attributes = {"testuser": {"email": "sz@example.com", "first_name": "S"}}
        result = backend.add_users_to_resource(
            waldur_resource, {"testuser"},
            user_cuids=user_cuids, user_attributes=user_attributes,
        )
        assert "testuser" in result
        mock_client.resolve_user_via_identity_bridge.assert_called_once_with(
            "testuser-cuid@idp.example.org", "isd:test",
            attributes={"email": "sz@example.com", "first_name": "S"},
        )

    def test_add_users_falls_back_to_offering_username_without_cuid(
        self, backend, mock_client,
    ):
        """Without user_cuids, offering username is used as fallback."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_users_to_resource(waldur_resource, {"testuser"})
        assert "testuser" in result
        mock_client.resolve_user_via_identity_bridge.assert_called_once_with(
            "testuser", "isd:test", attributes=None,
        )

    def test_remove_users_uses_cuid_for_identity_bridge(self, backend, mock_client):
        """remove_users_from_resource should resolve via CUID."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID
        mock_client.list_project_users.return_value = []

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        user_cuids = {"testuser": "testuser-cuid@idp.example.org"}
        result = backend.remove_users_from_resource(
            waldur_resource,
            {"testuser"},
            user_cuids=user_cuids,
            user_roles={"testuser": "PROJECT.ADMIN"},
        )
        assert "testuser" in result
        mock_client.resolve_user_via_identity_bridge.assert_called_once_with(
            "testuser-cuid@idp.example.org", "isd:test", attributes=None,
        )


class TestAddRemoveUser:
    """Tests for single-user add_user/remove_user methods and role mapping."""

    def test_add_user_calls_add_user_to_project(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_user(waldur_resource, "user1")
        assert result is True
        mock_client.add_user_to_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.ADMIN",
        )

    def test_add_user_passes_role_name(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_user(waldur_resource, "user1", role_name="PROJECT.MANAGER")
        assert result is True
        mock_client.add_user_to_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MANAGER",
        )

    def test_add_user_returns_false_when_user_not_found(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_user(waldur_resource, "user1")
        assert result is False
        mock_client.add_user_to_project.assert_not_called()

    def test_add_user_returns_false_when_project_not_found(self, backend, mock_client):
        mock_client.get_marketplace_resource.side_effect = Exception("Not found")

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_user(waldur_resource, "user1")
        assert result is False

    def test_remove_user_calls_remove_user_from_project(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.remove_user(waldur_resource, "user1", role_name="PROJECT.MANAGER")
        assert result is True
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MANAGER",
        )


class TestRoleMapping:
    """Tests for role_mapping backend setting."""

    def test_no_mapping_passes_role_through(self, backend):
        assert backend._map_role("PROJECT.MANAGER") == "PROJECT.MANAGER"
        assert backend._map_role("PROJECT.ADMIN") == "PROJECT.ADMIN"

    def test_mapping_translates_role(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {
            "PROJECT.MANAGER": "PROJECT.ADMIN",
            "PROJECT.ADMIN": "PROJECT.ADMIN",
        }
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        assert backend._map_role("PROJECT.MANAGER") == "PROJECT.ADMIN"
        assert backend._map_role("PROJECT.ADMIN") == "PROJECT.ADMIN"

    def test_unmapped_role_passes_through(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {
            "PROJECT.MANAGER": "PROJECT.ADMIN",
        }
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        # PROJECT.ADMIN is not in the mapping, so it passes through
        assert backend._map_role("PROJECT.ADMIN") == "PROJECT.ADMIN"

    def test_role_mapping_used_in_add_user(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {
            "PROJECT.MANAGER": "PROJECT.ADMIN",
        }
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        backend.add_user(waldur_resource, "user1", role_name="PROJECT.MANAGER")
        mock_client.add_user_to_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.ADMIN",  # mapped from PROJECT.MANAGER
        )

    def test_role_mapping_used_in_remove_user(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {
            "PROJECT.MANAGER": "PROJECT.ADMIN",
        }
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        backend.remove_user(waldur_resource, "user1", role_name="PROJECT.MANAGER")
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.ADMIN",  # mapped from PROJECT.MANAGER
        )

    def test_empty_role_mapping_is_passthrough(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {}
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        assert backend._map_role("PROJECT.MANAGER") == "PROJECT.MANAGER"


class TestRemoveUsersRoleHandling:
    """Tests for role-aware bulk REMOVE in remove_users_from_resource."""

    def _setup_resource(self, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID
        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)
        return waldur_resource

    def test_remove_forwards_role_from_user_roles(self, backend, mock_client):
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = []

        backend.remove_users_from_resource(
            waldur_resource,
            {"alice"},
            user_roles={"alice": "PROJECT.MANAGER"},
        )
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MANAGER",
        )

    def test_remove_falls_back_to_b_side_role(self, backend, mock_client):
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = [
            MagicMock(user_uuid=USER_UUID, role_name="PROJECT.MEMBER"),
        ]

        backend.remove_users_from_resource(waldur_resource, {"alice"})
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MEMBER",
        )

    def test_remove_applies_role_mapping(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        backend_settings["role_mapping"] = {"PROJECT.MANAGER": "PROJECT.ADMIN"}
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = []

        backend.remove_users_from_resource(
            waldur_resource,
            {"alice"},
            user_roles={"alice": "PROJECT.MANAGER"},
        )
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.ADMIN",
        )

    def test_remove_skips_when_role_unknown(self, backend, mock_client):
        """No user_roles entry and user not in B's project list -> skip."""
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = []

        result = backend.remove_users_from_resource(waldur_resource, {"alice"})
        assert result == []
        mock_client.remove_user_from_project.assert_not_called()


class TestReconcileExistingUserRoles:
    """Tests for role reconciliation of existing project members."""

    def _setup_resource(self, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_via_identity_bridge.return_value = USER_UUID
        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)
        return waldur_resource

    def test_noop_without_user_roles(self, backend, mock_client):
        waldur_resource = self._setup_resource(mock_client)
        backend.reconcile_existing_user_roles(waldur_resource, {"alice"}, {}, {})
        mock_client.list_project_users.assert_not_called()
        mock_client.remove_user_from_project.assert_not_called()
        mock_client.add_user_to_project.assert_not_called()

    def test_skips_when_role_matches(self, backend, mock_client):
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = [
            MagicMock(user_uuid=USER_UUID, role_name="PROJECT.MANAGER"),
        ]

        backend.reconcile_existing_user_roles(
            waldur_resource,
            {"alice"},
            {"alice": "PROJECT.MANAGER"},
            {},
        )
        mock_client.remove_user_from_project.assert_not_called()
        mock_client.add_user_to_project.assert_not_called()

    def test_updates_when_role_differs(self, backend, mock_client):
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = [
            MagicMock(user_uuid=USER_UUID, role_name="PROJECT.MEMBER"),
        ]

        backend.reconcile_existing_user_roles(
            waldur_resource,
            {"alice"},
            {"alice": "PROJECT.MANAGER"},
            {},
        )
        mock_client.remove_user_from_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MEMBER",
        )
        mock_client.add_user_to_project.assert_called_once_with(
            project_uuid=PROJECT_UUID,
            user_uuid=USER_UUID,
            role_name="PROJECT.MANAGER",
        )

    def test_role_mapping_applied_to_comparison(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """When role_mapping makes A→B equivalent, no update is issued."""
        backend_settings["role_mapping"] = {"PROJECT.MANAGER": "PROJECT.ADMIN"}
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = [
            MagicMock(user_uuid=USER_UUID, role_name="PROJECT.ADMIN"),
        ]

        backend.reconcile_existing_user_roles(
            waldur_resource,
            {"alice"},
            {"alice": "PROJECT.MANAGER"},
            {},
        )
        # Mapped role matches B-side role — no remove/add issued
        mock_client.remove_user_from_project.assert_not_called()
        mock_client.add_user_to_project.assert_not_called()

    def test_skips_user_missing_from_b(self, backend, mock_client):
        """User in existing set but not in B's project list — skip silently."""
        waldur_resource = self._setup_resource(mock_client)
        mock_client.list_project_users.return_value = []

        backend.reconcile_existing_user_roles(
            waldur_resource,
            {"alice"},
            {"alice": "PROJECT.MANAGER"},
            {},
        )
        mock_client.remove_user_from_project.assert_not_called()
        mock_client.add_user_to_project.assert_not_called()


class TestAttributePassthrough:
    """Tests for passthrough_attributes forwarding in create_resource_with_id."""

    def _setup_create_mocks(self, mock_client):
        """Configure mocks for create_resource_with_id calls."""
        mock_client.find_or_create_project.return_value = {
            "uuid": str(PROJECT_UUID),
            "url": f"/api/projects/{PROJECT_UUID}/",
            "name": "Test Project",
        }
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": str(PROJECT_UUID),
        }
        mock_client.get_project_url.return_value = f"/api/projects/{PROJECT_UUID}/"
        mock_client.get_offering_url.return_value = "/api/offerings/off-uuid/"
        mock_client.get_customer_url.return_value = "/api/customers/cust-uuid/"

        mock_order = MagicMock()
        mock_order.uuid = ORDER_UUID
        mock_order.marketplace_resource_uuid = RESOURCE_UUID
        mock_client.create_marketplace_order.return_value = mock_order

    def _make_waldur_resource(self, attributes=UNSET):
        """Create a mock WaldurResource with optional attributes."""
        resource = MagicMock()
        resource.uuid = "source-resource-uuid"
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_name = "Test Project"
        resource.name = "Test Resource"
        resource.end_date = None
        resource.limits = MagicMock()
        resource.limits.__contains__ = lambda self, key: key in {"cpu", "mem"}
        resource.limits.__getitem__ = lambda self, key: {"cpu": 100, "mem": 200}[key]
        resource.attributes = attributes
        return resource

    def test_create_resource_passes_through_attributes(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """Only configured keys from passthrough_attributes are forwarded."""
        backend_settings["passthrough_attributes"] = ["storage_data_type"]
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client
        self._setup_create_mocks(mock_client)

        mock_attrs = MagicMock()
        mock_attrs.to_dict.return_value = {
            "storage_data_type": "Store",
            "other_field": "should-not-be-forwarded",
        }
        waldur_resource = self._make_waldur_resource(attributes=mock_attrs)

        backend.create_resource_with_id(waldur_resource, "test-backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert call_kwargs["attributes"]["name"] == "Test Resource"
        assert call_kwargs["attributes"]["storage_data_type"] == "Store"
        assert "other_field" not in call_kwargs["attributes"]

    def test_create_resource_passthrough_empty_list(self, backend, mock_client):
        """Default config (no passthrough_attributes) sends only name."""
        self._setup_create_mocks(mock_client)

        mock_attrs = MagicMock()
        mock_attrs.to_dict.return_value = {"storage_data_type": "Store"}
        waldur_resource = self._make_waldur_resource(attributes=mock_attrs)

        backend.create_resource_with_id(waldur_resource, "test-backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert call_kwargs["attributes"] == {"name": "Test Resource"}

    def test_create_resource_passthrough_missing_attribute(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """Missing attribute key is silently skipped."""
        backend_settings["passthrough_attributes"] = ["nonexistent_key"]
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client
        self._setup_create_mocks(mock_client)

        mock_attrs = MagicMock()
        mock_attrs.to_dict.return_value = {"storage_data_type": "Store"}
        waldur_resource = self._make_waldur_resource(attributes=mock_attrs)

        backend.create_resource_with_id(waldur_resource, "test-backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert call_kwargs["attributes"] == {"name": "Test Resource"}

    def test_create_resource_passthrough_with_unset_attributes(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """UNSET resource attributes don't crash."""
        backend_settings["passthrough_attributes"] = ["storage_data_type"]
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client
        self._setup_create_mocks(mock_client)

        waldur_resource = self._make_waldur_resource(attributes=UNSET)

        backend.create_resource_with_id(waldur_resource, "test-backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert call_kwargs["attributes"] == {"name": "Test Resource"}

    def test_create_resource_uniqueness_error_handling(
        self, backend, mock_client
    ):
        """400 from target Waldur becomes BackendError with detail."""
        self._setup_create_mocks(mock_client)
        error_body = b'{"attributes":["Resource with this storage_data_type already exists"]}'
        mock_client.create_marketplace_order.side_effect = UnexpectedStatus(
            status_code=400,
            content=error_body,
            url=URL("https://waldur-b.example.com/api/marketplace-orders/"),
        )

        waldur_resource = self._make_waldur_resource()

        with pytest.raises(BackendError, match="Target Waldur rejected order"):
            backend.create_resource_with_id(waldur_resource, "test-backend-id")


class TestEndDateForwardingOnCreate:
    """Tests that end_date is forwarded to Waldur B during order creation."""

    def _setup_create_mocks(self, mock_client):
        mock_client.find_or_create_project.return_value = {
            "uuid": str(PROJECT_UUID),
            "url": f"/api/projects/{PROJECT_UUID}/",
            "name": "Test Project",
        }
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": str(PROJECT_UUID),
        }
        mock_client.get_project_url.return_value = f"/api/projects/{PROJECT_UUID}/"
        mock_client.get_offering_url.return_value = "/api/offerings/off-uuid/"
        mock_client.get_customer_url.return_value = "/api/customers/cust-uuid/"

        mock_order = MagicMock()
        mock_order.uuid = ORDER_UUID
        mock_order.marketplace_resource_uuid = RESOURCE_UUID
        mock_client.create_marketplace_order.return_value = mock_order

    def test_end_date_forwarded_to_target_order(
        self, backend, mock_client
    ):
        """When source resource has end_date, it is included in target order attributes."""
        self._setup_create_mocks(mock_client)

        resource = MagicMock()
        resource.uuid = "source-uuid"
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_name = "Test Project"
        resource.name = "Test Resource"
        resource.end_date = datetime.date(2026, 12, 31)
        resource.limits = {"cpu": 10, "mem": 20}
        resource.attributes = UNSET
        resource.offering_plugin_options = {}

        backend.create_resource_with_id(resource, "backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert call_kwargs["attributes"]["end_date"] == "2026-12-31"

    def test_no_end_date_when_source_has_none(
        self, backend, mock_client
    ):
        """When source resource has no end_date, attributes omit it."""
        self._setup_create_mocks(mock_client)

        resource = MagicMock()
        resource.uuid = "source-uuid"
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_name = "Test Project"
        resource.name = "Test Resource"
        resource.end_date = None
        resource.limits = {"cpu": 10, "mem": 20}
        resource.attributes = UNSET
        resource.offering_plugin_options = {}

        backend.create_resource_with_id(resource, "backend-id")

        call_kwargs = mock_client.create_marketplace_order.call_args.kwargs
        assert "end_date" not in call_kwargs["attributes"]


class TestNoOpMethods:
    def test_downscale_returns_true(self, backend):
        assert backend.downscale_resource("some-uuid") is True

    def test_pause_returns_true(self, backend):
        assert backend.pause_resource("some-uuid") is True

    def test_restore_returns_true(self, backend):
        assert backend.restore_resource("some-uuid") is True

    def test_get_resource_metadata(self, backend):
        metadata = backend.get_resource_metadata("some-uuid")
        assert metadata == {"waldur_b_resource_uuid": "some-uuid"}


WALDUR_A_OFFERING_UUID = "aaaaaaaa-0000-0000-0000-000000000001"
WALDUR_B_USER_UUID = UUID("bbbbbbbb-0000-0000-0000-000000000001")
WALDUR_B_USER_UUID_2 = UUID("bbbbbbbb-0000-0000-0000-000000000002")
WALDUR_A_OU_UUID = UUID("cccccccc-0000-0000-0000-000000000001")
WALDUR_A_OU_UUID_2 = UUID("cccccccc-0000-0000-0000-000000000002")


def _make_offering_user(
    uuid, user_username, username=UNSET, user_uuid=UNSET, state="OK"
):
    """Create a mock OfferingUser with the given fields."""
    ou = MagicMock()
    ou.uuid = uuid
    ou.user_username = user_username
    ou.username = username
    ou.user_uuid = user_uuid
    ou.state = state
    return ou


# Import the actual modules so we can patch their functions with patch.object
from waldur_api_client.api.marketplace_offering_users import (  # noqa: E402
    marketplace_offering_users_list as _ou_list_mod,
    marketplace_offering_users_partial_update as _ou_update_mod,
)


class TestSyncOfferingUserUsernames:
    """Tests for sync_offering_user_usernames pulling usernames from Waldur B."""

    def test_updates_username_from_waldur_b(self, backend, mock_client):
        """When Waldur B has a username and Waldur A differs, update Waldur A."""
        waldur_b_ou = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000001"),
            user_username="bob@idp.org",
            username="slurm_bob",
            user_uuid=WALDUR_B_USER_UUID,
        )
        mock_client.list_offering_users.return_value = [waldur_b_ou]

        waldur_a_ou = _make_offering_user(
            uuid=WALDUR_A_OU_UUID,
            user_username="bob@idp.org",
            username="bob@idp.org",
        )

        with (
            patch.object(_ou_list_mod, "sync_all", return_value=[waldur_a_ou]),
            patch.object(_ou_update_mod, "sync") as mock_update,
        ):
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is True
        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args
        assert call_kwargs.kwargs["uuid"] == WALDUR_A_OU_UUID
        assert call_kwargs.kwargs["body"].username == "slurm_bob"

    def test_skips_when_no_waldur_b_offering_users(self, backend, mock_client):
        """When no OK offering users on Waldur B, return False."""
        mock_client.list_offering_users.return_value = []

        with patch.object(_ou_list_mod, "sync_all") as mock_list:
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is False
        mock_list.assert_not_called()

    def test_skips_when_user_not_on_waldur_b(self, backend, mock_client):
        """When Waldur A user has no matching offering user on Waldur B, skip."""
        waldur_b_ou = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000001"),
            user_username="bob@idp.org",
            username="slurm_bob",
            user_uuid=WALDUR_B_USER_UUID,
        )
        mock_client.list_offering_users.return_value = [waldur_b_ou]

        waldur_a_ou = _make_offering_user(
            uuid=WALDUR_A_OU_UUID,
            user_username="unknown@idp.org",
            username="unknown@idp.org",
        )

        with (
            patch.object(_ou_list_mod, "sync_all", return_value=[waldur_a_ou]),
            patch.object(_ou_update_mod, "sync") as mock_update,
        ):
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is False
        mock_update.assert_not_called()

    def test_skips_when_username_already_matches(self, backend, mock_client):
        """When Waldur A already has the Waldur B username, no update needed."""
        waldur_b_ou = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000001"),
            user_username="bob@idp.org",
            username="slurm_bob",
            user_uuid=WALDUR_B_USER_UUID,
        )
        mock_client.list_offering_users.return_value = [waldur_b_ou]

        waldur_a_ou = _make_offering_user(
            uuid=WALDUR_A_OU_UUID,
            user_username="bob@idp.org",
            username="slurm_bob",
        )

        with (
            patch.object(_ou_list_mod, "sync_all", return_value=[waldur_a_ou]),
            patch.object(_ou_update_mod, "sync") as mock_update,
        ):
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is False
        mock_update.assert_not_called()

    def test_handles_api_failure_gracefully(self, backend, mock_client):
        """When Waldur B API fails, return False without crashing."""
        mock_client.list_offering_users.side_effect = Exception("API down")

        result = backend.sync_offering_user_usernames(
            WALDUR_A_OFFERING_UUID, MagicMock()
        )

        assert result is False

    def test_updates_creating_user(self, backend, mock_client):
        """CREATING state users on Waldur A should also get updated."""
        waldur_b_ou = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000001"),
            user_username="alice@idp.org",
            username="hpc_alice",
            user_uuid=WALDUR_B_USER_UUID,
        )
        mock_client.list_offering_users.return_value = [waldur_b_ou]

        waldur_a_ou = _make_offering_user(
            uuid=WALDUR_A_OU_UUID,
            user_username="alice@idp.org",
            username=None,
            state="Creating",
        )

        with (
            patch.object(_ou_list_mod, "sync_all", return_value=[waldur_a_ou]),
            patch.object(_ou_update_mod, "sync") as mock_update,
        ):
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is True
        mock_update.assert_called_once()
        assert mock_update.call_args.kwargs["body"].username == "hpc_alice"

    def test_multiple_users_partial_match(self, backend, mock_client):
        """Only users with a Waldur B match get updated."""
        b_ou_1 = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000001"),
            user_username="alice@idp.org",
            username="hpc_alice",
            user_uuid=WALDUR_B_USER_UUID,
        )
        b_ou_2 = _make_offering_user(
            uuid=UUID("dd000000-0000-0000-0000-000000000002"),
            user_username="bob@idp.org",
            username="hpc_bob",
            user_uuid=WALDUR_B_USER_UUID_2,
        )
        mock_client.list_offering_users.return_value = [b_ou_1, b_ou_2]

        a_ou_1 = _make_offering_user(
            uuid=WALDUR_A_OU_UUID,
            user_username="alice@idp.org",
            username="alice@idp.org",
        )
        a_ou_2 = _make_offering_user(
            uuid=WALDUR_A_OU_UUID_2,
            user_username="charlie@idp.org",
            username="charlie@idp.org",
        )

        with (
            patch.object(
                _ou_list_mod, "sync_all", return_value=[a_ou_1, a_ou_2]
            ),
            patch.object(_ou_update_mod, "sync") as mock_update,
        ):
            result = backend.sync_offering_user_usernames(
                WALDUR_A_OFFERING_UUID, MagicMock()
            )

        assert result is True
        assert mock_update.call_count == 1
        assert mock_update.call_args.kwargs["body"].username == "hpc_alice"


class TestEndDateSync:
    """Tests for sync_resource_end_date bidirectional sync."""

    BACKEND_ID = "bbbbbbbb-1234-1234-1234-123456789abc"

    _SENTINEL = object()

    def _make_waldur_resource(self, end_date, updated_at, backend_id=_SENTINEL):
        resource = MagicMock()
        resource.uuid = RESOURCE_UUID
        resource.backend_id = self.BACKEND_ID if backend_id is self._SENTINEL else backend_id
        resource.end_date = end_date
        resource.end_date_updated_at = updated_at
        return resource

    def _make_b_resource(self, end_date, updated_at):
        resource = MagicMock()
        resource.end_date = end_date
        resource.end_date_updated_at = updated_at
        return resource

    def test_sync_end_date_a_newer(self, backend, mock_client):
        """A's timestamp newer → pushes A's end_date to B."""
        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)
        b_date = datetime.date(2025, 5, 1)
        b_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, b_ts)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_called_once_with(
            UUID(self.BACKEND_ID), a_date
        )

    def test_sync_end_date_b_newer(self, backend, mock_client):
        """B's timestamp newer → pushes B's end_date to A."""
        a_date = datetime.date(2025, 5, 1)
        a_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)
        b_date = datetime.date(2025, 6, 1)
        b_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, b_ts)

        waldur_rest_client = MagicMock()

        with patch(
            "waldur_api_client.api.marketplace_provider_resources."
            "marketplace_provider_resources_set_end_date.sync_detailed"
        ) as mock_set:
            backend.sync_resource_end_date(waldur_resource, waldur_rest_client)
            mock_set.assert_called_once()
            call_kwargs = mock_set.call_args
            assert call_kwargs.kwargs["uuid"] == RESOURCE_UUID
            assert call_kwargs.kwargs["body"].end_date == b_date

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_same_timestamp(self, backend, mock_client):
        """Equal timestamps → no API calls."""
        ts = datetime.datetime(2025, 1, 10, 12, 0, 0)
        a_date = datetime.date(2025, 6, 1)
        b_date = datetime.date(2025, 5, 1)

        waldur_resource = self._make_waldur_resource(a_date, ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, ts)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_both_null_timestamp(self, backend, mock_client):
        """Both timestamps null → no-op."""
        a_date = datetime.date(2025, 6, 1)
        b_date = datetime.date(2025, 5, 1)

        waldur_resource = self._make_waldur_resource(a_date, None)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, None)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_only_a_has_timestamp(self, backend, mock_client):
        """Only A has timestamp → A is authoritative, pushes to B."""
        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)
        b_date = datetime.date(2025, 5, 1)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, None)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_called_once_with(
            UUID(self.BACKEND_ID), a_date
        )

    def test_sync_end_date_only_b_has_timestamp(self, backend, mock_client):
        """Only B has timestamp → B is authoritative, pushes to A."""
        a_date = datetime.date(2025, 5, 1)
        b_date = datetime.date(2025, 6, 1)
        b_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, None)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, b_ts)

        waldur_rest_client = MagicMock()

        with patch(
            "waldur_api_client.api.marketplace_provider_resources."
            "marketplace_provider_resources_set_end_date.sync_detailed"
        ) as mock_set:
            backend.sync_resource_end_date(waldur_resource, waldur_rest_client)
            mock_set.assert_called_once()

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_clear_from_a(self, backend, mock_client):
        """A cleared end_date (None), A newer → clears B."""
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)
        b_date = datetime.date(2025, 5, 1)
        b_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(None, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(b_date, b_ts)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_called_once_with(
            UUID(self.BACKEND_ID), None
        )

    def test_sync_end_date_clear_from_b(self, backend, mock_client):
        """B cleared end_date (None), B newer → clears A."""
        a_date = datetime.date(2025, 5, 1)
        a_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)
        b_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(None, b_ts)

        waldur_rest_client = MagicMock()

        with patch(
            "waldur_api_client.api.marketplace_provider_resources."
            "marketplace_provider_resources_set_end_date.sync_detailed"
        ) as mock_set:
            backend.sync_resource_end_date(waldur_resource, waldur_rest_client)
            mock_set.assert_called_once()
            assert mock_set.call_args.kwargs["body"].end_date is None

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_neither_has_date(self, backend, mock_client):
        """Both end_dates None → no-op (short-circuit on equal)."""
        waldur_resource = self._make_waldur_resource(None, None)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(None, None)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_api_error(self, backend, mock_client):
        """SDK error (UnexpectedStatus) → logs warning, doesn't crash."""
        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.side_effect = UnexpectedStatus(
            500, b"err", "https://b/api/marketplace-resources/"
        )

        waldur_rest_client = MagicMock()
        # Should not raise
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_same_end_date(self, backend, mock_client):
        """Both have same end_date → no API calls regardless of timestamps."""
        same_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)
        b_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(same_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(same_date, b_ts)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_unset_backend_id(self, backend, mock_client):
        """Resource with UNSET backend_id → no-op."""
        waldur_resource = self._make_waldur_resource(
            datetime.date(2025, 6, 1),
            datetime.datetime(2025, 1, 10, 12, 0, 0),
            backend_id=UNSET,
        )

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.get_marketplace_resource.assert_not_called()
        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_a_to_b_mode(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """a_to_b mode: A wins even when A has older timestamp."""
        backend_settings["end_date_sync_direction"] = "a_to_b"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)  # older
        b_date = datetime.date(2025, 5, 1)
        b_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)  # newer

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(
            b_date, b_ts
        )

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_called_once_with(
            UUID(self.BACKEND_ID), a_date
        )

    def test_sync_end_date_b_to_a_mode(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """b_to_a mode: B wins even when B has older timestamp."""
        backend_settings["end_date_sync_direction"] = "b_to_a"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)  # newer
        b_date = datetime.date(2025, 5, 1)
        b_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)  # older

        waldur_resource = self._make_waldur_resource(a_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(
            b_date, b_ts
        )

        waldur_rest_client = MagicMock()

        with patch(
            "waldur_api_client.api.marketplace_provider_resources."
            "marketplace_provider_resources_set_end_date.sync_detailed"
        ) as mock_set:
            backend.sync_resource_end_date(waldur_resource, waldur_rest_client)
            mock_set.assert_called_once()
            call_kwargs = mock_set.call_args
            assert call_kwargs.kwargs["uuid"] == RESOURCE_UUID
            assert call_kwargs.kwargs["body"].end_date == b_date

        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_disabled_mode(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """disabled mode: no API calls made at all."""
        backend_settings["end_date_sync_direction"] = "disabled"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        a_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(a_date, a_ts)

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.get_marketplace_resource.assert_not_called()
        mock_client.set_resource_end_date.assert_not_called()

    def test_sync_end_date_a_to_b_same_date(
        self, backend_settings, backend_components_passthrough, mock_client
    ):
        """a_to_b mode: no API calls when dates already match."""
        backend_settings["end_date_sync_direction"] = "a_to_b"
        backend = WaldurBackend(backend_settings, backend_components_passthrough)
        backend.client = mock_client

        same_date = datetime.date(2025, 6, 1)
        a_ts = datetime.datetime(2025, 1, 5, 12, 0, 0)
        b_ts = datetime.datetime(2025, 1, 10, 12, 0, 0)

        waldur_resource = self._make_waldur_resource(same_date, a_ts)
        mock_client.get_marketplace_resource.return_value = self._make_b_resource(
            same_date, b_ts
        )

        waldur_rest_client = MagicMock()
        backend.sync_resource_end_date(waldur_resource, waldur_rest_client)

        mock_client.set_resource_end_date.assert_not_called()


class TestSyncResourceProject:
    """Tests for sync_resource_project updating project description on Waldur B."""

    def test_updates_description_when_changed(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Old description",
        }

        resource = MagicMock()
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_description = "New description"

        backend.sync_resource_project(resource)

        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid", description="New description"
        )

    def test_skips_update_when_description_matches(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
        }

        resource = MagicMock()
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_description = "Same description"

        backend.sync_resource_project(resource)

        mock_client.update_project.assert_not_called()

    def test_skips_when_no_project_uuid(self, backend, mock_client):
        resource = MagicMock()
        resource.project_uuid = None

        backend.sync_resource_project(resource)

        mock_client.find_project_by_backend_id.assert_not_called()

    def test_skips_when_project_not_found(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = None

        resource = MagicMock()
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_description = "Some description"

        backend.sync_resource_project(resource)

        mock_client.update_project.assert_not_called()

    def test_skips_when_description_is_unset(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Existing",
        }

        resource = MagicMock()
        resource.project_uuid = "proj-uuid-a"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_description = UNSET

        backend.sync_resource_project(resource)

        mock_client.update_project.assert_not_called()

    def _resource(self):
        resource = MagicMock()
        resource.project_uuid = "11111111-1111-1111-1111-111111111111"
        resource.customer_uuid = "cust-uuid-a"
        resource.project_description = "Same description"
        return resource

    def test_syncs_oecd_and_is_industry_from_source(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": False,
        }

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = OecdFos2007CodeEnum("1.2")
        a_project.is_industry = True
        a_project.science_sub_domain_code = None

        backend.sync_resource_project(self._resource(), a_project)

        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid",
            oecd_fos_2007_code=OecdFos2007CodeEnum("1.2"),
            is_industry=True,
        )

    def test_skips_metadata_when_unchanged(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": True,
        }

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = OecdFos2007CodeEnum("1.1")
        a_project.is_industry = True
        a_project.science_sub_domain_code = None

        backend.sync_resource_project(self._resource(), a_project)

        mock_client.update_project.assert_not_called()

    def test_none_source_project_syncs_only_description(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Old description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": False,
        }

        resource = self._resource()
        resource.project_description = "New description"

        # source_project is None (core could not fetch it); description still syncs.
        backend.sync_resource_project(resource, None)

        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid", description="New description"
        )

    def test_clears_oecd_on_b_when_source_is_blank(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": False,
        }

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = None
        a_project.is_industry = False
        a_project.science_sub_domain_code = None

        backend.sync_resource_project(self._resource(), a_project)

        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid", oecd_fos_2007_code=None
        )

    def test_syncs_science_sub_domain_resolved_to_b_uuid(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": True,
            "science_sub_domain_code": "1.1",
        }
        b_sub_domain_uuid = UUID("22222222-2222-2222-2222-222222222222")
        mock_client.find_science_sub_domain_by_code.return_value = b_sub_domain_uuid

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = OecdFos2007CodeEnum("1.1")
        a_project.is_industry = True
        a_project.science_sub_domain_code = "1.2"

        backend.sync_resource_project(self._resource(), a_project)

        mock_client.find_science_sub_domain_by_code.assert_called_once_with("1.2")
        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid", science_sub_domain=b_sub_domain_uuid
        )

    def test_skips_science_sub_domain_when_code_missing_on_b(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": True,
            "science_sub_domain_code": "1.1",
        }
        # No sub-domain with this code exists on Waldur B.
        mock_client.find_science_sub_domain_by_code.return_value = None

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = OecdFos2007CodeEnum("1.1")
        a_project.is_industry = True
        a_project.science_sub_domain_code = "1.2"

        backend.sync_resource_project(self._resource(), a_project)

        mock_client.find_science_sub_domain_by_code.assert_called_once_with("1.2")
        # Unresolvable code is skipped, not pushed, so nothing else changed either.
        mock_client.update_project.assert_not_called()

    def test_clears_science_sub_domain_when_source_is_blank(self, backend, mock_client):
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Same description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": True,
            "science_sub_domain_code": "1.1",
        }

        a_project = MagicMock()
        a_project.oecd_fos_2007_code = OecdFos2007CodeEnum("1.1")
        a_project.is_industry = True
        a_project.science_sub_domain_code = None

        backend.sync_resource_project(self._resource(), a_project)

        # Clearing needs no lookup; B's sub-domain is unset directly.
        mock_client.find_science_sub_domain_by_code.assert_not_called()
        mock_client.update_project.assert_called_once_with(
            "proj-b-uuid", science_sub_domain=None
        )

    def test_write_failure_does_not_propagate(self, backend, mock_client):
        # A failed metadata PATCH on B must not bubble up, otherwise the
        # membership-sync cycle for this resource would be aborted.
        mock_client.find_project_by_backend_id.return_value = {
            "uuid": "proj-b-uuid",
            "url": "/api/projects/proj-b-uuid/",
            "name": "Test Project",
            "description": "Old description",
            "oecd_fos_2007_code": "1.1",
            "is_industry": False,
        }
        mock_client.update_project.side_effect = UnexpectedStatus(
            500, b"error", URL("https://waldur-b.example.com/api/projects/")
        )

        resource = self._resource()
        resource.project_description = "New description"

        # Must not raise.
        backend.sync_resource_project(resource)

        mock_client.update_project.assert_called_once()


class TestProjectEndDateSync:
    """Tests for sync_project_end_date (mirrors resource end_date sync)."""

    B_PROJECT_UUID = UUID("bbbbbbbb-1234-1234-1234-123456789abc")

    def _make_resource(
        self, project_end_date, project_uuid=PROJECT_UUID, customer_uuid="cust-uuid-a"
    ):
        resource = MagicMock()
        resource.backend_id = "rb-1"
        resource.project_uuid = project_uuid
        resource.customer_uuid = customer_uuid
        resource.project_end_date = project_end_date
        return resource

    def _make_b_project(self, end_date, updated_at=None):
        project = MagicMock()
        project.uuid = self.B_PROJECT_UUID
        project.end_date = end_date
        project.end_date_updated_at = updated_at
        return project

    def test_a_to_b_pushes_to_b(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B
        a_date = datetime.date(2025, 6, 1)
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(
            datetime.date(2025, 5, 1)
        )

        backend.sync_project_end_date(self._make_resource(a_date), MagicMock())

        mock_client.set_project_end_date.assert_called_once_with(self.B_PROJECT_UUID, a_date)

    def test_b_to_a_pushes_to_a(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.B_TO_A
        b_date = datetime.date(2025, 5, 1)
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(b_date)

        with patch(
            "waldur_api_client.api.projects.projects_partial_update.sync_detailed"
        ) as mock_patch:
            backend.sync_project_end_date(
                self._make_resource(datetime.date(2025, 6, 1)), MagicMock()
            )
            mock_patch.assert_called_once()
            assert mock_patch.call_args.kwargs["uuid"] == PROJECT_UUID
            assert mock_patch.call_args.kwargs["body"].end_date == b_date
        mock_client.set_project_end_date.assert_not_called()

    def test_b_to_a_permission_denied_is_logged_not_raised(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.B_TO_A
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(
            datetime.date(2025, 5, 1)
        )

        with patch(
            "waldur_api_client.api.projects.projects_partial_update.sync_detailed",
            side_effect=UnexpectedStatus(403, b"forbidden", "https://a/api/projects/x/"),
        ) as mock_patch:
            backend.sync_project_end_date(
                self._make_resource(datetime.date(2025, 6, 1)), MagicMock()
            )
            mock_patch.assert_called_once()

    def test_bidirectional_a_newer_pushes_to_b(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.BIDIRECTIONAL
        a_date = datetime.date(2025, 6, 1)
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(
            datetime.date(2025, 5, 1), datetime.datetime(2025, 1, 5, 12, 0, 0)
        )
        a_project = MagicMock()
        a_project.end_date_updated_at = datetime.datetime(2025, 1, 10, 12, 0, 0)

        backend.sync_project_end_date(self._make_resource(a_date), MagicMock(), a_project)

        mock_client.set_project_end_date.assert_called_once_with(self.B_PROJECT_UUID, a_date)

    def test_bidirectional_b_newer_pushes_to_a(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.BIDIRECTIONAL
        b_date = datetime.date(2025, 6, 1)
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(
            b_date, datetime.datetime(2025, 1, 10, 12, 0, 0)
        )
        a_project = MagicMock()
        a_project.end_date_updated_at = datetime.datetime(2025, 1, 5, 12, 0, 0)

        with patch(
            "waldur_api_client.api.projects.projects_partial_update.sync_detailed"
        ) as mock_patch:
            backend.sync_project_end_date(
                self._make_resource(datetime.date(2025, 5, 1)), MagicMock(), a_project
            )
            mock_patch.assert_called_once()
            assert mock_patch.call_args.kwargs["body"].end_date == b_date
        mock_client.set_project_end_date.assert_not_called()

    def test_bidirectional_both_null_timestamp_noop(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.BIDIRECTIONAL
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(
            datetime.date(2025, 5, 1), None
        )
        a_project = MagicMock()
        a_project.end_date_updated_at = None

        backend.sync_project_end_date(
            self._make_resource(datetime.date(2025, 6, 1)), MagicMock(), a_project
        )

        mock_client.set_project_end_date.assert_not_called()

    def test_disabled_is_noop(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.DISABLED

        backend.sync_project_end_date(
            self._make_resource(datetime.date(2025, 6, 1)), MagicMock()
        )

        mock_client.get_project_by_backend_id.assert_not_called()
        mock_client.set_project_end_date.assert_not_called()

    def test_equal_dates_short_circuit(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B
        same = datetime.date(2025, 6, 1)
        mock_client.get_project_by_backend_id.return_value = self._make_b_project(same)

        backend.sync_project_end_date(self._make_resource(same), MagicMock())

        mock_client.set_project_end_date.assert_not_called()

    def test_missing_project_uuid_noop(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B

        backend.sync_project_end_date(
            self._make_resource(datetime.date(2025, 6, 1), project_uuid=None), MagicMock()
        )

        mock_client.get_project_by_backend_id.assert_not_called()

    def test_b_project_not_found_noop(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B
        mock_client.get_project_by_backend_id.return_value = None

        backend.sync_project_end_date(
            self._make_resource(datetime.date(2025, 6, 1)), MagicMock()
        )

        mock_client.set_project_end_date.assert_not_called()

    def test_unexpected_status_is_caught(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B
        mock_client.get_project_by_backend_id.side_effect = UnexpectedStatus(
            500, b"err", "https://b/api/projects/"
        )

        # SDK errors during the sync are caught and logged, not propagated.
        backend.sync_project_end_date(
            self._make_resource(datetime.date(2025, 6, 1)), MagicMock()
        )

    def test_pre_create_passes_project_end_date(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.A_TO_B
        end_date = datetime.date(2025, 9, 1)
        resource = MagicMock()
        resource.project_uuid = PROJECT_UUID
        resource.customer_uuid = "cust-uuid-a"
        resource.project_name = "Test Project"
        resource.project_description = "desc"
        resource.project_end_date = end_date
        mock_client.find_or_create_project.return_value = {"uuid": "proj-b-uuid"}

        backend._pre_create_resource(resource)

        assert mock_client.find_or_create_project.call_args.kwargs["end_date"] == end_date

    def test_pre_create_disabled_passes_none(self, backend, mock_client):
        backend.end_date_sync_direction = EndDateSyncDirection.DISABLED
        resource = MagicMock()
        resource.project_uuid = PROJECT_UUID
        resource.customer_uuid = "cust-uuid-a"
        resource.project_name = "Test Project"
        resource.project_description = "desc"
        resource.project_end_date = datetime.date(2025, 9, 1)
        mock_client.find_or_create_project.return_value = {"uuid": "proj-b-uuid"}

        backend._pre_create_resource(resource)

        assert mock_client.find_or_create_project.call_args.kwargs["end_date"] is None
