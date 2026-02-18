"""Tests for WaldurBackend with mocked WaldurClient."""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from httpx import URL

from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from waldur_site_agent_waldur.backend import WaldurBackend

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


class TestResourceDeletion:
    def test_delete_resource(self, backend, mock_client):
        mock_client.get_resource.return_value = MagicMock()
        mock_client.create_terminate_order.return_value = ORDER_UUID
        mock_completed = MagicMock()
        mock_completed.state = OrderState.DONE
        mock_client.poll_order_completion.return_value = mock_completed

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_called_once()

    def test_delete_resource_not_found(self, backend, mock_client):
        mock_client.get_resource.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = "nonexistent-uuid"

        # Should not raise, just skip
        backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_not_called()

    def test_delete_resource_empty_backend_id(self, backend, mock_client):
        waldur_resource = MagicMock()
        waldur_resource.backend_id = ""

        backend.delete_resource(waldur_resource)
        mock_client.create_terminate_order.assert_not_called()


class TestSetLimits:
    def test_set_resource_limits(self, backend, mock_client):
        mock_client.create_update_order.return_value = ORDER_UUID
        mock_completed = MagicMock()
        mock_completed.state = OrderState.DONE
        mock_client.poll_order_completion.return_value = mock_completed

        backend.set_resource_limits(str(RESOURCE_UUID), {"cpu": 200})
        mock_client.create_update_order.assert_called_once()

    def test_set_resource_limits_with_conversion(self, backend_with_conversion, mock_client):
        mock_client.create_update_order.return_value = ORDER_UUID
        mock_completed = MagicMock()
        mock_client.poll_order_completion.return_value = mock_completed

        backend_with_conversion.set_resource_limits(
            str(RESOURCE_UUID), {"node_hours": 100}
        )

        # Verify the limits were converted
        call_args = mock_client.create_update_order.call_args
        target_limits = call_args.kwargs["limits"]
        assert target_limits == {"gpu_hours": 500, "storage_gb_hours": 1000}


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


class TestUserSync:
    def test_add_users_to_resource(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        mock_client.resolve_user_by_cuid.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.add_users_to_resource(waldur_resource, {"user1"})
        assert "user1" in result
        mock_client.add_user_to_project.assert_called_once()

    def test_add_users_user_not_found_warn(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_by_cuid.return_value = None

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
        mock_client.resolve_user_by_cuid.return_value = None

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        # Should not propagate - caught by add_users_to_resource
        result = backend.add_users_to_resource(waldur_resource, {"user1"})
        assert "user1" not in result

    def test_remove_users_from_resource(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        mock_client.resolve_user_by_cuid.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        result = backend.remove_users_from_resource(waldur_resource, {"user1"})
        assert "user1" in result
        mock_client.remove_user_from_project.assert_called_once()

    def test_user_resolution_caching(self, backend, mock_client):
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource
        mock_client.resolve_user_by_cuid.return_value = USER_UUID

        waldur_resource = MagicMock()
        waldur_resource.backend_id = str(RESOURCE_UUID)

        # First call
        backend.add_users_to_resource(waldur_resource, {"user1"})
        # Second call should use cache
        backend.add_users_to_resource(waldur_resource, {"user1"})

        # resolve_user_by_cuid should only be called once due to caching
        assert mock_client.resolve_user_by_cuid.call_count == 1

    def test_resolve_remote_user_does_not_cache_none(self, backend, mock_client):
        """When user resolution fails, None should NOT be cached so retries work."""
        mock_resource = MagicMock()
        mock_resource.project_uuid = PROJECT_UUID
        mock_client.get_marketplace_resource.return_value = mock_resource

        # First call: user not found
        mock_client.resolve_user_by_cuid.return_value = None
        result = backend._resolve_remote_user("unknown_user")
        assert result is None

        # Second call: user now exists (e.g., identity bridge created them)
        mock_client.resolve_user_by_cuid.return_value = USER_UUID
        result = backend._resolve_remote_user("unknown_user")
        assert result == USER_UUID

        # resolve_user_by_cuid should be called twice (None was not cached)
        assert mock_client.resolve_user_by_cuid.call_count == 2


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
