"""Tests for order processing logging improvements.

Verifies that OfferingOrderProcessor.process_order() produces the expected
log messages at key decision points: approval, state transitions, and
error handling.
"""

import logging
from http import HTTPStatus
from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.backends import PendingOrderDecision
from waldur_site_agent.backend.structures import BackendResourceInfo


ORDER_UUID = UUID("22222222-2222-2222-2222-222222222222")
RESOURCE_UUID = UUID("33333333-3333-3333-3333-333333333333")


def _log_contains(caplog, substring):
    """Check if any log record contains the substring.

    Handles both plain text and structlog JSON-formatted messages.
    """
    for record in caplog.records:
        msg = record.getMessage()
        if substring in msg:
            return True
    return False


@pytest.fixture()
def mock_order():
    """Minimal order in pending-provider state."""
    order = mock.Mock()
    order.uuid = ORDER_UUID
    order.state = OrderState.PENDING_PROVIDER
    order.type_ = RequestTypes.CREATE
    order.resource_name = "test-resource"
    order.marketplace_resource_uuid = RESOURCE_UUID
    order.backend_id = ""
    order.error_message = ""
    return order


@pytest.fixture()
def mock_resource_backend():
    """Mock resource backend that accepts all orders by default."""
    backend = mock.Mock()
    backend.evaluate_pending_order.return_value = PendingOrderDecision.ACCEPT
    backend.supports_async_orders = False
    return backend


@pytest.fixture()
def mock_resource():
    """Mock Waldur resource."""
    resource = mock.Mock()
    resource.uuid = RESOURCE_UUID
    resource.backend_id = ""
    resource.name = "test-resource"
    resource.slug = "test-resource"
    resource.project_slug = "test-project"
    resource.customer_slug = "test-customer"
    resource.project_name = "Test Project"
    resource.project_uuid = UUID("44444444-4444-4444-4444-444444444444")
    resource.state = mock.Mock(value="Creating")
    resource.limits = mock.Mock()
    resource.limits.additional_properties = {}
    resource.limits.to_dict.return_value = {}
    resource.attributes = UNSET
    resource.offering_plugin_options = {}
    resource.plan_uuid = None
    return resource


def _make_processor(mock_resource_backend):
    """Create a processor instance with minimal mocking."""
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.offering.backend_settings = {}
    processor.resource_backend = mock_resource_backend
    processor.service_provider = None
    return processor


_PATCH_PREFIX = "waldur_site_agent.common.processors"


@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_set_state_erred")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_set_state_done")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_retrieve")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_approve_by_provider")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_provider_resources_retrieve")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_provider_resources_team_list")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_offering_users_list")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_provider_resources_set_backend_id")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_provider_resources_set_as_ok")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_provider_resources_refresh_last_sync")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_service_providers_project_service_accounts_list")
@mock.patch(f"{_PATCH_PREFIX}.marketplace_service_providers_course_accounts_list")
class TestOrderProcessingLogging:
    """Tests for logging at key decision points during order processing."""

    def _setup_mocks_for_successful_create(
        self,
        mock_order,
        mock_resource,
        mock_resource_backend,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_res_retrieve,
    ):
        """Configure mocks for a successful create order flow."""
        # Approval returns 200
        mock_approve.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # After approval, retrieve returns executing order
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""

        # First call: refresh after approval; second call: refresh before set_done
        refreshed_for_done = mock.Mock()
        refreshed_for_done.state = OrderState.EXECUTING

        mock_retrieve.sync.side_effect = [executing_order, refreshed_for_done]

        # set_state_done returns 200
        mock_set_done.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # Resource retrieval
        mock_res_retrieve.sync.return_value = mock_resource

        # Backend creates resource
        mock_resource_backend.create_resource_with_id.return_value = (
            BackendResourceInfo(backend_id="test-backend-id")
        )
        mock_resource_backend._get_resource_backend_id.return_value = (
            "test-backend-id"
        )
        mock_resource_backend.pull_resource.return_value = None

    def test_logs_approval_response_status(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Approval response status code is logged."""
        self._setup_mocks_for_successful_create(
            mock_order,
            mock_resource,
            mock_resource_backend,
            mock_approve,
            mock_retrieve,
            mock_set_done,
            mock_res_retrieve,
        )
        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "Approval response status:")

    def test_logs_order_state_after_approval(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Order state after approval is logged."""
        self._setup_mocks_for_successful_create(
            mock_order,
            mock_resource,
            mock_resource_backend,
            mock_approve,
            mock_retrieve,
            mock_set_done,
            mock_res_retrieve,
        )
        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "Order state after approval: executing")

    def test_logs_order_is_done_true(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """order_is_done=True is logged on successful processing."""
        self._setup_mocks_for_successful_create(
            mock_order,
            mock_resource,
            mock_resource_backend,
            mock_approve,
            mock_retrieve,
            mock_set_done,
            mock_res_retrieve,
        )
        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "order_is_done=True")

    def test_logs_refreshed_state_and_set_done_response(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Refreshed state and set_state_done response are logged."""
        self._setup_mocks_for_successful_create(
            mock_order,
            mock_resource,
            mock_resource_backend,
            mock_approve,
            mock_retrieve,
            mock_set_done,
            mock_res_retrieve,
        )
        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "refreshed state: executing")
        assert _log_contains(caplog, "set_state_done response")

    def test_logs_warning_when_order_not_executing_at_done_time(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Warning is logged when order is not EXECUTING at set_state_done time."""
        self._setup_mocks_for_successful_create(
            mock_order,
            mock_resource,
            mock_resource_backend,
            mock_approve,
            mock_retrieve,
            mock_set_done,
            mock_res_retrieve,
        )

        # Override: second retrieve returns DONE instead of EXECUTING
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""

        already_done = mock.Mock()
        already_done.state = OrderState.DONE

        mock_retrieve.sync.side_effect = [executing_order, already_done]

        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.WARNING):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "instead of EXECUTING")
        assert _log_contains(caplog, "skipping set_state_done")
        mock_set_done.sync_detailed.assert_not_called()

    def test_existing_resource_completes_order(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Order completes when resource already exists on backend."""
        # Approval returns 200
        mock_approve.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # After approval, order is executing
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""
        mock_retrieve.sync.return_value = executing_order

        # Resource already has backend_id and exists in backend
        mock_resource.backend_id = "existing-id"
        mock_res_retrieve.sync.return_value = mock_resource
        mock_resource_backend.pull_resource.return_value = BackendResourceInfo(
            backend_id="existing-id"
        )

        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "already created, skipping creation")
        assert _log_contains(caplog, "order_is_done=True")

    def test_logs_set_state_erred_failure(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Failure to set order to erred state is logged."""
        # Approval returns 200
        mock_approve.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # After approval, order is executing
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""
        mock_retrieve.sync.return_value = executing_order

        # Resource retrieval triggers an exception during processing
        mock_res_retrieve.sync.side_effect = Exception("creation failed")

        # set_state_erred also fails
        mock_set_erred.sync_detailed.side_effect = Exception("API unreachable")

        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.ERROR):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "Failed to set order")
        assert _log_contains(caplog, "erred state")

    def test_logs_error_with_order_state(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """Error log includes the current order state."""
        # Approval returns 200
        mock_approve.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # After approval, order is executing
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""
        mock_retrieve.sync.return_value = executing_order

        # Trigger exception during processing
        mock_res_retrieve.sync.side_effect = Exception("unexpected error")

        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.ERROR):
            processor.process_order(mock_order)

        assert _log_contains(caplog, "current state")
        assert _log_contains(caplog, "executing")

    def test_existing_resource_skips_creation_and_marks_done(
        self,
        _mock_course_accounts,
        _mock_service_accounts,
        mock_refresh_sync,
        mock_set_ok,
        mock_set_backend_id,
        mock_offering_users,
        mock_team_list,
        mock_res_retrieve,
        mock_approve,
        mock_retrieve,
        mock_set_done,
        mock_set_erred,
        mock_order,
        mock_resource_backend,
        mock_resource,
        caplog,
    ):
        """When resource exists on backend, creation is skipped and order completes."""
        # Approval returns 200
        mock_approve.sync_detailed.return_value = mock.Mock(
            status_code=HTTPStatus.OK
        )

        # After approval, order is executing
        executing_order = mock.Mock()
        executing_order.uuid = mock_order.uuid
        executing_order.state = OrderState.EXECUTING
        executing_order.type_ = RequestTypes.CREATE
        executing_order.resource_name = "test-resource"
        executing_order.marketplace_resource_uuid = RESOURCE_UUID
        executing_order.backend_id = ""
        executing_order.error_message = ""
        mock_retrieve.sync.return_value = executing_order

        # Resource already has backend_id and exists in backend
        mock_resource.backend_id = "existing-id"
        mock_res_retrieve.sync.return_value = mock_resource
        mock_resource_backend.pull_resource.return_value = BackendResourceInfo(
            backend_id="existing-id"
        )

        processor = _make_processor(mock_resource_backend)

        with caplog.at_level(logging.INFO):
            processor.process_order(mock_order)

        # Resource creation should NOT have been called
        mock_resource_backend.create_resource.assert_not_called()
        # Order should be marked as done
        assert _log_contains(caplog, "already created, skipping creation")
        mock_set_done.sync_detailed.assert_called_once()
