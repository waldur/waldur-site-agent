"""Tests for transient Waldur API error handling in order and membership sync.

A transient server-side error (HTTP 5xx or a network failure) while talking to
the Waldur API must not mark the order or resource as erred: the object should
keep its current state so the agent can retry on the next cycle.
"""

from unittest import mock
from uuid import UUID

import httpx
import pytest
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
)


@pytest.fixture()
def mock_order():
    """Minimal order in pending-provider state."""
    order = mock.Mock()
    order.uuid = UUID("22222222-2222-2222-2222-222222222222")
    order.state = OrderState.PENDING_PROVIDER
    order.type_ = RequestTypes.CREATE
    order.resource_name = "test-resource"
    order.marketplace_resource_uuid = UUID("33333333-3333-3333-3333-333333333333")
    order.backend_id = ""
    order.error_message = ""
    return order


@pytest.fixture()
def processor():
    """Order processor with a mocked REST client and resource backend."""
    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock.Mock()
    processor.expose_backend_error_details = True
    return processor


def make_unexpected_status(
    status_code: int,
    url: str = "https://waldur.example.com/api/marketplace-orders/",
) -> UnexpectedStatus:
    return UnexpectedStatus(
        status_code=status_code,
        content=b"<html>Server Error</html>",
        url=httpx.URL(url),
    )


@pytest.fixture()
def mock_resource():
    resource = mock.Mock()
    resource.uuid = UUID("11111111-1111-1111-1111-111111111111")
    resource.backend_id = "test-allocation-01"
    resource.name = "test-resource"
    resource.state = ResourceState.OK
    return resource


@pytest.fixture()
def membership_processor():
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.offering.username_reconciliation_enabled = False
    processor.resource_backend = mock.Mock()
    processor.expose_backend_error_details = True
    processor._refresh_local_offering_users = mock.Mock(return_value=[])
    processor._sync_user_profiles_to_backend = mock.Mock()
    processor._fetch_source_project = mock.Mock(return_value=None)
    processor._sync_resource_users = mock.Mock(return_value=set())
    processor._sync_resource_service_accounts = mock.Mock()
    processor._sync_resource_course_accounts = mock.Mock()
    processor._sync_resource_status = mock.Mock()
    processor._sync_resource_limits = mock.Mock()
    processor._sync_resource_user_limits = mock.Mock()
    processor.resource_backend.sync_resource_project = mock.Mock()
    processor.resource_backend.sync_project_end_date = mock.Mock()
    processor.resource_backend.sync_resource_end_date = mock.Mock()
    processor.resource_backend.sync_resource_effective_id = mock.Mock()
    return processor


def _membership_resource_report(resource):
    return {
        resource.backend_id: (
            resource,
            BackendResourceInfo(backend_id=resource.backend_id),
        )
    }


def _refresh_last_sync_error(status_code: int) -> UnexpectedStatus:
    return make_unexpected_status(
        status_code,
        url=(
            "https://waldur.example.com/api/marketplace-provider-resources/"
            "11111111-1111-1111-1111-111111111111/refresh_last_sync/"
        ),
    )


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_server_error_during_evaluation_does_not_erred_order(
    mock_erred, processor, mock_order
):
    """A 5xx from the Waldur API during evaluation is re-raised, order not erred."""
    processor.resource_backend.evaluate_pending_order.side_effect = (
        make_unexpected_status(500)
    )

    with pytest.raises(UnexpectedStatus):
        processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_rate_limit_response_is_treated_as_transient(mock_erred, processor, mock_order):
    """HTTP 429 is re-raised for retry like a 5xx, order not erred."""
    processor.resource_backend.evaluate_pending_order.side_effect = (
        make_unexpected_status(429)
    )

    with pytest.raises(UnexpectedStatus):
        processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_transport_error_during_evaluation_does_not_erred_order(
    mock_erred, processor, mock_order
):
    """A network-level failure is re-raised, order not erred."""
    processor.resource_backend.evaluate_pending_order.side_effect = httpx.ConnectError(
        "connection refused"
    )

    with pytest.raises(httpx.ConnectError):
        processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_approve_by_provider")
def test_client_error_during_evaluation_keeps_order_pending(
    mock_approve, mock_erred, processor, mock_order
):
    """Even a 4xx during evaluation keeps the order pending: nothing is provisioned yet."""
    processor.resource_backend.evaluate_pending_order.side_effect = (
        make_unexpected_status(400)
    )

    processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()
    mock_approve.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_approve_by_provider")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_approval_failure_keeps_order_pending(mock_erred, mock_approve, processor, mock_order):
    """A non-transient failure while approving keeps the order pending: nothing provisioned."""
    from waldur_site_agent.backend.backends import PendingOrderDecision

    processor.resource_backend.evaluate_pending_order.return_value = PendingOrderDecision.ACCEPT
    mock_approve.sync_detailed.side_effect = make_unexpected_status(400)

    processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_server_error_during_execution_is_retried(
    mock_erred, mock_resource_retrieve, processor, mock_order
):
    """A 5xx while executing an order is re-raised for retry, order not erred."""
    processor.resource_backend.supports_async_orders = False
    mock_order.state = OrderState.EXECUTING
    mock_resource_retrieve.sync.side_effect = make_unexpected_status(500)

    with pytest.raises(UnexpectedStatus):
        processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_client_error_during_execution_marks_order_erred(
    mock_erred, mock_resource_retrieve, processor, mock_order
):
    """A 4xx while executing an order still marks it as erred."""
    processor.resource_backend.supports_async_orders = False
    mock_order.state = OrderState.EXECUTING
    mock_resource_retrieve.sync.side_effect = make_unexpected_status(400)

    processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_called_once()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_approve_by_provider")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_reject_by_provider")
def test_evaluation_bug_keeps_order_pending(
    mock_reject, mock_approve, mock_erred, processor, mock_order
):
    """Any evaluation failure keeps the order pending: no erred, approve or reject."""
    processor.resource_backend.evaluate_pending_order.side_effect = RuntimeError(
        "plugin bug"
    )

    processor.process_order(mock_order)

    mock_erred.sync_detailed.assert_not_called()
    mock_approve.sync_detailed.assert_not_called()
    mock_reject.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.sleep")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_retries_exhausted_leave_order_state_unchanged(
    mock_erred, mock_retrieve, mock_sleep, processor, mock_order
):
    """Persistent 5xx errors exhaust retries without marking the order erred."""
    processor.resource_backend.evaluate_pending_order.side_effect = (
        make_unexpected_status(500)
    )
    mock_retrieve.sync.return_value = mock_order

    processor.process_order_with_retries(mock_order, retry_count=3, delay=0)

    assert processor.resource_backend.evaluate_pending_order.call_count == 3
    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.sleep")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_transport_errors_are_retried(
    mock_erred, mock_retrieve, mock_sleep, processor, mock_order
):
    """Network-level failures are retried like 5xx responses."""
    processor.resource_backend.evaluate_pending_order.side_effect = httpx.ConnectError(
        "connection refused"
    )
    mock_retrieve.sync.return_value = mock_order

    processor.process_order_with_retries(mock_order, retry_count=3, delay=0)

    assert processor.resource_backend.evaluate_pending_order.call_count == 3
    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_membership_refresh_last_sync_500_does_not_erred_resource(
    mock_mark_erred, mock_refresh_last_sync, membership_processor, mock_resource
):
    mock_refresh_last_sync.sync_detailed.side_effect = _refresh_last_sync_error(500)

    membership_processor._process_resources(_membership_resource_report(mock_resource))

    mock_mark_erred.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_membership_refresh_last_sync_transport_error_does_not_erred_resource(
    mock_mark_erred, mock_refresh_last_sync, membership_processor, mock_resource
):
    mock_refresh_last_sync.sync_detailed.side_effect = httpx.ConnectError("connection refused")

    membership_processor._process_resources(_membership_resource_report(mock_resource))

    mock_mark_erred.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_membership_refresh_last_sync_429_does_not_erred_resource(
    mock_mark_erred, mock_refresh_last_sync, membership_processor, mock_resource
):
    mock_refresh_last_sync.sync_detailed.side_effect = _refresh_last_sync_error(429)

    membership_processor._process_resources(_membership_resource_report(mock_resource))

    mock_mark_erred.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_membership_backend_error_still_marks_resource_erred(
    mock_mark_erred, mock_refresh_last_sync, membership_processor, mock_resource
):
    membership_processor._sync_resource_users.side_effect = BackendError("SLURM is broken")

    membership_processor._process_resources(_membership_resource_report(mock_resource))

    mock_mark_erred.assert_called_once()
    mock_refresh_last_sync.sync_detailed.assert_not_called()
