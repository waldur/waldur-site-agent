"""Tests for order approval with OrderApproveByProviderRequest body.

Verifies that OfferingOrderProcessor.process_order() passes the required
body parameter when calling marketplace_orders_approve_by_provider.
"""

from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.models.order_approve_by_provider_request import (
    OrderApproveByProviderRequest,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.backends import PendingOrderDecision


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
def mock_resource_backend():
    """Mock resource backend that accepts all orders by default."""
    backend = mock.Mock()
    backend.evaluate_pending_order.return_value = PendingOrderDecision.ACCEPT
    return backend


@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_set_state_erred"
)
@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_retrieve"
)
@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_approve_by_provider"
)
def test_approve_passes_body(
    mock_approve, mock_retrieve, mock_erred, mock_order, mock_resource_backend
):
    """Approval call includes OrderApproveByProviderRequest body."""
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    # After approval, retrieve returns the order in executing state
    executing_order = mock.Mock()
    executing_order.uuid = mock_order.uuid
    executing_order.state = OrderState.EXECUTING
    executing_order.type_ = RequestTypes.CREATE
    executing_order.resource_name = "test-resource"
    executing_order.marketplace_resource_uuid = mock_order.marketplace_resource_uuid
    executing_order.backend_id = ""
    executing_order.error_message = ""
    mock_retrieve.sync.return_value = executing_order

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock_resource_backend

    # Mock resource retrieval and backend operations to avoid deeper calls
    with mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_resources_retrieve"
    ) as mock_res_retrieve, mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_resources_team_list"
    ), mock.patch(
        "waldur_site_agent.common.processors.marketplace_offering_users_list"
    ), mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_resources_set_backend_id"
    ), mock.patch(
        "waldur_site_agent.common.processors.marketplace_orders_set_state_done"
    ), mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_resources_set_as_ok"
    ), mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync"
    ):
        # Setup resource mock
        mock_resource = mock.Mock()
        mock_resource.uuid = mock_order.marketplace_resource_uuid
        mock_resource.backend_id = ""
        mock_resource.name = "test-resource"
        mock_resource.state = mock.Mock(value="Creating")
        mock_resource.limits = mock.Mock()
        mock_resource.limits.additional_properties = {}
        mock_resource.attributes = UNSET
        mock_resource.offering_plugin_options = UNSET
        mock_res_retrieve.sync.return_value = mock_resource

        # Backend returns a resource info
        from waldur_site_agent.backend.structures import BackendResourceInfo

        mock_resource_backend.create_resource.return_value = BackendResourceInfo(
            backend_id="test-backend-id"
        )

        processor.process_order(mock_order)

    # Verify evaluate_pending_order was called
    mock_resource_backend.evaluate_pending_order.assert_called_once_with(
        mock_order, processor.waldur_rest_client
    )

    # Verify approve was called with body parameter
    mock_approve.sync_detailed.assert_called_once()
    call_kwargs = mock_approve.sync_detailed.call_args
    assert "body" in call_kwargs.kwargs
    assert isinstance(call_kwargs.kwargs["body"], OrderApproveByProviderRequest)


@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_set_state_erred"
)
@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_reject_by_provider"
)
def test_pending_order_rejected(
    mock_reject, mock_erred, mock_order, mock_resource_backend
):
    """Order is rejected when evaluate_pending_order returns REJECT."""
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    mock_resource_backend.evaluate_pending_order.return_value = PendingOrderDecision.REJECT

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock_resource_backend

    processor.process_order(mock_order)

    # Verify reject was called
    mock_reject.sync_detailed.assert_called_once()
    call_kwargs = mock_reject.sync_detailed.call_args
    assert call_kwargs.kwargs["uuid"] == mock_order.uuid.hex


@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_set_state_erred"
)
@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_approve_by_provider"
)
@mock.patch(
    "waldur_site_agent.common.processors.marketplace_orders_reject_by_provider"
)
def test_pending_order_stays_pending(
    mock_reject, mock_approve, mock_erred, mock_order, mock_resource_backend
):
    """Order stays pending when evaluate_pending_order returns PENDING."""
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    mock_resource_backend.evaluate_pending_order.return_value = PendingOrderDecision.PENDING

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock_resource_backend

    processor.process_order(mock_order)

    # Neither approve nor reject should be called
    mock_approve.sync_detailed.assert_not_called()
    mock_reject.sync_detailed.assert_not_called()
