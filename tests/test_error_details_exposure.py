from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.utils import format_waldur_error_details


def test_unexpected_exception_hidden_when_flag_disabled() -> None:
    try:
        raise RuntimeError("boom from dependency")
    except Exception as exc:
        msg, tb = format_waldur_error_details(exc, expose_backend_error_details=False)

    assert msg == "Internal backend error. Please contact the service provider."
    assert tb == ""


def test_backend_error_message_exposed_when_flag_disabled() -> None:
    try:
        raise BackendError("safe user-facing message")
    except Exception as exc:
        msg, tb = format_waldur_error_details(exc, expose_backend_error_details=False)

    assert msg == "safe user-facing message"
    assert tb == ""


def test_full_details_exposed_when_flag_enabled() -> None:
    try:
        raise RuntimeError("boom")
    except Exception as exc:
        msg, tb = format_waldur_error_details(exc, expose_backend_error_details=True)

    assert msg == "boom"
    assert "RuntimeError" in tb


_PATCH_PREFIX = "waldur_site_agent.common.processors"

ORDER_UUID = UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture()
def failing_order():
    order = mock.Mock()
    order.uuid = ORDER_UUID
    order.state = OrderState.PENDING_PROVIDER
    order.type_ = RequestTypes.CREATE
    order.resource_name = "test-resource"
    return order


def _make_processor(expose: bool):
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.offering.backend_settings = {}
    processor.resource_backend = mock.Mock()
    processor.resource_backend.evaluate_pending_order.side_effect = RuntimeError(
        "internal lib error"
    )
    processor.expose_backend_error_details = expose
    return processor


@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_set_state_erred")
def test_processor_sends_generic_message_when_flag_disabled(
    mock_set_erred, failing_order
):
    processor = _make_processor(expose=False)
    processor.process_order(failing_order)

    mock_set_erred.sync_detailed.assert_called_once()
    body = mock_set_erred.sync_detailed.call_args.kwargs["body"]
    assert body.error_message == "Internal backend error. Please contact the service provider."
    assert body.error_traceback == ""


@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_set_state_erred")
def test_processor_sends_raw_message_when_flag_enabled(
    mock_set_erred, failing_order
):
    processor = _make_processor(expose=True)
    processor.process_order(failing_order)

    mock_set_erred.sync_detailed.assert_called_once()
    body = mock_set_erred.sync_detailed.call_args.kwargs["body"]
    assert body.error_message == "internal lib error"
    assert "RuntimeError" in body.error_traceback
