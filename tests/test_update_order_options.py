"""UPDATE orders that change resource options reach the backend hook."""

from unittest import mock

import pytest
from waldur_api_client.models.order_details_attributes import OrderDetailsAttributes
from waldur_api_client.models.order_details_limits import OrderDetailsLimits
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import (
    OfferingOrderProcessor,
    _order_new_options,
)

RETRIEVE = "waldur_site_agent.common.processors.marketplace_provider_resources_retrieve.sync"


def make_order(attributes=None, limits=None):
    order = mock.Mock()
    order.uuid = "order-1"
    order.resource_name = "res"
    order.marketplace_resource_uuid = "res-uuid"
    order.plan_uuid = "plan-uuid"
    order.attributes = (
        OrderDetailsAttributes.from_dict(attributes) if attributes is not None else UNSET
    )
    order.limits = OrderDetailsLimits.from_dict(limits) if limits is not None else None
    return order


def make_processor(applied=True):
    processor = mock.Mock()
    processor._poll_async_target_order.return_value = None
    processor.resource_backend.update_resource_options.return_value = applied
    processor.resource_backend.has_prepaid_components.return_value = False
    processor.resource_backend.set_resource_limits.return_value = None
    processor.resource_backend.supports_async_orders = False
    processor._resolve_plan_limits.return_value = {"storage": 1}
    return processor


@pytest.fixture
def waldur_resource():
    resource = mock.Mock()
    resource.name = "res"
    resource.backend_id = "acme$waldur-res"
    resource.limits.to_dict.return_value = {"storage": 1}
    with mock.patch(RETRIEVE, return_value=resource):
        yield resource


class TestOrderNewOptions:
    def test_reads_new_options_from_the_attributes(self):
        order = make_order({"old_options": {"a": 1}, "new_options": {"a": 2}})
        assert _order_new_options(order) == {"a": 2}

    @pytest.mark.parametrize(
        "attributes", [None, {}, {"name": "x"}, {"new_options": None}, {"new_options": "x"}]
    )
    def test_anything_else_is_no_options(self, attributes):
        assert _order_new_options(make_order(attributes)) == {}


class TestUpdateOrderOptions:
    def test_options_only_order_is_applied_and_done(self, waldur_resource):
        processor = make_processor()
        order = make_order({"old_options": {"max_storage_limit": 5}, "new_options": {"max_storage_limit": 7}})

        assert OfferingOrderProcessor._process_update_order(processor, order) is True

        processor.resource_backend.update_resource_options.assert_called_once_with(
            waldur_resource, {"max_storage_limit": 7}
        )
        # Options alone must not fall through to the plan-quota limits path.
        processor._resolve_plan_limits.assert_not_called()
        processor.resource_backend.set_resource_limits.assert_not_called()

    def test_a_backend_that_ignores_options_still_completes_the_order(self, waldur_resource):
        """Informational options keep working: the order is done, Waldur records them."""
        processor = make_processor(applied=False)
        order = make_order({"new_options": {"note": "x"}})

        assert OfferingOrderProcessor._process_update_order(processor, order) is True
        processor.resource_backend.update_resource_options.assert_called_once()

    def test_a_failing_backend_fails_the_order(self, waldur_resource):
        processor = make_processor()
        processor.resource_backend.update_resource_options.side_effect = RuntimeError("croit 500")
        order = make_order({"new_options": {"max_storage_limit": 7}})

        with pytest.raises(RuntimeError, match="croit 500"):
            OfferingOrderProcessor._process_update_order(processor, order)

    def test_options_with_new_limits_apply_both(self, waldur_resource):
        processor = make_processor()
        order = make_order({"new_options": {"size": 7}}, limits={"storage": 7})

        assert OfferingOrderProcessor._process_update_order(processor, order) is True

        processor.resource_backend.update_resource_options.assert_called_once()
        processor.resource_backend.set_resource_limits.assert_called_once_with(
            "acme$waldur-res", {"storage": 7}
        )

    def test_limits_only_order_does_not_touch_options(self, waldur_resource):
        processor = make_processor()
        order = make_order(limits={"storage": 7})

        OfferingOrderProcessor._process_update_order(processor, order)

        processor.resource_backend.update_resource_options.assert_not_called()
        processor.resource_backend.set_resource_limits.assert_called_once()
