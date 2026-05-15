"""Tests for optional per-cycle backend preflight in order processing."""

from unittest import mock

import pytest

from waldur_site_agent.backend.exceptions import BackendNotReadyError


_PATCH_PREFIX = "waldur_site_agent.common.processors"


@pytest.fixture()
def processor():
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    instance = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    instance.waldur_rest_client = mock.Mock()
    instance.offering = mock.Mock()
    instance.offering.name = "test-offering"
    instance.offering.uuid = "offering-uuid"
    instance.resource_backend = mock.Mock()
    instance.resource_backend.supports_cycle_preflight = False
    return instance


@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_list")
def test_process_offering_skips_orders_when_preflight_fails(mock_orders_list, processor):
    processor.resource_backend.supports_cycle_preflight = True
    processor.resource_backend.run_preflight.side_effect = BackendNotReadyError("remote down")

    processor.process_offering()

    processor.resource_backend.run_preflight.assert_called_once()
    mock_orders_list.sync_all.assert_not_called()


@mock.patch(f"{_PATCH_PREFIX}.marketplace_orders_list")
def test_process_offering_lists_orders_when_preflight_disabled(mock_orders_list, processor):
    mock_orders_list.sync_all.return_value = []

    processor.process_offering()

    processor.resource_backend.run_preflight.assert_not_called()
    mock_orders_list.sync_all.assert_called_once()
