"""Tests for the OFFERING_RESOURCES_SYNC event handler (forced reconciliation)."""

import json
import unittest
from unittest import mock

import stomp.utils
from waldur_api_client.models import ObservableObjectTypeEnum

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing import utils
from waldur_site_agent.event_processing.handlers import (
    on_offering_resources_sync_message_stomp,
)


def _make_offering(**overrides):
    defaults = {
        "name": "test-offering",
        "waldur_offering_uuid": "test-uuid",
        "waldur_api_url": "https://example.com/api/",
        "waldur_api_token": "token",
        "backend_type": "slurm",
        "membership_sync_backend": "slurm",
        "order_processing_backend": "slurm",
    }
    defaults.update(overrides)
    return structures.Offering(**defaults)


def _make_frame():
    frame = mock.Mock(spec=stomp.utils.Frame)
    frame.body = json.dumps(
        {
            "offering_uuid": "test-uuid",
            "requested_by_user_uuid": "user-uuid-1",
        }
    )
    return frame


@mock.patch("waldur_site_agent.event_processing.handlers.common_processors.OfferingOrderProcessor")
@mock.patch(
    "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
)
@mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering")
@mock.patch("waldur_site_agent.event_processing.handlers.common_utils.get_client")
@mock.patch("waldur_site_agent.event_processing.handlers.register_event_process_service")
class TestOfferingResourcesSyncHandler(unittest.TestCase):
    """Tests for on_offering_resources_sync_message_stomp."""

    def test_runs_forced_membership_sync(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        mock_get_backend.return_value = (mock.Mock(), "1.0")

        on_offering_resources_sync_message_stomp(_make_frame(), _make_offering(), "test-agent")

        mock_membership_processor_cls.return_value.process_offering.assert_called_once_with(
            recreate_missing_resources=True
        )

    def test_reprocesses_orders_via_processor(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        mock_get_backend.return_value = (mock.Mock(), "1.0")

        on_offering_resources_sync_message_stomp(_make_frame(), _make_offering(), "test-agent")

        # Order re-processing delegates to OfferingOrderProcessor.process_offering,
        # which fetches unfinished orders and runs the backend preflight itself.
        mock_order_processor_cls.return_value.process_offering.assert_called_once_with()

    def test_membership_sync_disabled_skips_recreation(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        offering = _make_offering(membership_sync_backend="")

        on_offering_resources_sync_message_stomp(_make_frame(), offering, "test-agent")

        mock_membership_processor_cls.assert_not_called()

    def test_order_processing_disabled_skips_orders(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        mock_get_backend.return_value = (mock.Mock(), "1.0")
        offering = _make_offering(order_processing_backend="")

        on_offering_resources_sync_message_stomp(_make_frame(), offering, "test-agent")

        mock_order_processor_cls.assert_not_called()

    def test_membership_sync_error_does_not_raise(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        mock_get_backend.return_value = (mock.Mock(), "1.0")
        mock_membership_processor_cls.return_value.process_offering.side_effect = Exception(
            "boom"
        )

        # Should not raise
        on_offering_resources_sync_message_stomp(_make_frame(), _make_offering(), "test-agent")

    def test_membership_sync_error_does_not_block_order_reprocessing(
        self,
        mock_register,
        mock_get_client,
        mock_get_backend,
        mock_membership_processor_cls,
        mock_order_processor_cls,
    ):
        mock_get_backend.return_value = (mock.Mock(), "1.0")
        mock_membership_processor_cls.return_value.process_offering.side_effect = Exception(
            "boom"
        )

        on_offering_resources_sync_message_stomp(_make_frame(), _make_offering(), "test-agent")

        # A membership-sync failure must not skip the order re-processing phase.
        mock_order_processor_cls.return_value.process_offering.assert_called_once_with()


class TestDetermineObjectTypesOfferingResourcesSync(unittest.TestCase):
    """Subscription type selection for the offering resources sync event."""

    def test_included_with_membership_sync(self):
        offering = _make_offering()
        result = utils._determine_observable_object_types(offering)
        self.assertIn(ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC, result)

    def test_not_included_without_membership_sync(self):
        offering = _make_offering(membership_sync_backend="")
        result = utils._determine_observable_object_types(offering)
        self.assertNotIn(ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC, result)
