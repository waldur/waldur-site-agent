"""Tests for periodic username/order/offering-user reconciliation and event processing main loop."""

import datetime
import inspect
import unittest
import uuid
from unittest import mock

from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.resource_api_key_state import ResourceApiKeyState
from waldur_api_client.models.resource_api_key_status import ResourceApiKeyStatus
from waldur_api_client.types import UNSET

from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing import utils


def _make_offering(**overrides) -> common_structures.Offering:
    """Create a minimal Offering with sensible defaults for reconciliation tests."""
    defaults = dict(
        name="test-offering",
        waldur_offering_uuid="test-uuid",
        waldur_api_url="https://example.com/api/",
        waldur_api_token="token",
        backend_type="slurm",
    )
    defaults.update(overrides)
    return common_structures.Offering(**defaults)


class TestRunPeriodicUsernameReconciliation(unittest.TestCase):
    """Tests for run_periodic_username_reconciliation function."""

    def test_skips_offering_with_toggle_disabled(self):
        """Offerings without username_reconciliation_enabled are skipped entirely."""
        offering = _make_offering(
            username_reconciliation_enabled=False,
            stomp_enabled=True,
            membership_sync_backend="slurm",
        )
        with mock.patch(
            "waldur_site_agent.event_processing.utils.get_client_for_offering"
        ) as mock_get_client:
            utils.run_periodic_username_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    def test_skips_offering_with_toggle_disabled_by_default(self):
        """Offerings without explicit toggle are skipped (default is False)."""
        offering = _make_offering(
            stomp_enabled=True,
            membership_sync_backend="waldur",
        )
        with mock.patch(
            "waldur_site_agent.event_processing.utils.get_client_for_offering"
        ) as mock_get_client:
            utils.run_periodic_username_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_calls_sync_for_qualifying_offering(self, mock_get_client, mock_get_backend):
        """Reconciliation calls sync_offering_user_usernames for enabled offerings."""
        offering = _make_offering(
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )
        mock_backend = mock.Mock()
        mock_backend.sync_offering_user_usernames.return_value = False
        mock_get_backend.return_value = (mock_backend, None)

        utils.run_periodic_username_reconciliation([offering], "agent")

        mock_get_client.assert_called_once()
        mock_get_backend.assert_called_once_with(offering, "membership_sync_backend")
        mock_backend.sync_offering_user_usernames.assert_called_once_with(
            offering.uuid, mock_get_client.return_value  # mock_get_client is now get_client_for_offering
        )

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_logs_when_usernames_updated(self, mock_get_client, mock_get_backend):
        """When sync returns True, an info log is emitted."""
        offering = _make_offering(
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )
        mock_backend = mock.Mock()
        mock_backend.sync_offering_user_usernames.return_value = True
        mock_get_backend.return_value = (mock_backend, None)

        with mock.patch("waldur_site_agent.event_processing.utils.logger") as mock_logger:
            utils.run_periodic_username_reconciliation([offering], "agent")
            mock_logger.info.assert_called_with(
                "Reconciliation: usernames updated for offering %s",
                offering.name,
            )

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_exception_is_logged_and_does_not_propagate(self, mock_get_client, mock_get_backend):
        """Backend exceptions are logged and don't crash the loop."""
        offering = _make_offering(
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )
        mock_backend = mock.Mock()
        mock_backend.sync_offering_user_usernames.side_effect = RuntimeError("backend down")
        mock_get_backend.return_value = (mock_backend, None)

        with mock.patch("waldur_site_agent.event_processing.utils.logger") as mock_logger:
            # Should not raise
            utils.run_periodic_username_reconciliation([offering], "agent")
            mock_logger.exception.assert_called_with(
                "Reconciliation failed for offering %s", offering.name
            )

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_processes_multiple_offerings_independently(self, mock_get_client, mock_get_backend):
        """Each qualifying offering is processed even if one fails."""
        offering_a = _make_offering(
            name="offering-a",
            waldur_offering_uuid="uuid-a",
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )
        offering_b = _make_offering(
            name="offering-b",
            waldur_offering_uuid="uuid-b",
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )

        mock_backend_a = mock.Mock()
        mock_backend_a.sync_offering_user_usernames.side_effect = RuntimeError("fail")
        mock_backend_b = mock.Mock()
        mock_backend_b.sync_offering_user_usernames.return_value = False

        mock_get_backend.side_effect = [
            (mock_backend_a, None),
            (mock_backend_b, None),
        ]

        utils.run_periodic_username_reconciliation([offering_a, offering_b], "agent")

        # Both backends were called despite first one failing
        mock_backend_a.sync_offering_user_usernames.assert_called_once()
        mock_backend_b.sync_offering_user_usernames.assert_called_once()

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_mixed_offerings_only_processes_qualifying(self, mock_get_client, mock_get_backend):
        """Only offerings with username_reconciliation_enabled are processed."""
        enabled = _make_offering(
            name="enabled",
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
        )
        disabled_explicit = _make_offering(
            name="disabled-explicit",
            username_reconciliation_enabled=False,
            membership_sync_backend="waldur",
        )
        disabled_default = _make_offering(
            name="disabled-default",
            stomp_enabled=True,
            membership_sync_backend="waldur",
        )

        mock_backend = mock.Mock()
        mock_backend.sync_offering_user_usernames.return_value = False
        mock_get_backend.return_value = (mock_backend, None)

        utils.run_periodic_username_reconciliation(
            [enabled, disabled_explicit, disabled_default], "agent"
        )

        # Only the enabled offering triggers backend calls
        self.assertEqual(mock_get_backend.call_count, 1)
        mock_get_backend.assert_called_with(enabled, "membership_sync_backend")


class TestRunPeriodicOrderReconciliation(unittest.TestCase):
    """Tests for run_periodic_order_reconciliation function."""

    def test_skips_offering_without_order_processing_backend(self):
        """Offerings without order_processing_backend are skipped entirely."""
        offering = _make_offering(
            stomp_enabled=True,
        )
        with mock.patch(
            "waldur_site_agent.event_processing.utils.get_client_for_offering"
        ) as mock_get_client:
            utils.run_periodic_order_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_processes_stuck_orders(
        self, mock_get_client, mock_orders_list, mock_processor_cls
    ):
        """Reconciliation fetches stuck orders and processes each one."""
        offering = _make_offering(
            order_processing_backend="slurm",
        )
        stuck_order = mock.Mock()
        mock_orders_list.sync_all.return_value = [stuck_order]
        mock_processor = mock.Mock()
        mock_processor_cls.return_value = mock_processor

        utils.run_periodic_order_reconciliation([offering], "agent")

        mock_get_client.assert_called_once()
        # Verify modified_before cutoff is passed
        call_kwargs = mock_orders_list.sync_all.call_args.kwargs
        self.assertIn("modified_before", call_kwargs)
        # Processor processes each stuck order individually
        mock_processor.process_order_with_retries.assert_called_once_with(stuck_order)

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_skips_when_no_stuck_orders(
        self, mock_get_client, mock_orders_list, mock_processor_cls
    ):
        """No processing when there are no stuck orders."""
        offering = _make_offering(
            order_processing_backend="slurm",
        )
        mock_orders_list.sync_all.return_value = []

        utils.run_periodic_order_reconciliation([offering], "agent")

        mock_processor_cls.assert_not_called()

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_exception_is_logged_and_does_not_propagate(
        self, mock_get_client, mock_orders_list, mock_processor_cls
    ):
        """Backend exceptions are logged and don't crash the loop."""
        offering = _make_offering(
            order_processing_backend="slurm",
        )
        mock_orders_list.sync_all.side_effect = RuntimeError("backend down")

        with mock.patch(
            "waldur_site_agent.event_processing.utils.logger"
        ) as mock_logger:
            utils.run_periodic_order_reconciliation([offering], "agent")
            mock_logger.exception.assert_called_with(
                "Order reconciliation failed for offering %s", offering.name
            )

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_processes_multiple_offerings_independently(
        self, mock_get_client, mock_orders_list, mock_processor_cls
    ):
        """Each qualifying offering is processed even if one fails."""
        offering_a = _make_offering(
            name="offering-a",
            waldur_offering_uuid="uuid-a",
            order_processing_backend="slurm",
        )
        offering_b = _make_offering(
            name="offering-b",
            waldur_offering_uuid="uuid-b",
            order_processing_backend="slurm",
        )

        order_a = mock.Mock()
        order_b = mock.Mock()
        mock_orders_list.sync_all.side_effect = [[order_a], [order_b]]

        mock_proc_a = mock.Mock()
        mock_proc_a.process_order_with_retries.side_effect = RuntimeError("fail")
        mock_proc_b = mock.Mock()
        mock_processor_cls.side_effect = [mock_proc_a, mock_proc_b]

        utils.run_periodic_order_reconciliation([offering_a, offering_b], "agent")

        mock_proc_a.process_order_with_retries.assert_called_once_with(order_a)
        mock_proc_b.process_order_with_retries.assert_called_once_with(order_b)

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_mixed_offerings_only_processes_qualifying(
        self, mock_get_client, mock_orders_list, mock_processor_cls
    ):
        """Only offerings with order_processing_backend are processed."""
        with_backend = _make_offering(
            name="with-backend",
            order_processing_backend="slurm",
        )
        without_backend = _make_offering(
            name="without-backend",
        )

        stuck_order = mock.Mock()
        mock_orders_list.sync_all.return_value = [stuck_order]
        mock_processor = mock.Mock()
        mock_processor_cls.return_value = mock_processor

        utils.run_periodic_order_reconciliation(
            [with_backend, without_backend], "agent"
        )

        self.assertEqual(mock_processor_cls.call_count, 1)
        mock_processor.process_order_with_retries.assert_called_once_with(stuck_order)


class TestRunPeriodicOfferingUserReconciliation(unittest.TestCase):
    """Tests for run_periodic_offering_user_reconciliation function."""

    def test_skips_offering_without_membership_sync_backend(self):
        """Offerings without membership_sync_backend are skipped."""
        offering = _make_offering(stomp_enabled=True)
        with mock.patch(
            "waldur_site_agent.event_processing.utils.get_client_for_offering"
        ) as mock_get_client:
            utils.run_periodic_offering_user_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.update_offering_users")
    @mock.patch(
        "waldur_site_agent.event_processing.utils.marketplace_offering_users_list"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_fetches_stuck_users_and_calls_update(
        self, mock_get_client, mock_ou_list, mock_update
    ):
        """Reconciliation fetches stuck offering users and calls update_offering_users."""
        offering = _make_offering(membership_sync_backend="slurm")
        stuck_user = mock.Mock()
        mock_ou_list.sync_all.return_value = [stuck_user]
        mock_update.return_value = True

        utils.run_periodic_offering_user_reconciliation([offering], "agent")

        mock_get_client.assert_called_once()
        mock_ou_list.sync_all.assert_called_once()
        call_kwargs = mock_ou_list.sync_all.call_args.kwargs
        self.assertEqual(
            set(call_kwargs["state"]),
            {
                OfferingUserState.REQUESTED,
                OfferingUserState.CREATING,
                OfferingUserState.ERROR_CREATING,
                OfferingUserState.PENDING_ACCOUNT_LINKING,
                OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
            },
        )
        mock_update.assert_called_once_with(
            offering, mock_get_client.return_value, [stuck_user]
        )

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.update_offering_users")
    @mock.patch(
        "waldur_site_agent.event_processing.utils.marketplace_offering_users_list"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_skips_when_no_stuck_users(
        self, mock_get_client, mock_ou_list, mock_update
    ):
        """No processing when there are no stuck offering users."""
        offering = _make_offering(membership_sync_backend="slurm")
        mock_ou_list.sync_all.return_value = []

        utils.run_periodic_offering_user_reconciliation([offering], "agent")

        mock_update.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.update_offering_users")
    @mock.patch(
        "waldur_site_agent.event_processing.utils.marketplace_offering_users_list"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_exception_does_not_propagate(
        self, mock_get_client, mock_ou_list, mock_update
    ):
        """Exceptions are logged and don't crash the loop."""
        offering = _make_offering(membership_sync_backend="slurm")
        mock_ou_list.sync_all.side_effect = RuntimeError("backend down")

        with mock.patch(
            "waldur_site_agent.event_processing.utils.logger"
        ) as mock_logger:
            utils.run_periodic_offering_user_reconciliation([offering], "agent")
            mock_logger.exception.assert_called_with(
                "Offering user reconciliation failed for %s", offering.name
            )


class TestMainLoopTimers(unittest.TestCase):
    """Tests for the event processing main loop timer logic."""

    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    @mock.patch("waldur_site_agent.event_processing.main.common_utils")
    def test_health_check_and_reconciliation_run_on_first_tick(
        self, mock_common_utils, mock_utils, mock_time
    ):
        """Both health check and reconciliation run immediately on first iteration."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"
        config.expose_backend_error_details = True

        # time.time() must exceed both HEALTH_CHECK_INTERVAL (1800) and
        # RECONCILIATION_INTERVAL (3600) since last_* starts at 0.0
        mock_time.time.return_value = 5000.0
        mock_time.sleep.side_effect = BaseException("break loop")
        mock_utils.signal_handling.return_value.__enter__ = mock.Mock()
        mock_utils.signal_handling.return_value.__exit__ = mock.Mock(return_value=False)

        with self.assertRaises(BaseException):
            main.start(config)

        # All should have been called on the first tick (last_* starts at 0.0)
        mock_utils.send_agent_health_checks.assert_called()
        mock_utils.run_periodic_username_reconciliation.assert_called()
        mock_utils.run_periodic_order_reconciliation.assert_called()
        mock_utils.run_periodic_offering_user_reconciliation.assert_called()

    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    @mock.patch("waldur_site_agent.event_processing.main.common_utils")
    def test_health_check_not_repeated_within_interval(
        self, mock_common_utils, mock_utils, mock_time
    ):
        """Health check does not repeat when interval hasn't elapsed."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"
        config.expose_backend_error_details = True

        first_tick = 5000.0  # Exceeds both intervals, triggers on first tick
        second_tick = first_tick + 60  # 1 minute later — well within 30-min interval

        mock_time.time.side_effect = [first_tick, second_tick, second_tick]
        mock_time.sleep.side_effect = [None, BaseException("break loop")]
        mock_utils.signal_handling.return_value.__enter__ = mock.Mock()
        mock_utils.signal_handling.return_value.__exit__ = mock.Mock(return_value=False)

        with self.assertRaises(BaseException):
            main.start(config)

        # Health check: called once on first tick, not again on second
        self.assertEqual(mock_utils.send_agent_health_checks.call_count, 1)

    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    @mock.patch("waldur_site_agent.event_processing.main.common_utils")
    def test_initial_processing_runs_before_loop(
        self, mock_common_utils, mock_utils, mock_time
    ):
        """run_initial_offering_processing is called before the main loop."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"
        config.expose_backend_error_details = True

        # Make start_stomp_consumers raise to exit early
        mock_utils.run_initial_offering_processing.return_value = None
        mock_utils.start_stomp_consumers.side_effect = RuntimeError("stop")

        with self.assertRaises(SystemExit):
            main.start(config)

        mock_utils.run_initial_offering_processing.assert_called_once_with(
            config.waldur_offerings,
            config.waldur_user_agent,
            expose_backend_error_details=True,
        )

    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    @mock.patch("waldur_site_agent.event_processing.main.common_utils")
    def test_exception_stops_consumers_and_exits(
        self, mock_common_utils, mock_utils, mock_time
    ):
        """Fatal exception in the loop stops STOMP consumers and calls sys.exit(1)."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"
        config.expose_backend_error_details = True

        stomp_map = {"key": "value"}
        mock_utils.start_stomp_consumers.return_value = stomp_map
        mock_utils.signal_handling.return_value.__enter__ = mock.Mock()
        mock_utils.signal_handling.return_value.__exit__ = mock.Mock(return_value=False)

        # First time.time() call raises to simulate loop failure
        mock_time.time.side_effect = RuntimeError("unexpected")

        with self.assertRaises(SystemExit) as ctx:
            main.start(config)

        self.assertEqual(ctx.exception.code, 1)
        mock_utils.stop_stomp_consumers.assert_called_once_with(stomp_map)


class TestRunPeriodicApiKeyReconciliation(unittest.TestCase):
    """Tests for run_periodic_api_key_reconciliation function."""

    @staticmethod
    def _stuck_key(uuid_hex=None, client_id="cid-1", backend_id="res-1"):
        """A real ResourceApiKeyStatus, not a Mock.

        A Mock answers to any attribute, so it would keep this suite green through a
        schema change that leaves the sweep calling a field the API stopped serving.
        """
        return ResourceApiKeyStatus(
            uuid=uuid.UUID(uuid_hex) if uuid_hex else uuid.uuid4(),
            resource_uuid=uuid.uuid4(),
            resource_backend_id=backend_id,
            modified=datetime.datetime.now(tz=datetime.timezone.utc),
            client_id=client_id,
            state=ResourceApiKeyState.UPDATING,
        )

    def test_skips_offering_without_order_processing_backend(self):
        """Rotation is an order-processing capability."""
        offering = _make_offering(stomp_enabled=True)
        with mock.patch(
            "waldur_site_agent.event_processing.utils.get_client_for_offering"
        ) as mock_get_client:
            utils.run_periodic_api_key_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_skips_backend_without_key_support(
        self, mock_get_backend, mock_get_client, mock_keys_list
    ):
        """A backend that leaves supports_resource_api_keys False is not swept."""
        offering = _make_offering(order_processing_backend="slurm")
        mock_get_backend.return_value = (mock.Mock(spec=[]), "1.0")

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        mock_keys_list.sync_all.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_re_issues_the_rotation_of_a_stuck_key(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        """A key stuck in Updating is rotated again, with its backend id."""
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        stuck = self._stuck_key()
        mock_keys_list.sync_all.return_value = [stuck]

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        call_kwargs = mock_keys_list.sync_all.call_args.kwargs
        # Only long-stuck keys: a rotation still in flight must be left alone.
        self.assertIn("modified_before", call_kwargs)
        self.assertEqual(call_kwargs["offering_uuid"], offering.waldur_offering_uuid)
        self.assertEqual(call_kwargs["state"], [ResourceApiKeyState.UPDATING])
        mock_rotate.assert_called_once_with(
            mock_get_client.return_value,
            stuck.uuid.hex,
            "cid-1",
            backend,
            "res-1",
            stuck.resource_uuid.hex,
            expose_backend_error_details=True,
        )

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_the_sweep_forwards_the_error_exposure_flag(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        """An offering that opted out of raw backend errors opted out everywhere.

        The STOMP handler already honours the flag; the sweep rotates the same keys
        by another route, so leaving it on the default leaked exactly what the flag
        exists to withhold.
        """
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.return_value = [self._stuck_key()]

        utils.run_periodic_api_key_reconciliation(
            [offering], "agent", expose_backend_error_details=False
        )

        self.assertIs(mock_rotate.call_args.kwargs["expose_backend_error_details"], False)

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_skips_when_nothing_is_stuck(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.return_value = []

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        mock_rotate.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_one_failing_key_does_not_stop_the_others(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        """A sweep must not abandon the remaining keys when one rotation throws."""
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.return_value = [
            self._stuck_key(client_id="cid-1"),
            self._stuck_key(client_id="cid-2"),
        ]
        mock_rotate.side_effect = [Exception("gateway down"), None]

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        self.assertEqual(mock_rotate.call_count, 2)

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_a_key_without_a_client_id_is_skipped(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        """client_id is Union[Unset, str]; an Unset object must not reach a URL.

        The STOMP handler rejects a falsy client_id, but the sweep reads the same
        field off a list response and had no equivalent guard.
        """
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.return_value = [self._stuck_key(client_id=UNSET)]

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        mock_rotate.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.rotate_resource_api_key")
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_a_key_without_a_resource_backend_id_is_skipped(
        self, mock_get_backend, mock_get_client, mock_keys_list, mock_rotate
    ):
        """An empty backend id makes envoy re-provision the key active on a paused
        resource: list_client_ids("") matches nothing, so the pause check sees no
        siblings and the fallback lands the key in the active Secret."""
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.return_value = [self._stuck_key(backend_id="")]

        utils.run_periodic_api_key_reconciliation([offering], "agent")

        mock_rotate.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_resource_api_keys_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_exception_is_logged_and_does_not_propagate(
        self, mock_get_backend, mock_get_client, mock_keys_list
    ):
        """One broken offering must not stop the tick loop."""
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")
        mock_keys_list.sync_all.side_effect = Exception("api down")

        utils.run_periodic_api_key_reconciliation([offering], "agent")

    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    def test_the_sweep_filters_exist_in_the_installed_client(
        self, mock_get_backend, mock_get_client
    ):
        """The sweep's filters must exist in the pinned waldur-api-client.

        Every other test here mocks the endpoint, so a pin predating the
        offering_uuid / state / modified_before filters sails through them and then
        TypeErrors on the first real tick — where the offering-level handler swallows
        it, leaving an ERROR per offering per tick and a sweep that never runs. Bind
        against the real function object instead: that is what a stale pin breaks.
        """
        offering = _make_offering(order_processing_backend="envoy")
        backend = mock.Mock()
        backend.supports_resource_api_keys = True
        mock_get_backend.return_value = (backend, "1.0")

        # Captured before patching — reading it afterwards would read the Mock's
        # own (*args, **kwargs), which accepts anything and proves nothing.
        bind_only = _BindOnly(utils.marketplace_resource_api_keys_list.sync_all)

        with mock.patch.object(
            utils.marketplace_resource_api_keys_list, "sync_all", side_effect=bind_only
        ), mock.patch.object(utils.logger, "exception") as mock_log_exception:
            utils.run_periodic_api_key_reconciliation([offering], "agent")

        mock_log_exception.assert_not_called()


class _BindOnly:
    """Bind arguments against a real function's signature, then return []."""

    def __init__(self, func):
        self._signature = inspect.signature(func)

    def __call__(self, *args, **kwargs):
        self._signature.bind(*args, **kwargs)
        return []
