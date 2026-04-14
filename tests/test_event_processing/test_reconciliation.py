"""Tests for periodic username/order/offering-user reconciliation and event processing main loop."""

import unittest
from unittest import mock

from waldur_api_client.models.offering_user_state import OfferingUserState

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
            "waldur_site_agent.event_processing.utils.get_client"
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
            "waldur_site_agent.event_processing.utils.get_client"
        ) as mock_get_client:
            utils.run_periodic_username_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
            offering.uuid, mock_get_client.return_value
        )

    @mock.patch("waldur_site_agent.event_processing.utils.get_backend_for_offering")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
            "waldur_site_agent.event_processing.utils.get_client"
        ) as mock_get_client:
            utils.run_periodic_order_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch(
        "waldur_site_agent.event_processing.utils.common_processors.OfferingOrderProcessor"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.marketplace_orders_list")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
            "waldur_site_agent.event_processing.utils.get_client"
        ) as mock_get_client:
            utils.run_periodic_offering_user_reconciliation([offering], "agent")
            mock_get_client.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.common_utils.update_offering_users")
    @mock.patch(
        "waldur_site_agent.event_processing.utils.marketplace_offering_users_list"
    )
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
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
    def test_health_check_and_reconciliation_run_on_first_tick(self, mock_utils, mock_time):
        """Both health check and reconciliation run immediately on first iteration."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"

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
    def test_health_check_not_repeated_within_interval(self, mock_utils, mock_time):
        """Health check does not repeat when interval hasn't elapsed."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"

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
    def test_initial_processing_runs_before_loop(self, mock_utils, mock_time):
        """run_initial_offering_processing is called before the main loop."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"

        # Make start_stomp_consumers raise to exit early
        mock_utils.run_initial_offering_processing.return_value = None
        mock_utils.start_stomp_consumers.side_effect = RuntimeError("stop")

        with self.assertRaises(SystemExit):
            main.start(config)

        mock_utils.run_initial_offering_processing.assert_called_once_with(
            config.waldur_offerings, config.waldur_user_agent
        )

    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_exception_stops_consumers_and_exits(self, mock_utils, mock_time):
        """Fatal exception in the loop stops STOMP consumers and calls sys.exit(1)."""
        from waldur_site_agent.event_processing import main

        config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
        config.waldur_offerings = [mock.Mock()]
        config.waldur_user_agent = "test-agent"

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
