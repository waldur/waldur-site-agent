"""Tests for tick-based main loops ensuring heartbeat is touched frequently.

The core invariant: touch_heartbeat() must be called every TICK_INTERVAL (60s),
regardless of how long the work interval is (5–30 minutes). This prevents
Kubernetes liveness probe failures.
"""

import unittest
from unittest import mock

from waldur_site_agent.common import structures as common_structures


def _make_config(**overrides):
    """Create a mock WaldurAgentConfiguration."""
    config = mock.Mock(spec=common_structures.WaldurAgentConfiguration)
    config.waldur_offerings = overrides.get("offerings", [mock.Mock()])
    config.waldur_user_agent = "test-agent"
    config.waldur_site_agent_mode = "report"
    config.timezone = "UTC"
    config.reporting_periods = 1
    config.global_proxy = None
    return config


class TestEventProcessHealthChecksOnly(unittest.TestCase):
    """Tests for event_processing._run_health_checks_only tick loop."""

    @mock.patch("waldur_site_agent.event_processing.main.touch_heartbeat")
    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_heartbeat_touched_every_tick(self, mock_utils, mock_time, mock_touch):
        """touch_heartbeat is called on every tick, not just when health check runs."""
        from waldur_site_agent.event_processing.main import _run_health_checks_only

        config = _make_config()

        # Simulate 3 ticks: first triggers health check, others don't
        mock_time.time.side_effect = [5000.0, 5060.0, 5120.0]
        mock_time.sleep.side_effect = [None, None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            _run_health_checks_only(config)

        # Heartbeat touched on every tick (3 times)
        self.assertEqual(mock_touch.call_count, 3)
        # Health check only on first tick
        self.assertEqual(mock_utils.send_agent_health_checks.call_count, 1)

    @mock.patch("waldur_site_agent.event_processing.main.touch_heartbeat")
    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_health_check_repeats_after_interval(self, mock_utils, mock_time, mock_touch):
        """Health check runs again after HEALTH_CHECK_INTERVAL elapses."""
        from waldur_site_agent.event_processing.main import (
            HEALTH_CHECK_INTERVAL,
            _run_health_checks_only,
        )

        config = _make_config()

        first = 5000.0
        after_interval = first + HEALTH_CHECK_INTERVAL + 1

        mock_time.time.side_effect = [first, after_interval, after_interval]
        mock_time.sleep.side_effect = [None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            _run_health_checks_only(config)

        self.assertEqual(mock_utils.send_agent_health_checks.call_count, 2)

    @mock.patch("waldur_site_agent.event_processing.main.touch_heartbeat")
    @mock.patch("waldur_site_agent.event_processing.main.time")
    @mock.patch("waldur_site_agent.event_processing.main.utils")
    def test_sleeps_tick_interval(self, mock_utils, mock_time, mock_touch):
        """Loop sleeps for TICK_INTERVAL (60s), not HEALTH_CHECK_INTERVAL."""
        from waldur_site_agent.event_processing.main import TICK_INTERVAL, _run_health_checks_only

        config = _make_config()
        mock_time.time.return_value = 5000.0
        mock_time.sleep.side_effect = StopIteration("break")

        with self.assertRaises(StopIteration):
            _run_health_checks_only(config)

        mock_time.sleep.assert_called_once_with(TICK_INTERVAL)


class TestReportTickLoop(unittest.TestCase):
    """Tests for polling_processing.agent_report tick loop."""

    @mock.patch("waldur_site_agent.polling_processing.agent_report.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_report.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_report._process_offerings")
    def test_heartbeat_touched_every_tick(self, mock_process, mock_time, mock_touch):
        """touch_heartbeat is called on every tick, not just when report runs."""
        from waldur_site_agent.polling_processing.agent_report import start

        config = _make_config()

        # 3 ticks: first triggers processing, rest don't
        mock_time.time.side_effect = [5000.0, 5000.0, 5060.0, 5120.0]
        mock_time.sleep.side_effect = [None, None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_touch.call_count, 3)
        self.assertEqual(mock_process.call_count, 1)

    @mock.patch("waldur_site_agent.polling_processing.agent_report.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_report.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_report._process_offerings")
    def test_processing_repeats_after_interval(self, mock_process, mock_time, mock_touch):
        """Report processing runs again after REPORT_INTERVAL elapses."""
        from waldur_site_agent.polling_processing.agent_report import REPORT_INTERVAL, start

        config = _make_config()

        first = 5000.0
        after_interval = first + REPORT_INTERVAL + 1

        mock_time.time.side_effect = [first, first, after_interval, after_interval]
        mock_time.sleep.side_effect = [None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_process.call_count, 2)

    @mock.patch("waldur_site_agent.polling_processing.agent_report.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_report.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_report._process_offerings")
    def test_sleeps_tick_interval(self, mock_process, mock_time, mock_touch):
        """Loop sleeps for TICK_INTERVAL, not REPORT_INTERVAL."""
        from waldur_site_agent.polling_processing.agent_report import TICK_INTERVAL, start

        config = _make_config()
        mock_time.time.side_effect = [5000.0, 5000.0]
        mock_time.sleep.side_effect = StopIteration("break")

        with self.assertRaises(StopIteration):
            start(config)

        mock_time.sleep.assert_called_once_with(TICK_INTERVAL)

    @mock.patch("waldur_site_agent.polling_processing.agent_report.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_report.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_report._process_offerings")
    def test_processing_runs_immediately_on_start(self, mock_process, mock_time, mock_touch):
        """Processing runs on the first tick (last_report starts at 0.0)."""
        from waldur_site_agent.polling_processing.agent_report import start

        config = _make_config()
        mock_time.time.side_effect = [5000.0, 5000.0]
        mock_time.sleep.side_effect = StopIteration("break")

        with self.assertRaises(StopIteration):
            start(config)

        mock_process.assert_called_once()


class TestMembershipSyncTickLoop(unittest.TestCase):
    """Tests for polling_processing.agent_membership_sync tick loop."""

    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.time")
    @mock.patch(
        "waldur_site_agent.polling_processing.agent_membership_sync._process_offerings"
    )
    def test_heartbeat_touched_every_tick(self, mock_process, mock_time, mock_touch):
        """touch_heartbeat is called on every tick."""
        from waldur_site_agent.polling_processing.agent_membership_sync import start

        config = _make_config()

        mock_time.time.side_effect = [5000.0, 5000.0, 5060.0, 5120.0]
        mock_time.sleep.side_effect = [None, None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_touch.call_count, 3)
        self.assertEqual(mock_process.call_count, 1)

    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.time")
    @mock.patch(
        "waldur_site_agent.polling_processing.agent_membership_sync._process_offerings"
    )
    def test_processing_repeats_after_interval(self, mock_process, mock_time, mock_touch):
        """Sync runs again after SYNC_INTERVAL elapses."""
        from waldur_site_agent.polling_processing.agent_membership_sync import (
            SYNC_INTERVAL,
            start,
        )

        config = _make_config()
        first = 5000.0
        after_interval = first + SYNC_INTERVAL + 1

        mock_time.time.side_effect = [first, first, after_interval, after_interval]
        mock_time.sleep.side_effect = [None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_process.call_count, 2)

    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_membership_sync.time")
    @mock.patch(
        "waldur_site_agent.polling_processing.agent_membership_sync._process_offerings"
    )
    def test_sleeps_tick_interval(self, mock_process, mock_time, mock_touch):
        """Loop sleeps for TICK_INTERVAL, not SYNC_INTERVAL."""
        from waldur_site_agent.polling_processing.agent_membership_sync import (
            TICK_INTERVAL,
            start,
        )

        config = _make_config()
        mock_time.time.side_effect = [5000.0, 5000.0]
        mock_time.sleep.side_effect = StopIteration("break")

        with self.assertRaises(StopIteration):
            start(config)

        mock_time.sleep.assert_called_once_with(TICK_INTERVAL)


class TestOrderProcessTickLoop(unittest.TestCase):
    """Tests for polling_processing.agent_order_process tick loop."""

    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process._process_offerings")
    def test_heartbeat_touched_every_tick(self, mock_process, mock_time, mock_touch):
        """touch_heartbeat is called on every tick."""
        from waldur_site_agent.polling_processing.agent_order_process import start

        config = _make_config()

        mock_time.time.side_effect = [5000.0, 5000.0, 5060.0, 5120.0]
        mock_time.sleep.side_effect = [None, None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_touch.call_count, 3)
        self.assertEqual(mock_process.call_count, 1)

    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process._process_offerings")
    def test_processing_repeats_after_interval(self, mock_process, mock_time, mock_touch):
        """Order processing runs again after ORDER_PROCESS_INTERVAL elapses."""
        from waldur_site_agent.polling_processing.agent_order_process import (
            ORDER_PROCESS_INTERVAL,
            start,
        )

        config = _make_config()
        first = 5000.0
        after_interval = first + ORDER_PROCESS_INTERVAL + 1

        mock_time.time.side_effect = [first, first, after_interval, after_interval]
        mock_time.sleep.side_effect = [None, StopIteration("break")]

        with self.assertRaises(StopIteration):
            start(config)

        self.assertEqual(mock_process.call_count, 2)

    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.touch_heartbeat")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process.time")
    @mock.patch("waldur_site_agent.polling_processing.agent_order_process._process_offerings")
    def test_sleeps_tick_interval(self, mock_process, mock_time, mock_touch):
        """Loop sleeps for TICK_INTERVAL, not ORDER_PROCESS_INTERVAL."""
        from waldur_site_agent.polling_processing.agent_order_process import (
            TICK_INTERVAL,
            start,
        )

        config = _make_config()
        mock_time.time.side_effect = [5000.0, 5000.0]
        mock_time.sleep.side_effect = StopIteration("break")

        with self.assertRaises(StopIteration):
            start(config)

        mock_time.sleep.assert_called_once_with(TICK_INTERVAL)


class TestHeartbeatStalenessInvariant(unittest.TestCase):
    """Cross-cutting tests verifying the heartbeat stays fresh."""

    @mock.patch("waldur_site_agent.common.healthz.Path")
    def test_touch_heartbeat_writes_timestamp(self, mock_path):
        """touch_heartbeat writes current time to the heartbeat file."""
        from waldur_site_agent.common.healthz import touch_heartbeat

        touch_heartbeat("/tmp/test-heartbeat")  # noqa: S108
        mock_path.return_value.write_text.assert_called_once()

    def test_liveness_check_passes_for_fresh_heartbeat(self):
        """Liveness check passes when heartbeat is recent."""
        import tempfile

        from waldur_site_agent.common.healthz import check_liveness, touch_heartbeat

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            path = f.name

        touch_heartbeat(path)
        self.assertTrue(check_liveness(max_age=300, path=path))

    def test_liveness_check_fails_for_missing_heartbeat(self):
        """Liveness check fails when heartbeat file doesn't exist."""
        from waldur_site_agent.common.healthz import check_liveness

        self.assertFalse(check_liveness(path="/tmp/nonexistent-heartbeat-file"))  # noqa: S108

    def test_liveness_check_fails_for_stale_heartbeat(self):
        """Liveness check fails when heartbeat file mtime is older than max_age."""
        import os
        import tempfile
        import time

        from waldur_site_agent.common.healthz import check_liveness, touch_heartbeat

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            path = f.name

        touch_heartbeat(path)
        # Set the file mtime to 400 seconds in the past
        old_time = time.time() - 400
        os.utime(path, (old_time, old_time))
        self.assertFalse(check_liveness(max_age=300, path=path))

    def test_tick_interval_within_heartbeat_max_age(self):
        """TICK_INTERVAL must be well below DEFAULT_MAX_AGE to prevent staleness."""
        from waldur_site_agent.common.healthz import DEFAULT_MAX_AGE
        from waldur_site_agent.event_processing.main import TICK_INTERVAL

        # With failureThreshold=3 and periodSeconds=120, the probe can tolerate
        # up to 360s without a heartbeat. TICK_INTERVAL must be much less.
        self.assertLess(TICK_INTERVAL, DEFAULT_MAX_AGE)
        # Specifically, TICK_INTERVAL should be at most 1/3 of DEFAULT_MAX_AGE
        # to ensure at least 3 heartbeats within the staleness window
        self.assertLessEqual(TICK_INTERVAL, DEFAULT_MAX_AGE // 3)
