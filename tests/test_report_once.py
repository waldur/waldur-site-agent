"""waldur_site_report runs one usage-report cycle and exits."""

import unittest
from contextlib import ExitStack
from unittest import mock

from waldur_site_agent.polling_processing.agent_report import _process_offerings, cli, main

_MODULE = "waldur_site_agent.polling_processing.agent_report"


class TestReportCycleResult(unittest.TestCase):
    """The report cycle reports every offering and records whether any failed."""

    def _config(self, offerings):
        configuration = mock.Mock()
        configuration.waldur_offerings = offerings
        return configuration

    @mock.patch(f"{_MODULE}.touch_heartbeat")
    @mock.patch(f"{_MODULE}.common_processors")
    @mock.patch(f"{_MODULE}.utils")
    @mock.patch(f"{_MODULE}.agent_identity_management")
    def test_returns_true_when_every_offering_is_reported(
        self, _telemetry, mock_utils, mock_processors, _heartbeat
    ):
        mock_utils.get_backend_for_offering.return_value = (mock.Mock(), "1.0.0")
        process_offering = mock_processors.OfferingReportProcessor.return_value.process_offering
        offerings = [mock.Mock(), mock.Mock()]

        self.assertTrue(_process_offerings(self._config(offerings)))

        self.assertEqual(process_offering.call_count, 2)
        self.assertEqual(_heartbeat.call_count, 2)

    @mock.patch(f"{_MODULE}.touch_heartbeat")
    @mock.patch(f"{_MODULE}.common_processors")
    @mock.patch(f"{_MODULE}.utils")
    @mock.patch(f"{_MODULE}.agent_identity_management")
    def test_skips_the_heartbeat_when_told_not_to_record_one(
        self, _telemetry, mock_utils, mock_processors, mock_heartbeat
    ):
        mock_utils.get_backend_for_offering.return_value = (mock.Mock(), "1.0.0")

        self.assertTrue(_process_offerings(self._config([mock.Mock()]), record_heartbeat=False))

        mock_heartbeat.assert_not_called()
        mock_processors.OfferingReportProcessor.return_value.process_offering.assert_called_once()

    @mock.patch(f"{_MODULE}.touch_heartbeat")
    @mock.patch(f"{_MODULE}.common_processors")
    @mock.patch(f"{_MODULE}.utils")
    @mock.patch(f"{_MODULE}.agent_identity_management")
    def test_returns_false_and_continues_after_an_offering_fails(
        self, _telemetry, mock_utils, mock_processors, _heartbeat
    ):
        mock_utils.get_backend_for_offering.return_value = (mock.Mock(), "1.0.0")
        process_offering = mock_processors.OfferingReportProcessor.return_value.process_offering
        process_offering.side_effect = [RuntimeError("sacct failed"), None]

        self.assertFalse(_process_offerings(self._config([mock.Mock(), mock.Mock()])))

        self.assertEqual(process_offering.call_count, 2)

    @mock.patch(f"{_MODULE}.common_processors")
    def test_returns_true_when_there_are_no_offerings(self, mock_processors):
        self.assertTrue(_process_offerings(self._config([])))
        mock_processors.OfferingReportProcessor.assert_not_called()


class TestReportOnceCommand(unittest.TestCase):
    """The command loads the report configuration, runs one cycle, and exits."""

    def _patches(self, stack, argv, process_patch):
        config = mock.Mock()
        stack.enter_context(mock.patch("sys.argv", argv))
        stack.enter_context(mock.patch(f"{_MODULE}.configure_logger"))
        load = stack.enter_context(
            mock.patch(f"{_MODULE}.utils.load_configuration", return_value=config)
        )
        versions = stack.enter_context(mock.patch(f"{_MODULE}.utils.log_versions"))
        setup = stack.enter_context(mock.patch(f"{_MODULE}.utils.setup_log_shippers"))
        teardown = stack.enter_context(mock.patch(f"{_MODULE}.utils.teardown_log_shippers"))
        process = stack.enter_context(mock.patch(f"{_MODULE}._process_offerings", **process_patch))
        return config, load, versions, setup, teardown, process

    def test_reports_once_in_report_mode_and_exits_zero(self):
        with ExitStack() as stack:
            config, load, versions, setup, teardown, process = self._patches(
                stack, ["waldur_site_report", "-c", "site.yaml"], {"return_value": True}
            )
            code = main()

        self.assertEqual(code, 0)
        load.assert_called_once_with("site.yaml", user_agent_suffix="report")
        self.assertEqual(config.waldur_site_agent_mode, "report")
        versions.assert_called_once_with(config)
        process.assert_called_once_with(config, record_heartbeat=False)
        setup.assert_called_once_with(config)
        teardown.assert_called_once_with()

    def test_exits_non_zero_when_an_offering_fails(self):
        with ExitStack() as stack:
            _config, load, _versions, _setup, teardown, process = self._patches(
                stack, ["waldur_site_report"], {"return_value": False}
            )
            code = main()

        self.assertEqual(code, 1)
        load.assert_called_once_with("waldur-site-agent-config.yaml", user_agent_suffix="report")
        process.assert_called_once()
        teardown.assert_called_once_with()

    def test_log_shippers_stop_when_the_cycle_raises(self):
        with ExitStack() as stack:
            _config, _load, _versions, _setup, teardown, _process = self._patches(
                stack, ["waldur_site_report"], {"side_effect": RuntimeError("bug")}
            )
            with self.assertRaises(RuntimeError):
                main()

        teardown.assert_called_once_with()

    @mock.patch(f"{_MODULE}.main", return_value=1)
    def test_cli_exits_with_the_cycle_status(self, mock_main):
        with self.assertRaises(SystemExit) as raised:
            cli()

        self.assertEqual(raised.exception.code, 1)
        mock_main.assert_called_once_with()
