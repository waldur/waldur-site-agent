"""Agent responsible for usage and limits reporting."""

import argparse
import sys
import time

from waldur_site_agent.backend import configure_logger, logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES,
    agent_identity_management,
    utils,
)
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.healthz import touch_heartbeat

REPORT_INTERVAL = WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60
TICK_INTERVAL = 60  # Wake up every minute to touch heartbeat


def _process_offerings(
    configuration: common_structures.WaldurAgentConfiguration,
    record_heartbeat: bool = True,
) -> bool:
    """Run a single report cycle for all offerings.

    Returns True when every offering was reported. A failed offering is logged
    and the remaining offerings are still reported.

    ``record_heartbeat`` is for the long-running agent, whose liveness probe
    reads the heartbeat file. The one-shot command leaves it false: that
    process is not probed, and writing the default file would keep a stalled
    daemon on the same host looking alive.
    """
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent
    failed = False

    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        if record_heartbeat:
            touch_heartbeat()
        try:
            waldur_rest_client = utils.get_client_for_offering(
                offering,
                user_agent,
                configuration.global_proxy,
            )

            agent_service = agent_identity_management.ensure_agent_telemetry(
                offering,
                waldur_rest_client,
                configuration.waldur_site_agent_mode,
                configuration.log_shipping,
            )

            # Create backend instance for dependency injection
            resource_backend, resource_backend_version = utils.get_backend_for_offering(
                offering, "reporting_backend"
            )

            processor = common_processors.OfferingReportProcessor(
                offering,
                waldur_rest_client,
                configuration.timezone,
                resource_backend=resource_backend,
                resource_backend_version=resource_backend_version,
                reporting_periods=configuration.reporting_periods,
                api_key_final_report_days=configuration.api_key_final_report_days,
                expose_backend_error_details=configuration.expose_backend_error_details,
            )
            processor.register(agent_service)

            processor.process_offering()
        except Exception as e:
            failed = True
            logger.exception("The application crashed due to the error: %s", e)
    return not failed


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the tick-based main loop for offering processing."""
    logger.info("Synching data to Waldur")
    last_report = 0.0
    utils.setup_log_shippers(configuration)
    try:
        while True:
            now = time.time()

            if now - last_report >= REPORT_INTERVAL:
                _process_offerings(configuration)
                last_report = time.time()

            touch_heartbeat()
            time.sleep(TICK_INTERVAL)
    finally:
        utils.teardown_log_shippers()


def main() -> int:
    """Report usage for every configured offering once, then exit.

    Same cycle as the long-running ``report`` agent. The exit status is 0 when
    every offering was reported, and 1 when at least one failed.
    """
    parser = argparse.ArgumentParser(
        description="Report usage to Waldur once and exit.",
    )
    utils.add_config_file_argument(parser)
    config_file_path = parser.parse_args().config_file_path

    # JSON logging before any log line, as init_configuration does for the agent.
    configure_logger()
    logger.info("Using %s as a config source", config_file_path)
    configuration = utils.load_configuration(
        config_file_path,
        user_agent_suffix=common_structures.AgentMode.REPORT.value,
    )
    configuration.waldur_site_agent_mode = common_structures.AgentMode.REPORT.value
    logger.info("Waldur site agent version: %s", configuration.waldur_site_agent_version)
    utils.log_versions(configuration)

    utils.setup_log_shippers(configuration)
    try:
        succeeded = _process_offerings(configuration, record_heartbeat=False)
        if not succeeded:
            logger.error("Usage report failed for one or more offerings")
    finally:
        utils.teardown_log_shippers()
    return 0 if succeeded else 1


def cli() -> None:
    """Console-script entry point for ``waldur_site_report``."""
    sys.exit(main())
