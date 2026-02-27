"""Entrypoint for event processing loop."""

import sys
import time

from waldur_site_agent.backend import logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES,
)
from waldur_site_agent.common import (
    structures as common_structures,
)
from waldur_site_agent.event_processing import utils

HEALTH_CHECK_INTERVAL = 30 * 60  # 30 minutes
RECONCILIATION_INTERVAL = WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES * 60
TICK_INTERVAL = 60  # Wake up every minute to check timers


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for event-based offering processing."""
    try:
        utils.run_initial_offering_processing(
            configuration.waldur_offerings, configuration.waldur_user_agent
        )

        stomp_consumers_map = utils.start_stomp_consumers(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
        )

        reconciliation_enabled = any(
            o.username_reconciliation_enabled for o in configuration.waldur_offerings
        )

        with utils.signal_handling(stomp_consumers_map):
            if reconciliation_enabled:
                _run_with_reconciliation(configuration)
            else:
                _run_health_checks_only(configuration)
    except Exception as e:
        logger.exception("Error in main process: %s", e)
        if "stomp_consumers_map" in locals():
            utils.stop_stomp_consumers(stomp_consumers_map)
        sys.exit(1)


def _run_health_checks_only(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Original main loop: health checks every 30 minutes."""
    while True:
        utils.send_agent_health_checks(
            configuration.waldur_offerings, configuration.waldur_user_agent
        )
        time.sleep(HEALTH_CHECK_INTERVAL)


def _run_with_reconciliation(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Tick-based main loop: health checks + periodic username reconciliation."""
    last_health_check = 0.0
    last_reconciliation = 0.0

    while True:
        now = time.time()

        if now - last_health_check >= HEALTH_CHECK_INTERVAL:
            utils.send_agent_health_checks(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_health_check = now

        if now - last_reconciliation >= RECONCILIATION_INTERVAL:
            utils.run_periodic_username_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_reconciliation = now

        time.sleep(TICK_INTERVAL)
