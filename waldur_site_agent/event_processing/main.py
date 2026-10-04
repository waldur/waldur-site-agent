"""Entrypoint for event processing loop."""

import sys
import time
from typing import Optional

from waldur_site_agent.backend import logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES,
    WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES,
)
from waldur_site_agent.common import (
    structures as common_structures,
)
from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.healthz import touch_heartbeat
from waldur_site_agent.event_processing import utils
from waldur_site_agent.event_processing.watchdog import StompWatchdog

HEALTH_CHECK_INTERVAL = 30 * 60  # 30 minutes
RECONCILIATION_INTERVAL = WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES * 60
TICK_INTERVAL = 60  # Wake up every minute to check timers
STOMP_UNHEALTHY_AFTER = WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES * 60


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for event-based offering processing."""
    common_utils.setup_log_shippers(configuration)
    try:
        utils.run_initial_offering_processing(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
            expose_backend_error_details=configuration.expose_backend_error_details,
        )

        stomp_consumers_map = utils.start_stomp_consumers(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
            global_proxy=configuration.global_proxy,
            expose_backend_error_details=configuration.expose_backend_error_details,
        )

        reconciliation_enabled = any(
            o.username_reconciliation_enabled for o in configuration.waldur_offerings
        )

        watchdog = StompWatchdog(
            stomp_consumers_map,
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
            unhealthy_after=STOMP_UNHEALTHY_AFTER,
            expose_backend_error_details=configuration.expose_backend_error_details,
        )

        with utils.signal_handling(stomp_consumers_map):
            if reconciliation_enabled:
                _run_with_reconciliation(configuration, watchdog)
            else:
                _run_without_username_reconciliation(configuration, watchdog)
    except Exception as e:
        logger.exception("Error in main process: %s", e)
        if "stomp_consumers_map" in locals():
            utils.stop_stomp_consumers(stomp_consumers_map)
        sys.exit(1)
    finally:
        common_utils.teardown_log_shippers()


def _run_without_username_reconciliation(
    configuration: common_structures.WaldurAgentConfiguration,
    watchdog: Optional[StompWatchdog] = None,
) -> None:
    """Tick-based main loop: health checks, order and offering user reconciliation."""
    last_health_check = 0.0
    last_reconciliation = 0.0

    while True:
        now = time.time()
        # Withheld while STOMP stays down so the liveness probe can restart the agent.
        if watchdog is None or watchdog.check(now):
            touch_heartbeat()

        if now - last_health_check >= HEALTH_CHECK_INTERVAL:
            utils.send_agent_health_checks(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_health_check = now

        if now - last_reconciliation >= RECONCILIATION_INTERVAL:
            utils.run_periodic_order_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            utils.run_periodic_api_key_reconciliation(
                configuration.waldur_offerings,
                configuration.waldur_user_agent,
                expose_backend_error_details=configuration.expose_backend_error_details,
            )
            utils.run_periodic_offering_user_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            utils.run_periodic_project_hierarchy_sync(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_reconciliation = now

        time.sleep(TICK_INTERVAL)


def _run_with_reconciliation(
    configuration: common_structures.WaldurAgentConfiguration,
    watchdog: Optional[StompWatchdog] = None,
) -> None:
    """Tick-based main loop: health checks + periodic username and order reconciliation."""
    last_health_check = 0.0
    last_reconciliation = 0.0

    while True:
        now = time.time()
        # Withheld while STOMP stays down so the liveness probe can restart the agent.
        if watchdog is None or watchdog.check(now):
            touch_heartbeat()

        if now - last_health_check >= HEALTH_CHECK_INTERVAL:
            utils.send_agent_health_checks(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_health_check = now

        if now - last_reconciliation >= RECONCILIATION_INTERVAL:
            utils.run_periodic_username_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            utils.run_periodic_order_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            utils.run_periodic_api_key_reconciliation(
                configuration.waldur_offerings,
                configuration.waldur_user_agent,
                expose_backend_error_details=configuration.expose_backend_error_details,
            )
            utils.run_periodic_offering_user_reconciliation(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            utils.run_periodic_project_hierarchy_sync(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
            last_reconciliation = now

        time.sleep(TICK_INTERVAL)
