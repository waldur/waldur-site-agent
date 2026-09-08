"""Agent responsible for membership control."""

import time

from waldur_site_agent.backend import logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES,
    agent_identity_management,
)
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.healthz import touch_heartbeat

SYNC_INTERVAL = WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60
TICK_INTERVAL = 60  # Wake up every minute to touch heartbeat


def _process_offerings(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Run a single membership sync cycle for all offerings."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent

    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        # Touch the heartbeat at the start of every offering so liveness stays
        # fresh (and the heartbeat file is created on the first iteration,
        touch_heartbeat()
        try:
            use_stomp = (
                offering.stomp_membership_sync_enabled
                if offering.stomp_membership_sync_enabled is not None
                else offering.stomp_enabled
            )
            if use_stomp:
                logger.info(
                    "Skipping HTTP polling for the offering %s, "
                    "because it uses event-based processing",
                    offering.name,
                )
                continue

            waldur_rest_client = common_utils.get_client_for_offering(
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
            resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
                offering, "membership_sync_backend"
            )

            processor = common_processors.OfferingMembershipProcessor(
                offering,
                waldur_rest_client,
                resource_backend=resource_backend,
                resource_backend_version=resource_backend_version,
                expose_backend_error_details=configuration.expose_backend_error_details,
            )
            processor.register(agent_service)

            processor.process_offering()
        except Exception as e:
            logger.exception("Unable to process the offering due to the error: %s", e)


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the tick-based main loop for offering processing."""
    last_sync = 0.0
    common_utils.setup_log_shippers(configuration)
    try:
        while True:
            now = time.time()

            if now - last_sync >= SYNC_INTERVAL:
                _process_offerings(configuration)
                last_sync = time.time()

            touch_heartbeat()
            time.sleep(TICK_INTERVAL)
    finally:
        common_utils.teardown_log_shippers()
