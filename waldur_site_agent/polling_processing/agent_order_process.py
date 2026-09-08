"""Module for order processing."""

import time

from waldur_site_agent.backend import logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES,
    agent_identity_management,
    processors,
    utils,
)
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.healthz import touch_heartbeat

ORDER_PROCESS_INTERVAL = WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES * 60
TICK_INTERVAL = 60  # Wake up every minute to touch heartbeat


def _process_offerings(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Run a single order processing cycle for all offerings."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent

    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        # Touch the heartbeat at the start of every offering so liveness stays
        # fresh (and the heartbeat file is created on the first iteration,
        touch_heartbeat()
        try:
            if offering.stomp_enabled:
                logger.info(
                    "Skipping HTTP polling for the offering %s, "
                    "because it uses event-based processing",
                    offering.name,
                )
                continue

            if not offering.order_processing_backend:
                logger.info(
                    "Order processing is disabled for offering %s, skipping it", offering.name
                )
                continue

            waldur_rest_client = utils.get_client(
                offering.api_url,
                offering.api_token,
                user_agent,
                offering.verify_ssl,
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
                offering, "order_processing_backend"
            )

            processor = processors.OfferingOrderProcessor(
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
    last_process = 0.0
    utils.setup_log_shippers(configuration)
    try:
        while True:
            now = time.time()

            if now - last_process >= ORDER_PROCESS_INTERVAL:
                _process_offerings(configuration)
                last_process = time.time()

            touch_heartbeat()
            time.sleep(TICK_INTERVAL)
    finally:
        utils.teardown_log_shippers()
