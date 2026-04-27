"""Agent responsible for usage and limits reporting."""

import time

from waldur_api_client.models import AgentIdentity

from waldur_site_agent.backend import logger
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
    agent_identities: dict[str, AgentIdentity],
) -> None:
    """Run a single report cycle for all offerings."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent

    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        touch_heartbeat()
        try:
            waldur_rest_client = utils.get_client(
                offering.api_url,
                offering.api_token,
                user_agent,
                offering.verify_ssl,
                configuration.global_proxy,
            )

            agent_identity_manager = agent_identity_management.AgentIdentityManager(
                offering, waldur_rest_client
            )

            identity_name = f"agent-{offering.uuid}"

            # Get an identity from the local cache
            agent_identity = agent_identities.get(offering.uuid)
            if agent_identity is None:
                # If no identities found locally, registering one
                agent_identity = agent_identity_manager.register_identity(identity_name)

            agent_service = agent_identity_manager.register_service(
                agent_identity,
                configuration.waldur_site_agent_mode,
                configuration.waldur_site_agent_mode,
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
            )
            processor.register(agent_service)

            processor.process_offering()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the tick-based main loop for offering processing."""
    logger.info("Synching data to Waldur")
    last_report = 0.0
    agent_identities: dict[str, AgentIdentity] = {}

    while True:
        now = time.time()

        if now - last_report >= REPORT_INTERVAL:
            _process_offerings(configuration, agent_identities)
            last_report = time.time()

        touch_heartbeat()
        time.sleep(TICK_INTERVAL)
