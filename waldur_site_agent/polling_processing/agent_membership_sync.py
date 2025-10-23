"""Agent responsible for membership control."""

from time import sleep

from waldur_api_client.models import AgentIdentity

from waldur_site_agent.backend import logger
from waldur_site_agent.common import (
    WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES,
    agent_identity_management,
)
from waldur_site_agent.common import processors as common_processors
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common import utils as common_utils


def start(configuration: common_structures.WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    waldur_offerings = configuration.waldur_offerings
    user_agent = configuration.waldur_user_agent
    # Local cache for agent identities per offering
    agent_identities: dict[str, AgentIdentity] = {}
    while True:
        logger.info("Number of offerings to process: %s", len(waldur_offerings))
        for offering in waldur_offerings:
            try:
                if offering.mqtt_enabled or offering.stomp_enabled:
                    logger.info(
                        "Skipping HTTP polling for the offering %s, "
                        "because it uses event-based processing",
                        offering.name,
                    )
                    continue

                waldur_rest_client = common_utils.get_client(
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
                resource_backend, resource_backend_version = common_utils.get_backend_for_offering(
                    offering, "membership_sync_backend"
                )

                processor = common_processors.OfferingMembershipProcessor(
                    offering,
                    waldur_rest_client,
                    resource_backend=resource_backend,
                    resource_backend_version=resource_backend_version,
                )
                processor.register(agent_service)

                processor.process_offering()
            except Exception as e:
                logger.exception("Unable to process the offering due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60)
