"""Classes for managing agent identities, event subscriptions, services, and processors."""

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_site_agent_identities import (
    marketplace_site_agent_identities_create,
    marketplace_site_agent_identities_list,
    marketplace_site_agent_identities_register_event_subscription,
    marketplace_site_agent_identities_register_service,
)
from waldur_api_client.api.marketplace_site_agent_services import (
    marketplace_site_agent_services_register_processor,
)
from waldur_api_client.models.agent_event_subscription_create_request import (
    AgentEventSubscriptionCreateRequest,
)
from waldur_api_client.models.agent_identity import AgentIdentity
from waldur_api_client.models.agent_identity_request import AgentIdentityRequest
from waldur_api_client.models.agent_processor import AgentProcessor
from waldur_api_client.models.agent_processor_create_request import AgentProcessorCreateRequest
from waldur_api_client.models.agent_service import AgentService
from waldur_api_client.models.agent_service_create_request import AgentServiceCreateRequest
from waldur_api_client.models.event_subscription import EventSubscription
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.backend import logger
from waldur_site_agent.common.structures import Offering


class AgentIdentityManager:
    """Manager for agent identities, event subscriptions, services, and processors."""

    def __init__(self, offering: Offering, waldur_rest_client: AuthenticatedClient) -> None:
        """Constructor."""
        self.offering = offering
        self.waldur_rest_client = waldur_rest_client

    def register_identity(self, name: str) -> AgentIdentity:
        """Register and agent identity for the offering.

        Args:
            name (str): Unique name of the agent within the offering

        Returns:
            AgentIdentity: Agent Identity Data
        """
        logger.info(
            "Registering a new identity for offering %s with name %s", self.offering.name, name
        )

        existing_identities = marketplace_site_agent_identities_list.sync(
            client=self.waldur_rest_client, name=name
        )
        if len(existing_identities) > 0:
            logger.info("Identity with name %s already exists, skipping creation", name)
            return existing_identities[0]

        body = AgentIdentityRequest(
            offering=self.offering.uuid,
            name=name,
            dependencies=[],
            config_file_path="",
            config_file_content="",
        )
        identity = marketplace_site_agent_identities_create.sync(
            body=body, client=self.waldur_rest_client
        )
        logger.info("Registered new identity %s with UUID %s", identity.name, identity.uuid.hex)
        return identity

    def register_event_subscription(
        self, identity: AgentIdentity, object_type: ObservableObjectTypeEnum
    ) -> EventSubscription:
        """Register an event subscription within the agent identity for the specified object type.

        Args:
            identity (AgentIdentity): Agent identity
            object_type (str): type of the observable object

        Returns:
            EventSubscription: Event subscription data
        """
        logger.info(
            "Registering event subscription for identity %s and object type %s",
            identity.name,
            object_type,
        )
        body = AgentEventSubscriptionCreateRequest(
            observable_object_type=object_type,
            description="Event subscription created by the site agent for identity {identity.name}",
        )
        event_subscription = marketplace_site_agent_identities_register_event_subscription.sync(
            uuid=identity.uuid.hex,
            body=body,
            client=self.waldur_rest_client,
        )
        logger.info("Registered new event subscription with UUID %s", event_subscription.uuid.hex)
        return event_subscription

    def register_service(self, identity: AgentIdentity, name: str, mode: str) -> AgentService:
        """Register a service within the agent identity.

        Args:
            identity (AgentIdentity): Agent identity
            name (str): Name of the service
            mode (str): Mode of the service

        Returns:
            AgentService: Agent service data
        """
        logger.info("Registering service %s for identity %s", name, identity.name)
        body = AgentServiceCreateRequest(
            name=name,
            mode=mode,
        )
        service = marketplace_site_agent_identities_register_service.sync(
            uuid=identity.uuid.hex,
            body=body,
            client=self.waldur_rest_client,
        )
        logger.info("Registered new service %s with UUID %s", service.name, service.uuid.hex)
        return service

    def register_processor(
        self, service: AgentService, processor_name: str, backend_type: str, backend_version: str
    ) -> AgentProcessor:
        """Register a processor within the agent service.

        Args:
            service (AgentService): Agent service
            processor_name (str): Name of the processor
            backend_type (str): Backend type
            backend_version (str): Backend version

        Returns:
            AgentProcessor: Agent processor data
        """
        logger.info(
            "Registering processor %s for service %s, backend info: %s, %s",
            processor_name,
            service.name,
            backend_type,
            backend_version,
        )
        body = AgentProcessorCreateRequest(
            name=processor_name,
            backend_type=backend_type,
            backend_version=backend_version,
        )
        processor = marketplace_site_agent_services_register_processor.sync(
            uuid=service.uuid.hex,
            body=body,
            client=self.waldur_rest_client,
        )
        logger.info("Registered new processor %s with UUID %s", processor.name, processor.uuid.hex)
        return processor
