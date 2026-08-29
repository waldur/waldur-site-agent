"""Classes for managing agent identities, event subscriptions, services, and processors."""

from __future__ import annotations

import datetime

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_site_agent_identities import (
    marketplace_site_agent_identities_create,
    marketplace_site_agent_identities_list,
    marketplace_site_agent_identities_register_queue,
    marketplace_site_agent_identities_register_service,
    marketplace_site_agent_identities_update,
)
from waldur_api_client.api.marketplace_site_agent_services import (
    marketplace_site_agent_services_register_processor,
)
from waldur_api_client.models import (
    AgentIdentity,
    AgentIdentityRequest,
    AgentProcessor,
    AgentProcessorCreateRequest,
    AgentQueueRegistrationRequest,
    AgentService,
    AgentServiceCreateRequest,
    ObservableObjectTypeEnum,
)
from waldur_api_client.models.agent_dependency_request import AgentDependencyRequest

from waldur_site_agent.backend import logger
from waldur_site_agent.common import WALDUR_SITE_AGENT_VERSION, utils
from waldur_site_agent.common.structures import Offering, UnifiedQueue


class AgentIdentityDoesNotExistError(Exception):
    """Error for a missing agent identity."""


class AgentIdentityManager:
    """Manager for agent identities, event subscriptions, services, and processors."""

    def __init__(self, offering: Offering, waldur_rest_client: AuthenticatedClient) -> None:
        """Constructor."""
        self.offering = offering
        self.waldur_rest_client = waldur_rest_client

    def get_identity(self, name: str) -> AgentIdentity:
        """Get an existing identity for the agent.

        Expected to call this method after agent registration and before processing.

        Args:
            name (str): Agent identity name

        Raises:
            AgentIdentityDoesNotExist: The expected identity doesn't exist

        Returns:
            AgentIdentity: The existing agent identity
        """
        logger.info("Fetching the existing identity %s", name)
        existing_identities = marketplace_site_agent_identities_list.sync(
            client=self.waldur_rest_client, name=name
        )
        if len(existing_identities) == 0:
            message = f"Unable to get the identity {name}"
            logger.error(message)
            raise AgentIdentityDoesNotExistError(message)

        existing_identity = existing_identities[0]
        logger.info("Successfully fetched the identity %s", existing_identity.uuid.hex)

        return existing_identity

    def register_identity(self, name: str) -> AgentIdentity:
        """Register and agent identity for the offering.

        Expected to call this method right after startup of the agent.

        Args:
            name (str): Unique name of the agent within the offering

        Returns:
            AgentIdentity: Agent Identity Data
        """
        logger.info(
            "Registering a new identity for offering %s with name %s", self.offering.name, name
        )

        # Timezone-aware UTC: a naive local timestamp is serialized without
        # an offset and interpreted as UTC by the backend, shifting
        # last_restarted into the future for any agent east of UTC and
        # rendering a negative uptime in the UI.
        last_restarted = datetime.datetime.now(datetime.timezone.utc)
        dependencies = [
            AgentDependencyRequest(package=dependency["package"], version=dependency["version"])
            for dependency in utils.DEPENDENCIES
        ]

        existing_identities = marketplace_site_agent_identities_list.sync(
            client=self.waldur_rest_client, name=name
        )
        if len(existing_identities) > 0:
            logger.info("Identity %s already exists, updating it", name)
            existing_identity = existing_identities[0]
            updated_identity = marketplace_site_agent_identities_update.sync(
                uuid=existing_identity.uuid.hex,
                body=AgentIdentityRequest(
                    offering=self.offering.uuid,
                    name=name,
                    last_restarted=last_restarted,
                    dependencies=dependencies,
                    version=WALDUR_SITE_AGENT_VERSION,
                    config_file_path="",
                    config_file_content="",
                ),
                client=self.waldur_rest_client,
            )
            logger.info("Updated the identity %s, UUID %s", name, updated_identity.uuid.hex)
            return updated_identity

        body = AgentIdentityRequest(
            offering=self.offering.uuid,
            name=name,
            version=WALDUR_SITE_AGENT_VERSION,
            last_restarted=last_restarted,
            dependencies=dependencies,
            config_file_path="",
            config_file_content="",
        )
        identity = marketplace_site_agent_identities_create.sync(
            body=body, client=self.waldur_rest_client
        )
        logger.info("Registered new identity %s, UUID %s", identity.name, identity.uuid.hex)
        return identity

    def register_queue(
        self,
        identity: AgentIdentity,
        object_types: list[ObservableObjectTypeEnum],
    ) -> UnifiedQueue:
        """Register (or refresh) the single unified event-consumer queue for this identity.

        Replaces the legacy per-object-type register_event_subscription +
        create_event_subscription_queue pair: one call provisions one RabbitMQ
        queue (consumer_{uuid}) that receives ALL the requested object types,
        with the object type carried in each message payload. The call is
        idempotent: re-registering returns the existing queue and refreshes the
        RabbitMQ password to match the presented API token.

        Args:
            identity: Agent identity owning the queue.
            object_types: Observable object types to receive. An empty list means
                all types; a non-empty list narrows the queue.

        Returns:
            UnifiedQueue: queue name, RMQ username/vhost, and the effective types.
        """
        logger.info(
            "Registering unified event queue for identity %s with object types %s",
            identity.name,
            [ot.value for ot in object_types],
        )
        body = AgentQueueRegistrationRequest(object_types=object_types)
        data = marketplace_site_agent_identities_register_queue.sync(
            uuid=identity.uuid.hex,
            body=body,
            client=self.waldur_rest_client,
        )
        logger.info(
            "Registered unified queue %s (rmq user %s) for identity %s",
            data.queue_name,
            data.rmq_username,
            identity.name,
        )
        return UnifiedQueue(
            queue_name=data.queue_name,
            rmq_username=data.rmq_username,
            vhost=data.vhost,
            observable_object_types=list(data.observable_object_types),
            agent_identity_uuid=identity.uuid.hex,
        )

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
