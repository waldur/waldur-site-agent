"""Abstract offering processors for different agent operational modes.

This module provides the core abstract base classes that define the interface
and common functionality for processing Waldur offerings. The processors handle
the integration between Waldur Mastermind and backend systems through different
operational patterns:

- OfferingOrderProcessor: Handles order lifecycle (create, update, terminate)
- OfferingMembershipProcessor: Manages user membership synchronization
- OfferingReportProcessor: Reports usage data from backends to Waldur

Each processor type implements specific aspects of the Waldur-backend integration
while sharing common patterns for error handling, retry logic, and API communication.
"""

from __future__ import annotations

import abc
import traceback
from time import sleep
from typing import Optional

from waldur_api_client.api.backend_resource_requests import (
    backend_resource_requests_retrieve,
    backend_resource_requests_set_done,
    backend_resource_requests_set_erred,
    backend_resource_requests_start_processing,
)
from waldur_api_client.api.backend_resources import backend_resources_create, backend_resources_list
from waldur_api_client.api.component_user_usage_limits import component_user_usage_limits_list
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_list,
    marketplace_component_usages_set_usage,
    marketplace_component_usages_set_user_usage,
)
from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_list,
)
from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_approve_by_provider,
    marketplace_orders_list,
    marketplace_orders_retrieve,
    marketplace_orders_set_state_done,
    marketplace_orders_set_state_erred,
)
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
    marketplace_provider_resources_refresh_last_sync,
    marketplace_provider_resources_retrieve,
    marketplace_provider_resources_set_as_ok,
    marketplace_provider_resources_set_backend_id,
    marketplace_provider_resources_set_backend_metadata,
    marketplace_provider_resources_team_list,
)
from waldur_api_client.api.marketplace_service_providers import (
    marketplace_service_providers_list,
    marketplace_service_providers_project_service_accounts_list,
)
from waldur_api_client.api.projects import projects_list
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import (
    ComponentUsageCreateRequest,
    ComponentUsageItemRequest,
)
from waldur_api_client.models.backend_resource_request import BackendResourceRequest
from waldur_api_client.models.backend_resource_request_set_erred_request import (
    BackendResourceRequestSetErredRequest,
)
from waldur_api_client.models.component_usage import ComponentUsage
from waldur_api_client.models.component_user_usage_create_request import (
    ComponentUserUsageCreateRequest,
)
from waldur_api_client.models.component_user_usage_limit import ComponentUserUsageLimit
from waldur_api_client.models.marketplace_orders_list_state_item import (
    MarketplaceOrdersListStateItem,
)
from waldur_api_client.models.marketplace_provider_resources_list_state_item import (
    MarketplaceProviderResourcesListStateItem,
)
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state_enum import OfferingUserStateEnum
from waldur_api_client.models.order_details import (
    OrderDetails,
)
from waldur_api_client.models.order_details_limits import (
    OrderDetailsLimits,
)
from waldur_api_client.models.order_set_state_erred_request import OrderSetStateErredRequest
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.project_user import ProjectUser
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_backend_id_request import ResourceBackendIDRequest
from waldur_api_client.models.resource_backend_metadata_request import (
    ResourceBackendMetadataRequest,
)
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import Unset

from waldur_site_agent.backend import BackendType, logger
from waldur_site_agent.backend import exceptions as backend_exceptions
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import structures, utils


class UsageAnomalyError(Exception):
    """Raised when usage anomaly is detected.

    This exception is raised when the system detects that new usage data
    is lower than previously reported usage for the same billing period,
    which typically indicates a data collection or processing error.
    """


class ObjectNotFoundError(Exception):
    """Raised when a required object cannot be found in Waldur or backend.

    This exception is used throughout the processors when attempting to
    retrieve resources, users, or other objects that should exist but
    cannot be located in either Waldur or the backend system.
    """


class OfferingBaseProcessor(abc.ABC):
    """Abstract base class for all offering processors.

    This class provides the common foundation for all offering processors,
    including Waldur API client setup, backend initialization, and shared
    utility methods. All concrete processor implementations inherit from
    this class to ensure consistent behavior and interface.

    The processor handles:
    - Waldur REST API client configuration and authentication
    - Backend system selection and initialization based on offering configuration
    - Username management backend setup
    - Common error handling and logging patterns

    Attributes:
        BACKEND_TYPE_KEY: Key used to identify which backend to use for this processor
        offering: The offering configuration being processed
        timezone: Timezone for billing period calculations
        waldur_rest_client: Authenticated client for Waldur API access
        resource_backend: Backend implementation for this offering
    """

    BACKEND_TYPE_KEY = "abstract"

    def __init__(
        self, offering: structures.Offering, user_agent: str = "", timezone: str = ""
    ) -> None:
        """Initialize the offering processor.

        Args:
            offering: The offering configuration to process
            user_agent: HTTP User-Agent string for API requests
            timezone: Timezone for billing period calculations (defaults to UTC)

        Raises:
            BackendError: If unable to create a backend for the offering
        """
        self.offering: structures.Offering = offering
        self.timezone: str = timezone
        self.waldur_rest_client: utils.AuthenticatedClient = utils.get_client(
            offering.api_url, offering.api_token, user_agent, offering.verify_ssl
        )
        self.resource_backend = utils.get_backend_for_offering(offering, self.BACKEND_TYPE_KEY)
        if self.resource_backend.backend_type == BackendType.UNKNOWN.value:
            raise backend_exceptions.BackendError(f"Unable to create backend for {self.offering}")

        self._print_current_user()

        self.waldur_offering = marketplace_provider_offerings_retrieve.sync(
            client=self.waldur_rest_client, uuid=self.offering.uuid
        )
        utils.extend_backend_components(self.offering, self.waldur_offering.components)

    def _print_current_user(self) -> None:
        """Log information about the current authenticated Waldur user."""
        current_user = utils.get_current_user_from_client(self.waldur_rest_client)
        utils.print_current_user(current_user)

    @abc.abstractmethod
    def process_offering(self) -> None:
        """Process the offering according to the specific processor type.

        This method must be implemented by concrete processor classes to handle
        their specific responsibilities (order processing, membership sync, or reporting).
        """

    def _update_offering_users(self, offering_users: list[OfferingUser]) -> None:
        """Generate usernames for offering users and update their state accordingly.

        Args:
            offering_users: List of offering users to process
        """
        utils.update_offering_users(self.offering, self.waldur_rest_client, offering_users)


class OfferingOrderProcessor(OfferingBaseProcessor):
    """Processor for handling Waldur marketplace orders.

    This processor fetches pending and executing orders from Waldur and manages
    their lifecycle by creating, updating, or terminating backend resources.
    It handles the complete order workflow from approval through completion.

    The processor supports three types of orders:
    - CREATE: Creates new resources in the backend system
    - UPDATE: Modifies existing resource limits or configurations
    - TERMINATE: Removes resources from the backend system

    Order processing includes user context management, retry logic for failures,
    and comprehensive error handling with automatic order state updates.
    """

    BACKEND_TYPE_KEY = "order_processing_backend"

    def log_order_processing_error(self, order: OrderDetails, e: Exception) -> None:
        """Log detailed error information for order processing failures.

        Args:
            order: The order that failed to process
            e: The exception that occurred during processing
        """
        name = order.attributes.get("name", "N/A") if order.attributes else "N/A"
        logger.exception(
            "Error while processing order %s (%s), type %s, state %s: %s",
            order.uuid,
            name,
            order.type_,
            order.state,
            e,
        )

    def process_offering(self) -> None:
        """Process all pending and executing orders for this offering.

        Fetches orders from Waldur with PENDING_PROVIDER or EXECUTING status
        and processes each order according to its type and current state.
        Includes error handling and logging for individual order failures.
        """
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )
        orders: list[OrderDetails] | None = marketplace_orders_list.sync(
            client=self.waldur_rest_client,
            offering_uuid=self.offering.uuid,
            state=[
                MarketplaceOrdersListStateItem.PENDING_PROVIDER,
                MarketplaceOrdersListStateItem.EXECUTING,
            ],
        )

        if not orders:
            logger.info("There are no pending or executing orders")
            return
        for order in orders:
            try:
                self.process_order_with_retries(order)
            except Exception as e:
                self.log_order_processing_error(order, e)

    def get_order_info(self, order_uuid: str) -> Optional[OrderDetails]:
        """Retrieve current order information from Waldur API.

        Args:
            order_uuid: UUID of the order to retrieve

        Returns:
            OrderDetails if found, None if retrieval fails
        """
        try:
            return marketplace_orders_retrieve.sync(client=self.waldur_rest_client, uuid=order_uuid)
        except UnexpectedStatus as e:
            logger.error("Failed to get order %s info: %s", order_uuid, e)
            return None

    def process_order_with_retries(
        self, order_info: OrderDetails, retry_count: int = 10, delay: int = 5
    ) -> None:
        """Process an order with automatic retry on failures.

        Args:
            order_info: The order to process
            retry_count: Maximum number of retry attempts (default: 10)
            delay: Delay in seconds between retry attempts (default: 5)
        """
        for attempt_number in range(retry_count):
            try:
                logger.info("Attempt %s of %s", attempt_number + 1, retry_count)
                order: Optional[OrderDetails] = self.get_order_info(order_info.uuid.hex)
                if order is None:
                    logger.error("Failed to get order %s info", order_info.uuid)
                    return
                self.process_order(order)
                break
            except UnexpectedStatus as e:
                if order is not None:
                    self.log_order_processing_error(order, e)
                else:
                    self.log_order_processing_error(order_info, e)
                logger.info("Retrying order %s processing in %s seconds", order_info.uuid, delay)
                sleep(delay)

        if attempt_number == retry_count - 1:
            logger.error(
                "Failed to process order %s after %s retries, skipping to the next one",
                order_info.uuid,
                retry_count,
            )

    def process_order(self, order: OrderDetails) -> None:
        """Process a single order through its complete lifecycle.

        Handles order approval (if needed), determines the order type,
        and delegates to the appropriate processing method. Updates
        order state to DONE on success or ERRED on failure.

        Args:
            order: The order to process
        """
        try:
            logger.info(
                "Processing order %s (%s) type %s, state %s",
                order.attributes.get("name", "N/A") if order.attributes else "N/A",
                order.uuid,
                order.type_,
                order.state,
            )

            if order.state in [OrderState.DONE, OrderState.ERRED]:
                logger.info(
                    "Order %s (%s) is in finished state %s, skipping processing",
                    order.attributes.get("name", "N/A") if order.attributes else "N/A",
                    order.uuid,
                    order.state,
                )
                return

            if order.state == OrderState.EXECUTING:
                logger.info("Order is executing already, no need for approval")
            elif order.state == OrderState.PENDING_PROVIDER:
                logger.info("Approving the order")
                marketplace_orders_approve_by_provider.sync_detailed(
                    client=self.waldur_rest_client, uuid=order.uuid.hex
                )
                logger.info("Refreshing the order")
                order = marketplace_orders_retrieve.sync(
                    client=self.waldur_rest_client, uuid=order.uuid.hex
                )
            else:
                logger.warning(
                    "The order %s %s (%s) is in unexpected state %s, skipping processing",
                    order.type_,
                    order.resource_name,
                    order.uuid,
                    order.state,
                )
                return

            order_is_done = False
            if order.type_ == RequestTypes.CREATE:
                order_is_done = self._process_create_order(order)

            if order.type_ == RequestTypes.UPDATE:
                order_is_done = self._process_update_order(order)

            if order.type_ == RequestTypes.TERMINATE:
                order_is_done = self._process_terminate_order(order)
            # TODO: no need for update of orders for marketplace SLURM offerings
            if order_is_done:
                logger.info("Marking order as done")
                marketplace_orders_set_state_done.sync_detailed(
                    client=self.waldur_rest_client, uuid=order.uuid
                )

                logger.info("The order has been successfully processed")
            else:
                logger.warning("The order processing was not finished, skipping to the next one")

        except Exception as e:
            logger.exception(
                "Error while processing order %s: %s",
                order.uuid,
                e,
            )
            order_set_state_erred_request = OrderSetStateErredRequest(
                error_message=str(e),
                error_traceback=traceback.format_exc(),
            )
            marketplace_orders_set_state_erred.sync_detailed(
                client=self.waldur_rest_client, uuid=order.uuid, body=order_set_state_erred_request
            )

    def _create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: dict,
    ) -> BackendResourceInfo | None:
        """Create a new resource in the backend system.

        Args:
            waldur_resource: Waldur resource information
            user_context: User context containing team and offering user data

        Returns:
            Created backend resource or None if creation failed

        Raises:
            BackendError: If resource creation fails
        """
        logger.info("Creating resource %s", waldur_resource.name)

        # Use the provided user context for resource creation
        backend_resource_info = self.resource_backend.create_resource(waldur_resource, user_context)
        if backend_resource_info.backend_id == "":
            msg = f"Unable to create a backend resource for offering {self.offering}"
            raise backend_exceptions.BackendError(msg)

        logger.info("Updating resource metadata in Waldur")
        marketplace_provider_resources_set_backend_id.sync(
            client=self.waldur_rest_client,
            uuid=waldur_resource.uuid.hex,
            body=ResourceBackendIDRequest(backend_id=backend_resource_info.backend_id),
        )

        return backend_resource_info

    def _fetch_user_context_for_resource(self, resource_uuid: str) -> dict:
        """Fetch comprehensive user context for resource operations.

        Retrieves project team members and offering users, creating mappings
        for efficient user lookup during resource creation and management.

        Args:
            resource_uuid: UUID of the resource to fetch context for

        Returns:
            Dictionary containing:
            - team: List of project team members
            - offering_users: List of offering users with usernames
            - user_mappings: Mapping of user UUIDs to ProjectUser objects
            - offering_user_mappings: Mapping of user UUIDs to OfferingUser objects

        Raises:
            ObjectNotFoundError: If no team members found for the resource
        """
        try:
            logger.info("Fetching user context for resource %s", resource_uuid)
            # Get project team members
            team: list[ProjectUser] | None = marketplace_provider_resources_team_list.sync(
                client=self.waldur_rest_client, uuid=resource_uuid
            )

            if not team:
                raise ObjectNotFoundError(f"No team members found for resource {resource_uuid}")  # noqa: TRY301

            user_uuids = {user.uuid for user in team}

            # Get offering users
            offering_users_all: list[OfferingUser] | None = marketplace_offering_users_list.sync(
                client=self.waldur_rest_client,
                offering_uuid=self.offering.uuid,
                is_restricted=False,
            )

            if not offering_users_all:
                logger.warning("No offering users found for offering %s", self.offering.uuid)
                offering_users = []
            else:
                # Filter offering users to only those in the project team
                offering_users = [
                    offering_user
                    for offering_user in offering_users_all
                    if offering_user.user_uuid in user_uuids
                ]

            # Create user mappings for easy lookup
            user_mappings = {user.uuid: user for user in team}
            offering_user_mappings = {
                offering_user.user_uuid: offering_user for offering_user in offering_users
            }
        except Exception as e:
            logger.warning("Failed to fetch user context for resource %s: %s", resource_uuid, e)
            return {
                "team": [],
                "offering_users": [],
                "user_mappings": {},
                "offering_user_mappings": {},
            }
        else:
            return {
                "team": team,
                "offering_users": offering_users,
                "user_mappings": user_mappings,
                "offering_user_mappings": offering_user_mappings,
            }

    def _add_users_to_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: dict,
    ) -> None:
        """Add offering users to the backend resource.

        Updates offering user usernames if needed and adds all users
        with valid usernames to the backend resource.

        Args:
            waldur_resource: The Waldur resource to add users to
            user_context: User context containing offering users and mappings
        """
        logger.info("Adding users to resource")
        offering_users = user_context["offering_users"]

        # Update offering user usernames (only for users with blank usernames)
        self._update_offering_users(offering_users)

        # Refresh local offering users
        user_context["offering_users"] = marketplace_offering_users_list.sync(
            offering_uuid=self.offering.uuid,
            is_restricted=False,
            client=self.waldur_rest_client,
        )

        # Use only non-blank usernames from users in OK state
        offering_usernames: set[str] = {
            offering_user.username
            for offering_user in user_context["offering_users"]
            if offering_user.state == OfferingUserStateEnum.OK and offering_user.username
        }

        if not offering_usernames:
            logger.info("No users to add to resource")
            return

        logger.info("Adding usernames to resource in backend")
        self.resource_backend.add_users_to_resource(
            waldur_resource.backend_id,
            offering_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

    def _process_create_order(self, order: OrderDetails | None) -> bool:
        """Process a CREATE order to establish a new resource.

        Waits for Waldur resource creation, fetches user context,
        creates the backend resource, and adds users to it.

        Args:
            order: The CREATE order to process

        Returns:
            True if processing completed successfully, False otherwise
        """
        if not order:
            logger.error("Error during order processing: Order is None")
            return False
        # Wait until Waldur resource is created
        attempts = 0
        max_attempts = 4
        while not order.marketplace_resource_uuid:
            if attempts > max_attempts:
                logger.error("Order processing timed out")
                return False

            if order.state != OrderState.EXECUTING:
                logger.error("Order has unexpected state %s", order.state)
                return False

            logger.info("Waiting for resource creation...")
            sleep(5)
            order_uuid = order.uuid
            updated_order: OrderDetails | None = marketplace_orders_retrieve.sync(
                client=self.waldur_rest_client, uuid=order_uuid
            )
            if not updated_order:
                raise ValueError(f"Failed to get order {order_uuid} info")
            order = updated_order
            attempts += 1
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=order.marketplace_resource_uuid.hex, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(
                f"Waldur resource {order.marketplace_resource_uuid.hex} not found"
            )
        create_resource = True
        if waldur_resource.backend_id != "":
            logger.info(
                "Waldur resource backend id is not empty %s, checking backend resource data",
                waldur_resource.backend_id,
            )
            backend_resource_info = self.resource_backend.pull_resource(waldur_resource)
            if backend_resource_info is not None:
                logger.info(
                    "Resource %s (%s) is already created, skipping creation",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                )
                create_resource = False
        # Fetch user context once for both resource creation and user addition
        user_context = self._fetch_user_context_for_resource(order.marketplace_resource_uuid.hex)

        backend_resource_info = None
        if create_resource:
            backend_resource_info = self._create_resource(waldur_resource, user_context)
            if backend_resource_info is None:
                msg = f"Unable to create the resource {waldur_resource.name}"
                raise backend_exceptions.BackendError(msg)

        if backend_resource_info is None:
            return False

        waldur_resource.backend_id = backend_resource_info.backend_id

        self._add_users_to_resource(waldur_resource, user_context)

        return True

    def _process_update_order(self, order: OrderDetails) -> bool:
        """Process an UPDATE order to modify resource limits.

        Args:
            order: The UPDATE order containing new limit specifications

        Returns:
            True if update completed successfully, False otherwise

        Raises:
            ObjectNotFoundError: If the target resource is not found
        """
        logger.info("Updating limits for %s", order.resource_name)
        resource_uuid = order.marketplace_resource_uuid
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")

        waldur_resource_backend_id = waldur_resource.backend_id
        new_limits: Unset | OrderDetailsLimits = order.limits
        if not new_limits:
            logger.error(
                "Order %s (resource %s) with type" + "Update does not include new limits",
                order.uuid,
                waldur_resource.name,
            )

        self.resource_backend.set_resource_limits(waldur_resource_backend_id, new_limits.to_dict())

        logger.info(
            "The limits for %s were updated successfully from %s to %s",
            waldur_resource.name,
            order.attributes.get("old_limits", "N/A") if order.attributes else "N/A",
            new_limits.to_dict() if new_limits else "N/A",
        )
        return True

    def _process_terminate_order(self, order: OrderDetails) -> bool:
        """Process a TERMINATE order to remove a resource.

        Args:
            order: The TERMINATE order specifying the resource to remove

        Returns:
            True if termination completed successfully, False otherwise

        Raises:
            ObjectNotFoundError: If the target resource is not found
        """
        logger.info("Terminating resource %s", order.resource_name)
        resource_uuid = order.marketplace_resource_uuid
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")
        project_slug = order.project_slug

        self.resource_backend.delete_resource(waldur_resource, project_slug=project_slug)

        logger.info("Allocation has been terminated successfully")
        return True


class OfferingMembershipProcessor(OfferingBaseProcessor):
    """Processor for synchronizing user memberships between Waldur and backends.

    This processor handles bidirectional synchronization of user access and
    memberships between Waldur and backend systems. It processes resource
    status changes, user additions/removals, and maintains consistency
    between the systems.

    Key responsibilities:
    - Synchronize user lists between Waldur project teams and backend resources
    - Handle resource status changes (paused, downscaled, restored)
    - Manage user limits and permissions on backend resources
    - Process event-driven user role changes
    - Update resource metadata and sync timestamps in Waldur

    The processor supports both full synchronization and incremental updates
    based on specific events or scheduled operations.
    """

    BACKEND_TYPE_KEY = "membership_sync_backend"

    def __init__(
        self, offering: structures.Offering, user_agent: str = "", timezone: str = ""
    ) -> None:
        """Constructor.

        Overrides the default constructor and adds service provider details to the instance.
        """
        super().__init__(offering, user_agent, timezone)
        service_providers = marketplace_service_providers_list.sync(
            customer_uuid=self.waldur_offering.customer_uuid.hex,
            client=self.waldur_rest_client,
        )

        self.service_provider = service_providers[0]

    def _get_waldur_resources(self, project_uuid: Optional[str] = None) -> list[WaldurResource]:
        """Fetch Waldur resources for this offering, optionally filtered by project.

        Args:
            project_uuid: If provided, only return resources from this project

        Returns:
            List of resources that have backend IDs and are in OK or ERRED state
        """
        filters = {
            "offering_uuid": self.offering.uuid,
            "state": [
                MarketplaceProviderResourcesListStateItem.OK,
                MarketplaceProviderResourcesListStateItem.ERRED,
            ],
        }

        if project_uuid is not None:
            filters["project_uuid"] = project_uuid
        waldur_resources: list[WaldurResource] | None = marketplace_provider_resources_list.sync(
            client=self.waldur_rest_client, **filters
        )
        if not waldur_resources:
            logger.info("No resources to process")
            return []

        return [
            waldur_resource for waldur_resource in waldur_resources if waldur_resource.backend_id
        ]

    def process_resource_by_uuid(self, resource_uuid: str) -> None:
        """Process a specific resource's status and membership data.

        This method processes a single resource identified by UUID,
        performing full synchronization of its status and user memberships.

        Args:
            resource_uuid: UUID of the resource to process

        Raises:
            ObjectNotFoundError: If the resource is not found in Waldur
        """
        logger.info("Processing resource state and membership data, uuid: %s", resource_uuid)
        logger.info("Fetching resource from Waldur")
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")
        logger.info(
            "Pulling resource %s (%s) from backend",
            waldur_resource.name,
            waldur_resource.backend_id,
        )
        resource_report = self.resource_backend.pull_resources([waldur_resource])
        self._process_resources(resource_report)

    def process_offering(self) -> None:
        """Process all resources in this offering for membership synchronization.

        Fetches all resources for the offering and processes each one to
        synchronize user memberships, resource status, and metadata between
        Waldur and the backend system.
        """
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )

        waldur_resources_info = self._get_waldur_resources()
        resource_report = self.resource_backend.pull_resources(waldur_resources_info)

        self._process_resources(resource_report)

    def _get_user_offering_users(self, user_uuid: str) -> list[OfferingUser]:
        """Fetch offering users for a specific user UUID.

        Args:
            user_uuid: UUID of the user to look up

        Returns:
            List of offering users associated with the specified user

        Raises:
            ObjectNotFoundError: If no offering users found for the user
        """
        offering_users: list[OfferingUser] = marketplace_offering_users_list.sync(
            client=self.waldur_rest_client, user_uuid=user_uuid, is_restricted=False
        )
        return offering_users

    def process_user_role_changed(self, user_uuid: str, project_uuid: str, granted: bool) -> None:
        """Process a user role change event.

        Handles adding or removing a user from backend resources when their
        project role changes. Respects resource access restrictions.

        Args:
            user_uuid: UUID of the user whose role changed
            project_uuid: UUID of the project where the role changed
            granted: True if access was granted, False if revoked
        """
        offering_users: list[OfferingUser] = self._get_user_offering_users(user_uuid)
        if len(offering_users) == 0:
            logger.info(
                "User %s is not linked to the offering %s (%s)",
                user_uuid,
                self.offering.name,
                self.offering.uuid,
            )
            return

        self._update_offering_users(offering_users)

        # Refresh offering users after username generation
        offering_users = self._get_user_offering_users(user_uuid)

        username = offering_users[0].username
        logger.info("Using offering user with username %s", username)
        if not username:
            logger.warning("Username is blank, skipping processing")
            return

        resources: list[WaldurResource] = self._get_waldur_resources(project_uuid=project_uuid)
        resource_report = self.resource_backend.pull_resources(resources)

        for waldur_resource, _ in resource_report.values():
            try:
                if granted:
                    if waldur_resource.restrict_member_access:
                        logger.info("The resource is restricted, skipping new role.")
                        continue
                    self.resource_backend.add_user(waldur_resource.backend_id, username)
                else:
                    self.resource_backend.remove_user(waldur_resource.backend_id, username)
            except Exception as exc:
                logger.error(
                    "Unable to add user %s to the resource %s, error: %s",
                    username,
                    waldur_resource.backend_id,
                    exc,
                )

    def process_project_user_sync(self, project_uuid: str) -> None:
        """Perform full user synchronization for all resources in a project.

        This method synchronizes all users across all resources within
        a specific project, ensuring consistency between Waldur project
        teams and backend resource access.

        Args:
            project_uuid: UUID of the project to synchronize
        """
        logger.info("Processing sync of all users for project %s", project_uuid)
        resources = self._get_waldur_resources(project_uuid=project_uuid)
        resource_report = self.resource_backend.pull_resources(resources)
        for waldur_resource, backend_resource_info in resource_report.values():
            try:
                logger.info(
                    "Syncing users for resource %s (%s)",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                )
                self._sync_resource_users(waldur_resource, backend_resource_info)
            except Exception as exc:
                logger.error(
                    "Unable to sync resource %s (%s), error: %s",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                    exc,
                )

    def _get_waldur_offering_users(self) -> list[OfferingUser]:
        """Fetch all offering users for this offering.

        Returns:
            List of all non-restricted offering users for this offering

        Raises:
            ObjectNotFoundError: If no offering users found
        """
        logger.info("Fetching Waldur offering users")
        offering_users: list[OfferingUser] = marketplace_offering_users_list.sync(
            client=self.waldur_rest_client,
            offering_uuid=self.offering.uuid,
            is_restricted=False,
        )
        return offering_users

    def _get_waldur_resource_team(self, resource: WaldurResource) -> list[ProjectUser] | None:
        logger.info("Fetching Waldur resource team")
        team: list[ProjectUser] | None = marketplace_provider_resources_team_list.sync(
            client=self.waldur_rest_client, uuid=resource.uuid.hex
        )

        return team

    def _group_resource_usernames(
        self, waldur_resource: WaldurResource, backend_resource_info: BackendResourceInfo
    ) -> tuple[set[str], set[str], set[str]]:
        logger.info("Fetching new, existing and stale resource users")
        usernames: list[str] = backend_resource_info.users
        local_usernames = set(usernames)
        logger.info("The usernames from the backend: %s", ", ".join(local_usernames))

        # Offering users sync
        # The service fetches offering users from Waldur and pushes them to the cluster
        # If an offering user is not in the team anymore, it will be removed from the backend
        team: list[ProjectUser] | None = self._get_waldur_resource_team(waldur_resource)
        if not team:
            logger.warning(
                "No team found for resource %s, treating as empty team", waldur_resource.uuid.hex
            )
            team_user_uuids = set()
        else:
            team_user_uuids = {user.uuid for user in team}

        offering_users: list[OfferingUser] = self._get_waldur_offering_users()
        self._update_offering_users(offering_users)

        # Refresh offering users after username generation
        offering_users = self._get_waldur_offering_users()

        resource_offering_usernames = {
            offering_user.username
            for offering_user in offering_users
            if offering_user.user_uuid in team_user_uuids
        }
        logger.info(
            "Resource offering usernames: %s",
            ", ".join(str(u) for u in resource_offering_usernames),
        )

        existing_usernames: set[str] = {
            offering_user.username
            for offering_user in offering_users
            if offering_user.username
            and offering_user.username in local_usernames
            and offering_user.user_uuid in team_user_uuids
        }
        logger.info("Resource existing usernames: %s", ", ".join(existing_usernames))

        new_usernames: set[str] = {
            offering_user.username
            for offering_user in offering_users
            if offering_user.username
            and offering_user.username not in local_usernames
            and offering_user.user_uuid in team_user_uuids
        }
        logger.info("Resource new usernames: %s", ", ".join(new_usernames))

        stale_usernames: set[str] = {
            offering_user.username
            for offering_user in offering_users
            if offering_user.username
            and offering_user.username in local_usernames
            and offering_user.user_uuid not in team_user_uuids
        }
        logger.info("Resource stale usernames: %s", ", ".join(stale_usernames))

        return existing_usernames, stale_usernames, new_usernames

    def _sync_resource_users(
        self, waldur_resource: WaldurResource, backend_resource_info: BackendResourceInfo
    ) -> set[str]:
        """Sync users for the resource between Waldur and the site.

        return: the actual resource usernames (existing + added)
        """
        logger.info("Syncing user list for resource %s", waldur_resource.name)
        existing_usernames, stale_usernames, new_usernames = self._group_resource_usernames(
            waldur_resource, backend_resource_info
        )

        if waldur_resource.restrict_member_access:
            # The idea is to remove the existing associations in both sides
            # and avoid creation of new associations
            logger.info(
                "Resource is restricted for members, removing all the existing associations"
            )

            self.resource_backend.remove_users_from_resource(
                waldur_resource.backend_id, existing_usernames
            )
            return set()

        added_usernames = self.resource_backend.add_users_to_resource(
            waldur_resource.backend_id,
            new_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

        self.resource_backend.remove_users_from_resource(
            waldur_resource.backend_id,
            stale_usernames,
        )

        return existing_usernames | added_usernames

    def _sync_resource_status(self, waldur_resource: WaldurResource) -> None:
        """Syncs resource status between Waldur and the backend."""
        logger.info(
            "Syncing resource status for resource %s (%s)",
            waldur_resource.name,
            waldur_resource.backend_id,
        )
        if waldur_resource.paused:
            logger.info("Resource pausing is requested, processing it")
            pausing_done = self.resource_backend.pause_resource(waldur_resource.backend_id)
            if pausing_done:
                logger.info("Pausing is successfully completed")
            else:
                logger.warning("Pausing is not done")
        elif waldur_resource.downscaled:
            logger.info("Resource downscaling is requested, processing it")
            downscaling_done = self.resource_backend.downscale_resource(waldur_resource.backend_id)
            if downscaling_done:
                logger.info("Downscaling is successfully completed")
            else:
                logger.warning("Downscaling is not done")
        else:
            logger.info(
                "The resource is not downscaled or paused, resetting the QoS to the default one"
            )
            restoring_done = self.resource_backend.restore_resource(waldur_resource.backend_id)
            if restoring_done:
                logger.info("Restoring is successfully completed")
            else:
                logger.info("Restoring is skipped")

        resource_metadata = self.resource_backend.get_resource_metadata(waldur_resource.backend_id)
        marketplace_provider_resources_set_backend_metadata.sync(
            uuid=waldur_resource.uuid.hex,
            client=self.waldur_rest_client,
            body=ResourceBackendMetadataRequest(backend_metadata=resource_metadata),
        )

    def _sync_resource_limits(self, waldur_resource: WaldurResource) -> None:
        """Syncs resource limits between Waldur and the backend."""
        utils.sync_waldur_resource_limits(
            self.resource_backend, self.waldur_rest_client, waldur_resource
        )

    # TODO: adapt for RabbitMQ-based processing
    # introduce new event and add support for the event in the agent
    def _sync_resource_user_limits(
        self, waldur_resource: WaldurResource, usernames: set[str]
    ) -> None:
        logger.info(
            "Synching resource user limits for resource %s (%s)",
            waldur_resource.name,
            waldur_resource.backend_id,
        )
        if waldur_resource.restrict_member_access:
            logger.info("Resource is restricted for members, skipping user limits setup")
            return

        backend_user_limits = self.resource_backend.get_resource_user_limits(
            waldur_resource.backend_id
        )

        for username in usernames:
            try:
                logger.info(
                    "Fetching user usage limits for %s, resource %s",
                    username,
                    waldur_resource.uuid.hex,
                )
                user_limits: list[ComponentUserUsageLimit] = component_user_usage_limits_list.sync(
                    client=self.waldur_rest_client,
                    resource_uuid=waldur_resource.uuid.hex,
                    username=username,
                )
                if len(user_limits) == 0:
                    existing_user_limits = backend_user_limits.get(username)
                    logger.info("The limits for user %s are not defined in Waldur", username)
                    if not existing_user_limits:
                        continue
                    logger.info("Unsetting the existing limits %s", existing_user_limits)
                    user_component_limits = {}
                else:
                    user_component_limits = {
                        user_limit.component_type: int(float(user_limit.limit))
                        for user_limit in user_limits
                    }
                self.resource_backend.set_resource_user_limits(
                    waldur_resource.backend_id, username, user_component_limits
                )
            except Exception as exc:
                logger.error(
                    "Unable to set user %s limits for resource %s (%s), reason: %s",
                    username,
                    waldur_resource.name,
                    waldur_resource.backend_id,
                    exc,
                )

    def _sync_resource_service_accounts(self, waldur_resource: WaldurResource) -> None:
        """Syncs project service accounts between Waldur and the backend resource."""
        logger.info(
            "Syncing service accounts for the resource %s (%s)",
            waldur_resource.name,
            waldur_resource.backend_id,
        )
        if self.service_provider is None:
            logger.warning("No service provider configured, skipping service accounts sync")
            return

        service_accounts = marketplace_service_providers_project_service_accounts_list.sync(
            service_provider_uuid=self.service_provider.uuid.hex,
            project_uuid=waldur_resource.project_uuid.hex,
            client=self.waldur_rest_client,
        )
        usernames = {account.username for account in service_accounts if account.username}
        self.resource_backend.add_users_to_resource(waldur_resource.backend_id, usernames)

    def _process_resources(
        self,
        resource_report: dict[str, tuple[WaldurResource, BackendResourceInfo]],
    ) -> None:
        """Sync status and membership data for the resource."""
        for waldur_resource, backend_resource_info in resource_report.values():
            try:
                resource_usernames = self._sync_resource_users(
                    waldur_resource, backend_resource_info
                )
                self._sync_resource_service_accounts(waldur_resource)
                self._sync_resource_status(waldur_resource)
                self._sync_resource_limits(waldur_resource)
                self._sync_resource_user_limits(waldur_resource, resource_usernames)

                logger.info(
                    "Refreshing resource %s (%s) last sync",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                )

                marketplace_provider_resources_refresh_last_sync.sync_detailed(
                    uuid=waldur_resource.uuid.hex,
                    client=self.waldur_rest_client,
                )
                if waldur_resource.state == ResourceState.ERRED:
                    logger.info(
                        "Setting resource %s (%s) state to OK",
                        waldur_resource.name,
                        waldur_resource.backend_id,
                    )
                    marketplace_provider_resources_set_as_ok.sync_detailed(
                        uuid=waldur_resource.uuid.hex,
                        client=self.waldur_rest_client,
                    )
            except Exception as e:
                logger.exception(
                    "Error while processing allocation %s: %s",
                    waldur_resource.backend_id,
                    e,
                )
                error_traceback = traceback.format_exc()
                utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    [waldur_resource],
                    error_details={
                        "error_message": str(e),
                        "error_traceback": error_traceback,
                    },
                )

    def process_service_account_creation(self, service_account_username: str) -> None:
        """Process service account creation."""
        service_accounts = marketplace_service_providers_project_service_accounts_list.sync(
            service_provider_uuid=self.service_provider.uuid.hex,
            username=service_account_username,
            client=self.waldur_rest_client,
        )
        if len(service_accounts) == 0:
            logger.info(
                "No service accounts found with username %s,"
                "skipping processing for offering %s (%s)",
                service_account_username,
                self.offering.uuid,
                self.offering.name,
            )
            return

        service_account = service_accounts[0]
        resources = self._get_waldur_resources(service_account.project_uuid.hex)
        for resource in resources:
            try:
                self.resource_backend.add_users_to_resource(
                    resource.backend_id, {service_account.username}
                )
            except BackendError as e:
                logger.error(
                    "Unable to add the service account %s to resource %s, reason: %s",
                    service_account.username,
                    resource.backend_id,
                    e,
                )


class OfferingReportProcessor(OfferingBaseProcessor):
    """Processor for collecting and reporting usage data from backends to Waldur.

    This processor handles the collection of resource usage data from backend
    systems and reports it to Waldur for billing and monitoring purposes.
    It processes both total resource usage and per-user usage breakdowns.

    Key responsibilities:
    - Collect usage data from backend systems for all resources
    - Validate usage data for anomalies (decreasing usage patterns)
    - Report total resource usage to Waldur marketplace
    - Report per-user usage breakdowns for detailed billing
    - Handle error cases and mark resources as erred when backend data is missing
    - Implement retry logic for transient failures

    The processor includes anomaly detection to prevent reporting usage data
    that appears to have decreased from previous reports, which typically
    indicates a data collection error.
    """

    BACKEND_TYPE_KEY = "reporting_backend"

    def process_offering(self) -> None:
        """Process all resources in this offering for usage reporting.

        Fetches all OK and ERRED resources for the offering and processes
        each one to collect and report usage data to Waldur. Includes
        error handling for individual resource failures.
        """
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )
        waldur_offering: ProviderOfferingDetails | None = (
            marketplace_provider_offerings_retrieve.sync(
                client=self.waldur_rest_client, uuid=self.offering.uuid
            )
        )
        waldur_resources: list[WaldurResource] | None = marketplace_provider_resources_list.sync(
            client=self.waldur_rest_client,
            offering_uuid=self.offering.uuid,
            state=[
                MarketplaceProviderResourcesListStateItem.OK,
                MarketplaceProviderResourcesListStateItem.ERRED,
            ],
        )
        if not waldur_resources:
            logger.info("No resources to process")
            return

        for waldur_resource in waldur_resources:
            try:
                self._process_resource_with_retries(waldur_resource, waldur_offering)
            except Exception as e:
                logger.exception(
                    "Error while processing allocation %s: %s",
                    waldur_resource.backend_id,
                    e,
                )
                error_traceback = traceback.format_exc()
                utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    [waldur_resource],
                    error_details={
                        "error_message": str(e),
                        "error_traceback": error_traceback,
                    },
                )

    def _process_resource_with_retries(
        self,
        waldur_resource: WaldurResource,
        waldur_offering: ProviderOfferingDetails,
        retry_count: int = 10,
        delay: int = 5,
    ) -> None:
        for attempt_number in range(retry_count):
            try:
                logger.info(
                    "Attempt %s of %s, processing resource usage %s (%s)",
                    attempt_number + 1,
                    retry_count,
                    waldur_resource.name,
                    waldur_resource.backend_id,
                )
                self._process_resource(waldur_resource, waldur_offering)
                break
            except Exception as e:
                logger.warning(
                    "Error while processing resource %s (%s): %s",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                    e,
                )
                logger.info(
                    "Retrying resource usage %s processing in %s seconds",
                    waldur_resource.backend_id,
                    delay,
                )
                if attempt_number == retry_count - 1:
                    # If last attempt failed, raise the exception
                    logger.warning(
                        "Failed to process resource usage %s after %s retries,"
                        "skipping to the next resource",
                        waldur_resource.backend_id,
                        retry_count,
                    )
                    raise
                # If not last attempt, wait and retry
                sleep(delay)

    def _check_usage_anomaly(
        self,
        component_type: str,
        current_usage: float,
        existing_usages: list[ComponentUsage] | None,
    ) -> bool:
        """Check if the current usage is lower than existing usage."""
        if not existing_usages:
            return False
        # Find all usage records for this component type
        component_usages = [usage for usage in existing_usages if usage.type_ == component_type]

        if not component_usages:
            return False

        if len(component_usages) > 1:
            logger.error(
                "Found multiple usage records for component %s in the same billing period: %d",
                component_type,
                len(component_usages),
            )
            return True

        component_usage = component_usages[0]
        existing_usage = float(component_usage.usage)

        if current_usage < existing_usage:
            logger.error(
                "Usage anomaly detected for component %s: "
                "Current usage %s is lower than existing usage %s",
                component_type,
                current_usage,
                existing_usage,
            )
            return True

        return False

    def _submit_total_usage_for_resource(
        self,
        waldur_resource: WaldurResource,
        total_usage: dict[str, float],
        waldur_components: list[OfferingComponent],
    ) -> None:
        """Reports total usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s: %s", waldur_resource.backend_id, total_usage)
        resource_uuid = waldur_resource.uuid.hex

        component_types: list[str] = [
            component.type_ for component in waldur_components if component.type_
        ]
        missing_components = set(total_usage) - set(component_types)
        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )

        current_time = backend_utils.get_current_time_in_timezone(self.timezone)
        month_start = backend_utils.month_start(current_time).date()
        existing_usages: list[ComponentUsage] | None = marketplace_component_usages_list.sync(
            client=self.waldur_rest_client, resource_uuid=resource_uuid, billing_period=month_start
        )
        for component, amount in total_usage.items():
            if component in component_types and self._check_usage_anomaly(
                component, amount, existing_usages
            ):
                logger.warning(
                    "Skipping usage update for resource %s due to anomaly detection",
                    waldur_resource.backend_id,
                )
                raise UsageAnomalyError(f"Usage anomaly detected for component {component}")

        usage_objects = [
            ComponentUsageItemRequest(type_=component, amount=str(amount))
            for component, amount in total_usage.items()
            if component in component_types
        ]
        request_body = ComponentUsageCreateRequest(usages=usage_objects, resource=resource_uuid)
        marketplace_component_usages_set_usage.sync_detailed(
            client=self.waldur_rest_client, body=request_body
        )

    def _submit_user_usage_for_resource(
        self,
        username: str,
        user_usage: dict[str, float],
        waldur_component_usages: list[ComponentUsage] | None,
    ) -> None:
        """Reports per-user usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s", username)
        if not waldur_component_usages:
            logger.warning(
                "No component usages found for resource %s",
                username,
            )
            return
        component_usage_types = [
            component_usage.type_
            for component_usage in waldur_component_usages
            if component_usage.type_
        ]
        missing_components = set(user_usage) - set(component_usage_types)

        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )
        # Assumed to be looking up offering users by the user's username
        offering_users: list[OfferingUser] = marketplace_offering_users_list.sync(
            client=self.waldur_rest_client, user_username=username, query=self.offering.uuid
        )
        offering_user = None if not offering_users else offering_users[0]

        for component_usage in waldur_component_usages:
            component_type = component_usage.type_
            usage = user_usage.get(component_type)
            if usage is None:
                logger.warning(
                    "No usage for Waldur component %s is found in SLURM user usage report",
                    component_type,
                )
                continue
            logger.info(
                "Submitting usage for username %s: %s -> %s",
                username,
                component_type,
                usage,
            )
            marketplace_component_usages_set_user_usage.sync_detailed(
                uuid=component_usage.uuid,
                client=self.waldur_rest_client,
                body=ComponentUserUsageCreateRequest(
                    username=username,
                    usage=usage,
                    user=offering_user.url if offering_user else None,
                ),
            )

    def _process_resource(
        self,
        waldur_resource: WaldurResource,
        waldur_offering: ProviderOfferingDetails,
    ) -> None:
        """Processes usage report for the resource."""
        current_time = backend_utils.get_current_time_in_timezone(self.timezone)
        month_start = backend_utils.month_start(current_time).date()
        resource_backend_id = waldur_resource.backend_id
        logger.info("Pulling resource %s (%s)", waldur_resource.name, resource_backend_id)
        backend_resource_info = self.resource_backend.pull_resource(waldur_resource)
        if backend_resource_info is None:
            logger.info("The resource %s is missing in backend", resource_backend_id)
            if waldur_resource.state != ResourceState.ERRED:
                logger.info("Marking resource %s as erred in Waldur", resource_backend_id)
                utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    [waldur_resource],
                    {
                        "error_message": f"The resource {resource_backend_id} "
                        "is missing on the backend"
                    },
                )
            return

        # Invalidate cache of Waldur resource
        logger.info(
            "Fetching Waldur resource data for %s (%s)", waldur_resource.name, resource_backend_id
        )
        waldur_resource_info: WaldurResource = marketplace_provider_resources_retrieve.sync(
            client=self.waldur_rest_client, uuid=waldur_resource.uuid.hex
        )

        if waldur_resource_info.state not in [
            MarketplaceProviderResourcesListStateItem.OK,
            MarketplaceProviderResourcesListStateItem.ERRED,
        ]:
            logger.error(
                "Waldur resource %s (%s) has incorrect state %s, skipping processing",
                waldur_resource_info.name,
                waldur_resource_info.backend_id,
                waldur_resource_info.state,
            )
            return
        # Set resource state OK if it is erred
        if waldur_resource_info.state == ResourceState.ERRED:
            marketplace_provider_resources_set_as_ok.sync_detailed(
                uuid=waldur_resource_info.uuid,
                client=self.waldur_rest_client,
            )

        usages: dict[str, dict[str, float]] = backend_resource_info.usage

        # Submit usage
        total_usage = usages.pop("TOTAL_ACCOUNT_USAGE")
        try:
            self._submit_total_usage_for_resource(
                waldur_resource_info,
                total_usage,
                waldur_offering.components,
            )
        except UsageAnomalyError:
            logger.info("Skipping per-user usage processing due to anomaly in usage reporting")
            return

        # Skip the following actions if the dict is empty
        if not usages:
            return

        waldur_component_usages: list[ComponentUsage] | None = (
            marketplace_component_usages_list.sync(
                client=self.waldur_rest_client,
                resource_uuid=waldur_resource_info.uuid.hex,
                date_after=month_start,
            )
        )
        logger.info("Setting per-user usages")
        for username, user_usage in usages.items():
            self._submit_user_usage_for_resource(username, user_usage, waldur_component_usages)


class OfferingImportableResourcesProcessor(OfferingBaseProcessor):
    """Processes importable resources for the offering and reports them to Waldur."""

    BACKEND_TYPE_KEY = "order_processing_backend"

    def process_offering(self) -> None:
        """This function is blank because the processor operates over backend resource request."""

    def _process_importable_resource(self, local_resource_info: BackendResourceInfo) -> None:
        logger.info("Processing importable resource %s", local_resource_info.backend_id)
        project_prefix = self.offering.backend_settings.get("project_prefix", "")
        parent_id = local_resource_info.parent_id
        if not parent_id.startswith(project_prefix):
            logger.info(
                "The parent_id %s of the resource %s does not have a required prefix, skipping it",
                parent_id,
                local_resource_info.backend_id,
            )
            return

        project_slug = parent_id.removeprefix(project_prefix)
        waldur_projects = projects_list.sync(
            slug=project_slug,
            client=self.waldur_rest_client,
        )
        if not waldur_projects:
            logger.info(
                "No Waldur project found for slug %s, skipping resource %s import",
                project_slug,
                local_resource_info.backend_id,
            )
            return
        waldur_project = waldur_projects[0]
        existing_backend_resources = backend_resources_list.sync(
            backend_id=local_resource_info.backend_id,
            project_uuid=waldur_project.uuid,
            offering_uuid=self.offering.uuid,
            client=self.waldur_rest_client,
        )

        if existing_backend_resources:
            logger.info(
                "Backend resource with id %s already exists in Waldur project %s, "
                "skipping submission",
                local_resource_info.backend_id,
                waldur_project.uuid.hex,
            )
            return

        logger.info(
            "Submitting backend resource %s to Waldur project %s",
            local_resource_info.backend_id,
            waldur_project.uuid.hex,
        )
        limits = self.resource_backend.get_resource_limits(local_resource_info.backend_id)
        backend_metadata = {
            "limits": limits,
        }
        payload = BackendResourceRequest(
            name=local_resource_info.backend_id,
            project=waldur_project.uuid,
            offering=self.offering.uuid,
            backend_id=local_resource_info.backend_id,
            backend_metadata=backend_metadata,
        )
        backend_resources_create.sync(
            body=payload,
            client=self.waldur_rest_client,
        )

    def _pre_process_backend_resource_request(self, request_uuid: str) -> None:
        backend_resource_request = backend_resource_requests_retrieve.sync(
            uuid=request_uuid,
            client=self.waldur_rest_client,
        )
        backend_resource_requests_start_processing.sync(
            uuid=backend_resource_request.uuid.hex,
            client=self.waldur_rest_client,
        )

    def _get_waldur_resources(self) -> dict[str, WaldurResource]:
        waldur_resource_list = marketplace_provider_resources_list.sync(
            offering_uuid=self.offering.uuid,
            state=[
                MarketplaceProviderResourcesListStateItem.OK,
                MarketplaceProviderResourcesListStateItem.ERRED,
                MarketplaceProviderResourcesListStateItem.CREATING,
            ],
            client=self.waldur_rest_client,
        )
        return {
            resource.backend_id: resource
            for resource in waldur_resource_list
            if resource.backend_id
        }

    def process_request(self, request_uuid: str) -> None:
        """Process backend resource request.

        List all resource in the backend, compare their backend_ids with ones from Waldur
        and report only ones absent in Waldur.
        """
        try:
            logger.info(
                "Processing backend resource request %s for offering %s",
                request_uuid,
                self.offering.name,
            )

            self._pre_process_backend_resource_request(request_uuid)
            waldur_resources: dict[str, WaldurResource] = self._get_waldur_resources()

            local_resource_list = self.resource_backend.list_resources()
            local_resources = {resource.backend_id: resource for resource in local_resource_list}

            importable_resource_backend_ids = local_resources.keys() - waldur_resources.keys()
            logger.info(
                "Found %d importable resources in the backend",
                len(importable_resource_backend_ids),
            )

            for backend_id in importable_resource_backend_ids:
                try:
                    local_resource = local_resources[backend_id]
                    self._process_importable_resource(local_resource)
                except Exception as e:
                    logger.error(
                        "Unable to process importable resource %s reason: %s", backend_id, e
                    )

            logger.info("Setting backend resource request %s as done", request_uuid)
            backend_resource_requests_set_done.sync(
                uuid=request_uuid,
                client=self.waldur_rest_client,
            )
        except Exception as e:
            logger.info("Unable to process importable resources reason: %s", e)
            payload = BackendResourceRequestSetErredRequest(
                error_message=str(e),
                error_traceback=traceback.format_exc(),
            )
            backend_resource_requests_set_erred.sync(
                uuid=request_uuid,
                client=self.waldur_rest_client,
                body=payload,
            )
