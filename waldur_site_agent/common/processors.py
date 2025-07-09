"""Module for abstract offering processor."""

from __future__ import annotations

import abc
import traceback
from time import sleep
from typing import Optional
from uuid import UUID

from waldur_api_client.api.component_user_usage_limits import component_user_usage_limits_list
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_list,
    marketplace_component_usages_set_usage,
    marketplace_component_usages_set_user_usage,
)
from waldur_api_client.api.marketplace_offering_users import marketplace_offering_users_list
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
    marketplace_provider_resources_set_limits,
    marketplace_provider_resources_team_list,
)
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import (
    ComponentUsageCreateRequest,
    ComponentUsageItemRequest,
)
from waldur_api_client.models.component_usage import ComponentUsage
from waldur_api_client.models.component_user_usage_create_request import (
    ComponentUserUsageCreateRequest,
)
from waldur_api_client.models.component_user_usage_limit import ComponentUserUsageLimit
from waldur_api_client.models.marketplace_orders_list_state_item import (
    MarketplaceOrdersListStateItem,
)
from waldur_api_client.models.marketplace_provider_resources_list_field_item import (
    MarketplaceProviderResourcesListFieldItem,
)
from waldur_api_client.models.marketplace_provider_resources_list_state_item import (
    MarketplaceProviderResourcesListStateItem,
)
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.offering_user import OfferingUser
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
from waldur_api_client.models.resource_set_limits_request import ResourceSetLimitsRequest
from waldur_api_client.types import Unset

from waldur_site_agent.backends import BackendType, logger
from waldur_site_agent.backends import utils as backend_utils
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.common import SERVICE_PROVIDER_USERNAME_GENERATION_POLICY, structures, utils


class UsageAnomalyError(Exception):
    """Raised when usage anomaly is detected (new usage is lower than existing usage)."""


class ObjectNotFoundError(Exception):
    """Object not found."""


class OfferingBaseProcessor(abc.ABC):
    """Abstract class for an offering processing."""

    def __init__(
        self, offering: structures.Offering, user_agent: str = "", timezone: str = ""
    ) -> None:
        """Constructor."""
        self.offering: structures.Offering = offering
        self.timezone: str = timezone
        self.waldur_rest_client: utils.AuthenticatedClient = utils.get_client(
            offering.api_url, offering.api_token, user_agent
        )
        self.resource_backend = utils.get_backend_for_offering(offering)
        if self.resource_backend.backend_type == BackendType.UNKNOWN.value:
            raise BackendError(f"Unable to create backend for {self.offering}")

        self.username_management_backend = utils.get_username_management_backend(offering)

        self._print_current_user()

        waldur_offering = marketplace_provider_offerings_retrieve.sync(
            client=self.waldur_rest_client, uuid=self.offering.uuid
        )
        utils.extend_backend_components(self.offering, waldur_offering.components)

    def _print_current_user(self) -> None:
        current_user = utils.get_current_user_from_client(self.waldur_rest_client)
        utils.print_current_user(current_user)

    def _collect_waldur_resource_info(self, resource_data: WaldurResource) -> Resource:
        return Resource(
            name=resource_data.name,
            backend_id=resource_data.backend_id,
            marketplace_uuid=str(resource_data.uuid.hex),
            backend_type=self.offering.backend_type,
            restrict_member_access=resource_data.restrict_member_access
            if resource_data.restrict_member_access
            else False,
            downscaled=resource_data.downscaled if resource_data.downscaled else False,
            paused=resource_data.paused if resource_data.paused else False,
            state=resource_data.state,
        )

    @abc.abstractmethod
    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""

    def _update_offering_users(self, offering_users: list[OfferingUser]) -> None:
        """Update offering users with blank usernames if possible."""
        if not offering_users:
            return

        # Check if usernames can be generated by a service provider
        offering_uuid = self.offering.uuid
        offering_details = marketplace_provider_offerings_retrieve.sync(
            client=self.waldur_rest_client, uuid=offering_uuid
        )
        if (
            offering_details.plugin_options.username_generation_policy
            != SERVICE_PROVIDER_USERNAME_GENERATION_POLICY
        ):
            return
        for offering_user in offering_users:
            if offering_user.username:
                continue
            username = self.username_management_backend.get_or_create_username(offering_user)
            if username:
                offering_user.username = username


class OfferingOrderProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related orders and creates necessary associations.
    """

    def log_order_processing_error(self, order: OrderDetails, e: Exception) -> None:
        """Log the error while processing an order."""
        name = (
            order.attributes.additional_properties.get("name", "N/A") if order.attributes else "N/A"
        )
        logger.exception(
            "Error while processing order %s (%s), type %s, state %s: %s",
            order.uuid,
            name,
            order.type_,
            order.state,
            e,
        )

    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""
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

    def get_order_info(self, order_uuid: UUID | str) -> Optional[OrderDetails]:
        """Get order info from Waldur."""
        try:
            return marketplace_orders_retrieve.sync(client=self.waldur_rest_client, uuid=order_uuid)
        except UnexpectedStatus as e:
            logger.error("Failed to get order %s info: %s", order_uuid, e)
            return None

    def process_order_with_retries(
        self, order_info: OrderDetails, retry_count: int = 10, delay: int = 5
    ) -> None:
        """Process order with retries."""
        for attempt_number in range(retry_count):
            try:
                logger.info("Attempt %s of %s", attempt_number + 1, retry_count)
                order: Optional[OrderDetails] = self.get_order_info(order_info.uuid)
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
        """Process a single order."""
        try:
            logger.info(
                "Processing order %s (%s) type %s, state %s",
                order.attributes.additional_properties.get("name", "N/A")
                if order.attributes
                else "N/A",
                order.uuid,
                order.type_,
                order.state,
            )

            if order.state == OrderState.EXECUTING:
                logger.info("Order is executing already, no need for approval")
            elif order.state == OrderState.PENDING_PROVIDER:
                logger.info("Approving the order")
                marketplace_orders_approve_by_provider.sync_detailed(
                    client=self.waldur_rest_client, uuid=order.uuid
                )
                logger.info("Refreshing the order")
                order = marketplace_orders_retrieve.sync(
                    client=self.waldur_rest_client, uuid=order.uuid
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
    ) -> Resource | None:
        resource_uuid = str(waldur_resource.uuid)
        resource_name = waldur_resource.name

        logger.info("Creating resource %s", resource_name)

        if not utils.is_uuid(resource_uuid):
            logger.error("Unexpected resource UUID format, skipping the order")
            return None

        # Use the provided user context for resource creation
        backend_resource = self.resource_backend.create_resource(waldur_resource, user_context)
        if backend_resource.backend_id == "":
            msg = f"Unable to create a backend resource for offering {self.offering}"
            raise BackendError(msg)

        logger.info("Updating resource metadata in Waldur")
        marketplace_provider_resources_set_backend_id.sync(
            client=self.waldur_rest_client,
            uuid=resource_uuid,
            body=ResourceBackendIDRequest(backend_id=backend_resource.backend_id),
        )

        return backend_resource

    def _fetch_user_context_for_resource(self, resource_uuid: str | UUID) -> dict:
        """Fetch user context for resource creation.

        Returns a dictionary containing:
        - team: List of project team members
        - offering_users: List of offering users
        - user_mappings: Mapping of user UUIDs to usernames
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
                    if offering_user.user_uuid in user_uuids and offering_user.username != ""
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
        backend_resource: Resource,
        user_context: dict,
    ) -> None:
        logger.info("Adding users to resource")

        offering_users = user_context["offering_users"]

        # Update offering user usernames (only for users with blank usernames)
        self._update_offering_users(offering_users)

        # Use only non-blank usernames
        offering_usernames: set[str] = {
            offering_user.username
            for offering_user in user_context["offering_users"]
            if offering_user.username != ""
        }

        if not offering_usernames:
            logger.info("No users to add to resource")
            return

        logger.info("Adding usernames to resource in backend")
        self.resource_backend.add_users_to_resource(
            backend_resource.backend_id,
            offering_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

    def _process_create_order(self, order: OrderDetails | None) -> bool:
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
            uuid=order.marketplace_resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(
                f"Waldur resource {order.marketplace_resource_uuid} not found"
            )
        waldur_resource_info = self._collect_waldur_resource_info(waldur_resource)
        create_resource = True
        if waldur_resource_info.backend_id != "":
            logger.info(
                "Waldur resource backend id is not empty %s, checking backend resource data",
                waldur_resource_info.backend_id,
            )
            backend_resource = self.resource_backend.pull_resource(waldur_resource_info)
            if backend_resource is not None:
                logger.info(
                    "Resource %s (%s) is already created, skipping creation",
                    waldur_resource.name,
                    waldur_resource.backend_id,
                )
                create_resource = False
        # Fetch user context once for both resource creation and user addition
        user_context = self._fetch_user_context_for_resource(order.marketplace_resource_uuid)

        if create_resource:
            waldur_resource.project_slug = order.project_slug
            waldur_resource.customer_slug = order.customer_slug

            backend_resource = self._create_resource(waldur_resource, user_context)
            if backend_resource is None:
                msg = "Unable to create a resource"
                raise BackendError(msg)

        if backend_resource is None:
            return False

        self._add_users_to_resource(backend_resource, user_context)

        return True

    def _process_update_order(self, order: OrderDetails) -> bool:
        logger.info("Updating limits for %s", order.resource_name)
        resource_uuid = order.marketplace_resource_uuid
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")

        resource_backend = utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        waldur_resource_backend_id = waldur_resource.backend_id
        new_limits: Unset | OrderDetailsLimits = order.limits
        if not new_limits:
            logger.error(
                "Order %s (resource %s) with type" + "Update does not include new limits",
                order.uuid,
                waldur_resource.name,
            )

        if new_limits:
            resource_backend.set_resource_limits(waldur_resource_backend_id, new_limits.to_dict())

        logger.info(
            "The limits for %s were updated successfully from %s to %s",
            waldur_resource.name,
            order.attributes.additional_properties.get("old_limits", "N/A")
            if order.attributes
            else "N/A",
            new_limits.to_dict() if new_limits else "N/A",
        )
        return True

    def _process_terminate_order(self, order: OrderDetails) -> bool:
        logger.info("Terminating resource %s", order.resource_name)
        resource_uuid = order.marketplace_resource_uuid
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")
        project_slug = order.project_slug

        resource_backend = utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        resource_backend.delete_resource(waldur_resource.backend_id, project_slug=project_slug)

        logger.info("Allocation has been terminated successfully")
        return True


class OfferingMembershipProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related resources and reports membership data to Waldur.
    """

    def _get_waldur_resources(self, project_uuid: Optional[str] = None) -> list[Resource]:
        filters = {
            "offering_uuid": self.offering.uuid,
            "state": [
                MarketplaceProviderResourcesListStateItem.OK,
                MarketplaceProviderResourcesListStateItem.ERRED,
            ],
            "field": [
                MarketplaceProviderResourcesListFieldItem.BACKEND_ID,
                MarketplaceProviderResourcesListFieldItem.UUID,
                MarketplaceProviderResourcesListFieldItem.NAME,
                MarketplaceProviderResourcesListFieldItem.RESOURCE_UUID,
                MarketplaceProviderResourcesListFieldItem.OFFERING_TYPE,
                MarketplaceProviderResourcesListFieldItem.RESTRICT_MEMBER_ACCESS,
                MarketplaceProviderResourcesListFieldItem.DOWNSCALED,
                MarketplaceProviderResourcesListFieldItem.PAUSED,
                MarketplaceProviderResourcesListFieldItem.STATE,
                MarketplaceProviderResourcesListFieldItem.LIMITS,
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
            self._collect_waldur_resource_info(resource_data)
            for resource_data in waldur_resources
            if resource_data.backend_id
        ]

    def process_resource_by_uuid(self, resource_uuid: str) -> None:
        """Processes resource status and membership data using resource UUID."""
        logger.info("Processing resource state and membership data, uuid: %s", resource_uuid)
        logger.info("Fetching resource from Waldur")
        waldur_resource: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=self.waldur_rest_client
        )
        if not waldur_resource:
            raise ObjectNotFoundError(f"Waldur resource {resource_uuid} not found")
        resource_info = self._collect_waldur_resource_info(waldur_resource)
        logger.info(
            "Pulling resource %s (%s) from backend", resource_info.name, resource_info.backend_id
        )
        resource_report = self.resource_backend.pull_resources([resource_info])
        self._process_resources(resource_report)

    def process_offering(self) -> None:
        """Processes offering and reports resources usage to Waldur."""
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )

        waldur_resources_info = self._get_waldur_resources()
        resource_report = self.resource_backend.pull_resources(waldur_resources_info)

        self._process_resources(resource_report)

    def _get_user_offering_users(self, user_uuid: str) -> list[OfferingUser]:
        offering_users: list[OfferingUser] | None = marketplace_offering_users_list.sync(
            client=self.waldur_rest_client, user_uuid=user_uuid, is_restricted=False
        )
        if not offering_users:
            raise ObjectNotFoundError(f"Offering users for user {user_uuid} not found")
        return offering_users

    def process_user_role_changed(self, user_uuid: str, project_uuid: str, granted: bool) -> None:
        """Process event of user role changing."""
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

        username = offering_users[0].username
        logger.info("Using offering user with username %s", username)
        if not username:
            logger.warning("Username is blank, skipping processing")
            return

        resources: list[Resource] = self._get_waldur_resources(project_uuid=project_uuid)
        resource_report = self.resource_backend.pull_resources(resources)

        for resource in resource_report.values():
            try:
                if granted:
                    if resource.restrict_member_access:
                        logger.info("The resource is restricted, skipping new role.")
                        continue
                    self.resource_backend.add_user(resource.backend_id, username)
                else:
                    self.resource_backend.remove_user(resource.backend_id, username)
            except Exception as exc:
                logger.error(
                    "Unable to add user %s to the resource %s, error: %s",
                    username,
                    resource.backend_id,
                    exc,
                )

    def process_project_user_sync(self, project_uuid: str) -> None:
        """Process full project user synchronization."""
        logger.info("Processing sync of all users for project %s", project_uuid)
        resources = self._get_waldur_resources(project_uuid=project_uuid)
        resource_report = self.resource_backend.pull_resources(resources)
        for resource in resource_report.values():
            try:
                logger.info(
                    "Syncing users for resource %s (%s)", resource.name, resource.backend_id
                )
                self._sync_resource_users(resource)
            except Exception as exc:
                logger.error(
                    "Unable to sync resource %s (%s), error: %s",
                    resource.name,
                    resource.backend_id,
                    exc,
                )

    def _get_waldur_offering_users(self) -> list[OfferingUser]:
        logger.info("Fetching Waldur offering users")
        offering_users: list[OfferingUser] | None = marketplace_offering_users_list.sync(
            client=self.waldur_rest_client, offering_uuid=self.offering.uuid, is_restricted=False
        )
        if not offering_users:
            raise ObjectNotFoundError(f"Offering users for offering {self.offering.uuid} not found")
        return offering_users

    def _get_waldur_resource_team(self, resource: Resource) -> list[ProjectUser] | None:
        logger.info("Fetching Waldur resource team")
        team: list[ProjectUser] | None = marketplace_provider_resources_team_list.sync(
            client=self.waldur_rest_client, uuid=resource.marketplace_uuid
        )

        return team

    def _get_resource_usernames(self, resource: Resource) -> tuple[set[str], set[str], set[str]]:
        logger.info("Fetching new, existing and stale resource users")
        usernames: list[str] = resource.users
        local_usernames = set(usernames)
        logger.info("The usernames from the backend: %s", ", ".join(local_usernames))

        # Offering users sync
        # The service fetches offering users from Waldur and pushes them to the cluster
        # If an offering user is not in the team anymore, it will be removed from the backend
        team: list[ProjectUser] | None = self._get_waldur_resource_team(resource)
        if not team:
            logger.warning(
                "No team found for resource %s, treating as empty team", resource.marketplace_uuid
            )
            team_user_uuids = set()
        else:
            team_user_uuids = {user.uuid for user in team}

        offering_users: list[OfferingUser] = self._get_waldur_offering_users()
        self._update_offering_users(offering_users)

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
        self,
        resource: Resource,
    ) -> set[str]:
        """Sync users for the resource between Waldur and the site.

        return: the actual resource usernames (existing + added)
        """
        logger.info("Syncing user list for resource %s", resource.name)
        existing_usernames, stale_usernames, new_usernames = self._get_resource_usernames(resource)

        if resource.restrict_member_access:
            # The idea is to remove the existing associations in both sides
            # and avoid creation of new associations
            logger.info(
                "Resource is restricted for members, removing all the existing associations"
            )

            self.resource_backend.remove_users_from_resource(
                resource.backend_id, existing_usernames
            )
            return set()

        added_usernames = self.resource_backend.add_users_to_resource(
            resource.backend_id,
            new_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

        self.resource_backend.remove_users_from_resource(
            resource.backend_id,
            stale_usernames,
        )

        return existing_usernames | added_usernames

    def _sync_resource_status(self, resource: Resource) -> None:
        """Syncs resource status between Waldur and the backend."""
        logger.info(
            "Syncing resource status for resource %s (%s)", resource.name, resource.backend_id
        )
        if resource.paused:
            logger.info("Resource pausing is requested, processing it")
            pausing_done = self.resource_backend.pause_resource(resource.backend_id)
            if pausing_done:
                logger.info("Pausing is successfully completed")
            else:
                logger.warning("Pausing is not done")
        elif resource.downscaled:
            logger.info("Resource downscaling is requested, processing it")
            downscaling_done = self.resource_backend.downscale_resource(resource.backend_id)
            if downscaling_done:
                logger.info("Downscaling is successfully completed")
            else:
                logger.warning("Downscaling is not done")
        else:
            logger.info(
                "The resource is not downscaled or paused, resetting the QoS to the default one"
            )
            restoring_done = self.resource_backend.restore_resource(resource.backend_id)
            if restoring_done:
                logger.info("Restoring is successfully completed")
            else:
                logger.info("Restoring is skipped")

        resource_metadata = self.resource_backend.get_resource_metadata(resource.backend_id)
        marketplace_provider_resources_set_backend_metadata.sync(
            uuid=resource.marketplace_uuid,
            client=self.waldur_rest_client,
            body=ResourceBackendMetadataRequest(backend_metadata=resource_metadata),
        )

    def _sync_resource_limits(self, resource: Resource) -> None:
        """Syncs resource limits between Waldur and the backend."""
        logger.info(
            "Syncing resource limits for resource %s (%s)", resource.name, resource.backend_id
        )
        waldur_limits = resource.limits
        backend_limits = self.resource_backend.get_resource_limits(resource.backend_id)

        if len(backend_limits) == 0:
            logger.warning("No limits are found in the backend")
            return

        if backend_limits == waldur_limits:
            logger.info("The limits are already in sync, skipping")
            return
        # For now, we report all the limits
        logger.info("Reporting the limits to Waldur: %s", backend_limits)

        marketplace_provider_resources_set_limits.sync(
            uuid=resource.marketplace_uuid,
            client=self.waldur_rest_client,
            body=ResourceSetLimitsRequest(limits=backend_limits),
        )

    # TODO: adapt for RabbitMQ-based processing
    # introduce new event and add support for the event in the agent
    def _sync_resource_user_limits(
        self, resource: Resource, usernames: Optional[set[str]] = None
    ) -> None:
        logger.info(
            "Synching resource user limits for resource %s (%s)", resource.name, resource.backend_id
        )
        if resource.restrict_member_access:
            logger.info("Resource is restricted for members, skipping user limits setup")
            return

        if usernames is None:
            existing_usernames, _, _ = self._get_resource_usernames(resource)
            usernames = existing_usernames

        backend_user_limits = self.resource_backend.get_resource_user_limits(resource.backend_id)

        for username in usernames:
            try:
                logger.info(
                    "Fetching user usage limits for %s, resource %s",
                    username,
                    resource.marketplace_uuid,
                )
                user_limits: list[ComponentUserUsageLimit] = component_user_usage_limits_list.sync(
                    client=self.waldur_rest_client,
                    resource_uuid=resource.marketplace_uuid,
                    username=username,
                )
                if len(user_limits) == 0:
                    existing_user_limits = backend_user_limits.get(username)
                    logger.info("The limits for user %s are not defined in Waldur", username)
                    if existing_user_limits is None:
                        continue
                    logger.info("Unsetting the existing limits %s", existing_user_limits)
                    user_component_limits = {}
                else:
                    user_component_limits = {
                        user_limit.component_type: int(float(user_limit.limit))
                        for user_limit in user_limits
                    }
                self.resource_backend.set_resource_user_limits(
                    resource.backend_id, username, user_component_limits
                )
            except Exception as exc:
                logger.error(
                    "Unable to set user %s limits for resource %s (%s), reason: %s",
                    username,
                    resource.name,
                    resource.backend_id,
                    exc,
                )

    def _process_resources(
        self,
        resource_report: dict[str, Resource],
    ) -> None:
        """Sync status and membership data for the resource."""
        for backend_resource in resource_report.values():
            try:
                resource_usernames = self._sync_resource_users(backend_resource)
                self._sync_resource_status(backend_resource)
                self._sync_resource_limits(backend_resource)
                self._sync_resource_user_limits(backend_resource, resource_usernames)

                logger.info(
                    "Refreshing resource %s (%s) last sync",
                    backend_resource.name,
                    backend_resource.backend_id,
                )

                marketplace_provider_resources_refresh_last_sync.sync_detailed(
                    uuid=backend_resource.marketplace_uuid,
                    client=self.waldur_rest_client,
                )
                if backend_resource.state == utils.RESOURCE_ERRED_STATE:
                    logger.info(
                        "Setting resource %s (%s) state to OK",
                        backend_resource.name,
                        backend_resource.backend_id,
                    )
                    marketplace_provider_resources_set_as_ok.sync_detailed(
                        uuid=backend_resource.marketplace_uuid,
                        client=self.waldur_rest_client,
                    )
            except Exception as e:
                logger.exception(
                    "Error while processing allocation %s: %s",
                    backend_resource.backend_id,
                    e,
                )
                error_traceback = traceback.format_exc()
                utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    [backend_resource],
                    error_details={
                        "error_message": str(e),
                        "error_traceback": error_traceback,
                    },
                )


class OfferingReportProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related resource and reports computing data to Waldur.
    """

    def process_offering(self) -> None:
        """Processes offering and reports resources usage to Waldur."""
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
            field=[
                MarketplaceProviderResourcesListFieldItem.BACKEND_ID,
                MarketplaceProviderResourcesListFieldItem.UUID,
                MarketplaceProviderResourcesListFieldItem.NAME,
                MarketplaceProviderResourcesListFieldItem.OFFERING_TYPE,
                MarketplaceProviderResourcesListFieldItem.STATE,
            ],
        )
        if not waldur_resources:
            logger.info("No resources to process")
            return

        waldur_resources_info = [
            Resource(
                name=resource_data.name,
                backend_id=resource_data.backend_id,
                marketplace_uuid=str(resource_data.uuid.hex),
                backend_type=self.offering.backend_type,
                state=resource_data.state,
            )
            for resource_data in waldur_resources
            if resource_data.backend_id
        ]

        for waldur_resource in waldur_resources_info:
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
        waldur_resource: Resource,
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
        backend_resource: Resource,
        total_usage: dict[str, float],
        waldur_components: list[OfferingComponent],
    ) -> None:
        """Reports total usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s: %s", backend_resource.backend_id, total_usage)
        resource_uuid = backend_resource.marketplace_uuid

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
                    backend_resource.backend_id,
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
        offering_users: list[OfferingUser] | None = marketplace_offering_users_list.sync(
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
        waldur_resource: Resource,
        waldur_offering: ProviderOfferingDetails,
    ) -> None:
        """Processes usage report for the resource."""
        current_time = backend_utils.get_current_time_in_timezone(self.timezone)
        month_start = backend_utils.month_start(current_time).date()
        resource_backend_id = waldur_resource.backend_id
        logger.info("Pulling resource %s (%s)", waldur_resource.name, resource_backend_id)
        backend_resource = self.resource_backend.pull_resource(waldur_resource)
        if backend_resource is None:
            logger.info("The resource %s is missing in backend", resource_backend_id)
            if waldur_resource.state != utils.RESOURCE_ERRED_STATE:
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
        waldur_resource_data: WaldurResource | None = marketplace_provider_resources_retrieve.sync(
            client=self.waldur_rest_client, uuid=waldur_resource.marketplace_uuid
        )

        waldur_resource = self._collect_waldur_resource_info(waldur_resource_data)
        waldur_resource.usage = backend_resource.usage
        waldur_resource.users = backend_resource.users
        waldur_resource.limits = backend_resource.limits

        if waldur_resource.state not in [
            MarketplaceProviderResourcesListStateItem.OK,
            MarketplaceProviderResourcesListStateItem.ERRED,
        ]:
            logger.error(
                "Waldur resource %s (%s) has incorrect state %s, skipping processing",
                waldur_resource.name,
                waldur_resource.backend_id,
                waldur_resource.state,
            )
            return
        # Set resource state OK if it is erred
        if waldur_resource.state == utils.RESOURCE_ERRED_STATE:
            marketplace_provider_resources_set_as_ok.sync_detailed(
                uuid=waldur_resource.marketplace_uuid,
                client=self.waldur_rest_client,
            )

        usages: dict[str, dict[str, float]] = waldur_resource.usage

        # Submit usage
        total_usage = usages.pop("TOTAL_ACCOUNT_USAGE")
        try:
            self._submit_total_usage_for_resource(
                waldur_resource,
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
                resource_uuid=waldur_resource.marketplace_uuid,
                date_after=month_start,
            )
        )
        logger.info("Setting per-user usages")
        for username, user_usage in usages.items():
            self._submit_user_usage_for_resource(username, user_usage, waldur_component_usages)
