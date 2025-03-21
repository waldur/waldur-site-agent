"""Module for abstract offering processor."""

from __future__ import annotations

import abc
import datetime
import traceback
from time import sleep
from typing import Dict, List, Optional, Set, Tuple

from waldur_client import (
    ComponentUsage,
    WaldurClient,
    is_uuid,
)

from waldur_site_agent.backends import BackendType, logger
from waldur_site_agent.backends import utils as backend_utils
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.common import structures, utils


class OfferingBaseProcessor(abc.ABC):
    """Abstract class for an offering processing."""

    def __init__(self, offering: structures.Offering, user_agent: str = "") -> None:
        """Constructor."""
        self.offering: structures.Offering = offering
        self.waldur_rest_client: WaldurClient = WaldurClient(
            offering.api_url, offering.api_token, user_agent
        )
        self.resource_backend = utils.get_backend_for_offering(offering)
        if self.resource_backend.backend_type == BackendType.UNKNOWN.value:
            raise BackendError(f"Unable to create backend for {self.offering}")

        self._print_current_user()

        waldur_offering = self.waldur_rest_client.get_marketplace_provider_offering(
            self.offering.uuid
        )
        utils.extend_backend_components(self.offering, waldur_offering["components"])

    def _print_current_user(self) -> None:
        current_user = self.waldur_rest_client.get_current_user()
        utils.print_current_user(current_user)

    def _collect_waldur_resource_info(self, resource_data: dict) -> Resource:
        return Resource(
            name=resource_data["name"],
            backend_id=resource_data["backend_id"],
            marketplace_uuid=resource_data["uuid"],
            backend_type=self.offering.backend_type,
            restrict_member_access=resource_data.get("restrict_member_access", False),
            downscaled=resource_data.get("downscaled", False),
            paused=resource_data.get("paused", False),
            state=resource_data["state"],
        )

    @abc.abstractmethod
    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""


class OfferingOrderProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related orders and creates necessary associations.
    """

    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )

        orders = self.waldur_rest_client.list_orders(
            {
                "offering_uuid": self.offering.uuid,
                "state": ["pending-provider", "executing"],
            }
        )

        if len(orders) == 0:
            logger.info("There are no pending or executing orders")
            return

        for order in orders:
            try:
                self.process_order_with_retries(order)
            except Exception as e:
                logger.exception(
                    "Error while processing offering order %s (%s), type %s, state %s: %s",
                    order["uuid"],
                    order["attributes"].get("name", "N/A"),
                    order["type"],
                    order["state"],
                    e,
                )

    def get_order_info(self, order_uuid: str) -> Optional[dict]:
        """Get order info from Waldur."""
        try:
            return self.waldur_rest_client.get_order(order_uuid)
        except Exception as e:
            logger.error("Failed to get order %s info: %s", order_uuid, e)
            return None

    def process_order_with_retries(
        self, order_info: dict, retry_count: int = 10, delay: int = 5
    ) -> None:
        """Process order with retries."""
        for attempt_number in range(retry_count):
            try:
                logger.info("Attempt %s of %s", attempt_number + 1, retry_count)
                order = self.get_order_info(order_info["uuid"])
                if order is None:
                    logger.error("Failed to get order %s info", order_info["uuid"])
                    return
                self.process_order(order)
                break
            except Exception as e:
                logger.exception(
                    "Error while processing order %s (%s), type %s, state %s: %s",
                    order_info["uuid"],
                    order_info["attributes"].get("name", "N/A"),
                    order_info["type"],
                    order["state"] if order is not None else order_info["state"],
                    e,
                )
                logger.info("Retrying order %s processing in %s seconds", order_info["uuid"], delay)
                sleep(delay)

        if attempt_number == retry_count - 1:
            logger.error(
                "Failed to process order %s after %s retries, skipping to the next one",
                order_info["uuid"],
                retry_count,
            )

    def process_order(self, order: dict) -> None:
        """Process a single order."""
        try:
            logger.info(
                "Processing order %s (%s) type %s, state %s",
                order["attributes"].get("name", "N/A"),
                order["uuid"],
                order["type"],
                order["state"],
            )

            if order["state"] == "executing":
                logger.info("Order is executing already, no need for approval")
            elif order["state"] == "pending-provider":
                logger.info("Approving the order")
                self.waldur_rest_client.marketplace_order_approve_by_provider(order["uuid"])
                logger.info("Refreshing the order")
                order = self.waldur_rest_client.get_order(order["uuid"])
            else:
                logger.warning(
                    "The order %s %s (%s) is in unexpected state %s, skipping processing",
                    order["type"],
                    order["resource_name"],
                    order["uuid"],
                    order["state"],
                )
                return

            order_is_done = False

            if order["type"] == "Create":
                order_is_done = self._process_create_order(order)

            if order["type"] == "Update":
                order_is_done = self._process_update_order(order)

            if order["type"] == "Terminate":
                order_is_done = self._process_terminate_order(order)

            # TODO: no need for update of orders for marketplace SLURM offerings
            if order_is_done:
                logger.info("Marking order as done")
                self.waldur_rest_client.marketplace_order_set_state_done(order["uuid"])

                logger.info("The order has been successfully processed")
            else:
                logger.warning("The order processing was not finished, skipping to the next one")

        except Exception as e:
            logger.exception(
                "Error while processing order %s: %s",
                order["uuid"],
                e,
            )
            self.waldur_rest_client.marketplace_order_set_state_erred(
                order["uuid"],
                error_message=str(e),
                error_traceback=traceback.format_exc(),
            )

    def _create_resource(
        self,
        waldur_resource: Dict,
    ) -> Resource | None:
        resource_uuid = waldur_resource["uuid"]
        resource_name = waldur_resource["name"]

        logger.info("Creating resource %s", resource_name)

        if not is_uuid(resource_uuid):
            logger.error("Unexpected resource UUID format, skipping the order")
            return None

        backend_resource = self.resource_backend.create_resource(waldur_resource)
        if backend_resource.backend_id == "":
            msg = f"Unable to create a backend resource for offering {self.offering}"
            raise BackendError(msg)

        logger.info("Updating resource metadata in Waldur")
        self.waldur_rest_client.marketplace_provider_resource_set_backend_id(
            resource_uuid, backend_resource.backend_id
        )

        return backend_resource

    def _add_users_to_resource(
        self,
        backend_resource: Resource,
    ) -> None:
        logger.info("Adding users to resource")
        logger.info("Fetching Waldur resource team")
        team = self.waldur_rest_client.marketplace_provider_resource_get_team(
            backend_resource.marketplace_uuid
        )
        user_uuids = {user["uuid"] for user in team}

        logger.info("Fetching Waldur offering users")
        offering_users_all = self.waldur_rest_client.list_remote_offering_users(
            {"offering_uuid": self.offering.uuid, "is_restricted": False}
        )
        offering_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users_all
            if offering_user["user_uuid"] in user_uuids and offering_user["username"] != ""
        }

        logger.info("Adding usernames to resource in backend")
        self.resource_backend.add_users_to_resource(
            backend_resource.backend_id,
            offering_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

    def _process_create_order(self, order: Dict) -> bool:
        # Wait until Waldur resource is created
        attempts = 0
        max_attempts = 4
        while "marketplace_resource_uuid" not in order:
            if attempts > max_attempts:
                logger.error("Order processing timed out")
                return False

            if order["state"] != "executing":
                logger.error("Order has unexpected state %s", order["state"])
                return False

            logger.info("Waiting for resource creation...")
            sleep(5)

            order = self.waldur_rest_client.get_order(order["uuid"])
            attempts += 1

        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(
            order["marketplace_resource_uuid"]
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
                    waldur_resource["name"],
                    waldur_resource["backend_id"],
                )
                create_resource = False

        if create_resource:
            waldur_resource["project_slug"] = order["project_slug"]
            waldur_resource["customer_slug"] = order["customer_slug"]

            backend_resource = self._create_resource(waldur_resource)
            if backend_resource is None:
                msg = "Unable to create a resource"
                raise BackendError(msg)

        if backend_resource is None:
            return False

        self._add_users_to_resource(
            backend_resource,
        )

        return True

    def _process_update_order(self, order: dict) -> bool:
        logger.info("Updating limits for %s", order["resource_name"])
        resource_uuid = order["marketplace_resource_uuid"]
        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(resource_uuid)

        resource_backend = utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        waldur_resource_backend_id = waldur_resource["backend_id"]

        new_limits = order["limits"]
        if not new_limits:
            logger.error(
                "Order %s (resource %s) with type" + "Update does not include new limits",
                order["uuid"],
                waldur_resource["name"],
            )

        if new_limits:
            resource_backend.set_resource_limits(waldur_resource_backend_id, new_limits)

        logger.info(
            "The limits for %s were updated successfully from %s to %s",
            waldur_resource["name"],
            order["attributes"]["old_limits"],
            new_limits,
        )
        return True

    def _process_terminate_order(self, order: dict) -> bool:
        logger.info("Terminating resource %s", order["resource_name"])
        resource_uuid = order["marketplace_resource_uuid"]

        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(resource_uuid)
        project_slug = order["project_slug"]

        resource_backend = utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        resource_backend.delete_resource(waldur_resource["backend_id"], project_slug=project_slug)

        logger.info("Allocation has been terminated successfully")
        return True


class OfferingMembershipProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related resources and reports membership data to Waldur.
    """

    def _get_waldur_resources(self, project_uuid: Optional[str] = None) -> List[Resource]:
        filters = {
            "offering_uuid": self.offering.uuid,
            "state": ["OK", utils.RESOURCE_ERRED_STATE],
            "field": [
                "backend_id",
                "uuid",
                "name",
                "resource_uuid",
                "offering_type",
                "restrict_member_access",
                "downscaled",
                "paused",
                "state",
                "limits",
            ],
        }

        if project_uuid is not None:
            filters["project_uuid"] = project_uuid

        waldur_resources = self.waldur_rest_client.filter_marketplace_provider_resources(filters)

        if len(waldur_resources) == 0:
            logger.info("No resources to process")
            return []

        return [
            self._collect_waldur_resource_info(resource_data)
            for resource_data in waldur_resources
            if resource_data["backend_id"]
        ]

    def process_resource_by_uuid(self, resource_uuid: str) -> None:
        """Processes resource status and membership data using resource UUID."""
        logger.info("Processing resource state and membership data, uuid: %s", resource_uuid)
        logger.info("Fetching resource from Waldur")
        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(resource_uuid)
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

    def _get_user_offering_users(self, user_uuid: str) -> List[dict]:
        return self.waldur_rest_client.list_remote_offering_users(
            {
                "offering_uuid": self.offering.uuid,
                "user_uuid": user_uuid,
                "is_restricted": False,
            }
        )

    def process_user_role_changed(self, user_uuid: str, project_uuid: str, granted: bool) -> None:
        """Process event of user role changing."""
        offering_users = self._get_user_offering_users(user_uuid)
        if len(offering_users) == 0:
            logger.info(
                "User %s is not linked to the offering %s (%s)",
                user_uuid,
                self.offering.name,
                self.offering.uuid,
            )
            return

        username = offering_users[0]["username"]
        logger.info("Using offering user with username %s", username)
        if not username:
            logger.warning("Username is blank, skipping processing")
            return

        resources = self._get_waldur_resources(project_uuid=project_uuid)
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

    def _get_waldur_offering_users(self) -> List[Dict]:
        logger.info("Fetching Waldur offering users")
        return self.waldur_rest_client.list_remote_offering_users(
            {
                "offering_uuid": self.offering.uuid,
                "is_restricted": False,
            }
        )

    def _get_waldur_resource_team(self, resource: Resource) -> List[Dict]:
        logger.info("Fetching Waldur resource team")
        return self.waldur_rest_client.marketplace_provider_resource_get_team(
            resource.marketplace_uuid
        )

    def _get_resource_usernames(self, resource: Resource) -> Tuple[Set[str], Set[str], Set[str]]:
        logger.info("Fetching new, existing and stale resource users")
        usernames = resource.users
        local_usernames = set(usernames)
        logger.info("The usernames from the backend: %s", ", ".join(local_usernames))

        # Offering users sync
        # The service fetches offering users from Waldur and pushes them to the cluster
        # If an offering user is not in the team anymore, it will be removed from the backend
        team = self._get_waldur_resource_team(resource)
        team_user_uuids = {user["uuid"] for user in team}

        offering_users = self._get_waldur_offering_users()
        resource_offering_usernames = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["user_uuid"] in team_user_uuids
        }
        logger.info("Resource offering usernames: %s", ", ".join(resource_offering_usernames))

        existing_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["username"] in local_usernames
            and offering_user["user_uuid"] in team_user_uuids
        }
        logger.info("Resource existing usernames: %s", ", ".join(existing_usernames))

        new_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["username"] not in local_usernames
            and offering_user["user_uuid"] in team_user_uuids
        }
        logger.info("Resource new usernames: %s", ", ".join(new_usernames))

        stale_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["username"] in local_usernames
            and offering_user["user_uuid"] not in team_user_uuids
        }
        logger.info("Resource stale usernames: %s", ", ".join(stale_usernames))

        return existing_usernames, stale_usernames, new_usernames

    def _sync_resource_users(
        self,
        resource: Resource,
    ) -> Set[str]:
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

            self.resource_backend.remove_users_from_account(resource.backend_id, existing_usernames)
            return set()

        added_usernames = self.resource_backend.add_users_to_resource(
            resource.backend_id,
            new_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

        self.resource_backend.remove_users_from_account(
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
        self.waldur_rest_client.marketplace_provider_resource_set_backend_metadata(
            resource.marketplace_uuid, resource_metadata
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
        self.waldur_rest_client.marketplace_provider_resource_set_limits(
            resource.marketplace_uuid, backend_limits
        )

    # TODO: adapt for RabbitMQ-based processing
    # introduce new event and add support for the event in the agent
    def _sync_resource_user_limits(
        self, resource: Resource, usernames: Optional[Set[str]] = None
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
                user_limits = self.waldur_rest_client.list_component_user_usage_limits(
                    {"resource_uuid": resource.marketplace_uuid, "username": username}
                )
                if len(user_limits) == 0:
                    existing_user_limits = backend_user_limits.get(username)
                    logger.info("The limits for user %s are not defined in Waldur")
                    if existing_user_limits is None:
                        continue
                    logger.info("Unsetting the existing limits %s", existing_user_limits)
                    user_component_limits = {}
                else:
                    user_component_limits = {
                        user_limit["component_type"]: int(float(user_limit["limit"]))
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
        resource_report: Dict[str, Resource],
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
                self.waldur_rest_client.marketplace_provider_resource_refresh_last_sync(
                    backend_resource.marketplace_uuid
                )
                if backend_resource.state == utils.RESOURCE_ERRED_STATE:
                    logger.info(
                        "Setting resource %s (%s) state to OK",
                        backend_resource.name,
                        backend_resource.backend_id,
                    )
                    self.waldur_rest_client.marketplace_provider_resource_set_as_ok(
                        backend_resource.marketplace_uuid
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

        waldur_offering = self.waldur_rest_client.get_marketplace_provider_offering(
            self.offering.uuid
        )

        waldur_resources = self.waldur_rest_client.filter_marketplace_provider_resources(
            {
                "offering_uuid": self.offering.uuid,
                "state": ["OK", utils.RESOURCE_ERRED_STATE],
                "field": ["backend_id", "uuid", "name", "offering_type", "state"],
            }
        )

        if len(waldur_resources) == 0:
            logger.info("No resources to process")
            return

        waldur_resources_info = [
            Resource(
                name=resource_data["name"],
                backend_id=resource_data["backend_id"],
                marketplace_uuid=resource_data["uuid"],
                backend_type=self.offering.backend_type,
                state=resource_data["state"],
            )
            for resource_data in waldur_resources
            if resource_data["backend_id"]
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
        waldur_offering: Dict,
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

    def _submit_total_usage_for_resource(
        self,
        backend_resource: Resource,
        total_usage: Dict[str, float],
        waldur_components: List[Dict],
    ) -> None:
        """Reports total usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s: %s", backend_resource.backend_id, total_usage)
        resource_uuid = backend_resource.marketplace_uuid

        component_types = [component["type"] for component in waldur_components]
        missing_components = set(total_usage) - set(component_types)

        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )

        usage_objects = [
            ComponentUsage(type=component, amount=amount)
            for component, amount in total_usage.items()
            if component in component_types
        ]
        self.waldur_rest_client.create_component_usages(
            resource_uuid=resource_uuid, usages=usage_objects
        )

    def _submit_user_usage_for_resource(
        self,
        username: str,
        user_usage: Dict[str, float],
        waldur_component_usages: List[Dict],
    ) -> None:
        """Reports per-user usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s", username)
        component_usage_types = [
            component_usage["type"] for component_usage in waldur_component_usages
        ]
        missing_components = set(user_usage) - set(component_usage_types)

        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )

        offering_users = self.waldur_rest_client.list_remote_offering_users(
            {"username": username, "query": self.offering.uuid}
        )
        offering_user_uuid = None

        if len(offering_users) > 0:
            offering_user_uuid = offering_users[0]["uuid"]

        for component_usage in waldur_component_usages:
            component_type = component_usage["type"]
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
            self.waldur_rest_client.create_component_user_usage(
                component_usage["uuid"], usage, username, offering_user_uuid
            )

    def _process_resource(
        self,
        waldur_resource: Resource,
        waldur_offering: Dict,
    ) -> None:
        """Processes usage report for the resource."""
        month_start = backend_utils.month_start(datetime.datetime.now()).date()
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
        waldur_resource_data = self.waldur_rest_client.get_marketplace_provider_resource(
            waldur_resource.marketplace_uuid
        )
        waldur_resource = self._collect_waldur_resource_info(waldur_resource_data)
        waldur_resource.usage = backend_resource.usage
        waldur_resource.users = backend_resource.users
        waldur_resource.limits = backend_resource.limits

        if waldur_resource.state not in ["OK", utils.RESOURCE_ERRED_STATE]:
            logger.error(
                "Waldur resource %s (%s) has incorrect state %s, skipping processing",
                waldur_resource.name,
                waldur_resource.backend_id,
                waldur_resource.state,
            )
            return
        # Set resource state OK if it is erred
        if waldur_resource.state == utils.RESOURCE_ERRED_STATE:
            self.waldur_rest_client.marketplace_provider_resource_set_as_ok(
                waldur_resource.marketplace_uuid
            )

        usages: Dict[str, Dict[str, float]] = waldur_resource.usage

        # Submit usage
        total_usage = usages.pop("TOTAL_ACCOUNT_USAGE")
        self._submit_total_usage_for_resource(
            waldur_resource,
            total_usage,
            waldur_offering["components"],
        )

        # Skip the following actions if the dict is empty
        if not usages:
            return

        waldur_component_usages = self.waldur_rest_client.list_component_usages(
            waldur_resource.marketplace_uuid, date_after=month_start
        )

        logger.info("Setting per-user usages")
        for username, user_usage in usages.items():
            self._submit_user_usage_for_resource(username, user_usage, waldur_component_usages)
