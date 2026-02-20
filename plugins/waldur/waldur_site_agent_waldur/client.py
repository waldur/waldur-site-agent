"""Waldur API client for Waldur-to-Waldur federation.

Wraps waldur_api_client.AuthenticatedClient to communicate with the target
Waldur B instance. Implements BaseClient abstract methods by delegating to
the generated API functions.
"""

from __future__ import annotations

import datetime
import logging
import time
from typing import Optional
from uuid import UUID

from waldur_api_client.api.version import version_retrieve
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.component_usage import ComponentUsage
from waldur_api_client.models.component_user_usage import ComponentUserUsage
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import OrderCreateRequestLimits
from waldur_api_client.models.order_details import OrderDetails
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.project_request import ProjectRequest
from waldur_api_client.models.remote_eduteams_request_request import RemoteEduteamsRequestRequest
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource import Resource
from waldur_api_client.models.resource_terminate_request import ResourceTerminateRequest
from waldur_api_client.models.resource_update_limits_request import ResourceUpdateLimitsRequest
from waldur_api_client.models.resource_update_limits_request_limits import (
    ResourceUpdateLimitsRequestLimits,
)
from waldur_api_client.models.user_role_create_request import UserRoleCreateRequest
from waldur_api_client.models.user_role_delete_request import UserRoleDeleteRequest
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource

logger = logging.getLogger(__name__)

# Terminal order states
TERMINAL_ORDER_STATES = {
    OrderState.DONE,
    OrderState.ERRED,
    OrderState.CANCELED,
    OrderState.REJECTED,
}

# Default role for project members
DEFAULT_PROJECT_ROLE = "admin"


class WaldurClient(BaseClient):
    """Client for communicating with target Waldur B via waldur_api_client."""

    def __init__(
        self,
        api_url: str,
        api_token: str,
        offering_uuid: str,
    ) -> None:
        super().__init__()
        self.api_url = api_url.rstrip("/")
        self.offering_uuid = offering_uuid
        self._api_client = AuthenticatedClient(
            base_url=self.api_url,
            token=api_token,
        )

    # --- Marketplace Order Operations ---

    def _get_offering_plan_url(self) -> Optional[str]:
        """Get the URL of the first plan for the configured offering.

        The plan URL for order creation uses the format:
        /api/marketplace-public-offerings/{offering_uuid}/plans/{plan_uuid}/

        Returns:
            Plan URL or None if no plans found.
        """
        if hasattr(self, "_cached_plan_url"):
            return self._cached_plan_url

        try:
            offering_hex = self._normalize_uuid(self.offering_uuid)
            response = self._api_client.get_httpx_client().get(
                f"/api/marketplace-public-offerings/{offering_hex}/plans/",
            )
            if response.status_code == 200:
                plans = response.json()
                if plans:
                    self._cached_plan_url: Optional[str] = plans[0].get("url")
                    return self._cached_plan_url
        except Exception:
            logger.debug("Failed to fetch plans for offering %s", self.offering_uuid)

        self._cached_plan_url = None
        return None

    def create_marketplace_order(
        self,
        project_url: str,
        offering_url: str,
        limits: dict[str, int],
        attributes: Optional[dict] = None,
    ) -> OrderDetails:
        """Create a marketplace order on Waldur B.

        Args:
            project_url: URL of the project on Waldur B.
            offering_url: URL of the offering on Waldur B.
            limits: Component limits for the order.
            attributes: Optional order attributes.

        Returns:
            OrderDetails from the created order.
        """
        from waldur_api_client.api.marketplace_orders import (  # noqa: PLC0415
            marketplace_orders_create,
        )

        order_limits = OrderCreateRequestLimits()
        for key, value in limits.items():
            order_limits[key] = value

        order_attrs = GenericOrderAttributes()
        if attributes:
            for key, value in attributes.items():
                order_attrs[key] = value

        plan_url = self._get_offering_plan_url()

        body = OrderCreateRequest(
            offering=offering_url,
            project=project_url,
            plan=plan_url,
            limits=order_limits,
            attributes=order_attrs,
            type_=RequestTypes.CREATE,
        )

        return marketplace_orders_create.sync(
            client=self._api_client,
            body=body,
        )

    def create_update_order(
        self,
        resource_uuid: UUID,
        limits: dict[str, int],
    ) -> UUID:
        """Create an update limits order on Waldur B.

        Returns:
            Order UUID.
        """
        from waldur_api_client.api.marketplace_resources import (  # noqa: PLC0415
            marketplace_resources_update_limits,
        )

        request_limits = ResourceUpdateLimitsRequestLimits()
        for key, value in limits.items():
            request_limits[key] = value

        body = ResourceUpdateLimitsRequest(limits=request_limits)
        result = marketplace_resources_update_limits.sync(
            uuid=resource_uuid,
            client=self._api_client,
            body=body,
        )
        return result.order_uuid

    def create_terminate_order(self, resource_uuid: UUID) -> UUID:
        """Create a termination order on Waldur B.

        Returns:
            Order UUID.
        """
        from waldur_api_client.api.marketplace_resources import (  # noqa: PLC0415
            marketplace_resources_terminate,
        )

        body = ResourceTerminateRequest()
        result = marketplace_resources_terminate.sync(
            uuid=resource_uuid,
            client=self._api_client,
            body=body,
        )
        return result.order_uuid

    def get_order(self, order_uuid: UUID) -> OrderDetails:
        """Retrieve order details from Waldur B."""
        from waldur_api_client.api.marketplace_orders import (  # noqa: PLC0415
            marketplace_orders_retrieve,
        )

        return marketplace_orders_retrieve.sync(
            uuid=order_uuid,
            client=self._api_client,
        )

    def poll_order_completion(
        self,
        order_uuid: UUID,
        timeout: int = 300,
        interval: int = 5,
    ) -> OrderDetails:
        """Poll until order reaches a terminal state.

        Args:
            order_uuid: UUID of the order to poll.
            timeout: Maximum seconds to wait.
            interval: Seconds between polls.

        Returns:
            Final OrderDetails.

        Raises:
            BackendError: If order times out, errors, or is canceled.
        """
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            order = self.get_order(order_uuid)
            state = order.state if not isinstance(order.state, type(UNSET)) else None

            if state in TERMINAL_ORDER_STATES:
                if state == OrderState.DONE:
                    return order
                msg = (
                    f"Order {order_uuid} reached terminal state: {state}. "
                    f"Error: {getattr(order, 'error_message', '')}"
                )
                raise BackendError(msg)

            logger.debug("Order %s state: %s, waiting...", order_uuid, state)
            time.sleep(interval)

        msg = f"Order {order_uuid} timed out after {timeout}s"
        raise BackendError(msg)

    # --- Resource Operations ---

    def get_marketplace_resource(self, resource_uuid: UUID) -> Resource:
        """Retrieve a marketplace resource by UUID from Waldur B."""
        from waldur_api_client.api.marketplace_resources import (  # noqa: PLC0415
            marketplace_resources_retrieve,
        )

        return marketplace_resources_retrieve.sync(
            uuid=resource_uuid,
            client=self._api_client,
        )

    def list_marketplace_resources(
        self,
        offering_uuid: Optional[UUID] = None,
    ) -> list[Resource]:
        """List marketplace resources, optionally filtered by offering."""
        from waldur_api_client.api.marketplace_resources import (  # noqa: PLC0415
            marketplace_resources_list,
        )

        kwargs = {}
        if offering_uuid:
            kwargs["offering_uuid"] = [offering_uuid]

        return marketplace_resources_list.sync(
            client=self._api_client,
            **kwargs,
        )

    def get_resource_team(self, resource_uuid: UUID) -> list:
        """Get the team (users) associated with a resource on Waldur B."""
        from waldur_api_client.api.marketplace_resources import (  # noqa: PLC0415
            marketplace_resources_team_list,
        )

        return marketplace_resources_team_list.sync(
            uuid=resource_uuid,
            client=self._api_client,
        )

    # --- Project Operations ---

    def find_project_by_backend_id(self, backend_id: str) -> Optional[dict]:
        """Find a project on Waldur B by its backend_id.

        Returns:
            Project dict with uuid/url/name or None if not found.
        """
        try:
            response = self._api_client.get_httpx_client().get(
                "/api/projects/",
                params={"backend_id": backend_id},
            )
            if response.status_code == 200:
                projects = response.json()
                if projects:
                    project = projects[0]
                    return {
                        "uuid": project.get("uuid", ""),
                        "url": project.get("url", ""),
                        "name": project.get("name", ""),
                    }
        except Exception:
            logger.exception("Failed to find project by backend_id=%s", backend_id)
        return None

    def create_project(
        self,
        customer_url: str,
        name: str,
        backend_id: str,
        description: str = "",
    ) -> dict:
        """Create a project on Waldur B.

        Returns:
            Dict with uuid and url of created project.
        """
        from waldur_api_client.api.projects import projects_create  # noqa: PLC0415

        body = ProjectRequest(
            name=name,
            customer=customer_url,
            backend_id=backend_id,
            description=description,
        )

        project = projects_create.sync(
            client=self._api_client,
            body=body,
        )

        return {
            "uuid": str(project.uuid) if not isinstance(project.uuid, type(UNSET)) else "",
            "url": project.url if not isinstance(project.url, type(UNSET)) else "",
            "name": project.name if not isinstance(project.name, type(UNSET)) else "",
        }

    def find_or_create_project(
        self,
        customer_url: str,
        name: str,
        backend_id: str,
    ) -> dict:
        """Find a project by backend_id, or create one if not found.

        Returns:
            Dict with uuid and url of the project.
        """
        existing = self.find_project_by_backend_id(backend_id)
        if existing:
            logger.info("Found existing project with backend_id=%s", backend_id)
            return existing

        logger.info("Creating new project %s with backend_id=%s", name, backend_id)
        return self.create_project(customer_url, name, backend_id)

    # --- User Operations ---

    def add_user_to_project(
        self,
        project_uuid: UUID,
        user_uuid: UUID,
        role: str = DEFAULT_PROJECT_ROLE,
    ) -> None:
        """Add a user to a project on Waldur B."""
        from waldur_api_client.api.projects import projects_add_user  # noqa: PLC0415

        body = UserRoleCreateRequest(
            role=role,
            user=user_uuid,
        )

        projects_add_user.sync(
            uuid=project_uuid,
            client=self._api_client,
            body=body,
        )

    def remove_user_from_project(
        self,
        project_uuid: UUID,
        user_uuid: UUID,
        role: str = DEFAULT_PROJECT_ROLE,
    ) -> None:
        """Remove a user from a project on Waldur B."""
        from waldur_api_client.api.projects import projects_delete_user  # noqa: PLC0415

        body = UserRoleDeleteRequest(
            role=role,
            user=user_uuid,
        )

        projects_delete_user.sync_detailed(
            uuid=project_uuid,
            client=self._api_client,
            body=body,
        )

    def list_project_users(self, project_uuid: UUID) -> list:
        """List users of a project on Waldur B."""
        from waldur_api_client.api.projects import projects_list_users_list  # noqa: PLC0415

        return projects_list_users_list.sync(
            uuid=project_uuid,
            client=self._api_client,
        )

    def resolve_user_by_cuid(self, cuid: str) -> Optional[UUID]:
        """Resolve a user on Waldur B by eduTeams CUID.

        Returns:
            User UUID on Waldur B, or None if not found.
        """
        from waldur_api_client.api.remote_eduteams import remote_eduteams  # noqa: PLC0415

        try:
            result = remote_eduteams.sync(
                client=self._api_client,
                body=RemoteEduteamsRequestRequest(cuid=cuid),
            )
            return result.uuid
        except Exception:
            logger.exception("Failed to resolve user by CUID: %s", cuid)
            return None

    def resolve_user_by_field(
        self, value: str, field: str = "email"
    ) -> Optional[UUID]:
        """Resolve a user on Waldur B by email or username.

        Args:
            value: The value to search for.
            field: Either "email" or "username".

        Returns:
            User UUID on Waldur B, or None if not found.
        """
        from waldur_api_client.api.users import users_list  # noqa: PLC0415

        try:
            kwargs = {field: value}
            users = users_list.sync(
                client=self._api_client,
                **kwargs,
            )
            if users:
                user = users[0]
                uuid_val = user.uuid if not isinstance(user.uuid, type(UNSET)) else None
                return uuid_val
        except Exception:
            logger.exception("Failed to resolve user by %s=%s", field, value)
        return None

    # --- Component Usage Operations ---

    def get_component_usages(
        self,
        resource_uuid: UUID,
        billing_period: Optional[datetime.date] = None,
    ) -> list[ComponentUsage]:
        """Get component usages for a resource on Waldur B.

        Args:
            resource_uuid: Resource UUID on Waldur B.
            billing_period: Optional billing period date to filter by.
        """
        from waldur_api_client.api.marketplace_component_usages import (  # noqa: PLC0415
            marketplace_component_usages_list,
        )
        from waldur_api_client.types import UNSET as _UNSET  # noqa: PLC0415

        return marketplace_component_usages_list.sync(
            client=self._api_client,
            resource_uuid=resource_uuid,
            billing_period=billing_period if billing_period is not None else _UNSET,
        )

    def get_component_user_usages(
        self,
        resource_uuid: UUID,
        billing_period: Optional[datetime.date] = None,
    ) -> list[ComponentUserUsage]:
        """Get per-user component usages for a resource on Waldur B.

        Args:
            resource_uuid: Resource UUID on Waldur B.
            billing_period: Optional billing period date to filter by.
        """
        from waldur_api_client.api.marketplace_component_user_usages import (  # noqa: PLC0415
            marketplace_component_user_usages_list,
        )

        kwargs: dict = {
            "client": self._api_client,
            "resource_uuid": resource_uuid,
        }
        if billing_period is not None:
            kwargs["component_usage_billing_period"] = billing_period

        return marketplace_component_user_usages_list.sync(**kwargs)

    # --- BaseClient Abstract Method Implementations ---

    def list_resources(self) -> list[ClientResource]:
        """List resources on Waldur B for the configured offering."""
        resources = self.list_marketplace_resources(
            offering_uuid=UUID(self.offering_uuid)
        )
        return [
            ClientResource(
                name=str(r.uuid) if not isinstance(r.uuid, type(UNSET)) else "",
                description=r.name if not isinstance(r.name, type(UNSET)) else "",
                organization=str(r.project_uuid) if not isinstance(r.project_uuid, type(UNSET)) else "",
            )
            for r in resources
        ]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get a resource by its UUID on Waldur B."""
        try:
            resource = self.get_marketplace_resource(UUID(resource_id))
            return ClientResource(
                name=str(resource.uuid) if not isinstance(resource.uuid, type(UNSET)) else "",
                description=resource.name if not isinstance(resource.name, type(UNSET)) else "",
                organization=str(resource.project_uuid)
                if not isinstance(resource.project_uuid, type(UNSET))
                else "",
            )
        except Exception:
            logger.debug("Resource %s not found on Waldur B", resource_id)
            return None

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Create resource — handled by backend via marketplace orders."""
        del description, organization, parent_name
        return name

    def delete_resource(self, name: str) -> str:
        """Delete resource — handled by backend via terminate orders."""
        return name

    def set_resource_limits(
        self, resource_id: str, limits_dict: dict[str, int]
    ) -> Optional[str]:
        """Set resource limits — handled by backend via update limits orders."""
        del resource_id, limits_dict
        return None

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get current resource limits from Waldur B."""
        try:
            resource = self.get_marketplace_resource(UUID(resource_id))
            limits = resource.limits
            if limits and not isinstance(limits, type(UNSET)):
                return dict(limits.additional_properties)
        except Exception:
            logger.exception("Failed to get resource limits for %s", resource_id)
        return {}

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Per-user limits not supported in Waldur federation."""
        del resource_id
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Per-user limits not supported in Waldur federation."""
        del resource_id, limits_dict
        return f"User limits not supported for {username}"

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Check if user is associated with resource on Waldur B.

        The 'user' here is a user UUID (string) on Waldur B,
        and 'resource_id' is the resource UUID on Waldur B.
        """
        try:
            resource = self.get_marketplace_resource(UUID(resource_id))
            project_uuid = resource.project_uuid
            if isinstance(project_uuid, type(UNSET)):
                return None

            users = self.list_project_users(project_uuid)
            for project_user in users:
                user_uuid = getattr(project_user, "uuid", None)
                if user_uuid and str(user_uuid) == user:
                    return Association(account=resource_id, user=user)
        except Exception:
            logger.debug("Failed to check association for user %s on resource %s", user, resource_id)
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create association — handled by backend user management."""
        del default_account
        return f"Association created for {username} in {resource_id}"

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association — handled by backend user management."""
        return f"Association deleted for {username} from {resource_id}"

    def get_usage_report(
        self, resource_ids: list[str], timezone: Optional[str] = None
    ) -> list:
        """Get raw usage data from Waldur B. Processing done by backend."""
        del timezone
        all_usages = []
        for resource_id in resource_ids:
            try:
                usages = self.get_component_usages(UUID(resource_id))
                all_usages.extend(usages)
            except Exception:
                logger.exception("Failed to get usage for resource %s", resource_id)
        return all_usages

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List users associated with a resource on Waldur B."""
        try:
            team = self.get_resource_team(UUID(resource_id))
            usernames = []
            for member in team:
                username = getattr(member, "username", None)
                if username and not isinstance(username, type(UNSET)):
                    usernames.append(username)
            return usernames
        except Exception:
            logger.exception("Failed to list users for resource %s", resource_id)
            return []

    def create_linux_user_homedir(self, username: str, umask: str = "") -> str:
        """Not applicable for Waldur federation."""
        del username, umask
        return ""

    # --- Utility Methods ---

    @staticmethod
    def _normalize_uuid(value: str) -> str:
        """Normalize a UUID string to the hex format used in Waldur API URLs."""
        return UUID(value).hex

    def get_offering_url(self) -> str:
        """Get the API URL for the target offering."""
        return f"{self.api_url}/api/marketplace-public-offerings/{self._normalize_uuid(self.offering_uuid)}/"

    def get_customer_url(self, customer_uuid: str) -> str:
        """Get the API URL for a customer on Waldur B."""
        return f"{self.api_url}/api/customers/{self._normalize_uuid(customer_uuid)}/"

    def get_project_url(self, project_uuid: str) -> str:
        """Get the API URL for a project on Waldur B."""
        return f"{self.api_url}/api/projects/{self._normalize_uuid(project_uuid)}/"

    def get_version(self) -> str:
        """Get the version of the remote Waldur B instance.

        Returns:
            Version string from Waldur B.
        """
        version_info = version_retrieve.sync(client=self._api_client)
        return version_info.version

    def ping(self) -> bool:
        """Test connectivity to Waldur B."""
        try:
            self.list_marketplace_resources(offering_uuid=UUID(self.offering_uuid))
        except Exception:
            logger.exception("Waldur B API not reachable")
            return False
        else:
            return True
