"""Waldur-to-Waldur federation backend.

Implements BaseBackend to forward orders from Waldur A to Waldur B,
pull usage back, and synchronize user memberships between instances.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional
from uuid import UUID

from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import UNSET

from waldur_api_client.models.observable_object_type_enum import (
    ObservableObjectTypeEnum,
)

from waldur_site_agent.backend import backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from waldur_site_agent_waldur.client import WaldurClient
from waldur_site_agent_waldur.component_mapping import ComponentMapper

logger = logging.getLogger(__name__)

TERMINAL_ERROR_STATES = {OrderState.ERRED, OrderState.CANCELED, OrderState.REJECTED}


class WaldurBackend(backends.BaseBackend):
    """Backend that federates to another Waldur instance.

    Creates resources on Waldur B via marketplace orders, pulls usage back
    with optional component conversion, and synchronizes user memberships.
    """

    supports_async_orders = True

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        super().__init__(backend_settings, backend_components)
        self.backend_type = "waldur"

        # Required settings
        for key in ("target_api_url", "target_api_token", "target_offering_uuid", "target_customer_uuid"):
            if key not in backend_settings:
                msg = f"Missing required backend setting: {key}"
                raise ValueError(msg)

        self.target_offering_uuid = backend_settings["target_offering_uuid"]
        self.target_customer_uuid = backend_settings["target_customer_uuid"]
        self.user_match_field = backend_settings.get("user_match_field", "cuid")
        self.order_poll_timeout = int(backend_settings.get("order_poll_timeout", 300))
        self.order_poll_interval = int(backend_settings.get("order_poll_interval", 5))
        self.user_not_found_action = backend_settings.get("user_not_found_action", "warn")

        self.client: WaldurClient = WaldurClient(
            api_url=backend_settings["target_api_url"],
            api_token=backend_settings["target_api_token"],
            offering_uuid=self.target_offering_uuid,
        )

        self.component_mapper = ComponentMapper(backend_components)

        # Cache for resolved user UUIDs on Waldur B: local_identifier -> remote_uuid
        self._user_uuid_cache: dict[str, Optional[UUID]] = {}

    # --- Abstract Method Implementations ---

    def ping(self, raise_exception: bool = False) -> bool:
        """Check connectivity to Waldur B."""
        try:
            return self.client.ping()
        except Exception as e:
            if raise_exception:
                raise BackendError(f"Waldur B not reachable: {e}") from e
            logger.exception("Waldur B not reachable")
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about the Waldur B connection."""
        logger.info("Waldur federation backend diagnostics:")
        logger.info("  Target API URL: %s", self.client.api_url)
        logger.info("  Target offering UUID: %s", self.target_offering_uuid)
        logger.info("  Target customer UUID: %s", self.target_customer_uuid)
        logger.info("  User match field: %s", self.user_match_field)
        logger.info("  Component mapper passthrough: %s", self.component_mapper.is_passthrough)
        return self.ping()

    def list_components(self) -> list[str]:
        """Return source component names from configuration."""
        return list(self.backend_components.keys())

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Find or create the project on Waldur B before creating the resource.

        Uses backend_id = "{customer_uuid}_{project_uuid}" to track
        the mapping between Waldur A and Waldur B projects.
        """
        del user_context

        project_uuid = waldur_resource.project_uuid
        customer_uuid = waldur_resource.customer_uuid
        project_name = waldur_resource.project_name or f"Project {project_uuid}"

        if not project_uuid:
            msg = "No project UUID in Waldur resource"
            raise BackendError(msg)

        backend_id = f"{customer_uuid}_{project_uuid}"
        customer_url = self.client.get_customer_url(self.target_customer_uuid)

        project = self.client.find_or_create_project(
            customer_url=customer_url,
            name=project_name,
            backend_id=backend_id,
        )

        if not project or not project.get("uuid"):
            msg = f"Failed to find or create project on Waldur B for backend_id={backend_id}"
            raise BackendError(msg)

        logger.info(
            "Waldur B project ready: uuid=%s, backend_id=%s",
            project["uuid"],
            backend_id,
        )

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create a resource on Waldur B via a marketplace order (non-blocking).

        Overrides BaseBackend.create_resource_with_id because the Waldur
        federation backend creates resources via marketplace orders on the
        target instance, not via the generic client.create_resource() path.

        Returns immediately after order submission. The target resource UUID
        is available on the order response (the resource is created in
        CREATING state). Order completion is tracked via ``pending_order_id``
        and checked by ``check_pending_order()`` on subsequent polling cycles.

        The target resource UUID is stored as ``backend_id`` on the source
        resource (Waldur A) by the core processor. The agent does NOT set
        ``backend_id`` on the target resource (Waldur B) — that is managed
        by B's own service provider.

        Steps:
            1. Find/create project on Waldur B (_pre_create_resource)
            2. Create marketplace order on Waldur B
            3. Return resource info with pending_order_id for async tracking
        """
        logger.info("Creating resource in the backend with ID: %s", resource_backend_id)

        # Pre-create: find or create project on Waldur B
        self._pre_create_resource(waldur_resource, user_context)

        # Collect and convert limits
        _backend_limits, waldur_limits = self._collect_resource_limits(waldur_resource)
        target_limits = self.component_mapper.convert_limits_to_target(waldur_limits)

        # Find the project on Waldur B
        project_uuid = waldur_resource.project_uuid
        customer_uuid = waldur_resource.customer_uuid
        project_backend_id = f"{customer_uuid}_{project_uuid}"
        project = self.client.find_project_by_backend_id(project_backend_id)

        if not project:
            msg = f"Project not found on Waldur B for backend_id={project_backend_id}"
            raise BackendError(msg)

        project_url = self.client.get_project_url(project["uuid"])
        offering_url = self.client.get_offering_url()

        # Create the order
        attributes = {
            "name": waldur_resource.name or "",
        }

        # Forward configured source attributes to the target order
        resource_attrs = waldur_resource.attributes
        if resource_attrs and not isinstance(resource_attrs, type(UNSET)):
            passthrough_keys = self.backend_settings.get("passthrough_attributes", [])
            if passthrough_keys:
                attrs_dict = resource_attrs.to_dict()
                for key in passthrough_keys:
                    if key in attrs_dict:
                        attributes[key] = attrs_dict[key]

        logger.info(
            "Creating marketplace order on Waldur B: offering=%s, project=%s, limits=%s",
            self.target_offering_uuid,
            project["uuid"],
            target_limits,
        )

        try:
            order = self.client.create_marketplace_order(
                project_url=project_url,
                offering_url=offering_url,
                limits=target_limits,
                attributes=attributes,
            )
        except UnexpectedStatus as e:
            if e.status_code == 400:
                error_detail = e.content.decode(errors="ignore")
                msg = f"Target Waldur rejected order (HTTP 400): {error_detail}"
                raise BackendError(msg) from e
            raise

        order_uuid = order.uuid if not isinstance(order.uuid, type(UNSET)) else None
        if not order_uuid:
            msg = "Order created but no UUID returned"
            raise BackendError(msg)

        # Target resource UUID is available immediately on the order response
        resource_uuid = order.marketplace_resource_uuid
        if isinstance(resource_uuid, type(UNSET)) or not resource_uuid:
            msg = f"Order {order_uuid} created but no marketplace_resource_uuid returned"
            raise BackendError(msg)

        target_resource_uuid = str(resource_uuid)
        target_order_uuid = str(order_uuid)

        logger.info(
            "Order submitted on Waldur B: order_uuid=%s, resource_uuid=%s (non-blocking)",
            target_order_uuid,
            target_resource_uuid,
        )

        # Return immediately — order completion deferred to check_pending_order()
        return BackendResourceInfo(
            backend_id=target_resource_uuid,
            pending_order_id=target_order_uuid,
            limits=waldur_limits,
        )

    def check_pending_order(self, order_backend_id: str) -> bool:
        """Check if target order on Waldur B has completed.

        Args:
            order_backend_id: Target order UUID on Waldur B.

        Returns:
            True if target order completed successfully, False if still pending.

        Raises:
            BackendError: If the target order failed or was cancelled.
        """
        target_order = self.client.get_order(UUID(order_backend_id))

        if target_order.state == OrderState.DONE:
            logger.info("Target order %s completed successfully", order_backend_id)
            return True

        if target_order.state in TERMINAL_ERROR_STATES:
            msg = f"Target order {order_backend_id} failed: {target_order.state}"
            raise BackendError(msg)

        logger.info(
            "Target order %s still in state %s", order_backend_id, target_order.state
        )
        return False

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> None:
        """Terminate resource on Waldur B via a marketplace order."""
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id or not resource_backend_id.strip():
            logger.warning("Empty backend_id for resource, skipping deletion")
            return

        # Check if resource exists on Waldur B
        if self.client.get_resource(resource_backend_id) is None:
            logger.warning("Resource %s not found on Waldur B, skipping", resource_backend_id)
            return

        try:
            order_uuid = self.client.create_terminate_order(UUID(resource_backend_id))
            logger.info(
                "Termination order %s created for resource %s on Waldur B",
                order_uuid,
                resource_backend_id,
            )
            self.client.poll_order_completion(
                order_uuid=order_uuid,
                timeout=self.order_poll_timeout,
                interval=self.order_poll_interval,
            )
            logger.info("Resource %s terminated on Waldur B", resource_backend_id)
        except BackendError:
            logger.exception("Failed to terminate resource %s on Waldur B", resource_backend_id)
            raise

    def set_resource_limits(
        self, resource_backend_id: str, limits: dict[str, int]
    ) -> None:
        """Update resource limits on Waldur B via an update order."""
        target_limits = self.component_mapper.convert_limits_to_target(limits)
        try:
            order_uuid = self.client.create_update_order(
                resource_uuid=UUID(resource_backend_id),
                limits=target_limits,
            )
            logger.info(
                "Update limits order %s created for resource %s",
                order_uuid,
                resource_backend_id,
            )
            self.client.poll_order_completion(
                order_uuid=order_uuid,
                timeout=self.order_poll_timeout,
                interval=self.order_poll_interval,
            )
        except BackendError:
            logger.exception(
                "Failed to update limits for resource %s on Waldur B", resource_backend_id
            )
            raise

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect limits from Waldur resource.

        For the Waldur federation backend, backend_limits and waldur_limits
        are the same since conversion happens at the component mapper level.
        """
        waldur_limits: dict[str, int] = {}
        limits = waldur_resource.limits

        if not limits or isinstance(limits, type(UNSET)):
            return {}, {}

        for component_key in self.backend_components:
            if component_key in limits:
                waldur_limits[component_key] = limits[component_key]

        # For Waldur federation, backend_limits = waldur_limits
        # (conversion to target happens separately via component_mapper)
        return waldur_limits, waldur_limits

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscaling not directly supported via Waldur API."""
        logger.info("Downscale not supported for Waldur federation resource %s", resource_backend_id)
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pausing not directly supported via Waldur API."""
        logger.info("Pause not supported for Waldur federation resource %s", resource_backend_id)
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore not directly supported via Waldur API."""
        logger.info("Restore not supported for Waldur federation resource %s", resource_backend_id)
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return metadata about the resource on Waldur B."""
        return {"waldur_b_resource_uuid": resource_backend_id}

    # --- Usage Reporting ---

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Pull usage from Waldur B and reverse-convert via ComponentMapper.

        For each resource:
        1. Fetch component usages from Waldur B
        2. Fetch per-user component usages from Waldur B
        3. Reverse-map target component usage -> source components
        4. Build report with TOTAL_ACCOUNT_USAGE key
        """
        report: dict[str, dict[str, dict[str, float]]] = {}

        for resource_id in resource_backend_ids:
            try:
                resource_report = self._get_single_resource_usage(resource_id)
                report[resource_id] = resource_report
            except Exception:
                logger.exception("Failed to get usage for resource %s", resource_id)
                # Return empty usage for this resource
                empty_usage: dict[str, float] = {
                    comp: 0.0 for comp in self.backend_components
                }
                report[resource_id] = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return report

    def _get_single_resource_usage(
        self,
        resource_id: str,
        billing_period: Optional[datetime.date] = None,
    ) -> dict[str, dict[str, float]]:
        """Get usage report for a single resource from Waldur B."""
        resource_uuid = UUID(resource_id)

        # Fetch total component usages
        component_usages = self.client.get_component_usages(
            resource_uuid, billing_period=billing_period
        )
        target_total_usage: dict[str, float] = {}
        for usage in component_usages:
            comp_type = usage.type_ if not isinstance(usage.type_, type(UNSET)) else None
            raw_usage = usage.usage if not isinstance(usage.usage, type(UNSET)) else 0
            comp_usage = float(raw_usage) if raw_usage else 0.0
            if comp_type:
                target_total_usage[comp_type] = (
                    target_total_usage.get(comp_type, 0) + comp_usage
                )

        # Reverse-convert total usage
        source_total_usage = self.component_mapper.convert_usage_from_target(target_total_usage)

        # Ensure all configured components are present
        for comp in self.backend_components:
            if comp not in source_total_usage:
                source_total_usage[comp] = 0.0

        resource_report: dict[str, dict[str, float]] = {
            "TOTAL_ACCOUNT_USAGE": source_total_usage,
        }

        # Fetch per-user usages
        user_usages = self.client.get_component_user_usages(
            resource_uuid, billing_period=billing_period
        )

        # Group user usages by username and component
        per_user_target: dict[str, dict[str, float]] = {}
        for user_usage in user_usages:
            username = (
                user_usage.username
                if not isinstance(user_usage.username, type(UNSET))
                else None
            )
            comp_type = (
                user_usage.component_type
                if not isinstance(user_usage.component_type, type(UNSET))
                else None
            )
            raw_user_usage = (
                user_usage.usage
                if not isinstance(user_usage.usage, type(UNSET))
                else 0
            )
            usage_val = float(raw_user_usage) if raw_user_usage else 0.0

            if username and comp_type:
                if username not in per_user_target:
                    per_user_target[username] = {}
                per_user_target[username][comp_type] = (
                    per_user_target[username].get(comp_type, 0) + usage_val
                )

        # Reverse-convert per-user usage
        for username, target_usage in per_user_target.items():
            source_usage = self.component_mapper.convert_usage_from_target(target_usage)
            resource_report[username] = source_usage

        return resource_report

    def get_usage_report_for_period(
        self, resource_backend_ids: list[str], year: int, month: int
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Pull usage from Waldur B for a specific billing period."""
        billing_period = datetime.date(year, month, 1)
        report: dict[str, dict[str, dict[str, float]]] = {}

        for resource_id in resource_backend_ids:
            try:
                resource_report = self._get_single_resource_usage(
                    resource_id, billing_period=billing_period
                )
                report[resource_id] = resource_report
            except Exception:
                logger.exception(
                    "Failed to get usage for resource %s (period %s-%s)",
                    resource_id, year, month,
                )
                empty_usage: dict[str, float] = {
                    comp: 0.0 for comp in self.backend_components
                }
                report[resource_id] = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return report

    # --- User/Membership Sync ---

    def _resolve_remote_user(self, local_username: str) -> Optional[UUID]:
        """Resolve a local username to a user UUID on Waldur B.

        Uses the configured user_match_field to look up the user.
        Results are cached to minimize API calls.
        """
        if local_username in self._user_uuid_cache:
            return self._user_uuid_cache[local_username]

        remote_uuid: Optional[UUID] = None

        if self.user_match_field == "cuid":
            remote_uuid = self.client.resolve_user_by_cuid(local_username)
        elif self.user_match_field in ("email", "username"):
            remote_uuid = self.client.resolve_user_by_field(
                local_username, self.user_match_field
            )
        else:
            logger.error("Unknown user_match_field: %s", self.user_match_field)

        if remote_uuid is not None:
            self._user_uuid_cache[local_username] = remote_uuid

        if remote_uuid is None:
            if self.user_not_found_action == "fail":
                msg = (
                    f"User {local_username} not found on Waldur B "
                    f"(match_field={self.user_match_field})"
                )
                raise BackendError(msg)
            logger.warning(
                "User %s not found on Waldur B (match_field=%s)",
                local_username,
                self.user_match_field,
            )

        return remote_uuid

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add users to the resource's project on Waldur B."""
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        if not user_ids:
            return set()

        logger.info(
            "Adding %d users to Waldur B resource %s",
            len(user_ids),
            resource_backend_id,
        )

        # Get the project UUID from the resource on Waldur B
        project_uuid = self._get_resource_project_uuid(resource_backend_id)
        if not project_uuid:
            logger.error(
                "Cannot find project for resource %s on Waldur B", resource_backend_id
            )
            return set()

        added_users: set[str] = set()
        for username in user_ids:
            try:
                remote_user_uuid = self._resolve_remote_user(username)
                if remote_user_uuid:
                    self.client.add_user_to_project(
                        project_uuid=project_uuid,
                        user_uuid=remote_user_uuid,
                    )
                    added_users.add(username)
                    logger.info("Added user %s to Waldur B project %s", username, project_uuid)
            except BackendError:
                logger.exception("Failed to add user %s to Waldur B", username)
            except Exception:
                logger.exception("Unexpected error adding user %s to Waldur B", username)

        return added_users

    def remove_users_from_resource(
        self, waldur_resource: WaldurResource, usernames: set[str]
    ) -> list[str]:
        """Remove users from the resource's project on Waldur B."""
        resource_backend_id = waldur_resource.backend_id
        if not usernames:
            return []

        logger.info(
            "Removing %d users from Waldur B resource %s",
            len(usernames),
            resource_backend_id,
        )

        project_uuid = self._get_resource_project_uuid(resource_backend_id)
        if not project_uuid:
            logger.error(
                "Cannot find project for resource %s on Waldur B", resource_backend_id
            )
            return []

        removed_users: list[str] = []
        for username in usernames:
            try:
                remote_user_uuid = self._resolve_remote_user(username)
                if remote_user_uuid:
                    self.client.remove_user_from_project(
                        project_uuid=project_uuid,
                        user_uuid=remote_user_uuid,
                    )
                    removed_users.append(username)
                    logger.info(
                        "Removed user %s from Waldur B project %s", username, project_uuid
                    )
            except Exception:
                logger.exception("Failed to remove user %s from Waldur B", username)

        return removed_users

    def _get_resource_project_uuid(self, resource_backend_id: str) -> Optional[UUID]:
        """Get the project UUID for a resource on Waldur B."""
        try:
            resource = self.client.get_marketplace_resource(UUID(resource_backend_id))
            project_uuid = resource.project_uuid
            if not isinstance(project_uuid, type(UNSET)):
                return project_uuid
        except Exception:
            logger.exception(
                "Failed to get project UUID for resource %s", resource_backend_id
            )
        return None

    # --- Offering User Username Sync ---

    def sync_offering_user_usernames(
        self,
        waldur_a_offering_uuid: str,
        waldur_rest_client: AuthenticatedClient,
    ) -> bool:
        """Pull offering user usernames from Waldur B and update on Waldur A.

        Lists offering users on Waldur B for the target offering, matches them
        to Waldur A offering users by resolving user identity, and updates the
        Waldur A offering user username when it differs from the Waldur B value.

        For offering users in CREATING state on Waldur A, setting the username
        auto-transitions them to OK in Mastermind.

        Args:
            waldur_a_offering_uuid: UUID of the offering on Waldur A.
            waldur_rest_client: Authenticated client for the Waldur A API.

        Returns:
            True if any offering user usernames were updated.
        """
        from waldur_api_client.api.marketplace_offering_users import (  # noqa: PLC0415
            marketplace_offering_users_list,
            marketplace_offering_users_partial_update,
        )
        from waldur_api_client.models.offering_user_state import (  # noqa: PLC0415
            OfferingUserState,
        )
        from waldur_api_client.models.patched_offering_user_request import (  # noqa: PLC0415
            PatchedOfferingUserRequest,
        )

        try:
            # 1. Fetch Waldur B offering users with assigned usernames (OK state)
            waldur_b_offering_users = self.client.list_offering_users(
                offering_uuid=self.target_offering_uuid,
                state=[OfferingUserState.OK],
            )
            if not waldur_b_offering_users:
                logger.debug("No OK offering users found on Waldur B")
                return False

            # 2. Build map: Waldur B user_uuid -> offering username
            b_uuid_to_username: dict[str, str] = {}
            for ou in waldur_b_offering_users:
                user_uuid = ou.user_uuid
                username = ou.username
                if (
                    not isinstance(user_uuid, type(UNSET))
                    and user_uuid
                    and not isinstance(username, type(UNSET))
                    and username
                ):
                    b_uuid_to_username[str(user_uuid)] = username

            if not b_uuid_to_username:
                return False

            # 3. Fetch Waldur A offering users that may need username sync
            waldur_a_offering_users = marketplace_offering_users_list.sync_all(
                client=waldur_rest_client,
                offering_uuid=[UUID(waldur_a_offering_uuid)],
                state=[
                    OfferingUserState.OK,
                    OfferingUserState.CREATING,
                    OfferingUserState.REQUESTED,
                ],
                is_restricted=False,
            )

            # 4. Match and update
            changed = False
            for a_ou in waldur_a_offering_users:
                a_user_username = a_ou.user_username
                if isinstance(a_user_username, type(UNSET)) or not a_user_username:
                    continue

                # Resolve Waldur A user identity -> Waldur B user UUID
                remote_uuid = self._resolve_remote_user(a_user_username)
                if remote_uuid is None:
                    continue

                # Look up the username Waldur B assigned
                b_username = b_uuid_to_username.get(str(remote_uuid))
                if not b_username:
                    continue

                # Update Waldur A offering user if username differs
                current = a_ou.username
                if isinstance(current, type(UNSET)):
                    current = None
                if current == b_username:
                    continue

                logger.info(
                    "Syncing offering user %s username from Waldur B: %s -> %s",
                    a_ou.uuid,
                    current,
                    b_username,
                )
                marketplace_offering_users_partial_update.sync(
                    uuid=a_ou.uuid,
                    client=waldur_rest_client,
                    body=PatchedOfferingUserRequest(username=b_username),
                )
                changed = True

            return changed

        except Exception:
            logger.exception("Failed to sync offering user usernames from Waldur B")
            return False

    # --- Target Event Subscriptions ---

    def setup_target_event_subscriptions(
        self,
        source_offering,
        user_agent: str = "",
        global_proxy: str = "",
    ) -> list:
        """Set up STOMP subscriptions to events on Waldur B.

        When target_stomp_enabled is True, subscribes to:
        - ORDER events: updates source orders on Waldur A when target orders complete.
        - OFFERING_USER events: syncs usernames from Waldur B to A when offering
          users are created or updated with a username.

        Args:
            source_offering: The source Waldur offering (Waldur A).
            user_agent: User agent string for API calls.
            global_proxy: Optional proxy configuration.

        Returns:
            List of StompConsumer tuples for lifecycle management.
        """
        if not self.backend_settings.get("target_stomp_enabled"):
            return []

        try:
            # Lazy imports: common.utils eagerly loads backend entry points
            # via entry_point.load(), which re-imports this module. Importing
            # anything that transitively touches common.utils at module level
            # causes a circular import when this plugin is loaded first.
            from waldur_site_agent.common.agent_identity_management import (
                AgentIdentityManager,
            )
            from waldur_site_agent.common.structures import Offering
            from waldur_site_agent.common.utils import get_client
            from waldur_site_agent.event_processing.event_subscription_manager import (
                WALDUR_LISTENER_NAME,
            )
            from waldur_site_agent.event_processing.utils import (
                _setup_single_stomp_subscription,
            )
            from waldur_site_agent_waldur.target_event_handler import (
                make_target_offering_user_handler,
                make_target_order_handler,
            )

            target_api_url = self.backend_settings["target_api_url"]
            target_api_token = self.backend_settings["target_api_token"]

            # The STOMP offering on B must be agent-based (Marketplace.Slurm)
            # since agent identity registration only accepts those.
            # Falls back to target_offering_uuid if not specified.
            target_stomp_offering = self.backend_settings.get(
                "target_stomp_offering_uuid", self.target_offering_uuid
            )

            # Create synthetic Offering for Waldur B to set up STOMP connection
            target_offering = Offering(
                name=f"Target: {source_offering.name}",
                waldur_api_url=target_api_url
                if target_api_url.endswith("/")
                else target_api_url + "/",
                waldur_api_token=target_api_token,
                waldur_offering_uuid=target_stomp_offering,
                backend_type="waldur",
                stomp_enabled=True,
                # Copy STOMP WebSocket settings from source if available
                stomp_ws_host=getattr(source_offering, "stomp_ws_host", None),
                stomp_ws_port=getattr(source_offering, "stomp_ws_port", None),
                stomp_ws_path=getattr(source_offering, "stomp_ws_path", None),
                websocket_use_tls=getattr(source_offering, "websocket_use_tls", True),
            )

            # Register agent identity on Waldur B
            target_client = get_client(
                target_offering.api_url,
                target_offering.api_token,
                user_agent,
                verify_ssl=target_offering.verify_ssl,
                proxy=global_proxy,
            )
            agent_identity_manager = AgentIdentityManager(
                target_offering, target_client
            )
            identity_name = f"agent-{target_offering.uuid}"
            try:
                agent_identity = agent_identity_manager.register_identity(identity_name)
            except Exception:
                logger.exception(
                    "Failed to register agent identity on Waldur B for target STOMP"
                )
                return []

            consumers = []

            # Set up STOMP subscription for ORDER events
            order_consumer = _setup_single_stomp_subscription(
                target_offering,
                agent_identity,
                agent_identity_manager,
                user_agent,
                ObservableObjectTypeEnum.ORDER,
                global_proxy,
            )
            if order_consumer is not None:
                connection, event_subscription, _ = order_consumer
                custom_handler = make_target_order_handler(source_offering)
                listener = connection.get_listener(WALDUR_LISTENER_NAME)
                if listener is not None:
                    listener.on_message_callback = custom_handler
                consumers.append((connection, event_subscription, target_offering))

            # Set up STOMP subscription for OFFERING_USER events.
            # OFFERING_USER events are published against the actual target
            # offering (target_offering_uuid), not the STOMP offering used for
            # agent identity registration.  When these differ, we need a
            # separate Offering/AgentIdentityManager so the
            # EventSubscriptionQueue and STOMP queue name use the correct UUID.
            if target_stomp_offering != self.target_offering_uuid:
                target_ou_offering = Offering(
                    name=f"Target OU: {source_offering.name}",
                    waldur_api_url=target_api_url
                    if target_api_url.endswith("/")
                    else target_api_url + "/",
                    waldur_api_token=target_api_token,
                    waldur_offering_uuid=self.target_offering_uuid,
                    backend_type="waldur",
                    stomp_enabled=True,
                    stomp_ws_host=getattr(source_offering, "stomp_ws_host", None),
                    stomp_ws_port=getattr(source_offering, "stomp_ws_port", None),
                    stomp_ws_path=getattr(source_offering, "stomp_ws_path", None),
                )
                ou_identity_manager = AgentIdentityManager(
                    target_ou_offering, target_client
                )
            else:
                target_ou_offering = target_offering
                ou_identity_manager = agent_identity_manager

            ou_consumer = _setup_single_stomp_subscription(
                target_ou_offering,
                agent_identity,
                ou_identity_manager,
                user_agent,
                ObservableObjectTypeEnum.OFFERING_USER,
                global_proxy,
            )
            if ou_consumer is not None:
                connection, event_subscription, _ = ou_consumer
                custom_handler = make_target_offering_user_handler(
                    source_offering, self
                )
                listener = connection.get_listener(WALDUR_LISTENER_NAME)
                if listener is not None:
                    listener.on_message_callback = custom_handler
                consumers.append((connection, event_subscription, target_ou_offering))

            if not consumers:
                logger.error(
                    "Failed to set up target STOMP subscriptions for %s",
                    source_offering.name,
                )
                return []

            logger.info(
                "Target STOMP subscriptions active for %s -> %s (%d subscriptions)",
                source_offering.name,
                target_offering.name,
                len(consumers),
            )

            return consumers

        except Exception:
            logger.exception(
                "Failed to set up target event subscriptions for %s",
                source_offering.name,
            )
            return []

    # --- Resource Listing ---

    def list_resources(self) -> list[BackendResourceInfo]:
        """List resources on Waldur B for the target offering."""
        resources = self.client.list_resources()
        return [
            BackendResourceInfo(
                backend_id=resource.name,
                parent_id=resource.organization,
            )
            for resource in resources
        ]
