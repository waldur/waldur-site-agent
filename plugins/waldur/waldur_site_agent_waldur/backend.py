"""Waldur-to-Waldur federation backend.

Implements BaseBackend to forward orders from Waldur A to Waldur B,
pull usage back, and synchronize user memberships between instances.
"""

from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import UNSET

from waldur_site_agent.backend import backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from waldur_site_agent_waldur.client import WaldurClient
from waldur_site_agent_waldur.component_mapping import ComponentMapper

logger = logging.getLogger(__name__)


class WaldurBackend(backends.BaseBackend):
    """Backend that federates to another Waldur instance.

    Creates resources on Waldur B via marketplace orders, pulls usage back
    with optional component conversion, and synchronizes user memberships.
    """

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
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
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

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Create a resource on Waldur B via a marketplace order.

        1. Find/create project on Waldur B
        2. Convert limits via ComponentMapper
        3. Create marketplace order on Waldur B
        4. Poll for order completion
        5. Return resource info with Waldur B resource UUID as backend_id
        """
        self._pre_create_resource(waldur_resource, user_context)

        # Collect and convert limits
        _backend_limits, waldur_limits = self._collect_resource_limits(waldur_resource)
        target_limits = self.component_mapper.convert_limits_to_target(waldur_limits)

        # Find the project on Waldur B
        project_uuid = waldur_resource.project_uuid
        customer_uuid = waldur_resource.customer_uuid
        backend_id = f"{customer_uuid}_{project_uuid}"
        project = self.client.find_project_by_backend_id(backend_id)

        if not project:
            msg = f"Project not found on Waldur B for backend_id={backend_id}"
            raise BackendError(msg)

        project_url = self.client.get_project_url(project["uuid"])
        offering_url = self.client.get_offering_url()

        # Create the order
        attributes = {
            "name": waldur_resource.name or "",
        }

        logger.info(
            "Creating marketplace order on Waldur B: offering=%s, project=%s, limits=%s",
            self.target_offering_uuid,
            project["uuid"],
            target_limits,
        )

        order = self.client.create_marketplace_order(
            project_url=project_url,
            offering_url=offering_url,
            limits=target_limits,
            attributes=attributes,
        )

        order_uuid = order.uuid if not isinstance(order.uuid, type(UNSET)) else None
        if not order_uuid:
            msg = "Order created but no UUID returned"
            raise BackendError(msg)

        # Poll for completion
        logger.info("Waiting for order %s to complete on Waldur B...", order_uuid)
        completed_order = self.client.poll_order_completion(
            order_uuid=order_uuid,
            timeout=self.order_poll_timeout,
            interval=self.order_poll_interval,
        )

        # Extract the created resource UUID
        resource_uuid = completed_order.marketplace_resource_uuid
        if isinstance(resource_uuid, type(UNSET)) or not resource_uuid:
            msg = f"Order {order_uuid} completed but no marketplace_resource_uuid returned"
            raise BackendError(msg)

        resource_backend_id = str(resource_uuid)
        logger.info(
            "Resource created on Waldur B: resource_uuid=%s, order_uuid=%s",
            resource_backend_id,
            order_uuid,
        )

        return BackendResourceInfo(
            backend_id=resource_backend_id,
            limits=waldur_limits,
        )

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:
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
        self, resource_id: str
    ) -> dict[str, dict[str, float]]:
        """Get usage report for a single resource from Waldur B."""
        resource_uuid = UUID(resource_id)

        # Fetch total component usages
        component_usages = self.client.get_component_usages(resource_uuid)
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
        user_usages = self.client.get_component_user_usages(resource_uuid)

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
