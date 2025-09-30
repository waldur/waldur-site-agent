"""OKD/OpenShift backend implementation for Waldur Site Agent."""

import pprint
from typing import Optional

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_okd.client import OkdClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo


class OkdBackend(backends.BaseBackend):
    """Backend for managing OKD/OpenShift projects and resources."""

    def __init__(self, okd_settings: dict, okd_components: dict[str, dict]) -> None:
        """Initialize OKD backend with settings and components."""
        super().__init__(okd_settings, okd_components)
        self.backend_type = "okd"
        self.client: OkdClient = OkdClient(okd_components, okd_settings)

        # OKD-specific settings
        self.namespace_prefix = okd_settings.get("namespace_prefix", "waldur-")
        self.customer_prefix = okd_settings.get("customer_prefix", "org-")
        self.project_prefix = okd_settings.get("project_prefix", "proj-")
        self.allocation_prefix = okd_settings.get("allocation_prefix", "alloc-")

        logger.info("Initialized OKD backend with namespace prefix: %s", self.namespace_prefix)

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if OKD cluster is accessible."""
        try:
            result = self.client.ping()
            if not result and raise_exception:
                msg = "Failed to ping OKD cluster"
                raise BackendError(msg)  # noqa: TRY301
            return result
        except Exception as e:
            if raise_exception:
                raise
            logger.error(f"Failed to ping OKD cluster: {e}")
            return False

    def diagnostics(self) -> bool:
        """Run diagnostics for OKD cluster connectivity and configuration."""
        format_string = "{:<30} = {:<10}"

        logger.info("=" * 60)
        logger.info("OKD Backend Diagnostics")
        logger.info("=" * 60)

        logger.info(
            format_string.format("API URL", self.backend_settings.get("api_url", "Not set"))
        )
        logger.info(format_string.format("Namespace prefix", self.namespace_prefix))
        logger.info(format_string.format("Customer prefix", self.customer_prefix))
        logger.info(format_string.format("Project prefix", self.project_prefix))
        logger.info(format_string.format("Allocation prefix", self.allocation_prefix))
        logger.info(
            format_string.format(
                "SSL verification", str(self.backend_settings.get("verify_cert", True))
            )
        )
        logger.info("")

        logger.info("OKD components configuration:")
        logger.info(pprint.pformat(self.backend_components))
        logger.info("")

        try:
            self.ping(raise_exception=True)
            logger.info("✓ OKD cluster connection successful")

            # Try to list existing projects
            resources = self.client.list_resources()
            logger.info(f"✓ Found {len(resources)} managed projects")

            return True

        except BackendError as err:
            logger.error("✗ Unable to connect to OKD cluster: %s", err)
            return False
        except Exception as e:
            logger.error("✗ Unexpected error during diagnostics: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of available resource components."""
        # Standard OKD/Kubernetes resource components
        return ["cpu", "memory", "storage", "pods"]

    def _get_customer_backend_id(self, customer_slug: str) -> str:
        """Generate backend ID for customer organization."""
        return f"{self.namespace_prefix}{self.customer_prefix}{customer_slug}"

    def _get_project_backend_id(self, project_slug: str) -> str:
        """Generate backend ID for project namespace."""
        return f"{self.namespace_prefix}{self.project_prefix}{project_slug}"

    def _get_allocation_backend_id(self, allocation_slug: str) -> str:
        """Generate backend ID for allocation/resource namespace."""
        return f"{self.namespace_prefix}{self.allocation_prefix}{allocation_slug}"

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Validate and prepare for resource creation."""
        if not waldur_resource.customer_slug or not waldur_resource.project_slug:
            logger.warning(
                "Resource %s has unset or missing slug fields. customer_slug: %s, project_slug: %s",
                waldur_resource.uuid,
                waldur_resource.customer_slug,
                waldur_resource.project_slug,
            )
            msg = (
                f"Resource {waldur_resource.uuid} has unset or missing slug fields. "
                f"customer_slug: {waldur_resource.customer_slug}, "
                f"project_slug: {waldur_resource.project_slug}. "
                "Cannot create OKD projects with invalid slug values."
            )
            raise BackendError(msg)

        del user_context

        # In OKD, we'll create a namespace for each allocation
        # Optionally, we could also create organization and project namespaces
        # depending on the deployment model

    def _create_backend_resource(
        self,
        resource_backend_id: str,
        resource_name: str,
        resource_organization: str,
        resource_parent_name: Optional[str] = None,
    ) -> bool:
        """Create a project/namespace in OKD cluster."""
        try:
            # Check if resource already exists
            existing = self.client.get_resource(resource_backend_id)
            if existing:
                logger.info(f"Project {resource_backend_id} already exists")
                return False

            # Create new project
            self.client.create_resource(
                name=resource_backend_id,
                description=resource_name,
                organization=resource_organization,
                parent_name=resource_parent_name,
            )

            logger.info(f"Created OKD project: {resource_backend_id}")
            return True

        except BackendError as e:
            logger.error(f"Failed to create project {resource_backend_id}: {e}")
            raise

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Create OKD project for the Waldur resource."""
        self._pre_create_resource(waldur_resource, user_context)

        # Generate backend IDs
        customer_backend_id = self._get_customer_backend_id(waldur_resource.customer_slug)
        project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)

        # For allocation resources, create a dedicated namespace
        if hasattr(waldur_resource, "backend_id") and waldur_resource.backend_id:
            allocation_backend_id = waldur_resource.backend_id
        else:
            # Generate allocation ID from resource UUID or slug
            allocation_slug = waldur_resource.uuid.hex[:8] if waldur_resource.uuid else "default"
            allocation_backend_id = self._get_allocation_backend_id(allocation_slug)

        # Create the allocation namespace
        self._create_backend_resource(
            allocation_backend_id, waldur_resource.name, customer_backend_id, project_backend_id
        )

        # Store the backend_id for future reference
        waldur_resource.backend_id = allocation_backend_id

        # Collect and set limits
        _, waldur_limits = self._collect_resource_limits(waldur_resource)
        if waldur_limits:
            self.client.set_resource_limits(allocation_backend_id, waldur_limits)

        return BackendResourceInfo(
            backend_id=allocation_backend_id,
            parent_id=project_backend_id,
            limits=waldur_limits,
        )

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:  # noqa: ARG002
        """Delete OKD project associated with the resource."""
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id or not resource_backend_id.strip():
            logger.warning(f"Resource {waldur_resource.uuid} has no backend_id, skipping deletion")
            return

        try:
            self.client.delete_resource(resource_backend_id)
            logger.info(f"Deleted OKD project: {resource_backend_id}")
        except BackendError as e:
            logger.error(f"Failed to delete project {resource_backend_id}: {e}")
            raise

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect current and requested resource limits."""
        backend_limits = {}
        waldur_limits = {}

        # Get current limits from OKD
        if waldur_resource.backend_id:
            backend_limits = self.client.get_resource_limits(waldur_resource.backend_id)

        # Get requested limits from Waldur
        for component_key in self.backend_components:
            waldur_value = getattr(waldur_resource.limits, component_key, 0)
            if waldur_value:
                waldur_limits[component_key] = waldur_value

        return backend_limits, waldur_limits

    def set_resource_limits(
        self,
        resource_backend_id: str,
        limits: dict[str, int],
    ) -> None:
        """Set resource quotas for the OKD project."""
        try:
            self.client.set_resource_limits(resource_backend_id, limits)
            logger.info(f"Set resource limits for {resource_backend_id}")
        except BackendError as e:
            logger.error(f"Failed to set limits for {resource_backend_id}: {e}")
            raise

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage metrics for specified projects."""
        usage_report = {}

        usage_data = self.client.get_usage_report(resource_backend_ids)

        for item in usage_data:
            resource_id = item["resource_id"]
            usage = item.get("usage", {})

            # Ensure all components have values (default to 0)
            normalized_usage = {}
            for component_key in self.backend_components:
                normalized_usage[component_key] = usage.get(component_key, 0)

            usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": normalized_usage}

        return usage_report

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource by setting minimal quotas."""
        try:
            # Set minimal limits to effectively pause resource consumption
            minimal_limits = {
                "cpu": 1,  # 1 core
                "memory": 1,  # 1 GB
                "storage": 1,  # 1 GB
                "pods": 1,  # Allow only 1 pod
            }
            self.client.set_resource_limits(resource_backend_id, minimal_limits)
            logger.info(f"Downscaled OKD project {resource_backend_id}")
            return True
        except BackendError as e:
            logger.error(f"Failed to downscale {resource_backend_id}: {e}")
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause resource by setting quotas to zero."""
        try:
            # Set zero limits to prevent any resource consumption
            zero_limits = {"cpu": 0, "memory": 0, "storage": 0, "pods": 0}
            self.client.set_resource_limits(resource_backend_id, zero_limits)
            logger.info(f"Paused OKD project {resource_backend_id}")
            return True
        except BackendError as e:
            logger.error(f"Failed to pause {resource_backend_id}: {e}")
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource by removing quota restrictions."""
        try:
            # Get the project's original limits from annotations or set defaults
            # For now, we'll set reasonable defaults
            default_limits = {
                "cpu": 10,  # 10 cores
                "memory": 32,  # 32 GB
                "storage": 100,  # 100 GB
                "pods": 50,  # 50 pods
            }
            self.client.set_resource_limits(resource_backend_id, default_limits)
            logger.info(f"Restored OKD project {resource_backend_id}")
            return True
        except BackendError as e:
            logger.error(f"Failed to restore {resource_backend_id}: {e}")
            return False

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get OKD-specific metadata for the project."""
        metadata = {}

        try:
            resource = self.client.get_resource(resource_backend_id)
            if resource:
                metadata["name"] = resource.name
                metadata["organization"] = resource.organization
                metadata["description"] = resource.description

                # Get current quotas
                limits = self.client.get_resource_limits(resource_backend_id)
                metadata["quotas"] = limits

                # Get users with access
                users = self.client.list_resource_users(resource_backend_id)
                metadata["users"] = users

        except Exception as e:
            logger.warning(f"Failed to get metadata for {resource_backend_id}: {e}")

        return metadata

    def create_user_association(
        self, resource_backend_id: str, user: OfferingUser, dryrun: bool = False
    ) -> bool:
        """Grant user access to the OKD project."""
        username = user.username

        if dryrun:
            logger.info(
                f"[DRYRUN] Would create association for {username} in {resource_backend_id}"
            )
            return True

        try:
            # Check if association already exists
            existing = self.client.get_association(username, resource_backend_id)
            if existing:
                logger.info(f"Association already exists for {username} in {resource_backend_id}")
                return True

            # Create new association with appropriate role
            role = self.backend_settings.get("default_role", "edit")
            self.client.create_association(username, resource_backend_id, role)
            logger.info(f"Created association for {username} in {resource_backend_id}")
            return True

        except BackendError as e:
            logger.error(f"Failed to create association for {username}: {e}")
            return False

    def delete_user_association(
        self, resource_backend_id: str, user: OfferingUser, dryrun: bool = False
    ) -> bool:
        """Remove user access from the OKD project."""
        username = user.username

        if dryrun:
            logger.info(
                f"[DRYRUN] Would delete association for {username} from {resource_backend_id}"
            )
            return True

        try:
            # Check if association exists
            existing = self.client.get_association(username, resource_backend_id)
            if not existing:
                logger.info(f"No association exists for {username} in {resource_backend_id}")
                return True

            # Delete the association
            self.client.delete_association(username, resource_backend_id)
            logger.info(f"Deleted association for {username} from {resource_backend_id}")
            return True

        except BackendError as e:
            logger.error(f"Failed to delete association for {username}: {e}")
            return False
