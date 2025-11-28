"""Rancher backend implementation for Waldur Site Agent."""

import pprint
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_rancher.keycloak_client import KeycloakClient
from waldur_site_agent_rancher.rancher_client import RancherClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo


class RancherBackend(backends.BaseBackend):
    """Backend for managing Rancher projects and Keycloak user groups."""

    def __init__(self, rancher_settings: dict, rancher_components: dict[str, dict]) -> None:
        """Initialize Rancher backend with settings and components."""
        super().__init__(rancher_settings, rancher_components)
        self.backend_type = "rancher"

        # Initialize clients
        self.rancher_client = RancherClient(rancher_settings)
        self.client: RancherClient = self.rancher_client  # Base backend expects self.client

        # Initialize Keycloak client if configured
        self.keycloak_client = None
        if rancher_settings.get("keycloak_enabled", False):
            # Get keycloak settings from nested config
            keycloak_settings = rancher_settings.get("keycloak", {})
            try:
                self.keycloak_client = KeycloakClient(keycloak_settings)
                logger.info("Keycloak integration enabled")
            except Exception as e:
                logger.warning(f"Failed to initialize Keycloak client: {e}")
                self.keycloak_client = None

        # Rancher-specific settings
        self.project_prefix = rancher_settings.get("project_prefix", "waldur-")
        self.cluster_id = rancher_settings.get("cluster_id", "")
        self.keycloak_role_name = rancher_settings.get("keycloak_role_name", "workloads-manage")
        self.keycloak_use_user_id = rancher_settings.get(
            "keycloak_use_user_id", True
        )  # Default: lookup by ID

        logger.info("Initialized Rancher backend with cluster ID: %s", self.cluster_id)

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if Rancher cluster and Keycloak are accessible."""
        try:
            # Test Rancher connectivity
            rancher_ok = self.rancher_client.ping()
            if not rancher_ok:
                if raise_exception:
                    msg = "Failed to ping Rancher cluster"
                    raise BackendError(msg)  # noqa: TRY301
                return False

            # Test Keycloak connectivity if enabled
            if self.keycloak_client:
                keycloak_ok = self.keycloak_client.ping()
                if not keycloak_ok:
                    if raise_exception:
                        msg = "Failed to ping Keycloak server"
                        raise BackendError(msg)  # noqa: TRY301
                    return False

            return True

        except Exception as e:
            if raise_exception:
                raise
            logger.error(f"Failed to ping Rancher/Keycloak: {e}")
            return False

    def diagnostics(self) -> bool:
        """Run diagnostics for Rancher and Keycloak connectivity."""
        format_string = "{:<30} = {:<10}"

        logger.info("=" * 60)
        logger.info("Rancher Backend Diagnostics")
        logger.info("=" * 60)

        logger.info(
            format_string.format("Rancher API URL", self.backend_settings.get("api_url", "Not set"))
        )
        logger.info(format_string.format("Cluster ID", self.cluster_id))
        logger.info(format_string.format("Project prefix", self.project_prefix))
        logger.info(
            format_string.format(
                "SSL verification", str(self.backend_settings.get("verify_cert", True))
            )
        )

        if self.keycloak_client:
            logger.info(format_string.format("Keycloak enabled", "Yes"))
            logger.info(
                format_string.format(
                    "Keycloak URL",
                    self.backend_settings.get("keycloak", {}).get("server_url", "Not set"),
                )
            )
        else:
            logger.info(format_string.format("Keycloak enabled", "No"))

        logger.info("")

        logger.info("Rancher components configuration:")
        logger.info(pprint.pformat(self.backend_components))
        logger.info("")

        try:
            self.ping(raise_exception=True)
            logger.info("✓ Rancher cluster connection successful")

            # Try to list existing projects
            projects = self.rancher_client.list_projects()
            logger.info(f"✓ Found {len(projects)} managed projects")

            if self.keycloak_client:
                logger.info("✓ Keycloak connection successful")

            return True

        except BackendError as err:
            logger.error("✗ Unable to connect to Rancher/Keycloak: %s", err)
            return False
        except Exception as e:
            logger.error("✗ Unexpected error during diagnostics: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of available resource components."""
        # Standard Rancher/Kubernetes resource components
        return ["cpu", "memory", "storage", "pods"]

    def _get_rancher_project_name(self, waldur_resource: WaldurResource) -> str:
        """Generate Rancher project name from Waldur resource."""
        # Use Waldur resource slug for the Rancher project name (more specific than project slug)
        resource_slug = waldur_resource.slug or f"resource-{str(waldur_resource.uuid)[:8]}"
        return f"{self.project_prefix}{resource_slug}"

    def _get_cluster_uuid_hex(self, waldur_resource: WaldurResource) -> str:  # noqa: ARG002
        """Get cluster UUID in hex format for parent group."""
        # This should be the Rancher cluster UUID, but for now use a placeholder
        # In practice, this would be configured or derived from the cluster_id
        return self.cluster_id.replace(":", "").replace("-", "")[:8]

    def _get_project_uuid_hex(self, waldur_resource: WaldurResource) -> str:
        """Get Waldur project UUID in hex format."""
        # Use the Waldur project UUID (this should be available in the resource)
        # For now, we'll use the resource UUID as a placeholder
        if waldur_resource.project_uuid:
            return str(waldur_resource.project_uuid).replace("-", "")
        return str(waldur_resource.uuid).replace("-", "")

    def _get_keycloak_parent_group_name(self, waldur_resource: WaldurResource) -> str:
        """Generate parent Keycloak group name (cluster level)."""
        cluster_uuid_hex = self._get_cluster_uuid_hex(waldur_resource)
        return f"c_{cluster_uuid_hex}"

    def _get_keycloak_child_group_name(self, waldur_resource: WaldurResource) -> str:
        """Generate child Keycloak group name (project level with role)."""
        project_slug = waldur_resource.project_slug or "unknown-project"
        return f"project_{project_slug}_{self.keycloak_role_name}"

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
                "Cannot create Rancher projects with invalid slug values."
            )
            raise BackendError(msg)

        del user_context  # Not used in Rancher backend

    def _create_rancher_project(
        self,
        waldur_resource: WaldurResource,
    ) -> tuple[str, str]:
        """Create a Rancher project for the Waldur resource."""
        project_name = self._get_rancher_project_name(waldur_resource)

        try:
            # Check if project already exists
            existing_projects = self.rancher_client.list_projects()
            for project in existing_projects:
                if project.name == project_name:
                    logger.info(f"Rancher project {project_name} already exists")
                    return project.backend_id

            # Create new project with descriptive information
            customer_info = waldur_resource.customer_name or waldur_resource.customer_slug
            project_info = waldur_resource.project_name or waldur_resource.project_slug
            description = (
                f"{waldur_resource.name} (Customer: {customer_info}, Project: {project_info})"
            )
            project_id = self.rancher_client.create_project(
                name=project_name,
                description=description,
                organization=waldur_resource.customer_slug,
                project_slug=waldur_resource.project_slug,
            )

            return project_id, project_name

        except Exception as e:
            logger.error("Failed to create Rancher project %s: %s", project_name, e)
            raise BackendError(f"Failed to create Rancher project: {e}") from e

    def _create_keycloak_groups(
        self, waldur_resource: WaldurResource
    ) -> tuple[Optional[str], Optional[str]]:
        """Create hierarchical Keycloak groups and return (parent_id, child_id)."""
        if not self.keycloak_client:
            logger.debug("Keycloak not configured, skipping group creation")
            return None, None

        parent_group_name = self._get_keycloak_parent_group_name(waldur_resource)
        child_group_name = self._get_keycloak_child_group_name(waldur_resource)

        try:
            # Create or get parent group (cluster level)
            parent_group = self.keycloak_client.get_group_by_name(parent_group_name)
            if parent_group:
                parent_group_id = parent_group["id"]
                logger.info(f"Using existing parent group: {parent_group_name}")
            else:
                parent_description = f"Cluster access group for {self.cluster_id}"
                parent_group_id = self.keycloak_client.create_group(
                    parent_group_name, parent_description
                )
                logger.info(f"Created parent group: {parent_group_name}")

            # Create child group (project + role level)
            child_group = self.keycloak_client.get_group_by_name(child_group_name)
            if child_group:
                child_group_id = child_group["id"]
                logger.info(f"Using existing child group: {child_group_name}")
            else:
                child_description = (
                    f"Project {waldur_resource.project_slug} members "
                    f"with role {self.keycloak_role_name}"
                )
                child_group_id = self.keycloak_client.create_group(
                    child_group_name, child_description, parent_group_id
                )
                logger.info(f"Created child group: {child_group_name}")

            return parent_group_id, child_group_id

        except Exception as e:
            logger.error(f"Failed to create Keycloak groups: {e}")
            # Don't fail the entire operation if Keycloak fails
            logger.warning("Continuing without Keycloak group creation")
            return None, None

    def _bind_keycloak_group_to_rancher_project(
        self, project_id: str, group_name: str, role: str
    ) -> None:
        """Bind a Keycloak group to a Rancher project role."""
        if not self.keycloak_client:
            return

        try:
            # Use the waldur-mastermind format for group reference
            group_reference = f"keycloakoidc_group://{group_name}"

            # Check if binding already exists
            existing_bindings = self.rancher_client.get_project_group_role(
                group_reference, project_id, role
            )

            if existing_bindings:
                logger.info(f"Group binding already exists: {group_name} → {project_id}")
                return

            # Create the binding
            self.rancher_client.create_project_group_role(group_reference, project_id, role)
            logger.info(f"Created group binding: {group_name} → {project_id} with role {role}")

        except Exception as e:
            logger.warning(f"Failed to bind group {group_name} to project {project_id}: {e}")

    def _cleanup_empty_keycloak_groups(self, waldur_resource: WaldurResource) -> None:
        """Clean up empty Keycloak groups and their Rancher bindings."""
        if not self.keycloak_client:
            return

        try:
            child_group_name = self._get_keycloak_child_group_name(waldur_resource)
            parent_group_name = self._get_keycloak_parent_group_name(waldur_resource)

            # Check child group
            child_group = self.keycloak_client.get_group_by_name(child_group_name)
            if child_group:
                members = self.keycloak_client.get_group_members(child_group["id"])
                if len(members) == 0:
                    logger.info(f"Removing empty Keycloak group: {child_group_name}")

                    # Remove Rancher binding first
                    try:
                        project_id = waldur_resource.backend_id
                        group_reference = f"keycloakoidc_group://{child_group_name}"
                        self.rancher_client.delete_project_group_role(
                            group_reference, project_id, self.keycloak_role_name
                        )
                        logger.info(f"Removed Rancher binding for group {child_group_name}")
                    except Exception as e:
                        logger.warning(f"Failed to remove Rancher binding: {e}")

                    # Delete the empty group
                    self.keycloak_client.delete_group(child_group["id"])
                    logger.info(f"Deleted empty group {child_group_name}")

                    # Check if parent group should be removed too
                    parent_group = self.keycloak_client.get_group_by_name(parent_group_name)
                    if parent_group:
                        parent_subgroups = parent_group.get("subGroups", [])
                        if len(parent_subgroups) == 0:
                            self.keycloak_client.delete_group(parent_group["id"])
                            logger.info(f"Deleted empty parent group {parent_group_name}")

        except Exception as e:
            logger.warning(f"Failed to cleanup empty groups: {e}")

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create Rancher project and Keycloak groups for the Waldur project."""
        del resource_backend_id
        self._pre_create_resource(waldur_resource, user_context)

        # Create Rancher project for the Waldur project
        project_id, project_name = self._create_rancher_project(waldur_resource)

        # Create Keycloak groups (parent cluster group + child project/role group)
        _, child_group_id = self._create_keycloak_groups(waldur_resource)

        # Bind the child Keycloak group to the Rancher project role
        if child_group_id:
            child_group_name = self._get_keycloak_child_group_name(waldur_resource)
            self._bind_keycloak_group_to_rancher_project(
                project_id, child_group_name, self.keycloak_role_name
            )

        # Store the project ID as backend_id
        waldur_resource.backend_id = project_id

        # Collect and set limits (only CPU and memory as quotas)
        _, waldur_limits = self._collect_resource_limits(waldur_resource)
        quota_components = self._filter_quota_components(waldur_limits)
        if quota_components:
            try:
                # Create a namespace
                self.client.create_namespace(project_id, project_name)
                # Setup namespace resource quotas
                self.client.set_namespace_custom_resource_quotas(project_name, quota_components)
            except Exception as e:
                logger.warning("Failed to set project quotas: %s", e)

        return BackendResourceInfo(
            backend_id=project_id,
            parent_id="",  # No parent in Rancher
            limits=waldur_limits,
        )

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:  # noqa: ARG002
        """Delete Rancher project and Keycloak group associated with the resource."""
        project_id = waldur_resource.backend_id
        if not project_id or not project_id.strip():
            logger.warning(f"Resource {waldur_resource.uuid} has no backend_id, skipping deletion")
            return

        try:
            # Delete Rancher project
            self.rancher_client.delete_project(project_id)
            logger.info(f"Deleted Rancher project: {project_id}")

            # Delete Keycloak group
            if self.keycloak_client:
                try:
                    group_name = self._get_keycloak_child_group_name(waldur_resource)
                    group = self.keycloak_client.get_group_by_name(group_name)
                    if group:
                        self.keycloak_client.delete_group(group["id"])
                        logger.info(f"Deleted Keycloak group: {group_name}")
                    else:
                        logger.info(f"Keycloak group {group_name} not found, skipping deletion")
                except Exception as e:
                    logger.warning(f"Failed to delete Keycloak group: {e}")

        except Exception as e:
            logger.error(f"Failed to delete project {project_id}: {e}")
            raise BackendError(f"Failed to delete project: {e}") from e

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect current and requested resource limits."""
        return {}, waldur_resource.limits.to_dict()

    def _filter_quota_components(self, limits: dict[str, int]) -> dict[str, int]:
        """Filter to only include components that should be set as Rancher project quotas."""
        quota_components = {}

        # Only CPU and memory should be set as Rancher project quotas
        # Storage and pods are reported from actual allocation, not enforced as limits
        quota_component_types = {"cpu", "ram", "storage", "gpu"}  # Types that get quotas

        for component_key, component_config in self.backend_components.items():
            component_type = component_config.get("type", "")
            if component_type in quota_component_types and component_key in limits:
                quota_components[component_key] = limits[component_key]
                logger.debug(f"Including quota component {component_key}: {limits[component_key]}")
            elif component_key in limits:
                logger.debug(
                    f"Skipping non-quota component {component_key}: {limits[component_key]}"
                )

        return quota_components

    def set_resource_limits(
        self,
        resource_backend_id: str,
        limits: dict[str, int],
    ) -> None:
        """Set resource quotas for the Rancher project."""
        try:
            # Setup namespace resource quotas
            namespaces = self.client.get_project_namespaces(resource_backend_id)
            if len(namespaces) > 0 and namespaces[0]:
                namespace = namespaces[0]
                self.client.set_namespace_custom_resource_quotas(namespace, limits)
        except Exception as e:
            logger.error(f"Failed to set limits for {resource_backend_id}: {e}")
            raise BackendError(f"Failed to set limits: {e}") from e

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage metrics for specified projects."""
        usage_report = {}

        for resource_id in resource_backend_ids:
            try:
                usage = self.rancher_client.get_project_usage(resource_id)

                # Ensure all components have values (default to 0)
                normalized_usage = {}
                for component_key in self.backend_components:
                    normalized_usage[component_key] = usage.get(component_key, 0)

                usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": normalized_usage}

            except Exception as e:
                logger.warning(f"Failed to get usage for project {resource_id}: {e}")
                # Return zero usage if we can't get metrics
                normalized_usage = dict.fromkeys(self.backend_components, 0)
                usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": normalized_usage}

        return usage_report

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get Rancher-specific metadata for the project."""
        metadata = {}

        try:
            project = self.rancher_client.get_project(resource_backend_id)
            if project:
                metadata["name"] = project.name
                metadata["organization"] = project.organization
                metadata["description"] = project.description

                # Get current quotas
                quotas = self.rancher_client.get_project_quotas(resource_backend_id)
                metadata["quotas"] = quotas

                # Get users with access
                users = self.rancher_client.list_project_users(resource_backend_id)
                metadata["users"] = users

        except Exception as e:
            logger.warning(f"Failed to get metadata for {resource_backend_id}: {e}")

        return metadata

    def _list_keycloak_group_users(self, waldur_resource: WaldurResource) -> list[str]:
        """List users currently in the Keycloak group for this resource."""
        if not self.keycloak_client:
            return []

        try:
            child_group_name = self._get_keycloak_child_group_name(waldur_resource)
            group = self.keycloak_client.get_group_by_name(child_group_name)

            if group:
                members = self.keycloak_client.get_group_members(group["id"])
                # Return the user identifiers for comparison with Waldur usernames
                usernames = []
                for member in members:
                    if self.keycloak_use_user_id:
                        # Use Keycloak user ID (which should match Waldur username)
                        user_id = member.get("id", "")
                        if user_id:
                            usernames.append(user_id)
                    else:
                        # Use Keycloak username
                        username = member.get("username", "")
                        if username:
                            usernames.append(username)
                logger.debug(f"Found {len(usernames)} users in group {child_group_name}")
                return usernames
            logger.debug(f"Group {child_group_name} not found")
            return []

        except Exception as e:
            logger.warning(f"Failed to list group users: {e}")
            return []

    def _pull_backend_resource(
        self, resource_backend_id: str, waldur_resource: WaldurResource = None
    ) -> Optional[BackendResourceInfo]:
        """Pull resource data including Keycloak group users for membership sync."""
        logger.info("Pulling resource %s", resource_backend_id)

        # Get basic resource info
        resource_info = self.client.get_resource(resource_backend_id)
        if resource_info is None:
            logger.warning("There is no resource with ID %s in the backend", resource_backend_id)
            return None

        # Get users from Keycloak group (if we have the resource context)
        users = []
        if waldur_resource and self.keycloak_client:
            users = self._list_keycloak_group_users(waldur_resource)
        else:
            # Fallback to client method (returns empty list for Rancher)
            users = self.client.list_resource_users(resource_backend_id)

        # Get usage report
        report = self._get_usage_report([resource_backend_id])
        usage = report.get(resource_backend_id)
        if usage is None:
            empty_usage = dict.fromkeys(self.backend_components, 0)
            usage = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return BackendResourceInfo(backend_id=resource_backend_id, users=users, usage=usage)

    def pull_resource(self, waldur_resource: WaldurResource) -> Optional[BackendResourceInfo]:
        """Pull resource with Keycloak group user information."""
        try:
            backend_id = waldur_resource.backend_id
            # Use our enhanced version that includes Keycloak group users
            return self._pull_backend_resource(backend_id, waldur_resource)
        except Exception as e:
            logger.exception("Error while pulling resource [%s]: %s", backend_id, e)
            return None

    def add_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Add user to Keycloak group (OIDC handles Rancher project access)."""
        resource_backend_id = waldur_resource.backend_id

        logger.info(f"Adding user {username} to resource {resource_backend_id}")

        try:
            # Only manage Keycloak group membership - OIDC handles Rancher access
            if self.keycloak_client:
                try:
                    # Now we have direct access to the project information!
                    child_group_name = self._get_keycloak_child_group_name(waldur_resource)

                    # Find the user in Keycloak (by ID or username based on setting)
                    keycloak_user = self.keycloak_client.find_user(
                        username, self.keycloak_use_user_id
                    )
                    if keycloak_user:
                        # Find or create the project-role group
                        group = self.keycloak_client.get_group_by_name(child_group_name)
                        if not group:
                            logger.info(
                                f"Creating missing Keycloak groups for resource "
                                f"{resource_backend_id}"
                            )
                            # Create the missing group structure
                            _, child_id = self._create_keycloak_groups(waldur_resource)
                            if child_id:
                                # Bind the new group to the Rancher project
                                self._bind_keycloak_group_to_rancher_project(
                                    resource_backend_id, child_group_name, self.keycloak_role_name
                                )
                                group = {"id": child_id}
                                logger.info(f"Created and bound group {child_group_name}")
                            else:
                                logger.error(f"Failed to create group {child_group_name}")
                                return False

                        # Add user to group
                        self.keycloak_client.add_user_to_group(keycloak_user["id"], group["id"])
                        logger.info(f"Added {username} to Keycloak group {child_group_name}")
                        logger.info("OIDC will automatically grant Rancher project access")
                    else:
                        logger.warning(f"User {username} not found in Keycloak")

                except Exception as e:
                    logger.warning(f"Failed to add user to Keycloak group: {e}")
            else:
                logger.info(f"Keycloak disabled - no group management for {username}")

            return True

        except Exception as e:
            logger.error(f"Failed to add user {username}: {e}")
            return False

    def remove_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Remove user from Keycloak group (OIDC handles Rancher project access removal)."""
        resource_backend_id = waldur_resource.backend_id

        logger.info(f"Removing user {username} from resource {resource_backend_id}")

        try:
            # Only manage Keycloak group membership - OIDC handles Rancher access removal
            if self.keycloak_client:
                try:
                    # Direct access to project information from the resource!
                    child_group_name = self._get_keycloak_child_group_name(waldur_resource)

                    # Find the user in Keycloak (by ID or username based on setting)
                    keycloak_user = self.keycloak_client.find_user(
                        username, self.keycloak_use_user_id
                    )
                    if keycloak_user:
                        # Find group and remove user
                        group = self.keycloak_client.get_group_by_name(child_group_name)
                        if group:
                            self.keycloak_client.remove_user_from_group(
                                keycloak_user["id"], group["id"]
                            )
                            logger.info(
                                f"Removed {username} from Keycloak group {child_group_name}"
                            )

                            # Check if group is now empty and clean up if needed
                            try:
                                self._cleanup_empty_keycloak_groups(waldur_resource)
                            except Exception as cleanup_error:
                                logger.warning(f"Failed to cleanup empty groups: {cleanup_error}")
                        else:
                            logger.warning(f"Keycloak group {child_group_name} not found")
                    else:
                        logger.warning(f"User {username} not found in Keycloak")

                except Exception as e:
                    logger.warning(f"Failed to remove user from Keycloak group: {e}")

            return True

        except Exception as e:
            logger.error(f"Failed to delete association for {username}: {e}")
            return False

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource by setting minimal quotas."""
        try:
            # Set minimal limits to effectively downscale resource consumption
            minimal_limits = {
                "cpu": 1,  # 1 core
                "memory": 1,  # 1 GB
                "storage": 1,  # 1 GB
            }
            # Setup namespace resource quotas
            namespaces = self.client.get_project_namespaces(resource_backend_id)
            if len(namespaces) > 0 and namespaces[0]:
                namespace = namespaces[0]
                self.client.set_namespace_custom_resource_quotas(namespace, minimal_limits)
                logger.info(
                    "Downscaled Rancher project %s namespace %s: %s",
                    resource_backend_id,
                    namespace,
                    minimal_limits,
                )
            return True
        except Exception as e:
            logger.error("Failed to downscale %s: %s", resource_backend_id, e)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause resource by setting quotas to zero."""
        try:
            # Set zero limits to prevent any resource consumption
            zero_limits = {"cpu": 0, "memory": 0, "storage": 0, "pods": 0}
            # Setup namespace resource quotas
            namespaces = self.client.get_project_namespaces(resource_backend_id)
            if len(namespaces) > 0 and namespaces[0]:
                namespace = namespaces[0]
                self.client.set_namespace_custom_resource_quotas(namespace, zero_limits)
                logger.info("Paused Rancher project %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to pause %s: %s", resource_backend_id, e)
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource by removing quota restrictions."""
        try:
            # Get the project's original limits from Rancher or set defaults
            # For now, we'll set reasonable defaults
            default_limits = {
                "cpu": 10,  # 10 cores
                "memory": 32,  # 32 GB
                "storage": 100,  # 100 GB
            }
            # Setup namespace resource quotas
            namespaces = self.client.get_project_namespaces(resource_backend_id)
            if len(namespaces) > 0 and namespaces[0]:
                namespace = namespaces[0]
                self.client.set_namespace_custom_resource_quotas(namespace, default_limits)
                logger.info("Restored Rancher project %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to restore %s: %s", resource_backend_id, e)
            return False
