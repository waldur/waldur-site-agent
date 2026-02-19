"""K8s UT ManagedNamespace backend for Waldur Site Agent."""

import pprint
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_keycloak_client import KeycloakClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent_k8s_ut_namespace.k8s_client import K8sUtNamespaceClient

# Default Waldur role -> namespace access level mapping
DEFAULT_ROLE_MAPPING = {
    "manager": "admin",
    "admin": "admin",
    "member": "readwrite",
}
NS_ROLES = ("admin", "readwrite", "readonly")

# Default Waldur component type -> ManagedNamespace quota field mapping
DEFAULT_COMPONENT_QUOTA_MAPPING = {
    "cpu": "cpu",
    "ram": "memory",
    "storage": "storage",
    "gpu": "gpu",
}

# Namespace role -> ManagedNamespace CR spec field for groups/users
NS_ROLE_TO_CR_GROUP_FIELD = {
    "admin": "adminGroups",
    "readwrite": "rwGroups",
    "readonly": "roGroups",
}
NS_ROLE_TO_CR_USER_FIELD = {
    "admin": "adminUsers",
    "readwrite": "rwUsers",
    "readonly": "roUsers",
}


class K8sUtNamespaceBackend(backends.BaseBackend):
    """Backend for managing Kubernetes ManagedNamespace CRs and Keycloak RBAC groups."""

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize K8s UT Namespace backend."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "k8s-ut-namespace"

        # Initialize K8s client
        self.k8s_client = K8sUtNamespaceClient(backend_settings)

        # Initialize Keycloak client if configured
        self.keycloak_client: Optional[KeycloakClient] = None
        if backend_settings.get("keycloak_enabled", False):
            keycloak_settings = backend_settings.get("keycloak", {})
            try:
                self.keycloak_client = KeycloakClient(keycloak_settings)
                logger.info("Keycloak integration enabled for K8s UT namespace backend")
            except Exception as e:
                logger.warning(f"Failed to initialize Keycloak client: {e}")
                self.keycloak_client = None

        self.namespace_prefix = backend_settings.get("namespace_prefix", "waldur-")
        self.cr_namespace = backend_settings.get("cr_namespace", "waldur-system")
        self.default_role = backend_settings.get("default_role", "readwrite")
        self.keycloak_use_user_id = backend_settings.get("keycloak_use_user_id", True)

        # Configurable mappings with sensible defaults
        self.role_mapping: dict[str, str] = {
            **DEFAULT_ROLE_MAPPING,
            **backend_settings.get("role_mapping", {}),
        }
        self.component_quota_mapping: dict[str, str] = {
            **DEFAULT_COMPONENT_QUOTA_MAPPING,
            **backend_settings.get("component_quota_mapping", {}),
        }

        logger.info(
            "Initialized K8s UT namespace backend (CR namespace: %s, prefix: %s)",
            self.cr_namespace,
            self.namespace_prefix,
        )

    # ── Keycloak group naming ──────────────────────────────────────────────

    def _get_keycloak_group_name(self, resource_slug: str, ns_role: str) -> str:
        """Generate Keycloak group name for a namespace role."""
        return f"ns_{resource_slug}_{ns_role}"

    def _get_keycloak_group_names(self, resource_slug: str) -> dict[str, str]:
        """Get all 3 Keycloak group names for a resource."""
        return {
            role: self._get_keycloak_group_name(resource_slug, role)
            for role in NS_ROLES
        }

    # ── Keycloak group management ──────────────────────────────────────────

    def _create_keycloak_groups(self, resource_slug: str) -> dict[str, str]:
        """Create 3 Keycloak groups for namespace RBAC and return {role: group_id}."""
        if not self.keycloak_client:
            return {}

        group_ids = {}
        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            try:
                existing = self.keycloak_client.get_group_by_name(group_name)
                if existing:
                    group_ids[role] = existing["id"]
                    logger.info("Using existing Keycloak group: %s", group_name)
                else:
                    description = f"Namespace {resource_slug} {role} access"
                    group_id = self.keycloak_client.create_group(group_name, description)
                    group_ids[role] = group_id
                    logger.info("Created Keycloak group: %s", group_name)
            except Exception as e:
                logger.error("Failed to create Keycloak group %s: %s", group_name, e)
                raise BackendError(
                    f"Failed to create Keycloak group {group_name}: {e}"
                ) from e
        return group_ids

    def _delete_keycloak_groups(self, resource_slug: str) -> None:
        """Delete all 3 Keycloak groups for a resource."""
        if not self.keycloak_client:
            return

        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            try:
                group = self.keycloak_client.get_group_by_name(group_name)
                if group:
                    self.keycloak_client.delete_group(group["id"])
                    logger.info("Deleted Keycloak group: %s", group_name)
            except Exception as e:
                logger.warning("Failed to delete Keycloak group %s: %s", group_name, e)

    def _get_keycloak_group_ids(self, resource_slug: str) -> dict[str, str]:
        """Look up existing group IDs for all 3 roles. Returns {role: group_id}."""
        if not self.keycloak_client:
            return {}

        group_ids = {}
        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            group = self.keycloak_client.get_group_by_name(group_name)
            if group:
                group_ids[role] = group["id"]
        return group_ids

    # ── Quota conversion ───────────────────────────────────────────────────

    @staticmethod
    def _validate_limits(limits: dict[str, int]) -> None:
        """Raise BackendError if any limit value is negative."""
        negative = {k: v for k, v in limits.items() if v < 0}
        if negative:
            raise BackendError(
                f"Negative resource limits are not allowed: {negative}"
            )

    def _waldur_limits_to_quota(self, limits: dict[str, int]) -> dict[str, str]:
        """Convert Waldur component limits to ManagedNamespace quota spec.

        Values are formatted as K8s resource quantities.
        """
        self._validate_limits(limits)
        quota = {}
        for component_key, value in limits.items():
            component_config = self.backend_components.get(component_key, {})
            component_type = component_config.get("type", component_key)
            quota_field = self.component_quota_mapping.get(component_type)
            if quota_field:
                if component_type in {"ram", "storage"}:
                    quota[quota_field] = f"{value}Gi"
                else:
                    quota[quota_field] = str(value)
        return quota

    # ── BaseBackend abstract methods ───────────────────────────────────────

    def ping(self, raise_exception: bool = False) -> bool:
        """Check K8s cluster and Keycloak connectivity."""
        try:
            k8s_ok = self.k8s_client.ping()
            if not k8s_ok:
                if raise_exception:
                    msg = "Failed to ping Kubernetes cluster"
                    raise BackendError(msg)  # noqa: TRY301
                return False

            if self.keycloak_client:
                kc_ok = self.keycloak_client.ping()
                if not kc_ok:
                    if raise_exception:
                        msg = "Failed to ping Keycloak server"
                        raise BackendError(msg)  # noqa: TRY301
                    return False

            return True
        except BackendError:
            if raise_exception:
                raise
            return False
        except Exception as e:
            if raise_exception:
                raise
            logger.error("Failed to ping K8s/Keycloak: %s", e)
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about the backend."""
        fmt = "{:<30} = {:<10}"

        logger.info("=" * 60)
        logger.info("K8s UT Namespace Backend Diagnostics")
        logger.info("=" * 60)

        logger.info(fmt.format("CR namespace", self.cr_namespace))
        logger.info(fmt.format("Namespace prefix", self.namespace_prefix))
        logger.info(
            fmt.format("Keycloak enabled", "Yes" if self.keycloak_client else "No")
        )

        logger.info("")
        logger.info("Backend components configuration:")
        logger.info(pprint.pformat(self.backend_components))
        logger.info("")

        try:
            self.ping(raise_exception=True)
            logger.info("K8s cluster connection successful")

            namespaces = self.k8s_client.list_managed_namespaces()
            logger.info("Found %d managed namespaces", len(namespaces))

            if self.keycloak_client:
                logger.info("Keycloak connection successful")

            return True
        except BackendError as err:
            logger.error("Unable to connect to K8s/Keycloak: %s", err)
            return False
        except Exception as e:
            logger.error("Unexpected error during diagnostics: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of available resource components."""
        return list(self.component_quota_mapping.keys())

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect and convert resource limits."""
        waldur_limits = waldur_resource.limits.to_dict()
        # For this backend, backend limits == Waldur limits (unit_factor = 1)
        backend_limits = dict(waldur_limits)
        return backend_limits, waldur_limits

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Return quota limits from ManagedNamespace CR spec as usage."""
        usage_report = {}
        for resource_id in resource_backend_ids:
            try:
                cr = self.k8s_client.get_managed_namespace(resource_id)
                if cr:
                    quota = cr.get("spec", {}).get("quota", {})
                    # Convert K8s quantities back to numeric values
                    usage = {}
                    for component_key, component_config in self.backend_components.items():
                        component_type = component_config.get("type", component_key)
                        quota_field = self.component_quota_mapping.get(component_type)
                        if quota_field and quota_field in quota:
                            raw = quota[quota_field]
                            usage[component_key] = self._parse_k8s_quantity(raw)
                        else:
                            usage[component_key] = 0
                    usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": usage}
                else:
                    empty = dict.fromkeys(self.backend_components, 0)
                    usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": empty}
            except Exception as e:
                logger.warning("Failed to get usage for %s: %s", resource_id, e)
                empty = dict.fromkeys(self.backend_components, 0)
                usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": empty}
        return usage_report

    @staticmethod
    def _parse_k8s_quantity(value: str) -> int:
        """Parse a K8s resource quantity string to an integer."""
        value = str(value)
        if value.endswith("Gi"):
            return int(value[:-2])
        if value.endswith("Mi"):
            return int(value[:-2]) // 1024
        if value.endswith("m"):
            return int(value[:-1]) // 1000
        try:
            return int(value)
        except ValueError:
            return 0

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get K8s-specific metadata for the resource."""
        metadata = {}
        try:
            cr = self.k8s_client.get_managed_namespace(resource_backend_id)
            if cr:
                metadata["name"] = cr.get("metadata", {}).get("name", "")
                metadata["quota"] = cr.get("spec", {}).get("quota", {})
                metadata["status"] = cr.get("status", {})
        except Exception as e:
            logger.warning("Failed to get metadata for %s: %s", resource_backend_id, e)
        return metadata

    # ── Resource lifecycle ─────────────────────────────────────────────────

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Validate resource before creation."""
        del user_context
        if not waldur_resource.slug:
            raise BackendError(
                f"Resource {waldur_resource.uuid} has no slug, "
                "cannot create ManagedNamespace"
            )

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create ManagedNamespace CR and Keycloak groups."""
        del resource_backend_id  # We generate our own name

        self._pre_create_resource(waldur_resource, user_context)

        resource_slug = waldur_resource.slug
        ns_name = f"{self.namespace_prefix}{resource_slug}"

        logger.info(
            "Creating K8s UT namespace %s for resource %s",
            ns_name,
            waldur_resource.uuid.hex,
        )

        # 1. Create Keycloak groups (3 roles)
        self._create_keycloak_groups(resource_slug)

        # 2. Build ManagedNamespace spec
        _, waldur_limits = self._collect_resource_limits(waldur_resource)
        quota = self._waldur_limits_to_quota(waldur_limits)

        spec: dict = {
            "name": ns_name,
            "quota": quota,
        }

        # Add group references per role (CRD uses adminGroups/rwGroups/roGroups)
        if self.keycloak_client:
            for role in NS_ROLES:
                group_name = self._get_keycloak_group_name(resource_slug, role)
                cr_field = NS_ROLE_TO_CR_GROUP_FIELD[role]
                spec[cr_field] = [group_name]

        # Add owner info as object (CRD expects {orgID, projectID})
        owner: dict[str, str] = {}
        if waldur_resource.customer_uuid:
            owner["orgID"] = str(waldur_resource.customer_uuid)
        if waldur_resource.project_uuid:
            owner["projectID"] = str(waldur_resource.project_uuid)
        if owner:
            spec["owner"] = owner

        # 3. Create ManagedNamespace CR
        try:
            self.k8s_client.create_managed_namespace(ns_name, spec)
        except BackendError:
            # Cleanup Keycloak groups on failure
            logger.error("Failed to create CR, cleaning up Keycloak groups")
            self._delete_keycloak_groups(resource_slug)
            raise

        return BackendResourceInfo(
            backend_id=ns_name,
            limits=waldur_limits,
        )

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> None:
        """Delete ManagedNamespace CR and Keycloak groups."""
        del kwargs
        ns_name = waldur_resource.backend_id
        if not ns_name or not ns_name.strip():
            logger.warning(
                "Resource %s has no backend_id, skipping deletion",
                waldur_resource.uuid,
            )
            return

        resource_slug = waldur_resource.slug or ns_name.removeprefix(self.namespace_prefix)

        try:
            self.k8s_client.delete_managed_namespace(ns_name)
            logger.info("Deleted ManagedNamespace CR: %s", ns_name)
        except Exception as e:
            logger.error("Failed to delete ManagedNamespace %s: %s", ns_name, e)
            raise BackendError(f"Failed to delete ManagedNamespace: {e}") from e

        self._delete_keycloak_groups(resource_slug)

    def set_resource_limits(
        self,
        resource_backend_id: str,
        limits: dict[str, int],
    ) -> None:
        """Patch ManagedNamespace CR spec.quota."""
        quota = self._waldur_limits_to_quota(limits)
        try:
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": quota}},
            )
        except Exception as e:
            logger.error("Failed to set limits for %s: %s", resource_backend_id, e)
            raise BackendError(f"Failed to set limits: {e}") from e

    # ── User management ────────────────────────────────────────────────────

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add users to correct Keycloak groups based on their Waldur roles.

        Uses `user_roles` kwarg to determine which group each user belongs to.
        Also reconciles role changes for all users in user_roles.
        """
        user_roles: dict[str, str] = kwargs.get("user_roles", {})

        if not self.keycloak_client:
            logger.info("Keycloak not configured, skipping user management")
            return user_ids

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)

        if not group_ids:
            logger.warning("No Keycloak groups found for resource %s", resource_slug)
            return set()

        added_users = set()

        # Reconcile ALL users that have roles (handles both new users and role changes)
        users_to_reconcile = {
            username: role
            for username, role in user_roles.items()
            if username in user_ids or username  # include all users with roles
        }

        for username, waldur_role in users_to_reconcile.items():
            target_ns_role = self.role_mapping.get(waldur_role, self.default_role)
            target_group_id = group_ids.get(target_ns_role)

            if not target_group_id:
                logger.warning(
                    "No group found for role %s, skipping user %s",
                    target_ns_role,
                    username,
                )
                continue

            kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
            if not kc_user:
                logger.warning("User %s not found in Keycloak", username)
                continue

            kc_user_id = kc_user["id"]

            # Remove from wrong groups
            for role, gid in group_ids.items():
                if (
                    role != target_ns_role
                    and self.keycloak_client.is_user_in_group(kc_user_id, gid)
                ):
                    try:
                        self.keycloak_client.remove_user_from_group(kc_user_id, gid)
                        logger.info(
                            "Removed %s from group %s (role change)",
                            username,
                            self._get_keycloak_group_name(resource_slug, role),
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to remove %s from group: %s", username, e
                        )

            # Add to correct group
            if not self.keycloak_client.is_user_in_group(kc_user_id, target_group_id):
                try:
                    self.keycloak_client.add_user_to_group(kc_user_id, target_group_id)
                    logger.info(
                        "Added %s to group %s (%s)",
                        username,
                        self._get_keycloak_group_name(resource_slug, target_ns_role),
                        target_ns_role,
                    )
                except Exception as e:
                    logger.warning("Failed to add %s to group: %s", username, e)
                    continue

            if username in user_ids:
                added_users.add(username)

        return added_users

    def add_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Add a single user to the default role group."""
        if not self.keycloak_client:
            return True

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)
        target_group_id = group_ids.get(self.default_role)

        if not target_group_id:
            logger.warning("No group found for default role %s", self.default_role)
            return False

        kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
        if not kc_user:
            logger.warning("User %s not found in Keycloak", username)
            return False

        try:
            self.keycloak_client.add_user_to_group(kc_user["id"], target_group_id)
            return True
        except Exception as e:
            logger.warning("Failed to add user %s to group: %s", username, e)
            return False

    def remove_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Remove user from ALL 3 Keycloak groups."""
        if not self.keycloak_client:
            return True

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)

        kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
        if not kc_user:
            logger.warning("User %s not found in Keycloak", username)
            return True  # User not in Keycloak, nothing to remove

        kc_user_id = kc_user["id"]
        for role, gid in group_ids.items():
            try:
                if self.keycloak_client.is_user_in_group(kc_user_id, gid):
                    self.keycloak_client.remove_user_from_group(kc_user_id, gid)
                    logger.info(
                        "Removed %s from group %s",
                        username,
                        self._get_keycloak_group_name(resource_slug, role),
                    )
            except Exception as e:
                logger.warning("Failed to remove %s from group: %s", username, e)

        return True

    # ── Pull resource (for membership sync) ────────────────────────────────

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> Optional[BackendResourceInfo]:
        """Pull resource data including users from all 3 Keycloak groups."""
        ns_name = waldur_resource.backend_id
        if not ns_name:
            logger.warning("Backend ID is missing for resource %s", waldur_resource.name)
            return None

        try:
            cr = self.k8s_client.get_managed_namespace(ns_name)
            if cr is None:
                logger.warning("ManagedNamespace %s not found", ns_name)
                return None

            # Collect users from all Keycloak groups
            users = self._list_all_keycloak_users(waldur_resource)

            # Get usage report
            report = self._get_usage_report([ns_name])
            usage = report.get(ns_name, {"TOTAL_ACCOUNT_USAGE": {}})

            return BackendResourceInfo(
                backend_id=ns_name,
                users=users,
                usage=usage,
            )
        except Exception as e:
            logger.exception("Error pulling resource %s: %s", ns_name, e)
            return None

    def _list_all_keycloak_users(self, waldur_resource: WaldurResource) -> list[str]:
        """List all users across all 3 Keycloak groups (union)."""
        if not self.keycloak_client:
            return []

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        all_users = set()

        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            group = self.keycloak_client.get_group_by_name(group_name)
            if group:
                members = self.keycloak_client.get_group_members(group["id"])
                for member in members:
                    if self.keycloak_use_user_id:
                        uid = member.get("id", "")
                    else:
                        uid = member.get("username", "")
                    if uid:
                        all_users.add(uid)

        return list(all_users)

    # ── Status operations ──────────────────────────────────────────────────

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale by patching CR quota to minimal values."""
        try:
            minimal_quota = {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": minimal_quota}},
            )
            logger.info("Downscaled namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to downscale %s: %s", resource_backend_id, e)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause by patching CR quota to zero."""
        try:
            zero_quota = {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": zero_quota}},
            )
            logger.info("Paused namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to pause %s: %s", resource_backend_id, e)
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore is a no-op; limits should be re-set via set_resource_limits."""
        logger.info("Restore for namespace %s is a no-op", resource_backend_id)
        return True
