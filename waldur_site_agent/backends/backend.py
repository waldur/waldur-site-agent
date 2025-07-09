"""Generic backend classes."""

from abc import ABC, abstractmethod
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backends import BackendType, logger, structures, utils
from waldur_site_agent.backends.base import BaseClient, UnknownClient
from waldur_site_agent.backends.exceptions import BackendError

UNKNOWN_BACKEND_TYPE = "unknown"


class BaseBackend(ABC):
    """Backend class with implemented generic methods and other abstract methods."""

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Init backend info."""
        self.backend_type = "abstract"
        self.backend_settings = backend_settings
        self.backend_components = backend_components
        self.client: BaseClient = UnknownClient()

    @abstractmethod
    def ping(self, raise_exception: bool = False) -> bool:
        """Check if backend is online."""

    @abstractmethod
    def list_components(self) -> list[str]:
        """Return a list of computing components on the backend."""

    @abstractmethod
    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage report for the specified resource_backend_ids."""

    @abstractmethod
    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale the resource with the ID on the backend."""

    @abstractmethod
    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause the resource on the backend."""

    @abstractmethod
    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore the resource after downscaling or pausing."""

    @abstractmethod
    def get_resource_metadata(self, _: str) -> dict:
        """Get backend-specific resource metadata."""

    @abstractmethod
    def _collect_resource_limits(
        self, waldur_resource: dict[str, dict]
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect limits for backend and waldur separately."""

    def pull_resources(
        self, resources_info: list[structures.Resource]
    ) -> dict[str, structures.Resource]:
        """Pull data of resources available in the backend."""
        report = {}
        for resource_info in resources_info:
            backend_id = resource_info.backend_id
            try:
                resource = self.pull_resource(resource_info)
                if resource is not None:
                    report[backend_id] = resource
            except Exception as e:
                logger.exception("Error while pulling resource [%s]: %s", backend_id, e)
        return report

    def pull_resource(self, resource_info: structures.Resource) -> Optional[structures.Resource]:
        """Pull resource from backend."""
        try:
            backend_id = resource_info.backend_id
            backend_resource = self._pull_backend_resource(backend_id)
            if backend_resource is None:
                return None
        except Exception as e:
            logger.exception("Error while pulling resource [%s]: %s", backend_id, e)
            return None
        else:
            backend_resource.name = resource_info.name
            backend_resource.marketplace_uuid = resource_info.marketplace_uuid
            backend_resource.restrict_member_access = resource_info.restrict_member_access
            backend_resource.downscaled = resource_info.downscaled
            backend_resource.paused = resource_info.paused
            backend_resource.state = resource_info.state
            return backend_resource

    def _pull_backend_resource(self, resource_backend_id: str) -> Optional[structures.Resource]:
        """Pull resource data from the backend."""
        logger.info("Pulling resource %s", resource_backend_id)
        resource_backend_info = self.client.get_account(resource_backend_id)

        if resource_backend_info is None:
            logger.warning("There is no resource with ID %s in the backend", resource_backend_id)
            return None

        users = self.client.list_account_users(resource_backend_id)

        report = self._get_usage_report([resource_backend_id])
        usage = report.get(resource_backend_id)

        if usage is None:
            empty_usage = dict.fromkeys(self.backend_components, 0)
            usage = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return structures.Resource(
            name="",
            marketplace_uuid="",
            backend_id=resource_backend_id,
            limits={},
            users=users,
            usage=usage,
            backend_type=self.backend_type,
        )

    def _pre_delete_resource(self, resource_backend_id: str) -> None:
        """Perform actions before deleting the resource in the backend."""
        del resource_backend_id

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Delete resource from the backend."""
        if not resource_backend_id.strip():
            logger.warning("Empty backend_id for resource, skipping deletion")
            return

        if self.client.get_account(resource_backend_id) is None:
            logger.warning(
                "No resource with ID %s is in %s", resource_backend_id, self.backend_type
            )
            return

        self._pre_delete_resource(resource_backend_id)
        self._delete_resource_safely(resource_backend_id)

        if "project_slug" in kwargs:
            project_backend_id = self._get_project_backend_id(kwargs["project_slug"])
            if (
                len(
                    [
                        resource
                        for resource in self.client.list_accounts()
                        if resource.organization == project_backend_id
                        and resource.name != project_backend_id
                    ]
                )
                == 0
            ):
                self._delete_resource_safely(project_backend_id)

    def _delete_resource_safely(self, resource_backend_id: str) -> None:
        if self.client.get_account(resource_backend_id):
            logger.info(
                "Deleting the resource with ID %s from %s", resource_backend_id, self.backend_type
            )
            self.client.delete_account(resource_backend_id)
        else:
            logger.warning(
                "No resource with ID %s is in %s", resource_backend_id, self.backend_type
            )

    def _create_backend_resource(
        self,
        resource_backend_id: str,
        resource_name: str,
        resource_organization: str,
        resource_parent_name: Optional[str] = None,
    ) -> bool:
        logger.info(
            "Creating resource %s in %s backend (backend ID = %s)",
            resource_name,
            self.backend_type,
            resource_backend_id,
        )
        if self.client.get_account(resource_backend_id) is None:
            self.client.create_account(
                name=resource_backend_id,
                description=resource_name,
                organization=resource_organization,
                parent_name=resource_parent_name,
            )
            return True
        logger.info("The resource with ID %s already exists in the cluster", resource_backend_id)
        return False

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> structures.Resource:
        """Create resource on the backend.

        Creates necessary objects and resources and sets up resource limits.

        Args:
            waldur_resource: Resource data from Waldur marketplace
            user_context: Optional user context including team members and offering users
        """
        logger.info("Creating resource in the backend")
        # Note: user_context is available for backends that need it during creation
        # Actions prior to resource creation
        self._pre_create_resource(waldur_resource, user_context)

        # Create resource in the backend
        backend_resource_id = self._create_resource_in_backend(waldur_resource)

        # # Setup limits
        self._setup_resource_limits(backend_resource_id, waldur_resource)
        resource = structures.Resource(
            backend_type=self.backend_type,
            name=waldur_resource.name,
            marketplace_uuid=waldur_resource.uuid.hex,
            backend_id=backend_resource_id,
            limits=self._collect_resource_limits(waldur_resource)[1],
        )
        # Actions after resource creation
        self.post_create_resource(resource, waldur_resource, user_context)
        return resource

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Actions performed prior to resource creation."""
        # Default implementation: setup customer and project accounts hierarchy
        del user_context
        project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)

        # Setup customer resource if using SLURM backend
        customer_backend_id = None
        if self.backend_type == BackendType.SLURM.value:
            customer_backend_id = self._get_customer_backend_id(waldur_resource.customer_slug)
            self._create_backend_resource(
                customer_backend_id, waldur_resource.customer_name, customer_backend_id
            )

        # Create project resource
        self._create_backend_resource(
            project_backend_id,
            waldur_resource.project_name,
            project_backend_id,
            customer_backend_id,
        )

    def _create_resource_in_backend(self, waldur_resource: WaldurResource) -> str:
        """Create backend resource with retry logic for name generation."""
        project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)

        # Determine resource name generation strategy
        use_project_slug = (
            waldur_resource.offering_plugin_options.get("account_name_generation_policy")
            == "project_slug"
        )

        resource_base_id = (
            waldur_resource.project_slug if use_project_slug else waldur_resource.slug
        )
        max_retries = 10 if use_project_slug else 1

        # Try creating resource with generated IDs
        for retry in range(max_retries):
            resource_backend_id = self._get_resource_backend_id(resource_base_id)
            if self._create_backend_resource(
                resource_backend_id, waldur_resource.name, project_backend_id, project_backend_id
            ):
                return resource_backend_id

            if use_project_slug:
                resource_base_id = f"{waldur_resource.project_slug}-{retry}"

        raise BackendError(
            f"Unable to create an resource: {resource_backend_id} already exists in the cluster"
        )

    def _setup_resource_limits(
        self, resource_backend_id: str, waldur_resource: WaldurResource
    ) -> None:
        """Setup resource limits for the resource in backend."""
        resource_backend_limits, _ = self._collect_resource_limits(waldur_resource)

        if not resource_backend_limits:
            logger.info("Skipping setting of limits")
            return

        # Convert limits for logging
        converted_limits = {
            key: value // self.backend_components[key].get("unit_factor", 1)
            for key, value in resource_backend_limits.items()
        }

        limits_str = utils.prettify_limits(converted_limits, self.backend_components)
        logger.info("Setting resource backend limits to: \n%s", limits_str)
        self.client.set_resource_limits(resource_backend_id, resource_backend_limits)

    def post_create_resource(
        self,
        resource: structures.Resource,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Perform customizable actions after resource creation."""
        del resource, waldur_resource, user_context  # Not used in base implementation

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add specified users to the resource on the backend."""
        del kwargs
        logger.info(
            "Adding users to resource %s on backend: %s", resource_backend_id, " ,".join(user_ids)
        )
        added_users = set()
        for username in user_ids:
            try:
                succeeded = self.add_user(resource_backend_id, username)
                if succeeded:
                    added_users.add(username)
            except BackendError as e:
                logger.exception(
                    "Unable to add user %s to resource %s, details: %s",
                    username,
                    resource_backend_id,
                    e,
                )

        return added_users

    def add_user(self, resource_backend_id: str, username: str) -> bool:
        """Add association between user and backend resource if it doesn't exists."""
        if not resource_backend_id.strip():
            message = "Empty backend ID for resource"
            raise BackendError(message)

        logger.info("Adding user %s to resource %s", username, resource_backend_id)
        if not username:
            logger.warning("Username is blank, skipping creation of association")
            return False

        if not self.client.get_association(username, resource_backend_id):
            logger.info("Creating association between %s and %s", username, resource_backend_id)
            try:
                self.client.create_association(
                    username,
                    resource_backend_id,
                    self.backend_settings.get("default_account", "root"),
                )
            except BackendError as err:
                logger.exception("Unable to create association on backend: %s", err)
                return False
        else:
            logger.info("Association already exists, skipping creation")
        return True

    def remove_users_from_resource(
        self, resource_backend_id: str, usernames: set[str]
    ) -> list[str]:
        """Remove specified users from the resource on the backend."""
        logger.info(
            "Removing users from resource %s on backend: %s",
            resource_backend_id,
            " ,".join(usernames),
        )
        removed_users = []
        for username in usernames:
            try:
                succeeded = self.remove_user(resource_backend_id, username)
                if succeeded:
                    removed_users.append(username)
            except BackendError as e:
                logger.exception(
                    "Unable to remove user %s from resource %s, details: %s",
                    username,
                    resource_backend_id,
                    e,
                )
        return removed_users

    def _pre_delete_user_actions(self, resource_backend_id: str, username: str) -> None:
        """Perform actions before removing the user from the resource."""
        del resource_backend_id, username

    def remove_user(self, resource_backend_id: str, username: str) -> bool:
        """Delete association between user and backend resource if it exists."""
        if not resource_backend_id.strip():
            message = "Empty resource backend ID"
            raise BackendError(message)

        logger.info("Removing user %s from resource %s", username, resource_backend_id)

        if self.client.get_association(username, resource_backend_id):
            logger.info("Deleting association between %s and %s", username, resource_backend_id)
            try:
                self._pre_delete_user_actions(resource_backend_id, username)
                self.client.delete_association(username, resource_backend_id)
            except BackendError as err:
                logger.exception("Unable to delete association in the backend: %s", err)
                return False
        return True

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Set limits for the resource on the backend."""
        self.client.set_resource_limits(resource_backend_id, limits)

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Get limits for the resource on the backend."""
        return self.client.get_resource_limits(resource_backend_id)

    def get_resource_user_limits(self, resource_backend_id: str) -> dict[str, dict[str, int]]:
        """Get limits for the resource users on the backend."""
        return self.client.get_resource_user_limits(resource_backend_id)

    def set_resource_user_limits(
        self, resource_backend_id: str, username: str, limits: dict[str, int]
    ) -> None:
        """Set limits for a specific user in a resource on the backend."""
        logger.info(
            "Setting user %s limits to %s for resource %s", username, limits, resource_backend_id
        )
        self.client.set_resource_user_limits(resource_backend_id, username, limits)

    def _get_resource_backend_id(self, resource_slug: str, prefix: str = "") -> str:
        prefix = self.backend_settings.get("allocation_prefix", "")
        return f"{prefix}{resource_slug}".lower()

    def _get_project_backend_id(self, slug: str) -> str:
        return f"{self.backend_settings.get('project_prefix', '')}{slug}"

    def _get_customer_backend_id(self, slug: str) -> str:
        return f"{self.backend_settings.get('customer_prefix', '')}{slug}"


class UnknownBackend(BaseBackend):
    """Common class for unknown backends."""

    def __init__(self) -> None:
        """Placeholder."""
        super().__init__({}, {})
        self.backend_type = UNKNOWN_BACKEND_TYPE

    def ping(self, _: bool = False) -> bool:
        """Placeholder."""
        return False

    def list_components(self) -> list[str]:
        """Placeholder."""
        return []

    def pull_resources(self, _: list[structures.Resource]) -> dict[str, structures.Resource]:
        """Placeholder."""
        return {}

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Placeholder."""
        del kwargs, resource_backend_id

    def create_resource(self, _: dict, user_context: Optional[dict] = None) -> structures.Resource:
        """Placeholder."""
        del user_context
        return structures.Resource()

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Placeholder."""
        del resource_backend_id
        return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Placeholder."""
        del resource_backend_id
        return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Placeholder."""
        del resource_backend_id
        return False

    def get_resource_metadata(self, _: str) -> dict:
        """Placeholder."""
        return {}

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Placeholder."""
        del kwargs, resource_backend_id
        return user_ids

    def set_resource_limits(self, _: str, limits: dict[str, int]) -> None:
        """Placeholder."""
        del limits

    def _collect_resource_limits(self, _: WaldurResource) -> tuple[dict[str, int], dict[str, int]]:
        return {"": 0}, {"": 0}

    def _pull_backend_resource(self, _: str) -> Optional[structures.Resource]:
        return None

    def _get_usage_report(self, _: list[str]) -> dict:
        return {}
