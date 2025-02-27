"""Generic backend classes."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple

from waldur_site_agent.backends.base import BaseClient, UnknownClient
from waldur_site_agent.backends.exceptions import BackendError

from . import BackendType, logger, structures, utils

UNKNOWN_BACKEND_TYPE = "unknown"


class BaseBackend(ABC):
    """Abstract backend class."""

    def __init__(self, backend_settings: Dict, backend_components: Dict[str, Dict]) -> None:
        """Init backend info."""
        self.backend_type = "abstract"
        self.backend_settings = backend_settings
        self.backend_components = backend_components
        self.client: BaseClient = UnknownClient()

    @abstractmethod
    def ping(self, raise_exception: bool = False) -> bool:
        """Check if backend is online."""

    @abstractmethod
    def list_components(self) -> List[str]:
        """Return a list of computing components on the backend."""

    def pull_resources(
        self, resources_info: List[structures.Resource]
    ) -> Dict[str, structures.Resource]:
        """Pull data of resources available in the backend."""
        report = {}
        for resource_info in resources_info:
            backend_id = resource_info.backend_id
            try:
                resource = self.pull_resource(resource_info)
                if resource is not None:
                    report[backend_id] = resource
            except Exception as e:
                logger.exception("Error while pulling allocation [%s]: %s", backend_id, e)
        return report

    def pull_resource(self, resource_info: structures.Resource) -> Optional[structures.Resource]:
        """Pull resource from backend."""
        try:
            backend_id = resource_info.backend_id
            backend_resource = self._pull_allocation(backend_id)
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

    def _pull_allocation(self, resource_backend_id: str) -> Optional[structures.Resource]:
        """Pull allocation data from the backend."""
        account = resource_backend_id
        logger.info("Pulling allocation %s", account)
        account_info = self.client.get_account(account)

        if account_info is None:
            logger.warning("There is no %s account in the backend", account)
            return None

        users = self.client.list_account_users(account)

        report = self._get_usage_report([account])
        usage = report.get(account)

        if usage is None:
            empty_usage = {tres: 0 for tres in self.backend_components}
            usage = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return structures.Resource(
            name="",
            marketplace_uuid="",
            backend_id=account,
            limits={},
            users=users,
            usage=usage,
            backend_type=self.backend_type,
        )

    @abstractmethod
    def _get_usage_report(self, accounts: List[str]) -> Dict:
        """Collect usage report for the specified accounts."""

    def _pre_delete_account_actions(self, account: str) -> None:
        """Perform actions before deleting the account."""
        del account

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Delete resource from the backend."""
        account = resource_backend_id

        if not account.strip():
            message = "Empty backend_id for allocation, skipping deletion"
            raise BackendError(message)

        if self.client.get_account(account) is None:
            logger.warning("No account %s is in %s", account, self.backend_type)
            return

        self._pre_delete_account_actions(account)
        self._delete_account_safely(account)

        if "project_slug" in kwargs:
            project_account = self._get_project_name(kwargs["project_slug"])
            if (
                len(
                    [
                        account
                        for account in self.client.list_accounts()
                        if account.organization == project_account
                        and account.name != project_account
                    ]
                )
                == 0
            ):
                self._delete_account_safely(project_account)

    def _delete_account_safely(self, account: str) -> None:
        if self.client.get_account(account):
            logger.info("Deleting the account %s from %s", account, self.backend_type)
            self.client.delete_account(account)
        else:
            logger.warning("No account %s is in %s", account, self.backend_type)

    def _create_account(
        self,
        account_name: str,
        account_description: str,
        account_organization: str,
        account_parent_name: Optional[str] = None,
    ) -> bool:
        logger.info(
            "Creating SLURM account %s (backend id = %s)",
            account_description,
            account_name,
        )
        if self.client.get_account(account_name) is None:
            self.client.create_account(
                name=account_name,
                description=account_description,
                organization=account_organization,
                parent_name=account_parent_name,
            )
            return True
        logger.info("The account %s already exists in the cluster", account_name)
        return False

    def create_resource(self, waldur_resource: Dict) -> structures.Resource:
        """Create resource on the backend.

        Creates necessary accounts hierarchy and sets up resource limits.
        """
        logger.info("Creating account in the backend")

        # Setup accounts hierarchy
        self._setup_accounts_hierarchy(waldur_resource)

        # Create allocation account
        allocation_account = self._create_allocation_account(waldur_resource)

        # Setup limits
        self._setup_resource_limits(allocation_account, waldur_resource)

        return structures.Resource(
            backend_type=self.backend_type,
            name=waldur_resource["name"],
            marketplace_uuid=waldur_resource["uuid"],
            backend_id=allocation_account,
            limits=self._collect_limits(waldur_resource)[1],
        )

    def _setup_accounts_hierarchy(self, waldur_resource: Dict) -> None:
        """Setup customer and project accounts hierarchy."""
        project_account = self._get_project_name(waldur_resource["project_slug"])

        # Setup customer account if using SLURM
        customer_account = None
        if self.backend_type == BackendType.SLURM.value:
            customer_account = self._get_customer_name(waldur_resource["customer_slug"])
            self._create_account(
                customer_account, waldur_resource["customer_name"], customer_account
            )

        # Create project account
        self._create_account(
            project_account, waldur_resource["project_name"], project_account, customer_account
        )

    def _create_allocation_account(self, waldur_resource: Dict) -> str:
        """Create allocation account with retry logic for name generation."""
        project_account = self._get_project_name(waldur_resource["project_slug"])

        # Determine allocation name generation strategy
        use_project_slug = (
            waldur_resource["offering_plugin_options"].get("account_name_generation_policy")
            == "project_slug"
        )

        allocation_name = (
            waldur_resource["project_slug"] if use_project_slug else waldur_resource["slug"]
        )
        max_retries = 10 if use_project_slug else 1

        # Try creating account with generated names
        for retry in range(max_retries):
            allocation_account = self._get_allocation_name(allocation_name)
            if self._create_account(
                allocation_account, waldur_resource["name"], project_account, project_account
            ):
                return allocation_account

            if use_project_slug:
                allocation_name = f"{waldur_resource['project_slug']}-{retry}"

        raise BackendError(
            f"Unable to create an allocation: {allocation_account} already exists in the cluster"
        )

    def _setup_resource_limits(self, allocation_account: str, waldur_resource: Dict) -> None:
        """Setup resource limits for the allocation account."""
        allocation_limits, _ = self._collect_limits(waldur_resource)

        if not allocation_limits:
            logger.info("Skipping setting of limits")
            return

        # Convert limits for logging
        converted_limits = {
            key: value // self.backend_components[key].get("unit_factor", 1)
            for key, value in allocation_limits.items()
        }

        limits_str = utils.prettify_limits(converted_limits, self.backend_components)
        logger.info("Setting allocation limits to: \n%s", limits_str)
        self.client.set_resource_limits(allocation_account, allocation_limits)

    @abstractmethod
    def downscale_resource(self, account: str) -> bool:
        """Downscale the account on the backend."""

    @abstractmethod
    def pause_resource(self, account: str) -> bool:
        """Pause the account on the backend."""

    @abstractmethod
    def restore_resource(self, account: str) -> bool:
        """Restore the account after downscaling or pausing."""

    @abstractmethod
    def get_resource_metadata(self, _: str) -> dict:
        """Get backend-specific resource metadata."""

    @abstractmethod
    def _collect_limits(
        self, waldur_resource: Dict[str, Dict]
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Collect limits for backend and waldur separately."""

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: Set[str], **kwargs: dict
    ) -> Set[str]:
        """Add specified users to the resource on the backend."""
        del kwargs
        logger.info(
            "Adding users to account %s on backend: %s", resource_backend_id, " ,".join(user_ids)
        )
        added_users = set()
        for username in user_ids:
            try:
                succeeded = self.add_user(resource_backend_id, username)
                if succeeded:
                    added_users.add(username)
            except BackendError as e:
                logger.exception(
                    "Unable to add user %s to account %s, details: %s",
                    username,
                    resource_backend_id,
                    e,
                )

        return added_users

    def add_user(self, account: str, username: str) -> bool:
        """Add association between user and backend account if it doesn't exists."""
        if not account.strip():
            message = "Empty backend_id for allocation"
            raise BackendError(message)

        logger.info("Adding user %s to account %s", username, account)
        if not username:
            logger.warning("Username is blank, skipping creation of association")
            return False

        if not self.client.get_association(username, account):
            logger.info("Creating association between %s and %s", username, account)
            try:
                self.client.create_association(
                    username, account, self.backend_settings.get("default_account", "root")
                )
            except BackendError as err:
                logger.exception("Unable to create association on backend: %s", err)
                return False
        else:
            logger.info("Association already exists, skipping creation")
        return True

    def remove_users_from_account(self, resource_backend_id: str, usernames: Set[str]) -> List[str]:
        """Remove specified users from the resource on the backend."""
        logger.info(
            "Removing users from account %s on backend: %s",
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
                    "Unable to remove user %s from account %s, details: %s",
                    username,
                    resource_backend_id,
                    e,
                )
        return removed_users

    def _pre_delete_user_actions(self, account: str, username: str) -> None:
        del account, username

    def remove_user(self, account: str, username: str) -> bool:
        """Delete association between user and an account if it exists."""
        if not account.strip():
            message = "Empty account name"
            raise BackendError(message)

        logger.info("Removing user %s from account %s", username, account)

        if self.client.get_association(username, account):
            logger.info("Deleting association between %s and %s", username, account)
            try:
                self._pre_delete_user_actions(account, username)
                self.client.delete_association(username, account)
            except BackendError as err:
                logger.exception("Unable to delete association in the backend: %s", err)
                return False
        return True

    def set_resource_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        """Set limits for the resource on the backend."""
        self.client.set_resource_limits(resource_backend_id, limits)

    def get_resource_limits(self, resource_backend_id: str) -> Dict[str, int]:
        """Get limits for the resource on the backend."""
        return self.client.get_resource_limits(resource_backend_id)

    def get_resource_user_limits(self, resource_backend_id: str) -> Dict[str, Dict[str, int]]:
        """Get limits for the resource users on the backend."""
        return self.client.get_resource_user_limits(resource_backend_id)

    def set_resource_user_limits(
        self, resource_backend_id: str, username: str, limits: Dict[str, int]
    ) -> None:
        """Set limits for a specific user in a resource on the backend."""
        logger.info(
            "Setting user %s limits to %s for resource %s", username, limits, resource_backend_id
        )
        self.client.set_resource_user_limits(resource_backend_id, username, limits)

    def _get_allocation_name(self, allocation_slug: str, prefix: str = "") -> str:
        prefix = self.backend_settings.get("allocation_prefix", "")
        return f"{prefix}{allocation_slug}".lower()

    def _get_project_name(self, slug: str) -> str:
        return f"{self.backend_settings.get('project_prefix', '')}{slug}"

    def _get_customer_name(self, slug: str) -> str:
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

    def list_components(self) -> List[str]:
        """Placeholder."""
        return []

    def pull_resources(self, _: List[structures.Resource]) -> Dict[str, structures.Resource]:
        """Placeholder."""
        return {}

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Placeholder."""
        del kwargs, resource_backend_id

    def create_resource(self, _: Dict) -> structures.Resource:
        """Placeholder."""
        return structures.Resource()

    def downscale_resource(self, account: str) -> bool:
        """Placeholder."""
        del account
        return False

    def pause_resource(self, account: str) -> bool:
        """Placeholder."""
        del account
        return False

    def restore_resource(self, account: str) -> bool:
        """Placeholder."""
        del account
        return False

    def get_resource_metadata(self, _: str) -> dict:
        """Placeholder."""
        return {}

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: Set[str], **kwargs: dict
    ) -> Set[str]:
        """Placeholder."""
        del kwargs, resource_backend_id
        return user_ids

    def set_resource_limits(self, _: str, limits: Dict[str, int]) -> None:
        """Placeholder."""
        del limits

    def _collect_limits(self, _: Dict[str, Dict]) -> Tuple[Dict[str, int], Dict[str, int]]:
        return {"": 0}, {"": 0}

    def _pull_allocation(self, _: str) -> Optional[structures.Resource]:
        return None

    def _get_usage_report(self, _: List[str]) -> Dict:
        return {}
