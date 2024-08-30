"""Generic backend classes."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple

from waldur_client import is_uuid

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
                resource = self._pull_allocation(backend_id)
                if resource is not None:
                    resource.name = resource_info.name
                    resource.marketplace_uuid = resource_info.marketplace_uuid
                    resource.marketplace_scope_uuid = resource_info.marketplace_scope_uuid
                    report[backend_id] = resource
            except Exception as e:
                logger.exception("Error while pulling allocation [%s]: %s", backend_id, e)
        return report

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

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Delete resource from the backend."""
        account = resource_backend_id

        if not account.strip():
            message = "Empty backend_id for allocation, skipping deletion"
            raise BackendError(message)

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

    def create_resource(self, waldur_resource: Dict) -> structures.Resource:
        """Create resource on the backend."""
        logger.info("Creating account in the backend")
        resource_uuid = waldur_resource["uuid"]
        resource_name = waldur_resource["name"]
        waldur_allocation_uuid = waldur_resource["resource_uuid"]

        if not is_uuid(waldur_allocation_uuid):
            logger.error("Unexpected allocation UUID format, skipping the order")
            return structures.Resource(backend_id="")

        project_name = waldur_resource["project_name"]
        customer_name = waldur_resource["customer_name"]

        project_account = self._get_project_name(waldur_resource["project_slug"])
        allocation_account = self._get_allocation_name(waldur_resource["slug"])

        customer_account = None
        if self.backend_type == BackendType.SLURM.value:
            customer_account = self._get_customer_name(waldur_resource["customer_slug"])
            if not self.client.get_account(customer_account):
                logger.info(
                    "Creating SLURM account for customer %s (backend id = %s)",
                    customer_name,
                    customer_account,
                )
                self.client.create_account(
                    name=customer_account,
                    description=customer_name,
                    organization=customer_account,
                )

        if not self.client.get_account(project_account):
            logger.info(
                "Creating an account for project %s (backend id = %s)",
                project_name,
                project_account,
            )
            self.client.create_account(
                name=project_account,
                description=project_name,
                organization=project_account,
                parent_name=customer_account,
            )

        if self.client.get_account(allocation_account) is not None:
            logger.info(
                "The account %s already exists in the cluster, skipping creation",
                allocation_account,
            )
        else:
            logger.info(
                "Creating an account for allocation %s (backend id = %s)",
                resource_name,
                allocation_account,
            )
            self.client.create_account(
                name=allocation_account,
                description=resource_name,
                organization=project_account,
            )

        allocation_limits, waldur_resource_limits = self._collect_limits(waldur_resource)

        # Convert limits (for correct logging only)
        converted_limits = {
            key: value // self.backend_components[key].get("unit_factor", 1)
            for key, value in allocation_limits.items()
        }

        limits_str = utils.prettify_limits(converted_limits, self.backend_components)
        logger.info("Setting allocation limits to: \n%s", limits_str)
        self.client.set_resource_limits(allocation_account, allocation_limits)

        return structures.Resource(
            backend_type=self.backend_type,
            name=resource_name,
            marketplace_uuid=resource_uuid,
            backend_id=allocation_account,
            limits=waldur_resource_limits,
        )

    @abstractmethod
    def _collect_limits(
        self, waldur_resource: Dict[str, Dict]
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Collect limits for backend and waldur separately."""

    def add_users_to_resource(self, resource_backend_id: str, user_ids: Set[str]) -> Set[str]:
        """Add specified users to the resource on the backend."""
        added_users = set()
        for username in user_ids:
            try:
                succeeded = self._add_user(resource_backend_id, username)
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

    def _add_user(self, account: str, username: str) -> bool:
        """Add association between user and backend account if it doesn't exists."""
        if not account.strip():
            message = "Empty backend_id for allocation"
            raise BackendError(message)

        if not self.client.get_association(username, account):
            logger.info("Creating association between %s and %s", username, account)
            try:
                self.client.create_association(
                    username, account, self.backend_settings.get("default_account", "root")
                )
            except BackendError as err:
                logger.exception("Unable to create association on backend: %s", err)
                return False
        return True

    def set_resource_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        """Set limits for the resource on the backend."""
        self.client.set_resource_limits(resource_backend_id, limits)

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

    def add_users_to_resource(self, _: str, user_ids: Set[str]) -> Set[str]:
        """Placeholder."""
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
