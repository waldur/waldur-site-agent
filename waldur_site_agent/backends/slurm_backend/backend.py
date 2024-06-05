"""SLURM-specific backend classes and functions."""

from __future__ import annotations

import re
from typing import Dict, List, Set

from waldur_client import is_uuid

from waldur_site_agent.backends import (
    BackendType,
    backend,
    logger,
)
from waldur_site_agent.backends import (
    structures as common_structures,
)
from waldur_site_agent.backends.exceptions import BackendError

from . import (
    SLURM_ALLOCATION_REGEX,
    utils,
)
from .client import SlurmClient


class SlurmBackend(backend.BaseBackend):
    """Main class for management of SLURM resources."""

    def __init__(self, slurm_settings: Dict, slurm_tres: Dict[str, Dict]) -> None:
        """Inits backend-related data and creates a corresponding client."""
        super().__init__()
        self.backend_type = BackendType.SLURM.value
        self.slurm_settings = slurm_settings
        self.slurm_tres = slurm_tres
        self.client = SlurmClient(slurm_tres)

    def ping(self, raise_exception: bool = False) -> bool:
        """Checks if the SLURM cluster is online."""
        try:
            self.client.list_accounts()
        except BackendError as err:
            if raise_exception:
                raise
            logger.info("Error: %s", err)
            return False
        else:
            return True

    def list_components(self) -> List[str]:
        """Returns a list of TRES on the SLURM cluster."""
        return self.client.list_tres()

    def pull_resources(
        self, resources_info: List[common_structures.Resource]
    ) -> Dict[str, common_structures.Resource]:
        """Pulls data of allocation available in the SLURM cluster."""
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

    def delete_resource(self, resource_backend_id: str, **kwargs: str) -> None:
        """Deletes allocation on the SLURM cluster."""
        account = resource_backend_id

        if not account.strip():
            message = "Empty backend_id for allocation"
            raise BackendError(message)

        self._delete_account_safely(account)

        if "project_uuid" in kwargs:
            project_account = self._get_project_name(kwargs["project_uuid"])
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

        # TODO: delete customer if it doesn't have any associated allocations

    def create_resource(self, waldur_resource: Dict) -> common_structures.Resource:
        """Creates allocation (aka account) on the SLURM cluster."""
        logger.info("Creating account in SLURM cluster")
        resource_uuid = waldur_resource["uuid"]
        resource_name = waldur_resource["name"]
        waldur_allocation_uuid = waldur_resource["resource_uuid"]

        if not is_uuid(waldur_allocation_uuid):
            logger.error("Unexpected allocation UUID format, skipping the order")
            return common_structures.Resource(backend_id="")

        project_name = waldur_resource["project_name"]
        customer_name = waldur_resource["customer_name"]
        allocation_limits = utils.get_slurm_tres_limits(self.slurm_tres)

        tres_list = set(self.slurm_tres.keys())
        limit_based_components = tres_list - set(allocation_limits.keys())

        for component_key in limit_based_components:
            allocation_limits[component_key] = (
                waldur_resource["limits"][component_key]
                * self.slurm_tres[component_key]["unit_factor"]
            )

        customer_account = self._get_customer_name(waldur_resource["customer_uuid"])
        project_account = self._get_project_name(waldur_resource["project_uuid"])
        allocation_account = self._get_allocation_name(resource_name, waldur_allocation_uuid)

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
                "Creating SLURM account for project %s (backend id = %s)",
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
                "Creating SLURM account for allocation %s (backend id = %s)",
                resource_name,
                allocation_account,
            )
            self.client.create_account(
                name=allocation_account,
                description=resource_name,
                organization=project_account,
            )

        # Convert limits (for correct logging only)
        converted_limits = {
            key: value // self.slurm_tres[key]["unit_factor"]
            for key, value in allocation_limits.items()
        }

        limits_str = utils.prettify_limits(converted_limits, self.slurm_tres)
        logger.info("Setting SLURM allocation limits to: \n%s", limits_str)
        self._set_allocation_limits(allocation_account, allocation_limits)

        # Keep only limit-based components for Waldur resource
        waldur_resource_limits = {
            key: value // self.slurm_tres[key]["unit_factor"]
            for key, value in allocation_limits.items()
            if key in limit_based_components
        }

        return common_structures.Resource(
            backend_type=self.backend_type,
            name=resource_name,
            marketplace_uuid=resource_uuid,
            backend_id=allocation_account,
            limits=waldur_resource_limits,
        )

    def add_users_to_resource(self, resource_backend_id: str, user_ids: Set[str]) -> Set[str]:
        """Adds specified users to the allocations on the SLURM cluster."""
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

        if self.slurm_settings.get("enable_user_homedir_account_creation", True):
            self._create_user_homedirs(added_users)

        return added_users

    def _remove_users_from_account(self, account: str, usernames: Set[str]) -> List[str]:
        removed_associations = []
        for username in usernames:
            try:
                succeeded = self._remove_user(account, username)
                if succeeded:
                    removed_associations.append(username)
            except BackendError as e:
                logger.exception(
                    "Unable to remove user %s from account %s, details: %s",
                    username,
                    account,
                    e,
                )
        return removed_associations

    def _add_user(self, account: str, username: str) -> bool:
        """Add association between user and SLURM account if it doesn't exists."""
        if not account.strip():
            message = "Empty backend_id for allocation"
            raise BackendError(message)

        if not self.client.get_association(username, account):
            logger.info("Creating association between %s and %s", username, account)
            try:
                self.client.create_association(
                    username, account, self.slurm_settings["default_account"]
                )
            except BackendError as err:
                logger.exception("Unable to create association in Slurm: %s", err)
                return False
        return True

    def _remove_user(self, account: str, username: str) -> bool:
        """Delete association between user and SLURM account if it exists."""
        if not account.strip():
            message = "Empty backend_id for allocation"
            raise BackendError(message)

        if self.client.get_association(username, account):
            logger.info("Deleting association between %s and %s", username, account)
            try:
                self.client.delete_association(username, account)
            except BackendError as err:
                logger.exception("Unable to delete association in Slurm: %s", err)
                return False
        return True

    def _create_user_homedirs(self, usernames: Set[str]) -> None:
        logger.info("Creating homedirs for users")
        for username in usernames:
            try:
                self.client.create_linux_user_homedir(username)
                logger.info("Homedir for user %s has been created", username)
            except BackendError as err:
                logger.exception(
                    "Unable to create user homedir for %s, reason: %s",
                    username,
                    err,
                )

    def _pull_allocation(self, resource_id: str) -> common_structures.Resource | None:
        account = resource_id
        logger.info("Pulling allocation %s", account)
        account_info = self.client.get_account(account)

        if account_info is None:
            logger.warning("There is no %s account in the SLURM cluster", account)
            return None

        users = self.client.list_account_users(account)

        report = self._get_usage_report([account])
        usage = report.get(account)
        if not usage:
            empty_usage = {tres: 0 for tres in self.slurm_tres}
            usage = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        # Convert SLURM units to Waldur ones
        usage_converted = {}
        for usage_account, usage_dict in usage.items():
            converted_usage_dict = utils.convert_slurm_units_to_waldur_ones(
                self.slurm_tres, usage_dict
            )
            usage_converted[usage_account] = converted_usage_dict

        limits: dict = self._get_allocation_limits(account)  # limits can be empty dict
        return common_structures.Resource(
            name="",
            marketplace_uuid="",
            backend_id=account,
            limits=limits,
            users=users,
            usage=usage_converted,
            backend_type=self.backend_type,
        )

    def _get_usage_report(self, accounts: List[str]) -> Dict[str, Dict[str, Dict[str, int]]]:
        """Example output.

        {
            "account_name": {
                "TOTAL_ACCOUNT_USAGE": {
                    'cpu': 1,
                    'gres/gpu': 2,
                    'mem': 3,
                },
                "user1": {
                    'cpu': 1,
                    'gres/gpu': 2,
                    'mem': 3,
                },
            }
        }
        """
        report: Dict[str, Dict[str, Dict[str, int]]] = {}
        lines = self.client.get_usage_report(accounts)

        for line in lines:
            logger.info(
                "Processing usage for %s account and %s user",
                line.account,
                line.user,
            )
            report.setdefault(line.account, {}).setdefault(line.user, {})
            tres_usage = line.tres_usage
            user_usage_existing = report[line.account][line.user]
            user_usage_new = utils.sum_dicts([user_usage_existing, tres_usage])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = list(account_usage.values())
            total = utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = total

        return report

    def _get_allocation_limits(self, account: str) -> Dict[str, int]:
        """Returns limits converted to Waldur-readable values."""
        lines = self.client.get_resource_limits(account)
        correct_lines = [
            association.tres_limits for association in lines if association.tres_limits
        ]
        if len(correct_lines) == 0:
            return {}

        return utils.convert_slurm_units_to_waldur_ones(
            self.slurm_tres, correct_lines[0], to_int=True
        )

    def _get_allocation_name(self, allocation_name: str, allocation_uuid: str) -> str:
        name = allocation_name
        prefix = self.slurm_settings["allocation_prefix"]
        hexpart = allocation_uuid[:5]

        raw_name = f"{prefix}{hexpart}_{name}"
        incorrect_symbols_regex = rf"[^{SLURM_ALLOCATION_REGEX}]+"
        sanitized_name = re.sub(incorrect_symbols_regex, "", raw_name)
        result_name = sanitized_name[: self.slurm_settings["allocation_name_max_len"]]
        return result_name.lower()

    def _get_project_name(self, name: str) -> str:
        return f"{self.slurm_settings['project_prefix']}{name}"

    def _get_customer_name(self, name: str) -> str:
        return f"{self.slurm_settings['customer_prefix']}{name}"

    def set_resource_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        """Sets limits for the SLURM allocation."""
        # Convert limits
        converted_limits = {
            key: value * self.slurm_tres[key]["unit_factor"] for key, value in limits.items()
        }

        usage_based_limits = utils.get_slurm_tres_limits(self.slurm_tres)
        missing_usage_based_limits = set(usage_based_limits.keys()) - set(converted_limits.keys())
        converted_limits.update(
            {
                tres: limit
                for tres, limit in usage_based_limits.items()
                if tres in missing_usage_based_limits
            }
        )

        self._set_allocation_limits(resource_backend_id, converted_limits)

    def _set_allocation_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        self.client.set_resource_limits(resource_backend_id, limits)

    def _delete_account_safely(self, account: str) -> None:
        if self.client.get_account(account):
            logger.info("Deleting account %s from SLURM cluster", account)
            self.client.delete_account(account)
        else:
            logger.warning("There is no account %s in SLURM cluster", account)
