import re
from typing import Dict, Set

from . import (
    SLURM_ALLOCATION_NAME_MAX_LEN,
    SLURM_ALLOCATION_PREFIX,
    SLURM_ALLOCATION_REGEX,
    SLURM_CONTAINER_NAME,
    SLURM_CUSTOMER_PREFIX,
    SLURM_DEFAULT_ACCOUNT,
    SLURM_DEPLOYMENT_TYPE,
    SLURM_PROJECT_PREFIX,
    logger,
    utils,
)
from .client import SlurmClient
from .exceptions import BackendError, SlurmError
from .structures import Allocation


class SlurmBackend:
    def __init__(
        self,
    ):
        self.client = SlurmClient(SLURM_DEPLOYMENT_TYPE, SLURM_CONTAINER_NAME)

    def ping(self, raise_exception=False):
        try:
            self.client.list_accounts()
        except SlurmError as err:
            if raise_exception:
                raise err
            logger.info("Error: %s", err)
            return False
        else:
            return True

    def list_tres(self):
        return self.client.list_tres()

    def pull_allocations(self):
        report = {}
        for account in self.client.list_accounts():
            try:
                logger.info("Pulling allocation %s", account.name)
                users, usage, limits = self.pull_allocation(account.name)
                report[account.name] = {
                    "users": users,
                    "usage": usage,
                    "limits": limits,  # limits can be None
                }
            except Exception as e:
                logger.exception(
                    "Error while pulling allocation [%s]: %s", account.name, e
                )
        return report

    def pull_allocation(self, account: str):
        users = self.client.list_account_users(account)

        report = self.get_usage_report([account])
        usage = report.get(account)
        if not usage:
            empty_usage = {tres: 0.00 for tres in utils.get_tres_list()}
            usage = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        limits: dict = self.get_allocation_limits(account)
        return users, usage, limits

    def get_usage_report(self, accounts):
        """
        Example output:
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
        report = {}
        lines = self.client.get_usage_report(accounts)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, {})
            tres_usage = line.tres_usage()
            user_usage_old = report[line.account][line.user]
            user_usage_new = utils.sum_dicts([user_usage_old, tres_usage])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = account_usage.values()
            total = utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = total

        return report

    def get_allocation_limits(self, account: str):
        lines = self.client.get_resource_limits(account)
        correct_lines = [
            association.tres_limits for association in lines if association.tres_limits
        ]
        if len(correct_lines) > 0:
            limits = {
                tres_name: round(limit) for tres_name, limit in correct_lines[0].items()
            }
            return limits

    def get_allocation_name(self, allocation: Allocation):
        name = allocation.name
        prefix = SLURM_ALLOCATION_PREFIX
        hexpart = allocation.uuid[:5]

        raw_name = "%s%s_%s" % (prefix, hexpart, name)
        incorrect_symbols_regex = r"[^%s]+" % SLURM_ALLOCATION_REGEX
        sanitized_name = re.sub(incorrect_symbols_regex, "", raw_name)
        result_name = sanitized_name[:SLURM_ALLOCATION_NAME_MAX_LEN]
        return result_name.lower()

    def get_project_name(self, name: str):
        return "%s%s" % (SLURM_PROJECT_PREFIX, name)

    def get_customer_name(self, name: str):
        return "%s%s" % (SLURM_CUSTOMER_PREFIX, name)

    def set_allocation_limits(
        self, allocation: Allocation, limits_dict: Dict[str, int]
    ):
        self.client.set_resource_limits(allocation.backend_id, limits_dict)

    def delete_customer(self, customer_backend_id):
        self.client.delete_account(customer_backend_id)

    def delete_project(self, project_backend_id):
        self.client.delete_account(project_backend_id)

    def delete_allocation(
        self,
        allocation: Allocation,
    ):
        account = allocation.backend_id
        project_account = self.get_project_name(allocation.project_uuid)

        if not account.strip():
            raise BackendError("Empty backend_id for allocation: %s" % allocation)

        existing_users = self.client.list_account_users(account)

        if self.client.get_account(account):
            self.client.delete_account(account)

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
            self.delete_project(project_account)

        # TODO: delete customer if it hasn't any associated allocations

        return existing_users

    def create_allocation(
        self,
        allocation: Allocation,
        customer_name: str,
        project_name: str,
        limits: dict,
    ):
        customer_account = self.get_customer_name(allocation.customer_uuid)
        project_account = self.get_project_name(allocation.project_uuid)
        allocation_account = self.get_allocation_name(allocation)

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
            return

        logger.info(
            "Creating SLURM account for allocation %s (backend id = %s)",
            allocation.name,
            allocation_account,
        )
        self.client.create_account(
            name=allocation_account,
            description=allocation.name,
            organization=project_account,
        )

        allocation.backend_id = allocation_account

        logger.info("Setting limits: %s", limits)
        self.set_allocation_limits(allocation, limits)

        return allocation_account

    def sync_users(
        self,
        allocation: Allocation,
        usernames: Set[str],
        all_usernames: Set[str],
    ):
        created_associations = self.add_users_to_account(allocation, usernames)

        all_backend_usernames = self.client.list_account_users(allocation.backend_id)
        backend_usernames = {
            username for username in all_backend_usernames if username in all_usernames
        }
        stale_usernames = backend_usernames - usernames

        removed_associations = self.remove_users_from_account(
            allocation, stale_usernames
        )

        return created_associations, removed_associations

    def add_users_to_account(self, allocation: Allocation, usernames: Set[str]):
        created_associations = []
        for username in usernames:
            try:
                succeeded = self.add_user(allocation, username)
                if succeeded:
                    created_associations.append(username)
            except BackendError as e:
                logger.exception(
                    "Unable to add user %s to account %s, details: %s",
                    username,
                    allocation.backend_id,
                    e,
                )
        return created_associations

    def remove_users_from_account(self, allocation: Allocation, usernames: Set[str]):
        removed_associations = []
        for username in usernames:
            try:
                succeeded = self.remove_user(allocation, username)
                if succeeded:
                    removed_associations.append(username)
            except BackendError as e:
                logger.exception(
                    "Unable to remove user %s from account %s, details: %s",
                    username,
                    allocation.backend_id,
                    e,
                )
        return removed_associations

    def add_user(self, allocation: Allocation, username: str):
        """
        Add association between user and SLURM account if it doesn't exists.
        """
        account = allocation.backend_id

        if not account.strip():
            raise BackendError("Empty backend_id for allocation: %s" % allocation)

        if not self.client.get_association(username, account):
            logger.info("Creating association between %s and %s", username, account)
            try:
                self.client.create_association(username, account, SLURM_DEFAULT_ACCOUNT)
            except SlurmError as err:
                logger.exception("Unable to create association in Slurm: %s", err)
                return False
        return True

    def remove_user(self, allocation, username):
        """
        Delete association between user and SLURM account if it exists.
        """
        account = allocation.backend_id

        if not account.strip():
            raise BackendError("Empty backend_id for allocation: %s" % allocation)

        if self.client.get_association(username, account):
            logger.info("Deleting association between %s and %s", username, account)
            try:
                self.client.delete_association(username, account)
            except SlurmError as err:
                logger.exception("Unable to delete association in Slurm: %s", err)
                return False
        return True

    def create_user_homedirs(self, usernames: Set[str]):
        logger.info("Creating homedirs for users")
        for username in usernames:
            try:
                self.client.create_linux_user_homedir(username)
                logger.info("Homedir for user %s has been created", username)
            except SlurmError as err:
                logger.exception(
                    "Unable to create user homedir for %s, reason: %s",
                    username,
                    err,
                )
