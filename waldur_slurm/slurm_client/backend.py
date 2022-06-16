import operator
import re
from functools import reduce
from typing import Set

from . import (
    SLURM_ALLOCATION_NAME_MAX_LEN,
    SLURM_ALLOCATION_PREFIX,
    SLURM_ALLOCATION_REGEX,
    SLURM_CUSTOMER_PREFIX,
    SLURM_DEFAULT_ACCOUNT,
    SLURM_DEFAULT_LIMITS,
    SLURM_DEPLOYMENT_TYPE,
    SLURM_PROJECT_PREFIX,
    logger,
)
from .client import SlurmClient
from .exceptions import BackendError, SlurmError
from .structures import Allocation, Quotas


class SlurmBackend:
    def __init__(
        self,
    ):
        self.client = SlurmClient(SLURM_DEPLOYMENT_TYPE)

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
                logger.error("Error while pulling allocation [%s]: %s", account.name, e)
        return report

    def ping(self, raise_exception=False):
        try:
            self.client.list_accounts()
        except SlurmError as e:
            if raise_exception:
                raise BackendError(e)
            return False
        else:
            return True

    def pull_allocation(self, account: str):
        users = self.client.list_account_users(account)

        report = self.get_usage_report([account])
        usage = report.get(account)
        if not usage:
            usage = {"TOTAL_ACCOUNT_USAGE": Quotas()}

        limits: Quotas = self.get_allocation_limits(account)
        return users, usage, limits

    def get_usage_report(self, accounts):
        report = {}
        lines = self.client.get_usage_report(accounts)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, Quotas())
            report[line.account][line.user] += line.quotas

        for usage in report.values():
            for user_usage in usage.values():
                user_usage.cpu = round(user_usage.cpu, 2)
                user_usage.gpu = round(user_usage.gpu, 2)
                user_usage.ram = round(user_usage.ram, 2)
            quotas = usage.values()
            total = reduce(operator.add, quotas)
            usage["TOTAL_ACCOUNT_USAGE"] = total

        return report

    def get_allocation_limits(self, account: str):
        lines = self.client.get_resource_limits(account)
        correct_lines = [
            association for association in lines if association.resource_limits
        ]
        if len(correct_lines) > 0:
            line = correct_lines[0]
            limits = Quotas(cpu=line.cpu, gpu=line.gpu, ram=line.ram)
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

    def set_allocation_limits(self, allocation: Allocation, limits_dict: dict):
        limits = Quotas(
            cpu=limits_dict["CPU"],
            gpu=limits_dict["GPU"],
            ram=limits_dict["RAM"],
        )
        self.client.set_resource_limits(allocation.backend_id, limits)
        return limits

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
        usernames: Set[str],
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

        if not self.client.get_account(allocation_account):
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

        limits = self.set_allocation_limits(allocation, SLURM_DEFAULT_LIMITS)
        added_users = self.add_users_to_account(allocation, usernames)
        return added_users, limits, allocation_account

    def sync_users(
        self,
        allocation: Allocation,
        usernames: Set[str],
        all_freeipa_usernames: Set[str],
    ):
        created_associations = self.add_users_to_account(allocation, usernames)

        all_backend_usernames = self.client.list_account_users(allocation.backend_id)
        backend_usernames = {
            username
            for username in all_backend_usernames
            if username in all_freeipa_usernames
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
                logger.error("Unable to create association in Slurm: %s", err)
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
                logger.error("Unable to delete association in Slurm: %s", err)
                return False
        return True
