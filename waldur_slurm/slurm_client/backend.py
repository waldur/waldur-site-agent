import dataclasses
import logging
import operator
import re
from functools import reduce
from typing import List

from . import (
    SLURM_ALLOCATION_NAME_MAX_LEN,
    SLURM_ALLOCATION_PREFIX,
    SLURM_ALLOCATION_REGEX,
    SLURM_CUSTOMER_PREFIX,
    SLURM_DEFAULT_ACCOUNT,
    SLURM_DEFAULT_LIMITS,
    SLURM_DEPLOYMENT_TYPE,
    SLURM_PROJECT_PREFIX,
)
from .client import SlurmClient
from .exceptions import BackendError, SlurmError
from .structures import Allocation, Quotas

logger = logging.getLogger(__name__)


class SlurmBackend:
    def __init__(
        self,
    ):
        self.client = SlurmClient(SLURM_DEPLOYMENT_TYPE)

    def pull_allocations(self):
        report = {}
        for account in self.client.list_accounts():
            try:
                logger.debug("About to pull allocation %s", account.name)
                users, usage, limits = self.pull_allocation(account.name)
                for user, user_usage in usage.items():
                    usage[user] = dataclasses.asdict(user_usage)
                report[account.name] = {
                    "users": users,
                    "usage": usage,
                    "limits": limits
                    and dataclasses.asdict(limits),  # limits can be None
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

    def set_resource_limits(self, allocation: Allocation, limits: dict):
        # TODO: add default limits configuration
        # (https://opennode.atlassian.net/browse/WAL-3037)
        limits = Quotas(
            cpu=limits["CPU"],
            gpu=limits["GPU"],
            ram=limits["RAM"],
        )
        self.client.set_resource_limits(allocation.backend_id, limits)

    def delete_customer(self, customer_backend_id):
        self.client.delete_account(customer_backend_id)

    def delete_project(self, project_backend_id):
        self.client.delete_account(project_backend_id)

    def delete_allocation(
        self,
        allocation: Allocation,
        project_backend_id,  # customer_backend_id
    ):
        account = allocation.backend_id

        if not account.strip():
            raise BackendError("Empty backend_id for allocation: %s" % allocation)

        if self.client.get_account(account):
            self.client.delete_account(account)

        if (
            len(
                [
                    account
                    for account in self.client.list_accounts()
                    if account.organization == project_backend_id
                    and account.name != project_backend_id
                ]
            )
            == 0
        ):
            self.delete_project(project_backend_id)

        # TODO: delete customer if it hasn't any associated allocations
        # if (
        #     self.get_allocation_queryset()
        #     .filter(project__customer=project.customer)
        #     .count()
        #     == 0
        # ):
        #     self.delete_customer(customer_backend_id)

    def create_allocation(
        self,
        allocation: Allocation,
        customer_name: str,
        project_name: str,
        usernames: List[str],
        freeipa_usernames: List[str] = None,
    ):
        customer_account = self.get_customer_name(customer_name)
        project_account = self.get_project_name(project_name)
        allocation_account = self.get_allocation_name(allocation)

        if not self.client.get_account(customer_account):
            self.client.create_account(customer_account, customer_name, customer_name)

        if not self.client.get_account(project_account):
            self.client.create_account(
                project_account,
                project_name,
                project_account,
                parent_name=customer_account,
            )

        if not self.client.get_account(allocation_account):
            self.client.create_account(
                name=allocation_account,
                description=allocation.name,
                organization=project_account,
            )
        allocation.backend_id = allocation_account

        self.set_resource_limits(allocation, SLURM_DEFAULT_LIMITS)
        return self.sync_users(allocation, usernames, freeipa_usernames)

    def sync_users(
        self,
        allocation: Allocation,
        usernames_unfiltered: List[str],
        freeipa_usernames: dict = None,
    ):
        created_associations = []
        removed_associations = []
        if freeipa_usernames:
            freeipa_usernames = [username.lower() for username in freeipa_usernames]
            usernames = list(set(usernames_unfiltered) & set(freeipa_usernames))
        else:
            usernames = usernames_unfiltered
        for username in usernames:
            succeeded = self.add_user(allocation, username)
            if succeeded:
                created_associations.append(username)

        all_backend_usernames = self.client.list_account_users(allocation.backend_id)
        if freeipa_usernames:
            backend_usernames = [
                username
                for username in freeipa_usernames
                if username in all_backend_usernames
            ]
        else:
            backend_usernames = all_backend_usernames

        stale_usernames = set(backend_usernames) - set(usernames)

        for username in stale_usernames:
            succeeded = self.delete_user(allocation, username)
            if succeeded:
                removed_associations.append(username)

        return created_associations, removed_associations

    def add_user(self, allocation: Allocation, username: str):
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

    def delete_user(self, allocation, username):
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
