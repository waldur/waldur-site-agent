"""Generic client class."""

import abc
import subprocess
from typing import Optional

from waldur_site_agent.backends import logger
from waldur_site_agent.backends.exceptions import (
    BackendError,
)
from waldur_site_agent.backends.structures import Account, Association


class BaseClient:
    """Generic cli-client for a backend communication."""

    def execute_command(self, command: list[str], silent: bool = False) -> str:
        """Execute command on backend."""
        try:
            logger.debug("Executing command: %s", " ".join(command))
            return subprocess.check_output(command, stderr=subprocess.STDOUT, encoding="utf-8")
        except subprocess.CalledProcessError as e:
            if not silent:
                logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            stdout = "\n".join(lines)
            raise BackendError(stdout) from e

    @abc.abstractmethod
    def list_accounts(self) -> list[Account]:
        """Get accounts list."""

    @abc.abstractmethod
    def get_account(self, name: str) -> Optional[Account]:
        """Get account info."""

    @abc.abstractmethod
    def create_account(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create account in the cluster."""

    @abc.abstractmethod
    def delete_account(self, name: str) -> str:
        """Delete account from the cluster."""

    @abc.abstractmethod
    def set_resource_limits(self, account: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set account limits."""

    @abc.abstractmethod
    def get_resource_limits(self, account: str) -> dict[str, int]:
        """Get account limits."""

    @abc.abstractmethod
    def get_resource_user_limits(self, account: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""

    @abc.abstractmethod
    def set_resource_user_limits(
        self, account: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set account limits for a specific user."""

    @abc.abstractmethod
    def get_association(self, user: str, account: str) -> Optional[Association]:
        """Get association between user and account."""

    @abc.abstractmethod
    def create_association(
        self, username: str, account: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between user and account."""

    @abc.abstractmethod
    def delete_association(self, username: str, account: str) -> str:
        """Delete association between user and account."""

    @abc.abstractmethod
    def get_usage_report(self, accounts: list[str]) -> list:
        """Get usage records."""

    @abc.abstractmethod
    def list_account_users(self, account: str) -> list[str]:
        """Get account users."""


class UnknownClient(BaseClient):
    """Unknown cli-client for a backend communication."""

    def list_accounts(self) -> list[Account]:
        """Get accounts list."""
        return []

    def get_account(self, _: str) -> Optional[Account]:
        """Get account info."""
        return None

    def create_account(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create account in the cluster."""
        del description, organization, parent_name
        return name

    def delete_account(self, name: str) -> str:
        """Delete account from the cluster."""
        return name

    def set_resource_limits(self, account: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set account limits."""
        del account, limits_dict
        return ""

    def get_resource_limits(self, _: str) -> dict[str, int]:
        """Get account limits."""
        return {}

    def get_resource_user_limits(self, _: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""
        return {}

    def set_resource_user_limits(
        self, account: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set account limits for a specific user."""
        del account, username, limits_dict
        return ""

    def get_association(self, user: str, account: str) -> Optional[Association]:
        """Get association between user and account."""
        del user, account
        return None

    def create_association(
        self, username: str, account: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between user and account."""
        del account, default_account
        return username

    def delete_association(self, username: str, account: str) -> str:
        """Delete association between user and account."""
        del account
        return username

    def get_usage_report(self, accounts: list[str]) -> list:
        """Get usages records."""
        return accounts

    def list_account_users(self, _: str) -> list[str]:
        """Get account users."""
        return []
