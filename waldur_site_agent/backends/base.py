"""Generic client class."""

import abc
import subprocess
from typing import Dict, List, Optional

from waldur_site_agent.backends import logger
from waldur_site_agent.backends.exceptions import (
    BackendError,
)
from waldur_site_agent.backends.structures import Account, Association


class BaseClient:
    """Generic cli-client for a backend communication."""

    def execute_command(self, command: List[str]) -> str:
        """Execute command on backend."""
        try:
            logger.debug("Executing command: %s", " ".join(command))
            return subprocess.check_output(command, stderr=subprocess.STDOUT, encoding="utf-8")
        except subprocess.CalledProcessError as e:
            logger.exception('Failed to execute command "%s".', command)
            stdout = e.output or ""
            lines = stdout.splitlines()
            if len(lines) > 0 and lines[0].startswith("Warning: Permanently added"):
                lines = lines[1:]
            stdout = "\n".join(lines)
            raise BackendError(stdout) from e

    @abc.abstractmethod
    def list_accounts(self) -> List[Account]:
        """Get accounts list."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_account(self, name: str) -> Optional[Account]:
        """Get account info."""
        raise NotImplementedError

    @abc.abstractmethod
    def create_account(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create account in the cluster."""
        raise NotImplementedError

    @abc.abstractmethod
    def delete_account(self, name: str) -> str:
        """Delete account from the cluster."""
        raise NotImplementedError

    @abc.abstractmethod
    def set_resource_limits(self, account: str, limits_dict: Dict[str, int]) -> Optional[str]:
        """Set account limits."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_association(self, user: str, account: str) -> Optional[Association]:
        """Get association between user and account."""
        raise NotImplementedError

    @abc.abstractmethod
    def create_association(
        self, username: str, account: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between user and account."""
        raise NotImplementedError

    @abc.abstractmethod
    def delete_association(self, username: str, account: str) -> str:
        """Delete association between user and account."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_usage_report(self, accounts: List[str]) -> List:
        """Get usage records."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_account_users(self, account: str) -> List[str]:
        """Get account users."""
        raise NotImplementedError


class UnknownClient(BaseClient):
    """Unknown cli-client for a backend communication."""

    def list_accounts(self) -> List[Account]:
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

    def set_resource_limits(self, account: str, limits_dict: Dict[str, int]) -> Optional[str]:
        """Set account limits."""
        del account, limits_dict
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

    def get_usage_report(self, accounts: List[str]) -> List:
        """Get usages records."""
        return accounts

    def list_account_users(self, _: str) -> List[str]:
        """Get account users."""
        return []
