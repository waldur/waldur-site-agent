"""Generic client class."""

import abc
import subprocess
from typing import Optional

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import (
    BackendError,
)
from waldur_site_agent.backend.structures import Association, ClientResource


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
    def list_resources(self) -> list[ClientResource]:
        """Get resource list."""

    @abc.abstractmethod
    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get the resource's info."""

    @abc.abstractmethod
    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create a resource in the cluster."""

    @abc.abstractmethod
    def delete_resource(self, name: str) -> str:
        """Delete a resource from the cluster."""

    @abc.abstractmethod
    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set account limits."""

    @abc.abstractmethod
    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get account limits."""

    @abc.abstractmethod
    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""

    @abc.abstractmethod
    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set resource limits for a specific user."""

    @abc.abstractmethod
    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Get association between the user and the resource."""

    @abc.abstractmethod
    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between the user and the resource."""

    @abc.abstractmethod
    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between the user and the resource."""

    @abc.abstractmethod
    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get usage records."""

    @abc.abstractmethod
    def list_resource_users(self, resource_id: str) -> list[str]:
        """Get resource users."""

    def create_linux_user_homedir(self, username: str, umask: str = "") -> str:
        """Creates homedir for the user in Linux system."""
        command = ["/sbin/mkhomedir_helper", username, umask]
        return self.execute_command(command)


class UnknownClient(BaseClient):
    """Unknown cli-client for a backend communication."""

    def list_resources(self) -> list[ClientResource]:
        """Get accounts list."""
        return []

    def get_resource(self, _: str) -> Optional[ClientResource]:
        """Get resource info."""
        return None

    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create resource in the cluster."""
        del description, organization, parent_name
        return name

    def delete_resource(self, name: str) -> str:
        """Delete resource from the cluster."""
        return name

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set resource limits."""
        del resource_id, limits_dict
        return ""

    def get_resource_limits(self, _: str) -> dict[str, int]:
        """Get resource limits."""
        return {}

    def get_resource_user_limits(self, _: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account."""
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set resource limits for a specific user."""
        del resource_id, username, limits_dict
        return ""

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Get association between user and account."""
        del user, resource_id
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between user and account."""
        del resource_id, default_account
        return username

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between user and account."""
        del resource_id
        return username

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get usages records."""
        return resource_ids

    def list_resource_users(self, _: str) -> list[str]:
        """Get resource users."""
        return []
