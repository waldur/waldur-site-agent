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
        """List all resources (accounts/allocations) on the backend.

        Returns:
            List of ClientResource objects. Each must have ``name`` (the backend ID)
            and ``organization`` (parent/project backend ID) populated.
        """

    @abc.abstractmethod
    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get a single resource by its backend ID.

        Args:
            resource_id: Backend identifier for the resource.

        Returns:
            ClientResource if found, None if the resource does not exist.
            Returning None signals to the caller that the resource is absent.
        """

    @abc.abstractmethod
    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create a new resource (account/allocation) on the backend.

        Args:
            name: Backend ID for the new resource.
            description: Human-readable resource name from Waldur.
            organization: Parent/project backend ID.
            parent_name: Optional parent resource name for hierarchical backends.

        Returns:
            A string identifier or command output confirming creation.

        Raises:
            BackendError: If creation fails.
        """

    @abc.abstractmethod
    def delete_resource(self, name: str) -> str:
        """Delete a resource from the backend.

        Args:
            name: Backend ID of the resource to delete.

        Returns:
            A string identifier or command output confirming deletion.

        Raises:
            BackendError: If deletion fails.
        """

    @abc.abstractmethod
    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set resource limits on the backend.

        Args:
            resource_id: Backend identifier for the resource.
            limits_dict: Component-to-value mapping in **backend-native units**
                (already converted via ``unit_factor``). Example:
                ``{"cpu": 60000, "mem": 61440}`` for SLURM minutes.

        Returns:
            Command output string, or None.

        Raises:
            BackendError: If setting limits fails.
        """

    @abc.abstractmethod
    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get current resource limits from the backend.

        Args:
            resource_id: Backend identifier for the resource.

        Returns:
            Component-to-value mapping in backend-native units.
            Example: ``{"cpu": 60000, "mem": 61440}``.
        """

    @abc.abstractmethod
    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for a resource.

        Args:
            resource_id: Backend identifier for the resource.

        Returns:
            Nested dict mapping username to component limits.
            Example: ``{"user1": {"cpu": 30000, "mem": 30720}}``.
        """

    @abc.abstractmethod
    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set resource limits for a specific user.

        Args:
            resource_id: Backend identifier for the resource.
            username: User whose limits are being set.
            limits_dict: Component-to-value mapping in backend-native units.

        Returns:
            Command output string confirming the operation.

        Raises:
            BackendError: If setting limits fails.
        """

    @abc.abstractmethod
    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Get association between a user and a resource.

        Args:
            user: Username to look up.
            resource_id: Backend identifier for the resource.

        Returns:
            Association object if the user is associated with the resource,
            None if no association exists.
        """

    @abc.abstractmethod
    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create an association between a user and a resource.

        Args:
            username: Username to associate.
            resource_id: Backend identifier for the resource.
            default_account: Optional default account for the user (from backend settings).

        Returns:
            Command output string confirming creation.

        Raises:
            BackendError: If association creation fails.
        """

    @abc.abstractmethod
    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete an association between a user and a resource.

        Args:
            username: Username to disassociate.
            resource_id: Backend identifier for the resource.

        Returns:
            Command output string confirming deletion.

        Raises:
            BackendError: If association deletion fails.
        """

    @abc.abstractmethod
    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get raw usage records from the backend.

        This returns raw backend-specific data structures that are then processed
        by ``BaseBackend._get_usage_report()`` into the standard usage report format.

        Args:
            resource_ids: List of backend resource identifiers.

        Returns:
            List of backend-specific usage record objects (e.g., ``SlurmReportLine``
            for SLURM). The exact type depends on the backend implementation.
        """

    @abc.abstractmethod
    def list_resource_users(self, resource_id: str) -> list[str]:
        """List all users associated with a resource.

        Args:
            resource_id: Backend identifier for the resource.

        Returns:
            List of usernames associated with the resource.
        """

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
