"""DigitalOcean client wrapper for Waldur Site Agent."""

from __future__ import annotations

import functools
import logging
from typing import Callable, Optional, TypeVar

import digitalocean

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource

logger = logging.getLogger(__name__)
ReturnType = TypeVar("ReturnType")


class DigitalOceanBackendError(BackendError):
    """Base error for DigitalOcean backend operations."""


class TokenScopeError(DigitalOceanBackendError):
    """Raised when token does not have required scopes."""


class NotFoundError(DigitalOceanBackendError):
    """Raised when a DigitalOcean resource is not found."""


class UnauthorizedError(DigitalOceanBackendError):
    """Raised when DigitalOcean authentication fails."""


def _map_do_error(error: digitalocean.DataReadError) -> DigitalOceanBackendError:
    error_messages = {
        "You do not have access for the attempted action.": TokenScopeError,
        "The resource you were accessing could not be found.": NotFoundError,
        "Unable to authenticate you.": UnauthorizedError,
    }
    error_cls = error_messages.get(str(error), DigitalOceanBackendError)
    return error_cls(str(error))


def digitalocean_error_handler(
    func: Callable[..., ReturnType],
) -> Callable[..., ReturnType]:
    """Convert DigitalOcean exceptions to backend-specific errors."""

    @functools.wraps(func)
    def wrapped(*args: object, **kwargs: object) -> ReturnType:
        logger.debug("Executing DO client method: %s", func.__name__)
        try:
            return func(*args, **kwargs)
        except digitalocean.DataReadError as e:
            raise _map_do_error(e) from e

    return wrapped


class DigitalOceanClient(BaseClient):
    """DigitalOcean API client wrapper."""

    def __init__(self, token: str) -> None:
        """Initialize DigitalOcean client."""
        self.token = token
        self.manager = digitalocean.Manager(token=token)

    def ping(self) -> bool:
        """Check if DigitalOcean API is reachable."""
        try:
            self.manager.get_account()
        except digitalocean.DataReadError:
            logger.exception("DigitalOcean API ping failed")
            return False
        return True

    @digitalocean_error_handler
    def list_resources(self) -> list[ClientResource]:
        """Return all droplets as client resources."""
        droplets = self.manager.get_all_droplets()
        return [
            ClientResource(
                name=str(droplet.id),
                description=droplet.name or "",
                organization="",
                backend_id=str(droplet.id),
            )
            for droplet in droplets
        ]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get droplet info as client resource."""
        try:
            droplet = self.manager.get_droplet(resource_id)
        except digitalocean.DataReadError as e:
            mapped_error = _map_do_error(e)
            if isinstance(mapped_error, NotFoundError):
                return None
            raise mapped_error from e
        return ClientResource(
            name=str(droplet.id),
            description=droplet.name or "",
            organization="",
            backend_id=str(droplet.id),
        )

    @digitalocean_error_handler
    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create droplet placeholder resource."""
        del description, organization, parent_name
        droplet = digitalocean.Droplet(token=self.token, name=name)
        droplet.create()
        return str(droplet.id)

    @digitalocean_error_handler
    def delete_resource(self, name: str) -> str:
        """Delete droplet by ID."""
        droplet = self.manager.get_droplet(name)
        droplet.destroy()
        return name

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """No-op for DigitalOcean."""
        del resource_id, limits_dict
        return None

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Return empty limits for base client API."""
        del resource_id
        return {}

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Return empty per-user limits."""
        del resource_id
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """No-op for per-user limits."""
        del resource_id, username, limits_dict
        return ""

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """No-op for associations."""
        del user, resource_id
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """No-op association create."""
        del resource_id, default_account
        return username

    def delete_association(self, username: str, resource_id: str) -> str:
        """No-op association delete."""
        del resource_id
        return username

    def get_usage_report(
        self, resource_ids: list[str], timezone: str | None = None
    ) -> list:
        """No-op usage report."""
        del resource_ids, timezone
        return []

    def list_resource_users(self, resource_id: str) -> list[str]:
        """No-op resource users."""
        del resource_id
        return []

    @digitalocean_error_handler
    def create_droplet(
        self,
        name: str,
        region: str,
        image: str | int,
        size_slug: str,
        user_data: Optional[str] = None,
        ssh_key_ids: Optional[list[int]] = None,
        tags: Optional[list[str]] = None,
    ) -> digitalocean.Droplet:
        """Create a DigitalOcean droplet."""
        droplet = digitalocean.Droplet(
            token=self.token,
            name=name,
            user_data=user_data or "",
            region=region,
            image=image,
            size_slug=size_slug,
            ssh_keys=ssh_key_ids or [],
            tags=tags or [],
        )
        droplet.create()
        return droplet

    def get_droplet(self, droplet_id: str) -> Optional[digitalocean.Droplet]:
        """Fetch droplet by ID."""
        try:
            return self.manager.get_droplet(droplet_id)
        except digitalocean.DataReadError as e:
            mapped_error = _map_do_error(e)
            if isinstance(mapped_error, NotFoundError):
                return None
            raise mapped_error from e

    @digitalocean_error_handler
    def shutdown_droplet(self, droplet_id: str) -> Optional[int]:
        """Shutdown droplet and return action ID."""
        droplet = self.manager.get_droplet(droplet_id)
        action = droplet.shutdown()
        return action.get("action", {}).get("id")

    @digitalocean_error_handler
    def power_on_droplet(self, droplet_id: str) -> Optional[int]:
        """Power on droplet and return action ID."""
        droplet = self.manager.get_droplet(droplet_id)
        action = droplet.power_on()
        return action.get("action", {}).get("id")

    @digitalocean_error_handler
    def reboot_droplet(self, droplet_id: str) -> Optional[int]:
        """Reboot droplet and return action ID."""
        droplet = self.manager.get_droplet(droplet_id)
        action = droplet.reboot()
        return action.get("action", {}).get("id")

    @digitalocean_error_handler
    def resize_droplet(self, droplet_id: str, size_slug: str, disk: bool = False) -> int:
        """Resize droplet and return action ID."""
        droplet = self.manager.get_droplet(droplet_id)
        action = droplet.resize(new_size_slug=size_slug, disk=disk)
        return action.get("action", {}).get("id")

    @digitalocean_error_handler
    def load_ssh_key(
        self,
        ssh_key_id: Optional[int] = None,
        ssh_key_fingerprint: Optional[str] = None,
        ssh_key_name: Optional[str] = None,
    ) -> digitalocean.SSHKey:
        """Load existing SSH key from DigitalOcean."""
        ssh_key = digitalocean.SSHKey(
            token=self.token,
            id=ssh_key_id,
            fingerprint=ssh_key_fingerprint,
            name=ssh_key_name,
        )
        ssh_key.load()
        return ssh_key

    @digitalocean_error_handler
    def create_ssh_key(self, name: str, public_key: str) -> digitalocean.SSHKey:
        """Create a new SSH key in DigitalOcean."""
        ssh_key = digitalocean.SSHKey(token=self.token, name=name, public_key=public_key)
        ssh_key.create()
        return ssh_key

    def resolve_ssh_key(
        self,
        ssh_key_id: Optional[int] = None,
        ssh_key_fingerprint: Optional[str] = None,
        ssh_key_public_key: Optional[str] = None,
        ssh_key_name: Optional[str] = None,
    ) -> Optional[digitalocean.SSHKey]:
        """Resolve or create an SSH key for droplet provisioning."""
        if ssh_key_id or ssh_key_fingerprint:
            try:
                return self.load_ssh_key(
                    ssh_key_id=ssh_key_id,
                    ssh_key_fingerprint=ssh_key_fingerprint,
                    ssh_key_name=ssh_key_name,
                )
            except NotFoundError:
                return None

        if ssh_key_public_key:
            key_name = ssh_key_name or "waldur-site-agent"
            try:
                return self.load_ssh_key(ssh_key_name=key_name)
            except NotFoundError:
                return self.create_ssh_key(key_name, ssh_key_public_key)
        return None
