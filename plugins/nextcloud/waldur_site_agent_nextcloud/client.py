"""Nextcloud client for waldur site agent.

Communicates with Nextcloud via OCS API v2 and the Group Folders app API.
Each Waldur resource maps to a Nextcloud Group + Group Folder, giving
per-folder quota without touching per-user quotas.
"""

import contextlib
import logging
from typing import Any, Optional

import httpx
from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.structures import Association, ClientResource

from waldur_site_agent_nextcloud.exceptions import (
    NextcloudAPIError,
    NextcloudAuthError,
    NextcloudGroupError,
    NextcloudGroupFolderError,
)

logger = logging.getLogger(__name__)

_GIB = 1024**3


def _bytes_to_gib(value: int) -> float:
    """Convert bytes to GiB rounded to 2 decimal places."""
    return round(value / _GIB, 2) if value > 0 else 0.0


def _gib_to_bytes(value: int) -> int:
    """Convert GiB to bytes."""
    return int(value) * _GIB


class NextcloudClient(BaseClient):
    """Client for the Nextcloud OCS and Group Folders APIs."""

    def __init__(
        self, nextcloud_url: str, admin_username: str, admin_password: str
    ) -> None:
        """Initialise client with admin credentials.

        Args:
            nextcloud_url: Base URL of the Nextcloud instance (no trailing slash).
            admin_username: Admin account username.
            admin_password: Admin account password.
        """
        super().__init__()
        self.nextcloud_url = nextcloud_url.rstrip("/")
        self.ocs_base = f"{self.nextcloud_url}/ocs/v2.php"
        self.gf_base = f"{self.nextcloud_url}/apps/groupfolders/folders"
        self.session = httpx.Client(
            auth=(admin_username, admin_password),
            headers={"OCS-APIRequest": "true", "Accept": "application/json"},
            timeout=30.0,
        )

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _make_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Execute an HTTP request with shared auth and headers.

        Args:
            method: HTTP method (GET, POST, DELETE, …).
            url: Full URL to request.
            **kwargs: Extra arguments forwarded to ``httpx.request``.

        Returns:
            The HTTP response.

        Raises:
            NextcloudAuthError: On 401 responses.
            NextcloudAPIError: On other HTTP or connection errors.
        """
        try:
            response = self.session.request(method, url, **kwargs)
            if response.status_code == 401:
                raise NextcloudAuthError("Authentication failed with Nextcloud API")
            response.raise_for_status()
        except httpx.HTTPError as exc:
            if isinstance(exc, httpx.ConnectError):
                raise NextcloudAPIError(
                    f"Cannot connect to Nextcloud at {self.nextcloud_url}"
                ) from exc
            details = ""
            if isinstance(exc, httpx.HTTPStatusError):
                with contextlib.suppress(Exception):
                    details = f" | {exc.response.status_code} | {exc.response.text[:500]}"
            raise NextcloudAPIError(f"API request failed: {exc}{details}") from exc

        return response

    def _parse_ocs(self, response: httpx.Response) -> Any:
        """Parse an OCS v2 response and return the ``data`` field.

        Raises:
            NextcloudAPIError: If the OCS status code indicates failure.
        """
        body = response.json()
        ocs = body.get("ocs", {})
        meta = ocs.get("meta", {})
        code = meta.get("statuscode", 0)
        if code not in (100, 200):
            raise NextcloudAPIError(
                f"OCS error {code}: {meta.get('message', 'unknown error')}"
            )
        return ocs.get("data")

    def ping(self) -> bool:
        """Return True if Nextcloud is reachable and reports installed=true."""
        try:
            response = self._make_request(
                "GET", f"{self.nextcloud_url}/status.php"
            )
            return response.status_code == 200 and response.json().get(
                "installed", False
            )
        except (NextcloudAPIError, NextcloudAuthError):
            return False

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    def user_exists(self, username: str) -> bool:
        """Return True if the user account exists in Nextcloud."""
        try:
            self._make_request("GET", f"{self.ocs_base}/cloud/users/{username}")
            return True
        except NextcloudAuthError:
            raise
        except NextcloudAPIError:
            return False

    def get_user(self, username: str) -> dict:
        """Return user details dict.

        Raises:
            NextcloudAPIError: If the user does not exist or the call fails.
        """
        response = self._make_request("GET", f"{self.ocs_base}/cloud/users/{username}")
        return self._parse_ocs(response)

    # ------------------------------------------------------------------
    # Group management
    # ------------------------------------------------------------------

    def group_exists(self, group_id: str) -> bool:
        """Return True if the group exists."""
        try:
            response = self._make_request(
                "GET", f"{self.ocs_base}/cloud/groups/{group_id}"
            )
            self._parse_ocs(response)
            return True
        except NextcloudAuthError:
            raise
        except NextcloudAPIError:
            return False

    def create_group(self, group_id: str) -> None:
        """Create a new group.  No-ops if it already exists.

        Args:
            group_id: Unique group identifier.

        Raises:
            NextcloudGroupError: On API failure.
        """
        if self.group_exists(group_id):
            logger.info("Nextcloud group %s already exists", group_id)
            return
        try:
            response = self._make_request(
                "POST",
                f"{self.ocs_base}/cloud/groups",
                data={"groupid": group_id},
            )
            self._parse_ocs(response)
            logger.info("Created Nextcloud group: %s", group_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupError(
                f"Failed to create group {group_id}: {exc}"
            ) from exc

    def delete_group(self, group_id: str) -> None:
        """Delete a group.  No-ops if it does not exist.

        Args:
            group_id: Group identifier to delete.

        Raises:
            NextcloudGroupError: On API failure.
        """
        if not self.group_exists(group_id):
            logger.warning("Nextcloud group %s does not exist, skipping delete", group_id)
            return
        try:
            response = self._make_request(
                "DELETE", f"{self.ocs_base}/cloud/groups/{group_id}"
            )
            self._parse_ocs(response)
            logger.info("Deleted Nextcloud group: %s", group_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupError(
                f"Failed to delete group {group_id}: {exc}"
            ) from exc

    def list_group_members(self, group_id: str) -> list[str]:
        """Return list of usernames that belong to the group."""
        try:
            response = self._make_request(
                "GET", f"{self.ocs_base}/cloud/groups/{group_id}"
            )
            data = self._parse_ocs(response)
            return data.get("users", [])
        except NextcloudAPIError:
            logger.exception("Failed to list members of group %s", group_id)
            return []

    def add_user_to_group(self, username: str, group_id: str) -> None:
        """Add *username* to *group_id*.

        Raises:
            NextcloudGroupError: On API failure.
        """
        try:
            response = self._make_request(
                "POST",
                f"{self.ocs_base}/cloud/users/{username}/groups",
                data={"groupid": group_id},
            )
            # OCS 102 = "User already in group" — idempotent, not an error
            if response.json().get("ocs", {}).get("meta", {}).get("statuscode") == 102:
                logger.debug("User %s is already in group %s", username, group_id)
                return
            self._parse_ocs(response)
            logger.info("Added user %s to group %s", username, group_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupError(
                f"Failed to add user {username} to group {group_id}: {exc}"
            ) from exc

    def remove_user_from_group(self, username: str, group_id: str) -> None:
        """Remove *username* from *group_id*.

        Raises:
            NextcloudGroupError: On API failure.
        """
        try:
            response = self._make_request(
                "DELETE",
                f"{self.ocs_base}/cloud/users/{username}/groups",
                data={"groupid": group_id},
            )
            self._parse_ocs(response)
            logger.info("Removed user %s from group %s", username, group_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupError(
                f"Failed to remove user {username} from group {group_id}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Group Folders
    # ------------------------------------------------------------------

    def list_group_folders(self) -> list[dict]:
        """Return all group folders as a list of dicts."""
        try:
            response = self._make_request("GET", self.gf_base)
            data = self._parse_ocs(response)
            if isinstance(data, dict):
                return list(data.values())
            return list(data) if data else []
        except NextcloudAPIError:
            logger.exception("Failed to list group folders")
            return []

    def create_group_folder(self, mount_point: str) -> int:
        """Create a new group folder.

        Args:
            mount_point: The path/name for the folder as seen by users.

        Returns:
            The integer folder ID assigned by Nextcloud.

        Raises:
            NextcloudGroupFolderError: On API failure.
        """
        try:
            response = self._make_request(
                "POST",
                self.gf_base,
                data={"mountpoint": mount_point},
            )
            data = self._parse_ocs(response)
            folder_id = int(data["id"])
            logger.info(
                "Created group folder '%s' with id %d", mount_point, folder_id
            )
            return folder_id
        except (NextcloudAPIError, KeyError, ValueError) as exc:
            raise NextcloudGroupFolderError(
                f"Failed to create group folder '{mount_point}': {exc}"
            ) from exc

    def get_group_folder(self, folder_id: int) -> dict:
        """Return group folder metadata dict.

        Raises:
            NextcloudGroupFolderError: If the folder does not exist or the call fails.
        """
        try:
            response = self._make_request(
                "GET", f"{self.gf_base}/{folder_id}"
            )
            return self._parse_ocs(response)
        except NextcloudAPIError as exc:
            raise NextcloudGroupFolderError(
                f"Failed to get group folder {folder_id}: {exc}"
            ) from exc

    def delete_group_folder(self, folder_id: int) -> None:
        """Delete a group folder.

        Raises:
            NextcloudGroupFolderError: On API failure.
        """
        try:
            response = self._make_request(
                "DELETE",
                f"{self.gf_base}/{folder_id}",
            )
            self._parse_ocs(response)
            logger.info("Deleted group folder %d", folder_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupFolderError(
                f"Failed to delete group folder {folder_id}: {exc}"
            ) from exc

    def add_group_to_folder(self, folder_id: int, group_id: str) -> None:
        """Grant *group_id* access to *folder_id*.

        Raises:
            NextcloudGroupFolderError: On API failure.
        """
        try:
            response = self._make_request(
                "POST",
                f"{self.gf_base}/{folder_id}/groups",
                data={"group": group_id},
            )
            self._parse_ocs(response)
            logger.info("Granted group %s access to folder %d", group_id, folder_id)
        except NextcloudAPIError as exc:
            raise NextcloudGroupFolderError(
                f"Failed to add group {group_id} to folder {folder_id}: {exc}"
            ) from exc

    def set_group_permissions(
        self, folder_id: int, group_id: str, permissions: int
    ) -> None:
        """Set the permission bitmask for a group on a folder.

        Bitmask: 1=Read, 2=Update, 4=Create, 8=Delete, 16=Share (31=all).
        Use 15 to grant all permissions except resharing.

        Raises:
            NextcloudGroupFolderError: On API failure.
        """
        try:
            response = self._make_request(
                "POST",
                f"{self.gf_base}/{folder_id}/groups/{group_id}",
                data={"permissions": permissions},
            )
            self._parse_ocs(response)
            logger.info(
                "Set permissions %d for group %s on folder %d",
                permissions, group_id, folder_id,
            )
        except NextcloudAPIError as exc:
            raise NextcloudGroupFolderError(
                f"Failed to set permissions for group {group_id} on folder {folder_id}: {exc}"
            ) from exc

    def set_folder_quota(self, folder_id: int, quota_bytes: int) -> None:
        """Set the storage quota for a group folder.

        Args:
            folder_id: Nextcloud folder ID.
            quota_bytes: Quota in bytes; use -3 for unlimited.

        Raises:
            NextcloudGroupFolderError: On API failure.
        """
        try:
            response = self._make_request(
                "POST",
                f"{self.gf_base}/{folder_id}/quota",
                data={"quota": quota_bytes},
            )
            self._parse_ocs(response)
            logger.info(
                "Set quota for folder %d to %d bytes", folder_id, quota_bytes
            )
        except NextcloudAPIError as exc:
            raise NextcloudGroupFolderError(
                f"Failed to set quota for folder {folder_id}: {exc}"
            ) from exc

    def get_folder_usage(self, folder_id: int) -> int:
        """Return bytes currently used in a group folder.

        Raises rather than degrading to 0: a transient Nextcloud failure must
        not be indistinguishable from genuinely empty storage, or the caller
        would report 0 usage over the period's real usage.  Callers are
        expected to omit the folder from their report instead.

        Raises:
            NextcloudGroupFolderError: If the folder lookup fails.
            ValueError: If the response carries a non-numeric size.
        """
        folder = self.get_group_folder(folder_id)
        return int(folder.get("size", 0))

    # ------------------------------------------------------------------
    # BaseClient abstract method implementations
    # ------------------------------------------------------------------

    def list_resources(self) -> list[ClientResource]:
        """Return all group folders as ClientResource objects."""
        folders = self.list_group_folders()
        return [
            ClientResource(
                name=folder.get("mount_point", ""),
                backend_id=str(folder.get("id", "")),
                description=folder.get("mount_point", ""),
            )
            for folder in folders
        ]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Return a ClientResource for the group folder identified by *resource_id*."""
        try:
            folder = self.get_group_folder(int(resource_id))
            return ClientResource(
                name=folder.get("mount_point", ""),
                backend_id=str(folder.get("id", resource_id)),
                description=folder.get("mount_point", ""),
            )
        except (NextcloudGroupFolderError, ValueError):
            return None

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Create a group and a group folder with matching name.

        Returns the new folder ID as a string (used as backend_id).
        """
        self.create_group(name)
        folder_id = self.create_group_folder(name)
        self.add_group_to_folder(folder_id, name)
        return str(folder_id)

    def delete_resource(self, resource_id: str) -> str:
        """Delete the group folder only.

        Group deletion is intentionally omitted here: the folder may have
        admin-attached groups that should not be removed.  The backend
        overrides this method and deletes the owned group explicitly via
        ``_group_id``.
        """
        self.delete_group_folder(int(resource_id))
        return resource_id

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Not used directly — user sync goes through group membership."""
        return []

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Return the storage quota in GiB for the folder."""
        try:
            folder = self.get_group_folder(int(resource_id))
            quota_bytes = int(folder.get("quota", -3))
            if quota_bytes == -3 or quota_bytes < 0:
                return {"storage": 0}
            return {"storage": int(_bytes_to_gib(quota_bytes))}
        except (NextcloudGroupFolderError, ValueError):
            return {}

    def set_resource_limits(
        self, resource_id: str, limits: dict[str, int]
    ) -> Optional[str]:
        """Update the storage quota for the folder."""
        storage_gb = limits.get("storage", 0)
        self.set_folder_quota(int(resource_id), _gib_to_bytes(storage_gb))
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """User membership is managed at the backend level via group operations."""
        return ""

    def get_association(
        self, username: str, resource_id: str
    ) -> Optional[Association]:
        """Not used — user sync is group-based."""
        return None

    def delete_association(self, username: str, resource_id: str) -> str:
        """User membership is managed at the backend level via group operations."""
        return ""

    def get_resource_user_limits(
        self, resource_id: str
    ) -> dict[str, dict[str, int]]:
        """Per-user quotas are not used — quota lives on the folder."""
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits: dict[str, int]
    ) -> str:
        """Per-user quotas are not used — quota lives on the folder."""
        return ""

    def get_usage_report(
        self, resource_ids: list[str], timezone: Optional[str] = None
    ) -> list:
        """Return usage records for the given folder IDs."""
        records = []
        for resource_id in resource_ids:
            try:
                usage_bytes = self.get_folder_usage(int(resource_id))
                records.append(
                    {
                        "resource_id": resource_id,
                        "storage": _bytes_to_gib(usage_bytes),
                    }
                )
            except Exception:
                logger.exception("Failed to get usage for folder %s", resource_id)
        return records
