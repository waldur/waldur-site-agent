"""Nextcloud backend for waldur site agent.

Each Waldur resource becomes a Nextcloud Group Folder with its own storage
quota.  A dedicated Nextcloud group controls who can access the folder.
Users added to the resource are added to that group and gain access as soon
as they log in via Keycloak SSO.

Mapping:
  Waldur resource  →  Nextcloud group  +  Group Folder (quota from plan)
  User on resource →  group membership
  Usage reporting  →  Group Folder bytes used
"""

import logging
import re
from typing import Optional

from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent.backend import backends, structures
from waldur_site_agent.backend.exceptions import BackendError, DuplicateResourceError

from waldur_site_agent_nextcloud.client import NextcloudClient, _bytes_to_gib, _gib_to_bytes
from waldur_site_agent_nextcloud.exceptions import (
    NextcloudError,
    NextcloudGroupError,
    NextcloudGroupFolderError,
)

logger = logging.getLogger(__name__)


class NextcloudBackend(backends.BaseBackend):
    """Nextcloud backend implementation for Waldur Site Agent."""

    supports_decreasing_usage = True

    def __init__(
        self,
        nextcloud_settings: dict,
        nextcloud_components: dict[str, dict],
    ) -> None:
        """Initialise the backend.

        Args:
            nextcloud_settings: Must contain ``nextcloud_url``,
                ``admin_username``, ``admin_password``.  Optional keys:
                ``group_prefix`` (default ``"waldur-"``),
                ``default_storage_quota_gb`` (default ``25``).
            nextcloud_components: Must contain a ``storage`` component with
                ``accounting_type == "limit"``.
        """
        super().__init__(nextcloud_settings, nextcloud_components)
        self.backend_type = "nextcloud"

        required = ["nextcloud_url", "admin_username", "admin_password"]
        for key in required:
            if key not in nextcloud_settings:
                raise ValueError(f"Missing required Nextcloud setting: {key}")

        if "storage" not in nextcloud_components:
            raise ValueError("Nextcloud backend requires a 'storage' component")

        if nextcloud_components["storage"].get("accounting_type") != "limit":
            raise ValueError(
                "Nextcloud backend storage component must have accounting_type='limit'"
            )

        self.group_prefix: str = nextcloud_settings.get("group_prefix", "waldur-")
        self.default_storage_quota_gb: int = nextcloud_settings.get(
            "default_storage_quota_gb", 25
        )
        self.allow_resharing: bool = nextcloud_settings.get("allow_resharing", False)
        # Retry team fetch up to 4 times to handle the race where the create-order
        # event arrives before Waldur commits the initial team membership row.
        self.team_fetch_attempts: int = 4

        self.client = NextcloudClient(
            nextcloud_url=nextcloud_settings["nextcloud_url"],
            admin_username=nextcloud_settings["admin_username"],
            admin_password=nextcloud_settings["admin_password"],
        )

        logger.info(
            "Initialised Nextcloud backend for %s (group_prefix=%r, default_quota=%d GiB)",
            nextcloud_settings["nextcloud_url"],
            self.group_prefix,
            self.default_storage_quota_gb,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _group_id(self, waldur_resource: WaldurResource) -> str:
        """Derive a stable Nextcloud group name from the Waldur resource UUID."""
        return f"{self.group_prefix}{waldur_resource.uuid}"

    @staticmethod
    def _sanitize_path_component(name: str) -> str:
        """Replace WebDAV-forbidden characters in a mount-point path segment."""
        sanitized = re.sub(r'[/\\:*?"<>|]', "-", name)
        sanitized = re.sub(r"-{2,}", "-", sanitized)
        return sanitized.strip("-. ") or "unnamed"

    def _find_orphaned_folder(self, mount_point: str, group_id: str) -> Optional[int]:
        """Return the ID of a folder with *mount_point* that lacks *group_id*.

        Used by create_resource to recover when the folder was created in a
        previous (partial) attempt but the group was never attached.
        """
        if not isinstance(self.client, NextcloudClient):
            return None
        for folder in self.client.list_group_folders():
            if (
                folder.get("mount_point") == mount_point
                and group_id not in folder.get("groups", {})
            ):
                return int(folder["id"])
        return None

    # ------------------------------------------------------------------
    # BaseBackend abstract methods
    # ------------------------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Return True if Nextcloud is reachable and installed."""
        try:
            if not isinstance(self.client, NextcloudClient):
                return False
            is_alive = self.client.ping()
            if not is_alive:
                logger.warning("Nextcloud is not responding correctly")
                if raise_exception:
                    raise BackendError("Nextcloud ping failed")
            return is_alive
        except Exception:
            logger.exception("Nextcloud ping error")
            if raise_exception:
                raise
            return False

    def diagnostics(self) -> bool:
        """Log diagnostics and return True if the backend is operational."""
        logger.info("=== Nextcloud Backend Diagnostics ===")
        logger.info("URL: %s", self.backend_settings["nextcloud_url"])
        logger.info("Group prefix: %s", self.group_prefix)
        logger.info("Default quota: %d GiB", self.default_storage_quota_gb)

        alive = self.ping()
        logger.info("Ping: %s", alive)

        if alive and isinstance(self.client, NextcloudClient):
            folders = self.client.list_group_folders()
            logger.info("Existing group folders: %d", len(folders))

        return alive

    def list_components(self) -> list[str]:
        """Return the list of component types managed by this backend."""
        return list(self.backend_components.keys())

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> structures.BackendResourceInfo:
        """Override to bypass the base-class flow.

        The base class assumes resource_backend_id is pre-determined, but
        Nextcloud assigns a numeric folder_id at creation time.  We check for
        duplicates ourselves (via the group name) and delegate to create_resource
        so the real folder_id is stored as backend_id.
        """
        if isinstance(self.client, NextcloudClient):
            group_id = self._group_id(waldur_resource)

            # If the folder Waldur recorded still exists and has our group
            # attached, refuse to create a duplicate.
            try:
                folder = self.client.get_group_folder(int(resource_backend_id))
                if group_id in folder.get("groups", {}):
                    raise DuplicateResourceError(resource_backend_id)
            except (NextcloudGroupFolderError, ValueError):
                pass  # Folder is gone or backend_id is not a valid int — proceed

            # Even without the original folder, an orphaned group linked to a
            # *different* folder means a duplicate already exists.
            if self.client.group_exists(group_id):
                for folder in self.client.list_group_folders():
                    if group_id in folder.get("groups", {}):
                        raise DuplicateResourceError(resource_backend_id)
                logger.warning(
                    "Group %s exists without a linked folder; recovering from partial creation",
                    group_id,
                )

        self._pre_create_resource(waldur_resource, user_context)
        backend_resource_info = self.create_resource(waldur_resource, user_context)
        self.post_create_resource(backend_resource_info, waldur_resource, user_context)
        return backend_resource_info

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """No-op — group and folder creation happens in create_resource."""

    def create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> structures.BackendResourceInfo:
        """Create a Nextcloud group and a shared Group Folder.

        Steps:
        1. Create a Nextcloud group named ``{prefix}{resource_uuid}``.
        2. Create a Group Folder with the same name as its mount point.
        3. Grant the group access to the folder.
        4. Apply the storage quota from the Waldur plan.

        Returns:
            BackendResourceInfo where ``backend_id`` is the folder's integer ID
            (as a string).  Subsequent operations use this to locate the folder.
        """
        group_id = self._group_id(waldur_resource)
        logger.info(
            "Creating Nextcloud resources for Waldur resource %s (group=%s)",
            waldur_resource.uuid,
            group_id,
        )

        if not isinstance(self.client, NextcloudClient):
            raise BackendError("Nextcloud client not initialised")

        project_name_raw = getattr(waldur_resource, "project_name", None) or ""
        uuid_suffix = str(waldur_resource.uuid)[:8]
        resource_name = (
            f"{self._sanitize_path_component(waldur_resource.name or group_id)}-{uuid_suffix}"
        )
        if project_name_raw:
            mount_point = f"{self._sanitize_path_component(project_name_raw)}/{resource_name}"
        else:
            mount_point = resource_name

        try:
            self.client.create_group(group_id)
        except NextcloudGroupError as exc:
            raise BackendError(f"Failed to create Nextcloud group: {exc}") from exc

        try:
            folder_id = self.client.create_group_folder(mount_point)
        except NextcloudGroupFolderError as exc:
            # May be a duplicate mount point from a previous partial creation.
            folder_id = self._find_orphaned_folder(mount_point, group_id)
            if folder_id is None:
                raise BackendError(
                    f"Failed to create Nextcloud group folder for '{mount_point}'"
                ) from exc
            logger.warning(
                "Found orphaned folder %d for '%s'; recovering from partial creation",
                folder_id, mount_point,
            )

        try:
            self.client.add_group_to_folder(folder_id, group_id)
        except NextcloudGroupFolderError as exc:
            raise BackendError(
                f"Failed to assign group to folder: {exc}"
            ) from exc

        if not self.allow_resharing:
            try:
                self.client.set_group_permissions(folder_id, group_id, 15)
            except NextcloudGroupFolderError as exc:
                logger.warning(
                    "Failed to restrict resharing for folder %d: %s", folder_id, exc
                )

        # Apply quota from plan limits
        backend_limits, waldur_limits = self._collect_resource_limits(waldur_resource)
        storage_gb = backend_limits.get("storage", self.default_storage_quota_gb)
        try:
            self.client.set_folder_quota(folder_id, _gib_to_bytes(storage_gb))
        except NextcloudGroupFolderError as exc:
            logger.warning("Failed to set quota for folder %d: %s", folder_id, exc)

        logger.info(
            "Created Nextcloud folder %d ('%s') for resource %s with %d GiB quota",
            folder_id,
            mount_point,
            waldur_resource.uuid,
            storage_gb,
        )

        # Add project team members immediately so access is instant rather than
        # waiting for the next membership-sync poll cycle.
        team_members = list((user_context or {}).get("team") or [])
        logger.info(
            "Team members at resource creation: count=%d, usernames=%s",
            len(team_members),
            [getattr(m, "username", None) for m in team_members],
        )
        for member in team_members:
            username = getattr(member, "username", None)
            if username:
                try:
                    self.client.add_user_to_group(username, group_id)
                except NextcloudGroupError as exc:
                    logger.warning(
                        "Failed to add team member %s to group %s at creation: %s",
                        username, group_id, exc,
                    )

        return structures.BackendResourceInfo(
            backend_id=str(folder_id),
            limits=waldur_limits,
        )

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> Optional[structures.BackendResourceInfo]:
        """Pull current state of a group folder from Nextcloud."""
        logger.info("Pulling Nextcloud folder %s", resource_backend_id)

        if not isinstance(self.client, NextcloudClient):
            return None

        try:
            folder = self.client.get_group_folder(int(resource_backend_id))
        except (NextcloudGroupFolderError, ValueError):
            logger.warning("Group folder %s not found", resource_backend_id)
            return None

        quota_bytes = int(folder.get("quota", -3))
        storage_gb = _bytes_to_gib(quota_bytes) if quota_bytes > 0 else 0
        usage_bytes = int(folder.get("size", 0))

        users: list[str] = []
        for gid in folder.get("groups", {}):
            users.extend(self.client.list_group_members(gid))

        return structures.BackendResourceInfo(
            backend_id=resource_backend_id,
            limits={"storage": storage_gb},
            usage={
                "TOTAL_ACCOUNT_USAGE": {
                    "storage": _bytes_to_gib(usage_bytes),
                }
            },
            users=users,
        )

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> Optional[str]:
        """Delete the group folder and associated Nextcloud group.

        Overrides the base class directly (rather than delegating to
        client.delete_resource) because two separate objects — folder and
        group — must be destroyed and the group_id is derived from the
        Waldur resource UUID, which is not available to the client layer.
        The base-class soft_delete path does not apply to Nextcloud.
        _pre_delete_resource and post_delete_resource hooks are still called.
        """
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        group_id = self._group_id(waldur_resource)

        if not resource_backend_id or not resource_backend_id.strip():
            logger.warning("Empty backend_id for resource %s, skipping", waldur_resource.uuid)
            return None

        if not isinstance(self.client, NextcloudClient):
            return None

        self._pre_delete_resource(waldur_resource)

        try:
            self.client.delete_group_folder(int(resource_backend_id))
            logger.info("Deleted group folder %s", resource_backend_id)
        except (NextcloudGroupFolderError, ValueError):
            logger.exception("Failed to delete folder %s", resource_backend_id)

        try:
            self.client.delete_group(group_id)
            logger.info("Deleted group %s", group_id)
        except NextcloudGroupError:
            logger.exception("Failed to delete group %s", group_id)

        self.post_delete_resource(waldur_resource)
        return None

    def add_users_to_resource(
        self,
        waldur_resource: WaldurResource,
        user_ids: set[str],
        **kwargs: dict,
    ) -> set[str]:
        """Add users to the Nextcloud group that controls folder access.

        Users must already exist in Nextcloud (they are created automatically
        on first login via Keycloak OIDC).  Usernames come from the
        ``preferred_username`` claim configured in Nextcloud's OIDC provider.

        Returns the set of usernames successfully added.
        """
        group_id = self._group_id(waldur_resource)
        if not isinstance(self.client, NextcloudClient):
            return set()

        added: set[str] = set()
        for username in user_ids:
            try:
                self.client.add_user_to_group(username, group_id)
                added.add(username)
            except NextcloudGroupError:
                logger.exception("Failed to add user %s to group %s", username, group_id)
        return added

    def remove_users_from_resource(
        self,
        waldur_resource: WaldurResource,
        usernames: set[str],
        **kwargs: dict,
    ) -> list[str]:
        """Remove users from the Nextcloud group.

        Returns a list of usernames successfully removed.
        """
        group_id = self._group_id(waldur_resource)
        if not isinstance(self.client, NextcloudClient):
            return []

        removed: list[str] = []
        for username in usernames:
            try:
                self.client.remove_user_from_group(username, group_id)
                removed.append(username)
            except NextcloudGroupError:
                logger.exception("Failed to remove user %s from group %s", username, group_id)
        return removed

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Return storage usage for the given folder IDs.

        Returns:
            Mapping of folder_id (str) → usage dict with TOTAL_ACCOUNT_USAGE.
        """
        report: dict = {}
        if not isinstance(self.client, NextcloudClient):
            return report

        for folder_id_str in resource_backend_ids:
            try:
                usage_bytes = self.client.get_folder_usage(int(folder_id_str))
                storage_gb = _bytes_to_gib(usage_bytes)
                report[folder_id_str] = {
                    "TOTAL_ACCOUNT_USAGE": {"storage": storage_gb}
                }
                logger.info(
                    "Folder %s using %.2f GiB", folder_id_str, storage_gb
                )
            except Exception:
                logger.exception(
                    "Usage report failed for folder %s, omitting from report",
                    folder_id_str,
                )

        return report

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Extract storage limits from the Waldur resource's plan."""
        storage_gb = self.default_storage_quota_gb

        if waldur_resource.limits and "storage" in waldur_resource.limits:
            storage_gb = waldur_resource.limits["storage"]

        limits = {"storage": storage_gb}
        return limits, limits

    def set_resource_limits(
        self, resource_backend_id: str, limits: dict[str, int]
    ) -> Optional[str]:
        """Update the Group Folder's storage quota."""
        if not isinstance(self.client, NextcloudClient):
            return None

        storage_gb = limits.get("storage", self.default_storage_quota_gb)
        try:
            self.client.set_folder_quota(int(resource_backend_id), _gib_to_bytes(storage_gb))
            logger.info(
                "Updated folder %s quota to %d GiB", resource_backend_id, storage_gb
            )
        except (NextcloudGroupFolderError, ValueError) as exc:
            raise BackendError(f"Failed to set folder quota: {exc}") from exc

        return None

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Return current storage quota in GiB for the folder."""
        if not isinstance(self.client, NextcloudClient):
            return {}

        return self.client.get_resource_limits(resource_backend_id)

    def sync_resource_limits(
        self, waldur_resource: WaldurResource, waldur_rest_client: AuthenticatedClient
    ) -> None:
        """Push Waldur plan limits to Nextcloud; skip when paused or downscaled.

        Waldur is the source of truth for Nextcloud quotas — users cannot
        set their own quotas, so we always push plan limits from Waldur to
        the Group Folder. When paused or downscaled the quota is intentionally
        reduced; skip the sync so the low quota is not overwritten.
        """
        if not isinstance(self.client, NextcloudClient):
            return

        if not isinstance(waldur_resource.backend_id, str):
            return

        is_paused = getattr(waldur_resource, "paused", False)
        is_downscaled = getattr(waldur_resource, "downscaled", False)

        if is_paused or is_downscaled:
            logger.debug(
                "Skipping limit sync for folder %s (paused=%s, downscaled=%s)",
                waldur_resource.backend_id, is_paused, is_downscaled,
            )
            return

        limits, _ = self._collect_resource_limits(waldur_resource)
        self.set_resource_limits(waldur_resource.backend_id, limits)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Reduce quota to 1 GiB (minimum usable state)."""
        if not isinstance(self.client, NextcloudClient):
            return False
        try:
            self.client.set_folder_quota(int(resource_backend_id), _gib_to_bytes(1))
            logger.info("Downscaled folder %s to 1 GiB", resource_backend_id)
            return True
        except (NextcloudError, ValueError):
            logger.exception("Failed to downscale folder %s", resource_backend_id)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause is not directly supported — sets quota to 0."""
        if not isinstance(self.client, NextcloudClient):
            return False
        try:
            self.client.set_folder_quota(int(resource_backend_id), 0)
            logger.info("Paused folder %s (quota=0)", resource_backend_id)
            return True
        except (NextcloudError, ValueError):
            logger.exception("Failed to pause folder %s", resource_backend_id)
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """No-op for Nextcloud — per-resource quotas are synced via _sync_resource_limits."""
        return False

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return Nextcloud-specific metadata for the folder."""
        if not isinstance(self.client, NextcloudClient):
            return {}
        try:
            folder = self.client.get_group_folder(int(resource_backend_id))
            return {
                "folder_id": folder.get("id"),
                "mount_point": folder.get("mount_point"),
                "groups": list(folder.get("groups", {}).keys()),
                "size_bytes": folder.get("size", 0),
                "quota_bytes": folder.get("quota", -3),
            }
        except (NextcloudGroupFolderError, ValueError):
            return {}
