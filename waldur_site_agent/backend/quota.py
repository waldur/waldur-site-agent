"""Filesystem quota providers for user home directories.

Supports applying and verifying quotas on user home directories
for CephFS (xattr), XFS (user quotas), and Lustre filesystems.
"""

from __future__ import annotations

import pwd
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

if TYPE_CHECKING:
    from waldur_site_agent.backend.clients import BaseClient

_SUPPORTED_PROVIDERS = frozenset({"ceph_xattr", "xfs", "lustre"})


class HomedirQuotaConfig(BaseModel):
    """Configuration for user home directory quotas.

    Provider-specific fields:
      - ``ceph_xattr``: uses ``max_bytes`` and ``max_files``
      - ``xfs``: uses ``mount_point``, ``block_softlimit``, ``block_hardlimit``,
        ``inode_softlimit``, ``inode_hardlimit``
      - ``lustre``: uses ``mount_point``, ``block_softlimit``, ``block_hardlimit``,
        ``inode_softlimit``, ``inode_hardlimit``
    """

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(
        ...,
        description="Quota provider type: ceph_xattr, xfs, or lustre",
    )

    # CephFS xattr options
    max_bytes: Optional[str] = Field(
        default=None,
        description="Maximum bytes for CephFS quota (e.g. '1099511627776' or '1T')",
    )
    max_files: Optional[int] = Field(
        default=None,
        description="Maximum number of files/inodes for CephFS quota",
    )

    # XFS / Lustre shared options
    mount_point: Optional[str] = Field(
        default=None,
        description="Filesystem mount point (required for xfs and lustre providers)",
    )
    block_softlimit: Optional[str] = Field(
        default=None,
        description=(
            "Block soft limit. "
            "XFS accepts human-readable suffixes (e.g. '900g'). "
            "Lustre expects kilobytes."
        ),
    )
    block_hardlimit: Optional[str] = Field(
        default=None,
        description="Block hard limit (same format as block_softlimit)",
    )
    inode_softlimit: Optional[int] = Field(
        default=None,
        description="Inode/file soft limit",
    )
    inode_hardlimit: Optional[int] = Field(
        default=None,
        description="Inode/file hard limit",
    )

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        """Ensure the provider is one of the supported types."""
        if v not in _SUPPORTED_PROVIDERS:
            msg = (
                f"Unsupported quota provider: {v!r}. "
                f"Must be one of: {', '.join(sorted(_SUPPORTED_PROVIDERS))}"
            )
            raise ValueError(msg)
        return v


def get_user_homedir(username: str, homedir_base_path: Optional[str] = None) -> str:
    """Resolve the home directory path for a user.

    Args:
        username: Linux username.
        homedir_base_path: Optional base path override. When set, the home
            directory is ``{homedir_base_path}/{username}``. Otherwise the
            path is looked up from the system password database.

    Returns:
        Absolute path to the user's home directory.
    """
    if homedir_base_path:
        return f"{homedir_base_path.rstrip('/')}/{username}"
    return pwd.getpwnam(username).pw_dir


def apply_homedir_quota(
    client: BaseClient,
    username: str,
    homedir_path: str,
    config: HomedirQuotaConfig,
) -> None:
    """Apply and verify a filesystem quota on a user's home directory.

    Dispatches to the appropriate provider implementation based on
    ``config.provider``.
    """
    provider = config.provider
    if provider == "ceph_xattr":
        _apply_ceph_quota(client, homedir_path, config)
    elif provider == "xfs":
        _apply_xfs_quota(client, username, homedir_path, config)
    elif provider == "lustre":
        _apply_lustre_quota(client, username, config)
    else:
        logger.error("Unknown quota provider: %s", provider)


# ---------------------------------------------------------------------------
# CephFS xattr provider
# ---------------------------------------------------------------------------


def _apply_ceph_quota(
    client: BaseClient, homedir_path: str, config: HomedirQuotaConfig
) -> None:
    """Apply CephFS quotas via extended attributes."""
    if config.max_bytes is not None:
        _set_and_verify_xattr(
            client,
            homedir_path,
            "ceph.quota.max_bytes",
            str(config.max_bytes),
        )
    if config.max_files is not None:
        _set_and_verify_xattr(
            client,
            homedir_path,
            "ceph.quota.max_files",
            str(config.max_files),
        )


def _set_and_verify_xattr(
    client: BaseClient, path: str, attr_name: str, expected_value: str
) -> None:
    """Set an extended attribute and verify the value was persisted."""
    client.execute_command(["setfattr", "-n", attr_name, "-v", expected_value, path])
    logger.info("Set %s=%s on %s", attr_name, expected_value, path)

    try:
        result = client.execute_command(
            ["getfattr", "--only-values", "-n", attr_name, path]
        )
        actual = result.strip()
        if actual != expected_value:
            logger.warning(
                "Quota verification mismatch for %s on %s: expected=%s, actual=%s",
                attr_name,
                path,
                expected_value,
                actual,
            )
        else:
            logger.info("Verified %s=%s on %s", attr_name, expected_value, path)
    except BackendError:
        logger.warning("Could not verify %s on %s", attr_name, path)


# ---------------------------------------------------------------------------
# XFS user quota provider
# ---------------------------------------------------------------------------


def _apply_xfs_quota(
    client: BaseClient,
    username: str,
    homedir_path: str,
    config: HomedirQuotaConfig,
) -> None:
    """Apply XFS user quotas via ``xfs_quota``."""
    mount_point = config.mount_point
    if not mount_point:
        logger.error(
            "XFS quota requires mount_point configuration; skipping quota for %s",
            homedir_path,
        )
        return

    limit_parts: list[str] = []
    if config.block_softlimit is not None:
        limit_parts.append(f"bsoft={config.block_softlimit}")
    if config.block_hardlimit is not None:
        limit_parts.append(f"bhard={config.block_hardlimit}")
    if config.inode_softlimit is not None:
        limit_parts.append(f"isoft={config.inode_softlimit}")
    if config.inode_hardlimit is not None:
        limit_parts.append(f"ihard={config.inode_hardlimit}")

    if not limit_parts:
        logger.warning("No XFS quota limits configured; skipping quota for %s", username)
        return

    limit_str = " ".join(limit_parts)
    client.execute_command(
        [
            "xfs_quota",
            "-x",
            "-c",
            f"limit -u {limit_str} {username}",
            mount_point,
        ]
    )
    logger.info("Set XFS user quota for %s on %s: %s", username, mount_point, limit_str)

    # Verify — log the current quota so admins can confirm
    try:
        result = client.execute_command(
            [
                "xfs_quota",
                "-x",
                "-c",
                f"quota -u -N -b -h {username}",
                mount_point,
            ]
        )
        logger.info(
            "XFS quota verification for %s on %s:\n%s",
            username,
            mount_point,
            result.strip(),
        )
    except BackendError:
        logger.warning("Could not verify XFS quota for %s on %s", username, mount_point)


# ---------------------------------------------------------------------------
# Lustre user quota provider
# ---------------------------------------------------------------------------


def _apply_lustre_quota(
    client: BaseClient, username: str, config: HomedirQuotaConfig
) -> None:
    """Apply Lustre user quotas via ``lfs setquota``."""
    mount_point = config.mount_point
    if not mount_point:
        logger.error(
            "Lustre quota requires mount_point configuration; skipping quota for %s",
            username,
        )
        return

    cmd: list[str] = ["lfs", "setquota", "-u", username]
    has_limits = False
    if config.block_softlimit is not None:
        cmd.extend(["-b", str(config.block_softlimit)])
        has_limits = True
    if config.block_hardlimit is not None:
        cmd.extend(["-B", str(config.block_hardlimit)])
        has_limits = True
    if config.inode_softlimit is not None:
        cmd.extend(["-i", str(config.inode_softlimit)])
        has_limits = True
    if config.inode_hardlimit is not None:
        cmd.extend(["-I", str(config.inode_hardlimit)])
        has_limits = True

    if not has_limits:
        logger.warning("No Lustre quota limits configured; skipping quota for %s", username)
        return

    cmd.append(mount_point)
    client.execute_command(cmd)
    logger.info("Set Lustre user quota for %s on %s", username, mount_point)

    # Verify — log the current quota so admins can confirm
    try:
        result = client.execute_command(["lfs", "quota", "-u", username, mount_point])
        logger.info(
            "Lustre quota verification for %s on %s:\n%s",
            username,
            mount_point,
            result.strip(),
        )
    except BackendError:
        logger.warning("Could not verify Lustre quota for %s on %s", username, mount_point)
