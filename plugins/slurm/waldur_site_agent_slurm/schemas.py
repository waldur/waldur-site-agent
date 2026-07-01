"""SLURM plugin-specific Pydantic schemas for configuration validation.

This module defines validation schemas for SLURM-specific configuration fields
based on actual SLURM plugin usage patterns and the periodic limits functionality.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional

from pydantic import ConfigDict, Field, field_validator, model_validator

from waldur_site_agent.backend.quota import HomedirQuotaConfig
from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class SlurmLimitType(Enum):
    """Enumeration of SLURM limit types for periodic limits."""

    GRP_TRES_MINS = "GrpTRESMins"
    MAX_TRES_MINS = "MaxTRESMins"
    GRP_TRES = "GrpTRES"


class ExecutionMode(str, Enum):
    """How the SLURM backend talks to the cluster.

    Inherits from ``str`` so the members compare equal to the raw YAML values
    ("cli"/"rest") and serialize transparently, while still giving callers a
    typed, exhaustive set of choices.
    """

    CLI = "cli"
    REST = "rest"


class SlurmComponentSchema(PluginComponentSchema):
    """SLURM-specific component field validation.

    The SLURM plugin does not require any component-specific fields beyond
    those defined by ``PluginComponentSchema``. Periodic-limits parameters
    (``period_type``, ``carryover_enabled``, ``grace_ratio`` …) used to live
    here, but the agent never read them — they're authored on the Mastermind
    side as fields of ``SlurmPeriodicUsagePolicy`` and consumed by the
    policy engine. The diagnostic CLI fetches them from Mastermind's REST
    API for display.

    ``extra="allow"`` is kept so per-deployment custom keys don't error.
    """

    model_config = ConfigDict(extra="allow")


class PeriodicLimitsConfig(PluginBackendSettingsSchema):
    """Periodic-limits enablement settings (nested within ``backend_settings``).

    Only fields the agent actually reads at runtime live here:

    * ``enabled`` gates whether the agent subscribes to the
      ``RESOURCE_PERIODIC_LIMITS`` STOMP topic and runs
      ``apply_periodic_settings``.
    * ``emulator_mode`` / ``emulator_base_url`` switch ``apply_periodic_settings``
      between sacctmgr writes and the SLURM emulator's REST API.
    * ``limit_type`` is a fallback used when an inbound STOMP payload
      omits the explicit ``limit_type`` key.

    Policy parameters (grace ratio, carryover factor, billing weights, raw
    usage reset cadence, …) are authored on Mastermind's
    ``SlurmPeriodicUsagePolicy`` and arrive in the STOMP payload; the agent
    applies what it receives, it does not configure those locally.

    ``extra="allow"`` lets deployments that still carry the legacy fields in
    their YAML load without warning until they're cleaned up.
    """

    model_config = ConfigDict(extra="allow")

    enabled: bool = Field(default=False, description="Enable periodic limits functionality")

    emulator_mode: Optional[bool] = Field(
        default=False, description="Use SLURM emulator for testing"
    )
    emulator_base_url: Optional[str] = Field(default=None, description="SLURM emulator API URL")

    limit_type: Optional[SlurmLimitType] = Field(
        default=SlurmLimitType.GRP_TRES_MINS,
        description="Fallback SLURM limit type when the STOMP payload omits it",
    )


class QosManagementConfig(PluginBackendSettingsSchema):
    """QoS management configuration for per-account QoS creation."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = Field(default=False, description="Enable per-account QoS creation")
    flags: Optional[str] = Field(
        default="DenyOnLimit,NoDecay",
        description="QoS flags (e.g., 'DenyOnLimit,NoDecay')",
    )
    grp_tres: Optional[str] = Field(
        default=None,
        description="Group TRES limits for the QoS (e.g., 'cpu=25600,node=100')",
    )
    max_jobs: Optional[int] = Field(default=None, description="Maximum concurrent jobs per QoS")
    max_submit: Optional[int] = Field(default=None, description="Maximum submitted jobs per QoS")
    max_wall: Optional[str] = Field(
        default=None,
        description="Maximum wall time (minutes or D-HH:MM:SS)",
    )
    min_tres_per_job: Optional[str] = Field(
        default=None,
        description="Minimum TRES per job (e.g., 'gres/gpu=1')",
    )
    additional_qos: Optional[list[str]] = Field(
        default=None,
        description="Additional QoS names to attach to accounts (e.g., ['2cpu-single-host'])",
    )


class LustreQuotaConfig(PluginBackendSettingsSchema):
    """Lustre filesystem quota configuration."""

    model_config = ConfigDict(extra="allow")

    mount_point: str = Field(default="/valhalla", description="Lustre mount point")
    block_softlimit: Optional[int] = Field(
        default=None, description="Block soft limit in kilobytes"
    )
    block_hardlimit: Optional[int] = Field(
        default=None, description="Block hard limit in kilobytes"
    )
    inode_softlimit: Optional[int] = Field(default=None, description="Inode soft limit")
    inode_hardlimit: Optional[int] = Field(default=None, description="Inode hard limit")


class ProjectDirectoryConfig(PluginBackendSettingsSchema):
    """Project directory creation and quota configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = Field(default=False, description="Enable project directory creation")
    base_path: str = Field(
        default="/valhalla/projects",
        description="Base path for project directories",
    )
    owner: str = Field(default="nobody", description="Owner of the project directory")
    permissions: str = Field(default="770", description="Directory permissions (octal)")
    set_gid: bool = Field(default=True, description="Set the setgid bit on the directory")
    set_acl: bool = Field(default=True, description="Set POSIX ACLs for the project group")
    lustre_quota: Optional[LustreQuotaConfig] = Field(
        default=None, description="Lustre quota settings"
    )


class SlurmRestApiConfig(PluginBackendSettingsSchema):
    """slurmrestd connection settings (used when ``execution_mode`` is ``rest``)."""

    model_config = ConfigDict(extra="allow")

    url: str = Field(
        ...,
        description=(
            "slurmrestd endpoint: 'http(s)://host:port' or 'unix:///path/to/socket'. "
            "slurmrestd speaks plain HTTP — use https only via a TLS-terminating proxy."
        ),
    )
    api_version: str = Field(
        default="v0.0.43",
        description="Pinned slurmrestd data_parser API version (e.g. 'v0.0.43')",
    )
    username: str = Field(
        ...,
        description="User the agent authenticates as (sent in X-SLURM-USER-NAME)",
    )
    token_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to a file with the JWT token. Re-read on HTTP 401, so an "
            "external rotator (e.g. cron running 'scontrol token') keeps the "
            "agent working without restarts."
        ),
    )
    token_env: Optional[str] = Field(
        default=None,
        description="Name of an environment variable holding the JWT token",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify TLS certificates (for https endpoints behind a proxy)",
    )
    timeout: int = Field(default=30, description="HTTP request timeout in seconds")

    @model_validator(mode="after")
    def validate_token_source(self) -> SlurmRestApiConfig:
        """Require at least one token source."""
        if not self.token_file and not self.token_env:
            msg = "rest_api requires either token_file or token_env to be set"
            raise ValueError(msg)
        return self


class SlurmBackendSettingsSchema(PluginBackendSettingsSchema):
    """SLURM-specific backend settings validation.

    Based on actual SLURM plugin usage patterns from backend.py analysis.
    """

    model_config = ConfigDict(extra="allow")  # Allow additional settings

    # Core SLURM account management (required by backend.py)
    default_account: str = Field(..., description="Default parent account in SLURM cluster")
    default_account_policy: Literal["common", "individual", "none"] = Field(
        default="common",
        description=(
            "Controls the DefaultAccount= argument passed to 'sacctmgr add user'.\n"
            "  common     — use the configured default_account (current behaviour).\n"
            "  individual — use the resource account itself; avoids implicit\n"
            "               associations with the org-level root account.\n"
            "  none       — omit DefaultAccount= entirely; sacctmgr auto-assigns\n"
            "               it for new users, existing users' default is unchanged."
        ),
    )
    customer_prefix: str = Field(..., description="Prefix for customer account names")
    project_prefix: str = Field(..., description="Prefix for project account names")
    allocation_prefix: str = Field(..., description="Prefix for allocation account names")

    # Optional: flat hierarchy with a single parent account
    parent_account: Optional[str] = Field(
        default=None,
        description=(
            "Parent account for new project accounts. "
            "When set, accounts are created directly under this parent "
            "instead of the customer/project hierarchy."
        ),
    )

    # Optional: scope sacctmgr/sacct commands and REST payloads to one cluster
    cluster_name: Optional[str] = Field(
        default=None,
        description=(
            "SLURM cluster name to scope operations to. "
            "Optional in CLI mode, required when execution_mode is 'rest'."
        ),
    )

    # Execution mode: shell out to SLURM CLI tools (default) or talk to slurmrestd
    execution_mode: ExecutionMode = Field(
        default=ExecutionMode.CLI,
        description=(
            "How to talk to SLURM: 'cli' (sacctmgr/sacct binaries, default) or "
            "'rest' (slurmrestd REST API; usage reporting still uses sacct — "
            "see docs/slurm-rest-api-design.md)"
        ),
    )
    rest_api: Optional[SlurmRestApiConfig] = Field(
        default=None,
        description="slurmrestd connection settings, required when execution_mode is 'rest'",
    )

    # Optional: default partition for user associations
    default_partition: Optional[str] = Field(
        default=None,
        description="Partition to assign to user-account associations (e.g., 'cn', 'common')",
    )

    # QoS management (used by backend.py)
    qos_default: Optional[str] = Field(default="normal", description="Default QoS for accounts")
    qos_downscaled: Optional[str] = Field(default=None, description="QoS for downscaled accounts")
    qos_paused: Optional[str] = Field(default=None, description="QoS for paused accounts")

    # Per-account QoS management (optional, for EFP-style deployments)
    qos_management: Optional[QosManagementConfig] = Field(
        default=None, description="Per-account QoS creation and management"
    )

    # User home directory management (used by backend.py)
    enable_user_homedir_account_creation: Optional[bool] = Field(
        default=True, description="Create home directories for users"
    )
    default_homedir_umask: Optional[str] = Field(
        default="0077", description="Umask for created home directories"
    )
    homedir_base_path: Optional[str] = Field(
        default=None,
        description=(
            "Base path for user home directories (e.g. '/cephfs/home'). "
            "When set, quota is applied to {homedir_base_path}/{username}. "
            "When unset, the path is looked up from the system passwd database."
        ),
    )
    homedir_quota: Optional[HomedirQuotaConfig] = Field(
        default=None,
        description="Filesystem quota settings for user home directories",
    )

    # Project directory management (optional, for sites with shared project storage)
    project_directory: Optional[ProjectDirectoryConfig] = Field(
        default=None, description="Project directory creation and quota settings"
    )

    # Periodic limits configuration (nested object)
    periodic_limits: Optional[PeriodicLimitsConfig] = Field(
        default=None, description="Periodic limits configuration"
    )

    @model_validator(mode="after")
    def validate_rest_mode(self) -> SlurmBackendSettingsSchema:
        """REST execution mode needs connection settings and an explicit cluster."""
        if self.execution_mode is ExecutionMode.REST:
            if self.rest_api is None:
                msg = "execution_mode is 'rest' but rest_api settings are missing"
                raise ValueError(msg)
            if not self.cluster_name:
                msg = (
                    "execution_mode is 'rest' but cluster_name is not set — REST "
                    "association payloads require an explicit cluster"
                )
                raise ValueError(msg)
        return self

    @field_validator("default_homedir_umask")
    @classmethod
    def validate_umask(cls, v: Optional[str]) -> Optional[str]:
        """Validate that umask is a valid octal permission."""

        def _raise_umask_error(value: str) -> None:
            msg = f"Invalid umask range: {value}"
            raise ValueError(msg)

        if v is not None:
            try:
                # Try to parse as octal
                umask_value = int(v, 8)
                max_umask = 0o777
                if umask_value < 0 or umask_value > max_umask:
                    _raise_umask_error(v)
            except ValueError as e:
                msg = f"default_homedir_umask must be valid octal permissions (e.g., '0077'): {e}"
                raise ValueError(msg) from e
        return v
