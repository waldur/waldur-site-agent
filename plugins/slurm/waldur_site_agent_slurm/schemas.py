"""SLURM plugin-specific Pydantic schemas for configuration validation.

This module defines validation schemas for SLURM-specific configuration fields
based on actual SLURM plugin usage patterns and the periodic limits functionality.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import ConfigDict, Field, field_validator

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class PeriodType(Enum):
    """Enumeration of period types for SLURM periodic limits."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class SlurmLimitType(Enum):
    """Enumeration of SLURM limit types for periodic limits."""

    GRP_TRES_MINS = "GrpTRESMins"
    MAX_TRES_MINS = "MaxTRESMins"
    GRP_TRES = "GrpTRES"


class QoSLevels(PluginBackendSettingsSchema):
    """QoS levels configuration for SLURM periodic limits.

    Based on actual usage in backend.py lines 611, 621, 631.
    """

    default: str = Field(..., description="Default QoS level for normal operation")
    slowdown: str = Field(..., description="QoS level when threshold is exceeded")
    blocked: Optional[str] = Field(default=None, description="QoS level for hard limit exceeded")


class SlurmComponentSchema(PluginComponentSchema):
    """SLURM-specific component field validation.

    Based on actual SLURM plugin usage, this validates fields used by:
    - Periodic limits functionality (period_type, carryover_enabled, grace_ratio)
    - Component-level configuration for SLURM accounting
    """

    model_config = ConfigDict(extra="allow")  # Allow core fields to pass through

    # Periodic limits component configuration (actual SLURM usage)
    period_type: Optional[PeriodType] = Field(
        default=None, description="Period type for periodic limits"
    )
    carryover_enabled: Optional[bool] = Field(
        default=None, description="Enable carryover for unused allocation to next period"
    )
    grace_ratio: Optional[float] = Field(
        default=None, description="Grace period ratio (0.0-1.0) for overconsumption allowance"
    )

    @field_validator("grace_ratio")
    @classmethod
    def validate_grace_ratio(cls, v: Optional[float]) -> Optional[float]:
        """Validate grace_ratio is between 0.0 and 1.0."""
        if v is not None and (v < 0.0 or v > 1.0):
            msg = "grace_ratio must be between 0.0 and 1.0"
            raise ValueError(msg)
        return v


class PeriodicLimitsConfig(PluginBackendSettingsSchema):
    """Periodic limits configuration schema (nested within backend_settings)."""

    # Core periodic limits settings
    enabled: bool = Field(default=False, description="Enable periodic limits functionality")

    # Emulator integration
    emulator_mode: Optional[bool] = Field(
        default=False, description="Use SLURM emulator for testing"
    )
    emulator_base_url: Optional[str] = Field(default=None, description="SLURM emulator API URL")

    # SLURM-specific settings
    limit_type: Optional[SlurmLimitType] = Field(
        default=SlurmLimitType.GRP_TRES_MINS, description="SLURM limit type for periodic limits"
    )
    tres_billing_enabled: Optional[bool] = Field(
        default=False, description="Use TRES billing units vs raw TRES"
    )
    tres_billing_weights: Optional[dict[str, float]] = Field(
        default=None, description="Billing weights for resource types (e.g., CPU: 0.015625)"
    )

    # Fairshare configuration
    fairshare_decay_half_life: Optional[int] = Field(
        default=15, description="Fairshare decay half-life in days"
    )
    raw_usage_reset: Optional[bool] = Field(
        default=True, description="Reset raw usage at period transitions"
    )

    # QoS levels for periodic limits
    qos_levels: Optional[QoSLevels] = Field(
        default=None, description="QoS levels for different states"
    )

    # Policy defaults
    default_grace_ratio: Optional[float] = Field(
        default=0.2, description="Default grace ratio for overconsumption (0.0-1.0)"
    )
    default_carryover_enabled: Optional[bool] = Field(
        default=True, description="Enable carryover by default"
    )

    # Command customization
    commands: Optional[dict[str, str]] = Field(
        default=None, description="Custom SLURM commands for operations"
    )
    api_endpoints: Optional[dict[str, str]] = Field(
        default=None, description="API endpoints for Waldur integration"
    )

    @field_validator("default_grace_ratio")
    @classmethod
    def validate_default_grace_ratio(cls, v: Optional[float]) -> Optional[float]:
        """Validate default_grace_ratio is between 0.0 and 1.0."""
        if v is not None and (v < 0.0 or v > 1.0):
            msg = "default_grace_ratio must be between 0.0 and 1.0"
            raise ValueError(msg)
        return v


class SlurmBackendSettingsSchema(PluginBackendSettingsSchema):
    """SLURM-specific backend settings validation.

    Based on actual SLURM plugin usage patterns from backend.py analysis.
    """

    model_config = ConfigDict(extra="allow")  # Allow additional settings

    # Core SLURM account management (required by backend.py)
    default_account: str = Field(..., description="Default parent account in SLURM cluster")
    customer_prefix: str = Field(..., description="Prefix for customer account names")
    project_prefix: str = Field(..., description="Prefix for project account names")
    allocation_prefix: str = Field(..., description="Prefix for allocation account names")

    # QoS management (used by backend.py)
    qos_default: Optional[str] = Field(default="normal", description="Default QoS for accounts")
    qos_downscaled: Optional[str] = Field(default=None, description="QoS for downscaled accounts")
    qos_paused: Optional[str] = Field(default=None, description="QoS for paused accounts")

    # User home directory management (used by backend.py)
    enable_user_homedir_account_creation: Optional[bool] = Field(
        default=True, description="Create home directories for users"
    )
    default_homedir_umask: Optional[str] = Field(
        default="0700", description="Umask for created home directories"
    )

    # Periodic limits configuration (nested object)
    periodic_limits: Optional[PeriodicLimitsConfig] = Field(
        default=None, description="Periodic limits configuration"
    )

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
                msg = f"default_homedir_umask must be valid octal permissions (e.g., '0700'): {e}"
                raise ValueError(msg) from e
        return v
