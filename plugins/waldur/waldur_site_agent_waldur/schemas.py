"""Pydantic validation schemas for Waldur federation plugin."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class TargetComponentConfig(BaseModel):
    """Configuration for a single target component mapping."""

    model_config = ConfigDict(extra="forbid")

    factor: float = Field(
        default=1.0,
        gt=0,
        description="Conversion factor: target_value = source_value * factor",
    )


class WaldurComponentSchema(PluginComponentSchema):
    """Schema for Waldur federation plugin component configuration.

    When ``target_components`` is empty or absent, the component operates in
    passthrough mode (same component name forwarded directly to Waldur B).
    """

    target_components: dict[str, TargetComponentConfig] = Field(
        default_factory=dict,
        description="Mapping of target component names to conversion config. "
        "Empty = passthrough mode.",
    )


class WaldurBackendSettingsSchema(PluginBackendSettingsSchema):
    """Schema for Waldur federation plugin backend settings."""

    target_api_url: str = Field(
        ..., description="Base URL for the target Waldur B API endpoint"
    )
    target_api_token: str = Field(
        ..., description="Authentication token for Waldur B API"
    )
    target_offering_uuid: str = Field(
        ..., description="UUID of the offering on Waldur B"
    )
    target_customer_uuid: str = Field(
        ..., description="UUID of the customer/organization on Waldur B"
    )
    user_match_field: Literal["cuid", "email", "username"] = Field(
        default="cuid",
        description="Field used to match users between Waldur A and Waldur B",
    )
    order_poll_timeout: int = Field(
        default=300,
        gt=0,
        description="Maximum seconds to wait for order completion on Waldur B",
    )
    order_poll_interval: int = Field(
        default=5,
        gt=0,
        description="Seconds between order state poll attempts",
    )
    user_not_found_action: Literal["warn", "fail"] = Field(
        default="warn",
        description="Action when a user cannot be resolved on Waldur B",
    )
    target_stomp_enabled: bool = Field(
        default=False,
        description="Enable STOMP event subscription on Waldur B "
        "for instant order completion notifications",
    )
    target_stomp_offering_uuid: str = Field(
        default="",
        description="UUID of an agent-based (Marketplace.Slurm) offering on Waldur B "
        "for STOMP agent identity registration. Falls back to target_offering_uuid.",
    )
    identity_bridge_source: str = Field(
        default="",
        description="ISD source identifier for identity bridge (e.g. 'isd:efp'). "
        "Required when username_management_backend is 'waldur-identity-bridge'.",
    )
