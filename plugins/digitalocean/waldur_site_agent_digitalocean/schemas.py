"""DigitalOcean plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class DigitalOceanComponentSchema(PluginComponentSchema):
    """DigitalOcean-specific component field validation."""

    model_config = ConfigDict(extra="allow")

    backend_name: Optional[str] = Field(
        default=None, description="Backend metric name (optional)"
    )


class DigitalOceanBackendSettingsSchema(PluginBackendSettingsSchema):
    """DigitalOcean-specific backend settings validation."""

    model_config = ConfigDict(extra="allow")

    token: str = Field(..., description="DigitalOcean API token")

    # Defaults for droplet creation
    default_region: Optional[str] = Field(
        default=None, description="Default region slug"
    )
    default_image: Optional[str] = Field(
        default=None, description="Default image slug or ID"
    )
    default_size: Optional[str] = Field(
        default=None, description="Default size slug"
    )
    default_user_data: Optional[str] = Field(
        default=None, description="Default cloud-init user data"
    )
    default_tags: Optional[list[str]] = Field(
        default=None, description="Default droplet tags"
    )

    # Default SSH key selection
    default_ssh_key_id: Optional[int] = Field(
        default=None, description="Default SSH key ID"
    )
    default_ssh_key_fingerprint: Optional[str] = Field(
        default=None, description="Default SSH key fingerprint"
    )
    default_ssh_key_name: Optional[str] = Field(
        default=None, description="Default SSH key name"
    )
    default_ssh_public_key: Optional[str] = Field(
        default=None, description="Default SSH public key (for create)"
    )

    # Optional size mapping for limit-based resize
    size_mapping: Optional[dict[str, dict[str, int]]] = Field(
        default=None, description="Size slug mapping to component limits"
    )
