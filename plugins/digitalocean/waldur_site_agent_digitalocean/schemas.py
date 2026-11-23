"""DigitalOcean plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from pydantic import ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
    PluginComponentSchema,
)


class DigitalOceanComponentSchema(PluginComponentSchema):
    """DigitalOcean-specific component field validation."""

    model_config = ConfigDict(extra="allow")

    backend_name: str | None = Field(
        default=None, description="Backend metric name (optional)"
    )


class DigitalOceanBackendSettingsSchema(CommonBackendSettingsSchema):
    """DigitalOcean-specific backend settings validation."""

    model_config = ConfigDict(extra="allow")

    token: str = Field(..., description="DigitalOcean API token", json_schema_extra=SECRET)

    # Defaults for droplet creation
    default_region: str | None = Field(
        default=None, description="Default region slug"
    )
    default_image: str | None = Field(
        default=None, description="Default image slug or ID"
    )
    default_size: str | None = Field(
        default=None, description="Default size slug"
    )
    default_user_data: str | None = Field(
        default=None, description="Default cloud-init user data", json_schema_extra=SENSITIVE
    )
    default_tags: list[str] | None = Field(
        default=None, description="Default droplet tags"
    )

    # Default SSH key selection
    default_ssh_key_id: int | None = Field(
        default=None, description="Default SSH key ID", json_schema_extra=SENSITIVE
    )
    default_ssh_key_fingerprint: str | None = Field(
        default=None, description="Default SSH key fingerprint", json_schema_extra=SENSITIVE
    )
    default_ssh_key_name: str | None = Field(
        default=None, description="Default SSH key name", json_schema_extra=SENSITIVE
    )
    default_ssh_public_key: str | None = Field(
        default=None, description="Default SSH public key (for create)", json_schema_extra=SENSITIVE
    )

    # Optional size mapping for limit-based resize
    size_mapping: dict[str, dict[str, int]] | None = Field(
        default=None, description="Size slug mapping to component limits"
    )
