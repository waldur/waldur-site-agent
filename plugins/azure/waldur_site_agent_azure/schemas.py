"""Azure plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class AzureComponentSchema(PluginComponentSchema):
    """Azure-specific component field validation."""

    model_config = ConfigDict(extra="allow")

    backend_name: Optional[str] = Field(
        default=None, description="Backend metric name (optional)"
    )


class AzureBackendSettingsSchema(PluginBackendSettingsSchema):
    """Azure-specific backend settings validation.

    The four credential fields carry the same names as the ``options`` keys of
    mastermind's Azure service settings, so an offering taken over from
    mastermind gets its credentials copied across without re-mapping.
    """

    model_config = ConfigDict(extra="allow")

    subscription_id: str = Field(..., description="Azure subscription ID")
    tenant_id: str = Field(..., description="Azure AD tenant ID")
    client_id: str = Field(..., description="Service principal client ID")
    client_secret: str = Field(..., description="Service principal client secret")

    # Defaults for resource creation. Locations and sizes are Azure-wide, so an
    # offering that serves one region names it once here instead of on every order.
    default_location: Optional[str] = Field(
        default=None, description="Default Azure region, e.g. 'westeurope'"
    )
    default_resource_group: Optional[str] = Field(
        default=None,
        description="Resource group to place resources in; created on demand when unset",
    )
    default_size: Optional[str] = Field(
        default=None, description="Default virtual machine size, e.g. 'Standard_B1s'"
    )
    default_image: Optional[str] = Field(
        default=None,
        description="Default image reference as 'publisher:offer:sku:version'",
    )

    # Every machine gets its own network, so the ranges may repeat across
    # machines; they only need to avoid colliding with what the operator peers
    # the network with.
    network_cidr: Optional[str] = Field(
        default=None, description="Address space of the virtual network (default 10.0.0.0/16)"
    )
    subnet_cidr: Optional[str] = Field(
        default=None, description="Address range of the subnet (default 10.0.0.0/24)"
    )

    allowed_ssh_ranges: Optional[list[str]] = Field(
        default=None,
        description="Address ranges allowed to reach machines over SSH, as CIDR prefixes. "
        "Without any, a machine accepts no SSH connections.",
    )
