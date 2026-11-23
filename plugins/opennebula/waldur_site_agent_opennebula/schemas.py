"""Backend settings schema for the opennebula backend."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field
from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
)
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema


class OpenNebulaBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``opennebula`` backend.

    The network and VM keys are fallbacks: a value set on the resource's
    offering plugin options wins over the one here.
    """

    api_url: str = Field(
        ..., description="OpenNebula XML-RPC endpoint", json_schema_extra=SENSITIVE
    )
    credentials: str = Field(..., description="'username:password'", json_schema_extra=SECRET)
    resource_type: Literal["vdc", "vm"] | None = Field(
        default=None, description="What a resource is: 'vdc' (default) or 'vm'"
    )
    zone_id: int | None = Field(default=None, description="OpenNebula zone id (default 0)")
    cluster_ids: list[int] | None = Field(default=None, description="Cluster ids for VDCs")
    create_opennebula_user: bool | None = Field(
        default=None, description="Create an OpenNebula user per resource (default false)"
    )
    keycloak_enabled: bool | None = Field(
        default=None, description="Manage members through Keycloak groups (default false)"
    )
    keycloak: KeycloakSettingsSchema | None = Field(
        default=None, description="Keycloak connection, see the keycloak-client README"
    )
    saml_mapping_file: str | None = Field(
        default=None,
        description="SAML group mapping file (default /var/lib/one/keycloak_groups.yaml)",
        json_schema_extra=SENSITIVE,
    )
    default_user_role: str | None = Field(
        default=None, description="VDC role for members without a mapping (default 'user')",
        json_schema_extra=SENSITIVE,
    )
    vdc_roles: list[dict[str, Any]] | None = Field(
        default=None, description="VDC role definitions; see the plugin README for the default",
        json_schema_extra=SENSITIVE,
    )
    # Network fallbacks (VDC)
    external_network_id: int | None = Field(
        default=None, description="External network id", json_schema_extra=SENSITIVE
    )
    virtual_router_template_id: int | None = Field(
        default=None, description="Virtual router template id", json_schema_extra=SENSITIVE
    )
    vn_mad: str | None = Field(default=None, description="Network driver (default 'vxlan')")
    vxlan_phydev: str | None = Field(default=None, description="Physical device for VXLAN")
    default_dns: str | None = Field(
        default=None, description="DNS server for new networks", json_schema_extra=SENSITIVE
    )
    internal_network_base: str | None = Field(default=None, description="Internal network base")
    internal_network_prefix: int | None = Field(
        default=None, description="Prefix length of the subnet pool (default 8)"
    )
    subnet_prefix_length: int | None = Field(default=None, description="Subnet prefix length")
    security_group_defaults: list[dict[str, Any]] | None = Field(
        default=None, description="Default inbound rules for new VDCs", json_schema_extra=SENSITIVE
    )
    sched_requirements: str | None = Field(default=None, description="Scheduler requirements")
    # VM fallbacks
    parent_vdc_backend_id: str | None = Field(
        default=None, description="VDC a VM is created in", json_schema_extra=SENSITIVE
    )
    template_id: int | str | None = Field(
        default=None, description="VM template id", json_schema_extra=SENSITIVE
    )
