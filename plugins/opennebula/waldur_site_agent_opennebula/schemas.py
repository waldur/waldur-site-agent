"""Backend settings schema for the opennebula backend."""

from __future__ import annotations

from typing import Any, Literal, Optional, Union

from pydantic import Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class OpenNebulaBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``opennebula`` backend.

    The network and VM keys are fallbacks: a value set on the resource's
    offering plugin options wins over the one here.
    """

    api_url: str = Field(..., description="OpenNebula XML-RPC endpoint")
    credentials: str = Field(..., description="'username:password'")
    resource_type: Optional[Literal["vdc", "vm"]] = Field(
        default=None, description="What a resource is: 'vdc' (default) or 'vm'"
    )
    zone_id: Optional[int] = Field(default=None, description="OpenNebula zone id (default 0)")
    cluster_ids: Optional[list[int]] = Field(default=None, description="Cluster ids for VDCs")
    create_opennebula_user: Optional[bool] = Field(
        default=None, description="Create an OpenNebula user per resource (default false)"
    )
    keycloak_enabled: Optional[bool] = Field(
        default=None, description="Manage members through Keycloak groups (default false)"
    )
    keycloak: Optional[KeycloakSettingsSchema] = Field(
        default=None, description="Keycloak connection, see the keycloak-client README"
    )
    saml_mapping_file: Optional[str] = Field(
        default=None,
        description="SAML group mapping file (default /var/lib/one/keycloak_groups.yaml)",
    )
    default_user_role: Optional[str] = Field(
        default=None, description="VDC role for members without a mapping (default 'user')"
    )
    vdc_roles: Optional[list[dict[str, Any]]] = Field(
        default=None, description="VDC role definitions; see the plugin README for the default"
    )
    # Network fallbacks (VDC)
    external_network_id: Optional[int] = Field(default=None, description="External network id")
    virtual_router_template_id: Optional[int] = Field(
        default=None, description="Virtual router template id"
    )
    vn_mad: Optional[str] = Field(default=None, description="Network driver (default 'vxlan')")
    vxlan_phydev: Optional[str] = Field(default=None, description="Physical device for VXLAN")
    default_dns: Optional[str] = Field(default=None, description="DNS server for new networks")
    internal_network_base: Optional[str] = Field(default=None, description="Internal network base")
    internal_network_prefix: Optional[int] = Field(
        default=None, description="Prefix length of the subnet pool (default 8)"
    )
    subnet_prefix_length: Optional[int] = Field(default=None, description="Subnet prefix length")
    security_group_defaults: Optional[list[dict[str, Any]]] = Field(
        default=None, description="Default inbound rules for new VDCs"
    )
    sched_requirements: Optional[str] = Field(default=None, description="Scheduler requirements")
    # VM fallbacks
    parent_vdc_backend_id: Optional[str] = Field(default=None, description="VDC a VM is created in")
    template_id: Optional[Union[int, str]] = Field(default=None, description="VM template id")
