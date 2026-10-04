"""Backend settings schema for the rancher backend."""

from __future__ import annotations

from typing import Optional, Union

from pydantic import Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class RancherBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``rancher`` backend."""

    backend_url: Optional[str] = Field(default=None, description="Rancher URL (API is <url>/v3)")
    username: Optional[str] = Field(default=None, description="Rancher API access key")
    password: Optional[str] = Field(default=None, description="Rancher API secret key")
    cluster_id: Optional[str] = Field(default=None, description="Cluster projects are created in")
    verify_cert: Optional[Union[bool, str]] = Field(
        default=None, description="Verify TLS: true/false, or a CA bundle path (default true)"
    )
    default_role: Optional[str] = Field(
        default=None, description="Project role for members (default 'workloads-manage')"
    )
    namespace_labels: Optional[dict[str, str]] = Field(
        default=None, description="Labels set on created namespaces"
    )
    keycloak_enabled: Optional[bool] = Field(
        default=None, description="Manage members through Keycloak groups (default false)"
    )
    keycloak_use_user_id: Optional[bool] = Field(
        default=None, description="Look Keycloak users up by id rather than username (default true)"
    )
    keycloak: Optional[KeycloakSettingsSchema] = Field(
        default=None, description="Keycloak connection, see the keycloak-client README"
    )
