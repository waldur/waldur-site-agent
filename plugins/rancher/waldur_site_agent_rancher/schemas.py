"""Backend settings schema for the rancher backend."""

from __future__ import annotations

from pydantic import Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
    NonEmptyStr,
)


class RancherBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``rancher`` backend."""

    backend_url: str | None = Field(
        default=None, description="Rancher URL (API is <url>/v3)", json_schema_extra=SENSITIVE
    )
    username: str | None = Field(default=None, description="Rancher API access key")
    password: str | None = Field(
        default=None, description="Rancher API secret key", json_schema_extra=SECRET
    )
    cluster_id: str | None = Field(
        default=None, description="Cluster projects are created in", json_schema_extra=SENSITIVE
    )
    verify_cert: bool | NonEmptyStr | None = Field(
        default=None, description="Verify TLS: true/false, or a CA bundle path (default true)",
        json_schema_extra=SENSITIVE,
    )
    default_role: str | None = Field(
        default=None, description="Project role for members (default 'workloads-manage')",
        json_schema_extra=SENSITIVE,
    )
    namespace_labels: dict[str, str] | None = Field(
        default=None, description="Labels set on created namespaces", json_schema_extra=SENSITIVE
    )
    keycloak_enabled: bool | None = Field(
        default=None, description="Manage members through Keycloak groups (default false)"
    )
    keycloak_use_user_id: bool | None = Field(
        default=None,
        description="Look Keycloak users up by id rather than username (default true)",
        json_schema_extra=SENSITIVE,
    )
    keycloak: KeycloakSettingsSchema | None = Field(
        default=None, description="Keycloak connection, see the keycloak-client README"
    )
