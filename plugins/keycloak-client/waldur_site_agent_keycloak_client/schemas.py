"""Settings schema for the nested ``keycloak`` block shared by several plugins."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import SECRET, SENSITIVE, NonEmptyStr


class KeycloakSettingsSchema(BaseModel):
    """The ``keycloak:`` block read by :class:`KeycloakClient`.

    Used by the rancher, k8s-ut-namespace and opennebula plugins.
    """

    model_config = ConfigDict(extra="forbid")

    keycloak_url: str | None = Field(
        default=None, description="Keycloak base URL (default https://localhost/auth/)",
        json_schema_extra=SENSITIVE,
    )
    keycloak_realm: str | None = Field(
        default=None, description="Realm groups are managed in (default 'waldur')",
        json_schema_extra=SENSITIVE,
    )
    keycloak_user_realm: str | None = Field(
        default=None, description="Realm the admin user authenticates against (default 'master')",
        json_schema_extra=SENSITIVE,
    )
    client_id: str | None = Field(
        default=None, description="Client used for the admin login (default 'admin-cli')"
    )
    keycloak_username: str | None = Field(default=None, description="Admin username")
    keycloak_password: str | None = Field(
        default=None, description="Admin password", json_schema_extra=SECRET
    )
    keycloak_ssl_verify: bool | NonEmptyStr | None = Field(
        default=None,
        description="Verify Keycloak's TLS: true/false, or a CA bundle path (default true)",
        json_schema_extra=SENSITIVE,
    )
