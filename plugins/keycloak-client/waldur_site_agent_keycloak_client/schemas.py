"""Settings schema for the nested ``keycloak`` block shared by several plugins."""

from __future__ import annotations

from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class KeycloakSettingsSchema(BaseModel):
    """The ``keycloak:`` block read by :class:`KeycloakClient`.

    Used by the rancher, k8s-ut-namespace and opennebula plugins.
    """

    model_config = ConfigDict(extra="forbid")

    keycloak_url: Optional[str] = Field(
        default=None, description="Keycloak base URL (default https://localhost/auth/)"
    )
    keycloak_realm: Optional[str] = Field(
        default=None, description="Realm groups are managed in (default 'waldur')"
    )
    keycloak_user_realm: Optional[str] = Field(
        default=None, description="Realm the admin user authenticates against (default 'master')"
    )
    client_id: Optional[str] = Field(
        default=None, description="Client used for the admin login (default 'admin-cli')"
    )
    keycloak_username: Optional[str] = Field(default=None, description="Admin username")
    keycloak_password: Optional[str] = Field(default=None, description="Admin password")
    keycloak_ssl_verify: Optional[Union[bool, str]] = Field(
        default=None,
        description="Verify Keycloak's TLS: true/false, or a CA bundle path (default true)",
    )
