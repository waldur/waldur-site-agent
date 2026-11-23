"""Backend settings schema for the rancher-kc-crd backend."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from waldur_site_agent.common.plugin_schemas import SECRET, SENSITIVE, CommonBackendSettingsSchema

from .translator import resolve_identity_settings


class RancherKcCrdBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``rancher-kc-crd`` backend."""

    namespace: str | None = Field(
        default=None,
        description="Namespace the custom resources are written to (default 'waldur-system')",
        json_schema_extra=SENSITIVE,
    )
    kubeconfig_path: str | None = Field(
        default=None, description="Kubeconfig file; in-cluster config when unset",
        json_schema_extra=SENSITIVE,
    )
    context: str | None = Field(
        default=None, description="Kubeconfig context", json_schema_extra=SENSITIVE
    )
    role_map: dict[str, str] | None = Field(
        default=None, description="Waldur project role to Rancher project role",
        json_schema_extra=SENSITIVE,
    )
    cluster_role_map: dict[str, str] | None = Field(
        default=None,
        description="Waldur role to Rancher cluster role; no cluster bindings when unset",
        json_schema_extra=SENSITIVE,
    )
    parent_group_name: str | None = Field(
        default=None, description="Keycloak parent group template (default 'c_${cluster_id}')",
        json_schema_extra=SENSITIVE,
    )
    group_name_template: str | None = Field(
        default=None,
        description="Project group template (default 'c_${cluster_id}_${rp_uuid}_${role_name}')",
        json_schema_extra=SENSITIVE,
    )
    cluster_group_name_template: str | None = Field(
        default=None,
        description="Cluster group template (default 'c_${cluster_id}_cluster_${role_name}')",
        json_schema_extra=SENSITIVE,
    )
    keycloak_use_user_id: bool | None = Field(
        default=None,
        description=(
            "Deprecated: same as keycloak_user_identity_source=uuid + keycloak_user_lookup=id"
        ),
        json_schema_extra=SENSITIVE,
    )
    keycloak_user_identity_source: Literal["username", "uuid", "civil_number"] | None = Field(
        default=None,
        description="Waldur user field that supplies the member identifier (default 'username')",
        json_schema_extra=SENSITIVE,
    )
    keycloak_user_lookup: Literal["username", "id", "attribute"] | None = Field(
        default=None,
        description=(
            "How the operator finds the member in the Rancher Keycloak: by username "
            "(default), user ID, or the user attribute named by keycloak_lookup_attribute"
        ),
        json_schema_extra=SENSITIVE,
    )
    keycloak_lookup_attribute: str | None = Field(
        default=None,
        description="Keycloak user attribute to match (keycloak_user_lookup=attribute)",
        json_schema_extra=SENSITIVE,
    )
    keycloak_user_identity_template: str | None = Field(
        default=None,
        description="Template for the member identifier, e.g. 'EE${value}' (default '${value}')",
        json_schema_extra=SENSITIVE,
    )
    keycloak_user_identity_lowercase: bool | None = Field(
        default=None,
        description=(
            "Lowercase the member identifier (default true for username lookup, false otherwise)"
        ),
    )
    waldur_api_url: str | None = Field(
        default=None,
        description=(
            "Waldur API the backend reads resource projects from; "
            "pull_resource does nothing without it"
        ),
        json_schema_extra=SENSITIVE,
    )
    waldur_api_token: str | None = Field(
        default=None, description="Token for waldur_api_url", json_schema_extra=SECRET
    )
    waldur_verify_ssl: bool | None = Field(
        default=None, description="Verify TLS for waldur_api_url (default true)",
        json_schema_extra=SENSITIVE,
    )

    @model_validator(mode="after")
    def validate_user_identity(self) -> RancherKcCrdBackendSettingsSchema:
        """Reject member-identity settings the backend would refuse to start with."""
        resolve_identity_settings(self.model_dump(exclude_none=True))
        return self
