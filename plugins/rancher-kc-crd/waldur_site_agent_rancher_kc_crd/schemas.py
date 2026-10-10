"""Backend settings schema for the rancher-kc-crd backend."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field, model_validator

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema

from .translator import resolve_identity_settings


class RancherKcCrdBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``rancher-kc-crd`` backend."""

    namespace: Optional[str] = Field(
        default=None,
        description="Namespace the custom resources are written to (default 'waldur-system')",
    )
    kubeconfig_path: Optional[str] = Field(
        default=None, description="Kubeconfig file; in-cluster config when unset"
    )
    context: Optional[str] = Field(default=None, description="Kubeconfig context")
    role_map: Optional[dict[str, str]] = Field(
        default=None, description="Waldur project role to Rancher project role"
    )
    cluster_role_map: Optional[dict[str, str]] = Field(
        default=None,
        description="Waldur role to Rancher cluster role; no cluster bindings when unset",
    )
    parent_group_name: Optional[str] = Field(
        default=None, description="Keycloak parent group template (default 'c_${cluster_id}')"
    )
    group_name_template: Optional[str] = Field(
        default=None,
        description="Project group template (default 'c_${cluster_id}_${rp_uuid}_${role_name}')",
    )
    cluster_group_name_template: Optional[str] = Field(
        default=None,
        description="Cluster group template (default 'c_${cluster_id}_cluster_${role_name}')",
    )
    keycloak_use_user_id: Optional[bool] = Field(
        default=None,
        description=(
            "Deprecated: same as keycloak_user_identity_source=uuid + keycloak_user_lookup=id"
        ),
    )
    keycloak_user_identity_source: Optional[Literal["username", "uuid", "civil_number"]] = Field(
        default=None,
        description="Waldur user field that supplies the member identifier (default 'username')",
    )
    keycloak_user_lookup: Optional[Literal["username", "id", "attribute"]] = Field(
        default=None,
        description=(
            "How the operator finds the member in the Rancher Keycloak: by username "
            "(default), user ID, or the user attribute named by keycloak_lookup_attribute"
        ),
    )
    keycloak_lookup_attribute: Optional[str] = Field(
        default=None,
        description="Keycloak user attribute to match (keycloak_user_lookup=attribute)",
    )
    keycloak_user_identity_template: Optional[str] = Field(
        default=None,
        description="Template for the member identifier, e.g. 'EE${value}' (default '${value}')",
    )
    keycloak_user_identity_lowercase: Optional[bool] = Field(
        default=None,
        description=(
            "Lowercase the member identifier (default true for username lookup, false otherwise)"
        ),
    )
    waldur_api_url: Optional[str] = Field(
        default=None,
        description=(
            "Waldur API the backend reads resource projects from; "
            "pull_resource does nothing without it"
        ),
    )
    waldur_api_token: Optional[str] = Field(default=None, description="Token for waldur_api_url")
    waldur_verify_ssl: Optional[bool] = Field(
        default=None, description="Verify TLS for waldur_api_url (default true)"
    )

    @model_validator(mode="after")
    def validate_user_identity(self) -> RancherKcCrdBackendSettingsSchema:
        """Reject member-identity settings the backend would refuse to start with."""
        resolve_identity_settings(self.model_dump(exclude_none=True))
        return self
