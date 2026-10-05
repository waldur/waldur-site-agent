"""Backend settings schema for the k8s-ut-namespace backend."""

from __future__ import annotations

from typing import Optional

from pydantic import Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class K8sUtNamespaceBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``k8s-ut-namespace`` backend."""

    kubeconfig_path: Optional[str] = Field(
        default=None, description="Kubeconfig file; in-cluster config when unset"
    )
    cr_namespace: Optional[str] = Field(
        default=None, description="Namespace of the ManagedNamespace CRs (default 'waldur-system')"
    )
    namespace_prefix: Optional[str] = Field(
        default=None, description="Prefix of managed namespaces (default 'waldur-')"
    )
    default_role: Optional[str] = Field(
        default=None, description="Role for members without a mapping (default 'readwrite')"
    )
    role_mapping: Optional[dict[str, str]] = Field(
        default=None, description="Waldur role to namespace role, merged over the defaults"
    )
    component_quota_mapping: Optional[dict[str, str]] = Field(
        default=None, description="Waldur component to ResourceQuota key, merged over the defaults"
    )
    namespace_labels: Optional[dict[str, str]] = Field(
        default=None, description="Labels set on managed namespaces"
    )
    namespace_annotations: Optional[dict[str, str]] = Field(
        default=None, description="Annotations set on managed namespaces"
    )
    sync_users_to_cr: Optional[bool] = Field(
        default=None, description="Write member identities into the CR (default false)"
    )
    cr_user_identity_field: Optional[str] = Field(
        default=None, description="User field written to the CR (default 'email')"
    )
    cr_user_identity_lowercase: Optional[bool] = Field(
        default=None, description="Lowercase identities written to the CR (default false)"
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
