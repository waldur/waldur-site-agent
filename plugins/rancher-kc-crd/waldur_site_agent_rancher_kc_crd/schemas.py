"""Backend settings schema for the rancher-kc-crd backend."""

from __future__ import annotations

from typing import Optional

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


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
        default=None, description="Identify members by Waldur user UUID rather than username"
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
