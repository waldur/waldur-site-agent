"""Backend settings schema for the k8s-ut-namespace and k8s-namespace backends."""

from __future__ import annotations

from typing import Literal, Optional, get_args

from pydantic import BaseModel, ConfigDict, Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import SECRET, CommonBackendSettingsSchema

# Pod Security Admission levels the ManagedNamespace CRD accepts, in ascending
# order of restriction. The backend refuses an order that names any other value,
# and the CRD itself would reject it, so the two lists are one list.
SecurityLevel = Literal["privileged", "baseline", "restricted"]
SECURITY_LEVELS: tuple[SecurityLevel, ...] = get_args(SecurityLevel)


class PrometheusSettingsSchema(BaseModel):
    """The ``prometheus:`` block: where consumption is read from, and how.

    Nested rather than flattened into the backend settings so the block's own keys are
    checked against its own model: ``extra="forbid"`` on the parent then reports a
    misspelt ``bearer_tokn`` as a problem with the ``prometheus`` block, which is what
    it is.
    """

    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = Field(
        default=None, description="Prometheus base URL, e.g. https://prometheus.example.org"
    )
    bearer_token: Optional[str] = Field(
        default=None,
        description="Bearer token for the query API",
        json_schema_extra=SECRET,
    )
    username: Optional[str] = Field(
        default=None, description="Username for basic auth, as an alternative to the token"
    )
    password: Optional[str] = Field(
        default=None,
        description="Password for basic auth, used with 'username'",
        json_schema_extra=SECRET,
    )
    ssl_verify: Optional[bool] = Field(
        default=None, description="Verify Prometheus's TLS certificate (default true)"
    )
    timeout: Optional[float] = Field(
        default=None, description="Per-request timeout in seconds (default 30)"
    )
    step: Optional[str] = Field(
        default=None,
        description=(
            "Resolution of the period averages, as a PromQL duration such as '1h' "
            "(default '1h')"
        ),
    )
    queries: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "PromQL per component type, merged over the built-in queries; each template "
            "may use {namespaces}, {window}, {window_seconds} and {step}"
        ),
    )


class K8sUtNamespaceBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``k8s-ut-namespace`` and ``k8s-namespace`` backends."""

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
    quota_enabled: Optional[bool] = Field(
        default=None,
        description="Manage ResourceQuota unless the order says otherwise (default true)",
    )
    default_security_level: Optional[SecurityLevel] = Field(
        default=None,
        description=(
            "Pod Security level applied unless the order says otherwise "
            "(default: the operator's own default)"
        ),
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
    prometheus: Optional[PrometheusSettingsSchema] = Field(
        default=None,
        description="Consumption billing from Prometheus; usage is not reported when unset",
    )
