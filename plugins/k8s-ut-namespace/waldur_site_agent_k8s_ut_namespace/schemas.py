"""Backend settings schema for the k8s-ut-namespace and k8s-namespace backends."""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Union, get_args

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import SECRET, CommonBackendSettingsSchema


class PrometheusSettingsSchema(BaseModel):
    """The ``prometheus:`` block: where consumption is read from, and how.

    Nested rather than flattened into the backend settings so the block's keys are
    checked against its own model: ``extra="forbid"`` reports a misspelt
    ``bearer_tokn`` as a problem with the ``prometheus`` block, which is what it is.
    """

    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = Field(
        default=None,
        description="Prometheus base URL, e.g. https://prometheus.example.org; no credentials",
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
    ssl_verify: Optional[Union[bool, str]] = Field(
        default=None,
        description="Verify Prometheus's TLS: true/false, or a CA bundle path (default true)",
    )
    timeout: Optional[float] = Field(
        default=None, description="Per-request timeout in seconds (default 30)"
    )
    step: Optional[Union[str, float]] = Field(
        default=None,
        description=(
            "Resolution of the gauge integrals, as a PromQL duration or seconds; at "
            "most 5m (default '1m')"
        ),
    )
    retention: Optional[Union[str, float]] = Field(
        default=None,
        description=(
            "How long Prometheus keeps samples, as a PromQL duration or seconds "
            "(default: read from Prometheus)"
        ),
    )
    closed_period_hours: Optional[float] = Field(
        default=None,
        description="Hours after a month ends during which it is measured again (default 24)",
    )
    selectors: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Label matchers pinning each metric source, merged over the defaults; "
            "used as {<name>_selector} in the queries"
        ),
    )
    queries: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "PromQL per component type, merged over the built-in queries; each must use "
            "{namespaces} and may use {window}, {window_seconds}, {step}, {step_seconds} "
            "and the selector placeholders"
        ),
    )

# Pod Security Admission levels the ManagedNamespace CRD accepts, in ascending
# order of restriction. The backend refuses an order that names any other value,
# and the CRD itself would reject it, so the two lists are one list.
SecurityLevel = Literal["privileged", "baseline", "restricted"]
SECURITY_LEVELS: tuple[SecurityLevel, ...] = get_args(SecurityLevel)


def _normalise_security_level(value: object) -> object:
    """Trim and lowercase a level, as the backend does, before it is validated.

    Without this ``default_security_level: "Restricted"`` would be reported as
    a schema problem at load time and then accepted by the backend anyway. A
    blank value reads as unset.
    """
    if isinstance(value, str):
        return value.strip().lower() or None
    return value


SecurityLevelSetting = Annotated[SecurityLevel, BeforeValidator(_normalise_security_level)]


class K8sUtNamespaceBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``k8s-namespace`` and ``k8s-ut-namespace`` backends."""

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
        description="Enforce the namespace ResourceQuota (default true)",
    )
    allow_quota_override: Optional[bool] = Field(
        default=None,
        description=(
            "Let the order's 'quota_enabled' attribute differ from 'quota_enabled' "
            "(default false)"
        ),
    )
    default_security_level: Optional[SecurityLevelSetting] = Field(
        default=None,
        description=(
            "Pod Security level of new namespaces (default: the operator's own default)"
        ),
    )
    allowed_security_levels: Optional[list[SecurityLevelSetting]] = Field(
        default=None,
        description=(
            "Levels the order's 'security_level' attribute may choose besides "
            "'default_security_level' (default: none)"
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
