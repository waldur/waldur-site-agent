"""Backend settings schema for the k8s-ut-namespace and k8s-namespace backends."""

from __future__ import annotations

from typing import Annotated, Literal, Optional, get_args

from pydantic import BeforeValidator, Field
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema

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
