"""Backend settings schema for the okd backend."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
    NonEmptyStr,
)


class OkdBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``okd`` backend."""

    api_url: str | None = Field(
        default=None, description="OpenShift/OKD API URL (default https://localhost:8443)",
        json_schema_extra=SENSITIVE,
    )
    token: str | None = Field(
        default=None, description="Static bearer token", json_schema_extra=SECRET
    )
    token_config: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Token refresh configuration "
            "(token_type: static, service_account, file, oauth)"
        ),
        json_schema_extra=SECRET,
    )
    verify_cert: bool | NonEmptyStr | None = Field(
        default=None, description="Verify TLS: true/false, or a CA bundle path (default true)",
        json_schema_extra=SENSITIVE,
    )
    namespace_prefix: str | None = Field(
        default=None, description="Prefix of created namespaces (default 'waldur-')",
        json_schema_extra=SENSITIVE,
    )
    default_role: str | None = Field(
        default=None, description="Role bound to project members (default 'edit')",
        json_schema_extra=SENSITIVE,
    )
