"""Backend settings schema for the okd backend."""

from __future__ import annotations

from typing import Any, Optional, Union

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import SECRET, CommonBackendSettingsSchema


class OkdBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``okd`` backend."""

    api_url: Optional[str] = Field(
        default=None, description="OpenShift/OKD API URL (default https://localhost:8443)"
    )
    token: Optional[str] = Field(
        default=None, description="Static bearer token", json_schema_extra=SECRET
    )
    token_config: Optional[dict[str, Any]] = Field(
        default=None,
        description=(
            "Token refresh configuration "
            "(token_type: static, service_account, file, oauth)"
        ),
        json_schema_extra=SECRET,
    )
    verify_cert: Optional[Union[bool, str]] = Field(
        default=None, description="Verify TLS: true/false, or a CA bundle path (default true)"
    )
    namespace_prefix: Optional[str] = Field(
        default=None, description="Prefix of created namespaces (default 'waldur-')"
    )
    default_role: Optional[str] = Field(
        default=None, description="Role bound to project members (default 'edit')"
    )
