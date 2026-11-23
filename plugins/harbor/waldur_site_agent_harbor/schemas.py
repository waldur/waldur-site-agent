"""Backend settings schema for the harbor backend."""

from __future__ import annotations

from pydantic import Field
from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
)


class HarborBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``harbor`` backend."""

    harbor_url: str = Field(..., description="Harbor base URL", json_schema_extra=SENSITIVE)
    robot_username: str = Field(..., description="Robot account name")
    robot_password: str = Field(..., description="Robot account secret", json_schema_extra=SECRET)
    default_storage_quota_gb: int | None = Field(
        default=None, ge=0, description="Quota when an order has no storage limit (default 10)"
    )
    oidc_group_prefix: str | None = Field(
        default=None, description="Prefix of the OIDC group per Waldur project (default 'waldur-')",
        json_schema_extra=SENSITIVE,
    )
    project_role_id: int | None = Field(
        default=None,
        description="Harbor role for the group: 1 admin, 2 developer (default), 3 guest, 4 maintainer",
        json_schema_extra=SENSITIVE,
    )
