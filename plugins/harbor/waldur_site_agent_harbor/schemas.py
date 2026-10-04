"""Backend settings schema for the harbor backend."""

from __future__ import annotations

from typing import Optional

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class HarborBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``harbor`` backend."""

    harbor_url: str = Field(..., description="Harbor base URL")
    robot_username: str = Field(..., description="Robot account name")
    robot_password: str = Field(..., description="Robot account secret")
    default_storage_quota_gb: Optional[int] = Field(
        default=None, ge=0, description="Quota when an order has no storage limit (default 10)"
    )
    oidc_group_prefix: Optional[str] = Field(
        default=None, description="Prefix of the OIDC group per Waldur project (default 'waldur-')"
    )
    project_role_id: Optional[int] = Field(
        default=None,
        description="Harbor role for the group: 1 admin, 2 developer (default), 3 guest, 4 maintainer",
    )
