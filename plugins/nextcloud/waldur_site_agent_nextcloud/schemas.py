"""Backend settings schema for the nextcloud backend."""

from __future__ import annotations

from typing import Optional

from pydantic import Field
from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class NextcloudBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``nextcloud`` backend."""

    nextcloud_url: str = Field(..., description="Nextcloud base URL")
    admin_username: str = Field(..., description="Admin account used for the OCS API")
    admin_password: str = Field(..., description="Admin password or app password")
    group_prefix: Optional[str] = Field(
        default=None, description="Prefix of the group per resource (default 'waldur-')"
    )
    default_storage_quota_gb: Optional[int] = Field(
        default=None, ge=0, description="Quota when an order has no storage limit (default 25)"
    )
    allow_resharing: Optional[bool] = Field(
        default=None, description="Allow members to re-share the group folder (default false)"
    )
