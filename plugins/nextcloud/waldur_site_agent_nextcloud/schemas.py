"""Backend settings schema for the nextcloud backend."""

from __future__ import annotations

from pydantic import Field
from waldur_site_agent.common.plugin_schemas import SECRET, SENSITIVE, CommonBackendSettingsSchema


class NextcloudBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``nextcloud`` backend."""

    nextcloud_url: str = Field(..., description="Nextcloud base URL", json_schema_extra=SENSITIVE)
    admin_username: str = Field(..., description="Admin account used for the OCS API")
    admin_password: str = Field(
        ..., description="Admin password or app password", json_schema_extra=SECRET
    )
    group_prefix: str | None = Field(
        default=None, description="Prefix of the group per resource (default 'waldur-')",
        json_schema_extra=SENSITIVE,
    )
    default_storage_quota_gb: int | None = Field(
        default=None, ge=0, description="Quota when an order has no storage limit (default 25)"
    )
    allow_resharing: bool | None = Field(
        default=None, description="Allow members to re-share the group folder (default false)",
        json_schema_extra=SENSITIVE,
    )
