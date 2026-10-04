"""Backend settings schema for the ceph_s3 and croit_usage backends.

Which keys are required depends on ``flavour``; ``settings.validate_settings``
enforces that at backend construction. The schema lists every key the plugin
reads, so a misspelt one is reported.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class CephS3BackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``ceph_s3`` (management) and ``croit_usage`` (reporting) backends."""

    flavour: Optional[Literal["croit", "radosgw", "CROIT", "RADOSGW"]] = Field(
        default=None, description="'croit' (default) or 'radosgw'"
    )
    s3_endpoint: Optional[str] = Field(
        default=None, description="S3 endpoint users connect to (required)"
    )
    s3_region: Optional[str] = Field(default=None, description="S3 region (default 'default')")
    # croit flavour
    api_url: Optional[str] = Field(default=None, description="croit API URL (croit flavour)")
    username: Optional[str] = Field(default=None, description="croit username (croit flavour)")
    password: Optional[str] = Field(default=None, description="croit password (croit flavour)")
    token: Optional[str] = Field(
        default=None, description="croit API token, instead of username/password"
    )
    default_tenant: Optional[str] = Field(default=None, description="RGW tenant for new users")
    default_storage_class: Optional[str] = Field(
        default=None, description="Default storage class for new users"
    )
    # radosgw flavour
    admin_access_key: Optional[str] = Field(
        default=None, description="RGW admin access key (radosgw flavour)"
    )
    admin_secret_key: Optional[str] = Field(
        default=None, description="RGW admin secret key (radosgw flavour)"
    )
    admin_path: Optional[str] = Field(
        default=None, description="RGW admin API path (default 'admin')"
    )
    # both
    default_placement: Optional[str] = Field(
        default=None, description="Default placement target for new users"
    )
    verify_ssl: Optional[bool] = Field(default=None, description="Verify TLS (default true)")
    timeout: Optional[float] = Field(default=None, description="HTTP timeout in seconds (default 30)")
