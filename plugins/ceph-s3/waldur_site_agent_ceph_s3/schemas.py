"""Backend settings schema for the ceph_s3 and croit_usage backends.

Which keys are required depends on ``flavour``; ``settings.validate_settings``
enforces that at backend construction. The schema lists every key the plugin
reads, so a misspelt one is reported.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import SECRET, CommonBackendSettingsSchema


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
    password: Optional[str] = Field(
        default=None, description="croit password (croit flavour)", json_schema_extra=SECRET
    )
    token: Optional[str] = Field(
        default=None, description="croit API token, instead of username/password",
        json_schema_extra=SECRET
    )
    tenant_template: Optional[str] = Field(
        default=None,
        description=(
            "RGW tenant for new users, rendered per resource (croit flavour), e.g. "
            "'{customer_slug}_{customer_uuid:.8}'. Fields: resource_slug, "
            "resource_uuid, customer_slug, customer_uuid, project_slug, "
            "project_uuid, offering_slug, offering_uuid"
        ),
    )
    default_tenant: Optional[str] = Field(
        default=None, description="Fixed RGW tenant for new users; older form of tenant_template"
    )
    default_storage_class: Optional[str] = Field(
        default=None, description="Default storage class for new users"
    )
    # radosgw flavour
    admin_access_key: Optional[str] = Field(
        default=None, description="RGW admin access key (radosgw flavour)", json_schema_extra=SECRET
    )
    admin_secret_key: Optional[str] = Field(
        default=None, description="RGW admin secret key (radosgw flavour)", json_schema_extra=SECRET
    )
    admin_path: Optional[str] = Field(
        default=None, description="RGW admin API path (default 'admin')"
    )
    rgw_accounts: Optional[bool] = Field(
        default=None,
        description=(
            "Provision an RGW account with its own root user per resource, so "
            "consumers manage IAM users and policies themselves (radosgw flavour, "
            "Ceph 20.2.2 or later)"
        ),
    )
    # both
    default_placement: Optional[str] = Field(
        default=None, description="Default placement target for new users"
    )
    verify_ssl: Optional[bool] = Field(default=None, description="Verify TLS (default true)")
    timeout: Optional[float] = Field(default=None, description="HTTP timeout in seconds (default 30)")
