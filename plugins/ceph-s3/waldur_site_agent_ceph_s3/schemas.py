"""Backend settings schema for the ceph_s3 and croit_usage backends.

Which keys are required depends on ``flavour``; ``settings.validate_settings``
enforces that at backend construction. The schema lists every key the plugin
reads, so a misspelt one is reported.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

from waldur_site_agent.common.plugin_schemas import SECRET, SENSITIVE, CommonBackendSettingsSchema


class CroitReportingSettings(BaseModel):
    """croit's address and credentials, for reading its statistics only."""

    api_url: str = Field(description="croit API URL", json_schema_extra=SENSITIVE)
    token: Optional[str] = Field(default=None, description="croit API token", json_schema_extra=SECRET)
    username: Optional[str] = Field(default=None, description="croit username")
    password: Optional[str] = Field(
        default=None, description="croit password", json_schema_extra=SECRET
    )
    verify_ssl: Optional[bool] = Field(
        default=None, description="Verify TLS (default true)", json_schema_extra=SENSITIVE
    )
    timeout: Optional[float] = Field(default=None, description="HTTP timeout in seconds")
    bucket_state_file: Optional[str] = Field(
        default=None,
        description=(
            "File remembering the buckets each account resource held this billing "
            "period, so buckets deleted before an agent restart stay billed"
        ),
        json_schema_extra=SENSITIVE,
    )


class CephS3BackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``ceph_s3`` (management) and ``croit_usage`` (reporting) backends."""

    flavour: Optional[Literal["croit", "radosgw", "CROIT", "RADOSGW"]] = Field(
        default=None, description="'croit' (default) or 'radosgw'"
    )
    s3_endpoint: Optional[str] = Field(
        default=None, description="S3 endpoint users connect to (required)",
        json_schema_extra=SENSITIVE,
    )
    s3_region: Optional[str] = Field(default=None, description="S3 region (default 'default')")
    # croit flavour
    api_url: Optional[str] = Field(
        default=None, description="croit API URL (croit flavour)", json_schema_extra=SENSITIVE
    )
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
        json_schema_extra=SENSITIVE,
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
        default=None, description="RGW admin API path (default 'admin')",
        json_schema_extra=SENSITIVE,
    )
    croit_reporting: Optional[CroitReportingSettings] = Field(
        default=None,
        description=(
            "croit connection for croit_usage reporting when management uses the "
            "radosgw flavour on a croit cluster"
        ),
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
    verify_ssl: Optional[bool] = Field(
        default=None, description="Verify TLS (default true)", json_schema_extra=SENSITIVE
    )
    timeout: Optional[float] = Field(default=None, description="HTTP timeout in seconds (default 30)")
