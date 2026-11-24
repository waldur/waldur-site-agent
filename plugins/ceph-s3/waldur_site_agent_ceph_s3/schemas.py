"""Backend settings schema for the ceph_s3, croit_usage and rgw_usage backends.

Users are managed through RGW Admin Ops; ``settings.validate_settings`` enforces
the required keys at backend construction, and refuses croit's former management
settings with the steps that migrate them. The schema lists every key the plugin
reads, so a misspelt one is reported.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field
from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
)


class CroitReportingSettings(BaseModel):
    """croit's address and credentials, for reading its statistics only."""

    api_url: str = Field(description="croit API URL", json_schema_extra=SENSITIVE)
    token: str | None = Field(default=None, description="croit API token", json_schema_extra=SECRET)
    username: str | None = Field(default=None, description="croit username")
    password: str | None = Field(
        default=None, description="croit password", json_schema_extra=SECRET
    )
    verify_ssl: bool | None = Field(
        default=None, description="Verify TLS (default true)", json_schema_extra=SENSITIVE
    )
    timeout: float | None = Field(default=None, description="HTTP timeout in seconds")
    bucket_state_file: str | None = Field(
        default=None,
        description=(
            "File remembering the buckets each account resource held this billing "
            "period, so buckets deleted before an agent restart stay billed"
        ),
        json_schema_extra=SENSITIVE,
    )


class CephS3BackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``ceph_s3``, ``croit_usage`` and ``rgw_usage`` backends.

    ``ceph_s3`` is the management backend; the other two are reporting backends.
    """

    flavour: Literal["radosgw", "RADOSGW"] | None = Field(
        default=None, description="'radosgw', the only one (may be left out)"
    )
    s3_endpoint: str | None = Field(
        default=None, description="S3 endpoint users connect to (required)",
        json_schema_extra=SENSITIVE,
    )
    s3_region: str | None = Field(default=None, description="S3 region (default 'default')")
    tenant_template: str | None = Field(
        default=None,
        description=(
            "RGW tenant each new resource gets for itself, and its backend id. Must "
            "contain {resource_uuid} or {resource_slug}; fields can be cut with "
            "{field:.N}. Default: '{customer_abbreviation:.12}_{project_name:.12}_"
            "{resource_name:.20}_{resource_uuid:.8}'. Fields: resource_slug, "
            "resource_uuid, resource_name, customer_slug, customer_uuid, "
            "customer_name, customer_abbreviation, project_slug, project_uuid, "
            "project_name, offering_slug, offering_uuid"
        ),
        json_schema_extra=SENSITIVE,
    )
    default_tenant: str | None = Field(
        default=None,
        description=(
            "No longer supported: one tenant shared by every resource. The agent "
            "refuses to start while it is set"
        ),
    )
    default_storage_class: str | None = Field(
        default=None,
        description="Default storage class for new users; needs default_placement",
    )
    # RGW Admin Ops
    admin_access_key: str | None = Field(
        default=None, description="RGW admin access key (required)", json_schema_extra=SECRET
    )
    admin_secret_key: str | None = Field(
        default=None, description="RGW admin secret key (required)", json_schema_extra=SECRET
    )
    admin_path: str | None = Field(
        default=None, description="RGW admin API path (default 'admin')",
        json_schema_extra=SENSITIVE,
    )
    croit_reporting: CroitReportingSettings | None = Field(
        default=None,
        description=(
            "croit connection for croit_usage reporting: croit is read for its "
            "statistics only"
        ),
    )
    rgw_accounts: bool | None = Field(
        default=None,
        description=(
            "Provision an RGW account with its own root user per resource, so "
            "consumers manage IAM users and policies themselves (Ceph 20.2.2 or later)"
        ),
    )
    storage_size: Literal["logical", "allocated"] | None = Field(
        default=None,
        description=(
            "Bucket size rgw_usage bills: 'logical' (default; the bytes stored, "
            "as croit's statistics bill them) or 'allocated' (RGW size_actual, "
            "including allocation overhead)"
        ),
    )
    # both
    default_placement: str | None = Field(
        default=None, description="Default placement target for new users"
    )
    verify_ssl: bool | None = Field(
        default=None, description="Verify TLS (default true)", json_schema_extra=SENSITIVE
    )
    timeout: float | None = Field(default=None, description="HTTP timeout in seconds (default 30)")
