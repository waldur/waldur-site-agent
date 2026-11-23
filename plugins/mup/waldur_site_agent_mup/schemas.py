"""Backend settings schema for the mup backend."""

from __future__ import annotations

from pydantic import Field
from waldur_site_agent.common.plugin_schemas import (
    SECRET,
    SENSITIVE,
    CommonBackendSettingsSchema,
)


class MUPBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``mup`` backend."""

    api_url: str = Field(..., description="MUP API URL", json_schema_extra=SENSITIVE)
    username: str = Field(..., description="MUP API username")
    password: str = Field(..., description="MUP API password", json_schema_extra=SECRET)
    default_research_field: int | None = Field(
        default=None, description="Research field id for new projects (default 1)"
    )
    default_agency: str | None = Field(
        default=None, description="Funding agency for new projects (default 'FCT')"
    )
    default_storage_limit: int | None = Field(
        default=None, description="Storage limit in GB for new allocations (default 1000)"
    )
    default_user_salutation: str | None = Field(default=None, description="Default 'Dr.'")
    default_user_gender: str | None = Field(default=None, description="Default 'Other'")
    default_user_birth_year: int | None = Field(default=None, description="Default 1990")
    default_user_country: str | None = Field(default=None, description="Default 'Portugal'")
    default_user_institution_type: str | None = Field(
        default=None, description="Default 'Academic'"
    )
    default_user_institution: str | None = Field(
        default=None, description="Default 'Research Institution'"
    )
    default_user_biography: str | None = Field(
        default=None, description="Biography set on users the agent creates"
    )
    user_funding_agency_prefix: str | None = Field(
        default=None, description="Default 'WALDUR-SITE-AGENT-'"
    )
