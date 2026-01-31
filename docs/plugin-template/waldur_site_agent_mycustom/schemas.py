"""Optional Pydantic schemas for configuration validation.

Register these via entry points in pyproject.toml to enable
automatic validation of backend_settings and backend_components.
"""

from pydantic import BaseModel, Field


class MyCustomComponentSchema(BaseModel):
    """Schema for validating a single component in backend_components.

    TODO: Add fields matching your component configuration.
    """

    limit: int = Field(ge=0, description="Default limit in Waldur units")
    measured_unit: str = Field(description="Display unit in Waldur UI")
    unit_factor: int = Field(ge=1, description="Waldur-to-backend conversion factor")
    accounting_type: str = Field(
        pattern="^(usage|limit)$",
        description="Component accounting type: 'usage' (metered) or 'limit' (quota)",
    )
    label: str = Field(description="Display label in Waldur UI")


class MyCustomBackendSettingsSchema(BaseModel):
    """Schema for validating backend_settings.

    TODO: Add fields matching your backend_settings configuration.
    """

    default_account: str = Field(default="root", description="Default parent account")
    customer_prefix: str = Field(default="", description="Prefix for customer accounts")
    project_prefix: str = Field(default="", description="Prefix for project accounts")
    allocation_prefix: str = Field(default="", description="Prefix for allocation accounts")
