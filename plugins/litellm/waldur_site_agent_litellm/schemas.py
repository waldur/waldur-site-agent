"""LiteLLM plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class LiteLLMComponentSchema(PluginComponentSchema):
    """LiteLLM management component validation."""

    model_config = ConfigDict(extra="allow")


class LiteLLMBackendSettingsSchema(PluginBackendSettingsSchema):
    """LiteLLM management backend settings validation."""

    model_config = ConfigDict(extra="allow")

    api_url: str = Field(
        ..., description="LiteLLM proxy base URL, e.g. https://litellm.example.com"
    )
    api_token: str = Field(
        ..., description="LiteLLM master or admin key used for the management API"
    )
    models: Optional[list] = Field(
        default=None,
        description="Model allowlist pushed onto every key; omit to allow every model",
    )
    budget_duration: Optional[str] = Field(
        default=None,
        description="Reset period for the max_budget backstop, e.g. '30d'",
    )
    tpm_limit: Optional[int] = Field(
        default=None, description="Default tokens-per-minute cap applied to each key"
    )
    rpm_limit: Optional[int] = Field(
        default=None, description="Default requests-per-minute cap applied to each key"
    )
    verify_ssl: Optional[bool] = Field(
        default=None,
        description="Verify the proxy's TLS certificate (default true)",
    )
    timeout: Optional[float] = Field(
        default=None, description="Per-request timeout in seconds (default 30)"
    )


class LiteLLMUsageComponentSchema(PluginComponentSchema):
    """LiteLLM usage reporting component validation (token and cost meters)."""

    model_config = ConfigDict(extra="allow")


class LiteLLMUsageBackendSettingsSchema(PluginBackendSettingsSchema):
    """LiteLLM usage reporting backend settings validation."""

    model_config = ConfigDict(extra="allow")

    api_url: str = Field(..., description="LiteLLM proxy base URL")
    usage_cache_ttl: Optional[float] = Field(
        default=None,
        description=(
            "Seconds a fetched month of usage rows stays reusable across the resources "
            "of one reporting pass; 0 disables the reuse. Defaults to half the agent's "
            "report period (WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES, 30 by default), "
            "so one pass is covered and the next refetches"
        ),
    )
    api_token: str = Field(
        ..., description="LiteLLM master or admin key used for the spend API"
    )
    verify_ssl: Optional[bool] = Field(
        default=None,
        description="Verify the proxy's TLS certificate (default true)",
    )
    timeout: Optional[float] = Field(
        default=None, description="Per-request timeout in seconds (default 30)"
    )
