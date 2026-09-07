"""LiteLLM plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class LiteLLMComponentSchema(PluginComponentSchema):
    """LiteLLM management component validation."""

    model_config = ConfigDict(extra="allow")


class OpenWebUISettingsSchema(BaseModel):
    """Open WebUI (chat surface) settings, nested under the LiteLLM backend settings.

    Optional as a whole: an offering that sells API keys only omits the block and the
    agent never talks to Open WebUI.
    """

    model_config = ConfigDict(extra="allow")

    api_url: str = Field(
        ..., description="Open WebUI base URL for the admin API, e.g. https://chat.example.com"
    )
    api_token: str = Field(..., description="Admin API token used to manage accounts")
    url: Optional[str] = Field(
        default=None,
        description=(
            "User-facing chat URL surfaced on the resource in the portal; "
            "defaults to not advertising a chat surface at all"
        ),
    )
    account_provisioning: Optional[str] = Field(
        default="sso",
        description=(
            "'sso' (default): accounts appear on first login through the identity "
            "provider and the agent only ever revokes. 'managed_password': the agent "
            "creates the account with 'initial_password'"
        ),
    )
    initial_password: Optional[str] = Field(
        default=None,
        description=(
            "Temporary password for accounts the agent creates; required when "
            "account_provisioning is 'managed_password'"
        ),
    )
    delete_accounts_on_removal: Optional[bool] = Field(
        default=False,
        description=(
            "Delete the Open WebUI account when access is revoked instead of demoting "
            "it to 'pending'. Demotion is the default: it ends access just as "
            "completely and keeps the person's chat history"
        ),
    )
    verify_ssl: Optional[bool] = Field(
        default=None, description="Verify Open WebUI's TLS certificate (default true)"
    )
    timeout: Optional[float] = Field(
        default=None, description="Per-request timeout in seconds (default 30)"
    )


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
    openwebui: Optional[OpenWebUISettingsSchema] = Field(
        default=None,
        description=(
            "Chat surface. Omit for an API-only offering. When set, membership sync "
            "also provisions and revokes Open WebUI accounts, and chat usage is billed "
            "back to the resource that owns the person's address"
        ),
    )


class LiteLLMUsageComponentSchema(PluginComponentSchema):
    """LiteLLM usage reporting component validation (token and cost meters)."""

    model_config = ConfigDict(extra="allow")


class LiteLLMUsageBackendSettingsSchema(PluginBackendSettingsSchema):
    """LiteLLM usage reporting backend settings validation."""

    model_config = ConfigDict(extra="allow")

    api_url: str = Field(..., description="LiteLLM proxy base URL")
    component_metrics: Optional[dict] = Field(
        default=None,
        description=(
            "Extra Waldur component -> usage metric pairs, for components not named "
            "after the metric they read. Components called input_tokens, "
            "output_tokens or token_cost are metered without being listed here. The "
            "metric must be one of those three; token_cost is LiteLLM's own per-model "
            "USD spend. Two components may read the same metric, which is how an "
            "offering caps cost on a LIMIT component and bills it on a USAGE one: "
            "{inference_cost: token_cost}"
        ),
    )
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
