"""Envoy AI Gateway plugin-specific Pydantic schemas for configuration validation."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict, Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class EnvoyAIGatewayComponentSchema(PluginComponentSchema):
    """Envoy AI Gateway component validation (token meters)."""

    model_config = ConfigDict(extra="allow")


class EnvoyAIGatewayBackendSettingsSchema(PluginBackendSettingsSchema):
    """Envoy AI Gateway backend settings validation."""

    model_config = ConfigDict(extra="allow")

    namespace: str = Field(
        ..., description="Namespace holding the Envoy AI Gateway api-key Secrets"
    )
    gateway_url: str = Field(
        ..., description="Public base URL of the gateway, e.g. https://llm-ng.hpc.ut.ee"
    )
    apikey_secret: str = Field(
        default="envoy-ai-gateway-apikeys",
        description="Secret the SecurityPolicy reads keys from (clientID: key)",
    )
    blocked_secret: Optional[str] = Field(
        default=None,
        description="Secret holding blocked keys; defaults to <apikey_secret>-blocked",
    )
    kubeconfig_path: Optional[str] = Field(
        default=None, description="Path to kubeconfig; omit to use in-cluster config"
    )
    kube_context: Optional[str] = Field(
        default=None,
        description="kubeconfig context to target (e.g. docker-desktop); omit for in-cluster",
    )


class EnvoyUsageComponentSchema(PluginComponentSchema):
    """usage reporting component validation (token meters)."""

    model_config = ConfigDict(extra="allow")


class EnvoyUsageBackendSettingsSchema(PluginBackendSettingsSchema):
    """usage reporting backend settings validation (usage warehouse the envoy keys report from)."""

    model_config = ConfigDict(extra="allow")

    api_url: str = Field(..., description="usage warehouse base URL, e.g. http://usage-warehouse:9000")
    api_token: Optional[str] = Field(
        default=None, description="Optional bearer token for the usage warehouse API"
    )
