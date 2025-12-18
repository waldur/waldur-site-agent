"""Common structures and data classes for the Waldur Site Agent.

This module defines the core data structures used throughout the agent:
- Configuration data classes for offerings and agent settings
- Enumerations for agent operational modes
- Structured representations of Waldur integration configuration

These structures provide type safety and clear interfaces for configuration
management across different agent components and backend plugins.
"""

from __future__ import annotations

from enum import Enum

# Import after to avoid circular imports
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError, field_validator


class AccountingType(Enum):
    """Enumeration of component accounting types."""

    USAGE = "usage"
    LIMIT = "limit"


class BackendComponent(BaseModel):
    """Configuration for a single backend component (e.g., CPU, memory, storage).

    Components define how Waldur marketplace offerings map to backend resources
    and how usage/limits are calculated and reported.

    Plugin-specific fields can be added dynamically and will be preserved.
    """

    model_config = ConfigDict(extra="allow")

    # Core fields (required by all backends)
    measured_unit: str = Field(..., description="Unit of measurement (e.g., 'Hours', 'GB')")
    unit_factor: float = Field(default=1.0, description="Factor for conversion to backend units")
    accounting_type: AccountingType = Field(..., description="Component accounting type")
    label: str = Field(..., description="Human-readable label for display")
    limit: Optional[float] = Field(default=None, description="Component limit value")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for backward compatibility with BaseBackend."""
        # Use mode='json' to automatically serialize enums to their values
        return self.model_dump(exclude_unset=True, mode="json")


class Offering(BaseModel):
    """Configuration structure for a Waldur marketplace offering.

    This model represents a single offering configuration from the agent's
    YAML configuration file. Each offering defines how the agent connects to
    Waldur and which backends to use for different operations.

    Attributes:
        name: Human-readable name for the offering
        waldur_api_url: Base URL for the Waldur API endpoint
        waldur_api_token: Authentication token for Waldur API access
        waldur_offering_uuid: Unique identifier of the offering in Waldur
        backend_type: Legacy backend type identifier (deprecated)
        backend_settings: Backend-specific configuration parameters
        backend_components: Component definitions for the offering
        mqtt_enabled: Whether MQTT event processing is enabled
        websocket_use_tls: Whether to use TLS for websocket connections
        stomp_enabled: Whether STOMP event processing is enabled
        order_processing_backend: Backend name for order processing operations
        membership_sync_backend: Backend name for membership synchronization
        reporting_backend: Backend name for usage reporting
        username_management_backend: Backend name for username management
    """

    name: str = Field(..., description="Human-readable name for the offering")
    waldur_api_url: str = Field(..., description="Base URL for the Waldur API endpoint")
    waldur_api_token: str = Field(..., description="Authentication token for Waldur API")
    waldur_offering_uuid: str = Field(..., description="UUID of the offering in Waldur")

    # Backend configuration
    backend_type: str = Field(..., description="Backend type identifier")
    backend_settings: dict[str, Any] = Field(
        default_factory=dict, description="Backend-specific settings"
    )
    backend_components: dict[str, BackendComponent] = Field(
        default_factory=dict, description="Component definitions"
    )

    # Event processing
    mqtt_enabled: bool = Field(default=False, description="Enable MQTT event processing")
    websocket_use_tls: bool = Field(default=True, description="Use TLS for websocket connections")
    stomp_enabled: bool = Field(default=False, description="Enable STOMP event processing")
    stomp_ws_host: Optional[str] = Field(default=None, description="STOMP WebSocket host")
    stomp_ws_port: Optional[int] = Field(default=None, description="STOMP WebSocket port")
    stomp_ws_path: Optional[str] = Field(default=None, description="STOMP WebSocket path")

    # Backend selection for different operations
    order_processing_backend: Optional[str] = Field(
        default="", description="Backend for order processing"
    )
    membership_sync_backend: Optional[str] = Field(
        default="", description="Backend for membership sync"
    )
    reporting_backend: Optional[str] = Field(default="", description="Backend for usage reporting")
    username_management_backend: str = Field(
        default="base", description="Backend for username management"
    )

    # Additional settings
    resource_import_enabled: bool = Field(default=False, description="Enable resource import")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")

    @field_validator("waldur_api_url")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        """Validate that waldur_api_url is a valid HTTP/HTTPS URL."""
        if not v.startswith(("http://", "https://")):
            msg = "waldur_api_url must start with http:// or https://"
            raise ValueError(msg)
        if not v.endswith("/"):
            v = v + "/"
        return v

    @field_validator("backend_type")
    @classmethod
    def validate_backend_type(cls, v: str) -> str:
        """Convert backend_type to lowercase for consistency."""
        return v.lower()

    # Legacy properties for backward compatibility
    @property
    def api_url(self) -> str:
        """Legacy property for backward compatibility."""
        return self.waldur_api_url

    @property
    def api_token(self) -> str:
        """Legacy property for backward compatibility."""
        return self.waldur_api_token

    @property
    def uuid(self) -> str:
        """Legacy property for backward compatibility."""
        return self.waldur_offering_uuid

    @property
    def backend_components_dict(self) -> dict[str, dict[str, Any]]:
        """Convert backend components to dictionary format for BaseBackend compatibility."""
        result = {}
        for name, component in self.backend_components.items():
            if isinstance(component, BackendComponent):
                result[name] = component.to_dict()
            elif isinstance(component, dict):
                # Handle case where component is still a dict (e.g., from model_copy)
                result[name] = component
            else:
                # Fallback - convert to dict
                result[name] = dict(component)
        return result


class AgentMode(Enum):
    """Enumeration of operational modes for the Waldur Site Agent.

    The agent can operate in different modes, each handling specific aspects
    of the integration between Waldur and backend systems:

    - ORDER_PROCESS: Fetches orders from Waldur and creates/updates backend resources
    - REPORT: Collects usage data from backends and reports to Waldur
    - MEMBERSHIP_SYNC: Synchronizes user memberships between Waldur and backends
    - EVENT_PROCESS: Handles event-based processing via MQTT/STOMP
    """

    ORDER_PROCESS = "order_process"
    REPORT = "report"
    MEMBERSHIP_SYNC = "membership_sync"
    EVENT_PROCESS = "event_process"


class WaldurAgentConfiguration(BaseModel):
    """Complete configuration structure for the Waldur Site Agent.

    This model holds configuration data from YAML files plus runtime fields
    added programmatically from CLI arguments and system information.

    YAML Configuration Fields:
        offerings: List of offering configurations from config file
        sentry_dsn: Sentry DSN URL for error reporting (optional)
        timezone: Timezone for billing period calculations
        global_proxy: Global proxy URL for all API connections (optional)

    Runtime Fields (added programmatically, not from YAML):
        waldur_site_agent_mode: Set via CLI argument (-m order_process)
        waldur_user_agent: Generated HTTP User-Agent string
        waldur_site_agent_version: Agent version from package metadata
        config_file_path: Path to the loaded configuration file
    """

    # YAML configuration fields (validated by Pydantic)
    offerings: list[Offering] = Field(
        default_factory=list, description="List of configured offerings"
    )
    sentry_dsn: Optional[str] = Field(
        default=None, description="Sentry DSN for error reporting (URL)"
    )
    timezone: str = Field(default="UTC", description="Timezone for billing calculations")
    global_proxy: str = Field(default="", description="Global proxy URL for API connections")

    # Runtime fields (set programmatically, not validated)
    waldur_site_agent_mode: str = ""
    waldur_user_agent: str = ""
    waldur_site_agent_version: str = ""
    config_file_path: str = ""

    @field_validator("sentry_dsn")
    @classmethod
    def validate_sentry_dsn(cls, v: Optional[str]) -> Optional[str]:
        """Validate that sentry_dsn is a valid URL when provided."""
        if v is None or v == "":
            return None
        # Use pydantic's URL validation
        try:
            HttpUrl(v)
            return v
        except ValidationError as e:
            msg = f"sentry_dsn must be a valid URL: {e}"
            raise ValueError(msg) from e

    # Legacy property for backward compatibility
    @property
    def waldur_offerings(self) -> list[Offering]:
        """Legacy property for backward compatibility."""
        return self.offerings


class RootConfiguration(BaseModel):
    """Root configuration model for parsing YAML configuration files.

    This model represents the top-level structure of the YAML configuration
    and handles the transformation to WaldurAgentConfiguration.
    """

    offerings: list[dict[str, Any]] = Field(..., description="Raw offering configurations")
    sentry_dsn: Optional[str] = Field(
        default=None, description="Sentry DSN for error reporting (URL)"
    )
    timezone: str = Field(default="UTC", description="Timezone for billing calculations")
    global_proxy: str = Field(default="", description="Global proxy URL for API connections")

    @field_validator("sentry_dsn")
    @classmethod
    def validate_sentry_dsn(cls, v: Optional[str]) -> Optional[str]:
        """Validate that sentry_dsn is a valid URL when provided."""
        if v is None or v == "":
            return None
        # Use pydantic's URL validation
        try:
            HttpUrl(v)
            return v
        except ValidationError as e:
            msg = f"sentry_dsn must be a valid URL: {e}"
            raise ValueError(msg) from e

    def to_agent_configuration(self) -> WaldurAgentConfiguration:
        """Convert raw configuration to typed agent configuration."""
        # Parse backend_components for each offering
        parsed_offerings = []
        for offering_data in self.offerings:
            # Convert backend_components dict to BackendComponent instances
            if "backend_components" in offering_data:
                components = {}
                backend_type = offering_data.get("backend_type", "")

                for name, component_data in offering_data["backend_components"].items():
                    # Apply plugin-specific validation if available
                    from waldur_site_agent.common import plugin_schemas  # noqa: PLC0415

                    validated_data = plugin_schemas.validate_component_with_plugin_schema(
                        backend_type, name, component_data
                    )
                    components[name] = BackendComponent(**validated_data)
                offering_data["backend_components"] = components

            # Apply plugin-specific backend settings validation if available
            if "backend_settings" in offering_data:
                from waldur_site_agent.common import plugin_schemas  # noqa: PLC0415

                backend_type = offering_data.get("backend_type", "")
                validated_settings = plugin_schemas.validate_backend_settings_with_plugin_schema(
                    backend_type, offering_data["backend_settings"]
                )
                offering_data["backend_settings"] = validated_settings

            parsed_offerings.append(Offering(**offering_data))

        return WaldurAgentConfiguration(
            offerings=parsed_offerings,
            sentry_dsn=self.sentry_dsn,
            timezone=self.timezone,
            global_proxy=self.global_proxy,
        )


class AccountType(Enum):
    """Enumeration of service account types for the Waldur Site Agent."""

    SERVICE_ACCOUNT = "service_account"
    COURSE_ACCOUNT = "course_account"
