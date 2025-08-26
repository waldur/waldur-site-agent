"""Common structures and data classes for the Waldur Site Agent.

This module defines the core data structures used throughout the agent:
- Configuration data classes for offerings and agent settings
- Enumerations for agent operational modes
- Structured representations of Waldur integration configuration

These structures provide type safety and clear interfaces for configuration
management across different agent components and backend plugins.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


@dataclass
class Offering:
    """Configuration structure for a Waldur marketplace offering.

    This data class represents a single offering configuration from the agent's
    YAML configuration file. Each offering defines how the agent connects to
    Waldur and which backends to use for different operations.

    Attributes:
        name: Human-readable name for the offering
        api_url: Base URL for the Waldur API endpoint
        api_token: Authentication token for Waldur API access
        uuid: Unique identifier of the offering in Waldur
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

    name: str = ""
    api_url: str = ""
    api_token: str = ""
    uuid: str = ""
    backend_type: str = ""
    backend_settings: dict = field(default_factory=dict)
    backend_components: dict = field(default_factory=dict)
    mqtt_enabled: bool = False
    websocket_use_tls: bool = True
    stomp_enabled: bool = False
    order_processing_backend: Optional[str] = ""
    membership_sync_backend: Optional[str] = ""
    reporting_backend: Optional[str] = ""
    username_management_backend: str = "base"
    resource_import_enabled: bool = False
    verify_ssl: bool = True


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


@dataclass
class WaldurAgentConfiguration:
    """Complete configuration structure for the Waldur Site Agent.

    This data class holds the parsed and processed configuration from both
    command-line arguments and configuration files. It serves as the central
    configuration object used throughout the agent's operation.

    Attributes:
        waldur_offerings: List of offering configurations to process
        waldur_site_agent_mode: Current operational mode of the agent
        waldur_user_agent: HTTP User-Agent string for API requests
        waldur_site_agent_version: Version of the agent software
        sentry_dsn: Sentry DSN for error reporting (optional)
        timezone: Timezone for billing period calculations
    """

    waldur_offerings: list = field(default_factory=list)
    waldur_site_agent_mode: str = ""
    waldur_user_agent: str = ""
    waldur_site_agent_version: str = ""
    sentry_dsn: str = ""
    timezone: str = ""
