"""Configuration loader for CSCS Storage Proxy."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

import yaml

logger = logging.getLogger(__name__)


@dataclass
class AuthConfig:
    """Authentication configuration."""

    disable_auth: bool = False
    keycloak_url: str = "https://auth-tds.cscs.ch/auth/"
    keycloak_realm: str = "cscs"
    keycloak_client_id: Optional[str] = None
    keycloak_client_secret: Optional[str] = None


@dataclass
class HpcUserApiConfig:
    """HPC User API configuration."""

    api_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    oidc_token_url: Optional[str] = None
    oidc_scope: Optional[str] = None
    socks_proxy: Optional[str] = None  # SOCKS proxy URL (e.g., "socks5://localhost:12345")


@dataclass
class StorageProxyConfig:
    """Configuration for the CSCS Storage Proxy."""

    # Waldur API settings
    waldur_api_url: str
    waldur_api_token: str
    backend_settings: dict[str, Any]
    backend_components: dict[str, dict[str, Any]]
    storage_systems: dict[str, str]
    waldur_verify_ssl: bool = True
    auth: Optional[AuthConfig] = None
    hpc_user_api: Optional[HpcUserApiConfig] = None

    @property
    def offering_slugs(self) -> list[str]:
        """Get list of offering slugs from storage systems configuration."""
        return list(self.storage_systems.values())

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "StorageProxyConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with config_path.open() as f:
            data = yaml.safe_load(f)

        # Parse auth config if present
        auth_config = None
        if "auth" in data:
            auth_data = data["auth"]
            auth_config = AuthConfig(
                disable_auth=auth_data.get("disable_auth", False),
                keycloak_url=auth_data.get("keycloak_url", "https://auth-tds.cscs.ch/auth/"),
                keycloak_realm=auth_data.get("keycloak_realm", "cscs"),
                keycloak_client_id=auth_data.get("keycloak_client_id"),
                keycloak_client_secret=auth_data.get("keycloak_client_secret"),
            )

        # Parse HPC User API config if present
        hpc_user_api_config = None
        if "hpc_user_api" in data:
            hpc_api_data = data["hpc_user_api"]
            hpc_user_api_config = HpcUserApiConfig(
                api_url=hpc_api_data.get("api_url"),
                client_id=hpc_api_data.get("client_id"),
                client_secret=hpc_api_data.get("client_secret"),
                oidc_token_url=hpc_api_data.get("oidc_token_url"),
                oidc_scope=hpc_api_data.get("oidc_scope"),
                socks_proxy=hpc_api_data.get("socks_proxy"),
            )

        return cls(
            waldur_api_url=data["waldur_api_url"],
            waldur_api_token=data["waldur_api_token"],
            waldur_verify_ssl=data.get("waldur_verify_ssl", True),
            backend_settings=data.get("backend_settings", {}),
            backend_components=data.get("backend_components", {}),
            storage_systems=data.get("storage_systems", {}),
            auth=auth_config,
            hpc_user_api=hpc_user_api_config,
        )

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.waldur_api_url:
            msg = "waldur_api_url is required"
            raise ValueError(msg)
        if not self.storage_systems:
            msg = "At least one storage_system mapping is required"
            raise ValueError(msg)
        if not self.backend_components:
            msg = "backend_components is required"
            raise ValueError(msg)

        # Validate that storage component exists
        if "storage" not in self.backend_components:
            msg = "'storage' component is required in backend_components"
            raise ValueError(msg)

        logger.info("Configuration validated successfully")
        logger.info("  Waldur API URL: %s", self.waldur_api_url)
        logger.info("  Storage systems: %s", self.storage_systems)
        logger.info("  Backend components: %s", list(self.backend_components.keys()))
