"""Module for API server used by the CSCS storage management system."""

import logging
import os
import sys

import yaml

from waldur_site_agent.common.utils import get_client
from waldur_site_agent_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_site_agent_cscs_hpc_storage.sync_script import setup_logging
from waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.config import StorageProxyConfig

# Check if debug mode is enabled via environment variable
DEBUG_MODE = os.getenv("DEBUG", "false").lower() in ("true", "yes", "1")

logger = logging.getLogger(__name__)
setup_logging(verbose=DEBUG_MODE)

if DEBUG_MODE:
    logger.info("Debug mode is enabled")
    # Set debug level for the backend logger specifically
    backend_logger = logging.getLogger("waldur_site_agent.backend")
    backend_logger.setLevel(logging.DEBUG)

    # Also set debug for the cscs backend logger
    cscs_logger = logging.getLogger("waldur_site_agent_cscs_hpc_storage.backend")
    cscs_logger.setLevel(logging.DEBUG)

config_file_path = os.getenv("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH")

if not config_file_path:
    logger.error("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH variable is not set")
    sys.exit(1)

# Load simplified proxy configuration
try:
    config = StorageProxyConfig.from_yaml(config_file_path)
    config.validate()
except (FileNotFoundError, ValueError, yaml.YAMLError):
    logger.exception("Failed to load configuration")
    sys.exit(1)

logger.info("Using configuration file: %s", config_file_path)
logger.info("Configured storage systems: %s", config.storage_systems)

# Override verify SSL from environment if set
waldur_verify_ssl = os.getenv("WALDUR_VERIFY_SSL")
if waldur_verify_ssl is not None:
    config.waldur_verify_ssl = waldur_verify_ssl.lower() in ("true", "yes", "1")

# Override proxy from environment if set
waldur_socks_proxy = os.getenv("WALDUR_SOCKS_PROXY")
if waldur_socks_proxy is not None:
    config.waldur_socks_proxy = waldur_socks_proxy

# Log proxy configuration
if config.waldur_socks_proxy:
    logger.info("Using SOCKS proxy for Waldur API connections: %s", config.waldur_socks_proxy)
else:
    logger.info("No SOCKS proxy configured for Waldur API connections")

# Create Waldur API client
WALDUR_API_TOKEN = os.getenv("WALDUR_API_TOKEN", "")
if not WALDUR_API_TOKEN and config.waldur_api_token:
    WALDUR_API_TOKEN = config.waldur_api_token

waldur_client = get_client(
    api_url=config.waldur_api_url,
    access_token=WALDUR_API_TOKEN,
    verify_ssl=config.waldur_verify_ssl,
    proxy=config.waldur_socks_proxy,
)

cscs_storage_backend = CscsHpcStorageBackend(config.backend_settings, config.backend_components)

# Authentication settings - environment variables override config file
disable_auth_env = os.getenv("DISABLE_AUTH")
if disable_auth_env is not None:
    DISABLE_AUTH = disable_auth_env.lower() in ("true", "yes", "1")
elif config.auth:
    DISABLE_AUTH = config.auth.disable_auth
else:
    DISABLE_AUTH = False

CSCS_KEYCLOAK_URL = os.getenv("CSCS_KEYCLOAK_URL")
if CSCS_KEYCLOAK_URL is None and config.auth:
    CSCS_KEYCLOAK_URL = config.auth.keycloak_url
if CSCS_KEYCLOAK_URL is None:
    CSCS_KEYCLOAK_URL = "https://auth-tds.cscs.ch/auth/"

CSCS_KEYCLOAK_REALM = os.getenv("CSCS_KEYCLOAK_REALM")
if CSCS_KEYCLOAK_REALM is None and config.auth:
    CSCS_KEYCLOAK_REALM = config.auth.keycloak_realm
if CSCS_KEYCLOAK_REALM is None:
    CSCS_KEYCLOAK_REALM = "cscs"

CSCS_KEYCLOAK_CLIENT_ID = os.getenv("CSCS_KEYCLOAK_CLIENT_ID")
if CSCS_KEYCLOAK_CLIENT_ID is None and config.auth:
    CSCS_KEYCLOAK_CLIENT_ID = config.auth.keycloak_client_id

CSCS_KEYCLOAK_CLIENT_SECRET = os.getenv("CSCS_KEYCLOAK_CLIENT_SECRET")
if CSCS_KEYCLOAK_CLIENT_SECRET is None and config.auth:
    CSCS_KEYCLOAK_CLIENT_SECRET = config.auth.keycloak_client_secret

CSCS_KEYCLOAK_REDIRECT_URL = os.getenv(
    "CSCS_KEYCLOAK_REDIRECT_URL", "https://api-storage.waldur.tds.cscs.ch/api/storage-resources/"
)
