"""Module for API server used by the CSCS storage management system."""

import logging
import os
import sys
from pathlib import Path

from waldur_site_agent.common.structures import Offering
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_site_agent_cscs_hpc_storage.sync_script import setup_logging

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

# Load configuration
config_path = Path(config_file_path)
if not config_path.exists():
    logger.error("Configuration file not found: %s", config_path)
    sys.exit(1)

logger.info("Using configuration file: %s", config_path)
configuration = load_configuration(str(config_path), user_agent_suffix="sync")
offering_config: Offering = configuration.waldur_offerings[0]

waldur_verify_ssl = os.getenv("WALDUR_VERIFY_SSL", "true").lower() in ("true", "yes")
waldur_client = get_client(
    api_url=offering_config.api_url,
    access_token=offering_config.api_token,
    verify_ssl=waldur_verify_ssl,
)
# Initialize backend
backend_settings = offering_config.backend_settings
backend_components = offering_config.backend_components

cscs_storage_backend = CscsHpcStorageBackend(backend_settings, backend_components)

DISABLE_AUTH = os.getenv("DISABLE_AUTH", "false").lower() in ("true", "yes", "1")
CSCS_KEYCLOAK_URL = os.getenv("CSCS_KEYCLOAK_URL", "https://auth-tds.cscs.ch/auth/")
CSCS_KEYCLOAK_REALM = os.getenv("CSCS_KEYCLOAK_REALM", "cscs")
CSCS_KEYCLOAK_CLIENT_ID = os.getenv("CSCS_KEYCLOAK_CLIENT_ID")
CSCS_KEYCLOAK_CLIENT_SECRET = os.getenv("CSCS_KEYCLOAK_CLIENT_SECRET")
CSCS_KEYCLOAK_REDIRECT_URL = os.getenv(
    "CSCS_KEYCLOAK_REDIRECT_URL", "https://api-storage.waldur.tds.cscs.ch/api/storage-resources/"
)
