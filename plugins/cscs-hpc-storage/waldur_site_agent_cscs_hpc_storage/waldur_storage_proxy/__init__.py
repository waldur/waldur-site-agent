"""Module for API server used by the CSCS storage management system."""

import logging
import os
import sys
from pathlib import Path

from waldur_site_agent.common.structures import Offering
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_site_agent_cscs_hpc_storage.sync_script import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

config_file_path = os.environ.get("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH")

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
waldur_client = get_client(
    api_url=offering_config.api_url,
    access_token=offering_config.api_token,
)
# Initialize backend
backend_settings = offering_config.backend_settings
backend_components = offering_config.backend_components

cscs_storage_backend = CscsHpcStorageBackend(backend_settings, backend_components)
