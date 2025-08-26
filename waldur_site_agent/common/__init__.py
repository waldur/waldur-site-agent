"""Module containing common processing classes, functions, and shared constants.

This module provides:
- Environment variable constants for agent configuration
- Service provider settings and marketplace constants
- Common configuration values used across different agent modes

The constants defined here control timing intervals for different agent modes
and provide default values that can be overridden via environment variables.
"""

import os

# Marketplace offering type constants
MARKETPLACE_SLURM_OFFERING_TYPE = "Marketplace.Slurm"
# Agent processing intervals (in minutes) - configurable via environment variables
WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES", "5")
)
WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES", "30")
)
WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES", "5")
)
waldur_verify_ssl = os.getenv("WALDUR_VERIFY_SSL", "true").lower() in ("true", "yes")
