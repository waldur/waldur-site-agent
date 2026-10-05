"""Module containing common processing classes, functions, and shared constants.

This module provides:
- Environment variable constants for agent configuration
- Service provider settings and marketplace constants
- Common configuration values used across different agent modes

The constants defined here control timing intervals for different agent modes
and provide default values that can be overridden via environment variables.
"""

import os
import sys

# Handle different Python versions
if sys.version_info >= (3, 10):
    from importlib.metadata import version
else:
    from importlib_metadata import version

# Marketplace offering type constants
MARKETPLACE_SLURM_OFFERING_TYPE = "Marketplace.Slurm"
# Agent processing intervals (in minutes) - configurable via environment variables
WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES = float(
    os.environ.get("WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES", "5")
)
WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES", "30")
)
WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES", "5")
)
WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES = int(
    os.environ.get("WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES", "60")
)
# Event mode: how long a STOMP consumer may stay disconnected (or an offering's
# STOMP setup keep failing transiently) before the agent stops touching its
# liveness heartbeat. Above the listener's own reconnect window (~10 min), so the
# watchdog only acts once the listener has given up.
WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES = float(
    os.environ.get("WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES", "15")
)

# Event mode: a message handler running longer than this counts as a stuck queue
# (it holds back every later message) and, past the STOMP unhealthy threshold,
# withholds the liveness heartbeat. Well above normal order processing.
WALDUR_SITE_AGENT_STOMP_HANDLER_STUCK_AFTER_MINUTES = float(
    os.environ.get("WALDUR_SITE_AGENT_STOMP_HANDLER_STUCK_AFTER_MINUTES", "30")
)
# Event mode: how often to re-apply every resource's paused/downscaled status, so
# a lost or skipped resource message is caught on the next pass. 0 disables it.
WALDUR_SITE_AGENT_RESOURCE_STATUS_RECONCILIATION_MINUTES = float(
    os.environ.get("WALDUR_SITE_AGENT_RESOURCE_STATUS_RECONCILIATION_MINUTES", "60")
)

WALDUR_SITE_AGENT_VERSION = version("waldur-site-agent")
