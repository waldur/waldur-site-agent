"""Lightweight health check for Kubernetes probes.

Liveness: checks that the agent's main loop touched the heartbeat file
recently (within ``max_age`` seconds).

Readiness: verifies connectivity to Waldur A with an authenticated
``GET /api/users/me/?field=uuid`` call.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from waldur_api_client.api.users import users_me_retrieve
from waldur_api_client.models.user_field_enum import UserFieldEnum

from waldur_site_agent.common.utils import get_client, init_configuration_from_file

HEARTBEAT_PATH = "/tmp/waldur-site-agent-heartbeat"  # noqa: S108
DEFAULT_MAX_AGE = 300  # seconds

logger = logging.getLogger(__name__)


def touch_heartbeat(path: str = HEARTBEAT_PATH) -> None:
    """Update the heartbeat file mtime. Called from main loops."""
    Path(path).write_text(str(time.time()))


def check_liveness(max_age: int = DEFAULT_MAX_AGE, path: str = HEARTBEAT_PATH) -> bool:
    """Return True if heartbeat file exists and was updated within *max_age* seconds."""
    try:
        mtime = Path(path).stat().st_mtime
        return (time.time() - mtime) < max_age
    except FileNotFoundError:
        return False


def check_readiness(config_file: str) -> bool:
    """Return True if Waldur A responds to GET /api/users/me/?field=uuid."""
    try:
        configuration = init_configuration_from_file(config_file)
    except Exception:
        return False

    for offering in configuration.waldur_offerings:
        try:
            client = get_client(
                offering.api_url,
                offering.api_token,
                configuration.waldur_user_agent,
                offering.verify_ssl,
            )
            users_me_retrieve.sync(client=client, field=[UserFieldEnum.UUID])
            return True
        except Exception:
            logger.debug("Readiness check failed for %s", offering.api_url)
    return False


def main() -> int:
    """CLI entry point for ``waldur_site_healthz``."""
    parser = argparse.ArgumentParser(description="Waldur site agent health check")
    parser.add_argument(
        "--config-file",
        default="/etc/waldur-site-agent/config.yaml",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=DEFAULT_MAX_AGE,
        help="Maximum heartbeat age in seconds for liveness",
    )
    parser.add_argument(
        "--liveness-only",
        action="store_true",
        help="Only check liveness (heartbeat), skip readiness",
    )
    args = parser.parse_args()

    if not check_liveness(max_age=args.max_age):
        logger.warning("Heartbeat stale or missing")
        return 1

    if not args.liveness_only and not check_readiness(args.config_file):
        logger.warning("Cannot reach Waldur API")
        return 1

    return 0


def cli() -> None:
    """Wrapper for console_scripts entry point."""
    sys.exit(main())
