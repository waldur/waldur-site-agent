"""Lightweight health check for Kubernetes probes.

Liveness: checks that the agent's main loop touched the heartbeat file
recently (within ``max_age`` seconds).

Readiness: verifies connectivity to Waldur A with an authenticated
``GET /api/users/me/?field=uuid`` call.

Nothing outside the standard library is imported at module level. The kubelet
runs this as a fresh process on every probe tick, and importing
``waldur_api_client`` (~3000 generated model modules) together with every
installed backend plugin cost ~2.3 s per invocation -- more on a CPU-capped
pod whose read-only root filesystem forces Python to recompile the sources
each time. That overran ``timeoutSeconds`` and got containers killed by the
liveness probe. Liveness needs one ``stat()``; readiness imports the client
lazily, where the cost is paid against a network call anyway.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

HEARTBEAT_PATH = "/tmp/waldur-site-agent-heartbeat"  # noqa: S108
DEFAULT_MAX_AGE = 300  # seconds
# Kept below the probe's own timeoutSeconds: the API client otherwise defaults
# to a 600s timeout, so a stalled Waldur blocks the probe process until the
# kubelet kills it, which reads as a failure with no diagnostics.
DEFAULT_READINESS_TIMEOUT = 5  # seconds

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


def check_readiness(config_file: str, timeout: float = DEFAULT_READINESS_TIMEOUT) -> bool:
    """Return True if Waldur A responds to GET /api/users/me/?field=uuid."""
    # Deliberately not at module level: see the module docstring. PLC0415 is
    # the rule this whole change exists to break.
    from waldur_api_client.api.users import users_me_retrieve  # noqa: PLC0415
    from waldur_api_client.models.user_me_field_enum import UserMeFieldEnum  # noqa: PLC0415

    from waldur_site_agent.common.utils import (  # noqa: PLC0415
        get_client_for_offering,
        init_configuration_from_file,
    )

    try:
        configuration = init_configuration_from_file(config_file)
    except Exception:
        return False

    for offering in configuration.waldur_offerings:
        try:
            client = get_client_for_offering(
                offering, configuration.waldur_user_agent, timeout=timeout
            )
            users_me_retrieve.sync(client=client, field=[UserMeFieldEnum.UUID])
            return True
        except Exception as exc:
            logger.debug("Readiness check failed for %s, details: %s", offering.api_url, exc)
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
        "--readiness-timeout",
        type=float,
        default=DEFAULT_READINESS_TIMEOUT,
        help="HTTP timeout in seconds for the readiness call to Waldur",
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

    if not args.liveness_only and not check_readiness(
        args.config_file, timeout=args.readiness_timeout
    ):
        logger.warning("Cannot reach Waldur API")
        return 1

    return 0


def cli() -> None:
    """Wrapper for console_scripts entry point."""
    sys.exit(main())
