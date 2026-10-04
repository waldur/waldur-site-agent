"""Lightweight health check for Kubernetes probes.

Liveness: checks that the agent's main loop touched the heartbeat file
recently (within ``max_age`` seconds).

Readiness: verifies connectivity to Waldur A with an authenticated
``GET /api/users/me/?field=uuid`` call.

The kubelet runs this as a fresh process on every probe tick, so import cost
is paid on every probe. ``waldur_api_client`` (~3000 generated model modules)
and ``common.utils`` (which loads every installed backend plugin) together
took ~12 s on a CPU-capped pod whose read-only root filesystem forces Python
to recompile the sources each time -- well past ``timeoutSeconds``. Neither
probe touches them: liveness needs one ``stat()``, and readiness reads the
offerings straight from the YAML and makes a single plain ``httpx`` request.
Nothing outside the standard library is imported at module level.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import httpx

HEARTBEAT_PATH = "/tmp/waldur-site-agent-heartbeat"  # noqa: S108
# Agents sharing a host (several systemd units, or a test run next to a live
# agent) must not share one heartbeat file: any of them touching it keeps a
# stalled one looking alive. Set a distinct path per process.
HEARTBEAT_PATH_ENV = "WALDUR_SITE_AGENT_HEARTBEAT_PATH"
DEFAULT_MAX_AGE = 300  # seconds
# Kept below the probe's own timeoutSeconds: the API client otherwise defaults
# to a 600s timeout, so a stalled Waldur blocks the probe process until the
# kubelet kills it, which reads as a failure with no diagnostics.
DEFAULT_READINESS_TIMEOUT = 5  # seconds

logger = logging.getLogger(__name__)


def heartbeat_path() -> str:
    """The heartbeat file: ``$WALDUR_SITE_AGENT_HEARTBEAT_PATH`` or the default."""
    return os.environ.get(HEARTBEAT_PATH_ENV) or HEARTBEAT_PATH


def touch_heartbeat(path: Optional[str] = None) -> None:
    """Update the heartbeat file mtime. Called from main loops."""
    Path(path or heartbeat_path()).write_text(str(time.time()))


def check_liveness(max_age: int = DEFAULT_MAX_AGE, path: Optional[str] = None) -> bool:
    """Return True if heartbeat file exists and was updated within *max_age* seconds."""
    try:
        mtime = Path(path or heartbeat_path()).stat().st_mtime
        return (time.time() - mtime) < max_age
    except FileNotFoundError:
        return False


def _offering_auth_header(offering: dict, client: httpx.Client) -> str:
    """Return the Authorization header value for *offering*, fetching a JWT if needed."""
    token = offering.get("waldur_api_token")
    if token:
        return f"Token {token}"
    response = client.post(
        offering["oidc_token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": offering["oidc_client_id"],
            "client_secret": offering["oidc_client_secret"],
        },
    )
    response.raise_for_status()
    return f"Bearer {response.json()['access_token']}"


def check_readiness(config_file: str, timeout: float = DEFAULT_READINESS_TIMEOUT) -> bool:
    """Return True if Waldur A responds to GET /api/users/me/?field=uuid."""
    # Deliberately not at module level, and deliberately not the agent's own
    # config loader or API client: see the module docstring.
    import httpx  # noqa: PLC0415
    import yaml  # noqa: PLC0415

    try:
        with Path(config_file).open(encoding="UTF-8") as stream:
            config = yaml.safe_load(stream)
        offerings = config["offerings"]
        proxy = config.get("global_proxy") or None
    except Exception as exc:
        logger.debug("Cannot read config %s, details: %s", config_file, exc)
        return False

    for offering in offerings:
        api_url = offering.get("waldur_api_url", "")
        try:
            base_url = api_url.rstrip("/").removesuffix("/api")
            with httpx.Client(
                verify=offering.get("verify_ssl", True), proxy=proxy, timeout=timeout
            ) as client:
                response = client.get(
                    f"{base_url}/api/users/me/",
                    params={"field": "uuid"},
                    headers={
                        "Authorization": _offering_auth_header(offering, client),
                        "User-Agent": "waldur-site-agent-healthz",
                    },
                )
            response.raise_for_status()
            return True
        except Exception as exc:
            logger.debug("Readiness check failed for %s, details: %s", api_url, exc)
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
        "--heartbeat-path",
        default=None,
        help=f"Heartbeat file to check (default: ${HEARTBEAT_PATH_ENV} or {HEARTBEAT_PATH})",
    )
    parser.add_argument(
        "--liveness-only",
        action="store_true",
        help="Only check liveness (heartbeat), skip readiness",
    )
    args = parser.parse_args()

    if not check_liveness(max_age=args.max_age, path=args.heartbeat_path):
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
