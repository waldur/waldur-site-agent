"""Privileged Waldur B accounts that the Identity Bridge refuses to push.

Since Waldur Mastermind 8.1.4 (waldur/waldur-mastermind#1060), the Identity
Bridge returns 403 when a non-staff caller pushes or removes a staff, support
or identity-manager account. The agent normally runs as an identity manager,
so it cannot sync those users. The refusal is intended, and nothing on the
agent side can fix it.

This module recognises that refusal and remembers it for about one sync
cycle. The profile push, membership sync and order processing all send users
to the bridge, each through its own short-lived backend instance. With a
shared record they send such a user at most once per cycle and log one
WARNING instead of an ERROR with a traceback on every call.
"""

from __future__ import annotations

import logging
import threading
import time

from waldur_site_agent.common import WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES

logger = logging.getLogger(__name__)

PRIVILEGED_ACCOUNT_STATUS = 403
# Part of the message waldur_auth_social.views.PRIVILEGED_ACCOUNT_MESSAGE
# returns: "Only staff may push or remove staff, support or identity manager
# accounts."
PRIVILEGED_ACCOUNT_MARKER = "staff, support or identity manager accounts"

# Keep a refusal until shortly before the next membership sync pass: one pass
# sends a privileged user to the bridge at most once, and the next pass asks
# again in case the account has lost its privileges.
PRIVILEGED_ACCOUNT_TTL_SECONDS = max(60, WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60 - 60)

PRIVILEGED_PUSH_WARNING = (
    "User %s is a staff, support or identity manager account on Waldur B (%s). "
    "Only a staff caller may push such accounts through the Identity Bridge, so "
    "the agent cannot resolve this user and does not sync their project roles. "
    "Manage their roles on Waldur B, or run the agent with a staff token "
    "(see the Waldur plugin README)."
)
PRIVILEGED_REMOVE_WARNING = (
    "User %s is a staff, support or identity manager account on Waldur B (%s). "
    "Only a staff caller may remove such accounts from an ISD through the "
    "Identity Bridge, so the agent leaves this user unchanged."
)


class IdentityBridgePrivilegedAccountError(Exception):
    """The Identity Bridge refused a privileged account (staff, support, IdM)."""

    def __init__(self, username: str) -> None:
        """Keep the refused username."""
        super().__init__(f"Identity Bridge refused privileged account {username}")
        self.username = username


def is_privileged_account_refusal(
    status_code: int, body: str | bytes | None
) -> bool:
    """Whether an Identity Bridge response is the privileged-account 403."""
    if status_code != PRIVILEGED_ACCOUNT_STATUS or not body:
        return False
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    return PRIVILEGED_ACCOUNT_MARKER in body


def _target_key(target_url: str) -> str:
    """Normalise a Waldur B URL so ``.../api/`` and its base URL match."""
    url = str(target_url).rstrip("/")
    if url.endswith("/api"):
        url = url[: -len("/api")]
    return url


class PrivilegedAccountRegistry:
    """Thread-safe record of refused accounts, kept for a TTL.

    Keyed by Waldur B base URL and the username sent to the bridge.
    """

    def __init__(self, ttl_seconds: float = PRIVILEGED_ACCOUNT_TTL_SECONDS) -> None:
        """Create an empty record."""
        self.ttl_seconds = ttl_seconds
        self._expiry: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()

    def is_known(self, target_url: str, username: str) -> bool:
        """Whether this account was refused within the TTL."""
        key = (_target_key(target_url), username)
        with self._lock:
            expiry = self._expiry.get(key)
            if expiry is None:
                return False
            if expiry <= time.monotonic():
                del self._expiry[key]
                return False
            return True

    def record(self, target_url: str, username: str) -> bool:
        """Record a refusal. Returns True unless it was already on record."""
        key = (_target_key(target_url), username)
        now = time.monotonic()
        with self._lock:
            expiry = self._expiry.get(key)
            known = expiry is not None and expiry > now
            if not known:
                self._expiry[key] = now + self.ttl_seconds
            return not known

    def clear(self) -> None:
        """Forget every recorded refusal."""
        with self._lock:
            self._expiry.clear()


privileged_accounts = PrivilegedAccountRegistry()


def note_privileged_account(target_url: str, username: str, *, removal: bool = False) -> None:
    """Record a refusal and log one WARNING the first time it is seen."""
    if privileged_accounts.record(target_url, username):
        message = PRIVILEGED_REMOVE_WARNING if removal else PRIVILEGED_PUSH_WARNING
        logger.warning(message, username, _target_key(target_url))
    else:
        logger.debug("Privileged Waldur B account %s already reported", username)
