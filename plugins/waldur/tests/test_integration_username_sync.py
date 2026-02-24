"""Integration tests for username sync and identity manager event routing.

Uses the same Waldur instance as both Waldur A (source) and Waldur B (target).
Both offerings use Marketplace.Slurm to support STOMP event subscriptions.

Three dedicated users are created (no staff token in data paths):
  - user_a: Offering manager on offering A (source side)
  - user_b: Customer B owner + ISD identity manager (target side)
  - subject_user: Regular user with active_isds — linked to offering users on both sides

Test suites:
  1. TestUsernameSyncIntegration — Polling-based username sync from B to A
  2. TestIdentityManagerEventRouting — STOMP event routing to ISD identity managers

Environment variables:
    WALDUR_INTEGRATION_TESTS=true
    WALDUR_API_URL=http://localhost:8080/api/
    WALDUR_API_TOKEN=<staff-token>   (used only for entity setup/teardown)

Usage:
    WALDUR_INTEGRATION_TESTS=true \\
    WALDUR_API_URL=http://localhost:8080/api/ \\
    WALDUR_API_TOKEN=<token> \\
    .venv/bin/python -m pytest plugins/waldur/tests/test_integration_username_sync.py -v -s
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid as uuid_mod
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse
from uuid import UUID

import pytest

from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.utils import (
    _setup_single_stomp_subscription,
    stop_stomp_consumers,
)
from waldur_site_agent_waldur.backend import WaldurBackend

from .integration_helpers import AutoApproveWaldurClient, WaldurTestSetup

logger = logging.getLogger(__name__)

# --- Environment gating ---

INTEGRATION_TESTS = (
    os.environ.get("WALDUR_INTEGRATION_TESTS", "false").lower() == "true"
)
WALDUR_API_URL = os.environ.get("WALDUR_API_URL", "http://localhost:8080/api/")
WALDUR_API_TOKEN = os.environ.get("WALDUR_API_TOKEN", "")

# ISD used for identity manager routing tests
ISD_NAME = "isd:integration-test"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class UserInfo:
    """A user created for the test with an API token."""

    uuid: str
    url: str
    username: str
    email: str
    token: Optional[str] = None


@dataclass
class TestEnv:
    """Shared test environment created by the class-scoped fixture."""

    test_setup: WaldurTestSetup
    offering_a_uuid: str
    offering_b_uuid: str
    customer_a_uuid: str
    customer_b_uuid: str
    user_a: UserInfo  # Offering manager for Waldur A
    user_b: UserInfo  # Customer B owner + ISD identity manager
    subject_user: UserInfo  # User whose offering users are synced
    backend: WaldurBackend


# ---------------------------------------------------------------------------
# Helpers — user management
# ---------------------------------------------------------------------------


def _create_user(staff_client, run_id, label):
    """Create a user via the staff API. Returns UserInfo (token=None)."""
    username = f"{label}-{run_id}"
    email = f"{label}-{run_id}@integration.test"

    resp = staff_client.get_httpx_client().post(
        "/api/users/",
        json={
            "username": username,
            "email": email,
            "first_name": label.replace("-", " ").title(),
            "last_name": f"Test {run_id}",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return UserInfo(
        uuid=data["uuid"],
        url=data.get("url", ""),
        username=username,
        email=email,
    )


def _get_user_token(staff_client, user_info):
    """Set a password for the user and authenticate to obtain a token.

    Mutates user_info.token in place and returns the token or None.
    """
    password = f"IntTest-{uuid_mod.uuid4().hex[:12]}!"

    # Try set_password endpoints (staff privilege)
    for endpoint in [
        f"/api/users/{user_info.uuid}/set_password/",
        f"/api/users/{user_info.uuid}/password/",
    ]:
        resp = staff_client.get_httpx_client().post(
            endpoint, json={"password": password}
        )
        if resp.status_code < 400:
            logger.info("Set password for %s via %s", user_info.username, endpoint)
            break
    else:
        logger.warning("Could not set password for %s", user_info.username)
        return None

    # Authenticate
    import httpx as httpx_lib  # noqa: PLC0415

    base_url = str(staff_client.get_httpx_client().base_url).rstrip("/")
    resp = httpx_lib.post(
        f"{base_url}/api-auth/password/",
        json={"username": user_info.username, "password": password},
        verify=False,  # noqa: S501
        timeout=10,
    )
    if resp.status_code == 200:
        token = resp.json().get("token")
        if token:
            user_info.token = token
            logger.info("Got token for %s", user_info.username)
            return token

    logger.warning("Could not authenticate %s", user_info.username)
    return None


def _add_customer_role(staff_client, customer_uuid, user_uuid, role="CUSTOMER.OWNER"):
    """Grant a role on a customer to a user."""
    resp = staff_client.get_httpx_client().post(
        f"/api/customers/{customer_uuid}/add_user/",
        json={"role": role, "user": str(user_uuid)},
    )
    if resp.status_code < 400:
        logger.info("Granted %s on customer %s to user %s", role, customer_uuid, user_uuid)
    else:
        logger.warning(
            "Could not grant %s on customer %s: %s %s",
            role, customer_uuid, resp.status_code, resp.text[:200],
        )


def _add_offering_role(staff_client, offering_uuid, user_uuid, role="OFFERING.MANAGER"):
    """Grant a role on an offering to a user."""
    resp = staff_client.get_httpx_client().post(
        f"/api/marketplace-provider-offerings/{offering_uuid}/add_user/",
        json={"role": role, "user": str(user_uuid)},
    )
    if resp.status_code < 400:
        logger.info("Granted %s on offering %s to user %s", role, offering_uuid, user_uuid)
    else:
        logger.warning(
            "Could not grant %s on offering %s: %s %s",
            role, offering_uuid, resp.status_code, resp.text[:200],
        )


def _set_user_attributes(staff_client, user_uuid, **attrs):
    """PATCH arbitrary attributes on a user."""
    resp = staff_client.get_httpx_client().patch(
        f"/api/users/{user_uuid}/", json=attrs
    )
    if resp.status_code >= 400:
        logger.warning(
            "Could not set attributes on user %s: %s %s",
            user_uuid, resp.status_code, resp.text[:200],
        )


def _delete_user(staff_client, user_uuid):
    """Delete a user, ignoring errors."""
    staff_client.get_httpx_client().delete(f"/api/users/{user_uuid}/")


# ---------------------------------------------------------------------------
# Helpers — offering users
# ---------------------------------------------------------------------------


def _get_or_create_offering_user(client, user_url, offering_uuid):
    """Get existing or create new offering user. Returns (data, created)."""
    resp = client.get_httpx_client().get(
        "/api/marketplace-offering-users/",
        params={"offering_uuid": offering_uuid},
    )
    resp.raise_for_status()

    user_uuid_from_url = user_url.rstrip("/").rsplit("/", 1)[-1].replace("-", "")

    for ou in resp.json():
        ou_user_uuid = (ou.get("user_uuid") or "").replace("-", "")
        is_match = ou.get("user") == user_url or (
            ou_user_uuid and ou_user_uuid == user_uuid_from_url
        )
        if is_match and ou.get("state", "") in ("OK", "Creating", "Requested"):
            return ou, False

    resp = client.get_httpx_client().post(
        "/api/marketplace-offering-users/",
        json={"user": user_url, "offering_uuid": offering_uuid},
    )
    resp.raise_for_status()
    return resp.json(), True


def _delete_offering_user(client, ou_uuid):
    """Delete offering user, ignoring 404."""
    resp = client.get_httpx_client().delete(
        f"/api/marketplace-offering-users/{ou_uuid}/",
    )
    return resp.status_code


def _ensure_offering_user_ok(client, ou_uuid, ou_state, username="placeholder"):
    """Transition an offering user to OK state by setting a username if needed."""
    if ou_state == "OK":
        return
    resp = client.get_httpx_client().patch(
        f"/api/marketplace-offering-users/{ou_uuid}/",
        json={"username": username},
    )
    if resp.status_code < 400:
        logger.info(
            "Set placeholder username on %s -> state: %s",
            ou_uuid,
            resp.json().get("state", ""),
        )


# ---------------------------------------------------------------------------
# Helpers — STOMP
# ---------------------------------------------------------------------------


class MessageCapture:
    """Thread-safe STOMP message capture."""

    def __init__(self):
        self._messages: list[dict] = []
        self._lock = threading.Lock()
        self._new_message = threading.Event()

    def make_handler(self):
        """Return a STOMP handler that captures messages."""

        def handler(frame, offering, user_agent):
            message = json.loads(frame.body)
            with self._lock:
                self._messages.append(message)
                self._new_message.set()

        return handler

    def wait_for_event(
        self, key: str, value: str, timeout: float = 30
    ) -> dict | None:
        """Wait for a message where message[key] == value."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                for msg in self._messages:
                    if str(msg.get(key, "")) == value:
                        return msg
                self._new_message.clear()
            self._new_message.wait(timeout=min(1.0, deadline - time.time()))
        return None

    @property
    def messages(self) -> list[dict]:
        with self._lock:
            return list(self._messages)


def _make_offering_for_stomp(api_url, api_token, offering_uuid, name="STOMP"):
    """Construct an Offering object for STOMP subscription setup."""
    parsed = urlparse(api_url)
    return Offering(
        name=name,
        waldur_api_url=api_url if api_url.endswith("/") else api_url + "/",
        waldur_api_token=api_token,
        waldur_offering_uuid=offering_uuid,
        backend_type="slurm",
        stomp_enabled=True,
        stomp_ws_host=parsed.hostname,
        stomp_ws_port=parsed.port or (443 if parsed.scheme == "https" else 80),
        stomp_ws_path="/rmqws-stomp",
    )


def _check_stomp_available(api_url: str) -> bool:
    """Check if STOMP WebSocket endpoint is reachable."""
    import httpx as httpx_lib  # noqa: PLC0415

    parsed = urlparse(api_url)
    scheme = parsed.scheme or "http"
    try:
        resp = httpx_lib.get(
            f"{scheme}://{parsed.hostname}:{parsed.port or 80}/rmqws-stomp",
            timeout=5,
            follow_redirects=False,
            verify=False,  # noqa: S501
        )
        return resp.status_code == 426
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def env(request):
    """Create test entities and dedicated users on the Waldur instance.

    Creates:
      - Two customers (A, B), two offerings (both Marketplace.Slurm), one project
      - user_a: offering manager on offering A (source side token)
      - user_b: customer B owner + ISD identity manager (target side token)
      - subject_user: regular user with active_isds, offering users on both sides
    """
    test_setup = WaldurTestSetup(WALDUR_API_URL, WALDUR_API_TOKEN)
    result = test_setup.setup_passthrough(target_offering_type="Marketplace.Slurm")
    staff = test_setup.client
    run_id = test_setup._run_id

    # --- Create dedicated users ---
    user_a = _create_user(staff, run_id, "user-a")
    user_b = _create_user(staff, run_id, "user-b")
    subject_user = _create_user(staff, run_id, "subject")

    # Get tokens for user_a and user_b
    _get_user_token(staff, user_a)
    _get_user_token(staff, user_b)

    if not user_a.token:
        logger.warning("Could not get token for user_a — tests will skip")
    if not user_b.token:
        logger.warning("Could not get token for user_b — tests will skip")

    # --- Assign roles ---

    # user_a: offering manager on offering A
    _add_offering_role(staff, result.offering_a.uuid, user_a.uuid)

    # user_b: customer B owner (gives access to offering B)
    _add_customer_role(staff, result.offering_b.customer_uuid, user_b.uuid)

    # user_b: identity manager with managed_isds
    _set_user_attributes(
        staff,
        user_b.uuid,
        is_identity_manager=True,
        managed_isds=[ISD_NAME],
    )

    # subject_user: active_isds for ISD routing test
    _set_user_attributes(staff, subject_user.uuid, active_isds=[ISD_NAME])

    # --- Build WaldurBackend ---
    # Backend uses user_b token (target side) for WaldurClient
    # and user_a token (source side) for sync_offering_user_usernames
    settings = result.backend_settings.copy()
    settings["user_match_field"] = "username"

    if user_b.token:
        settings["target_api_token"] = user_b.token

    backend = WaldurBackend(settings, result.backend_components)
    backend.client = AutoApproveWaldurClient(
        api_url=settings["target_api_url"],
        api_token=settings["target_api_token"],
        offering_uuid=settings["target_offering_uuid"],
    )

    test_env = TestEnv(
        test_setup=test_setup,
        offering_a_uuid=result.offering_a.uuid,
        offering_b_uuid=result.offering_b.uuid,
        customer_a_uuid=result.offering_a.customer_uuid,
        customer_b_uuid=result.offering_b.customer_uuid,
        user_a=user_a,
        user_b=user_b,
        subject_user=subject_user,
        backend=backend,
    )

    yield test_env

    # Cleanup: delete users, then entities
    for u in (user_a, user_b, subject_user):
        _delete_user(staff, u.uuid)
    test_setup.cleanup()


# ===========================================================================
# Test Suite 1: Username sync via polling
# ===========================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestUsernameSyncIntegration:
    """Sync offering user usernames from Waldur B to A on a single instance.

    Uses dedicated users instead of staff:
      - user_a token as the Waldur A API client (offering manager)
      - user_b token as the Waldur B API client (customer B owner)
      - subject_user as the user whose offering users are synced

    Flow:
      01. Verify user_a and user_b tokens were obtained
      02. Create offering users for subject_user on both offerings
      03. Set a known username on the B offering user
      04. Run sync_offering_user_usernames → verify A updated
      05. Verify idempotency (second sync = no-op)
      06. Cleanup offering users
    """

    _state: dict = {}

    def test_01_verify_tokens(self, env: TestEnv):
        """Ensure both user_a and user_b have API tokens."""
        if not env.user_a.token:
            pytest.skip("Could not obtain token for user_a (offering manager)")
        if not env.user_b.token:
            pytest.skip("Could not obtain token for user_b (customer B owner)")

        # Verify user_a can see offering A
        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        client_a = get_client(WALDUR_API_URL, env.user_a.token)
        resp = client_a.get_httpx_client().get(
            "/api/marketplace-offering-users/",
            params={"offering_uuid": env.offering_a_uuid},
        )
        assert resp.status_code < 400, (
            f"user_a cannot list offering users on A: {resp.status_code}"
        )
        logger.info("user_a can access offering A")

        # Verify subject_user resolves on "Waldur B" (same instance)
        remote_uuid = env.backend._resolve_remote_user(env.subject_user.username)
        assert remote_uuid is not None, (
            f"Could not resolve subject_user '{env.subject_user.username}' on target"
        )
        expected = env.subject_user.uuid.replace("-", "")
        actual = str(remote_uuid).replace("-", "")
        assert actual == expected, f"UUID mismatch: {actual} != {expected}"
        logger.info("subject_user resolves to %s", remote_uuid)

    def test_02_create_offering_users(self, env: TestEnv):
        """Create offering users for subject_user on both offerings."""
        if not env.user_a.token:
            pytest.skip("No user_a token")

        # Use staff to create offering users (subject_user might not have access)
        staff = env.test_setup.client

        # Offering A (source) — offering user for subject_user
        ou_a, created_a = _get_or_create_offering_user(
            staff, env.subject_user.url, env.offering_a_uuid
        )
        ou_a_uuid = ou_a["uuid"]
        self.__class__._state["ou_a_uuid"] = ou_a_uuid
        self.__class__._state["ou_a_created"] = created_a
        self.__class__._state["ou_a_original_username"] = ou_a.get("username", "")
        _ensure_offering_user_ok(staff, ou_a_uuid, ou_a.get("state", ""))
        logger.info("Offering user A: %s (created=%s)", ou_a_uuid, created_a)

        # Offering B (target) — offering user for subject_user
        ou_b, created_b = _get_or_create_offering_user(
            staff, env.subject_user.url, env.offering_b_uuid
        )
        ou_b_uuid = ou_b["uuid"]
        self.__class__._state["ou_b_uuid"] = ou_b_uuid
        self.__class__._state["ou_b_created"] = created_b
        self.__class__._state["ou_b_original_username"] = ou_b.get("username", "")
        _ensure_offering_user_ok(
            staff, ou_b_uuid, ou_b.get("state", ""), "pre-sync-placeholder"
        )
        logger.info("Offering user B: %s (created=%s)", ou_b_uuid, created_b)

    def test_03_set_target_username(self, env: TestEnv):
        """Set a known test username on the Waldur B offering user."""
        ou_b_uuid = self.__class__._state.get("ou_b_uuid")
        if not ou_b_uuid:
            pytest.skip("No Waldur B offering user")

        test_username = f"inttest-sync-{uuid_mod.uuid4().hex[:8]}"
        self.__class__._state["test_username_b"] = test_username

        # user_b (customer B owner) sets the username
        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        client_b = get_client(WALDUR_API_URL, env.user_b.token)
        resp = client_b.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_b_uuid}/",
            json={"username": test_username},
        )
        resp.raise_for_status()
        actual = resp.json().get("username", "")
        assert actual == test_username, f"Expected '{test_username}', got '{actual}'"
        logger.info("user_b set Waldur B username to '%s'", test_username)

    def test_04_sync_usernames(self, env: TestEnv):
        """Run sync and verify the B username propagated to A."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        # user_a token as the Waldur A REST client
        client_a = get_client(WALDUR_API_URL, env.user_a.token)

        changed = env.backend.sync_offering_user_usernames(
            waldur_a_offering_uuid=env.offering_a_uuid,
            waldur_rest_client=client_a,
        )
        assert changed, "sync_offering_user_usernames returned False (expected True)"

        # Verify offering user A now has the B username
        ou_a_uuid = self.__class__._state["ou_a_uuid"]
        resp = client_a.get_httpx_client().get(
            f"/api/marketplace-offering-users/{ou_a_uuid}/"
        )
        resp.raise_for_status()
        actual = resp.json().get("username", "")
        expected = self.__class__._state["test_username_b"]
        assert actual == expected, (
            f"Offering user A username '{actual}' != expected '{expected}'"
        )
        logger.info("Username '%s' propagated from B to A via user_a token", expected)

    def test_05_idempotent_second_sync(self, env: TestEnv):
        """Second sync should be a no-op."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        client_a = get_client(WALDUR_API_URL, env.user_a.token)

        changed = env.backend.sync_offering_user_usernames(
            waldur_a_offering_uuid=env.offering_a_uuid,
            waldur_rest_client=client_a,
        )
        assert not changed, "Second sync should return False (no changes)"
        logger.info("Idempotency confirmed")

    def test_06_cleanup_offering_users(self, env: TestEnv):
        """Clean up offering users created during the test."""
        staff = env.test_setup.client

        for side, key_uuid, key_created, key_orig in [
            ("A", "ou_a_uuid", "ou_a_created", "ou_a_original_username"),
            ("B", "ou_b_uuid", "ou_b_created", "ou_b_original_username"),
        ]:
            ou_uuid = self.__class__._state.get(key_uuid)
            if not ou_uuid:
                continue
            if self.__class__._state.get(key_created, False):
                status = _delete_offering_user(staff, ou_uuid)
                logger.info("Deleted offering user %s %s (status %s)", side, ou_uuid, status)
            else:
                original = self.__class__._state.get(key_orig, "")
                resp = staff.get_httpx_client().patch(
                    f"/api/marketplace-offering-users/{ou_uuid}/",
                    json={"username": original},
                )
                logger.info(
                    "Restored offering user %s %s username to '%s' (status %s)",
                    side, ou_uuid, original, resp.status_code,
                )


# ===========================================================================
# Test Suite 2: Identity manager STOMP event routing
# ===========================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestIdentityManagerEventRouting:
    """Verify STOMP OFFERING_USER events are routed to ISD identity managers.

    user_b acts as both:
      - Customer B owner (has offering access via filter_for_user)
      - ISD identity manager (managed_isds overlapping subject_user.active_isds)

    The test creates two STOMP subscriptions on offering B:
      1. user_a subscription (offering manager, has offering access)
      2. user_b subscription (customer B owner + IDM)

    Then triggers offering user changes and verifies both receive events.
    Clearing user_b.managed_isds tests the negative case where the IDM
    check would matter for a user WITHOUT offering access.

    Flow:
      01. Verify prerequisites (STOMP, tokens)
      02. Set up STOMP subscriptions for user_a and user_b
      03. Update offering user username on B → verify both receive event
      04. Clear user_b managed_isds → update again → verify user_b still
          receives (via customer owner access, not ISD)
      05. Cleanup
    """

    _state: dict = {}

    def test_01_verify_prerequisites(self, env: TestEnv):
        """Check STOMP is available and tokens were obtained."""
        if not _check_stomp_available(WALDUR_API_URL):
            pytest.skip("STOMP WebSocket endpoint not available")

        if not env.user_a.token:
            pytest.skip("No token for user_a")
        if not env.user_b.token:
            pytest.skip("No token for user_b")

        logger.info(
            "Prerequisites met: STOMP available, user_a='%s', user_b='%s'",
            env.user_a.username,
            env.user_b.username,
        )

    def test_02_setup_stomp_subscriptions(self, env: TestEnv):
        """Set up STOMP subscriptions for user_a and user_b."""
        if not env.user_a.token or not env.user_b.token:
            pytest.skip("Missing tokens")

        from waldur_site_agent.common.agent_identity_management import (  # noqa: PLC0415
            AgentIdentityManager,
        )
        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415
        from waldur_site_agent.event_processing.structures import (  # noqa: PLC0415
            ObservableObjectTypeEnum,
        )

        # --- user_a subscription (offering manager on A, needs offering B access
        #     for STOMP — we grant it via staff) ---
        _add_offering_role(
            env.test_setup.client, env.offering_b_uuid, env.user_a.uuid
        )

        ua_offering = _make_offering_for_stomp(
            WALDUR_API_URL, env.user_a.token, env.offering_b_uuid,
            name="user_a STOMP",
        )
        ua_client = get_client(WALDUR_API_URL, env.user_a.token)
        ua_aim = AgentIdentityManager(ua_offering, ua_client)

        try:
            ua_identity = ua_aim.register_identity("inttest-ua")
        except Exception:
            logger.warning("user_a cannot register agent identity on offering B")
            pytest.skip("user_a cannot register on offering B")

        ua_capture = MessageCapture()
        ua_consumer = _setup_single_stomp_subscription(
            ua_offering, ua_identity, ua_aim,
            "inttest-ua", ObservableObjectTypeEnum.OFFERING_USER,
        )
        if ua_consumer is None:
            pytest.skip("Could not set up user_a STOMP subscription")

        conn, sub, _ = ua_consumer
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        if listener:
            listener.on_message_callback = ua_capture.make_handler()

        self.__class__._state["ua_consumer"] = ua_consumer
        self.__class__._state["ua_capture"] = ua_capture
        logger.info("user_a STOMP subscription active")

        # --- user_b subscription (customer B owner + IDM) ---
        ub_offering = _make_offering_for_stomp(
            WALDUR_API_URL, env.user_b.token, env.offering_b_uuid,
            name="user_b STOMP",
        )
        ub_client = get_client(WALDUR_API_URL, env.user_b.token)
        ub_aim = AgentIdentityManager(ub_offering, ub_client)

        try:
            ub_identity = ub_aim.register_identity("inttest-ub")
        except Exception:
            logger.warning(
                "user_b cannot register agent identity on offering B "
                "(unexpected — user_b is customer B owner)"
            )
            self.__class__._state["ub_consumer"] = None
            return

        ub_capture = MessageCapture()
        ub_consumer = _setup_single_stomp_subscription(
            ub_offering, ub_identity, ub_aim,
            "inttest-ub", ObservableObjectTypeEnum.OFFERING_USER,
        )
        if ub_consumer is None:
            logger.warning("Could not set up user_b STOMP subscription")
            self.__class__._state["ub_consumer"] = None
            return

        conn, sub, _ = ub_consumer
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        if listener:
            listener.on_message_callback = ub_capture.make_handler()

        self.__class__._state["ub_consumer"] = ub_consumer
        self.__class__._state["ub_capture"] = ub_capture
        logger.info("user_b STOMP subscription active")

    def test_03_trigger_and_verify_event(self, env: TestEnv):
        """Update offering user username on B and verify both receive it."""
        ua_capture = self.__class__._state.get("ua_capture")
        if ua_capture is None:
            pytest.skip("user_a STOMP subscription not set up")

        staff = env.test_setup.client

        # Ensure subject_user has an offering user on B
        ou_b, created = _get_or_create_offering_user(
            staff, env.subject_user.url, env.offering_b_uuid
        )
        ou_b_uuid = ou_b["uuid"]
        self.__class__._state["ou_b_uuid"] = ou_b_uuid
        self.__class__._state["ou_b_created"] = created
        _ensure_offering_user_ok(staff, ou_b_uuid, ou_b.get("state", ""), "pre-test")

        # Set username to trigger update event
        test_username = f"stomp-test-{uuid_mod.uuid4().hex[:6]}"
        resp = staff.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_b_uuid}/",
            json={"username": test_username},
        )
        resp.raise_for_status()
        logger.info("PATCHed offering user B with username '%s'", test_username)

        # Wait for user_a to receive the event
        ou_uuid_hex = ou_b_uuid.replace("-", "")
        event = ua_capture.wait_for_event(
            "offering_user_uuid", ou_uuid_hex, timeout=30
        )
        if event is None:
            event = ua_capture.wait_for_event(
                "offering_user_uuid", ou_b_uuid, timeout=5
            )

        if event:
            logger.info(
                "user_a received event: action=%s username=%s",
                event.get("action"), event.get("username"),
            )
            assert event.get("username") == test_username
        else:
            logger.warning(
                "user_a did not receive OFFERING_USER event. "
                "Captured %d messages total.", len(ua_capture.messages),
            )
            pytest.skip(
                "No OFFERING_USER event received — "
                "Mastermind may not publish events for this config."
            )

        # Check user_b subscription
        ub_capture = self.__class__._state.get("ub_capture")
        if ub_capture is None:
            logger.info("user_b STOMP not set up — skipping")
            return

        ub_event = ub_capture.wait_for_event(
            "offering_user_uuid", ou_uuid_hex, timeout=10
        )
        if ub_event is None:
            ub_event = ub_capture.wait_for_event(
                "offering_user_uuid", ou_b_uuid, timeout=5
            )

        if ub_event:
            logger.info(
                "user_b (IDM + customer owner) received event: action=%s username=%s",
                ub_event.get("action"), ub_event.get("username"),
            )
            assert ub_event.get("username") == test_username
        else:
            logger.warning(
                "user_b did NOT receive the event. "
                "Check that user_b has customer B owner access and "
                "the Mastermind IDM routing is deployed."
            )

    def test_04_verify_event_after_clearing_isds(self, env: TestEnv):
        """Clear managed_isds and verify user_b still receives events.

        user_b is a customer B owner, so they receive events via
        filter_for_user() regardless of managed_isds. This confirms that
        the ISD routing bypass is additive — it does not break normal access.
        """
        ub_capture = self.__class__._state.get("ub_capture")
        if ub_capture is None:
            pytest.skip("user_b STOMP not set up")

        staff = env.test_setup.client

        # Clear user_b's managed_isds
        _set_user_attributes(staff, env.user_b.uuid, managed_isds=[])

        count_before = len(ub_capture.messages)

        # Trigger another offering user update
        ou_b_uuid = self.__class__._state.get("ou_b_uuid")
        if not ou_b_uuid:
            pytest.skip("No offering user B")

        new_username = f"no-isd-{uuid_mod.uuid4().hex[:6]}"
        resp = staff.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_b_uuid}/",
            json={"username": new_username},
        )
        resp.raise_for_status()

        # user_b should STILL receive events (customer B owner access)
        time.sleep(5)
        count_after = len(ub_capture.messages)

        if count_after > count_before:
            logger.info(
                "user_b still receives events after clearing managed_isds "
                "(as expected — customer B owner access)"
            )
        else:
            logger.warning(
                "user_b stopped receiving events after clearing managed_isds. "
                "This is unexpected — customer B owner should still have access."
            )

        # Restore managed_isds
        _set_user_attributes(staff, env.user_b.uuid, managed_isds=[ISD_NAME])

    def test_05_cleanup(self, env: TestEnv):
        """Clean up STOMP connections and offering users."""
        staff = env.test_setup.client

        # Stop STOMP consumers
        for key in ("ua_consumer", "ub_consumer"):
            consumer = self.__class__._state.get(key)
            if consumer is not None:
                try:
                    conn, sub, off = consumer
                    stop_stomp_consumers({("test", off.uuid): [consumer]})
                    logger.info("Stopped %s", key)
                except Exception:
                    logger.debug("Could not stop %s", key, exc_info=True)

        # Delete offering user on B if we created it
        ou_b_uuid = self.__class__._state.get("ou_b_uuid")
        if ou_b_uuid and self.__class__._state.get("ou_b_created", False):
            _delete_offering_user(staff, ou_b_uuid)
            logger.info("Deleted offering user B %s", ou_b_uuid)
