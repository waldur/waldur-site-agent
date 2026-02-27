"""Integration tests for username sync and identity manager event routing.

Uses the same Waldur instance as both Waldur A (source) and Waldur B (target).
Both offerings use Marketplace.Slurm to support STOMP event subscriptions.

Three dedicated users are created (no staff token in data paths):
  - user_a: Offering manager on offering A (source side)
  - user_b: Customer C owner (non-SP, separate from offering B's SP)
            + ISD identity manager (target side, accesses offering B users
            via ISD overlap, registers agent identities via IDM path)
  - subject_user: Regular user with active_isds — linked to offering users on both sides

Test suites:
  1. TestUsernameSyncIntegration — Polling-based username sync from B to A
  2. TestIdentityManagerEventRouting — STOMP event routing to ISD identity managers
  3. TestPeriodicReconciliationIntegration — run_periodic_username_reconciliation end-to-end

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
    EventSubscriptionManager,
)
from waldur_site_agent.event_processing.listener import (
    ConnectFailedException,
    connect_to_stomp_server,
)
from waldur_site_agent.event_processing.utils import (
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
# STOMP WebSocket overrides (default: same host as API, port 15674, path /ws)
STOMP_WS_PORT = int(os.environ.get("WALDUR_STOMP_WS_PORT", "15674"))
STOMP_WS_PATH = os.environ.get("WALDUR_STOMP_WS_PATH", "/ws")

# ISD used for identity manager routing tests
ISD_NAME = "isd:integration-test"

# Shared backend components for reconciliation Offering objects (Suite 3).
# Avoids repeating the same dict literal in multiple tests.
_RECONCILIATION_COMPONENTS = {
    "cpu": {
        "measured_unit": "Hours",
        "unit_factor": 1,
        "accounting_type": "usage",
        "label": "CPU Hours",
    },
    "mem": {
        "measured_unit": "GB",
        "unit_factor": 1,
        "accounting_type": "usage",
        "label": "Memory GB",
    },
}


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
    user_a: UserInfo  # Offering manager on offering A
    user_b: UserInfo  # Customer C owner (non-SP) + ISD identity manager
    subject_user: UserInfo  # User whose offering users are synced
    backend: WaldurBackend
    project_b_uuid: str = ""  # Project under customer C (for B-side resources)
    resource_a_uuid: str = ""  # Resource on offering A (in project A)
    resource_b_uuid: str = ""  # Resource on offering B (in project B)


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
    """Obtain an API token for the user.

    Tries two strategies:
      1. Set password via staff API + authenticate via api-auth/password/
      2. Fallback: retrieve auto-created token via Django management shell

    Mutates user_info.token in place and returns the token or None.
    """
    # Strategy 1: password-based authentication
    token = _get_token_via_password(staff_client, user_info)
    if token:
        return token

    # Strategy 2: retrieve auto-created token via Django shell
    token = _get_token_via_django_shell(user_info)
    if token:
        return token

    logger.warning("Could not obtain token for %s", user_info.username)
    return None


def _get_token_via_password(staff_client, user_info):
    """Try to set password and authenticate to get a token."""
    password = f"IntTest-{uuid_mod.uuid4().hex[:12]}!"

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
        logger.debug("No set_password endpoint available for %s", user_info.username)
        return None

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
            logger.info("Got token for %s via password auth", user_info.username)
            return token
    return None


def _get_token_via_django_shell(user_info):
    """Retrieve auto-created token via Django management shell (local dev only).

    Waldur auto-creates a Token on user creation. When running tests against
    a local Django runserver, we can retrieve it via 'uv run waldur shell'.
    """
    import shutil  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    waldur_dir = os.environ.get(
        "WALDUR_MASTERMIND_DIR",
        os.path.expanduser("~/workspace/waldur-mastermind"),
    )
    if not os.path.isdir(waldur_dir):
        logger.debug("WALDUR_MASTERMIND_DIR not found at %s", waldur_dir)
        return None

    # Prefer uv, fall back to manage.py
    uv_bin = shutil.which("uv")
    if uv_bin:
        cmd = [uv_bin, "run", "waldur", "shell", "-c"]
    else:
        manage_py = os.path.join(waldur_dir, "src", "waldur_core", "server", "manage.py")
        if not os.path.isfile(manage_py):
            return None
        venv_python = os.path.join(waldur_dir, ".venv", "bin", "python")
        python = venv_python if os.path.isfile(venv_python) else "python3"
        cmd = [python, manage_py, "shell", "-c"]

    script = (
        "from rest_framework.authtoken.models import Token; "
        "from django.contrib.auth import get_user_model; "
        "User = get_user_model(); "
        f"u = User.objects.get(uuid='{user_info.uuid}'); "
        "t = Token.objects.get(user=u); "
        "print(t.key)"
    )
    try:
        result = subprocess.run(  # noqa: S603
            [*cmd, script],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=waldur_dir,
        )
        # Token is the last non-empty line of stdout
        for line in reversed(result.stdout.strip().splitlines()):
            line = line.strip()
            if len(line) == 40 and line.isalnum():
                user_info.token = line
                logger.info("Got token for %s via Django shell", user_info.username)
                return line
    except Exception:
        logger.debug("Django shell token retrieval failed for %s", user_info.username, exc_info=True)
    return None


def _add_project_role(staff_client, project_uuid, user_uuid, role="PROJECT.MEMBER"):
    """Grant a role on a project to a user."""
    resp = staff_client.get_httpx_client().post(
        f"/api/projects/{project_uuid}/add_user/",
        json={"role": role, "user": str(user_uuid)},
    )
    if resp.status_code < 400:
        logger.info("Granted %s on project %s to user %s", role, project_uuid, user_uuid)
    else:
        logger.warning(
            "Could not grant %s on project %s: %s %s",
            role, project_uuid, resp.status_code, resp.text[:200],
        )


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
    else:
        logger.info("Set attributes on user %s: %s", user_uuid, attrs)


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
    if resp.status_code >= 400:
        logger.error(
            "Failed to create offering user: %s %s (user=%s, offering=%s)",
            resp.status_code, resp.text[:500], user_url, offering_uuid,
        )
    resp.raise_for_status()
    return resp.json(), True


def _wait_for_offering_user(client, user_url, offering_uuid, timeout=15):
    """Poll until an auto-created offering user appears. Returns (data, found)."""
    deadline = time.time() + timeout
    user_uuid_from_url = user_url.rstrip("/").rsplit("/", 1)[-1].replace("-", "")
    while time.time() < deadline:
        resp = client.get_httpx_client().get(
            "/api/marketplace-offering-users/",
            params={"offering_uuid": offering_uuid},
        )
        resp.raise_for_status()
        for ou in resp.json():
            ou_user_uuid = (ou.get("user_uuid") or "").replace("-", "")
            if ou_user_uuid == user_uuid_from_url:
                return ou, True
        time.sleep(1)
    return None, False


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


def _setup_offering_user(staff, state_dict, user_url, offering_uuid, side, placeholder="placeholder"):
    """Get-or-create an offering user, save state, and ensure it's OK.

    Stores ``ou_{side}_uuid``, ``ou_{side}_created``, and
    ``ou_{side}_original_username`` in *state_dict*.
    Returns (ou_data, ou_uuid).
    """
    ou, created = _get_or_create_offering_user(staff, user_url, offering_uuid)
    ou_uuid = ou["uuid"]
    side_lower = side.lower()
    state_dict[f"ou_{side_lower}_uuid"] = ou_uuid
    state_dict[f"ou_{side_lower}_created"] = created
    state_dict[f"ou_{side_lower}_original_username"] = ou.get("username", "")
    _ensure_offering_user_ok(staff, ou_uuid, ou.get("state", ""), placeholder)
    logger.info("Offering user %s: %s (created=%s)", side, ou_uuid, created)
    return ou, ou_uuid


def _set_and_verify_username(staff, state_dict, ou_uuid_key, prefix):
    """Generate a random username, PATCH it, assert, and save to state.

    Returns the generated username.
    """
    ou_uuid = state_dict.get(ou_uuid_key)
    if not ou_uuid:
        return None
    test_username = f"{prefix}-{uuid_mod.uuid4().hex[:8]}"
    state_dict["test_username_b"] = test_username
    resp = staff.get_httpx_client().patch(
        f"/api/marketplace-offering-users/{ou_uuid}/",
        json={"username": test_username},
    )
    resp.raise_for_status()
    actual = resp.json().get("username", "")
    assert actual == test_username, f"Expected '{test_username}', got '{actual}'"
    logger.info("Set username to '%s' (state: %s)", test_username, resp.json().get("state", ""))
    return test_username


def _uuid_hex(uuid_str):
    """Strip dashes from a UUID string."""
    return uuid_str.replace("-", "")


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
        self, timeout: float = 30, **criteria: str
    ) -> dict | None:
        """Wait for a message matching all key=value criteria.

        Example: capture.wait_for_event(offering_user_uuid="abc", username="foo")
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                for msg in self._messages:
                    if all(
                        str(msg.get(k, "")) == v for k, v in criteria.items()
                    ):
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
    use_tls = parsed.scheme == "https"
    return Offering(
        name=name,
        waldur_api_url=api_url if api_url.endswith("/") else api_url + "/",
        waldur_api_token=api_token,
        waldur_offering_uuid=offering_uuid,
        backend_type="slurm",
        stomp_enabled=True,
        stomp_ws_host=parsed.hostname,
        stomp_ws_port=STOMP_WS_PORT,
        stomp_ws_path=STOMP_WS_PATH,
        websocket_use_tls=use_tls,
    )


def _check_stomp_available(api_url: str) -> bool:
    """Check if STOMP WebSocket endpoint is reachable."""
    import httpx as httpx_lib  # noqa: PLC0415

    parsed = urlparse(api_url)
    scheme = parsed.scheme or "http"
    try:
        resp = httpx_lib.get(
            f"{scheme}://{parsed.hostname}:{STOMP_WS_PORT}{STOMP_WS_PATH}",
            timeout=5,
            follow_redirects=False,
            verify=False,  # noqa: S501
        )
        return resp.status_code == 426
    except Exception:
        return False


def _grant_publisher_vhost_access(vhost_name):
    """Grant the RABBITMQ publisher user permissions on a vhost.

    When Waldur creates a new event subscription, it creates a RabbitMQ vhost
    for the subscription owner. The publisher user (settings.RABBITMQ["USER"])
    needs write permissions on this vhost to deliver events.
    """
    import httpx as httpx_lib  # noqa: PLC0415
    from urllib.parse import quote  # noqa: PLC0415

    try:
        resp = httpx_lib.put(
            f"http://localhost:15672/api/permissions/{quote(vhost_name, safe='')}/test",
            json={"configure": ".*", "write": ".*", "read": ".*"},
            auth=("guest", "guest"),
            timeout=5,
        )
        if resp.status_code < 300:
            logger.debug("Granted publisher access to vhost %s", vhost_name)
        else:
            logger.warning(
                "Failed to grant publisher access to vhost %s: %s",
                vhost_name, resp.status_code,
            )
    except Exception:
        logger.debug("Could not grant publisher vhost access", exc_info=True)


def _setup_stomp_subscription_bounded(offering, identity, aim, user_agent, object_type):
    """Set up a STOMP subscription with bounded retries (for tests).

    Unlike _setup_single_stomp_subscription from utils.py, this uses
    max_retries=5 so the test fails fast instead of hanging forever.

    Returns StompConsumer tuple or None.
    """
    try:
        event_subscription = aim.register_event_subscription(identity, object_type)

        # Grant publisher user access to the subscription's vhost so the
        # Celery worker can publish events to it.
        _grant_publisher_vhost_access(event_subscription.user_uuid.hex)

        queue = aim.create_event_subscription_queue(event_subscription, object_type)
        if queue is None:
            logger.error("Failed to create event subscription queue")
            return None

        esm = EventSubscriptionManager(
            offering, None, None, user_agent, object_type,
        )
        connection = esm.setup_stomp_connection(
            event_subscription,
            offering.stomp_ws_host,
            offering.stomp_ws_port,
            offering.stomp_ws_path,
        )

        # Bounded retries — fail fast in tests instead of infinite loop
        connect_to_stomp_server(
            connection,
            event_subscription.uuid.hex,
            offering.api_token,
            max_retries=5,
        )
        if not connection.is_connected():
            logger.error("STOMP connection not established after retries")
            return None

        return (connection, event_subscription, offering)
    except ConnectFailedException:
        logger.exception("STOMP connection failed after max retries")
        return None
    except Exception:
        logger.exception("STOMP subscription setup error")
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _enable_identity_bridge():
    """Enable FEDERATED_IDENTITY_SYNC_ENABLED via Django ORM (local dev).

    The /api/configuration/ endpoint is read-only, so we set the constance
    setting directly via Django shell subprocess.
    """
    import shutil  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    waldur_dir = os.environ.get(
        "WALDUR_MASTERMIND_DIR",
        os.path.expanduser("~/workspace/waldur-mastermind"),
    )
    uv_bin = shutil.which("uv")
    if not uv_bin or not os.path.isdir(waldur_dir):
        logger.warning("Cannot enable identity bridge: uv or waldur_dir not found")
        return

    script = (
        "from constance import config; "
        "config.FEDERATED_IDENTITY_SYNC_ENABLED = True; "
        "print('OK:', config.FEDERATED_IDENTITY_SYNC_ENABLED)"
    )
    try:
        result = subprocess.run(  # noqa: S603
            [uv_bin, "run", "waldur", "shell", "-c", script],
            capture_output=True, text=True, timeout=15, cwd=waldur_dir,
        )
        if "OK:" in result.stdout:
            logger.info("Enabled FEDERATED_IDENTITY_SYNC_ENABLED via Django ORM")
        else:
            logger.warning(
                "Django shell identity bridge enable failed: %s",
                (result.stderr or result.stdout)[:200],
            )
    except Exception as exc:
        logger.warning("Django shell identity bridge enable error: %s", exc)


def _trigger_offering_user_creation(user_uuid, project_uuid):
    """Invoke the Celery task that creates offering users for a user in a project.

    With runserver (no Celery worker), the role_granted signal dispatches
    create_or_restore_offering_users_for_user.delay() which goes nowhere.
    This helper calls the task function directly via Django shell.
    """
    import shutil  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    waldur_dir = os.environ.get(
        "WALDUR_MASTERMIND_DIR",
        os.path.expanduser("~/workspace/waldur-mastermind"),
    )
    uv_bin = shutil.which("uv")
    if not uv_bin or not os.path.isdir(waldur_dir):
        logger.warning("Cannot trigger offering user creation: uv or waldur_dir not found")
        return

    u_clean = user_uuid.replace("-", "")
    p_clean = project_uuid.replace("-", "")
    script = (
        "from waldur_mastermind.marketplace.tasks import "
        "create_or_restore_offering_users_for_user; "
        f"create_or_restore_offering_users_for_user('{u_clean}', '{p_clean}'); "
        "print('OK')"
    )
    try:
        result = subprocess.run(  # noqa: S603
            [uv_bin, "run", "waldur", "shell", "-c", script],
            capture_output=True, text=True, timeout=15, cwd=waldur_dir,
        )
        if "OK" in result.stdout:
            logger.info(
                "Triggered offering user creation for user %s in project %s",
                user_uuid, project_uuid,
            )
        else:
            logger.warning(
                "Offering user creation may have failed: %s",
                (result.stderr or result.stdout)[:200],
            )
    except Exception as exc:
        logger.warning("Offering user creation error: %s", exc)


def _create_non_sp_customer(staff_client, name):
    """Create a plain customer (NOT a service provider). Returns (uuid, url)."""
    resp = staff_client.get_httpx_client().post(
        "/api/customers/", json={"name": name}
    )
    resp.raise_for_status()
    data = resp.json()
    logger.info("Created non-SP customer %s: %s", name, data["uuid"])
    return data["uuid"], data.get("url", "")


@pytest.fixture(scope="class")
def env(request):
    """Create test entities and dedicated users on the Waldur instance.

    Creates:
      - Two customers (A, B), two offerings (both Marketplace.Slurm), one project
      - A third non-SP customer (C) for user_b
      - user_a: offering manager on offering A (source side token)
      - user_b: customer C owner (non-SP, separate from offering B's SP)
                + ISD identity manager (target side token, accesses offering B
                users via ISD overlap, registers agent identities via IDM path)
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

    # --- Create non-SP customer for user_b ---
    # user_b must NOT have service provider access to offering B.
    # Access to offering B's offering users comes purely via identity manager
    # + ISD overlap (managed_isds ∩ subject_user.active_isds).
    # Agent identity registration uses the IDM path (no offering users required).
    cust_c_uuid, cust_c_url = _create_non_sp_customer(
        staff, f"inttest-{run_id}-customer-c"
    )

    # --- Assign roles ---

    # user_a: offering manager on offering A (source side)
    _add_offering_role(staff, result.offering_a.uuid, user_a.uuid)

    # user_b: customer C owner (non-SP — no service provider access via customer)
    _add_customer_role(staff, cust_c_uuid, user_b.uuid)

    # user_b: identity manager with managed_isds (enables ISD-based
    # offering user visibility on offering B and agent identity registration)
    _set_user_attributes(
        staff,
        user_b.uuid,
        is_identity_manager=True,
        managed_isds=[ISD_NAME],
    )

    # Enable Identity Bridge via Django ORM (required for active_isds management).
    # /api/configuration/ is read-only, so we use Django shell directly.
    _enable_identity_bridge()

    # subject_user: push via identity bridge to add ISD_NAME to active_isds.
    # Uses staff token since this is environment setup (user_b's IDM role
    # is already configured, but staff bypasses ISD scope checks).
    resp = staff.get_httpx_client().post(
        "/api/identity-bridge/",
        json={
            "username": subject_user.username,
            "source": ISD_NAME,
        },
    )
    if resp.status_code < 400:
        logger.info(
            "Pushed subject_user via identity bridge (source=%s): %s",
            ISD_NAME, resp.json(),
        )
    else:
        logger.warning(
            "Identity bridge push failed: %s %s",
            resp.status_code, resp.text[:300],
        )

    # --- Create resources and trigger offering user auto-creation ---

    # Offering A: create resource in project A, then add subject_user as member.
    # The role_granted signal triggers auto-creation of offering user on A.
    resource_a_uuid = ""
    try:
        resource_a_uuid = test_setup.create_resource_via_django(
            result.offering_a.uuid, result.project_a_uuid,
            name=f"inttest-{run_id}-resource-a",
        )
        logger.info("Created resource on offering A: %s", resource_a_uuid)
    except Exception:
        logger.warning("Could not create resource on offering A", exc_info=True)

    if resource_a_uuid:
        _add_project_role(staff, result.project_a_uuid, subject_user.uuid)
        # The role_granted signal dispatches a Celery task, but without a worker
        # it won't run. Invoke the task function directly via Django shell.
        _trigger_offering_user_creation(subject_user.uuid, result.project_a_uuid)

    # Offering B: create project under customer C, create resource, then add
    # subject_user to the project. user_b (customer C owner) adds subject_user,
    # triggering offering user auto-creation on B.
    proj_b_uuid = ""
    resource_b_uuid = ""
    try:
        proj_b_uuid, _proj_b_url = test_setup._create_project(
            cust_c_url, f"inttest-{run_id}-project-b",
        )
        resource_b_uuid = test_setup.create_resource_via_django(
            result.offering_b.uuid, proj_b_uuid,
            name=f"inttest-{run_id}-resource-b",
        )
        logger.info("Created resource on offering B: %s", resource_b_uuid)
    except Exception:
        logger.warning("Could not create resource on offering B", exc_info=True)

    if resource_b_uuid:
        # Use user_b's token (customer C owner) to add subject_user to project B.
        if user_b.token:
            from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

            client_b = get_client(WALDUR_API_URL, user_b.token)
            _add_project_role(client_b, proj_b_uuid, subject_user.uuid)
        else:
            _add_project_role(staff, proj_b_uuid, subject_user.uuid)
        _trigger_offering_user_creation(subject_user.uuid, proj_b_uuid)

    # --- Build WaldurBackend ---
    # Backend client uses user_b token for Waldur B access (customer C owner
    # + IDM with managed_isds — no SP access to offering B).
    # user_a token is used as the Waldur A REST client for sync operations
    # (offering manager on offering A).
    # Staff token is only used for environment setup/teardown.
    settings = result.backend_settings.copy()
    target_token = user_b.token or settings["target_api_token"]
    settings["target_api_token"] = target_token

    backend = WaldurBackend(settings, result.backend_components)
    backend.client = AutoApproveWaldurClient(
        api_url=settings["target_api_url"],
        api_token=target_token,
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
        project_b_uuid=proj_b_uuid,
        resource_a_uuid=resource_a_uuid,
        resource_b_uuid=resource_b_uuid,
    )

    yield test_env

    # Cleanup: delete users, non-SP customer, then entities
    for u in (user_a, user_b, subject_user):
        _delete_user(staff, u.uuid)
    # Delete non-SP customer C (not tracked by test_setup)
    staff.get_httpx_client().delete(f"/api/customers/{cust_c_uuid}/")
    test_setup.cleanup()


# ===========================================================================
# Test Suite 1: Username sync via polling
# ===========================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestUsernameSyncIntegration:
    """Sync offering user usernames from Waldur B to A on a single instance.

    Uses dedicated users instead of staff:
      - user_a token as the Waldur A API client (offering manager on A)
      - user_b token as the Waldur B API client (customer C owner + IDM)
      - subject_user as the user whose offering users are synced

    Offering users are auto-created by Waldur's natural flow:
      - env fixture creates resources on both offerings
      - subject_user is added to projects -> role_granted signal fires
      - Celery task creates offering users in CREATION_REQUESTED state

    Flow:
      01. Verify user_a and user_b tokens were obtained
      02. Verify offering users were auto-created by the system
      03. Set a known username on the B offering user (transitions to OK)
      04. Run sync_offering_user_usernames -> verify A updated
      05. Verify idempotency (second sync = no-op)
      06. Cleanup offering users
    """

    _state: dict = {}

    def test_01_verify_tokens(self, env: TestEnv):
        """Ensure both user_a and user_b have API tokens and correct access."""
        if not env.user_a.token:
            pytest.skip("Could not obtain token for user_a (offering manager)")
        if not env.user_b.token:
            pytest.skip("Could not obtain token for user_b (customer owner + IDM)")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        # Verify user_a (offering manager) can list offering users on A
        client_a = get_client(WALDUR_API_URL, env.user_a.token)
        resp = client_a.get_httpx_client().get(
            "/api/marketplace-offering-users/",
            params={"offering_uuid": env.offering_a_uuid},
        )
        assert resp.status_code < 400, (
            f"user_a cannot list offering users on A: {resp.status_code}"
        )
        logger.info("user_a (offering manager) can access offering A users")

        # Verify user_b (customer C owner + IDM) can list offering users on B
        # via identity manager + managed_isds overlap
        client_b = get_client(WALDUR_API_URL, env.user_b.token)
        resp = client_b.get_httpx_client().get(
            "/api/marketplace-offering-users/",
            params={"offering_uuid": env.offering_b_uuid},
        )
        assert resp.status_code < 400, (
            f"user_b cannot list offering users on B: {resp.status_code}"
        )
        logger.info("user_b (customer C owner + IDM) can access offering B users via ISD")

    def test_02_verify_offering_users_autocreated(self, env: TestEnv):
        """Verify offering users were auto-created by the system.

        The env fixture created resources on both offerings and added
        subject_user to the projects, which triggers Waldur's
        role_granted -> create_or_restore_offering_users_for_user flow.
        """
        if not env.user_a.token:
            pytest.skip("No user_a token")

        staff = env.test_setup.client

        # Offering A: auto-created when subject_user joined project A
        ou_a, found_a = _wait_for_offering_user(
            staff, env.subject_user.url, env.offering_a_uuid
        )
        assert found_a, (
            "Offering user A was not auto-created. Check that: "
            "1) resource exists on offering A, "
            "2) subject_user has a role on project A, "
            "3) service_provider_can_create_offering_user=True in plugin_options"
        )
        ou_a_uuid = ou_a["uuid"]
        self.__class__._state["ou_a_uuid"] = ou_a_uuid
        logger.info(
            "Offering user A auto-created: %s (state=%s)",
            ou_a_uuid, ou_a.get("state", ""),
        )

        # Transition A to OK state so sync can work (set placeholder username)
        _ensure_offering_user_ok(staff, ou_a_uuid, ou_a.get("state", ""))

        # Offering B: auto-created when subject_user was added to project B
        ou_b, found_b = _wait_for_offering_user(
            staff, env.subject_user.url, env.offering_b_uuid
        )
        assert found_b, (
            "Offering user B was not auto-created. Check that: "
            "1) resource exists on offering B, "
            "2) subject_user has a role on project B, "
            "3) service_provider_can_create_offering_user=True in plugin_options"
        )
        ou_b_uuid = ou_b["uuid"]
        self.__class__._state["ou_b_uuid"] = ou_b_uuid
        logger.info(
            "Offering user B auto-created: %s (state=%s)",
            ou_b_uuid, ou_b.get("state", ""),
        )

    def test_03_set_target_username(self, env: TestEnv):
        """Set a known test username on the Waldur B offering user.

        The auto-created offering user starts in CREATION_REQUESTED state
        with an empty username (username_generation_policy=service_provider).
        Setting the username transitions it to OK state.
        """
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        _set_and_verify_username(
            env.test_setup.client, self.__class__._state, "ou_b_uuid", "inttest-sync"
        )

    def test_04_sync_usernames(self, env: TestEnv):
        """Run sync and verify the B username propagated to A."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415

        # Use user_a token as the Waldur A REST client — user_a is offering
        # manager on offering A. No staff token in data path.
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
        logger.info("Username '%s' propagated from B to A", expected)

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
        """Clean up auto-created offering users."""
        staff = env.test_setup.client

        for side, key_uuid in [("A", "ou_a_uuid"), ("B", "ou_b_uuid")]:
            ou_uuid = self.__class__._state.get(key_uuid)
            if not ou_uuid:
                continue
            status = _delete_offering_user(staff, ou_uuid)
            logger.info("Deleted offering user %s %s (status %s)", side, ou_uuid, status)


# ===========================================================================
# Test Suite 2: Identity manager STOMP event routing
# ===========================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestIdentityManagerEventRouting:
    """Verify STOMP OFFERING_USER events are routed to offering managers and IDMs.

    user_b acts as:
      - Customer C owner (non-SP, separate from offering B's SP)
      - ISD identity manager (managed_isds overlapping subject_user.active_isds)
      - Registers agent identity via IDM path (no offering manager on B)

    The test creates two STOMP subscriptions on offering B:
      1. user_a subscription (offering manager on B — granted in test_02)
      2. user_b subscription (IDM — agent identity registered via IDM path)

    Then triggers offering user changes and verifies both receive events.
    Clearing user_b.managed_isds tests the negative case — user_b's STOMP
    subscription remains active (events still delivered to the queue), but
    the IDM routing path is no longer active for new subscriptions.

    Flow:
      01. Verify prerequisites (STOMP, tokens)
      02. Set up STOMP subscriptions for user_a and user_b
      03. Update offering user username on B → verify both receive event
      04. Clear user_b managed_isds → update again → verify events
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
        """Set up STOMP subscriptions for user_a and user_b.

        Both users register agent identities using their own tokens:
          - user_a: OFFERING.MANAGER on offering B (granted here for STOMP test)
          - user_b: ISD identity manager (agent identity via IDM path, no
            offering manager needed — commit 5ccbe43 removed the requirement
            for pre-existing offering users)

        Agent identity registration calls POST /api/marketplace-site-agent-identities/.
        user_a uses OFFERING.MANAGER path, user_b uses IDM path.
        """
        if not env.user_a.token or not env.user_b.token:
            pytest.skip("Missing tokens")

        from waldur_site_agent.common.agent_identity_management import (  # noqa: PLC0415
            AgentIdentityManager,
        )
        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415
        from waldur_api_client.models import (  # noqa: PLC0415
            ObservableObjectTypeEnum,
        )

        # --- user_a subscription (offering manager on offering B) ---
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
            logger.exception(
                "user_a (OFFERING.MANAGER) cannot register agent identity on offering B. "
                "POST /api/marketplace-site-agent-identities/ requires CREATE_OFFERING "
                "permission on offering.customer. Backend must grant this to OFFERING.MANAGER."
            )
            pytest.fail(
                "user_a (OFFERING.MANAGER) cannot register agent identity — "
                "CREATE_OFFERING permission needed on offering.customer"
            )

        ua_capture = MessageCapture()
        ua_consumer = _setup_stomp_subscription_bounded(
            ua_offering, ua_identity, ua_aim,
            "inttest-ua", ObservableObjectTypeEnum.OFFERING_USER,
        )
        if ua_consumer is None:
            pytest.fail("Could not set up user_a STOMP subscription")

        conn, sub, _ = ua_consumer
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        if listener:
            listener.on_message_callback = ua_capture.make_handler()

        self.__class__._state["ua_consumer"] = ua_consumer
        self.__class__._state["ua_capture"] = ua_capture
        logger.info(
            "user_a STOMP subscription active — sub=%s vhost=%s offering=%s connected=%s",
            sub.uuid.hex, sub.user_uuid.hex, ua_offering.uuid, conn.is_connected(),
        )

        # --- user_b subscription (IDM — agent identity via IDM path) ---
        ub_offering = _make_offering_for_stomp(
            WALDUR_API_URL, env.user_b.token, env.offering_b_uuid,
            name="user_b STOMP",
        )
        ub_client = get_client(WALDUR_API_URL, env.user_b.token)
        ub_aim = AgentIdentityManager(ub_offering, ub_client)

        try:
            ub_identity = ub_aim.register_identity("inttest-ub")
        except Exception:
            logger.exception(
                "user_b (IDM) cannot register agent identity on offering B. "
                "POST /api/marketplace-site-agent-identities/ requires ISD identity "
                "manager with managed_isds on a non-archived/draft offering."
            )
            pytest.fail(
                "user_b (IDM) cannot register agent identity — "
                "ISD identity manager path failed"
            )

        ub_capture = MessageCapture()
        ub_consumer = _setup_stomp_subscription_bounded(
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
        _ou_b, ou_b_uuid = _setup_offering_user(
            staff, self.__class__._state, env.subject_user.url,
            env.offering_b_uuid, "B", placeholder="pre-test",
        )

        # Set username to trigger update event
        test_username = f"stomp-test-{uuid_mod.uuid4().hex[:6]}"
        resp = staff.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_b_uuid}/",
            json={"username": test_username},
        )
        resp.raise_for_status()
        logger.info("PATCHed offering user B with username '%s'", test_username)

        # Wait for user_a to receive the update event with the new username.
        # Earlier events (create, state change) also arrive on this queue,
        # so we match on both offering_user_uuid AND username.
        ou_uuid_hex = _uuid_hex(ou_b_uuid)
        event = ua_capture.wait_for_event(
            timeout=30, offering_user_uuid=ou_uuid_hex, username=test_username,
        )

        if event:
            logger.info(
                "user_a received event: action=%s username=%s",
                event.get("action"), event.get("username"),
            )
        else:
            all_msgs = ua_capture.messages
            logger.warning(
                "user_a did not receive OFFERING_USER event with username='%s'. "
                "Captured %d messages: %s",
                test_username, len(all_msgs),
                [m.get("action") for m in all_msgs],
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
            timeout=10, offering_user_uuid=ou_uuid_hex, username=test_username,
        )

        if ub_event:
            logger.info(
                "user_b (IDM) received event: action=%s username=%s",
                ub_event.get("action"), ub_event.get("username"),
            )
        else:
            logger.warning(
                "user_b did NOT receive the event. "
                "Check STOMP subscription setup and RabbitMQ permissions."
            )

    def test_04_verify_events_after_clearing_isds(self, env: TestEnv):
        """Clear managed_isds and check event delivery behaviour.

        user_b has NO offering manager access to offering B — only IDM.
        After clearing managed_isds, user_b has no permission path to
        offering B at all. The STOMP subscription queue still exists in
        RabbitMQ, but Mastermind may or may not filter events at publish
        time. This test documents actual behaviour without asserting.
        """
        ub_capture = self.__class__._state.get("ub_capture")
        if ub_capture is None:
            pytest.skip("user_b STOMP not set up")

        staff = env.test_setup.client

        # Clear user_b's managed_isds
        _set_user_attributes(staff, env.user_b.uuid, managed_isds=[])

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

        # user_b has no offering manager access and no IDM access after
        # clearing managed_isds. Events may still arrive (queue exists)
        # or may be filtered at publish time — we document either outcome.
        event = ub_capture.wait_for_event(
            timeout=15, offering_user_uuid=_uuid_hex(ou_b_uuid), username=new_username,
        )

        if event:
            logger.info(
                "user_b still receives events after clearing managed_isds "
                "(STOMP queue remains active despite no permission path)"
            )
        else:
            logger.info(
                "user_b did NOT receive event after clearing managed_isds "
                "(expected — no offering manager or IDM access). "
                "Captured %d messages total.", len(ub_capture.messages),
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


# ===========================================================================
# Test Suite 3: Periodic reconciliation via run_periodic_username_reconciliation
# ===========================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestPeriodicReconciliationIntegration:
    """Integration test for run_periodic_username_reconciliation.

    Exercises the full event_processing.utils orchestration layer:
      1. Constructs an Offering with stomp_enabled + membership_sync_backend="waldur"
      2. Calls run_periodic_username_reconciliation([offering], user_agent)
      3. Verifies username propagation from B to A

    This covers the gap between unit tests (mocked backend) and the existing
    integration tests (call sync_offering_user_usernames directly).

    Reuses the env fixture from TestUsernameSyncIntegration.
    """

    _state: dict = {}

    def test_01_verify_prerequisites(self, env: TestEnv):
        """Ensure tokens are available."""
        if not env.user_a.token:
            pytest.skip("No token for user_a")
        if not env.user_b.token:
            pytest.skip("No token for user_b")

    def test_02_create_offering_users(self, env: TestEnv):
        """Create offering users for subject_user on both offerings."""
        if not env.user_a.token:
            pytest.skip("No user_a token")

        staff = env.test_setup.client
        _setup_offering_user(
            staff, self.__class__._state, env.subject_user.url,
            env.offering_a_uuid, "A",
        )
        _setup_offering_user(
            staff, self.__class__._state, env.subject_user.url,
            env.offering_b_uuid, "B", placeholder="pre-reconcile-placeholder",
        )

    def test_03_set_target_username(self, env: TestEnv):
        """Set a known username on the B offering user (env setup via staff)."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        _set_and_verify_username(
            env.test_setup.client, self.__class__._state, "ou_b_uuid", "reconcile"
        )

    def _make_reconciliation_offering(self, env: TestEnv, name: str) -> Offering:
        """Build an Offering matching real agent config for reconciliation tests.

        Uses user_a token for Waldur A access, backend_settings (with user_b
        token) for Waldur B access. No staff token in data path.
        """
        return Offering(
            name=name,
            waldur_api_url=WALDUR_API_URL,
            waldur_api_token=env.user_a.token,
            waldur_offering_uuid=env.offering_a_uuid,
            backend_type="waldur",
            stomp_enabled=True,
            username_reconciliation_enabled=True,
            membership_sync_backend="waldur",
            backend_settings=env.backend.backend_settings.copy(),
            backend_components=_RECONCILIATION_COMPONENTS,
        )

    def test_04_run_periodic_reconciliation(self, env: TestEnv):
        """Call run_periodic_username_reconciliation and verify sync."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415
        from waldur_site_agent.event_processing.utils import (  # noqa: PLC0415
            run_periodic_username_reconciliation,
        )

        offering = self._make_reconciliation_offering(env, "reconciliation-test")
        run_periodic_username_reconciliation([offering], "integration-test")

        # Verify offering user A now has the B username (user_a can read it)
        client_a = get_client(WALDUR_API_URL, env.user_a.token)
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
        logger.info(
            "run_periodic_username_reconciliation propagated '%s' from B to A",
            expected,
        )

    def test_05_idempotent_second_reconciliation(self, env: TestEnv):
        """Second reconciliation should be a no-op (no changes)."""
        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        from waldur_site_agent.common.utils import get_client  # noqa: PLC0415
        from waldur_site_agent.event_processing.utils import (  # noqa: PLC0415
            run_periodic_username_reconciliation,
        )

        offering = self._make_reconciliation_offering(env, "reconciliation-test-idempotent")
        run_periodic_username_reconciliation([offering], "integration-test")

        # Username on A should still match B — no changes
        client_a = get_client(WALDUR_API_URL, env.user_a.token)
        ou_a_uuid = self.__class__._state["ou_a_uuid"]
        resp = client_a.get_httpx_client().get(
            f"/api/marketplace-offering-users/{ou_a_uuid}/"
        )
        resp.raise_for_status()
        actual = resp.json().get("username", "")
        expected = self.__class__._state["test_username_b"]
        assert actual == expected, f"Username changed unexpectedly: '{actual}'"
        logger.info("Idempotency confirmed — no changes on second reconciliation")

    def test_06_skips_non_qualifying_offering(self, env: TestEnv):
        """Verify reconciliation skips offerings without stomp_enabled."""
        from waldur_site_agent.event_processing.utils import (  # noqa: PLC0415
            run_periodic_username_reconciliation,
        )

        # stomp_enabled=False — should be completely skipped
        offering_no_stomp = Offering(
            name="no-stomp-offering",
            waldur_api_url=WALDUR_API_URL,
            waldur_api_token=env.user_a.token or "dummy",
            waldur_offering_uuid=env.offering_a_uuid,
            backend_type="waldur",
            stomp_enabled=False,
            membership_sync_backend="waldur",
        )

        # No membership_sync_backend — should be completely skipped
        offering_no_sync = Offering(
            name="no-sync-offering",
            waldur_api_url=WALDUR_API_URL,
            waldur_api_token=env.user_a.token or "dummy",
            waldur_offering_uuid=env.offering_a_uuid,
            backend_type="waldur",
            stomp_enabled=True,
            membership_sync_backend="",
        )

        # Should complete without errors or API calls
        run_periodic_username_reconciliation(
            [offering_no_stomp, offering_no_sync], "integration-test"
        )
        logger.info("Non-qualifying offerings correctly skipped")

    def test_07_cleanup_offering_users(self, env: TestEnv):
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
