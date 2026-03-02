"""E2E tests for STOMP WebSocket event delivery with SLURM emulator backend.

These tests verify that the STOMP event pipeline works end-to-end:
  1. STOMP connections establish to RabbitMQ via web_stomp
  2. Order events arrive via WebSocket when orders are created
  3. The full order-processing pipeline works with STOMP active

The STOMP tests use a separate config (WALDUR_E2E_STOMP_CONFIG) with
stomp_enabled=true and a single offering. The same Docker stack (including
RabbitMQ with rabbitmq_web_stomp) is reused from the REST E2E tests.

Environment variables:
    WALDUR_E2E_TESTS=true                   - Gate: skip all if not set
    WALDUR_E2E_STOMP_CONFIG=<path>          - Path to STOMP agent config YAML
    WALDUR_E2E_PROJECT_A_UUID=<uuid>        - Project UUID on Waldur
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx as httpx_lib
import pytest
from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_create,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.utils import (
    setup_stomp_offering_subscriptions,
    stop_stomp_consumers,
)

# Reuse helpers from conftest (same package)
from .conftest import (
    ReportWriter,
    _make_response_hook,
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
    snapshot_resource,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_STOMP_CONFIG_PATH = os.environ.get("WALDUR_E2E_STOMP_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")


# ---------------------------------------------------------------------------
# STOMP helpers (adapted from plugins/waldur/tests/e2e/conftest.py)
# ---------------------------------------------------------------------------


class MessageCapture:
    """Thread-safe STOMP message capture for E2E tests."""

    def __init__(self):
        self._messages: list[dict] = []
        self._lock = threading.Lock()
        self._waiters: dict[str, threading.Event] = {}

    def make_handler(self, delegate=None):
        """Return a STOMP handler that captures messages and optionally delegates.

        The returned function has the standard STOMP handler signature:
        ``(frame, offering, user_agent) -> None``
        """

        def handler(frame, offering, user_agent):
            message = json.loads(frame.body)
            with self._lock:
                self._messages.append(message)
                # Signal any waiters whose key:value matches this message
                for waiter_id, evt in list(self._waiters.items()):
                    if ":" in waiter_id:
                        k, v = waiter_id.split(":", 1)
                        if str(message.get(k, "")) == v:
                            evt.set()
            if delegate:
                delegate(frame, offering, user_agent)

        return handler

    def wait_for_order_event(self, order_uuid: str, timeout: float = 60) -> dict | None:
        """Wait for an ORDER event matching the given UUID. Returns message or None."""
        return self.wait_for_event("order_uuid", order_uuid, timeout)

    def wait_for_event(self, key: str, value: str, timeout: float = 60) -> dict | None:
        """Wait for any event where message[key] == value. Returns message or None."""
        waiter_id = f"{key}:{value}"
        with self._lock:
            for msg in self._messages:
                if msg.get(key) == value:
                    return msg
            event = threading.Event()
            self._waiters[waiter_id] = event

        if event.wait(timeout=timeout):
            with self._lock:
                for msg in reversed(self._messages):
                    if msg.get(key) == value:
                        return msg
        return None

    @property
    def messages(self) -> list[dict]:
        with self._lock:
            return list(self._messages)


def check_stomp_available(api_url: str, stomp_ws_host: str, stomp_ws_port: int) -> bool:
    """Check if the RabbitMQ web_stomp WebSocket endpoint is reachable.

    A plain HTTP GET to the WebSocket endpoint should return HTTP 400
    (Bad Request) or HTTP 426 (Upgrade Required), confirming the endpoint exists.
    """
    try:
        resp = httpx_lib.get(
            f"http://{stomp_ws_host}:{stomp_ws_port}/ws",
            timeout=5,
            follow_redirects=False,
        )
        # web_stomp returns 400 for non-WebSocket requests
        return resp.status_code in (400, 426)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Fixtures (module-scoped, separate from REST E2E fixtures)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stomp_config():
    """Load STOMP agent configuration from YAML file."""
    if not E2E_STOMP_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_STOMP_CONFIG not set")
    return load_configuration(E2E_STOMP_CONFIG_PATH, user_agent_suffix="e2e-stomp-test")


@pytest.fixture(scope="module")
def stomp_offering(stomp_config):
    """First offering from the STOMP config."""
    if not stomp_config.offerings:
        pytest.skip("No offerings in STOMP config")
    offering = stomp_config.offerings[0]
    if not offering.stomp_enabled:
        pytest.skip("STOMP not enabled in offering config")
    return offering


@pytest.fixture(scope="module")
def stomp_report(request, stomp_offering):
    """Module-scoped ReportWriter for STOMP tests."""
    rw = ReportWriter()
    rw.heading(1, "E2E STOMP Test Report")
    rw.text(f"**Date:** {datetime.now(tz=timezone.utc).isoformat()}")
    rw.text(f"**Config:** `{E2E_STOMP_CONFIG_PATH}`")
    rw.text(f"**Waldur:** {stomp_offering.waldur_api_url}")
    rw.text("**Backend:** SLURM emulator")
    rw.text(f"**Offering:** {stomp_offering.waldur_offering_uuid}")
    rw.text("")

    def finalizer():
        stem = Path(E2E_STOMP_CONFIG_PATH).stem if E2E_STOMP_CONFIG_PATH else "e2e-stomp"
        path = Path(__file__).parent / f"{stem}-report.md"
        rw.write(path)
        logger.info("STOMP E2E report written to %s", path)

    request.addfinalizer(finalizer)
    return rw


@pytest.fixture(scope="module")
def stomp_waldur_client(stomp_offering, stomp_report):
    """AuthenticatedClient for Waldur, with response logging."""
    client = get_client(stomp_offering.waldur_api_url, stomp_offering.waldur_api_token)
    httpx_client = client.get_httpx_client()
    httpx_client.event_hooks["response"].append(_make_response_hook(stomp_report))
    return client


@pytest.fixture(scope="module")
def _stomp_emulator_cleanup(stomp_offering):
    """Reset slurm-emulator state before the STOMP test module runs."""
    slurm_bin_path = stomp_offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")

    try:
        subprocess.check_output(
            [sacctmgr, "cleanup", "all"],
            stderr=subprocess.STDOUT,
            timeout=10,
        )
        logger.info("Emulator state reset via 'sacctmgr cleanup all'")
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning("sacctmgr cleanup failed (%s), deleting state file", exc)
        state_file = Path("/tmp/slurm_emulator_db.json")
        if state_file.exists():
            state_file.unlink()
            logger.info("Deleted emulator state file %s", state_file)


@pytest.fixture(scope="module")
def stomp_slurm_backend(stomp_offering, _stomp_emulator_cleanup):
    """SlurmBackend using the slurm-emulator CLI entry points."""
    settings = stomp_offering.backend_settings
    components = stomp_offering.backend_components_dict
    return SlurmBackend(settings, components)


@pytest.fixture(scope="module")
def stomp_project_uuid():
    """UUID of the project on Waldur to create orders in."""
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(scope="module")
def order_capture():
    """MessageCapture instance for intercepting ORDER events."""
    return MessageCapture()


@pytest.fixture(scope="module")
def stomp_consumers(request, stomp_offering, order_capture):
    """Set up STOMP subscriptions, replacing the ORDER handler with capture handler.

    Yields the list of StompConsumer tuples. Teardown stops all consumers.
    """
    # Check that STOMP endpoint is reachable
    stomp_host = stomp_offering.stomp_ws_host
    stomp_port = stomp_offering.stomp_ws_port or 15674
    if not check_stomp_available(stomp_offering.waldur_api_url, stomp_host, stomp_port):
        pytest.skip(f"STOMP endpoint not reachable at {stomp_host}:{stomp_port}")

    consumers = setup_stomp_offering_subscriptions(stomp_offering, "e2e-stomp-test")
    if not consumers:
        pytest.skip("No STOMP consumers could be established")

    # Replace the ORDER handler in each ORDER consumer's listener with our capture handler
    capture_handler = order_capture.make_handler(delegate=handlers.on_order_message_stomp)

    for conn, event_subscription, offering in consumers:
        # Check if this consumer handles ORDER events
        observable_objects = getattr(event_subscription, "observable_objects", [])
        for obj in observable_objects:
            if obj.get("object_type") == ObservableObjectTypeEnum.ORDER.value:
                listener = conn.get_listener(WALDUR_LISTENER_NAME)
                if listener:
                    listener.on_message_callback = capture_handler
                    logger.info(
                        "Replaced ORDER handler with capture handler for offering %s",
                        offering.name,
                    )
                break

    consumers_map = {(stomp_offering.name, stomp_offering.uuid): consumers}

    def finalizer():
        stop_stomp_consumers(consumers_map)
        logger.info("STOMP consumers stopped")

    request.addfinalizer(finalizer)
    return consumers


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("stomp_consumers")
class TestStompEventProcessing:
    """STOMP event delivery and processing tests.

    Test order matters: connections must be verified first, then event delivery,
    then full pipeline processing.
    """

    # Class-level state shared across ordered tests
    _state: dict = {}

    def test_01_stomp_connections_establish(self, stomp_consumers, stomp_report):
        """Verify that STOMP connections are established and connected."""
        stomp_report.heading(2, "Test 01: STOMP Connections Establish")

        connected_count = 0
        total_count = len(stomp_consumers)

        for conn, event_subscription, offering in stomp_consumers:
            is_connected = conn.is_connected()
            observable_type = "N/A"
            observable_objects = getattr(event_subscription, "observable_objects", [])
            if observable_objects:
                observable_type = observable_objects[0].get("object_type", "N/A")

            stomp_report.status_snapshot(
                f"Consumer: {observable_type}",
                {
                    "offering": offering.name,
                    "connected": str(is_connected),
                    "subscription_uuid": event_subscription.uuid.hex,
                },
            )

            if is_connected:
                connected_count += 1

        stomp_report.text(f"\n**Connected:** {connected_count}/{total_count}\n")
        assert connected_count > 0, "No STOMP connections established"
        assert connected_count == total_count, (
            f"Only {connected_count}/{total_count} STOMP connections established"
        )

    def test_02_stomp_order_event_received(
        self,
        stomp_consumers,
        stomp_waldur_client,
        stomp_offering,
        stomp_project_uuid,
        order_capture,
        stomp_report,
    ):
        """Create an order and verify the STOMP ORDER event is received."""
        stomp_report.heading(2, "Test 02: STOMP Order Event Received")

        # Get offering info for order creation
        offering_url, plan_url = get_offering_info(
            stomp_waldur_client, stomp_offering.waldur_offering_uuid
        )
        project_url = get_project_url(stomp_waldur_client, stomp_project_uuid)
        stomp_report.flush_api_log("Setup API calls")

        # Create an order
        order_uuid = create_source_order(
            stomp_waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100, "ram": 200},
            name="stomp-event-test",
        )
        stomp_report.text(f"**Created order:** `{order_uuid}`\n")
        stomp_report.flush_api_log("Order creation API calls")

        # Wait for the STOMP ORDER event
        msg = order_capture.wait_for_order_event(order_uuid, timeout=30)

        if msg:
            stomp_report.status_snapshot(
                "Received ORDER event",
                {
                    "order_uuid": msg.get("order_uuid", "?"),
                    "order_state": msg.get("order_state", "?"),
                },
            )
        else:
            stomp_report.text("**ERROR:** No ORDER event received within timeout\n")

        assert msg is not None, f"No STOMP ORDER event received for order {order_uuid} within 30s"
        assert msg["order_uuid"] == order_uuid

        # Store order UUID for subsequent tests
        TestStompEventProcessing._state["order_uuid"] = order_uuid
        TestStompEventProcessing._state["offering_url"] = offering_url
        TestStompEventProcessing._state["plan_url"] = plan_url
        TestStompEventProcessing._state["project_url"] = project_url

    def test_03_stomp_driven_order_processing(
        self,
        stomp_offering,
        stomp_waldur_client,
        stomp_slurm_backend,
        stomp_report,
    ):
        """Verify full order processing works with STOMP active."""
        stomp_report.heading(2, "Test 03: STOMP-Driven Order Processing")

        order_uuid = TestStompEventProcessing._state.get("order_uuid")
        if not order_uuid:
            pytest.skip("No order from test_02")

        # Run processor to handle the order
        final_state = run_processor_until_order_terminal(
            stomp_offering,
            stomp_waldur_client,
            stomp_slurm_backend,
            order_uuid,
            max_cycles=10,
            cycle_delay=2,
            report=stomp_report,
        )

        stomp_report.text(f"\n**Final order state:** `{final_state}`\n")

        assert final_state == OrderState.DONE, (
            f"Order {order_uuid} ended in {final_state}, expected DONE"
        )

        # Retrieve resource UUID for cleanup
        order = marketplace_orders_retrieve.sync(client=stomp_waldur_client, uuid=order_uuid)
        if not isinstance(order.marketplace_resource_uuid, type(UNSET)):
            resource_uuid = (
                order.marketplace_resource_uuid.hex
                if hasattr(order.marketplace_resource_uuid, "hex")
                else str(order.marketplace_resource_uuid)
            )
            TestStompEventProcessing._state["resource_uuid"] = resource_uuid
            snapshot_resource(stomp_report, stomp_waldur_client, resource_uuid, "Created resource")
            stomp_report.flush_api_log()

    def test_04_cleanup(
        self,
        stomp_offering,
        stomp_waldur_client,
        stomp_slurm_backend,
        stomp_report,
    ):
        """Terminate test resources created by STOMP tests."""
        stomp_report.heading(2, "Test 04: Cleanup")

        resource_uuid = TestStompEventProcessing._state.get("resource_uuid")
        if not resource_uuid:
            stomp_report.text("No resource to clean up.\n")
            return

        offering_url = TestStompEventProcessing._state["offering_url"]
        plan_url = TestStompEventProcessing._state["plan_url"]
        project_url = TestStompEventProcessing._state["project_url"]

        # Create a TERMINATE order
        body = OrderCreateRequest(
            offering=offering_url,
            project=project_url,
            plan=plan_url,
            attributes=GenericOrderAttributes(),
            type_=RequestTypes.TERMINATE,
        )

        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=stomp_waldur_client
        )
        resource_url = res.url if not isinstance(res.url, type(UNSET)) else None

        if resource_url:
            body.additional_properties["resource"] = resource_url

        try:
            order = marketplace_orders_create.sync(client=stomp_waldur_client, body=body)
            terminate_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
            stomp_report.text(f"**Terminate order:** `{terminate_uuid}`\n")

            final_state = run_processor_until_order_terminal(
                stomp_offering,
                stomp_waldur_client,
                stomp_slurm_backend,
                terminate_uuid,
                max_cycles=10,
                cycle_delay=2,
                report=stomp_report,
            )
            stomp_report.text(f"**Terminate final state:** `{final_state}`\n")
        except Exception as exc:
            stomp_report.text(f"**Cleanup error:** `{exc}`\n")
            logger.warning("Cleanup failed: %s", exc)
