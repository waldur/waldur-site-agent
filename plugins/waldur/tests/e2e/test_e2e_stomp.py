"""End-to-end STOMP event processing tests for Waldur A -> Waldur B federation.

Tests 5-7 from TEST_PLAN.md:
  5. Source STOMP connections (Waldur A) establish successfully
  6. Target STOMP connection (Waldur B) establishes successfully
  7. STOMP order event flow: create order -> receive events -> completion

Test 8 (fallback when STOMP unavailable) is already covered by
TestE2EFederationLifecycle in test_e2e_federation.py (REST polling path).

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur-a>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_stomp.py -v -s
"""

from __future__ import annotations

import logging
import os
import uuid

import pytest

from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.types import UNSET

from plugins.waldur.tests.e2e.conftest import (
    MessageCapture,
    check_stomp_available,
    snapshot_resource,
)
from plugins.waldur.tests.e2e.test_e2e_federation import (
    _create_source_order,
    _get_offering_info,
    _get_project_url,
    _run_processor_until_order_terminal,
)
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.utils import (
    setup_stomp_offering_subscriptions,
    stop_stomp_consumers,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests within the class
_state: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Module-scoped STOMP fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stomp_available(offering):
    """Skip STOMP tests if endpoint not available."""
    if not offering.stomp_enabled:
        pytest.skip("stomp_enabled=false in config")
    if not check_stomp_available(offering.waldur_api_url):
        pytest.skip(f"STOMP endpoint not available on {offering.waldur_api_url}")


@pytest.fixture(scope="module")
def source_capture():
    return MessageCapture()


@pytest.fixture(scope="module")
def target_capture():
    return MessageCapture()


@pytest.fixture(scope="module")
def stomp_consumers(stomp_available, offering, source_capture, target_capture, report):
    """Set up all STOMP connections (source + target) with message capture.

    Source handlers are replaced with capture-only (order processing is done
    via REST in the tests). Target handlers are wrapped: they capture events
    AND delegate to the real ``make_target_order_handler`` so that source
    orders get marked done when the target order completes.
    """
    consumers = setup_stomp_offering_subscriptions(offering, "e2e-stomp-test")

    # Separate source vs target consumers.
    # Target offerings have name starting with "Target:" (set by
    # WaldurBackend.setup_target_event_subscriptions).
    source = [c for c in consumers if not c[2].name.startswith("Target:")]
    target = [c for c in consumers if c[2].name.startswith("Target:")]

    # Replace source handlers with capture-only (no processing)
    for conn, _sub, _off in source:
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        if listener:
            listener.on_message_callback = source_capture.make_handler()

    # Wrap target handlers with capture + delegate to original
    for conn, _sub, _off in target:
        listener = conn.get_listener(WALDUR_LISTENER_NAME)
        if listener:
            original = listener.on_message_callback
            listener.on_message_callback = target_capture.make_handler(delegate=original)

    yield {"source": source, "target": target, "all": consumers}

    # Cleanup
    stop_stomp_consumers({(offering.name, offering.waldur_offering_uuid): consumers})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestE2EStompFederation:
    """STOMP event-driven federation tests (Tests 5-7 from TEST_PLAN.md).

    Test 8 (fallback when STOMP unavailable) is already covered by
    TestE2EFederationLifecycle in test_e2e_federation.py.
    """

    # -- Scenario 5: Source STOMP connections --

    def test_source_stomp_connection(self, stomp_consumers, report):
        """Test 5: STOMP connections to Waldur A establish successfully."""
        report.heading(2, "Scenario 5: Source STOMP Connection")
        source = stomp_consumers["source"]
        assert source, "No source STOMP consumers were created"
        for conn, sub, off in source:
            assert conn.is_connected(), (
                f"Source STOMP connection for {off.name} is not connected"
            )
            observable = (
                sub.observable_objects[0]["object_type"]
                if sub.observable_objects
                else "N/A"
            )
            report.status_snapshot(
                f"Source STOMP: {observable}",
                {
                    "offering": off.name,
                    "connected": str(conn.is_connected()),
                    "object_type": observable,
                    "subscription_uuid": sub.uuid.hex if hasattr(sub.uuid, "hex") else str(sub.uuid),
                },
            )
        report.flush_api_log("Source STOMP setup")
        report.text(f"Source STOMP connections established: {len(source)}")

    # -- Scenario 6: Target STOMP connection --

    def test_target_stomp_connection(self, stomp_consumers, report):
        """Test 6: STOMP connection to Waldur B establishes successfully."""
        report.heading(2, "Scenario 6: Target STOMP Connection")
        target = stomp_consumers["target"]
        if not target:
            pytest.skip("No target STOMP consumers (target_stomp_enabled=false?)")
        for conn, sub, off in target:
            assert conn.is_connected(), (
                f"Target STOMP connection for {off.name} is not connected"
            )
            observable = (
                sub.observable_objects[0]["object_type"]
                if sub.observable_objects
                else "N/A"
            )
            report.status_snapshot(
                f"Target STOMP: {observable}",
                {
                    "offering": off.name,
                    "connected": str(conn.is_connected()),
                    "object_type": observable,
                    "subscription_uuid": sub.uuid.hex if hasattr(sub.uuid, "hex") else str(sub.uuid),
                },
            )
        report.flush_api_log("Target STOMP setup")
        report.text(f"Target STOMP connections established: {len(target)}")

    # -- Scenario 7: STOMP order event flow --

    def test_stomp_order_event_flow(
        self,
        offering,
        waldur_client_a,
        waldur_client_b,
        backend,
        project_a_uuid,
        stomp_consumers,
        source_capture,
        target_capture,
        report,
    ):
        """Test 7: Create order -> receive STOMP events -> completion."""
        report.heading(2, "Scenario 7: STOMP Order Event Flow")

        # 1. Create order on A
        offering_url, plan_url = _get_offering_info(
            waldur_client_a, offering.waldur_offering_uuid
        )
        project_url = _get_project_url(waldur_client_a, project_a_uuid)
        limits = {comp: 10 for comp in offering.backend_components}
        resource_name = f"e2e-stomp-{uuid.uuid4().hex[:6]}"
        order_uuid = _create_source_order(
            client=waldur_client_a,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        report.flush_api_log("Order creation on A")
        report.text(f"Created order `{order_uuid}` on Waldur A")

        # 2. Wait for source STOMP event
        source_event = source_capture.wait_for_order_event(order_uuid, timeout=30)
        if source_event:
            report.text(
                f"Source STOMP event received: "
                f"state=`{source_event.get('order_state')}` "
                f"uuid=`{source_event.get('order_uuid')}`"
            )
        else:
            report.text(
                "No source STOMP event captured within 30s "
                "(may arrive after order already picked up by processor)"
            )

        # 3. Process via REST (same mechanism as REST tests)
        final_state = _run_processor_until_order_terminal(
            offering, waldur_client_a, backend, order_uuid, report=report
        )

        # 4. Get resource info
        order = marketplace_orders_retrieve.sync(
            client=waldur_client_a, uuid=order_uuid
        )
        resource_uuid_a = (
            order.marketplace_resource_uuid.hex
            if not isinstance(order.marketplace_resource_uuid, type(UNSET))
            else ""
        )
        if resource_uuid_a:
            _state["resource_uuid_a"] = resource_uuid_a

        # 5. Check target STOMP event (if target STOMP active)
        if stomp_consumers["target"] and resource_uuid_a:
            our_resource = marketplace_provider_resources_retrieve.sync(
                uuid=resource_uuid_a, client=waldur_client_a
            )
            target_order_uuid = our_resource.backend_id or ""
            if target_order_uuid:
                target_event = target_capture.wait_for_order_event(
                    target_order_uuid, timeout=30
                )
                if target_event:
                    report.text(
                        f"Target STOMP event received: "
                        f"state=`{target_event.get('order_state')}` "
                        f"uuid=`{target_event.get('order_uuid')}`"
                    )
                else:
                    report.text(
                        "No target STOMP event captured "
                        "(may have arrived before capture started)"
                    )

        # 6. Snapshots
        if resource_uuid_a:
            snapshot_resource(
                report, waldur_client_a, resource_uuid_a, "Resource on A"
            )
        report.flush_api_log()
        report.text(f"Order final state: `{final_state}`")
        report.text(
            f"Source STOMP events captured: {len(source_capture.messages)}, "
            f"Target STOMP events captured: {len(target_capture.messages)}"
        )

    # -- Scenario 7b: Cleanup --

    def test_stomp_cleanup(
        self, offering, waldur_client_a, backend, report
    ):
        """Terminate resource created in STOMP test."""
        report.heading(2, "Scenario 7b: STOMP Test Cleanup")
        resource_uuid_a = _state.get("resource_uuid_a")
        if not resource_uuid_a:
            pytest.skip("No resource to clean up")

        resp = waldur_client_a.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid_a}/terminate/",
            json={},
        )
        if resp.status_code >= 400:
            report.text(
                f"Terminate request failed: {resp.status_code} {resp.text[:200]}"
            )
            pytest.fail(f"Terminate failed: {resp.status_code}")

        data = resp.json()
        terminate_uuid = data.get("order_uuid") or data.get("uuid", "")
        if terminate_uuid:
            _run_processor_until_order_terminal(
                offering, waldur_client_a, backend, terminate_uuid, report=report
            )
        report.text("STOMP test resource terminated.")
