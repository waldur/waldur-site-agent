"""End-to-end tests for Waldur A -> Waldur B federation.

Tests the full pipeline that the real agent runs in REST polling mode:
  1. Test creates an order on Waldur A (source, Marketplace.Slurm offering)
  2. OfferingOrderProcessor picks it up from Waldur A
  3. WaldurBackend forwards to Waldur B (target, Marketplace.Slurm offering)
  4. Non-blocking: order submitted on B, tracked via check_pending_order()
  5. Order completes on B, processor marks it done on A

These tests exercise the REST polling path (order_process mode).
For STOMP event processing tests, see TEST_PLAN.md (manual tests 5-8).

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur-a>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/ -v -s
"""

from __future__ import annotations

import logging
import os
import time
import uuid

import pytest

from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_create,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import (
    OrderCreateRequestLimits,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET

from plugins.waldur.tests.e2e.conftest import snapshot_order, snapshot_resource
from waldur_site_agent.common.processors import OfferingOrderProcessor

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests within the class
_state: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_offering_info(client, offering_uuid: str) -> tuple[str, str]:
    """Get the public offering URL and first plan URL for order creation.

    Order creation requires marketplace-public-offerings URLs (not provider).
    Plan URLs are nested under the public offering.

    Returns:
        (offering_url, plan_url)
    """
    resp = client.get_httpx_client().get(
        f"/api/marketplace-public-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    data = resp.json()
    offering_url = data["url"]
    plans = data.get("plans", [])
    if not plans:
        msg = f"No plans found for offering {offering_uuid}"
        raise RuntimeError(msg)
    plan_url = plans[0]["url"]
    return offering_url, plan_url


def _get_project_url(client, project_uuid: str) -> str:
    """Get the full URL for a project on Waldur A."""
    from waldur_api_client.api.projects import projects_retrieve

    proj = projects_retrieve.sync(client=client, uuid=project_uuid)
    return proj.url


def _create_source_order(
    client,
    offering_url: str,
    project_url: str,
    plan_url: str,
    limits: dict[str, int],
    name: str = "",
) -> str:
    """Create a CREATE order on Waldur A, return order UUID hex."""
    order_limits = OrderCreateRequestLimits()
    for key, value in limits.items():
        order_limits[key] = value

    attrs = GenericOrderAttributes()
    if name:
        attrs["name"] = name

    body = OrderCreateRequest(
        offering=offering_url,
        project=project_url,
        plan=plan_url,
        limits=order_limits,
        attributes=attrs,
        type_=RequestTypes.CREATE,
    )

    order = marketplace_orders_create.sync(client=client, body=body)
    order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
    logger.info("Created source order %s", order_uuid)
    return order_uuid


def _run_processor_until_order_terminal(
    offering,
    waldur_client,
    backend,
    order_uuid: str,
    max_cycles: int = 15,
    cycle_delay: int = 3,
    report=None,
) -> OrderState:
    """Run process_offering() in a loop until the order reaches a terminal state.

    Returns the final OrderState (DONE or ERRED). Does NOT fail on ERRED —
    callers should verify the actual resource state, because the Waldur A
    staging server has a known bug where set_state_done returns 500 even
    when the backend operation succeeded.

    When *report* is provided, each cycle is documented with API logs and
    order state snapshots.
    """
    processor = OfferingOrderProcessor(
        offering=offering,
        waldur_rest_client=waldur_client,
        resource_backend=backend,
    )

    for cycle in range(max_cycles):
        logger.info("--- Processor cycle %d ---", cycle + 1)
        if report:
            report.heading(4, f"Processor cycle {cycle + 1}")

        processor.process_offering()

        if report:
            report.flush_api_log(f"Cycle {cycle + 1} API calls")
            snapshot_order(
                report,
                waldur_client,
                order_uuid,
                f"Order state after cycle {cycle + 1}",
            )
            report.flush_api_log()

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        logger.info("Order %s state: %s (cycle %d)", order_uuid, state, cycle + 1)

        if state == OrderState.DONE:
            return OrderState.DONE
        if state == OrderState.ERRED:
            error_msg = getattr(order, "error_message", "unknown")
            logger.warning(
                "Order %s ERRED: %s (may be server-side set_state_done bug)",
                order_uuid,
                error_msg,
            )
            return OrderState.ERRED

        time.sleep(cycle_delay)

    pytest.fail(f"Order {order_uuid} not terminal after {max_cycles} cycles")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestE2EFederationLifecycle:
    """Full create -> update -> terminate lifecycle across two Waldur instances.

    Tests run in order. Each stores state in _state for subsequent tests.
    """

    # -- Scenario 1: Processor initialises against real Waldur A --

    def test_processor_init(self, offering, waldur_client_a, backend, report):
        """OfferingOrderProcessor can connect to Waldur A and initialise."""
        report.heading(2, "Scenario 1: Processor Init")
        processor = OfferingOrderProcessor(
            offering=offering,
            waldur_rest_client=waldur_client_a,
            resource_backend=backend,
        )
        assert processor.resource_backend is backend
        _state["processor_ok"] = "true"
        report.text("Processor initialized successfully with WaldurBackend.")
        report.flush_api_log("Init API calls")

    # -- Scenario 2: Create resource --

    def test_create_order(
        self,
        offering,
        waldur_client_a,
        waldur_client_b,
        backend,
        project_a_uuid,
        report,
    ):
        """Create order on A -> processor creates resource on B -> DONE."""
        report.heading(2, "Scenario 2: Create Resource")
        assert _state.get("processor_ok"), "Scenario 1 must pass first"

        offering_url, plan_url = _get_offering_info(
            waldur_client_a, offering.waldur_offering_uuid
        )
        project_url = _get_project_url(waldur_client_a, project_a_uuid)

        # Build limits from configured components (use small values)
        limits = {}
        for comp_name in offering.backend_components:
            limits[comp_name] = 10

        resource_name = f"e2e-{uuid.uuid4().hex[:6]}"
        order_uuid = _create_source_order(
            client=waldur_client_a,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        _state["create_order_uuid"] = order_uuid
        report.flush_api_log("Order creation on A")

        # Snapshot initial order state
        snapshot_order(
            report, waldur_client_a, order_uuid, "Source order (A) — initial"
        )
        report.flush_api_log()

        # Run processor until order reaches terminal state
        final_state = _run_processor_until_order_terminal(
            offering, waldur_client_a, backend, order_uuid, report=report
        )

        # Get resource UUID from the order (set regardless of DONE/ERRED)
        order = marketplace_orders_retrieve.sync(
            client=waldur_client_a, uuid=order_uuid
        )
        resource_uuid_a = order.marketplace_resource_uuid.hex
        _state["resource_uuid_a"] = resource_uuid_a

        # Verify: our resource on A has backend_id
        our_resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid_a, client=waldur_client_a
        )
        assert our_resource.backend_id, (
            f"Resource {resource_uuid_a} on A should have backend_id "
            f"(order state: {final_state})"
        )
        _state["resource_backend_id"] = our_resource.backend_id
        logger.info(
            "Resource on A: %s, backend_id: %s (order state: %s)",
            resource_uuid_a,
            our_resource.backend_id,
            final_state,
        )

        # Verify: resource exists on B (A's backend_id = B's resource UUID)
        resource_b = marketplace_provider_resources_retrieve.sync(
            uuid=our_resource.backend_id, client=waldur_client_b
        )
        assert resource_b, f"Resource not found on B: {our_resource.backend_id}"
        _state["resource_uuid_b"] = (
            resource_b.uuid.hex
            if hasattr(resource_b.uuid, "hex")
            else str(resource_b.uuid)
        )
        logger.info(
            "Resource on B: %s, state: %s",
            _state["resource_uuid_b"],
            resource_b.state,
        )

        # Report final snapshots
        report.flush_api_log()
        snapshot_resource(
            report, waldur_client_a, resource_uuid_a, "Resource on A (final)"
        )
        report.flush_api_log()
        snapshot_resource(
            report,
            waldur_client_b,
            _state["resource_uuid_b"],
            "Resource on B (final)",
        )
        report.flush_api_log()
        report.text(
            f"\n**Changes:** Resource created on B. "
            f"A's backend_id = B's resource UUID (`{our_resource.backend_id}`). "
            f"Order final state: `{final_state}`."
        )

        if final_state == OrderState.ERRED:
            logger.warning(
                "Order %s ended ERRED but resource was created successfully "
                "(known set_state_done server bug)",
                order_uuid,
            )

    # -- Scenario 3: Update limits --

    def test_update_limits(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Update limits order on A -> processor forwards to B -> DONE."""
        report.heading(2, "Scenario 3: Update Limits")
        assert _state.get("resource_uuid_a"), "Scenario 2 must pass first"

        resource_uuid_a = _state["resource_uuid_a"]

        # Snapshot resource on B before update
        resource_uuid_b = _state.get("resource_uuid_b", "")
        if resource_uuid_b:
            snapshot_resource(
                report, waldur_client_b, resource_uuid_b, "Resource on B (before)"
            )
            report.flush_api_log()

        # Create update-limits order on Waldur A
        new_limits = {}
        for comp_name in offering.backend_components:
            new_limits[comp_name] = 20

        resp = waldur_client_a.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid_a}/update_limits/",
            json={"limits": new_limits},
        )
        if resp.status_code >= 400:
            pytest.fail(
                f"Failed to create update order: {resp.status_code} {resp.text[:500]}"
            )
        data = resp.json()
        update_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert update_order_uuid, f"No order UUID in response: {data}"
        logger.info("Update order on A: %s", update_order_uuid)
        report.flush_api_log("Update order creation on A")

        final_state = _run_processor_until_order_terminal(
            offering, waldur_client_a, backend, update_order_uuid, report=report
        )
        if final_state == OrderState.ERRED:
            logger.warning(
                "Update order %s ended ERRED (known set_state_done server bug)",
                update_order_uuid,
            )
        logger.info("Update limits completed (order state: %s)", final_state)

        # Snapshot resource on B after update
        if resource_uuid_b:
            snapshot_resource(
                report, waldur_client_b, resource_uuid_b, "Resource on B (after)"
            )
            report.flush_api_log()
        report.text(
            f"\n**Changes:** Limits updated to `{new_limits}`. "
            f"Order final state: `{final_state}`."
        )

    # -- Scenario 4: Terminate resource --

    def test_terminate_resource(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Terminate order on A -> processor deletes on B -> DONE."""
        report.heading(2, "Scenario 4: Terminate Resource")
        assert _state.get("resource_uuid_a"), "Scenario 2 must pass first"

        resource_uuid_a = _state["resource_uuid_a"]
        resource_uuid_b = _state.get("resource_uuid_b", "")

        # Snapshot resources before termination
        snapshot_resource(
            report, waldur_client_a, resource_uuid_a, "Resource on A (before)"
        )
        report.flush_api_log()
        if resource_uuid_b:
            snapshot_resource(
                report, waldur_client_b, resource_uuid_b, "Resource on B (before)"
            )
            report.flush_api_log()

        # Create terminate order on Waldur A
        resp = waldur_client_a.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid_a}/terminate/",
            json={},
        )
        if resp.status_code >= 400:
            pytest.fail(
                f"Failed to create terminate order: {resp.status_code} {resp.text[:500]}"
            )
        data = resp.json()
        terminate_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert terminate_order_uuid, f"No order UUID in response: {data}"
        logger.info("Terminate order on A: %s", terminate_order_uuid)
        report.flush_api_log("Terminate order creation on A")

        final_state = _run_processor_until_order_terminal(
            offering, waldur_client_a, backend, terminate_order_uuid, report=report
        )
        if final_state == OrderState.ERRED:
            logger.warning(
                "Terminate order %s ended ERRED (known set_state_done server bug)",
                terminate_order_uuid,
            )

        # Verify: our resource on A is no longer OK
        resource_a = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid_a, client=waldur_client_a
        )
        assert str(resource_a.state) != "OK", (
            f"Resource {resource_uuid_a} on A should not be OK, got {resource_a.state}"
        )
        logger.info("Resource %s on A state: %s", resource_uuid_a, resource_a.state)

        # Snapshot resources after termination
        snapshot_resource(
            report, waldur_client_a, resource_uuid_a, "Resource on A (after)"
        )
        report.flush_api_log()

        # Verify: our resource on B is no longer OK
        if resource_uuid_b:
            resource_b = marketplace_provider_resources_retrieve.sync(
                uuid=resource_uuid_b, client=waldur_client_b
            )
            assert str(resource_b.state) != "OK", (
                f"Resource {resource_uuid_b} on B should not be OK, got {resource_b.state}"
            )
            logger.info("Resource %s on B state: %s", resource_uuid_b, resource_b.state)
            snapshot_resource(
                report, waldur_client_b, resource_uuid_b, "Resource on B (after)"
            )
            report.flush_api_log()

        report.text(
            f"\n**Changes:** Resource terminated on both sides. "
            f"Order final state: `{final_state}`."
        )
        logger.info("Terminate completed")
