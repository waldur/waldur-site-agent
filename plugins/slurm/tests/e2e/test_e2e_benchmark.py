"""API call benchmark — measures request counts and response sizes for core
processor operations.

Works on both main (unoptimized) and feature/optimise-requests (optimized)
branches. No optimization-specific assertions — just counts API calls and bytes.

Usage:
    WALDUR_E2E_TESTS=true \
    WALDUR_E2E_CONFIG=e2e-local-config.yaml \
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_benchmark.py -v -s

    # Multi-resource benchmark (N=10 by default):
    WALDUR_E2E_BENCH_RESOURCES=20 \
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_benchmark.py -v -s -k multi
"""

from __future__ import annotations

import logging
import os
import time

import pytest

from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
    OfferingReportProcessor,
)

from .conftest import (
    E2E_TESTS,
    create_source_order,
    get_offering_info,
    get_project_url,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    not E2E_TESTS, reason="WALDUR_E2E_TESTS not set"
)

BENCH_RESOURCE_COUNT = int(os.environ.get("WALDUR_E2E_BENCH_RESOURCES", "800"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_order_to_terminal(offering, client, backend, order_uuid, max_cycles=10):
    """Run order processor until order reaches terminal state, return final state."""
    processor = OfferingOrderProcessor(
        offering=offering,
        waldur_rest_client=client,
        resource_backend=backend,
    )

    for cycle in range(max_cycles):
        processor.process_offering()

        order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None

        if state in (OrderState.DONE, OrderState.ERRED):
            return state
        time.sleep(1)

    return None


def _process_all_pending_orders(offering, client, backend, max_cycles=50):
    """Run order processor until no pending orders remain.

    Returns number of cycles needed.
    """
    processor = OfferingOrderProcessor(
        offering=offering,
        waldur_rest_client=client,
        resource_backend=backend,
    )

    for cycle in range(max_cycles):
        processor.process_offering()

        # Check if any pending orders remain (use raw HTTP to avoid import issues)
        resp = client.get_httpx_client().get(
            "/api/marketplace-orders/",
            params={
                "offering_uuid": offering.waldur_offering_uuid,
                "state": ["pending-provider", "executing"],
                "page_size": 1,
            },
        )
        remaining = len(resp.json()) if resp.status_code == 200 else 0
        logger.info("Processor cycle %d done, %d orders still pending", cycle + 1, remaining)
        if remaining == 0:
            return cycle + 1

    return max_cycles


# ---------------------------------------------------------------------------
# Single-resource benchmark tests
# ---------------------------------------------------------------------------


@pytest.mark.incremental
class TestBenchmarkOrderProcessing:
    """Benchmark API calls for order create/update/terminate cycle."""

    _state: dict = {}

    def test_01_create(
        self, offering, waldur_client, slurm_backend, project_uuid, report
    ):
        """CREATE order -> process -> measure API calls."""
        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 10, "ram": 10},
            name="bench-create",
        )

        # Measure only the processor calls
        before = report.total_calls
        state = _run_order_to_terminal(
            offering, waldur_client, slurm_backend, order_uuid
        )
        after = report.total_calls

        logger.info(
            "CREATE: %d API calls, final state: %s", after - before, state
        )
        report.heading(2, "Benchmark: CREATE order processing")
        report.text(f"API calls: **{after - before}**")
        report.text(f"Order state: `{state}`")

        assert state == OrderState.DONE, f"Order {order_uuid} ended in {state}"

        # Save resource UUID for next tests
        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        res_uuid = (
            order.marketplace_resource_uuid.hex
            if hasattr(order.marketplace_resource_uuid, "hex")
            else str(order.marketplace_resource_uuid)
        )
        self.__class__._state["resource_uuid"] = res_uuid
        self.__class__._state["order_uuid"] = order_uuid

    def test_02_update(self, offering, waldur_client, slurm_backend, report):
        """UPDATE limits -> process -> measure API calls."""
        resource_uuid = self._state.get("resource_uuid")
        if not resource_uuid:
            pytest.skip("No resource from test_01")

        # Create update order
        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/update_limits/",
            json={"limits": {"cpu": 20, "ram": 20}},
        )
        resp.raise_for_status()
        update_order_uuid = resp.json()["order_uuid"]

        before = report.total_calls
        state = _run_order_to_terminal(
            offering, waldur_client, slurm_backend, update_order_uuid
        )
        after = report.total_calls

        logger.info(
            "UPDATE: %d API calls, final state: %s", after - before, state
        )
        report.heading(2, "Benchmark: UPDATE order processing")
        report.text(f"API calls: **{after - before}**")
        report.text(f"Order state: `{state}`")

        assert state == OrderState.DONE, f"Update order ended in {state}"

    def test_03_terminate(self, offering, waldur_client, slurm_backend, report):
        """TERMINATE -> process -> measure API calls."""
        resource_uuid = self._state.get("resource_uuid")
        if not resource_uuid:
            pytest.skip("No resource from test_01")

        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/terminate/",
            json={},
        )
        resp.raise_for_status()
        terminate_order_uuid = resp.json()["order_uuid"]

        before = report.total_calls
        state = _run_order_to_terminal(
            offering, waldur_client, slurm_backend, terminate_order_uuid
        )
        after = report.total_calls

        logger.info(
            "TERMINATE: %d API calls, final state: %s", after - before, state
        )
        report.heading(2, "Benchmark: TERMINATE order processing")
        report.text(f"API calls: **{after - before}**")
        report.text(f"Order state: `{state}`")

        assert state == OrderState.DONE, f"Terminate order ended in {state}"


class TestBenchmarkMembershipSync:
    """Benchmark API calls for membership sync cycle (existing resources)."""

    def test_01_sync(self, offering, waldur_client, slurm_backend, report):
        """Run full membership sync -> measure API calls."""
        processor = OfferingMembershipProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )

        before = report.total_calls
        processor.process_offering()
        after = report.total_calls

        logger.info("MEMBERSHIP SYNC: %d API calls", after - before)
        report.heading(2, "Benchmark: Membership sync")
        report.text(f"API calls: **{after - before}**")


class TestBenchmarkReporting:
    """Benchmark API calls for usage reporting cycle."""

    _state: dict = {}

    def test_01_setup_resource(
        self, offering, waldur_client, slurm_backend, project_uuid, report
    ):
        """Create a resource for reporting benchmark."""
        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100, "ram": 100},
            name="bench-report",
        )

        state = _run_order_to_terminal(
            offering, waldur_client, slurm_backend, order_uuid
        )
        assert state == OrderState.DONE

        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        res_uuid = (
            order.marketplace_resource_uuid.hex
            if hasattr(order.marketplace_resource_uuid, "hex")
            else str(order.marketplace_resource_uuid)
        )
        self.__class__._state["resource_uuid"] = res_uuid

    def test_02_report(self, offering, waldur_client, slurm_backend, report):
        """Run usage reporting -> measure API calls."""
        if not self._state.get("resource_uuid"):
            pytest.skip("No resource from test_01")

        processor = OfferingReportProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )

        before = report.total_calls
        processor.process_offering()
        after = report.total_calls

        logger.info("REPORTING: %d API calls", after - before)
        report.heading(2, "Benchmark: Usage reporting")
        report.text(f"API calls: **{after - before}**")


# ---------------------------------------------------------------------------
# Multi-resource benchmark — scales with WALDUR_E2E_BENCH_RESOURCES
# ---------------------------------------------------------------------------


class TestMultiResourceBenchmark:
    """Create N resources, then benchmark membership sync and reporting.

    Set WALDUR_E2E_BENCH_RESOURCES=N (default 800) to control the count.
    This is where caching and field filtering show their impact at scale.

    Resources are batch-created (all orders submitted first, then processed
    together) for speed — creating 800 resources one-by-one would be too slow.
    """

    _state: dict = {}

    def test_01_create_resources(
        self, offering, waldur_client, slurm_backend, project_uuid, report
    ):
        """Batch-create N resources: submit all orders, then process together."""
        n = BENCH_RESOURCE_COUNT
        report.heading(2, f"Multi-resource benchmark: setup ({n} resources)")

        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        # Phase 1: submit all orders (just API calls, no processing)
        order_uuids = []
        t0 = time.monotonic()
        for i in range(n):
            order_uuid = create_source_order(
                waldur_client,
                offering_url,
                project_url,
                plan_url,
                limits={"cpu": 10 + (i % 100), "ram": 10 + (i % 100)},
                name=f"bench-multi-{i:04d}",
            )
            order_uuids.append(order_uuid)
            if (i + 1) % 100 == 0:
                logger.info("Submitted %d/%d orders (%.1fs)", i + 1, n, time.monotonic() - t0)

        t_submit = time.monotonic() - t0
        logger.info("All %d orders submitted in %.1fs", n, t_submit)
        report.text(f"Submitted **{n}** orders in {t_submit:.1f}s")

        # Phase 2: process all pending orders (processor handles batches of 100)
        t1 = time.monotonic()
        cycles = _process_all_pending_orders(offering, waldur_client, slurm_backend)
        t_process = time.monotonic() - t1
        logger.info("All orders processed in %d cycles, %.1fs", cycles, t_process)
        report.text(f"Processed in **{cycles}** cycles, {t_process:.1f}s")

        # Phase 3: collect resource UUIDs from completed orders
        created = []
        for order_uuid in order_uuids:
            order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
            state = order.state if not isinstance(order.state, type(UNSET)) else None
            if state == OrderState.DONE and not isinstance(
                order.marketplace_resource_uuid, type(UNSET)
            ):
                res_uuid = (
                    order.marketplace_resource_uuid.hex
                    if hasattr(order.marketplace_resource_uuid, "hex")
                    else str(order.marketplace_resource_uuid)
                )
                created.append(res_uuid)

        report.text(f"Created **{len(created)}** / {n} resources successfully.")
        self.__class__._state["resource_uuids"] = created
        assert len(created) >= n * 0.95, f"Only {len(created)}/{n} resources created"

    def test_02_membership_sync(
        self, offering, waldur_client, slurm_backend, report
    ):
        """Membership sync with N active resources."""
        if not self._state.get("resource_uuids"):
            pytest.skip("No resources from test_01")

        n = len(self._state["resource_uuids"])
        report.heading(2, f"Multi-resource benchmark: membership sync ({n} resources)")

        processor = OfferingMembershipProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )

        before_calls = report.total_calls
        before_bytes = report.total_response_bytes
        t0 = time.monotonic()
        processor.process_offering()
        elapsed = time.monotonic() - t0
        after_calls = report.total_calls
        after_bytes = report.total_response_bytes

        calls = after_calls - before_calls
        resp_bytes = after_bytes - before_bytes
        logger.info(
            "MULTI MEMBERSHIP SYNC (%d resources): %d calls, %d bytes, %.1fs",
            n, calls, resp_bytes, elapsed,
        )
        report.text(f"Resources: **{n}**")
        report.text(f"API calls: **{calls}** (per resource: {calls / n:.1f})")
        report.text(f"Response bytes: **{resp_bytes}** (per resource: {resp_bytes / n:.0f})")
        report.text(f"Duration: **{elapsed:.1f}s**")

    def test_03_reporting(
        self, offering, waldur_client, slurm_backend, report
    ):
        """Usage reporting with N active resources."""
        if not self._state.get("resource_uuids"):
            pytest.skip("No resources from test_01")

        n = len(self._state["resource_uuids"])
        report.heading(2, f"Multi-resource benchmark: reporting ({n} resources)")

        processor = OfferingReportProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )

        before_calls = report.total_calls
        before_bytes = report.total_response_bytes
        t0 = time.monotonic()
        processor.process_offering()
        elapsed = time.monotonic() - t0
        after_calls = report.total_calls
        after_bytes = report.total_response_bytes

        calls = after_calls - before_calls
        resp_bytes = after_bytes - before_bytes
        logger.info(
            "MULTI REPORTING (%d resources): %d calls, %d bytes, %.1fs",
            n, calls, resp_bytes, elapsed,
        )
        report.text(f"Resources: **{n}**")
        report.text(f"API calls: **{calls}** (per resource: {calls / n:.1f})")
        report.text(f"Response bytes: **{resp_bytes}** (per resource: {resp_bytes / n:.0f})")
        report.text(f"Duration: **{elapsed:.1f}s**")

    def test_04_cleanup(
        self, offering, waldur_client, slurm_backend, report
    ):
        """Terminate all benchmark resources."""
        resource_uuids = self._state.get("resource_uuids", [])
        if not resource_uuids:
            pytest.skip("No resources to clean up")

        report.heading(2, "Multi-resource benchmark: cleanup")
        terminated = 0
        for res_uuid in resource_uuids:
            try:
                resp = waldur_client.get_httpx_client().post(
                    f"/api/marketplace-resources/{res_uuid}/terminate/",
                    json={},
                )
                resp.raise_for_status()
                terminated += 1
            except Exception:
                logger.warning("Failed to terminate %s", res_uuid)

        # Process all terminate orders
        if terminated:
            _process_all_pending_orders(offering, waldur_client, slurm_backend)

        report.text(f"Terminated **{terminated}** / {len(resource_uuids)} resources.")
