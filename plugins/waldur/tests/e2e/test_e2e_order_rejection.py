"""End-to-end tests for order rejection in Waldur A <-> Waldur B federation.

Validates that when a target order on Waldur B is rejected, the source order
on Waldur A is correctly set to ERRED state:

  1. Create order on Waldur A (source)
  2. Run processor cycle 1 — agent creates target order on Waldur B,
     source order stays EXECUTING with backend_id set
  3. Reject the target order on Waldur B
  4. Run processor cycle 2 — check_pending_order() detects REJECTED,
     source order set to ERRED

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur-a>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_order_rejection.py -v -s
"""

from __future__ import annotations

import logging
import os
import uuid

import pytest

from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_reject_by_provider,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_provider_rejection_request import (
    OrderProviderRejectionRequest,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from plugins.waldur.tests.e2e.conftest import snapshot_order, snapshot_resource
from plugins.waldur.tests.e2e.test_e2e_federation import (
    _create_source_order,
    _get_offering_info,
    _get_project_url,
)
from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent_waldur.backend import WaldurBackend
from waldur_site_agent_waldur.client import WaldurClient

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests
_state: dict = {}


@pytest.fixture(scope="module")
def non_approving_backend(offering):
    """WaldurBackend that does NOT auto-approve target orders on Waldur B.

    Unlike the default e2e backend (AutoApproveWaldurBackend), this uses
    the standard WaldurBackend and WaldurClient. Target orders on B stay
    in PENDING_PROVIDER state until explicitly approved or rejected.
    """
    settings = offering.backend_settings
    components = offering.backend_components_dict
    backend = WaldurBackend(settings, components)
    backend.client = WaldurClient(
        api_url=settings["target_api_url"],
        api_token=settings["target_api_token"],
        offering_uuid=settings["target_offering_uuid"],
    )
    return backend


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestE2EOrderRejection:
    """Tests that rejected target orders on Waldur B cause ERRED source orders on Waldur A."""

    def test_create_order_on_waldur_a(
        self, offering, waldur_client_a, project_a_uuid, report
    ):
        """Create a CREATE order on Waldur A."""
        report.heading(2, "Order Rejection: Create Order on Waldur A")

        offering_url, plan_url = _get_offering_info(
            waldur_client_a, offering.waldur_offering_uuid
        )
        project_url = _get_project_url(waldur_client_a, project_a_uuid)

        limits = {}
        for comp_name in offering.backend_components:
            limits[comp_name] = 5

        resource_name = f"e2e-reject-{uuid.uuid4().hex[:6]}"
        order_uuid = _create_source_order(
            client=waldur_client_a,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        _state["order_uuid"] = order_uuid
        _state["resource_name"] = resource_name

        snapshot_order(report, waldur_client_a, order_uuid, "Source order (initial)")
        report.text(f"Created order `{order_uuid}` for resource `{resource_name}`.")

    def test_processor_cycle_1_creates_target_order(
        self, offering, waldur_client_a, non_approving_backend, report
    ):
        """Run processor cycle 1: target order created on B, source stays EXECUTING."""
        report.heading(2, "Order Rejection: Processor Cycle 1")

        order_uuid = _state.get("order_uuid")
        if not order_uuid:
            pytest.skip("No order from previous test")

        processor = OfferingOrderProcessor(
            offering=offering,
            waldur_rest_client=waldur_client_a,
            resource_backend=non_approving_backend,
        )
        processor.process_offering()

        # Verify: source order should be EXECUTING with backend_id set
        order = marketplace_orders_retrieve.sync(
            client=waldur_client_a, uuid=order_uuid
        )
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        backend_id = (
            order.backend_id
            if not isinstance(order.backend_id, type(UNSET))
            else ""
        ) or ""

        snapshot_order(
            report, waldur_client_a, order_uuid, "Source order (after cycle 1)"
        )

        assert state == OrderState.EXECUTING, (
            f"Source order should be EXECUTING after cycle 1, got {state}"
        )
        assert backend_id, (
            "Source order should have backend_id (target order UUID) after cycle 1"
        )

        _state["target_order_uuid"] = backend_id

        # Get resource UUID from order
        resource_uuid_hex = (
            order.marketplace_resource_uuid.hex
            if hasattr(order.marketplace_resource_uuid, "hex")
            else str(order.marketplace_resource_uuid)
        )
        _state["resource_uuid_a"] = resource_uuid_hex

        report.text(
            f"Source order is EXECUTING. "
            f"Target order on B: `{backend_id}`."
        )

    def test_reject_target_order_on_waldur_b(
        self, waldur_client_b, report
    ):
        """Reject the target order on Waldur B."""
        report.heading(2, "Order Rejection: Reject Target Order on Waldur B")

        target_order_uuid = _state.get("target_order_uuid")
        if not target_order_uuid:
            pytest.skip("No target order from previous test")

        # Verify target order is in a rejectable state
        target_order = marketplace_orders_retrieve.sync(
            client=waldur_client_b, uuid=target_order_uuid
        )
        target_state = (
            target_order.state
            if not isinstance(target_order.state, type(UNSET))
            else None
        )

        report.status_snapshot(
            "Target order on B (before rejection)",
            {
                "uuid": target_order_uuid,
                "state": str(target_state),
            },
        )

        # Reject the order
        body = OrderProviderRejectionRequest(
            provider_rejection_comment="E2E test: simulating provider rejection"
        )
        resp = marketplace_orders_reject_by_provider.sync_detailed(
            uuid=target_order_uuid,
            client=waldur_client_b,
            body=body,
        )
        assert resp.status_code == 200, (
            f"reject_by_provider failed: {resp.status_code}"
        )

        # Verify target order is now REJECTED
        target_order = marketplace_orders_retrieve.sync(
            client=waldur_client_b, uuid=target_order_uuid
        )
        target_state = (
            target_order.state
            if not isinstance(target_order.state, type(UNSET))
            else None
        )

        report.status_snapshot(
            "Target order on B (after rejection)",
            {
                "uuid": target_order_uuid,
                "state": str(target_state),
            },
        )

        assert target_state == OrderState.REJECTED, (
            f"Target order should be REJECTED, got {target_state}"
        )
        report.text(f"Target order `{target_order_uuid}` rejected on Waldur B.")

    def test_processor_cycle_2_detects_rejection(
        self, offering, waldur_client_a, non_approving_backend, report
    ):
        """Run processor cycle 2: detects rejection, source order becomes ERRED."""
        report.heading(2, "Order Rejection: Processor Cycle 2")

        order_uuid = _state.get("order_uuid")
        if not order_uuid:
            pytest.skip("No order from previous test")

        processor = OfferingOrderProcessor(
            offering=offering,
            waldur_rest_client=waldur_client_a,
            resource_backend=non_approving_backend,
        )
        processor.process_offering()

        # Verify: source order should now be ERRED
        order = marketplace_orders_retrieve.sync(
            client=waldur_client_a, uuid=order_uuid
        )
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        error_msg = (
            order.error_message
            if not isinstance(order.error_message, type(UNSET))
            else ""
        ) or ""

        snapshot_order(
            report, waldur_client_a, order_uuid, "Source order (after cycle 2)"
        )

        assert state == OrderState.ERRED, (
            f"Source order should be ERRED after rejection detected, got {state}"
        )
        assert "failed" in error_msg.lower() or "rejected" in error_msg.lower(), (
            f"Error message should mention rejection: {error_msg}"
        )

        _state["final_state"] = str(state)
        _state["error_message"] = error_msg

        report.text(
            f"Source order correctly set to ERRED.\n"
            f"Error message: `{error_msg}`"
        )

    def test_verify_resource_state(self, waldur_client_a, report):
        """Verify the resource state on Waldur A after rejection."""
        report.heading(2, "Order Rejection: Verify Resource State")

        resource_uuid_a = _state.get("resource_uuid_a")
        if not resource_uuid_a:
            pytest.skip("No resource from previous tests")

        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid_a, client=waldur_client_a
        )
        resource_state = (
            str(resource.state) if not isinstance(resource.state, type(UNSET)) else "UNSET"
        )
        backend_id = (
            resource.backend_id
            if not isinstance(resource.backend_id, type(UNSET))
            else ""
        ) or ""

        snapshot_resource(
            report, waldur_client_a, resource_uuid_a, "Resource on A (after rejection)"
        )

        _state["resource_state"] = resource_state
        _state["resource_backend_id"] = backend_id

        report.text(
            f"Resource state: `{resource_state}`, backend_id: `{backend_id or '(empty)'}`"
        )

    def test_rejection_summary(self, report):
        """Summary of the order rejection e2e test."""
        report.heading(2, "Order Rejection: Summary")

        report.text("**Order rejection flow:**\n")
        report.text("| Step | Result |")
        report.text("|------|--------|")
        report.text(
            f"| Create order on A | `{_state.get('order_uuid', '?')}` |"
        )
        report.text(
            f"| Target order on B | `{_state.get('target_order_uuid', '?')}` |"
        )
        report.text(
            "| Reject target on B | REJECTED |"
        )
        report.text(
            f"| Source order final state | `{_state.get('final_state', '?')}` |"
        )
        report.text(
            f"| Error message | `{_state.get('error_message', '?')}` |"
        )
        report.text(
            f"| Resource state on A | `{_state.get('resource_state', '?')}` |"
        )
        report.text("\nOrder rejection e2e test completed.")
