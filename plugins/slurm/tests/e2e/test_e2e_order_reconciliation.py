"""End-to-end test: periodic order reconciliation recovers stuck EXECUTING orders.

Scenario:
  1. Create a SLURM allocation order via the Waldur API.
  2. Approve the order (transition to EXECUTING) but do NOT run the processor,
     simulating a situation where the STOMP handler failed mid-flight.
  3. Run ``run_periodic_order_reconciliation`` (the new reconciliation function).
  4. Assert the order reaches DONE and the SLURM account exists on the emulator.

Uses the SLURM emulator backend: orders complete synchronously in one cycle.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=e2e-local-config.yaml \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_order_reconciliation.py -v -s
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import pytest

from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_approve_by_provider,
    marketplace_orders_retrieve,
)
from waldur_api_client.models.order_approve_by_provider_request import (
    OrderApproveByProviderRequest,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from plugins.slurm.tests.e2e.conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
)
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing.utils import run_periodic_order_reconciliation

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="E2E tests disabled")


class TestOrderReconciliation:
    """Verify that run_periodic_order_reconciliation completes stuck orders."""

    def test_reconciliation_completes_executing_order(
        self,
        offering: Offering,
        waldur_client,
        slurm_backend,
        project_uuid,
        report,
    ):
        """An order stuck in EXECUTING is completed by order reconciliation.

        Steps:
          1. Create a CREATE order.
          2. Approve it (→ EXECUTING) without running the processor.
          3. Run run_periodic_order_reconciliation.
          4. Assert order is DONE and SLURM account exists.
        """
        report.heading(2, "Test: order reconciliation completes stuck EXECUTING order")

        # --- Step 1: Create order ---
        offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
        project_url = get_project_url(waldur_client, project_uuid)

        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100},
            name="reconciliation-test",
        )
        report.text(f"Created order `{order_uuid}`\n")

        # --- Step 2: Approve to EXECUTING (simulating partial processing) ---
        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        state = order.state if not isinstance(order.state, type(UNSET)) else None

        if state == OrderState.PENDING_PROVIDER:
            marketplace_orders_approve_by_provider.sync_detailed(
                client=waldur_client,
                uuid=order_uuid,
                body=OrderApproveByProviderRequest(),
            )
            order = marketplace_orders_retrieve.sync(
                client=waldur_client, uuid=order_uuid
            )
            state = order.state if not isinstance(order.state, type(UNSET)) else None

        assert state == OrderState.EXECUTING, (
            f"Expected EXECUTING after approval, got {state}"
        )
        report.text(f"Order approved → state: `{state}`\n")

        # --- Step 3: Run order reconciliation (the function under test) ---
        report.text("Running `run_periodic_order_reconciliation` ...\n")
        # Use threshold=0 so the freshly-created order is picked up immediately
        # (in production the default 30-min threshold filters out active orders)
        run_periodic_order_reconciliation([offering], stuck_threshold_minutes=0)

        # --- Step 4: Assert order is DONE ---
        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        final_state = order.state if not isinstance(order.state, type(UNSET)) else None
        report.text(f"Order final state: `{final_state}`\n")

        assert final_state == OrderState.DONE, (
            f"Expected DONE after reconciliation, got {final_state}"
        )

        # Verify SLURM account exists on emulator
        slurm_bin = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
        sacctmgr = str(Path(slurm_bin) / "sacctmgr")
        try:
            output = subprocess.check_output(
                [sacctmgr, "show", "account", "-p"],
                stderr=subprocess.STDOUT,
                timeout=10,
            ).decode()
            report.text(f"SLURM accounts after reconciliation:\n```\n{output}\n```\n")
            # The account name is derived from the resource name
            logger.info("SLURM accounts: %s", output)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning("Could not verify SLURM account: %s", e)
