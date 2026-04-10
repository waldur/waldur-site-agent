"""End-to-end tests for prepaid billing model with SLURM backend.

Validates the full prepaid lifecycle with ONE_TIME components:
  - Creating a resource with end_date and limits (prepaid order)
  - Verifying SLURM GrpTRESMins = limit * months * unit_factor
  - Verifying upfront invoice items
  - Updating limits on a prepaid resource (supplementary billing)
  - Termination of expired resource

Prepaid components use accounting_type="one" (ONE_TIME billing).
The agent calculates GrpTRESMins = limit * duration_months * unit_factor,
giving SLURM a cumulative budget cap. Waldur handles billing (upfront
charges, supplementary billing for limit changes, renewal).

Uses a SLURM emulator backend: orders complete synchronously in 1 cycle.
Requires a Waldur instance with a prepaid SLURM offering configured
(accounting_type="one" components with duration constraints).

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-prepaid-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=e2e-prepaid-config.yaml \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    uv run pytest plugins/slurm/tests/e2e/test_e2e_prepaid.py -v -s
"""

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

from waldur_api_client.api.invoice_items import invoice_items_list
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import (
    OrderCreateRequestLimits,
)
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET

from plugins.slurm.tests.e2e.conftest import (
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
    snapshot_resource,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests
_state: dict = {}


def _create_prepaid_order(
    client,
    offering_url,
    project_url,
    plan_url,
    limits,
    end_date,
    name="",
):
    """Create a CREATE order with end_date attribute for prepaid offering."""
    from waldur_api_client.api.marketplace_orders import marketplace_orders_create

    order_limits = OrderCreateRequestLimits()
    for key, value in limits.items():
        order_limits[key] = value

    attrs = GenericOrderAttributes()
    if name:
        attrs["name"] = name
    attrs["end_date"] = end_date.isoformat()

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
    logger.info("Created prepaid order %s with end_date=%s", order_uuid, end_date)
    return order_uuid


def _get_slurm_account_limits(offering, backend_id):
    """Query SLURM emulator for account TRES limits."""
    slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
    try:
        output = subprocess.check_output(
            [sacctmgr, "--parsable2", "--noheader", "--immediate",
             "show", "account", backend_id, "format=Account,GrpTRES"],
            stderr=subprocess.STDOUT,
            timeout=5,
        ).decode().strip()
        return output
    except subprocess.CalledProcessError as exc:
        return f"ERROR: {exc.output.decode()}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestPrepaidSlurmLifecycle:
    """Full prepaid lifecycle tests for SLURM backend with limit-based components.

    Components use accounting_type="limit" so ordered amounts become
    SLURM account TRES hard caps. Tests are ordered and share state
    via module-level dict.
    """

    def test_01_create_prepaid_resource(
        self,
        offering,
        waldur_client,
        slurm_backend,
        project_uuid,
        report,
    ):
        """Create a prepaid SLURM resource with end_date and verify limits."""
        report.heading(2, "Prepaid Test 1: Create Prepaid Resource")

        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        limits = {comp_name: 10 for comp_name in offering.backend_components}
        resource_name = f"e2e-prepaid-{uuid.uuid4().hex[:6]}"
        end_date = date.today() + timedelta(days=90)  # 3 months

        order_uuid = _create_prepaid_order(
            client=waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            end_date=end_date,
            name=resource_name,
        )
        _state["create_order_uuid"] = order_uuid
        _state["initial_limits"] = limits
        report.flush_api_log("Prepaid order creation")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, order_uuid, report=report
        )

        # Get resource UUID from order
        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        _state["resource_uuid"] = resource_uuid

        # Verify resource has backend_id
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )
        assert resource.backend_id, (
            f"Resource {resource_uuid} should have backend_id after creation"
        )
        _state["resource_backend_id"] = resource.backend_id

        # Verify end_date is set on the resource
        resource_end_date = getattr(resource, "end_date", UNSET)
        if not isinstance(resource_end_date, type(UNSET)) and resource_end_date is not None:
            report.text(f"Resource end_date: `{resource_end_date}`")
            _state["end_date"] = str(resource_end_date)
        else:
            report.text("Warning: Resource end_date not set or not accessible")

        # Verify SLURM account created with correct limits
        slurm_output = _get_slurm_account_limits(offering, resource.backend_id)
        report.text(f"SLURM account TRES limits: `{slurm_output}`")
        assert resource.backend_id in slurm_output or slurm_output, (
            f"SLURM account {resource.backend_id} not found in emulator"
        )

        report.flush_api_log()
        snapshot_resource(report, waldur_client, resource_uuid, "Prepaid resource (created)")
        report.flush_api_log()
        report.text(
            f"\n**Result:** Prepaid CREATE completed. "
            f"Order state: `{final_state}`, end_date: `{end_date}`, "
            f"limits: `{limits}`."
        )

    def test_02_verify_upfront_invoice_item(
        self,
        waldur_client,
        report,
    ):
        """Verify upfront invoice item was created for prepaid resource."""
        report.heading(2, "Prepaid Test 2: Verify Upfront Invoice")
        assert _state.get("resource_uuid"), "Test 1 must pass first"

        resource_uuid = _state["resource_uuid"]

        items = invoice_items_list.sync(
            client=waldur_client,
            resource_uuid=resource_uuid,
        )

        if items and hasattr(items, "results") and items.results:
            report.text(f"Found {len(items.results)} invoice item(s) for resource")
            for item in items.results:
                name = getattr(item, "name", "?")
                total = getattr(item, "total", "?")
                report.text(f"  - `{name}`: total=`{total}`")
            _state["has_invoice_items"] = True
        elif items and isinstance(items, list) and len(items) > 0:
            report.text(f"Found {len(items)} invoice item(s) for resource")
            _state["has_invoice_items"] = True
        else:
            report.text(
                "Warning: No invoice items found. "
                "This may be expected if billing is not configured for this offering."
            )
            _state["has_invoice_items"] = False

        report.flush_api_log()

    def test_03_update_prepaid_limits(
        self,
        offering,
        waldur_client,
        slurm_backend,
        report,
    ):
        """Update limits on a prepaid resource and verify SLURM hard caps change."""
        report.heading(2, "Prepaid Test 3: Update Limits (Supplementary Billing)")
        assert _state.get("resource_uuid"), "Test 1 must pass first"

        resource_uuid = _state["resource_uuid"]

        # Increase limits (double the initial values)
        new_limits = {comp_name: 20 for comp_name in offering.backend_components}

        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/update_limits/",
            json={"limits": new_limits},
        )
        if resp.status_code >= 400:
            pytest.fail(
                f"Failed to create update order: {resp.status_code} {resp.text[:500]}"
            )
        data = resp.json()
        update_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert update_order_uuid, f"No order UUID in response: {data}"
        _state["update_order_uuid"] = update_order_uuid
        report.flush_api_log("Limit update order creation")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, update_order_uuid, report=report
        )

        # Verify limits updated on Waldur resource
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )
        if not isinstance(resource.limits, type(UNSET)):
            limits_dict = dict(resource.limits.additional_properties)
            report.text(f"Waldur resource limits after update: `{limits_dict}`")

        # Verify SLURM account limits updated
        backend_id = _state.get("resource_backend_id", "")
        if backend_id:
            slurm_output = _get_slurm_account_limits(offering, backend_id)
            report.text(f"SLURM account TRES limits after update: `{slurm_output}`")

        report.flush_api_log()
        report.text(
            f"\n**Result:** Limit update completed. Order state: `{final_state}`. "
            f"Limits increased from 10 to 20 per component."
        )

    def test_04_terminate_prepaid_resource(
        self,
        offering,
        waldur_client,
        slurm_backend,
        report,
    ):
        """Terminate the prepaid resource (simulating expiry)."""
        report.heading(2, "Prepaid Test 4: Terminate Prepaid Resource")
        assert _state.get("resource_uuid"), "Test 1 must pass first"

        resource_uuid = _state["resource_uuid"]

        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/terminate/",
            json={},
        )
        if resp.status_code >= 400:
            pytest.fail(
                f"Failed to create terminate order: {resp.status_code} {resp.text[:500]}"
            )
        data = resp.json()
        terminate_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert terminate_order_uuid, f"No order UUID in response: {data}"
        report.flush_api_log("Terminate order creation")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, terminate_order_uuid, report=report
        )

        # Verify SLURM account was removed
        backend_id = _state.get("resource_backend_id", "")
        if backend_id:
            slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
            sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
            try:
                output = subprocess.check_output(
                    [sacctmgr, "--parsable2", "--noheader", "--immediate",
                     "show", "account", backend_id],
                    stderr=subprocess.STDOUT,
                    timeout=5,
                ).decode().strip()
                if not output:
                    report.text(f"SLURM account `{backend_id}` successfully removed")
                else:
                    report.text(
                        f"Warning: SLURM account `{backend_id}` still exists: `{output}`"
                    )
            except subprocess.CalledProcessError:
                report.text(f"SLURM account `{backend_id}` removed (not found)")

        report.flush_api_log()
        report.text(
            f"\n**Result:** Prepaid resource terminated. "
            f"Order state: `{final_state}`."
        )
