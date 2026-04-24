"""End-to-end tests for SLURM resource restoration from TERMINATED state.

Validates two recovery scenarios using the SLURM emulator:
  1. Hard-delete recovery (soft_delete=False): account was removed by sacctmgr,
     RESTORE order recreates it with the same backend_id.
  2. Soft-delete recovery (soft_delete=True): account was preserved with zeroed
     limits, RESTORE order detects it exists and completes immediately.

Both paths verify that the original backend_id (SLURM account name) is preserved
and that user associations are re-created after restore.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=e2e-local-config.yaml \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_restore.py -v -s
"""

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from pathlib import Path

import pytest
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import OfferingMembershipProcessor

from .conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
    snapshot_order,
    snapshot_resource,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

_state: dict = {}


def _sacctmgr_cmd(offering) -> str:
    """Return the path to the sacctmgr binary (emulator)."""
    slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    return str(Path(slurm_bin_path) / "sacctmgr")


def _account_exists(offering, backend_id: str) -> bool:
    """Check if a SLURM account exists in the emulator."""
    sacctmgr = _sacctmgr_cmd(offering)
    try:
        output = subprocess.check_output(
            [sacctmgr, "--parsable2", "--noheader", "--immediate",
             "show", "account", backend_id],
            stderr=subprocess.STDOUT,
            timeout=5,
        ).decode().strip()
        return bool(output and backend_id in output)
    except subprocess.CalledProcessError:
        return False


def _get_account_limits(offering, backend_id: str) -> str:
    """Get GrpTRESMins for a SLURM account from the emulator."""
    sacctmgr = _sacctmgr_cmd(offering)
    try:
        output = subprocess.check_output(
            [sacctmgr, "--parsable2", "--noheader", "--immediate",
             "show", "account", backend_id, "format=Account,GrpTRESMins"],
            stderr=subprocess.STDOUT,
            timeout=5,
        ).decode().strip()
        return output
    except subprocess.CalledProcessError:
        return ""


def _restore_resource(waldur_client, resource_uuid: str) -> str:
    """Call the restore action on a marketplace provider resource, return order UUID."""
    resp = waldur_client.get_httpx_client().post(
        f"/api/marketplace-provider-resources/{resource_uuid}/restore/",
        json={},
    )
    if resp.status_code >= 400:
        pytest.fail(
            f"Failed to create restore order: {resp.status_code} {resp.text[:500]}"
        )
    data = resp.json()
    order_uuid = data.get("order_uuid") or data.get("uuid", "")
    assert order_uuid, f"No order UUID in restore response: {data}"
    return order_uuid


# ---------------------------------------------------------------------------
# Test class 1: Restore after hard-delete (default, soft_delete=False)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestRestoreAfterHardDelete:
    """Full lifecycle: CREATE → TERMINATE (hard-delete) → RESTORE.

    After termination the SLURM account is removed. The RESTORE order
    must recreate the account with the same backend_id.
    """

    def test_01_create_resource(
        self, offering, waldur_client, slurm_backend, project_uuid, report,
    ):
        """CREATE a SLURM resource and record its backend_id."""
        report.heading(2, "Restore E2E: Step 1 — Create Resource")

        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        limits = {comp_name: 10 for comp_name in offering.backend_components}
        resource_name = f"e2e-restore-{uuid.uuid4().hex[:6]}"

        order_uuid = create_source_order(
            client=waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        report.flush_api_log("Create order")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"CREATE order ended {final_state}"

        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )

        assert resource.backend_id, "Resource should have a backend_id after creation"
        assert _account_exists(offering, resource.backend_id), (
            f"SLURM account {resource.backend_id} should exist after creation"
        )

        _state["resource_uuid"] = resource_uuid
        _state["backend_id"] = resource.backend_id

        report.text(f"Resource `{resource_uuid}` created with backend_id `{resource.backend_id}`")
        snapshot_resource(report, waldur_client, resource_uuid, "Resource after CREATE")
        report.flush_api_log()

    def test_02_terminate_resource(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """TERMINATE the resource — hard-deletes the SLURM account."""
        resource_uuid = _state.get("resource_uuid")
        backend_id = _state.get("backend_id")
        assert resource_uuid, "test_01 must pass first"

        report.heading(2, "Restore E2E: Step 2 — Terminate (hard-delete)")

        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/terminate/",
            json={},
        )
        if resp.status_code >= 400:
            pytest.fail(f"Terminate failed: {resp.status_code} {resp.text[:500]}")
        data = resp.json()
        term_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert term_order_uuid, f"No order UUID in terminate response: {data}"
        report.flush_api_log("Terminate order")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, term_order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"TERMINATE order ended {final_state}"

        # Verify the account was removed from SLURM
        assert not _account_exists(offering, backend_id), (
            f"SLURM account {backend_id} should NOT exist after hard-delete termination"
        )

        # Verify resource is TERMINATED in Waldur
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )
        assert str(resource.state).lower() == "terminated", (
            f"Resource should be Terminated, got {resource.state}"
        )

        report.text(f"Resource terminated. Account `{backend_id}` removed from SLURM.")
        snapshot_resource(report, waldur_client, resource_uuid, "Resource after TERMINATE")
        report.flush_api_log()

    def test_03_restore_resource(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """RESTORE the terminated resource — recreates account with same backend_id."""
        resource_uuid = _state.get("resource_uuid")
        original_backend_id = _state.get("backend_id")
        assert resource_uuid, "test_02 must pass first"

        report.heading(2, "Restore E2E: Step 3 — Restore (recreate with same backend_id)")

        restore_order_uuid = _restore_resource(waldur_client, resource_uuid)
        report.flush_api_log("Restore order creation")

        snapshot_order(report, waldur_client, restore_order_uuid, "Restore order — initial")
        report.flush_api_log()

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, restore_order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"RESTORE order ended {final_state}"

        # Verify the account was recreated in SLURM with the SAME name
        assert _account_exists(offering, original_backend_id), (
            f"SLURM account {original_backend_id} should be recreated after restore"
        )

        # Verify resource is back to OK in Waldur with the same backend_id
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )
        assert resource.backend_id == original_backend_id, (
            f"backend_id should be preserved: expected {original_backend_id}, "
            f"got {resource.backend_id}"
        )

        report.text(
            f"**Result:** Resource restored. Account `{original_backend_id}` "
            f"recreated in SLURM with same name. Order state: `{final_state}`."
        )
        snapshot_resource(report, waldur_client, resource_uuid, "Resource after RESTORE")
        report.flush_api_log()

    def test_04_membership_sync_after_restore(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """Membership sync re-creates SLURM user associations after restore."""
        backend_id = _state.get("backend_id")
        assert backend_id, "test_03 must pass first"

        report.heading(2, "Restore E2E: Step 4 — Membership sync after restore")

        processor = OfferingMembershipProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )
        processor.process_offering()
        report.flush_api_log("Membership sync")

        # Collect offering user usernames
        offering_users = processor._get_cached_offering_users()
        usernames = [
            ou.username
            for ou in offering_users
            if not isinstance(ou.username, type(UNSET)) and ou.username
        ]

        if not usernames:
            report.text("No offering users with usernames — skipping association check.")
            return

        # Verify SLURM associations exist for each user on the restored account
        for uname in usernames:
            assoc = slurm_backend.client.get_association(uname, backend_id)
            assert assoc is not None, (
                f"SLURM association {uname}@{backend_id} should exist after restore + membership sync"
            )

        report.text(
            f"**Result:** {len(usernames)} user associations verified on "
            f"restored account `{backend_id}`: {', '.join(usernames)}"
        )
        report.flush_api_log()


# ---------------------------------------------------------------------------
# Test class 2: Restore after soft-delete (soft_delete=True)
# ---------------------------------------------------------------------------

_soft_state: dict = {}


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestRestoreAfterSoftDelete:
    """Full lifecycle: CREATE → TERMINATE (soft-delete) → RESTORE.

    With soft_delete enabled, the SLURM account is preserved with zeroed
    limits. The RESTORE order detects it exists and completes immediately
    without recreating the account.

    NOTE: This test requires offering backend_settings to include
    ``soft_delete: true``. If not configured, the test is skipped.
    """

    def test_01_create_resource(
        self, offering, waldur_client, slurm_backend, project_uuid, report,
    ):
        """CREATE a resource for soft-delete testing."""
        if not offering.backend_settings.get("soft_delete"):
            pytest.skip("soft_delete not enabled in backend_settings")

        report.heading(2, "Soft-Delete Restore E2E: Step 1 — Create Resource")

        offering_url, plan_url = get_offering_info(
            waldur_client, offering.waldur_offering_uuid
        )
        project_url = get_project_url(waldur_client, project_uuid)

        limits = {comp_name: 10 for comp_name in offering.backend_components}
        resource_name = f"e2e-softdel-{uuid.uuid4().hex[:6]}"

        order_uuid = create_source_order(
            client=waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        report.flush_api_log("Create order")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"CREATE order ended {final_state}"

        order = marketplace_orders_retrieve.sync(
            client=waldur_client, uuid=order_uuid
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )

        _soft_state["resource_uuid"] = resource_uuid
        _soft_state["backend_id"] = resource.backend_id

        report.text(f"Resource created: `{resource_uuid}`, backend_id: `{resource.backend_id}`")
        report.flush_api_log()

    def test_02_terminate_soft_delete(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """TERMINATE with soft_delete — account preserved, limits zeroed."""
        if not offering.backend_settings.get("soft_delete"):
            pytest.skip("soft_delete not enabled in backend_settings")

        resource_uuid = _soft_state.get("resource_uuid")
        backend_id = _soft_state.get("backend_id")
        assert resource_uuid, "test_01 must pass first"

        report.heading(2, "Soft-Delete Restore E2E: Step 2 — Terminate (soft-delete)")

        resp = waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/terminate/",
            json={},
        )
        if resp.status_code >= 400:
            pytest.fail(f"Terminate failed: {resp.status_code} {resp.text[:500]}")
        data = resp.json()
        term_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        report.flush_api_log("Terminate order")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, term_order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"TERMINATE order ended {final_state}"

        # Account should STILL exist (soft-delete preserves it)
        assert _account_exists(offering, backend_id), (
            f"SLURM account {backend_id} should still exist after soft-delete"
        )

        report.text(f"Account `{backend_id}` preserved after soft-delete termination.")
        report.flush_api_log()

    def test_03_restore_after_soft_delete(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """RESTORE after soft-delete — account exists, completes immediately."""
        if not offering.backend_settings.get("soft_delete"):
            pytest.skip("soft_delete not enabled in backend_settings")

        resource_uuid = _soft_state.get("resource_uuid")
        original_backend_id = _soft_state.get("backend_id")
        assert resource_uuid, "test_02 must pass first"

        report.heading(2, "Soft-Delete Restore E2E: Step 3 — Restore (account exists)")

        restore_order_uuid = _restore_resource(waldur_client, resource_uuid)
        report.flush_api_log("Restore order creation")

        final_state = run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, restore_order_uuid, report=report
        )
        assert final_state == OrderState.DONE, f"RESTORE order ended {final_state}"

        # Account should still exist with same name
        assert _account_exists(offering, original_backend_id), (
            f"SLURM account {original_backend_id} should exist after restore"
        )

        resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=waldur_client
        )
        assert resource.backend_id == original_backend_id, (
            f"backend_id should be preserved: expected {original_backend_id}, "
            f"got {resource.backend_id}"
        )

        report.text(
            f"**Result:** Resource restored immediately (account existed). "
            f"backend_id `{original_backend_id}` preserved."
        )
        snapshot_resource(report, waldur_client, resource_uuid, "Resource after RESTORE")
        report.flush_api_log()

    def test_04_membership_sync_after_soft_restore(
        self, offering, waldur_client, slurm_backend, report,
    ):
        """Membership sync re-creates SLURM user associations after soft-delete restore."""
        if not offering.backend_settings.get("soft_delete"):
            pytest.skip("soft_delete not enabled in backend_settings")

        backend_id = _soft_state.get("backend_id")
        assert backend_id, "test_03 must pass first"

        report.heading(2, "Soft-Delete Restore E2E: Step 4 — Membership sync after restore")

        processor = OfferingMembershipProcessor(
            offering=offering,
            waldur_rest_client=waldur_client,
            resource_backend=slurm_backend,
        )
        processor.process_offering()
        report.flush_api_log("Membership sync")

        offering_users = processor._get_cached_offering_users()
        usernames = [
            ou.username
            for ou in offering_users
            if not isinstance(ou.username, type(UNSET)) and ou.username
        ]

        if not usernames:
            report.text("No offering users with usernames — skipping association check.")
            return

        for uname in usernames:
            assoc = slurm_backend.client.get_association(uname, backend_id)
            assert assoc is not None, (
                f"SLURM association {uname}@{backend_id} should exist after "
                f"soft-delete restore + membership sync"
            )

        report.text(
            f"**Result:** {len(usernames)} user associations verified on "
            f"restored account `{backend_id}`: {', '.join(usernames)}"
        )
        report.flush_api_log()
