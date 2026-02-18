"""End-to-end usage sync tests for Waldur B -> Waldur A federation.

Validates the reporting pipeline:
  1. Submit component usage on Waldur B (target)
  2. Submit per-user component usage on Waldur B
  3. Run OfferingReportProcessor to sync usage to Waldur A (source)
  4. Verify total and per-user usages on Waldur A with correct
     reverse component conversion

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>

Usage:
    WALDUR_E2E_TESTS=true \
    WALDUR_E2E_CONFIG=<config.yaml> \
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_usage_sync.py -v -s
"""

from __future__ import annotations

import logging
import os

import pytest

from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_list,
    marketplace_component_usages_set_usage,
    marketplace_component_usages_set_user_usage,
)
from waldur_api_client.api.marketplace_component_user_usages import (
    marketplace_component_user_usages_list,
)
from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_list,
)
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.api.marketplace_resources import marketplace_resources_retrieve
from waldur_api_client.models import ComponentUsageCreateRequest, ComponentUsageItemRequest
from waldur_api_client.models.component_user_usage_create_request import (
    ComponentUserUsageCreateRequest,
)
from waldur_api_client.models.marketplace_provider_resources_list_state_item import (
    MarketplaceProviderResourcesListStateItem,
)
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import OfferingReportProcessor

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests
_state: dict = {}


def _find_usable_resource(waldur_client_a, waldur_client_b, offering_uuid):
    """Find a resource on Waldur A with backend_id whose Waldur B counterpart is OK."""
    resources = marketplace_provider_resources_list.sync_all(
        client=waldur_client_a,
        offering_uuid=[offering_uuid],
        state=[MarketplaceProviderResourcesListStateItem("OK")],
    )
    for r in resources:
        backend_id = r.backend_id
        if not backend_id or isinstance(backend_id, type(UNSET)) or not backend_id.strip():
            continue
        # Verify the resource on Waldur B is also in a valid state
        resource_uuid_b = backend_id.replace("-", "")
        try:
            resource_b = marketplace_resources_retrieve.sync(
                client=waldur_client_b, uuid=resource_uuid_b
            )
            state_b = str(resource_b.state) if not isinstance(resource_b.state, type(UNSET)) else ""
            if state_b == "OK":
                return r
            logger.info("Skipping resource %s — Waldur B state is %s", r.name, state_b)
        except Exception:
            logger.info("Skipping resource %s — cannot fetch from Waldur B", r.name)
    return None


def _find_offering_user_username(waldur_client_b, target_offering_uuid, resource_uuid):
    """Find a username from offering users on the target offering."""
    offering_users = marketplace_offering_users_list.sync_all(
        client=waldur_client_b,
        query=target_offering_uuid,
    )
    for ou in offering_users:
        username = ou.username if not isinstance(ou.username, type(UNSET)) else None
        if username:
            return username
    return None


def _get_component_usage_uuid(waldur_client, resource_uuid, component_type):
    """Get the UUID of a component usage record for a given resource and type."""
    usages = marketplace_component_usages_list.sync_all(
        client=waldur_client,
        resource_uuid=resource_uuid,
    )
    for u in usages:
        u_type = u.type_ if not isinstance(u.type_, type(UNSET)) else None
        if u_type == component_type:
            return u.uuid.hex if hasattr(u.uuid, "hex") else str(u.uuid)
    return None


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestE2EUsageSync:
    """Usage sync tests: submit on Waldur B, run report processor, verify on Waldur A."""

    def test_find_resource_for_usage(
        self, waldur_client_a, waldur_client_b, offering, report
    ):
        """Find a resource on Waldur A with a valid backend_id for usage testing."""
        report.heading(2, "Usage Sync: Find Resource")

        resource = _find_usable_resource(
            waldur_client_a, waldur_client_b, offering.waldur_offering_uuid
        )
        assert resource is not None, (
            "No OK resource with backend_id found on both Waldur A and B"
        )

        resource_uuid_a = resource.uuid.hex if hasattr(resource.uuid, "hex") else str(resource.uuid)
        backend_id = resource.backend_id

        _state["resource_uuid_a"] = resource_uuid_a
        _state["resource_backend_id"] = backend_id
        _state["resource_name"] = resource.name or "(unnamed)"

        report.status_snapshot(
            "Resource for usage test",
            {
                "name": _state["resource_name"],
                "uuid_on_A": resource_uuid_a,
                "backend_id (uuid_on_B)": backend_id,
            },
        )
        report.text(f"Selected resource `{_state['resource_name']}` for usage sync test.")

    def test_submit_total_usage_on_waldur_b(
        self, waldur_client_b, offering, report
    ):
        """Submit total component usage on Waldur B."""
        report.heading(2, "Usage Sync: Submit Total Usage on Waldur B")

        resource_backend_id = _state.get("resource_backend_id")
        if not resource_backend_id:
            pytest.skip("No resource found in previous test")

        # Determine target component names and usage values from config
        component_mapper = offering.backend_components_dict
        target_usages = {}
        expected_source_usages = {}

        for source_name, comp_config in component_mapper.items():
            target_components = comp_config.get("target_components", {})
            for target_name, target_conf in target_components.items():
                factor = target_conf.get("factor", 1.0)
                # Use a test value that divides cleanly
                target_value = factor * 2.0  # 2 source units worth
                target_usages[target_name] = target_value
                expected_source_usages[source_name] = 2.0

        if not target_usages:
            # Passthrough mode — no target_components mapping
            for source_name in component_mapper:
                target_usages[source_name] = 10.0
                expected_source_usages[source_name] = 10.0

        _state["target_usages"] = target_usages
        _state["expected_source_usages"] = expected_source_usages

        # Remove dashes from backend_id to get pure UUID for API
        resource_uuid_b = resource_backend_id.replace("-", "")

        usages = [
            ComponentUsageItemRequest(type_=comp, amount=amount)
            for comp, amount in target_usages.items()
        ]
        body = ComponentUsageCreateRequest(usages=usages, resource=resource_uuid_b)
        resp = marketplace_component_usages_set_usage.sync_detailed(
            client=waldur_client_b, body=body
        )

        report.status_snapshot(
            "Total usage submitted on Waldur B",
            {
                **{f"target:{k}": str(v) for k, v in target_usages.items()},
                "expected_source": str(expected_source_usages),
                "status": str(resp.status_code),
            },
        )
        assert resp.status_code == 201, f"set_usage failed: {resp.status_code}"
        report.text("Total usage submitted successfully on Waldur B.")

    def test_submit_user_usage_on_waldur_b(
        self, waldur_client_b, offering, report
    ):
        """Submit per-user component usage on Waldur B."""
        report.heading(2, "Usage Sync: Submit Per-User Usage on Waldur B")

        resource_backend_id = _state.get("resource_backend_id")
        target_usages = _state.get("target_usages", {})
        if not resource_backend_id or not target_usages:
            pytest.skip("No resource or usages from previous tests")

        resource_uuid_b = resource_backend_id.replace("-", "")
        target_offering_uuid = offering.backend_settings["target_offering_uuid"]

        # Find a username on the target offering
        username = _find_offering_user_username(
            waldur_client_b, target_offering_uuid, resource_uuid_b
        )
        if not username:
            report.text("No offering user found on Waldur B — skipping per-user usage.")
            _state["user_usage_username"] = None
            return

        _state["user_usage_username"] = username

        # User gets half of total usage
        user_target_usages = {k: v / 2.0 for k, v in target_usages.items()}
        _state["user_target_usages"] = user_target_usages

        # Compute expected source values
        component_mapper = offering.backend_components_dict
        expected_user_source = {}
        for source_name, comp_config in component_mapper.items():
            target_components = comp_config.get("target_components", {})
            for target_name, target_conf in target_components.items():
                factor = target_conf.get("factor", 1.0)
                if target_name in user_target_usages:
                    expected_user_source[source_name] = user_target_usages[target_name] / factor

        if not expected_user_source:
            # Passthrough
            expected_user_source = dict(user_target_usages)
        _state["expected_user_source"] = expected_user_source

        # Get component usage UUIDs to call set_user_usage
        for comp_type, usage_amount in user_target_usages.items():
            comp_usage_uuid = _get_component_usage_uuid(
                waldur_client_b, resource_uuid_b, comp_type
            )
            if not comp_usage_uuid:
                report.text(f"Component usage for `{comp_type}` not found on Waldur B — skipping.")
                continue

            body = ComponentUserUsageCreateRequest(
                username=username,
                usage=usage_amount,
            )
            resp = marketplace_component_usages_set_user_usage.sync_detailed(
                uuid=comp_usage_uuid, client=waldur_client_b, body=body
            )
            assert resp.status_code == 201, (
                f"set_user_usage for {comp_type} failed: {resp.status_code}"
            )

        report.status_snapshot(
            "Per-user usage submitted on Waldur B",
            {
                "username": username,
                **{f"target:{k}": str(v) for k, v in user_target_usages.items()},
                "expected_source": str(expected_user_source),
            },
        )
        report.text(f"Per-user usage submitted for `{username}` on Waldur B.")

    def test_run_report_processor(
        self, offering, waldur_client_a, backend, report
    ):
        """Run OfferingReportProcessor to sync usage from Waldur B to Waldur A.

        Processes only the target resource instead of the entire offering
        to avoid long timeouts from broken resources in the test environment.
        """
        report.heading(2, "Usage Sync: Run Report Processor")

        resource_uuid_a = _state.get("resource_uuid_a")
        if not resource_uuid_a:
            pytest.skip("No resource from previous tests")

        processor = OfferingReportProcessor(
            offering,
            waldur_client_a,
            timezone="UTC",
            resource_backend=backend,
        )

        # Fetch the specific resource and offering, then process only that one
        waldur_offering = marketplace_provider_offerings_retrieve.sync(
            client=waldur_client_a, uuid=offering.waldur_offering_uuid
        )
        waldur_resource = marketplace_provider_resources_retrieve.sync(
            client=waldur_client_a, uuid=resource_uuid_a
        )
        processor._process_resource_with_retries(waldur_resource, waldur_offering)

        report.text("Report processor completed successfully for target resource.")

    def test_verify_total_usage_on_waldur_a(self, waldur_client_a, report):
        """Verify total usage was synced to Waldur A with correct reverse conversion."""
        report.heading(2, "Usage Sync: Verify Total Usage on Waldur A")

        resource_uuid_a = _state.get("resource_uuid_a")
        expected = _state.get("expected_source_usages", {})
        if not resource_uuid_a or not expected:
            pytest.skip("No resource or expected usages from previous tests")

        usages = marketplace_component_usages_list.sync_all(
            client=waldur_client_a,
            resource_uuid=resource_uuid_a,
        )

        actual = {}
        for u in usages:
            u_type = u.type_ if not isinstance(u.type_, type(UNSET)) else None
            u_usage = float(u.usage) if not isinstance(u.usage, type(UNSET)) else 0.0
            if u_type:
                actual[u_type] = u_usage

        report.status_snapshot(
            "Total usage on Waldur A (after sync)",
            {
                **{f"actual:{k}": str(v) for k, v in actual.items()},
                **{f"expected:{k}": str(v) for k, v in expected.items()},
            },
        )

        for comp, expected_val in expected.items():
            actual_val = actual.get(comp)
            assert actual_val is not None, (
                f"Component {comp} not found in Waldur A usages"
            )
            assert abs(actual_val - expected_val) < 0.01, (
                f"Usage mismatch for {comp}: expected={expected_val}, actual={actual_val}"
            )

        report.text("Total usage on Waldur A matches expected values after reverse conversion.")

    def test_verify_user_usage_on_waldur_a(self, waldur_client_a, report):
        """Verify per-user usage was synced to Waldur A."""
        report.heading(2, "Usage Sync: Verify Per-User Usage on Waldur A")

        resource_uuid_a = _state.get("resource_uuid_a")
        username = _state.get("user_usage_username")
        expected = _state.get("expected_user_source", {})

        if not resource_uuid_a:
            pytest.skip("No resource from previous tests")
        if not username:
            report.text("No per-user usage was submitted — skipping verification.")
            return

        user_usages = marketplace_component_user_usages_list.sync_all(
            client=waldur_client_a,
            resource_uuid=resource_uuid_a,
        )

        actual = {}
        for uu in user_usages:
            uu_username = (
                uu.username if not isinstance(uu.username, type(UNSET)) else None
            )
            uu_comp = (
                uu.component_type
                if not isinstance(uu.component_type, type(UNSET))
                else None
            )
            uu_usage = float(uu.usage) if not isinstance(uu.usage, type(UNSET)) else 0.0
            if uu_username and uu_comp:
                if uu_username not in actual:
                    actual[uu_username] = {}
                actual[uu_username][uu_comp] = uu_usage

        report.status_snapshot(
            "Per-user usage on Waldur A (after sync)",
            {
                "username": username,
                **{f"actual:{k}": str(v) for k, v in actual.get(username, {}).items()},
                **{f"expected:{k}": str(v) for k, v in expected.items()},
            },
        )

        user_actual = actual.get(username)
        if not user_actual:
            # The username on B might differ from what appears on A
            # (e.g., CUID on B resolved to display name by the agent)
            # Check if any user has matching values
            for actual_user, actual_vals in actual.items():
                if all(
                    abs(actual_vals.get(comp, -1) - exp_val) < 0.01
                    for comp, exp_val in expected.items()
                ):
                    user_actual = actual_vals
                    report.text(
                        f"Username on A is `{actual_user}` (mapped from `{username}` on B)."
                    )
                    break

        assert user_actual is not None, (
            f"No per-user usage found on Waldur A for any user matching expected values. "
            f"Expected: {expected}, Actual users: {list(actual.keys())}"
        )

        for comp, expected_val in expected.items():
            actual_val = user_actual.get(comp)
            assert actual_val is not None, (
                f"Component {comp} not found in per-user usage on Waldur A"
            )
            assert abs(actual_val - expected_val) < 0.01, (
                f"Per-user usage mismatch for {comp}: expected={expected_val}, actual={actual_val}"
            )

        report.text("Per-user usage on Waldur A matches expected values.")

    def test_usage_sync_summary(self, report):
        """Summary of the usage sync e2e test."""
        report.heading(2, "Usage Sync: Summary")

        target_usages = _state.get("target_usages", {})
        expected_source = _state.get("expected_source_usages", {})
        username = _state.get("user_usage_username")
        expected_user_source = _state.get("expected_user_source", {})

        report.text("**Component mapping (reverse conversion):**\n")
        report.text("| Target Component | Target Value | Source Component | Source Value |")
        report.text("|-----------------|-------------|-----------------|-------------|")
        for target_comp, target_val in target_usages.items():
            source_comp = None
            source_val = None
            for sc, sv in expected_source.items():
                source_comp = sc
                source_val = sv
                break
            report.text(
                f"| {target_comp} | {target_val} | {source_comp or '?'} | {source_val or '?'} |"
            )

        if username:
            report.text(f"\n**Per-user:** `{username}`")
            for comp, val in expected_user_source.items():
                report.text(f"  - {comp}: {val}")

        report.text("\nUsage sync e2e test completed.")
